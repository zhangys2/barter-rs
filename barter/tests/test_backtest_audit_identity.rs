//! Identical inputs produce a byte-identical Audit Stream (W2.8).
use barter::{
    EngineEvent,
    backtest::{
        IntoMockMarketKind,
        market_data::{BacktestMarketData, MarketDataInMemory},
    },
    engine::{
        Engine,
        audit::{AuditTick, EngineAudit},
        clock::HistoricalClock,
        state::builder::EngineStateBuilder,
        state::{
            global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
            trading::TradingState,
        },
    },
    execution::builder::ExecutionBuilder,
    risk::DefaultRiskManager,
    strategy::DefaultStrategy,
    system::{
        builder::{AuditMode, EngineFeedMode, SystemBuild},
        config::{ExecutionConfig, SystemConfig},
    },
};
use barter_data::{event::DataKind, streams::consumer::MarketStreamEvent};
use barter_execution::exchange::mock::MockMarketEvent;
use barter_instrument::{index::IndexedInstruments, instrument::InstrumentIndex};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::Arc,
};
use tokio::sync::oneshot;

#[derive(Deserialize)]
struct Config {
    system: SystemConfig,
}

fn canonical_audit<Output>(tick: &AuditTick<EngineAudit<EngineEvent<DataKind>, Output>>) -> String {
    match &tick.event {
        EngineAudit::FeedEnded => {
            format!(
                "{}|{}|feed_ended",
                tick.context.sequence.0, tick.context.time
            )
        }
        EngineAudit::Process(process) => {
            let tag = match &process.event {
                EngineEvent::Market(MarketStreamEvent::Item(event)) => format!(
                    "market|{}|{}|{}",
                    event.instrument.index(),
                    event.time_exchange,
                    event.kind.kind_name()
                ),
                EngineEvent::Market(MarketStreamEvent::Reconnecting(exchange)) => {
                    format!("market_reconnect|{exchange}")
                }
                EngineEvent::Account(_) => "account".into(),
                EngineEvent::Shutdown(_) => "shutdown".into(),
                EngineEvent::Command(_) => "command".into(),
                EngineEvent::TradingStateUpdate(state) => format!("trading|{state:?}"),
            };
            format!("{}|{}|{tag}", tick.context.sequence.0, tick.context.time)
        }
    }
}

async fn collect_audit_stream() -> Vec<String> {
    let config: Config = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/config/backtest_config.json"
    )))
    .unwrap();
    let events = BufReader::new(
        File::open(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/examples/data/binance_spot_trades_l1_btcusdt_ethusdt_solusdt.json"
        ))
        .unwrap(),
    )
    .lines()
    .map(|line| {
        serde_json::from_str::<MarketStreamEvent<InstrumentIndex, DataKind>>(&line.unwrap())
            .unwrap()
    })
    .collect::<Vec<_>>();
    let market_data = MarketDataInMemory::new(Arc::new(events));
    let instruments = IndexedInstruments::new(config.system.instruments);
    let time_engine_start = market_data.time_first_event().await.unwrap();
    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    let clock = HistoricalClock::new(time_engine_start);
    let market_stream = market_data.stream().await.unwrap();

    let execution = config
        .system
        .executions
        .into_iter()
        .try_fold(
            ExecutionBuilder::new(&instruments),
            |builder, cfg| match cfg {
                ExecutionConfig::Mock(mock_config) => builder.add_mock(mock_config, clock.clone()),
                ExecutionConfig::BinanceSpot(live) => builder
                    .add_live::<barter_execution::client::BinanceSpot>(
                        live,
                        std::time::Duration::from_secs(5),
                    ),
            },
        )
        .unwrap()
        .build();

    let engine = Engine::new(
        clock,
        engine_state,
        execution.execution_tx_map,
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
    );

    let instruments_for_bridge = instruments.clone();
    let mock_market_txs = execution.mock_market_txs;
    let market_stream = market_stream.filter_map(move |event| {
        let mock_event = if let barter_data::streams::reconnect::Event::Item(market_event) = &event
        {
            let sender = mock_market_txs.get(&market_event.exchange).cloned();
            let instrument = instruments_for_bridge
                .instruments()
                .get(market_event.instrument.index())
                .map(|instrument| instrument.value.name_exchange.clone());
            sender.zip(instrument).and_then(|(sender, instrument)| {
                market_event.kind.into_mock_market_kind().map(|kind| {
                    let (applied_tx, applied_rx) = oneshot::channel();
                    (
                        sender,
                        MockMarketEvent {
                            instrument,
                            time_exchange: market_event.time_exchange,
                            kind,
                            applied: Some(applied_tx),
                        },
                        applied_rx,
                    )
                })
            })
        } else {
            None
        };
        async move {
            if let Some((mut sender, mock_event, applied_rx)) = mock_event
                && sender.send(mock_event).await.is_ok()
            {
                let _ = applied_rx.await;
            }
            Some(event)
        }
    });

    let mut system = SystemBuild::new(
        engine,
        EngineFeedMode::Stream,
        AuditMode::Enabled,
        market_stream,
        execution.account_channel,
        execution.futures,
    )
    .init()
    .await
    .unwrap();

    let audit = system.audit.take().expect("audit enabled");
    let snapshot = format!("snapshot|{}", audit.snapshot.context.sequence.0);
    let collector = tokio::spawn(async move {
        let mut ticks = vec![snapshot];
        let mut stream = audit.updates.into_stream();
        while let Some(tick) = stream.next().await {
            ticks.push(canonical_audit(&tick));
        }
        ticks
    });

    let _ = system.shutdown_after_backtest().await.unwrap();
    collector.await.unwrap()
}

#[tokio::test]
async fn identical_backtests_have_byte_identical_audit_streams() {
    let first = collect_audit_stream().await;
    let second = collect_audit_stream().await;
    assert_eq!(first, second);
    assert!(!first.is_empty());
}
