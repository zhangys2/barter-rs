/// Backtesting utilities for algorithmic trading strategies.
///
/// This module provides tools for running historical simulations of trading strategies
/// using market data, and analyzing the performance of these simulations.
use crate::{
    backtest::{
        market_data::BacktestMarketData,
        summary::{BacktestSummary, MultiBacktestSummary},
    },
    engine::{
        Processor,
        clock::HistoricalClock,
        execution_tx::MultiExchangeTxMap,
        state::{EngineState, instrument::data::InstrumentDataState},
    },
    error::BarterError,
    risk::RiskManager,
    statistic::time::TimeInterval,
    strategy::{
        algo::AlgoStrategy, close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy, on_trading_disabled::OnTradingDisabled,
    },
    system::{builder::EngineFeedMode, config::ExecutionConfig},
};
use crate::{
    engine::Engine,
    execution::builder::{ExecutionBuild, ExecutionBuilder},
    system::builder::{AuditMode, SystemBuild},
};
use barter_data::{
    books::Level,
    event::{DataKind, MarketEvent},
    subscription::book::OrderBookEvent,
};
use barter_execution::{
    AccountEvent,
    exchange::mock::{MockMarketEvent, MockMarketEventKind, MockMarketLevel},
};
use barter_instrument::{
    exchange::ExchangeId, index::IndexedInstruments, instrument::InstrumentIndex,
};
use fnv::FnvHashMap;
use futures::{SinkExt, StreamExt, future::try_join_all};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use smol_str::SmolStr;
use std::{fmt::Debug, sync::Arc};
use tokio::sync::oneshot;

fn accept_market_timestamp(
    latest_by_instrument: &mut FnvHashMap<
        (ExchangeId, InstrumentIndex),
        chrono::DateTime<chrono::Utc>,
    >,
    exchange: ExchangeId,
    instrument: InstrumentIndex,
    current: chrono::DateTime<chrono::Utc>,
) -> bool {
    let previous = latest_by_instrument.get(&(exchange, instrument));
    if previous.is_some_and(|previous| current < *previous) {
        return false;
    }
    latest_by_instrument.insert((exchange, instrument), current);
    true
}

/// Defines the interface and implementations for different types of market data sources
/// that can be used in backtests.
pub mod market_data;

/// Contains data structures for representing backtest results and metrics.
pub mod summary;

#[allow(clippy::wrong_self_convention)]
pub trait IntoMockMarketKind {
    fn into_mock_market_kind(&self) -> Option<MockMarketEventKind>;
}

impl IntoMockMarketKind for DataKind {
    fn into_mock_market_kind(&self) -> Option<MockMarketEventKind> {
        match self {
            DataKind::Trade(trade) => Some(MockMarketEventKind::Trade {
                price: Decimal::from_f64(trade.price)?,
                quantity: Decimal::from_f64(trade.amount)?.abs(),
            }),
            DataKind::OrderBookL1(book) => Some(MockMarketEventKind::OrderBook {
                bids: book
                    .best_bid
                    .map(|level| {
                        vec![MockMarketLevel {
                            price: level.price,
                            quantity: level.amount,
                        }]
                    })
                    .unwrap_or_default(),
                asks: book
                    .best_ask
                    .map(|level| {
                        vec![MockMarketLevel {
                            price: level.price,
                            quantity: level.amount,
                        }]
                    })
                    .unwrap_or_default(),
            }),
            DataKind::OrderBook(OrderBookEvent::Snapshot(book))
            | DataKind::OrderBook(OrderBookEvent::Update(book)) => {
                let bids = book
                    .bids()
                    .levels()
                    .iter()
                    .map(|level: &Level| MockMarketLevel {
                        price: level.price,
                        quantity: level.amount,
                    })
                    .collect();
                let asks = book
                    .asks()
                    .levels()
                    .iter()
                    .map(|level: &Level| MockMarketLevel {
                        price: level.price,
                        quantity: level.amount,
                    })
                    .collect();
                Some(MockMarketEventKind::OrderBook { bids, asks })
            }
            _ => None,
        }
    }
}

/// Configuration for constants used across all backtests in a batch.
///
/// Contains shared inputs like instruments, execution configurations,
/// market data, and summary time intervals.
#[derive(Debug, Clone)]
pub struct BacktestArgsConstant<MarketData, SummaryInterval, State> {
    /// Set of trading instruments indexed by unique identifiers.
    pub instruments: IndexedInstruments,
    /// Exchange execution configurations.
    pub executions: Vec<ExecutionConfig>,
    /// Historical market data to use for simulation.
    pub market_data: MarketData,
    /// Time interval for aggregating and reporting summary statistics.
    pub summary_interval: SummaryInterval,
    /// EngineState.
    pub engine_state: State,
}

/// Configuration for variables that can change between individual backtests.
///
/// Contains parameters that define a specific strategy variant to test.
#[derive(Debug, Clone)]
pub struct BacktestArgsDynamic<Strategy, Risk> {
    /// Unique identifier for this backtest.
    pub id: SmolStr,
    /// Risk-free return rate used for performance metrics.
    pub risk_free_return: Decimal,
    /// Trading strategy to backtest.
    pub strategy: Strategy,
    /// Risk management rules.
    pub risk: Risk,
}
/// Run multiple backtests concurrently, each with different strategy parameters.
///
/// Takes the shared constants and an iterator of different strategy configurations,
/// then executes all backtests in parallel, collecting the results.
pub async fn run_backtests<
    MarketData,
    SummaryInterval,
    Strategy,
    Risk,
    GlobalData,
    InstrumentData,
>(
    args_constant: Arc<
        BacktestArgsConstant<MarketData, SummaryInterval, EngineState<GlobalData, InstrumentData>>,
    >,
    args_dynamic_iter: impl IntoIterator<Item = BacktestArgsDynamic<Strategy, Risk>>,
) -> Result<MultiBacktestSummary<SummaryInterval>, BarterError>
where
    MarketData: BacktestMarketData<Kind = InstrumentData::MarketEventKind>,
    SummaryInterval: TimeInterval,
    Strategy: AlgoStrategy<State = EngineState<GlobalData, InstrumentData>>
        + ClosePositionsStrategy<State = EngineState<GlobalData, InstrumentData>>
        + OnTradingDisabled<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + OnDisconnectStrategy<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + Send
        + 'static,
    <Strategy as OnTradingDisabled<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnTradingDisabled: Debug + Clone + Send,
    <Strategy as OnDisconnectStrategy<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnDisconnect: Debug + Clone + Send,
    Risk: RiskManager<State = EngineState<GlobalData, InstrumentData>> + Send + 'static,
    GlobalData: for<'a> Processor<&'a MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>>
        + for<'a> Processor<&'a AccountEvent>
        + Debug
        + Clone
        + Default
        + Send
        + 'static,
    InstrumentData: InstrumentDataState + Default + Send + 'static,
    InstrumentData::MarketEventKind:
        IntoMockMarketKind + crate::system::observation::ObservationKind,
{
    let time_start = std::time::Instant::now();

    let backtest_futures = args_dynamic_iter
        .into_iter()
        .map(|args_dynamic| backtest(Arc::clone(&args_constant), args_dynamic));

    // Run all backtests concurrently
    let summaries = try_join_all(backtest_futures).await?;

    Ok(MultiBacktestSummary::new(
        std::time::Instant::now().duration_since(time_start),
        summaries,
    ))
}

/// Run a single backtest with the given parameters.
///
/// Simulates a trading strategy using historical market data and generates performance metrics.
pub async fn backtest<MarketData, SummaryInterval, Strategy, Risk, GlobalData, InstrumentData>(
    args_constant: Arc<
        BacktestArgsConstant<MarketData, SummaryInterval, EngineState<GlobalData, InstrumentData>>,
    >,
    args_dynamic: BacktestArgsDynamic<Strategy, Risk>,
) -> Result<BacktestSummary<SummaryInterval>, BarterError>
where
    MarketData: BacktestMarketData<Kind = InstrumentData::MarketEventKind>,
    SummaryInterval: TimeInterval,
    Strategy: AlgoStrategy<State = EngineState<GlobalData, InstrumentData>>
        + ClosePositionsStrategy<State = EngineState<GlobalData, InstrumentData>>
        + OnTradingDisabled<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + OnDisconnectStrategy<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + Send
        + 'static,
    <Strategy as OnTradingDisabled<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnTradingDisabled: Debug + Clone + Send,
    <Strategy as OnDisconnectStrategy<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnDisconnect: Debug + Clone + Send,
    Risk: RiskManager<State = EngineState<GlobalData, InstrumentData>> + Send + 'static,
    GlobalData: for<'a> Processor<&'a MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>>
        + for<'a> Processor<&'a AccountEvent>
        + Debug
        + Clone
        + Default
        + Send
        + 'static,
    InstrumentData: InstrumentDataState + Send + 'static,
    InstrumentData::MarketEventKind:
        IntoMockMarketKind + crate::system::observation::ObservationKind,
{
    let clock = args_constant
        .market_data
        .time_first_event()
        .await
        .map(HistoricalClock::new)?;
    let market_stream = args_constant.market_data.stream().await?;

    // Build Execution infrastructure
    let ExecutionBuild {
        execution_tx_map,
        mock_market_txs,
        account_channel,
        futures,
    } = args_constant
        .executions
        .clone()
        .into_iter()
        .try_fold(
            ExecutionBuilder::new(&args_constant.instruments),
            |builder, config| match config {
                ExecutionConfig::Mock(mock_config) => builder.add_mock(mock_config, clock.clone()),
                ExecutionConfig::BinanceSpot(config) => builder
                    .add_live::<barter_execution::client::BinanceSpot>(
                    config,
                    std::time::Duration::from_secs(5),
                ),
            },
        )?
        .build();

    // Deliver each market event to the Mock Exchange before yielding it to the Engine. Awaiting
    // the bounded send preserves event ordering and prevents a burst from silently dropping the
    // book update that determines a corresponding fill.
    let instruments = args_constant.instruments.clone();
    let mut latest_exchange_time = FnvHashMap::default();
    let market_stream = market_stream.filter_map(move |event| {
        let in_order = match &event {
            barter_data::streams::reconnect::Event::Item(market_event) => accept_market_timestamp(
                &mut latest_exchange_time,
                market_event.exchange,
                market_event.instrument,
                market_event.time_exchange,
            ),
            barter_data::streams::reconnect::Event::Reconnecting(_) => true,
        };
        let mock_event = if in_order {
            if let barter_data::streams::reconnect::Event::Item(market_event) = &event {
                let sender = mock_market_txs.get(&market_event.exchange).cloned();
                let instrument = instruments
                    .instruments()
                    .get(market_event.instrument.index())
                    .map(|instrument| instrument.value.name_exchange.clone());
                sender.zip(instrument).and_then(|(sender, instrument)| {
                    market_event.kind.into_mock_market_kind().map(|kind| {
                        (sender, {
                            let (applied_tx, applied_rx) = oneshot::channel();
                            (
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
                })
            } else {
                None
            }
        } else {
            None
        };
        async move {
            if !in_order {
                return None;
            }
            if let Some((mut sender, (mock_event, applied_rx))) = mock_event
                && sender.send(mock_event).await.is_ok()
            {
                let _ = applied_rx.await;
            }
            Some(event)
        }
    });

    let engine = Engine::new(
        clock,
        args_constant.engine_state.clone(),
        execution_tx_map,
        args_dynamic.strategy,
        args_dynamic.risk,
    );

    let system = SystemBuild::new(
        engine,
        EngineFeedMode::Stream,
        AuditMode::Disabled,
        market_stream,
        account_channel,
        futures,
    )
    .init()
    .await?;

    let (engine, _shutdown_audit) = system.shutdown_after_backtest().await?;

    let trading_summary = engine
        .trading_summary_generator(args_dynamic.risk_free_return)
        .generate(args_constant.summary_interval);

    Ok(BacktestSummary {
        id: args_dynamic.id,
        risk_free_return: args_dynamic.risk_free_return,
        trading_summary,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_data::books::OrderBook;
    use barter_data::{
        event::DataKind,
        streams::consumer::MarketStreamEvent,
        subscription::{book::OrderBookEvent, trade::PublicTrade},
    };
    use rust_decimal_macros::dec;
    use serde::Deserialize;
    use std::{
        fs::File,
        io::{BufRead, BufReader},
    };

    #[test]
    fn market_data_bridge_supports_l1_and_public_trades() {
        let l1 = DataKind::OrderBookL1(barter_data::subscription::book::OrderBookL1 {
            last_update_time: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
            best_bid: Some(Level {
                price: dec!(10),
                amount: dec!(2),
            }),
            best_ask: Some(Level {
                price: dec!(11),
                amount: dec!(3),
            }),
        });
        assert_eq!(
            l1.into_mock_market_kind(),
            Some(MockMarketEventKind::OrderBook {
                bids: vec![MockMarketLevel {
                    price: dec!(10),
                    quantity: dec!(2),
                }],
                asks: vec![MockMarketLevel {
                    price: dec!(11),
                    quantity: dec!(3),
                }],
            })
        );
        let trade = DataKind::Trade(PublicTrade {
            id: "trade-1".into(),
            price: 10.5,
            amount: 2.0,
            side: barter_instrument::Side::Buy,
        });
        assert_eq!(
            trade.into_mock_market_kind(),
            Some(MockMarketEventKind::Trade {
                price: dec!(10.5),
                quantity: dec!(2),
            })
        );
    }

    #[test]
    fn identical_ordered_market_inputs_have_byte_identical_event_streams() {
        fn encoded_stream(inputs: &[DataKind]) -> Vec<u8> {
            let events = inputs
                .iter()
                .enumerate()
                .filter_map(|(sequence, input)| {
                    input.into_mock_market_kind().map(|kind| MockMarketEvent {
                        instrument: "BTCUSDT".into(),
                        time_exchange: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH
                            + chrono::TimeDelta::seconds(sequence as i64),
                        kind,
                        applied: None,
                    })
                })
                .collect::<Vec<_>>();
            serde_json::to_vec(&events).unwrap()
        }

        fn build_inputs() -> Vec<DataKind> {
            vec![
                DataKind::OrderBookL1(barter_data::subscription::book::OrderBookL1 {
                    last_update_time: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
                    best_bid: Some(Level {
                        price: dec!(10),
                        amount: dec!(2),
                    }),
                    best_ask: Some(Level {
                        price: dec!(11),
                        amount: dec!(3),
                    }),
                }),
                DataKind::Trade(PublicTrade {
                    id: "trade-1".into(),
                    price: 10.5,
                    amount: 2.0,
                    side: barter_instrument::Side::Buy,
                }),
                DataKind::OrderBook(OrderBookEvent::Snapshot(OrderBook::new(
                    7,
                    None,
                    [(dec!(10), dec!(2)), (dec!(9), dec!(1))],
                    [(dec!(11), dec!(3)), (dec!(12), dec!(4))],
                ))),
            ]
        }

        // The public backtest result includes runtime-dependent timing fields, so compare the
        // complete deterministic bridge event streams from two independently built inputs.
        assert_eq!(
            encoded_stream(&build_inputs()),
            encoded_stream(&build_inputs())
        );

        let mut latest = FnvHashMap::default();
        let exchange = ExchangeId::BinanceSpot;
        let first_instrument = InstrumentIndex::new(0);
        let second_instrument = InstrumentIndex::new(1);
        assert!(accept_market_timestamp(
            &mut latest,
            exchange,
            first_instrument,
            chrono::DateTime::<chrono::Utc>::UNIX_EPOCH + chrono::TimeDelta::seconds(10),
        ));
        // Interleaved instruments have independent exchange-time monotonicity.
        assert!(accept_market_timestamp(
            &mut latest,
            exchange,
            second_instrument,
            chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
        ));
        assert!(!accept_market_timestamp(
            &mut latest,
            exchange,
            first_instrument,
            chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
        ));
    }

    #[tokio::test]
    async fn identical_backtests_have_byte_identical_trading_summaries() {
        #[derive(Deserialize)]
        struct Config {
            risk_free_return: Decimal,
            system: crate::system::config::SystemConfig,
        }

        async fn build_inputs() -> (
            Arc<
                BacktestArgsConstant<
                    market_data::MarketDataInMemory<DataKind>,
                    crate::statistic::time::Daily,
                    crate::engine::state::EngineState<
                        crate::engine::state::global::DefaultGlobalData,
                        crate::engine::state::instrument::data::DefaultInstrumentMarketData,
                    >,
                >,
            >,
            BacktestArgsDynamic<
                crate::strategy::DefaultStrategy<
                    crate::engine::state::EngineState<
                        crate::engine::state::global::DefaultGlobalData,
                        crate::engine::state::instrument::data::DefaultInstrumentMarketData,
                    >,
                >,
                crate::risk::DefaultRiskManager<
                    crate::engine::state::EngineState<
                        crate::engine::state::global::DefaultGlobalData,
                        crate::engine::state::instrument::data::DefaultInstrumentMarketData,
                    >,
                >,
            >,
        ) {
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
            let market_data = market_data::MarketDataInMemory::new(Arc::new(events));
            let instruments = IndexedInstruments::new(config.system.instruments);
            let time_engine_start = market_data.time_first_event().await.unwrap();
            let engine_state = crate::engine::state::builder::EngineStateBuilder::new(
                &instruments,
                crate::engine::state::global::DefaultGlobalData,
                |_| crate::engine::state::instrument::data::DefaultInstrumentMarketData::default(),
            )
            .time_engine_start(time_engine_start)
            .trading_state(crate::engine::state::trading::TradingState::Enabled)
            .build();
            let dynamic = BacktestArgsDynamic {
                id: "deterministic".into(),
                risk_free_return: config.risk_free_return,
                strategy: crate::strategy::DefaultStrategy::default(),
                risk: crate::risk::DefaultRiskManager::default(),
            };
            (
                Arc::new(BacktestArgsConstant {
                    instruments,
                    executions: config.system.executions,
                    market_data,
                    summary_interval: crate::statistic::time::Daily,
                    engine_state,
                }),
                dynamic,
            )
        }

        let (first_constant, first_dynamic) = build_inputs().await;
        let (second_constant, second_dynamic) = build_inputs().await;
        let first = backtest(first_constant, first_dynamic).await.unwrap();
        let second = backtest(second_constant, second_dynamic).await.unwrap();
        fn canonical_summary_bytes(
            summary: &crate::statistic::summary::TradingSummary<crate::statistic::time::Daily>,
        ) -> Vec<u8> {
            let mut instruments: Vec<_> = summary
                .instruments
                .iter()
                .map(|(key, value)| (format!("{key:?}"), value))
                .collect();
            instruments.sort_by(|left, right| left.0.cmp(&right.0));
            let mut assets: Vec<_> = summary
                .assets
                .iter()
                .map(|(key, value)| (format!("{key:?}"), value))
                .collect();
            assets.sort_by(|left, right| left.0.cmp(&right.0));
            serde_json::to_vec(&(
                summary.time_engine_start,
                summary.time_engine_end,
                instruments,
                assets,
            ))
            .unwrap()
        }

        assert_eq!(
            canonical_summary_bytes(&first.trading_summary),
            canonical_summary_bytes(&second.trading_summary),
        );
    }

    #[test]
    fn l2_backtest_bridge_preserves_sorted_levels_deterministically() {
        let book = OrderBook::new(
            7,
            None,
            [(dec!(10), dec!(2)), (dec!(9), dec!(1))],
            [(dec!(11), dec!(3)), (dec!(12), dec!(4))],
        );
        let kind = DataKind::OrderBook(OrderBookEvent::Snapshot(book));
        let MockMarketEventKind::OrderBook { bids, asks } = kind.into_mock_market_kind().unwrap()
        else {
            panic!("expected L2 order book");
        };
        assert_eq!(bids[0].price, dec!(10));
        assert_eq!(asks[0].price, dec!(11));
    }
}
