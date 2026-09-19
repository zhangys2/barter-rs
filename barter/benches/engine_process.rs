//! Criterion: Engine::process per market message and small burst.
use barter::{
    EngineEvent,
    engine::{
        Engine, Processor,
        clock::HistoricalClock,
        execution_tx::MultiExchangeTxMap,
        state::{
            builder::EngineStateBuilder, global::DefaultGlobalData,
            instrument::data::DefaultInstrumentMarketData, trading::TradingState,
        },
    },
    execution::request::ExecutionRequest,
    risk::DefaultRiskManager,
    strategy::DefaultStrategy,
};
use barter_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
    subscription::trade::PublicTrade,
};
use barter_instrument::{
    Side, Underlying,
    exchange::ExchangeId,
    index::IndexedInstruments,
    instrument::{
        Instrument, InstrumentIndex,
        spec::{
            InstrumentSpec, InstrumentSpecNotional, InstrumentSpecPrice, InstrumentSpecQuantity,
            OrderQuantityUnits,
        },
    },
};
use barter_integration::channel::{UnboundedTx, mpsc_unbounded};
use chrono::{DateTime, Utc};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rust_decimal_macros::dec;

const START: DateTime<Utc> = DateTime::<Utc>::MIN_UTC;

fn market_trade(price: f64) -> EngineEvent<DataKind> {
    EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
        time_exchange: START,
        time_received: START,
        exchange: ExchangeId::BinanceSpot,
        instrument: InstrumentIndex(0),
        kind: DataKind::Trade(PublicTrade {
            id: "1".into(),
            price,
            amount: 1.0,
            side: Side::Buy,
        }),
    }))
}

type BenchEngine = Engine<
    HistoricalClock,
    barter::engine::state::EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
    MultiExchangeTxMap<UnboundedTx<ExecutionRequest>>,
    DefaultStrategy<
        barter::engine::state::EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
    >,
    DefaultRiskManager<
        barter::engine::state::EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
    >,
>;

fn build_engine() -> (BenchEngine, UnboundedTx<ExecutionRequest>) {
    let instruments = IndexedInstruments::builder()
        .add_instrument(Instrument::spot(
            ExchangeId::BinanceSpot,
            "binance_spot_btc_usdt",
            "BTCUSDT",
            Underlying::new("btc", "usdt"),
            Some(InstrumentSpec::new(
                InstrumentSpecPrice::new(dec!(0.01), dec!(0.01)),
                InstrumentSpecQuantity::new(
                    OrderQuantityUnits::Quote,
                    dec!(0.00001),
                    dec!(0.00001),
                ),
                InstrumentSpecNotional::new(dec!(5.0)),
            )),
        ))
        .build();
    let (execution_tx, _execution_rx) = mpsc_unbounded();
    let state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(START)
    .trading_state(TradingState::Disabled)
    .build();
    let engine = Engine::new(
        HistoricalClock::new(START),
        state,
        MultiExchangeTxMap::from_iter([(ExchangeId::BinanceSpot, Some(execution_tx.clone()))]),
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
    );
    (engine, execution_tx)
}

fn engine_process(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_process");
    group.warm_up_time(std::time::Duration::from_millis(300));
    group.measurement_time(std::time::Duration::from_secs(2));
    group.sample_size(20);

    group.bench_function("per_message", |b| {
        b.iter_batched(
            build_engine,
            |(mut engine, _tx)| {
                engine.process(black_box(market_trade(10_000.0)));
            },
            criterion::BatchSize::SmallInput,
        );
    });

    for burst in [1usize, 64] {
        group.throughput(Throughput::Elements(burst as u64));
        group.bench_with_input(BenchmarkId::new("burst", burst), &burst, |b, &burst| {
            b.iter_batched(
                build_engine,
                |(mut engine, _tx)| {
                    for i in 0..burst {
                        engine.process(market_trade(10_000.0 + i as f64));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, engine_process);
criterion_main!(benches);
