//! Gated Binance Spot Testnet System round-trip: open, cancel, reconcile Engine State.
use barter::{
    engine::{
        clock::LiveClock,
        state::{
            global::DefaultGlobalData, instrument::data::DefaultInstrumentMarketData,
            order::manager::OrderManager, trading::TradingState,
        },
    },
    risk::DefaultRiskManager,
    strategy::DefaultStrategy,
    system::{
        builder::{AuditMode, EngineFeedMode, SystemArgs, SystemBuilder},
        config::ExecutionConfig,
    },
};
use barter_data::{event::DataKind, streams::consumer::MarketStreamEvent};
use barter_execution::{
    client::BinanceSpotConfig,
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::RequestOpen,
    },
};
use barter_instrument::{
    Side, Underlying,
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{Instrument, InstrumentIndex, name::InstrumentNameExchange},
};
use barter_integration::collection::one_or_many::OneOrMany;
use futures::stream;
use rust_decimal::Decimal;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires BINANCE_API_KEY/BINANCE_API_SECRET and explicit BARTER_LIVE_TESTS=1"]
async fn gated_testnet_system_open_cancel_reconciles_engine_state() {
    if std::env::var("BARTER_LIVE_TESTS").as_deref() != Ok("1")
        || std::env::var("BINANCE_API_KEY")
            .ok()
            .filter(|value| !value.is_empty())
            .is_none()
        || std::env::var("BINANCE_API_SECRET")
            .ok()
            .filter(|value| !value.is_empty())
            .is_none()
    {
        return;
    }

    let instrument = Instrument::spot(
        ExchangeId::BinanceSpot,
        "binance_spot-btc_usdt",
        "BTCUSDT",
        Underlying::new(
            barter_instrument::asset::Asset::new_from_exchange("btc"),
            barter_instrument::asset::Asset::new_from_exchange("usdt"),
        ),
        None,
    );
    let instruments = IndexedInstruments::new(vec![instrument]);
    let mut config = BinanceSpotConfig::testnet();
    config.instruments = vec![InstrumentNameExchange::from("BTCUSDT")];

    let args = SystemArgs::new(
        &instruments,
        vec![ExecutionConfig::BinanceSpot(config)],
        LiveClock,
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
        stream::pending::<MarketStreamEvent<InstrumentIndex, DataKind>>(),
        DefaultGlobalData,
        |_| DefaultInstrumentMarketData::default(),
    );

    let system = SystemBuilder::new(args)
        .engine_feed_mode(EngineFeedMode::Stream)
        .audit_mode(AuditMode::Disabled)
        .trading_state(TradingState::Disabled)
        .build()
        .unwrap()
        .init_with_runtime(tokio::runtime::Handle::current())
        .await
        .unwrap();

    let cid = ClientOrderId::random();
    system.send_open_requests(OneOrMany::One(OrderEvent {
        key: OrderKey {
            exchange: ExchangeIndex(0),
            instrument: InstrumentIndex(0),
            strategy: StrategyId::new("live-round-trip"),
            cid: cid.clone(),
        },
        state: RequestOpen {
            side: Side::Buy,
            price: Decimal::from(1000),
            quantity: Decimal::new(1, 5),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: true },
        },
    }));

    tokio::time::sleep(Duration::from_secs(2)).await;
    system.cancel_orders(barter::engine::state::instrument::filter::InstrumentFilter::None);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let (engine, _) = system.shutdown().await.unwrap();
    let remaining = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0))
        .orders
        .orders()
        .filter(|order| order.key.cid == cid)
        .count();
    assert_eq!(
        remaining, 0,
        "Engine State must drop the cancelled order after the venue acknowledges cancel"
    );
}
