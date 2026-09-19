//! Paper-Trading (Mock) vs Live-Trading (Binance Spot) by changing only `BINANCE_MODE`.
//!
//! - `BINANCE_MODE=paper` (default): live market data + Mock execution
//! - `BINANCE_MODE=live`: live market data + Binance Spot Testnet execution
//!
//! Live mode reads credentials from `BINANCE_API_KEY` / `BINANCE_API_SECRET`.
use barter::{
    engine::{
        clock::LiveClock,
        state::{
            global::DefaultGlobalData,
            instrument::{data::DefaultInstrumentMarketData, filter::InstrumentFilter},
            order::manager::OrderManager,
            trading::TradingState,
        },
    },
    logging::init_logging,
    risk::DefaultRiskManager,
    strategy::DefaultStrategy,
    system::{
        builder::{AuditMode, EngineFeedMode, SystemArgs, SystemBuilder},
        config::{ExecutionConfig, SystemConfig},
    },
};
use barter_data::{
    streams::builder::dynamic::indexed::init_indexed_multi_exchange_market_stream,
    subscription::SubKind,
};
use barter_execution::{
    client::BinanceSpotConfig,
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::RequestOpen,
    },
};
use barter_instrument::{
    Side, exchange::ExchangeIndex, index::IndexedInstruments, instrument::InstrumentIndex,
};
use barter_integration::collection::one_or_many::OneOrMany;
use rust_decimal::Decimal;
use std::{fs::File, io::BufReader, time::Duration};

const FILE_PATH_SYSTEM_CONFIG: &str = "barter/examples/config/system_config.json";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let live = std::env::var("BINANCE_MODE").as_deref() == Ok("live");
    let SystemConfig {
        instruments,
        executions,
    } = load_config()?;
    let instruments = IndexedInstruments::new(instruments);

    let executions = if live {
        let mut config = BinanceSpotConfig::testnet();
        config.instruments =
            vec![barter_instrument::instrument::name::InstrumentNameExchange::from("BTCUSDT")];
        vec![ExecutionConfig::BinanceSpot(config)]
    } else {
        executions
    };

    let market_stream = init_indexed_multi_exchange_market_stream(
        &instruments,
        &[SubKind::PublicTrades, SubKind::OrderBooksL1],
    )
    .await?;

    let args = SystemArgs::new(
        &instruments,
        executions,
        LiveClock,
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
        market_stream,
        DefaultGlobalData,
        |_| DefaultInstrumentMarketData::default(),
    );

    let system = SystemBuilder::new(args)
        .engine_feed_mode(EngineFeedMode::Iterator)
        .audit_mode(AuditMode::Disabled)
        .trading_state(TradingState::Disabled)
        .build()?
        .init_with_runtime(tokio::runtime::Handle::current())
        .await?;

    system.send_open_requests(OneOrMany::One(OrderEvent {
        key: OrderKey {
            exchange: ExchangeIndex(0),
            instrument: InstrumentIndex(0),
            strategy: StrategyId::new("paper-to-live"),
            cid: ClientOrderId::random(),
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
    system.cancel_orders(InstrumentFilter::None);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (engine, _shutdown) = system.shutdown().await?;
    let open = engine
        .state
        .instruments
        .instrument_index(&InstrumentIndex(0))
        .orders
        .orders()
        .count();
    println!(
        "{} system shutdown with {open} tracked open orders on BTCUSDT",
        if live { "live" } else { "paper" }
    );
    Ok(())
}

fn load_config() -> Result<SystemConfig, Box<dyn std::error::Error>> {
    let file = File::open(FILE_PATH_SYSTEM_CONFIG)?;
    let reader = BufReader::new(file);
    Ok(serde_json::from_reader(reader)?)
}
