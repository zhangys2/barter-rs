//! Paper-to-live configuration handoff for Binance Spot.
//!
//! `BINANCE_MODE=paper` (default) talks to Spot Testnet. `BINANCE_MODE=live` talks to mainnet.
//! Credentials stay in `BINANCE_API_KEY` / `BINANCE_API_SECRET`.
//!
//! This example is a real ExecutionClient round-trip: snapshot, far-from-market limit, cancel,
//! reconcile open orders, and fetch historical trades through the public trait.
use barter_execution::{
    client::{BinanceSpot, BinanceSpotConfig, ExecutionClient},
    order::{
        OrderEvent, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::{RequestCancel, RequestOpen},
    },
};
use barter_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let live = std::env::var("BINANCE_MODE").as_deref() == Ok("live");
    let mut config = if live {
        BinanceSpotConfig::default()
    } else {
        BinanceSpotConfig::testnet()
    };
    let instrument = InstrumentNameExchange::from("BTCUSDT");
    config.instruments = vec![instrument.clone()];

    println!(
        "{} mode: {} (credentials remain in {} / {})",
        if live { "live" } else { "paper" },
        config.api_base_url,
        config.api_key_env,
        config.api_secret_env
    );

    if std::env::var(&config.api_key_env).is_err() {
        eprintln!(
            "Set {} and {} to run the open/cancel round-trip.",
            config.api_key_env, config.api_secret_env
        );
        return Ok(());
    }

    let client = BinanceSpot::new(config);
    let snapshot = client
        .account_snapshot(&[], std::slice::from_ref(&instrument))
        .await?;
    println!("snapshot balances={}", snapshot.balances.len());

    let cid = ClientOrderId::random();
    let open = client
        .open_order(OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::BinanceSpot,
                instrument: &instrument,
                strategy: StrategyId::new("paper-to-live"),
                cid: cid.clone(),
            },
            state: RequestOpen {
                side: Side::Buy,
                price: Decimal::from(1000),
                quantity: Decimal::new(1, 5),
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: true },
            },
        })
        .await
        .ok_or("missing open response")?;
    match &open.state {
        Ok(opened) => {
            println!("opened cid={cid} id={}", opened.id);
            let _ = client
                .cancel_order(OrderEvent {
                    key: OrderKey {
                        exchange: ExchangeId::BinanceSpot,
                        instrument: &instrument,
                        strategy: StrategyId::new("paper-to-live"),
                        cid: cid.clone(),
                    },
                    state: RequestCancel {
                        id: Some(opened.id.clone()),
                    },
                })
                .await;
        }
        Err(error) => println!("open rejected (book/filters may differ on testnet): {error}"),
    }

    let opens = client
        .fetch_open_orders(std::slice::from_ref(&instrument))
        .await?;
    println!(
        "open orders after cancel={}",
        opens.iter().filter(|order| order.key.cid == cid).count()
    );
    let trades = client.fetch_trades(DateTime::<Utc>::UNIX_EPOCH).await?;
    println!("historical trades via ExecutionClient={}", trades.len());
    Ok(())
}
