//! Opt-in Binance Spot Testnet account snapshot.
//! Credentials are read from BINANCE_API_KEY and BINANCE_API_SECRET.
use barter_execution::client::{BinanceSpot, BinanceSpotConfig, ExecutionClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("BARTER_LIVE_TESTS").as_deref() != Ok("1") {
        eprintln!("Set BARTER_LIVE_TESTS=1 to enable the Binance Spot Testnet example.");
        return Ok(());
    }
    let client = BinanceSpot::new(BinanceSpotConfig::testnet());
    let snapshot = client.account_snapshot(&[], &[]).await?;
    println!("received {} balances", snapshot.balances.len());
    Ok(())
}
