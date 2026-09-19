//! Paper-to-live configuration handoff for Binance Spot.
//! Start with `BINANCE_MODE=paper` (testnet), then switch to `live` only after validation.
use barter_execution::client::BinanceSpotConfig;

fn main() {
    let live = std::env::var("BINANCE_MODE").as_deref() == Ok("live");
    let config = if live {
        BinanceSpotConfig::default()
    } else {
        BinanceSpotConfig::testnet()
    };
    println!(
        "{} mode: {} (credentials remain in {} / {})",
        if live { "live" } else { "paper" },
        config.api_base_url,
        config.api_key_env,
        config.api_secret_env
    );
}
