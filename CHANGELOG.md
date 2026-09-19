# Changelog

All notable changes to this fork are listed here. Breaking API changes are called out with `BREAKING`.

## Unreleased

### Added

- Binance Spot live round-trip: paper/live System example, ExecutionClient open/cancel/reconcile example, gated testnet tests (`BARTER_LIVE_TESTS=1`).
- Weight-aware Binance limiter using `barter_integration::rate_limit::WeightWindow` plus `X-MBX-USED-WEIGHT-1M`.
- `BinanceSpotConfig.instruments` seeds `ExecutionClient::fetch_trades` so history does not require a side API.
- Mock Exchange enforces `InstrumentSpec` (tick size, min price, quantity increment, min quantity, min notional) when present.
- `docs/upstream-split.md` describing five independently reviewable upstream PRs.

### BREAKING

Already landed on this fork in `0ce0161` / PR #1 (crates remain 0.x until an upstream release):

- Engine runners (`sync_run`, `sync_run_with_audit`, `async_run`, `async_run_with_audit`) take optional process-latency samples.
- `System::market_latency_percentile` requires a `LatencyHop`.
- `HistoricalClock` is event-time only (no wall-clock interpolation).
- `MockExecutionConfig` adds `feed_latency_ms` / `order_latency_ms` (`new` preserved; `with_latencies` added).

### Notes

- Crate versions stay `barter 0.14.0`, `barter-execution 0.9.0`, `barter-integration 0.12.0`, `barter-data 0.13.0`, `barter-instrument 0.3.3` until the split PRs are offered upstream.
