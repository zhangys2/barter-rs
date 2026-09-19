# Upstream PR split

W1–W7 landed on this fork as one merge (`6ff1105`, [PR #1](https://github.com/zhangys2/barter-rs/pull/1)). Upstream [`barter-rs/barter-rs`](https://github.com/barter-rs/barter-rs) is still at `9770b27`. Open question 2 in `docs/spec-improvements.md` says workstreams should stay independently reviewable.

This document is the extract plan. It is **not** a dump PR.

## Why a single upstream PR is not opened from `main`

The merge mixed Mock correctness, backtest determinism, Binance live, bounded channels, `HistoricalClock`, and CI in one tree. A mock-only cherry-pick does not compile without also taking:

- `MockExecutionConfig::{feed_latency_ms,order_latency_ms}`
- event-time `HistoricalClock` (identity tests)
- execution-builder market channel bounds used by the mock market feed

Opening `zhangys2:main` → `barter-rs/barter-rs` would be a ~6k-line dump and would violate the spec's "independently reviewable" constraint. Reconstructing five compile-clean patches is the next packaging step, not this ticket's dump.

## Proposed PRs (in order)

| PR | Scope | Primary files | Breaking API |
|---|---|---|---|
| 1. Mock correctness (W1 + W2 minus live/CI) | Balance accounting, cancels, L2 fills, IOC/FOK, reserved balances, split feed/order latency | `barter-execution/src/exchange/mock/**`, `barter-execution/src/client/mock/mod.rs`, `barter-execution/src/order/mod.rs` | `MockExecutionConfig` latency fields (`new` preserved) |
| 2. Backtest determinism (W2.8 + W3 money tests) | Byte-identical Trading Summary and Audit Stream, Position/Order property tests, golden fixtures, L2 sequence-gap rebuild | `barter/src/backtest/mod.rs`, `barter/src/engine/clock.rs`, `barter/src/engine/state/{position,order,connectivity}`, `barter/tests/test_backtest_audit_identity.rs`, `barter-data/tests/fixtures/**`, `barter-data/src/exchange/binance/*/l2.rs` | `HistoricalClock` event-time only |
| 3. Binance Spot ExecutionClient (W4 + live round-trip) | Client, config variant, gated tests, paper-to-live examples, weight limiter | `barter-execution/src/client/binance/mod.rs`, `barter-execution/examples/binance_*.rs`, `barter/examples/engine_paper_to_live.rs`, `barter/tests/test_binance_live_round_trip.rs`, `barter/src/system/config.rs`, `barter-integration/src/rate_limit.rs` | none required if `fetch_trades` stays symbol-less and is seeded from config |
| 4. Market-path bounds + latency (W5) | Keyed Conflate, three-hop percentiles, Criterion benches | `barter-integration/src/channel.rs`, `barter/src/system/{builder,mod,observation}.rs`, `barter/src/engine/run.rs`, `barter-data/benches/hot_path.rs`, `barter/benches/engine_process.rs` | Engine runners take process-latency samples; `market_latency_percentile(hop, p)` |
| 5. CI/docs (W6/W7) | `dtolnay/rust-toolchain`, `cargo deny`, docs, examples, manual live-test job | `.github/workflows/ci.yml`, `deny.toml`, `CONTEXT.md`, `docs/**`, `CONTRIBUTING.md`, `CHANGELOG.md` | none |

PR 4 should **not** include the noisy CI checkout-baseline Criterion gate until a committed baseline exists. Record HEAD-only until then.

PR 5 must pin `cargo test` to `--lib --tests` (see `CONTRIBUTING.md`). Do not copy `cargo test --all-targets`.

## Breaking API already on this fork

Listed in `CHANGELOG.md` (Unreleased). Crate versions stay 0.x until an upstream release.

## File ownership (quick map)

- Mock: `barter-execution/src/exchange/mock/`, `barter-execution/src/client/mock/`
- Clock / backtest identity: `barter/src/engine/clock.rs`, `barter/src/backtest/mod.rs`, `barter/tests/test_backtest_audit_identity.rs`
- Live: `barter-execution/src/client/binance/`, `barter/src/system/config.rs`
- Channels / hops: `barter-integration/src/channel.rs`, `barter-integration/src/rate_limit.rs`, `barter/src/system/`, `barter/src/engine/run.rs`
- CI: `.github/workflows/ci.yml`, `scripts/compare_criterion_baselines.py`
