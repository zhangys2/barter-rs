# Contributing

This is the `zhangys2/barter-rs` fork of [barter-rs/barter-rs](https://github.com/barter-rs/barter-rs). See `docs/upstream-split.md` before opening a PR against upstream.

## Checks

Run these locally (Windows MSVC included):

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --lib --tests -- --test-threads=1
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps --all-features
cargo build --workspace --examples
cargo test --workspace --benches --no-run
```

Do **not** use `cargo test --workspace --all-targets`. That **runs** Criterion benches (`barter/benches/backtest`, `engine_process`, `barter-data/benches/hot_path`, `barter-integration/benches/channel`) instead of only compiling them.

CI's test job is pinned to `--lib --tests` for the same reason. Compile benches with `--benches --no-run`.

## Windows MSVC

Leftover test executables lock `target\debug\deps\barter-*.exe` (`LNK1104`). Run tests serially (`--test-threads=1`). If linking fails, kill stale processes:

```
tasklist /FI "IMAGENAME eq cargo.exe"
tasklist /FI "IMAGENAME eq barter-*.exe"
taskkill /PID <pid> /T /F
```

## Live tests

Ignored tests that hit Binance Spot Testnet require `BARTER_LIVE_TESTS=1`, `BINANCE_API_KEY`, and `BINANCE_API_SECRET`. They are not part of default CI; the `live-test` workflow job is `workflow_dispatch` only.
