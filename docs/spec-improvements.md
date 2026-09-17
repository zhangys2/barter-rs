# Spec: Barter fork improvements

- **Fork:** [zhangys2/barter-rs](https://github.com/zhangys2/barter-rs) (upstream: [barter-rs/barter-rs](https://github.com/barter-rs/barter-rs), base `9770b27`)
- **Status:** Draft
- **Language:** terms in **bold** are defined in [`CONTEXT.md`](../CONTEXT.md); code anchors are in [`domain-code-map.md`](domain-code-map.md).

## Problem

Barter's Engine, Engine State and Audit Stream design is sound, but results produced with it cannot yet be trusted:

1. **Back-Tests and Paper-Trading are wrong, not just optimistic.** The **Mock Exchange** has balance-accounting bugs, ignores market data when filling, rejects Limit Orders and never answers Cancel Requests.
2. **Live-Trading is not possible.** There is no real **Execution Client** in the repo.
3. **Money logic is thinly tested.** One integration test; **Position**, **Order State** transitions and parsers lack property/golden tests.
4. **Throughput under bursts is unbounded** — memory and latency grow instead of degrading gracefully.

## Goals

- G1. A Back-Test of a given Strategy on given data produces the **same, correct** Trading Summary every run.
- G2. Mock Exchange fills are **market-realistic enough** to evaluate passive and aggressive strategies (spread, depth, partial fills, latency, queue position).
- G3. At least one exchange supports Live-Trading end-to-end.
- G4. Core money logic is covered by property tests; exchange parsers by golden-file tests.
- G5. Bursty market data degrades by an explicit policy, with measured latency.

## Non-goals

- Matching NautilusTrader's venue count or order-type breadth.
- Python bindings, UI, or hosted services.
- Kernel-bypass / co-located networking.
- Changing the license (stays MIT) or the core Engine/Audit architecture.

## Constraints

- Each workstream lands as independently reviewable PRs, structured so they could be offered upstream.
- Breaking public API changes are allowed but must be listed in the PR and the changelog (`fix!`/`feat!` commit convention already in use).
- Canonical terms from `CONTEXT.md` are used in code, docs and PRs; new terms are added to the glossary as they are settled.

---

## W1 — Mock Exchange correctness (P0)

Verified defects and hazards in [`barter-execution/src/exchange/mock/mod.rs`](../barter-execution/src/exchange/mock/mod.rs):

| # | Defect | Evidence |
|---|---|---|
| W1.1 | A Sell debits the **quote** Balance by the **base** quantity (+fees), and reports `BalanceInsufficient` against the quote Asset. The comment says base is required. | `open_order`, `Side::Sell` branch calls `balance_mut(&underlying.quote)` |
| W1.2 | The current fill accounting updates only one Balance and never applies the asset credit: a Buy debits quote but does not credit base; a Sell debits quote instead of base and does not credit quote proceeds. Balances drift from reality after every Trade. | `open_order` returns a single balance snapshot |
| W1.3 | Cancel Requests are logged and dropped; the dropped response channel surfaces to the Engine as `ConnectivityError::ExchangeOffline`, a false connectivity failure. `MockExchange::cancel_order` itself is `unimplemented!()`. | `run`, `CancelOrder` arm; `client/mock/mod.rs` `cancel_order` |
| W1.4 | `assert_eq!(balance.total, balance.free)` is incompatible with reserved funds and will panic the Mock Exchange task once W2 adds resting orders. | `open_order`, both branches |
| W1.5 | Open responses report `filled_quantity = quantity` and emit a Trade at the **requested** price, regardless of market. (Fixed properly by W2; W1 only documents it.) | `open_order` |

**Requirements**
- R1.1 A Trade updates both Assets of the Underlying: Buy = −(quote value + fees) / +base quantity; Sell = −base quantity / +(quote value − fees). Fee asset follows the existing Quote Asset convention.
- R1.2 Balance insufficiency is checked against the correct Asset and reported with that Asset.
- R1.3 Cancel Requests receive a real response: `Cancelled` for an active Order, a typed `OrderError` (not a connectivity error) for an unknown or inactive Order.
- R1.4 No `assert!`/`unimplemented!` on request paths; invariant violations become typed errors.

**Acceptance**
- Unit tests reproducing W1.1–W1.4 fail on `9770b27` and pass after the fix.
- Property test: for any accepted zero-fee fill, the quote-balance change plus the base-balance change valued at that fill's price is zero; over a sequence, the test accounts for the changing valuation price rather than asserting a single fixed-price wealth invariant.

## W2 — Realistic fill simulation (P0)

**Problem.** The Mock Exchange never sees market data, so it cannot know the spread, depth or whether a Limit Order would fill.

**Requirements**
- R2.1 In a Back-Test, the Mock Exchange receives the same per-Instrument Market Events as the Engine, in timestamp order, before the corresponding Engine event is processed.
- R2.2 **Market Orders** fill against the opposite side of the Order Book, walking Levels for size. If only Order Book L1 is available, use the best bid/ask; if only Public Trades are available, use the last trade. Unfillable remainder follows Time In Force.
- R2.3 **Limit Orders** are accepted, reserve Balance (free < total), rest until crossed, and fill fully or partially. Post-only Orders that would cross are rejected.
- R2.4 Time In Force: Good Until Cancelled, Immediate Or Cancel, Fill Or Kill honoured; Good Until End Of Day may be deferred (documented).
- R2.5 **Queue position** is a pluggable model (trait), with at least: `NoQueue` (fill on touch) and a conservative "fill only when traded through" model.
- R2.6 **Latency** is a pluggable model separating feed latency and order latency (replacing the single `latency_ms` split in half).
- R2.7 Partial fills emit one Trade per fill and Order snapshots with updated filled quantity.
- R2.8 Back-Tests are deterministic: identical inputs ⇒ identical Audit Stream and Trading Summary.

**Acceptance**
- Scenario tests: market Buy larger than best ask walks two Levels at the volume-weighted price; resting Limit Buy fills only after a Public Trade at/below its price (under the conservative model); IOC remainder is Expired; Cancel of a partially filled Order leaves the correct Balance.
- The `backtests_concurrent` example produces byte-identical Trading Summaries for two runs with identical inputs.
- Existing examples still run (Market-only Strategies unaffected apart from realistic prices).

**Open question.** Is a mid-level fidelity (L2 + probabilistic queue) enough, or should L3 queue tracking (à la hftbacktest) be in scope? Default: out of scope for this spec.

## W3 — Test coverage for money logic (P1)

**Requirements**
- R3.1 Property tests (`proptest`) for **Position**: increase / reduce / exact close / flip; invariants on quantity, average entry price, realised PnL and fees; **Position Exited** emitted exactly when quantity crosses or reaches zero.
- R3.2 Property/state-machine tests for **Order State**: only legal transitions are accepted; out-of-order snapshots never regress a newer state; terminal states (Cancelled, Fully Filled, Open Failed, Expired) remove a tracked Order from the active-order collection.
- R3.3 Golden-file tests per supported Exchange parser: recorded raw messages → expected normalised Market Events, checked into the repo.
- R3.4 **Connectivity** tests: Reconnecting events set Health and invoke On Disconnect; recovery restores Healthy.
- R3.5 Order Book sequence-gap test: a gap invalidates the book and triggers snapshot rebuild.

**Acceptance**
- Every *Unknown* row in `domain-code-map.md` that is covered by R3.1–R3.5 has at least one corresponding test, and the map is updated; unrelated Unknown rows are either addressed by a separate workstream or remain explicitly out of scope.
- CI runs the new tests on every PR.

## W4 — First live Execution Client (P1)

**Requirements**
- R4.1 Implement `ExecutionClient` for one Exchange (candidate: Binance Spot, since market data support already exists; decide in open questions).
- R4.2 Support: Account Snapshot, Account Event stream, open Market and Limit Orders, cancel, fetch open Orders / Balances / Trades.
- R4.3 Request signing and rate-limit handling via `barter-integration` primitives.
- R4.4 On Account stream reconnect: re-fetch Account Snapshot before resuming, so Engine State cannot miss fills.
- R4.5 Add an `ExecutionConfig` variant for the live client; configuration contains no inline secrets and stores only an environment-variable or file reference.

**Acceptance**
- Integration tests against the exchange testnet, gated behind a feature flag / env var and excluded from default CI.
- A new example runs Paper-Trading → Live-Trading by changing only config.

## W5 — Throughput under bursts (P2)

**Evidence.** 15 unbounded channels across `barter`, `barter-data`, `barter-execution` (e.g. `System::feed_tx`, Engine audit, Execution request/response, dynamic stream fan-out). Order Book sides are `Vec<Level>` of `Decimal` updated by binary search + insert/remove. Only benchmark: `barter/benches/backtest`.

**Requirements**
- R5.1 Bounded channels on the Market Stream → Engine path with a configurable overflow policy: `Block`, `DropOldest`, `Conflate` (latest per Instrument per data kind).
- R5.2 Account Events are **never** dropped or conflated.
- R5.3 Criterion benchmarks: parse → normalise → Order Book update → `Engine::process`, per message and per burst.
- R5.4 Latency instrumentation: exchange time → received time → Engine process time, exportable as percentiles.
- R5.5 Evaluate integer-tick Order Book levels and faster JSON parsing; adopt only with benchmark evidence.

**Acceptance**
- Under a documented synthetic 10× burst, memory stays bounded and the chosen policy is observable in metrics.
- Benchmarks run in CI against a recorded baseline; a regression greater than 10% on the agreed benchmark workload fails the job.

## W6 — Domain and API cleanup (P3)

- R6.1 Remove the unreferenced `AssetId` / `InstrumentId` types; dense `AssetIndex` / `InstrumentIndex` remain the canonical runtime keys.
- R6.2 Document whether the existing two-state **Health** model (with an initial `Reconnecting` value) is sufficient; add a distinct "never connected" state only if a concrete consumer requires it.
- R6.3 Remove the unsupported `OrderBooksL3` subscription and document L2 as the simulation fidelity ceiling; per-order queue tracking is out of scope.
- R6.4 Fix doc drift: the `ExecutionConfig` documentation says it is for backtesting even though it is also used for Paper-Trading; the `Orders` lifecycle documentation omits Open Failed; and the `DefaultStrategy` documentation cites `OnDisconnectStrategy` instead of `OnTradingDisabled` for the disabled-trading behavior.

**Acceptance.** Decisions for R6.1–R6.3 are recorded in the relevant documentation (the glossary where applicable), and no unresolved item in `CONTEXT.md`'s Flagged ambiguities section contradicts them.

## W7 — CI and tooling (P3)

- R7.1 Replace archived `actions-rs/*` with `dtolnay/rust-toolchain`; `actions/checkout@v4`.
- R7.2 Add `cargo deny` (licenses, advisories), `cargo doc` with `-D warnings`, and `cargo build --examples`.
- R7.3 Add a job for feature-gated live tests (manual trigger only).

---

## Sequencing

| Milestone | Contents | Exit criterion |
|---|---|---|
| M1 — Trustworthy mock | W1, W3.R3.1–R3.2, W7.R7.1 | Balance/Position/Order property tests green |
| M2 — Realistic Back-Test | W2, W3.R3.3–R3.5 | Scenario + determinism tests green |
| M3 — Live | W4 | Testnet round-trip green |
| M4 — Scale | W5, W6, rest of W7 | Burst test + benchmarks in CI |

## Open questions

1. Which Exchange first for W4: Binance Spot, Bybit, or OKX?
2. Keep the fork upstream-compatible (small PRs offered to barter-rs) or diverge freely?
3. Fill-model fidelity ceiling for W2 (see W2 open question).
