# Domain-to-code map

Where the language in [`CONTEXT.md`](../CONTEXT.md) lives in code. Row names are glossary terms; code names are given where they differ. Orientation aid, not a call graph.

Crate shorthand: `instrument` = `barter-instrument`, `data` = `barter-data`, `execution` = `barter-execution`, `barter` = `barter` (Engine).

Tests are mostly inline `mod tests` in the file named under *Ownership*; "inline" below means that. The only integration test is [`barter/tests/test_engine_process_engine_event_with_audit.rs`](../barter/tests/test_engine_process_engine_event_with_audit.rs) (cited as *engine IT*).

## Venues and instruments

| Domain concept | Entry point | Ownership / behavior | Seam or interface | Evidence |
|---|---|---|---|---|
| **Exchange** | [`instrument/src/exchange.rs`](../barter-instrument/src/exchange.rs) — `ExchangeId` | same; dense key `ExchangeIndex` | `data` `Connector::ID`, `execution` `ExecutionClient::EXCHANGE` | inline |
| **Asset** | [`instrument/src/asset/mod.rs`](../barter-instrument/src/asset/mod.rs) — `Asset`, `ExchangeAsset` | same; dense key `AssetIndex` (one per `ExchangeAsset`) | `IndexedInstruments` | [`index/mod.rs`](../barter-instrument/src/index/mod.rs) inline |
| **Instrument** | [`instrument/src/instrument/mod.rs`](../barter-instrument/src/instrument/mod.rs) — `Instrument<ExchangeKey, AssetKey>` | same; dense key `InstrumentIndex` | generic key params `ExchangeKey`/`AssetKey`/`InstrumentKey` used crate-wide | [`index/mod.rs`](../barter-instrument/src/index/mod.rs) inline |
| **Market Data Instrument** | [`instrument/src/instrument/market_data/mod.rs`](../barter-instrument/src/instrument/market_data/mod.rs) — `MarketDataInstrument` | same | `data` `Subscription::instrument` | inline |
| **Underlying**, **Base Asset** | [`instrument/src/lib.rs`](../barter-instrument/src/lib.rs) — `Underlying` | `BaseAsset` marker in [`asset/mod.rs`](../barter-instrument/src/asset/mod.rs) | `InstrumentFilter::Underlyings` | Unknown |
| **Quote Asset** | [`instrument/src/instrument/quote.rs`](../barter-instrument/src/instrument/quote.rs) — `InstrumentQuoteAsset` | `QuoteAsset` marker in [`asset/mod.rs`](../barter-instrument/src/asset/mod.rs) | fee/PnL types keyed `QuoteAsset` (`Trade`, `Position`) | Unknown |
| **Instrument Kind** | [`instrument/src/instrument/kind/mod.rs`](../barter-instrument/src/instrument/kind/mod.rs) — `InstrumentKind` | `kind/{perpetual,future,option}.rs` | `MarketDataInstrumentKind` (data-side twin) | Unknown |
| **Instrument Spec** | [`instrument/src/instrument/spec.rs`](../barter-instrument/src/instrument/spec.rs) — `InstrumentSpec` | same | `Instrument::spec` (optional) | Unknown |
| **Internal Name** | [`instrument/src/instrument/name.rs`](../barter-instrument/src/instrument/name.rs) — `InstrumentNameInternal`; [`asset/name.rs`](../barter-instrument/src/asset/name.rs) — `AssetNameInternal` | same | `TradingSummary` keys | Unknown |
| **Exchange Name** | same files — `InstrumentNameExchange`, `AssetNameExchange` | same | `ExecutionClient` methods; `Unindexed*` type aliases in `execution` | Unknown |
| **Indexed Instruments** | [`instrument/src/index/mod.rs`](../barter-instrument/src/index/mod.rs) — `IndexedInstruments::new` | [`index/builder.rs`](../barter-instrument/src/index/builder.rs) — `IndexedInstrumentsBuilder` | `EngineState::builder`, `ExecutionBuilder`, `init_indexed_multi_exchange_market_stream` | inline (`index/mod.rs`, `index/builder.rs`) |

> **Discrepancy:** `AssetId` and `InstrumentId` exist in `instrument` but are referenced nowhere. No glossary term maps to them.

## Market data

| Domain concept | Entry point | Ownership / behavior | Seam or interface | Evidence |
|---|---|---|---|---|
| **Subscription** | [`data/src/subscription/mod.rs`](../barter-data/src/subscription/mod.rs) — `Subscription` | [`subscriber/mod.rs`](../barter-data/src/subscriber/mod.rs) — `Subscriber` (connect + subscribe), [`subscriber/validator.rs`](../barter-data/src/subscriber/validator.rs) | `Connector` in [`exchange/mod.rs`](../barter-data/src/exchange/mod.rs) | inline (`subscription/mod.rs`) |
| **Subscription Kind** | [`data/src/subscription/mod.rs`](../barter-data/src/subscription/mod.rs) — `SubKind`, `SubscriptionKind` trait | per-kind markers: `PublicTrades`, `OrderBooksL1/L2/L3`, `Candles`, `Liquidations` in `subscription/*.rs` | `SubscriptionKind::Event` | inline |
| **Market Event** | [`data/src/event.rs`](../barter-data/src/event.rs) — `MarketEvent`, `DataKind` | same | `ExchangeTransformer` in [`transformer/mod.rs`](../barter-data/src/transformer/mod.rs) produces them; `InstrumentDataState` consumes them | Unknown |
| **Public Trade** | [`data/src/subscription/trade.rs`](../barter-data/src/subscription/trade.rs) — `PublicTrade` | per-exchange transformers under `exchange/*/trade.rs` | `DataKind::Trade` | per-exchange inline tests |
| **Order Book L1** | [`data/src/subscription/book.rs`](../barter-data/src/subscription/book.rs) — `OrderBookL1` | same (mid / micro price) | `DefaultInstrumentMarketData::l1` in `barter` | per-exchange inline tests |
| **Order Book** | [`data/src/books/mod.rs`](../barter-data/src/books/mod.rs) — `OrderBook`; `OrderBookEvent` in `subscription/book.rs` | [`books/manager.rs`](../barter-data/src/books/manager.rs) — `OrderBookL2Manager`; `SnapshotFetcher` in [`lib.rs`](../barter-data/src/lib.rs) | `OrderBookMap` in [`books/map.rs`](../barter-data/src/books/map.rs) | inline (`books/mod.rs`); [`examples/order_books_l2_manager.rs`](../barter-data/examples/order_books_l2_manager.rs) |
| **Level** | [`data/src/books/mod.rs`](../barter-data/src/books/mod.rs) — `Level` | same | — | inline |
| **Market Stream** | [`data/src/streams/builder/dynamic/indexed.rs`](../barter-data/src/streams/builder/dynamic/indexed.rs) — `init_indexed_multi_exchange_market_stream`; [`streams/builder/mod.rs`](../barter-data/src/streams/builder/mod.rs) — `StreamBuilder`; `DynamicStreams` | [`streams/mod.rs`](../barter-data/src/streams/mod.rs) — `Streams`; reconnection in [`streams/reconnect/stream.rs`](../barter-data/src/streams/reconnect/stream.rs) | `MarketStream` trait in [`lib.rs`](../barter-data/src/lib.rs); reaches Engine as `EngineEvent::Market(MarketStreamEvent)` | [`data/examples/`](../barter-data/examples) |

## Account and execution

| Domain concept | Entry point | Ownership / behavior | Seam or interface | Evidence |
|---|---|---|---|---|
| **Account** | Partial — no single type; = `AccountSnapshot` + per-Instrument `Orders` + `AssetStates` balances | Engine view: [`barter/src/engine/state/mod.rs`](../barter/src/engine/state/mod.rs); Mock Exchange view: [`execution/src/exchange/mock/account.rs`](../barter-execution/src/exchange/mock/account.rs) — `AccountState` | — | *engine IT* |
| **Account Event** | [`execution/src/lib.rs`](../barter-execution/src/lib.rs) — `AccountEvent`, `AccountEventKind` | indexed by [`execution/src/indexer.rs`](../barter-execution/src/indexer.rs) — `AccountEventIndexer`; applied by `EngineState::update_from_account` | `ExecutionClient::account_stream`; reaches Engine as `EngineEvent::Account(AccountStreamEvent)` ([`barter/src/execution/mod.rs`](../barter/src/execution/mod.rs)) | *engine IT* |
| **Account Snapshot** | [`execution/src/lib.rs`](../barter-execution/src/lib.rs) — `AccountSnapshot`, `InstrumentAccountSnapshot` | `InstrumentState::update_from_account_snapshot` | `ExecutionClient::account_snapshot` | *engine IT* |
| **Balance** | [`execution/src/balance.rs`](../barter-execution/src/balance.rs) — `Balance`, `AssetBalance` | [`barter/src/engine/state/asset/mod.rs`](../barter/src/engine/state/asset/mod.rs) — `AssetState::update_from_balance` | `ExecutionClient::fetch_balances` | inline (`asset/mod.rs`) |
| **Order** | [`execution/src/order/mod.rs`](../barter-execution/src/order/mod.rs) — `Order`, `OrderKey` | [`barter/src/engine/state/order/mod.rs`](../barter/src/engine/state/order/mod.rs) — `Orders` | `OrderManager` in [`order/manager.rs`](../barter/src/engine/state/order/manager.rs) | inline (`order/mod.rs`); *engine IT* |
| **Client Order Id**, **Order Id** | [`execution/src/order/id.rs`](../barter-execution/src/order/id.rs) — `ClientOrderId`, `OrderId` | `Orders` is keyed by `ClientOrderId` | — | inline (`order/mod.rs`) |
| **Order Request** | [`execution/src/order/request.rs`](../barter-execution/src/order/request.rs) — `OrderRequestOpen`, `OrderRequestCancel` | routed by [`barter/src/engine/action/send_requests.rs`](../barter/src/engine/action/send_requests.rs) → [`barter/src/execution/manager.rs`](../barter/src/execution/manager.rs) — `ExecutionManager` | `ExecutionRequest` ([`barter/src/execution/request.rs`](../barter/src/execution/request.rs)); `ExecutionTxMap` ([`engine/execution_tx.rs`](../barter/src/engine/execution_tx.rs)) | inline (`send_requests.rs`) |
| **Order State**, **In-Flight** | [`execution/src/order/state.rs`](../barter-execution/src/order/state.rs) — `OrderState`, `ActiveOrderState`, `InactiveOrderState` | transitions in [`barter/src/engine/state/order/mod.rs`](../barter/src/engine/state/order/mod.rs) | `InFlightRequestRecorder` in [`order/in_flight_recorder.rs`](../barter/src/engine/state/order/in_flight_recorder.rs) | inline (`order/mod.rs`) |
| **Order Kind**, **Time In Force** | [`execution/src/order/mod.rs`](../barter-execution/src/order/mod.rs) — `OrderKind`, `TimeInForce` | Mock Exchange supports a subset | — | Unknown |
| **Trade** | [`execution/src/trade.rs`](../barter-execution/src/trade.rs) — `Trade`, `AssetFees` | `InstrumentState::update_from_trade` → `PositionManager` | `AccountEventKind::Trade` | inline (`position.rs`); *engine IT* |
| **Execution Client** | [`execution/src/client/mod.rs`](../barter-execution/src/client/mod.rs) — `ExecutionClient` trait | per-exchange impls under `client/`; wired by [`barter/src/execution/builder.rs`](../barter/src/execution/builder.rs) — `ExecutionBuilder` | `ExecutionManager` | Partial |
| **Mock Exchange** | [`execution/src/exchange/mock/mod.rs`](../barter-execution/src/exchange/mock/mod.rs) — `MockExchange` | account in [`exchange/mock/account.rs`](../barter-execution/src/exchange/mock/account.rs) | reached via [`client/mock/mod.rs`](../barter-execution/src/client/mock/mod.rs) — `MockExecution` (`ExecutionClient`), configured by `MockExecutionConfig` | [`barter/examples/`](../barter/examples) (mock execution examples) |

> **Discrepancy:** `MockExecution` is the only `ExecutionClient` implementation. [`client/binance/mod.rs`](../barter-execution/src/client/binance/mod.rs) is empty, and `ExecutionConfig` ([`barter/src/system/config.rs`](../barter/src/system/config.rs)) has only a `Mock` variant. The glossary's **Live-Trading** has no real Execution Client in this repo.

## Engine and state

| Domain concept | Entry point | Ownership / behavior | Seam or interface | Evidence |
|---|---|---|---|---|
| **Engine** | [`barter/src/engine/mod.rs`](../barter/src/engine/mod.rs) — `Engine`, `Processor::process` | runners in [`engine/run.rs`](../barter/src/engine/run.rs) — `sync_run*`, `async_run*` | `Processor<Event>` + `Auditor` traits | *engine IT* |
| **Engine Event** | [`barter/src/lib.rs`](../barter/src/lib.rs) — `EngineEvent` | dispatched in `Engine::process` ([`engine/mod.rs`](../barter/src/engine/mod.rs)) | `System::feed_tx` | *engine IT* |
| **Engine State** | [`barter/src/engine/state/mod.rs`](../barter/src/engine/state/mod.rs) — `EngineState` | [`state/builder.rs`](../barter/src/engine/state/builder.rs) — `EngineStateBuilder` | `GlobalData` type param ([`state/global.rs`](../barter/src/engine/state/global.rs)) | *engine IT* |
| **Instrument State** | [`barter/src/engine/state/instrument/mod.rs`](../barter/src/engine/state/instrument/mod.rs) — `InstrumentState`, `InstrumentStates` | same | — | *engine IT* |
| **Instrument Data** | [`barter/src/engine/state/instrument/data.rs`](../barter/src/engine/state/instrument/data.rs) — `InstrumentDataState` trait | default: `DefaultInstrumentMarketData` | `InstrumentDataState::price` | *engine IT* |
| **Trading State** | [`barter/src/engine/state/trading/mod.rs`](../barter/src/engine/state/trading/mod.rs) — `TradingState` | `Engine::update_from_trading_state_update` | `System::trading_state`; `EngineEvent::TradingStateUpdate` | inline (`trading/mod.rs`) |
| **Command** | [`barter/src/engine/command.rs`](../barter/src/engine/command.rs) — `Command` | `Engine::action`; actions in [`engine/action/`](../barter/src/engine/action) (`cancel_orders.rs`, `close_positions.rs`, `send_requests.rs`) | `System::send_open_requests`, `send_cancel_requests`, `close_positions`, `cancel_orders` ([`system/mod.rs`](../barter/src/system/mod.rs)) | *engine IT* |
| **Instrument Filter** | [`barter/src/engine/state/instrument/filter.rs`](../barter/src/engine/state/instrument/filter.rs) — `InstrumentFilter` | applied in `InstrumentStates` ([`instrument/mod.rs`](../barter/src/engine/state/instrument/mod.rs)) | `Command::{ClosePositions, CancelOrders}` | Unknown |
| **Connectivity**, **Health** | [`barter/src/engine/state/connectivity/mod.rs`](../barter/src/engine/state/connectivity/mod.rs) — `ConnectivityStates`, `ConnectivityState`, `Health` | same | reconnect signal: `MarketStreamEvent`/`AccountStreamEvent::Reconnecting` | *engine IT* |
| **Position** | [`barter/src/engine/state/position.rs`](../barter/src/engine/state/position.rs) — `Position`, `PositionManager` | `Position::update_from_trade` | — | inline (`position.rs`) |
| **Position Exited** | [`barter/src/engine/state/position.rs`](../barter/src/engine/state/position.rs) — `PositionExited` | returned from `EngineState::update_from_account` | `EngineOutput::PositionExit`; `TearSheetGenerator` | inline (`position.rs`) |
| **Clock** | [`barter/src/engine/clock.rs`](../barter/src/engine/clock.rs) — `EngineClock` | `LiveClock`, `HistoricalClock` | `TimeExchange` trait | inline (`clock.rs`) |

## Strategy and risk

| Domain concept | Entry point | Ownership / behavior | Seam or interface | Evidence |
|---|---|---|---|---|
| **Strategy** | Partial — no single type; the Engine's `Strategy` type param must implement the four traits below | [`barter/src/strategy/mod.rs`](../barter/src/strategy/mod.rs) — `DefaultStrategy` (demo only) | `Engine<_, _, _, Strategy, _>` | [`examples/engine_sync_with_multiple_strategies.rs`](../barter/examples/engine_sync_with_multiple_strategies.rs) |
| **Strategy Id** | [`execution/src/order/id.rs`](../barter-execution/src/order/id.rs) — `StrategyId` | stamped on `OrderKey` and `Trade` | — | *engine IT* |
| **Algo Strategy** | [`barter/src/strategy/algo.rs`](../barter/src/strategy/algo.rs) — `AlgoStrategy` | invoked by [`engine/action/generate_algo_orders.rs`](../barter/src/engine/action/generate_algo_orders.rs) only when `TradingState::Enabled` | `AlgoStrategy::generate_algo_orders` | *engine IT* |
| **Close Positions Strategy** | [`barter/src/strategy/close_positions.rs`](../barter/src/strategy/close_positions.rs) — `ClosePositionsStrategy` | default helper `close_open_positions_with_market_orders` | invoked by [`engine/action/close_positions.rs`](../barter/src/engine/action/close_positions.rs) | *engine IT* |
| **On Disconnect** | [`barter/src/strategy/on_disconnect.rs`](../barter/src/strategy/on_disconnect.rs) — `OnDisconnectStrategy` | — | `EngineOutput::{MarketDisconnect, AccountDisconnect}` | Unknown |
| **On Trading Disabled** | [`barter/src/strategy/on_trading_disabled.rs`](../barter/src/strategy/on_trading_disabled.rs) — `OnTradingDisabled` | — | `EngineOutput::OnTradingDisabled` | Unknown |
| **Risk Manager** | [`barter/src/risk/mod.rs`](../barter/src/risk/mod.rs) — `RiskManager`, `RiskApproved`, `RiskRefused` | reusable checks in [`risk/check/mod.rs`](../barter/src/risk/check/mod.rs); `DefaultRiskManager` approves all (demo only) | called only from `generate_algo_orders.rs`; Command actions bypass it | [`examples/engine_sync_with_risk_manager_open_order_checks.rs`](../barter/examples/engine_sync_with_risk_manager_open_order_checks.rs) |

## System, audit and results

| Domain concept | Entry point | Ownership / behavior | Seam or interface | Evidence |
|---|---|---|---|---|
| **System** | [`barter/src/system/builder.rs`](../barter/src/system/builder.rs) — `SystemBuilder`, `SystemArgs` | [`system/mod.rs`](../barter/src/system/mod.rs) — `System`; config in [`system/config.rs`](../barter/src/system/config.rs) — `SystemConfig` | `EngineFeedMode`, `AuditMode` | [`barter/examples/`](../barter/examples) |
| **Live-Trading** | Unknown — no real Execution Client (see discrepancy above) | — | — | — |
| **Paper-Trading** | `SystemBuilder` + `LiveClock` + live Market Stream + `ExecutionConfig::Mock` | — | — | [`examples/engine_sync_with_live_market_data_and_mock_execution_and_audit.rs`](../barter/examples/engine_sync_with_live_market_data_and_mock_execution_and_audit.rs) |
| **Back-Test** | [`barter/src/backtest/mod.rs`](../barter/src/backtest/mod.rs) — `backtest`, `run_backtests` | historical data: [`backtest/market_data.rs`](../barter/src/backtest/market_data.rs) — `BacktestMarketData`; results: [`backtest/summary.rs`](../barter/src/backtest/summary.rs) | `BacktestArgsConstant` / `BacktestArgsDynamic` | [`examples/backtests_concurrent.rs`](../barter/examples/backtests_concurrent.rs) |
| **Audit Tick** | [`barter/src/engine/audit/mod.rs`](../barter/src/engine/audit/mod.rs) — `AuditTick`, `EngineAudit`, `ProcessAudit` | context in [`audit/context.rs`](../barter/src/engine/audit/context.rs) — `EngineContext` | `Auditor` trait | *engine IT* |
| **Audit Stream** | [`barter/src/system/mod.rs`](../barter/src/system/mod.rs) — `System::audit` (snapshot + updates) | emitted by `sync_run_with_audit` / `async_run_with_audit` ([`engine/run.rs`](../barter/src/engine/run.rs)) | `AuditMode::Enabled` | [`examples/engine_sync_with_audit_replica_engine_state.rs`](../barter/examples/engine_sync_with_audit_replica_engine_state.rs) |
| **State Replica** | [`barter/src/engine/audit/state_replica.rs`](../barter/src/engine/audit/state_replica.rs) — `StateReplicaManager` | same | consumes Audit Stream | [`examples/engine_sync_with_audit_replica_engine_state.rs`](../barter/examples/engine_sync_with_audit_replica_engine_state.rs) |
| **Sequence** | [`barter/src/lib.rs`](../barter/src/lib.rs) — `Sequence` | incremented per audit in `Auditor for Engine` | `EngineMeta::sequence`, `EngineContext::sequence` | *engine IT* |
| **Trading Summary** | [`barter/src/statistic/summary/mod.rs`](../barter/src/statistic/summary/mod.rs) — `TradingSummary`, `TradingSummaryGenerator` | `Engine::trading_summary_generator` | — | [`examples/statistical_trading_summary.rs`](../barter/examples/statistical_trading_summary.rs) |
| **Tear Sheet** | [`barter/src/statistic/summary/instrument.rs`](../barter/src/statistic/summary/instrument.rs) — `TearSheet`; [`summary/asset.rs`](../barter/src/statistic/summary/asset.rs) — `TearSheetAsset` | generators held in `InstrumentState::tear_sheet`, `AssetState::statistics`; metrics in [`statistic/metric/`](../barter/src/statistic/metric) | — | inline (`statistic/metric/*`, `summary/asset.rs`) |
