# Barter

An algorithmic trading ecosystem for live-trading, paper-trading and back-testing. Market data and account activity from many exchanges are normalised into one language, fed to a single Engine, which maintains trading state and generates orders.

## Language

### Venues and instruments

**Exchange**:
A trading venue Barter integrates with, identified per product line (e.g. Binance spot and Binance USD futures are distinct Exchanges).
_Avoid_: Venue, market, broker

**Asset**:
A tradeable or holdable unit of value (e.g. btc, usdt). In trading state, an Asset is always scoped to one Exchange: btc on Binance and btc on Okx are distinct.
_Avoid_: Currency, coin, token

**Instrument**:
A specific contract tradeable on one Exchange, defined by its Underlying, Instrument Kind, Quote Asset and Instrument Spec. Binance btc_usdt spot and Okx btc_usdt spot are different Instruments.
_Avoid_: Symbol, market, pair, ticker

**Market Data Instrument**:
An exchange-agnostic description of what to subscribe to: a base, a quote and an Instrument Kind. Carries none of the trading rules of an Instrument.
_Avoid_: Instrument (when no Exchange is implied)

**Underlying**:
The base and quote Asset pair an Instrument is derived from (e.g. btc/usdt).
_Avoid_: Pair, symbol

**Base Asset**:
The Asset being bought or sold in an Underlying (btc in btc/usdt).

**Quote Asset**:
The Asset an Instrument is priced in. Usually the Underlying's quote; for "in-kind" derivatives it is the Underlying's base.
_Avoid_: Counter currency

**Instrument Kind**:
One of Spot, Perpetual, Future or Option. Derivative kinds carry a contract size and settlement Asset.

**Instrument Spec**:
The Exchange's trading rules for an Instrument: minimum price, tick size, minimum quantity, quantity increment, quantity units and minimum notional.
_Avoid_: Filters, rules, precision

**Internal Name**:
Barter's own name for an Asset or Instrument, unique across all Exchanges (e.g. `binance_spot-btc_usdt`).

**Exchange Name**:
The name an Exchange itself uses for an Asset or Instrument (e.g. `XBT-USDT`). Not unique across Exchanges.
_Avoid_: Symbol

**Indexed Instruments**:
The fixed, immutable universe of Exchanges, Assets and Instruments a System trades, each assigned a dense index. Cannot change once a System is built.
_Avoid_: Instrument registry, universe

### Market data

**Subscription**:
A request for one Subscription Kind of data about one Market Data Instrument on one Exchange.

**Subscription Kind**:
The category of public data subscribed to: Public Trades, Order Books L1, Order Books L2, Order Books L3, Candles or Liquidations.
_Avoid_: Channel, feed type

**Market Event**:
A normalised piece of public market data for one Instrument, stamped with both the Exchange's time and the time Barter received it.
_Avoid_: Tick, quote, market update

**Public Trade**:
A trade that occurred on an Exchange between any participants, observed via market data.
_Avoid_: Trade (reserved for the System's own fills)

**Order Book L1**:
The best bid and best ask for an Instrument.
_Avoid_: Top of book, BBO, quote

**Order Book**:
All price Levels for an Instrument — aggregated per price (L2) or per individual order (L3). Arrives as a Snapshot followed by Updates.

**Level**:
A price and the total amount resting at that price on one side of an Order Book.

**Market Stream**:
A live, normalised, auto-reconnecting stream of Market Events from one or more Exchanges.
_Avoid_: Feed, socket

### Account and execution

**Account**:
The System's private state on one Exchange: its Balances and its Orders per Instrument.
_Avoid_: Wallet, portfolio

**Account Event**:
A private update from an Exchange about the Account: a full Account Snapshot, a Balance snapshot, an Order snapshot, an Order cancellation response, or a Trade.

**Account Snapshot**:
The complete current Account on one Exchange; replaces all prior Account state for that Exchange.

**Balance**:
The total and free amount of one Asset held on one Exchange; used = total − free.
_Avoid_: Holdings, funds

**Order**:
An instruction to buy or sell a quantity of an Instrument at a price, owned by one Strategy and identified by its Client Order Id.

**Client Order Id**:
The identifier Barter assigns an Order before the Exchange sees it. The primary key for tracking an Order.
_Avoid_: cid (in prose)

**Order Id**:
The identifier the Exchange assigns an Order once it is Open.
_Avoid_: Exchange order id

**Order Request**:
An intent to either open a new Order (Open Request) or cancel an existing one (Cancel Request), sent from the Engine towards an Exchange.
_Avoid_: Order command, signal

**Order State**:
Where an Order is in its lifecycle. **Active**: Open In-Flight, Open, Cancel In-Flight. **Inactive** (terminal): Cancelled, Fully Filled, Open Failed, Expired.

**In-Flight**:
An Order Request has been sent but the Exchange has not yet responded. An Open In-Flight Order has no Order Id yet.
_Avoid_: Pending

**Order Kind**:
Market or Limit.

**Time In Force**:
How long an Order remains working: Good Until Cancelled (optionally post-only), Good Until End Of Day, Fill Or Kill, Immediate Or Cancel.

**Trade**:
A fill of one of the System's own Orders, with price, quantity and fees paid in the Quote Asset.
_Avoid_: Fill, execution, Public Trade

**Execution Client**:
The adapter that speaks one Exchange's private API: fetches Account Snapshots, streams Account Events, and opens and cancels Orders.
_Avoid_: Broker, connector

**Mock Exchange**:
A simulated Exchange that fills Orders against configured latency and fees, used for paper-trading and back-testing.
_Avoid_: Simulator, fake exchange

### Engine and state

**Engine**:
The single-threaded core that processes Engine Events in order, maintains Engine State, and emits Order Requests.
_Avoid_: Trader, bot, core

**Engine Event**:
Any input to the Engine: a Market Event, an Account Event, a Command, a Trading State update, or Shutdown.

**Engine State**:
Everything the Engine knows: Trading State, Connectivity, per-Asset state, per-Instrument state, and user-defined global data.

**Instrument State**:
The Engine's view of one Instrument: its current Position, its active Orders, performance statistics, and user-defined Instrument Data.

**Instrument Data**:
User-defined per-Instrument state (e.g. latest Order Book L1, indicators) that the Engine updates from events and uses to price the Instrument.

**Trading State**:
Whether algorithmic order generation is Enabled or Disabled. When Disabled the Engine still tracks state and actions Commands. Starts Disabled.
_Avoid_: Paused, running, live

**Command**:
An instruction from outside the Engine (e.g. a UI) to send Open or Cancel Requests, Cancel Orders, or Close Positions for an Instrument Filter.
_Avoid_: Action, request

**Instrument Filter**:
A selection of Instruments by all, Exchange, Instrument, or Underlying.

**Connectivity**:
The Health of the market data connection and the account connection for each Exchange, plus an overall Health that is Healthy only when every connection is.

**Health**:
Healthy or Reconnecting. A connection not yet established counts as Reconnecting.
_Avoid_: Connected/disconnected, status

**Position**:
The System's net exposure in one Instrument, built from Trades: a side (long = buy, short = sell), average entry price, quantity, and realised and unrealised PnL including fees. An Instrument has at most one Position.
_Avoid_: Holding, exposure (as a noun)

**Position Exited**:
The record of a Position that has been fully closed, whether exactly or by flipping to the opposite side.
_Avoid_: Closed position, round trip

**Clock**:
The Engine's source of "now": wall-clock time when live, or time derived from event Exchange timestamps when back-testing.

### Strategy and risk

**Strategy**:
The user-supplied decision logic plugged into the Engine, made of four parts: Algo Strategy, Close Positions Strategy, On Disconnect, and On Trading Disabled.
_Avoid_: Algo, model, bot

**Strategy Id**:
The label stamped on every Order and Trade identifying which Strategy produced it.

**Algo Strategy**:
The part of a Strategy that turns Engine State into Open and Cancel Requests. Only consulted while Trading State is Enabled.
_Avoid_: Signal generator

**Close Positions Strategy**:
The part of a Strategy that decides which Order Requests will neutralise selected Positions.

**On Disconnect**:
The part of a Strategy that reacts when an Exchange connection stops being Healthy.

**On Trading Disabled**:
The part of a Strategy that reacts when Trading State becomes Disabled.

**Risk Manager**:
The gatekeeper that reviews every Order Request from the Algo Strategy and either approves it or refuses it with a reason. Commands bypass it.
_Avoid_: Risk engine, risk filter

### System, audit and results

**System**:
A running Engine wired to its Market Stream, Execution Clients and Audit Stream, controllable from outside via Commands and Trading State updates.
_Avoid_: Bot, app, runtime

**Live-Trading**:
Running a System against real Exchanges with real Execution Clients.

**Paper-Trading**:
Running a System against live Market Streams but Mock Exchanges.

**Back-Test**:
Running a System against historical Market Events, Mock Exchanges and a historical Clock, usually many in parallel with different Strategy or Risk Manager parameters.
_Avoid_: Simulation, replay

**Audit Tick**:
A record of one unit of Engine work — the event processed, any outputs and unrecoverable errors — stamped with the Engine's Sequence and Clock time.

**Audit Stream**:
The ordered stream of Audit Ticks, starting from a full Engine State snapshot, from which external components can rebuild an identical State Replica.

**State Replica**:
A copy of Engine State maintained outside the Engine by replaying the Audit Stream, for non-hot-path consumers such as UIs.

**Sequence**:
The monotonically increasing count of events the Engine has processed in the current run.

**Trading Summary**:
The end-of-session performance report: a Tear Sheet per Instrument and per exchange Asset.

**Tear Sheet**:
Performance metrics for one Instrument (PnL, return, Sharpe, Sortino, Calmar, drawdowns, win rate, profit factor) or one Asset (balance changes).
_Avoid_: Report, stats

## Relationships

- An **Exchange** lists many **Instruments**; each **Instrument** belongs to exactly one **Exchange**.
- An **Instrument** has one **Underlying** (a **Base Asset** and a quote **Asset**) and one **Quote Asset**.
- A **Subscription** names a **Market Data Instrument**; the **Market Stream** yields **Market Events** keyed to **Instruments**.
- An **Order** belongs to one **Instrument**, one **Exchange** and one **Strategy Id**; one **Order** may produce many **Trades**.
- **Trades** build a **Position**; closing a **Position** produces a **Position Exited**, which feeds the **Tear Sheet**.
- **Algo Strategy** output passes through the **Risk Manager** before becoming **Order Requests**; **Command** output does not.
- Every **Engine Event** processed yields one **Audit Tick**.

## Flagged ambiguities

- "Trade" meant both the System's own fills and anyone's trades on an Exchange — resolved: **Trade** is the System's own fill; the other is a **Public Trade**.
- "Instrument" was used both for an exchange-specific contract and for an exchange-agnostic base/quote/kind — resolved: **Instrument** vs **Market Data Instrument**.
- "Strategy" meant both the pluggable decision logic and the label on Orders — resolved: **Strategy** vs **Strategy Id**.
- "Closed" position vs "exited" — resolved: **Position Exited**.
