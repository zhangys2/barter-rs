use crate::{
    engine::{
        Engine, Processor,
        audit::{Auditor, context::EngineContext},
        clock::EngineClock,
        execution_tx::MultiExchangeTxMap,
        run::{async_run, async_run_with_audit, sync_run, sync_run_with_audit},
        state::{EngineState, builder::EngineStateBuilder, trading::TradingState},
    },
    error::BarterError,
    execution::{
        AccountStreamEvent,
        builder::{ExecutionBuildFutures, ExecutionBuilder},
    },
    shutdown::SyncShutdown,
    system::{System, SystemAuxillaryHandles, config::ExecutionConfig},
};
use barter_data::streams::reconnect::stream::ReconnectingStream;
use barter_execution::balance::Balance;
use barter_instrument::{
    Keyed,
    asset::{AssetIndex, ExchangeAsset, name::AssetNameInternal},
    exchange::{ExchangeId, ExchangeIndex},
    index::IndexedInstruments,
    instrument::{Instrument, InstrumentIndex},
};
use barter_integration::{
    FeedEnded, Terminal,
    channel::{
        Channel, ChannelTxDroppable, LatencySamples, OverflowPolicy, mpsc_bounded, mpsc_unbounded,
    },
    collection::snapshot::SnapUpdates,
};
use derive_more::Constructor;
use fnv::FnvHashMap;
use futures::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Debug, Clone)]
struct TimedMarket<Event> {
    event: Event,
    started: Instant,
}

async fn merge_event_feeds<Event, AccountStream>(
    mut market_rx: barter_integration::channel::BoundedRx<TimedMarket<Event>>,
    mut account_stream: AccountStream,
    mut feed_tx: barter_integration::channel::BoundedTx<Event>,
    market_latency: Arc<Mutex<LatencySamples>>,
) where
    AccountStream: Stream + Unpin,
    Event: From<AccountStream::Item> + Send,
{
    let mut account_done = false;
    let mut market_done = false;
    loop {
        if account_done && market_done {
            break;
        }
        tokio::select! {
            biased;
            account = account_stream.next(), if !account_done => {
                match account {
                    Some(account) => {
                        if SinkExt::send(&mut feed_tx, Event::from(account)).await.is_err() { break; }
                    }
                    None => account_done = true,
                }
            }
            market = StreamExt::next(&mut market_rx), if !market_done => {
                match market {
                    Some(timed) => {
                        if let Ok(mut samples) = market_latency.lock() { samples.record(timed.started); }
                        if SinkExt::send(&mut feed_tx, timed.event).await.is_err() { break; }
                    }
                    None => market_done = true,
                }
            }
        }
    }
}

/// Defines how the `Engine` processes input events.
///
/// Use this to control whether the `Engine` runs in a synchronous blocking thread
/// with an `Iterator` or asynchronously with a `Stream`.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Default)]
pub enum EngineFeedMode {
    /// Process events synchronously with an `Iterator` in a blocking thread (default).
    #[default]
    Iterator,

    /// Process events asynchronously with a `Stream` and tokio tasks.
    ///
    /// Useful when running concurrent backtests at scale.
    Stream,
}

/// Defines if the `Engine` sends the audit events it produces on the audit channel.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Default)]
pub enum AuditMode {
    /// Enable audit event sending.
    Enabled,

    /// Disable audit event sending (default).
    #[default]
    Disabled,
}

/// Arguments required for building a full Barter trading system.
///
/// Contains all the required components to build and initialise a full Barter trading system,
/// including the `Engine` and all supporting infrastructure.
#[derive(Debug, Clone, PartialEq, PartialOrd, Constructor)]
pub struct SystemArgs<'a, Clock, Strategy, Risk, MarketStream, GlobalData, FnInstrumentData> {
    /// Indexed collection of instruments the system will track.
    pub instruments: &'a IndexedInstruments,

    /// Execution configurations for exchange execution links.
    pub executions: Vec<ExecutionConfig>,

    /// `EngineClock` implementation for time keeping.
    ///
    /// For example, `HistoricalClock` for backtesting and `LiveClock` for live/paper trading.
    pub clock: Clock,

    /// Engine `Strategy` implementation.
    pub strategy: Strategy,

    /// Engine `RiskManager` implementation.
    pub risk: Risk,

    /// `Stream` of `MarketStreamEvent`s.
    pub market_stream: MarketStream,

    /// `EngineState` `GlobalData`
    pub global_data: GlobalData,

    /// Closure used when building the `EngineState` to initialise every
    /// instrument's `InstrumentDataState`.
    pub instrument_data_init: FnInstrumentData,
}

/// Builder for constructing a full Barter trading system.
#[derive(Debug)]
pub struct SystemBuilder<'a, Clock, Strategy, Risk, MarketStream, GlobalData, FnInstrumentData> {
    args: SystemArgs<'a, Clock, Strategy, Risk, MarketStream, GlobalData, FnInstrumentData>,
    engine_feed_mode: Option<EngineFeedMode>,
    audit_mode: Option<AuditMode>,
    trading_state: Option<TradingState>,
    balances: FnvHashMap<ExchangeAsset<AssetNameInternal>, Balance>,
    market_channel_capacity: usize,
    market_overflow_policy: OverflowPolicy,
}

impl<'a, Clock, Strategy, Risk, MarketStream, GlobalData, FnInstrumentData>
    SystemBuilder<'a, Clock, Strategy, Risk, MarketStream, GlobalData, FnInstrumentData>
{
    /// Create a new `SystemBuilder` with the provided `SystemArguments`.
    ///
    /// Initialises a builder with default values for optional configurations.
    pub fn new(
        config: SystemArgs<'a, Clock, Strategy, Risk, MarketStream, GlobalData, FnInstrumentData>,
    ) -> Self {
        Self {
            args: config,
            engine_feed_mode: None,
            audit_mode: None,
            trading_state: None,
            balances: FnvHashMap::default(),
            market_channel_capacity: 4096,
            market_overflow_policy: OverflowPolicy::DropOldest,
        }
    }

    /// Optionally configure the [`EngineFeedMode`] (`Iterator` or `Stream`).
    ///
    /// Controls whether the engine processes events synchronously or asynchronously.
    pub fn engine_feed_mode(self, value: EngineFeedMode) -> Self {
        Self {
            engine_feed_mode: Some(value),
            ..self
        }
    }

    /// Optionally configure the [`AuditMode`] (enabled or disabled).
    ///
    /// Controls whether the engine sends the audit events it produces.
    pub fn audit_mode(self, value: AuditMode) -> Self {
        Self {
            audit_mode: Some(value),
            ..self
        }
    }

    /// Optionally configure the initial [`TradingState`] (enabled or disabled).
    ///
    /// Sets whether algorithmic trading is initially enabled when the system starts.
    pub fn trading_state(self, value: TradingState) -> Self {
        Self {
            trading_state: Some(value),
            ..self
        }
    }

    /// Optionally provide initial exchange asset `Balance`s.
    ///
    /// Useful for back-test scenarios where seeding EngineState with initial `Balance`s is
    /// required.
    ///
    /// Note the internal implementation uses a `HashMap`, so duplicate
    /// `ExchangeAsset<AssetNameInternal>` keys are overwritten.
    pub fn balances<BalanceIter, KeyedBalance>(mut self, balances: BalanceIter) -> Self
    where
        BalanceIter: IntoIterator<Item = KeyedBalance>,
        KeyedBalance: Into<Keyed<ExchangeAsset<AssetNameInternal>, Balance>>,
    {
        self.balances.extend(balances.into_iter().map(|keyed| {
            let Keyed { key, value } = keyed.into();

            (key, value)
        }));
        self
    }

    /// Configure the bounded market-feed capacity and overflow behavior.
    pub fn market_channel(self, capacity: usize, policy: OverflowPolicy) -> Self {
        assert!(capacity > 0, "market channel capacity must be positive");
        Self {
            market_channel_capacity: capacity,
            market_overflow_policy: policy,
            ..self
        }
    }

    /// Build the [`SystemBuild`] with the configured builder settings.
    ///
    /// This constructs all the system components but does not start any tasks or streams.
    ///
    /// Initialise the `SystemBuild` instance to start the system.
    pub fn build<Event, InstrumentData>(
        self,
    ) -> Result<
        SystemBuild<
            Engine<
                Clock,
                EngineState<GlobalData, InstrumentData>,
                MultiExchangeTxMap,
                Strategy,
                Risk,
            >,
            Event,
            MarketStream,
        >,
        BarterError,
    >
    where
        Clock: EngineClock + Clone + Send + Sync + 'static,
        FnInstrumentData: Fn(
            &'a Keyed<InstrumentIndex, Instrument<Keyed<ExchangeIndex, ExchangeId>, AssetIndex>>,
        ) -> InstrumentData,
    {
        let Self {
            args:
                SystemArgs {
                    instruments,
                    executions,
                    clock,
                    strategy,
                    risk,
                    market_stream,
                    global_data,
                    instrument_data_init,
                },
            engine_feed_mode,
            audit_mode,
            trading_state,
            balances,
            market_channel_capacity,
            market_overflow_policy,
        } = self;

        // Default if not provided
        let engine_feed_mode = engine_feed_mode.unwrap_or_default();
        let audit_mode = audit_mode.unwrap_or_default();
        let trading_state = trading_state.unwrap_or_default();

        // Build Execution infrastructure
        let execution = executions
            .into_iter()
            .try_fold(
                ExecutionBuilder::new(instruments),
                |builder, config| match config {
                    ExecutionConfig::Mock(mock_config) => {
                        builder.add_mock(mock_config, clock.clone())
                    }
                    ExecutionConfig::BinanceSpot(config) => builder
                        .add_live::<barter_execution::client::BinanceSpot>(
                        config,
                        std::time::Duration::from_secs(5),
                    ),
                },
            )?
            .build();

        // Build EngineState
        let state = EngineStateBuilder::new(instruments, global_data, instrument_data_init)
            .time_engine_start(clock.time())
            .trading_state(trading_state)
            .balances(
                balances
                    .into_iter()
                    .map(|(key, value)| Keyed::new(key, value)),
            )
            .build();

        // Construct Engine
        let engine = Engine::new(clock, state, execution.execution_tx_map, strategy, risk);

        Ok(SystemBuild {
            engine,
            engine_feed_mode,
            audit_mode,
            market_stream,
            account_channel: execution.account_channel,
            execution_build_futures: execution.futures,
            market_channel_capacity,
            market_overflow_policy,
            market_latency: Arc::new(Mutex::new(LatencySamples::default())),
            phantom_event: PhantomData,
        })
    }
}

/// Fully constructed `SystemBuild` ready to be initialised.
///
/// This is an intermediate step before spawning tasks and running the system.
#[allow(missing_debug_implementations)]
pub struct SystemBuild<Engine, Event, MarketStream> {
    /// Constructed `Engine` instance.
    pub engine: Engine,

    /// Selected [`EngineFeedMode`].
    pub engine_feed_mode: EngineFeedMode,

    /// Selected [`AuditMode`].
    pub audit_mode: AuditMode,

    /// `Stream` of `MarketStreamEvent`s.
    pub market_stream: MarketStream,

    /// Channel for `AccountStreamEvent`.
    pub account_channel: Channel<AccountStreamEvent>,

    /// Futures for initialising `ExecutionBuild` components.
    pub execution_build_futures: ExecutionBuildFutures,
    pub market_channel_capacity: usize,
    pub market_overflow_policy: OverflowPolicy,
    pub market_latency: Arc<Mutex<LatencySamples>>,

    phantom_event: PhantomData<Event>,
}

impl<Engine, Event, MarketStream> SystemBuild<Engine, Event, MarketStream>
where
    Engine: Processor<Event>
        + Auditor<Engine::Audit, Context = EngineContext>
        + SyncShutdown
        + Send
        + 'static,
    Engine::Audit: From<FeedEnded> + Terminal + Debug + Clone + Send + 'static,
    Event: From<MarketStream::Item> + From<AccountStreamEvent> + Debug + Clone + Send + 'static,
    MarketStream: Stream + Send + 'static,
{
    /// Construct a new `SystemBuild` from the provided components.
    pub fn new(
        engine: Engine,
        engine_feed_mode: EngineFeedMode,
        audit_mode: AuditMode,
        market_stream: MarketStream,
        account_channel: Channel<AccountStreamEvent>,
        execution_build_futures: ExecutionBuildFutures,
    ) -> Self {
        Self {
            engine,
            engine_feed_mode,
            audit_mode,
            market_stream,
            account_channel,
            execution_build_futures,
            market_channel_capacity: 4096,
            market_overflow_policy: OverflowPolicy::DropOldest,
            market_latency: Arc::new(Mutex::new(LatencySamples::default())),
            phantom_event: Default::default(),
        }
    }

    /// Initialise the system using the current tokio runtime.
    ///
    /// Spawns all necessary tasks and returns the running `System` instance.
    pub async fn init(self) -> Result<System<Engine, Event>, BarterError> {
        self.init_internal(tokio::runtime::Handle::current()).await
    }

    /// Initialise the system using the provided tokio runtime.
    ///
    /// Allows specifying a custom runtime for spawning tasks.
    pub async fn init_with_runtime(
        self,
        runtime: tokio::runtime::Handle,
    ) -> Result<System<Engine, Event>, BarterError> {
        self.init_internal(runtime).await
    }

    async fn init_internal(
        self,
        runtime: tokio::runtime::Handle,
    ) -> Result<System<Engine, Event>, BarterError> {
        let Self {
            mut engine,
            engine_feed_mode,
            audit_mode,
            market_stream,
            account_channel,
            execution_build_futures,
            market_channel_capacity,
            market_overflow_policy,
            market_latency,
            phantom_event: _,
        } = self;

        // Initialise all execution components
        let execution = execution_build_futures
            .init_with_runtime(runtime.clone())
            .await?;

        // Initialise central Engine channel
        let (feed_tx, mut feed_rx) =
            mpsc_bounded::<Event>(market_channel_capacity, OverflowPolicy::Block);

        // Bound the burst-prone market path while keeping account events lossless on their
        // dedicated unbounded channel. DropOldest is explicit: the next book update can rebuild
        // state, while account/order events must never share this overflow policy.
        let (market_tx, market_rx) =
            mpsc_bounded::<TimedMarket<Event>>(market_channel_capacity, market_overflow_policy);
        // Market ingress is bounded and account events are merged separately with priority.
        // The merger applies backpressure to the central bounded feed without allowing market
        // overflow policy to discard account/order events.
        let market_to_engine = runtime.clone().spawn(async move {
            let _ = market_stream
                .map(|event| TimedMarket {
                    event: Event::from(event),
                    started: Instant::now(),
                })
                .forward_to(market_tx)
                .await;
        });

        let account_stream = account_channel.rx.into_stream();
        let account_to_engine = runtime.spawn(merge_event_feeds(
            market_rx,
            account_stream,
            feed_tx.clone(),
            Arc::clone(&market_latency),
        ));

        // Run Engine in configured mode
        let (engine, audit) = match (engine_feed_mode, audit_mode) {
            (EngineFeedMode::Iterator, AuditMode::Enabled) => {
                // Initialise Audit channel
                let (audit_tx, audit_rx) = mpsc_unbounded();
                let mut audit_tx = ChannelTxDroppable::new(audit_tx);

                let audit = SnapUpdates {
                    snapshot: engine.audit_snapshot(),
                    updates: audit_rx,
                };

                let handle = runtime.spawn_blocking(move || {
                    let shutdown_audit =
                        sync_run_with_audit(&mut feed_rx, &mut engine, &mut audit_tx);

                    (engine, shutdown_audit)
                });

                (handle, Some(audit))
            }
            (EngineFeedMode::Iterator, AuditMode::Disabled) => {
                let handle = runtime.spawn_blocking(move || {
                    let shutdown_audit = sync_run(&mut feed_rx, &mut engine);
                    (engine, shutdown_audit)
                });

                (handle, None)
            }
            (EngineFeedMode::Stream, AuditMode::Enabled) => {
                // Initialise Audit channel
                let (audit_tx, audit_rx) = mpsc_unbounded();
                let mut audit_tx = ChannelTxDroppable::new(audit_tx);

                let audit = SnapUpdates {
                    snapshot: engine.audit_snapshot(),
                    updates: audit_rx,
                };

                let handle = runtime.spawn(async move {
                    let shutdown_audit =
                        async_run_with_audit(&mut feed_rx, &mut engine, &mut audit_tx).await;
                    (engine, shutdown_audit)
                });

                (handle, Some(audit))
            }
            (EngineFeedMode::Stream, AuditMode::Disabled) => {
                let handle = runtime.spawn(async move {
                    let shutdown_audit = async_run(&mut feed_rx, &mut engine).await;
                    (engine, shutdown_audit)
                });

                (handle, None)
            }
        };

        Ok(System {
            engine,
            handles: SystemAuxillaryHandles {
                execution,
                market_to_engine,
                account_to_engine,
            },
            feed_tx,
            audit,
            market_latency,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::channel::{OverflowPolicy, mpsc_bounded};
    use futures::{StreamExt, stream};

    #[tokio::test]
    async fn merger_preserves_account_event_during_market_burst() {
        let (market_tx, market_rx) = mpsc_bounded(2, OverflowPolicy::DropOldest);
        for value in 0..100u64 {
            market_tx
                .try_send(TimedMarket {
                    event: value,
                    started: Instant::now(),
                })
                .unwrap();
        }
        drop(market_tx);
        let (feed_tx, mut feed_rx) = mpsc_bounded(1, OverflowPolicy::Block);
        let latency = Arc::new(Mutex::new(LatencySamples::default()));
        let merger = tokio::spawn(merge_event_feeds(
            market_rx,
            stream::iter([999u64]),
            feed_tx,
            latency,
        ));
        assert_eq!(StreamExt::next(&mut feed_rx).await, Some(999));
        assert!(StreamExt::next(&mut feed_rx).await.is_some());
        merger.await.unwrap();
    }
}
