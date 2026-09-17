/// Backtesting utilities for algorithmic trading strategies.
///
/// This module provides tools for running historical simulations of trading strategies
/// using market data, and analyzing the performance of these simulations.
use crate::{
    backtest::{
        market_data::BacktestMarketData,
        summary::{BacktestSummary, MultiBacktestSummary},
    },
    engine::{
        Processor,
        clock::HistoricalClock,
        execution_tx::MultiExchangeTxMap,
        state::{EngineState, instrument::data::InstrumentDataState},
    },
    error::BarterError,
    risk::RiskManager,
    statistic::time::TimeInterval,
    strategy::{
        algo::AlgoStrategy, close_positions::ClosePositionsStrategy,
        on_disconnect::OnDisconnectStrategy, on_trading_disabled::OnTradingDisabled,
    },
    system::{builder::EngineFeedMode, config::ExecutionConfig},
};
use crate::{
    engine::Engine,
    execution::builder::{ExecutionBuild, ExecutionBuilder},
    system::builder::{AuditMode, SystemBuild},
};
use barter_data::{
    books::Level,
    event::{DataKind, MarketEvent},
    subscription::book::OrderBookEvent,
};
use barter_execution::{
    AccountEvent,
    exchange::mock::{MockMarketEvent, MockMarketEventKind, MockMarketLevel},
};
use barter_instrument::{index::IndexedInstruments, instrument::InstrumentIndex};
use futures::{StreamExt, future::try_join_all};
use rust_decimal::Decimal;
use smol_str::SmolStr;
use std::{fmt::Debug, sync::Arc};

/// Defines the interface and implementations for different types of market data sources
/// that can be used in backtests.
pub mod market_data;

/// Contains data structures for representing backtest results and metrics.
pub mod summary;

pub trait IntoMockMarketKind {
    fn into_mock_market_kind(&self) -> Option<MockMarketEventKind>;
}

impl IntoMockMarketKind for DataKind {
    fn into_mock_market_kind(&self) -> Option<MockMarketEventKind> {
        let book = match self {
            DataKind::OrderBook(OrderBookEvent::Snapshot(book))
            | DataKind::OrderBook(OrderBookEvent::Update(book)) => book,
            _ => return None,
        };
        let bids = book
            .bids()
            .levels()
            .iter()
            .map(|level: &Level| MockMarketLevel {
                price: level.price,
                quantity: level.amount,
            })
            .collect();
        let asks = book
            .asks()
            .levels()
            .iter()
            .map(|level: &Level| MockMarketLevel {
                price: level.price,
                quantity: level.amount,
            })
            .collect();
        Some(MockMarketEventKind::OrderBook { bids, asks })
    }
}

/// Configuration for constants used across all backtests in a batch.
///
/// Contains shared inputs like instruments, execution configurations,
/// market data, and summary time intervals.
#[derive(Debug, Clone)]
pub struct BacktestArgsConstant<MarketData, SummaryInterval, State> {
    /// Set of trading instruments indexed by unique identifiers.
    pub instruments: IndexedInstruments,
    /// Exchange execution configurations.
    pub executions: Vec<ExecutionConfig>,
    /// Historical market data to use for simulation.
    pub market_data: MarketData,
    /// Time interval for aggregating and reporting summary statistics.
    pub summary_interval: SummaryInterval,
    /// EngineState.
    pub engine_state: State,
}

/// Configuration for variables that can change between individual backtests.
///
/// Contains parameters that define a specific strategy variant to test.
#[derive(Debug, Clone)]
pub struct BacktestArgsDynamic<Strategy, Risk> {
    /// Unique identifier for this backtest.
    pub id: SmolStr,
    /// Risk-free return rate used for performance metrics.
    pub risk_free_return: Decimal,
    /// Trading strategy to backtest.
    pub strategy: Strategy,
    /// Risk management rules.
    pub risk: Risk,
}
/// Run multiple backtests concurrently, each with different strategy parameters.
///
/// Takes the shared constants and an iterator of different strategy configurations,
/// then executes all backtests in parallel, collecting the results.
pub async fn run_backtests<
    MarketData,
    SummaryInterval,
    Strategy,
    Risk,
    GlobalData,
    InstrumentData,
>(
    args_constant: Arc<
        BacktestArgsConstant<MarketData, SummaryInterval, EngineState<GlobalData, InstrumentData>>,
    >,
    args_dynamic_iter: impl IntoIterator<Item = BacktestArgsDynamic<Strategy, Risk>>,
) -> Result<MultiBacktestSummary<SummaryInterval>, BarterError>
where
    MarketData: BacktestMarketData<Kind = InstrumentData::MarketEventKind>,
    SummaryInterval: TimeInterval,
    Strategy: AlgoStrategy<State = EngineState<GlobalData, InstrumentData>>
        + ClosePositionsStrategy<State = EngineState<GlobalData, InstrumentData>>
        + OnTradingDisabled<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + OnDisconnectStrategy<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + Send
        + 'static,
    <Strategy as OnTradingDisabled<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnTradingDisabled: Debug + Clone + Send,
    <Strategy as OnDisconnectStrategy<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnDisconnect: Debug + Clone + Send,
    Risk: RiskManager<State = EngineState<GlobalData, InstrumentData>> + Send + 'static,
    GlobalData: for<'a> Processor<&'a MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>>
        + for<'a> Processor<&'a AccountEvent>
        + Debug
        + Clone
        + Default
        + Send
        + 'static,
    InstrumentData: InstrumentDataState + Default + Send + 'static,
    InstrumentData::MarketEventKind: IntoMockMarketKind,
{
    let time_start = std::time::Instant::now();

    let backtest_futures = args_dynamic_iter
        .into_iter()
        .map(|args_dynamic| backtest(Arc::clone(&args_constant), args_dynamic));

    // Run all backtests concurrently
    let summaries = try_join_all(backtest_futures).await?;

    Ok(MultiBacktestSummary::new(
        std::time::Instant::now().duration_since(time_start),
        summaries,
    ))
}

/// Run a single backtest with the given parameters.
///
/// Simulates a trading strategy using historical market data and generates performance metrics.
pub async fn backtest<MarketData, SummaryInterval, Strategy, Risk, GlobalData, InstrumentData>(
    args_constant: Arc<
        BacktestArgsConstant<MarketData, SummaryInterval, EngineState<GlobalData, InstrumentData>>,
    >,
    args_dynamic: BacktestArgsDynamic<Strategy, Risk>,
) -> Result<BacktestSummary<SummaryInterval>, BarterError>
where
    MarketData: BacktestMarketData<Kind = InstrumentData::MarketEventKind>,
    SummaryInterval: TimeInterval,
    Strategy: AlgoStrategy<State = EngineState<GlobalData, InstrumentData>>
        + ClosePositionsStrategy<State = EngineState<GlobalData, InstrumentData>>
        + OnTradingDisabled<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + OnDisconnectStrategy<
            HistoricalClock,
            EngineState<GlobalData, InstrumentData>,
            MultiExchangeTxMap,
            Risk,
        > + Send
        + 'static,
    <Strategy as OnTradingDisabled<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnTradingDisabled: Debug + Clone + Send,
    <Strategy as OnDisconnectStrategy<
        HistoricalClock,
        EngineState<GlobalData, InstrumentData>,
        MultiExchangeTxMap,
        Risk,
    >>::OnDisconnect: Debug + Clone + Send,
    Risk: RiskManager<State = EngineState<GlobalData, InstrumentData>> + Send + 'static,
    GlobalData: for<'a> Processor<&'a MarketEvent<InstrumentIndex, InstrumentData::MarketEventKind>>
        + for<'a> Processor<&'a AccountEvent>
        + Debug
        + Clone
        + Default
        + Send
        + 'static,
    InstrumentData: InstrumentDataState + Send + 'static,
    InstrumentData::MarketEventKind: IntoMockMarketKind,
{
    let clock = args_constant
        .market_data
        .time_first_event()
        .await
        .map(HistoricalClock::new)?;
    let market_stream = args_constant.market_data.stream().await?;

    // Build Execution infrastructure
    let ExecutionBuild {
        execution_tx_map,
        mock_market_txs,
        account_channel,
        futures,
    } = args_constant
        .executions
        .clone()
        .into_iter()
        .try_fold(
            ExecutionBuilder::new(&args_constant.instruments),
            |builder, config| match config {
                ExecutionConfig::Mock(mock_config) => builder.add_mock(mock_config, clock.clone()),
                ExecutionConfig::BinanceSpot(config) => builder
                    .add_live::<barter_execution::client::BinanceSpot>(
                    config,
                    std::time::Duration::from_secs(5),
                ),
            },
        )?
        .build();

    // Tee normalized L2 events into the deterministic mock exchange without changing the
    // engine-facing stream. Non-book market events are intentionally ignored by the L2 simulator.
    let instruments = args_constant.instruments.clone();
    let market_stream = market_stream.inspect(move |event| {
        if let barter_data::streams::reconnect::Event::Item(market_event) = event {
            if let Some(kind) = market_event.kind.into_mock_market_kind() {
                if let Some(sender) = mock_market_txs.get(&market_event.exchange) {
                    if let Some(instrument) = instruments
                        .instruments()
                        .get(market_event.instrument.index())
                    {
                        let _ = sender.try_send(MockMarketEvent {
                            instrument: instrument.value.name_exchange.clone(),
                            time_exchange: market_event.time_exchange,
                            kind,
                        });
                    }
                }
            }
        }
    });

    let engine = Engine::new(
        clock,
        args_constant.engine_state.clone(),
        execution_tx_map,
        args_dynamic.strategy,
        args_dynamic.risk,
    );

    let system = SystemBuild::new(
        engine,
        EngineFeedMode::Stream,
        AuditMode::Disabled,
        market_stream,
        account_channel,
        futures,
    )
    .init()
    .await?;

    let (engine, _shutdown_audit) = system.shutdown_after_backtest().await?;

    let trading_summary = engine
        .trading_summary_generator(args_dynamic.risk_free_return)
        .generate(args_constant.summary_interval);

    Ok(BacktestSummary {
        id: args_dynamic.id,
        risk_free_return: args_dynamic.risk_free_return,
        trading_summary,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_data::books::OrderBook;
    use barter_data::subscription::book::OrderBookEvent;
    use rust_decimal_macros::dec;

    #[test]
    fn l2_backtest_bridge_preserves_sorted_levels_deterministically() {
        let book = OrderBook::new(
            7,
            None,
            [(dec!(10), dec!(2)), (dec!(9), dec!(1))],
            [(dec!(11), dec!(3)), (dec!(12), dec!(4))],
        );
        let kind = DataKind::OrderBook(OrderBookEvent::Snapshot(book));
        let MockMarketEventKind::OrderBook { bids, asks } = kind.into_mock_market_kind().unwrap()
        else {
            panic!("expected L2 order book");
        };
        assert_eq!(bids[0].price, dec!(10));
        assert_eq!(asks[0].price, dec!(11));
    }
}
