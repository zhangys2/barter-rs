use crate::{
    AccountEventKind, InstrumentAccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::mock::MockExecutionConfig,
    error::{ApiError, UnindexedApiError, UnindexedOrderError},
    exchange::mock::{
        account::AccountState,
        market::MockOrderBook,
        request::{MockExchangeRequest, MockExchangeRequestKind},
    },
    order::{
        Order, OrderEvent, OrderKind, TimeInForce, UnindexedOrder,
        id::OrderId,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{InactiveOrderState, Open, OrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use barter_instrument::{
    Side,
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::{Instrument, name::InstrumentNameExchange},
};
use barter_integration::{channel::BoundedRx, collection::snapshot::Snapshot};
use chrono::{DateTime, TimeDelta, Utc};
use fnv::FnvHashMap;
use futures::stream::BoxStream;
use itertools::Itertools;
use rust_decimal::Decimal;
use smol_str::ToSmolStr;
use std::{
    fmt::{self, Debug},
    sync::Arc,
};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::{error, info};

pub mod account;
pub mod market;
pub use market::{
    ConservativeQueue, FixedLatency, LatencyModel, MockMarketEvent, MockMarketEventKind,
    MockMarketLevel, NoQueue, PriceTimeQueue, QueueModel,
};
pub mod request;

pub struct MockExchange {
    pub exchange: ExchangeId,
    pub latency_ms: u64,
    pub latency_model: Arc<dyn LatencyModel>,
    pub queue_model: Arc<dyn QueueModel>,
    pub fees_percent: Decimal,
    pub request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
    pub market_rx: BoundedRx<MockMarketEvent>,
    pub event_tx: broadcast::Sender<UnindexedAccountEvent>,
    pub instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    pub account: AccountState,
    pub order_sequence: u64,
    pub time_exchange_latest: DateTime<Utc>,
    pub market_books: FnvHashMap<InstrumentNameExchange, MockOrderBook>,
    pub last_trades: FnvHashMap<InstrumentNameExchange, Decimal>,
}

impl fmt::Debug for MockExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MockExchange")
            .field("exchange", &self.exchange)
            .field("latency_ms", &self.latency_ms)
            .field("fees_percent", &self.fees_percent)
            .field("order_sequence", &self.order_sequence)
            .field("time_exchange_latest", &self.time_exchange_latest)
            .field("market_books", &self.market_books)
            .field("last_trades", &self.last_trades)
            .finish()
    }
}

impl MockExchange {
    pub fn new(
        config: MockExecutionConfig,
        request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
        market_rx: BoundedRx<MockMarketEvent>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    ) -> Self {
        Self::new_with_latency_model(
            config,
            request_rx,
            market_rx,
            event_tx,
            instruments,
            Arc::new(FixedLatency),
        )
    }

    pub fn new_with_latency_model(
        config: MockExecutionConfig,
        request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
        market_rx: BoundedRx<MockMarketEvent>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
        latency_model: Arc<dyn LatencyModel>,
    ) -> Self {
        Self::new_with_models(
            config,
            request_rx,
            market_rx,
            event_tx,
            instruments,
            latency_model,
            Arc::new(NoQueue),
        )
    }

    pub fn new_with_models(
        config: MockExecutionConfig,
        request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
        market_rx: BoundedRx<MockMarketEvent>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
        latency_model: Arc<dyn LatencyModel>,
        queue_model: Arc<dyn QueueModel>,
    ) -> Self {
        let fees_percent = config.fees_percent;
        let mut account = AccountState::from(config.initial_state);
        account.restore_reservations(&instruments, fees_percent, DateTime::<Utc>::UNIX_EPOCH);

        Self {
            exchange: config.mocked_exchange,
            latency_ms: config.latency_ms,
            latency_model,
            queue_model,
            fees_percent,
            request_rx,
            market_rx,
            event_tx,
            instruments,
            account,
            order_sequence: 0,
            time_exchange_latest: Default::default(),
            market_books: FnvHashMap::default(),
            last_trades: FnvHashMap::default(),
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(market) = tokio_stream::StreamExt::next(&mut self.market_rx) => {
                    let delay = self.effective_feed_latency_ms();
                    if delay > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    }
                    let mut market = market;
                    let applied = market.applied.take();
                    self.apply_market_event(market);
                    if let Some(applied) = applied {
                        let _ = applied.send(());
                    }
                },
                request = self.request_rx.recv() => {
                    let Some(request) = request else { break; };
                    self.update_time_exchange(request.time_request);

                    match request.kind {
                MockExchangeRequestKind::FetchAccountSnapshot { response_tx } => {
                    let snapshot = self.account_snapshot();
                    self.respond_with_latency(response_tx, snapshot);
                }
                MockExchangeRequestKind::FetchBalances {
                    response_tx,
                    assets,
                } => {
                    let balances = self
                        .account
                        .balances()
                        .filter(|balance| assets.contains(&balance.asset))
                        .cloned()
                        .collect();
                    self.respond_with_latency(response_tx, balances);
                }
                MockExchangeRequestKind::FetchOrdersOpen {
                    response_tx,
                    instruments,
                } => {
                    let orders_open = self
                        .account
                        .orders_open()
                        .filter(|order| instruments.contains(&order.key.instrument))
                        .cloned()
                        .collect();
                    self.respond_with_latency(response_tx, orders_open);
                }
                MockExchangeRequestKind::FetchTrades {
                    response_tx,
                    time_since,
                } => {
                    let trades = self.account.trades(time_since).cloned().collect();
                    self.respond_with_latency(response_tx, trades);
                }
                MockExchangeRequestKind::CancelOrder {
                    response_tx,
                    request,
                } => {
                    let response = self.cancel_order(request);
                    self.respond_with_latency(response_tx, response);
                }
                MockExchangeRequestKind::OpenOrder {
                    response_tx,
                    request,
                } => {
                    let (response, notifications) = self.open_order(request);
                    self.respond_with_latency(response_tx, response);

                    if let Some(notifications) = notifications {
                        if let Some(trade) = &notifications.trade {
                            self.account.ack_trade(trade.clone());
                        }
                        self.send_notifications_with_latency(notifications);
                    }
                }
                    }
                }
            }
        }

        info!(exchange = %self.exchange, "MockExchange shutting down");
    }

    fn apply_market_event(&mut self, event: MockMarketEvent) {
        self.time_exchange_latest = event.time_exchange;
        match event.kind {
            MockMarketEventKind::OrderBook { bids, asks } => {
                self.market_books
                    .entry(event.instrument.clone())
                    .or_default()
                    .update(MockMarketEventKind::OrderBook { bids, asks });
                self.fill_resting_orders(&event.instrument);
            }
            MockMarketEventKind::Trade { price, quantity } => {
                self.last_trades.insert(event.instrument.clone(), price);
                self.fill_resting_orders_on_trade(&event.instrument, price, quantity);
            }
        }
    }

    fn fill_resting_orders(&mut self, instrument: &InstrumentNameExchange) {
        let Some(book) = self.market_books.get(instrument).cloned() else {
            return;
        };
        let orders: Vec<_> = self
            .account
            .orders_open()
            .filter(|order| &order.key.instrument == instrument)
            .cloned()
            .collect();
        for order in orders {
            let remaining = order.quantity.abs() - order.state.filled_quantity.abs();
            if remaining <= Decimal::ZERO {
                continue;
            }
            let fill = match order.side {
                Side::Buy => book.walk_asks_until_with(
                    remaining,
                    Some(order.price),
                    self.queue_model.as_ref(),
                ),
                Side::Sell => book.walk_bids_until_with(
                    remaining,
                    Some(order.price),
                    self.queue_model.as_ref(),
                ),
            };
            if let Some((quantity, price)) = fill {
                self.fill_resting_order(&order, quantity, price);
            }
        }
    }

    fn fill_resting_orders_on_trade(
        &mut self,
        instrument: &InstrumentNameExchange,
        trade_price: Decimal,
        trade_quantity: Decimal,
    ) {
        let orders: Vec<_> = self
            .account
            .orders_open()
            .filter(|order| &order.key.instrument == instrument)
            .cloned()
            .collect();
        let mut remaining_trade = trade_quantity;
        for order in orders {
            if remaining_trade <= Decimal::ZERO {
                break;
            }
            let remaining = order.quantity.abs() - order.state.filled_quantity.abs();
            let quantity = self.queue_model.executable_trade_quantity(
                remaining_trade,
                remaining,
                order.side,
                order.price,
                trade_price,
            );
            if quantity > Decimal::ZERO {
                remaining_trade -= quantity;
                self.fill_resting_order(&order, quantity, order.price);
            }
        }
    }

    fn fill_resting_order(
        &mut self,
        order: &Order<ExchangeId, InstrumentNameExchange, Open>,
        quantity: Decimal,
        price: Decimal,
    ) {
        let Some(instrument_data) = self.instruments.get(&order.key.instrument) else {
            return;
        };
        let underlying = instrument_data.underlying.clone();
        let notional = quantity * price;
        let fees_quote = notional * self.fees_percent;
        let (debit_asset, debit_amount, credit_asset, credit_amount) = match order.side {
            Side::Buy => (
                underlying.quote.clone(),
                notional + fees_quote,
                underlying.base.clone(),
                quantity,
            ),
            Side::Sell => (
                underlying.base.clone(),
                quantity,
                underlying.quote.clone(),
                notional - fees_quote,
            ),
        };
        let previous_reservation = self.account.reservation(&order.key.cid).cloned();
        if previous_reservation.is_some() {
            self.account
                .release_reservation(&order.key.cid, self.time_exchange_latest);
        }
        let Ok(mut balances) = self.account.apply_fill_balances(
            &debit_asset,
            debit_amount,
            &credit_asset,
            credit_amount,
            self.time_exchange_latest,
        ) else {
            if let Some((asset, amount)) = previous_reservation {
                let _ = self.account.reserve_balance(
                    &order.key.cid,
                    &asset,
                    amount,
                    self.time_exchange_latest,
                );
            }
            return;
        };
        let filled_quantity = order.state.filled_quantity.abs() + quantity;
        let remaining_after_fill = order.quantity.abs() - filled_quantity;
        if let Some(open) = self.account.open_order_mut(&order.key.cid) {
            open.state.filled_quantity = filled_quantity;
        }
        let order_snapshot = if remaining_after_fill <= Decimal::ZERO {
            self.account
                .remove_filled_order(&order.key.cid)
                .map(|filled| {
                    Snapshot(Order {
                        key: filled.key,
                        side: filled.side,
                        price: filled.price,
                        quantity: filled.quantity,
                        kind: filled.kind,
                        time_in_force: filled.time_in_force,
                        state: OrderState::Inactive(InactiveOrderState::FullyFilled),
                    })
                })
        } else {
            self.account
                .open_order_mut(&order.key.cid)
                .cloned()
                .map(|open| {
                    Snapshot(Order {
                        key: open.key,
                        side: open.side,
                        price: open.price,
                        quantity: open.quantity,
                        kind: open.kind,
                        time_in_force: open.time_in_force,
                        state: OrderState::active(open.state),
                    })
                })
        };
        if remaining_after_fill > Decimal::ZERO {
            let reservation_amount = match order.side {
                Side::Buy => {
                    remaining_after_fill * order.price * (Decimal::ONE + self.fees_percent)
                }
                Side::Sell => remaining_after_fill,
            };
            if self
                .account
                .reserve_balance(
                    &order.key.cid,
                    &debit_asset,
                    reservation_amount,
                    self.time_exchange_latest,
                )
                .is_err()
            {
                return;
            }
        }
        if let Some(balance) = self.account.balance(&debit_asset).cloned() {
            if let Some(snapshot) = balances
                .iter_mut()
                .find(|snapshot| snapshot.asset == debit_asset)
            {
                *snapshot = balance;
            }
        }
        let trade_id = self.order_id_sequence_fetch_add().0;
        let trade = Trade {
            id: TradeId(trade_id.clone()),
            order_id: order.state.id.clone(),
            instrument: order.key.instrument.clone(),
            strategy: order.key.strategy.clone(),
            time_exchange: self.time_exchange_latest,
            side: order.side,
            price,
            quantity,
            fees: AssetFees::quote_fees(fees_quote),
        };
        self.account.ack_trade(trade.clone());
        self.send_notifications_with_latency(OpenOrderNotifications {
            balances: balances.into_iter().map(Snapshot).collect(),
            trade: Some(trade),
            order: order_snapshot,
        });
    }

    fn update_time_exchange(&mut self, time_request: DateTime<Utc>) {
        let client_to_exchange_latency = self.effective_latency_ms() / 2;

        self.time_exchange_latest = time_request
            .checked_add_signed(TimeDelta::milliseconds(client_to_exchange_latency as i64))
            .unwrap_or(time_request);

        self.account.update_time_exchange(self.time_exchange_latest)
    }

    fn effective_feed_latency_ms(&self) -> u64 {
        self.latency_model.feed_delay_ms(self.latency_ms)
    }

    fn effective_order_latency_ms(&self) -> u64 {
        self.latency_model.order_delay_ms(self.latency_ms)
    }

    fn effective_latency_ms(&self) -> u64 {
        self.effective_order_latency_ms()
    }

    pub fn time_exchange(&self) -> DateTime<Utc> {
        self.time_exchange_latest
    }

    pub fn account_snapshot(&self) -> UnindexedAccountSnapshot {
        let balances = self.account.balances().cloned().collect();

        let orders_open = self
            .account
            .orders_open()
            .cloned()
            .map(UnindexedOrder::from);

        let orders_cancelled = self
            .account
            .orders_cancelled()
            .cloned()
            .map(UnindexedOrder::from);

        let orders_all = orders_open.chain(orders_cancelled);
        let orders_all = orders_all.sorted_unstable_by_key(|order| order.key.instrument.clone());
        let orders_by_instrument = orders_all.chunk_by(|order| order.key.instrument.clone());

        let instruments = orders_by_instrument
            .into_iter()
            .map(|(instrument, orders)| InstrumentAccountSnapshot {
                instrument,
                orders: orders.into_iter().collect(),
            })
            .collect();

        UnindexedAccountSnapshot {
            exchange: self.exchange,
            balances,
            instruments,
        }
    }

    /// Sends the provided `Response` via the [`oneshot::Sender`] after waiting for the latency
    /// [`Duration`].
    ///
    /// Used to simulate network latency between the exchange and client.
    fn respond_with_latency<Response>(
        &self,
        response_tx: oneshot::Sender<Response>,
        response: Response,
    ) where
        Response: Send + 'static,
    {
        let exchange = self.exchange;
        let latency = std::time::Duration::from_millis(self.effective_latency_ms());

        tokio::spawn(async move {
            tokio::time::sleep(latency).await;
            if response_tx.send(response).is_err() {
                error!(
                    %exchange,
                    kind = std::any::type_name::<Response>(),
                    "MockExchange failed to send oneshot response to client"
                );
            }
        });
    }

    /// Sends the provided `OpenOrderNotifications` via the `MockExchanges`
    /// `broadcast::Sender<UnindexedAccountEvent>` after waiting for the latency
    /// [`Duration`].
    ///
    /// Used to simulate network latency between the exchange and client.
    fn send_notifications_with_latency(&self, notifications: OpenOrderNotifications) {
        let balances = notifications
            .balances
            .into_iter()
            .map(|balance| self.build_account_event(balance))
            .collect::<Vec<_>>();
        let trade = notifications
            .trade
            .map(|trade| self.build_account_event(trade));
        let order = notifications
            .order
            .map(|order| self.build_account_event(order));

        let exchange = self.exchange;
        let latency = std::time::Duration::from_millis(self.effective_latency_ms());
        let tx = self.event_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(latency).await;

            for balance in balances {
                if tx.send(balance).is_err() {
                    error!(
                        %exchange,
                        kind = "Snapshot<AssetBalance<AssetNameExchange>",
                        "MockExchange failed to send AccountEvent notification to client"
                    );
                }
            }

            if let Some(trade) = trade {
                if tx.send(trade).is_err() {
                    error!(
                        %exchange,
                        kind = "Trade<QuoteAsset, InstrumentNameExchange>",
                        "MockExchange failed to send AccountEvent notification to client"
                    );
                }
            }
            if let Some(order) = order {
                if tx.send(order).is_err() {
                    error!(
                        %exchange,
                        kind = "OrderSnapshot",
                        "MockExchange failed to send OrderSnapshot notification to client"
                    );
                }
            }
        });
    }

    pub fn account_stream(&self) -> BoxStream<'static, UnindexedAccountEvent> {
        futures::StreamExt::boxed(BroadcastStream::new(self.event_tx.subscribe()).map_while(
            |result| match result {
                Ok(event) => Some(event),
                Err(error) => {
                    error!(
                        ?error,
                        "MockExchange Broadcast AccountStream lagged - terminating"
                    );
                    None
                }
            },
        ))
    }

    pub fn cancel_order(
        &mut self,
        request: OrderRequestCancel<ExchangeId, InstrumentNameExchange>,
    ) -> UnindexedOrderResponseCancel {
        let key = request.key.clone();
        let state = self.account.cancel_order(
            &request.key.cid,
            request.state.id.as_ref(),
            self.time_exchange(),
        );
        OrderEvent { key, state }
    }

    pub fn open_order(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>,
        Option<OpenOrderNotifications>,
    ) {
        if self
            .account
            .orders_open()
            .any(|order| order.key.cid == request.key.cid)
        {
            return (
                build_open_order_err_response(
                    request,
                    ApiError::OrderRejected("client order id is already active".into()),
                ),
                None,
            );
        }

        if let Err(error) = self.validate_order_kind_supported(request.state.kind) {
            return (build_open_order_err_response(request, error), None);
        }

        if request.state.price <= Decimal::ZERO || request.state.quantity <= Decimal::ZERO {
            return (
                build_open_order_err_response(
                    request,
                    ApiError::OrderRejected("price and quantity must be positive".into()),
                ),
                None,
            );
        }

        let underlying = match self.find_instrument_data(&request.key.instrument) {
            Ok(instrument) => instrument.underlying.clone(),
            Err(error) => return (build_open_order_err_response(request, error), None),
        };

        let time_exchange = self.time_exchange();

        let requested_quantity = request.state.quantity.abs();
        let (quantity, execution_price) = if request.state.kind == OrderKind::Market {
            let fill = self
                .market_books
                .get(&request.key.instrument)
                .and_then(|book| match request.state.side {
                    Side::Buy => book.walk_asks_until_with(requested_quantity, None, &NoQueue),
                    Side::Sell => book.walk_bids_until_with(requested_quantity, None, &NoQueue),
                })
                .or_else(|| {
                    self.last_trades
                        .get(&request.key.instrument)
                        .copied()
                        .map(|price| (requested_quantity, price))
                });
            let Some((quantity, execution_price)) = fill else {
                return (
                    build_open_order_err_response(
                        request,
                        ApiError::OrderRejected(
                            "market data is unavailable for this instrument".into(),
                        ),
                    ),
                    None,
                );
            };
            if matches!(request.state.time_in_force, TimeInForce::FillOrKill)
                && quantity < requested_quantity
            {
                return (
                    build_open_order_err_response(
                        request,
                        ApiError::OrderRejected(
                            "market fill-or-kill was only partially executable".into(),
                        ),
                    ),
                    None,
                );
            }
            (quantity, execution_price)
        } else {
            let fill =
                self.market_books
                    .get(&request.key.instrument)
                    .and_then(|book| match request.state.side {
                        Side::Buy => book.walk_asks_until_with(
                            requested_quantity,
                            Some(request.state.price),
                            &NoQueue,
                        ),
                        Side::Sell => book.walk_bids_until_with(
                            requested_quantity,
                            Some(request.state.price),
                            &NoQueue,
                        ),
                    });
            if request.state.time_in_force == (TimeInForce::GoodUntilCancelled { post_only: true })
                && fill.is_some()
            {
                return (
                    build_open_order_err_response(
                        request,
                        ApiError::OrderRejected(
                            "post-only limit order would cross the book".into(),
                        ),
                    ),
                    None,
                );
            }
            let Some((filled, price)) = fill else {
                if matches!(
                    request.state.time_in_force,
                    TimeInForce::ImmediateOrCancel | TimeInForce::FillOrKill
                ) {
                    return (
                        build_open_order_err_response(
                            request,
                            ApiError::OrderRejected(
                                "limit order did not cross the available L2 book".into(),
                            ),
                        ),
                        None,
                    );
                }
                let order_id = self.order_id_sequence_fetch_add();
                let order = Order {
                    key: request.key.clone(),
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: Open {
                        id: order_id,
                        time_exchange,
                        filled_quantity: Decimal::ZERO,
                    },
                };
                let reservation_amount = match order.side {
                    Side::Buy => {
                        order.quantity.abs() * order.price * (Decimal::ONE + self.fees_percent)
                    }
                    Side::Sell => order.quantity.abs(),
                };
                let reservation_asset = match order.side {
                    Side::Buy => underlying.quote.clone(),
                    Side::Sell => underlying.base.clone(),
                };
                if let Err(error) = self.account.reserve_balance(
                    &order.key.cid,
                    &reservation_asset,
                    reservation_amount,
                    time_exchange,
                ) {
                    return (build_open_order_err_response(request, error), None);
                }
                self.account.insert_open_order(order.clone());
                return (
                    Order {
                        key: order.key,
                        side: order.side,
                        price: order.price,
                        quantity: order.quantity,
                        kind: order.kind,
                        time_in_force: order.time_in_force,
                        state: Ok(order.state),
                    },
                    None,
                );
            };
            if matches!(request.state.time_in_force, TimeInForce::FillOrKill)
                && filled < requested_quantity
            {
                return (
                    build_open_order_err_response(
                        request,
                        ApiError::OrderRejected(
                            "limit fill-or-kill was only partially executable".into(),
                        ),
                    ),
                    None,
                );
            }
            (filled, price)
        };
        let order_value_quote = execution_price * quantity;
        let fees_quote = order_value_quote * self.fees_percent;
        let (debit_asset, debit_amount, credit_asset, credit_amount) = match request.state.side {
            Side::Buy => (
                underlying.quote.clone(),
                order_value_quote + fees_quote,
                underlying.base.clone(),
                quantity,
            ),
            Side::Sell => (
                underlying.base.clone(),
                quantity,
                underlying.quote.clone(),
                order_value_quote - fees_quote,
            ),
        };

        let Some(debit_balance) = self.account.balance(&debit_asset) else {
            return (
                build_open_order_err_response(
                    request,
                    ApiError::AssetInvalid(
                        debit_asset,
                        "MockExchange has no configured balance for this asset".into(),
                    ),
                ),
                None,
            );
        };
        if debit_balance.balance.free < debit_amount {
            return (
                build_open_order_err_response(
                    request,
                    ApiError::BalanceInsufficient(
                        debit_asset,
                        format!(
                            "Available Balance: {}, Required Balance: {}",
                            debit_balance.balance.free, debit_amount
                        ),
                    ),
                ),
                None,
            );
        }
        if self.account.balance(&credit_asset).is_none() {
            return (
                build_open_order_err_response(
                    request,
                    ApiError::AssetInvalid(
                        credit_asset,
                        "MockExchange has no configured balance for this asset".into(),
                    ),
                ),
                None,
            );
        }

        let remaining = requested_quantity - quantity;
        let should_rest = request.state.kind == OrderKind::Limit
            && remaining > Decimal::ZERO
            && matches!(
                request.state.time_in_force,
                TimeInForce::GoodUntilCancelled { .. } | TimeInForce::GoodUntilEndOfDay
            );
        if should_rest {
            let reserve_amount = match request.state.side {
                Side::Buy => remaining * request.state.price * (Decimal::ONE + self.fees_percent),
                Side::Sell => remaining,
            };
            let available_after_fill = self
                .account
                .balance(&debit_asset)
                .map(|balance| balance.balance.free)
                .unwrap_or_default();
            if available_after_fill < debit_amount + reserve_amount {
                return (
                    build_open_order_err_response(
                        request,
                        ApiError::BalanceInsufficient(
                            debit_asset,
                            format!(
                                "Available Balance: {}, Required Balance: {}",
                                available_after_fill, reserve_amount
                            ),
                        ),
                    ),
                    None,
                );
            }
        }

        let balance_snapshots = match self.account.apply_fill_balances(
            &debit_asset,
            debit_amount,
            &credit_asset,
            credit_amount,
            time_exchange,
        ) {
            Ok(balances) => balances.into_iter().map(Snapshot).collect(),
            Err(error) => return (build_open_order_err_response(request, error), None),
        };
        let fees = AssetFees::quote_fees(fees_quote);

        let order_id = self.order_id_sequence_fetch_add();
        let trade_id = TradeId(order_id.0.clone());

        if should_rest {
            let reserve_amount = match request.state.side {
                Side::Buy => remaining * request.state.price * (Decimal::ONE + self.fees_percent),
                Side::Sell => remaining,
            };
            let _ = self.account.reserve_balance(
                &request.key.cid,
                &debit_asset,
                reserve_amount,
                time_exchange,
            );
            self.account.insert_open_order(Order {
                key: request.key.clone(),
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: Open {
                    id: order_id.clone(),
                    time_exchange,
                    filled_quantity: quantity,
                },
            });
        }

        let order_response = Order {
            key: request.key.clone(),
            side: request.state.side,
            price: execution_price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            state: Ok(Open {
                id: order_id.clone(),
                time_exchange: self.time_exchange(),
                filled_quantity: quantity,
            }),
        };

        let order_snapshot = if should_rest {
            self.account
                .orders_open()
                .find(|order| order.key.cid == request.key.cid)
                .cloned()
                .map(|open| {
                    Snapshot(Order {
                        key: open.key,
                        side: open.side,
                        price: open.price,
                        quantity: open.quantity,
                        kind: open.kind,
                        time_in_force: open.time_in_force,
                        state: OrderState::active(open.state),
                    })
                })
        } else {
            let inactive = if remaining > Decimal::ZERO
                && request.state.time_in_force == TimeInForce::ImmediateOrCancel
            {
                InactiveOrderState::Expired
            } else {
                InactiveOrderState::FullyFilled
            };
            Some(Snapshot(Order {
                key: request.key.clone(),
                side: request.state.side,
                price: execution_price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: OrderState::Inactive(inactive),
            }))
        };
        let notifications = OpenOrderNotifications {
            balances: balance_snapshots,
            trade: Some(Trade {
                id: trade_id,
                order_id: order_id.clone(),
                instrument: request.key.instrument,
                strategy: request.key.strategy,
                time_exchange: self.time_exchange(),
                side: request.state.side,
                price: execution_price,
                quantity,
                fees,
            }),
            order: order_snapshot,
        };

        (order_response, Some(notifications))
    }

    pub fn validate_order_kind_supported(
        &self,
        order_kind: OrderKind,
    ) -> Result<(), UnindexedOrderError> {
        if matches!(order_kind, OrderKind::Market | OrderKind::Limit) {
            Ok(())
        } else {
            Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                format!("MockExchange does not supported OrderKind: {order_kind}"),
            )))
        }
    }

    pub fn find_instrument_data(
        &self,
        instrument: &InstrumentNameExchange,
    ) -> Result<&Instrument<ExchangeId, AssetNameExchange>, UnindexedApiError> {
        self.instruments.get(instrument).ok_or_else(|| {
            ApiError::InstrumentInvalid(
                instrument.clone(),
                format!("MockExchange is not set-up for managing: {instrument}"),
            )
        })
    }

    fn order_id_sequence_fetch_add(&mut self) -> OrderId {
        let sequence = self.order_sequence;
        self.order_sequence += 1;
        OrderId::new(sequence.to_smolstr())
    }

    fn build_account_event<Kind>(&self, kind: Kind) -> UnindexedAccountEvent
    where
        Kind: Into<AccountEventKind<ExchangeId, AssetNameExchange, InstrumentNameExchange>>,
    {
        UnindexedAccountEvent {
            exchange: self.exchange,
            kind: kind.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AccountSnapshot, InstrumentAccountSnapshot,
        balance::Balance,
        order::{
            OrderEvent, OrderKey, TimeInForce,
            id::{ClientOrderId, StrategyId},
            request::{RequestCancel, RequestOpen},
        },
    };
    use barter_instrument::{Underlying, instrument::Instrument};
    use barter_integration::channel::{OverflowPolicy, mpsc_bounded};

    fn exchange() -> MockExchange {
        let base = AssetNameExchange::from("BTC");
        let quote = AssetNameExchange::from("USDT");
        let time = DateTime::<Utc>::UNIX_EPOCH;
        let balances = vec![
            AssetBalance {
                asset: base.clone(),
                balance: Balance {
                    total: Decimal::new(100, 0),
                    free: Decimal::new(100, 0),
                },
                time_exchange: time,
            },
            AssetBalance {
                asset: quote.clone(),
                balance: Balance {
                    total: Decimal::new(10_000, 0),
                    free: Decimal::new(10_000, 0),
                },
                time_exchange: time,
            },
        ];
        let snapshot = AccountSnapshot {
            exchange: ExchangeId::Mock,
            balances,
            instruments: vec![InstrumentAccountSnapshot {
                instrument: InstrumentNameExchange::from("BTCUSDT"),
                orders: vec![],
            }],
        };
        let (_request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, _) = broadcast::channel(8);
        let instrument = Instrument::spot(
            ExchangeId::Mock,
            "BTCUSDT",
            "BTCUSDT",
            Underlying::new(base, quote),
            None,
        );
        let instruments: FnvHashMap<_, _> = [(InstrumentNameExchange::from("BTCUSDT"), instrument)]
            .into_iter()
            .collect();
        MockExchange::new(
            MockExecutionConfig {
                mocked_exchange: ExchangeId::Mock,
                initial_state: snapshot,
                latency_ms: 0,
                fees_percent: Decimal::ZERO,
            },
            request_rx,
            mpsc_bounded(1, OverflowPolicy::DropOldest).1,
            event_tx,
            instruments,
        )
    }

    fn request(
        side: Side,
        quantity: i64,
        price: i64,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        OrderEvent {
            key: OrderKey {
                exchange: ExchangeId::Mock,
                instrument: InstrumentNameExchange::from("BTCUSDT"),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new(format!("cid-{quantity}-{price}")),
            },
            state: RequestOpen {
                side,
                price: Decimal::new(price, 0),
                quantity: Decimal::new(quantity, 0),
                kind: OrderKind::Market,
                time_in_force: TimeInForce::ImmediateOrCancel,
            },
        }
    }

    #[tokio::test]
    async fn conservative_queue_fills_only_on_public_trade() {
        let mut exchange = exchange();
        exchange.queue_model = Arc::new(ConservativeQueue);
        let mut resting = request(Side::Buy, 1, 8);
        resting.state.kind = OrderKind::Limit;
        resting.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: false };
        let (response, _) = exchange.open_order(resting);
        assert!(response.state.is_ok());
        exchange.apply_market_event(MockMarketEvent {
            instrument: InstrumentNameExchange::from("BTCUSDT"),
            time_exchange: DateTime::<Utc>::UNIX_EPOCH,
            kind: MockMarketEventKind::OrderBook {
                bids: vec![],
                asks: vec![MockMarketLevel {
                    price: Decimal::from(8),
                    quantity: Decimal::from(1),
                }],
            },
            applied: None,
        });
        assert_eq!(
            exchange
                .account
                .orders_open()
                .next()
                .unwrap()
                .state
                .filled_quantity,
            Decimal::ZERO
        );
        exchange.apply_market_event(MockMarketEvent {
            instrument: InstrumentNameExchange::from("BTCUSDT"),
            time_exchange: DateTime::<Utc>::UNIX_EPOCH,
            kind: MockMarketEventKind::Trade {
                price: Decimal::from(8),
                quantity: Decimal::from(1),
            },
            applied: None,
        });
        assert_eq!(exchange.account.orders_open().count(), 0);
    }

    #[tokio::test]
    async fn limit_orders_rest_or_fill_against_l2_and_ioc_does_not_rest() {
        let mut exchange = exchange();
        exchange.market_books.insert(
            InstrumentNameExchange::from("BTCUSDT"),
            MockOrderBook {
                asks: vec![MockMarketLevel {
                    price: Decimal::from(10),
                    quantity: Decimal::from(2),
                }],
                bids: vec![MockMarketLevel {
                    price: Decimal::from(9),
                    quantity: Decimal::from(2),
                }],
            },
        );

        let balances_before = exchange.account.balances().cloned().collect::<Vec<_>>();
        let mut resting = request(Side::Buy, 1, 8);
        resting.state.kind = OrderKind::Limit;
        resting.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: false };
        let (response, notifications) = exchange.open_order(resting);
        assert!(response.state.is_ok());
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("USDT"))
                .unwrap()
                .balance
                .total,
            balances_before
                .iter()
                .find(|balance| balance.asset == AssetNameExchange::from("USDT"))
                .unwrap()
                .balance
                .total
        );
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("USDT"))
                .unwrap()
                .balance
                .free,
            Decimal::from(9_992)
        );
        assert_eq!(
            exchange.account.reservation(&response.key.cid),
            Some(&(AssetNameExchange::from("USDT"), Decimal::from(8)))
        );
        assert_eq!(
            response.state.as_ref().unwrap().filled_quantity,
            Decimal::ZERO
        );
        assert!(notifications.is_none());
        assert_eq!(exchange.account.orders_open().count(), 1);

        exchange.apply_market_event(MockMarketEvent {
            instrument: InstrumentNameExchange::from("BTCUSDT"),
            time_exchange: DateTime::<Utc>::UNIX_EPOCH,
            kind: MockMarketEventKind::OrderBook {
                bids: vec![],
                asks: vec![MockMarketLevel {
                    price: Decimal::from(7),
                    quantity: Decimal::from(1),
                }],
            },
            applied: None,
        });
        assert_eq!(exchange.account.orders_open().count(), 0);

        let mut ioc = request(Side::Buy, 1, 6);
        ioc.state.kind = OrderKind::Limit;
        let (response, _) = exchange.open_order(ioc);
        assert!(response.state.is_err());
        assert_eq!(exchange.account.orders_open().count(), 0);

        let mut crossing = request(Side::Buy, 1, 7);
        crossing.state.kind = OrderKind::Limit;
        crossing.state.time_in_force = TimeInForce::ImmediateOrCancel;
        let (response, notifications) = exchange.open_order(crossing);
        assert_eq!(
            response.state.as_ref().unwrap().filled_quantity,
            Decimal::from(1)
        );
        assert_eq!(response.price, Decimal::from(7));
        assert!(notifications.is_some());
    }

    #[test]
    fn duplicate_active_client_order_id_is_rejected_without_leaking_reservation() {
        let mut exchange = exchange();
        let mut resting = request(Side::Buy, 1, 8);
        resting.state.kind = OrderKind::Limit;
        resting.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: false };
        let duplicate = resting.clone();

        let (accepted, _) = exchange.open_order(resting);
        assert!(accepted.state.is_ok());
        let balance_before = exchange
            .account
            .balance(&AssetNameExchange::from("USDT"))
            .unwrap()
            .balance;

        let (rejected, notifications) = exchange.open_order(duplicate);
        assert!(matches!(
            rejected.state,
            Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(_)))
        ));
        assert!(notifications.is_none());
        assert_eq!(exchange.account.orders_open().count(), 1);
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("USDT"))
                .unwrap()
                .balance,
            balance_before
        );
        assert_eq!(
            exchange.account.reservation(&accepted.key.cid),
            Some(&(AssetNameExchange::from("USDT"), Decimal::from(8)))
        );
    }

    #[test]
    fn restored_active_order_cancel_releases_reservation() {
        let mut source = exchange();
        let mut resting = request(Side::Buy, 1, 8);
        resting.state.kind = OrderKind::Limit;
        resting.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: false };
        let (accepted, _) = source.open_order(resting);
        let cid = accepted.key.cid.clone();
        let snapshot = source.account_snapshot();

        let mut restored = AccountState::from(snapshot);
        restored.restore_reservations(
            &source.instruments,
            source.fees_percent,
            DateTime::<Utc>::UNIX_EPOCH,
        );
        assert_eq!(
            restored.reservation(&cid),
            Some(&(AssetNameExchange::from("USDT"), Decimal::from(8)))
        );
        let balance_before_cancel = restored
            .balance(&AssetNameExchange::from("USDT"))
            .unwrap()
            .balance;
        let cancelled = restored
            .cancel_order(&cid, None, DateTime::<Utc>::UNIX_EPOCH)
            .unwrap();
        assert_eq!(cancelled.id, accepted.state.unwrap().id);
        assert_eq!(restored.reservation(&cid), None);
        assert_eq!(
            restored
                .balance(&AssetNameExchange::from("USDT"))
                .unwrap()
                .balance
                .free,
            balance_before_cancel.free + Decimal::from(8)
        );
    }

    #[test]
    fn sell_side_resting_order_reserves_base_and_cancel_restores_it() {
        let mut exchange = exchange();
        let mut resting = request(Side::Sell, 3, 11);
        resting.state.kind = OrderKind::Limit;
        resting.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: false };
        let key = resting.key.clone();
        let (accepted, notifications) = exchange.open_order(resting);
        assert!(accepted.state.is_ok());
        assert!(notifications.is_none());
        assert_eq!(
            exchange.account.reservation(&key.cid),
            Some(&(AssetNameExchange::from("BTC"), Decimal::from(3)))
        );
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("BTC"))
                .unwrap()
                .balance,
            Balance {
                total: Decimal::from(100),
                free: Decimal::from(97),
            }
        );

        let cancelled = exchange.cancel_order(OrderEvent {
            key,
            state: RequestCancel { id: None },
        });
        assert!(cancelled.state.is_ok());
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("BTC"))
                .unwrap()
                .balance,
            Balance {
                total: Decimal::from(100),
                free: Decimal::from(100),
            }
        );
    }

    #[tokio::test]
    async fn partial_resting_fill_adjusts_reservation_and_cancel_releases_remainder() {
        let mut exchange = exchange();
        exchange.market_books.insert(
            InstrumentNameExchange::from("BTCUSDT"),
            MockOrderBook {
                asks: vec![MockMarketLevel {
                    price: Decimal::from(10),
                    quantity: Decimal::from(2),
                }],
                bids: vec![],
            },
        );

        let mut resting = request(Side::Buy, 2, 8);
        resting.state.kind = OrderKind::Limit;
        resting.state.time_in_force = TimeInForce::GoodUntilCancelled { post_only: false };
        let key = resting.key.clone();
        let (response, notifications) = exchange.open_order(resting);
        assert!(response.state.is_ok());
        assert!(notifications.is_none());
        assert_eq!(
            exchange.account.reservation(&key.cid),
            Some(&(AssetNameExchange::from("USDT"), Decimal::from(16)))
        );
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("USDT"))
                .unwrap()
                .balance,
            Balance {
                total: Decimal::from(10_000),
                free: Decimal::from(9_984),
            }
        );

        exchange.apply_market_event(MockMarketEvent {
            instrument: InstrumentNameExchange::from("BTCUSDT"),
            time_exchange: DateTime::<Utc>::UNIX_EPOCH,
            kind: MockMarketEventKind::OrderBook {
                bids: vec![],
                asks: vec![MockMarketLevel {
                    price: Decimal::from(7),
                    quantity: Decimal::from(1),
                }],
            },
            applied: None,
        });
        assert_eq!(
            exchange.account.reservation(&key.cid),
            Some(&(AssetNameExchange::from("USDT"), Decimal::from(8)))
        );
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("USDT"))
                .unwrap()
                .balance,
            Balance {
                total: Decimal::from(9_993),
                free: Decimal::from(9_985),
            }
        );

        let cancelled = exchange.cancel_order(OrderEvent {
            key,
            state: RequestCancel { id: None },
        });
        assert!(cancelled.state.is_ok());
        assert!(exchange.account.reservation(&cancelled.key.cid).is_none());
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("USDT"))
                .unwrap()
                .balance,
            Balance {
                total: Decimal::from(9_993),
                free: Decimal::from(9_993),
            }
        );
    }

    #[test]
    fn open_order_updates_both_assets_for_buy_and_sell() {
        let mut exchange = exchange();
        exchange.fees_percent = Decimal::new(1, 2);
        exchange.market_books.insert(
            InstrumentNameExchange::from("BTCUSDT"),
            MockOrderBook {
                asks: vec![MockMarketLevel {
                    price: Decimal::from(10),
                    quantity: Decimal::from(10),
                }],
                bids: vec![MockMarketLevel {
                    price: Decimal::from(10),
                    quantity: Decimal::from(10),
                }],
            },
        );
        let (buy, notifications) = exchange.open_order(request(Side::Buy, 2, 10));
        assert!(buy.state.is_ok());
        let notifications = notifications.expect("accepted fill emits notifications");
        assert_eq!(notifications.balances.len(), 2);
        assert!(notifications.balances.iter().any(|balance| {
            balance.0.asset == AssetNameExchange::from("USDT")
                && balance.0.balance.free == Decimal::new(99798, 1)
        }));
        assert!(notifications.balances.iter().any(|balance| {
            balance.0.asset == AssetNameExchange::from("BTC")
                && balance.0.balance.free == Decimal::new(102, 0)
        }));
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("USDT"))
                .unwrap()
                .balance
                .free,
            Decimal::new(99798, 1)
        );
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("BTC"))
                .unwrap()
                .balance
                .free,
            Decimal::new(102, 0)
        );
        let (sell, _) = exchange.open_order(request(Side::Sell, 1, 10));
        assert!(sell.state.is_ok());
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("BTC"))
                .unwrap()
                .balance
                .free,
            Decimal::new(101, 0)
        );
        assert_eq!(
            exchange
                .account
                .balance(&AssetNameExchange::from("USDT"))
                .unwrap()
                .balance
                .free,
            Decimal::new(998970, 2)
        );
    }

    #[tokio::test]
    async fn run_returns_typed_cancel_error_for_unknown_order() {
        let mut exchange = exchange();
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        exchange.request_rx = request_rx;
        let cancel = OrderEvent {
            key: request(Side::Buy, 1, 1).key,
            state: RequestCancel { id: None },
        };
        let (response_tx, response_rx) = oneshot::channel();
        request_tx
            .send(MockExchangeRequest::cancel_order(
                DateTime::<Utc>::UNIX_EPOCH,
                response_tx,
                cancel,
            ))
            .unwrap();
        let task = tokio::spawn(exchange.run());
        let response = response_rx.await.unwrap();
        assert!(matches!(
            response.state,
            Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(_)))
        ));
        drop(request_tx);
        task.await.unwrap();
    }

    #[test]
    fn non_positive_price_is_rejected_without_mutating_balances() {
        for price in [0, -1] {
            let mut exchange = exchange();
            let before = exchange.account.balances().cloned().collect::<Vec<_>>();
            let mut bad = request(Side::Sell, 1, 1);
            bad.state.price = Decimal::new(price, 0);
            let (response, notifications) = exchange.open_order(bad);
            assert!(matches!(
                response.state,
                Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(_)))
            ));
            assert!(notifications.is_none());
            assert_eq!(
                exchange.account.balances().cloned().collect::<Vec<_>>(),
                before
            );
        }
    }
}

fn build_open_order_err_response<E>(
    request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    error: E,
) -> Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>
where
    E: Into<UnindexedOrderError>,
{
    Order {
        key: request.key,
        side: request.state.side,
        price: request.state.price,
        quantity: request.state.quantity,
        kind: request.state.kind,
        time_in_force: request.state.time_in_force,
        state: Err(error.into()),
    }
}

#[derive(Debug)]
pub struct OpenOrderNotifications {
    pub balances: Vec<Snapshot<AssetBalance<AssetNameExchange>>>,
    pub trade: Option<Trade<QuoteAsset, InstrumentNameExchange>>,
    pub order: Option<
        Snapshot<
            Order<
                ExchangeId,
                InstrumentNameExchange,
                OrderState<AssetNameExchange, InstrumentNameExchange>,
            >,
        >,
    >,
}
