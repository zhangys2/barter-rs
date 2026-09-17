use crate::{
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    error::{ApiError, UnindexedOrderError},
    order::{
        Order,
        id::{ClientOrderId, OrderId},
        state::{ActiveOrderState, Cancelled, InactiveOrderState, Open, OrderState},
    },
    trade::Trade,
};
use barter_instrument::{
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use fnv::FnvHashMap;

#[derive(Debug, Constructor)]
pub struct AccountState {
    balances: FnvHashMap<AssetNameExchange, AssetBalance<AssetNameExchange>>,
    orders_open: FnvHashMap<ClientOrderId, Order<ExchangeId, InstrumentNameExchange, Open>>,
    orders_cancelled:
        FnvHashMap<ClientOrderId, Order<ExchangeId, InstrumentNameExchange, Cancelled>>,
    trades: Vec<Trade<QuoteAsset, InstrumentNameExchange>>,
}

impl AccountState {
    pub fn update_time_exchange(&mut self, time_exchange: DateTime<Utc>) {
        for balance in self.balances.values_mut() {
            balance.time_exchange = time_exchange;
        }

        for order in self.orders_open.values_mut() {
            order.state.time_exchange = time_exchange;
        }
    }

    pub fn balances(&self) -> impl Iterator<Item = &AssetBalance<AssetNameExchange>> + '_ {
        self.balances.values()
    }

    pub fn orders_open(
        &self,
    ) -> impl Iterator<Item = &Order<ExchangeId, InstrumentNameExchange, Open>> + '_ {
        self.orders_open.values()
    }

    pub fn orders_cancelled(
        &self,
    ) -> impl Iterator<Item = &Order<ExchangeId, InstrumentNameExchange, Cancelled>> + '_ {
        self.orders_cancelled.values()
    }

    pub fn trades(
        &self,
        time_since: DateTime<Utc>,
    ) -> impl Iterator<Item = &Trade<QuoteAsset, InstrumentNameExchange>> + '_ {
        self.trades
            .iter()
            .filter(move |trade| trade.time_exchange >= time_since)
    }

    pub fn balance(&self, asset: &AssetNameExchange) -> Option<&AssetBalance<AssetNameExchange>> {
        self.balances.get(asset)
    }

    pub fn balance_mut(
        &mut self,
        asset: &AssetNameExchange,
    ) -> Option<&mut AssetBalance<AssetNameExchange>> {
        self.balances.get_mut(asset)
    }

    pub fn apply_fill_balances(
        &mut self,
        debit_asset: &AssetNameExchange,
        debit_amount: rust_decimal::Decimal,
        credit_asset: &AssetNameExchange,
        credit_amount: rust_decimal::Decimal,
        time_exchange: DateTime<Utc>,
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedOrderError> {
        let Some(debit) = self.balances.get(debit_asset) else {
            return Err(ApiError::AssetInvalid(
                debit_asset.clone(),
                "MockExchange has no configured balance for this asset".into(),
            )
            .into());
        };
        let Some(_) = self.balances.get(credit_asset) else {
            return Err(ApiError::AssetInvalid(
                credit_asset.clone(),
                "MockExchange has no configured balance for this asset".into(),
            )
            .into());
        };
        if debit_amount < rust_decimal::Decimal::ZERO || credit_amount < rust_decimal::Decimal::ZERO
        {
            return Err(
                ApiError::OrderRejected("balance changes must be non-negative".into()).into(),
            );
        }
        if debit.balance.free < debit_amount {
            return Err(ApiError::BalanceInsufficient(
                debit_asset.clone(),
                format!(
                    "Available Balance: {}, Required Balance: {}",
                    debit.balance.free, debit_amount
                ),
            )
            .into());
        }
        let debit_balance = self.apply_balance_delta(debit_asset, -debit_amount, time_exchange)?;
        let credit_balance =
            self.apply_balance_delta(credit_asset, credit_amount, time_exchange)?;
        Ok(vec![debit_balance, credit_balance])
    }

    pub fn apply_balance_delta(
        &mut self,
        asset: &AssetNameExchange,
        delta: rust_decimal::Decimal,
        time_exchange: DateTime<Utc>,
    ) -> Result<AssetBalance<AssetNameExchange>, UnindexedOrderError> {
        let Some(current) = self.balances.get_mut(asset) else {
            return Err(ApiError::AssetInvalid(
                asset.clone(),
                "MockExchange has no configured balance for this asset".into(),
            )
            .into());
        };
        let new_balance = current.balance.free + delta;
        if new_balance < rust_decimal::Decimal::ZERO {
            return Err(ApiError::BalanceInsufficient(
                asset.clone(),
                format!(
                    "Available Balance: {}, Required change: {}",
                    current.balance.free, -delta
                ),
            )
            .into());
        }
        current.balance.free = new_balance;
        current.balance.total += delta;
        current.time_exchange = time_exchange;
        Ok(current.clone())
    }

    pub fn cancel_order(
        &mut self,
        cid: &ClientOrderId,
        id: Option<&OrderId>,
        time_exchange: DateTime<Utc>,
    ) -> Result<Cancelled, UnindexedOrderError> {
        let order = self.orders_open.get(cid).ok_or_else(|| {
            ApiError::OrderRejected(format!("order {cid} is unknown or inactive"))
        })?;

        if id.is_some_and(|id| id != &order.state.id) {
            return Err(ApiError::OrderRejected(format!("order {cid} id does not match")).into());
        }

        let Some(order) = self.orders_open.remove(cid) else {
            return Err(
                ApiError::OrderRejected(format!("order {cid} is unknown or inactive")).into(),
            );
        };
        let cancelled = Cancelled {
            id: order.state.id,
            time_exchange,
        };
        self.orders_cancelled.insert(
            cid.clone(),
            Order {
                key: order.key,
                side: order.side,
                price: order.price,
                quantity: order.quantity,
                kind: order.kind,
                time_in_force: order.time_in_force,
                state: cancelled.clone(),
            },
        );
        Ok(cancelled)
    }

    pub fn insert_open_order(&mut self, order: Order<ExchangeId, InstrumentNameExchange, Open>) {
        self.orders_open.insert(order.key.cid.clone(), order);
    }

    pub fn open_order_mut(
        &mut self,
        cid: &ClientOrderId,
    ) -> Option<&mut Order<ExchangeId, InstrumentNameExchange, Open>> {
        self.orders_open.get_mut(cid)
    }

    pub fn ack_trade(&mut self, trade: Trade<QuoteAsset, InstrumentNameExchange>) {
        self.trades.push(trade);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        balance::Balance,
        order::{OrderKey, OrderKind, TimeInForce, id::StrategyId},
    };
    use barter_instrument::Side;
    use chrono::TimeZone;
    use proptest::prelude::*;

    fn account_with_balances() -> AccountState {
        let mut balances = FnvHashMap::default();
        for asset in [
            AssetNameExchange::from("BTC"),
            AssetNameExchange::from("USDT"),
        ] {
            balances.insert(
                asset.clone(),
                AssetBalance {
                    asset: asset.clone(),
                    balance: Balance {
                        total: if asset == AssetNameExchange::from("USDT") {
                            rust_decimal::Decimal::new(10_000, 0)
                        } else {
                            rust_decimal::Decimal::new(100, 0)
                        },
                        free: if asset == AssetNameExchange::from("USDT") {
                            rust_decimal::Decimal::new(10_000, 0)
                        } else {
                            rust_decimal::Decimal::new(100, 0)
                        },
                    },
                    time_exchange: Utc.timestamp_opt(0, 0).unwrap(),
                },
            );
        }
        AccountState::new(
            balances,
            FnvHashMap::default(),
            FnvHashMap::default(),
            vec![],
        )
    }

    proptest! {
        #[test]
        fn fill_conserves_value_at_fill_price(
            quantity in 1i64..10,
            price in 1i64..100,
            buy in any::<bool>(),
        ) {
            let mut account = account_with_balances();
            let base = AssetNameExchange::from("BTC");
            let quote = AssetNameExchange::from("USDT");
            let quantity = rust_decimal::Decimal::from(quantity);
            let price = rust_decimal::Decimal::from(price);
            let before = account.balance(&quote).unwrap().balance.free
                + account.balance(&base).unwrap().balance.free * price;
            if buy {
                account.apply_fill_balances(&quote, quantity * price, &base, quantity, Utc.timestamp_opt(1, 0).unwrap()).unwrap();
            } else {
                account.apply_fill_balances(&base, quantity, &quote, quantity * price, Utc.timestamp_opt(1, 0).unwrap()).unwrap();
            }
            let after = account.balance(&quote).unwrap().balance.free
                + account.balance(&base).unwrap().balance.free * price;
            prop_assert_eq!(before, after);
        }

        #[test]
        fn fill_sequence_conserves_value_at_each_changing_fill_price(
            fills in proptest::collection::vec((1i64..10, 1i64..100, any::<bool>()), 1..8),
        ) {
            let mut account = account_with_balances();
            let base = AssetNameExchange::from("BTC");
            let quote = AssetNameExchange::from("USDT");

            for (quantity, price, buy) in fills {
                let quantity = rust_decimal::Decimal::from(quantity);
                let price = rust_decimal::Decimal::from(price);
                let before = account.balance(&quote).unwrap().balance.free
                    + account.balance(&base).unwrap().balance.free * price;
                if buy {
                    account.apply_fill_balances(
                        &quote,
                        quantity * price,
                        &base,
                        quantity,
                        Utc.timestamp_opt(1, 0).unwrap(),
                    ).unwrap();
                } else {
                    account.apply_fill_balances(
                        &base,
                        quantity,
                        &quote,
                        quantity * price,
                        Utc.timestamp_opt(1, 0).unwrap(),
                    ).unwrap();
                }
                let after = account.balance(&quote).unwrap().balance.free
                    + account.balance(&base).unwrap().balance.free * price;
                prop_assert_eq!(before, after);
            }
        }
    }

    #[test]
    fn apply_balance_delta_updates_total_and_free() {
        let mut account = account_with_balances();
        let btc = AssetNameExchange::from("BTC");
        let updated = account
            .apply_balance_delta(
                &btc,
                rust_decimal::Decimal::new(-2, 0),
                Utc.timestamp_opt(1, 0).unwrap(),
            )
            .unwrap();
        assert_eq!(updated.balance.total, rust_decimal::Decimal::new(98, 0));
        assert_eq!(updated.balance.free, rust_decimal::Decimal::new(98, 0));
    }

    #[test]
    fn insufficient_balance_reports_the_debited_asset_without_mutating_state() {
        let mut account = account_with_balances();
        let base = AssetNameExchange::from("BTC");
        let quote = AssetNameExchange::from("USDT");
        let before = account.balances().cloned().collect::<Vec<_>>();
        let error = account
            .apply_fill_balances(
                &base,
                rust_decimal::Decimal::new(101, 0),
                &quote,
                rust_decimal::Decimal::ONE,
                Utc.timestamp_opt(1, 0).unwrap(),
            )
            .unwrap_err();
        assert!(
            matches!(error, UnindexedOrderError::Rejected(ApiError::BalanceInsufficient(asset, _)) if asset == base)
        );
        assert_eq!(account.balances().cloned().collect::<Vec<_>>(), before);
    }

    #[test]
    fn cancel_unknown_and_mismatched_orders_are_typed_errors() {
        let mut account = account_with_balances();
        let cid = ClientOrderId::new("missing");
        let error = account
            .cancel_order(&cid, None, Utc.timestamp_opt(1, 0).unwrap())
            .unwrap_err();
        assert!(matches!(
            error,
            UnindexedOrderError::Rejected(ApiError::OrderRejected(_))
        ));
    }

    #[test]
    fn cancel_order_moves_active_order_to_cancelled() {
        let mut account = account_with_balances();
        let cid = ClientOrderId::new("client-1");
        let order_id = OrderId::new("exchange-1");
        let key = OrderKey {
            exchange: ExchangeId::Mock,
            instrument: InstrumentNameExchange::from("BTCUSDT"),
            strategy: StrategyId::new("strategy-1"),
            cid: cid.clone(),
        };
        account.orders_open.insert(
            cid.clone(),
            Order {
                key,
                side: Side::Buy,
                price: rust_decimal::Decimal::new(10, 0),
                quantity: rust_decimal::Decimal::new(1, 0),
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                state: Open {
                    id: order_id.clone(),
                    time_exchange: Utc.timestamp_opt(0, 0).unwrap(),
                    filled_quantity: rust_decimal::Decimal::ZERO,
                },
            },
        );
        let wrong_id = OrderId::new("wrong-id");
        let mismatch = account
            .cancel_order(&cid, Some(&wrong_id), Utc.timestamp_opt(1, 0).unwrap())
            .unwrap_err();
        assert!(matches!(
            mismatch,
            UnindexedOrderError::Rejected(ApiError::OrderRejected(_))
        ));
        let cancelled = account
            .cancel_order(&cid, Some(&order_id), Utc.timestamp_opt(1, 0).unwrap())
            .unwrap();
        assert_eq!(cancelled.id, order_id);
        assert!(account.orders_open.get(&cid).is_none());
        assert!(account.orders_cancelled.get(&cid).is_some());

        let inactive = account
            .cancel_order(&cid, Some(&order_id), Utc.timestamp_opt(2, 0).unwrap())
            .unwrap_err();
        assert!(matches!(
            inactive,
            UnindexedOrderError::Rejected(ApiError::OrderRejected(_))
        ));
    }
}

impl From<UnindexedAccountSnapshot> for AccountState {
    fn from(value: UnindexedAccountSnapshot) -> Self {
        let UnindexedAccountSnapshot {
            exchange: _,
            balances,
            instruments,
        } = value;

        let balances = balances
            .into_iter()
            .map(|asset_balance| (asset_balance.asset.clone(), asset_balance))
            .collect();

        let (orders_open, orders_cancelled) = instruments.into_iter().fold(
            (FnvHashMap::default(), FnvHashMap::default()),
            |(mut orders_open, mut orders_cancelled), snapshot| {
                for order in snapshot.orders {
                    match order.state {
                        OrderState::Active(ActiveOrderState::Open(open)) => {
                            orders_open.insert(
                                order.key.cid.clone(),
                                Order {
                                    key: order.key,
                                    side: order.side,
                                    price: order.price,
                                    quantity: order.quantity,
                                    kind: order.kind,
                                    time_in_force: order.time_in_force,
                                    state: open,
                                },
                            );
                        }
                        OrderState::Inactive(InactiveOrderState::Cancelled(cancelled)) => {
                            orders_cancelled.insert(
                                order.key.cid.clone(),
                                Order {
                                    key: order.key,
                                    side: order.side,
                                    price: order.price,
                                    quantity: order.quantity,
                                    kind: order.kind,
                                    time_in_force: order.time_in_force,
                                    state: cancelled,
                                },
                            );
                        }
                        _ => {}
                    }
                }

                (orders_open, orders_cancelled)
            },
        );

        Self {
            balances,
            orders_open,
            orders_cancelled,
            trades: vec![],
        }
    }
}
