use barter_instrument::instrument::name::InstrumentNameExchange;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Debug, Serialize, Deserialize)]
pub struct MockMarketEvent {
    pub instrument: InstrumentNameExchange,
    pub time_exchange: DateTime<Utc>,
    pub kind: MockMarketEventKind,
    #[serde(skip)]
    pub applied: Option<oneshot::Sender<()>>,
}

impl PartialEq for MockMarketEvent {
    fn eq(&self, other: &Self) -> bool {
        self.instrument == other.instrument
            && self.time_exchange == other.time_exchange
            && self.kind == other.kind
    }
}

impl Clone for MockMarketEvent {
    fn clone(&self) -> Self {
        Self {
            instrument: self.instrument.clone(),
            time_exchange: self.time_exchange,
            kind: self.kind.clone(),
            applied: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MockMarketEventKind {
    OrderBook {
        bids: Vec<MockMarketLevel>,
        asks: Vec<MockMarketLevel>,
    },
    Trade {
        price: Decimal,
        quantity: Decimal,
    },
}

pub trait QueueModel: Send + Sync {
    /// Quantity available when a resting order is touched by an order-book update.
    fn executable_quantity(&self, available: Decimal, requested: Decimal) -> Decimal;

    /// Quantity available when a public trade occurs at `trade_price`.
    fn executable_trade_quantity(
        &self,
        available: Decimal,
        requested: Decimal,
        side: barter_instrument::Side,
        order_price: Decimal,
        trade_price: Decimal,
    ) -> Decimal {
        let _ = (available, requested, side, order_price, trade_price);
        Decimal::ZERO
    }
}

/// Fill a resting order as soon as its price is touched by the current book.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoQueue;

/// Conservative queue model: a resting order only fills after public volume trades
/// through its limit price. Book updates alone never fill it.
#[derive(Debug, Default, Clone, Copy)]
pub struct ConservativeQueue;

/// Backwards-compatible name for the historical touch-fill model.
pub type PriceTimeQueue = NoQueue;

pub trait LatencyModel: Send + Sync + std::fmt::Debug {
    fn delay_ms(&self, configured_ms: u64) -> u64;

    fn feed_delay_ms(&self, configured_ms: u64) -> u64 {
        self.delay_ms(configured_ms)
    }

    fn order_delay_ms(&self, configured_ms: u64) -> u64 {
        self.delay_ms(configured_ms)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FixedLatency;

impl LatencyModel for FixedLatency {
    fn delay_ms(&self, configured_ms: u64) -> u64 {
        configured_ms
    }
}

impl QueueModel for NoQueue {
    fn executable_quantity(&self, available: Decimal, requested: Decimal) -> Decimal {
        available.min(requested)
    }
}

impl QueueModel for ConservativeQueue {
    fn executable_quantity(&self, _available: Decimal, _requested: Decimal) -> Decimal {
        Decimal::ZERO
    }

    fn executable_trade_quantity(
        &self,
        available: Decimal,
        requested: Decimal,
        side: barter_instrument::Side,
        order_price: Decimal,
        trade_price: Decimal,
    ) -> Decimal {
        let crossed = match side {
            barter_instrument::Side::Buy => trade_price <= order_price,
            barter_instrument::Side::Sell => trade_price >= order_price,
        };
        crossed
            .then_some(available.min(requested))
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MockMarketLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, Default)]
pub struct MockOrderBook {
    pub bids: Vec<MockMarketLevel>,
    pub asks: Vec<MockMarketLevel>,
}

impl MockOrderBook {
    pub fn update(&mut self, kind: MockMarketEventKind) {
        if let MockMarketEventKind::OrderBook { bids, asks } = kind {
            self.bids = bids;
            self.asks = asks;
            self.bids.sort_by(|a, b| b.price.cmp(&a.price));
            self.asks.sort_by(|a, b| a.price.cmp(&b.price));
        }
    }

    pub fn walk_asks(&self, quantity: Decimal) -> Option<Decimal> {
        self.walk_asks_until(quantity, None)
            .and_then(|(filled, price)| (filled == quantity).then_some(price))
    }

    pub fn walk_asks_until(
        &self,
        quantity: Decimal,
        max_price: Option<Decimal>,
    ) -> Option<(Decimal, Decimal)> {
        self.walk_asks_until_with(quantity, max_price, &NoQueue)
    }

    pub fn walk_asks_until_with<Q: QueueModel + ?Sized>(
        &self,
        mut quantity: Decimal,
        max_price: Option<Decimal>,
        queue: &Q,
    ) -> Option<(Decimal, Decimal)> {
        let mut notional = Decimal::ZERO;
        let mut filled = Decimal::ZERO;
        for level in &self.asks {
            if max_price.is_some_and(|limit| level.price > limit) {
                break;
            }
            let amount = queue.executable_quantity(level.quantity, quantity);
            notional += amount * level.price;
            filled += amount;
            quantity -= amount;
            if quantity.is_zero() {
                break;
            }
        }
        if filled.is_zero() {
            None
        } else {
            Some((filled, notional / filled))
        }
    }

    pub fn walk_bids(&self, quantity: Decimal) -> Option<Decimal> {
        self.walk_bids_until(quantity, None)
            .and_then(|(filled, price)| (filled == quantity).then_some(price))
    }

    pub fn walk_bids_until(
        &self,
        quantity: Decimal,
        min_price: Option<Decimal>,
    ) -> Option<(Decimal, Decimal)> {
        self.walk_bids_until_with(quantity, min_price, &NoQueue)
    }

    pub fn walk_bids_until_with<Q: QueueModel + ?Sized>(
        &self,
        mut quantity: Decimal,
        min_price: Option<Decimal>,
        queue: &Q,
    ) -> Option<(Decimal, Decimal)> {
        let mut notional = Decimal::ZERO;
        let mut filled = Decimal::ZERO;
        for level in &self.bids {
            if min_price.is_some_and(|limit| level.price < limit) {
                break;
            }
            let amount = queue.executable_quantity(level.quantity, quantity);
            notional += amount * level.price;
            filled += amount;
            quantity -= amount;
            if quantity.is_zero() {
                break;
            }
        }
        if filled.is_zero() {
            None
        } else {
            Some((filled, notional / filled))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walks_multiple_ask_levels_deterministically() {
        let book = MockOrderBook {
            asks: vec![
                MockMarketLevel {
                    price: Decimal::from(10),
                    quantity: Decimal::from(2),
                },
                MockMarketLevel {
                    price: Decimal::from(12),
                    quantity: Decimal::from(3),
                },
            ],
            ..Default::default()
        };
        assert_eq!(book.walk_asks(Decimal::from(5)), Some(Decimal::new(112, 1)));
        assert_eq!(book.walk_asks(Decimal::from(6)), None);

        struct HalfQueue;
        impl QueueModel for HalfQueue {
            fn executable_quantity(&self, available: Decimal, requested: Decimal) -> Decimal {
                (available / Decimal::from(2)).min(requested)
            }
        }
        let (filled, _) = book
            .walk_asks_until_with(Decimal::from(2), None, &HalfQueue)
            .unwrap();
        assert_eq!(filled, Decimal::from(2));
    }

    #[test]
    fn latency_model_exposes_independent_feed_and_order_delays() {
        #[derive(Debug)]
        struct SplitLatency;
        impl LatencyModel for SplitLatency {
            fn delay_ms(&self, configured_ms: u64) -> u64 {
                configured_ms
            }

            fn feed_delay_ms(&self, _configured_ms: u64) -> u64 {
                3
            }

            fn order_delay_ms(&self, _configured_ms: u64) -> u64 {
                7
            }
        }
        let model = SplitLatency;
        assert_eq!(model.feed_delay_ms(100), 3);
        assert_eq!(model.order_delay_ms(100), 7);
    }

    #[test]
    fn conservative_queue_waits_for_trade_through() {
        let book = MockOrderBook {
            asks: vec![MockMarketLevel {
                price: Decimal::from(10),
                quantity: Decimal::from(5),
            }],
            ..Default::default()
        };
        assert_eq!(
            book.walk_asks_until_with(Decimal::from(2), None, &ConservativeQueue),
            None
        );
        assert_eq!(
            ConservativeQueue.executable_trade_quantity(
                Decimal::from(3),
                Decimal::from(2),
                barter_instrument::Side::Buy,
                Decimal::from(10),
                Decimal::from(9),
            ),
            Decimal::from(2)
        );
        assert_eq!(
            ConservativeQueue.executable_trade_quantity(
                Decimal::from(3),
                Decimal::from(2),
                barter_instrument::Side::Buy,
                Decimal::from(10),
                Decimal::from(11),
            ),
            Decimal::ZERO
        );
    }
}
