use barter_instrument::instrument::name::InstrumentNameExchange;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq)]
pub struct MockMarketEvent {
    pub instrument: InstrumentNameExchange,
    pub time_exchange: DateTime<Utc>,
    pub kind: MockMarketEventKind,
}

#[derive(Debug, Clone, PartialEq)]
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
    fn executable_quantity(&self, available: Decimal, requested: Decimal) -> Decimal;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PriceTimeQueue;

pub trait LatencyModel: Send + Sync + std::fmt::Debug {
    fn delay_ms(&self, configured_ms: u64) -> u64;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FixedLatency;

impl LatencyModel for FixedLatency {
    fn delay_ms(&self, configured_ms: u64) -> u64 {
        configured_ms
    }
}

impl QueueModel for PriceTimeQueue {
    fn executable_quantity(&self, available: Decimal, requested: Decimal) -> Decimal {
        available.min(requested)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
        self.walk_asks_until_with(quantity, max_price, &PriceTimeQueue)
    }

    pub fn walk_asks_until_with<Q: QueueModel>(
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
        self.walk_bids_until_with(quantity, min_price, &PriceTimeQueue)
    }

    pub fn walk_bids_until_with<Q: QueueModel>(
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
}
