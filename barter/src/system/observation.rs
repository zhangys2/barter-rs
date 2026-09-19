use barter_data::{
    event::DataKind,
    streams::consumer::MarketStreamEvent,
    subscription::{
        book::{OrderBookEvent, OrderBookL1},
        candle::Candle,
        liquidation::Liquidation,
        trade::PublicTrade,
    },
};
use barter_integration::channel::ObservationKey;
use std::{
    hash::{Hash, Hasher},
    time::Duration,
};

/// Latest-observation identity for the Market Stream → Engine path.
///
/// Used by [`barter_integration::channel::OverflowPolicy::Conflate`] to keep the newest event
/// per Instrument per data kind.
pub trait MarketObservationKey {
    fn market_observation_key(&self) -> Option<ObservationKey>;
}

/// Exchange-to-received hop used by W5.4 latency percentiles.
pub trait MarketLatencyTimes {
    fn exchange_to_received(&self) -> Option<Duration>;
}

/// Discriminator for conflating distinct market-data kinds independently.
pub trait ObservationKind {
    fn kind_tag(&self) -> u8;
}

impl<InstrumentKey, Kind> MarketObservationKey for MarketStreamEvent<InstrumentKey, Kind>
where
    InstrumentKey: Hash,
    Kind: ObservationKind,
{
    fn market_observation_key(&self) -> Option<ObservationKey> {
        match self {
            barter_data::streams::reconnect::Event::Item(event) => {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                event.instrument.hash(&mut hasher);
                Some(ObservationKey {
                    instrument: hasher.finish(),
                    kind: event.kind.kind_tag(),
                })
            }
            barter_data::streams::reconnect::Event::Reconnecting(_) => None,
        }
    }
}

impl<InstrumentKey, Kind> MarketLatencyTimes for MarketStreamEvent<InstrumentKey, Kind> {
    fn exchange_to_received(&self) -> Option<Duration> {
        match self {
            barter_data::streams::reconnect::Event::Item(event) => event
                .time_received
                .signed_duration_since(event.time_exchange)
                .to_std()
                .ok(),
            barter_data::streams::reconnect::Event::Reconnecting(_) => None,
        }
    }
}

impl ObservationKind for DataKind {
    fn kind_tag(&self) -> u8 {
        match self {
            DataKind::Trade(_) => 0,
            DataKind::OrderBookL1(_) => 1,
            DataKind::OrderBook(_) => 2,
            DataKind::Candle(_) => 3,
            DataKind::Liquidation(_) => 4,
        }
    }
}

impl ObservationKind for PublicTrade {
    fn kind_tag(&self) -> u8 {
        0
    }
}

impl ObservationKind for OrderBookL1 {
    fn kind_tag(&self) -> u8 {
        1
    }
}

impl ObservationKind for OrderBookEvent {
    fn kind_tag(&self) -> u8 {
        2
    }
}

impl ObservationKind for Candle {
    fn kind_tag(&self) -> u8 {
        3
    }
}

impl ObservationKind for Liquidation {
    fn kind_tag(&self) -> u8 {
        4
    }
}
