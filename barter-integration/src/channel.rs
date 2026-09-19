use crate::Unrecoverable;
use derive_more::{Constructor, Display};
use futures::{Sink, Stream};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fmt::Debug,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};
use tracing::warn;

pub trait Tx
where
    Self: Debug + Clone + Send,
{
    type Item;
    type Error: Unrecoverable + Debug;
    fn send<Item: Into<Self::Item>>(&self, item: Item) -> Result<(), Self::Error>;
}

/// Convenience type that holds the [`UnboundedTx`] and [`UnboundedRx`].
#[derive(Debug)]
pub struct Channel<T> {
    pub tx: UnboundedTx<T>,
    pub rx: UnboundedRx<T>,
}

impl<T> Channel<T> {
    /// Construct a new unbounded [`Channel`].
    pub fn new() -> Self {
        let (tx, rx) = mpsc_unbounded();
        Self { tx, rx }
    }
}

impl<T> Default for Channel<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct UnboundedTx<T> {
    pub tx: tokio::sync::mpsc::UnboundedSender<T>,
}

impl<T> UnboundedTx<T> {
    pub fn new(tx: tokio::sync::mpsc::UnboundedSender<T>) -> Self {
        Self { tx }
    }
}

impl<T> Tx for UnboundedTx<T>
where
    T: Debug + Clone + Send,
{
    type Item = T;
    type Error = tokio::sync::mpsc::error::SendError<T>;

    fn send<Item: Into<Self::Item>>(&self, item: Item) -> Result<(), Self::Error> {
        self.tx.send(item.into())
    }
}

impl<T> Unrecoverable for tokio::sync::mpsc::error::SendError<T> {
    fn is_unrecoverable(&self) -> bool {
        true
    }
}

impl<T> Sink<T> for UnboundedTx<T> {
    type Error = tokio::sync::mpsc::error::SendError<T>;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // UnboundedTx is always ready
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.tx.send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // UnboundedTx does not buffer, so no flushing is required
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // UnboundedTx requires no closing logic
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Constructor)]
pub struct UnboundedRx<T> {
    pub rx: tokio::sync::mpsc::UnboundedReceiver<T>,
}

impl<T> Iterator for UnboundedRx<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.rx.try_recv() {
                Ok(event) => break Some(event),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => continue,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break None,
            }
        }
    }
}

impl<T> UnboundedRx<T> {
    pub fn into_stream(self) -> tokio_stream::wrappers::UnboundedReceiverStream<T> {
        tokio_stream::wrappers::UnboundedReceiverStream::new(self.rx)
    }
}

impl<T> Stream for UnboundedRx<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct ChannelTxDroppable<ChannelTx> {
    pub state: ChannelState<ChannelTx>,
}

impl<ChannelTx> ChannelTxDroppable<ChannelTx> {
    pub fn new(tx: ChannelTx) -> Self {
        Self {
            state: ChannelState::Active(tx),
        }
    }

    pub fn new_disabled() -> Self {
        Self {
            state: ChannelState::Disabled,
        }
    }

    pub fn disable(&mut self) {
        self.state = ChannelState::Disabled
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize, Display)]
pub enum ChannelState<Tx> {
    Active(Tx),
    Disabled,
}

impl<ChannelTx> ChannelTxDroppable<ChannelTx>
where
    ChannelTx: Tx,
{
    pub fn send(&mut self, item: ChannelTx::Item) {
        let ChannelState::Active(tx) = &self.state else {
            return;
        };

        if tx.send(item).is_err() {
            let name = std::any::type_name::<ChannelTx::Item>();
            warn!(
                name,
                "ChannelTxDroppable receiver dropped - items will no longer be sent"
            );
            self.state = ChannelState::Disabled
        }
    }
}
pub fn mpsc_unbounded<T>() -> (UnboundedTx<T>, UnboundedRx<T>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (UnboundedTx::new(tx), UnboundedRx::new(rx))
}

/// Overflow behavior for a bounded producer-to-consumer channel.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub enum OverflowPolicy {
    /// Reject the send when the queue is full; callers may retry or apply backpressure.
    Block,
    /// Evict the oldest queued item before enqueueing the new item.
    DropOldest,
    /// Keep the newest observation. When a key function is provided via
    /// [`mpsc_bounded_keyed`], this is latest-per-key (instrument + data kind);
    /// otherwise the channel keeps only the newest queued item.
    Conflate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BoundedSendError<T> {
    Full(T),
    Closed(T),
}

impl<T: Debug> Unrecoverable for BoundedSendError<T> {
    fn is_unrecoverable(&self) -> bool {
        matches!(self, Self::Closed(_))
    }
}

#[derive(Debug)]
struct BoundedState<T> {
    queue: VecDeque<T>,
    waker: Option<Waker>,
    send_waker: Option<Waker>,
    closed: bool,
    senders: usize,
}

/// Identifies one latest observation on a conflating market feed.
///
/// Used by [`OverflowPolicy::Conflate`] with [`mpsc_bounded_keyed`] to keep the newest
/// item per instrument and data kind rather than a single global latest item.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ObservationKey {
    pub instrument: u64,
    pub kind: u8,
}

/// A small bounded adapter with explicit overflow semantics.
///
/// Unkeyed `Conflate` keeps the latest event. Keyed `Conflate` (see [`mpsc_bounded_keyed`])
/// keeps the latest event per [`ObservationKey`].
pub struct BoundedTx<T> {
    state: Arc<Mutex<BoundedState<T>>>,
    capacity: usize,
    policy: OverflowPolicy,
    key_of: fn(&T) -> Option<ObservationKey>,
}

#[derive(Debug)]
pub struct BoundedRx<T> {
    state: Arc<Mutex<BoundedState<T>>>,
}

impl<T> BoundedRx<T> {
    pub fn try_recv(&mut self) -> Option<T> {
        let mut state = self.state.lock().expect("bounded channel mutex poisoned");
        let item = state.queue.pop_front();
        if item.is_some()
            && let Some(waker) = state.send_waker.take()
        {
            waker.wake();
        }
        item
    }
}

pub fn mpsc_bounded<T>(capacity: usize, policy: OverflowPolicy) -> (BoundedTx<T>, BoundedRx<T>) {
    assert!(capacity > 0, "bounded channel capacity must be positive");
    let state = Arc::new(Mutex::new(BoundedState {
        queue: VecDeque::with_capacity(capacity),
        waker: None,
        send_waker: None,
        closed: false,
        senders: 1,
    }));
    (
        BoundedTx {
            state: Arc::clone(&state),
            capacity,
            policy,
            key_of: |_| None,
        },
        BoundedRx { state },
    )
}

/// Bounded channel whose [`OverflowPolicy::Conflate`] path keeps the latest item per key.
pub fn mpsc_bounded_keyed<T>(
    capacity: usize,
    policy: OverflowPolicy,
    key_of: fn(&T) -> Option<ObservationKey>,
) -> (BoundedTx<T>, BoundedRx<T>) {
    let (mut tx, rx) = mpsc_bounded(capacity, policy);
    tx.key_of = key_of;
    (tx, rx)
}

impl<T> Clone for BoundedTx<T> {
    fn clone(&self) -> Self {
        if let Ok(mut state) = self.state.lock() {
            state.senders += 1;
        }
        Self {
            state: Arc::clone(&self.state),
            capacity: self.capacity,
            policy: self.policy,
            key_of: self.key_of,
        }
    }
}

impl<T> Drop for BoundedTx<T> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.senders = state.senders.saturating_sub(1);
            if state.senders == 0 {
                state.closed = true;
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
                if let Some(waker) = state.send_waker.take() {
                    waker.wake();
                }
            }
        }
    }
}

impl<T> Debug for BoundedTx<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedTx")
            .field("capacity", &self.capacity)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl<T> Tx for BoundedTx<T>
where
    T: Debug + Clone + Send,
{
    type Item = T;
    type Error = BoundedSendError<T>;

    fn send<Item: Into<Self::Item>>(&self, item: Item) -> Result<(), Self::Error> {
        self.try_send(item.into())
    }
}

impl<T> BoundedTx<T> {
    pub fn try_send(&self, item: T) -> Result<(), BoundedSendError<T>> {
        let mut state = self.state.lock().expect("bounded channel mutex poisoned");
        if state.closed {
            return Err(BoundedSendError::Closed(item));
        }
        match self.policy {
            OverflowPolicy::Block if state.queue.len() >= self.capacity => {
                return Err(BoundedSendError::Full(item));
            }
            OverflowPolicy::DropOldest if state.queue.len() >= self.capacity => {
                state.queue.pop_front();
            }
            OverflowPolicy::Conflate => {
                if let Some(key) = (self.key_of)(&item) {
                    state
                        .queue
                        .retain(|queued| (self.key_of)(queued) != Some(key));
                    if state.queue.len() >= self.capacity {
                        state.queue.pop_front();
                    }
                } else {
                    // Unkeyed conflation is a single latest observation.
                    state.queue.clear();
                }
            }
            _ => {}
        }
        state.queue.push_back(item);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
        Ok(())
    }

    pub fn send_blocking(&self, mut item: T) -> Result<(), BoundedSendError<T>> {
        loop {
            match self.try_send(item) {
                Ok(()) => return Ok(()),
                Err(BoundedSendError::Full(next)) => {
                    item = next;
                    std::thread::yield_now();
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn policy(&self) -> OverflowPolicy {
        self.policy
    }

    pub fn len(&self) -> usize {
        self.state
            .lock()
            .expect("bounded channel mutex poisoned")
            .queue
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Default, Clone)]
pub struct LatencySamples {
    samples: VecDeque<Duration>,
}

const LATENCY_SAMPLE_CAPACITY: usize = 4096;

/// Named hop on the Market Stream to Engine path (W5.4).
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum LatencyHop {
    /// Exchange timestamp -> local receive timestamp.
    ExchangeToReceived,
    /// Local receive -> Engine feed enqueue.
    ReceivedToEngine,
    /// Time spent in Engine process.
    EngineProcess,
}

/// Percentile-exportable latency samples for the three W5.4 hops.
#[derive(Debug, Default, Clone)]
pub struct MarketLatency {
    pub exchange_to_received: LatencySamples,
    pub received_to_engine: LatencySamples,
    pub engine_process: LatencySamples,
}

impl MarketLatency {
    pub fn percentile(&self, hop: LatencyHop, percentile: f64) -> Option<Duration> {
        match hop {
            LatencyHop::ExchangeToReceived => self.exchange_to_received.percentile(percentile),
            LatencyHop::ReceivedToEngine => self.received_to_engine.percentile(percentile),
            LatencyHop::EngineProcess => self.engine_process.percentile(percentile),
        }
    }
}

impl LatencySamples {
    pub fn record(&mut self, started: Instant) {
        self.record_duration(started.elapsed());
    }

    pub fn record_duration(&mut self, duration: Duration) {
        if self.samples.len() == LATENCY_SAMPLE_CAPACITY {
            self.samples.pop_front();
        }
        self.samples.push_back(duration);
    }

    pub fn percentile(&self, percentile: f64) -> Option<Duration> {
        if self.samples.is_empty() {
            return None;
        }
        let mut samples: Vec<_> = self.samples.iter().copied().collect();
        samples.sort_unstable();
        let rank = ((samples.len() - 1) as f64 * percentile.clamp(0.0, 1.0)).round() as usize;
        samples.get(rank).copied()
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

impl<T> Sink<T> for BoundedTx<T>
where
    T: Send,
{
    type Error = BoundedSendError<T>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state = self.state.lock().expect("bounded channel mutex poisoned");
        if self.policy == OverflowPolicy::Block && state.queue.len() >= self.capacity {
            state.send_waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.try_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<T> Iterator for BoundedRx<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.try_recv() {
                return Some(item);
            }
            let closed = self
                .state
                .lock()
                .expect("bounded channel mutex poisoned")
                .closed;
            if closed {
                return None;
            }
            std::thread::yield_now();
        }
    }
}

impl<T> Stream for BoundedRx<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.state.lock().expect("bounded channel mutex poisoned");
        if let Some(item) = state.queue.pop_front() {
            if let Some(waker) = state.send_waker.take() {
                waker.wake();
            }
            return Poll::Ready(Some(item));
        }
        if state.closed {
            return Poll::Ready(None);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Drop for BoundedRx<T> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        }
    }
}

#[cfg(test)]
mod bounded_tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn drop_oldest_keeps_capacity_bounded() {
        let (tx, mut rx) = mpsc_bounded(2, OverflowPolicy::DropOldest);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        tx.try_send(3).unwrap();
        assert_eq!(StreamExt::next(&mut rx).await, Some(2));
        assert_eq!(StreamExt::next(&mut rx).await, Some(3));
    }

    #[test]
    fn synthetic_10x_burst_never_exceeds_capacity_and_percentiles_are_available() {
        // Documented synthetic 10x burst: 100 sends into a capacity-10 DropOldest queue.
        let (tx, mut rx) = mpsc_bounded(10, OverflowPolicy::DropOldest);
        for value in 0..100 {
            tx.try_send(value).unwrap();
        }
        assert_eq!(tx.len(), 10);
        assert_eq!(rx.try_recv(), Some(90));

        let mut hops = MarketLatency::default();
        hops.exchange_to_received
            .record_duration(Duration::from_millis(2));
        hops.received_to_engine.record(Instant::now());
        hops.engine_process.record(Instant::now());
        assert!(
            hops.percentile(LatencyHop::ExchangeToReceived, 0.50)
                .is_some()
        );
        assert!(
            hops.percentile(LatencyHop::ReceivedToEngine, 0.95)
                .is_some()
        );
        assert!(hops.percentile(LatencyHop::EngineProcess, 0.99).is_some());
    }

    #[test]
    fn iterator_receiver_wakes_blocking_sender() {
        let (tx, mut rx) = mpsc_bounded(1, OverflowPolicy::Block);
        tx.try_send(1).unwrap();
        let producer = std::thread::spawn(move || tx.send_blocking(2).unwrap());
        assert_eq!(Iterator::next(&mut rx), Some(1));
        producer.join().unwrap();
        assert_eq!(Iterator::next(&mut rx), Some(2));
    }

    #[test]
    fn account_events_are_not_dropped_by_market_overflow() {
        let (market_tx, mut market_rx) = mpsc_bounded(2, OverflowPolicy::DropOldest);
        let (account_tx, mut account_rx) = mpsc_bounded(2, OverflowPolicy::Block);
        account_tx.try_send("account-1").unwrap();
        for value in 0..100 {
            market_tx.try_send(value).unwrap();
        }
        assert_eq!(account_rx.try_recv(), Some("account-1"));
        assert_eq!(market_rx.try_recv(), Some(98));
    }

    #[tokio::test]
    async fn conflate_replaces_stale_item_before_capacity_is_reached() {
        let (tx, mut rx) = mpsc_bounded(4, OverflowPolicy::Conflate);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        assert_eq!(StreamExt::next(&mut rx).await, Some(2));
    }

    #[tokio::test]
    async fn conflate_keeps_only_latest_item() {
        let (tx, mut rx) = mpsc_bounded(2, OverflowPolicy::Conflate);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        tx.try_send(3).unwrap();
        assert_eq!(StreamExt::next(&mut rx).await, Some(3));
    }

    #[derive(Debug, Clone)]
    struct KeyedItem {
        key: ObservationKey,
        value: u64,
    }

    fn keyed_item_key(item: &KeyedItem) -> Option<ObservationKey> {
        Some(item.key)
    }

    #[tokio::test]
    async fn keyed_conflate_keeps_latest_per_instrument_and_kind() {
        let (tx, mut rx) = mpsc_bounded_keyed(8, OverflowPolicy::Conflate, keyed_item_key);
        let trade = ObservationKey {
            instrument: 1,
            kind: 0,
        };
        let book = ObservationKey {
            instrument: 1,
            kind: 2,
        };
        let other = ObservationKey {
            instrument: 2,
            kind: 0,
        };
        tx.try_send(KeyedItem {
            key: trade,
            value: 1,
        })
        .unwrap();
        tx.try_send(KeyedItem {
            key: book,
            value: 2,
        })
        .unwrap();
        tx.try_send(KeyedItem {
            key: other,
            value: 3,
        })
        .unwrap();
        tx.try_send(KeyedItem {
            key: trade,
            value: 4,
        })
        .unwrap();
        let mut seen = Vec::new();
        while let Some(item) = rx.try_recv() {
            seen.push((item.key, item.value));
        }
        assert_eq!(seen, vec![(book, 2), (other, 3), (trade, 4)]);
    }
}
