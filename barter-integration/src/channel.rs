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
    /// Keep only the newest queued item when the queue is full.
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

/// A small bounded adapter with explicit overflow semantics.
///
/// `Conflate` is intentionally generic: it keeps the latest event, but does not claim
/// instrument-aware conflation. Callers that need per-instrument conflation must key events
/// before using this adapter.
pub struct BoundedTx<T> {
    state: Arc<Mutex<BoundedState<T>>>,
    capacity: usize,
    policy: OverflowPolicy,
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
        },
        BoundedRx { state },
    )
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
            .finish()
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
        if state.queue.len() >= self.capacity {
            match self.policy {
                OverflowPolicy::Block => return Err(BoundedSendError::Full(item)),
                OverflowPolicy::DropOldest => {
                    state.queue.pop_front();
                }
                OverflowPolicy::Conflate => {
                    state.queue.clear();
                }
            }
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

impl LatencySamples {
    pub fn record(&mut self, started: Instant) {
        if self.samples.len() == LATENCY_SAMPLE_CAPACITY {
            self.samples.pop_front();
        }
        self.samples.push_back(started.elapsed());
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
    fn synthetic_burst_never_exceeds_capacity_and_percentiles_are_available() {
        let (tx, mut rx) = mpsc_bounded(4, OverflowPolicy::DropOldest);
        for value in 0..100 {
            tx.try_send(value).unwrap();
        }
        assert_eq!(tx.len(), 4);
        assert_eq!(rx.try_recv(), Some(96));

        let mut samples = LatencySamples::default();
        samples.record(Instant::now());
        assert_eq!(samples.len(), 1);
        assert!(samples.percentile(0.50).is_some());
        assert!(samples.percentile(0.95).is_some());
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
    async fn conflate_keeps_only_latest_item() {
        let (tx, mut rx) = mpsc_bounded(2, OverflowPolicy::Conflate);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        tx.try_send(3).unwrap();
        assert_eq!(StreamExt::next(&mut rx).await, Some(3));
    }
}
