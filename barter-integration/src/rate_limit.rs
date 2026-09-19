//! Request-weight window used by live execution clients.
//!
//! Binance (and similar venues) budget HTTP calls by weight per rolling interval, not by a
//! fixed sleep. `WeightWindow` is the integration primitive: reserve weight, sleep when the
//! budget is exhausted, and optionally observe the venue's reported usage.

use std::time::{Duration, Instant};

/// Rolling request-weight budget.
#[derive(Debug, Clone)]
pub struct WeightWindow {
    /// Maximum weight that may be consumed inside [`Self::window`].
    pub limit: u32,
    /// Length of the rolling budget window.
    pub window: Duration,
    used: u32,
    window_start: Instant,
}

impl WeightWindow {
    /// Construct a window that starts empty at `now`.
    pub fn new(limit: u32, window: Duration) -> Self {
        Self {
            limit: limit.max(1),
            window,
            used: 0,
            window_start: Instant::now(),
        }
    }

    /// Weight consumed in the current window.
    pub fn used(&self) -> u32 {
        self.used
    }

    /// Reset the window if `now` is past the current interval.
    pub fn roll(&mut self, now: Instant) {
        if now.duration_since(self.window_start) >= self.window {
            self.window_start = now;
            self.used = 0;
        }
    }

    /// Duration to wait before `weight` can be consumed without exceeding [`Self::limit`].
    ///
    /// Returns [`Duration::ZERO`] when the weight is reserved immediately.
    pub fn reserve(&mut self, weight: u32, now: Instant) -> Duration {
        let weight = weight.max(1);
        self.roll(now);
        if self.used.saturating_add(weight) <= self.limit {
            self.used += weight;
            Duration::ZERO
        } else {
            self.window
                .saturating_sub(now.duration_since(self.window_start))
        }
    }

    /// After waiting out a full window, start a new interval and consume `weight`.
    pub fn start_new_window(&mut self, weight: u32, now: Instant) {
        self.window_start = now;
        self.used = weight.max(1);
    }

    /// Observe venue-reported usage (for example `X-MBX-USED-WEIGHT-1M`).
    pub fn observe_used(&mut self, used: u32, now: Instant) {
        self.roll(now);
        self.used = self.used.max(used);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_reserve_waits_when_weight_would_exceed_limit() {
        let mut window = WeightWindow::new(20, Duration::from_millis(50));
        let start = Instant::now();
        assert_eq!(window.reserve(20, start), Duration::ZERO);
        let wait = window.reserve(20, start);
        assert!(wait > Duration::ZERO);
        assert!(wait <= Duration::from_millis(50));
    }

    #[test]
    fn observe_used_raises_consumed_weight() {
        let mut window = WeightWindow::new(100, Duration::from_secs(60));
        let now = Instant::now();
        assert_eq!(window.reserve(1, now), Duration::ZERO);
        window.observe_used(80, now);
        assert_eq!(window.used(), 80);
    }
}
