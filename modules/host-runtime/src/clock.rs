//! Process-local monotonic clocks. Wall-clock time is not used for deadlines.
//!
//! The production clock reads real monotonic time and nothing else: it exposes
//! no way to inject an offset or skip ahead (ADR-057 §6 — a production clock
//! that tests can fast-forward is a debug backdoor, banned by
//! `.spec/rules/system.md`). Tests that need to cross a deadline inject
//! [`TestMonotonicClock`] instead.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Millisecond monotonic clock used by reconnect and host timers.
pub trait HostClock: Send + Sync {
    /// Milliseconds since this clock's origin.
    fn now_ms(&self) -> u64;

    /// Deterministic clocks are manually advanced by tests and must not be
    /// driven by the production owner cadence.
    fn is_deterministic(&self) -> bool {
        false
    }

    /// The deterministic test clock behind this handle, when there is one.
    ///
    /// Real clocks return `None`, which is what keeps time-skipping out of
    /// production: there is no path from a [`SharedClock`] to moving a real
    /// clock forward.
    fn as_test_clock(&self) -> Option<&TestMonotonicClock> {
        None
    }
}

/// Shared handle to a [`HostClock`].
#[derive(Clone)]
pub struct SharedClock {
    inner: Arc<dyn HostClock>,
}

impl SharedClock {
    /// Wraps any clock implementation.
    #[must_use]
    pub fn new(clock: Arc<dyn HostClock>) -> Self {
        Self { inner: clock }
    }

    /// Real monotonic system clock. Cannot be advanced by anyone.
    #[must_use]
    pub fn system() -> Self {
        Self::new(Arc::new(SystemMonotonicClock::new()))
    }

    /// Deterministic test clock starting at zero.
    #[must_use]
    pub fn test() -> Self {
        Self::new(Arc::new(TestMonotonicClock::default()))
    }

    #[must_use]
    pub fn is_deterministic(&self) -> bool {
        self.inner.is_deterministic()
    }

    /// Advances the clock **only** when it is a deterministic test clock.
    ///
    /// Returns `false` — and moves nothing — for a real clock. Production code
    /// therefore cannot skip time through this handle; see the module docs.
    /// The result is `#[must_use]` so a silent no-op cannot pass unnoticed.
    #[must_use]
    pub fn advance_test_clock(&self, delta_ms: u64) -> bool {
        match self.inner.as_test_clock() {
            Some(clock) => {
                clock.advance_ms(delta_ms);
                true
            }
            None => false,
        }
    }
}

impl HostClock for SharedClock {
    fn now_ms(&self) -> u64 {
        self.inner.now_ms()
    }

    fn is_deterministic(&self) -> bool {
        self.inner.is_deterministic()
    }

    fn as_test_clock(&self) -> Option<&TestMonotonicClock> {
        self.inner.as_test_clock()
    }
}

/// Stopwatch-based monotonic clock. Reads real elapsed time only.
pub struct SystemMonotonicClock {
    origin: Instant,
}

impl SystemMonotonicClock {
    /// Starts the clock at the current monotonic instant.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl HostClock for SystemMonotonicClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

/// Deterministic clock that only moves when [`TestMonotonicClock::advance_ms`]
/// is called. Test-support only: never installed by the server binary.
#[derive(Default)]
pub struct TestMonotonicClock {
    now_ms: AtomicI64,
}

impl TestMonotonicClock {
    /// Moves this deterministic clock forward.
    pub fn advance_ms(&self, delta_ms: u64) {
        let delta = i64::try_from(delta_ms).unwrap_or(i64::MAX);
        self.now_ms.fetch_add(delta, Ordering::SeqCst);
    }
}

impl HostClock for TestMonotonicClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.now_ms.load(Ordering::SeqCst).max(0)).unwrap_or(0)
    }

    fn is_deterministic(&self) -> bool {
        true
    }

    fn as_test_clock(&self) -> Option<&TestMonotonicClock> {
        Some(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_starts_at_zero_and_advances() {
        let clock = TestMonotonicClock::default();
        assert_eq!(clock.now_ms(), 0);
        clock.advance_ms(300_000);
        assert_eq!(clock.now_ms(), 300_000);
    }

    #[test]
    fn a_real_clock_cannot_be_advanced_through_the_shared_handle() {
        let clock = SharedClock::system();
        let before = clock.now_ms();
        assert!(
            !clock.advance_test_clock(300_000),
            "ADR-057 §6: real clocks must refuse to skip time"
        );
        assert!(
            clock.now_ms() < before + 300_000,
            "the real clock must not have moved"
        );
        assert!(clock.as_test_clock().is_none());
    }

    #[test]
    fn a_deterministic_clock_advances_through_the_shared_handle() {
        let clock = SharedClock::test();
        assert!(clock.advance_test_clock(300_000));
        assert_eq!(clock.now_ms(), 300_000);
        assert!(clock.is_deterministic());
    }
}
