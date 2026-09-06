//! Bounded MPSC channel. Full and closed are explicit; there is no unbounded path.

use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Why a send failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError<T> {
    /// Capacity is exhausted; the value is returned.
    Full(T),
    /// The receiver was dropped; the value is returned.
    Closed(T),
}

/// Why a receive failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// The channel is empty.
    Empty,
    /// Every sender was dropped.
    Closed,
}

/// Sending end of a bounded channel.
pub struct Sender<T> {
    inner: mpsc::SyncSender<T>,
    wake: Arc<(Mutex<bool>, Condvar)>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            wake: self.wake.clone(),
        }
    }
}

/// Receiving end of a bounded channel.
pub struct Receiver<T> {
    inner: mpsc::Receiver<T>,
    wake: Arc<(Mutex<bool>, Condvar)>,
}

/// Creates a bounded MPSC channel with `capacity` slots.
///
/// # Panics
///
/// Panics when `capacity` is zero.
#[must_use]
pub fn bounded_channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "bounded channel capacity must be positive");
    let (tx, rx) = mpsc::sync_channel(capacity);
    let wake = Arc::new((Mutex::new(false), Condvar::new()));
    (
        Sender {
            inner: tx,
            wake: wake.clone(),
        },
        Receiver { inner: rx, wake },
    )
}

impl<T> Sender<T> {
    /// Sends before a deadline without losing the value when capacity is exhausted.
    /// Timeout is a marshalling deadline, not a simulation clock.
    ///
    /// # Errors
    /// Returns `Full(value)` at the deadline or `Closed(value)` after receiver drop.
    pub fn send_timeout(&self, mut value: T, timeout: Duration) -> Result<(), SendError<T>> {
        let start = Instant::now();
        let (lock, changed) = &*self.wake;
        let mut closed = lock.lock().expect("channel wake lock");
        loop {
            if *closed {
                return Err(SendError::Closed(value));
            }
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(SendError::Closed(v)) => return Err(SendError::Closed(v)),
                Err(SendError::Full(v)) => value = v,
            }
            let Some(remaining) = timeout.checked_sub(start.elapsed()) else {
                return Err(SendError::Full(value));
            };
            let (guard, result) = changed
                .wait_timeout(closed, remaining)
                .expect("channel wake wait");
            closed = guard;
            if result.timed_out() {
                if *closed {
                    return Err(SendError::Closed(value));
                }
                return self.try_send(value);
            }
        }
    }

    /// Non-blocking send.
    ///
    /// # Errors
    ///
    /// [`SendError::Full`] when the bound is reached, [`SendError::Closed`]
    /// when the receiver is gone.
    pub fn try_send(&self, value: T) -> Result<(), SendError<T>> {
        match self.inner.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(value)) => Err(SendError::Full(value)),
            Err(TrySendError::Disconnected(value)) => Err(SendError::Closed(value)),
        }
    }

    /// Blocking send used by owner-thread marshalling.
    ///
    /// # Errors
    ///
    /// [`SendError::Closed`] when the receiver is gone.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.inner
            .send(value)
            .map_err(|error| SendError::Closed(error.0))
    }
}

impl<T> Receiver<T> {
    fn notify_capacity(&self) {
        let _guard = self.wake.0.lock().expect("channel wake lock");
        self.wake.1.notify_all();
    }

    /// Blocking receive.
    ///
    /// # Errors
    ///
    /// [`RecvError::Closed`] when every sender is gone.
    pub fn recv(&self) -> Result<T, RecvError> {
        let result = self.inner.recv().map_err(|_| RecvError::Closed);
        self.notify_capacity();
        result
    }

    /// Non-blocking receive.
    ///
    /// # Errors
    ///
    /// [`RecvError::Empty`] or [`RecvError::Closed`].
    pub fn try_recv(&self) -> Result<T, RecvError> {
        match self.inner.try_recv() {
            Ok(value) => {
                self.notify_capacity();
                Ok(value)
            }
            Err(TryRecvError::Empty) => Err(RecvError::Empty),
            Err(TryRecvError::Disconnected) => Err(RecvError::Closed),
        }
    }

    /// Receive with a timeout. Used only to observe cancel; not a business timer.
    ///
    /// # Errors
    ///
    /// [`RecvError::Empty`] on timeout, [`RecvError::Closed`] when disconnected.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvError> {
        match self.inner.recv_timeout(timeout) {
            Ok(value) => {
                self.notify_capacity();
                Ok(value)
            }
            Err(RecvTimeoutError::Timeout) => Err(RecvError::Empty),
            Err(RecvTimeoutError::Disconnected) => Err(RecvError::Closed),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        *self.wake.0.lock().expect("channel wake lock") = true;
        self.wake.1.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_when_full_instead_of_growing() {
        let (tx, rx) = bounded_channel::<u8>(1);
        tx.try_send(1).expect("first slot");
        match tx.try_send(2) {
            Err(SendError::Full(2)) => {}
            other => panic!("expected full, got {other:?}"),
        }
        assert_eq!(rx.recv().expect("drain"), 1);
        tx.try_send(3).expect("slot freed");
    }

    #[test]
    fn closed_receive_is_explicit() {
        let (tx, rx) = bounded_channel::<u8>(1);
        drop(tx);
        assert_eq!(rx.recv(), Err(RecvError::Closed));
    }

    #[test]
    fn sender_clones_without_clone_payload() {
        struct NoClone;
        let (tx, rx) = bounded_channel::<NoClone>(1);
        let tx2 = tx.clone();
        assert!(tx2.try_send(NoClone).is_ok());
        drop(tx);
        assert!(rx.recv().is_ok());
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;
    #[test]
    fn deadline_returns_the_unsent_value() {
        let (tx, _rx) = bounded_channel(1);
        tx.try_send(1).expect("first");
        assert_eq!(tx.send_timeout(2, Duration::ZERO), Err(SendError::Full(2)));
    }
    #[test]
    fn dropping_receiver_wakes_a_timed_sender() {
        let (tx, rx) = bounded_channel(1);
        tx.try_send(1).expect("first");
        let task = std::thread::spawn(move || tx.send_timeout(2, Duration::from_secs(10)));
        drop(rx);
        assert_eq!(task.join().expect("join"), Err(SendError::Closed(2)));
    }
    #[test]
    fn receive_wakes_a_timed_sender() {
        let (tx, rx) = bounded_channel(1);
        tx.try_send(1).expect("first");
        let task = std::thread::spawn(move || tx.send_timeout(2, Duration::from_secs(2)));
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(task.join().expect("join"), Ok(()));
        assert_eq!(rx.recv(), Ok(2));
    }
}
