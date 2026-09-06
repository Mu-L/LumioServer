//! Cooperative cancellation and bounded, observable task supervision.
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Debug, Default)]
struct CancelState {
    cancelled: AtomicBool,
    lock: Mutex<()>,
    changed: Condvar,
}

/// Shared cancellation with a wakeable wait for blocking adapters.
#[derive(Clone, Debug, Default)]
pub struct CancelToken {
    state: Arc<CancelState>,
}
impl CancelToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }
    pub fn cancel(&self) {
        let _guard = self.state.lock.lock().expect("cancel lock");
        self.state.cancelled.store(true, Ordering::Release);
        self.state.changed.notify_all();
    }
    /// Waits for cancellation, returning false only when the wait expires.
    #[must_use]
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let guard = self.state.lock.lock().expect("cancel lock");
        let _result = self
            .state
            .changed
            .wait_timeout_while(guard, timeout, |()| !self.is_cancelled())
            .expect("cancel wait");
        self.is_cancelled()
    }
}

/// Panic reported independently of join, allowing an owner to react immediately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPanicked {
    pub name: String,
    pub detail: String,
}

#[derive(Default)]
struct Completion {
    done: bool,
    failure: Option<TaskPanicked>,
}

/// A timeout is not a claim that the underlying thread stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskJoinTimedOut {
    pub name: String,
}

pub struct SupervisedTask {
    name: String,
    cancel: CancelToken,
    join: Option<JoinHandle<()>>,
    completion: Arc<(Mutex<Completion>, Condvar)>,
}
impl SupervisedTask {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    #[must_use]
    pub fn cancel_token(&self) -> CancelToken {
        self.cancel.clone()
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.completion.0.lock().expect("task state").done
    }
    #[must_use]
    pub fn failure(&self) -> Option<TaskPanicked> {
        self.completion
            .0
            .lock()
            .expect("task state")
            .failure
            .clone()
    }
    /// Explicit unbounded join. Use `join_timeout` for host shutdown.
    pub fn join(&mut self) -> Option<TaskPanicked> {
        self.cancel();
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
        self.failure()
    }
    /// Cancels and joins only within the supplied deadline.
    ///
    /// # Errors
    /// A timeout retains the handle; the caller must escalate or try again.
    pub fn join_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<TaskPanicked>, TaskJoinTimedOut> {
        self.cancel();
        let state = self.completion.0.lock().expect("task state");
        let (state, _) = self
            .completion
            .1
            .wait_timeout_while(state, timeout, |state| !state.done)
            .expect("task completion wait");
        if !state.done {
            return Err(TaskJoinTimedOut {
                name: self.name.clone(),
            });
        }
        drop(state);
        Ok(self.join())
    }
}
impl Drop for SupervisedTask {
    fn drop(&mut self) {
        match self.join_timeout(Duration::from_secs(2)) {
            Ok(Some(failure)) => eprintln!(
                "supervised task failed: {}: {}",
                failure.name, failure.detail
            ),
            Err(timeout) => {
                eprintln!("supervised task did not stop before deadline: {} (process escalation required)", timeout.name);
                // Rust cannot safely terminate an arbitrary thread. Detach rather
                // than deadlock Drop; owned resources stay alive in that thread.
                drop(self.join.take());
            }
            Ok(None) => {}
        }
    }
}

/// Starts a named thread; all panics are latched before completion is signalled.
///
/// # Panics
/// Panics if the OS refuses thread creation.
pub fn spawn_supervised<F>(name: &str, body: F) -> SupervisedTask
where
    F: FnOnce(CancelToken) + Send + 'static,
{
    let cancel = CancelToken::new();
    let token = cancel.clone();
    let completion = Arc::new((Mutex::new(Completion::default()), Condvar::new()));
    let completed = completion.clone();
    let task_name = name.to_owned();
    let join = thread::Builder::new()
        .name(task_name.clone())
        .spawn(move || {
            let failure = panic::catch_unwind(AssertUnwindSafe(|| body(token)))
                .err()
                .map(|payload| TaskPanicked {
                    name: task_name,
                    detail: panic_detail(payload.as_ref()),
                });
            let mut state = completed.0.lock().expect("task state");
            state.failure = failure;
            state.done = true;
            completed.1.notify_all();
        })
        .unwrap_or_else(|error| panic!("failed to spawn supervised thread `{name}`: {error}"));
    SupervisedTask {
        name: name.to_owned(),
        cancel,
        join: Some(join),
        completion,
    }
}
fn panic_detail(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|v| (*v).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel;
    use std::sync::mpsc::sync_channel;

    #[test]
    fn named_thread_runs_and_joins() {
        let (tx, rx) = sync_channel(1);
        let mut task = spawn_supervised("lumio-host-runtime-test", move |_| {
            tx.send(std::thread::current().name().map(str::to_owned))
                .expect("send");
        });
        let name = rx.recv().expect("name");
        assert_eq!(name.as_deref(), Some("lumio-host-runtime-test"));
        assert!(task.join().is_none());
    }

    #[test]
    fn panic_becomes_task_panicked() {
        let mut task = spawn_supervised("lumio-host-runtime-panic", |_| {
            panic!("owner exploded");
        });
        let event = task.join().expect("panic event");
        assert_eq!(event.name, "lumio-host-runtime-panic");
        assert!(event.detail.contains("owner exploded"));
    }

    #[test]
    fn cancel_is_visible_to_the_body() {
        let (tx, rx) = bounded_channel::<()>(1);
        let mut task = spawn_supervised("lumio-host-runtime-cancel", move |cancel| {
            while !cancel.is_cancelled() {
                if rx.recv().is_err() {
                    break;
                }
            }
        });
        task.cancel();
        drop(tx);
        assert!(task.join().is_none());
    }
}

#[cfg(test)]
mod hardening_tests {
    use super::*;
    #[test]
    fn cancellation_wakes_waiters() {
        let token = CancelToken::new();
        let other = token.clone();
        let worker = std::thread::spawn(move || other.wait_timeout(Duration::from_secs(30)));
        token.cancel();
        assert!(worker.join().expect("join"));
    }
    #[test]
    fn panic_is_queryable_without_consuming_join() {
        let mut task = spawn_supervised("failure", |_| panic!("expected failure"));
        assert!(task
            .join_timeout(Duration::from_secs(2))
            .expect("join")
            .is_some());
        assert_eq!(task.failure().expect("latched").detail, "expected failure");
    }
    #[test]
    fn join_deadline_preserves_the_handle_for_retry() {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let mut task = spawn_supervised("blocked", move |_| {
            let _ = rx.recv();
        });
        assert!(task.join_timeout(Duration::ZERO).is_err());
        tx.send(()).expect("unblock");
        assert!(task.join_timeout(Duration::from_secs(2)).is_ok());
    }
}
