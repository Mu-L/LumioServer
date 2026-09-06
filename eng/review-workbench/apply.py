from pathlib import Path

ROOT = Path('.')

def replace(path, before, after, count=1):
    p = ROOT / path
    s = p.read_text(encoding='utf-8')
    assert s.count(before) == count, (path, before[:100], s.count(before))
    p.write_text(s.replace(before, after), encoding='utf-8')

def function(path, marker, replacement):
    p = ROOT / path
    s = p.read_text(encoding='utf-8')
    start = s.index(marker)
    brace = s.index('{', start)
    depth = 1
    end = brace + 1
    while depth:
        if s[end] == '{': depth += 1
        elif s[end] == '}': depth -= 1
        end += 1
    p.write_text(s[:start] + replacement + s[end:], encoding='utf-8')

# A second old assertion was still codifying ingress-driven logical time.
function('modules/process/tests/entity_chat_host.rs', 'fn host_wire_ingress_ticks_at_max_chat_inputs()', '''fn host_wire_ingress_waits_for_a_clock_tick_at_the_batch_limit() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    let (observed, rx) = bounded_channel(MAX_CHAT_INPUTS_PER_TICK);
    host.attach_wire_input_observer(observed);
    assert!(host.admit("room-main".to_owned(), "c-bot01".to_owned(), credential(&keys, "Bot01", true)).accepted);
    let mut client = lumio_server_process::entity_chat::RoomClient::connect(&host.listen_uri(), "c-bot01").expect("connect");
    let _ = client.recv_text();
    for _ in 0..MAX_CHAT_INPUTS_PER_TICK {
        client.send_text(RUNTIME_WIRE_CHAT_INPUT).expect("wire input");
    }
    for _ in 0..MAX_CHAT_INPUTS_PER_TICK {
        rx.recv_timeout(std::time::Duration::from_secs(2)).expect("observed input");
    }
    assert!(host.try_self_lookup("c-bot01".to_owned()).is_some());
    assert!(runtime.lock().run_tick_input_counts().is_empty(), "ingress must not advance the clock");
    assert!(host.run_tick("room-main".to_owned()).ok);
    assert_eq!(runtime.lock().run_tick_input_counts(), &[MAX_CHAT_INPUTS_PER_TICK]);
}''')

# Preserve the mature std MPSC channel and add deadline-aware marshalling.
c = 'modules/host-runtime/src/channel.rs'
replace(c, 'use std::time::Duration;', 'use std::time::{Duration, Instant};\nuse std::sync::{Arc, Condvar, Mutex};')
replace(c, '    inner: mpsc::SyncSender<T>,', '    inner: mpsc::SyncSender<T>,\n    wake: Arc<(Mutex<bool>, Condvar)>,')
replace(c, '            inner: self.inner.clone(),', '            inner: self.inner.clone(),\n            wake: self.wake.clone(),')
replace(c, '    inner: mpsc::Receiver<T>,', '    inner: mpsc::Receiver<T>,\n    wake: Arc<(Mutex<bool>, Condvar)>,')
replace(c, '    (Sender { inner: tx }, Receiver { inner: rx })', '''    let wake = Arc::new((Mutex::new(false), Condvar::new()));
    (Sender { inner: tx, wake: wake.clone() }, Receiver { inner: rx, wake })''')
replace(c, 'impl<T> Sender<T> {', '''impl<T> Sender<T> {
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
            if *closed { return Err(SendError::Closed(value)); }
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(SendError::Closed(v)) => return Err(SendError::Closed(v)),
                Err(SendError::Full(v)) => value = v,
            }
            let Some(remaining) = timeout.checked_sub(start.elapsed()) else {
                return Err(SendError::Full(value));
            };
            let (guard, result) = changed.wait_timeout(closed, remaining).expect("channel wake wait");
            closed = guard;
            if result.timed_out() {
                if *closed { return Err(SendError::Closed(value)); }
                return self.try_send(value);
            }
        }
    }
''')
replace(c, '        self.inner.recv().map_err(|_| RecvError::Closed)', '''        let result = self.inner.recv().map_err(|_| RecvError::Closed);
        self.notify_capacity();
        result''')
replace(c, '            Ok(value) => Ok(value),', '            Ok(value) => { self.notify_capacity(); Ok(value) },', 2)
replace(c, 'impl<T> Receiver<T> {', '''impl<T> Receiver<T> {
    fn notify_capacity(&self) {
        let _guard = self.wake.0.lock().expect("channel wake lock");
        self.wake.1.notify_all();
    }
''')
replace(c, '#[cfg(test)]\nmod tests {', '''impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        *self.wake.0.lock().expect("channel wake lock") = true;
        self.wake.1.notify_all();
    }
}

#[cfg(test)]
mod tests {''')
p = Path(c)
p.write_text(p.read_text() + '''
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
''')

s = 'modules/host-runtime/src/supervisor.rs'
p = Path(s)
old_tests = p.read_text()[p.read_text().index('#[cfg(test)]'):]
p.write_text('''//! Cooperative cancellation and bounded, observable task supervision.
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
pub struct CancelToken { state: Arc<CancelState> }
impl CancelToken {
    #[must_use]
    pub fn new() -> Self { Self::default() }
    #[must_use]
    pub fn is_cancelled(&self) -> bool { self.state.cancelled.load(Ordering::Acquire) }
    pub fn cancel(&self) {
        let _guard = self.state.lock.lock().expect("cancel lock");
        self.state.cancelled.store(true, Ordering::Release);
        self.state.changed.notify_all();
    }
    /// Waits for cancellation, returning false only when the wait expires.
    #[must_use]
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let guard = self.state.lock.lock().expect("cancel lock");
        let _result = self.state.changed.wait_timeout_while(guard, timeout, |()| !self.is_cancelled()).expect("cancel wait");
        self.is_cancelled()
    }
}

/// Panic reported independently of join, allowing an owner to react immediately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPanicked { pub name: String, pub detail: String }

#[derive(Default)]
struct Completion { done: bool, failure: Option<TaskPanicked> }

/// A timeout is not a claim that the underlying thread stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskJoinTimedOut { pub name: String }

pub struct SupervisedTask {
    name: String,
    cancel: CancelToken,
    join: Option<JoinHandle<()>>,
    completion: Arc<(Mutex<Completion>, Condvar)>,
}
impl SupervisedTask {
    #[must_use]
    pub fn name(&self) -> &str { &self.name }
    #[must_use]
    pub fn cancel_token(&self) -> CancelToken { self.cancel.clone() }
    pub fn cancel(&self) { self.cancel.cancel(); }
    #[must_use]
    pub fn is_finished(&self) -> bool { self.completion.0.lock().expect("task state").done }
    #[must_use]
    pub fn failure(&self) -> Option<TaskPanicked> {
        self.completion.0.lock().expect("task state").failure.clone()
    }
    /// Explicit unbounded join. Use `join_timeout` for host shutdown.
    pub fn join(&mut self) -> Option<TaskPanicked> {
        self.cancel();
        if let Some(handle) = self.join.take() { let _ = handle.join(); }
        self.failure()
    }
    /// Cancels and joins only within the supplied deadline.
    ///
    /// # Errors
    /// A timeout retains the handle; the caller must escalate or try again.
    pub fn join_timeout(&mut self, timeout: Duration) -> Result<Option<TaskPanicked>, TaskJoinTimedOut> {
        self.cancel();
        let state = self.completion.0.lock().expect("task state");
        let (state, _) = self.completion.1.wait_timeout_while(state, timeout, |state| !state.done).expect("task completion wait");
        if !state.done { return Err(TaskJoinTimedOut { name: self.name.clone() }); }
        drop(state);
        Ok(self.join())
    }
}
impl Drop for SupervisedTask {
    fn drop(&mut self) {
        match self.join_timeout(Duration::from_secs(2)) {
            Ok(Some(failure)) => eprintln!("supervised task failed: {}: {}", failure.name, failure.detail),
            Err(timeout) => {
                eprintln!("supervised task did not stop before deadline: {} (process escalation required)", timeout.name);
                // Rust cannot safely terminate an arbitrary thread. Detach rather
                // than deadlock Drop; owned resources stay alive in that thread.
                self.join.take();
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
where F: FnOnce(CancelToken) + Send + 'static {
    let cancel = CancelToken::new();
    let token = cancel.clone();
    let completion = Arc::new((Mutex::new(Completion::default()), Condvar::new()));
    let completed = completion.clone();
    let task_name = name.to_owned();
    let join = thread::Builder::new().name(task_name.clone()).spawn(move || {
        let failure = panic::catch_unwind(AssertUnwindSafe(|| body(token))).err().map(|payload| TaskPanicked {
            name: task_name, detail: panic_detail(payload.as_ref()),
        });
        let mut state = completed.0.lock().expect("task state");
        state.failure = failure;
        state.done = true;
        completed.1.notify_all();
    }).unwrap_or_else(|error| panic!("failed to spawn supervised thread `{name}`: {error}"));
    SupervisedTask { name: name.to_owned(), cancel, join: Some(join), completion }
}
fn panic_detail(payload: &(dyn std::any::Any + Send)) -> String {
    payload.downcast_ref::<&str>().map(|v| (*v).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_owned())
}

''' + old_tests + '''
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
        assert!(task.join_timeout(Duration::from_secs(2)).expect("join").is_some());
        assert_eq!(task.failure().expect("latched").detail, "expected failure");
    }
    #[test]
    fn join_deadline_preserves_the_handle_for_retry() {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let mut task = spawn_supervised("blocked", move |_| { let _ = rx.recv(); });
        assert!(task.join_timeout(Duration::ZERO).is_err());
        tx.send(()).expect("unblock");
        assert!(task.join_timeout(Duration::from_secs(2)).is_ok());
    }
}
''')

# Owner marshalling no longer waits forever or ignores cooperative shutdown.
h = 'modules/process/src/entity_chat/host.rs'
replace(h, 'move |_cancel| {', 'move |cancel| {')
replace(h, '''                let work = if self_drive {
                    rx.recv_timeout(Duration::from_millis(OWNER_CADENCE_MS))
                } else {
                    rx.recv().map_err(|_| lumio_host_runtime::RecvError::Closed)
                };''', '''                if cancel.is_cancelled() { break; }
                let work = rx.recv_timeout(Duration::from_millis(OWNER_CADENCE_MS));''')
replace(h, '''        let forward = spawn_supervised("lumio-entity-chat-wire-fwd", move |_| {
            while let Ok(event) = wire_rx.recv() {
                if forward_tx.send(OwnerWork::Wire(event)).is_err() {
                    break;
                }
            }
        });''', '''        let forward = spawn_supervised("lumio-entity-chat-wire-fwd", move |cancel| {
            while !cancel.is_cancelled() {
                match wire_rx.recv_timeout(Duration::from_millis(10)) {
                    Ok(event) => {
                        if forward_tx.send_timeout(OwnerWork::Wire(event), Duration::from_secs(2)).is_err() {
                            panic!("owner forwarding deadline exceeded or owner closed");
                        }
                    }
                    Err(lumio_host_runtime::RecvError::Empty) => {}
                    Err(lumio_host_runtime::RecvError::Closed) => break,
                }
            }
        });''')
replace(h, '''            .send(OwnerWork::Run(Box::new(move |inner| {
                let _ = tx.send(work(inner));
            })))''', '''            .send_timeout(OwnerWork::Run(Box::new(move |inner| {
                let _ = tx.try_send(work(inner));
            })), Duration::from_secs(2))''')
replace(h, '        rx.recv().expect("entity-chat owner result")', '        rx.recv_timeout(Duration::from_secs(2)).expect("entity-chat owner result deadline")')
replace(h, 'impl Inner {', '''impl Drop for EntityChatHost {
    fn drop(&mut self) {
        self._owner.cancel();
        self._forward.cancel();
    }
}

impl Inner {''')

# Evidence never fills a missing observation with the expected passing answer.
e = 'modules/process/src/entity_chat/suite.rs'
replace(e, '''    let mut all_ok = blocked.is_none();
    for value in scenarios.values() {
        if value.get("ok") == Some(&Value::Bool(false)) {
            all_ok = false;
        }
    }''', '''    let all_ok = blocked.is_none() && scenarios.len() == 11
        && (1..=11).all(|n| scenarios.get(&n.to_string())
            .and_then(|value| value.get("ok")).and_then(Value::as_bool) == Some(true));''')
replace(e, '.and_then(Value::as_str).unwrap_or("wrong_password")', '.cloned().unwrap_or(Value::Null)')
replace(e, '"historyCount": 0,', '"historyCount": persist.get("clientWindowAfterRestore").cloned().unwrap_or(Value::Null),')
replace(e, '"windowAfter": 0,', '"windowAfter": persist.get("clientWindowAfterRestore").cloned().unwrap_or(Value::Null),')
function(e, 'fn write_evidence(out_dir: &Path, evidence: &Value, audit: &str)', '''fn write_evidence(out_dir: &Path, evidence: &Value, audit: &str) -> std::io::Result<()> {
    if evidence.get("ok").and_then(Value::as_bool) == Some(true) {
        for n in 1..=11 {
            if evidence.pointer(&format!("/scenarios/{n}/ok")).and_then(Value::as_bool) != Some(true) {
                return Err(std::io::Error::other("successful evidence missing a successful scenario"));
            }
        }
        if evidence.pointer("/traces/account/wrongPasswordCode").and_then(Value::as_str).is_none() {
            return Err(std::io::Error::other("successful evidence missing account observation"));
        }
    }
    std::fs::create_dir_all(out_dir)?;
    // Publish the success-bearing evidence last. A failed log write must not
    // leave evidence.json claiming this run completed.
    write_oracle_logs(out_dir, evidence, audit)?;
    std::fs::write(out_dir.join("host-audit.ndjson"), audit)?;
    std::fs::write(out_dir.join("admit-trace.ndjson"), audit)?;
    let bytes = serde_json::to_vec_pretty(evidence)?;
    let staging = out_dir.join("evidence.json.tmp");
    std::fs::write(&staging, bytes)?;
    std::fs::File::open(&staging)?.sync_all()?;
    std::fs::rename(staging, out_dir.join("evidence.json"))
}''')
replace(e, 'fn write_oracle_logs(out_dir: &Path, evidence: &Value, audit: &str) {', 'fn write_oracle_logs(out_dir: &Path, evidence: &Value, audit: &str) -> std::io::Result<()> {')
replace(e, '''    if std::fs::create_dir_all(&server_dir).is_err()
        || std::fs::create_dir_all(&client_dir).is_err()
    {
        return;
    }''', '''    std::fs::create_dir_all(&server_dir)?;
    std::fs::create_dir_all(&client_dir)?;''')
# Limit mechanical I/O propagation to the oracle function, preserving other code.
p = Path(e); text = p.read_text(); a = text.index('fn write_oracle_logs('); b = text.index('\nfn decode_hex_text', a)
chunk = text[a:b].replace('let _ = std::fs::write(', 'std::fs::write(').replace('    );\n', '    )?;\n')
# The deferred push is not an I/O operation.
chunk = chunk.replace('json!({ "kind": "deferred", "scenario": 10, "reason": "ADR-058 §11 multi-room deferred" }),\n    )?;', 'json!({ "kind": "deferred", "scenario": 10, "reason": "ADR-058 §11 multi-room deferred" }),\n    );')
chunk = chunk.rstrip(); assert chunk.endswith('}')
chunk = chunk[:-1] + '    Ok(())\n}\n'
p.write_text(text[:a] + chunk + text[b:])
replace(e, '''    write_evidence(out_dir, &evidence, &host_audit);
    evidence''', '''    match write_evidence(out_dir, &evidence, &host_audit) {
        Ok(()) => evidence,
        Err(error) => json!({ "ok": false, "blocked": "evidence_write_failed", "detail": error.to_string() }),
    }''')
function(e, 'fn write_blocked(out_dir: &Path, reason: &str)', '''fn write_blocked(out_dir: &Path, reason: &str) -> Value {
    let mut evidence = json!({ "ok": false, "blocked": reason });
    if let Err(error) = write_evidence(out_dir, &evidence, "")
        .and_then(|()| std::fs::write(out_dir.join("blocked.txt"), format!("{reason}\\n"))) {
        evidence["evidenceWriteError"] = json!(error.to_string());
    }
    evidence
}''')
replace(e, '''    let _ = std::fs::write(
        options.out_dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap_or_default() + "\\n",
    );''', '''    let manifest_write = serde_json::to_vec_pretty(&manifest)
        .map_err(std::io::Error::other)
        .and_then(|bytes| std::fs::write(options.out_dir.join("manifest.json"), bytes));
    if let Err(error) = manifest_write {
        return SuiteReport { ok: false, blocked: Some(format!("manifest_write_failed: {error}")), rounds };
    }''')
p = Path(e)
p.write_text(p.read_text() + '''
#[cfg(test)]
mod evidence_hardening_tests {
    use super::*;
    #[test]
    fn missing_account_observation_is_null_not_a_passing_default() {
        let dir = tempfile::tempdir().expect("temp");
        write_oracle_logs(dir.path(), &json!({"ok": false}), "").expect("write");
        let text = std::fs::read_to_string(dir.path().join("server/server.ndjson")).expect("read");
        let account: Value = text.lines().map(|s| serde_json::from_str::<Value>(s).expect("json")).find(|v| v["kind"] == "account").expect("account");
        assert!(account["wrongPasswordCode"].is_null());
    }
    #[test]
    fn success_requires_all_scenario_observations() {
        let dir = tempfile::tempdir().expect("temp");
        assert!(write_evidence(dir.path(), &json!({"ok":true}), "").is_err());
        assert!(!dir.path().join("evidence.json").exists());
    }
    #[test]
    fn evidence_write_failure_is_not_ignored() {
        let dir = tempfile::tempdir().expect("temp");
        std::fs::write(dir.path().join("server"), b"not a directory").expect("fixture");
        assert!(write_evidence(dir.path(), &json!({"ok":false}), "").is_err());
        assert!(!dir.path().join("evidence.json").exists());
    }
}
''')

# Make this materialization one-shot; the workflow itself is removed at handoff.
Path(__file__).unlink()
print('Applied owner, supervision, channel deadline, and evidence hardening.')
