from pathlib import Path

p=Path('modules/process/src/ds.rs');s=p.read_text().replace('.as_bytes().chunks_exact(2)', '.as_bytes().as_chunks::<2>().0')
a=s.index('async fn run(config: Config) -> Result<(), String> {');b=s.index('    let started = tokio::time::Instant::now();',a)
old=s[a:b];body=old[old.index('{')+1:]
# Config is immutable input. All native boot inputs are explicit and retained.
replacement='''struct RunningHost {
    host: Arc<EntityChatHost>,
    store: Arc<Mutex<CheckpointStore>>,
}

fn boot(config: &Config) -> Result<RunningHost, String> {
'''+body+'''    Ok(RunningHost { host, store })
}

async fn run(config: Config) -> Result<(), String> {
    let RunningHost { host, store } = boot(&config)?;
'''
s=s[:a]+replacement+s[b:];p.write_text(s)
p=Path('modules/process/src/entity_chat/host_hardening_tests.rs');s=p.read_text()
s=s.replace('    ticks: Vec<(String, u64)>,','    ticks: Vec<(String, u64)>,\n    next_outcome: Option<ChatOperation>,',1)
s=s.replace('''        ChatOperation::admitted()
    }''','''        self.0.lock().expect("trace lock").next_outcome.take().unwrap_or_else(ChatOperation::admitted)
    }''',1)
s+='''
#[test]
fn quiesced_world_rejects_input_and_does_not_advance_cadence() {
    let (mut inner, trace) = owner();
    admit(&mut inner, "room", "a");
    inner.health.draining.store(true, Ordering::Release);
    input(&mut inner, "a", "must not apply");
    inner.drive_owner_cadence();
    assert!(inner.pending_wire_inputs.is_empty());
    assert!(trace.lock().expect("trace").ticks.is_empty());
    assert!(!inner.admit_verified("room", "b", &AdmissionPayload { key_id:1, account_id:"b".into(), login_name:"bbb".into(), bot_tool_context:false, issued_at:1, expires_at:9000 }).accepted);
}

#[test]
fn rejected_input_is_counted_and_fatal_input_seals_world() {
    let (mut inner, trace) = owner();
    admit(&mut inner, "room", "a");
    trace.lock().expect("trace").next_outcome=Some(ChatOperation::rejected("bad_envelope"));
    input(&mut inner, "a", "rejected");
    assert!(inner.run_tick("room").ok);
    assert_eq!(inner.health.rejected_inputs.load(Ordering::Relaxed),1);
    trace.lock().expect("trace").next_outcome=Some(ChatOperation { kind:ChatOpKind::Fatal, error_code:Some("runtime_failure".into()) });
    input(&mut inner, "a", "fatal");
    assert!(!inner.run_tick("room").ok);
    assert!(inner.health.faulted.load(Ordering::Acquire));
    assert!(!inner.run_tick("room").ok);
    assert_eq!(trace.lock().expect("trace").ticks.len(),1);
}
''';p.write_text(s)
# Configure the new manager fully before making it the active owner.
p=Path('entity-chat-host/src/Lumio.Server.EntityChat.HostEntry/HostEntry.cs');s=p.read_text()
s=s.replace('''        Manager = restored;
        Bindings = restoredBindings;
        if (!string.IsNullOrEmpty(roomId))
            BindingType.GetMethod("RestoreRoomBindings", BindingFlags.Public | BindingFlags.Instance)!.Invoke(Bindings, new object?[] { roomId });''','''        if (!string.IsNullOrEmpty(roomId))
            BindingType.GetMethod("RestoreRoomBindings", BindingFlags.Public | BindingFlags.Instance)!.Invoke(restoredBindings, new object?[] { roomId });
        Manager = restored;
        Bindings = restoredBindings;''',1)
p.write_text(s)
p=Path('modules/process/src/entity_chat/suite.rs');s=p.read_text()
s=s.replace('''    if evidence.get("ok").and_then(Value::as_bool) == Some(true) {''','''    let published = out_dir.join("evidence.json");
    match std::fs::remove_file(&published) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    if evidence.get("ok").and_then(Value::as_bool) == Some(true) {''',1)
p.write_text(s)
p=Path('eng/verify.py');s=p.read_text().replace('''        digest = hashlib.file_digest(path.open("rb"), "sha256").hexdigest()''','''        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()''');p.write_text(s)
Path(__file__).unlink()
