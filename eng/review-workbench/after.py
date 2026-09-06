from pathlib import Path

def change(path, old, new, count=1):
    p=Path(path); s=p.read_text(); assert s.count(old)==count,(path,old[:80],s.count(old));p.write_text(s.replace(old,new))

# Explicit source configuration: no filesystem search for a vaguely suitable DLL.
p=Path('modules/process/src/entity_chat/clr.rs');s=p.read_text();s=s.replace('#[derive(Debug, Clone)]\npub struct ClrGameplayConfig', '#[derive(Debug, Clone, serde::Deserialize)]\n#[serde(deny_unknown_fields)]\npub struct ClrGameplayConfig',1);p.write_text(s)

h='modules/process/src/entity_chat/host.rs'
p=Path(h);s=p.read_text()
s=s.replace('    tick_id: u64,', '    tick_id: u64,\n    last_committed_tick: u64,',1)
s=s.replace('                tick_id: 0,', '                tick_id: 0,\n                last_committed_tick: 0,',1)
s=s.replace('''        admission_verifier: Option<BoundAdmissionVerifier>,
    ) -> Result<Self, String> {''', '''        admission_verifier: Option<BoundAdmissionVerifier>,
        initial_snapshot: Option<(String, u64, PersistRecord)>,
    ) -> Result<Self, String> {''',1)
# Add new optional boot-only restore to both existing constructor calls.
s=s.replace('''            kernel,
            None,
        )''', '''            kernel,
            None,
            None,
        )''',1)
s=s.replace('''            kernel,
            Some(verifier),
        )''', '''            kernel,
            Some(verifier),
            None,
        )''',1)
marker='    fn build('
pos=s.index(marker)
s=s[:pos]+'''    /// Restores before publishing Ready or consuming any socket work.
    pub fn new_authenticated_restored(reconnect_window_ms: u64, runtime: Box<dyn RuntimeSurface>, kernel: Box<dyn KernelTimer>, verifier: BoundAdmissionVerifier, snapshot: Option<(u64, PersistRecord)>) -> Result<Self, String> {
        let initial = snapshot.map(|(tick, bytes)| (verifier.allocation.room_id.clone(), tick, bytes));
        Self::build((reconnect_window_ms, verifier.clock.clone(), verifier.key_id, verifier.public_key.clone(), verifier.unix_origin), runtime, kernel, Some(verifier), initial)
    }

'''+s[pos:]
s=s.replace('''            if let Some(verifier) = &inner.admission_verifier {''', '''            if let Some((room, tick, bytes)) = initial_snapshot {
                if inner.runtime.restore(&room, &bytes.bytes).is_err() {
                    inner.health.faulted.store(true, Ordering::Release);
                    return;
                }
                inner.last_committed_tick = tick;
            }
            if let Some(verifier) = &inner.admission_verifier {''',1)
s=s.replace('(inner.tick_id, bytes)', '(inner.last_committed_tick, bytes)')
s=s.replace('''        let tick = self.runtime.run_tick(room_id, self.tick_id);''', '''        let tick = self.runtime.run_tick(room_id, self.tick_id);
        if tick.ok { self.last_committed_tick = tick.applied_tick; }''',1)
s=s.replace('''            .unwrap_or_else(|_| panic!("entity-chat owner thread closed"));''', '''            .unwrap_or_else(|_| {
                self.health.faulted.store(true, Ordering::Release);
                panic!("entity-chat owner thread closed or queue deadline exceeded")
            });''',1)
s=s.replace('''        rx.recv_timeout(Duration::from_secs(2))
            .expect("entity-chat owner result deadline")''', '''        rx.recv_timeout(Duration::from_secs(2)).unwrap_or_else(|_| {
            self.health.faulted.store(true, Ordering::Release);
            panic!("entity-chat owner result deadline: outcome may be indeterminate")
        })''',1)
p.write_text(s)
p=Path('modules/process/src/entity_chat/host_hardening_tests.rs');s=p.read_text().replace('        tick_id: 0,','        tick_id: 0,\n        last_committed_tick: 0,',1);p.write_text(s)

Path('modules/process/src/ds.rs').write_text(r'''//! Explicit authenticated DS process. The old Hello/replay executables are
//! test-harness targets; this binary never enables unauthenticated attachment.
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use futures_util::FutureExt;
use lumio_host_runtime::{NativeAbiKernel, SharedClock};
use lumio_server_process::entity_chat::{AllocationContext, BoundAdmissionVerifier, ClrGameplay, ClrGameplayConfig, EntityChatHost, PersistRecord};
use lumio_server_process::persistence::{Checkpoint, CheckpointStore, StorageDurability, StoreIdentity};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    allocation: AllocationContext,
    admission_key_id: u8,
    admission_public_key_hex: String,
    clr: ClrGameplayConfig,
    store_path: PathBuf,
    content_fingerprint: String,
    durability: String,
    checkpoint_seconds: u64,
    watchdog_timeout_ms: u64,
}
impl Config {
    fn validate(&self) -> Result<(Vec<u8>, StorageDurability), String> {
        self.allocation.validate()?;
        if !(1..=3600).contains(&self.checkpoint_seconds) || !(100..=60_000).contains(&self.watchdog_timeout_ms) || self.content_fingerprint.is_empty() {
            return Err("invalid checkpoint/watchdog/content configuration".to_owned());
        }
        if self.admission_public_key_hex.len() != 64 || !self.admission_public_key_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("admission_public_key_hex must contain exactly 32 public-key bytes".to_owned());
        }
        let mut key = Vec::with_capacity(32);
        for pair in self.admission_public_key_hex.as_bytes().chunks_exact(2) {
            let text = std::str::from_utf8(pair).map_err(|e| e.to_string())?;
            key.push(u8::from_str_radix(text,16).map_err(|e| e.to_string())?);
        }
        let profile = match self.durability.as_str() {
            "process-crash" => StorageDurability::ProcessCrash,
            "power-loss" => StorageDurability::PowerLoss,
            _ => return Err("durability must be process-crash or power-loss".to_owned()),
        };
        Ok((key, profile))
    }
}
fn read_config(path: &Path) -> Result<Config,String> {
    let metadata=std::fs::metadata(path).map_err(|e| e.to_string())?;
    if metadata.len()>65_536 { return Err("configuration exceeds 64 KiB".into()); }
    let mut config:Config=serde_json::from_slice(&std::fs::read(path).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
    let base=path.parent().unwrap_or_else(||Path::new("."));
    for field in [&mut config.store_path,&mut config.clr.engine_native,&mut config.clr.hostfxr,&mut config.clr.runtime_config,&mut config.clr.assembly,&mut config.clr.replication_assembly,&mut config.clr.ecs_assembly] {
        if field.is_relative() { *field=base.join(&*field); }
    }
    if let Some(registry)=&mut config.clr.registry_assembly { if registry.is_relative() { *registry=base.join(&*registry); } }
    config.validate()?;
    Ok(config)
}

#[tokio::main]
async fn main() {
    let args:Vec<String>=std::env::args().skip(1).collect();
    if args.len()<2 || args[0]!="--config" || args.len()>3 || (args.len()==3 && args[2]!="--check-config") {
        eprintln!("Usage: lumio-ds --config <server.json> [--check-config]"); std::process::exit(3);
    }
    let config=match read_config(Path::new(&args[1])) { Ok(c)=>c,Err(e)=>{eprintln!("configuration rejected: {e}");std::process::exit(3);} };
    if args.len()==3 { println!("configuration_valid"); return; }
    let result=std::panic::AssertUnwindSafe(run(config)).catch_unwind().await;
    let code=match result { Ok(Ok(()))=>0,Ok(Err(error))=>{eprintln!("DS_FATAL {error}");2},Err(_)=>{eprintln!("DS_FATAL owner or process panic");2} };
    // A timed-out native/managed call cannot be safely killed as a Rust thread.
    // Process exit is the final fault boundary, never a fictitious task cancel.
    std::process::exit(code);
}

fn save(host:&EntityChatHost,store:&Mutex<CheckpointStore>,room:&str)->Result<u64,String> {
    let (tick, snapshot)=host.checkpoint(room.to_owned())?;
    let mut store=store.lock().map_err(|_|"checkpoint writer poisoned".to_owned())?;
    // Runtime-only world profile. Voxel worlds require the shared committed-cut
    // provider; refusing to fabricate a second participant is intentional.
    let generation=store.publish(&Checkpoint {tick,wal_sequence:0,runtime:snapshot.bytes,voxel:None}).map_err(|e|e.to_string())?;
    store.retain_latest(3).map_err(|e|e.to_string())?;
    Ok(generation)
}

async fn run(config:Config)->Result<(),String> {
    let (key,profile)=config.validate()?;
    let identity=StoreIdentity {room_id:config.allocation.room_id.clone(),release_id:config.allocation.game_release_id.clone(),contract_id:config.allocation.contract_id.clone(),content_fingerprint:config.content_fingerprint.clone()};
    let store=CheckpointStore::open(&config.store_path,identity,64*1024*1024,profile).map_err(|e|e.to_string())?;
    let recovered=store.recover().map_err(|e|e.to_string())?;
    if recovered.as_ref().is_some_and(|c|c.voxel.is_some() || c.wal_sequence!=0) { return Err("checkpoint needs a Voxel/WAL Runtime recovery provider; refusing partial restoration".into()); }
    let snapshot=recovered.map(|c|(c.tick,PersistRecord {bytes:c.runtime}));
    let runtime=ClrGameplay::start(&config.clr)?;
    let kernel=NativeAbiKernel::load(&config.clr.engine_native)?;
    let clock=SharedClock::system();
    let unix=SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e|e.to_string())?.as_secs();
    let verifier=BoundAdmissionVerifier::new(config.allocation.clone(),config.admission_key_id,key,clock,unix)?;
    let host=Arc::new(EntityChatHost::new_authenticated_restored(300_000,Box::new(runtime),Box::new(kernel),verifier,snapshot)?);
    let store=Arc::new(Mutex::new(store));
    let started=tokio::time::Instant::now();
    let mut heartbeat=tokio::time::interval(Duration::from_millis(100));
    while !host.health().ready {
        if host.health().faulted || started.elapsed()>Duration::from_secs(30) { return Err("runtime startup failed or exceeded deadline".into()); }
        heartbeat.tick().await;
    }
    println!("DS_READY {}",json!({"pid":std::process::id(),"endpoint":host.listen_uri(),"roomId":config.allocation.room_id,"gameReleaseId":config.allocation.game_release_id,"durability":config.durability,"persistenceProfile":"runtime-checkpoint-only"}));
    let shutdown=shutdown_requested(); tokio::pin!(shutdown);
    let mut checkpoint=tokio::time::interval(Duration::from_secs(config.checkpoint_seconds)); checkpoint.tick().await;
    let mut saving:Option<tokio::task::JoinHandle<Result<u64,String>>>=None;
    let mut save_started=tokio::time::Instant::now();
    loop {
        tokio::select! {
            result=&mut shutdown=>{ result?; break; }
            _=heartbeat.tick()=>{
                let health=host.health();
                if health.faulted || health.heartbeat_age_ms>config.watchdog_timeout_ms { return Err("watchdog: owner failed or stopped progressing".into()); }
                if saving.as_ref().is_some_and(tokio::task::JoinHandle::is_finished) {
                    let job=saving.take().ok_or("checkpoint state missing")?;
                    let generation=job.await.map_err(|e|e.to_string())??;
                    println!("DS_CHECKPOINT {}",json!({"generation":generation}));
                }
                if saving.is_some() && save_started.elapsed()>Duration::from_secs(30) { return Err("checkpoint I/O deadline exceeded".into()); }
            }
            _=checkpoint.tick(),if saving.is_none()=>{
                let h=host.clone();let s=store.clone();let room=config.allocation.room_id.clone();
                save_started=tokio::time::Instant::now();
                saving=Some(tokio::task::spawn_blocking(move||save(&h,&s,&room)));
            }
        }
    }
    println!("DS_DRAINING");
    let h=host.clone();
    tokio::time::timeout(Duration::from_secs(3),tokio::task::spawn_blocking(move||h.quiesce())).await.map_err(|_|"quiesce timeout")?.map_err(|e|e.to_string())?;
    if let Some(job)=saving { tokio::time::timeout(Duration::from_secs(30),job).await.map_err(|_|"checkpoint timeout")?.map_err(|e|e.to_string())??; }
    let h=host.clone();let s=store.clone();let room=config.allocation.room_id;
    let generation=tokio::time::timeout(Duration::from_secs(30),tokio::task::spawn_blocking(move||save(&h,&s,&room))).await.map_err(|_|"final checkpoint timeout")?.map_err(|e|e.to_string())??;
    drop(host); println!("DS_STOPPED {}",json!({"checkpointGeneration":generation})); Ok(())
}
async fn shutdown_requested()->Result<(),String> {
    #[cfg(unix)]
    {
        let mut term=tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).map_err(|e|e.to_string())?;
        tokio::select! { result=tokio::signal::ctrl_c()=>result.map_err(|e|e.to_string()),_=term.recv()=>Ok(()) }
    }
    #[cfg(not(unix))]
    { tokio::signal::ctrl_c().await.map_err(|e|e.to_string()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configuration_requires_explicit_bound_identity_and_durability() {
        let config:Result<Config,_>=serde_json::from_value(json!({"allocation":{},"durability":"pretend-durable"}));
        assert!(config.is_err());
    }
    #[test]
    fn oversize_configuration_is_rejected_before_loading_native_code() {
        let d=tempfile::tempdir().unwrap();let p=d.path().join("server.json");std::fs::write(&p,vec![b' ';65_537]).unwrap();assert!(read_config(&p).is_err());
    }
}
''')
p=Path('modules/process/Cargo.toml');s=p.read_text()+'\n[[bin]]\nname = "lumio-ds"\npath = "src/ds.rs"\n';p.write_text(s)

# Cache span decoding once at managed boot, rather than generating IL per input.
c='entity-chat-host/src/Lumio.Server.EntityChat.HostEntry/HostEntry.cs'
p=Path(c);s=p.read_text()
s=s.replace('    private static object? Manager;', '''    private static object? Manager;
    private static InputDecodeBridge? CachedInputDecoder;
    private static Type? CachedNetEntityIdType;
    private static MethodInfo? CachedParseNetEntityId;
    private static PropertyInfo? CachedInstanceId;
    private static PropertyInfo? CachedCounter;''',1)
s=s.replace('''        ulong instanceId = root.TryGetProperty''','''        CachedNetEntityIdType = Ecs.GetType("Lumio.GameRuntime.Ecs.NetEntityId", throwOnError: true)!;
        CachedParseNetEntityId = CachedNetEntityIdType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new[] { typeof(string) }, null)
            ?? throw new MissingMethodException(CachedNetEntityIdType.FullName, "Parse(string)");
        CachedInstanceId = CachedNetEntityIdType.GetProperty("InstanceId") ?? throw new MissingMemberException("NetEntityId.InstanceId");
        CachedCounter = CachedNetEntityIdType.GetProperty("Counter") ?? throw new MissingMemberException("NetEntityId.Counter");
        CachedInputDecoder = BuildInputDecoder(CachedNetEntityIdType);
        ulong instanceId = root.TryGetProperty''',1)
s=s.replace('''        Type netEntityIdType = Ecs!.GetType("Lumio.GameRuntime.Ecs.NetEntityId")!;
        object sender = netEntityIdType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static)!.Invoke(null, new object?[] { senderText })!;''','''        Type netEntityIdType = CachedNetEntityIdType ?? throw new InvalidOperationException("host not booted");
        object sender = CachedParseNetEntityId!.Invoke(null, new object?[] { senderText })!;''',1)
a=s.index('    private static object DecodeInput(');b=s.index('    private delegate object InputDecodeBridge',a)
old=s[a:b];body_start=old.index('        MethodInfo decode');body_end=old.index('        ulong instance =')
builder=old[body_start:body_end].replace('        InputDecodeBridge bridge = (InputDecodeBridge)method.CreateDelegate(bridgeType);','        return (InputDecodeBridge)method.CreateDelegate(bridgeType);')
s=s[:a]+'''    private static object DecodeInput(byte[] envelope, object sender, Type netEntityIdType, Type messageType)
    {
        _ = netEntityIdType;
        _ = messageType;
        InputDecodeBridge bridge = CachedInputDecoder ?? throw new InvalidOperationException("decoder not initialized");
        ulong instance = Convert.ToUInt64(CachedInstanceId!.GetValue(sender), System.Globalization.CultureInfo.InvariantCulture);
        ulong counter = Convert.ToUInt64(CachedCounter!.GetValue(sender), System.Globalization.CultureInfo.InvariantCulture);
        return bridge(envelope, instance, counter);
    }

    private static InputDecodeBridge BuildInputDecoder(Type netEntityIdType)
    {
'''+builder+'    }\n\n'+s[b:]
# Deterministic assembly loading and visible failure, not silent partial boot.
s=s.replace('''        foreach (string path in Directory.GetFiles(directory, "Lumio.GameRuntime.*.dll")) try { Assembly.LoadFrom(path); } catch (Exception) { }''','''        string[] paths = Directory.GetFiles(directory, "Lumio.GameRuntime.*.dll");
        Array.Sort(paths, StringComparer.Ordinal);
        foreach (string path in paths) Assembly.LoadFrom(path);''',1)
# Crash restore must not transfer live connection grants from the previous world.
a=s.index('        var previousConnections = SnapshotConnections();');b=s.index('        return (EntrySuccess, Ok());',a)
s=s[:a]+'''        object? previousManager = Manager;
        object restored = ManagerType!.GetMethod("CreateFromSnapshot", BindingFlags.Public | BindingFlags.Static)!.Invoke(null, new object?[] { new ReadOnlyMemory<byte>(Convert.FromBase64String(encoded!)) })!;
        ManagerType.GetMethod("Start")!.Invoke(restored, new object?[] { System.Threading.Thread.CurrentThread });
        object? restoredBindings = BindingType!.GetMethod("Create", new[] { ManagerType })!.Invoke(null, new[] { restored });
        if (restoredBindings is null) return (EntrySuccess, Fail("restore_failed"));
        Manager = restored;
        Bindings = restoredBindings;
        if (!string.IsNullOrEmpty(roomId))
            BindingType.GetMethod("RestoreRoomBindings", BindingFlags.Public | BindingFlags.Instance)!.Invoke(Bindings, new object?[] { roomId });
        if (previousManager is IDisposable disposable) disposable.Dispose();
'''+s[b:]
# Delete dead connection-copy helper, not retain a second restore path.
a=s.index('    private static List<object> SnapshotConnections()');b=s.index('    private static object NewMessage',a);s=s[:a]+s[b:]
p.write_text(s)
Path(__file__).unlink()
