from pathlib import Path

def change(path,old,new,count=1):
    p=Path(path);s=p.read_text();assert s.count(old)==count,(path,old[:70],s.count(old));p.write_text(s.replace(old,new))

p=Path('modules/process/src/ds.rs');s=p.read_text()
a=s.index('    if let Some(registry) = &mut config.clr.registry_assembly {');b=s.index('    config.validate()?;',a)
s=s[:a]+'''    if config.clr.registry_assembly.is_relative() {
        config.clr.registry_assembly = base.join(&config.clr.registry_assembly);
    }
'''+s[b:];p.write_text(s)
p=Path('modules/process/src/entity_chat/host.rs');s=p.read_text()
for name in ['owner','forward','listener']: s=s.replace(f'{name}: {name},',f'{name},')
s=s.replace('ready: self.health.ready.load(Ordering::Acquire) && self.is_healthy(),','ready: self.health.ready.load(Ordering::Acquire) && !self.health.draining.load(Ordering::Acquire) && self.is_healthy(),')
s=s.replace('''        let Ok(fired) = self.kernel.advance_tick_frame(self.tick_id) else {
            return;
        };''','''        let Ok(fired) = self.kernel.advance_tick_frame(self.tick_id) else {
            self.health.faulted.store(true, Ordering::Release);
            return;
        };''')
p.write_text(s)
change('modules/process/src/entity_chat/host_hardening_tests.rs','    let inner = Inner {','    let inner = Inner {\n        health: Arc::new(HealthState::default()),')
change('entity-chat-host/src/Lumio.Server.EntityChat.HostEntry/HostEntry.cs','Type messageType = Ecs.GetType("Lumio.GameRuntime.Ecs.InputCommandMessage")!;','Type messageType = Ecs!.GetType("Lumio.GameRuntime.Ecs.InputCommandMessage")!;')
p=Path('modules/process/src/entity_chat/wire.rs');s=p.read_text()
s=s.replace('#[cfg(any(test, feature = "test-harness"))]\nuse tokio_tungstenite::tungstenite::http::HeaderValue;','use tokio_tungstenite::tungstenite::http::HeaderValue;')
s=s.replace('|request: &Request, response: Response| {','|request: &Request, mut response: Response| {',1)
s=s.replace('''            Ok(response)
        },''','''            if request.headers().get("Sec-WebSocket-Protocol").and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.split(',').any(|item| item.trim() == "lumio.mvp.v0")) {
                response.headers_mut().insert("Sec-WebSocket-Protocol", HeaderValue::from_static("lumio.mvp.v0"));
            }
            Ok(response)
        },''',1)
p.write_text(s)
p=Path('.gitignore');s=p.read_text();p.write_text(s+'\n/artifacts/\n__pycache__/\n')
Path(__file__).unlink()
