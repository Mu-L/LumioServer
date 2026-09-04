# entity-chat-host

Slice-scoped CoreCLR managed entry for R-00408. It loads Runtime `WorldManager`, `EntityBindingQuery`, `ChatCommandRuntime`, and `WireCodec`, then exposes a small JSON op protocol. Lifecycle controls are enqueued and committed by the Runtime owner thread; the Rust host owns admission verification, NativeCore timer drain, and Room WebSocket transport.

```text
cd entity-chat-host
dotnet build src/Lumio.Server.EntityChat.HostEntry/Lumio.Server.EntityChat.HostEntry.csproj
```

Boot JSON must include `replicationAssembly` and `ecsAssembly` paths (from `LUMIO_RUNTIME_REPLICATION_DLL` / `LUMIO_RUNTIME_ECS_DLL`). A generated gameplay registry assembly is also required; pass it as `registryAssembly` or set `LUMIO_RUNTIME_GAMEPLAY_DLL`. Missing artifacts are BLOCKED.

Entry: `Lumio.Server.EntityChat.HostEntry.HostEntry, Lumio.Server.EntityChat.HostEntry` / `LumioEntityChatEntry`.
