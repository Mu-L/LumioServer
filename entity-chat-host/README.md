# entity-chat-host

CoreCLR managed adapter for Runtime `WorldManager`, `EntityBindingQuery` and `WireCodec`. Entry operations remain `boot`, `enqueue`, `tick`, `drain`, `snapshot` and `restore`; the Rust host owns credential verification, NativeCore timers and transport.

```sh
dotnet restore src/Lumio.Server.EntityChat.HostEntry/Lumio.Server.EntityChat.HostEntry.csproj
dotnet build src/Lumio.Server.EntityChat.HostEntry/Lumio.Server.EntityChat.HostEntry.csproj --no-restore -c Release
```

Boot requires explicit replication/ECS/registry assemblies. The input span decoder and identity accessors are bound once at boot rather than creating DynamicMethod on every input. Sibling assemblies load in deterministic order, and load failures are not silently ignored. BufferTooSmall retries retain the original response rather than repeating destructive operations.

Restore constructs the replacement manager and binding view before activation, does not transfer old socket authorization, and disposes the previous manager when its public interface supports disposal.

**Remaining boundary:** this adapter still has one process-scoped static managed context under the existing entry contract. Explicit multi-context handles, fully typed/generated Runtime bindings and real Native/Runtime lifecycle integration are not claimed complete. The default DS profile is one process, one room; do not infer multi-slot or hot-reload support from this adapter.

Entry type: `Lumio.Server.EntityChat.HostEntry.HostEntry, Lumio.Server.EntityChat.HostEntry`; method: `LumioEntityChatEntry`.
