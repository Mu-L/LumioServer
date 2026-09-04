using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace Lumio.Server.EntityChat.HostEntry;

/// <summary>Native boundary for the Runtime-owned world manager and wire codec.</summary>
public static class HostEntry
{
    private const int EntrySuccess = 0;
    private const int EntryInvalidInput = 1;
    private const int EntryBufferTooSmall = 2;
    private const int EntryRuntimeFailure = 3;

    private static readonly object Gate = new();
    private static Assembly? Replication;
    private static Assembly? Ecs;
    private static Type? BindingType;
    private static Type? ChatType;
    private static Type? ChatEnvelopeType;
    private static Type? ManagerType;
    private static Type? WireCodecType;
    private static Type? EcsRegistryType;
    private static object? Bindings;
    private static object? Chat;
    private static object? Manager;

    [UnmanagedCallersOnly(EntryPoint = "lumio_entity_chat_entry")]
    public static unsafe int LumioEntityChatEntry(byte* input, int inputLength, byte* output, int outputCapacity, int* bytesWritten)
    {
        if (bytesWritten is null) return EntryInvalidInput;
        bytesWritten[0] = 0;
        if (inputLength < 0 || outputCapacity < 0 || (inputLength > 0 && input is null) || (outputCapacity > 0 && output is null)) return EntryInvalidInput;
        int code;
        byte[] response;
        try { (code, response) = Execute(input, inputLength); }
        catch (Exception) { code = EntryRuntimeFailure; response = Fail("runtime_failure"); }
        if (response.Length > outputCapacity)
        {
            bytesWritten[0] = response.Length;
            return EntryBufferTooSmall;
        }
        response.AsSpan().CopyTo(new Span<byte>(output, response.Length));
        bytesWritten[0] = response.Length;
        return code;
    }

    private static unsafe (int, byte[]) Execute(byte* input, int inputLength)
    {
        try
        {
            using JsonDocument document = JsonDocument.Parse(new ReadOnlySpan<byte>(input, inputLength).ToArray());
            return Dispatch(document.RootElement);
        }
        catch (JsonException) { return (EntryInvalidInput, Fail("bad_envelope")); }
    }

    private static (int, byte[]) Dispatch(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("op", out JsonElement op) || op.ValueKind != JsonValueKind.String) return (EntryInvalidInput, Fail("bad_envelope"));
        lock (Gate)
        {
            return op.GetString() switch
            {
                "boot" => Boot(root),
                "admit" => Admit(root),
                "disconnect" => Disconnect(root),
                "rebind" => Rebind(root),
                "expire" => Expire(root),
                "self_lookup" => SelfLookup(root),
                "resolve" => Resolve(root),
                "query" => Query(root),
                "live_ids" => LiveIds(),
                "attach_member" => AttachMember(root),
                "admit_input" => AdmitInput(root),
                "tick" => Tick(root),
                "drain" => DrainOutbox(),
                "snapshot" => CaptureSnapshot(),
                "restore" => Restore(root),
                "shutdown" => Shutdown(),
                _ => (EntryInvalidInput, Fail("bad_envelope")),
            };
        }
    }

    private static (int, byte[]) Boot(JsonElement root)
    {
        if (!TryString(root, "replicationAssembly", out string? replicationPath) || !TryString(root, "ecsAssembly", out string? ecsPath) || string.IsNullOrEmpty(replicationPath) || string.IsNullOrEmpty(ecsPath) || !File.Exists(replicationPath) || !File.Exists(ecsPath)) return (EntrySuccess, Fail("boot_failed"));
        LoadSiblingAssemblies(Path.GetDirectoryName(replicationPath));
        LoadSiblingAssemblies(Path.GetDirectoryName(ecsPath));
        string? registryPath = Read(root, "registryAssembly") ?? Environment.GetEnvironmentVariable("LUMIO_RUNTIME_GAMEPLAY_DLL");
        if (!string.IsNullOrWhiteSpace(registryPath) && File.Exists(registryPath))
        {
            try { Assembly.LoadFrom(registryPath); } catch (Exception) { return (EntrySuccess, Fail("registry_load_failed")); }
        }
        Replication = Assembly.LoadFrom(replicationPath);
        Ecs = Assembly.LoadFrom(ecsPath);
        BindingType = Replication.GetType("Lumio.GameRuntime.Replication.Binding.EntityBindingQuery");
        ChatType = Replication.GetType("Lumio.GameRuntime.Replication.Chat.ChatCommandRuntime");
        ChatEnvelopeType = Replication.GetType("Lumio.GameRuntime.Replication.Chat.ChatEnvelope");
        ManagerType = Ecs.GetType("Lumio.GameRuntime.Ecs.WorldManager");
        WireCodecType = Ecs.GetType("Lumio.GameRuntime.Ecs.WireCodec");
        EcsRegistryType = Ecs.GetType("Lumio.GameRuntime.Ecs.EcsRegistry");
        if (BindingType is null || ChatType is null || ChatEnvelopeType is null || ManagerType is null || WireCodecType is null || EcsRegistryType is null) return (EntrySuccess, Fail("boot_failed"));
        object? registry = EcsRegistryType.GetProperty("Current", BindingFlags.Public | BindingFlags.Static)?.GetValue(null) ?? FindGeneratedRegistry();
        if (registry is null) return (EntrySuccess, Fail("registry_required"));
        ulong instanceId = root.TryGetProperty("instanceId", out JsonElement id) && id.TryGetUInt64(out ulong supplied) ? supplied : 1UL;
        Manager = ManagerType.GetMethod("Create", BindingFlags.Public | BindingFlags.Static)!.Invoke(null, new object?[] { registry, instanceId });
        ManagerType.GetMethod("Start", BindingFlags.Public | BindingFlags.Instance)!.Invoke(Manager, new object?[] { System.Threading.Thread.CurrentThread });
        Bindings = BindingType.GetMethod("Create", new[] { ManagerType })!.Invoke(null, new[] { Manager });
        Chat = ChatType.GetMethod("Create", new[] { BindingType, typeof(bool) })!.Invoke(null, new object?[] { Bindings, false });
        return Manager is null || Bindings is null || Chat is null ? (EntrySuccess, Fail("boot_failed")) : (EntrySuccess, Ok());
    }

    private static object? FindGeneratedRegistry()
    {
        foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
            foreach (Type type in GetTypes(assembly))
                if (EcsRegistryType!.IsAssignableFrom(type) && !type.IsAbstract)
                    if (type.GetProperty("Instance", BindingFlags.Public | BindingFlags.Static)?.GetValue(null) is object instance) return instance;
        return null;
    }

    private static IEnumerable<Type> GetTypes(Assembly assembly)
    {
        try { return assembly.GetTypes(); }
        catch (ReflectionTypeLoadException error)
        {
            var types = new List<Type>();
            foreach (Type? type in error.Types) if (type is not null) types.Add(type);
            return types;
        }
    }

    private static void LoadSiblingAssemblies(string? directory)
    {
        if (string.IsNullOrEmpty(directory) || !Directory.Exists(directory)) return;
        foreach (string path in Directory.GetFiles(directory, "Lumio.GameRuntime.*.dll")) try { Assembly.LoadFrom(path); } catch (Exception) { }
    }

    private static (int, byte[]) Admit(JsonElement root)
    {
        if (!TryString(root, "connection", out string? connection) || !TryString(root, "accountId", out string? account) || !TryString(root, "roomId", out string? room) || !TryString(root, "entityType", out string? entityType)) return (EntrySuccess, Fail("invalid_request"));
        Enqueue(NewMessage("AdmitConnectionMessage", connection, account, room, entityType));
        List<object>? messages = TickManager();
        object result = BindingType!.GetMethod("ResolveByConnection")!.Invoke(Bindings, new object[] { room!, connection! })!;
        return FromBindingResult(result, messages);
    }

    private static (int, byte[]) Disconnect(JsonElement root)
    {
        if (!TryString(root, "connection", out string? connection)) return (EntrySuccess, Fail("invalid_request"));
        object lookup = BindingType!.GetMethod("SelfLookup")!.Invoke(Bindings, new object?[] { connection, "client-replica" })!;
        object? binding = lookup.GetType().GetProperty("Binding")?.GetValue(lookup);
        if (binding is null) return (EntrySuccess, Fail("binding_not_found"));
        Enqueue(NewMessage("DisconnectConnectionMessage", connection));
        List<object>? messages = TickManager();
        return (EntrySuccess, Json(new Dictionary<string, object?>
        {
            ["ok"] = true,
            ["outcome"] = "accepted",
            ["binding"] = BindingDict(binding),
            ["frames"] = EncodeFrames(messages),
        }));
    }

    private static (int, byte[]) Rebind(JsonElement root)
    {
        if (!TryString(root, "connection", out string? connection) || !TryString(root, "accountId", out string? account) || !TryString(root, "roomId", out string? room) || !TryString(root, "mode", out string? mode)) return (EntrySuccess, Fail("invalid_request"));
        Enqueue(NewMessage("RebindConnectionMessage", connection, account, room, mode));
        List<object>? messages = TickManager();
        object result = BindingType!.GetMethod("ResolveByConnection")!.Invoke(Bindings, new object[] { room!, connection! })!;
        return FromBindingResult(result, messages);
    }

    private static (int, byte[]) Expire(JsonElement root)
    {
        if (!TryString(root, "netEntityId", out string? id)) return (EntrySuccess, Fail("invalid_request"));
        object result = BindingType!.GetMethod("Expire", new[] { typeof(string) })!.Invoke(Bindings, new object[] { id! })!;
        return FromBindingResult(result);
    }

    private static (int, byte[]) SelfLookup(JsonElement root)
    {
        if (!TryString(root, "connection", out string? connection)) return (EntrySuccess, Fail("invalid_request"));
        object result = BindingType!.GetMethod("SelfLookup")!.Invoke(Bindings, new object?[] { connection, "client-replica" })!;
        return FromBindingResult(result);
    }

    private static (int, byte[]) Resolve(JsonElement root)
    {
        if (!TryString(root, "roomId", out string? room) || !TryString(root, "netEntityId", out string? id)) return (EntrySuccess, Fail("invalid_request"));
        object result = BindingType!.GetMethod("ResolveByNetEntityId")!.Invoke(Bindings, new object?[] { room, id, null, "server-authoritative" })!;
        return FromBindingResult(result);
    }

    private static (int, byte[]) Query(JsonElement root)
    {
        if (Bindings is null || Replication is null) return (EntrySuccess, Fail("invalid_request"));
        Type requestType = Replication.GetType("Lumio.GameRuntime.Replication.Binding.AttributeQueryRequest")!;
        object request = Activator.CreateInstance(requestType)!;
        requestType.GetProperty("CallerScope")!.SetValue(request, Read(root, "callerScope"));
        requestType.GetProperty("RoomId")!.SetValue(request, Read(root, "roomId"));
        requestType.GetProperty("NetEntityId")!.SetValue(request, Read(root, "netEntityId"));
        requestType.GetProperty("AttributeId")!.SetValue(request, Read(root, "attributeId"));
        if (root.TryGetProperty("connectionGeneration", out JsonElement generation) && generation.TryGetUInt64(out ulong value)) requestType.GetProperty("ConnectionGeneration")!.SetValue(request, value);
        object result = BindingType!.GetMethod("QueryAttribute")!.Invoke(Bindings, new object?[] { request, null })!;
        return FromBindingResult(result);
    }

    private static (int, byte[]) AttachMember(JsonElement root)
    {
        if (!TryString(root, "roomId", out string? room) || !TryString(root, "connection", out string? connection)) return (EntrySuccess, Fail("invalid_request"));
        object result = ChatType!.GetMethod("AttachMember")!.Invoke(Chat, new object[] { room!, connection! })!;
        bool ok = Convert.ToBoolean(result.GetType().GetProperty("Succeeded")!.GetValue(result), System.Globalization.CultureInfo.InvariantCulture);
        return (EntrySuccess, ok ? Ok() : Fail("runtime_failure"));
    }

    private static (int, byte[]) LiveIds()
    {
        if (Chat is null) return (EntrySuccess, Fail("runtime_failure"));
        object? ids = ChatType!.GetProperty("LiveNetEntityIds")!.GetValue(Chat);
        var rows = new List<string>();
        if (ids is IEnumerable values) foreach (object value in values) rows.Add(value.ToString() ?? string.Empty);
        return (EntrySuccess, Json(new Dictionary<string, object?> { ["ok"] = true, ["ids"] = rows }));
    }

    private static (int, byte[]) AdmitInput(JsonElement root)
    {
        if (!TryString(root, "roomId", out string? room) || !TryString(root, "connection", out string? connection) || !TryString(root, "envelopeBase64", out string? encoded) || !root.TryGetProperty("connectionGeneration", out JsonElement generation) || !generation.TryGetUInt64(out ulong value)) return (EntrySuccess, Fail("invalid_request"));
        byte[] envelope;
        try { envelope = Convert.FromBase64String(encoded!); }
        catch (FormatException) { return (EntrySuccess, Fail("bad_envelope")); }
        if (WireCodecType!.GetMethod("DecodeInput", BindingFlags.Public | BindingFlags.Static) is null) return (EntrySuccess, Fail("boot_failed"));
        object[] parse = { Encoding.UTF8.GetString(envelope), null!, null! };
        bool parsed = Convert.ToBoolean(ChatEnvelopeType!.GetMethod("TryParseInputCommand")!.Invoke(null, parse), System.Globalization.CultureInfo.InvariantCulture);
        if (!parsed)
        {
            object? failure = parse[2];
            string parseCode = failure?.GetType().GetProperty("Code")?.GetValue(failure) as string ?? "bad_envelope";
            return (EntrySuccess, Fail(parseCode));
        }
        string text = parse[1] as string ?? string.Empty;
        Type inputType = Replication!.GetType("Lumio.GameRuntime.Replication.Chat.ChatInput")!;
        object input = inputType.GetConstructor(new[] { typeof(string) })!.Invoke(new object?[] { text });
        object result = ChatType!.GetMethod("AdmitInput")!.Invoke(Chat, new[] { room, connection, value, input })!;
        bool ok = Convert.ToBoolean(result.GetType().GetProperty("Succeeded")!.GetValue(result), System.Globalization.CultureInfo.InvariantCulture);
        string? code = result.GetType().GetProperty("Code")!.GetValue(result) as string;
        return (EntrySuccess, ok ? Ok() : Fail(code ?? "invalid_request"));
    }

    private static (int, byte[]) Tick(JsonElement root)
    {
        _ = root;
        IReadOnlyList<object>? messages = TickManager();
        ulong applied = WorldValue("Tick");
        ulong revision = WorldValue("Revision");
        int events = 0;
        if (messages is not null)
            foreach (object message in messages)
                if (message.GetType().Name == "WorldChangeMessage")
                {
                    events = (message.GetType().GetProperty("Rpcs")!.GetValue(message) as ICollection)?.Count ?? 0;
                    break;
                }
        return (EntrySuccess, Json(new Dictionary<string, object?> { ["ok"] = true, ["appliedTick"] = applied, ["revision"] = revision, ["eventCount"] = events, ["frames"] = EncodeFrames(messages) }));
    }

    private static (int, byte[]) DrainOutbox()
    {
        return (EntrySuccess, Json(new Dictionary<string, object?> { ["ok"] = true, ["frames"] = EncodeFrames(DrainManager()) }));
    }

    private static (int, byte[]) CaptureSnapshot()
    {
        byte[] bytes = (byte[])ManagerType!.GetMethod("CaptureSnapshot")!.Invoke(Manager, null)!;
        return (EntrySuccess, Json(new Dictionary<string, object?> { ["ok"] = true, ["bytesBase64"] = Convert.ToBase64String(bytes) }));
    }

    private static (int, byte[]) Restore(JsonElement root)
    {
        if (!TryString(root, "bytesBase64", out string? encoded)) return (EntrySuccess, Fail("invalid_request"));
        object restored = ManagerType!.GetMethod("CreateFromSnapshot", BindingFlags.Public | BindingFlags.Static)!.Invoke(null, new object?[] { new ReadOnlyMemory<byte>(Convert.FromBase64String(encoded!)) })!;
        Manager = restored;
        ManagerType.GetMethod("Start")!.Invoke(Manager, new object?[] { System.Threading.Thread.CurrentThread });
        Bindings = BindingType!.GetMethod("Create", new[] { ManagerType })!.Invoke(null, new[] { Manager });
        Chat = ChatType!.GetMethod("Create", new[] { BindingType, typeof(bool) })!.Invoke(null, new object?[] { Bindings, false });
        return (EntrySuccess, Ok());
    }

    private static (int, byte[]) Shutdown()
    {
        (Chat as IDisposable)?.Dispose();
        (Manager as IDisposable)?.Dispose();
        Chat = null; Bindings = null; Manager = null;
        return (EntrySuccess, Ok());
    }

    private static object NewMessage(string typeName, params string?[] args) => Activator.CreateInstance(Ecs!.GetType("Lumio.GameRuntime.Ecs." + typeName)!, args)!;
    private static void Enqueue(object message) => ManagerType!.GetMethod("Enqueue")!.Invoke(Manager, new[] { message });
    private static List<object>? TickManager() { ManagerType!.GetMethod("Tick")!.Invoke(Manager, null); return DrainManager(); }
    private static List<object>? DrainManager() => ManagerType!.GetMethod("DrainOutbox")!.Invoke(Manager, null) is IEnumerable rows ? ToObjectList(rows) : null;
    private static List<object> ToObjectList(IEnumerable rows) { var result = new List<object>(); foreach (object row in rows) result.Add(row); return result; }
    private static ulong WorldValue(string property) { object world = ManagerType!.GetProperty("World")!.GetValue(Manager)!; return Convert.ToUInt64(world.GetType().GetProperty(property)!.GetValue(world), System.Globalization.CultureInfo.InvariantCulture); }

    private static (int, byte[]) FromBindingResult(object result, IEnumerable<object>? messages = null)
    {
        Type type = result.GetType();
        string outcome = type.GetProperty("Outcome")!.GetValue(result) as string ?? "request_error";
        var payload = new Dictionary<string, object?> { ["ok"] = outcome is "ok" or "accepted", ["outcome"] = outcome, ["code"] = type.GetProperty("Code")!.GetValue(result) as string };
        if (messages is not null) payload["frames"] = EncodeFrames(messages);
        if (type.GetProperty("Binding")!.GetValue(result) is object binding) payload["binding"] = BindingDict(binding);
        if (type.GetProperty("Value")!.GetValue(result) is object value) payload["value"] = Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
        foreach (string name in new[] { "NetEntityId", "RoomId", "EntityType", "AttributeId", "ObservedRevision", "ObservedTick" })
        {
            object? field = type.GetProperty(name)?.GetValue(result);
            if (field is not null) payload[char.ToLowerInvariant(name[0]) + name[1..]] = field;
        }
        return (EntrySuccess, Json(payload));
    }

    private static Dictionary<string, object?> BindingDict(object binding)
    {
        Type type = binding.GetType();
        return new Dictionary<string, object?> { ["accountId"] = type.GetProperty("AccountId")!.GetValue(binding), ["roomId"] = type.GetProperty("RoomId")!.GetValue(binding), ["netEntityId"] = type.GetProperty("NetEntityId")!.GetValue(binding), ["entityType"] = type.GetProperty("EntityType")!.GetValue(binding), ["connectionGeneration"] = type.GetProperty("ConnectionGeneration")!.GetValue(binding) };
    }

    private static bool TryString(JsonElement root, string name, out string? value)
    {
        value = null;
        return root.TryGetProperty(name, out JsonElement element) && element.ValueKind == JsonValueKind.String && (value = element.GetString()) is not null;
    }
    private static string? Read(JsonElement root, string name) => TryString(root, name, out string? value) ? value : null;
    private static byte[] Ok() => Encoding.UTF8.GetBytes("{\"ok\":true}");
    private static byte[] Fail(string code) => Json(new Dictionary<string, object?> { ["ok"] = false, ["code"] = code });
    private static byte[] Json(Dictionary<string, object?> payload) => Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload));

    private static List<Dictionary<string, object?>> EncodeFrames(IEnumerable<object>? messages)
    {
        var frames = new List<Dictionary<string, object?>>();
        if (messages is null) return frames;
        foreach (object message in messages)
        {
            byte[] bytes = (byte[])WireCodecType!.GetMethod("EncodePack")!.Invoke(null, new[] { message })!;
            frames.Add(new Dictionary<string, object?>
            {
                ["connection"] = message.GetType().GetProperty("Connection")?.GetValue(message),
                ["bytesBase64"] = Convert.ToBase64String(bytes),
            });
        }
        return frames;
    }

}
