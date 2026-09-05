using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Reflection.Emit;
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
    private static Type? ManagerType;
    private static Type? WireCodecType;
    private static Type? EcsRegistryType;
    private static object? Bindings;
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
                "enqueue" => EnqueueWorldMessage(root),
                "tick" => Tick(root),
                "drain" => DrainOutbox(),
                "snapshot" => CaptureSnapshot(),
                "restore" => Restore(root),
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
        ManagerType = Ecs.GetType("Lumio.GameRuntime.Ecs.WorldManager");
        WireCodecType = Ecs.GetType("Lumio.GameRuntime.Ecs.WireCodec");
        EcsRegistryType = Ecs.GetType("Lumio.GameRuntime.Ecs.EcsRegistry");
        if (BindingType is null || ManagerType is null || WireCodecType is null || EcsRegistryType is null) return (EntrySuccess, Fail("boot_failed"));
        if (!HasPublicStaticMethod(WireCodecType, "DecodeInput") || !HasPublicStaticMethod(WireCodecType, "EncodePack")) return (EntrySuccess, Fail("boot_failed"));
        object? registry = EcsRegistryType.GetProperty("Current", BindingFlags.Public | BindingFlags.Static)?.GetValue(null) ?? FindGeneratedRegistry();
        if (registry is null) return (EntrySuccess, Fail("registry_required"));
        ulong instanceId = root.TryGetProperty("instanceId", out JsonElement id) && id.TryGetUInt64(out ulong supplied) ? supplied : 1UL;
        Manager = ManagerType.GetMethod("Create", BindingFlags.Public | BindingFlags.Static)!.Invoke(null, new object?[] { registry, instanceId });
        ManagerType.GetMethod("Start", BindingFlags.Public | BindingFlags.Instance)!.Invoke(Manager, new object?[] { System.Threading.Thread.CurrentThread });
        Bindings = BindingType.GetMethod("Create", new[] { ManagerType })!.Invoke(null, new[] { Manager });
        return Manager is null || Bindings is null ? (EntrySuccess, Fail("boot_failed")) : (EntrySuccess, Ok());
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

    private static bool HasPublicStaticMethod(Type type, string name)
    {
        foreach (MethodInfo method in type.GetMethods(BindingFlags.Public | BindingFlags.Static))
            if (method.Name == name) return true;
        return false;
    }

    private static (int, byte[]) EnqueueWorldMessage(JsonElement root)
    {
        if (!TryString(root, "messageType", out string? messageType)) return (EntryInvalidInput, Fail("bad_envelope"));
        try
        {
            Enqueue(CreateWorldMessage(messageType!, root));
            return (EntrySuccess, Ok());
        }
        catch (FormatException) { return (EntryInvalidInput, Fail("bad_envelope")); }
        catch (TargetInvocationException error) when (error.InnerException is ArgumentException or InvalidOperationException)
        {
            return (EntrySuccess, Fail("invalid_request"));
        }
    }

    private static object CreateWorldMessage(string messageType, JsonElement root)
    {
        string type = messageType.EndsWith("Message", StringComparison.Ordinal) ? messageType : messageType + "Message";
        if (type == "AdmitConnectionMessage")
        {
            return NewMessage(type,
                RequiredString(root, "connection"),
                RequiredString(root, "accountId"),
                RequiredString(root, "roomId"),
                RequiredString(root, "entityType"));
        }
        if (type == "DisconnectConnectionMessage") return NewMessage(type, RequiredString(root, "connection"));
        if (type == "RebindConnectionMessage")
        {
            return NewMessage(type,
                RequiredString(root, "connection"),
                RequiredString(root, "accountId"),
                RequiredString(root, "roomId"),
                RequiredString(root, "mode"));
        }
        if (type == "ExpireEntityMessage")
        {
            return NewMessage(type,
                RequiredString(root, "requestId"),
                RequiredString(root, "netEntityId"),
                OptionalString(root, "connection"));
        }
        if (type == "ResolveBindingMessage")
        {
            return NewMessage(type,
                RequiredString(root, "requestId"),
                RequiredString(root, "roomId"),
                RequiredString(root, "netEntityId"),
                OptionalUInt64(root, "connectionGeneration"),
                OptionalString(root, "connection"));
        }
        if (type == "AttributeQueryMessage")
        {
            return NewMessage(type,
                RequiredString(root, "requestId"),
                RequiredString(root, "callerScope"),
                RequiredString(root, "roomId"),
                RequiredString(root, "netEntityId"),
                RequiredString(root, "attributeId"),
                OptionalUInt64(root, "connectionGeneration"),
                OptionalString(root, "connection"));
        }
        if (type == "InputCommandMessage") return CreateInputMessage(root);
        throw new FormatException("unsupported world message");
    }

    private static object CreateInputMessage(JsonElement root)
    {
        string senderText = RequiredString(root, "senderNetEntityId");
        string encoded = RequiredString(root, "envelopeBase64");
        byte[] envelope = Convert.FromBase64String(encoded);
        Type netEntityIdType = Ecs!.GetType("Lumio.GameRuntime.Ecs.NetEntityId")!;
        object sender = netEntityIdType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static)!.Invoke(null, new object?[] { senderText })!;
        Type messageType = Ecs.GetType("Lumio.GameRuntime.Ecs.InputCommandMessage")!;
        object message = DecodeInput(envelope, sender, netEntityIdType, messageType);
        if (Read(root, "connection") is string connection)
            messageType.GetProperty("Connection")?.SetValue(message, connection);
        return message;
    }

    private static object DecodeInput(byte[] envelope, object sender, Type netEntityIdType, Type messageType)
    {
        MethodInfo decode = WireCodecType!.GetMethod(
            "DecodeInput",
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            types: new[] { typeof(ReadOnlySpan<byte>), netEntityIdType },
            modifiers: null) ?? throw new MissingMethodException(WireCodecType.FullName, "DecodeInput");
        Type bridgeType = typeof(InputDecodeBridge);
        var method = new DynamicMethod(
            "lumio_decode_input",
            typeof(object),
            new[] { typeof(ReadOnlySpan<byte>), typeof(ulong), typeof(ulong) },
            typeof(HostEntry).Module,
            skipVisibility: true);
        ILGenerator il = method.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Ldarg_2);
        il.Emit(OpCodes.Newobj, netEntityIdType.GetConstructor(new[] { typeof(ulong), typeof(ulong) })!);
        il.Emit(OpCodes.Call, decode);
        il.Emit(OpCodes.Ret);
        InputDecodeBridge bridge = (InputDecodeBridge)method.CreateDelegate(bridgeType);
        ulong instance = Convert.ToUInt64(netEntityIdType.GetProperty("InstanceId")!.GetValue(sender), System.Globalization.CultureInfo.InvariantCulture);
        ulong counter = Convert.ToUInt64(netEntityIdType.GetProperty("Counter")!.GetValue(sender), System.Globalization.CultureInfo.InvariantCulture);
        return bridge(envelope, instance, counter);
    }

    private delegate object InputDecodeBridge(ReadOnlySpan<byte> envelope, ulong instanceId, ulong counter);

    private static (int, byte[]) Tick(JsonElement root)
    {
        _ = root;
        TickManager();
        ulong applied = WorldValue("Tick");
        ulong revision = WorldValue("Revision");
        return (EntrySuccess, Json(new Dictionary<string, object?> { ["ok"] = true, ["appliedTick"] = applied, ["revision"] = revision }));
    }

    private static (int, byte[]) DrainOutbox()
    {
        // Runtime 37eb7b0 returns a WorldDrainResponse containing C-1 frames and internal queries.
        object response = ManagerType!.GetMethod("DrainOutbox", BindingFlags.Public | BindingFlags.Instance)!.Invoke(Manager, null)!;
        Type responseType = response.GetType();
        List<object> frames = ToObjectList(responseType.GetProperty("Frames")!.GetValue(response) as IEnumerable);
        List<object> queries = ToObjectList(responseType.GetProperty("Queries")!.GetValue(response) as IEnumerable);
        return (EntrySuccess, Json(new Dictionary<string, object?>
        {
            ["ok"] = true,
            ["frames"] = EncodeFrames(frames),
            ["queries"] = EncodeQueries(queries),
        }));
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
        return (EntrySuccess, Ok());
    }

    private static object NewMessage(string typeName, params object?[] args) => Activator.CreateInstance(Ecs!.GetType("Lumio.GameRuntime.Ecs." + typeName)!, args)!;
    private static void Enqueue(object message) => ManagerType!.GetMethod("Enqueue")!.Invoke(Manager, new[] { message });
    private static void TickManager() => ManagerType!.GetMethod("Tick")!.Invoke(Manager, null);
    private static List<object> ToObjectList(IEnumerable? rows) { var result = new List<object>(); if (rows is null) return result; foreach (object row in rows) result.Add(row); return result; }
    private static ulong WorldValue(string property) { object world = ManagerType!.GetProperty("World")!.GetValue(Manager)!; return Convert.ToUInt64(world.GetType().GetProperty(property)!.GetValue(world), System.Globalization.CultureInfo.InvariantCulture); }

    private static string RequiredString(JsonElement root, string name) => TryString(root, name, out string? value) ? value! : throw new FormatException("missing field: " + name);

    private static string? OptionalString(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out JsonElement element) || element.ValueKind == JsonValueKind.Null) return null;
        if (element.ValueKind == JsonValueKind.String && element.GetString() is string value) return value;
        throw new FormatException("invalid field: " + name);
    }

    private static ulong? OptionalUInt64(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out JsonElement element) || element.ValueKind == JsonValueKind.Null) return null;
        if (element.TryGetUInt64(out ulong value)) return value;
        throw new FormatException("invalid field: " + name);
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
            var frame = new Dictionary<string, object?>
            {
                ["connection"] = message.GetType().GetProperty("Connection")?.GetValue(message),
                ["bytesBase64"] = Convert.ToBase64String(bytes),
                ["messageType"] = message.GetType().Name.EndsWith("Message", StringComparison.Ordinal)
                    ? message.GetType().Name[..^"Message".Length]
                    : message.GetType().Name,
            };
            Type type = message.GetType();
            object? observer = type.GetProperty("Self")?.GetValue(message)
                ?? type.GetProperty("ObserverId")?.GetValue(message)
                ?? type.GetProperty("NetEntityId")?.GetValue(message);
            if (observer is not null && !(type.GetProperty("IsDefault")?.GetValue(observer) as bool? ?? false))
                frame["observerNetEntityId"] = observer.ToString();
            object? generation = type.GetProperty("ConnectionGeneration")?.GetValue(message)
                ?? type.GetProperty("NewConnectionGeneration")?.GetValue(message);
            if (generation is not null)
                frame["connectionGeneration"] = generation;
            if (type.GetProperty("Code")?.GetValue(message) is string code)
                frame["code"] = code;
            frames.Add(frame);
        }
        return frames;
    }

    private static List<Dictionary<string, object?>> EncodeQueries(IEnumerable<object>? messages)
    {
        var queries = new List<Dictionary<string, object?>>();
        if (messages is null) return queries;
        foreach (object message in messages)
        {
            Type type = message.GetType();
            var record = new Dictionary<string, object?>
            {
                ["type"] = type.Name,
                ["requestId"] = type.GetProperty("RequestId")!.GetValue(message),
                ["outcome"] = type.GetProperty("Outcome")!.GetValue(message),
            };
            if (type.Name == "ResolveBindingResult" && type.GetProperty("Binding")!.GetValue(message) is object binding)
                record["binding"] = EncodeBindingRecord(binding);
            foreach (string name in new[] { "ObservedRevision", "ObservedTick", "NetEntityId", "RoomId", "AttributeId", "Value", "Code", "Detail" })
            {
                object? value = type.GetProperty(name)?.GetValue(message);
                if (value is not null) record[char.ToLowerInvariant(name[0]) + name[1..]] = value;
            }
            queries.Add(record);
        }
        return queries;
    }

    private static Dictionary<string, object?> EncodeBindingRecord(object binding)
    {
        Type type = binding.GetType();
        return new Dictionary<string, object?>
        {
            ["accountId"] = type.GetProperty("AccountId")!.GetValue(binding),
            ["roomId"] = type.GetProperty("RoomId")!.GetValue(binding),
            ["netEntityId"] = type.GetProperty("NetEntityId")!.GetValue(binding),
            ["entityType"] = type.GetProperty("EntityType")!.GetValue(binding),
            ["connectionGeneration"] = type.GetProperty("ConnectionGeneration")!.GetValue(binding),
        };
    }

}
