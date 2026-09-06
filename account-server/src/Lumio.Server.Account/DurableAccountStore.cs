using System;
using System.IO;
using System.Text.Json;

namespace Lumio.Server.Account;

internal sealed class DurableAccountStore : IDisposable
{
    public const string IdentityFileName = "account-identities.json";
    public const string CredentialFileName = "account-credentials.json";

    private const int IdentityVersion = 1;
    private const int CredentialVersion = 1;
    private readonly string identityPath;
    private readonly string credentialPath;
    private bool poisoned;

    public DurableAccountStore(string directory)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        DirectoryPath = Path.GetFullPath(directory);
        Directory.CreateDirectory(DirectoryPath);
        identityPath = Path.Combine(DirectoryPath, IdentityFileName);
        credentialPath = Path.Combine(DirectoryPath, CredentialFileName);
    }

    public string DirectoryPath { get; }

    public string ActiveDirectory => DirectoryPath;

    public void Load(AccountWorld world, CredentialStore credentials)
    {
        ArgumentNullException.ThrowIfNull(world);
        ArgumentNullException.ThrowIfNull(credentials);

        if (File.Exists(identityPath))
        {
            using var document = JsonDocument.Parse(File.ReadAllBytes(identityPath));
            var root = document.RootElement;
            if (!root.TryGetProperty("entities", out var entities) || entities.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidDataException("identity store missing entities");
            }

            foreach (var entity in entities.EnumerateArray())
            {
                var component = new AccountIdentityComponent(
                    entity.GetProperty("entityId").GetUInt64(),
                    entity.GetProperty("accountId").GetString() ?? throw new InvalidDataException("accountId"),
                    entity.GetProperty("loginName").GetString() ?? throw new InvalidDataException("loginName"),
                    entity.GetProperty("createdAt").GetUInt64());
                world.Restore(component);
            }
        }

        if (File.Exists(credentialPath))
        {
            using var document = JsonDocument.Parse(File.ReadAllBytes(credentialPath));
            var root = document.RootElement;
            if (!root.TryGetProperty("hashes", out var hashes) || hashes.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidDataException("credential store missing hashes");
            }

            foreach (var hash in hashes.EnumerateArray())
            {
                var accountId = hash.GetProperty("accountId").GetString()
                    ?? throw new InvalidDataException("credential accountId");
                var encoded = hash.GetProperty("argon2id").GetString()
                    ?? throw new InvalidDataException("argon2id");
                credentials.Put(accountId, encoded);
            }
        }
    }

    public void Save(AccountWorld world, CredentialStore credentials)
    {
        ArgumentNullException.ThrowIfNull(world);
        ArgumentNullException.ThrowIfNull(credentials);

        if (poisoned) throw new InvalidOperationException("account store requires reopen after a failed write");
        poisoned = true;

        var identities = world.Snapshot();
        using (var stream = new MemoryStream())
        {
            using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = false }))
            {
                writer.WriteStartObject();
                writer.WriteNumber("version", IdentityVersion);
                writer.WriteStartArray("entities");
                foreach (var identity in identities)
                {
                    writer.WriteStartObject();
                    writer.WriteNumber("entityId", identity.EntityId);
                    writer.WriteString("accountId", identity.AccountId);
                    writer.WriteString("loginName", identity.LoginName);
                    writer.WriteNumber("createdAt", identity.CreatedAtUnixSeconds);
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }

            AtomicWrite(identityPath, stream.ToArray());
        }

        var hashes = credentials.Snapshot();
        using (var stream = new MemoryStream())
        {
            using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = false }))
            {
                writer.WriteStartObject();
                writer.WriteNumber("version", CredentialVersion);
                writer.WriteStartArray("hashes");
                foreach (var pair in hashes)
                {
                    writer.WriteStartObject();
                    writer.WriteString("accountId", pair.Key);
                    writer.WriteString("argon2id", pair.Value);
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }

            AtomicWrite(credentialPath, stream.ToArray());
        }

        poisoned = false;
    }

    public void Dispose() { }

    private static void AtomicWrite(string path, byte[] bytes)
    {
        var temp = path + ".tmp";
        using (var stream = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            stream.Write(bytes);
            stream.Flush(flushToDisk: true);
        }
        if (File.Exists(path))
        {
            File.Replace(temp, path, destinationBackupFileName: null);
        }
        else
        {
            File.Move(temp, path);
        }
    }
}
