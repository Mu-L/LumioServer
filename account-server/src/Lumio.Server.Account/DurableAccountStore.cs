using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace Lumio.Server.Account;

internal sealed class DurableAccountStore : IDisposable
{
    public const string IdentityFileName = "account-identities.json";
    public const string CredentialFileName = "account-credentials.json";

    private const int IdentityVersion = 1;
    private const int CredentialVersion = 1;
    private string identityPath;
    private string credentialPath;
    private readonly FileStream writerLease;
    private readonly string pointerPath;
    private bool poisoned;
    public const string PointerFileName = "active-account-group";
    public string ActiveDirectory => Path.GetDirectoryName(identityPath)!;

    public DurableAccountStore(string directory)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        DirectoryPath = Path.GetFullPath(directory);
        Directory.CreateDirectory(DirectoryPath);
        pointerPath = Path.Combine(DirectoryPath, PointerFileName);
        writerLease = new FileStream(Path.Combine(DirectoryPath, "account-writer.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        identityPath = Path.Combine(DirectoryPath, IdentityFileName);
        credentialPath = Path.Combine(DirectoryPath, CredentialFileName);
    }

    public string DirectoryPath { get; }

    public void Load(AccountWorld world, CredentialStore credentials)
    {
        ArgumentNullException.ThrowIfNull(world);
        ArgumentNullException.ThrowIfNull(credentials);

        if (File.Exists(pointerPath))
        {
            var group = File.ReadAllText(pointerPath).Trim();
            if (!group.StartsWith("group-", StringComparison.Ordinal) || group.Length != 38
                || !Guid.TryParseExact(group[6..], "N", out _))
                throw new InvalidDataException("invalid account group pointer");
            var active = Path.Combine(DirectoryPath, group);
            identityPath = Path.Combine(active, IdentityFileName);
            credentialPath = Path.Combine(active, CredentialFileName);
            if (!File.Exists(identityPath) || !File.Exists(credentialPath))
                throw new InvalidDataException("committed account group is incomplete");
        }
        else if (Directory.EnumerateDirectories(DirectoryPath, "group-*").Any())
        {
            // A group without its publication pointer has never been committed.
            // It is not a reason to guess which orphaned group should win.
            identityPath = Path.Combine(DirectoryPath, IdentityFileName);
            credentialPath = Path.Combine(DirectoryPath, CredentialFileName);
        }
        if (File.Exists(identityPath) != File.Exists(credentialPath))
            throw new InvalidDataException("legacy account files do not form a complete pair");
        foreach (var path in new[] { identityPath, credentialPath })
            if (File.Exists(path) && new FileInfo(path).Length > 64 * 1024 * 1024)
                throw new InvalidDataException("account file exceeds bounded import size");

        if (File.Exists(identityPath))
        {
            using var document = JsonDocument.Parse(File.ReadAllBytes(identityPath));
            var root = document.RootElement;
            if (!root.TryGetProperty("version", out var version) || version.GetInt32() != 1)
                throw new InvalidDataException("unsupported account store version");
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
            if (!root.TryGetProperty("version", out var version) || version.GetInt32() != 1)
                throw new InvalidDataException("unsupported account store version");
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
        var identityIds = world.Snapshot().Select(row => row.AccountId).ToHashSet(StringComparer.Ordinal);
        var credentialIds = credentials.Snapshot().Select(row => row.Key).ToHashSet(StringComparer.Ordinal);
        if (!identityIds.SetEquals(credentialIds))
            throw new InvalidDataException("account identity/credential cohort mismatch");
    }

    public void Save(AccountWorld world, CredentialStore credentials)
    {
        ArgumentNullException.ThrowIfNull(world);
        ArgumentNullException.ThrowIfNull(credentials);

        if (poisoned) throw new InvalidOperationException("account writer requires reopen after a failed transaction");
        var previousDirectory = ActiveDirectory;
        var groupName = "group-" + Guid.NewGuid().ToString("N");
        var stagingDirectory = Path.Combine(DirectoryPath, "draft-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(stagingDirectory);
        var stagedIdentityPath = Path.Combine(stagingDirectory, IdentityFileName);
        var stagedCredentialPath = Path.Combine(stagingDirectory, CredentialFileName);
        // Until publication succeeds, even an exception in serialization seals
        // the writer; Dispose must never publish this partially mutated world.
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

            AtomicWrite(stagedIdentityPath, stream.ToArray());
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

            AtomicWrite(stagedCredentialPath, stream.ToArray());
        }
        var publishedDirectory = Path.Combine(DirectoryPath, groupName);
        Directory.Move(stagingDirectory, publishedDirectory);
        AtomicWrite(pointerPath, System.Text.Encoding.UTF8.GetBytes(groupName));
        identityPath = Path.Combine(publishedDirectory, IdentityFileName);
        credentialPath = Path.Combine(publishedDirectory, CredentialFileName);
        poisoned = false;
        // Keep the current and previous complete groups. Orphan draft groups
        // are ignored by recovery and may be removed by explicit maintenance.
        foreach (var old in Directory.EnumerateDirectories(DirectoryPath, "group-*"))
            if (!string.Equals(old, publishedDirectory, StringComparison.Ordinal)
                && !string.Equals(old, previousDirectory, StringComparison.Ordinal))
                Directory.Delete(old, recursive: true);
    }

    public void Dispose() => writerLease.Dispose();

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
