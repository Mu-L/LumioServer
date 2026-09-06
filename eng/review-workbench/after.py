from pathlib import Path

p=Path('account-server/src/Lumio.Server.Account/DurableAccountStore.cs');s=p.read_text()
s=s.replace('using System.IO;','using System.IO;\nusing System.Linq;',1)
s=s.replace('internal sealed class DurableAccountStore','internal sealed class DurableAccountStore : IDisposable',1)
s=s.replace('    private readonly string identityPath;\n    private readonly string credentialPath;','''    private string identityPath;
    private string credentialPath;
    private readonly FileStream writerLease;
    private readonly string pointerPath;
    private bool poisoned;
    public const string PointerFileName = "active-account-group";
    public string ActiveDirectory => Path.GetDirectoryName(identityPath)!;''',1)
s=s.replace('''        identityPath = Path.Combine(DirectoryPath, IdentityFileName);''','''        pointerPath = Path.Combine(DirectoryPath, PointerFileName);
        writerLease = new FileStream(Path.Combine(DirectoryPath, "account-writer.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        identityPath = Path.Combine(DirectoryPath, IdentityFileName);''',1)
s=s.replace('''        if (File.Exists(identityPath))''','''        if (File.Exists(pointerPath))
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

        if (File.Exists(identityPath))''',1)
s=s.replace('''            var root = document.RootElement;''','''            var root = document.RootElement;
            if (!root.TryGetProperty("version", out var version) || version.GetInt32() != 1)
                throw new InvalidDataException("unsupported account store version");''',2)
# Verify cohort equality after parsing both participants.
marker='''    public void Save(AccountWorld world, CredentialStore credentials)'''
idx=s.index(marker);before=s[:idx]
pos=before.rfind('    }')
before=before[:pos]+'''        var identityIds = world.Snapshot().Select(row => row.AccountId).ToHashSet(StringComparer.Ordinal);
        var credentialIds = credentials.Snapshot().Select(row => row.Key).ToHashSet(StringComparer.Ordinal);
        if (!identityIds.SetEquals(credentialIds))
            throw new InvalidDataException("account identity/credential cohort mismatch");
'''+before[pos:];s=before+s[idx:]
s=s.replace('''        var identities = world.Snapshot();''','''        if (poisoned) throw new InvalidOperationException("account writer requires reopen after a failed transaction");
        var previousDirectory = ActiveDirectory;
        var groupName = "group-" + Guid.NewGuid().ToString("N");
        var stagingDirectory = Path.Combine(DirectoryPath, "draft-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(stagingDirectory);
        var stagedIdentityPath = Path.Combine(stagingDirectory, IdentityFileName);
        var stagedCredentialPath = Path.Combine(stagingDirectory, CredentialFileName);
        // Until publication succeeds, even an exception in serialization seals
        // the writer; Dispose must never publish this partially mutated world.
        poisoned = true;
        var identities = world.Snapshot();''',1)
s=s.replace('AtomicWrite(identityPath, stream.ToArray());','AtomicWrite(stagedIdentityPath, stream.ToArray());',1)
s=s.replace('AtomicWrite(credentialPath, stream.ToArray());','AtomicWrite(stagedCredentialPath, stream.ToArray());',1)
marker='    private static void AtomicWrite'
idx=s.index(marker);before=s[:idx];pos=before.rfind('    }')
before=before[:pos]+'''        var publishedDirectory = Path.Combine(DirectoryPath, groupName);
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
'''+before[pos:];s=before+s[idx:]
s=s.replace('''        File.WriteAllBytes(temp, bytes);''','''        using (var stream = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            stream.Write(bytes);
            stream.Flush(flushToDisk: true);
        }''',1)
s=s.replace('''    private static void AtomicWrite''','''    public void Dispose() => writerLease.Dispose();

    private static void AtomicWrite''',1)
p.write_text(s)

p=Path('account-server/src/Lumio.Server.Account/AccountRuntime.cs');s=p.read_text()
s=s.replace('    private readonly Dictionary<string, object> nameGates = new(StringComparer.Ordinal);','    private bool faulted;')
s=s.replace('''        store.Load(world, credentials);''','''        try { store.Load(world, credentials); }
        catch { store.Dispose(); throw; }''',1)
s=s.replace('''        lock (NameGate(loginName))
        {''','''        lock (gate)
        {
            if (faulted) throw new InvalidOperationException("account store requires recovery after failed write");
            ObjectDisposedException.ThrowIf(disposed, this);''',1)
s=s.replace('''            var created = world.Create(accountId, loginName, options.Clock.UnixSeconds);
            credentials.Put(accountId, Argon2idPasswordHasher.Hash(password));''','''            var encodedPassword = Argon2idPasswordHasher.Hash(password);
            var created = world.Create(accountId, loginName, options.Clock.UnixSeconds);
            credentials.Put(accountId, encodedPassword);''',1)
s=s.replace('''                store.Save(world, credentials);''','''                try { store.Save(world, credentials); }
                catch { faulted = true; throw; }''',1)
a=s.index('    public void Flush()');b=s.index('    internal AccountWorld World',a)
s=s[:a]+'''    public void Flush()
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (faulted) throw new InvalidOperationException("account writer faulted");
            try { store.Save(world, credentials); }
            catch { faulted = true; throw; }
        }
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed) return;
            disposed = true;
            try { if (!faulted) store.Save(world, credentials); }
            finally
            {
                CryptographicOperations.ZeroMemory(options.AdmissionPrivateSeed);
                store.Dispose();
            }
        }
    }

    // All identity/credential mutations share the same transaction boundary.
    // No unbounded dictionary of attacker-controlled username locks exists.
    internal object NameGate(string loginName)
    {
        ArgumentNullException.ThrowIfNull(loginName);
        return gate;
    }

'''+s[b:]
p.write_text(s)
p=Path('account-server/tests/Lumio.Server.Account.Tests/CredentialIsolationTests.cs');s=p.read_text().replace('Path.Combine(harness.StorePath, DurableAccountStore.','Path.Combine(harness.Runtime.Store.ActiveDirectory, DurableAccountStore.');p.write_text(s)
Path('account-server/tests/Lumio.Server.Account.Tests/StorageTransactionTests.cs').write_text('''using System;
using System.IO;
using Lumio.Server.Account;
using Xunit;
namespace Lumio.Server.Account.Tests;

public sealed class StorageTransactionTests
{
    [Fact]
    public void FailedPublicationCannotPublishOnDispose()
    {
        using var harness = new AccountHarness();
        Assert.True(harness.Runtime.LoginOrRegister("alice", AccountTestProfile.Password, null).Accepted);
        var pointer = Path.Combine(harness.StorePath, DurableAccountStore.PointerFileName);
        var committed = File.ReadAllText(pointer);
        Directory.CreateDirectory(pointer + ".tmp");
        Assert.ThrowsAny<IOException>(() => harness.Runtime.LoginOrRegister("bravo", AccountTestProfile.Password, null));
        Assert.Throws<InvalidOperationException>(() => harness.Runtime.LoginOrRegister("charlie", AccountTestProfile.Password, null));
        harness.Runtime.Dispose();
        Assert.Equal(committed, File.ReadAllText(pointer));
        var identity = File.ReadAllText(Path.Combine(harness.StorePath, committed, DurableAccountStore.IdentityFileName));
        Assert.DoesNotContain("bravo", identity, StringComparison.Ordinal);
    }

    [Fact]
    public void ASecondWriterCannotOpenTheSameAccountStore()
    {
        using var harness = new AccountHarness();
        Assert.ThrowsAny<IOException>(() => new DurableAccountStore(harness.StorePath));
    }

    [Fact]
    public void PublishedGroupContainsBothParticipants()
    {
        using var harness = new AccountHarness();
        Assert.True(harness.Runtime.LoginOrRegister("alice", AccountTestProfile.Password, null).Accepted);
        var group = harness.Runtime.Store.ActiveDirectory;
        Assert.True(File.Exists(Path.Combine(group, DurableAccountStore.IdentityFileName)));
        Assert.True(File.Exists(Path.Combine(group, DurableAccountStore.CredentialFileName)));
        Assert.NotEqual(harness.StorePath, group);
    }
}
''')
Path(__file__).unlink()
