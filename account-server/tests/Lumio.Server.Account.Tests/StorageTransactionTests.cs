using System;
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
