using System;
using System.IO;
using Lumio.Server.Account;
using Xunit;

namespace Lumio.Server.Account.Tests;

public sealed class StorageTransactionTests
{
    [Fact]
    public void NameGateIsSingleSharedInstanceAcrossAllLoginNames()
    {
        using var harness = new AccountHarness();
        var gateAlice = harness.Runtime.NameGate("alice");
        var gateBob = harness.Runtime.NameGate("bob");
        Assert.Same(gateAlice, gateBob);
        Assert.Same(gateAlice, harness.Runtime.Gate);
    }

    [Fact]
    public void FailedWriteLocksFaultedStateAndPreventsSaveOnDispose()
    {
        using var harness = new AccountHarness();
        Assert.True(harness.Runtime.LoginOrRegister("alice", AccountTestProfile.Password, null).Accepted);

        var identityPath = Path.Combine(harness.StorePath, DurableAccountStore.IdentityFileName);
        var committedText = File.ReadAllText(identityPath);

        var tempFileAsDir = identityPath + ".tmp";
        Directory.CreateDirectory(tempFileAsDir);
        try
        {
            var failure = Record.Exception(() => harness.Runtime.LoginOrRegister("bravo", AccountTestProfile.Password, null));
            Assert.True(failure is IOException or UnauthorizedAccessException);

            Assert.Throws<InvalidOperationException>(() => harness.Runtime.LoginOrRegister("charlie", AccountTestProfile.Password, null));

            harness.Runtime.Dispose();
            Assert.Equal(committedText, File.ReadAllText(identityPath));
            Assert.DoesNotContain("bravo", File.ReadAllText(identityPath), StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(tempFileAsDir)) Directory.Delete(tempFileAsDir);
        }
    }

    [Fact]
    public void AtomicWriteFlushesToDiskAndCreatesCommittedFiles()
    {
        using var harness = new AccountHarness();
        Assert.True(harness.Runtime.LoginOrRegister("alice", AccountTestProfile.Password, null).Accepted);
        harness.Runtime.Flush();

        var identityPath = Path.Combine(harness.StorePath, DurableAccountStore.IdentityFileName);
        var credentialPath = Path.Combine(harness.StorePath, DurableAccountStore.CredentialFileName);

        Assert.True(File.Exists(identityPath));
        Assert.True(File.Exists(credentialPath));
        Assert.False(File.Exists(identityPath + ".tmp"));
        Assert.False(File.Exists(credentialPath + ".tmp"));

        var content = File.ReadAllText(identityPath);
        Assert.Contains("alice", content, StringComparison.Ordinal);
    }
}
