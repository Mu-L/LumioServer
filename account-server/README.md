# account-server — retained integration fixture

This loopback C# account process is retained for the historical eleven-scenario test. **It is not the current production account authority, and its legacy short admission credential cannot enter the new authenticated DS.** Production account/login and allocation-bound Launch issuance belong to LumioPlatform.

The `contract/` files retain the old `2b7e321` fixture source, not a claim that the current public contract is frozen there. Current protocol truth remains LumioGameEngine. Retire this directory only after the real Platform path satisfies ADR 0011; do not replace the wrong-password observation with locally minted credentials.

```sh
cd account-server
dotnet restore build.proj --locked-mode
dotnet build build.proj -c Release --no-restore
dotnet test tests/Lumio.Server.Account.Tests/Lumio.Server.Account.Tests.csproj -c Release --no-build
```

## Storage safety

An exclusive writer lease serializes one store owner. Identity and password-hash files are written into a new complete group, flushed, and activated through `active-account-group`. Existing legacy files are imported only as a complete, version-checked matching cohort. A publication error seals the runtime; Dispose never republishes the failed transaction. This is a process-crash fixture guarantee, not a PostgreSQL replacement or certified hardware power-loss guarantee.

## Fixture launch

Listen only on loopback (`127.0.0.1:0`). Inject Ed25519 keys through the environment; never commit or log private keys.

```text
lumio-account-server --store-path <dir> [--listen 127.0.0.1:0] [--admission-key-id 0-255]
```

Environment: `LUMIO_ACCOUNT_ADMISSION_PRIVATE_KEY_HEX` is a 32-byte seed; `LUMIO_ACCOUNT_BOT_TOOL_PUBLIC_KEY_HEX` is a 32-byte public key; `LUMIO_ACCOUNT_ADMISSION_KEY_ID` is the optional key id. Values are hexadecimal without `0x`.

Ready line: `ACCOUNT_SERVER_READY` with port, PID, contractId and storePath. Exit codes: 0 normal, 1 initialization failure, 2 fatal, 3 usage.
