---
name: ds-runbook
description: LumioServer 独立 Dedicated Server (lumio-ds) 构建、运行、配置、准入与验收手册
metadata:
  type: doc
  status: 已交付
---

# LumioServer DS 运行与验收手册

## 构建与运行

```sh
cargo build -p lumio-server-process --release --locked --bin lumio-ds
./target/release/lumio-ds --config /absolute/path/server.json --check-config
./target/release/lumio-ds --config /absolute/path/server.json
```

从 `eng/server.example.json` 复制并填写真实 SDK、Runtime、Registry 与平台公钥。所有相对文件路径相对于配置文件目录，而不是当前工作目录。

一个进程对应一个可信 allocation 的房间与版本。可信分配上下文来自运维/平台配置，不能从客户端反向修改。版本切换采用新进程，旧进程收到 SIGTERM 排空、保存后退出。

## 准入与浏览器

默认监听仅 loopback 动态端口，Ready 行给出实际端口。公网由 TLS/WSS 边缘代理转发，不能把明文监听端口直接映射公网。边缘限制连接速率并关闭敏感头部日志（特别是 `Authorization` 和 `Sec-WebSocket-Protocol`）。

Native/Bot 通过 HTTP Upgrade 的 `Authorization: Bearer <admissionCredential>` 提交平台 **Launch** 房间票。普通 `/account` 票为 unbound，不可进入 DS。浏览器适配器位于 `eng/connect-ds.mjs`，使用额外的 `lumio-admission.<credential>` subprotocol offer 承载同一张票；服务端只回应 `lumio.mvp.v0`，不回显凭据。这个 Upgrade 适配器不改变 C-1 Gameplay 编解码。

当前消费跟架构仓 main 的账号票据格式：签名、期限及六个 allocation 维度一起校验。按 ADR-061 / ADR-068 / ADR 0014 的 bearer 策略，**不设 nonce 单次消费表**。每个 socket 都重新验证，并获得服务器生成的私有连接身份。账号接管由 Runtime 决定。

## 状态与退出

`DS_READY` 只在真实 Runtime boot/restore 后输出。`DS_CHECKPOINT` 代表一次存储发布完成；`DS_DRAINING` 后不再接受新准入或输入；成功最终保存并关闭后输出 `DS_STOPPED`。参数错误退出 3，运行或初始化故障退出 2，正常退出 0。

Owner 请求/结果各有 2 秒期限。Watchdog 不向 Owner 排队，直接读取心跳/故障状态。无法确认是否已执行的超时按故障处理，不自动重试可能有副作用的命令。独立进程退出是不可逆挂死时的最终处置。

## 耐久与恢复

当前部署 profile 为 `runtime-only`。它定期在 Owner 侧获取 Runtime 快照，再由存储后台线程写入检查点（遵守 ADR 0013）。检查点发布期间保留旧组，文件校验失败只允许整组回退，不会从不同组拼 Runtime 与 Voxel。

`process-crash`：覆盖进程异常退出，提供文件原子性保证。`power-loss`：在支持目录 fsync 的 Unix 文件系统上提供完整同步；不支持的环境不虚报。检查点容量上限与组保留策略由配置决定（默认 64 MiB、保留最近 3 组）；发布错误封锁写端。账号 fixture 按 ADR 0011 与 R-00502 维持最小防护，待 R-00420 退役。

## 验证层级

```sh
# 1. spec 与 lint
node .spec/tools/spec-lint.mjs
node --test .spec/tools/spec-lint.test.mjs

# 2. Rust 检查与单元/集成测试
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked

# 3. 托管层构建与测试
dotnet build entity-chat-host/src/Lumio.Server.EntityChat.HostEntry/Lumio.Server.EntityChat.HostEntry.csproj -c Release
dotnet test account-server/tests/Lumio.Server.Account.Tests/Lumio.Server.Account.Tests.csproj

# 4. 真实 11 场景跨仓集成（需指向 LumioGame main 检出，显式开启 test-harness）
LUMIO_GAME_ROOT=/path/to/LumioGame cargo test -p lumio-server-process --features="test-harness" --locked --test entity_chat_acceptance -- --nocapture
```

模块检查必须全部通过；缺工具/环境为非零退出。跨仓集成跟各依赖仓 main（ADR-068），不钉 SHA、不维护锁文件。历史 11 场景受显式 `test-harness` 门保护；S1 缺观测不再补等于通过值的默认值（R-00501）。

## 关联

- 决策：[`../../decisions/0012-lumio-ds-entry-and-configuration.md`](../../decisions/0012-lumio-ds-entry-and-configuration.md)
- 决策：[`../../decisions/0013-checkpoint-profile-durability-and-container-layer-ownership.md`](../../decisions/0013-checkpoint-profile-durability-and-container-layer-ownership.md)
- 决策：[`../../decisions/0014-v1-replay-boundary.md`](../../decisions/0014-v1-replay-boundary.md)
