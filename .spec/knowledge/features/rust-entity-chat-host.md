---
name: rust-entity-chat-host
description: 切片级最小 Rust host——查 entity-chat 纯托管：Runtime 绑定/查询/快照、NativeCore 定时、Room 网线广播
metadata:
  type: doc
  status: 已交付
---

# 切片级最小 Rust host（entity-chat 与 lumio-ds）

`modules/process` 的 `lumio-ds` 是独立 Dedicated Server 的默认二进制入口，`entity_chat` 是其网络准入、房间生命周期与 Runtime 桥接模块（同时保留 RM-00011 切片测试 harness）。宿主负责进程管理、网络传输、有界资源控制、存储容器调度与平台 Launch 凭据验签；世界状态计算、实体绑定、发号与快照内容序列化纯消费 CoreCLR Runtime。宿主不拥有绑定表、发号、查询 switch 或快照旁路。

## 背景 / 目标

ADR-056 与 ADR 0007：Rust 宿主是接力交付面，只做托管与传输。Room 世界是 Runtime `EcsWorld`；绑定/查询/快照/Persist 只在 Runtime；定时只在 NativeCore 内核（wallClock + tickFrame）。

## 核心设计与硬约束

- **会话表**：只保存 `connection ↔ Runtime 绑定句柄` 与 `sess-*` 会话号。`NetEntityId` 由 Runtime 身份表发号（32 位小写 hex）。宿主不按账号 ID 建索引。
- **Runtime 消费**：CoreCLR `entity-chat-host` 转发 `Admit` / `Disconnect` / `Rebind` / `Expire` / `QueryAttribute` / `BuildFullSnapshot` / `BuildDelta` / `CapturePersist` / `RestorePersist`。恢复路径不 `Admit`、不新建 Active 绑定。
- **定时**：host-runtime 是 NativeCore ABI 适配层。五分钟断线保留走 `wallClock` one-shot；Tick 走 `tickFrame` repeating。删除 `expire_due` 轮询。
- **时钟无后门**：生产时钟 `SystemMonotonicClock` 只读真实单调时间，不持有任何可变时间状态，因此**没有任何方法能把它推快**（ADR-057 §6；架构守卫 `entity_chat_architecture.rs` 按结构不变量校验）。需要跨越期限的测试注入确定性的 `TestMonotonicClock`。
- **验收口径**：11 场景 replay 用短重连窗（`SUITE_RECONNECT_WINDOW_MS`，harness 专用）真实等待到期，S9 因此只证明短窗下的到期路径，**不覆盖五分钟这个产品数值**；产品窗仍是 `RECONNECT_WINDOW_MS`，由单元测试守。
- **Room 网线与连接安全**：loopback WebSocket。每 socket 建立时在 HTTP Upgrade 阶段通过 Bearer 凭证独立验票，由服务端分配单调递增的私有 `connectionId`，禁止客户端冒用。准入/重连发送 Runtime `BuildFullSnapshot`（含 `stateBlocks`）；每 Tick 把 `BuildDelta` 字节广播给本 Room 连接；顶号先发 `ConnectionSuperseded` 再关旧连接。S8 证据 `connectionSupersededReceived` 只来自旧 `RoomClient` 收帧，不得用宿主 `takeover` 布尔冒充。S3 在 101 条 `chat.input` 之前挂上 `c-browser` Room WS；`playwrightRan` 只在浏览器真正从网线收到 Room 帧时为 true。
- **关闭原因码词表**：传输层标准实现 10 种精确的 WebSocket 关闭状态码与字符串标识（遵循 ADR-057 / ADR-068 / ds-server §6）：`normal_closure` (1000)、`going_away` (1001)、`unsupported_contract` (4001)、`bad_envelope` (4002)、`auth_failed` (4003)、`rate_limited` (4004)、`queue_full` (4005)、`session_limit` (4006)、`role_taken` (4007)、`unknown_mapping` (4008)。
- **解析 / 查询**：`ResolveByNetEntityId` 接受 Runtime 32-hex 与 C-1 u64；HostEntry 把 Runtime `OkEntity`（无 Binding）补成列出的五元组。S5 unauthorized 走声明过的 claim-scoped `EntityIdentity.claimedMark`（`restrictedFlag` 未声明 → `RequestError`，不得冒充 Unauthorized）。
- **Tick 分批与输入队列**：Runtime `ChatCommandRuntime.RunTick` 经 `ChatIngressWorld` 默认 `EcsBudget.MaxChangeEntries=128`。每条 `chat.input` 写两个 ChatComponent 字段，单 Tick 最多 64 条；超过则 `Command reservation budget exceeded`、Runtime `_faulted`、`BuildDelta` 为 `changedBlocks:[]`。宿主按 `MAX_CHAT_INPUTS_PER_TICK` 穿插 `tickFrame`，`pending > 64` 不得 `RunTick`。`apply_pending_chat_ticks` 在 `tick.ok` 为 false 或 pending 不下降时结束，不自建第二份事件队列。网络输入采用公平轮询批次调度，不直接触发额外的逻辑帧。
- **房间生命周期与 `active_rooms`**：宿主维护有界的活跃房间集合。当房间内客户端连接全部断开时，房间不会立即停算，而是进入断线保留和持续模拟，避免将 socket 连接数与房间生命周期耦合。
- **Client Bot**：S6 发言由 suite 拉起 `Lumio.Client.Bot.Host`（可执行路径经 `LumioClientRoot` / `LUMIO_CLIENT_ROOT` / `LUMIO_BOT_HOST` 或仓根相对 `LumioClient` 兄弟发现，缺失 BLOCKED）。Harness 只传参并读其日志目录，禁止 startup-hooks 环境注入、禁止生成 hook csproj、禁止宿主自写 ABI 装载。启动参数：`--server`、`--account-from` / `--account-to`、`--engine-native`、`--log-dir`。Bot.Host 往 `--log-dir/bot-host.ndjson` 写 JSON lines（`kind=chat.input`）；`timer-trace.json` 不是证据。suite 按宿主 `pending_wire_chat_inputs` 穿插 tick，写 `release.flag` 结束进程；禁止 `host.admit_chat_input` 冒充 Bot 发言，禁止把常量 `[5,10,15]` 写成证据。验收尺子只有 Game `verify-evidence.mjs`，本仓不保留第二把尺。
- **Persist 与检查点**：`CapturePersist` / `RestorePersist` 走 Runtime 公开面（`RestorePersist` 的第二参是 `ReadOnlyMemory<byte>`）。存储架构遵守 ADR 0013：容器层归 Server，内容层归 Runtime。Server 检查点采用成组发布（draft 目录 → `checkpoint-<id>` 目录 → `active-checkpoint` 指针），目录与文件经 `fsync` 刷盘，支持 `process-crash` 与 Unix `power-loss` 耐久保证。Server 宿主不保留第二份未经集成的 Journal 机制。
- **配置化资源上限与健康监控**：传输上限统一进 `HostLimitsConfig`（`max_connections`、`max_pending_inputs`、`max_message_bytes`、`rate_limit_per_second`、`handshake_timeout_ms`、`drain_timeout_ms` 等）。进程侧通过 `HostHealth` 监控心跳与故障状态，Watchdog 发现超时或不可逆挂死时触发独立进程安全退出。
- **发现**：外部产物经 `LUMIO_*` 环境变量与仓根相对路径；缺失即 BLOCKED，不硬编码开发机绝对路径。
- **复跑**：`lumio-entity-chat-replay` 两轮；`manifest.conclusion=SUCCESS` 只在 Game `verify-evidence.mjs` oracle 通过之后写。`--restore-snapshot` 供 S7 进程 B 单独启 CLR 恢复。缺失观测一律输出 `null`，严禁补等于通过值的虚假默认值（R-00501）。

## 待解决

- 完整 101 实体 acceptance 依赖 Runtime / NativeCore / Game 产物路径；缺失时测试以 BLOCKED 失败而非跳过。S3 的 Playwright Room 观察同样依赖 `LUMIO_GAME_ROOT`。
- Runtime `ChatIngressWorld.Create` 默认预算装不下 101 实体 Persist；S7 跨进程恢复待 Runtime 放大 `MaxSnapshotBytes`。
- `account-server/` 整目录待 LumioPlatform 账号服务满足 ADR 0011 与考卷全绿后，由 R-00420 统一退役。

## 相关

- 决策：[`../../decisions/0007-rust-host-consume-runtime.md`](../../decisions/0007-rust-host-consume-runtime.md)
- 决策：[`../../decisions/0011-account-server-retirement-requires-platform-account-port.md`](../../decisions/0011-account-server-retirement-requires-platform-account-port.md)
- 决策：[`../../decisions/0012-lumio-ds-entry-and-configuration.md`](../../decisions/0012-lumio-ds-entry-and-configuration.md)
- 决策：[`../../decisions/0013-checkpoint-profile-durability-and-container-layer-ownership.md`](../../decisions/0013-checkpoint-profile-durability-and-container-layer-ownership.md)
- 决策：[`../../decisions/0014-v1-replay-boundary.md`](../../decisions/0014-v1-replay-boundary.md)
- 运行手册：[`ds-runbook.md`](ds-runbook.md)
- 实现：[`../../../modules/process/src/entity_chat/`](../../../modules/process/src/entity_chat/)
- 托管入口：[`../../../entity-chat-host/`](../../../entity-chat-host/)
