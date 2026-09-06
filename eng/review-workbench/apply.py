from pathlib import Path

p=Path('modules/process/src/legacy.rs');s=p.read_text();s=s.replace('use std::io::Write as _;','use std::io::Write as _;\nuse crate::{wire, sdk_loader, server};',1);p.write_text(s)
p=Path('modules/process/src/entity_chat/host.rs');s=p.read_text()
old='''                let result = self.admit_verified(&proof.room_id, &connection_id, &proof.payload);'''
new='''                if self.admission_verifier.as_ref().is_some_and(|v| v.now() > proof.payload.expires_at || v.allocation.room_id != proof.room_id) {
                    let _ = egress.try_close();
                    return;
                }
                let result = self.admit_verified(&proof.room_id, &connection_id, &proof.payload);'''
assert old in s;s=s.replace(old,new,1)
s=s.replace('/// Bounded sink for exact Runtime-admitted input bytes used by external evidence consumers.','/// Bounded pre-admission ingress probe for test synchronization; not proof of Runtime commit.')
s=s.replace('    /// Attaches a bounded observer for exact admitted input bytes.','    /// Attaches a test synchronization probe for received input bytes.\n    /// Observation precedes Runtime admission and must not count as a commit.\n    #[cfg(any(test, feature = "test-harness"))]',1)
p.write_text(s)
p=Path('modules/process/src/entity_chat/suite.rs');s=p.read_text().replace('std::fs::File::open(&staging)?.sync_all()?;','std::fs::OpenOptions::new().write(true).open(&staging)?.sync_all()?;');p.write_text(s)
p=Path('modules/process/src/persistence.rs');s=p.read_text().replace('''        durability.sync_directory(root)?;
        let lock''','''        if durability == StorageDurability::PowerLoss {
            let canonical = root.canonicalize()?;
            for directory in canonical.ancestors() { durability.sync_directory(directory)?; }
        }
        durability.sync_directory(root)?;
        let lock''',1);p.write_text(s)
p=Path('account-server/src/Lumio.Server.Account/DurableAccountStore.cs');s=p.read_text()
a=s.index('        else if (Directory.EnumerateDirectories(DirectoryPath, "group-*").Any())');b=s.index('        if (File.Exists(identityPath) != File.Exists(credentialPath))',a)
s=s[:a]+'''        else if (Directory.EnumerateDirectories(DirectoryPath, "group-*").Any())
        {
            throw new InvalidDataException("account groups exist without a publication pointer; explicit recovery required");
        }
'''+s[b:];p.write_text(s)
p=Path('modules/process/src/ds.rs');s=p.read_text().replace('    durability: String,','    durability: String,\n    world_profile: String,',1)
s=s.replace('''        self.allocation.validate()?;''','''        self.allocation.validate()?;
        if self.world_profile != "runtime-only" {
            return Err("this host profile requires runtime-only; Voxel/WAL recovery needs a committed-cut provider".to_owned());
        }''',1);p.write_text(s)

Path('modules/process/README.md').write_text('''# process 模块

> 当前代码的组合根、认证传输、Runtime 桥与存储执行。本文不再引用已删除的 13 个模块骨架。

## 可执行入口

| 目标 | 构建 | 实际范围 |
|---|---|---|
| `lumio-ds` | 默认，无 `test-harness` | 分配绑定验票、异步连接、Runtime 启动/恢复、Owner Tick、健康检查、定期检查点、信号排空与最终保存 |
| `lumio-server` | 显式 `test-harness` | 历史 Hello World 集成入口，不是公网服务器 |
| `lumio-entity-chat-replay` | 显式 `test-harness` | 历史 11 场景考卷，需要外部 Game/SDK/托管产物 |

`lumio-ds` 目前只接受显式的 `world_profile=runtime-only`。有体素参与者或非零 WAL 水位的恢复材料会拒绝启动，不会恢复半个世界。

## 代码职责

- `ds.rs`：配置验证、实际 Native/CLR 组合、Ready、Watchdog、SIGTERM/SIGINT、检查点调度和退出码。
- `entity_chat/host.rs`：单 Owner 状态、有界排队、房间激活、准入结果、输出路由、过期请求、quiesce 与快照屏障。权威实体与复制语义仍归 Runtime。
- `entity_chat/secure.rs`：消费架构仓账号凭据格式，校验签名、期限及可信分配上下文。普通账号登录票不能进入房间。
- `entity_chat/wire.rs`：单个 Tokio reactor、受限连接任务、异步读写、握手/写入超时、尺寸限制、浏览器 Upgrade 适配、明确断连。
- `entity_chat/clr.rs` 与 `entity-chat-host/`：消费 Runtime；启动时构造解码委托，恢复不回灌旧连接授权。
- `persistence.rs`：不透明 Runtime/Voxel 检查点成组发布、身份校验、损坏组回退、单写锁、带校验链的提交记录日志。
- `legacy.rs`、`server.rs`、`session.rs`、`world.rs`：仅 Hello 测试构建使用，默认库不暴露旧的无认证 `run`。
- `entity_chat/{account,bots,browser,discover,suite}.rs`：测试功能构建专用，不能作为默认 DS 启动前置。

## 线程与失败语义

接收只入队，永不因输入数量增加逻辑帧。Owner 定时驱动 Runtime，活动房间不依赖当前连接数量。Runtime 拒绝有计数；致命/不确定失败封锁世界。Owner 心跳可从外部读取，不需要向已经卡住的 Owner 排队。

网络 Reactor 持有最多 256 个连接任务，握手时间上限 3 秒，写入期限 3 秒，应用帧上限 64 KiB。等待可写由异步 I/O 完成，不再每连接创建线程并 `sleep(5ms)`。这些是当前配置常量，不是压力测试得出的容量承诺。

Owner 请求入队与结果等待各有 2 秒期限；超时的操作可能已执行，因此封锁，而不是谎称已取消。线程不能安全强杀，监督器的有界 Join 只报告结果；独立 `lumio-ds` 进程是最终故障边界。

## 存储范围与限制

`CheckpointStore` 已实现成组文件发布，`Journal` 已实现有界记录、校验链、刷盘回执及不完整尾部处理。它们不生成 ECS/Voxel 变更集。

当前默认入口接入的是 **Runtime 检查点模式**，没有把 Journal 冒充已接通的世界 WAL。完整 WAL 重放、ECS/Voxel 同切点提供方、真实游戏迁移/长压测仍需跨仓闭环。

账号 fixture 使用独立的成组文件事务与失败封锁；账号权威仍应归 LumioPlatform。退役条件遵循本仓 ADR 0011，不能在平台登录/Launch 与原验收尺子尚未全部打通时删目录。

## 验证

```sh
python eng/verify.py --profile rust
python eng/verify.py --profile managed
python eng/verify.py --profile integration --inputs /absolute/path/resolved-inputs.json
```

前两个是模块验证，不要求外部游戏工作区。第三个要求固定代码与制品清单，缺环境非零退出；通过模块检查不能声称真实跨仓集成完成。详细运行与收尾见 [运行手册](../../eng/DS_RUNBOOK.md)。
''')
Path('modules/host-runtime/README.md').write_text('''# host-runtime

Rust Host 的少量共用原语，不拥有 Gameplay、账号、ECS 或体素状态。

## 当前实现

| 源码 | 实际职责 |
|---|---|
| `clock.rs` | 不可推进的真实单调钟；测试只通过显式测试时钟推进，保留 R-00493 的生产时钟红线 |
| `channel.rs` | 标准有界 MPSC 封装；Full/Closed 返回原值；容量通知与有期限发送 |
| `supervisor.rs` | 可唤醒取消、完成/故障锁存、可查询 panic、有界 Join；超时不谎称线程已经停止 |
| `kernel.rs` / `native_timer.rs` / `timer.rs` | NativeCore 定时 ABI 适配，不在 Host 再实现逻辑定时内核 |
| `native_abi.rs` | 唯一动态库加载器与根表定义，消费 SDK 边界 |

## 所有权

正常默认服务器使用 `SystemMonotonicClock`，测试不能给它注入 offset。`SharedClock::advance_test_clock` 只对确定性测试时钟成功。

Channel 接收方退出会唤醒等待发送方。发送期限用 `Full(value)` 表示容量等待超时；这不是已提交操作的取消保证。

`SupervisedTask::failure` 可以不消费 Join 就读取故障。`join_timeout` 超时保留句柄；Drop 不无限等待，但会记录未停止的线程。任意线程/Native/CLR 卡死最终要由独立进程退出处置，不支持安全的线程强杀。

## 验证

`cargo test -p lumio-host-runtime --locked` 覆盖时钟不可快进、容量释放、接收端关闭、取消唤醒、panic 锁存和 Join 超时后重试。真实 Native 动态库的跨仓验证属于单独集成范围。
''')
Path('modules/README.md').write_text('''# LumioServer 模块

公共架构与接口唯一来源仍是 `LumioGameEngine` 的现行 `engine/abi`、`engine/wire` 与 `.spec/knowledge/features/`。此处只描述实际代码，不重建已删除的骨架。

| crate | 依赖和职责 |
|---|---|
| [host-runtime](host-runtime/README.md) | 单调时间、有界通道、监督线程、Native 加载与定时适配 |
| [process](process/README.md) | 默认 `lumio-ds`、认证传输、Owner、托管桥、存储 I/O 和进程关闭 |
| [lumio-host-testkit](../crates/lumio-host-testkit) | 测试原语，不能作为生产状态真值 |

依赖方向为 `process → host-runtime`。`test-harness` 构建才包含 Hello、Replay、账号/浏览器/Bot 启动器和旧 connectionId observer 附着；默认 `lumio-ds` 始终使用认证构造器。

## 测试面

`eng/verify.py --profile rust` 验证默认构建、显式测试构建、Host/传输/Native 加载结构/文件存储的测试。`--profile managed` 构建实际 HostEntry 并执行账号模块测试。

`--profile integration` 保留原 11 场景考卷，并要求固定仓库和实际制品清单。缺少环境即失败，不再让此任务通过 `continue-on-error` 混入模块绿色状态。它仍是历史切片考卷，不能单独证明新 DS 的完整商业化能力。
''')
p=Path('README.md');s=p.read_text();a=s.index('## 线程、队列与资源治理');b=s.index('## Source / Compile-Time Dependencies',a)
s=s[:a]+'''## 线程、队列与资源治理

默认候选入口是 `lumio-ds`：单个异步网络 Reactor、单 Owner 逻辑驱动、有界队列与有期限关闭。网络流量不驱动 Tick；线程和队列原语来自 `host-runtime`。生产单调钟不可快进。

旧 Hello、Replay 和免认证 observer 附着仅保留在显式 `test-harness` 构建中，不是默认运行路径。当前部署 profile 为 `runtime-only` 检查点模式，不宣称 ECS/Voxel 世界 WAL 已接通。

启动、凭据、浏览器连接、耐久档位和收尾限制见 [DS 运行手册](eng/DS_RUNBOOK.md)。模块及实际调用路径见 [process 模块](modules/process/README.md)。

'''+s[b:]
s=s.replace('`entity_chat_acceptance`（11 场景）需要 Game 侧根目录，CI 中以 `continue-on-error` 单列。','`entity_chat_acceptance` 是独立的跨仓集成范围，需要固定 Game/SDK/托管制品；模块 CI 不替它宣称通过。')
p.write_text(s)
p=Path('modules/process/src/lib.rs');s=p.read_text();a=s.index('pub mod audit;');s='''//! Lumio dedicated-server infrastructure.
//!
//! `lumio-ds` is the authenticated deployment entry. The historical Hello
//! composition root is compiled only for tests or the explicit test-harness
//! feature. Runtime owns all authoritative entity/gameplay semantics.

'''+s[a:];p.write_text(s)

Path('eng/DS_RUNBOOK.md').write_text('''# LumioServer DS 运行与验收手册

## 构建与运行

```sh
cargo build -p lumio-server-process --release --locked --bin lumio-ds
./target/release/lumio-ds --config /absolute/path/server.json --check-config
./target/release/lumio-ds --config /absolute/path/server.json
```

从 [配置模板](server.example.json) 复制并填写真实 SDK、Runtime、Registry 与平台公钥。模板故意没有可直接使用的签票私钥或测试账号；占位公钥不能通过校验。所有相对文件路径相对于配置文件目录，而不是当前工作目录。

一个进程对应一个可信 allocation 的房间与版本。可信分配上下文必须来自运维/平台配置，不能从客户端 Launch 响应反向修改服务器配置。版本切换采用新进程，旧进程收到 SIGTERM 排空、保存后退出；本 PR 没有实现外部集群控制器。

## 准入与浏览器

默认监听仅 loopback 动态端口，Ready 行给出实际端口。公网必须由 TLS/WSS 边缘代理转发，不能把明文监听端口直接映射公网。边缘要限制连接速率、关闭头部日志，特别是 `Authorization` 和 `Sec-WebSocket-Protocol`。

Native/Bot 通过 HTTP Upgrade 的 `Authorization: Bearer <admissionCredential>` 提交平台 **Launch** 房间票。普通 `/account` 票为 unbound，不可进入 DS。浏览器适配器是 [connect-ds.mjs](connect-ds.mjs)，使用额外的 `lumio-admission.<credential>` subprotocol offer 承载同一张票；服务端只回应 `lumio.mvp.v0`，不回显凭据。这个 Upgrade 适配器不改变 C-1 Gameplay 编解码。应用需要显式接入 helper；本仓 socket/Node 测试不等于完整浏览器 E2E 已通过。

当前消费 `LumioGameEngine@23401e178fdf346a0361b51a1ff881daf4d42554` 的账号票据格式：签名、期限及六个 allocation 维度一起校验。按该版明确的 bearer replay policy，**不新增 nonce 单次消费表**。同票在期限内重复使用不等于一个免认证 connectionId；每个 socket 都重新验证，并获得服务器生成的私有连接身份。账号接管仍由 Runtime 决定。

## 状态与退出

`DS_READY` 只在真实 Runtime boot/restore 后输出。`DS_CHECKPOINT` 代表一次存储发布完成；`DS_DRAINING` 后不再接受新准入或输入；成功最终保存并关闭后输出 `DS_STOPPED`。参数错误退出 3，运行或初始化故障退出 2，正常退出 0。

Owner 请求/结果各有 2 秒期限。Watchdog 不向 Owner 排队，直接读取心跳/故障状态。无法确认是否已执行的超时按故障处理，不自动重试可能有副作用的命令。Native/CLR 挂死不能安全强杀线程，因此独立进程退出是最终处置。

## 耐久与恢复

当前唯一部署 profile 为 `runtime-only`。它定期在 Owner 侧获取 Runtime 快照，再由存储线程写入检查点。检查点发布期间保留旧组，文件校验失败只允许整组回退，不会从不同组拼 Runtime 与 Voxel。

`process-crash`：覆盖进程异常退出，不宣称文件系统/硬件断电保证。`power-loss`：目前要求支持目录 fsync 的 Unix 文件系统，文件及目录同步失败立即返回错误；Windows 不虚报同等级保证。实际硬件、挂载与云盘语义仍需部署验证。

`Journal` 是已测试的有界不透明提交记录存储，有校验链、完整记录验证、不完整尾段处理、单写锁及刷盘回执。**它尚未接到 Runtime/Voxel 的共同提交记录提供方和重放入口，默认 DS 没有假装运行世界 WAL。** 有体素参与者或非零 WAL 水位的检查点在当前 profile 会被拒绝，避免半恢复。

检查点容量 64 MiB、保留最近 3 组；发布错误封锁写端，先恢复/检查存储再重启。不要直接修改 active 组。账号 fixture 的身份和凭据已成组发布，写失败后关闭也不会重新保存失败事务；它不是平台 PostgreSQL 的替代品。

## 验证层级

```sh
python eng/verify.py --profile rust
python eng/verify.py --profile managed
python eng/verify.py --profile integration --inputs /absolute/path/resolved-inputs.json
```

每次生成准确 source SHA、命令、退出码与完整日志。模块检查必须全部通过；缺工具/环境为 BLOCKED_ENV 且非零退出。集成输入必须列出 Server/Engine/Runtime/Game/Platform 的路径与完整 SHA、制品路径与 SHA256，`LUMIO_GAME_ROOT` 必须指向相同 Game。不要把“已有文件”或某轮旧 manifest 当成这次代码已验证。

历史 11 场景仍使用显式 harness；S9 按上游 R-00493 使用短窗口真实等待，不能伪称跑过产品五分钟时长。S1 缺错误口令证据不再补通过值。新 DS 的发布证明还需要平台登录→Launch→实际 Browser/Bot→Native/CoreCLR→Runtime 的全链路考卷。

## 尚未闭合的验收边界

完整 ECS/Voxel WAL 接缝与故障重放；真实 Platform `/account` 和 Launch 替换历史 11 场景；Managed HostEntry 显式 context handle（当前 ABI 仍是单进程单入口，尚有静态托管运行态）；完整慢客户端四级阶梯、外部池滚动更新及目标硬件压测/Soak。它们不能由本 PR 的单元/文件/socket 测试替代。

`account-server/` 只有在真实平台路径满足 ADR 0011 后才能删除。没有伪造该验收、没有修改外部 Game 的验收尺子、没有把本 PR 合并到 main。
''')
Path('eng/server.example.json').write_text('''{
  "allocation": {
    "serverAudience": "replace-server-audience",
    "gameId": "replace-game-id",
    "gameReleaseId": "replace-release-id",
    "contractId": "replace-contract-id",
    "roomId": "replace-room-id",
    "allocationId": "replace-allocation-id"
  },
  "admission_key_id": 1,
  "admission_public_key_hex": "REPLACE_WITH_PLATFORM_32_BYTE_PUBLIC_KEY_HEX",
  "clr": {
    "engine_native": "sdk/liblumio_engine.so",
    "hostfxr": "dotnet/libhostfxr.so",
    "runtime_config": "managed/Lumio.Server.EntityChat.HostEntry.runtimeconfig.json",
    "assembly": "managed/Lumio.Server.EntityChat.HostEntry.dll",
    "entry_type": "Lumio.Server.EntityChat.HostEntry.HostEntry, Lumio.Server.EntityChat.HostEntry",
    "entry_method": "LumioEntityChatEntry",
    "replication_assembly": "managed/Lumio.GameRuntime.Replication.dll",
    "ecs_assembly": "managed/Lumio.GameRuntime.Ecs.dll",
    "registry_assembly": "managed/YourGame.Server.dll"
  },
  "store_path": "state/room",
  "content_fingerprint": "REPLACE_WITH_EXACT_CONTENT_FINGERPRINT",
  "world_profile": "runtime-only",
  "durability": "process-crash",
  "checkpoint_seconds": 30,
  "watchdog_timeout_ms": 2000
}
''')
Path(__file__).unlink()
