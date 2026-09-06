# LumioServer DS 运行与验收手册

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
