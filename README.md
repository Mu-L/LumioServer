# LumioServer

> Rust Dedicated Server Host、网络基础设施、Release Pool、WorldSlot 和服务器运维生命周期。

<!-- lumio-community:start -->
<div align="center">
<table>
<tr>
<td align="center" width="50%" valign="top">
<a href="https://qm.qq.com/q/PGkXh4tCyQ"><img src="https://raw.githubusercontent.com/LumioGames/.github/main/profile/assets/qr-qq.svg" width="170" alt="QQ 交流群 972220164"></a><br>
<a href="https://qm.qq.com/q/PGkXh4tCyQ"><img src="https://img.shields.io/badge/QQ%20%E4%BA%A4%E6%B5%81%E7%BE%A4-972220164-6171F0?style=for-the-badge&logo=tencentqq&logoColor=white" alt="QQ 交流群 972220164"></a><br>
<sub>什么都能聊</sub>
</td>
<td align="center" width="50%" valign="top">
<a href="https://applink.feishu.cn/client/chat/chatter/add_by_link?link_token=fffn1ae7-fd83-4315-96ac-6fa3aba3968e"><img src="https://raw.githubusercontent.com/LumioGames/.github/main/profile/assets/qr-engine.svg" width="170" alt="LumioEngine 开发者社区"></a><br>
<a href="https://applink.feishu.cn/client/chat/chatter/add_by_link?link_token=fffn1ae7-fd83-4315-96ac-6fa3aba3968e"><img src="https://img.shields.io/badge/%E9%A3%9E%E4%B9%A6%E7%BE%A4-LumioEngine%20%E5%BC%80%E5%8F%91%E8%80%85%E7%A4%BE%E5%8C%BA-5DE2C6?style=for-the-badge&logoColor=1E2A3A" alt="LumioEngine 开发者社区"></a><br>
<sub>飞书话题群 · Rust / C# 引擎层</sub>
</td>
</tr>
</table>
<sub>先进群再看代码。其它群和整体介绍见 <a href="https://github.com/LumioGames">LumioGames 主页</a>。</sub>
</div>
<!-- lumio-community:end -->

## 架构与开发说明

本仓处于预上线 Living Architecture 阶段，不发布也不复制冻结基线。跨仓边界与公共语义的唯一来源是架构仓
`LumioGameEngine`：整体架构见 `.spec/knowledge/features/architecture.md`，本仓的设计现状见
`.spec/knowledge/features/ds-server.md`，可运行接口是 `engine/abi/native-abi.json` 与
`engine/wire/*.json`。本仓不保存架构镜像，也不在本文复述任何公共契约字段——需要字段口径请直接读上述文件。

`LumioServer` 拥有服务器进程、连接、Release 身份代理、WorldSlot 聚合根、Host Pacing、CoreCLR Hosting、滚动更新与强制维护的本进程侧执行。集群期望状态（Pool 存在性、Release 指派、实例替换时机）归外部控制面（架构源 ADR-012）。它加载稳定 Runtime 与 Server Gameplay，但不拥有 ECS/Voxel 内部状态，也不定义 Gameplay 语义。

## 拥有的状态与生命周期

- 进程、监听 Endpoint、认证、Connection、Session Admission、重连窗口、限流和背压。
- `WorldSlot` 句柄、生命周期 epoch、Quiesce 序列、资源配额、Watchdog 与 Crash Recovery 编排。
- CoreCLR、稳定 Runtime、Server Gameplay Assembly 的启动、激活、重载和关闭流程。
- Host Wall Clock（单调时钟归 `host-runtime`）、Tick pacing、Ingress/Egress 队列和运维状态。

Runtime 拥有 Logical Tick、GameWorld 和 Coordinator；VoxelEngine 拥有 VoxelWorld；Server 保存句柄、Context、Snapshot 元数据和编排状态，不直接访问内部 Storage。

## 子模块

模块职责、依赖方向与线程/队列现状见 [modules/README.md](modules/README.md)。下表只列仓内实际存在的目录。

| 目录 | 责任 |
| --- | --- |
| [`modules/process`](modules/process) | 服务器进程组合根：WebSocket 监听与会话准入、SDK DLL 校验、CoreCLR 运行时桥、权威 tick 路由、NDJSON 审计、entity-chat 切片 |
| [`modules/host-runtime`](modules/host-runtime) | 宿主运行时原语：单调时钟、有界 MPSC channel、受监督线程、全仓唯一的 Native SDK 加载器与根表、`NativeCore` 定时器 ABI 适配 |
| [`crates/lumio-host-testkit`](crates/lumio-host-testkit) | dev-only 确定性测试支撑：测试时钟、故障计划、fixture 加载、有界端口探针 |
| [`entity-chat-host`](entity-chat-host/README.md) | 切片级 CoreCLR 托管入口，暴露 `boot` / `enqueue` / `tick` / `drain` / `snapshot` / `restore` |
| [`account-server`](account-server/README.md) | 独立 C# 进程，实现 `lumio.account-port.v1` 的 login-or-register、AccountEntity 与准入凭证签发 |

## 职责

- 启动配置、Endpoint、WorldSlot、健康检查、资源预算、Watchdog、日志和 Metrics。
- 收包、Envelope/Release/权限校验、可靠/不可靠通道、Ack、重传、分片、认证、防重放和背压。
- 将网络/IO/Native Completion 通过有界 Queue/Batch 交给 Runtime Tick，网络线程不得调用 Gameplay。
- 统一加载一个 Native 引擎包、托管 Runtime 和 Server Gameplay ALC。
- 驱动 Host Wall Clock；在 Runtime 规定的 Phase 入口调用逻辑 Tick。
- 编排 Release 路由、Session 排空、强制维护、Snapshot/WAL 落盘和恢复。

## 明确不负责什么

- 不决定技能、物品、战斗、建筑、经济、任务或其他 Gameplay 语义。
- 不创建、销毁或直接访问 ECS Storage；只调用 Runtime 公开 API。
- 不实现 Voxel Chunk/Mutation 内部逻辑，不加载第二套 Native 包。
- 不拥有 Logical Tick Phase、Replication Mapping、Client Prediction 机制或 Game Content。
- 不定义公共协议字段，也不在本仓保存第二份契约真值。
- 不在网络线程直接调用 Hot Gameplay，不把第三方网络类型写入稳定契约。

## 线程、队列与资源治理

默认候选入口是 `lumio-ds`：单个异步网络 Reactor、单 Owner 逻辑驱动、有界队列与有期限关闭。网络流量不驱动 Tick；线程和队列原语来自 `host-runtime`。生产单调钟不可快进。

旧 Hello、Replay 和免认证 observer 附着仅保留在显式 `test-harness` 构建中，不是默认运行路径。当前部署 profile 为 `runtime-only` 检查点模式，不宣称 ECS/Voxel 世界 WAL 已接通。

启动、凭据、浏览器连接、耐久档位和收尾限制见 [DS 运行手册](eng/DS_RUNBOOK.md)。模块及实际调用路径见 [process 模块](modules/process/README.md)。

## Source / Compile-Time Dependencies

- Rust toolchain、网络/IO/日志基础 crates 和平台 SDK。
- 架构仓 `LumioGameEngine` 的 Native 引擎包与 `engine/abi/native-abi.json` ABI 绑定。
- `LumioGameRuntime` 稳定 Managed Host；不编译依赖 Client 或 Game 实现源码。
- Release/Gameplay Payload 只通过架构仓 `engine/wire/*.json` 描述的版本化接口消费。

## Headless Test Surface

- DS 启停、Admission、握手、重连、Session/WorldSlot、Tick pacing、Watchdog。
- Wire 帧编解码、可靠性、Ack、限流、背压、认证、防重放和网络故障注入。
- entity-chat 切片：Runtime 绑定与查询、快照/恢复、NativeCore 定时、Room 网线广播。
- Account Server login-or-register、账号库跨重启 AccountId 稳定、准入凭证解形/验签。

仓内测试面见 [modules/README.md](modules/README.md)「测试面」。

## 开源优先与供应链

优先采用成熟开源的 Reactor、TLS、日志、配置、序列化、指标和进程治理框架。依赖通过 Adapter 隔离，锁定版本/Commit，检查许可证、漏洞、SBOM、AOT、确定性和性能；默认优先宽松许可证。

## 开发规范

- Host 只负责时钟、进程、连接和编排；权威状态变更必须进入 Runtime Tick Barrier。
- 网络回调只入有界队列，错误映射为可重试、可拒绝、可致命三类。
- 跨模块协作走类型化命令 + 显式 ack，禁止任意回调注册与共享可变状态。
- 升级不覆盖旧 Release/Snapshot；所有操作可审计、可恢复、可回放。
- 资源配额、队列和维护超时必须有 Metrics 与故障测试。
- 开发规程见 [`.spec/AGENTS.md`](.spec/AGENTS.md) 与 [`.spec/knowledge/`](.spec/knowledge/README.md)。

## 当前阶段与开发节奏

1. **entity-chat 切片**（当前）：Rust 宿主经 CoreCLR 消费 Runtime 的绑定/查询/快照，跑通 Room 网线与准入。
2. **Foundation 补齐**：进程、Reactor、WorldSlot 单实例与有界队列的生产化。
3. **Vertical Slice**：接入 Voxel 与 Game，跑通 Snapshot/WAL、Release 拒绝和 Replay。
4. **Production Hardening**：Release Pool 滚动更新、强制维护、Crash Recovery、Soak 与人数基线。
