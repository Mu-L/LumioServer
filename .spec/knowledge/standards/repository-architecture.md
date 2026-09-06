---
name: repository-architecture
description: 仓库边界与 Living Architecture 契约纪律——LumioServer 拥有什么、公共语义从哪里取；改连接、WorldSlot 或维护流程前查
metadata:
  type: doc
  status: 已交付
---

# LumioServer 仓库边界与契约纪律

> 项目治理与验证入口见 [`AGENTS.md`](../../AGENTS.md)、[`knowledge/README.md`](../README.md) 和 [`rules/system.md`](../../rules/system.md)；仓内 crate 现状见 [`modules/README.md`](../../../modules/README.md)。

## 1. 唯一事实源

本仓处于预上线 Living Architecture 阶段，**不保存架构镜像、不冻结基线、不复制公共字段**。

- 公共架构与设计现状：架构仓 `LumioGameEngine` 的 `.spec/knowledge/features/`——整体见 `architecture.md`，本仓设计现状见 `ds-server.md`。
- 可运行接口：同仓 `engine/abi/native-abi.json`（唯一 ABI 真值）与 `engine/wire/<name>-v1.json`（公共语义各一份）。
- 决策：跨仓公共语义的 ADR 只在架构仓维护；本仓 [`decisions/`](../../decisions/README.md) 只记 Server 内部实现决策。

纪律：本仓只消费契约所有者发布的接口。实现、README 或测试**不得反向定义公共语义**，也不得在仓内保存第二份契约真值。公共语义要变，先在架构仓更新对应 ABI/wire 定义与正/失败 fixture，再重编译直接消费者；本仓不私改 Envelope、Release、Capability、FaultClass、状态机或错误码。需要字段口径时直接读架构仓文件，不在本仓抄一份。

## 2. 仓库边界与 LumioServer 所有权

按架构仓 `architecture.md` §2 现状（Native 聚合已并入架构仓 `engine/native/`，不再是独立仓库节点；账号权威已迁入 `LumioPlatform`，ADR-061）：

| 仓库 | 职责 | LumioServer 不得接管 |
| --- | --- | --- |
| `LumioGameEngine` | SDK 组装、Native 聚合、ABI/Binding、共享 Loader、集成验证与 SDK 产物 | 全部——本仓只消费 |
| `LumioNativeCore` | 领域无关 Rust Kernel、Handle、Error、Capability、内存、Job、空间基础 | Voxel、ECS、Gameplay、网络、Host |
| `LumioVoxelEngine` | VoxelWorld、Section / Chunk、Block 编码与目录、Revision、批量读、Mutation、Streaming、Snapshot、Voxel Migration 与体素派生计算 | Socket、Session、ECS Storage、Host 生命周期 |
| `LumioGameRuntime` | ECS、Tick、Coordinator、Replication、GAS、Persistence、Config、Determinism | 进程、Socket、端口、Voxel 内部、具体玩法 |
| **`LumioServer`（本仓）** | Server Host、网络、Session、WorldSlot、CoreCLR Hosting、维护与升级编排、`verify_admission` 离线验票 | — |
| `LumioClient` | Client Connection、Replica、Prediction、Unity/HybridCLR Adapter、Headless Bot | Server 权威、Runtime 组件定义、Native 聚合 |
| `LumioGame` | Gameplay、Mapping、配置、内容、Scenario、Migration、Server/Client 组合 | Runtime/Host 生命周期、通用 ABI、Voxel 内部 |
| `LumioPlatform` | 唯一账号权威（注册 / 登录 / 口令哈希 / 准入凭证签发 / Bot 命名空间设防）、游戏目录与大厅、launch 端口与房间分配器接口 | — |

**本仓拥有**：进程、连接与 Endpoint、认证与接纳、每进程 Release 身份、本进程 Pool member/health、`WorldSlotHost` 聚合根、Host Wall Clock/Pacing、CoreCLR Hosting、本地 Snapshot/WAL 编排、维护代理与可观测性。
**本仓不拥有**：Runtime 语义、ECS/Voxel 权威数据、Logical Tick Phase、Gameplay 规则、集群 desired state（归外部控制面），以及账号权威（归 `LumioPlatform`）。本仓只保存 Runtime/Voxel 的不透明句柄，不复制或修改 `GameWorld`、`VoxelWorld`、ECS、GAS、Chunk 或 Gameplay 状态。

## 3. 仓内现状

workspace 只有三个成员：`crates/lumio-host-testkit`（dev-only）、`modules/host-runtime`、`modules/process`；另有 `entity-chat-host/`（切片级 CoreCLR 托管入口）与 `account-server/`（独立 C# 进程，其账号权威地位已被 `LumioPlatform` 取代）。职责、线程与有界队列现状见 [`modules/README.md`](../../../modules/README.md)。

`process` 是唯一 Composition Root，可在组装期知道全部具体模块并接线；这不使它获得跨模块业务所有权。禁止创建 `common`、`globals`、`event_bus` 或同义的上帝 crate/file。

## 4. 并发与队列纪律

- 并发原语全部来自 `host-runtime`：线程经 `spawn_supervised` 受监督创建并配 `CancelToken`，panic 以 `TaskPanicked` 上报。生产代码不得直接 `std::thread::spawn`、`tokio::spawn`、`sleep`、轮询或构造无界 channel。
- 队列只有 `bounded_channel` 一种形态，满载与关闭是显式错误（`SendError::Full` / `SendError::Closed`）；满载必须拒绝、降级或升级故障，不得无界增长或覆盖权威数据。
- 单调时间只从 `HostClock` 取，定时经 `KernelTimer` / `HostTimer` 与 `NativeAbiKernel`；Timer 不是业务回调注册。
- Reactor、IO、Native worker 和平台回调只写入有界队列，不得直接调用 Gameplay、C# 或修改 World；权威状态只在 Runtime Tick Barrier 提交。
- `FaultClass` 路径固定为 Runtime witness → 托管入口原样转交 → Host 裁决；可捕获异常、Rust panic、网络/磁盘错误都不能自行推断故障域，缺 witness 按最保守档处理。
- `ServerConnectionSession` 是 Host 私有的每连接记录，禁止命名或建模为 `ClientReplicaSession`；客户端 World 不是 Server WorldSlot 的物理对象。

## 5. 变更与审查红线

- 跨线程 / 跨进程 / 跨语言 / 跨 World 的 effect 必须是 typed command/event + bounded port + 显式 ack/correlation；禁止 closure callback registry、全局 EventBus、Service Locator 和共享可变 registry。
- 任何新线程、Timer 或 async task 先接入 `host-runtime` 监督；任何公共语义改动先回架构仓改 ABI/wire 定义与 fixture。
- 生成物不得手改；密钥、credential、signature 原文不得入库、进日志、进 fixture 或进任务卡。
- 交付前至少运行 `node .spec/tools/spec-lint.mjs && node --test .spec/tools/spec-lint.test.mjs` 与适用的 Cargo 门，并在证据中记录实际退出码与输出摘要。没有新鲜证据不得声称完成。
