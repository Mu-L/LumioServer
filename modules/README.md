# LumioServer 模块

> **公共契约来源**：`LumioGameEngine` 的 `engine/abi/native-abi.json` 与 `engine/wire/*.json`，设计现状见其
> `.spec/knowledge/features/architecture.md` 与 `ds-server.md`。本仓不保存架构镜像、不复述公共契约字段。
> **本文定位**：只描述本仓现有 crate 的职责、线程与有界队列现状。没有实现的东西不写在这里。

## 现有 crate

| crate | 路径 | 职责 |
| --- | --- | --- |
| `lumio-host-runtime` | [`modules/host-runtime`](host-runtime) | 宿主运行时原语：单调时钟、有界 MPSC channel、受监督线程、`NativeCore` 定时器 ABI 适配 |
| `lumio-server-process` | [`modules/process`](process) | 服务器进程组合根：WebSocket 监听与会话准入、SDK DLL 校验、CoreCLR 运行时桥、权威 tick 路由、NDJSON 审计、entity-chat 切片 |
| `lumio-host-testkit` | [`../crates/lumio-host-testkit`](../crates/lumio-host-testkit) | **dev-only** 确定性测试支撑：测试时钟、故障计划、fixture 加载、有界端口探针。生产 crate 只能经 dev-dependency 或测试目标引用 |

依赖方向单向：`process` → `host-runtime`；`testkit` 只被测试目标引用，不进生产依赖。

## 线程与有界队列现状

- **有界是唯一形态**：`host-runtime` 只导出 `bounded_channel`，没有无界路径。发送失败区分 `SendError::Full`（容量耗尽，值原样返还）与 `SendError::Closed`（接收端已丢弃）；接收失败区分 `RecvError::Empty` 与 `RecvError::Closed`。背压由调用方显式处理，不得静默丢弃或无限缓冲。
- **线程受监督**：并发一律经 `spawn_supervised` 拿到 `SupervisedTask`，配 `CancelToken` 协作式取消；线程 panic 被捕获并以 `TaskPanicked` 上报，不静默吞掉。生产代码不直接 `std::thread::spawn`。
- **时间经时钟抽象**：单调时间只从 `HostClock` 取（生产 `SystemMonotonicClock`，测试 `TestMonotonicClock`）；定时器经 `KernelTimer` / `HostTimer`，`NativeAbiKernel` 把引擎 `NativeCore` 定时 ABI 适配进来。生产代码不直接 `sleep` 或轮询。
- **进程生命周期**：`process` 的启动序列是 contract → audit → SDK → CLR host → listener，关闭序列是 sessions closed → bridge shutdown → CLR destroyed → audit flushed。退出码 0 正常关闭、1 初始化失败、2 运行期致命错误、3 参数错误。

## 测试面

`modules/process/tests/` 下 `entity_chat_architecture`、`entity_chat_host`、`entity_chat_wire` 是必过面，
`entity_chat_acceptance`（11 场景）需要 Game 侧根目录，CI 中以 `continue-on-error` 单列。
