# host-runtime 模块设计与实现

## 模块定位与目标

`host-runtime` 把“时间从哪里读、定时任务在哪个线程唤醒、异步任务 panic 之后谁知道、取消如何级联”收拢为一个最底层原语模块。设立它的原因是：重连窗口、健康检查、防重放窗口、Checkpoint 调度、Watchdog、维护 deadline 等定时语义如果散落各模块，会催生多种隐式线程与隐式时间语义（架构门审 P1-01 与 ADR 0001/0009/0010）。本模块不含任何业务语义，是编译依赖图的最底层；其他模块只做时间与任务的**消费方**。

## 负责什么

- **单调时钟**：进程唯一的单调时间源；墙钟只用于日志时间戳与外部协议字段，deadline 一律以单调时钟表达（架构源 ADR-012 与 ADR-057 的宿主侧支撑）。
- **NativeCore 定时适配**：适配 NativeCore ABI，为宿主提供内核定时驱动（`NativeAbiKernel`、`HostTimer`），分发 `wallClock` 与 `tickFrame` 定时。
- **取消机制（Condvar 唤醒）**：`CancelToken` 提供可被条件变量立即唤醒的取消等待（`wait_timeout`），支持阻塞适配器的安全取消与唤醒。
- **任务监督与故障锁存**：`spawn_supervised` 启动受监督任务（`SupervisedTask`）——panic 被捕获、转为稳定的 `TaskPanicked` 并锁存；调用方可经 `failure()` 在不消费 Join 的前提下主动感知故障；提供有界 Join（`join_timeout`），超时保留句柄供重试或进程级处置。
- **有界传输原语**：`bounded_channel` 提供严格有界的 MPSC 管道，明确区分 `Full(value)` 与 `Closed(value)` 且返回原值；支持带超时的发送 `send_timeout` 与容量唤醒通知。
- **确定性测试时钟**：测试 Profile 下以确定性时钟（`TestMonotonicClock`）替换单调时钟源，生产环境强制真实单调时钟（`SystemMonotonicClock`）且不可被推快。
- **唯一跨平台 Native 加载器**：收敛动态库加载与 `RootApiV1` 根表绑定（ADR 0010），杜绝平台差异分支。

## 明确不负责什么

- 不拥有 TickRate 与 Tick 触发判定（归 `pacing` 或宿主调度器）；宿主是本模块时钟的消费方。
- 不拥有任何业务定时语义：重连窗口由业务宿主持有，Checkpoint 调度由持久化模块持有——本模块只提供机械事实与定时接口。
- 不定义业务队列的容量与满载策略数值（由各业务模块与配置声明）。
- 不做日志/事件管道；监督事件由调用者经类型化接口获取与处置。

## 拥有的状态与资源

- 单调时钟源（及测试下的确定性替身）。
- 受监督任务状态（线程句柄、完成标记、锁存的 `failure`）。
- 有界通道的内部同步与条件变量唤醒状态。

## 输入、输出与稳定接口

- **输入**：取消请求、受监督闭包、通道发送数据。
- **输出**：`TaskPanicked` 监督故障、通道接收数据、单调时间读数。
- **稳定接口**：
  - `clock`：`SystemMonotonicClock::now()`、`SharedClock`
  - `channel`：`bounded_channel(capacity) -> (Sender<T>, Receiver<T>)`；`Sender::send_timeout(value, timeout)`、`Sender::try_send(value)`、`Sender::send(value)`；`Receiver::recv()`、`Receiver::try_recv()`
  - `supervisor`：`spawn_supervised(name, task) -> SupervisedTask`；`SupervisedTask::cancel()`、`SupervisedTask::is_finished()`、`SupervisedTask::failure() -> Option<TaskPanicked>`、`SupervisedTask::join() -> Option<TaskPanicked>`、`SupervisedTask::join_timeout(timeout) -> Result<Option<TaskPanicked>, TaskJoinTimedOut>`
  - `supervisor`：`CancelToken::new()`、`CancelToken::cancel()`、`CancelToken::is_cancelled()`、`CancelToken::wait_timeout(timeout) -> bool`
  - `native_abi`：`NativeLibrary::load(path)`、`RootApiV1`、`NativeAbiKernel`

## 上游与下游依赖

- **上游**：全部拥有定时/异步/Native 加载语义的模块（`process`、`lumio-ds`、`entity_chat`）。
- **下游**：无。本模块是编译依赖最底层。

## 线程、队列与并发所有权

- 任务线程具名且受监督。
- 取消令牌采用原子状态与条件变量（Condvar），保证取消信号能够即时唤醒处于睡眠或等待状态的线程。
- 通道满载时阻塞或超时返回，接收端关闭立即通知并唤醒发送端，避免发送者死锁。

## 正常数据流与失败路径

- **正常**：线程由 `spawn_supervised` 启动 → 周期执行或监听事件 → 收到 `cancel` 退出 → `join_timeout` 成功收集。
- **失败路径**：
  - 线程 panic：panic 被捕获，状态置为 done，锁存 `TaskPanicked`；`failure()` 可立即返回，避免静默死亡。
  - Join 超时：`join_timeout` 到期返回 `Err(TaskJoinTimedOut)`，句柄保留在任务对象中，上层可决定重试或将整个进程退出。
  - 通道满：`send_timeout` 耗尽等待时间后返回 `Err(SendError::Full(value))`，保留未送达对象。

## 错误分类、恢复与降级

- **可重试**：通道满超时后调用方按策略重试；Join 超时后重试。
- **可致命**：Native/CLR 挂死或受监督核心线程 panic 导致宿主无法继续提供服务——进程级退出是最终处置。

## 测试面、故障矩阵与性能指标

- **测试面**：通道容量边界、满载唤醒、接收端析构通知、取消即时唤醒、panic 捕获与锁存、有界 Join 超时保留句柄。
- **故障矩阵**：通道满投递失败、取消与等待并发、线程内部 panic。
- **性能指标**：微秒级时钟开销、高并发通道吞吐、纳秒级条件变量唤醒。

## 对应 ADR、Schema 与 Fixture

- ADR 0001（边界收敛与 host-runtime 新设）
- ADR 0003（五分钟重连窗由 Host Timer 持有）
- ADR 0007（Rust 宿主纯消费 Runtime 与 NativeCore 定时）
- ADR 0010（唯一跨平台 Native 加载器）

## 尚未批准的决策门

- **SRV-D-012**（执行器与 Timer 模型：专用具名线程 vs 共享执行器、timer wheel 精度、panic 重启策略）：临时默认值为每所有者专用具名线程 + NativeCore 定时内核 + 不隐式重启；按线程数与调度开销测量确认。
