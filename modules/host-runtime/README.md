# host-runtime

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
