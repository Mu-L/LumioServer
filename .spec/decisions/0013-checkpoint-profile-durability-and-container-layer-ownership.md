# 0013 · 检查点 profile、耐久档与「容器层归 Server / 内容层归 Runtime」归属分界

- 日期:2026-09-06
- 状态:生效

## 背景

Server 进程需要支持运行态快照与故障恢复。此前宿主与 Runtime 的快照职责边界不清，曾出现宿主直接操作内部 ECS/Voxel 文件结构、甚至试图在 Server 侧引入未集成的 Journal 记录与双文件成组的偏离（见 ADR-061、ADR-067 与复核审计）。

## 决策

1. **容器层与内容层的严格分界**：
   - 遵从架构仓公共契约 `engine/wire/persistence-container-v1.json` 与 ADR-067。
   - **容器层归 Server 宿主**：Server 拥有外层持久化容器的目录管理、成组原子发布（draft 目录 → `checkpoint-<id>` 目录 → `active-checkpoint` 指针）、原子重命名、元数据清单校验、fsync 刷盘与故障封锁。
   - **内容层归 Runtime**：世界内部具体状态（ECS Component / Voxel Chunk / WAL）的数据布局与序列化完全归 Runtime 拥有，Server 仅透传 `ReadOnlyMemory<byte>` 载荷，不拆包解析其内部数据。
2. **部署 Profile 与耐久保证**：
   - 当前唯一部署 profile 为 `runtime-only`。定期由 Owner 线程触发 Runtime 快照，再由受监督的存储后台线程写入检查点。
   - 耐久档（Durability Level）：支持 `process-crash`（覆盖进程异常退出的文件原子性与回滚）与 Unix 文件系统上的 `power-loss`（显式调用目录与文件的 `fsync` 刷盘）。在不支持目录 fsync 的环境下不虚标 `power-loss` 保证。
3. **彻底删除未经集成的 Server 侧 Journal**：
   - Server 宿主不单独建第二份 WAL/Journal 机制（删除未使用的 Journal/JournalRecord/DurableReceipt），世界状态的 WAL 机制归架构仓与 Runtime 契约统筹。

## 后果

- Server 存储代码（`persistence.rs`）极度收敛，只关注容器层成组发布的原子性与刷盘可靠性。
- 去除了冗余的伪 WAL 实现，消除了与架构仓统一持久化规范的冲突。
