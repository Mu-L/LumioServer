# process 模块

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

`CheckpointStore` 已实现成组文件发布，不生成 ECS/Voxel 变更集。当前默认入口接入的是 **Runtime 检查点模式**。完整 WAL 重放、ECS/Voxel 同切点提供方、真实游戏迁移/长压测仍需跨仓闭环。

账号 fixture 使用独立的成组文件事务与失败封锁；账号权威仍应归 LumioPlatform。退役条件遵循本仓 ADR 0011，不能在平台登录/Launch 与原验收尺子尚未全部打通时删目录。

## 验证

```sh
python eng/verify.py --profile rust
python eng/verify.py --profile managed
python eng/verify.py --profile integration --inputs /absolute/path/resolved-inputs.json
```

前两个是模块验证，不要求外部游戏工作区。第三个要求固定代码与制品清单，缺环境非零退出；通过模块检查不能声称真实跨仓集成完成。详细运行与收尾见 [运行手册](../../eng/DS_RUNBOOK.md)。
