---
name: rust-entity-chat-host
description: Rust DS 实施现状——认证入口、Owner Tick、有界传输、Runtime 桥、检查点与显式测试切片
metadata:
  type: doc
  status: 实施中
---

# Rust DS 宿主与 entity-chat 测试切片

当前默认候选入口为 `modules/process/src/ds.rs` 的 `lumio-ds`。`entity_chat` 中的 Owner、Runtime Adapter 和网线由此消费；旧 Hello 与 11 场景启动器只在显式 `test-harness` 构建中出现。

## 状态与边界

Rust Host 不创建 ECS Storage，不另立绑定/发号/属性真值，不自己编码 Gameplay WorldChange。Runtime 负责实体、查询、复制与规范快照；NativeCore 负责定时 due 判定；Host 负责调度、准入、字节、资源和存储执行。

生产时钟只读真实单调时间，保留 R-00493 的不可快进保证。测试通过 `SharedClock::advance_test_clock` 明确请求确定性时钟推进，真实时钟会拒绝。11 场景 S9 使用短窗口并真实等待，不声称验证过五分钟产品时长。

## 现役路径

`lumio-ds` 显式读取配置与制品路径 → 校验 allocation/public key/profile → 打开单写检查点存储 → 启动 Native/CLR → Owner boot/restore → 发布 Ready → 认证 socket → Owner 队列 → Runtime Tick → Runtime 输出 → 有界发送。

凭据必须是平台 Launch 签发且与六个可信 allocation 维度一致的房间票；普通账号票不允许入房。Socket 连接号由服务端创建，不接受客户端用已知 connectionId 取得既有会话。浏览器 Upgrade 适配不把凭据放入 URL，也不在服务端回显。

网络输入永不推进额外 Tick。同连接 FIFO、活动房间无连接时的继续模拟、积压输出顺序均有回归测试。当前输入批上限仍是历史 Runtime 切片预算，不意味着已完成通用 Gameplay 配额协议。

一个异步 reactor 管理受限 socket 任务，不再逐连接创建 OS 线程。网络与 Owner 队列有界；握手、写入、外部 Owner 调用及 Join 有期限。Owner 故障/心跳可从进程侧读取，Native/CLR 不可终止的线程由最终进程退出隔离。

## 持久化

`CheckpointStore` 成组发布并校验 Runtime/可选 Voxel 材料；`Journal` 存储调用方提供的不透明已提交记录。默认 DS **只接通 runtime-only 检查点模式**，不谎称 World WAL 已接上，不恢复需要 Voxel/WAL 提供方的半份材料。

账号 fixture 的身份与凭据成组发布且失败封锁，仍需完成真实平台路径验收才能按 ADR 0011 退役。

## 测试路径

`test-harness` 才允许旧 connectionId observer、测试签票、Playwright/Bot/Account 进程启动器以及 Hello/Replay。它们不能作为默认 DS 的准入证明。

模块验证入口为 `eng/verify.py --profile rust|managed`。跨仓 `integration` 使用固定仓库与实际制品清单，缺输入非零退出。证据缺失、输出写失败、未运行均不得补成通过值；外部 Game 原验收尺子未修改。

## 未闭合部分

完整 Runtime/Voxel 共同提交记录与 WAL 重放；Platform 登录/Launch 替换历史考卷；托管入口显式 context handle；完整慢客户端四级调度；外部池生命周期与目标硬件性能/Soak。详见 `eng/PR38_HANDOFF.md`，不得把模块测试通过写成这些能力全部通过。

实现详情和实际命令集中在 [process README](../../../modules/process/README.md)、[运行手册](../../../eng/DS_RUNBOOK.md) 和 [PR38 交接](../../../eng/PR38_HANDOFF.md)。
