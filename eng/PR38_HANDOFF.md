# PR #38 集中修复交接

仓库：LumioGames/LumioServer。分支：`fix/server-review-correctness-20260906`。本次没有合并 main、没有部署生产、没有迁移或删除用户数据。

原审查基线 `ce34ac75303f4746c48ba74b706cff6189b038c5`；实施中合并并保留了 main 的 R-00493 / PR #39（`8c3883d42759b384742c6017b358ad824cb2f1e6`）生产时钟与 S1/S9 修复。请按本 PR 完整 diff 审查，不要回到第一批三文件补丁。

## 结论口径

这是一份已经实施并经过模块验证的集中修复，不是“完整商业 DS 全部验收完成”的证明。单仓明确 bug、默认认证入口、资源治理、检查点底座与验证可信性已有代码；需要真实上游 Runtime/Voxel/Platform 的部分保留为明确未闭合项，不用假实现或删除验收标准掩盖。

## 原审查项逐项对应

| 发现 | 本 PR 落点 | 当前结论 |
|---|---|---|
| F01 CI 总绿不代表完整链路 | `eng/verify.py`、两平台模块 CI、独立 pinned-integration workflow | 模块/集成分离；缺环境非零退出；真实跨仓集成尚未通过 |
| F02 缺证据补通过值 | `entity_chat/suite.rs`，证据缺失/写失败回归 | 已修具体假绿与写失败路径；保留上游 S1/S9 真实观察修复 |
| F03 socket 可冒用 connectionId | `secure.rs`、`wire.rs`、认证构造器、真实 socket 测试 | 默认路径每 socket 验票、服务端发连接身份；旧 observer 仅 fixture |
| F04 未认证资源失控 | 单 reactor、任务上限、握手期限、输入尺寸/发送队列、速率上限 | 主要资源边界已实现；公网 TLS、边缘限流与容量压测不是本地测试结论 |
| F05 输入驱动逻辑帧 | `host.rs` 与两组新回归 | 已删除输入触发 Tick，保留 Owner 调度 |
| F06 同连接输入逆序 | 稳定批次选择/FIFO、拒绝计数、致命封锁 | 已修并有确定性回归 |
| F07 无连接后房间停算 | 有界活动房间集合、空连接调度测试 | 已修，不把 socket 数量当房间生命周期 |
| F08 时间固定/重放政策 | 不可快进生产钟、当前绑定票据验证器 | 验票时间推进已修；按当前上游 bearer policy 不引入单次 nonce 消费 |
| F09 监督与取消不可靠 | Condvar 取消唤醒、deadline channel、故障锁存、bounded join、进程 watchdog | 基础闭环已实施；真实 Native/CLR 卡死和部署恢复演练仍需集成 |
| F10 完整慢客户端调度 | 非阻塞发送、积压 FIFO、有界队列/写期限 | 局部修复；revision 深度四级阶梯与通用字节预算未完整接通 |
| F11 快照不等于耐久世界 | `persistence.rs` 成组检查点、校验 Journal、`lumio-ds` 定期/退出保存 | runtime-only 检查点已接入；完整 ECS/Voxel WAL 提供方与重放未接通 |
| F12 账号跨文件不一致 | 成组存储、单写租约、失败封锁、35 项账号测试 | 具体一致性修复已做；Platform 真实路径替换/旧目录退役未完成 |
| F13 托管反射/静态状态 | 解码委托启动缓存、明确加载失败、恢复不复制旧连接 | 热路径与恢复已修；静态 context 和完整生成绑定尚未退出 |
| F14 双入口与旧文档 | 默认 `lumio-ds`，旧 run/Hello/Replay/启动器显式 test-harness，README 重写 | 默认入口及测试边界已收敛；部分聊天预算命名仍是切片耦合 |

## 已实际获得的验证

Linux GitHub runner 对 `2121b1b948e29034ae3a06fb7e332ba3289c7413` 完成以下检查，全部通过，运行号 `34034705490`：

- `cargo fmt --all -- --check`、默认 workspace check、all-targets/all-features Clippy `-D warnings`。
- Rust：Host Runtime 24、Testkit 10、Process 库 146、DS 配置 2、架构 50、Host 47、Wire 15、Native loader 架构 3、认证 socket 4，共 301 项；这些不是 301 项真实引擎 E2E。
- Node：浏览器连接适配 3、spec-lint 测试 13。
- C#：实际 HostEntry Release 构建、账号应用构建、35 项账号测试。

最终源代码 SHA 与 Windows/Linux 最后结果以 CI artifact 的 `source-sha.txt`、各 `*-result.json` 和日志为准。本说明不预先把还未完成的 Windows 或集成任务写成通过。

临时修复 workbench 和一次性脚本在交付树中删除，永久工作流只需 read 权限，不在验证时修改分支。最终 CI 打包精确源代码与相对 PR base 的补丁。

## 本地验收命令

```sh
git fetch origin fix/server-review-correctness-20260906
git switch --track origin/fix/server-review-correctness-20260906
python eng/verify.py --profile rust
python eng/verify.py --profile managed
cargo build -p lumio-server-process --release --locked --bin lumio-ds
```

已有本地分支时不要重复 `switch --track`，直接切换并核对远端 SHA。配置和进程启动见 [DS_RUNBOOK.md](DS_RUNBOOK.md)。源码包是完整源码，不是预编译、签名或可直接上线的发行包。

跨仓考卷：

```sh
python eng/verify.py --profile integration --inputs /absolute/path/resolved-inputs.json
```

需要固定 Server/Engine/Runtime/Game/Platform 代码与真实制品清单；未准备输入会明确失败。提供的手动 workflow 使用 `lumio-integration` 标签的自有 runner，**未创建或声称已有该 runner**。CLI 可在你自己的既有集成工作区运行。

## 接手 Agent 的验收提示词

对 PR #38 的实际最新 head 做独立审查，先读 `eng/DS_RUNBOOK.md` 和本文件。不得把测试双替代的 Runtime 当真实引擎。保持 main 上 R-00493 的真实时钟不可快进，不恢复旧自动 Tick 测试，不修改 Game 验收尺子或补默认通过值。

先跑 rust/managed 两个 profile，记录源 SHA、每个命令与失败日志。用真实 SDK/CoreCLR/Runtime 和平台 Launch 接入新 `lumio-ds`；测试合法准入、错误分配/过期票拒绝、浏览器 Upgrade 凭据不泄露、正常停服与检查点恢复。然后按表中未闭合项核对上游真实端口，优先完成 ECS/Voxel 同切点 WAL、平台路径替换、Managed context 和慢客户端策略。任何缺失都写成具体未通过项，不能用空实现、跳过或自动刷新期望值收口。只有真实平台账号语义满足 ADR 0011 后才删除 account-server。
