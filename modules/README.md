# LumioServer 模块

公共架构与接口唯一来源仍是 `LumioGameEngine` 的现行 `engine/abi`、`engine/wire` 与 `.spec/knowledge/features/`。此处只描述实际代码，不重建已删除的骨架。

| crate | 依赖和职责 |
|---|---|
| [host-runtime](host-runtime/README.md) | 单调时间、有界通道、监督线程、Native 加载与定时适配 |
| [process](process/README.md) | 默认 `lumio-ds`、认证传输、Owner、托管桥、存储 I/O 和进程关闭 |
| [lumio-host-testkit](../crates/lumio-host-testkit) | 测试原语，不能作为生产状态真值 |

依赖方向为 `process → host-runtime`。`test-harness` 构建才包含 Hello、Replay、账号/浏览器/Bot 启动器和旧 connectionId observer 附着；默认 `lumio-ds` 始终使用认证构造器。

## 测试面

`eng/verify.py --profile rust` 验证默认构建、显式测试构建、Host/传输/Native 加载结构/文件存储的测试。`--profile managed` 构建实际 HostEntry 并执行账号模块测试。

`--profile integration` 保留原 11 场景考卷，并要求固定仓库和实际制品清单。缺少环境即失败，不再让此任务通过 `continue-on-error` 混入模块绿色状态。它仍是历史切片考卷，不能单独证明新 DS 的完整商业化能力。
