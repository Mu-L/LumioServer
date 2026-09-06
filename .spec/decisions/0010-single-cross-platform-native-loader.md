# 0010 · 全仓只保留一份跨平台 Native 加载器，根表定义收敛到 host-runtime

- 日期:2026-09-06
- 状态:生效
- 取代:[0009](0009-exit-legacy-contract-regime.md) 的「`account-server/` 目录随 R-00408 删除（D22）」一句(仅该句;0009 其余部分不受影响)

## 背景

来源为 2026-09-05 引擎总监盘点的 Owner 裁决 **D24**（记于 R-00408 评论），载体为 R-00484。

`origin/main` = `96d49b9` 上,同一个进程把同一个 Native 库包装了两次:

- `modules/process/src/sdk_loader.rs` 无条件 `#[link(name = "kernel32")]` + `LoadLibraryW` / `GetProcAddress` / `FreeLibrary`,持有一份 88 字节的 `RootApiV1`（`CoreCLR` 槽);
- `modules/host-runtime/src/native_timer.rs` 另有一份 `kernel32` 外部块与另一份 `RootApiV1`（含 `timer_*` 槽),被 `#[cfg(windows)]` 门住——那三个 cfg-gate 提交来自 R-00388 的 cherry-pick,D24 明确只作起点、不是方案。

后果是 macOS / Linux 上整个 workspace 编译不过:`native_timer.rs` 的 `ENTRY_SYMBOL` 与 `GetApiV1` 在 `cfg(not(windows))` 下成为 dead-code 被 `-D warnings` 判死,`sdk_loader.rs` 的无门 `kernel32` 在链接阶段失败。两份根表还意味着 ABI 布局要在两处各维护一次。

## 决策

**一份加载器、一份根表,平台无关。**

1. 新设 `modules/host-runtime/src/native_abi.rs`,持有全仓唯一的 `RootApiV1`（`CoreCLR` 前缀 + `NativeCore` `timer_*` 槽,布局真值仍是架构仓 `engine/abi/native-abi.json`）与唯一的 `NativeLibrary` 加载器。加载走 `libloading`,**不再有任何 Windows-only 路径**。落在 `host-runtime` 是因为依赖方向是 `lumio-server-process` → `lumio-host-runtime`,后者零依赖。
2. `NativeLibrary` 按库自报的 `struct_size` 只拷贝已发布的字节,其余留零——`Option<fn>` 的空指针优化让未发布的尾部槽自然读成 `None`,由各消费方自己判断所需槽是否齐全。旧实现无视 `struct_size` 整块拷贝,是更弱的做法。
3. 两个消费方各取所需,不再各自加载:`sdk_loader` 只要 `CLR_SLOTS_SIZE`(88 字节前缀),`NativeAbiKernel` 要 `TIMER_SLOTS_SIZE`(全表)。`sdk_loader` 保留它独有的职责——`build-info.json` 侧车的 SHA-256 / `abiHash` / `buildId` 校验与 `ping` 探测,行为不变。
4. 库句柄的生命周期由 `NativeLibrary` 持有:根表里都是指向镜像的函数指针,提前卸载就会悬垂。手写的 `FreeLibrary` 与 `Drop for SdkLease` 随之删除,由 `libloading` 负责卸载。
5. **不得用 `cfg` 门、`#[allow(dead_code)]` 或兼容别名绕过**,由 `modules/process/tests/native_loader_architecture.rs` 三条断言机器守住:无 Windows-only 加载符号、`RootApiV1` 只有一处定义且在 `host-runtime`、加载器文件无平台门。
6. CI `cargo-entity-chat` 作业改矩阵 `windows-latest` + `ubuntu-latest`,两条腿都必过,并补上 `cargo fmt --check` 与 `cargo clippy --workspace --all-targets --locked -- -D warnings`——此前 CI 里没有任何 clippy 步骤。

**附带取代 0009 的一句**:`account-server/` 的删除归 **R-00420**（P5-1）而非 R-00408。R-00420 已逐字拥有该文件集（`account-server/**`、`README.md` 条目、`.spec/knowledge/**` 条目）,而 R-00408 是三仓一卡且已在「验收中」、4 条验收项全部 `passed`,倒流会连带打回 Client / Game 的验收成果。D22 描述的技术路径(11 场景改自签测试凭据、脱离对 LumioPlatform `/account` 的等待)仍然有效,作为 R-00420 开工时的实现建议记在该卡评论上。本 ADR 只改归属,不改是否删除。

## 后果

- macOS / Linux 上 workspace 首次可编译,`cargo clippy --workspace --all-targets --locked -- -D warnings` 与 `cargo test --workspace --locked` 在本机（macOS）全绿;Linux 的证据由 CI 的 `ubuntu-latest` 腿承担。
- 新增一个第三方依赖 `libloading`。这是刻意的取舍:手写 FFI 加载省下一个依赖,代价是每加一个平台就要再写一份,而 D24 要的正是「只写一份」。
- ABI 布局今后只在 `native_abi.rs` 维护一处;`sdk_loader` 侧的黄金布局测试收窄为守 `CoreCLR` 前缀偏移(56 / 64 / 72 / 80,共 88 字节),全表尺寸由 `native_abi` 自己的测试守。
- 修复期间顺带修正了 `suite.rs` 一处 clippy `unnested_or_patterns`——它在 `origin/main` 上已存在,只是此前 macOS 编译更早失败、Windows CI 又没有 clippy 步骤,从未暴露。行为等价的一行改动,是让收口门槛转绿的必要条件。
- 11 场景独立验收测试 `entity_chat_acceptance` 仍在无 `LUMIO_GAME_ROOT` 时 BLOCKED,与本决策无关,归 R-00392。
