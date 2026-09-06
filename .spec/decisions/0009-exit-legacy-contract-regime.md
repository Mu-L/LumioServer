# 0009 · 退出旧合同制残留，全仓收敛为「消费 SDK 的 Rust 宿主」

- 日期:2026-09-06
- 状态:生效
- 取代:[0001](0001-gate-review-remediation.md)、[0002](0002-room-admission-host-binding-registry.md)、[0008](0008-csharp-mvp-host-frozen-after-adr-056.md)

## 背景

架构仓已按 ADR-059 转入 Living Architecture：唯一 ABI 真值是 `engine/abi/native-abi.json`，公共语义各落一份 `engine/wire/<name>-v1.json`，Baseline、`tools/lumio_contract.py` 与生成源仓 `LumioGameEngineArchitecture` 全部不存在；本仓设计现状在架构仓 `.spec/knowledge/features/ds-server.md`。

而 LumioServer 仍整仓包在 V1.4 合同制里：`docs/architecture/` 6 版正文加 `.baseline.sha256`、568 KB 的框架实现蓝图、已冻结 C# mvp-host 的 `docs/specs/`、从已退役仓 git rev `3d5e29d` 拉依赖的 `generated/`、写死 `C:/Work/...` 的 `contracts/*.lock.toml`、只做 V1.4 校验与 15 模块守卫的 `tools/xtask`（6844 行）、15 个模块目录里 13 个只剩 README 的蓝图骨架、断言 v1.4 正文与 `sha256sum -c` 的 CI，以及 `README.md` / `modules/README.md` / `.spec/` 全套合同制口径。RM-00006 的 52 张旧卡已于 D15 作废，这些文件已没有任何卡在引用。

## 决策

同 NativeCore（D1）、VoxelEngine（D8）、Runtime（D12）口径：**全清、不留兼容**。来源为 td-progress-audit 2026-09-06 LumioServer 站的 D15 / D21 / D22 裁决（R-00478，RM-00006）。

删除清单（432 文件，−70792 行）：

1. `docs/` 整目录——`architecture/` 6 版正文与 `.baseline.sha256`、`LumioServer_Framework_Implementation_Design_2026-08-27/` 蓝图、`specs/` C# host 设计与卡；仓根 `.wf-report-R-00359.md`、`.wf-report-live11.md`。
2. `mvp-host/` 整目录（含 `contract-mirror/`、`eng/*.sh|*.ps1`），连同只描述其 `Admission` 实现的 `.spec/knowledge/features/room-admission.md`。**这一条取代 0008 与 0002**：C# mvp-host 不再是冻结对照而是删除；Room 准入的 Host 绑定登记随之不再有本仓文档载体，`verify_admission` 的现行口径归 Rust 宿主与架构仓 `ds-server.md`。
3. `generated/`（3 crate）、`contracts/`（3 lock）、`tools/xtask/`（整 crate，含其 42 条自带测试）、`.spec/guards/`、`tests/policy/`；`Cargo.toml` members 收敛为三个，`.cargo/config.toml` 去 `xtask` alias，`Cargo.lock` 随之重生成（6 个 `lumio-gen-*` git 依赖归零）。
4. 13 个 README-only 模块目录（auth、control-plane-adapter、coreclr-host、host-profiles、maintenance-agent、observability、pacing、persistence-host、protocol-dispatch、release-agent、session、transport、world-slot）。**这一条取代 0001**：0001 依「模块骨架门审」定的 15 模块划分、聚合根收权与依赖图，其载体（骨架目录、`modules/README.md` 的三张图、`.spec/guards/` 的 DAG 与队列守卫）已全部删除；它描述的边界现在由架构仓 `architecture.md` §2 与本仓实际 crate 结构承担。
5. CI `repository-policy.yml`：`readme` job 去掉 v1.4 正文断言、`LGE-V1.4-2026-08-27` grep 与 `sha256sum -c`；`mvp-host` job 整段删除。两个 `cargo-entity-chat` job 逐字不变（归 R-00408）。
6. 口径重写：`README.md`、`modules/README.md`、`.spec/AGENTS.md`、`.spec/knowledge/standards/repository-architecture.md` 与导航行——唯一事实源改为架构仓 `engine/` 与 `.spec/knowledge/features/`，不再保存镜像、不复述公共契约字段。

不做的事：不碰 `modules/process/**`、`modules/host-runtime/**`、`entity-chat-host/**` 与两个 Cargo CI 作业；不修 macOS / Linux 编译；不新增任何 `cfg` 门、`#[allow]`、兼容别名或「先保留」开关。`account-server/` 本卡不删——其账号权威地位已被 `LumioPlatform` 取代（架构仓 ADR-061），目录随 R-00408 删除（D22）。

## 后果

仓里只剩「消费 SDK 的 Rust 宿主」这一种形状：`modules/process` + `modules/host-runtime` + `crates/lumio-host-testkit` + `entity-chat-host`（加待删的 `account-server/`）。没有第二份契约真值、没有蓝图骨架、没有指向已退役仓的依赖或路径。

代价：① 0001 门审结论里尚未实现的模块边界只剩散文形式，重建时需回架构仓 `ds-server.md` 取现状，不能再照抄骨架；② C# mvp-host 的 r2 路径对照消失，Room 顶号等历史实现细节只能从 git 历史取；③ `tools/xtask` 的 42 条守卫测试一并移除，仓内 DAG、源码红线与队列登记不再有机器校验，短期内靠评审保证。
