# 0012 · `lumio-ds` 默认入口、`server.json` 配置格式与 `test-harness` 门

- 日期:2026-09-06
- 状态:生效

## 背景

在 PR #38 之前，本仓缺乏统一的独立 DS 生产可执行文件入口，存在多个分散的切片测试启动器（如 `run`、`lumio-entity-chat-replay` 等）。切片入口与测试环境深度耦合，硬编码大量常量和本地路径，且缺少显式的生产/测试分界。同时，资源配额（速率限制、连接上限、队列容量等）散落在源文件中，缺少统一的配置模型与检验机制。

## 决策

1. **默认二进制入口收敛为 `lumio-ds`**：
   - 生产宿主以 `lumio-ds` 二进制为唯一默认可执行入口（`lumio-server-process` 的主要 bin）。
   - 启动必须提供 `--config <path>`，支持 `--check-config` 模式用于运维预检。
2. **统一配置模型 `server.json`**：
   - 包含必须字段：`listen`（默认 loopback）、`sdk`（CoreCLR / native 路径）、`runtime`、`gameplayRegistry`、`allocation`（环境与平台 Ed25519 公钥）、`persistence`（检查点目录与 profile）。
   - 包含可选 `hostLimits`（`HostLimitsConfig`），包含：`max_connections`、`max_pending_inputs`、`max_message_bytes`、`rate_limit_per_second`、`handshake_timeout_ms`、`drain_timeout_ms` 等，未配置时使用安全默认值。
3. **`test-harness` 门禁隔离**：
   - 历史 11 场景切片测试、重放套件（`suite.rs`）、短重连窗与特定测试辅助入口，必须严格受 `#[cfg(any(test, feature = "test-harness"))]` 保护。
   - 生产构建默认不包含 `test-harness` feature，严禁在生产宿主暴露快进时钟或跳过认证的后门。

## 后果

- 运维和集成必须通过 `server.json` 提供合法配置启动 DS。
- 保证测试代码与生产二进制的物理隔离，杜绝生产代码带入测试作弊路径。
