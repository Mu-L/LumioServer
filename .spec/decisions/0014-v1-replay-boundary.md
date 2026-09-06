# 0014 · v1 重放边界与 Bearer 准入票据生命周期

- 日期:2026-09-06
- 状态:生效

## 背景

在网络通信与身份校验中，曾存在未认证 socket 耗尽宿主资源、客户端冒用已建立的 `connectionId`、以及对平台发行的 Bearer 凭证重放策略理解分歧（例如试图在宿主层引入带内存开销的有界单次 nonce 消费表）。同时，重放边界的口径需要与架构仓 `account-port-v1.json` 与 `ds-server.md` M1 对齐。

## 决策

1. **每 Socket 独立验票与私有连接身份**：
   - 客户端建立 WebSocket 必须在握手阶段通过 `Authorization: Bearer <credential>`（或浏览器 subprotocol 载体）提交平台 Launch 凭据。
   - 每个物理 socket 独立验证签名、有效期限及 6 个 allocation 维度（`environment`、`gameId`、`roomId`、`version` 等）。
   - 验证通过后，由服务端独立分配单调递增的私有连接身份（`connectionId`），禁止客户端自报或冒用他人 `connectionId`。
2. **Bearer 票据策略不设 Nonce 消费表**：
   - 遵循架构仓 `engine/wire/account-port-v1.json` 的 `replayNote` 与 ADR-061 / ADR-068。
   - 准入凭据为 Bearer 票据，在有效期与分配维度内允许合法的重连或新连接握手；宿主**不维护单次 nonce 消费表**。
   - 同一账号的顶号与会话接管决策归 Runtime 权威管理，宿主按 Runtime 指令执行断开并返回相应的关闭原因码（`session_closed` / `role_taken`）。
3. **v1 重放边界**：
   - 重放（Replay）针对的是已经过网线准入并由服务端定序的确定性输入序列，而不是网络层重放 raw socket 握手。

## 后果

- 彻底根除了客户端冒用会话的漏洞，连接生命周期与准入凭据边界清晰。
- 宿主不再承担不必要的 nonce 有状态过滤开销，完全契合架构仓协议规范。
