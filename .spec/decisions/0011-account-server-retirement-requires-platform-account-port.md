# 0011 · `account-server/` 退役必须走平台 `/account` 路径，自签凭据的短路径不成立

- 日期:2026-09-06
- 状态:生效
- 取代:[0010](0010-single-cross-platform-native-loader.md) 的「D22 描述的技术路径(11 场景改自签测试凭据、脱离对 LumioPlatform `/account` 的等待)仍然有效」一句(仅该句;0010 关于唯一加载器的决策与 `account-server/` 归属 R-00420 的裁决均不受影响)

## 背景

Owner 裁决 D22（记于 R-00408 评论,2026-09-05）给出的退役做法是:「`suite.rs` 已 `generate_keys()` 持有签票私钥,改为自签测试凭据(`issue_admission_credential`)后删 `account.rs` / `discover.rs` 的拉起路径与 `account-server/` 整目录」。0010 据此写下该短路径「仍然有效」,可以不等 LumioPlatform 的 `/account` 就绪。

实测证伪。11 场景的 S1 断言的是**账号语义**,而不是准入凭证的签名有效性:

- Game 侧尺子 `verify-evidence.mjs:431-433`（LumioGame `origin/main` = `be6978e`,测量时刻 2026-09-06T05:50:48Z,经 `git show origin/main:<path>` 只读已提交对象）硬断言 `account.wrongPasswordCode === 'wrong_password'`,否则记 `s1:wrong-password` 失败。
- 产出侧在本仓 `suite.rs:223` —— 用错误口令对**真实 account-server** 发起 `login_or_register(..., "654321", ...)`,`:229-230` 断言 `!accepted && error_code == Some("wrong_password")`,`:237` / `:1042` 写入证据,`:1471-1472` 发射为 `{"kind":"account","wrongPasswordCode":…}`。

自签准入凭据**根本不经过口令校验**,因此产不出 `wrong_password`。删掉 `account-server/` 后 S1 必然失败,而该尺子在 LumioGame 仓、且 R-00420 与 R-00392 都明令「不改验收尺子」。D22 第 3 条低估了 S1 对账号语义的依赖。

## 决策

**`account-server/` 的退役只能走 ADR-061 第 11 条的原路径**:集成考卷指向 LumioPlatform `/account` → 由平台承担 login-or-register 与 `wrong_password` 语义 → 考卷全绿 → 删整目录。R-00420 的前置 **R-00416 / R-00415 是真前置,绕不过去**;不得按 D22 的自签短路径提前删除。

归属不变:退役动作仍归 R-00420（0010 的该项裁决继续生效）。

**附带记录一处假绿风险(不在本 ADR 修,归属待 R-00393 判定)**:`suite.rs:1472` 的发射写法是
`evidence.pointer("/traces/account/wrongPasswordCode").and_then(Value::as_str).unwrap_or("wrong_password")`——
**兜底默认值恰好等于尺子要求的通过值**。account trace 一旦缺失（account-server 未拉起、login 调用失败,或将来真被退役）,发射侧仍写出 `"wrong_password"`,S1 不经任何真实校验即通过。兜底方向选反了:应当「缺失即失败」。该文件是 R-00392 在审证据的产生面,已报 R-00392 / R-00393,本仓不擅自修改。

## 后果

- R-00420 的 LumioServer 半边**不能**与 LumioGame 半边解耦提前交付;两半共享同一个前置链。本仓在 R-00416 / R-00415 合入前对 `account-server/` 无可执行动作。
- `.spec/knowledge/features/account-server.md` 继续描述现状（该进程仍在仓内且仍是 11 场景 S1 的唯一账号语义提供方）,不提前改成「已退役」。
- 记下一条通用教训:**判断「能否绕过跨仓前置」必须追到验收尺子实际断言的语义,而不是只看代码调用面能否替换。** 本例中调用面（准入凭证）确实可以自签替换,但尺子断言的是口令拒绝语义,替换后无从产出。
