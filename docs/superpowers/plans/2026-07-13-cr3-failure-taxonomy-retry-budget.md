# CR-3:失败分类法 + retry 预算

> 日期:2026-07-13
> 来源:[`veps/TODO.md`](../../veps/TODO.md) multica CR Backlog · Tier 1(★推荐先做)
> 关联:[`veps/fixus-product-substrate-design.md`](../../veps/fixus-product-substrate-design.md) §6 基座完备性缺口
> 纪律:**先 CR → 再 TDD 写测试 → 最后实施**(本文是第一步)

---

## 1. 问题(代码取证后的真实状态)

TODO 原述"只有 `retryable:bool` + `attempt:i32`,二元、无预算"。代码取证后,实际**更严重**,分 4 层:

### 1.1 失败重试已存在,但无界、无分类

`orchestrator.rs:701 handle_turn_execution_error`(agent 崩溃主失败路径)对每个 `turn_execution_error` **无条件 redo**:

```
取 turn_started → redo_count+1 → 注入 LLM 缓存 → dispatch_execute_turn (orchestrator.rs:725-773)
```

- **没有上限**。同一 turn 反复崩溃会无限 redo —— 烧 token、可能死循环。
- `fail_turn_and_respond`(orchestrator.rs:794,redo_dispatch_failed 与"无 turn_started"兜底)写完 `turn_failed` 直接返回,**task 永远停在 Executing**(见 1.3)。

### 1.2 `redo_group` 同时扛"崩溃恢复"与"失败重试"两职

- 崩溃恢复:`recovery.rs`(`recover_task`,启动期 / 未完成 turn 检测)。
- 失败重试:`orchestrator.rs:744` `handle_turn_execution_error`。
- 两者共用 `turn_id` + `redo_group` + `redo_count` + 幂等键。`redo_count` 语义混淆(既含崩溃重做,又含失败重试)。

### 1.3 `fail_task` 在生产路径里是死代码

- `grep` 取证:`service::fail_task` 仅 `service.rs:130` 定义,**零 live caller**(生产路径只调 `succeed_task`/`cancel_task`)。
- 后果:turn 终态失败时 `turn_failed` 写了,但 task 不迁移到 Failed —— **task 永远卡 Executing**(`projection.rs:161` 由 `turn_started` 推入 Executing,之后无人推到 Failed)。
- 这是**潜在 bug**,不只是"缺特性"。

### 1.4 失败负载缺分类

- `LlmFailedPayload` / `ToolFailedPayload` 有 `retryable:bool`(`models.rs:644` / `682`),但 `record_llm_failed` **从未被调用**,`record_tool_failed` 仅 `recovery.rs:207` 调用。
- `TurnFailedPayload`(`models.rs:601`)无 retryable / failure_reason 字段。
- Task 级失败负载走 `TaskTransitionPayload.reason`(自由文本),无结构化分类。

---

## 2. 目标 / 非目标

### 目标

- **G1 分类法**:引入 `FailureReason` 枚举,区分"基础设施类(该重试)"与"应用 / 终态类(不该重试)"。
- **G2 预算**:引入 `max_attempts`,终结 1.1 的无限 redo。
- **G3 终态收口**:终态失败时真正 `fail_task`(`Executing→Failed`),修 1.3 卡死 bug,且失败原因可审计。
- **G4 TDD**:测试先行,覆盖分类、预算、终态迁移。

### 非目标(显式排除,记为后续)

- **N1 失败重试与崩溃恢复分轴**:失败重试用 Orchestrator in-memory `retry_attempts` 计数器(§3.6);崩溃恢复(`recovery.rs`)继续用 `redo_group`,两者不共用计数。`turn_started.redo_count` 在事件里恒为 0(从不回写),不再当预算用。幂等安全始终由 `redo_group` + 稳定 idempotency_key 保证,与计数器无关。
- **N2 不引入 multica 式 attempt+1 子任务**。fixus 的 redo(同 turn_id + 缓存注入 + 稳定幂等键)对重试更安全,保留。
- **N3 LLM/Tool 失败分类**:本次把 `retryable:bool` **替换**为结构化 `failure_reason`(无商用版本,不保留兼容性,见 §3.4);但 LLM/Tool 失败**仍非 live 重试决策入口**(决策在 turn 级),分类字段先到位备用。
- **N4 不接外部配置中心**;`max_attempts` 走 env(默认 2,同 multica)。

---

## 3. 设计

### 3.1 `FailureReason` 枚举(`models.rs`)

输入空间(已 grep 确认,4 个已知 error_type):`agent_process_exited` / `agent_spawn_failed` / `session_create_failed` / `redo_dispatch_failed`,外加未知类型与未来扩展。

```rust
/// 失败原因分类法(CR-3)。
/// 区分"基础设施类(瞬态,预算内重试)"与"应用/终态类(不重试)"。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureReason {
    // ── 基础设施类:retryable,预算内重试 ──
    AgentSpawnFailed,     // fixlet 起不来 agent 进程 (router.rs:410)
    SessionCreateFailed,  // agent 在 session/new 阶段死掉 (router.rs:482)
    AgentProcessExited,   // agent 进程中途退出 (router.rs:327) —— 瞬态,重试合理
    RedoDispatchFailed,   // fixus 自己派发 redo 失败 (orchestrator.rs:411/783)
    BrokerError,          // broker 不可达(预留)
    SandboxTimeout,       // 沙箱超时(预留)

    // ── 应用/终态类:不重试 ──
    ApplicationError,     // agent 产出错误 / 工具语义失败 —— 重试只会再失败
    Policy,               // 策略拦截(预留)
    Canceled,             // 取消

    // ── 兜底 ──
    Unknown,              // 未知类型 → 按 retryable 处理(预算内重试),避免新类型静默杀 task
}

impl FailureReason {
    /// 是否值得重试(预算内)。
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::AgentSpawnFailed | Self::SessionCreateFailed | Self::AgentProcessExited
                | Self::RedoDispatchFailed | Self::BrokerError | Self::SandboxTimeout
                | Self::Unknown
        )
        // ApplicationError | Policy | Canceled → 终态
    }

    pub fn as_str(&self) -> &'static str { /* snake_case 字面量,用于负载序列化 */ }
}

/// 从 error_type(+ error_message 辅助)推断分类。纯函数,无副作用。
pub fn classify_failure(error_type: &str, _error_message: &str) -> FailureReason {
    match error_type {
        "agent_spawn_failed"    => FailureReason::AgentSpawnFailed,
        "session_create_failed" => FailureReason::SessionCreateFailed,
        "agent_process_exited"  => FailureReason::AgentProcessExited,
        "redo_dispatch_failed"  => FailureReason::RedoDispatchFailed,
        "broker_error"          => FailureReason::BrokerError,
        "sandbox_timeout"       => FailureReason::SandboxTimeout,
        "application_error"     => FailureReason::ApplicationError,
        "policy"                => FailureReason::Policy,
        "canceled"              => FailureReason::Canceled,
        _                       => FailureReason::Unknown,
    }
}
```

### 3.2 retry 策略模块(新文件 `src/retry.rs`)

把"分类 + 预算"聚成一个纯决策,可单测、与 orchestrator 解耦:

```rust
pub struct RetryPolicy {
    pub max_attempts: i32, // 默认 2(env FIXUS_MAX_RETRY_ATTEMPTS 覆盖)
}

pub enum RetryDecision {
    /// 还在预算内 + 可重试 → 继续 redo(调用方走原 redo 路径)
    Retry { reason: FailureReason, next_redo_count: i32 },
    /// 超预算 或 终态原因 → 终态收口(调用方 fail_task)
    Fail { reason: FailureReason, budget_exhausted: bool },
}

impl RetryPolicy {
    pub fn decide(&self, reason: FailureReason, current_redo_count: i32) -> RetryDecision {
        if reason.is_retryable() && current_redo_count < self.max_attempts {
            RetryDecision::Retry { reason, next_redo_count: current_redo_count + 1 }
        } else {
            RetryDecision::Fail {
                reason,
                budget_exhausted: reason.is_retryable(), // 可重试但超预算=预算耗尽;否则=终态原因
            }
        }
    }
}
```

> 语义:`current_redo_count` = 该 turn 已失败重试过几次(来自 Orchestrator in-memory 计数器,见 §3.3/§3.6)。`max_attempts=2` ⇒ 一个 turn 最多执行 3 次(首跑 + 2 重试),与 multica `max_attempts=2` 对齐。

### 3.3 接线(`orchestrator.rs`)

> **实现注记(2026-07-13)**:取证发现 `start_turn` 恒写 `redo_count: 0`(`service.rs:229`),redo 路径**从不回写**递增后的 redo_count——即 persisted `turn_started.redo_count` 永远是 0,不能直接当预算计数器(否则恒 < max → 永远 Retry)。故预算计数走 **Orchestrator in-memory `retry_attempts: Map<"task_id:turn_id", i32>`**(见 §3.6 的权衡)。

1. **`handle_turn_execution_error`**(agent 崩溃,异步非阻塞路径):
   - `classify_failure(error_type, error_message)` → 读 `turn_started` 拿 `user_input`/`redo_group`(redo 上下文)。
   - `current = retry_attempts[key]`(in-memory);`policy.decide(reason, current)`。
   - `Retry` ⇒ 计数器 +1 → `dispatch_with_retry` 重派(fixlet 重跑整轮,成本高)。
   - `Fail` ⇒ 清计数器 → `fail_task_with_reason`(写 `turn_failed`+`fail_task`,修卡 Executing bug)。

2. **dispatch 失败**(`redo_dispatch_failed`):由 `dispatch_with_retry` 做 **in-process 有界 produce 重试**(短退避,最多 `max_attempts` 次);耗尽 → `fail_task_with_reason(RedoDispatchFailed)`。与 turn 级计数器**正交**(dispatch 失败不产生新 turn_started、不递增计数),两层共用 `max_attempts` 上限。一次 broker 抖动不杀 task,反复失败由预算兜底。

3. **同步阻塞路径**(`timeout` / `channel_closed`,行 245/256):**不重试**(重试会延长阻塞的 HTTP 调用)→ `fail_turn_and_respond` 分类后直接 `fail_task_with_reason`。异步 agent 崩溃路径才走预算。

> 关键修复:1.3 的"task 卡 Executing"由 `fail_task_with_reason` 真正迁移到 `Failed` 修复。`fail_task` 从死代码变 live。

### 3.6 in-memory 计数器的权衡(已知局限)

预算计数用 Orchestrator 进程内 `retry_attempts` map,而非持久事件:

- **修掉的**:agent 崩溃循环时 fixus 在线 → 计数器累加 → 预算生效(§1.1 的 P0 无限 redo)。
- **已知局限**:fixus 重启会重置计数器;重启后由 `recovery.rs` 接管(独立有界路径)。一个 task 跨 fixus 重启可能累计超过 `max_attempts` 次重试。
- **硬化方向(CR-3c)**:加 `TurnRetryScheduled` 持久事件(turn 级、非终态),每次 Retry 写一条带 attempt 号;`current` = 该 turn 此类事件计数。代价:EventType +1(→23),波及 ~8 处 match 臂 + projection + stream 过滤 + "22 种"文档。短期不必,等线上日志显示跨重启预算失控再做。

### 3.4 负载改造(`models.rs`)—— 无商用版本,不保留兼容性

> 决策(2026-07-13):fixus 尚无商用 / 已部署数据,**不管向前兼容**。`retryable:bool` 与 `failure_reason.is_retryable()` 是双重真相,并存不合理 → 直接**替换**。

- `FailureReason` 枚举(§3.1)直接进负载:`#[serde(rename_all = "snake_case")]` → 序列化为 `"agent_process_exited"` 等,与 error_type 字面量同形。
- `TurnFailedPayload`:加 `#[serde(default)] failure_reason: Option<FailureReason>`。
- `TaskTransitionPayload`(ready/claimed/.../failed 通用):加 `#[serde(default, skip_serializing_if = "Option::is_none")] failure_reason: Option<FailureReason>`(仅 failed 时填)。task_failed 事件可直接查失败原因,不必扫 turn_failed(便于 CR-4 按因统计)。
- `LlmFailedPayload` / `ToolFailedPayload`:**删 `retryable:bool`**,加 `#[serde(default)] failure_reason: Option<FailureReason>`。`attempt:i32` **保留**(正交轴:第几次调用,非可重试性)。
- `record_llm_failed` / `record_tool_failed` 签名:`retryable: bool` → `failure_reason: FailureReason`;`record_tool_failed` 唯一调用方 `recovery.rs:207` 同步改。

### 3.5 配置(`main.rs`)

`max_attempts` 从 env `FIXUS_MAX_RETRY_ATTEMPTS` 读,默认 2,构造 `RetryPolicy` 注入 `Orchestrator`。

---

## 4. TDD 测试清单(先写,应失败)

> CLAUDE.md「测试优先」。先写下列测试,确认编译失败/断言失败,再实现。

### 4.1 单元测试(`src/retry.rs` 内联 `#[cfg(test)]`)

- `classify_failure` 映射:9 个已知 error_type 各自 → 正确 variant;未知串 → `Unknown`。
- `FailureReason::is_retryable`:infra/Unknown → true;Application/Policy/Canceled → false。
- `RetryPolicy::decide`:
  - retryable + redo_count=0, max=2 → `Retry{next=1}`
  - retryable + redo_count=1, max=2 → `Retry{next=2}`
  - retryable + redo_count=2, max=2 → `Fail{budget_exhausted=true}`
  - ApplicationError + redo_count=0, max=2 → `Fail{budget_exhausted=false}`(终态原因,立即 fail)
  - Unknown + redo_count=0, max=2 → `Retry`(兜底重试)

### 4.2 集成测试(`src/orchestrator.rs` 现有测试 harness:in-process logdbd)

- **预算耗尽 → task Failed**:注入 N 次 `turn_execution_error`(`agent_process_exited`)于同一 turn,N > max_attempts。断言:
  - 最终 `get_task_state == Failed`(修 1.3 卡死)。
  - 事件流含 `turn_failed` 且 `payload.failure_reason == "agent_process_exited"`。
  - `dispatch_execute_turn` 恰被调用 `max_attempts` 次(不再无限)。
- **终态原因立即 fail**:注入 `application_error` 一次。断言:不重试、task → Failed、redo 派发 0 次。
- **预算内重试成功**:注入 1 次 `agent_process_exited` 后正常 `turn_execution_done`。断言:task → Succeeded,redo 派发 1 次。
- **`redo_dispatch_failed` 预算内重试 → 成功**:让 `dispatch_execute_turn` 前 `max_attempts-1` 次 produce 失败、第 N 次成功。断言:不 fail、task 最终 Succeeded、dispatch 被调用 `max_attempts` 次。
- **`redo_dispatch_failed` 超预算 → Fail**:`dispatch_execute_turn` 连续失败 `> max_attempts` 次。断言:task → Failed、`failure_reason == "redo_dispatch_failed"`、dispatch 被调用恰 `max_attempts` 次(不再无限)。

### 4.3 回归测试(`src/recovery.rs` 不动)

- 现有崩溃恢复测试全绿(recovery.rs 路径不经过 RetryPolicy,确保正交)。

---

## 5. 实施步骤(测试通过后)

1. `models.rs`:加 `FailureReason` 枚举 + `is_retryable` + `as_str` + `classify_failure`;按 §3.4 改负载(`TurnFailedPayload` / `TaskTransitionPayload` 加 `failure_reason`;`LlmFailedPayload` / `ToolFailedPayload` 删 `retryable:bool` 换 `failure_reason`)。
2. 新建 `src/retry.rs`:`RetryPolicy` + `RetryDecision` + `impl`;内联单测(4.1)。
3. `lib.rs`:`pub mod retry;`。
4. `service.rs`:`fail_turn` / `fail_task` 签名加 `failure_reason: Option<FailureReason>`;`record_llm_failed` / `record_tool_failed` 的 `retryable:bool` 换 `failure_reason: FailureReason`;`recovery.rs:207` 同步。
5. `orchestrator.rs`:`Orchestrator` 加 `retry_policy: RetryPolicy` 字段;改 `handle_turn_execution_error` / `fail_turn_and_respond` 走 `decide()`;新增 `fail_task_with_reason`;集成测试(4.2)。
6. `main.rs`:读 env `FIXUS_MAX_RETRY_ATTEMPTS`,注入。
7. `cargo test --lib`(全绿)→ `cargo build --release`。
8. 手动冒烟:kill agent 进程触发 `agent_process_exited` > max 次,确认 task → Failed 而非卡 Executing。

---

## 6. 取证附录

| 事实 | 位置 |
|---|---|
| 无界 redo | `src/orchestrator.rs:701-790`(`handle_turn_execution_error`) |
| redo_group 双职 | `src/recovery.rs` vs `src/orchestrator.rs:744` |
| `fail_task` 死代码 | `src/service.rs:130`(定义),`grep` 零 live caller |
| task 卡 Executing | `src/projection.rs:159-161`(turn_started → Executing,无 Failed 出口) |
| `record_llm_failed` 未调用 | `grep` 空 |
| Executing→Failed 合法 | `src/models.rs:446` |
| error_type 输入空间 | `src/bin/fixlet/router.rs:327/410/482`、`src/orchestrator.rs:411/783` |
| max_attempts 同 multica | multica `pkg/taskfailure` `max_attempts=2` |

---

## 7. 风险

- **R1 现有崩溃恢复行为变化**:此前 agent 崩溃无限 redo,现在 max=2 后转 Failed。这是**预期行为**(无限重试是 bug),但若有外部依赖"fixus 会一直重试"需告知。缓解:max_attempts 可调 env。
- **R2 redo_count 混淆**(N1):崩溃重做与失败重试共用预算。短期可接受(幂等安全不依赖计数器语义);长期看 CR-3b。
