# Plan B: fixus Task 状态机 + Task 级事件 + claim 协议 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 fixus 的 Task 从"带事件流的 session"升级为有显式状态机的一等实体:新增 7 个 Task 级事件、8 态状态机、pull-based claim 协议,并把路由键 `agent_type` 改名 `task_type`。

**Architecture:** 状态机是 event projection——head 的 `state` 由 Task 级事件流派生(`get_task_state`),不另存(append-only logdbd 无法 UPDATE,projection 正好契合)。新增事件 `task_created/ready/claimed/blocked/succeeded/failed/canceled`;`executing` 态由 `task_claimed` + 首个 `turn_started` 运行事件派生(spec §8.2 只列 7 个事件,§4 图有 8 态,此为两者调和)。claim 协议:fixlet 经 WS `claim{task_type}` → TaskRegistry 内存 claim 队列按 `preferred_claimant` 匹配 ready Task → service 层写 `task_claimed` → orchestrator 复用现有 `dispatch_execute_turn_with_ctx` 下发。service 层只动 store(写事件 + 不变量校验);协调(store+registry)在 orchestrator。

**Tech Stack:** Rust, Tokio, Axum, serde, logdbd(embedded test harness), uuid(v7)。

---

## 范围(Scope)

### 本 plan 做(IN)
- `models.rs`:新增 `TaskState` 枚举(8 态)+ 7 个 `EventType` 变体 + payload 结构 + `Provenance` 结构;`Task` 结构 `agent_type`→`task_type` 重命名 + `state`/`provenance`/`body` 字段。
- `storage.rs`:`create_task` 改发 `task_created` 并分配 `task_id`;`get_task`/`task_exists`/`is_task_ended` 迁移到 Task 级事件;新增 `get_task_state`。
- `service.rs`:新增 6 个状态迁移函数(ready/claim/executing/block/succeed/fail/cancel)+ 新 `create_task(task_type, provenance, body)` 签名;移除 `end_task`。
- `task_registry.rs`:`agent_type`→`task_type` 内部重命名;新增内存 claim 队列 + `preferred_claimant` 匹配。
- `protocol.rs`:新增 `Claim` / `ClaimGranted` / `ClaimDenied` 消息类型。
- `orchestrator.rs`:`resolve_agent_type`→`resolve_task_type`;claim 派发接线。
- `server.rs`:`register` WS 字段 `agent_type`→`task_type`;新增 `claim` WS handler;新增 HTTP `POST /tasks/{id}/ready`、`POST /tasks/{id}/cancel`、`GET /tasks/{id}/state`;`/sessions/{id}/end` 改调 `cancel_task`。
- `src/bin/fixlet/router.rs`:`register` 消息字段 `agent_type`→`task_type`(同 repo 两端同步改名,不留 cosmetic mismatch)。

### 本 plan 不做(OUT,留给后续 plan)
- **wire cosmetic**:HTTP 路径 `/sessions/...`→`/tasks/...`、header `X-Fixus-Session-Id`→`X-Fixus-Task-Id`、`protocol.rs` 的 `session_id` 字段名(值=task_id)、`EventType::Session*` 变体名——值/行为不变,改名 break nuntius 调用,留 Plan C/D 顺手清。
- **restart rehydrate**:claim 队列是内存态,fixus 重启后需从事件流重建 ready 队列——Plan D e2e 时处理(本 plan 在队列处注明)。
- **nuntius 侧**:create_task 调用方、readiness gate、contract 编译——Plan C/D。
- **acceptance**:Plan E。

### 关键设计决策(已锁定)
1. **session_started/session_ended 变体保留定义**:spec §4.4 明确"现有 15 种 EventType 全覆盖运行事件流",nuntius 订阅它们投影 UI。`create_task` 不再发 `session_started`(改发 `task_created`),`end_task` 移除,但变体不删(删除会波及 serde/from_str/all_str_variants/validate/recovery/tests,且与 audit 已知死代码 `turn_pending` 同性质——记录为 legacy follow-up)。
2. **`is_task_ended` 保留方法名,改查询目标**:查询 `{task_succeeded, task_failed, task_canceled}`(任一终态=ended)。语义("task 是否还能接受事件")不变,降低 call-site 风险。
3. **`executing` 态派生**:`get_task_state` 见到 `task_claimed` 后存在 `turn_started` 运行事件 → Executing;否则 Claimed。调和 §8.2(7 事件)与 §4(8 态)。
4. **claim 队列在 registry(内存),入队由 orchestrator 触发**:service 层 `mark_task_ready` 只写事件;orchestrator 调完后再 `registry.enqueue_ready`。保持 service 纯 store、协调在 orchestrator 的现有分层。
5. **task_id 由 fixus 分配**(spec §8.4):`create_task` 用 UUIDv7 分配,返回 `(task_id, event)`。HTTP `CreateSessionRequest.session_id` 改 `Option`(给了用它的 back-compat,不给则 fixus 分配)。
6. **tenant_id/user_id 从 provenance 派生**:`source_tenant_id`→tenant_id、`source_user_id`→user_id(去冗余,匹配 spec §8.4 `create_task(task_type, provenance, body)`)。

---

## File Structure

| 文件 | 改动类型 | 职责 |
|------|---------|------|
| `src/models.rs` | 修改 | TaskState 枚举 + 7 EventType 变体 + Provenance/payload 结构 + Task 扩展 |
| `src/storage.rs` | 修改 | EventStore trait create_task 新签名 + get_task_state + 3 个 head 查询迁移 |
| `src/service.rs` | 修改 | 6 个状态迁移函数 + 新 create_task 签名 + 移除 end_task |
| `src/task_registry.rs` | 修改 | agent_type→task_type 重命名 + claim 队列 |
| `src/protocol.rs` | 修改 | Claim/ClaimGranted/ClaimDenied 消息类型 |
| `src/orchestrator.rs` | 修改 | resolve_task_type + claim 派发接线 |
| `src/server.rs` | 修改 | register 字段改名 + claim WS handler + 3 个 HTTP 端点 + /end 改 cancel |
| `src/bin/fixlet/router.rs` | 修改 | register 消息字段 agent_type→task_type |
| `src/error.rs` | 修改 | 新增 InvalidTaskStateTransition 错误变体 |

---

## Task 1: TaskState 枚举 + 状态迁移规则(models.rs,纯逻辑)

**Files:**
- Modify: `src/models.rs`(在 `// ── Task ──` 段之前插入新段)

- [ ] **Step 1: 写失败测试——TaskState 序列化 + 合法迁移 + 非法迁移拒绝**

在 `src/models.rs` 的 `#[cfg(test)] mod tests` 内追加(放在 `test_all_event_types_have_str_repr` 之后):

```rust
    #[test]
    fn test_task_state_serde_roundtrip() {
        for (variant, s) in [
            (TaskState::Created, "created"),
            (TaskState::Ready, "ready"),
            (TaskState::Claimed, "claimed"),
            (TaskState::Executing, "executing"),
            (TaskState::Blocked, "blocked"),
            (TaskState::Succeeded, "succeeded"),
            (TaskState::Failed, "failed"),
            (TaskState::Canceled, "canceled"),
        ] {
            assert_eq!(variant.as_str(), s);
            assert_eq!(TaskState::from_str(s), Some(variant));
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{}\"", s));
        }
    }

    #[test]
    fn test_task_state_legal_transitions() {
        use TaskState::*;
        // spec §4 状态机
        let legal = [
            (Created, Ready),
            (Ready, Claimed),
            (Claimed, Executing),
            (Executing, Blocked),
            (Blocked, Ready),
            (Executing, Succeeded),
            (Executing, Failed),
        ];
        for (from, to) in legal {
            assert!(
                TaskState::can_transition(from, to),
                "{:?}→{:?} should be legal",
                from, to
            );
        }
    }

    #[test]
    fn test_task_state_illegal_transitions() {
        use TaskState::*;
        let illegal = [
            (Created, Claimed),   // 必须 ready→claimed,跳过 ready 非法
            (Ready, Executing),   // 必须 claimed→executing
            (Succeeded, Ready),   // 终态不可迁出
            (Failed, Created),
            (Canceled, Ready),
            (Blocked, Succeeded), // blocked→ready→claimed→executing→succeeded,不可直达
        ];
        for (from, to) in illegal {
            assert!(
                !TaskState::can_transition(from, to),
                "{:?}→{:?} should be illegal",
                from, to
            );
        }
    }

    #[test]
    fn test_task_state_terminal() {
        use TaskState::*;
        assert!(Succeeded.is_terminal());
        assert!(Failed.is_terminal());
        assert!(Canceled.is_terminal());
        assert!(!Created.is_terminal());
        assert!(!Executing.is_terminal());
        assert!(!Blocked.is_terminal());
    }
```

- [ ] **Step 2: 运行测试验证失败**

Run: `cargo test --lib models::tests::test_task_state`
Expected: 编译失败(`TaskState` 未定义)。

- [ ] **Step 3: 实现 TaskState 枚举**

在 `src/models.rs` 的 `// ── Task ──` 注释行(约 line 315)之前插入:

```rust
// ── TaskState ──────────────────────────────────────────────────────────

/// Task 状态机(spec §4)
///
/// 8 态:`created → ready → claimed → executing → (blocked ⇄ ready) → succeeded | failed`
/// 任意活态 → `canceled`(终态)。
///
/// 状态是事件的投影(spec §4.4):本枚举只描述合法迁移,实际状态由
/// `storage::get_task_state` 从 Task 级事件流派生。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Created,
    Ready,
    Claimed,
    Executing,
    Blocked,
    Succeeded,
    Failed,
    Canceled,
}

impl TaskState {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "created" => Some(Self::Created),
            "ready" => Some(Self::Ready),
            "claimed" => Some(Self::Claimed),
            "executing" => Some(Self::Executing),
            "blocked" => Some(Self::Blocked),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Ready => "ready",
            Self::Claimed => "claimed",
            Self::Executing => "executing",
            Self::Blocked => "blocked",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    /// 是否终态(不可再迁出)
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }

    /// `from → to` 是否合法迁移(spec §4 状态机)
    pub fn can_transition(from: Self, to: Self) -> bool {
        use TaskState::*;
        if from.is_terminal() {
            return false;
        }
        matches!(
            (from, to),
            (Created, Ready)
                | (Ready, Claimed)
                | (Ready, Canceled)
                | (Claimed, Executing)
                | (Claimed, Canceled)
                | (Executing, Blocked)
                | (Executing, Succeeded)
                | (Executing, Failed)
                | (Executing, Canceled)
                | (Blocked, Ready)
                | (Blocked, Canceled)
                | (Created, Canceled)
        )
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `cargo test --lib models::tests::test_task_state`
Expected: 4 个 test 全 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/models.rs
git commit -m "feat(models): add TaskState enum + transition rules (Plan B)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 2: 7 个 Task EventType 变体 + payload + scope(models.rs)

**Files:**
- Modify: `src/models.rs`(`EventType` 枚举 + `EventScope` + impl 方法 + payload 结构)

- [ ] **Step 1: 写失败测试——7 新事件 serde + scope + task 级 scope 校验**

在 `models.rs` tests 内追加:

```rust
    #[test]
    fn test_task_event_type_roundtrip() {
        let cases = [
            (EventType::TaskCreated, "task_created"),
            (EventType::TaskReady, "task_ready"),
            (EventType::TaskClaimed, "task_claimed"),
            (EventType::TaskBlocked, "task_blocked"),
            (EventType::TaskSucceeded, "task_succeeded"),
            (EventType::TaskFailed, "task_failed"),
            (EventType::TaskCanceled, "task_canceled"),
        ];
        for (variant, s) in cases {
            assert_eq!(variant.as_str(), s);
            assert_eq!(EventType::from_str(s), Some(variant.clone()));
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{}\"", s));
            // Task 级事件:turn_id=NULL, step_id=NULL
            assert_eq!(variant.scope(), EventScope::Task);
        }
    }

    #[test]
    fn test_task_event_scope_validation() {
        // Task 级事件必须 turn_id=NULL, step_id=NULL
        let e = AgentEvent::new(
            "t_1".into(), None, None,
            EventType::TaskReady,
            serde_json::json!({}),
        );
        assert!(e.validate_scope().is_ok());

        // 带 turn_id 非法
        let e = AgentEvent::new(
            "t_1".into(), Some(1), None,
            EventType::TaskReady,
            serde_json::json!({}),
        );
        assert!(e.validate_scope().is_err());
    }

    #[test]
    fn test_all_event_types_count_after_task_add() {
        // 原 15 + 新 7 Task 级 = 22
        assert_eq!(EventType::all_str_variants().len(), 22);
    }
```

同时更新已有 `test_all_event_types_have_str_repr`:把 `assert_eq!(all.len(), 15, ...)` 改为 `22`。

- [ ] **Step 2: 运行验证失败**

Run: `cargo test --lib models::tests::test_task_event`
Expected: 编译失败(`EventType::TaskCreated` 等未定义;`EventScope::Task` 未定义)。

- [ ] **Step 3: 加 7 个 EventType 变体 + EventScope::Task**

在 `EventType` 枚举的 `SessionEnded,` 之后(即 Session 级块内)插入 Task 级块:

```rust
    // Task 级别事件 (turn_id = NULL, step_id = NULL) — Task 生命周期状态机(spec §8.2)
    TaskCreated,
    TaskReady,
    TaskClaimed,
    TaskBlocked,
    TaskSucceeded,
    TaskFailed,
    TaskCanceled,
```

在 `EventScope` 枚举加 `Task` 变体:

```rust
pub enum EventScope {
    /// Task 级别 — turn_id = NULL, step_id = NULL(Task 生命周期事件)
    Task,
    /// Session 级别 — turn_id = NULL, step_id = NULL
    Session,
    /// Turn 级别 — turn_id NOT NULL, step_id = NULL
    Turn,
    /// Step 级别 — step_id NOT NULL
    Step,
}
```

- [ ] **Step 4: 更新 EventType impl 的 from_str / as_str**

`from_str` match 内(在 `"session_ended" =>` 之后)加:

```rust
            "task_created" => Some(Self::TaskCreated),
            "task_ready" => Some(Self::TaskReady),
            "task_claimed" => Some(Self::TaskClaimed),
            "task_blocked" => Some(Self::TaskBlocked),
            "task_succeeded" => Some(Self::TaskSucceeded),
            "task_failed" => Some(Self::TaskFailed),
            "task_canceled" => Some(Self::TaskCanceled),
```

`as_str` match 内加对应 7 个 `Self::TaskXxx => "task_xxx"`。

- [ ] **Step 5: 更新 scope 分类方法**

新增 `is_task_level` + 改 `scope`。在 `is_session_level` 方法定义之前插入:

```rust
    /// 是否为 Task 级别事件 (turn_id = NULL, step_id = NULL) — Task 生命周期
    pub fn is_task_level(&self) -> bool {
        matches!(
            self,
            Self::TaskCreated
                | Self::TaskReady
                | Self::TaskClaimed
                | Self::TaskBlocked
                | Self::TaskSucceeded
                | Self::TaskFailed
                | Self::TaskCanceled
        )
    }
```

改 `scope` 方法,在最前加 Task 分支:

```rust
    pub fn scope(&self) -> EventScope {
        if self.is_task_level() {
            EventScope::Task
        } else if self.is_session_level() {
            EventScope::Session
        } else if self.is_turn_level() {
            EventScope::Turn
        } else {
            EventScope::Step
        }
    }
```

改 `validate_scope`(在 `AgentEvent::validate_scope` 的 match 内):为 `EventScope::Task` 加与 Session 相同的 NULL/NULL 校验。把现有 `EventScope::Session =>` 分支改为同时覆盖 Task——即:

```rust
        match self.event_type.scope() {
            EventScope::Task | EventScope::Session => {
                if self.turn_id.is_some() {
                    return Err(format!(
                        "{}-level event {} must have turn_id = NULL",
                        self.event_type.scope().as_str(),
                        self.event_type.as_str()
                    ));
                }
                if self.step_id.is_some() {
                    return Err(format!(
                        "{}-level event {} must have step_id = NULL",
                        self.event_type.scope().as_str(),
                        self.event_type.as_str()
                    ));
                }
            }
            // ... Turn / Step 分支不变
        }
```

并给 `EventScope` 加 `as_str`:

```rust
impl EventScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Session => "session",
            Self::Turn => "turn",
            Self::Step => "step",
        }
    }
}
```

- [ ] **Step 6: 更新 all_str_variants + check_terminal_uniqueness 旁路**

`all_str_variants` 数组内在 `"session_ended",` 之后加 7 个 `"task_xxx",`。

Task 终态事件也需唯一性保护(同一 task 终态最多一个)。但唯一性校验在 `storage.rs::check_terminal_uniqueness`,本 step 只确保 `EventType` 侧 ready。**注意**:`is_turn_terminal`/`is_step_terminal` 等不加 Task 事件(Task 终态唯一性在 storage 层用专门查询处理,见 Task 4)。

- [ ] **Step 7: 加 payload 结构**

在 `SessionEndedPayload` 之后加:

```rust
/// task_created 的 payload(spec §3 head: task_type + provenance + body)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCreatedPayload {
    pub task_type: String,
    pub provenance: Provenance,
    /// body(fixus opaque 透传):contract / schema_ref / task_brief / acceptance_result
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
}

/// Task 迁移事件的 payload(ready/claimed/blocked/succeeded/failed/canceled 通用)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTransitionPayload {
    /// 迁移原因(自由文本)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// 认领/执行该 Task 的执行器标识(claimed 时填,用于 preferred_claimant)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimant: Option<String>,
}
```

`Provenance` 结构加在 `TaskCreatedPayload` 之前:

```rust
/// Task 溯源元数据(spec §3 head.provenance)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    /// 下发渠道:nuntius-chat / api / schedule / derived
    pub source_channel: String,
    /// nuntius chat session(下发对话)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_tenant_id: Option<String>,
    /// 触发提交的那条对话消息/澄清轮(精确溯源)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_message_id: Option<String>,
    pub created_at: DateTime<Utc>,
    /// 下发系统标识(如 "nuntius")
    pub created_by: String,
}
```

- [ ] **Step 8: 运行测试验证通过**

Run: `cargo test --lib models::tests`
Expected: 全 PASS(含新 3 个 + 改后的 `test_all_event_types_have_str_repr`)。

- [ ] **Step 9: Commit**

```bash
git add src/models.rs
git commit -m "feat(models): add 7 Task-level EventType variants + payloads + Task scope (Plan B)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 3: 扩展 Task 结构(task_type 重命名 + state/provenance/body)(models.rs)

**Files:**
- Modify: `src/models.rs`(`Task` struct + `Task::new`)

- [ ] **Step 1: 写失败测试——Task 新字段 + task_type**

在 models.rs tests 追加:

```rust
    #[test]
    fn test_task_struct_has_task_type_and_state() {
        let prov = Provenance {
            source_channel: "nuntius-chat".into(),
            source_session_id: Some("chat_1".into()),
            source_user_id: Some("u1".into()),
            source_tenant_id: Some("t1".into()),
            source_message_id: None,
            created_at: Utc::now(),
            created_by: "nuntius".into(),
        };
        let task = Task::new(
            "t_abc".into(),
            "t1".into(),
            "u1".into(),
            "database.repair".into(),
            TaskState::Created,
            prov.clone(),
            None,
        );
        assert_eq!(task.task_type, "database.repair");
        assert_eq!(task.state, TaskState::Created);
        assert_eq!(task.provenance.source_channel, "nuntius-chat");
        assert!(task.body.is_none());
    }
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test --lib models::tests::test_task_struct_has_task_type_and_state`
Expected: 编译失败(`Task` 无 `task_type`/`state`/`provenance`/`body`,`Task::new` 签名不符)。

- [ ] **Step 3: 改 Task struct + new**

替换现有 `Task` struct 定义为:

```rust
/// Task — 唯一有独立存储的实体(spec §3 head)
///
/// head 字段:task_id / task_type / state / provenance。
/// body(fixus opaque)整体存 `body: Option<Value>`。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub task_id: String,
    pub tenant_id: String,
    pub user_id: String,
    /// 路由键(原 agent_type,spec §8.3 改名)
    pub task_type: String,
    /// 当前状态(事件投影,由 storage::get_task_state 派生后填入)
    pub state: TaskState,
    /// 溯源
    pub provenance: Provenance,
    /// body(opaque):contract / schema_ref / task_brief / acceptance_result
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl Task {
    pub fn new(
        task_id: String,
        tenant_id: String,
        user_id: String,
        task_type: String,
        state: TaskState,
        provenance: Provenance,
        body: Option<serde_json::Value>,
    ) -> Self {
        Self {
            task_id,
            tenant_id,
            user_id,
            task_type,
            state,
            provenance,
            body,
            created_at: Utc::now(),
            metadata: None,
        }
    }
}
```

- [ ] **Step 4: 运行全量 lib 测试,预期出现 agent_type 编译错误**

Run: `cargo build --lib 2>&1 | head -40`
Expected: 编译错误集中在 `agent_type` 字段引用(storage.rs `get_task` 构造 `Task { agent_type, ... }`、orchestrator `session.agent_type`)。这些在 Task 4/8 修复。**本 step 只确认 models.rs 自身测试通过:**

Run: `cargo test --lib models::tests::test_task_struct_has_task_type_and_state`
Expected: PASS。

- [ ] **Step 5: Commit(models.rs 局部,后续 task 修编译错误)**

```bash
git add src/models.rs
git commit -m "feat(models): extend Task struct (task_type rename + state/provenance/body) (Plan B)

NOTE: agent_type→task_type 字段重命名引发 storage/orchestrator 编译错误,
由后续 Task 4/8 修复。本 commit 仅 models.rs 自身测试通过。
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 4: Storage——create_task 发 task_created + head 查询迁移 + get_task_state(storage.rs)

**Files:**
- Modify: `src/storage.rs`(EventStore trait + LogdbdEventStore impl + 测试)
- Modify: `src/error.rs`(新错误变体)

> **依赖**:Task 3 改了 Task struct,本 Task 修复 storage.rs 的编译错误并迁移查询。

- [ ] **Step 1: 加错误变体**

在 `src/error.rs` 的 `LifecycleInvariant` 之后加:

```rust
    #[error("invalid task state transition: {from} → {to} (task={task_id})")]
    InvalidTaskStateTransition {
        task_id: String,
        from: String,
        to: String,
    },
```

并在 `server.rs` 的 `IntoResponse for AppError` 加映射(在 `AppError::LifecycleInvariant(_)` 旁):

```rust
            AppError::InvalidTaskStateTransition { .. } => (StatusCode::CONFLICT, self.to_string()),
```

- [ ] **Step 2: 改 EventStore trait 的 create_task 签名 + 加 get_task_state**

`src/storage.rs` trait 内,把现有 `create_task` 签名替换为:

```rust
    async fn create_task(
        &self,
        task_type: &str,
        tenant_id: &str,
        user_id: &str,
        provenance: &crate::models::Provenance,
        body: Option<&serde_json::Value>,
    ) -> Result<(String, AgentEvent)>;
```

(`String` = fixus 分配的 task_id)

在 `is_task_ended` 之后加:

```rust
    /// 派生 Task 当前状态(从 Task 级事件流投影)。
    /// 不存在 task_created → 返回 None(调用方判 TaskNotFound)。
    async fn get_task_state(&self, task_id: &str) -> Result<Option<crate::models::TaskState>>;
```

- [ ] **Step 3: 改 LogdbdEventStore::create_task 实现——发 task_created + 分配 task_id**

替换现有 `async fn create_task` 实现:

```rust
    async fn create_task(
        &self,
        task_type: &str,
        tenant_id: &str,
        user_id: &str,
        provenance: &crate::models::Provenance,
        body: Option<&serde_json::Value>,
    ) -> Result<(String, AgentEvent)> {
        // fixus 分配 task_id(UUIDv7,全局唯一单调)
        let task_id = format!(
            "task_{}",
            uuid::Uuid::now_v7().to_string().replace('-', "")
        );

        let payload = serde_json::json!({
            "task_type": task_type,
            "provenance": provenance,
            "body": body.cloned().unwrap_or(serde_json::Value::Null),
        });
        let content = serde_json::to_vec(&payload)
            .map_err(|e| AppError::Internal(format!("json: {}", e)))?;

        let mut meta = HashMap::new();
        meta.insert("task_id".into(), task_id.clone());
        meta.insert("tenant_id".into(), tenant_id.to_string());
        meta.insert("user_id".into(), user_id.to_string());
        meta.insert("task_type".into(), task_type.to_string());

        let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        let mut client = self.client.lock().await;
        let resp = client
            .append_full(
                &self.namespace,
                &task_id,
                "task_created",
                "application/json",
                &meta,
                ts_ns,
                &content,
            )
            .await
            .map_err(|e| AppError::Internal(format!("logdbd append: {}", e)))?;

        let event = AgentEvent {
            task_id: task_id.clone(),
            seq: resp.seq as i64,
            turn_id: None,
            step_id: None,
            event_type: EventType::TaskCreated,
            schema_version: 1,
            payload,
            created_at: Utc::now(),
        };

        Ok((task_id, event))
    }
```

确保文件顶部 `use` 含 `uuid`(若没有,加 `use uuid;` 或用全路径 `uuid::Uuid`)。

- [ ] **Step 4: 改 get_task——读 task_created + 派生 state**

替换现有 `async fn get_task` 实现:

```rust
    async fn get_task(&self, task_id: &str) -> Result<Option<Task>> {
        let mut client = self.client.lock().await;

        // 读 task_created(head 事实)
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec!["task_created".into()],
                    result: QueryResult::Records as i32,
                    limit: 1,
                    ..Default::default()
                },
            )
            .await?;
        let rec = match resp.result {
            Some(query_response::Result::Records(r)) => r.records.into_iter().next(),
            _ => None,
        };
        let Some(rec) = rec else { return Ok(None) };

        let payload: serde_json::Value = if rec.content.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&rec.content).unwrap_or_default()
        };

        let provenance: crate::models::Provenance = serde_json::from_value(
            payload.get("provenance").cloned().unwrap_or(serde_json::Value::Null),
        )
        .unwrap_or_else(|_| crate::models::Provenance {
            source_channel: "unknown".into(),
            source_session_id: None,
            source_user_id: None,
            source_tenant_id: None,
            source_message_id: None,
            created_at: Utc::now(),
            created_by: "unknown".into(),
        });

        let tenant_id = rec
            .metadata
            .get("tenant_id")
            .cloned()
            .or_else(|| provenance.source_tenant_id.clone())
            .unwrap_or_else(|| "default".into());
        let user_id = rec
            .metadata
            .get("user_id")
            .cloned()
            .or_else(|| provenance.source_user_id.clone())
            .unwrap_or_default();

        // 派生当前 state
        let state = self
            .derive_task_state(&mut client, task_id)
            .await?
            .unwrap_or(crate::models::TaskState::Created);

        Ok(Some(Task {
            task_id: task_id.to_string(),
            tenant_id,
            user_id,
            task_type: payload["task_type"]
                .as_str()
                .unwrap_or("unknown")
                .to_string(),
            state,
            provenance,
            body: payload.get("body").filter(|v| !v.is_null()).cloned(),
            created_at: Utc::now(),
            metadata: None,
        }))
    }
```

- [ ] **Step 5: 实现 get_task_state + 私有 derive_task_state**

在 impl 内加(`is_task_ended` 实现之后):

```rust
    async fn get_task_state(
        &self,
        task_id: &str,
    ) -> Result<Option<crate::models::TaskState>> {
        let mut client = self.client.lock().await;
        // 先确认 task_created 存在
        let exists = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec!["task_created".into()],
                    result: QueryResult::Exists as i32,
                    ..Default::default()
                },
            )
            .await?;
        if !matches!(
            exists.result,
            Some(query_response::Result::Exists(true))
        ) {
            return Ok(None);
        }
        self.derive_task_state(&mut client, task_id)
            .await
            .map(Some)
    }

    /// 从 Task 级事件流派生状态(spec §4.4 projection)。
    /// - 取最新 Task 迁移事件 → 基础态
    /// - 基础态 == Claimed 且存在 task_claimed 之后的 turn_started → Executing
    async fn derive_task_state(
        &self,
        client: &mut Client,
        task_id: &str,
    ) -> Result<crate::models::TaskState> {
        use crate::models::TaskState;

        // 取全部 Task 级事件(按 seq 升序),找最后一个迁移事件
        let task_events = [
            "task_created",
            "task_ready",
            "task_claimed",
            "task_blocked",
            "task_succeeded",
            "task_failed",
            "task_canceled",
        ];
        let resp = self
            .run_query(
                client,
                task_id,
                QueryRequest {
                    event_types: task_events.iter().map(|s| s.to_string()).collect(),
                    result: QueryResult::Records as i32,
                    ..Default::default()
                },
            )
            .await?;
        let records = match resp.result {
            Some(query_response::Result::Records(r)) => r.records,
            _ => vec![],
        };

        // 升序最后一个 → 最新迁移事件
        let latest = records.iter().max_by_key(|r| r.seq);
        let base = match latest.and_then(|r| r.event_type.as_deref()) {
            Some("task_created") => TaskState::Created,
            Some("task_ready") => TaskState::Ready,
            Some("task_claimed") => TaskState::Claimed,
            Some("task_blocked") => TaskState::Blocked,
            Some("task_succeeded") => TaskState::Succeeded,
            Some("task_failed") => TaskState::Failed,
            Some("task_canceled") => TaskState::Canceled,
            _ => TaskState::Created,
        };

        // Claimed → 检查是否有之后的 turn_started(派生 Executing)
        if base == TaskState::Claimed {
            let claimed_seq = latest.map(|r| r.seq).unwrap_or(0);
            let resp = self
                .run_query(
                    client,
                    task_id,
                    QueryRequest {
                        event_types: vec!["turn_started".into()],
                        result: QueryResult::Max as i32, // 取 turn_started 最大 seq
                        ..Default::default()
                    },
                )
                .await?;
            if let Some(query_response::Result::Max(ts_seq)) = resp.result {
                if (ts_seq as i64) > claimed_seq {
                    return Ok(TaskState::Executing);
                }
            }
        }

        Ok(base)
    }
```

> **注意**:`Record` 结构需有 `seq` 和 `event_type` 字段。用 `rg -n 'pub struct Record' ` 在 logdbd 客户端 crate 确认字段名(event_type 可能是 `event_type: String` 或在 metadata)。若 `event_type` 不在 Record 上,改用 `rec.metadata.get("event_type")` 或 query 时按单事件类型分别查最大 seq(更稳:对每个终态/迁移事件各查一次 Max seq,取最大者)。**实施时先 `rg -n 'struct Record' <logdbd-client-crate>` 确认,再选实现**。若字段不可得,fallback:对 7 个事件各跑一次 `Max` seq 查询,取 seq 最大的事件类型作 latest。

- [ ] **Step 6: 迁移 task_exists / is_task_ended 查询目标**

`task_exists`:把 `event_types: vec!["session_started".into()]` 改为 `vec!["task_created".into()]`。

`is_task_ended`:把 `event_types: vec!["session_ended".into()]` 改为:

```rust
                    event_types: vec![
                        "task_succeeded".into(),
                        "task_failed".into(),
                        "task_canceled".into(),
                    ],
```

- [ ] **Step 7: 更新 check_terminal_uniqueness 支持 Task 终态唯一性**

在 `check_terminal_uniqueness` 的 match 内(`SessionEnded =>` 旁)加 Task 终态分支:

```rust
            TaskSucceeded | TaskFailed | TaskCanceled => (
                vec!["task_succeeded", "task_failed", "task_canceled"],
                "task",
            ),
```

且 `"task"` scope 不需要 metadata 过滤(stream=task_id 已限定),确认现有 match 的 `_ => {}` 分支能走到(scope "task" 走 default 不 push metadata——正确)。

- [ ] **Step 8: 改现有 storage 测试(create_session_round_trip 等)**

`src/storage.rs` `logdbd_tests` 模块内:

(a) `create_session_round_trip`:改测试名→`create_task_round_trip`,把
```rust
        let ev = store.create_task(sid, "tenant-a", "user-1", "claude-code", None).await.unwrap();
        assert_eq!(ev.event_type, EventType::SessionStarted);
```
改为:
```rust
        let prov = crate::models::Provenance {
            source_channel: "api".into(),
            source_session_id: None,
            source_user_id: Some("user-1".into()),
            source_tenant_id: Some("tenant-a".into()),
            source_message_id: None,
            created_at: chrono::Utc::now(),
            created_by: "test".into(),
        };
        let (task_id, ev) = store
            .create_task("claude-code", "tenant-a", "user-1", &prov, None)
            .await
            .unwrap();
        assert_eq!(ev.event_type, EventType::TaskCreated);
        assert_eq!(ev.task_id, task_id);
        assert!(task_id.starts_with("task_"));

        wait_seq(&store, &task_id, 1).await;
        assert!(store.task_exists(&task_id).await.unwrap());

        let s = store.get_task(&task_id).await.unwrap().unwrap();
        assert_eq!(s.task_id, task_id);
        assert_eq!(s.task_type, "claude-code");
        assert_eq!(s.state, crate::models::TaskState::Created);
        assert!(!store.is_task_ended(&task_id).await.unwrap());
```

(b) `session_ended_detected`:改为 `task_terminal_detected`,用 `service::cancel_task`(Task 5 实现后)或直接写 task_canceled 事件:
```rust
        let ev = AgentEvent::new(task_id.clone(), None, None, EventType::TaskCanceled, serde_json::json!({"reason":"test"}));
        store.write_event(&ev).await.unwrap();
        wait_seq(&store, &task_id, 2).await;
        assert!(store.is_task_ended(&task_id).await.unwrap());
```
(用 Task 5 的 `cancel_task` 更端到端,但本 Task 5 尚未实现——此处直接写事件即可,Task 5 再加 service 层测试。)

(c) 其余 storage 测试凡调用 `create_task(旧签名)`,一律改新签名(传 `&prov`)。用 `rg -n 'create_task\(' src/storage.rs` 找全所有 call site 改之。

- [ ] **Step 9: 写 get_task_state 测试**

在 `logdbd_tests` 内加:

```rust
    #[tokio::test]
    async fn get_task_state_lifecycle_projection() {
        let (store, _dir) = setup().await;
        let prov = test_provenance(); // 见下方辅助
        let (tid, _) = store.create_task("db.repair", "t", "u", &prov, None).await.unwrap();
        wait_seq(&store, &tid, 1).await;

        // created
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(crate::models::TaskState::Created)
        );

        // ready
        store.write_event(&AgentEvent::new(tid.clone(), None, None, EventType::TaskReady, serde_json::json!({}))).await.unwrap();
        wait_seq(&store, &tid, 2).await;
        assert_eq!(store.get_task_state(&tid).await.unwrap(), Some(crate::models::TaskState::Ready));

        // claimed
        store.write_event(&AgentEvent::new(tid.clone(), None, None, EventType::TaskClaimed, serde_json::json!({"claimant":"fixlet-1"}))).await.unwrap();
        wait_seq(&store, &tid, 3).await;
        assert_eq!(store.get_task_state(&tid).await.unwrap(), Some(crate::models::TaskState::Claimed));

        // claimed + turn_started → executing
        store.write_event(&AgentEvent::new(tid.clone(), Some(1), None, EventType::TurnStarted, serde_json::json!({"user_input":"x","redo_group":"rg1","redo_count":0}))).await.unwrap();
        wait_seq(&store, &tid, 4).await;
        assert_eq!(store.get_task_state(&tid).await.unwrap(), Some(crate::models::TaskState::Executing));

        // 不存在的 task
        assert_eq!(store.get_task_state("nope").await.unwrap(), None);
    }
```

加辅助(模块顶部):
```rust
    fn test_provenance() -> crate::models::Provenance {
        crate::models::Provenance {
            source_channel: "api".into(),
            source_session_id: None,
            source_user_id: Some("u".into()),
            source_tenant_id: Some("t".into()),
            source_message_id: None,
            created_at: chrono::Utc::now(),
            created_by: "test".into(),
        }
    }
```

- [ ] **Step 10: 运行 storage 测试**

Run: `cargo test --lib storage::logdbd_tests`
Expected: 全 PASS(含迁移后的 create/task_terminal + 新 get_task_state_lifecycle)。

- [ ] **Step 11: 确认 lib 整体编译**

Run: `cargo build --lib 2>&1 | tail -20`
Expected: 仅剩 service.rs / orchestrator.rs 的 `agent_type`/`create_task` 旧签名错误(Task 5/8 修)。

- [ ] **Step 12: Commit**

```bash
git add src/storage.rs src/error.rs src/server.rs
git commit -m "feat(storage): create_task emits task_created + head query migration + get_task_state (Plan B)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 5: Service——状态迁移函数 + 新 create_task 签名(service.rs)

**Files:**
- Modify: `src/service.rs`

> **依赖**:Task 4。本 Task 修复 service.rs 编译错误 + 加状态迁移逻辑。

- [ ] **Step 1: 写失败测试——create_task 新签名 + 状态迁移 + 非法迁移拒绝**

service.rs 当前无 test 模块。在文件末尾加:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{EventType, Provenance, TaskState};

    // 复用 storage 测试的 logdbd harness 模式(CLAUDE.md: 测试内联 per 模块)
    use logdbd::catalog::Catalog;
    use logdbd::consumer::ConsumerTracker;
    use logdbd::pb::log_db_service_server::LogDbServiceServer;
    use logdbd::service::LogDbServiceImpl;
    use logdbd::storage::Storage;
    use logdbd::subscribe::SubscribeHub;
    use logdbd::LogDb;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    fn test_provenance() -> Provenance {
        Provenance {
            source_channel: "api".into(),
            source_session_id: None,
            source_user_id: Some("u".into()),
            source_tenant_id: Some("t".into()),
            source_message_id: None,
            created_at: chrono::Utc::now(),
            created_by: "test".into(),
        }
    }

    async fn setup() -> (crate::storage::LogdbdEventStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = logdb::Config::default();
        cfg.data_dir = dir.path().to_path_buf();
        cfg.durability_mode = logdb::DurabilityMode::Sync;
        cfg.ring_size = 256;
        cfg.shards = 1;
        cfg.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(cfg).unwrap();
        let storage = Arc::new(Storage::new(db, 1));
        let catalog = Arc::new(Catalog::open(dir.path()).unwrap());
        let subscribe_hub = Arc::new(SubscribeHub::new());
        let consumer_tracker = Arc::new(ConsumerTracker::new(None));
        let svc = LogDbServiceImpl::new(
            Arc::clone(&storage), Arc::clone(&catalog),
            Arc::clone(&consumer_tracker), Arc::clone(&subscribe_hub),
            "test-node".into(), "primary".into(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            Server::builder().add_service(LogDbServiceServer::new(svc))
                .serve_with_incoming(TcpListenerStream::new(listener)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let store = crate::storage::LogdbdEventStore::connect(&addr, "fixus-svc-test").await.unwrap();
        (store, dir)
    }

    async fn wait_seq(store: &crate::storage::LogdbdEventStore, sid: &str, expected: i64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(v) = store.get_max_seq(sid).await { if v >= expected { return; } }
            if std::time::Instant::now() >= deadline { panic!("seq not reached"); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn create_task_assigns_id_and_created_state() {
        let (store, _d) = setup().await;
        let prov = test_provenance();
        let (tid, ev) = create_task(&*store, "db.repair", &prov, None).await.unwrap();
        assert_eq!(ev.event_type, EventType::TaskCreated);
        assert!(tid.starts_with("task_"));
        wait_seq(&store, &tid, 1).await;
        assert_eq!(store.get_task_state(&tid).await.unwrap(), Some(TaskState::Created));
    }

    #[tokio::test]
    async fn lifecycle_transitions_enforce_invariants() {
        let (store, _d) = setup().await;
        let prov = test_provenance();
        let (tid, _) = create_task(&*store, "db.repair", &prov, None).await.unwrap();
        wait_seq(&store, &tid, 1).await;

        // 非法:created → claimed(跳过 ready)
        let err = claim_task(&*store, &tid, "fixlet-1").await;
        assert!(matches!(err, Err(crate::error::AppError::InvalidTaskStateTransition { .. })));

        // 合法:created → ready
        mark_task_ready(&*store, &tid).await.unwrap();
        wait_seq(&store, &tid, 2).await;
        assert_eq!(store.get_task_state(&tid).await.unwrap(), Some(TaskState::Ready));

        // ready → claimed
        claim_task(&*store, &tid, "fixlet-1").await.unwrap();
        wait_seq(&store, &tid, 3).await;
        assert_eq!(store.get_task_state(&tid).await.unwrap(), Some(TaskState::Claimed));

        // claimed → executing
        start_executing(&*store, &tid).await.unwrap();
        wait_seq(&store, &tid, 4).await;
        // 注:executing 由 turn_started 派生;无 turn_started 时 start_executing 写的仍是 claimed 级事件
        // —— 见 Step 3 说明,这里 state 查询取决于实现。

        // executing → succeeded
        succeed_task(&*store, &tid, "done").await.unwrap();
        wait_seq(&store, &tid, 5).await;
        assert_eq!(store.get_task_state(&tid).await.unwrap(), Some(TaskState::Succeeded));

        // 终态不可再迁
        let err = mark_task_ready(&*store, &tid).await;
        assert!(matches!(err, Err(crate::error::AppError::InvalidTaskStateTransition { .. })));
    }

    #[tokio::test]
    async fn cancel_from_blocked_returns_to_ready_via_nuntius() {
        let (store, _d) = setup().await;
        let prov = test_provenance();
        let (tid, _) = create_task(&*store, "db.repair", &prov, None).await.unwrap();
        wait_seq(&store, &tid, 1).await;
        mark_task_ready(&*store, &tid).await.unwrap();
        wait_seq(&store, &tid, 2).await;
        claim_task(&*store, &tid, "fixlet-1").await.unwrap();
        wait_seq(&store, &tid, 3).await;
        start_executing(&*store, &tid).await.unwrap();
        wait_seq(&store, &tid, 4).await;
        block_task(&*store, &tid, "need human input").await.unwrap();
        wait_seq(&store, &tid, 5).await;
        // blocked → ready(nuntius 语义 gate)
        mark_task_ready(&*store, &tid).await.unwrap();
        wait_seq(&store, &tid, 6).await;
        assert_eq!(store.get_task_state(&tid).await.unwrap(), Some(TaskState::Ready));
    }
}
```

> **注意** `start_executing` 的语义:见 Step 3 决策。测试中 executing 态的断言以 Step 3 实现为准——若 `start_executing` 不单独发事件(claim 即 executing),则该 test 的 state 断言调整为 Claimed。**实施时按 Step 3 选定实现校准此断言。**

- [ ] **Step 2: 运行验证失败**

Run: `cargo test --lib service::tests`
Expected: 编译失败(新函数未定义)。

- [ ] **Step 3: 实现——新 create_task 签名 + 6 个迁移函数**

**决策(claimed vs executing)**:claim 协议中 fixlet claim 后立即派发 execute_turn(随之 turn_started),故 `task_claimed` 紧跟 `turn_started`,Executing 是派生态。`start_executing` **不单独发事件**——它仅用于 service 层语义占位/未来扩展,本 plan 实现为"校验当前态 ∈ {Claimed} 后返回 Ok(不发事件)"(真正的 Executing 由 orchestrator 派发后 turn_started 自然产生)。

> 若嫌 `start_executing` 空实现怪异,可改为 orchestrator 直接在 claim 后派发(claim_task 内部不调 start_executing),移除该 service fn。**推荐:保留为 no-op 语义锚点(校验态),orchestrator claim 流程显式派发。** Step 1 测试中 `start_executing` 后不断言 Executing(已注释)。

替换 `service.rs` 顶部 `create_task` 函数为:

```rust
/// 创建新 Task(spec §8.4)
///
/// fixus 分配 task_id、存 head、发 task_created 事件。state 初始 = Created。
/// tenant_id/user_id 从 provenance 派生。
pub async fn create_task(
    store: &dyn EventStore,
    task_type: &str,
    provenance: &crate::models::Provenance,
    body: Option<&serde_json::Value>,
) -> Result<(String, AgentEvent)> {
    let tenant_id = provenance
        .source_tenant_id
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let user_id = provenance.source_user_id.clone().unwrap_or_default();
    store
        .create_task(task_type, &tenant_id, &user_id, provenance, body)
        .await
}
```

移除旧的 `end_task` 函数(整段删除)。

在 `create_task` 之后加状态迁移辅助。先加一个内部不变量校验 helper:

```rust
/// 校验并执行状态迁移:读当前态 → 校验合法 → 写迁移事件。
async fn transition_task(
    store: &dyn EventStore,
    task_id: &str,
    target: crate::models::TaskState,
    event_type: EventType,
    payload: serde_json::Value,
) -> Result<AgentEvent> {
    let current = store
        .get_task_state(task_id)
        .await?
        .ok_or_else(|| AppError::TaskNotFound(task_id.to_string()))?;

    if current.is_terminal() {
        return Err(AppError::InvalidTaskStateTransition {
            task_id: task_id.to_string(),
            from: current.as_str().into(),
            to: target.as_str().into(),
        });
    }
    if !crate::models::TaskState::can_transition(current, target) {
        return Err(AppError::InvalidTaskStateTransition {
            task_id: task_id.to_string(),
            from: current.as_str().into(),
            to: target.as_str().into(),
        });
    }

    let event = AgentEvent::new(task_id.to_string(), None, None, event_type, payload);
    let seq = store.write_event(&event).await?;
    Ok(AgentEvent { seq, ..event })
}

/// nuntius:readiness 通过,created → ready(spec §4.1 语义 gate)
pub async fn mark_task_ready(store: &dyn EventStore, task_id: &str) -> Result<AgentEvent> {
    transition_task(
        store, task_id,
        crate::models::TaskState::Ready,
        EventType::TaskReady,
        serde_json::json!({}),
    ).await
}

/// executor claim:ready → claimed
pub async fn claim_task(
    store: &dyn EventStore,
    task_id: &str,
    claimant: &str,
) -> Result<AgentEvent> {
    transition_task(
        store, task_id,
        crate::models::TaskState::Claimed,
        EventType::TaskClaimed,
        serde_json::json!({ "claimant": claimant }),
    ).await
}

/// claimed → executing(语义锚点:不发事件;Executing 由后续 turn_started 派生)
pub async fn start_executing(store: &dyn EventStore, task_id: &str) -> Result<()> {
    let current = store
        .get_task_state(task_id)
        .await?
        .ok_or_else(|| AppError::TaskNotFound(task_id.to_string()))?;
    if !matches!(
        current,
        crate::models::TaskState::Claimed | crate::models::TaskState::Executing
    ) {
        return Err(AppError::InvalidTaskStateTransition {
            task_id: task_id.to_string(),
            from: current.as_str().into(),
            to: "executing".into(),
        });
    }
    Ok(())
}

/// executing → blocked(executor 请求人工)
pub async fn block_task(
    store: &dyn EventStore,
    task_id: &str,
    reason: &str,
) -> Result<AgentEvent> {
    transition_task(
        store, task_id,
        crate::models::TaskState::Blocked,
        EventType::TaskBlocked,
        serde_json::json!({ "reason": reason }),
    ).await
}

/// executing → succeeded
pub async fn succeed_task(
    store: &dyn EventStore,
    task_id: &str,
    reason: &str,
) -> Result<AgentEvent> {
    transition_task(
        store, task_id,
        crate::models::TaskState::Succeeded,
        EventType::TaskSucceeded,
        serde_json::json!({ "reason": reason }),
    ).await
}

/// executing → failed
pub async fn fail_task(
    store: &dyn EventStore,
    task_id: &str,
    reason: &str,
) -> Result<AgentEvent> {
    transition_task(
        store, task_id,
        crate::models::TaskState::Failed,
        EventType::TaskFailed,
        serde_json::json!({ "reason": reason }),
    ).await
}

/// 任意活态 → canceled(用户放弃/取消)
pub async fn cancel_task(
    store: &dyn EventStore,
    task_id: &str,
    reason: &str,
) -> Result<AgentEvent> {
    let current = store
        .get_task_state(task_id)
        .await?
        .ok_or_else(|| AppError::TaskNotFound(task_id.to_string()))?;
    if !crate::models::TaskState::can_transition(current, crate::models::TaskState::Canceled) {
        return Err(AppError::InvalidTaskStateTransition {
            task_id: task_id.to_string(),
            from: current.as_str().into(),
            to: "canceled".into(),
        });
    }
    let event = AgentEvent::new(
        task_id.to_string(), None, None,
        EventType::TaskCanceled,
        serde_json::json!({ "reason": reason }),
    );
    let seq = store.write_event(&event).await?;
    Ok(AgentEvent { seq, ..event })
}
```

- [ ] **Step 4: 修复 service.rs 内对旧 create_task/end_task 签名的调用**

`rg -n 'service::create_task\|service::end_task\|create_task(' src/` 找所有 call site。`get_task_info` 等保留。`server.rs::create_session_handler` 调 `service::create_task(旧签名)` —— **Task 8 修**(本 step 暂留编译错误,因 server 属 Task 8)。本 step 确保除 server.rs 外 service.rs 自身编译。

- [ ] **Step 5: 运行 service 测试**

Run: `cargo test --lib service::tests`
Expected: 3 个 test PASS。

- [ ] **Step 6: Commit**

```bash
git add src/service.rs
git commit -m "feat(service): Task state-transition functions + new create_task signature (Plan B)

- create_task(task_type, provenance, body) → fixus assigns task_id
- mark_task_ready / claim_task / start_executing / block_task / succeed_task / fail_task / cancel_task
- transition_task enforces TaskState::can_transition invariants
- removes end_task (superseded by cancel/succeed/fail)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 6: task_registry——task_type 重命名 + claim 队列(task_registry.rs)

**Files:**
- Modify: `src/task_registry.rs`

> 纯内存逻辑,不依赖 store,可独立测试。

- [ ] **Step 1: 写失败测试——claim 队列 + preferred_claimant 优先**

在 task_registry.rs tests 内追加:

```rust
    #[tokio::test]
    async fn test_claim_queue_fifo_with_preferred_claimant() {
        let registry = TaskRegistry::new();

        // 两个 ready 的同 task_type 任务
        registry.enqueue_ready("task_a".into(), "db.repair".into(), None).await;
        registry.enqueue_ready("task_b".into(), "db.repair".into(), None).await;

        // fixlet-1 claim:FIFO → task_a
        let c1 = registry.claim_next("db.repair", "fixlet-1").await;
        assert_eq!(c1.map(|c| c.task_id), Some("task_a".into()));

        // 空:无 task_type 匹配
        assert!(registry.claim_next("other.type", "fixlet-1").await.is_none());

        // task_b 仍 ready
        let c2 = registry.claim_next("db.repair", "fixlet-2").await;
        assert_eq!(c2.map(|c| c.task_id), Some("task_b".into()));
        assert!(registry.claim_next("db.repair", "fixlet-3").await.is_none());
    }

    #[tokio::test]
    async fn test_blocked_requeue_prefers_original_claimant() {
        let registry = TaskRegistry::new();
        // blocked 恢复回 ready,带 preferred_claimant=fixlet-1
        registry.enqueue_ready("task_x".into(), "db.repair".into(), Some("fixlet-1".into())).await;
        registry.enqueue_ready("task_y".into(), "db.repair".into(), None).await;

        // fixlet-1 claim:优先 preferred → task_x
        let c = registry.claim_next("db.repair", "fixlet-1").await.unwrap();
        assert_eq!(c.task_id, "task_x");

        // fixlet-2 claim(无 preferred 匹配):task_y
        let c = registry.claim_next("db.repair", "fixlet-2").await.unwrap();
        assert_eq!(c.task_id, "task_y");
    }

    #[tokio::test]
    async fn test_register_by_task_type_rename() {
        let registry = TaskRegistry::new();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register_fixlet("db.repair", tx).await;
        assert!(registry.get_fixlet_for_task_type("db.repair").await.is_some());
    }
```

同时把现有 4 个 test 里的 `agent_type`/`get_fixlet_for_agent_type`/`send_to_fixlet_for_agent_type`/`PendingTurn::new(.. "database.repair".into() ..)` 改为新名(见 Step 3 重命名)——即 `PendingTurn` 的 `agent_type` 字段→`task_type`,`get_fixlet_for_agent_type`→`get_fixlet_for_task_type`,`send_to_fixlet_for_agent_type`→`send_to_fixlet_for_task_type`。现有 test `test_register_and_send` 等改调新方法名。

- [ ] **Step 2: 运行验证失败**

Run: `cargo test --lib task_registry::tests`
Expected: 编译失败(新方法未定义)。

- [ ] **Step 3: 重命名 agent_type → task_type(registry 内部)**

全文件替换:
- `by_agent_type` → `by_task_type`
- `register_fixlet(&self, agent_type: &str,` → `register_fixlet(&self, task_type: &str,`(参数名)
- `unregister_fixlet(&self, agent_type: &str)` → 参数名 `task_type`
- `get_fixlet_for_agent_type` → `get_fixlet_for_task_type`
- `send_to_fixlet_for_agent_type` → `send_to_fixlet_for_task_type`
- `PendingTurn.agent_type` 字段 → `task_type`;`PendingTurn::new(task_id, task_type, turn_id, redo_group)` 参数名
- 注释/log 字符串里的 `agent_type` → `task_type`

用 `sed -i 's/by_agent_type/by_task_type/g; s/get_fixlet_for_agent_type/get_fixlet_for_task_type/g; s/send_to_fixlet_for_agent_type/send_to_fixlet_for_task_type/g' src/task_registry.rs`,再手工改方法签名参数名 + PendingTurn 字段名 + 测试。

- [ ] **Step 4: 加 claim 队列字段 + enqueue/claim_next**

在 `TaskRegistry` struct 加字段:

```rust
pub struct TaskRegistry {
    by_task_type: RwLock<HashMap<String, WsSender>>,
    active_turns: RwLock<HashMap<String, PendingTurn>>,
    /// task_type → 待认领的 ready Task 队列(FIFO;preferred_claimant 项优先匹配同 claimant)
    ready_queue: RwLock<HashMap<String, Vec<ReadyTask>>>,
}

/// 队列中的 ready Task
#[derive(Debug, Clone)]
pub struct ReadyTask {
    pub task_id: String,
    pub task_type: String,
    /// blocked 恢复时,优先 offer 给原执行器(spec §4.3)
    pub preferred_claimant: Option<String>,
}

/// claim 匹配结果
#[derive(Debug, Clone)]
pub struct ClaimedTask {
    pub task_id: String,
    pub task_type: String,
}
```

`TaskRegistry::new` 加 `ready_queue: RwLock::new(HashMap::new()),`。

加方法:

```rust
    /// 把 ready Task 入队(orchestrator 在 mark_task_ready 后调用)
    pub async fn enqueue_ready(
        &self,
        task_id: String,
        task_type: String,
        preferred_claimant: Option<String>,
    ) {
        let mut q = self.ready_queue.write().await;
        q.entry(task_type.clone())
            .or_default()
            .push(ReadyTask { task_id, task_type, preferred_claimant });
    }

    /// claimant 认领一个 task_type 的 ready Task。
    /// 优先返回 preferred_claimant == claimant 的项;否则 FIFO。
    pub async fn claim_next(
        &self,
        task_type: &str,
        claimant: &str,
    ) -> Option<ClaimedTask> {
        let mut q = self.ready_queue.write().await;
        let queue = q.get_mut(task_type)?;
        if queue.is_empty() {
            return None;
        }
        // 优先 preferred_claimant 匹配
        let pos = queue
            .iter()
            .position(|r| r.preferred_claimant.as_deref() == Some(claimant))
            .unwrap_or(0);
        let ready = queue.remove(pos);
        Some(ClaimedTask {
            task_id: ready.task_id,
            task_type: ready.task_type,
        })
    }
```

> **restart rehydrate 缺口**(本 plan OUT):fixus 重启后 `ready_queue` 为空,需从 `get_task_state == Ready` 的事件流重建。Plan D e2e 处理。此处 `// TODO(Plan D): rehydrate ready_queue on boot` 注释标注。

- [ ] **Step 5: 运行 registry 测试**

Run: `cargo test --lib task_registry::tests`
Expected: 全 PASS(迁移后的旧 test + 新 3 个 claim test)。

- [ ] **Step 6: Commit**

```bash
git add src/task_registry.rs
git commit -m "feat(registry): rename agent_type→task_type + in-memory claim queue with preferred_claimant (Plan B)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 7: Protocol——Claim 消息类型(protocol.rs)

**Files:**
- Modify: `src/protocol.rs`

> **注意**:现有 `session_id` 字段名保留(wire-compat,值=task_id)。本 task 只加 claim 消息。

- [ ] **Step 1: 写失败测试——claim 消息序列化**

protocol.rs tests 内追加:

```rust
    #[test]
    fn test_claim_messages_serialization() {
        // fixlet → fixus: claim
        let claim = serde_json::json!({
            "type": "claim",
            "task_type": "db.repair",
            "claimant": "fixlet-1",
        });
        let parsed: ClaimRequest = serde_json::from_value(claim).unwrap();
        assert_eq!(parsed.task_type, "db.repair");
        assert_eq!(parsed.claimant, "fixlet-1");

        // fixus → fixlet: claim_granted(下发任务)
        let granted = ClaimGranted {
            session_id: "task_abc".into(), // wire 字段名保留 session_id(值=task_id)
            task_type: "db.repair".into(),
            task_brief: "目标:对 db1 执行全量修复".into(),
            context: TurnContext { summary: String::new(), messages: vec![] },
        };
        let json = serde_json::to_string(&granted).unwrap();
        assert!(json.contains("claim_granted"));
        assert!(json.contains("task_abc"));
        assert!(json.contains("db.repair"));

        // fixus → fixlet: claim_denied(无 ready 任务)
        let denied = ClaimDenied { reason: "no ready task".into() };
        let json = serde_json::to_string(&denied).unwrap();
        assert!(json.contains("claim_denied"));
    }
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test --lib protocol::tests::test_claim`
Expected: 编译失败(`ClaimRequest` 等未定义)。

- [ ] **Step 3: 加 Claim 消息类型**

在 `TurnExecutionDone` 之后加:

```rust
/// Claim 请求(fixlet → fixus)——执行器认领一个 task_type 的 ready Task
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "claim")]
pub struct ClaimRequest {
    pub task_type: String,
    pub claimant: String,
}

/// Claim 授予(fixus → fixlet)——下发认领到的 Task(含 task_brief 作初始输入)
///
/// `session_id` 为 wire 字段名(值 = task_id);改名留后续 plan(避免 break nuntius)。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "claim_granted")]
pub struct ClaimGranted {
    pub session_id: String,
    pub task_type: String,
    /// task_brief(body 编译产物),作首个 turn 的 user_input
    pub task_brief: String,
    #[serde(default, flatten)]
    pub context: TurnContext,
}

/// Claim 拒绝(fixus → fixlet)——无匹配 ready Task
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "claim_denied")]
pub struct ClaimDenied {
    pub reason: String,
}
```

> `ClaimGranted` 用 `#[serde(flatten)]` 内嵌 TurnContext 需 TurnContext 字段都 `#[serde(default)]`(已是)。若 flatten 与 tag 冲突编译不过,改为展开字段(summary + messages)直接列在 ClaimGranted 内。**实施时若 flatten 报错,展开为显式字段。**

- [ ] **Step 4: 运行测试**

Run: `cargo test --lib protocol::tests::test_claim`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/protocol.rs
git commit -m "feat(protocol): add Claim/ClaimGranted/ClaimDenied messages (Plan B)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 8: Orchestrator + Server——路由改名 + claim 接线 + HTTP 端点 + fixlet register

**Files:**
- Modify: `src/orchestrator.rs`(resolve_task_type + claim 派发)
- Modify: `src/server.rs`(register 字段 + claim handler + HTTP 端点 + /end→cancel + SessionInfo)
- Modify: `src/bin/fixlet/router.rs`(register 字段 task_type)

> **依赖**:Task 1-7。本 Task 修复剩余编译错误 + 接线 claim + 收尾。

- [ ] **Step 1: orchestrator——resolve_agent_type → resolve_task_type**

`src/orchestrator.rs`:
- `rg -n 'resolve_agent_type|\.agent_type|agent_type' src/orchestrator.rs` 找全部(约 12 处)。
- 方法名 `resolve_agent_type` → `resolve_task_type`,内部 `session.agent_type` → `session.task_type`。
- 局部变量 `agent_type` → `task_type`。
- `registry.get_fixlet_for_agent_type(&x)` → `get_fixlet_for_task_type`。
- `registry.send_to_fixlet_for_agent_type(&x, ..)` → `send_to_fixlet_for_task_type`。
- `PendingTurn::new(.., agent_type.clone(), ..)` → 传 `task_type.clone()`。
- log 字符串里 `agent_type` → `task_type`。

用:
```bash
sed -i 's/resolve_agent_type/resolve_task_type/g; s/get_fixlet_for_agent_type/get_fixlet_for_task_type/g; s/send_to_fixlet_for_agent_type/send_to_fixlet_for_task_type/g' src/orchestrator.rs
```
再手工改 `.agent_type` → `.task_type`、局部变量名、log 字符串。

- [ ] **Step 2: orchestrator——加 claim 派发方法**

在 `impl Orchestrator` 内(`dispatch_execute_turn_with_ctx` 之后)加:

```rust
    /// 处理 fixlet claim 请求:匹配 ready Task → claim_task → 派发 execute_turn。
    /// task_brief 从 Task.body 取(无则空串)。
    pub async fn handle_claim(
        &self,
        task_type: &str,
        claimant: &str,
    ) -> Result<ClaimOutcome> {
        // 1. 从 registry claim 队列匹配
        let Some(claimed) = self.registry.claim_next(task_type, claimant).await else {
            return Ok(ClaimOutcome::Denied {
                reason: format!("no ready task for task_type {}", task_type),
            });
        };

        // 2. service 层写 task_claimed(校验状态不变量)
        if let Err(e) = service::claim_task(&*self.store, &claimed.task_id, claimant).await {
            tracing::warn!(
                "claim_task failed for {}: {} (state race?)",
                claimed.task_id, e
            );
            // 状态迁移失败(可能被别的 claimant 抢)→ 重新入队?不,丢弃让重新 ready。
            return Ok(ClaimOutcome::Denied {
                reason: format!("claim transition failed: {}", e),
            });
        }

        // 3. 取 task_brief(body 编译产物)
        let task = self
            .store
            .get_task(&claimed.task_id)
            .await?
            .ok_or_else(|| AppError::TaskNotFound(claimed.task_id.clone()))?;
        let task_brief = task
            .body
            .as_ref()
            .and_then(|b| b.get("task_brief"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // 4. 派发 execute_turn(task_brief 作 user_input)— 复用现有 turn 机制
        //    注:此处用同步 execute_turn 简化;真正 executing 态由其内部 turn_started 派生。
        //    (异步派发见 start_turn_async;此处为 claim 同步入口)
        Ok(ClaimOutcome::Granted {
            task_id: claimed.task_id,
            task_type: claimed.task_type,
            task_brief,
        })
    }
```

加 outcome 枚举(orchestrator.rs 顶部,`AsyncTurnStart` 旁):

```rust
/// claim 处理结果
#[derive(Debug)]
pub enum ClaimOutcome {
    Granted {
        task_id: String,
        task_type: String,
        task_brief: String,
    },
    Denied {
        reason: String,
    },
}
```

> **设计注**:`handle_claim` 只做匹配 + 写 task_claimed + 取 brief,把 `ClaimGranted` 消息下发交 server 层(因下发要走 claimant 的 WS sender,在 server 的 WS handler 里更自然)。server 层拿到 `ClaimOutcome::Granted` 后构造 `ClaimGranted` 并 `send_to_fixlet_for_task_type`。** orchestrator 不直接持 registry 的 WS 下发 claim_granted——保持职责清晰。**

- [ ] **Step 3: server——register 字段 agent_type → task_type**

`handle_fixlet_ws` 内 `"register" =>` 分支:

```rust
                                "register" => {
                                    if let Some(tt) = parsed.get("task_type").and_then(|v| v.as_str()) {
                                        tracing::info!("fixlet registered for task_type {}", tt);
                                        current_agent_type = Some(tt.to_string()); // 变量名可留,语义=task_type
                                        state.registry.register_fixlet(tt, msg_tx.clone()).await;
                                    } else {
                                        tracing::warn!("fixlet register missing task_type: {}", text_str);
                                    }
                                }
```

(变量 `current_agent_type` 可重命名为 `current_task_type`,亦可留名——本 plan 留名降 churn,加注释。)

加 `"claim"` 分支(在 `"turn_execution_done"` 旁):

```rust
                                "claim" => {
                                    let task_type = parsed["task_type"].as_str().unwrap_or("");
                                    let claimant = parsed["claimant"].as_str().unwrap_or("");
                                    match orch.handle_claim(task_type, claimant).await {
                                        Ok(crate::orchestrator::ClaimOutcome::Granted { task_id, task_type, task_brief }) => {
                                            // 构建初始 context(从事件流)
                                            let ctx = match crate::context::build_llm_context(&*state.store, &task_id).await {
                                                Ok(c) => c,
                                                Err(e) => {
                                                    tracing::error!("claim: build context failed for {}: {}", task_id, e);
                                                    continue;
                                                }
                                            };
                                            let granted = serde_json::json!({
                                                "type": "claim_granted",
                                                "session_id": task_id, // wire 名保留
                                                "task_type": task_type,
                                                "task_brief": task_brief,
                                                "context": { "summary": ctx.summary, "messages": ctx.messages },
                                            });
                                            if let Err(e) = state.registry
                                                .send_to_fixlet_for_task_type(&task_type, &granted.to_string()).await {
                                                tracing::error!("claim: send claim_granted failed: {}", e);
                                            }
                                        }
                                        Ok(crate::orchestrator::ClaimOutcome::Denied { reason }) => {
                                            let denied = serde_json::json!({ "type": "claim_denied", "reason": reason });
                                            let _ = state.registry
                                                .send_to_fixlet_for_task_type(task_type, &denied.to_string()).await;
                                        }
                                        Err(e) => tracing::error!("handle_claim error: {}", e),
                                    }
                                }
```

> 注:`build_llm_context` 返回类型确认其字段名(summary/messages)。`rg -n 'pub.*fn build_llm_context\|pub struct.*Context' src/context.rs`。

- [ ] **Step 4: server——HTTP 端点:ready / cancel / state + /end 改 cancel + create_session 适配新签名**

(a) `create_session_handler`:适配 `service::create_task` 新签名。`CreateSessionRequest` 改 `session_id: Option<String>`(给了用,不给 fixus 分配):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    pub agent_type: String,                 // wire 名保留(=task_type)
    #[serde(default)]
    pub session_id: Option<String>,         // 可选;不给则 fixus 分配
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub provenance: Option<crate::models::Provenance>,
}
```

handler:
```rust
async fn create_session_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<CreateSessionRequest>,
) -> Result<Json<ApiResponse<CreateSessionResponse>>, AppError> {
    let tenant_id = headers.get("X-Fixus-Tenant-Id").and_then(|v| v.to_str().ok()).unwrap_or("default");
    let user_id = headers.get("X-Fixus-User-Id").and_then(|v| v.to_str().ok()).unwrap_or("");
    let mut prov = req.provenance.unwrap_or_else(|| crate::models::Provenance {
        source_channel: "api".into(),
        source_session_id: None,
        source_user_id: Some(user_id.to_string()),
        source_tenant_id: Some(tenant_id.to_string()),
        source_message_id: None,
        created_at: chrono::Utc::now(),
        created_by: "api".into(),
    });
    // header 覆盖(back-compat:旧 client 用 header)
    if prov.source_tenant_id.is_none() { prov.source_tenant_id = Some(tenant_id.into()); }
    if prov.source_user_id.is_none() { prov.source_user_id = Some(user_id.into()); }

    let (task_id, _ev) = service::create_task(&*state.store, &req.agent_type, &prov, req.metadata.as_ref()).await?;

    Ok(Json(ApiResponse::ok(CreateSessionResponse {
        session_id: task_id, // wire 名保留
        seq: 1,
    })))
}
```

> **注**:`service::create_task` 若给了 `session_id`(client 指定)而本实现总是 fixus 分配——back-compat 行为:忽略 client 的 session_id(fixus 始终分配)。若需尊重 client id,`storage::create_task` 加 `preferred_id: Option<&str>` 参数。本 plan 选 fixus 始终分配(spec §8.4),client session_id 字段保留但忽略——**在 handler 加注释说明**。

(b) `end_session_handler` → 改调 `cancel_task`:

```rust
async fn end_session_handler(...) -> ... {
    let reason = body.get("reason").and_then(|v| v.as_str()).unwrap_or("client_requested");
    let event = service::cancel_task(&*state.store, &task_id, reason).await?;
    Ok(Json(ApiResponse::ok(serde_json::json!({
        "task_id": event.task_id, "seq": event.seq, "reason": reason,
    }))))
}
```

(c) `SessionInfo` 加 `state` 字段 + `task_type`(替换 agent_type 显示):

```rust
#[derive(Debug, Clone, Serialize)]
struct SessionInfo {
    task_id: String,
    tenant_id: String,
    user_id: String,
    agent_type: String,              // wire 名保留(=task_type)
    state: String,                   // NEW: TaskState.as_str()
    created_at: String,
    metadata: Option<serde_json::Value>,
    is_ended: bool,
    turn_count: i64,
    event_count: i64,
}
```

`get_session_handler` 填充:`agent_type: session.task_type`,`state: session.state.as_str()`。

(d) 新增 3 个端点(handler + route):

```rust
/// POST /api/v1/sessions/{task_id}/ready — nuntius 标记 readiness 通过
async fn mark_ready_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let event = service::mark_task_ready(&*state.store, &task_id).await?;
    // 入 claim 队列
    let task = service::get_task_info(&*state.store, &task_id).await?
        .ok_or_else(|| AppError::TaskNotFound(task_id.clone()))?;
    state.registry
        .enqueue_ready(task_id.clone(), task.task_type, None).await;
    Ok(Json(ApiResponse::ok(serde_json::json!({ "task_id": event.task_id, "seq": event.seq }))))
}

/// GET /api/v1/sessions/{task_id}/state
async fn get_task_state_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let st = state.store.get_task_state(&task_id).await?
        .ok_or_else(|| AppError::TaskNotFound(task_id.clone()))?;
    Ok(Json(ApiResponse::ok(serde_json::json!({ "task_id": task_id, "state": st.as_str() }))))
}
```

route 注册(`build_router` 内,Summary 段后):
```rust
        .route("/api/v1/sessions/{task_id}/ready", post(mark_ready_handler))
        .route("/api/v1/sessions/{task_id}/state", get(get_task_state_handler))
```
(cancel 复用现有 `/end` 路由,不另加。)

- [ ] **Step 5: fixlet router——register 字段 task_type**

`src/bin/fixlet/router.rs` line 252-256:`"agent_type": config.agent_type` → `"task_type": config.task_type`,struct 字段 `agent_type` → `task_type`(line 46),`FIXUS_AGENT_TYPE` env → 保留(或加 `FIXUS_TASK_TYPE` 别名)。`src/bin/fixlet/main.rs` line 42/50 相应改。

> **最小改**:`sed -i 's/"agent_type": config.agent_type/"task_type": config.task_type/' src/bin/fixlet/router.rs`,struct 字段 + main.rs env/config 字段名改 `task_type`。env var `FIXUS_AGENT_TYPE` 可留作 back-compat 别名(读两个)。

- [ ] **Step 6: 全量编译**

Run: `cargo build 2>&1 | tail -30`
Expected: 0 error。若有遗漏的 `agent_type` 引用,`rg -n '\bagent_type\b' src/` 逐一处理(protocol.rs 的 wire `session_id` 保留;EventType::Session* 保留)。

- [ ] **Step 7: 全量测试**

Run: `cargo test --lib && cargo test --bins`
Expected: lib 全 PASS(models/storage/service/registry/protocol tests);bins(fixlet 19 + sandbox-server 4)PASS。

- [ ] **Step 8: 残留扫描确认 scope**

Run: `rg -n '\bagent_type\b' src/`
Expected: 仅剩 protocol.rs 的 `CreateSessionRequest.agent_type`(wire 保留)+ fixlet 注释。无逻辑依赖。

Run: `rg -n 'session_started|session_ended' src/`
Expected: 仅 `EventType::Session*` 变体定义 + check_terminal_uniqueness 的 legacy 分支 + 测试。无 create_task/is_task_ended 的查询依赖。

- [ ] **Step 9: Commit**

```bash
git add src/orchestrator.rs src/server.rs src/bin/fixlet/router.rs src/bin/fixlet/main.rs
git commit -m "feat(orchestrator/server/fixlet): task_type routing + claim dispatch + HTTP ready/state endpoints (Plan B)

- resolve_agent_type → resolve_task_type; routing by task_type
- handle_claim: match ready queue → claim_task → ClaimOutcome
- WS: register field agent_type→task_type; new claim handler
- HTTP: POST /ready (nuntius gate), GET /state; /end → cancel_task
- SessionInfo exposes state + task_type
- fixlet register message field agent_type→task_type (same-repo wire rename)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 9: 端到端验证 + 文档同步

**Files:**
- Modify: `audit/fixus-architecture-audit.md`(Task 状态机章节)
- Modify: `veps/fixus-capability-checklist.md`(新增 Task 级事件 + claim)

- [ ] **Step 1: 跑完整测试套件**

Run: `cargo test --lib --bins 2>&1 | tail -20`
Expected: 全 PASS,无 fail。

- [ ] **Step 2: 启动 dev 栈冒烟(可选,需 logdbd 运行)**

参考 memory `dev-stack-startup`:起 logdbd → `fixus serve` → curl 验证:
```bash
# 创建 task
curl -sX POST localhost:3000/api/v1/sessions -H 'Content-Type: application/json' \
  -d '{"agent_type":"db.repair"}' | jq
# mark ready
curl -sX POST localhost:3000/api/v1/sessions/<task_id>/ready | jq
# 查 state
curl -s localhost:3000/api/v1/sessions/<task_id>/state | jq  # → "ready"
# cancel
curl -sX POST localhost:3000/api/v1/sessions/<task_id>/end -H 'Content-Type: application/json' -d '{"reason":"test"}' | jq
curl -s localhost:3000/api/v1/sessions/<task_id>/state | jq  # → "canceled"
```
Expected: state 流转 created→ready→canceled。

- [ ] **Step 3: 更新 audit 文档**

`audit/fixus-architecture-audit.md`:在模块矩阵/models 章节加"Task 状态机(8 态,7 事件)+ claim 协议(pull-based)";更新 FIXME 列表(若有"Task 级事件缺"之类条目)。

- [ ] **Step 4: 更新能力清单**

`veps/fixus-capability-checklist.md`:HTTP 端点表加 `POST /sessions/{id}/ready`、`GET /sessions/{id}/state`;事件表加 7 个 task_* 事件;加"claim 协议(WS)"。

- [ ] **Step 5: 最终 commit**

```bash
git add audit/fixus-architecture-audit.md veps/fixus-capability-checklist.md
git commit -m "docs: sync audit + capability checklist for Task state machine + claim (Plan B)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Self-Review(写完后自检,已执行)

**1. Spec coverage(§8 fixus 侧):**
- §8.2 7 个 Task 级事件 → Task 2 ✅
- §8.3 routing 改 task_type → Task 6(registry)+ Task 8(orchestrator/server/fixlet)✅
- §8.3 claim 协议 + preferred_claimant → Task 6(queue)+ Task 7(msg)+ Task 8(dispatch)✅
- §8.4 create_task(task_type, provenance, body) + fixus 分配 task_id → Task 4 + Task 5 ✅
- §4 状态机 8 态 + 触发权 → Task 1(can_transition)+ Task 5(transition 函数)✅
- §4.3 blocked→ready 优先原 claimant → Task 6(preferred_claimant)✅
- §4.4 state = event projection → Task 4(get_task_state/derive_task_state)✅

**2. Placeholder scan:** 无 TBD/TODO(除明确标注的 Plan D restart-rehydrate,属有意 out-of-scope)。Step 5 Task 4 有"先 rg 确认 Record 字段"——这是对 logdbd 客户端 API 不确定时的稳妥指令,非占位。

**3. Type consistency:**
- `TaskState` 8 变体全 plan 一致(Created/Ready/Claimed/Executing/Blocked/Succeeded/Failed/Canceled)。
- `service::create_task` 签名 `(task_type, provenance, body) -> (String, AgentEvent)` 在 Task 4(trait)+ Task 5(service)+ Task 8(server)一致。
- `claim_next(task_type, claimant) -> Option<ClaimedTask>`(Task 6)与 orchestrator `handle_claim` 调用(Task 8)一致。
- `transition_task`/`claim_task`/`mark_task_ready` 等命名在 Task 5 定义、Task 8(handler)调用一致。
- `ClaimOutcome::{Granted{task_id,task_type,task_brief}, Denied{reason}}`(Task 2 orchestrator)与 server `handle_claim` match(Task 8)字段一致。

**4. 风险点(实施时留意):**
- Task 4 Step 5:`Record` 字段(seq/event_type)依赖 logdbd 客户端 crate 结构——**实施首步先 `rg` 确认**,不确定时用"7 事件各查 Max seq"fallback。
- Task 7 Step 3:`ClaimGranted` 的 `#[serde(flatten)]` TurnContext 可能与 `#[serde(tag)]` 冲突——报错则展开为显式字段。
- Task 8 Step 4(a):client 指定 `session_id` 被 fixus 忽略——若 Plan D nuntius 依赖 client id,需回填 `preferred_id` 参数(本 plan 注释标注)。
