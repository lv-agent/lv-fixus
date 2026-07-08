# Plan A:fixus Session → Task rename 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 fixus 的 `Session` 概念重命名为 `Task`(`session_id`→`task_id`,值不变),为后续 Task 状态机/事件/body 打地基。行为完全等价,现有测试全过。

**Architecture:** 纯代码层 rename,不引入新行为/新字段/新事件。`task_id` 复用原 `session_id` 的 opaque UUID 值 → logdbd stream 名不变 → logdbd 零感知。wire 字段名(与 fixlet 通信)暂保留 `session_id`(值兼容),内部 Rust 类型全部改名。

**Tech Stack:** Rust,Tokio,logdbd(gRPC event store)。

**Spec:** `docs/superpowers/specs/2026-07-07-task-model-design.md` §8.1(本 plan 是其拆分执行:YAGNI,state/provenance/body 字段与 Task 级事件分别在 Plan B/C 加,本 plan 只 rename)。

---

## 范围

### In scope(fixus lib + 主二进制,内部类型 rename)

`src/models.rs`、`src/storage.rs`、`src/service.rs`、`src/context.rs`、`src/recovery.rs`、`src/orchestrator.rs`、`src/session_registry.rs`、`src/error.rs`、`src/lib.rs`、`src/server.rs`、`src/main.rs`(若引用)。

### Out of scope(本 plan 不动)

| 项 | 原因 |
|----|------|
| `EventType::SessionStarted` / `SessionEnded` 事件名 | Plan B 引入 `task_*` 事件族时统一演进;本 plan 保留,避免 churn |
| 事件字符串 `"session_started"` / `"session_ended"`(logdbd event_type) | 同上,storage 层写/查仍用旧串 |
| logdbd metadata map key `"session_id"`(storage.rs:353) | opaque 存储键,保留 |
| `protocol.rs` 的 wire 字段名 `session_id` | **wire 契约**:fixlet 二进制不依赖 lib crate、有自己的 wire 类型;改 fixus 单边会 break JSON 反序列化。本 plan 保留 wire 字段名(值 = task_id),Plan C/D 对齐 fixlet 时一并改 |
| HTTP 路径 `/api/v1/sessions/...`、header `X-Fixus-Session-Id` | cosmetic,改 fixus 路径会 break nuntius;留 Plan C/D |
| `src/bin/fixlet/acp.rs` 的 ACP `session` | **不同概念**(Agent Client Protocol 的 session/new 等 JSON-RPC),不是 fixus Task |
| `src/bin/sandbox-server/*` | **不同概念**(沙箱进程工作目录键),独立二进制 |
| `src/bin/fixus-stream/*` | 独立二进制,值兼容即可;其内部 `session_id` 留后续 plan |
| `src/bin/fixlet/router.rs`、`idempotency.rs` 的 `session_id` | fixus 概念,但属 fixlet 二进制;**本 plan 改**(wire 值不变,内部名对齐)—— 见 Task 6 |

### Wire 兼容策略(关键)

fixus ↔ fixlet 通信的 wire 字段名 **保持 `session_id`**(JSON key 不变),仅 fixus 内部 Rust 类型改名。因此:
- `protocol.rs` 的 struct 字段名 `session_id` **不改**(wire 类型原样)。
- fixus 内部代码(model/storage/service 等)用 `task_id`,在序列化/发给 fixlet 时仍写入 wire 字段 `session_id`(由 protocol.rs 的 struct 保证)。
- fixlet 二进制:router.rs/idempotency.rs 的 fixus-session_id 概念可对齐改名(Task 6),但它读到的 wire 字段名不变,值不变 → 兼容。

## 符号 rename 映射

| 旧 | 新 |
|----|-----|
| `Session`(struct) | `Task` |
| `session_id`(字段/参数/变量,内部代码) | `task_id` |
| `create_session`(trait 方法 + service fn) | `create_task` |
| `get_session` | `get_task` |
| `session_exists` | `task_exists` |
| `is_session_ended` | `is_task_ended` |
| `SessionRegistry`(struct + 文件名 `session_registry.rs`) | `TaskRegistry`(`task_registry.rs`) |
| `SessionNotFound` | `TaskNotFound` |
| `SessionAlreadyExists` | `TaskAlreadyExists` |
| `SessionAlreadyEnded` | `TaskAlreadyEnded` |
| `TurnNotFound { session_id, .. }` 等枚举字段 | `{ task_id, .. }` |
| `AgentEvent.session_id` | `AgentEvent.task_id`(内部字段) |

**保留**:`EventType::SessionStarted/SessionEnded`、事件串、metadata key、protocol.rs wire 字段、HTTP 路径/header、注释里的 "session_started"(可顺手改注释,非必须)。

---

## 前置:基线

- [ ] **Step 0:确认基线测试全过**

Run: `cargo test --lib 2>&1 | tail -20`
Expected: 全绿(约 95 个内联测试,含 storage.rs 的 `logdbd_tests`)。这是 rename 等价性的参照基线。若此时有红,先修红再开始。

- [ ] **Step 0b:确认工作区干净**

Run: `git status --short`
Expected: 仅 `?? docs/`(spec/plan 文档)。无 `M` 源文件。

---

## Task 1:models.rs — Session struct → Task + AgentEvent 字段

**Files:** `src/models.rs`

- [ ] **Step 1:rename struct + 字段**

`Session` struct(models.rs:329)→ `Task`;字段 `session_id`→`task_id`。`Session::new`→`Task::new`(参数同步)。`SessionStartedPayload`/`SessionEndedPayload` **保留**(Plan B 演进)。

```rust
pub struct Task {
    pub task_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub agent_type: String,
    pub created_at: DateTime<Utc>,
    pub metadata: Option<serde_json::Value>,
}
```

`AgentEvent`(models.rs:227)字段 `session_id: String` → `task_id: String`。`AgentEvent::new` 签名参数同步。

- [ ] **Step 2:EventType 保留,不动**

`EventType::SessionStarted/SessionEnded` 原样保留。**不要**改成 TaskStarted(Task B 会引入 `task_*` 族)。

- [ ] **Step 3:`validate_scope` 等校验函数里 "Session" 字样**

注释可顺改("Session-level" → "Task-level"),逻辑不动。

- [ ] **Step 4:models.rs 内联测试**

把测试里 `Session {...}` → `Task {...}`、`.session_id` → `.task_id`、`EventType::SessionStarted` 保留。运行:

Run: `cargo test --lib models 2>&1 | tail -20`
Expected: models 模块测试绿(注:此时编译会因下游模块还用旧名而整体失败,故此步仅确认 models.rs 自身改完;完整编译验证在 Task 7)。

> **说明**:跨模块 rename 无法逐文件编译通过(下游引用旧名),故 Task 1-5 连续改完,Task 7 统一 `cargo build` 验证。每 Task 改完只做"该文件内符号一致"的自检。

## Task 2:storage.rs — EventStore trait + LogdbdEventStore impl rename

**Files:** `src/storage.rs`

- [ ] **Step 1:trait 方法名 + 参数**

23 个方法的 `session_id: &str` 参数 → `task_id: &str`;`create_session`→`create_task`、`get_session`→`get_task`、`session_exists`→`task_exists`、`is_session_ended`→`is_task_ended`。

- [ ] **Step 2:impl 内部**

`LogdbdEventStore::create_session`(storage.rs:336)→ `create_task`:内部 `meta.insert("session_id", ...)` 的 **metadata key 字符串保留**(opaque,logdbd 存储),但写入值用 `task_id` 参数。`session_id` 局部变量 → `task_id`。logdbd `append_full(&self.namespace, task_id, "session_started", ...)` —— stream 名用 `task_id` 值(= 原 session_id 值,logdbd 无感),event_type 串 `"session_started"` **保留**。

`get_session`(385)→ `get_task`:query SQL 里 `event_type = 'session_started'` **保留**;重建 `Task { task_id, ... }`(原 `Session`)。

`session_exists`→`task_exists`、`is_session_ended`→`is_task_ended`:query 串保留,变量改名。

- [ ] **Step 3:其余 query 方法**

`get_max_seq`/`get_turn_events`/`get_events_after_seq`/.../`archive_events_before_seq` 等全部 `session_id` 参数 → `task_id`,内部 `client.query(&self.namespace, task_id, ...)` 的 stream 参数用 `task_id` 值。

- [ ] **Step 4:测试模块 `logdbd_tests`**

`store.create_session(sid,...)` → `create_task`;`store.session_exists(sid)` → `task_exists`;`store.get_session(sid)` → `get_task`;`s.session_id` → `s.task_id`;`AgentEvent::new(sid.into(), ...)` 的字段名跟随;`EventType::SessionStarted` 保留。`wait_seq(store, sid, ..)` 参数名可改 `tid`(局部)。

- [ ] **Step 5:自检**

确认 storage.rs 内所有 `session_id`/`Session` 已改(除保留项:`"session_started"` 串、metadata key、注释)。

Run: `rg -n 'session_id|Session\b' src/storage.rs`
Expected:仅命中保留项(event_type 串、metadata key 字符串、注释)。

## Task 3:error.rs + lib.rs

**Files:** `src/error.rs`、`src/lib.rs`

- [ ] **Step 1:error.rs**

`SessionNotFound(String)`→`TaskNotFound(String)`、`SessionAlreadyExists`→`TaskAlreadyExists`、`SessionAlreadyEnded`→`TaskAlreadyEnded`;`TurnNotFound { session_id, turn_id }`→`{ task_id, turn_id }`;`TurnAlreadyTerminal`/`TurnNotStarted`/`SeqGap` 的 `session_id` 字段 → `task_id`。所有构造点 `SessionNotFound(x)` → `TaskNotFound(x)` 同步。

- [ ] **Step 2:lib.rs**

`pub use models::{Session, ...}` → `pub use models::{Task, ...}`;`pub mod session_registry` → `pub mod task_registry`(配合 Task 5 文件改名)。模块 doc "Agent Session Event Store" → "Agent Task Event Store"(顺手)。

## Task 4:context.rs + recovery.rs + orchestrator.rs + service.rs

**Files:** `src/context.rs`、`src/recovery.rs`、`src/orchestrator.rs`、`src/service.rs`

- [ ] **Step 1:context.rs**

`build_llm_context(store, session_id)`→`(store, task_id)`;`full_replay`/`build_turn_context`/`build_recent_turns_context` 同步。内部 `store.get_xxx(session_id, ..)` 跟随 Task 2 的新方法名(`get_events_after_seq(task_id)` 等)。

- [ ] **Step 2:recovery.rs**

`SessionRecoveryState.session_id`→`.task_id`;`RedoContext.session_id`→`.task_id`;`check_session_recovery(store, session_id)`→`(store, task_id)`;`recover_session`→`recover_task`(或保留 `recover_session` 名?为一致改名 `recover_task`)。内部 store 调用跟随新方法名。

- [ ] **Step 3:orchestrator.rs**

~30 个方法的 `session_id: &str` → `task_id: &str`。`build_tool_idempotency_key(session_id, tool_name, args)` → `(task_id, ...)`,内部格式串 `"{session_id}:mcp:{tool_name}:{hash}"` → `"{task_id}:mcp:..."`(**注意:这是幂等键格式,值=task_id 不变,键字符串模板里字面量改名无功能影响**,因为值一样)。所有 `store` 调用跟随新方法名。

- [ ] **Step 4:service.rs**

`create_session`/`end_session`/`get_session_info`/`is_session_ended`/`get_max_turn_id`/`get_max_seq` 的 `session_id` 参数 → `task_id`;函数名 `create_session`→`create_task`、`end_session`→`end_task`、`get_session_info`→`get_task_info`。内部 `store.session_exists`→`task_exists`、`store.create_session`→`create_task` 跟随。`AgentEvent::new(session_id, ...)` 字段跟随。

## Task 5:session_registry.rs → task_registry.rs(文件改名)

**Files:** rename `src/session_registry.rs` → `src/task_registry.rs`;`src/lib.rs`(已在 Task 3 改 mod 名)

- [ ] **Step 1:文件改名 + struct rename**

`mv src/session_registry.rs src/task_registry.rs`。`SessionRegistry`→`TaskRegistry`;`PendingTurn.session_id`→`.task_id`;`active_turns: HashMap<String, PendingTurn>` 注释 `/* session_id */`→`/* task_id */`;`register_pending_turn(session_id, ...)`→`(task_id, ...)`;`take_pending_turn`/`complete_pending_turn` 同步。

- [ ] **Step 2:保留路由语义不变**

按 `agent_type` 路由的逻辑(4-9 行注释)不动 —— 路由键仍是 `agent_type`(= task_type),只是 pending-turn map 的 key 从 session_id 值改为 task_id 值(同值)。one-pending-turn 不变量保持。

## Task 6:server.rs + main.rs(内部 rename,HTTP 路径保留)

**Files:** `src/server.rs`、`src/main.rs`

- [ ] **Step 1:server.rs handler 内部变量**

`Path(session_id): Path<String>` → `Path(task_id): Path<String>`(路径段 `/sessions/{session_id}` 字面量**保留**)。handler 内部 `store.get_session(session_id)` → `get_task(task_id)` 等。`SessionInfo` 响应 struct → `TaskInfo`(或保留名?改名 `TaskInfo`),字段 `session_id`→`task_id`。MCP header `X-Fixus-Session-Id` 读取**保留 header 名**,绑定到 `task_id` 变量。`stream_url_for(session_id, turn_id)` → `(task_id, ...)`,内部路径模板 `/sessions/{id}/turns/{tid}/stream` 字面量保留。

- [ ] **Step 2:main.rs**

若引用 `Session`/`session_registry`,改 `Task`/`task_registry`。doc 注释顺手改。

## Task 7:fixlet 二进制的 fixus-session 概念(router.rs / idempotency.rs)

**Files:** `src/bin/fixlet/router.rs`、`src/bin/fixlet/idempotency.rs`

- [ ] **Step 1:router.rs**

`ExecuteTurnRequest.session_id` 消费 → 变量名 `task_id`(wire 字段名 `session_id` 保留,见 Wire 策略)。`TurnContext.session_id`→`.task_id`(idempotency.rs:132)。派生 ACP sessionId 的 `"{session_id}:turn_{turn_id}"` 模板 → `"{task_id}:turn_..."`(值不变)。

- [ ] **Step 2:idempotency.rs**

`build_idempotency_key(session_id, redo_group, ..)` → `(task_id, ..)`;格式串 `"{session_id}:{redo_group}:..."` → `"{task_id}:..."`(值不变)。注释 "step_id 在 Session 内全局唯一" → "在 Task 内"。

- [ ] **Step 3:acp.rs 不动**

`AcpClient.session_id`(ACP 概念)、`session/new`/`session/prompt`/`session/cancel` JSON-RPC 方法名**全部保留** —— 这是 ACP 协议,不是 fixus Task。

## Task 8:统一编译 + 测试验证

- [ ] **Step 1:cargo build**

Run: `cargo build 2>&1 | tail -30`
Expected: 编译通过(零错误)。若有遗漏的旧名引用,按报错逐个修。

- [ ] **Step 2:cargo test --lib**

Run: `cargo test --lib 2>&1 | tail -25`
Expected: 全绿(等价基线)。`logdbd_tests` 用 in-process logdbd(start_server),无需外部服务。

- [ ] **Step 3:cargo test --bins(fixlet 编译验证)**

Run: `cargo test --bins 2>&1 | tail -20`
Expected: fixlet/sandbox-server/fixus-stream 二进制编译通过(它们 session 概念独立,未改应照常)。fixlet 的 idempotency/router 测试绿。

- [ ] **Step 4:残留扫描**

Run: `rg -n '\bSession\b|session_id' src/ --glob '!docs/**' | grep -v 'session_started\|session_ended\|SessionStarted\|SessionEnded\|"session_id"' | grep -v 'src/bin/sandbox-server\|src/bin/fixus-stream\|src/bin/fixlet/acp.rs'`
Expected: 空或仅注释。若命中真实代码,补改。

- [ ] **Step 5:commit**

```bash
git add -A
git commit -m "rename: fixus Session → Task (session_id→task_id, 等价重构)

Task 取代 Session 成为顶层实体概念。task_id 复用原 session_id 值,
logdbd stream 名不变、wire 字段名保留,行为完全等价。
EventType/wire/HTTP 路径留后续 plan 演进。

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Self-Review(plan 写完后自检)

- **Spec 覆盖**:spec §8.1 的 rename 部分由 Task 1-8 覆盖;state/provenance/body 字段(§3)与 Task 级事件(§8.2)明确推迟到 Plan B/C(YAGNI),本 plan 不做 —— 已在 plan 开头声明。✓
- **占位符**:无 TBD/TODO;每步有具体符号/文件/命令。✓
- **类型一致**:Task struct 字段、trait 方法名在各 Task 间一致(Task→Task,task_id→task_id,create_task→create_task)。✓
- **范围风险**:wire 字段名/HTTP 路径/EventType/metadata key 全部明确"保留",避免 break fixlet/nuntius/logdbd。✓

## 验收

1. `cargo build` + `cargo test --lib` + `cargo test --bins` 全绿。
2. `rg` 残留扫描无真实代码命中(仅保留项)。
3. fixus ↔ fixlet wire 兼容(字段名/值未变,fixlet acp.rs/sandbox/fixus-stream 未动)。
4. 一个 commit,等价重构。
