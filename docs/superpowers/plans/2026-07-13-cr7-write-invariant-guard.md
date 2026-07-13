# CR-7:write 路径不变量守护(defense-in-depth)

> 日期:2026-07-13
> 来源:[`veps/TODO.md`](../../veps/TODO.md) CR-7(storage/projection 级不变量守护强化,Tier 2,**M**)
> 纪律:**先 CR → 再 TDD 写测试 → 最后实施**(本文是第一步)

---

## 1. 问题(代码取证后的真实状态)

### 1.1 不变量校验全在 service 层;production write 路径几乎裸奔

取证:

- 合法迁移表 `TaskState::can_transition(from, to)`(`models.rs:432`)—— **已是纯函数**,但**只有 `service.rs::transition_task`(`:57`)调用**。
- **production 路径 `broker_store.write_event`(`:194`)**:仅 `validate_scope()` + `validate_payload_required_fields()`。**无 task 迁移合法性校验、无终态唯一校验**。⇒ 绕过 service 直接 `write_event` 可写非法迁移(如 Created→Failed)、重复终态。
- **test 路径 `LogdbdEventStore.write_event`**:`check_terminal_uniqueness`(终态唯一 ✓),但仍**无 task 迁移合法性**。
- `projection.apply`(`projection.rs:139`):task 事件**盲设** `self.state = X`(TurnStarted 有静默 guard,但不 error)。

⇒ 防御纵深缺口:**write chokepoint 不校验迁移合法性**。multaca 的 `status CHECK` + 部分唯一索引防的就是这种"绕过 app 层的脏写"。fixus 借其**原则**(write 时强校验),不照搬 SQL。

### 1.2 turn_started 的特殊性

pull-based 模型下 fixus 写 `turn_started` 时 task 从 **Ready 或 Claimed** 直入 Executing(`projection.rs:164`,可跳过 claimed)。但 `can_transition` 只列 `(Claimed, Executing)`,**无 `(Ready, Executing)`**。⇒ guard 不能机械套 can_transition,turn_started 需特判 `Ready|Claimed → Executing`。

### 1.3 已有杠杆

- `can_transition` 纯函数已在 models.rs(无需重写规则表)。
- broker_store 有**投影缓存**(`self.cache`,`write_event:205` 更新)—— read 它得 current state,无需额外 I/O 即可校验。

---

## 2. 目标 / 非目标

### 目标

- **G1 共享纯 guard**:`validate_task_event_transition(current: Option<TaskState>, event_type) -> Result<()>`,集中迁移规则(含 turn_started 特判),可单测。
- **G2 write chokepoint 强校验**:`broker_store.write_event`(production)+ `LogdbdEventStore.write_event`(test)均在写前调 guard,非法迁移 → `LifecycleInvariant`(防绕过 service 脏写)。
- **G3 终态唯一也补到 broker_store**:production 路径此前无终态唯一校验;顺带补(task 终态 + turn/step 终态)。
- **G4 TDD**:guard 纯函数全覆盖(合法/非法矩阵 + turn_started 特判 + None 当前态)+ 集成(broker_store/storage 直接 write 非法迁移被拒)。

### 非目标(显式排除)

- **N1 不改 projection.apply 的宽容性**:projection 用于历史回放,保持宽容(不 error),避免脏历史让 read 全崩。严格性放在 write chokepoint。
- **N2 不做"同类活跃 task 唯一"**:那是 multaca 的 issue+agent 唯一索引语义,fixus 无等价概念(task_id 已全局唯一)。借不上。
- **N3 不动 service 层校验**:service.transition_task 保留(它返回更精确的 `InvalidTaskStateTransition` + 读态语义);guard 是第二道防线,返回 `LifecycleInvariant`。

---

## 3. 设计

### 3.1 `validate_task_event_transition`(models.rs)

```rust
/// write chokepoint 的 task 迁移合法性 guard(CR-7 defense-in-depth)。
/// 非任务事件(llm/tool/turn-非 started)→ Ok(不关心 task 态)。
pub fn validate_task_event_transition(
    current: Option<TaskState>,
    event_type: EventType,
) -> Result<()> {
    use TaskState::*;
    let target = match event_type {
        EventType::TaskCreated => Some(Created),
        EventType::TaskReady => Some(Ready),
        EventType::TaskClaimed => Some(Claimed),
        EventType::TaskBlocked => Some(Blocked),
        EventType::TaskSucceeded => Some(Succeeded),
        EventType::TaskFailed => Some(Failed),
        EventType::TaskCanceled => Some(Canceled),
        EventType::TurnStarted => Some(Executing), // 特判见下
        _ => return Ok(()), // 非任务态事件
    };
    let target = target.unwrap();
    match current {
        None => if event_type == EventType::TaskCreated { Ok(()) }
                else { Err(LifecycleInvariant("非首事件但无当前态(首事件须 task_created)")) },
        Some(from) => {
            let legal = if event_type == EventType::TurnStarted {
                matches!(from, Ready | Claimed) // pull-based 可跳 claimed
            } else {
                TaskState::can_transition(from, target)
            };
            if legal { Ok(()) } else { Err(LifecycleInvariant("非法迁移 from→target")) }
        }
    }
}
```

### 3.2 broker_store.write_event 接线

```rust
async fn write_event(&self, event: &AgentEvent) -> Result<i64> {
    event.validate_scope().map_err(...)?;
    validate_payload_required_fields(...)?;
    // CR-7:task 迁移合法性(defense-in-depth)。current 从投影缓存读(冷缓存=None
    // ⇒ 仅 task_created 合法,符合"首事件"语义)。
    let current = match self.cache.get(&event.task_id).await {
        Some(proj) => Some(proj.read().await.state),
        None => None,
    };
    crate::models::validate_task_event_transition(current, event.event_type.clone())?;
    // ...produce + apply(现有)
}
```

缓存冷=无当前态 ⇒ 仅 task_created 放行;非首事件需缓存已 warm(create_task 已建缓存,正常流程满足)。

### 3.3 LogdbdEventStore.write_event —— 不装(实施时决定)

原计划装,但实施发现 LogdbdEventStore 的 storage-mechanics 测试(`seq_monotonic_and_no_gaps` 写 3 个连续 turn_started、`incomplete_turns_parse_redo_group`、`terminal_uniqueness_rejects_duplicate_turn_terminal`)需直接写**非全生命周期**事件序列测 seq/终态唯一/incomplete,**与 guard 不兼容**(guard 会拒 turn_started 从 Executing 等)。

⇒ **guard 只装 production 路径 `broker_store.write_event`**。LogdbdEventStore 是 test impl,service 层 transition_task 已为它的 service 路径校验;mechanics 测试保持原样。asymmetry 由两者角色不同正当化(production chokepoint 才需要 defense-in-depth)。

### 3.4 终态唯一 —— 由 guard 顺带覆盖

`can_transition` 终态不可迁出 ⇒ task 终态(Succeeded/Failed/Canceled)重复写必被 guard 拒(从终态迁出非法)。**无需单独的 task 终态唯一检查**。turn/step 终态唯一在 LogdbdEventStore 已有(`check_terminal_uniqueness`);broker_store 的 turn/step 终态唯一留后续(需读 turn/step 事件,本 CR 聚焦 task 迁移)。

---

## 4. TDD 测试清单(先写,跑红)

### 4.1 `models.rs` guard 纯函数

- [ ] **`guard_allows_legal_task_transitions`**:Created→Ready、Ready→Claimed、Claimed→Executing(turn_started)、Executing→Succeeded/Failed/Canceled、Blocked→Ready 等全合法。
- [ ] **`guard_rejects_illegal_task_transitions`**:Created→Failed(跳过)、Succeeded→Ready(终态迁出)、Created→Executing(非 turn_started 直跳)被拒。
- [ ] **`guard_turn_started_special_from_ready`**:turn_started 从 Ready 合法(pull-based 跳 claimed);从 Executing/Blocked 非法。
- [ ] **`guard_none_current_only_allows_task_created`**:current=None 时 TaskCreated Ok,其它(含 turn_started)拒。
- [ ] **`guard_ignores_non_task_events`**:llm_invoked/tool_invoked/turn_completed 等 → Ok(不关心 task 态)。

### 4.2 集成(write chokepoint)

- [ ] **`storage_rejects_illegal_transition_via_write_event`**:LogdbdEventStore,建 task(Created),直接 write `task_failed` → `LifecycleInvariant`。
- [ ] **(broker_store 同测,需 live broker → --skip,纯函数层已覆盖)**

### 4.3 性能(`#[ignore] perf_`,§5 必做)

- [ ] **`perf_validate_task_event_transition`**:guard 纯函数 N 次调用,测 ns/次(应 ~ns,O(1) match)。

---

## 5. 实施步骤(perf 写进必做)

- [ ] **CR-7a**:`models.rs` 加 `validate_task_event_transition` + §4.1 单测。
- [ ] **CR-7b**:`broker_store.write_event` 接 guard + 终态唯一;`LogdbdEventStore.write_event` 接 guard。§4.2 集成测试。
- [ ] **CR-7c**:§4.3 perf + 全量 `cargo test --lib -- --skip broker_store` + `cargo build --release`;勾 TODO CR-7。

---

## 6. 证据附录

### 6.1 测试(全绿)

- §4.1 `write_invariant_tests`(guard 纯函数):5/5 —— 合法矩阵 / 非法拒 / turn_started 特判(Ready|Claimed) / None 仅 task_created / 非任务态事件忽略。
- §4.2 write chokepoint:broker_store.write_event 装 guard(production,代码路径 + 编译通过;E2E 需 live broker,`--skip broker_store`)。LogdbdEventStore 不装(§3.3)。guard 逻辑由 §4.1 纯函数全覆盖。
- §4.3 `perf_validate_task_event_transition` 绿。

全量 lib(跳过 broker_store):**83 passed, 0 failed**, 7 ignored(基线 78 → +5 guard + 1 perf)。

### 6.2 性能

```
[perf] validate_task_event_transition  n=50000  p50=91ns  p99=92ns
```

guard 是 O(1) match,~100ns/次,每个任务态事件调一次(罕见)⇒ 可忽略。

### 6.3 构建

`cargo build --release` 成功(74s)。

---

## 7. 风险与权衡

- **R1 broker_store 冷缓存误拒**:冷缓存=current=None ⇒ 非首事件被拒。正常流程 create_task 先 warm 缓存,不触发;异常冷写(罕见)会被拒(可接受,本就是异常)。
- **R2 现有 storage 测试 fixture 写非法序列被新 guard 破坏**:跑测试发现即修 fixture(改成合法序列);projection 测试走 `p.apply` 不经 write_event,不受影响。
- **R3 双层校验开销**:service + write 都校验;task 事件罕见,write 侧多一次缓存读/投影,可忽略。turn/llm/tool 事件不受影响(非任务态事件 guard 直接 Ok)。
- **R4 turn_started 特判偏离 can_transition**:已知(§1.2);特判 `Ready|Claimed` 与 projection 一致。若未来 claimed 复活,需同步 can_transition 与特判。
