# CR-1+2:派发调度器(优先级 + per-type 并发闸)

> 日期:2026-07-13
> 来源:合并 [`veps/TODO.md`](../../veps/TODO.md) CR-1(优先级调度)+ CR-2(每 agent 并发上限)
> 关联:CR-3 失败分类(`2026-07-13-cr3-failure-taxonomy-retry-budget.md`)——本 CR 的终态回调依赖 CR-3 的 `fail_task_with_reason`
> 纪律:**先 CR → 再 TDD 写测试 → 最后实施**(本文是第一步)

---

## 1. 问题(代码取证后的真实状态)

### 1.1 派发是即时 produce、broker 流序 FIFO,priority/concurrency 无处注入

取证(`grep priority|concurrent|semaphore|ready_queue|scheduler` 全空):

- `POST /turns` → `orchestrator.execute_turn` → `start_turn`(WAL)→ `run_turn_to_completion` → `dispatch_execute_turn_with_ctx` → **即时 `publish_turn_begin`** 到 `task-begin-{type}` broker 流(`orchestrator.rs:579`)。
- fixlet 以 group `fixlets-{type}` 竞争消费,**按流序(seq)= produce 序 = 请求到达序** 认领。
- `task_registry` **无 claim 队列**(只有 `register/take/complete_pending_turn` 的 oneshot 协调,`task_registry.rs:85/103/109`);`task_ready` 已退化为纯 WAL 状态事件(`server.rs:323` 注释明确)。
- **无 ready_queue / scheduler / dispatch loop / drain**(grep 全空)。

### 1.2 根因:pull-based 认领换掉了排序层

multica 用 DB 队列 `ORDER BY priority DESC, created_at ASC` 天然支持优先级与 `max_concurrent_tasks` 并发闸。fixus 为换 broker pull-based 认领(分布式 claim、preferred_claimant 的好处),**把那层队列换成了 broker 流**,丢失了优先级与并发上限的杠杆。

### 1.3 CR-1 与 CR-2 是同一个缺失的两面

- **优先级**(CR-1):高优先级 turn 要先被认领 ⇒ fixus 必须**按 priority 序 produce**(高优先级拿更低 broker seq)。
- **per-type 并发闸**(CR-2):不要把 fixlet 池打满 ⇒ fixus 要**追踪在途数**,达 `max_concurrent` 就暂停 produce,等在途 turn 终态再放下一个。

两者都需要 **fixus 侧加一个"派发调度器"**:请求入队 → 调度器按容量+优先级决定何时派哪个。故**合并为一个 CR**。

---

## 2. 目标 / 非目标

### 目标

- **G1 priority 真生效**:同 task_type 多个待派 turn 时,高优先级先 produce(先被 fixlet 认领)。
- **G2 并发闸真生效**:per-task_type 在途 turn 达 `max_concurrent` 时,后续 turn 在调度器排队,有在途终态才放行。
- **G3 统一派发入口**:`execute_turn` / `handle_turn_execution_error` 的 Retry / `spawn_background_recovery` 的派发**都经调度器**(一致守容量与优先级)。
- **G4 Task 加 `priority` 字段** + 创建 API 透传;turn 入队时继承其 task 的 priority。
- **G5 TDD**:调度器纯逻辑(入队顺序、容量闸、终态释放)单测 + 端到端集成测试。

### 非目标(显式排除)

- **N1 不做 per-fixlet-instance 粒度**:fixus 看不到各 fixlet 实例的负载,只能 per-task_type 全局在途计数(近似"别打满该 type 的 fixlet 池")。per-instance 需 fixlet 心跳上报,留后续。
- **N2 不做动态 per-type 配置**:v1 用全局 env `FIXUS_MAX_CONCURRENT_PER_TYPE`(默认 6,同 multica);per-type 覆盖留后续。
- **N3 不抢占**:已派发在跑的 turn 不打断;优先级只影响"下一个派哪个"。
- **N4 不分队列超时 / 执行超时**:v1 队列等待计入现有 `turn_timeout`(300s)。低优先级 turn 可能在队列里超时——可接受(可调 timeout),分离留后续。
- **N5 不动 broker / fixlet 协议**:调度器在 fixus 内,produce 目标仍是 `task-begin-{type}` 单流。

---

## 3. 设计

### 3.1 数据模型(`models.rs`)

`Task` 加字段:

```rust
/// 优先级(CR-1),大者优先。默认 0。turn 派发时继承。
#[serde(default)]
pub priority: i32,
```

- `create_task` 签名加 `priority: i32`;`protocol.rs` 的 `CreateSessionRequest` 加 `priority`(可选,默认 0)。
- 投影:`projection.rs` 把 `priority` 纳入 Task head(从 task_created payload 或 head 读)。

### 3.2 调度器组件(新 `src/dispatcher.rs`)

```rust
/// 一个待派发的 turn(经调度器,非直派)。
struct QueuedTurn {
    task_type: String,
    task_id: String,
    turn_id: i64,
    user_input: String,
    redo_group: String,
    redo_count: i32,
    cached_llm: Vec<String>,
    priority: i32,
    seq: u64,              // 入队序号,同 priority 时 FIFO(稳定排序)
}

/// per-task_type 队列状态
struct TypeQueue {
    pending: BinaryHeap<Reverse<(OrderedPriority, Seq)>>, // 大 priority 先、同 priority seq 小先
    in_flight: i32,
}

pub struct Dispatcher {
    queues: Arc<Mutex<HashMap<String /*task_type*/, TypeQueue>>>,
    config: DispatchConfig,                  // max_concurrent_per_type
    notify: Arc<tokio::sync::Notify>,        // 容量释放 / 新入队时唤醒派发循环
}
```

> BinaryHeap 用 `(Reverse(priority_big_first), seq_small_first)` 实现稳定优先队列。

### 3.3 接线(`orchestrator.rs`)

**入口改造**:`execute_turn` / `start_turn_async` / `handle_turn_execution_error` 的 Retry / `spawn_background_recovery` 不再直调 `dispatch_execute_turn_with_ctx`,改为:

1. 构建 `QueuedTurn`(priority 从 task head 读)。
2. `dispatcher.enqueue(queued).await` —— 入队 + 触发 `try_dispatch`。
3. `try_dispatch(type)`:锁内 `while in_flight < max && pending.non_empty()`:pop 最高优先 + `in_flight++`;**锁外** await `dispatch_execute_turn_with_ctx`(produce 到 broker)。produce 失败 ⇒ 走 CR-3 的 `fail_task_with_reason`(也是终态,触发 `on_turn_terminal`)。
4. 调用方仍在 `result_rx` 上等(现有 PendingTurn + `turn_timeout` 机制不变)。

**终态回调**(在途计数维护):
- `handle_turn_execution_done`(turn 完成)⇒ `dispatcher.on_turn_terminal(type)`:`in_flight--` + `try_dispatch`。
- CR-3 的 `fail_task_with_reason`(turn 终态失败)⇒ 同样 `on_turn_terminal`。
- `fail_turn_and_respond`(timeout / channel_closed)⇒ 同样 `on_turn_terminal`。

> 不变量:`in_flight` 每次 `+1`(dispatch 成功)必有对应 `-1`(任一终态路径)。所有终态路径已在 CR-3 收口到 `fail_task_with_reason` / `handle_turn_execution_done`,故回调点集中、不漏。

### 3.4 配置

- `FIXUS_MAX_CONCURRENT_PER_TYPE`(默认 6)—— per-task_type 在途上限。
- `Dispatcher` 注入 `Orchestrator`(类似 CR-3 的 `retry_policy`),`with_dispatch_config` 覆盖。

### 3.5 与 CR-3 的关系

- CR-3 已把所有终态失败收口到 `fail_task_with_reason` + 成功到 `handle_turn_execution_done`——**正好是调度器需要的两个终态回调点**。本 CR 在这两个点(以及 timeout/channel_closed)加 `on_turn_terminal`。两 CR 正交协同。

---

## 4. TDD 测试清单(先写,应失败)

### 4.1 调度器单测(`src/dispatcher.rs` 内联 `#[cfg(test)]`,纯逻辑,不打 broker)

用 `MockDispatcher` 或可注入 `dispatch_fn: Fn` 避免真 produce:

- **优先级序**:入队 3 个 turn(priority 0/5/2)同 type,max=10。`try_dispatch` 派发顺序 = priority 5 → 2 → 0。
- **稳定排序**:两个 priority 相同,先入队先派发(seq 升序)。
- **并发闸**:max=2,入队 5 个 ⇒ 仅 2 个被派发,余 3 留 pending。
- **终态释放**:`on_turn_terminal` 后 `in_flight--`,pending 里的下一个被派发。
- **per-type 隔离**:type A 满(type A in_flight=max)不影响 type B 派发。
- **跨 type 计数独立**:type A 与 type B 各自 in_flight。

### 4.2 集成测试(`src/orchestrator.rs` 现有 harness)

- **优先级端到端**:建 2 个 task(type 同,priority 0 和 9),max=1。先后 POST turn(低优先进队先)。断言:高 priority 的 turn 先 dispatch(publish_turn_begin 调用序 = 高 priority 在前)。用 dispatch 计数 / 序捕获(LogdbdEventStore publish_turn_begin 是 no-op,需在调度器层观测派发顺序)。
- **并发闸端到端**:max=1,POST 2 个 turn,无 fixlet 消费 ⇒ 仅 1 个 in_flight,第 2 个排队;模拟第 1 个 done ⇒ 第 2 个被派发。

> 注:`publish_turn_begin` 在 `LogdbdEventStore` 是 no-op(`storage.rs:115`),无法从 broker 侧观测派发序;集成测试在调度器内部 hook(dispatch_fn 注入)观测。

---

## 5. 实施步骤(测试通过后)

1. `models.rs`:`Task.priority`;`projection.rs` 投影 priority。
2. 新建 `src/dispatcher.rs`:`QueuedTurn` / `TypeQueue` / `Dispatcher` + `enqueue` / `try_dispatch` / `on_turn_terminal`;内联单测(4.1)。
3. `lib.rs`:`pub mod dispatcher;`。
4. `service.rs`:`create_task` 加 `priority`;`protocol.rs` `CreateSessionRequest` 加 `priority`;`server.rs` 创建 handler 透传。
5. `orchestrator.rs`:`Orchestrator` 加 `dispatcher` 字段 + `with_dispatch_config`;`execute_turn`/`start_turn_async`/Retry/`spawn_background_recovery` 派发改走 `dispatcher.enqueue`;`handle_turn_execution_done`/`fail_task_with_reason`/`fail_turn_and_respond` 加 `on_turn_terminal`;集成测试(4.2)。
6. `main.rs`/`server.rs`:env `FIXUS_MAX_CONCURRENT_PER_TYPE` 注入。
7. `cargo test --lib`(跳过 broker_store)全绿 + `cargo build --release`。

---

## 6. 取证附录

| 事实 | 位置 |
|---|---|
| 即时 produce 派发 | `src/orchestrator.rs:579`(`publish_turn_begin`) |
| task_registry 无队列 | `src/task_registry.rs:85/103/109`(仅 pending oneshot) |
| task_ready 退化为 WAL 事件 | `src/server.rs:323` |
| priority/concurrent 全空 | `grep priority\|concurrent\|semaphore\|scheduler` src/ 无命中 |
| 终态收口点(CR-3 已建) | `handle_turn_execution_done` / `fail_task_with_reason` / `fail_turn_and_respond` |
| LogdbdEventStore publish_turn_begin no-op | `src/storage.rs:115` |
| max_concurrent 同 multica | multica `agent.max_concurrent_tasks=6` |

---

## 7. 风险

- **R1 队列改变延迟**:低 priority turn 可能排队等待(计入 turn_timeout,可能超时)。可接受(timeout 可调);分离队列/执行超时留后续(N4)。
- **R2 在途计数泄漏**:若某 turn 终态未回调 `on_turn_terminal`,`in_flight` 不减,容量逐渐萎缩。缓解:所有终态已收口到 3 个回调点(CR-3);timeout 兜底(turn 超时也走 fail_turn_and_respond → on_turn_terminal)。
- **R3 优先级只在"派发槽开放时"重排**:单个 lone turn 立即派发(无需重排);多个待派时按优先级。语义正确,但"已派发在跑"的不抢占(N3)。
- **R4 broker 仍按 produce 序投递**:fixus 现在控制 produce 序(= 优先级序),故 fixlet 认领序正确。无需改 broker/fixlet。
