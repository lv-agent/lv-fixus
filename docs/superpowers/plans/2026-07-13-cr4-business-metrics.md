# CR-4:业务 Prometheus 指标

> 日期:2026-07-13
> 来源:[`veps/TODO.md`](../../veps/TODO.md) CR-4(业务 Prometheus 指标)← 关联 P3「监控指标」
> 前置依赖:CR-3(`fail_task_with_reason` 单一失败收口)、CR-1+2(`Dispatcher` 已持有 in_flight/pending 计数)—— 本 CR 的打点挂在它们暴露的终态/队列回调上
> 纪律:**先 CR → 再 TDD 写测试 → 最后实施**(本文是第一步)

---

## 1. 问题(代码取证后的真实状态)

### 1.1 fixus 零业务指标,turn 超时 300s 调优全凭猜

取证(`grep -rnE "prometheus|metrics::|histogram" src/ Cargo.toml` 全空):

- 只有 `tracing` 日志(`orchestrator.rs` 各处 `tracing::info!`),**无任何数值化指标**。
- `turn_timeout = Duration::from_secs(300)`(`orchestrator.rs:71`)**硬编码、无数据支撑**:不知道 turn 实际跑多久、队列等了多久、有多少超时、retry 预算(CR-3)被烧了多少次。
- `/health`(`server.rs:220`、`health_handler:678`)返回 JSON 健康摘要(broker/sandbox `last_ok_secs_ago`),但**不是 Prometheus 抓取格式**,Prometheus 无法直接消费。

### 1.2 multica 的参照

multica `internal/metrics/business.go`:`taskEnqueued/Dispatched/Started/Terminal/Failed` CounterVec + `taskQueueWait`/`taskRunSeconds` HistogramVec。fixus 抄其**指标语义**,不抄其实现(Go 全局默认 registry;fixus 用**自带 Registry** 实例隔离,便于单测)。

### 1.3 打点位置已就绪(CR-3 + CR-1+2 铺好了回调)

CR-3 + CR-1+2 已经把 turn 生命周期收口到几个明确函数,打点挂在上面即可,**零新增分支逻辑**:

| 阶段 | 收口函数(已存在) | 打点 |
|------|------------------|------|
| 入队 | `enqueue_and_dispatch`(`orchestrator.rs:1000`) | `turn_enqueued_total` + 记 `enqueued_at` |
| 派发 | `dispatch_pending`(`:1032`)`try_pop` 成功后 | `turn_dispatched_total` + `queue_wait` hist + 记 `dispatch_time` |
| 成功终态 | `handle_turn_execution_done`(`:673`) | `terminal_total{success}` + `duration` hist |
| 失败终态 | `fail_task_with_reason`(`:866`,所有失败唯一漏斗) | `terminal_total{failed}` + `duration` hist |
| 重试决策 | `handle_turn_execution_error` Retry 分支(`:803`) | `retry_attempts_total` |
| 负载快照 | `Dispatcher`(已持有 in_flight/pending) | `in_flight` / `pending` Gauge(pull) |
| 依赖健康 | `health()`(`:94`) | `dependency_up` Gauge(pull) |

---

## 2. 目标 / 非目标

### 目标

- **G1 `/metrics` 端点**:Prometheus 文本格式(`text/plain; version=0.0.4`),`GET /metrics`,无需鉴权(同 `/health`)。
- **G2 turn 生命周期指标**:入队数、派发数、队列等待时长、执行时长、终态计数(成功/失败)、重试次数 —— 支撑 timeout 调优 + CR-3 retry 预算可观测。
- **G3 负载快照 Gauge**:per-task_type 在途(in_flight)与排队(pending)深度 —— 一眼看负载。
- **G4 依赖健康 Gauge**:broker / sandbox up(1)/down(0) —— 复用 `health()`,可告警。
- **G5 自带 Registry 隔离**:`Metrics` 结构持有独立 `prometheus::Registry`(非全局默认),每个 `Metrics::new()` 互不污染,**TDD 可断言 `render()` 文本**。
- **G6 零侵入收口**:打点只挂在 CR-3/CR-1+2 已有的终态/队列回调上,不改 turn 状态机、不改 broker 协议。

### 非目标(显式排除)

- **N1 不做分布式指标聚合**:单进程 `Metrics`;多 fixus 节点聚合留 P3「多 fixus 节点」(Prometheus federation / 外部 aggregator 解决)。
- **N2 不做指标鉴权**:`/metrics` 同 `/health` 裸暴露;鉴权随 P3「API 鉴权」统一做。
- **N3 不做 token 用量计量**:那是 CR-8(`runtime_usage` 聚合投影),本 CR 只做 turn 级运行时指标。token 数已在事件里,计量是另一层。
- **N4 不做直方图桶精调**:用 prometheus 默认桶(Duration 默认桶覆盖 0.005..10s + Inf);turn 执行可达 300s,会在 `+Inf` 桶堆积 —— v1 可接受(看 `_count`/`_sum` 仍可调优);自定义长尾桶留后续。
- **N5 不做 feature flag**:prometheus 作为普通依赖引入;指标常开、端点只读、开销极小。
- **N6 不区分 timeout/canceled 子类**:`terminal_total` 的 `outcome` 只取 `success|failed`(timeout 走 fail 路径,failure_reason 已在事件/日志里细分)。保持低基数。

---

## 3. 设计

### 3.1 依赖

`Cargo.toml` 加:

```toml
prometheus = "0.13"
```

只被 `metrics.rs`(`lib`)用;`server.rs` / `orchestrator.rs` 经 `Arc<Metrics>` 引用,不直接碰 prometheus crate API。bins 编译会传递引入(构建成本,非运行耦合)。

### 3.2 `src/metrics.rs` —— 指标目录 + 渲染

```rust
//! 业务 Prometheus 指标(CR-4)。
//! 自带 Registry 实例隔离(非全局默认),便于单测断言 render() 文本。
use std::sync::Arc;
use prometheus::{
    HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_vec_with_registry,
};

/// turn 终态 outcome 标签值
pub const OUTCOME_SUCCESS: &str = "success";
pub const OUTCOME_FAILED: &str = "failed";
/// 依赖标签值
pub const DEP_BROKER: &str = "broker";
pub const DEP_SANDBOX: &str = "sandbox";

/// 业务指标集(自带 Registry,实例隔离)。
#[derive(Clone)]
pub struct Metrics {
    reg: Arc<Registry>,
    turn_enqueued: IntCounterVec,      // {task_type}
    turn_dispatched: IntCounterVec,    // {task_type}
    turn_terminal: IntCounterVec,      // {task_type, outcome}
    retry_attempts: IntCounterVec,     // {task_type}
    task_created: IntCounterVec,       // {task_type}
    queue_wait: HistogramVec,          // {task_type}  秒
    turn_duration: HistogramVec,       // {task_type, outcome}  秒
    in_flight: IntGaugeVec,            // {task_type}
    pending: IntGaugeVec,              // {task_type}
    dependency_up: IntGaugeVec,        // {dependency}
}

impl Metrics {
    pub fn new() -> Arc<Self> { /* 构造 + 全部 register_*_with_registry */ }

    // ── push(打点)──
    pub fn record_turn_enqueued(&self, task_type: &str) { self.turn_enqueued.with_label_values(&[task_type]).inc(); }
    pub fn record_turn_dispatched(&self, task_type: &str, queue_wait_secs: f64) {
        self.turn_dispatched.with_label_values(&[task_type]).inc();
        self.queue_wait.with_label_values(&[task_type]).observe(queue_wait_secs);
    }
    pub fn record_turn_terminal(&self, task_type: &str, outcome: &str, duration_secs: f64) {
        self.turn_terminal.with_label_values(&[task_type, outcome]).inc();
        self.turn_duration.with_label_values(&[task_type, outcome]).observe(duration_secs);
    }
    pub fn record_retry(&self, task_type: &str) { self.retry_attempts.with_label_values(&[task_type]).inc(); }
    pub fn record_task_created(&self, task_type: &str) { self.task_created.with_label_values(&[task_type]).inc(); }

    // ── pull(渲染前 sync)──
    pub fn set_in_flight(&self, task_type: &str, n: i64) { self.in_flight.with_label_values(&[task_type]).set(n); }
    pub fn set_pending(&self, task_type: &str, n: i64) { self.pending.with_label_values(&[task_type]).set(n); }
    pub fn set_dependency_up(&self, dep: &str, up: bool) { self.dependency_up.with_label_values(&[dep]).set(if up {1}else{0}); }

    /// 渲染 Prometheus 文本格式(供 /metrics handler)。
    pub fn render(&self) -> String {
        let mut buf = String::new();
        let encoder = TextEncoder::new();
        for m in self.reg.gather() {
            encoder.encode_utf8(&m, &mut buf).ok();
        }
        buf
    }
}
```

**指标全表**(命名前缀 `fixus_`,标签低基数):

| 指标 | 类型 | 标签 | 打点时机 |
|------|------|------|----------|
| `fixus_turn_enqueued_total` | Counter | task_type | enqueue |
| `fixus_turn_dispatched_total` | Counter | task_type | dispatch |
| `fixus_turn_terminal_total` | Counter | task_type, outcome(success\|failed) | 终态 |
| `fixus_retry_attempts_total` | Counter | task_type | Retry 决策 |
| `fixus_task_created_total` | Counter | task_type | create_task |
| `fixus_turn_queue_wait_seconds` | Histogram | task_type | dispatch(enqueue→dispatch) |
| `fixus_turn_duration_seconds` | Histogram | task_type, outcome | 终态(dispatch→terminal) |
| `fixus_in_flight_turns` | Gauge | task_type | render 前 sync |
| `fixus_pending_turns` | Gauge | task_type | render 前 sync |
| `fixus_dependency_up` | Gauge | dependency(broker\|sandbox) | render 前 sync |

### 3.3 `QueuedTurn.enqueued_at` + `Dispatcher::snapshot`(CR-1+2 扩展)

`dispatcher.rs`:

```rust
pub struct QueuedTurn {
    pub task_type: String,
    pub task_id: String,
    pub turn_id: i64,
    pub user_input: String,
    pub redo_group: String,
    pub redo_count: i32,
    pub cached_llm: Vec<String>,
    pub priority: i32,
    pub enqueued_at: std::time::Instant,   // ← 新增:队列等待计时起点
}

impl Dispatcher {
    /// per-type (pending 深度, in_flight 数) 快照,供 Gauge pull。无该 type → 不含。
    pub fn snapshot(&self) -> Vec<(String, usize, usize)> { /* lock + collect */ }
}
```

`enqueued_at = Instant::now()` 在 `enqueue_and_dispatch` 构造 `QueuedTurn` 时填入。`Instant`(std)保持 dispatcher 纯 std、`Send+Sync`。

### 3.4 orchestrator 打点接线

`Orchestrator` 加两个字段:

```rust
metrics: Arc<Metrics>,                                          // CR-4
dispatch_times: Arc<tokio::sync::Mutex<HashMap<String, std::time::Instant>>>,  // key=`{task_id}:{turn_id}`
```

`new()`:`metrics = Metrics::new()`、`dispatch_times = 默认空 map`。加 `.with_metrics(Arc<Metrics>)` builder(测试可注入共享实例)。所有现存的 `Orchestrator { ... }` 字面量(bg 任务两个)补这两字段。

打点挂载(逐函数):

1. **`enqueue_and_dispatch`**(`enqueue()` 后):
   ```rust
   self.metrics.record_turn_enqueued(&task_type);
   self.dispatcher.enqueue(QueuedTurn { /* … */ enqueued_at: Instant::now() });
   ```
2. **`dispatch_pending`**(`try_pop` 成功 → `dispatch_with_retry` 之前):
   ```rust
   let queue_wait = turn.enqueued_at.elapsed().as_secs_f64();
   self.metrics.record_turn_dispatched(&task_type, queue_wait);
   self.dispatch_times.lock().await.insert(format!("{}:{}", tid, turn_id), Instant::now());
   ```
3. **`handle_turn_execution_done`**(WAL `complete_turn` 后,`release_slot` 前):
   ```rust
   if let Some(t0) = self.dispatch_times.lock().await.remove(&key) {
       let dur = t0.elapsed().as_secs_f64();
       let tt = self.resolve_task_type(task_id).await.unwrap_or_else(|_| "unknown".into());
       self.metrics.record_turn_terminal(&tt, OUTCOME_SUCCESS, dur);
   }
   ```
4. **`fail_task_with_reason`**(所有失败唯一漏斗,清 `retry_attempts` 之后):
   ```rust
   if let Some(t0) = self.dispatch_times.lock().await.remove(&key) {
       let dur = t0.elapsed().as_secs_f64();
       let tt = self.resolve_task_type(task_id).await.unwrap_or_else(|_| "unknown".into());
       self.metrics.record_turn_terminal(&tt, OUTCOME_FAILED, dur);
   }
   ```
5. **`handle_turn_execution_error`** Retry 分支(`dispatch_with_retry` 前):
   ```rust
   let tt = self.resolve_task_type(task_id).await.unwrap_or_else(|_| "unknown".into());
   self.metrics.record_retry(&tt);
   ```
6. **`dispatcher_counts()`**(新增 pub,供 Gauge pull):
   ```rust
   pub fn dispatcher_counts(&self) -> Vec<(String, usize, usize)> { self.dispatcher.snapshot() }
   ```

**dispatch_time 语义**:只在**首次派发**(`dispatch_pending`)写入;turn 级 Retry(`handle_turn_execution_error`→`dispatch_with_retry`,槽位已持有)**不重置** → 终态观察到的是「首次派发→终态」总壁钟,正是 timeout 调优要的量。终态唯一(CR-3 保证),`remove` 恰好一次,无泄漏。

**resolve_task_type 的开销**:终态/重试路径已做 store 读;多一次 `get_task`(或复用已有读)可接受。`outcome`/`task_type` 标签低基数(`task_type` 实际就几种 agent 类型),无爆炸。

### 3.5 `/metrics` 路由 + health Gauge(`server.rs`)

`AppState` 加字段:

```rust
pub metrics: Arc<Metrics>,
```

`build_router` 加:

```rust
.route("/metrics", get(metrics_handler))
```

handler(渲染前 sync Gauge —— pull):

```rust
async fn metrics_handler(State(state): State<AppState>) -> Response {
    let m = state.metrics.clone();
    // 1. dispatcher Gauge
    for (tt, pending, in_flight) in state.orchestrator.dispatcher_counts() {
        m.set_pending(&tt, pending as i64);
        m.set_in_flight(&tt, in_flight as i64);
    }
    // 2. 依赖健康 Gauge(复用 health())
    let h = state.orchestrator.health().await;
    m.set_dependency_up(DEP_BROKER, h.broker.status == "ok");
    m.set_dependency_up(DEP_SANDBOX, h.sandbox.status == "ok");
    // 3. 渲染
    let body = m.render();
    ([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], body).into_response()
}
```

`server::start()`:`Orchestrator::new(...)` 后不接 `.with_metrics()`(用默认实例);`AppState` 填 `metrics: orch.metrics_handle()`(新增 pub 访问器,返回 `Arc<Metrics>`)。**orchestrator 与 /metrics handler 共享同一 `Arc<Metrics>` 实例**,counter/gauge 才能对上。

### 3.6 task_created 打点(create_task 路径)

`create_session_handler` → `service::create_task`(或 `storage.create_task`)成功后:`metrics.record_task_created(&task_type)`。在 handler 层打点(orchestrator 不参与 task 创建),task_type 来自 `CreateSessionRequest.task_type`。需要 handler 能拿到 metrics —— 已在 AppState。

---

## 4. TDD 测试清单(先写,跑红)

### 4.1 `metrics.rs` 单测(纯单元,无 I/O,自带 Registry 隔离)

- [ ] **`metrics_render_contains_catalog`**:`Metrics::new()` 后 `render()` 含全部 10 个指标名(`fixus_turn_enqueued_total` … `fixus_dependency_up`)—— 证明注册成功。
- [ ] **`counter_inc_visible_in_render`**:`record_turn_enqueued("claude")` 3 次 → render 含 `fixus_turn_enqueued_total{task_type="claude"} 3`。
- [ ] **`dispatched_records_counter_and_histogram`**:`record_turn_dispatched("claude", 0.5)` → render 含 `fixus_turn_dispatched_total{task_type="claude"} 1` **且** `fixus_turn_queue_wait_seconds_count{task_type="claude"} 1`。
- [ ] **`terminal_outcome_labels`**:success 与 failed 各 1 → render 含两条 `fixus_turn_terminal_total{...,outcome="success"}` / `outcome="failed"`。
- [ ] **`registry_isolation`**:两个 `Metrics::new()` 实例互不污染 —— A 计数,B 的 render 不含 A 的标签值。
- [ ] **`gauge_set_visible`**:`set_in_flight("claude", 2)` → render 含 `fixus_in_flight_turns{task_type="claude"} 2`。

### 4.2 `dispatcher.rs` 单测(CR-4b 扩展)

- [ ] **`snapshot_reports_pending_and_in_flight`**:enqueue 2 个同 type + pop 1 个 → snapshot 含 `(type, pending=1, in_flight=1)`。
- [ ] **`enqueued_at_set_on_enqueue`**:enqueue 后 pop 出的 `QueuedTurn.enqueued_at` `<= Instant::now()`(非默认零值)。

### 4.3 orchestrator 集成测试(打点接线)

- [ ] **`cr4_metrics_record_turn_lifecycle`**:setup task at executing → `enqueue_and_dispatch` → `handle_turn_execution_done` → 读 `orch.metrics_handle().render()`,断言含 `fixus_turn_enqueued_total`、`fixus_turn_dispatched_total`、`fixus_turn_queue_wait_seconds_count`、`fixus_turn_terminal_total{...,outcome="success"}`、`fixus_turn_duration_seconds_count`。
- [ ] **`cr4_metrics_terminal_failed_on_fail_path`**:同上但走 `handle_turn_execution_error`(terminal reason,application_error 不重试直接 Fail)→ render 含 `outcome="failed"`。
- [ ] **`cr4_metrics_retry_counter`**:retryable reason + 预算内 → Retry 分支触发 → render 含 `fixus_retry_attempts_total{...} 1`。

> 集成测试沿用 CR-3/CR-1+2 的 `cr3_setup_task_at_executing` helper + `wait_seq` 模式(logdbd 最终一致性)。

---

## 5. 实施步骤(实施阶段,逐 [ ] 推进)

- [ ] **CR-4a**:`Cargo.toml` 加 `prometheus = "0.13"`;新建 `src/metrics.rs`(§3.2 全部,先写 §4.1 测试跑红 → 填实现跑绿);`lib.rs` 加 `pub mod metrics;`。
- [ ] **CR-4b**:`dispatcher.rs` `QueuedTurn` 加 `enqueued_at`(更新测试 helper `turn()`);加 `Dispatcher::snapshot()`(§4.2 测试)。`orchestrator.rs` 构造 `QueuedTurn` 处补 `enqueued_at: Instant::now()`。
- [ ] **CR-4c**:`orchestrator.rs` 加 `metrics` + `dispatch_times` 字段;`new()` + 两个 bg 字面量补字段;`.with_metrics()` + `metrics_handle()`;§3.4 六处打点 + `dispatcher_counts()`(§4.3 测试)。
- [ ] **CR-4d**:`server.rs` `AppState` 加 `metrics`;`build_router` 加 `/metrics`;`metrics_handler`(§3.5);`start()` 接 `AppState.metrics`;`create_session_handler` 打 `task_created`。
- [ ] **CR-4e**:集成测试(§4.3);`cargo build --release`;全量 `cargo test --lib -- --skip broker_store`(无回归);勾掉 TODO CR-4。

---

## 6. 证据附录

### 6.1 测试(全绿)

| 套件 | 计数 |
|------|------|
| §4.1 `metrics::tests`(render 目录 / counter / dispatched+histogram / outcome 标签 / Registry 隔离 / gauge) | 6/6 |
| §4.2 `dispatcher::tests`(snapshot pending/in_flight、enqueued_at 填入) | 2/2(总 9/9 含原有 7) |
| §4.3 orchestrator 集成(lifecycle 全指标、failed 终态、retry 计数) | 3/3 |
| 全量 lib(跳过 broker_store,需 live broker) | **74 passed, 0 failed**, 3 ignored, 5 filtered |

(基线 63 → +11 CR-4 测试 = 74。)

### 6.2 `cargo build --release`

成功,60s,仅既有 unused-import 警告(非 CR-4 引入)。

### 6.3 真实 `curl /metrics`(本地 dev 栈:logdbd + broker + fixlet + sandbox + 新 fixus)

`GET /health`:`{"fixus":"ok","broker":{"status":"unknown",...},"sandbox":{"status":"ok",...}}`

`GET /metrics`(节选,`text/plain; version=0.0.4`):

```
# HELP fixus_dependency_up Dependency health (1=ok, 0=degraded/unknown)
# TYPE fixus_dependency_up gauge
fixus_dependency_up{dependency="broker"} 0
fixus_dependency_up{dependency="sandbox"} 1
# HELP fixus_turn_duration_seconds Seconds from first dispatch to terminal ...
# TYPE fixus_turn_duration_seconds histogram
fixus_turn_duration_seconds_bucket{outcome="success",task_type="default",le="0.05"} 17
...
fixus_turn_duration_seconds_bucket{outcome="success",task_type="default",le="300"} 17
fixus_turn_duration_seconds_bucket{outcome="success",task_type="default",le="600"} 17
fixus_turn_duration_seconds_bucket{outcome="success",task_type="default",le="+Inf"} 17
fixus_turn_duration_seconds_sum{outcome="success",task_type="default"} 0
fixus_turn_duration_seconds_count{outcome="success",task_type="default"} 17
fixus_turn_terminal_total{outcome="success",task_type="default"} 17
```

- 自定义长尾桶(0.05…600s)按设计渲染;`outcome`/`task_type` 标签字母序(prometheus 默认)。
- `sum=0` / 仅 `turn_terminal`+`dependency_up` 出现:fixus **刚重启**,lifecycle consumer 重放了 broker 里历史的 `turn_execution_done`(17 个),dispatch_times map 为空(R3 跨重启局限)→ 这 17 个 duration 记 0.0;新 POST /turns 派发的 turn 才会触 `turn_enqueued`/`turn_dispatched`/`queue_wait` 并记真实 duration。机制与设计一致,非 bug。


---

## 7. 风险与权衡

- **R1 prometheus 默认直方图桶对长 turn 不友好**(turn 可跑 300s → 落 `+Inf` 桶):v1 接受(`_count`/`_sum` 仍可调优),自定义长尾桶留后续(N4)。
- **R2 resolve_task_type 在终态多一次 store 读**:终态路径已非热路径(每 turn 一次),开销可忽略;若成瓶颈可缓存 task_type 到 dispatch_times map 的 value(改成 `(Instant, String)`)。
- **R3 dispatch_times map 跨重启丢失**:同 CR-3 retry_attempts 的已知局限;重启后旧 turn 由 recovery 接管(dispatch_time 无 → 该 turn 的 duration 不观测,可接受)。
- **R4 prometheus crate 引入拉长 fixus 编译时间**:prometheus 是轻依赖(无重传递依赖);bins 编译会带上但非运行耦合。若不可接受可 feature-gate(N5 已排除)。
- **R5 Gauge pull 在 handler 内 sync,高频抓取有锁成本**:`Dispatcher::snapshot` 持 `std::sync::Mutex` 极短;`health()` 持两个 tokio mutex 极短;抓取间隔默认 15s+,无虞。
