# CR-6:turn 看门狗(派发→终态无响应回收)

> 日期:2026-07-13
> 来源:[`veps/TODO.md`](../../veps/TODO.md) CR-6(claim→start prepare-lease + 认领响应丢失回收,Tier 2,**M**)
> 前置:CR-3(`handle_turn_execution_error` 失败治理)、CR-4(`dispatch_times` 已追踪所有派发路径)
> 纪律:**先 CR → 再 TDD 写测试 → 最后实施**(本文是第一步)

---

## 1. 问题(代码取证后的真实状态)

### 1.1 CR-6 原始前提不成立:`Claimed` 态是死代码

取证(`grep -rn "claim_task" src/ src/bin/` 排除测试):

- `claim_task`(`service.rs:83`)—— **仅测试调用**(`service.rs:924/939/978` 全在 `#[cfg(test)]`)。生产**无人写 `task_claimed`**。
- 投影 `TaskClaimed → Claimed`(`projection.rs:155`)有,但触发不了。pull-based 模型里,**broker group 消费本身就是认领**,fixus 不再写 `task_claimed`。
- 故 TODO 原文"认领→executing 之间靠 broker 成员驱逐""turn 卡 claimed 的边角"——**claimed 边角不存在**(claimed 态被绕过)。

### 1.2 真实 gap:派发后无终态的 turn,只同步路径有超时

fixus 派发时自己写 `turn_started`(`orchestrator.rs:240/330` `service::start_turn`)⇒ task 直接 `Ready→Executing`。若 fixlet 此后崩溃/卡住,**永不产生终态事件**:

- **同步路径** `execute_turn` → `run_turn_to_completion`(`:279`):`tokio::time::timeout(turn_timeout=300s, result_rx)` 兜底 → `fail_turn_and_respond`。✅ 有治理。
- **异步路径** `start_turn_async`(`:309`):spawn 后台跑 `run_turn_to_completion`——**有 timeout**(同样经 run_turn_to_completion)。✅
- **后台恢复** `spawn_background_recovery`(`:345`)→ `enqueue_and_dispatch` → `dispatch_pending`:turn 派出去后**无任何 timeout/watcher**。❌
- **dispatcher 在途槽位**:turn 派发后 `in_flight++`,只在终态(`release_slot_and_redispatch`)减。若终态永不来,**槽位永久占用**,CR-1+2 的并发闸被逐步饿死。❌❌(最严重)

⇒ 真问题不是"claimed 卡死",是**"派发后无终态的 turn 卡住 dispatcher 槽 + PendingTurn",且 background recovery 路径完全无治理**。

### 1.3 已有的杠杆:CR-4 `dispatch_times` + CR-3 失败治理

- CR-4 已在 `dispatch_pending` 给每个派发的 turn 记 `dispatch_times[{task}:{turn}] = Instant`,终态(`record_turn_terminal_metric`)清掉。⇒ **"在 dispatch_times 里" = "已派发未终态"**,正是看门狗要扫的集合,无需新 DB 扫描(也扫不了 —— EventStore 无按态列举)。
- CR-3 `handle_turn_execution_error` 是失败治理唯一入口(retry/fail + release_slot)。看门狗只需触发它,治理复用。

### 1.4 multica 参照 + 诚实裁剪

multica:认领后 45s prepare-lease + 90s 认领响应丢失回收。fixus 抄"派发后 N 秒无终态即回收"语义,但**不做 claim/start 双窗**(那需要 fixlet 回报 turn_started,而 fixus 自己写 turn_started、无此区分)⇒ 单窗 `turn_lease`,统一治所有路径。claim/start 双窗留 CR-6c(需协议改:fixlet 回报 ack)。

---

## 2. 目标 / 非目标

### 目标

- **G1 统一看门狗**:周期扫描 `dispatch_times`,派发超 `turn_lease` 未终态的 turn → 触发 `handle_turn_execution_error`(reason `agent_unresponsive`)→ CR-3 治理(retry/fail + release_slot)。
- **G2 覆盖所有派发路径**:sync / async / background recovery 都被扫(background 路径此前完全无治理)。
- **G3 收敛有界**:看门狗触发后刷新 `dispatch_times`(重计 lease 窗);CR-3 retry 预算(`max_attempts`)封顶 → 最多 `max_attempts+1` 轮看门狗后 task 终态 Failed。无无限循环。
- **G4 可配 + 可观测**:`FIXUS_TURN_LEASE_SECS`(默认 300s,同 turn_timeout)+ metrics(`fixus_turn_watchdog_reclaims_total`)+ 日志。

### 非目标(显式排除)

- **N1 不做 claim/start 双窗 lease**:需 fixlet 回报 turn_started(协议改)。单窗 `turn_lease` 治"派发后无终态",够覆盖已知 gap。双窗留 CR-6c。
- **N2 不复活 Claimed 态**:pull-based 认领=broker group 消费,写回 `task_claimed` 是倒退。Claimed 保持 vestigial。
- **N3 不动 sync 路径的 `tokio::time::timeout`**:它管 HTTP 响应及时性;看门狗管槽位/终态回收。两者重叠时由终态唯一性(CR-3)保证幂等(二次触发 no-op)。
- **N4 不做跨重启看门狗**:`dispatch_times` in-memory(同 CR-3/CR-4 局限),重启后旧在途 turn 由 fixus 重启 + fixlet 重连自然重派;看门狗只管本进程在途。

---

## 3. 设计

### 3.1 看门狗(orchestrator)

```rust
impl Orchestrator {
    /// 周期扫描 dispatch_times,派发超 lease 未终态的 turn → handle_turn_execution_error。
    /// 刷新该 turn 的 dispatch_times(重计窗),由 CR-3 预算封顶收敛。
    pub fn spawn_turn_watchdog(self: &Arc<Self>, lease: Duration, interval: Duration) {
        let orch = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let stale = {
                    let now = Instant::now();
                    let dt = orch.dispatch_times.lock().await;
                    dt.iter()
                        .filter(|(_, t)| now.duration_since(**t) > lease)
                        .map(|(k, _)| k.clone())
                        .collect::<Vec<_>>()
                };
                for key in stale {
                    let (task_id, turn_id) = parse_key(&key);
                    tracing::warn!("turn watchdog: {} reclaim (no terminal in {:?})", key, lease);
                    orch.metrics.record_watchdog_reclaim(&task_id);
                    // 刷新 lease 窗(重计),避免每轮扫描重复触发;CR-3 预算封顶
                    orch.dispatch_times.lock().await.insert(key.clone(), Instant::now());
                    let _ = orch.handle_turn_execution_error(&task_id, turn_id, "agent_unresponsive",
                        "no terminal event within turn_lease").await;
                }
            }
        });
    }
}
```

- `parse_key("task:turn")` → `(task_id, turn_id)`(key 格式 `{task_id}:{turn_id}`,CR-4 已定)。
- 看门狗触发 `handle_turn_execution_error`(CR-3):分类 `agent_unresponsive` → **retryable** ⇒ CR-3 预算内重试(`dispatch_with_retry`),超预算 Fail。重试不重置 dispatch_times,但看门狗刷新了 ⇒ 下一窗再判。
- 收敛:每轮看门狗 = 1 次 retry 决策;CR-3 `max_attempts`(默认 2)⇒ 最多 3 轮看门狗后 Failed。

### 3.2 `FailureReason` 加 `AgentUnresponsive`

`models.rs`:`FailureReason::AgentUnresponsive` 加入 **retryable** 集(`is_retryable` 返回 true)。`classify_failure("agent_unresponsive", _)` → `AgentUnresponsive`。看门狗触发的失败走重试预算(同其它基础设施类失败),与 CR-3 一致。

### 3.3 metrics 加计数(CR-4 扩展)

`metrics.rs`:`fixus_turn_watchdog_reclaims_total{task_type}` Counter。看门狗每次回收 +1(CR-3 可观测性的补充:看挂了的 turn 有多少)。

### 3.4 接线 + 配置

`server.rs::start`:`orchestrator.spawn_turn_watchdog(lease, interval)`,env:
- `FIXUS_TURN_LEASE_SECS`(默认 300)
- `FIXUS_WATCHDOG_INTERVAL_SECS`(默认 60)

---

## 4. TDD 测试清单(先写,跑红)

### 4.1 `models.rs`(`AgentUnresponsive` retryable)

- [ ] `FailureReason::AgentUnresponsive.is_retryable() == true`。
- [ ] `classify_failure("agent_unresponsive", "") == AgentUnresponsive`。

### 4.2 orchestrator 集成(看门狗接线)

- [ ] **`cr6_watchdog_reclaims_stale_turn`**:setup task at executing → enqueue_and_dispatch(dispatch_times 记录)→ **backdate dispatch_times** 超 lease → 手动调一次看门狗扫描逻辑(或 `lease=0` + sleep)→ 断言 turn 走了 `handle_turn_execution_error`(task 未卡 Executing;turn_failed/retry 发生)。
- [ ] **`cr6_watchdog_skips_fresh_turn`**:同上但不 backdate(< lease)→ turn 不被回收(dispatch_times 仍在,task 状态不变)。
- [ ] **`cr6_watchdog_converges_via_retry_budget`**:max_attempts=1 + 反复触发 → 最终 task Failed(retry 预算耗尽),不无限循环。

### 4.3 性能(`#[ignore] perf_`,写进 §5 必做)

- [ ] **`perf_watchdog_scan_at_scale`**:dispatch_times 填 N(如 5000)条目,扫一次(全 fresh,不触发),测扫描耗时(应 µs 级 —— 纯 HashMap 遍历)。

---

## 5. 实施步骤(perf 写进必做,不再事后补)

- [ ] **CR-6a**:`models.rs` 加 `AgentUnresponsive`(retryable + classify)。§4.1 测试。
- [ ] **CR-6b**:`metrics.rs` 加 `fixus_turn_watchdog_reclaims_total` + `record_watchdog_reclaim`。`metrics_render_contains_catalog` 加该名。
- [ ] **CR-6c**:`orchestrator.rs` 加 `spawn_turn_watchdog` + `parse_key`;`server.rs::start` 接线 + env。§4.2 集成测试。
- [ ] **CR-6d**:§4.3 perf 测试 + 全量 `cargo test --lib -- --skip broker_store` + `cargo build --release`;勾掉 TODO CR-6。

---

## 6. 证据附录

### 6.1 测试(全绿)

- §4.1 `failure_reason_tests`:`AgentUnresponsive` retryable + classify 映射(5/5 绿,含新增断言)。
- §4.2 orchestrator 集成(4 绿):
  - `parse_turn_key_splits_task_and_turn` —— key 解析(含无冒号/非数字兜底)。
  - `cr6_watchdog_reclaims_stale_turn` —— stale(max_attempts=0)→ 回收 → Failed + watchdog 计数 +1。
  - `cr6_watchdog_skips_fresh_turn` —— fresh(< lease)不回收、状态不变。
  - `cr6_watchdog_converges_via_retry_budget` —— max_attempts=1,2 轮后 Failed(无无限循环)。
- §4.3 perf:`perf_watchdog_scan_at_scale`(5000 in-flight)绿。

全量 lib(跳过 broker_store):**78 passed, 0 failed**, 6 ignored(基线 74 → +4 CR-6)。

### 6.2 性能

```
[perf] watchdog scan (5000 in-flight)  n=50  p50=286µs  p99=455µs
```

5000 在途 turn 扫一次 < 0.5ms,每 60s 跑一次 ⇒ 可忽略,不阻塞 dispatch_times mutex 的其它消费者(dispatch_pending / record_turn_terminal_metric)。

### 6.3 构建 + 配置

`cargo build --release` 成功(77s)。env:`FIXUS_TURN_LEASE_SECS`(默认 300)/ `FIXUS_WATCHDOG_INTERVAL_SECS`(默认 60)。

---

## 7. 风险与权衡

- **R1 看门狗误杀长跑 turn**:`turn_lease` 默认 300s = turn_timeout;若 turn 真跑 >300s(超 turn_timeout),sync 路径本来也会超时失败,看门狗行为一致。调大 `FIXUS_TURN_LEASE_SECS` 即可放宽。
- **R2 看门狗与 sync `tokio::timeout` 竞态**:两者都可能对同一 turn 触发;终态唯一性(CR-3 + storage 校验)保证第二次 no-op(turn_failed/Failed 已写,再写被拒)。幂等。
- **R3 dispatch_times 跨重启丢失**:同 CR-3/CR-4 局限;重启后本进程在途 turn 表为空,看门狗无目标。旧 turn 由 fixlet 重连/broker 重派自然恢复。
- **R4 扫描频率 vs lease**:`interval`(60s)< `lease`(300s)⇒ 一个 lease 窗内扫多次,但只在超 lease 时触发一次(触发后刷新 timestamp)。无风暴。
- **R5 不做 claim/start 双窗**:无法区分"fixlet 没认领"与"fixlet 跑得久"。单窗 lease 是保守上界(= turn_timeout)。精确双窗需 fixlet 回报 ack(CR-6c 协议改),留后续。
