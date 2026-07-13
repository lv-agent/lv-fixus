//! 派发调度器(CR-1+2)—— 优先级 + per-task_type 并发闸。
//!
//! fixus 的 turn 派发原本是「POST /turns → 即时 produce 到 `task-begin-{type}` broker 流」,
//! fixlet 按流序 FIFO 认领,既无优先级也无并发上限(见
//! `docs/superpowers/plans/2026-07-13-cr1-2-dispatch-scheduler.md` §1)。
//!
//! 本模块是 **纯策略层**:只管 per-type 优先队列 + 在途计数 + 容量闸,
//! **不碰 broker I/O**。orchestrator 调 `enqueue` 入队、`try_pop` 取下一个该派的 turn
//! (取出即 `in_flight++`)并自行 produce、turn 终态时调 `on_turn_terminal`(`in_flight--`)。
//! 纯策略 ⇒ 可单测,无需 broker / mock。
//!
//! 与 CR-3 正交:终态回调点(`handle_turn_execution_done` / `fail_task_with_reason` /
//! `fail_turn_and_respond`)已在 CR-3 建好,本模块只提供 `on_turn_terminal`。

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Mutex;

/// 一个待派发的 turn(经调度器,非直派)。
#[derive(Debug, Clone)]
pub struct QueuedTurn {
    pub task_type: String,
    pub task_id: String,
    pub turn_id: i64,
    pub user_input: String,
    pub redo_group: String,
    pub redo_count: i32,
    pub cached_llm: Vec<String>,
    pub priority: i32,
    /// 入队时刻(CR-4):队列等待时长(enqueue→dispatch)计时起点。
    pub enqueued_at: std::time::Instant,
}

/// 优先队列条目。`Ord` 使 BinaryHeap 弹出:**priority 大者优先;同 priority 入队早者(seq 小)优先**。
#[derive(Debug, Clone)]
struct PriEntry {
    priority: i32,
    seq: u64,
    turn: QueuedTurn,
}

impl PartialEq for PriEntry {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}
impl Eq for PriEntry {}
impl PartialOrd for PriEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PriEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // 大 priority 排前;同 priority 小 seq 排前(稳定 FIFO)
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

/// per-task_type 队列状态
#[derive(Default)]
struct TypeQueue {
    pending: BinaryHeap<PriEntry>,
    in_flight: usize,
}

/// 派发调度器:per-type 优先队列 + 在途计数 + 容量闸。
pub struct Dispatcher {
    queues: Mutex<HashMap<String, TypeQueue>>,
    /// per-task_type 在途上限(同 type 最多多少个 turn 同时派出去未终态)。
    max_concurrent: usize,
    next_seq: AtomicU64,
}

impl Dispatcher {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            queues: Mutex::new(HashMap::new()),
            max_concurrent: max_concurrent.max(1),
            next_seq: AtomicU64::new(0),
        }
    }

    /// 入队。返回入队后该 type 的 pending 数。
    pub fn enqueue(&self, turn: QueuedTurn) -> usize {
        let seq = self.next_seq.fetch_add(1, AtomicOrdering::Relaxed);
        let task_type = turn.task_type.clone();
        let entry = PriEntry {
            priority: turn.priority,
            seq,
            turn,
        };
        let mut queues = self.queues.lock().unwrap();
        let q = queues.entry(task_type).or_default();
        q.pending.push(entry);
        q.pending.len()
    }

    /// 有空闲容量(`in_flight < max_concurrent`)则弹出最高优先 turn 并 `in_flight++`;
    /// 否则返回 `None`。**取出即视为已派发在途**,调用方负责 produce。
    pub fn try_pop(&self, task_type: &str) -> Option<QueuedTurn> {
        let mut queues = self.queues.lock().unwrap();
        let q = queues.get_mut(task_type)?;
        if q.in_flight >= self.max_concurrent {
            return None;
        }
        let entry = q.pending.pop()?;
        q.in_flight += 1;
        Some(entry.turn)
    }

    /// turn 终态(完成 / 失败 / 超时)回调:`in_flight--`。容量释放后调用方应再 `try_pop`。
    /// 未知 type / 已为 0 时 no-op(防御)。
    pub fn on_turn_terminal(&self, task_type: &str) {
        if let Some(q) = self.queues.lock().unwrap().get_mut(task_type) {
            if q.in_flight > 0 {
                q.in_flight -= 1;
            }
        }
    }

    /// 该 type 当前在途 turn 数。
    pub fn in_flight(&self, task_type: &str) -> usize {
        self.queues
            .lock()
            .unwrap()
            .get(task_type)
            .map(|q| q.in_flight)
            .unwrap_or(0)
    }

    /// 该 type 当前排队待派 turn 数。
    pub fn pending_count(&self, task_type: &str) -> usize {
        self.queues
            .lock()
            .unwrap()
            .get(task_type)
            .map(|q| q.pending.len())
            .unwrap_or(0)
    }

    /// per-type `(task_type, pending 深度, in_flight 数)` 快照(CR-4),供 `/metrics` Gauge pull。
    /// 只含 pending>0 或 in_flight>0 的 type;顺序稳定(按 task_type 字典序)。
    pub fn snapshot(&self) -> Vec<(String, usize, usize)> {
        let queues = self.queues.lock().unwrap();
        let mut out: Vec<(String, usize, usize)> = queues
            .iter()
            .map(|(tt, q)| (tt.clone(), q.pending.len(), q.in_flight))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn(type_: &str, id: &str, prio: i32) -> QueuedTurn {
        QueuedTurn {
            task_type: type_.into(),
            task_id: id.into(),
            turn_id: 0,
            user_input: String::new(),
            redo_group: String::new(),
            redo_count: 0,
            cached_llm: vec![],
            priority: prio,
            enqueued_at: std::time::Instant::now(),
        }
    }

    // ── 性能测试(#[ignore],cargo test --lib -- --ignored perf_ --nocapture)──
    // 测量 turn 派发热路径:enqueue + try_pop + on_turn_terminal 一次循环(= 一个 turn
    // 经调度器的全部调度器侧成本)。不断言阈值(WSL2 抖动),数字供人读。

    fn report(name: &str, unit: &str, mut samples: Vec<u64>) {
        samples.sort_unstable();
        let n = samples.len();
        if n == 0 {
            println!("[perf] {}: no samples", name);
            return;
        }
        let p = |q: usize| samples[(q * n / 100).min(n.saturating_sub(1))];
        let sum: u64 = samples.iter().sum();
        println!(
            "[perf] {:<34} n={:>6}  p50={:>7}{}  p95={:>7}{}  p99={:>7}{}  avg={:>7}{}",
            name, n, p(50), unit, p(95), unit, p(99), unit, sum / n as u64, unit
        );
    }

    #[test]
    #[ignore]
    fn perf_dispatcher_enqueue_pop_cycle() {
        let d = Dispatcher::new(100_000);
        // warm-up(primes BinaryHeap / Mutex)
        for i in 0..1000usize {
            d.enqueue(turn("t", &format!("w{}", i), (i % 8) as i32));
            let _ = d.try_pop("t");
            d.on_turn_terminal("t");
        }
        let n = 20_000;
        let mut ns = Vec::with_capacity(n);
        for i in 0..n {
            let t0 = std::time::Instant::now();
            d.enqueue(turn("t", &format!("p{}", i), (i % 8) as i32));
            let popped = d.try_pop("t").expect("capacity ample");
            d.on_turn_terminal("t");
            ns.push(t0.elapsed().as_nanos() as u64);
            assert_eq!(popped.task_id, format!("p{}", i)); // 功能正确(单 turn 立即 pop)
        }
        report("dispatcher enqueue+pop+terminal", "ns", ns);
    }


    // ── 优先级序 ──

    #[test]
    fn pop_order_is_priority_desc() {
        let d = Dispatcher::new(10);
        d.enqueue(turn("t", "low", 0));
        d.enqueue(turn("t", "high", 5));
        d.enqueue(turn("t", "mid", 2));

        let first = d.try_pop("t").unwrap();
        let second = d.try_pop("t").unwrap();
        let third = d.try_pop("t").unwrap();

        assert_eq!(first.task_id, "high", "最高优先先派");
        assert_eq!(second.task_id, "mid");
        assert_eq!(third.task_id, "low");
    }

    #[test]
    fn tie_breaks_by_enqueue_order_fifo() {
        let d = Dispatcher::new(10);
        d.enqueue(turn("t", "a", 3)); // 先入队
        d.enqueue(turn("t", "b", 3)); // 同优先,后入队

        assert_eq!(d.try_pop("t").unwrap().task_id, "a", "同优先 FIFO");
        assert_eq!(d.try_pop("t").unwrap().task_id, "b");
    }

    // ── 并发闸 ──

    #[test]
    fn capacity_caps_in_flight() {
        let d = Dispatcher::new(2);
        for i in 0..5 {
            d.enqueue(turn("t", &format!("t{}", i), 0));
        }
        // max=2 ⇒ 只能弹出 2 个,余 3 留 pending
        assert_eq!(d.try_pop("t").unwrap().task_id, "t0");
        assert_eq!(d.try_pop("t").unwrap().task_id, "t1");
        assert!(d.try_pop("t").is_none(), "达上限应返回 None");
        assert_eq!(d.in_flight("t"), 2);
        assert_eq!(d.pending_count("t"), 3);
    }

    #[test]
    fn terminal_releases_capacity_for_next() {
        let d = Dispatcher::new(2);
        for i in 0..4 {
            d.enqueue(turn("t", &format!("t{}", i), 0));
        }
        d.try_pop("t");
        d.try_pop("t");
        assert!(d.try_pop("t").is_none());
        assert_eq!(d.in_flight("t"), 2);

        // 一个终态 ⇒ 释放 1 个槽 ⇒ 可再派 1 个
        d.on_turn_terminal("t");
        assert_eq!(d.in_flight("t"), 1);
        assert_eq!(d.try_pop("t").unwrap().task_id, "t2");
        assert_eq!(d.in_flight("t"), 2);
    }

    // ── per-type 隔离 ──

    #[test]
    fn per_type_independent_counts() {
        let d = Dispatcher::new(2);
        d.enqueue(turn("A", "a1", 0));
        d.enqueue(turn("A", "a2", 0));
        d.enqueue(turn("B", "b1", 0));

        // A 满(max=2),B 不受影响
        d.try_pop("A");
        d.try_pop("A");
        assert!(d.try_pop("A").is_none(), "A 满");
        assert!(d.try_pop("B").is_some(), "B 仍有容量");
        assert_eq!(d.in_flight("A"), 2);
        assert_eq!(d.in_flight("B"), 1);
    }

    #[test]
    fn terminal_is_per_type() {
        let d = Dispatcher::new(1);
        d.enqueue(turn("A", "a1", 0));
        d.enqueue(turn("B", "b1", 0));
        d.try_pop("A");
        d.try_pop("B");

        // A 终态只释放 A 的槽,不影响 B
        d.on_turn_terminal("A");
        assert_eq!(d.in_flight("A"), 0);
        assert_eq!(d.in_flight("B"), 1, "B 在途不受 A 终态影响");
    }

    #[test]
    fn enqueue_returns_pending_count() {
        let d = Dispatcher::new(10);
        assert_eq!(d.enqueue(turn("t", "a", 0)), 1);
        assert_eq!(d.enqueue(turn("t", "b", 0)), 2);
    }

    // ── CR-4:snapshot + enqueued_at ──

    #[test]
    fn snapshot_reports_pending_and_in_flight() {
        let d = Dispatcher::new(10);
        d.enqueue(turn("A", "a1", 0));
        d.enqueue(turn("A", "a2", 0));
        d.enqueue(turn("B", "b1", 0));
        d.try_pop("A"); // A: pending 1, in_flight 1
        let mut snap = d.snapshot();
        snap.sort();
        assert_eq!(
            snap,
            vec![("A".to_string(), 1, 1), ("B".to_string(), 1, 0)],
            "snapshot 应反映 per-type pending/in_flight"
        );
    }

    #[test]
    fn enqueued_at_set_on_enqueue() {
        let d = Dispatcher::new(10);
        let before = std::time::Instant::now();
        d.enqueue(turn("t", "a", 0));
        let popped = d.try_pop("t").unwrap();
        // enqueued_at 在 before 之后(elapsed 非负即说明字段真被填了,非默认零值)
        assert!(popped.enqueued_at >= before, "enqueued_at 应在 enqueue 时写入");
    }
}
