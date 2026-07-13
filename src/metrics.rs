//! 业务 Prometheus 指标(CR-4)。
//!
//! 自带 [`prometheus::Registry`] 实例隔离(非全局默认),每个 [`Metrics::new`] 互不污染,
//! 便于单测断言 [`Metrics::render`] 文本。设计见
//! `docs/superpowers/plans/2026-07-13-cr4-business-metrics.md`。

use std::sync::Arc;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
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
    turn_enqueued: IntCounterVec,   // {task_type}
    turn_dispatched: IntCounterVec, // {task_type}
    turn_terminal: IntCounterVec,   // {task_type, outcome}
    retry_attempts: IntCounterVec,  // {task_type}
    task_created: IntCounterVec,    // {task_type}
    queue_wait: HistogramVec,       // {task_type}  秒
    turn_duration: HistogramVec,    // {task_type, outcome}  秒
    in_flight: IntGaugeVec,         // {task_type}
    pending: IntGaugeVec,           // {task_type}
    dependency_up: IntGaugeVec,     // {dependency}
}

impl Metrics {
    /// 构造并注册全部指标到自带 Registry。
    pub fn new() -> Arc<Self> {
        let reg = Arc::new(Registry::new());

        let turn_enqueued = IntCounterVec::new(
            Opts::new("fixus_turn_enqueued_total", "Turns entered the dispatch queue"),
            &["task_type"],
        )
        .expect("turn_enqueued opts");
        let turn_dispatched = IntCounterVec::new(
            Opts::new(
                "fixus_turn_dispatched_total",
                "Turns popped from the queue and dispatched to the broker",
            ),
            &["task_type"],
        )
        .expect("turn_dispatched opts");
        let turn_terminal = IntCounterVec::new(
            Opts::new(
                "fixus_turn_terminal_total",
                "Turns reaching a terminal state (success|failed)",
            ),
            &["task_type", "outcome"],
        )
        .expect("turn_terminal opts");
        let retry_attempts = IntCounterVec::new(
            Opts::new(
                "fixus_retry_attempts_total",
                "Turn-level retry decisions (CR-3 retry budget consumed)",
            ),
            &["task_type"],
        )
        .expect("retry_attempts opts");
        let task_created = IntCounterVec::new(
            Opts::new("fixus_task_created_total", "Tasks created"),
            &["task_type"],
        )
        .expect("task_created opts");
        let queue_wait = HistogramVec::new(
            HistogramOpts::new(
                "fixus_turn_queue_wait_seconds",
                "Seconds a turn waited in the dispatch queue (enqueue→dispatch)",
            ),
            &["task_type"],
        )
        .expect("queue_wait opts");
        let turn_duration = HistogramVec::new(
            HistogramOpts::new(
                "fixus_turn_duration_seconds",
                "Seconds from first dispatch to terminal (first-dispatch→terminal wall clock)",
            )
            // 长尾桶:turn 可跑 300s,默认桶停在 10s。显式覆盖到 300s+。
            .buckets(vec![
                0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
            ]),
            &["task_type", "outcome"],
        )
        .expect("turn_duration opts");
        let in_flight = IntGaugeVec::new(
            Opts::new(
                "fixus_in_flight_turns",
                "Turns currently dispatched and awaiting terminal (per task_type)",
            ),
            &["task_type"],
        )
        .expect("in_flight opts");
        let pending = IntGaugeVec::new(
            Opts::new(
                "fixus_pending_turns",
                "Turns queued awaiting dispatch (per task_type)",
            ),
            &["task_type"],
        )
        .expect("pending opts");
        let dependency_up = IntGaugeVec::new(
            Opts::new(
                "fixus_dependency_up",
                "Dependency health (1=ok, 0=degraded/unknown)",
            ),
            &["dependency"],
        )
        .expect("dependency_up opts");

        reg.register(Box::new(turn_enqueued.clone()))
            .expect("register turn_enqueued");
        reg.register(Box::new(turn_dispatched.clone()))
            .expect("register turn_dispatched");
        reg.register(Box::new(turn_terminal.clone()))
            .expect("register turn_terminal");
        reg.register(Box::new(retry_attempts.clone()))
            .expect("register retry_attempts");
        reg.register(Box::new(task_created.clone()))
            .expect("register task_created");
        reg.register(Box::new(queue_wait.clone()))
            .expect("register queue_wait");
        reg.register(Box::new(turn_duration.clone()))
            .expect("register turn_duration");
        reg.register(Box::new(in_flight.clone()))
            .expect("register in_flight");
        reg.register(Box::new(pending.clone()))
            .expect("register pending");
        reg.register(Box::new(dependency_up.clone()))
            .expect("register dependency_up");

        Arc::new(Self {
            reg,
            turn_enqueued,
            turn_dispatched,
            turn_terminal,
            retry_attempts,
            task_created,
            queue_wait,
            turn_duration,
            in_flight,
            pending,
            dependency_up,
        })
    }

    // ── push(打点)──────────────────────────────────────────────────────

    pub fn record_turn_enqueued(&self, task_type: &str) {
        self.turn_enqueued.with_label_values(&[task_type]).inc();
    }

    pub fn record_turn_dispatched(&self, task_type: &str, queue_wait_secs: f64) {
        self.turn_dispatched.with_label_values(&[task_type]).inc();
        self.queue_wait
            .with_label_values(&[task_type])
            .observe(queue_wait_secs);
    }

    pub fn record_turn_terminal(&self, task_type: &str, outcome: &str, duration_secs: f64) {
        self.turn_terminal
            .with_label_values(&[task_type, outcome])
            .inc();
        self.turn_duration
            .with_label_values(&[task_type, outcome])
            .observe(duration_secs);
    }

    pub fn record_retry(&self, task_type: &str) {
        self.retry_attempts.with_label_values(&[task_type]).inc();
    }

    pub fn record_task_created(&self, task_type: &str) {
        self.task_created.with_label_values(&[task_type]).inc();
    }

    // ── pull(渲染前 sync)───────────────────────────────────────────────

    pub fn set_in_flight(&self, task_type: &str, n: i64) {
        self.in_flight.with_label_values(&[task_type]).set(n);
    }

    pub fn set_pending(&self, task_type: &str, n: i64) {
        self.pending.with_label_values(&[task_type]).set(n);
    }

    pub fn set_dependency_up(&self, dep: &str, up: bool) {
        self.dependency_up
            .with_label_values(&[dep])
            .set(if up { 1 } else { 0 });
    }

    /// 渲染 Prometheus 文本格式(供 `/metrics` handler)。
    pub fn render(&self) -> String {
        let encoder = TextEncoder::new();
        let mut buf = Vec::new();
        encoder
            .encode(&self.reg.gather(), &mut buf)
            .expect("prometheus encode");
        String::from_utf8_lossy(&buf).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_render_contains_catalog() {
        let m = Metrics::new();
        // gather() 只输出被 touch 过的指标(无样本的 vec 不 emit);故逐个打一下再验目录。
        m.record_turn_enqueued("claude");
        m.record_turn_dispatched("claude", 0.1);
        m.record_turn_terminal("claude", OUTCOME_SUCCESS, 0.1);
        m.record_retry("claude");
        m.record_task_created("claude");
        m.set_in_flight("claude", 0);
        m.set_pending("claude", 0);
        m.set_dependency_up(DEP_BROKER, true);
        let out = m.render();
        for name in [
            "fixus_turn_enqueued_total",
            "fixus_turn_dispatched_total",
            "fixus_turn_terminal_total",
            "fixus_retry_attempts_total",
            "fixus_task_created_total",
            "fixus_turn_queue_wait_seconds",
            "fixus_turn_duration_seconds",
            "fixus_in_flight_turns",
            "fixus_pending_turns",
            "fixus_dependency_up",
        ] {
            assert!(out.contains(name), "render missing metric name `{}`", name);
        }
    }

    #[test]
    fn counter_inc_visible_in_render() {
        let m = Metrics::new();
        m.record_turn_enqueued("claude");
        m.record_turn_enqueued("claude");
        m.record_turn_enqueued("claude");
        let out = m.render();
        assert!(
            out.contains("fixus_turn_enqueued_total{task_type=\"claude\"} 3"),
            "expected counter value 3 in render, got:\n{}",
            out
        );
    }

    #[test]
    fn dispatched_records_counter_and_histogram() {
        let m = Metrics::new();
        m.record_turn_dispatched("claude", 0.5);
        let out = m.render();
        assert!(
            out.contains("fixus_turn_dispatched_total{task_type=\"claude\"} 1"),
            "dispatched counter missing:\n{}",
            out
        );
        assert!(
            out.contains("fixus_turn_queue_wait_seconds_count{task_type=\"claude\"} 1"),
            "queue_wait histogram count missing:\n{}",
            out
        );
    }

    #[test]
    fn terminal_outcome_labels() {
        let m = Metrics::new();
        m.record_turn_terminal("claude", OUTCOME_SUCCESS, 1.2);
        m.record_turn_terminal("hermes", OUTCOME_FAILED, 3.4);
        let out = m.render();
        assert!(
            out.contains("fixus_turn_terminal_total{outcome=\"success\",task_type=\"claude\"} 1"),
            "success outcome missing:\n{}",
            out
        );
        assert!(
            out.contains("fixus_turn_terminal_total{outcome=\"failed\",task_type=\"hermes\"} 1"),
            "failed outcome missing:\n{}",
            out
        );
    }

    #[test]
    fn registry_isolation() {
        let a = Metrics::new();
        let b = Metrics::new();
        a.record_turn_enqueued("claude");
        b.record_turn_enqueued("hermes");
        let ra = a.render();
        let rb = b.render();
        assert!(
            ra.contains("task_type=\"claude\"") && !ra.contains("task_type=\"hermes\""),
            "A polluted by B:\n{}",
            ra
        );
        assert!(
            rb.contains("task_type=\"hermes\"") && !rb.contains("task_type=\"claude\""),
            "B polluted by A:\n{}",
            rb
        );
    }

    #[test]
    fn gauge_set_visible() {
        let m = Metrics::new();
        m.set_in_flight("claude", 2);
        m.set_dependency_up(DEP_BROKER, true);
        let out = m.render();
        assert!(
            out.contains("fixus_in_flight_turns{task_type=\"claude\"} 2"),
            "in_flight gauge missing:\n{}",
            out
        );
        assert!(
            out.contains("fixus_dependency_up{dependency=\"broker\"} 1"),
            "dependency_up gauge missing:\n{}",
            out
        );
    }

    // ── 性能测试(#[ignore],cargo test --lib -- --ignored perf_ --nocapture)──
    // /metrics scrape 每次调 render()(gather 全部 MetricFamily + TextEncoder 编码)。
    // 测量多 task_type × 多观测下的 render 成本。不断言阈值,数字供人读。

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
    fn perf_metrics_render() {
        let m = Metrics::new();
        // 模拟 4 task_type × ~3000 turn 生命周期观测
        for tt in ["claude", "hermes", "codex", "cursor"] {
            for _ in 0..1000 {
                m.record_turn_enqueued(tt);
                m.record_turn_dispatched(tt, 0.01);
                m.record_turn_terminal(tt, OUTCOME_SUCCESS, 1.0);
                m.record_turn_terminal(tt, OUTCOME_FAILED, 2.0);
                m.record_retry(tt);
            }
            m.set_in_flight(tt, 5);
            m.set_pending(tt, 3);
        }
        m.set_dependency_up(DEP_BROKER, true);
        m.set_dependency_up(DEP_SANDBOX, true);
        // warm-up
        for _ in 0..20 {
            let _ = m.render();
        }
        let n = 1000;
        let mut us = Vec::with_capacity(n);
        for _ in 0..n {
            let t0 = std::time::Instant::now();
            let out = m.render();
            us.push(t0.elapsed().as_micros() as u64);
            assert!(!out.is_empty());
        }
        report("metrics render (4 types × ~5k obs)", "µs", us);
    }
}
