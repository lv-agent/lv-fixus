//! retry 策略(CR-3)—— 失败分类 + 预算决策。
//!
//! 与 [`recovery.rs`](crate::recovery)(崩溃恢复 / `redo_group`)正交:
//! 本模块只管「非崩溃类失败要不要再试、试几次」。崩溃恢复由 redo_group + 幂等键负责,
//! 失败重试由本模块的预算负责,两者共用 `redo_count` 计数(见 CR N1 的取舍)。
//!
//! 详见 `docs/superpowers/plans/2026-07-13-cr3-failure-taxonomy-retry-budget.md`。

use crate::models::FailureReason;

/// retry 预算策略。
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// 一个 turn 最多重试次数(**不含**首跑)。默认 2(env `FIXUS_MAX_RETRY_ATTEMPTS`)。
    /// `max_attempts == 0` ⇒ 任何失败立即终态(连可重试的也不试)。
    pub max_attempts: i32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_attempts: 2 }
    }
}

/// [`RetryPolicy::decide`] 的决策结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    /// 还在预算内 + 原因可重试 → 调用方继续重派(turn redo 或 dispatch 重发)。
    /// `next_redo_count` = 下一次重做应使用的 redo_count(= current + 1)。
    Retry {
        reason: FailureReason,
        next_redo_count: i32,
    },
    /// 超预算 或 终态原因 → 调用方终态收口(`fail_task`)。
    /// `budget_exhausted`:true=可重试但预算用尽;false=终态原因(本就不该重试)。
    Fail {
        reason: FailureReason,
        budget_exhausted: bool,
    },
}

impl RetryPolicy {
    /// 根据失败原因 + 当前已重做次数,决定 [`RetryDecision::Retry`] 或 [`RetryDecision::Fail`]。
    ///
    /// - `reason` —— [`crate::models::classify_failure`] 的输出。
    /// - `current_redo_count` —— 该 turn 已重做过几次(来自 `turn_started.redo_count`)。
    ///
    /// 规则:可重试 且 `current < max_attempts` → Retry;否则 Fail。
    pub fn decide(&self, reason: FailureReason, current_redo_count: i32) -> RetryDecision {
        if reason.is_retryable() && current_redo_count < self.max_attempts {
            RetryDecision::Retry {
                reason,
                next_redo_count: current_redo_count + 1,
            }
        } else {
            RetryDecision::Fail {
                reason,
                budget_exhausted: reason.is_retryable(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── RetryPolicy::decide:可重试 + 预算内 ──

    #[test]
    fn retryable_under_budget_retries() {
        let p = RetryPolicy { max_attempts: 2 };
        assert_eq!(
            p.decide(FailureReason::AgentProcessExited, 0),
            RetryDecision::Retry { reason: FailureReason::AgentProcessExited, next_redo_count: 1 }
        );
        assert_eq!(
            p.decide(FailureReason::AgentProcessExited, 1),
            RetryDecision::Retry { reason: FailureReason::AgentProcessExited, next_redo_count: 2 }
        );
    }

    #[test]
    fn all_retryable_reasons_retry_under_budget() {
        let p = RetryPolicy { max_attempts: 2 };
        for r in [
            FailureReason::AgentSpawnFailed,
            FailureReason::SessionCreateFailed,
            FailureReason::AgentProcessExited,
            FailureReason::RedoDispatchFailed,
            FailureReason::BrokerError,
            FailureReason::SandboxTimeout,
            FailureReason::Unknown,
        ] {
            assert!(
                matches!(
                    p.decide(r, 0),
                    RetryDecision::Retry { reason, .. } if reason == r
                ),
                "{:?} 应在预算内 Retry",
                r
            );
        }
    }

    // ── decide:可重试 + 超预算 ──

    #[test]
    fn retryable_over_budget_fails() {
        let p = RetryPolicy { max_attempts: 2 };
        // redo_count == max_attempts ⇒ 预算用尽
        assert_eq!(
            p.decide(FailureReason::AgentProcessExited, 2),
            RetryDecision::Fail { reason: FailureReason::AgentProcessExited, budget_exhausted: true }
        );
        assert_eq!(
            p.decide(FailureReason::RedoDispatchFailed, 5),
            RetryDecision::Fail { reason: FailureReason::RedoDispatchFailed, budget_exhausted: true }
        );
    }

    #[test]
    fn zero_max_attempts_means_no_retry() {
        let p = RetryPolicy { max_attempts: 0 };
        // 即便可重试,预算为 0 也立即 fail(budget_exhausted=true)
        assert_eq!(
            p.decide(FailureReason::AgentProcessExited, 0),
            RetryDecision::Fail { reason: FailureReason::AgentProcessExited, budget_exhausted: true }
        );
    }

    // ── decide:终态原因 ──

    #[test]
    fn terminal_reason_fails_immediately() {
        let p = RetryPolicy { max_attempts: 2 };
        for r in [FailureReason::ApplicationError, FailureReason::Policy, FailureReason::Canceled] {
            assert_eq!(
                p.decide(r, 0),
                RetryDecision::Fail { reason: r, budget_exhausted: false },
                "{:?} 应立即 Fail 且非预算耗尽",
                r
            );
        }
    }

    #[test]
    fn terminal_reason_fails_even_with_budget_remaining() {
        let p = RetryPolicy { max_attempts: 5 };
        assert_eq!(
            p.decide(FailureReason::ApplicationError, 0),
            RetryDecision::Fail { reason: FailureReason::ApplicationError, budget_exhausted: false }
        );
    }

    // ── decide:Unknown 兜底(可重试但有预算)──

    #[test]
    fn unknown_is_retryable_but_budgeted() {
        let p = RetryPolicy { max_attempts: 2 };
        assert!(matches!(
            p.decide(FailureReason::Unknown, 0),
            RetryDecision::Retry { reason: FailureReason::Unknown, next_redo_count: 1 }
        ));
        assert_eq!(
            p.decide(FailureReason::Unknown, 2),
            RetryDecision::Fail { reason: FailureReason::Unknown, budget_exhausted: true }
        );
    }

    #[test]
    fn default_policy_is_two_attempts() {
        assert_eq!(RetryPolicy::default().max_attempts, 2);
    }

    // ── perf(CR-3):decide 在重试热路径的开销 ──────────────────────

    #[ignore]
    #[test]
    fn perf_retry_decide_at_scale() {
        use std::time::Instant;
        let policy = RetryPolicy { max_attempts: 2 };
        let reasons = [
            FailureReason::AgentProcessExited,
            FailureReason::BrokerError,
            FailureReason::ApplicationError,
            FailureReason::Unknown,
            FailureReason::SandboxTimeout,
        ];
        const N: usize = 50_000;
        let mut samples: Vec<u64> = Vec::with_capacity(N);
        // 防编译器优化掉 decide:累加决策输出
        let mut acc: u64 = 0;
        for i in 0..N {
            let reason = reasons[i % reasons.len()];
            let redo = (i % 4) as i32;
            let t0 = Instant::now();
            let d = policy.decide(reason, redo);
            samples.push(t0.elapsed().as_nanos() as u64);
            match d {
                RetryDecision::Retry { next_redo_count, .. } => acc += next_redo_count as u64,
                RetryDecision::Fail { budget_exhausted, .. } => acc += budget_exhausted as u64,
            }
        }
        report("perf_retry_decide", &mut samples);
        // correctness:同输入同输出(确定性),且 acc 被用到
        assert_eq!(
            policy.decide(FailureReason::AgentProcessExited, 0),
            RetryDecision::Retry { reason: FailureReason::AgentProcessExited, next_redo_count: 1 }
        );
        println!("perf_retry_decide acc={acc}");
    }

    fn report(label: &str, samples: &mut [u64]) {
        samples.sort_unstable();
        let n = samples.len();
        let pct = |p: f64| samples[((n as f64 - 1.0) * p).round() as usize];
        let avg = samples.iter().sum::<u64>() / n as u64;
        println!("{label}: n={n} p50={}ns p95={}ns p99={}ns avg={}ns", pct(0.50), pct(0.95), pct(0.99), avg);
    }
}
