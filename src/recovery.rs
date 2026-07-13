//! 恢复管理器 — Turn 级崩溃恢复
//!
//! 实现设计文档第 10 节故障恢复协议。
//!
//! # 核心策略：Turn 级重做
//!
//! 检测到 Turn 内任何 Step 不完整或 seq 不连续时，
//! 整 Turn 标记为需要重做，用同一 redo_group 重新执行。
//!
//! # 恢复入口
//!
//! Client 请求进入 fixus → fixus 检查是否有未完成 Turn →
//! 有未完成 Turn → 读取 redo_group → 判断 Tool 类型 → 下发重做

use crate::error::Result;
use crate::models::{AgentEvent, EventType, FailureReason, IncompleteStep, IncompleteTurn};
use crate::storage::EventStore;
use crate::service;

// ── Tool 幂等性分类 ────────────────────────────────────────────────────

/// Tool 恢复策略分类（设计文档 10.3 节）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolRecoveryStrategy {
    /// 纯读 — 直接重试
    ReadOnly,
    /// 幂等写 — 用 redo_group 稳定 idempotency_key 重试
    IdempotentWrite,
    /// 非幂等写 — 写 tool_failed，转人工
    NonIdempotentWrite,
}

/// 根据 tool_name 判断 Tool 恢复策略
///
/// 默认未知 Tool 归为 NonIdempotentWrite（安全优先）。
///
/// 注：实际系统中此映射应通过配置文件或注册表管理。
pub fn classify_tool(tool_name: &str) -> ToolRecoveryStrategy {
    const READ_ONLY_TOOLS: &[&str] = &[
        "get_weather",
        "search",
        "read_file",
        "list_directory",
        "get_order_status",
        "query_database",
        "fetch_url",
        "calculate",
        "parse",
    ];

    const IDEMPOTENT_TOOLS: &[&str] = &[
        "update_config",
        "set_status",
        "upsert_record",
        "create_or_update",
        "deploy_versioned",
    ];

    const NON_IDEMPOTENT_TOOLS: &[&str] = &[
        "send_email",
        "create_order",
        "process_payment",
        "send_notification",
        "delete_record",
        "submit_form",
    ];

    if READ_ONLY_TOOLS.contains(&tool_name) {
        ToolRecoveryStrategy::ReadOnly
    } else if IDEMPOTENT_TOOLS.contains(&tool_name) {
        ToolRecoveryStrategy::IdempotentWrite
    } else if NON_IDEMPOTENT_TOOLS.contains(&tool_name) {
        ToolRecoveryStrategy::NonIdempotentWrite
    } else {
        ToolRecoveryStrategy::NonIdempotentWrite
    }
}

// ── 恢复状态 ────────────────────────────────────────────────────────────

/// Turn 恢复决策
#[derive(Debug, Clone)]
pub enum RecoveryDecision {
    /// 可以直接重做（所有未完成 Step 都是纯读或幂等写）
    SafeToRedo {
        turn_id: i64,
        redo_group: String,
        redo_count: i32,
        incomplete_steps: Vec<IncompleteStep>,
    },
    /// 需要人工介入（存在非幂等写的未完成 Step）
    RequiresHumanIntervention {
        turn_id: i64,
        redo_group: String,
        blocking_steps: Vec<IncompleteStep>,
    },
    /// 无需恢复
    None,
}

/// Session 恢复状态
#[derive(Debug, Clone)]
pub struct SessionRecoveryState {
    pub task_id: String,
    pub incomplete_turns: Vec<IncompleteTurn>,
}

// ── 恢复检查 ────────────────────────────────────────────────────────────

/// 检查 Session 的恢复状态
///
/// 返回所有未完成的 Turn。
pub async fn check_session_recovery(
    store: &dyn EventStore,
    task_id: &str,
) -> Result<SessionRecoveryState> {
    let incomplete_turns = store.get_incomplete_turns(task_id).await?;

    Ok(SessionRecoveryState {
        task_id: task_id.to_string(),
        incomplete_turns,
    })
}

/// 对单个未完成 Turn 做出恢复决策
///
/// 分析 Turn 内未完成 Step 的 Tool 类型，决定恢复策略。
/// 决策核心是纯函数 [`decide_recovery_from_steps`];此处只负责取数(本 turn 的未完成 step)。
pub async fn decide_turn_recovery(
    store: &dyn EventStore,
    task_id: &str,
    incomplete_turn: &IncompleteTurn,
) -> Result<RecoveryDecision> {
    let _turn_events = store
        .get_turn_events(task_id, incomplete_turn.turn_id)
        .await?;

    let all_incomplete = store.get_incomplete_steps(task_id).await?;
    let turn_incomplete: Vec<_> = all_incomplete
        .into_iter()
        .filter(|s| s.turn_id == incomplete_turn.turn_id)
        .collect();

    Ok(decide_recovery_from_steps(incomplete_turn, &turn_incomplete))
}

/// 纯函数:根据本 turn 的未完成 step 列表做恢复决策(无 store 依赖,便于测试)。
///
/// 决策规则:
/// - 无未完成 step → [`RecoveryDecision::SafeToRedo`](空),redo_count/redo_group 原样透传。
/// - 含任一非幂等写 step → [`RecoveryDecision::RequiresHumanIntervention`],
///   只有非幂等 step 进 `blocking_steps`(安全 step 不混入)。
/// - 全为纯读/幂等写 → [`RecoveryDecision::SafeToRedo`],全部 step 进 `incomplete_steps`。
///
/// 注:未知 tool 名 / payload 缺 `tool_name` 默认按非幂等处理(安全优先),即进入 blocking。
fn decide_recovery_from_steps(
    incomplete_turn: &IncompleteTurn,
    turn_incomplete: &[IncompleteStep],
) -> RecoveryDecision {
    if turn_incomplete.is_empty() {
        return RecoveryDecision::SafeToRedo {
            turn_id: incomplete_turn.turn_id,
            redo_group: incomplete_turn.redo_group.clone(),
            redo_count: incomplete_turn.redo_count,
            incomplete_steps: vec![],
        };
    }

    let mut blocking_steps = Vec::new();
    let mut safe_steps = Vec::new();

    for step in turn_incomplete {
        let tool_name = step
            .payload
            .get("tool_name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        match classify_tool(tool_name) {
            ToolRecoveryStrategy::NonIdempotentWrite => {
                blocking_steps.push(step.clone());
            }
            _ => {
                safe_steps.push(step.clone());
            }
        }
    }

    if blocking_steps.is_empty() {
        RecoveryDecision::SafeToRedo {
            turn_id: incomplete_turn.turn_id,
            redo_group: incomplete_turn.redo_group.clone(),
            redo_count: incomplete_turn.redo_count,
            incomplete_steps: safe_steps,
        }
    } else {
        RecoveryDecision::RequiresHumanIntervention {
            turn_id: incomplete_turn.turn_id,
            redo_group: incomplete_turn.redo_group.clone(),
            blocking_steps,
        }
    }
}

// ── 恢复执行 ────────────────────────────────────────────────────────────

/// 为非幂等 Tool 写入 tool_failed 事件
///
/// 对应设计文档 10.3 节：非幂等写 → 写 tool_failed → 人工介入。
pub async fn skip_non_idempotent_step(
    store: &dyn EventStore,
    task_id: &str,
    turn_id: i64,
    step: &IncompleteStep,
) -> Result<AgentEvent> {
    let tool_call_id = step
        .payload
        .get("tool_call_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let error_type = "recovery_skipped_non_idempotent";

    service::record_tool_failed(
        store,
        task_id,
        Some(turn_id),
        &step.step_id,
        tool_call_id,
        error_type,
        &format!(
            "Recovery skipped: non-idempotent tool '{}' had an in-flight step at crash time. Requires human review.",
            step.payload.get("tool_name").and_then(|v| v.as_str()).unwrap_or("unknown")
        ),
        // 非幂等写在崩溃点的悬空 step —— 终态(需人工),不可重试
        Some(FailureReason::ApplicationError),
        1,
        0,
    )
    .await
}

/// 处理非幂等 Step 阻止的 Turn：写 tool_failed → turn_failed
pub async fn fail_turn_with_non_idempotent_block(
    store: &dyn EventStore,
    task_id: &str,
    turn_id: i64,
    blocking_steps: &[IncompleteStep],
) -> Result<()> {
    for step in blocking_steps {
        skip_non_idempotent_step(store, task_id, turn_id, step).await?;
    }

    service::fail_turn(
        store,
        task_id,
        turn_id,
        "recovery_blocked",
        &format!(
            "Recovery blocked: {} non-idempotent step(s) require human intervention",
            blocking_steps.len()
        ),
        None,
        Some(FailureReason::ApplicationError),
    )
    .await?;

    Ok(())
}

/// 准备重做上下文
///
/// 重做时 redo_count 递增，redo_group 不变。
#[derive(Debug, Clone)]
pub struct RedoContext {
    pub task_id: String,
    pub turn_id: i64,
    pub redo_group: String,
    pub redo_count: i32,
    pub user_input: String,
}

/// 从 turn_started payload 中提取 redo 上下文
///
/// 取数(本 turn 事件流) + 委托纯函数 [`build_redo_context_from_events`]。
pub async fn build_redo_context(
    store: &dyn EventStore,
    task_id: &str,
    incomplete_turn: &IncompleteTurn,
) -> Result<Option<RedoContext>> {
    let events = store
        .get_turn_events(task_id, incomplete_turn.turn_id)
        .await?;

    Ok(build_redo_context_from_events(task_id, incomplete_turn, &events))
}

/// 纯函数:从 turn 事件流中定位 TurnStarted,构造重做上下文(无 store 依赖,便于测试)。
///
/// - 找到 TurnStarted → [`Some`](`Option::Some`)(`redo_count` 递增,`redo_group` 不变,
///   `user_input` 取自 payload,缺省为空串)。
/// - 找不到 TurnStarted → [`None`](`Option::None`)(无法重建上下文)。
fn build_redo_context_from_events(
    task_id: &str,
    incomplete_turn: &IncompleteTurn,
    events: &[AgentEvent],
) -> Option<RedoContext> {
    let turn_started = events
        .iter()
        .find(|e| e.event_type == EventType::TurnStarted)?;

    let user_input = turn_started
        .payload
        .get("user_input")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    Some(RedoContext {
        task_id: task_id.to_string(),
        turn_id: incomplete_turn.turn_id,
        redo_group: incomplete_turn.redo_group.clone(),
        redo_count: incomplete_turn.redo_count + 1,
        user_input,
    })
}

/// Session 级恢复入口
///
/// 检查 Session 中所有未完成 Turn，对每个 Turn 做出恢复决策：
/// - SafeToRedo → 返回重做上下文，由上层分发 execute_turn (redo)
/// - RequiresHumanIntervention → 自动写 tool_failed + turn_failed
/// - None → 跳过
pub async fn recover_task(
    store: &dyn EventStore,
    task_id: &str,
) -> Result<Vec<RedoContext>> {
    let state = check_session_recovery(store, task_id).await?;

    if state.incomplete_turns.is_empty() {
        return Ok(vec![]);
    }

    let mut redo_queue = Vec::new();

    for incomplete_turn in &state.incomplete_turns {
        let decision = decide_turn_recovery(store, task_id, incomplete_turn).await?;

        match decision {
            RecoveryDecision::SafeToRedo { .. } => {
                if let Some(ctx) = build_redo_context(store, task_id, incomplete_turn).await? {
                    redo_queue.push(ctx);
                }
            }
            RecoveryDecision::RequiresHumanIntervention {
                turn_id,
                blocking_steps,
                ..
            } => {
                tracing::warn!(
                    "Turn {} requires human intervention due to {} non-idempotent step(s)",
                    turn_id,
                    blocking_steps.len()
                );
                fail_turn_with_non_idempotent_block(
                    store,
                    task_id,
                    turn_id,
                    &blocking_steps,
                )
                .await?;
            }
            RecoveryDecision::None => {}
        }
    }

    Ok(redo_queue)
}

// ── seq 连续性校验 ──────────────────────────────────────────────────────

/// 检查 Turn 内 seq 是否连续（用于 Turn 级重做判断）
pub async fn validate_turn_seq_continuity(
    store: &dyn EventStore,
    task_id: &str,
    turn_id: i64,
) -> Result<bool> {
    store.is_turn_seq_continuous(task_id, turn_id).await
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    // ── fixture ───────────────────────────────────────────────────────

    fn incomplete_turn(turn_id: i64, redo_group: &str, redo_count: i32) -> IncompleteTurn {
        IncompleteTurn {
            turn_id,
            redo_group: redo_group.to_string(),
            redo_count,
            turn_started_at: Utc::now(),
        }
    }

    fn incomplete_step(turn_id: i64, step_id: &str, tool_name: &str) -> IncompleteStep {
        IncompleteStep {
            seq: 1,
            turn_id,
            step_id: step_id.to_string(),
            start_event_type: "tool_invoked".to_string(),
            payload: json!({ "tool_name": tool_name, "tool_call_id": "tcid-1" }),
            started_at: Utc::now(),
        }
    }

    // ── classify_tool(纯函数,穷举三桶 + 默认)──────────────────────

    #[test]
    fn classify_read_only_tools() {
        for t in &[
            "get_weather",
            "search",
            "read_file",
            "list_directory",
            "get_order_status",
            "query_database",
            "fetch_url",
            "calculate",
            "parse",
        ] {
            assert_eq!(
                classify_tool(t),
                ToolRecoveryStrategy::ReadOnly,
                "tool {t} 应为 ReadOnly"
            );
        }
    }

    #[test]
    fn classify_idempotent_write_tools() {
        for t in &[
            "update_config",
            "set_status",
            "upsert_record",
            "create_or_update",
            "deploy_versioned",
        ] {
            assert_eq!(
                classify_tool(t),
                ToolRecoveryStrategy::IdempotentWrite,
                "tool {t} 应为 IdempotentWrite"
            );
        }
    }

    #[test]
    fn classify_non_idempotent_write_tools() {
        for t in &[
            "send_email",
            "create_order",
            "process_payment",
            "send_notification",
            "delete_record",
            "submit_form",
        ] {
            assert_eq!(
                classify_tool(t),
                ToolRecoveryStrategy::NonIdempotentWrite,
                "tool {t} 应为 NonIdempotentWrite"
            );
        }
    }

    #[test]
    fn classify_unknown_defaults_to_non_idempotent_safe() {
        // 未知 tool 默认非幂等(安全优先)—— 重做会保守转人工
        assert_eq!(classify_tool("brand_new_tool"), ToolRecoveryStrategy::NonIdempotentWrite);
        assert_eq!(classify_tool(""), ToolRecoveryStrategy::NonIdempotentWrite);
    }

    // ── decide_recovery_from_steps(纯函数)──────────────────────────

    #[test]
    fn decide_empty_steps_is_safe_to_redo() {
        let it = incomplete_turn(7, "rg-1", 0);
        match decide_recovery_from_steps(&it, &[]) {
            RecoveryDecision::SafeToRedo {
                turn_id,
                redo_group,
                redo_count,
                incomplete_steps,
            } => {
                assert_eq!(turn_id, 7);
                assert_eq!(redo_group, "rg-1");
                assert_eq!(redo_count, 0);
                assert!(incomplete_steps.is_empty());
            }
            other => panic!("应为 SafeToRedo, 得到 {other:?}"),
        }
    }

    #[test]
    fn decide_all_read_only_is_safe_to_redo() {
        let it = incomplete_turn(7, "rg", 1);
        let steps = vec![
            incomplete_step(7, "s1", "read_file"),
            incomplete_step(7, "s2", "search"),
        ];
        match decide_recovery_from_steps(&it, &steps) {
            RecoveryDecision::SafeToRedo { incomplete_steps, .. } => {
                assert_eq!(incomplete_steps.len(), 2);
            }
            other => panic!("应为 SafeToRedo, 得到 {other:?}"),
        }
    }

    #[test]
    fn decide_all_idempotent_writes_are_safe() {
        let it = incomplete_turn(7, "rg", 2);
        let steps = vec![incomplete_step(7, "s1", "upsert_record")];
        match decide_recovery_from_steps(&it, &steps) {
            RecoveryDecision::SafeToRedo { incomplete_steps, .. } => {
                assert_eq!(incomplete_steps.len(), 1);
            }
            other => panic!("应为 SafeToRedo, 得到 {other:?}"),
        }
    }

    #[test]
    fn decide_mixed_read_and_idempotent_is_safe() {
        let it = incomplete_turn(7, "rg", 0);
        let steps = vec![
            incomplete_step(7, "s1", "read_file"),
            incomplete_step(7, "s2", "set_status"),
        ];
        match decide_recovery_from_steps(&it, &steps) {
            RecoveryDecision::SafeToRedo { incomplete_steps, .. } => {
                assert_eq!(incomplete_steps.len(), 2);
            }
            other => panic!("应为 SafeToRedo, 得到 {other:?}"),
        }
    }

    #[test]
    fn decide_single_non_idempotent_blocks_turn() {
        let it = incomplete_turn(7, "rg", 0);
        let steps = vec![incomplete_step(7, "s1", "send_email")];
        match decide_recovery_from_steps(&it, &steps) {
            RecoveryDecision::RequiresHumanIntervention { blocking_steps, .. } => {
                assert_eq!(blocking_steps.len(), 1);
                assert_eq!(blocking_steps[0].step_id, "s1");
            }
            other => panic!("应为 RequiresHumanIntervention, 得到 {other:?}"),
        }
    }

    #[test]
    fn decide_non_idempotent_among_safe_blocks_and_only_carries_blocking() {
        // 一个 blocking 出现 → 整 turn 转人工;且只有非幂等进 blocking,安全 step 不混入
        let it = incomplete_turn(7, "rg", 0);
        let steps = vec![
            incomplete_step(7, "s1", "read_file"),       // safe
            incomplete_step(7, "s2", "process_payment"), // blocking
            incomplete_step(7, "s3", "send_email"),      // blocking
        ];
        match decide_recovery_from_steps(&it, &steps) {
            RecoveryDecision::RequiresHumanIntervention { blocking_steps, .. } => {
                assert_eq!(blocking_steps.len(), 2);
                let ids: Vec<&str> = blocking_steps.iter().map(|s| s.step_id.as_str()).collect();
                assert!(ids.contains(&"s2"));
                assert!(ids.contains(&"s3"));
            }
            other => panic!("应为 RequiresHumanIntervention, 得到 {other:?}"),
        }
    }

    #[test]
    fn decide_unknown_tool_is_treated_as_blocking() {
        // 未知 tool 默认非幂等 → blocking
        let it = incomplete_turn(7, "rg", 0);
        let steps = vec![incomplete_step(7, "s1", "something_new")];
        assert!(matches!(
            decide_recovery_from_steps(&it, &steps),
            RecoveryDecision::RequiresHumanIntervention { .. }
        ));
    }

    #[test]
    fn decide_step_missing_tool_name_defaults_to_blocking() {
        // payload 无 tool_name → unwrap_or("unknown") → 非幂等 → blocking
        let it = incomplete_turn(7, "rg", 0);
        let mut step = incomplete_step(7, "s1", "x");
        step.payload = json!({});
        assert!(matches!(
            decide_recovery_from_steps(&it, std::slice::from_ref(&step)),
            RecoveryDecision::RequiresHumanIntervention { .. }
        ));
    }

    #[test]
    fn decide_propagates_turn_metadata_into_blocking() {
        let it = incomplete_turn(42, "group-abc", 3);
        match decide_recovery_from_steps(&it, &[incomplete_step(42, "s1", "send_email")]) {
            RecoveryDecision::RequiresHumanIntervention { turn_id, redo_group, .. } => {
                assert_eq!(turn_id, 42);
                assert_eq!(redo_group, "group-abc");
            }
            other => panic!("应为 RequiresHumanIntervention, 得到 {other:?}"),
        }
    }

    // ── build_redo_context_from_events(纯函数)──────────────────────

    #[test]
    fn redo_context_none_when_no_turn_started() {
        let it = incomplete_turn(5, "rg", 0);
        let events = vec![AgentEvent::new(
            "t1".into(),
            Some(5),
            None,
            EventType::LlmCompleted,
            json!({}),
        )];
        assert!(build_redo_context_from_events("t1", &it, &events).is_none());
    }

    #[test]
    fn redo_context_extracts_user_input_and_increments_count() {
        let it = incomplete_turn(5, "rg-7", 2);
        let events = vec![AgentEvent::new(
            "t1".into(),
            Some(5),
            None,
            EventType::TurnStarted,
            json!({ "user_input": "redo me" }),
        )];
        let ctx = build_redo_context_from_events("t1", &it, &events).expect("应有 ctx");
        assert_eq!(ctx.task_id, "t1");
        assert_eq!(ctx.turn_id, 5);
        assert_eq!(ctx.redo_group, "rg-7");
        assert_eq!(ctx.redo_count, 3); // 2 + 1
        assert_eq!(ctx.user_input, "redo me");
    }

    #[test]
    fn redo_context_empty_user_input_when_payload_missing() {
        let it = incomplete_turn(5, "rg", 0);
        let events = vec![AgentEvent::new(
            "t1".into(),
            Some(5),
            None,
            EventType::TurnStarted,
            json!({}),
        )];
        let ctx = build_redo_context_from_events("t1", &it, &events).expect("应有 ctx");
        assert_eq!(ctx.user_input, "");
    }

    #[test]
    fn redo_context_finds_turn_started_among_many_events() {
        let it = incomplete_turn(5, "rg", 0);
        let events = vec![
            AgentEvent::new("t1".into(), Some(5), None, EventType::LlmInvoked, json!({})),
            AgentEvent::new(
                "t1".into(),
                Some(5),
                None,
                EventType::TurnStarted,
                json!({ "user_input": "hi" }),
            ),
            AgentEvent::new("t1".into(), Some(5), None, EventType::LlmCompleted, json!({})),
        ];
        let ctx = build_redo_context_from_events("t1", &it, &events).expect("应有 ctx");
        assert_eq!(ctx.user_input, "hi");
    }
}
