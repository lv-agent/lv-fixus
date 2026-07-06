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
use crate::models::{AgentEvent, EventType, IncompleteStep, IncompleteTurn};
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
    pub session_id: String,
    pub incomplete_turns: Vec<IncompleteTurn>,
}

// ── 恢复检查 ────────────────────────────────────────────────────────────

/// 检查 Session 的恢复状态
///
/// 返回所有未完成的 Turn。
pub async fn check_session_recovery(
    store: &dyn EventStore,
    session_id: &str,
) -> Result<SessionRecoveryState> {
    let incomplete_turns = store.get_incomplete_turns(session_id).await?;

    Ok(SessionRecoveryState {
        session_id: session_id.to_string(),
        incomplete_turns,
    })
}

/// 对单个未完成 Turn 做出恢复决策
///
/// 分析 Turn 内未完成 Step 的 Tool 类型，决定恢复策略。
pub async fn decide_turn_recovery(
    store: &dyn EventStore,
    session_id: &str,
    incomplete_turn: &IncompleteTurn,
) -> Result<RecoveryDecision> {
    let _turn_events = store
        .get_turn_events(session_id, incomplete_turn.turn_id)
        .await?;

    let all_incomplete = store.get_incomplete_steps(session_id).await?;
    let turn_incomplete: Vec<_> = all_incomplete
        .into_iter()
        .filter(|s| s.turn_id == incomplete_turn.turn_id)
        .collect();

    if turn_incomplete.is_empty() {
        return Ok(RecoveryDecision::SafeToRedo {
            turn_id: incomplete_turn.turn_id,
            redo_group: incomplete_turn.redo_group.clone(),
            redo_count: incomplete_turn.redo_count,
            incomplete_steps: vec![],
        });
    }

    let mut blocking_steps = Vec::new();
    let mut safe_steps = Vec::new();

    for step in &turn_incomplete {
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
        Ok(RecoveryDecision::SafeToRedo {
            turn_id: incomplete_turn.turn_id,
            redo_group: incomplete_turn.redo_group.clone(),
            redo_count: incomplete_turn.redo_count,
            incomplete_steps: safe_steps,
        })
    } else {
        Ok(RecoveryDecision::RequiresHumanIntervention {
            turn_id: incomplete_turn.turn_id,
            redo_group: incomplete_turn.redo_group.clone(),
            blocking_steps,
        })
    }
}

// ── 恢复执行 ────────────────────────────────────────────────────────────

/// 为非幂等 Tool 写入 tool_failed 事件
///
/// 对应设计文档 10.3 节：非幂等写 → 写 tool_failed → 人工介入。
pub async fn skip_non_idempotent_step(
    store: &dyn EventStore,
    session_id: &str,
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
        session_id,
        Some(turn_id),
        &step.step_id,
        tool_call_id,
        error_type,
        &format!(
            "Recovery skipped: non-idempotent tool '{}' had an in-flight step at crash time. Requires human review.",
            step.payload.get("tool_name").and_then(|v| v.as_str()).unwrap_or("unknown")
        ),
        false,
        1,
        0,
    )
    .await
}

/// 处理非幂等 Step 阻止的 Turn：写 tool_failed → turn_failed
pub async fn fail_turn_with_non_idempotent_block(
    store: &dyn EventStore,
    session_id: &str,
    turn_id: i64,
    blocking_steps: &[IncompleteStep],
) -> Result<()> {
    for step in blocking_steps {
        skip_non_idempotent_step(store, session_id, turn_id, step).await?;
    }

    service::fail_turn(
        store,
        session_id,
        turn_id,
        "recovery_blocked",
        &format!(
            "Recovery blocked: {} non-idempotent step(s) require human intervention",
            blocking_steps.len()
        ),
        None,
    )
    .await?;

    Ok(())
}

/// 准备重做上下文
///
/// 重做时 redo_count 递增，redo_group 不变。
#[derive(Debug, Clone)]
pub struct RedoContext {
    pub session_id: String,
    pub turn_id: i64,
    pub redo_group: String,
    pub redo_count: i32,
    pub user_input: String,
}

/// 从 turn_started payload 中提取 redo 上下文
pub async fn build_redo_context(
    store: &dyn EventStore,
    session_id: &str,
    incomplete_turn: &IncompleteTurn,
) -> Result<Option<RedoContext>> {
    let events = store
        .get_turn_events(session_id, incomplete_turn.turn_id)
        .await?;

    let turn_started = events
        .iter()
        .find(|e| e.event_type == EventType::TurnStarted);

    match turn_started {
        Some(event) => {
            let user_input = event
                .payload
                .get("user_input")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            Ok(Some(RedoContext {
                session_id: session_id.to_string(),
                turn_id: incomplete_turn.turn_id,
                redo_group: incomplete_turn.redo_group.clone(),
                redo_count: incomplete_turn.redo_count + 1,
                user_input,
            }))
        }
        None => Ok(None),
    }
}

/// Session 级恢复入口
///
/// 检查 Session 中所有未完成 Turn，对每个 Turn 做出恢复决策：
/// - SafeToRedo → 返回重做上下文，由上层分发 execute_turn (redo)
/// - RequiresHumanIntervention → 自动写 tool_failed + turn_failed
/// - None → 跳过
pub async fn recover_session(
    store: &dyn EventStore,
    session_id: &str,
) -> Result<Vec<RedoContext>> {
    let state = check_session_recovery(store, session_id).await?;

    if state.incomplete_turns.is_empty() {
        return Ok(vec![]);
    }

    let mut redo_queue = Vec::new();

    for incomplete_turn in &state.incomplete_turns {
        let decision = decide_turn_recovery(store, session_id, incomplete_turn).await?;

        match decision {
            RecoveryDecision::SafeToRedo { .. } => {
                if let Some(ctx) = build_redo_context(store, session_id, incomplete_turn).await? {
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
                    session_id,
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
    session_id: &str,
    turn_id: i64,
) -> Result<bool> {
    store.is_turn_seq_continuous(session_id, turn_id).await
}

// ── 测试 ────────────────────────────────────────────────────────────────
