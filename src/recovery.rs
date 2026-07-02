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

use sqlx::SqlitePool;

use crate::error::Result;
use crate::models::{AgentEvent, EventType, IncompleteStep, IncompleteTurn};
use crate::{service, storage};

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
    // 已知的纯读 Tool（示例）
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

    // 已知的幂等写 Tool（示例）
    const IDEMPOTENT_TOOLS: &[&str] = &[
        "update_config",
        "set_status",
        "upsert_record",
        "create_or_update",
        "deploy_versioned",
    ];

    // 已知的非幂等写 Tool（示例）
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
        // 未知 Tool 默认非幂等（安全优先）
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
    pool: &SqlitePool,
    session_id: &str,
) -> Result<SessionRecoveryState> {
    let incomplete_turns = storage::get_incomplete_turns(pool, session_id).await?;

    Ok(SessionRecoveryState {
        session_id: session_id.to_string(),
        incomplete_turns,
    })
}

/// 对单个未完成 Turn 做出恢复决策
///
/// 分析 Turn 内未完成 Step 的 Tool 类型，决定恢复策略。
pub async fn decide_turn_recovery(
    pool: &SqlitePool,
    session_id: &str,
    incomplete_turn: &IncompleteTurn,
) -> Result<RecoveryDecision> {
    // 获取该 Turn 内未完成的 Step
    let _turn_events = storage::get_turn_events(pool, session_id, incomplete_turn.turn_id).await?;

    // 找出该 Turn 内的等价未完成 Step
    let all_incomplete = storage::get_incomplete_steps(pool, session_id).await?;
    let turn_incomplete: Vec<_> = all_incomplete
        .into_iter()
        .filter(|s| s.turn_id == incomplete_turn.turn_id)
        .collect();

    if turn_incomplete.is_empty() {
        // 没有未完成 Step，但 Turn 没有 terminal — 可能是空 Turn
        // 安全重做
        return Ok(RecoveryDecision::SafeToRedo {
            turn_id: incomplete_turn.turn_id,
            redo_group: incomplete_turn.redo_group.clone(),
            redo_count: incomplete_turn.redo_count,
            incomplete_steps: vec![],
        });
    }

    // 检查每个未完成 Step 的 Tool 类型
    let mut blocking_steps = Vec::new();
    let mut safe_steps = Vec::new();

    for step in &turn_incomplete {
        // 从 payload 中提取 tool_name
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
    pool: &SqlitePool,
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
        pool,
        session_id,
        Some(turn_id),
        &step.step_id,
        tool_call_id,
        error_type,
        &format!(
            "Recovery skipped: non-idempotent tool '{}' had an in-flight step at crash time. Requires human review.",
            step.payload.get("tool_name").and_then(|v| v.as_str()).unwrap_or("unknown")
        ),
        false, // 不可重试（非幂等写）
        1,
        0, // local_seq 未知，置 0
    )
    .await
}

/// 处理非幂等 Step 阻止的 Turn：写 tool_failed → turn_failed
pub async fn fail_turn_with_non_idempotent_block(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
    blocking_steps: &[IncompleteStep],
) -> Result<()> {
    // 先为每个 blocking step 写 tool_failed
    for step in blocking_steps {
        skip_non_idempotent_step(pool, session_id, turn_id, step).await?;
    }

    // 再写 turn_failed
    service::fail_turn(
        pool,
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
    pub redo_count: i32, // 已递增
    pub user_input: String,
}

/// 从 turn_started payload 中提取 redo 上下文
pub async fn build_redo_context(
    pool: &SqlitePool,
    session_id: &str,
    incomplete_turn: &IncompleteTurn,
) -> Result<Option<RedoContext>> {
    // 读取 turn_started 事件获取 user_input
    let events = storage::get_turn_events(pool, session_id, incomplete_turn.turn_id).await?;

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
pub async fn recover_session(pool: &SqlitePool, session_id: &str) -> Result<Vec<RedoContext>> {
    let state = check_session_recovery(pool, session_id).await?;

    if state.incomplete_turns.is_empty() {
        return Ok(vec![]);
    }

    let mut redo_queue = Vec::new();

    for incomplete_turn in &state.incomplete_turns {
        let decision = decide_turn_recovery(pool, session_id, incomplete_turn).await?;

        match decision {
            RecoveryDecision::SafeToRedo { .. } => {
                if let Some(ctx) =
                    build_redo_context(pool, session_id, incomplete_turn).await?
                {
                    redo_queue.push(ctx);
                }
            }
            RecoveryDecision::RequiresHumanIntervention {
                turn_id,
                blocking_steps,
                ..
            } => {
                // 自动失败这些 Turn
                tracing::warn!(
                    "Turn {} requires human intervention due to {} non-idempotent step(s)",
                    turn_id,
                    blocking_steps.len()
                );
                fail_turn_with_non_idempotent_block(
                    pool,
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
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
) -> Result<bool> {
    storage::is_turn_seq_continuous(pool, session_id, turn_id).await
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn setup() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        crate::storage::run_migrations_on(&pool).await.unwrap();
        pool
    }

    async fn setup_session(pool: &SqlitePool) -> String {
        let sid = format!("rec_sess_{}", uuid::Uuid::now_v7().to_string().replace('-', ""));
        service::create_session(pool, &sid, "default", "", "test_agent", None)
            .await
            .unwrap();
        sid
    }

    #[tokio::test]
    async fn test_classify_read_only_tool() {
        assert_eq!(classify_tool("get_weather"), ToolRecoveryStrategy::ReadOnly);
        assert_eq!(classify_tool("search"), ToolRecoveryStrategy::ReadOnly);
        assert_eq!(classify_tool("calculate"), ToolRecoveryStrategy::ReadOnly);
    }

    #[tokio::test]
    async fn test_classify_idempotent_write_tool() {
        assert_eq!(
            classify_tool("update_config"),
            ToolRecoveryStrategy::IdempotentWrite
        );
        assert_eq!(
            classify_tool("upsert_record"),
            ToolRecoveryStrategy::IdempotentWrite
        );
    }

    #[tokio::test]
    async fn test_classify_non_idempotent_write_tool() {
        assert_eq!(
            classify_tool("send_email"),
            ToolRecoveryStrategy::NonIdempotentWrite
        );
        assert_eq!(
            classify_tool("process_payment"),
            ToolRecoveryStrategy::NonIdempotentWrite
        );
    }

    #[tokio::test]
    async fn test_classify_unknown_tool_defaults_to_non_idempotent() {
        // 未知 Tool 默认非幂等（安全优先）
        assert_eq!(
            classify_tool("some_unknown_tool"),
            ToolRecoveryStrategy::NonIdempotentWrite
        );
    }

    #[tokio::test]
    async fn test_recovery_clean_session() {
        let pool = setup().await;
        let sid = setup_session(&pool).await;

        let state = check_session_recovery(&pool, &sid).await.unwrap();
        assert!(state.incomplete_turns.is_empty());
    }

    #[tokio::test]
    async fn test_recovery_incomplete_turn_safe_to_redo() {
        let pool = setup().await;
        let sid = setup_session(&pool).await;

        // 创建未完成的 Turn（有 turn_started，无 terminal）
        let (turn_id, redo_group, _) =
            service::start_turn(&pool, &sid, "test input", None)
                .await
                .unwrap();

        // 检测未完成 Turn
        let incomplete = storage::get_incomplete_turns(&pool, &sid)
            .await
            .unwrap();
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].turn_id, turn_id);
        assert_eq!(incomplete[0].redo_group, redo_group);

        // 恢复决策：安全重做（没有未完成 Step）
        let decision = decide_turn_recovery(&pool, &sid, &incomplete[0])
            .await
            .unwrap();
        match decision {
            RecoveryDecision::SafeToRedo {
                turn_id: tid,
                redo_group: rg,
                incomplete_steps: steps,
                ..
            } => {
                assert_eq!(tid, turn_id);
                assert_eq!(rg, redo_group);
                assert!(steps.is_empty());
            }
            _ => panic!("Expected SafeToRedo"),
        }
    }

    #[tokio::test]
    async fn test_recovery_with_read_only_tool_incomplete() {
        let pool = setup().await;
        let sid = setup_session(&pool).await;

        let (turn_id, _redo_group, _) =
            service::start_turn(&pool, &sid, "test", None)
                .await
                .unwrap();

        // 写一个 tool_invoked（纯读 Tool）但不写 terminal
        service::record_tool_invoked(
            &pool,
            &sid,
            Some(turn_id),
            "step_rd",
            "get_weather",
            "call_wx",
            "sess:rg:get_weather:{}",
            &serde_json::json!({"city": "Beijing"}),
            None,
            1,
        )
        .await
        .unwrap();

        let incomplete = storage::get_incomplete_turns(&pool, &sid)
            .await
            .unwrap();
        let decision = decide_turn_recovery(&pool, &sid, &incomplete[0])
            .await
            .unwrap();

        match decision {
            RecoveryDecision::SafeToRedo { incomplete_steps, .. } => {
                assert_eq!(incomplete_steps.len(), 1);
                assert_eq!(incomplete_steps[0].start_event_type, "tool_invoked");
            }
            _ => panic!("Expected SafeToRedo with incomplete step"),
        }
    }

    #[tokio::test]
    async fn test_recovery_with_non_idempotent_tool_incomplete() {
        let pool = setup().await;
        let sid = setup_session(&pool).await;

        let (turn_id, _, _) = service::start_turn(&pool, &sid, "send mail", None)
            .await
            .unwrap();

        // 写一个 tool_invoked（非幂等写 Tool）但不写 terminal
        service::record_tool_invoked(
            &pool,
            &sid,
            Some(turn_id),
            "step_email",
            "send_email",
            "call_email",
            "sess:rg:send_email:{}",
            &serde_json::json!({"to": "user@example.com"}),
            None,
            1,
        )
        .await
        .unwrap();

        let incomplete = storage::get_incomplete_turns(&pool, &sid)
            .await
            .unwrap();
        let decision = decide_turn_recovery(&pool, &sid, &incomplete[0])
            .await
            .unwrap();

        match decision {
            RecoveryDecision::RequiresHumanIntervention {
                blocking_steps, ..
            } => {
                assert_eq!(blocking_steps.len(), 1);
            }
            _ => panic!("Expected RequiresHumanIntervention"),
        }
    }

    #[tokio::test]
    async fn test_fail_turn_with_non_idempotent_block() {
        let pool = setup().await;
        let sid = setup_session(&pool).await;

        let (turn_id, _, _) = service::start_turn(&pool, &sid, "send mail", None)
            .await
            .unwrap();

        // 创建 blocking step
        let blocking_step = IncompleteStep {
            seq: 3,
            turn_id,
            step_id: "step_email".into(),
            start_event_type: "tool_invoked".into(),
            payload: serde_json::json!({
                "tool_name": "send_email",
                "tool_call_id": "call_email"
            }),
            started_at: chrono::Utc::now(),
        };

        fail_turn_with_non_idempotent_block(&pool, &sid, turn_id, &[blocking_step])
            .await
            .unwrap();

        // Turn 应已结束
        let incomplete = storage::get_incomplete_turns(&pool, &sid)
            .await
            .unwrap();
        assert!(incomplete.iter().all(|t| t.turn_id != turn_id));

        // 检查 turn_failed 存在
        let events = storage::get_turn_events(&pool, &sid, turn_id)
            .await
            .unwrap();
        let has_turn_failed = events
            .iter()
            .any(|e| e.event_type == EventType::TurnFailed);
        assert!(has_turn_failed);
    }

    #[tokio::test]
    async fn test_session_recovery_workflow() {
        let pool = setup().await;
        let sid = setup_session(&pool).await;

        // 创建两个未完成 Turn
        let (t1, _, _) = service::start_turn(&pool, &sid, "turn 1", None)
            .await
            .unwrap();
        let (t2, _, _) = service::start_turn(&pool, &sid, "turn 2", None)
            .await
            .unwrap();

        // 恢复 session
        let redo_queue = recover_session(&pool, &sid).await.unwrap();
        // 两个 Turn 都是空的（没有未完成 Step），都应该可以安全重做
        assert_eq!(redo_queue.len(), 2);

        // 重做上下文中 turn_id 存在
        let tids: Vec<i64> = redo_queue.iter().map(|c| c.turn_id).collect();
        assert!(tids.contains(&t1));
        assert!(tids.contains(&t2));

        // redo_count 递增
        for ctx in &redo_queue {
            assert_eq!(ctx.redo_count, 1, "redo_count should be incremented from 0 to 1");
        }
    }
}
