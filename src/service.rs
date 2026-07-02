//! Turn/Session/Summary 业务逻辑层
//!
//! 实现设计文档：
//! - 第 6 节：生命周期不变量
//! - 第 9 节：写入协议（Write-Ahead）
//! - 第 12 节：长会话摘要管理
//!
//! 所有 pub API 返回 `Result<T, AppError>`，不 panic。

use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::error::{AppError, Result};
use crate::models::{
    AgentEvent, EventType, SummaryMarkerPayload,
    TurnCompletedPayload, TurnFailedPayload, TurnStartedPayload,
};
use crate::storage;

// ── Session 生命周期管理 ────────────────────────────────────────────────

/// 创建新 Session
///
/// 执行设计文档 9.2 节的完整事务：
/// 1. 写入 sessions 表
/// 2. 初始化 seq counter
/// 3. 写入 session_started (seq = 1)
pub async fn create_session(
    pool: &SqlitePool,
    session_id: &str,
    tenant_id: &str,
    user_id: &str,
    agent_type: &str,
    metadata: Option<serde_json::Value>,
) -> Result<AgentEvent> {
    if storage::session_exists(pool, session_id).await? {
        return Err(AppError::SessionAlreadyExists(session_id.to_string()));
    }
    storage::create_session(pool, session_id, tenant_id, user_id, agent_type, metadata).await
}

/// 结束 Session
///
/// 写入 session_ended 事件。写入后该 session 不再接受任何新的业务事件。
pub async fn end_session(pool: &SqlitePool, session_id: &str, reason: &str) -> Result<AgentEvent> {
    if storage::is_session_ended(pool, session_id).await? {
        return Err(AppError::SessionAlreadyEnded(session_id.to_string()));
    }

    let event = AgentEvent::new(
        session_id.to_string(),
        None,
        None,
        EventType::SessionEnded,
        serde_json::json!({"reason": reason}),
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok(AgentEvent {
        seq,
        ..event
    })
}

// ── Turn 生命周期管理 ──────────────────────────────────────────────────

/// 开始一个新的 Turn
///
/// 生成 redo_group，写入 turn_started 事件。
///
/// ## turn_id 生成规则（设计文档 2.2 节）
/// - 单线程串行前提下，通过 `MAX(turn_id) + 1` 生成
/// - 崩溃恢复时重建计数器
pub async fn start_turn(
    pool: &SqlitePool,
    session_id: &str,
    user_input: &str,
    redo_group: Option<&str>,
) -> Result<(i64, String, AgentEvent)> {
    // 检查 session 存在且未结束
    if !storage::session_exists(pool, session_id).await? {
        return Err(AppError::SessionNotFound(session_id.to_string()));
    }
    if storage::is_session_ended(pool, session_id).await? {
        return Err(AppError::SessionAlreadyEnded(session_id.to_string()));
    }

    // 生成 turn_id
    let max_turn = storage::get_max_turn_id(pool, session_id).await?;
    let turn_id = max_turn + 1;

    // 生成 redo_group（幂等锚点）
    let redo_group = redo_group
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("rg_{}", Uuid::now_v7().to_string().replace('-', "")));

    let payload = serde_json::to_value(TurnStartedPayload {
        user_input: user_input.to_string(),
        redo_group: redo_group.clone(),
        redo_count: 0,
    })?;

    let event = AgentEvent::new(
        session_id.to_string(),
        Some(turn_id),
        None,
        EventType::TurnStarted,
        payload,
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok((
        turn_id,
        redo_group,
        AgentEvent { seq, ..event },
    ))
}

/// 完成一个 Turn
///
/// 写入 turn_completed 事件。
/// 写入前必须先完成所有 active step（写入 terminal event）。
pub async fn complete_turn(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
    final_output: &str,
) -> Result<AgentEvent> {
    let payload = serde_json::to_value(TurnCompletedPayload {
        final_output: final_output.to_string(),
    })?;

    let event = AgentEvent::new(
        session_id.to_string(),
        Some(turn_id),
        None,
        EventType::TurnCompleted,
        payload,
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// Turn 执行失败
///
/// 写入 turn_failed 事件。
///
/// ## 注意事项（设计文档 6.4 节）
/// turn_failed 表示 Turn 容器级失败，不替代 Step 的 terminal event。
/// 调用方应先在 active step 上写入 llm_failed/tool_failed，再调用此函数。
pub async fn fail_turn(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
    error_type: &str,
    error_message: &str,
    stack_trace: Option<&str>,
) -> Result<AgentEvent> {
    let payload = serde_json::to_value(TurnFailedPayload {
        error_type: error_type.to_string(),
        error_message: error_message.to_string(),
        stack_trace: stack_trace.map(|s| s.to_string()),
    })?;

    let event = AgentEvent::new(
        session_id.to_string(),
        Some(turn_id),
        None,
        EventType::TurnFailed,
        payload,
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 用户取消 Turn
pub async fn cancel_turn(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
    reason: &str,
) -> Result<AgentEvent> {
    let event = AgentEvent::new(
        session_id.to_string(),
        Some(turn_id),
        None,
        EventType::TurnCanceled,
        serde_json::json!({"reason": reason}),
    );
    let seq = storage::write_event(pool, &event).await?;
    Ok(AgentEvent { seq, ..event })
}

/// 阻塞 Turn（非幂等 Tool 悬空）
pub async fn block_turn(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
    reason: &str,
) -> Result<AgentEvent> {
    let event = AgentEvent::new(
        session_id.to_string(),
        Some(turn_id),
        None,
        EventType::TurnBlocked,
        serde_json::json!({"reason": reason}),
    );
    let seq = storage::write_event(pool, &event).await?;
    Ok(AgentEvent { seq, ..event })
}

/// 以重做方式开始 Turn（恢复时使用）
///
/// redo_count 递增，redo_group 保持不变。
pub async fn start_turn_redo(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
    user_input: &str,
    redo_group: &str,
    redo_count: i32,
) -> Result<AgentEvent> {
    let payload = serde_json::to_value(TurnStartedPayload {
        user_input: user_input.to_string(),
        redo_group: redo_group.to_string(),
        redo_count,
    })?;

    let event = AgentEvent::new(
        session_id.to_string(),
        Some(turn_id),
        None,
        EventType::TurnStarted,
        payload,
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok(AgentEvent { seq, ..event })
}

// ── Step 生命周期管理 ──────────────────────────────────────────────────

/// 记录 LLM 调用开始
///
/// 遵循 Write-Ahead 原则：先写 Event，再执行操作。
pub async fn record_llm_invoked(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    model: &str,
    messages: &[crate::models::Message],
    tools: Option<&[serde_json::Value]>,
    temperature: Option<f64>,
    local_seq: i64,
) -> Result<AgentEvent> {
    let mut payload_map = serde_json::json!({
        "step_type": "llm_call",
        "model": model,
        "messages": messages,
        "local_seq": local_seq,
    });

    if let Some(t) = tools {
        payload_map["tools"] = serde_json::Value::Array(t.to_vec());
    }
    if let Some(temp) = temperature {
        payload_map["temperature"] = serde_json::json!(temp);
    }

    let event = AgentEvent::new(
        session_id.to_string(),
        turn_id,
        Some(step_id.to_string()),
        EventType::LlmInvoked,
        payload_map,
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 记录 LLM 调用完成
pub async fn record_llm_completed(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    model: &str,
    content: Option<&str>,
    tool_calls: Option<&[crate::models::ToolCall]>,
    usage: Option<&crate::models::Usage>,
    finish_reason: Option<&str>,
    local_seq: i64,
) -> Result<AgentEvent> {
    let mut payload_map = serde_json::json!({
        "model": model,
        "local_seq": local_seq,
    });

    if let Some(c) = content {
        payload_map["content"] = serde_json::json!(c);
    }
    if let Some(tc) = tool_calls {
        payload_map["tool_calls"] = serde_json::to_value(tc)?;
    }
    if let Some(u) = usage {
        payload_map["usage"] = serde_json::to_value(u)?;
    }
    if let Some(fr) = finish_reason {
        payload_map["finish_reason"] = serde_json::json!(fr);
    }

    let event = AgentEvent::new(
        session_id.to_string(),
        turn_id,
        Some(step_id.to_string()),
        EventType::LlmCompleted,
        payload_map,
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 记录 LLM 调用失败
pub async fn record_llm_failed(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    error_type: &str,
    error_message: &str,
    retryable: bool,
    attempt: i32,
    local_seq: i64,
) -> Result<AgentEvent> {
    let payload = serde_json::json!({
        "error_type": error_type,
        "error_message": error_message,
        "retryable": retryable,
        "attempt": attempt,
        "local_seq": local_seq,
    });

    let event = AgentEvent::new(
        session_id.to_string(),
        turn_id,
        Some(step_id.to_string()),
        EventType::LlmFailed,
        payload,
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 记录 Tool 调用开始
///
/// ## idempotency_key 格式（设计文档 10.2 节）
/// `{session_id}:{redo_group}:{tool_name}:{call_signature}`
pub async fn record_tool_invoked(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    tool_name: &str,
    tool_call_id: &str,
    idempotency_key: &str,
    input: &serde_json::Value,
    parent_step_id: Option<&str>,
    local_seq: i64,
) -> Result<AgentEvent> {
    let mut payload_map = serde_json::json!({
        "step_type": "tool_call",
        "tool_name": tool_name,
        "tool_call_id": tool_call_id,
        "idempotency_key": idempotency_key,
        "input": input,
        "local_seq": local_seq,
    });

    if let Some(psid) = parent_step_id {
        payload_map["parent_step_id"] = serde_json::json!(psid);
    }

    let event = AgentEvent::new(
        session_id.to_string(),
        turn_id,
        Some(step_id.to_string()),
        EventType::ToolInvoked,
        payload_map,
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 记录 Tool 调用完成
pub async fn record_tool_completed(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    tool_call_id: &str,
    output: &serde_json::Value,
    is_error: bool,
    local_seq: i64,
) -> Result<AgentEvent> {
    let payload = serde_json::json!({
        "tool_call_id": tool_call_id,
        "output": output,
        "is_error": is_error,
        "local_seq": local_seq,
    });

    let event = AgentEvent::new(
        session_id.to_string(),
        turn_id,
        Some(step_id.to_string()),
        EventType::ToolCompleted,
        payload,
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 记录 Tool 调用失败
pub async fn record_tool_failed(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    tool_call_id: &str,
    error_type: &str,
    error_message: &str,
    retryable: bool,
    attempt: i32,
    local_seq: i64,
) -> Result<AgentEvent> {
    let payload = serde_json::json!({
        "tool_call_id": tool_call_id,
        "error_type": error_type,
        "error_message": error_message,
        "retryable": retryable,
        "attempt": attempt,
        "local_seq": local_seq,
    });

    let event = AgentEvent::new(
        session_id.to_string(),
        turn_id,
        Some(step_id.to_string()),
        EventType::ToolFailed,
        payload,
    );

    let seq = storage::write_event(pool, &event).await?;

    Ok(AgentEvent { seq, ..event })
}

// ── 摘要管理 ────────────────────────────────────────────────────────────

/// 摘要触发条件（设计文档 12.1 节）
#[derive(Debug, Clone)]
pub struct SummaryTrigger {
    pub turn_count_since_last_summary: i64,
    pub token_estimate: i64,
    pub event_count_since_last_summary: i64,
}

impl SummaryTrigger {
    /// 是否满足摘要触发条件
    ///
    /// - turn_count_since_last_summary ≥ 50
    /// - token_estimate ≥ 80000
    /// - event_count_since_last_summary ≥ 500
    pub fn should_summarize(&self) -> bool {
        self.turn_count_since_last_summary >= 50
            || self.token_estimate >= 80_000
            || self.event_count_since_last_summary >= 500
    }
}

/// 计算摘要触发条件
pub async fn check_summary_trigger(
    pool: &SqlitePool,
    session_id: &str,
) -> Result<SummaryTrigger> {
    let latest_summary = storage::get_latest_summary(pool, session_id).await?;

    let summarized_up_to_seq = latest_summary
        .as_ref()
        .and_then(|s| s.payload.get("summarized_up_to_seq"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    // 统计自上次摘要后的 Turn 数
    let row = sqlx::query(
        r#"
        SELECT COUNT(DISTINCT turn_id) as turn_count
        FROM agent_events
        WHERE session_id = ?1
          AND seq > ?2
          AND turn_id IS NOT NULL
          AND event_type = 'turn_started'
        "#,
    )
    .bind(session_id)
    .bind(summarized_up_to_seq)
    .fetch_one(pool)
    .await?;
    let turn_count: i64 = row.get("turn_count");

    // 统计自上次摘要后的事件数（仅业务事件）
    let row = sqlx::query(
        r#"
        SELECT COUNT(*) as cnt
        FROM agent_events
        WHERE session_id = ?1
          AND seq > ?2
          AND (
              (turn_id IS NOT NULL AND step_id IS NULL AND event_type IN ('turn_pending', 'turn_started', 'turn_completed', 'turn_failed', 'turn_canceled', 'turn_blocked'))
              OR
              (turn_id IS NOT NULL AND step_id IS NOT NULL AND event_type IN ('llm_invoked', 'llm_completed', 'llm_failed', 'tool_invoked', 'tool_completed', 'tool_failed'))
          )
        "#,
    )
    .bind(session_id)
    .bind(summarized_up_to_seq)
    .fetch_one(pool)
    .await?;
    let event_count: i64 = row.get("cnt");

    // Token 估算（从 llm_completed 累积 usage）
    let row = sqlx::query(
        r#"
        SELECT payload
        FROM agent_events
        WHERE session_id = ?1
          AND seq > ?2
          AND event_type = 'llm_completed'
        "#,
    )
    .bind(session_id)
    .bind(summarized_up_to_seq)
    .fetch_all(pool)
    .await?;

    let mut token_estimate: i64 = 0;
    for r in &row {
        let payload_str: String = r.get("payload");
        if let Ok(p) = serde_json::from_str::<serde_json::Value>(&payload_str) {
            if let Some(usage) = p.get("usage") {
                if let Some(total) = usage.get("total_tokens").and_then(|v| v.as_i64()) {
                    token_estimate += total;
                }
            }
        }
    }

    Ok(SummaryTrigger {
        turn_count_since_last_summary: turn_count,
        token_estimate,
        event_count_since_last_summary: event_count,
    })
}

/// 写入 summary_marker 事件
///
/// 对应设计文档 12.2 节步骤 7。
///
/// 摘要的完整流程（由上层编排）：
/// 1. 检测触发条件 → check_summary_trigger
/// 2. 确定边界 → current_max_seq
/// 3. 用 llm_invoked/llm_completed 记录摘要 LLM 调用（Session 级 Step）
/// 4. 调用此函数写入 summary_marker
pub async fn write_summary_marker(
    pool: &SqlitePool,
    session_id: &str,
    summarized_up_to_turn_id: i64,
    summarized_up_to_seq: i64,
    summary: &str,
    covered_event_count: i64,
) -> Result<AgentEvent> {
    let payload = serde_json::to_value(SummaryMarkerPayload {
        summarized_up_to_turn_id,
        summarized_up_to_seq,
        summary: summary.to_string(),
        covered_event_count,
    })?;

    let event = AgentEvent::new(
        session_id.to_string(),
        None,
        None,
        EventType::SummaryMarker,
        payload,
    );

    let seq = storage::write_event(pool, &event).await?;

    // 自动归档已摘要覆盖的 Event（后台异步，best-effort）
    if summarized_up_to_seq > 0 {
        let pool_clone = pool.clone();
        let sid = session_id.to_string();
        tokio::spawn(async move {
            match storage::archive_events_before_seq(&pool_clone, &sid, summarized_up_to_seq + 1).await {
                Ok(result) if result.archived > 0 => {
                    tracing::info!("Auto-archived {} events for session {}", result.archived, sid);
                }
                Err(e) => tracing::warn!("Auto-archive failed for session {}: {}", sid, e),
                _ => {}
            }
        });
    }

    Ok(AgentEvent { seq, ..event })
}

/// 使用便捷 payload 类型的通用事件记录函数（fixlet 事件回传入库用）
pub async fn record_event(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: Option<i64>,
    step_id: Option<&str>,
    event_type: EventType,
    payload: serde_json::Value,
) -> Result<i64> {
    let event = AgentEvent::new(
        session_id.to_string(),
        turn_id,
        step_id.map(|s| s.to_string()),
        event_type,
        payload,
    );

    storage::write_event(pool, &event).await
}

// ── 查询封装（供 server 层调用，不直接暴露 storage） ──────────────────────

/// 读取某个 Turn 的完整执行过程
pub async fn get_turn_events(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
) -> Result<Vec<AgentEvent>> {
    storage::get_turn_events(pool, session_id, turn_id).await
}

/// Turn 内的 Step 列表（含耗时）
pub async fn get_turn_steps(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
) -> Result<Vec<crate::models::StepExecution>> {
    storage::get_turn_steps(pool, session_id, turn_id).await
}

/// 批量写入 Event（在同一事务中）
pub async fn write_events_batch(
    pool: &SqlitePool,
    events: &[AgentEvent],
) -> Result<Vec<i64>> {
    storage::write_events_batch(pool, events).await
}

/// LLM Token 消耗统计
pub async fn get_token_usage_stats(
    pool: &SqlitePool,
    session_id: &str,
) -> Result<Vec<crate::models::TokenUsageStats>> {
    storage::get_token_usage_stats(pool, session_id).await
}

/// Session 详情聚合查询（供 server 层使用）
///
/// 合并 session 基本信息 + is_ended + turn_count + event_count，
/// 避免上层直接调用多个 storage 函数。
pub async fn get_session_info(
    pool: &SqlitePool,
    session_id: &str,
) -> Result<Option<crate::models::Session>> {
    storage::get_session(pool, session_id).await
}

/// 检查 Session 是否已结束
pub async fn is_session_ended(pool: &SqlitePool, session_id: &str) -> Result<bool> {
    storage::is_session_ended(pool, session_id).await
}

/// 获取 Session 当前最大 turn_id
pub async fn get_max_turn_id(pool: &SqlitePool, session_id: &str) -> Result<i64> {
    storage::get_max_turn_id(pool, session_id).await
}

/// 获取 Session 当前最大 seq
pub async fn get_max_seq(pool: &SqlitePool, session_id: &str) -> Result<i64> {
    storage::get_max_seq(pool, session_id).await
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn setup() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        storage::run_migrations_on(&pool).await.unwrap();
        pool
    }

    async fn setup_with_session(pool: &SqlitePool) -> String {
        let sid = format!("sess_{}", Uuid::now_v7().to_string().replace('-', ""));
        create_session(pool, &sid, "default", "", "test_agent", None)
            .await
            .unwrap();
        sid
    }

    #[tokio::test]
    async fn test_simple_turn_lifecycle() {
        let pool = setup().await;
        let sid = setup_with_session(&pool).await;

        // Start turn
        let (turn_id, redo_group, started) =
            start_turn(&pool, &sid, "Hello, world!", None).await.unwrap();
        assert_eq!(turn_id, 1);
        assert!(!redo_group.is_empty());
        assert_eq!(started.event_type, EventType::TurnStarted);
        assert_eq!(started.payload["redo_group"], redo_group);

        // Record llm step
        let step_id = "step_001";
        let _llm_inv = record_llm_invoked(
            &pool, &sid, Some(turn_id), step_id,
            "gpt-4",
            &[crate::models::Message { role: "user".into(), content: "Hello".into() }],
            None, None, 1,
        ).await.unwrap();

        let _llm_cmp = record_llm_completed(
            &pool, &sid, Some(turn_id), step_id,
            "gpt-4",
            Some("Hi there!"),
            None,
            Some(&crate::models::Usage { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 }),
            Some("stop"),
            2,
        ).await.unwrap();

        // Complete turn
        let completed = complete_turn(&pool, &sid, turn_id, "Hi there!")
            .await
            .unwrap();
        assert_eq!(completed.event_type, EventType::TurnCompleted);

        // Verify turn events
        let events = storage::get_turn_events(&pool, &sid, turn_id)
            .await
            .unwrap();
        assert_eq!(events.len(), 4); // turn_started, llm_invoked, llm_completed, turn_completed
    }

    #[tokio::test]
    async fn test_turn_failed_lifecycle() {
        let pool = setup().await;
        let sid = setup_with_session(&pool).await;

        let (turn_id, _, _) = start_turn(&pool, &sid, "test", None).await.unwrap();

        // Simulate step failure then turn failure
        let step_id = "step_fail_1";
        let _tool_inv = record_tool_invoked(
            &pool, &sid, Some(turn_id), step_id,
            "bad_tool", "call_001",
            "sess:rg:bad_tool:{}",
            &serde_json::json!({"arg": 1}),
            None, 1,
        ).await.unwrap();

        let _tool_fail = record_tool_failed(
            &pool, &sid, Some(turn_id), step_id,
            "call_001", "external_api_error", "API returned 503",
            true, 1, 2,
        ).await.unwrap();

        let failed = fail_turn(
            &pool, &sid, turn_id, "execution_error", "Turn failed due to tool error", None,
        ).await.unwrap();
        assert_eq!(failed.event_type, EventType::TurnFailed);

        // Verify turn is not incomplete
        let incomplete = storage::get_incomplete_turns(&pool, &sid)
            .await
            .unwrap();
        assert!(incomplete.iter().all(|t| t.turn_id != turn_id));
    }

    #[tokio::test]
    async fn test_duplicate_turn_terminal_prevented() {
        let pool = setup().await;
        let sid = setup_with_session(&pool).await;

        let (turn_id, _, _) = start_turn(&pool, &sid, "test", None).await.unwrap();
        complete_turn(&pool, &sid, turn_id, "done").await.unwrap();

        // 重复写 terminal 应失败
        let result = complete_turn(&pool, &sid, turn_id, "done again").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_consecutive_turns() {
        let pool = setup().await;
        let sid = setup_with_session(&pool).await;

        for i in 1..=3 {
            let (turn_id, _, _) = start_turn(
                &pool, &sid, &format!("turn {}", i), None,
            ).await.unwrap();
            assert_eq!(turn_id, i);

            complete_turn(&pool, &sid, turn_id, &format!("result {}", i))
                .await
                .unwrap();
        }

        let max_turn = storage::get_max_turn_id(&pool, &sid).await.unwrap();
        assert_eq!(max_turn, 3);
    }

    #[tokio::test]
    async fn test_session_ended_prevents_new_turns() {
        let pool = setup().await;
        let sid = setup_with_session(&pool).await;

        end_session(&pool, &sid, "done").await.unwrap();

        let result = start_turn(&pool, &sid, "hello", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_summary_trigger_check() {
        let pool = setup().await;
        let sid = setup_with_session(&pool).await;

        // 写入一些 llm_completed 事件后检查摘要触发条件
        let trigger = check_summary_trigger(&pool, &sid).await.unwrap();
        // 新 session 应该不触发
        assert!(!trigger.should_summarize());
        assert_eq!(trigger.turn_count_since_last_summary, 0);
    }

    #[tokio::test]
    async fn test_write_summary_marker() {
        let pool = setup().await;
        let sid = setup_with_session(&pool).await;

        let marker = write_summary_marker(&pool, &sid, 5, 100, "Summary of first 5 turns", 100)
            .await
            .unwrap();
        assert_eq!(marker.event_type, EventType::SummaryMarker);
        assert_eq!(marker.payload["summarized_up_to_turn_id"], 5);
        assert_eq!(marker.payload["summarized_up_to_seq"], 100);

        // 读回最新摘要
        let latest = storage::get_latest_summary(&pool, &sid).await.unwrap().unwrap();
        assert_eq!(latest.payload["summary"], "Summary of first 5 turns");
    }

    #[tokio::test]
    async fn test_cancel_turn() {
        let pool = setup().await;
        let sid = setup_with_session(&pool).await;
        let (turn_id, _, _) = start_turn(&pool, &sid, "test", None).await.unwrap();

        let event = cancel_turn(&pool, &sid, turn_id, "user canceled").await.unwrap();
        assert_eq!(event.event_type, EventType::TurnCanceled);
        assert!(event.payload["reason"].as_str().unwrap().contains("canceled"));

        // Terminal written → no longer incomplete
        let incomplete = storage::get_incomplete_turns(&pool, &sid).await.unwrap();
        assert!(incomplete.iter().all(|t| t.turn_id != turn_id));
    }

    #[tokio::test]
    async fn test_block_turn() {
        let pool = setup().await;
        let sid = setup_with_session(&pool).await;
        let (turn_id, _, _) = start_turn(&pool, &sid, "test", None).await.unwrap();

        let event = block_turn(&pool, &sid, turn_id, "non-idempotent tool in-flight").await.unwrap();
        assert_eq!(event.event_type, EventType::TurnBlocked);

        let incomplete = storage::get_incomplete_turns(&pool, &sid).await.unwrap();
        assert!(incomplete.iter().all(|t| t.turn_id != turn_id));
    }
}
