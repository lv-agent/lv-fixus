//! Turn/Session/Summary 业务逻辑层
//!
//! 实现设计文档：
//! - 第 6 节：生命周期不变量
//! - 第 9 节：写入协议（Write-Ahead）
//! - 第 12 节：长会话摘要管理
//!
//! 所有 pub API 返回 `Result<T, AppError>`，不 panic。

use uuid::Uuid;

use crate::error::{AppError, Result};
use crate::models::{
    AgentEvent, EventType, SummaryMarkerPayload, TurnCompletedPayload, TurnFailedPayload,
    TurnStartedPayload,
};
use crate::storage::EventStore;

// ── Session 生命周期管理 ────────────────────────────────────────────────

/// 创建新 Session
///
/// 执行设计文档 9.2 节的完整事务：
/// 1. 写入 sessions 表
/// 2. 初始化 seq counter
/// 3. 写入 session_started (seq = 1)
pub async fn create_session(
    store: &dyn EventStore,
    session_id: &str,
    tenant_id: &str,
    user_id: &str,
    agent_type: &str,
    metadata: Option<serde_json::Value>,
) -> Result<AgentEvent> {
    if store.session_exists(session_id).await? {
        return Err(AppError::SessionAlreadyExists(session_id.to_string()));
    }
    store
        .create_session(session_id, tenant_id, user_id, agent_type, metadata)
        .await
}

/// 结束 Session
///
/// 写入 session_ended 事件。写入后该 session 不再接受任何新的业务事件。
pub async fn end_session(
    store: &dyn EventStore,
    session_id: &str,
    reason: &str,
) -> Result<AgentEvent> {
    if store.is_session_ended(session_id).await? {
        return Err(AppError::SessionAlreadyEnded(session_id.to_string()));
    }

    let event = AgentEvent::new(
        session_id.to_string(),
        None,
        None,
        EventType::SessionEnded,
        serde_json::json!({"reason": reason}),
    );

    let seq = store.write_event(&event).await?;

    Ok(AgentEvent { seq, ..event })
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
    store: &dyn EventStore,
    session_id: &str,
    user_input: &str,
    redo_group: Option<&str>,
) -> Result<(i64, String, AgentEvent)> {
    // 检查 session 存在且未结束。
    // session_exists 经 query cache(logdbd Indexer 异步索引),紧随 create_session
    // 调用 start_turn 时可能短暂查不到(表未建 / 未索引)→ 容忍一个短窗口重试。
    let mut exists = false;
    for _ in 0..30 {
        match store.session_exists(session_id).await {
            Ok(true) => {
                exists = true;
                break;
            }
            Ok(false) | Err(_) => {} // 缓存尚未追平,继续
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    if !exists {
        return Err(AppError::SessionNotFound(session_id.to_string()));
    }
    if store.is_session_ended(session_id).await? {
        return Err(AppError::SessionAlreadyEnded(session_id.to_string()));
    }

    // 生成 turn_id
    let max_turn = store.get_max_turn_id(session_id).await?;
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

    let seq = store.write_event(&event).await?;

    Ok((turn_id, redo_group, AgentEvent { seq, ..event }))
}

/// 完成一个 Turn
///
/// 写入 turn_completed 事件。
/// 写入前必须先完成所有 active step（写入 terminal event）。
pub async fn complete_turn(
    store: &dyn EventStore,
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

    let seq = store.write_event(&event).await?;

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
    store: &dyn EventStore,
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

    let seq = store.write_event(&event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 用户取消 Turn
pub async fn cancel_turn(
    store: &dyn EventStore,
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
    let seq = store.write_event(&event).await?;
    Ok(AgentEvent { seq, ..event })
}

/// 阻塞 Turn（非幂等 Tool 悬空）
pub async fn block_turn(
    store: &dyn EventStore,
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
    let seq = store.write_event(&event).await?;
    Ok(AgentEvent { seq, ..event })
}

/// 以重做方式开始 Turn（恢复时使用）
///
/// redo_count 递增，redo_group 保持不变。
pub async fn start_turn_redo(
    store: &dyn EventStore,
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

    let seq = store.write_event(&event).await?;

    Ok(AgentEvent { seq, ..event })
}

// ── Step 生命周期管理 ──────────────────────────────────────────────────

/// 记录 LLM 调用开始
///
/// 遵循 Write-Ahead 原则：先写 Event，再执行操作。
pub async fn record_llm_invoked(
    store: &dyn EventStore,
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

    let seq = store.write_event(&event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 记录 LLM 调用完成
pub async fn record_llm_completed(
    store: &dyn EventStore,
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

    let seq = store.write_event(&event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 记录 LLM 调用失败
pub async fn record_llm_failed(
    store: &dyn EventStore,
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

    let seq = store.write_event(&event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 记录 Tool 调用开始
///
/// ## idempotency_key 格式（设计文档 10.2 节）
/// `{session_id}:{redo_group}:{tool_name}:{call_signature}`
pub async fn record_tool_invoked(
    store: &dyn EventStore,
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

    let seq = store.write_event(&event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 记录 Tool 调用完成
pub async fn record_tool_completed(
    store: &dyn EventStore,
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

    let seq = store.write_event(&event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 记录 Tool 调用失败
pub async fn record_tool_failed(
    store: &dyn EventStore,
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

    let seq = store.write_event(&event).await?;

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
    store: &dyn EventStore,
    session_id: &str,
) -> Result<SummaryTrigger> {
    let latest_summary = store.get_latest_summary(session_id).await?;

    let summarized_up_to_seq = latest_summary
        .as_ref()
        .and_then(|s| s.payload.get("summarized_up_to_seq"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    // 统计自上次摘要后的 Turn 数（通过 trait 方法）
    let turn_count = store
        .count_turns_after_seq(session_id, summarized_up_to_seq)
        .await?;

    // 统计自上次摘要后的事件数（通过 trait 方法）
    let event_count = store
        .count_events_after_seq(session_id, summarized_up_to_seq)
        .await?;

    // Token 估算（从 llm_completed 累积 usage）
    let payloads = store
        .get_llm_payloads_after_seq(session_id, summarized_up_to_seq)
        .await?;

    let mut token_estimate: i64 = 0;
    for payload_str in &payloads {
        if let Ok(p) = serde_json::from_str::<serde_json::Value>(payload_str) {
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
///
/// 注意：自动归档由调用方负责（调用 archive_events_before_seq）。
pub async fn write_summary_marker(
    store: &dyn EventStore,
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

    let seq = store.write_event(&event).await?;

    Ok(AgentEvent { seq, ..event })
}

/// 使用便捷 payload 类型的通用事件记录函数（fixlet 事件回传入库用）
pub async fn record_event(
    store: &dyn EventStore,
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

    store.write_event(&event).await
}

// ── 查询封装（供 server 层调用，不直接暴露 storage） ──────────────────────

/// 读取某个 Turn 的完整执行过程
pub async fn get_turn_events(
    store: &dyn EventStore,
    session_id: &str,
    turn_id: i64,
) -> Result<Vec<AgentEvent>> {
    store.get_turn_events(session_id, turn_id).await
}

/// Turn 内的 Step 列表（含耗时）
pub async fn get_turn_steps(
    store: &dyn EventStore,
    session_id: &str,
    turn_id: i64,
) -> Result<Vec<crate::models::StepExecution>> {
    store.get_turn_steps(session_id, turn_id).await
}

/// 批量写入 Event（在同一事务中）
pub async fn write_events_batch(
    store: &dyn EventStore,
    events: &[AgentEvent],
) -> Result<Vec<i64>> {
    store.write_events_batch(events).await
}

/// LLM Token 消耗统计
pub async fn get_token_usage_stats(
    store: &dyn EventStore,
    session_id: &str,
) -> Result<Vec<crate::models::TokenUsageStats>> {
    store.get_token_usage_stats(session_id).await
}

/// Session 详情聚合查询（供 server 层使用）
///
/// 合并 session 基本信息 + is_ended + turn_count + event_count，
/// 避免上层直接调用多个 storage 函数。
pub async fn get_session_info(
    store: &dyn EventStore,
    session_id: &str,
) -> Result<Option<crate::models::Session>> {
    store.get_session(session_id).await
}

/// 检查 Session 是否已结束
pub async fn is_session_ended(store: &dyn EventStore, session_id: &str) -> Result<bool> {
    store.is_session_ended(session_id).await
}

/// 获取 Session 当前最大 turn_id
pub async fn get_max_turn_id(store: &dyn EventStore, session_id: &str) -> Result<i64> {
    store.get_max_turn_id(session_id).await
}

/// 获取 Session 当前最大 seq
pub async fn get_max_seq(store: &dyn EventStore, session_id: &str) -> Result<i64> {
    store.get_max_seq(session_id).await
}

// ── 测试 ────────────────────────────────────────────────────────────────
