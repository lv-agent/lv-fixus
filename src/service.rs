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
    AgentEvent, EventType, FailureReason, SummaryMarkerPayload, TurnCompletedPayload,
    TurnFailedPayload, TurnStartedPayload,
};
use crate::storage::EventStore;

// ── Session 生命周期管理 ────────────────────────────────────────────────

/// 创建新 Task(spec §8.4)
///
/// fixus 分配 task_id、存 head、发 task_created 事件。state 初始 = Created。
/// tenant_id/user_id 从 provenance 派生。task_id 由 fixus 分配(UUIDv7,保证唯一)。
pub async fn create_task(
    store: &dyn EventStore,
    task_type: &str,
    provenance: &crate::models::Provenance,
    body: Option<&serde_json::Value>,
    priority: i32,
) -> Result<(String, AgentEvent)> {
    let tenant_id = provenance
        .source_tenant_id
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let user_id = provenance.source_user_id.clone().unwrap_or_default();
    store
        .create_task(task_type, &tenant_id, &user_id, provenance, body, priority)
        .await
}

/// 校验并执行状态迁移:读当前态 → 校验合法 → 写迁移事件。
///
/// 终态不可迁出;非法迁移返回 `InvalidTaskStateTransition`。
async fn transition_task(
    store: &dyn EventStore,
    task_id: &str,
    target: crate::models::TaskState,
    event_type: EventType,
    payload: serde_json::Value,
) -> Result<AgentEvent> {
    let current = store
        .get_task_state(task_id)
        .await?
        .ok_or_else(|| AppError::TaskNotFound(task_id.to_string()))?;

    if !crate::models::TaskState::can_transition(current, target) {
        return Err(AppError::InvalidTaskStateTransition {
            task_id: task_id.to_string(),
            from: current.as_str().into(),
            to: target.as_str().into(),
        });
    }

    let event = AgentEvent::new(task_id.to_string(), None, None, event_type, payload);
    let seq = store.write_event(&event).await?;
    Ok(AgentEvent { seq, ..event })
}

/// nuntius:readiness 通过,created → ready(spec §4.1 语义 gate)
pub async fn mark_task_ready(store: &dyn EventStore, task_id: &str) -> Result<AgentEvent> {
    transition_task(
        store,
        task_id,
        crate::models::TaskState::Ready,
        EventType::TaskReady,
        serde_json::json!({}),
    )
    .await
}

/// executor claim:ready → claimed(spec §4.3)
pub async fn claim_task(
    store: &dyn EventStore,
    task_id: &str,
    claimant: &str,
) -> Result<AgentEvent> {
    transition_task(
        store,
        task_id,
        crate::models::TaskState::Claimed,
        EventType::TaskClaimed,
        serde_json::json!({ "claimant": claimant }),
    )
    .await
}

/// executing → blocked(executor 请求人工)
pub async fn block_task(
    store: &dyn EventStore,
    task_id: &str,
    reason: &str,
) -> Result<AgentEvent> {
    transition_task(
        store,
        task_id,
        crate::models::TaskState::Blocked,
        EventType::TaskBlocked,
        serde_json::json!({ "reason": reason }),
    )
    .await
}

/// executing → succeeded
pub async fn succeed_task(
    store: &dyn EventStore,
    task_id: &str,
    reason: &str,
) -> Result<AgentEvent> {
    transition_task(
        store,
        task_id,
        crate::models::TaskState::Succeeded,
        EventType::TaskSucceeded,
        serde_json::json!({ "reason": reason }),
    )
    .await
}

/// executing → failed
///
/// `failure_reason`(CR-3):结构化失败分类,写入 task_failed 事件 payload,便于审计/按因统计。
pub async fn fail_task(
    store: &dyn EventStore,
    task_id: &str,
    reason: &str,
    failure_reason: Option<FailureReason>,
) -> Result<AgentEvent> {
    let mut payload = serde_json::json!({ "reason": reason });
    if let Some(fr) = failure_reason {
        payload["failure_reason"] = serde_json::to_value(fr)?;
    }
    transition_task(
        store,
        task_id,
        crate::models::TaskState::Failed,
        EventType::TaskFailed,
        payload,
    )
    .await
}

/// 任意活态 → canceled(用户放弃/取消,spec §4)
pub async fn cancel_task(
    store: &dyn EventStore,
    task_id: &str,
    reason: &str,
) -> Result<AgentEvent> {
    let current = store
        .get_task_state(task_id)
        .await?
        .ok_or_else(|| AppError::TaskNotFound(task_id.to_string()))?;
    if !crate::models::TaskState::can_transition(current, crate::models::TaskState::Canceled) {
        return Err(AppError::InvalidTaskStateTransition {
            task_id: task_id.to_string(),
            from: current.as_str().into(),
            to: "canceled".into(),
        });
    }
    let event = AgentEvent::new(
        task_id.to_string(),
        None,
        None,
        EventType::TaskCanceled,
        serde_json::json!({ "reason": reason }),
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
    task_id: &str,
    user_input: &str,
    redo_group: Option<&str>,
) -> Result<(i64, String, AgentEvent)> {
    // 检查 session 存在且未结束。
    // task_exists 直读 committed cursor(cr-027,无 Indexer/SQLite);紧随
    // create_task 调用 start_turn 时 committed 游标推进有亚毫秒级延迟,
    // 可能短暂查不到 → 容忍一个短窗口重试。
    let mut exists = false;
    for _ in 0..30 {
        match store.task_exists(task_id).await {
            Ok(true) => {
                exists = true;
                break;
            }
            Ok(false) | Err(_) => {} // committed 游标尚未推进,继续
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    if !exists {
        return Err(AppError::TaskNotFound(task_id.to_string()));
    }
    if store.is_task_ended(task_id).await? {
        return Err(AppError::TaskAlreadyEnded(task_id.to_string()));
    }

    // 生成 turn_id
    let max_turn = store.get_max_turn_id(task_id).await?;
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
        task_id.to_string(),
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
    task_id: &str,
    turn_id: i64,
    final_output: &str,
) -> Result<AgentEvent> {
    let payload = serde_json::to_value(TurnCompletedPayload {
        final_output: final_output.to_string(),
    })?;

    let event = AgentEvent::new(
        task_id.to_string(),
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
    task_id: &str,
    turn_id: i64,
    error_type: &str,
    error_message: &str,
    stack_trace: Option<&str>,
    failure_reason: Option<FailureReason>,
) -> Result<AgentEvent> {
    let payload = serde_json::to_value(TurnFailedPayload {
        error_type: error_type.to_string(),
        error_message: error_message.to_string(),
        failure_reason,
        stack_trace: stack_trace.map(|s| s.to_string()),
    })?;

    let event = AgentEvent::new(
        task_id.to_string(),
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
    task_id: &str,
    turn_id: i64,
    reason: &str,
) -> Result<AgentEvent> {
    let event = AgentEvent::new(
        task_id.to_string(),
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
    task_id: &str,
    turn_id: i64,
    reason: &str,
) -> Result<AgentEvent> {
    let event = AgentEvent::new(
        task_id.to_string(),
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
    task_id: &str,
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
        task_id.to_string(),
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
    task_id: &str,
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
        task_id.to_string(),
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
    task_id: &str,
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
        task_id.to_string(),
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
    task_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    error_type: &str,
    error_message: &str,
    failure_reason: Option<FailureReason>,
    attempt: i32,
    local_seq: i64,
) -> Result<AgentEvent> {
    let mut payload = serde_json::json!({
        "error_type": error_type,
        "error_message": error_message,
        "attempt": attempt,
        "local_seq": local_seq,
    });
    if let Some(fr) = failure_reason {
        payload["failure_reason"] = serde_json::to_value(fr)?;
    }

    let event = AgentEvent::new(
        task_id.to_string(),
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
/// `{task_id}:{redo_group}:{tool_name}:{call_signature}`
pub async fn record_tool_invoked(
    store: &dyn EventStore,
    task_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    tool_name: &str,
    tool_call_id: &str,
    idempotency_key: &str,
    input: &serde_json::Value,
    parent_step_id: Option<&str>,
    local_seq: i64,
    work_dir: Option<&str>,
    timeout_ms: Option<u64>,
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
    if let Some(wd) = work_dir {
        payload_map["work_dir"] = serde_json::json!(wd);
    }
    if let Some(tmo) = timeout_ms {
        payload_map["timeout_ms"] = serde_json::json!(tmo);
    }

    let event = AgentEvent::new(
        task_id.to_string(),
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
    task_id: &str,
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
        task_id.to_string(),
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
    task_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    tool_call_id: &str,
    error_type: &str,
    error_message: &str,
    failure_reason: Option<FailureReason>,
    attempt: i32,
    local_seq: i64,
) -> Result<AgentEvent> {
    let mut payload = serde_json::json!({
        "tool_call_id": tool_call_id,
        "error_type": error_type,
        "error_message": error_message,
        "attempt": attempt,
        "local_seq": local_seq,
    });
    if let Some(fr) = failure_reason {
        payload["failure_reason"] = serde_json::to_value(fr)?;
    }

    let event = AgentEvent::new(
        task_id.to_string(),
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
    task_id: &str,
) -> Result<SummaryTrigger> {
    let latest_summary = store.get_latest_summary(task_id).await?;

    let summarized_up_to_seq = latest_summary
        .as_ref()
        .and_then(|s| s.payload.get("summarized_up_to_seq"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    // 统计自上次摘要后的 Turn 数（通过 trait 方法）
    let turn_count = store
        .count_turns_after_seq(task_id, summarized_up_to_seq)
        .await?;

    // 统计自上次摘要后的事件数（通过 trait 方法）
    let event_count = store
        .count_events_after_seq(task_id, summarized_up_to_seq)
        .await?;

    // Token 估算（从 llm_completed 累积 usage）
    let payloads = store
        .get_llm_payloads_after_seq(task_id, summarized_up_to_seq)
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
    task_id: &str,
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
        task_id.to_string(),
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
    task_id: &str,
    turn_id: Option<i64>,
    step_id: Option<&str>,
    event_type: EventType,
    payload: serde_json::Value,
) -> Result<i64> {
    let event = AgentEvent::new(
        task_id.to_string(),
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
    task_id: &str,
    turn_id: i64,
) -> Result<Vec<AgentEvent>> {
    store.get_turn_events(task_id, turn_id).await
}

/// Turn 内的 Step 列表（含耗时）
pub async fn get_turn_steps(
    store: &dyn EventStore,
    task_id: &str,
    turn_id: i64,
) -> Result<Vec<crate::models::StepExecution>> {
    store.get_turn_steps(task_id, turn_id).await
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
    task_id: &str,
) -> Result<Vec<crate::models::TokenUsageStats>> {
    store.get_token_usage_stats(task_id).await
}

/// Session 详情聚合查询（供 server 层使用）
///
/// 合并 session 基本信息 + is_ended + turn_count + event_count，
/// 避免上层直接调用多个 storage 函数。
pub async fn get_task_info(
    store: &dyn EventStore,
    task_id: &str,
) -> Result<Option<crate::models::Task>> {
    store.get_task(task_id).await
}

/// 检查 Session 是否已结束
pub async fn is_task_ended(store: &dyn EventStore, task_id: &str) -> Result<bool> {
    store.is_task_ended(task_id).await
}

/// 获取 Session 当前最大 turn_id
pub async fn get_max_turn_id(store: &dyn EventStore, task_id: &str) -> Result<i64> {
    store.get_max_turn_id(task_id).await
}

/// 获取 Session 当前最大 seq
pub async fn get_max_seq(store: &dyn EventStore, task_id: &str) -> Result<i64> {
    store.get_max_seq(task_id).await
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{EventType, Provenance, TaskState};

    // logdbd harness(CLAUDE.md: 测试内联 per 模块;与 storage 测试同模式)
    use logdbd::catalog::Catalog;
    use logdbd::consumer::ConsumerTracker;
    use logdbd::pb::log_db_service_server::LogDbServiceServer;
    use logdbd::service::LogDbServiceImpl;
    use logdbd::storage::Storage;
    use logdbd::subscribe::SubscribeHub;
    use logdb::Config as LogdbConfig;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    fn test_provenance() -> Provenance {
        Provenance {
            source_channel: "api".into(),
            source_session_id: None,
            source_user_id: Some("u".into()),
            source_tenant_id: Some("t".into()),
            source_message_id: None,
            created_at: chrono::Utc::now(),
            created_by: "test".into(),
        }
    }

    async fn setup() -> (crate::storage::LogdbdEventStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = LogdbConfig::default();
        cfg.data_dir = dir.path().to_path_buf();
        cfg.durability_mode = logdb::DurabilityMode::Sync;
        cfg.ring_size = 256;
        cfg.shards = 1;
        cfg.flush_timeout = Duration::from_secs(5);
        let db = logdb::LogDb::open(cfg).unwrap();
        let storage = Arc::new(Storage::new(db, 1));
        let catalog = Arc::new(Catalog::open(dir.path()).unwrap());
        let subscribe_hub = Arc::new(SubscribeHub::new());
        let consumer_tracker = Arc::new(ConsumerTracker::new(None));
        let svc = LogDbServiceImpl::new(
            Arc::clone(&storage),
            Arc::clone(&catalog),
            Arc::clone(&consumer_tracker),
            Arc::clone(&subscribe_hub),
            "test-node".into(),
            "primary".into(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            Server::builder()
                .add_service(LogDbServiceServer::new(svc))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let store =
            crate::storage::LogdbdEventStore::connect(&addr, "fixus-svc-test").await.unwrap();
        (store, dir)
    }

    async fn wait_seq(store: &crate::storage::LogdbdEventStore, sid: &str, expected: i64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(v) = store.get_max_seq(sid).await {
                if v >= expected {
                    return;
                }
            }
            if std::time::Instant::now() >= deadline {
                panic!("seq {} not reached for {}", expected, sid);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// 模拟 orchestrator 派发 execute_turn 后写的 turn_started(使 Task 进入 Executing)
    async fn write_turn_started(store: &dyn EventStore, tid: &str) {
        let ev = AgentEvent::new(
            tid.into(),
            Some(1),
            None,
            EventType::TurnStarted,
            serde_json::json!({"user_input":"x","redo_group":"rg1","redo_count":0}),
        );
        store.write_event(&ev).await.unwrap();
    }

    #[tokio::test]
    async fn create_task_assigns_id_and_created_state() {
        let (store, _d) = setup().await;
        let prov = test_provenance();
        let (tid, ev) = create_task(&store, "db.repair", &prov, None, 0)
            .await
            .unwrap();
        assert_eq!(ev.event_type, EventType::TaskCreated);
        assert!(tid.starts_with("task_"));
        wait_seq(&store, &tid, 1).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Created)
        );
    }

    #[tokio::test]
    async fn lifecycle_transitions_enforce_invariants() {
        let (store, _d) = setup().await;
        let prov = test_provenance();
        let (tid, _) = create_task(&store, "db.repair", &prov, None, 0).await.unwrap();
        wait_seq(&store, &tid, 1).await;

        // 非法:created → claimed(跳过 ready)
        let err = claim_task(&store, &tid, "fixlet-1").await;
        assert!(matches!(
            err,
            Err(crate::error::AppError::InvalidTaskStateTransition { .. })
        ));

        // 合法:created → ready
        mark_task_ready(&store, &tid).await.unwrap();
        wait_seq(&store, &tid, 2).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Ready)
        );

        // ready → claimed
        claim_task(&store, &tid, "fixlet-1").await.unwrap();
        wait_seq(&store, &tid, 3).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Claimed)
        );

        // claimed + turn_started → executing(orchestrator 派发)
        write_turn_started(&store, &tid).await;
        wait_seq(&store, &tid, 4).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Executing)
        );

        // executing → succeeded
        succeed_task(&store, &tid, "done").await.unwrap();
        wait_seq(&store, &tid, 5).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Succeeded)
        );

        // 终态不可再迁
        let err = mark_task_ready(&store, &tid).await;
        assert!(matches!(
            err,
            Err(crate::error::AppError::InvalidTaskStateTransition { .. })
        ));
    }

    #[tokio::test]
    async fn cancel_from_blocked_returns_to_ready() {
        let (store, _d) = setup().await;
        let prov = test_provenance();
        let (tid, _) = create_task(&store, "db.repair", &prov, None, 0).await.unwrap();
        wait_seq(&store, &tid, 1).await;
        mark_task_ready(&store, &tid).await.unwrap();
        wait_seq(&store, &tid, 2).await;
        claim_task(&store, &tid, "fixlet-1").await.unwrap();
        wait_seq(&store, &tid, 3).await;
        write_turn_started(&store, &tid).await;
        wait_seq(&store, &tid, 4).await;
        block_task(&store, &tid, "need human input").await.unwrap();
        wait_seq(&store, &tid, 5).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Blocked)
        );

        // blocked → ready(nuntius 语义 gate)
        mark_task_ready(&store, &tid).await.unwrap();
        wait_seq(&store, &tid, 6).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Ready)
        );
    }

    #[tokio::test]
    async fn cancel_task_from_any_active_state() {
        let (store, _d) = setup().await;
        let prov = test_provenance();
        // created → canceled
        let (tid, _) = create_task(&store, "db.repair", &prov, None, 0).await.unwrap();
        wait_seq(&store, &tid, 1).await;
        cancel_task(&store, &tid, "abandoned").await.unwrap();
        wait_seq(&store, &tid, 2).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Canceled)
        );
    }
}
