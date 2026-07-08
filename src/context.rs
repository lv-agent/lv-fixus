//! Context Builder — Events → Messages 转换
//!
//! 实现设计文档 11.1 节"构建 LLM 对话上下文"。
//!
//! 目标：从不可变 Events 重建 LLM 可用的对话上下文。
//!
//! 上下文构建策略：
//! - 审计/Debug/回放 → 全量 Events
//! - LLM 对话上下文 → summary + 最近 N 个 Turn 的 Events
//! - 精确状态恢复 → full replay

use crate::error::Result;
use crate::models::{AgentEvent, EventType, Message};
use crate::storage::EventStore;

// ── 上下文构建结果 ──────────────────────────────────────────────────────

/// LLM 上下文构建结果
#[derive(Debug, Clone)]
pub struct LlmContext {
    /// 摘要文本（如果有）
    pub summary: String,
    /// 摘要覆盖到的物理边界 seq
    pub summarized_up_to_seq: i64,
    /// 摘要覆盖到的业务边界 turn_id
    pub summarized_up_to_turn_id: Option<i64>,
    /// 转换为 messages 格式的增量事件
    pub messages: Vec<Message>,
    /// 增量事件的起始 seq（不含摘要）
    pub incremental_start_seq: i64,
}

/// 完整回放结果
#[derive(Debug, Clone)]
pub struct FullReplay {
    pub events: Vec<AgentEvent>,
    pub messages: Vec<Message>,
}

// ── Events → Messages 转换 ──────────────────────────────────────────────

/// 将 AgentEvent 列表转换为 LLM messages
///
/// 转换规则：
/// - turn_started.payload.user_input → role: "user"
/// - llm_completed.payload.content → role: "assistant"
/// - tool_invoked / tool_completed → 可选，作为上下文元数据
pub fn events_to_messages(events: &[AgentEvent]) -> Vec<Message> {
    let mut messages = Vec::new();

    for event in events {
        match event.event_type {
            EventType::TurnStarted => {
                if let Some(user_input) = event.payload.get("user_input").and_then(|v| v.as_str())
                {
                    messages.push(Message {
                        role: "user".to_string(),
                        content: user_input.to_string(),
                    });
                }
            }
            EventType::LlmCompleted => {
                if let Some(content) = event.payload.get("content").and_then(|v| v.as_str()) {
                    if !content.is_empty() {
                        messages.push(Message {
                            role: "assistant".to_string(),
                            content: content.to_string(),
                        });
                    }
                }
                if let Some(tool_calls) = event.payload.get("tool_calls") {
                    if tool_calls.is_array() && !tool_calls.as_array().unwrap().is_empty() {
                        messages.push(Message {
                            role: "assistant".to_string(),
                            content: format!(
                                "[tool_calls] {}",
                                serde_json::to_string(tool_calls).unwrap_or_default()
                            ),
                        });
                    }
                }
            }
            EventType::ToolCompleted => {
                if let Some(output) = event.payload.get("output") {
                    let tool_name = event
                        .payload
                        .get("tool_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("tool");

                    messages.push(Message {
                        role: "tool".to_string(),
                        content: format!(
                            "[{}] {}",
                            tool_name,
                            serde_json::to_string(output).unwrap_or_default()
                        ),
                    });
                }
            }
            _ => {}
        }
    }

    messages
}

/// 将单个 Turn 转换为 messages
pub fn turn_to_messages(events: &[AgentEvent]) -> Vec<Message> {
    events_to_messages(events)
}

// ── LLM 上下文构建 ──────────────────────────────────────────────────────

/// 构建 LLM 对话上下文（高效，summary + 增量）
///
/// 对应设计文档 11.1 节的三步流程：
/// 1. 找最新的 summary_marker
/// 2. 读取增量事件（seq > summarized_up_to_seq）
/// 3. 应用层构建 messages
pub async fn build_llm_context(
    store: &dyn EventStore,
    task_id: &str,
) -> Result<LlmContext> {
    let latest_summary = store.get_latest_summary(task_id).await?;

    let (summary_text, summarized_up_to_seq, summarized_up_to_turn_id) =
        if let Some(ref summary_event) = latest_summary {
            let text = summary_event
                .payload
                .get("summary")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let seq = summary_event
                .payload
                .get("summarized_up_to_seq")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let turn_id = summary_event
                .payload
                .get("summarized_up_to_turn_id")
                .and_then(|v| v.as_i64());
            (text, seq, turn_id)
        } else {
            (String::new(), 0, None)
        };

    let incremental_events = store
        .get_events_after_seq(task_id, summarized_up_to_seq)
        .await?;

    let messages = events_to_messages(&incremental_events);

    Ok(LlmContext {
        summary: summary_text,
        summarized_up_to_seq,
        summarized_up_to_turn_id,
        messages,
        incremental_start_seq: summarized_up_to_seq + 1,
    })
}

/// 全量回放 — 将 Session 的所有事件转为 messages
pub async fn full_replay(
    store: &dyn EventStore,
    task_id: &str,
) -> Result<FullReplay> {
    let events = store.get_events_after_seq(task_id, 0).await?;
    let messages = events_to_messages(&events);

    Ok(FullReplay { events, messages })
}

/// 构建特定 Turn 的上下文（仅该 Turn 的 messages）
pub async fn build_turn_context(
    store: &dyn EventStore,
    task_id: &str,
    turn_id: i64,
) -> Result<Vec<Message>> {
    let events = store.get_turn_events(task_id, turn_id).await?;
    Ok(events_to_messages(&events))
}

/// 构建最近 N 个 Turn 的上下文（不含摘要）
pub async fn build_recent_turns_context(
    store: &dyn EventStore,
    task_id: &str,
    recent_turns: i64,
) -> Result<Vec<Message>> {
    let turn_ids = store.get_recent_turn_ids(task_id, recent_turns).await?;

    let mut all_messages = Vec::new();
    for turn_id in turn_ids.iter().rev() {
        let events = store.get_turn_events(task_id, *turn_id).await?;
        all_messages.extend(events_to_messages(&events));
    }

    Ok(all_messages)
}

/// 构建系统提示（从 context 构建结果生成）
pub fn build_system_prompt(context: &LlmContext) -> String {
    if context.summary.is_empty() {
        String::new()
    } else {
        format!(
            "Previous conversation summary (up to turn {}):\n{}",
            context
                .summarized_up_to_turn_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "?".to_string()),
            context.summary
        )
    }
}

// ── 测试 ────────────────────────────────────────────────────────────────
