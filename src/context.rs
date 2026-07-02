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

use sqlx::{Row, SqlitePool};

use crate::error::Result;
use crate::models::{AgentEvent, EventType, Message};
use crate::storage;

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
                // 优先取 content，其次取 tool_calls 的 JSON 表示
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
                // Tool 输出作为 tool role 消息
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
pub async fn build_llm_context(pool: &SqlitePool, session_id: &str) -> Result<LlmContext> {
    // Step 1: 找最新 summary_marker
    let latest_summary = storage::get_latest_summary(pool, session_id).await?;

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

    // Step 2: 读取增量事件
    let incremental_events =
        storage::get_events_after_seq(pool, session_id, summarized_up_to_seq).await?;

    // Step 3: 转换为 messages
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
///
/// 用于审计/Debug/精确状态恢复场景。
pub async fn full_replay(pool: &SqlitePool, session_id: &str) -> Result<FullReplay> {
    // 获取所有事件（从 seq=1 开始）
    let events = storage::get_events_after_seq(pool, session_id, 0).await?;
    let messages = events_to_messages(&events);

    Ok(FullReplay { events, messages })
}

/// 构建特定 Turn 的上下文（仅该 Turn 的 messages）
pub async fn build_turn_context(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
) -> Result<Vec<Message>> {
    let events = storage::get_turn_events(pool, session_id, turn_id).await?;
    Ok(events_to_messages(&events))
}

/// 构建最近 N 个 Turn 的上下文（不含摘要）
pub async fn build_recent_turns_context(
    pool: &SqlitePool,
    session_id: &str,
    recent_turns: i64,
) -> Result<Vec<Message>> {
    // 找最近的 turn_started
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT turn_id
        FROM agent_events
        WHERE session_id = ?1 AND event_type = 'turn_started'
        ORDER BY turn_id DESC
        LIMIT ?2
        "#,
    )
    .bind(session_id)
    .bind(recent_turns)
    .fetch_all(pool)
    .await?;

    let mut all_messages = Vec::new();
    for row in rows.iter().rev() {
        let turn_id: i64 = row.get("turn_id");
        let events = storage::get_turn_events(pool, session_id, turn_id).await?;
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
        let sid = format!("ctx_{}", uuid::Uuid::now_v7().to_string().replace('-', ""));
        service::create_session(pool, &sid, "default", "", "test_agent", None)
            .await
            .unwrap();
        sid
    }

    #[tokio::test]
    async fn test_events_to_messages_conversion() {
        let event = AgentEvent::new(
            "sess_1".into(),
            Some(1),
            None,
            EventType::TurnStarted,
            serde_json::json!({"user_input": "Hello, world!", "redo_group": "rg_1", "redo_count": 0}),
        );

        let messages = events_to_messages(&[event]);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, "user");
        assert_eq!(messages[0].content, "Hello, world!");
    }

    #[tokio::test]
    async fn test_llm_completed_to_message() {
        let event = AgentEvent::new(
            "sess_1".into(),
            Some(1),
            Some("step_1".into()),
            EventType::LlmCompleted,
            serde_json::json!({
                "model": "gpt-4",
                "content": "Hi there!",
                "local_seq": 2
            }),
        );

        let messages = events_to_messages(&[event]);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, "assistant");
        assert_eq!(messages[0].content, "Hi there!");
    }

    #[tokio::test]
    async fn test_build_llm_context_empty_session() {
        let pool = setup().await;
        let sid = setup_session(&pool).await;

        let context = build_llm_context(&pool, &sid).await.unwrap();
        assert!(context.summary.is_empty());
        assert_eq!(context.summarized_up_to_seq, 0);
        assert_eq!(context.incremental_start_seq, 1);
    }

    #[tokio::test]
    async fn test_build_llm_context_with_turns() {
        let pool = setup().await;
        let sid = setup_session(&pool).await;

        // 创建并完成一个 Turn
        let (turn_id, _, _) = service::start_turn(&pool, &sid, "Hello!", None)
            .await
            .unwrap();
        let step_id = "step_ctx";
        service::record_llm_invoked(
            &pool, &sid, Some(turn_id), step_id,
            "gpt-4",
            &[Message { role: "user".into(), content: "Hello!".into() }],
            None, None, 1,
        ).await.unwrap();
        service::record_llm_completed(
            &pool, &sid, Some(turn_id), step_id,
            "gpt-4",
            Some("Hi!"),
            None,
            Some(&crate::models::Usage { prompt_tokens: 5, completion_tokens: 2, total_tokens: 7 }),
            Some("stop"),
            2,
        ).await.unwrap();
        service::complete_turn(&pool, &sid, turn_id, "Hi!")
            .await
            .unwrap();

        let context = build_llm_context(&pool, &sid).await.unwrap();
        assert_eq!(context.messages.len(), 2); // user + assistant
        assert_eq!(context.messages[0].role, "user");
        assert_eq!(context.messages[0].content, "Hello!");
        assert_eq!(context.messages[1].role, "assistant");
        assert_eq!(context.messages[1].content, "Hi!");
    }

    #[tokio::test]
    async fn test_build_llm_context_with_summary() {
        let pool = setup().await;
        let sid = setup_session(&pool).await;

        // 先完成一个 Turn
        let (turn_id, _, _) = service::start_turn(&pool, &sid, "turn 1", None)
            .await
            .unwrap();
        service::complete_turn(&pool, &sid, turn_id, "result 1")
            .await
            .unwrap();

        // 写摘要
        service::write_summary_marker(&pool, &sid, 1, 3, "Summary of turn 1", 3)
            .await
            .unwrap();

        // 再创建第二个 Turn
        let (turn_id2, _, _) = service::start_turn(&pool, &sid, "turn 2", None)
            .await
            .unwrap();
        service::complete_turn(&pool, &sid, turn_id2, "result 2")
            .await
            .unwrap();

        let context = build_llm_context(&pool, &sid).await.unwrap();
        assert_eq!(context.summary, "Summary of turn 1");
        assert_eq!(context.summarized_up_to_seq, 3);
        // 只有 turn 2 的增量 messages（turn_started → user message）
        assert_eq!(context.messages.len(), 1);

        let system_prompt = build_system_prompt(&context);
        assert!(system_prompt.contains("Summary of turn 1"));
    }

    #[tokio::test]
    async fn test_full_replay() {
        let pool = setup().await;
        let sid = setup_session(&pool).await;

        let (turn_id, _, _) = service::start_turn(&pool, &sid, "hi", None)
            .await
            .unwrap();
        service::complete_turn(&pool, &sid, turn_id, "hello")
            .await
            .unwrap();

        let replay = full_replay(&pool, &sid).await.unwrap();
        assert!(!replay.events.is_empty());
        assert!(!replay.messages.is_empty());
    }
}
