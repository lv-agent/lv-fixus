//! 数据模型 — AgentEvent, EventType, Task 等核心数据结构
//!
//! 类型命名与设计文档 8.1 节 Rust 侧定义保持一致。
//!
//! 设计原则：状态是 Events 的派生产物。不直接持久化 Agent 的当前状态，
//! 而是持久化所有产生该状态的 Events，状态通过重放 Events 重建。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── EventType 枚举 ────────────────────────────────────────────────────────

/// Agent 事件类型枚举
///
/// 命名规则：全部 snake_case，全部小写，动词使用过去式。
/// - invoked   → Agent 主动发起的调用
/// - completed → 调用正常完成
/// - failed    → 调用失败
/// - started   → 有生命周期的容器开始
/// - ended     → 容器正常结束
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    // Session 级别事件 (turn_id = NULL, step_id = NULL)
    SessionStarted,
    SessionEnded,
    SummaryMarker,

    // Turn 级别事件 (turn_id NOT NULL, step_id = NULL)
    TurnPending,   // 排队等待
    TurnStarted,
    TurnCompleted,
    TurnFailed,    // Agent 崩溃等，可 redo
    TurnCanceled,  // 用户取消，不可 redo
    TurnBlocked,   // 非幂等 Tool 悬空，需人工

    // Step 级别事件 — LLM (step_id NOT NULL)
    LlmInvoked,
    LlmCompleted,
    LlmFailed,

    // Step 级别事件 — Tool (step_id NOT NULL)
    ToolInvoked,
    ToolCompleted,
    ToolFailed,
}

impl EventType {
    /// 从 snake_case 字符串解析事件类型
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "session_started" => Some(Self::SessionStarted),
            "session_ended" => Some(Self::SessionEnded),
            "summary_marker" => Some(Self::SummaryMarker),
            "turn_pending" => Some(Self::TurnPending),
            "turn_started" => Some(Self::TurnStarted),
            "turn_completed" => Some(Self::TurnCompleted),
            "turn_failed" => Some(Self::TurnFailed),
            "turn_canceled" => Some(Self::TurnCanceled),
            "turn_blocked" => Some(Self::TurnBlocked),
            "llm_invoked" => Some(Self::LlmInvoked),
            "llm_completed" => Some(Self::LlmCompleted),
            "llm_failed" => Some(Self::LlmFailed),
            "tool_invoked" => Some(Self::ToolInvoked),
            "tool_completed" => Some(Self::ToolCompleted),
            "tool_failed" => Some(Self::ToolFailed),
            _ => None,
        }
    }

    /// 转为 snake_case 字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SessionStarted => "session_started",
            Self::SessionEnded => "session_ended",
            Self::SummaryMarker => "summary_marker",
            Self::TurnPending => "turn_pending",
            Self::TurnStarted => "turn_started",
            Self::TurnCompleted => "turn_completed",
            Self::TurnFailed => "turn_failed",
            Self::TurnCanceled => "turn_canceled",
            Self::TurnBlocked => "turn_blocked",
            Self::LlmInvoked => "llm_invoked",
            Self::LlmCompleted => "llm_completed",
            Self::LlmFailed => "llm_failed",
            Self::ToolInvoked => "tool_invoked",
            Self::ToolCompleted => "tool_completed",
            Self::ToolFailed => "tool_failed",
        }
    }

    /// 是否为 Session 级别事件 (turn_id = NULL, step_id = NULL)
    pub fn is_session_level(&self) -> bool {
        matches!(
            self,
            Self::SessionStarted | Self::SessionEnded | Self::SummaryMarker
        )
    }

    /// 是否为 Turn 级别事件 (turn_id NOT NULL, step_id = NULL)
    pub fn is_turn_level(&self) -> bool {
        matches!(
            self,
            Self::TurnPending
                | Self::TurnStarted
                | Self::TurnCompleted
                | Self::TurnFailed
                | Self::TurnCanceled
                | Self::TurnBlocked
        )
    }

    /// 是否为 Step 级别事件 (step_id NOT NULL)
    pub fn is_step_level(&self) -> bool {
        matches!(
            self,
            Self::LlmInvoked
                | Self::LlmCompleted
                | Self::LlmFailed
                | Self::ToolInvoked
                | Self::ToolCompleted
                | Self::ToolFailed
        )
    }

    /// 是否为 Step 启动事件
    pub fn is_step_start(&self) -> bool {
        matches!(self, Self::LlmInvoked | Self::ToolInvoked)
    }

    /// 是否为 Step 终止事件
    pub fn is_step_terminal(&self) -> bool {
        matches!(
            self,
            Self::LlmCompleted | Self::LlmFailed | Self::ToolCompleted | Self::ToolFailed
        )
    }

    /// 是否为 Turn 终止事件（不需要 redo 的终态）
    pub fn is_turn_terminal(&self) -> bool {
        matches!(
            self,
            Self::TurnCompleted | Self::TurnFailed | Self::TurnCanceled | Self::TurnBlocked
        )
    }

    /// 是否可以在恢复时跳过（不 redo）
    pub fn is_turn_skip_on_recovery(&self) -> bool {
        matches!(self, Self::TurnPending | Self::TurnCanceled | Self::TurnBlocked)
    }

    /// 获取事件所属的作用域级别
    pub fn scope(&self) -> EventScope {
        if self.is_session_level() {
            EventScope::Session
        } else if self.is_turn_level() {
            EventScope::Turn
        } else {
            EventScope::Step
        }
    }

    /// 检查 start/terminal 类型匹配
    /// llm_invoked  → llm_completed 或 llm_failed
    /// tool_invoked → tool_completed 或 tool_failed
    pub fn matches_start_type(start: &Self, terminal: &Self) -> bool {
        matches!(
            (start, terminal),
            (Self::LlmInvoked, Self::LlmCompleted)
                | (Self::LlmInvoked, Self::LlmFailed)
                | (Self::ToolInvoked, Self::ToolCompleted)
                | (Self::ToolInvoked, Self::ToolFailed)
        )
    }

    /// 对应给定 start 类型的合法 terminal 事件类型列表
    pub fn valid_terminals_for(start: &Self) -> &[Self] {
        match start {
            Self::LlmInvoked => &[Self::LlmCompleted, Self::LlmFailed],
            Self::ToolInvoked => &[Self::ToolCompleted, Self::ToolFailed],
            _ => &[],
        }
    }

    /// 所有事件类型的字符串列表（用于 CHECK 约束）
    pub fn all_str_variants() -> &'static [&'static str] {
        &[
            "session_started",
            "session_ended",
            "summary_marker",
            "turn_pending",
            "turn_started",
            "turn_completed",
            "turn_failed",
            "turn_canceled",
            "turn_blocked",
            "llm_invoked",
            "llm_completed",
            "llm_failed",
            "tool_invoked",
            "tool_completed",
            "tool_failed",
        ]
    }
}

/// 事件作用域级别
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventScope {
    /// Session 级别 — turn_id = NULL, step_id = NULL
    Session,
    /// Turn 级别 — turn_id NOT NULL, step_id = NULL
    Turn,
    /// Step 级别 — step_id NOT NULL
    Step,
}

// ── AgentEvent ──────────────────────────────────────────────────────────

/// Agent 不可变事件 — 一切状态的唯一来源
///
/// 每个 Event 通过 `turn_id` 和 `step_id` 标注自己的归属。
/// `seq` 是 Task 内的全局单调递增序号。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvent {
    /// Task 命名空间
    pub task_id: String,
    /// Task 内全局单调递增序号（物理坐标）
    pub seq: i64,
    /// Turn 归属标签（Turn 级和 Step 级事件有值）
    pub turn_id: Option<i64>,
    /// Step 归属标签（Task 内全局唯一，Step 级事件有值）
    pub step_id: Option<String>,
    /// 事件语义类型
    pub event_type: EventType,
    /// payload 结构版本号
    pub schema_version: i32,
    /// 事件内容（JSON）
    pub payload: serde_json::Value,
    /// 数据库写入时间
    pub created_at: DateTime<Utc>,
}

impl AgentEvent {
    /// 创建新的事件（seq 和 created_at 由数据库填充）
    pub fn new(
        task_id: String,
        turn_id: Option<i64>,
        step_id: Option<String>,
        event_type: EventType,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            task_id,
            seq: 0, // 由数据库分配
            turn_id,
            step_id,
            event_type,
            schema_version: 1,
            payload,
            created_at: Utc::now(), // 会被数据库覆盖
        }
    }

    /// 校验事件的作用域约束
    ///
    /// 对应数据库 CONSTRAINT chk_event_scope：
    /// - Session 级别：turn_id = NULL, step_id = NULL
    /// - Turn 级别：turn_id NOT NULL, step_id = NULL
    /// - Step 级别：step_id NOT NULL
    pub fn validate_scope(&self) -> Result<(), String> {
        match self.event_type.scope() {
            EventScope::Session => {
                if self.turn_id.is_some() {
                    return Err(format!(
                        "Session-level event {} must have turn_id = NULL",
                        self.event_type.as_str()
                    ));
                }
                if self.step_id.is_some() {
                    return Err(format!(
                        "Session-level event {} must have step_id = NULL",
                        self.event_type.as_str()
                    ));
                }
            }
            EventScope::Turn => {
                if self.turn_id.is_none() {
                    return Err(format!(
                        "Turn-level event {} must have turn_id NOT NULL",
                        self.event_type.as_str()
                    ));
                }
                if self.step_id.is_some() {
                    return Err(format!(
                        "Turn-level event {} must have step_id = NULL",
                        self.event_type.as_str()
                    ));
                }
            }
            EventScope::Step => {
                if self.step_id.is_none() {
                    return Err(format!(
                        "Step-level event {} must have step_id NOT NULL",
                        self.event_type.as_str()
                    ));
                }
                // turn_id 可为 NULL（Session 级后台 Step）
            }
        }
        Ok(())
    }
}

// ── TaskState ──────────────────────────────────────────────────────────

/// Task 状态机(spec §4)
///
/// 8 态:`created → ready → claimed → executing → (blocked ⇄ ready) → succeeded | failed`
/// 任意活态 → `canceled`(终态)。
///
/// 状态是事件的投影(spec §4.4):本枚举只描述合法迁移,实际状态由
/// `storage::get_task_state` 从 Task 级事件流派生。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Created,
    Ready,
    Claimed,
    Executing,
    Blocked,
    Succeeded,
    Failed,
    Canceled,
}

impl TaskState {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "created" => Some(Self::Created),
            "ready" => Some(Self::Ready),
            "claimed" => Some(Self::Claimed),
            "executing" => Some(Self::Executing),
            "blocked" => Some(Self::Blocked),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Ready => "ready",
            Self::Claimed => "claimed",
            Self::Executing => "executing",
            Self::Blocked => "blocked",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    /// 是否终态(不可再迁出)
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }

    /// `from → to` 是否合法迁移(spec §4 状态机)
    pub fn can_transition(from: Self, to: Self) -> bool {
        use TaskState::*;
        if from.is_terminal() {
            return false;
        }
        matches!(
            (from, to),
            (Created, Ready)
                | (Ready, Claimed)
                | (Ready, Canceled)
                | (Claimed, Executing)
                | (Claimed, Canceled)
                | (Executing, Blocked)
                | (Executing, Succeeded)
                | (Executing, Failed)
                | (Executing, Canceled)
                | (Blocked, Ready)
                | (Blocked, Canceled)
                | (Created, Canceled)
        )
    }
}

// ── Task ─────────────────────────────────────────────────────────────────

/// Tenant — 多租户隔离单元
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// Task — 唯一有独立存储的实体
///
/// `agent_type`、初始配置等信息不来自任何 Event，是真正独立的业务信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub task_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub agent_type: String,
    pub created_at: DateTime<Utc>,
    pub metadata: Option<serde_json::Value>,
}

impl Task {
    pub fn new(
        task_id: String,
        tenant_id: String,
        user_id: String,
        agent_type: String,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        Self {
            task_id,
            tenant_id,
            user_id,
            agent_type,
            created_at: Utc::now(),
            metadata,
        }
    }
}

// ── 类型化 Payload ──────────────────────────────────────────────────────

/// session_started 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStartedPayload {
    pub agent_type: String,
    #[serde(default)]
    pub initial_config: serde_json::Value,
}

/// session_ended 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEndedPayload {
    pub reason: String,
}

/// summary_marker 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryMarkerPayload {
    /// 摘要覆盖到的业务语义边界 Turn
    pub summarized_up_to_turn_id: i64,
    /// 摘要覆盖到的物理边界 seq
    pub summarized_up_to_seq: i64,
    /// 摘要文本
    pub summary: String,
    /// 本次摘要覆盖的业务事件数量（非累计）
    pub covered_event_count: i64,
}

/// turn_started 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnStartedPayload {
    pub user_input: String,
    /// Turn 幂等锚点，首次创建时生成，重做时复用
    pub redo_group: String,
    /// 第几次重做，0 = 首次
    #[serde(default)]
    pub redo_count: i32,
}

/// turn_completed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnCompletedPayload {
    pub final_output: String,
}

/// turn_failed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnFailedPayload {
    pub error_type: String,
    pub error_message: String,
    #[serde(default)]
    pub stack_trace: Option<String>,
}

/// llm_invoked 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmInvokedPayload {
    pub step_type: String, // "llm_call"
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(default)]
    pub tools: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub temperature: Option<f64>,
    /// fixlet 侧的本地递增序号
    pub local_seq: i64,
}

/// llm_completed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmCompletedPayload {
    /// 冗余写入 model，便于审计
    pub model: String,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(default)]
    pub usage: Option<Usage>,
    #[serde(default)]
    pub finish_reason: Option<String>,
    pub local_seq: i64,
}

/// llm_failed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmFailedPayload {
    pub error_type: String,
    pub error_message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub attempt: i32,
    pub local_seq: i64,
}

/// tool_invoked 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInvokedPayload {
    pub step_type: String, // "tool_call"
    pub tool_name: String,
    pub tool_call_id: String,
    /// 幂等键：{session_id}:{redo_group}:{tool_name}:{call_signature_hash}
    pub idempotency_key: String,
    pub input: serde_json::Value,
    /// 父 Step ID（支持嵌套 Step）
    #[serde(default)]
    pub parent_step_id: Option<String>,
    pub local_seq: i64,
}

/// tool_completed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCompletedPayload {
    pub tool_call_id: String,
    pub output: serde_json::Value,
    #[serde(default)]
    pub is_error: bool,
    pub local_seq: i64,
}

/// tool_failed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolFailedPayload {
    pub tool_call_id: String,
    pub error_type: String,
    pub error_message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub attempt: i32,
    pub local_seq: i64,
}

// ── 辅助类型 ────────────────────────────────────────────────────────────

/// LLM 对话消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

/// LLM Tool Call
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub call_type: String,
    pub function: FunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

/// Token 使用量
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
}

// ── 查询结果类型 ─────────────────────────────────────────────────────────

/// 未完成 Turn（恢复查询结果）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncompleteTurn {
    pub turn_id: i64,
    pub redo_group: String,
    pub redo_count: i32,
    pub turn_started_at: DateTime<Utc>,
}

/// 未完成 Step（诊断查询结果）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncompleteStep {
    pub seq: i64,
    pub turn_id: i64,
    pub step_id: String,
    pub start_event_type: String,
    pub payload: serde_json::Value,
    pub started_at: DateTime<Utc>,
}

/// Step 执行信息（含耗时）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepExecution {
    pub step_id: String,
    pub step_type: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub end_event: Option<String>,
    pub duration_ms: Option<f64>,
}

/// Token 消耗统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsageStats {
    pub model: String,
    pub call_count: i64,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
}

// ── 校验工具 ────────────────────────────────────────────────────────────

/// 对 payload JSON 进行关键字段非空校验
///
/// 根据设计文档 9.5 节的要求，部分 event_type 的 payload 关键字段
/// 必须在应用层强校验。
pub fn validate_payload_required_fields(
    event_type: &EventType,
    payload: &serde_json::Value,
) -> Result<(), crate::error::AppError> {
    let required: &[&str] = match event_type {
        EventType::LlmInvoked => &["model", "messages", "local_seq"],
        EventType::LlmCompleted => &["model", "local_seq"],
        EventType::ToolInvoked => &["tool_name", "tool_call_id", "idempotency_key", "local_seq"],
        EventType::SummaryMarker => &[
            "summarized_up_to_seq",
            "summarized_up_to_turn_id",
            "summary",
        ],
        _ => return Ok(()),
    };

    for field in required {
        match payload.get(field) {
            None | Some(serde_json::Value::Null) => {
                return Err(crate::error::AppError::PayloadValidation {
                    event_type: event_type.as_str().to_string(),
                    field: field.to_string(),
                });
            }
            _ => {}
        }
    }

    // llm_completed 额外检查 usage 子字段
    if *event_type == EventType::LlmCompleted {
        if let Some(usage) = payload.get("usage") {
            for sub_field in &["prompt_tokens", "completion_tokens"] {
                if usage.get(sub_field).is_none()
                    || usage.get(sub_field) == Some(&serde_json::Value::Null)
                {
                    return Err(crate::error::AppError::PayloadValidation {
                        event_type: event_type.as_str().to_string(),
                        field: format!("usage.{}", sub_field),
                    });
                }
            }
        }
    }

    Ok(())
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_serde_roundtrip() {
        let cases = vec![
            (EventType::SessionStarted, "session_started"),
            (EventType::SessionEnded, "session_ended"),
            (EventType::SummaryMarker, "summary_marker"),
            (EventType::TurnStarted, "turn_started"),
            (EventType::TurnCompleted, "turn_completed"),
            (EventType::TurnFailed, "turn_failed"),
            (EventType::LlmInvoked, "llm_invoked"),
            (EventType::LlmCompleted, "llm_completed"),
            (EventType::LlmFailed, "llm_failed"),
            (EventType::ToolInvoked, "tool_invoked"),
            (EventType::ToolCompleted, "tool_completed"),
            (EventType::ToolFailed, "tool_failed"),
        ];

        for (variant, s) in cases {
            assert_eq!(variant.as_str(), s);
            assert_eq!(EventType::from_str(s), Some(variant.clone()));

            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{}\"", s));
            let parsed: EventType = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_event_type_scope() {
        // Session 级别
        assert!(EventType::SessionStarted.is_session_level());
        assert!(EventType::SessionEnded.is_session_level());
        assert!(EventType::SummaryMarker.is_session_level());
        assert_eq!(EventType::SessionStarted.scope(), EventScope::Session);

        // Turn 级别
        assert!(EventType::TurnStarted.is_turn_level());
        assert!(EventType::TurnCompleted.is_turn_level());
        assert!(EventType::TurnFailed.is_turn_level());
        assert_eq!(EventType::TurnStarted.scope(), EventScope::Turn);

        // Step 级别
        assert!(EventType::LlmInvoked.is_step_level());
        assert!(EventType::ToolCompleted.is_step_level());
        assert_eq!(EventType::LlmInvoked.scope(), EventScope::Step);
    }

    #[test]
    fn test_step_start_terminal_matching() {
        assert!(EventType::matches_start_type(
            &EventType::LlmInvoked,
            &EventType::LlmCompleted
        ));
        assert!(EventType::matches_start_type(
            &EventType::LlmInvoked,
            &EventType::LlmFailed
        ));
        assert!(EventType::matches_start_type(
            &EventType::ToolInvoked,
            &EventType::ToolCompleted
        ));
        assert!(EventType::matches_start_type(
            &EventType::ToolInvoked,
            &EventType::ToolFailed
        ));

        // 不匹配的情况
        assert!(!EventType::matches_start_type(
            &EventType::LlmInvoked,
            &EventType::ToolCompleted
        ));
        assert!(!EventType::matches_start_type(
            &EventType::ToolInvoked,
            &EventType::LlmFailed
        ));
    }

    #[test]
    fn test_agent_event_scope_validation() {
        // 正确的 Session 级事件
        let e = AgentEvent::new(
            "sess_1".into(),
            None,
            None,
            EventType::SessionStarted,
            serde_json::json!({"agent_type": "test"}),
        );
        assert!(e.validate_scope().is_ok());

        // 错误的 Session 级事件：turn_id 有值
        let e = AgentEvent::new(
            "sess_1".into(),
            Some(1),
            None,
            EventType::SessionStarted,
            serde_json::json!({}),
        );
        assert!(e.validate_scope().is_err());

        // 正确的 Turn 级事件
        let e = AgentEvent::new(
            "sess_1".into(),
            Some(1),
            None,
            EventType::TurnStarted,
            serde_json::json!({"user_input": "hi", "redo_group": "rg_1"}),
        );
        assert!(e.validate_scope().is_ok());

        // 错误的 Turn 级事件：turn_id 为 None
        let e = AgentEvent::new(
            "sess_1".into(),
            None,
            None,
            EventType::TurnStarted,
            serde_json::json!({}),
        );
        assert!(e.validate_scope().is_err());

        // 正确的 Step 级事件
        let e = AgentEvent::new(
            "sess_1".into(),
            Some(1),
            Some("step_1".into()),
            EventType::LlmInvoked,
            serde_json::json!({"model": "gpt-4", "messages": [], "local_seq": 1}),
        );
        assert!(e.validate_scope().is_ok());

        // Session 级后台 Step：turn_id = NULL, step_id NOT NULL
        let e = AgentEvent::new(
            "sess_1".into(),
            None,
            Some("step_s1".into()),
            EventType::LlmInvoked,
            serde_json::json!({"model": "gpt-4", "messages": [], "local_seq": 1}),
        );
        assert!(e.validate_scope().is_ok());

        // 错误的 Step 级事件：step_id 为 None
        let e = AgentEvent::new(
            "sess_1".into(),
            Some(1),
            None,
            EventType::LlmInvoked,
            serde_json::json!({}),
        );
        assert!(e.validate_scope().is_err());
    }

    #[test]
    fn test_payload_validation() {
        // llm_invoked 必须有 model, messages, local_seq
        let payload = serde_json::json!({"model": "gpt-4", "messages": [], "local_seq": 1});
        assert!(validate_payload_required_fields(&EventType::LlmInvoked, &payload).is_ok());

        // 缺少 model
        let payload = serde_json::json!({"messages": [], "local_seq": 1});
        assert!(validate_payload_required_fields(&EventType::LlmInvoked, &payload).is_err());

        // summary_marker 必须字段
        let payload = serde_json::json!({
            "summarized_up_to_seq": 100,
            "summarized_up_to_turn_id": 5,
            "summary": "test summary"
        });
        assert!(validate_payload_required_fields(&EventType::SummaryMarker, &payload).is_ok());
    }

    #[test]
    fn test_turn_started_payload_serde() {
        let payload = TurnStartedPayload {
            user_input: "hello".into(),
            redo_group: "rg_001".into(),
            redo_count: 0,
        };
        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["user_input"], "hello");
        assert_eq!(json["redo_group"], "rg_001");
        assert_eq!(json["redo_count"], 0);
    }

    #[test]
    fn test_all_event_types_have_str_repr() {
        let all = EventType::all_str_variants();
        assert_eq!(all.len(), 15, "Expected 15 event types");
        for s in all {
            assert!(EventType::from_str(s).is_some(), "failed for: {}", s);
        }
    }

    #[test]
    fn test_task_state_serde_roundtrip() {
        for (variant, s) in [
            (TaskState::Created, "created"),
            (TaskState::Ready, "ready"),
            (TaskState::Claimed, "claimed"),
            (TaskState::Executing, "executing"),
            (TaskState::Blocked, "blocked"),
            (TaskState::Succeeded, "succeeded"),
            (TaskState::Failed, "failed"),
            (TaskState::Canceled, "canceled"),
        ] {
            assert_eq!(variant.as_str(), s);
            assert_eq!(TaskState::from_str(s), Some(variant));
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{}\"", s));
        }
    }

    #[test]
    fn test_task_state_legal_transitions() {
        use TaskState::*;
        // spec §4 状态机
        let legal = [
            (Created, Ready),
            (Ready, Claimed),
            (Claimed, Executing),
            (Executing, Blocked),
            (Blocked, Ready),
            (Executing, Succeeded),
            (Executing, Failed),
        ];
        for (from, to) in legal {
            assert!(
                TaskState::can_transition(from, to),
                "{:?}→{:?} should be legal",
                from,
                to
            );
        }
    }

    #[test]
    fn test_task_state_illegal_transitions() {
        use TaskState::*;
        let illegal = [
            (Created, Claimed),   // 必须 ready→claimed,跳过 ready 非法
            (Ready, Executing),   // 必须 claimed→executing
            (Succeeded, Ready),   // 终态不可迁出
            (Failed, Created),
            (Canceled, Ready),
            (Blocked, Succeeded), // blocked→ready→claimed→executing→succeeded,不可直达
        ];
        for (from, to) in illegal {
            assert!(
                !TaskState::can_transition(from, to),
                "{:?}→{:?} should be illegal",
                from,
                to
            );
        }
    }

    #[test]
    fn test_task_state_terminal() {
        use TaskState::*;
        assert!(Succeeded.is_terminal());
        assert!(Failed.is_terminal());
        assert!(Canceled.is_terminal());
        assert!(!Created.is_terminal());
        assert!(!Executing.is_terminal());
        assert!(!Blocked.is_terminal());
    }
}
