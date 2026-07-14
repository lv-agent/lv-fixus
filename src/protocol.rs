//! fixus HTTP API + 事件回传类型
//!
//! broker 架构后,fixus↔fixlet 不再有私有 WS 协议——turn 派发/完成走 broker
//! (`task-begin-{type}` / `task-end`)。本模块只剩:
//! - HTTP API 请求/响应类型(server.rs 网关端点)
//! - 事件批量回传(`AgentEventReport`,fixlet→fixus 经 `/events/batch`)
//! - `ToolDefinition`(fixlet 据此构建 ACP 工具定义)
//!
//! 遗留 fixus↔fixlet WS 帧类型(ProtocolError/Heartbeat/WsFrame)已随 broker 化移除。

use serde::{Deserialize, Serialize};

use crate::models::Message;

pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

// ── fixlet → fixus 消息 ─────────────────────────────────────────────────

/// Agent Event 回传（fixlet → fixus）
///
/// 设计文档 16.3.2 节。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "agent_event")]
pub struct AgentEventReport {
    /// 请求追踪 ID
    pub request_id: String,
    pub session_id: String,
    pub turn_id: i64,
    pub step_id: String,
    /// fixlet 侧本地序号
    pub local_seq: i64,
    /// 事件类型（snake_case 字符串）
    pub event_type: String,
    /// 事件 payload
    #[serde(default)]
    pub payload: serde_json::Value,
}

// ── HTTP API 请求/响应类型 ──────────────────────────────────────────────

/// 创建 Session 请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(default)]
    pub session_id: Option<String>,
    pub agent_type: String,
    /// task_type 用于 broker 路由(fixlet 按 task_type 订阅 `tasks-{task_type}` stream)。
    /// 未设置时 fallback 到 agent_type。
    #[serde(default)]
    pub task_type: Option<String>,
    /// body(fixus opaque):contract / schema_ref / task_brief / acceptance_result。
    /// nuntius 侧设置,future:task schema 定义后做 schema validation。
    #[serde(default)]
    pub body: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    /// 优先级(CR-1):大者优先派发。默认 0。
    #[serde(default)]
    pub priority: i32,
    /// Phase 1 沙箱边界:task 创建者声明的策略(agent_role + 可选 policy)。
    /// fixus resolve_and_validate(operator, tenant, task_policy, role);越权 → 400。
    #[serde(default)]
    pub policy: Option<crate::models::TaskPolicyRequest>,
}

/// 创建 Session 响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionResponse {
    pub session_id: String,
    pub seq: i64,
}

/// 开始 Turn 请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartTurnRequest {
    pub user_input: String,
    #[serde(default)]
    pub redo_group: Option<String>,
}

/// 开始 Turn 响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartTurnResponse {
    pub turn_id: i64,
    pub redo_group: String,
    pub seq: i64,
}

/// 完成 Turn 请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteTurnRequest {
    pub final_output: String,
}

/// 记录事件请求（fixlet 回传单个事件）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordEventRequest {
    pub session_id: String,
    pub turn_id: Option<i64>,
    pub step_id: String,
    pub local_seq: i64,
    pub event_type: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

/// 记录事件响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordEventResponse {
    pub seq: i64,
}

/// 批量记录事件请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordEventsBatchRequest {
    pub events: Vec<AgentEventReport>,
}

/// 批量记录事件响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordEventsBatchResponse {
    pub seqs: Vec<i64>,
}

/// Session 恢复状态响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryStatusResponse {
    pub session_id: String,
    pub incomplete_turns: Vec<crate::models::IncompleteTurn>,
    pub redo_queue: Vec<RedoInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedoInfo {
    pub turn_id: i64,
    pub redo_group: String,
    pub redo_count: i32,
    pub user_input: String,
}

/// LLM 上下文响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextResponse {
    pub summary: String,
    pub summarized_up_to_seq: i64,
    pub summarized_up_to_turn_id: Option<i64>,
    pub messages: Vec<Message>,
}

/// 通用 API 响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T: Serialize> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn err(error: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error.into()),
        }
    }
}

