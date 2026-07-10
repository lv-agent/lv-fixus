//! fixus ↔ fixlet Protocol 消息定义
//!
//! 实现设计文档 16.3 节协议规范。
//!
//! ## 消息类型
//!
//! ### Command Channel（fixus → fixlet）
//! - `execute_turn` — 正常执行
//! - `execute_turn` (redo_count > 0) — 恢复重做
//!
//! ### Event Channel（fixlet → fixus）
//! - `agent_event` — fixlet 回传内部事件
//! - `turn_execution_done` — Turn 执行完成信号
//!
//! ### Control
//! - `error` — 错误响应
//! - `ping` / `pong` — 心跳

use serde::{Deserialize, Serialize};

use crate::models::Message;

// ── 协议版本 ────────────────────────────────────────────────────────────

/// 当前协议版本
pub const PROTOCOL_VERSION: &str = "1.0";

// ── fixus → fixlet 消息 ─────────────────────────────────────────────────

/// Execute Turn 请求（fixus → fixlet）
///
/// 设计文档 16.3.1 节。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "execute_turn")]
pub struct ExecuteTurnRequest {
    pub session_id: String,
    pub turn_id: i64,
    pub input: TurnInput,
    pub context: TurnContext,
    #[serde(default)]
    pub tools: Vec<ToolDefinition>,
    pub redo_group: String,
    #[serde(default)]
    pub redo_count: i32,
    /// 协议版本
    #[serde(default = "default_protocol_version")]
    pub protocol_version: String,
}

fn default_protocol_version() -> String {
    PROTOCOL_VERSION.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnInput {
    pub user_input: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TurnContext {
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Turn 执行完成信号（fixlet → fixus）
///
/// 设计文档 16.3.3 节。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "turn_execution_done")]
pub struct TurnExecutionDone {
    pub session_id: String,
    pub turn_id: i64,
    /// fixlet 侧最大 local_seq
    pub max_local_seq: i64,
    pub final_output: String,
}

// ── Claim 协议(pull-based 执行,spec §8.3)──────────────────────────────

/// Claim 请求(fixlet → fixus)——执行器认领一个 task_type 的 ready Task
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "claim")]
pub struct ClaimRequest {
    pub task_type: String,
    pub claimant: String,
}

/// Claim 授予(fixus → fixlet)——下发认领到的 Task(含 task_brief 作初始输入)
///
/// `session_id` 为 wire 字段名(值 = task_id);改名留后续 plan(避免 break nuntius)。
/// `context` 用嵌套字段(非 flatten),规避 `#[serde(tag)]` + flatten 冲突。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "claim_granted")]
pub struct ClaimGranted {
    pub session_id: String,
    pub task_type: String,
    /// task_brief(body 编译产物),作首个 turn 的 user_input
    pub task_brief: String,
    #[serde(default)]
    pub context: TurnContext,
}

/// Claim 拒绝(fixus → fixlet)——无匹配 ready Task
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "claim_denied")]
pub struct ClaimDenied {
    pub reason: String,
}

// ── 通用消息 ────────────────────────────────────────────────────────────

/// 错误响应
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "error")]
pub struct ProtocolError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// 心跳
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Heartbeat {
    #[serde(rename = "ping")]
    Ping { timestamp: String },
    #[serde(rename = "pong")]
    Pong { timestamp: String },
}

// ── 协议消息枚举 ────────────────────────────────────────────────────────

/// fixus 下发给 fixlet 的消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FixusToFixletMessage {
    #[serde(rename = "execute_turn")]
    ExecuteTurn(ExecuteTurnRequest),
    #[serde(rename = "ping")]
    Ping { timestamp: String },
    #[serde(rename = "pong")]
    Pong { timestamp: String },
    #[serde(rename = "error")]
    Error(ProtocolError),
}

/// fixlet 上报给 fixus 的消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FixletToFixusMessage {
    #[serde(rename = "agent_event")]
    AgentEvent(AgentEventReport),
    #[serde(rename = "turn_execution_done")]
    TurnExecutionDone(TurnExecutionDone),
    #[serde(rename = "ping")]
    Ping { timestamp: String },
    #[serde(rename = "pong")]
    Pong { timestamp: String },
}

// ── WebSocket 帧 ────────────────────────────────────────────────────────

/// WebSocket 消息帧
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsFrame {
    /// 消息类型标签
    #[serde(rename = "type")]
    pub frame_type: String,
    /// JSON payload
    #[serde(flatten)]
    pub data: serde_json::Value,
}

// ── HTTP API 请求/响应类型 ──────────────────────────────────────────────

/// 创建 Session 请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(default)]
    pub session_id: Option<String>,
    pub agent_type: String,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
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

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_turn_serialization() {
        let req = ExecuteTurnRequest {
            session_id: "sess_001".into(),
            turn_id: 1,
            input: TurnInput {
                user_input: "Hello".into(),
            },
            context: TurnContext {
                summary: "Previous: ...".into(),
                messages: vec![],
            },
            tools: vec![],
            redo_group: "rg_abc123".into(),
            redo_count: 0,
            protocol_version: PROTOCOL_VERSION.into(),
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("execute_turn"));
        assert!(json.contains("sess_001"));
        assert!(json.contains("rg_abc123"));
    }

    #[test]
    fn test_agent_event_report_serialization() {
        let report = AgentEventReport {
            request_id: "req_001".into(),
            session_id: "sess_001".into(),
            turn_id: 1,
            step_id: "step_001".into(),
            local_seq: 3,
            event_type: "tool_invoked".into(),
            payload: serde_json::json!({
                "tool_name": "get_weather",
                "tool_call_id": "call_abc",
                "idempotency_key": "sess_001:rg_abc:get_weather:{}",
                "input": {"city": "Beijing"}
            }),
        };

        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("agent_event"));
        assert!(json.contains("tool_invoked"));
        assert!(json.contains("get_weather"));
    }

    #[test]
    fn test_turn_execution_done_serialization() {
        let done = TurnExecutionDone {
            session_id: "sess_001".into(),
            turn_id: 1,
            max_local_seq: 6,
            final_output: "Task completed.".into(),
        };

        let json = serde_json::to_string(&done).unwrap();
        assert!(json.contains("turn_execution_done"));
        assert!(json.contains("\"max_local_seq\":6"));
    }

    #[test]
    fn test_heartbeat_serialization() {
        let ping = Heartbeat::Ping {
            timestamp: "2026-06-11T12:00:00Z".into(),
        };
        let json = serde_json::to_string(&ping).unwrap();
        assert!(json.contains("ping"));

        let pong = Heartbeat::Pong {
            timestamp: "2026-06-11T12:00:01Z".into(),
        };
        let json = serde_json::to_string(&pong).unwrap();
        assert!(json.contains("pong"));
    }

    #[test]
    fn test_claim_messages_serialization() {
        // fixlet → fixus: claim
        let claim = serde_json::json!({
            "type": "claim",
            "task_type": "db.repair",
            "claimant": "fixlet-1",
        });
        let parsed: ClaimRequest = serde_json::from_value(claim).unwrap();
        assert_eq!(parsed.task_type, "db.repair");
        assert_eq!(parsed.claimant, "fixlet-1");

        // fixus → fixlet: claim_granted(下发任务;context 嵌套字段,不用 flatten 避免 tag 冲突)
        let granted = ClaimGranted {
            session_id: "task_abc".into(), // wire 字段名保留 session_id(值=task_id)
            task_type: "db.repair".into(),
            task_brief: "目标:对 db1 执行全量修复".into(),
            context: TurnContext {
                summary: String::new(),
                messages: vec![],
            },
        };
        let json = serde_json::to_string(&granted).unwrap();
        assert!(json.contains("claim_granted"), "json: {}", json);
        assert!(json.contains("task_abc"));
        assert!(json.contains("db.repair"));
        assert!(json.contains("\"context\""));

        // fixus → fixlet: claim_denied(无 ready 任务)
        let denied = ClaimDenied {
            reason: "no ready task".into(),
        };
        let json = serde_json::to_string(&denied).unwrap();
        assert!(json.contains("claim_denied"));
    }
}
