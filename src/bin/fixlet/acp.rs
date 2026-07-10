//! ACP (Agent Client Protocol) 客户端
//!
//! 实现 ACP JSON-RPC 2.0 消息的构造和解析。
//! 通过 stdio 与 Agent 子进程通信。
//!
//! 参考: https://agentclientprotocol.com

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ── ACP JSON-RPC 基础类型 ───────────────────────────────────────────────

/// ACP JSON-RPC 请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcpRequest {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// ACP JSON-RPC 通知（无 id 的请求）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcpNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// ACP 事件（从 Agent 收到的任何消息）
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AcpEvent {
    Response(AcpResponse),
    Notification { method: String, params: Value },
}

#[derive(Debug, Clone, Deserialize)]
pub struct AcpResponse {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<i64>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<AcpError>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AcpError {
    pub code: i64,
    pub message: String,
}

impl AcpRequest {
    pub fn new(id: i64, method: &str, params: Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id: Some(id),
            method: method.into(),
            params,
        }
    }

    pub fn notification(method: &str, params: Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id: None,
            method: method.into(),
            params,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

// ── ACP 方法参数 ────────────────────────────────────────────────────────

/// initialize 方法参数
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct InitializeParams {
    pub protocolVersion: String,
    pub clientInfo: ClientInfo,
    pub capabilities: ClientCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct ClientCapabilities {
    #[serde(default)]
    pub tools: bool,
    #[serde(default)]
    pub terminal: bool,
    #[serde(default)]
    pub fileSystem: bool,
}

/// session/new 方法参数
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct SessionNewParams {
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: Option<Value>,
    #[serde(default)]
    pub model: Option<String>,
}

/// session/prompt 方法参数
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct SessionPromptParams {
    pub sessionId: String,
    pub prompt: Vec<PromptBlock>,
    #[serde(default)]
    pub tools: Vec<AcpToolDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PromptBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "resource")]
    Resource { resource: Value },
}

impl PromptBlock {
    pub fn user(text: &str) -> Self {
        Self::Text {
            text: format!("user: {}", text),
        }
    }

    pub fn system(text: &str) -> Self {
        Self::Text {
            text: format!("system: {}", text),
        }
    }

    pub fn assistant(text: &str) -> Self {
        Self::Text {
            text: format!("assistant: {}", text),
        }
    }
}

/// ACP Tool 定义
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct AcpToolDefinition {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub inputSchema: Value,
}

// ── ACP 事件解析 ────────────────────────────────────────────────────────

/// Tool Call 事件（Agent → Client）
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ToolCallEvent {
    #[serde(default)]
    pub sessionId: Option<String>,
    pub toolCallId: String,
    pub name: String,
    pub arguments: Value,
}

/// Tool Result 消息（Client → Agent）
#[derive(Debug, Clone, Serialize)]
#[allow(non_snake_case)]
pub struct ToolResultMessage {
    pub sessionId: String,
    pub toolCallId: String,
    pub content: Vec<ToolResultContent>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ToolResultContent {
    #[serde(rename = "text")]
    Text { text: String },
}

/// Final Message 事件（Agent → Client）
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct FinalMessageEvent {
    #[serde(default)]
    pub sessionId: Option<String>,
    pub content: Vec<FinalContentBlock>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum FinalContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "resource")]
    Resource { resource: Value },
}

/// 解析 ACP 事件类型
#[derive(Debug, Clone)]
pub enum ParsedAcpEvent {
    /// Agent 请求执行 Tool
    ToolCall(ToolCallEvent),
    /// Agent 流式消息片段
    MessageChunk(String),
    /// Agent 流式思考片段（可忽略）
    ThoughtChunk(String),
    /// Agent 返回最终响应（流式消息已通过 MessageChunk 发送，附带 usage）
    FinalMessage { usage: Option<LlmUsage> },
    /// Agent 错误
    Error(String),
    /// 其他未识别的事件
    Other(Value),
}

/// LLM 用量信息（从 ACP 最终响应中提取）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LlmUsage {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    #[serde(default)]
    pub cached_read_tokens: i64,
    #[serde(default)]
    pub cached_write_tokens: i64,
}

/// 从原始 ACP 消息解析出业务事件
///
/// Claude Code ACP 实际协议格式：
/// - 流式消息: `session/update` notification with `agent_message_chunk`
/// - 流式思考: `session/update` notification with `agent_thought_chunk`
/// - Tool 调用: `session/update` notification with `tool_call` in update
/// - 最终响应: 标准 JSON-RPC response with `stopReason` and `usage`
pub fn parse_acp_message(msg: &str) -> Option<ParsedAcpEvent> {
    let value: Value = serde_json::from_str(msg).ok()?;

    let method = value.get("method").and_then(|m| m.as_str());

    match method {
        // session/update — 流式消息和 tool call
        Some("session/update") => {
            let params = value.get("params")?;
            let update = params.get("update")?;
            let update_type = update.get("sessionUpdate").and_then(|v| v.as_str())?;

            match update_type {
                "agent_message_chunk" => {
                    let text = update
                        .get("content")
                        .and_then(|c| c.get("text"))
                        .and_then(|t| t.as_str())
                        .unwrap_or("");
                    // 返回为 MessageChunk，由上层累积
                    return Some(ParsedAcpEvent::MessageChunk(text.to_string()));
                }
                "agent_thought_chunk" => {
                    // 忽略思考过程（或积累到 debug 日志）
                    let text = update
                        .get("content")
                        .and_then(|c| c.get("text"))
                        .and_then(|t| t.as_str())
                        .unwrap_or("");
                    return Some(ParsedAcpEvent::ThoughtChunk(text.to_string()));
                }
                "tool_call" => {
                    // Claude Code 的 tool_call 在 session/update 中
                    if let Ok(tc) = serde_json::from_value::<ToolCallEvent>(update.clone()) {
                        return Some(ParsedAcpEvent::ToolCall(tc));
                    }
                }
                "available_commands_update" | "usage_update" => {
                    // 元信息，忽略（可记录日志）
                    return Some(ParsedAcpEvent::Other(value));
                }
                _ => {
                    tracing::debug!("Unknown session/update type: {}", update_type);
                }
            }
        }

        // 旧式 tool_call 通知（兼容）
        Some("tool_call") => {
            if let Ok(event) = serde_json::from_value::<ToolCallEvent>(
                value.get("params").cloned().unwrap_or_default(),
            ) {
                return Some(ParsedAcpEvent::ToolCall(event));
            }
        }

        _ => {}
    }

    // 检查是否是最终响应（带 result，id 是 prompt 请求的 id）
    if let Some(result) = value.get("result") {
        if let Some(stop_reason) = result.get("stopReason").and_then(|v| v.as_str()) {
            match stop_reason {
                "end_turn" => {
                    // 提取 usage 数据
                    let usage = result.get("usage").map(|u| LlmUsage {
                        input_tokens: u.get("inputTokens").and_then(|v| v.as_i64()).unwrap_or(0),
                        output_tokens: u.get("outputTokens").and_then(|v| v.as_i64()).unwrap_or(0),
                        total_tokens: u.get("totalTokens").and_then(|v| v.as_i64()).unwrap_or(0),
                        cached_read_tokens: u.get("cachedReadTokens").and_then(|v| v.as_i64()).unwrap_or(0),
                        cached_write_tokens: u.get("cachedWriteTokens").and_then(|v| v.as_i64()).unwrap_or(0),
                    });
                    return Some(ParsedAcpEvent::FinalMessage { usage });
                }
                "tool_use" => {
                    return Some(ParsedAcpEvent::Other(value));
                }
                _ => {}
            }
        }
        // 其他 result（如 initialize、session/new）
        return Some(ParsedAcpEvent::Other(value));
    }

    // 检查是否是 error response
    if let Some(error) = value.get("error") {
        let msg = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("Unknown ACP error");
        return Some(ParsedAcpEvent::Error(msg.into()));
    }

    // 兜底
    Some(ParsedAcpEvent::Other(value))
}

// ── ACP Client ───────────────────────────────────────────────────────────

/// ACP Client — 通过 stdio 与 Agent 进程通信
///
/// fixlet 启动 Agent 子进程后，通过此 client 发送 ACP 请求并接收响应。
pub struct AcpClient {
    /// 下一个请求 ID
    next_id: i64,
    /// 当前 session ID
    session_id: String,
    /// Agent 子进程的 stdin writer（序列化的 JSON 行）
    stdin_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
}

impl AcpClient {
    /// 创建新的 ACP Client
    pub fn new(session_id: String) -> Self {
        Self {
            next_id: 1,
            session_id,
            stdin_tx: None,
        }
    }

    /// 设置 stdin 发送通道
    pub fn set_stdin_tx(&mut self, tx: tokio::sync::mpsc::UnboundedSender<String>) {
        self.stdin_tx = Some(tx);
    }

    pub fn next_req_id(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// 发送任意原始消息到 Agent（用于绕过类型封装）
    pub fn send_raw(&self, msg: &str) {
        self.send(msg);
    }

    fn next_id_internal(&mut self) -> i64 {
        self.next_req_id()
    }

    /// 发送 ACP 消息到 Agent
    fn send(&self, msg: &str) {
        if let Some(ref tx) = self.stdin_tx {
            let _ = tx.send(msg.to_string());
        }
    }

    /// 发送 initialize 请求
    pub fn initialize(&mut self) {
        let params = serde_json::json!({
            "protocolVersion": 1,
            "clientInfo": {"name": "fixlet", "version": "0.1.0"},
            "capabilities": {"tools": true, "terminal": true, "fileSystem": true}
        });

        let req = AcpRequest::new(self.next_req_id(), "initialize", params);
        self.send(&req.to_json());
    }

    /// 创建新 session
    pub fn session_new(&mut self, cwd: Option<&str>) {
        let model = std::env::var("ANTHROPIC_MODEL").ok();
        let params = serde_json::to_value(SessionNewParams {
            cwd: cwd.map(|s| s.to_string()),
            env: None,
            model,
        })
        .unwrap();

        let req = AcpRequest::new(self.next_req_id(), "session/new", params);
        self.send(&req.to_json());
    }

    /// 发送 prompt
    pub fn session_prompt(
        &mut self,
        session_id: &str,
        prompt: Vec<PromptBlock>,
        tools: Vec<AcpToolDefinition>,
    ) {
        let params = serde_json::to_value(SessionPromptParams {
            sessionId: session_id.to_string(),
            prompt,
            tools,
        })
        .unwrap();

        let req = AcpRequest::new(self.next_req_id(), "session/prompt", params);
        self.send(&req.to_json());
    }

    /// 发送 tool_result 回 Agent
    pub fn tool_result(
        &mut self,
        session_id: &str,
        tool_call_id: &str,
        result_text: &str,
    ) {
        let msg = ToolResultMessage {
            sessionId: session_id.to_string(),
            toolCallId: tool_call_id.to_string(),
            content: vec![ToolResultContent::Text {
                text: result_text.to_string(),
            }],
        };

        let req = AcpRequest::notification("tool_result", serde_json::to_value(msg).unwrap());
        self.send(&req.to_json());
    }

    /// 取消 session
    pub fn session_cancel(&mut self, session_id: &str) {
        let params = serde_json::json!({"sessionId": session_id});
        let req = AcpRequest::new(self.next_req_id(), "session/cancel", params);
        self.send(&req.to_json());
    }

    /// 发送 ping
    pub fn send_ping(&mut self) {
        let req = AcpRequest::new(self.next_req_id(), "ping", Value::Null);
        self.send(&req.to_json());
    }
}

// ── 上下文构建辅助 ──────────────────────────────────────────────────────

/// 从 fixus context 构建 ACP prompt blocks
pub fn build_acp_prompt(
    summary: &str,
    messages: &[fixus::Message],
    user_input: &str,
) -> Vec<PromptBlock> {
    let mut blocks = Vec::new();

    // System prompt（摘要 + 可用工具说明）
    if !summary.is_empty() {
        blocks.push(PromptBlock::system(&format!(
            "Previous conversation summary:\n{}",
            summary
        )));
    }

    // 历史消息
    for msg in messages {
        match msg.role.as_str() {
            "user" => blocks.push(PromptBlock::user(&msg.content)),
            "assistant" => blocks.push(PromptBlock::assistant(&msg.content)),
            "tool" => blocks.push(PromptBlock::system(&format!(
                "[tool result] {}",
                msg.content
            ))),
            _ => {}
        }
    }

    // 当前用户输入
    blocks.push(PromptBlock::user(user_input));

    blocks
}

/// 从 fixus ToolDefinition 构建 ACP AcpToolDefinition
pub fn build_acp_tools(
    tools: &[fixus::protocol::ToolDefinition],
) -> Vec<AcpToolDefinition> {
    tools
        .iter()
        .map(|t| AcpToolDefinition {
            name: t.name.clone(),
            description: t.description.clone(),
            inputSchema: t.parameters.clone(),
        })
        .collect()
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acp_request_serialization() {
        let req = AcpRequest::new(
            1,
            "initialize",
            serde_json::json!({
                "protocolVersion": "1.0",
                "clientInfo": {"name": "fixlet", "version": "0.1.0"}
            }),
        );

        let json = req.to_json();
        let parsed: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["jsonrpc"], "2.0");
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["method"], "initialize");
    }

    #[test]
    fn test_notification_no_id() {
        let req = AcpRequest::notification("tool_result", serde_json::json!({}));
        let json = req.to_json();
        let parsed: Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("id").is_none() || parsed["id"].is_null());
    }

    #[test]
    fn test_prompt_block_types() {
        let user = PromptBlock::user("hello");
        let sys = PromptBlock::system("context");

        let user_json = serde_json::to_string(&user).unwrap();
        let sys_json = serde_json::to_string(&sys).unwrap();

        assert!(user_json.contains("user:"));
        assert!(sys_json.contains("system:"));
    }

    #[test]
    fn test_parse_tool_call() {
        let msg = r#"{
            "jsonrpc": "2.0",
            "method": "tool_call",
            "params": {
                "sessionId": "sess_001:turn_1",
                "toolCallId": "call_42",
                "name": "Bash",
                "arguments": {"command": "echo hello"}
            }
        }"#;

        let parsed = parse_acp_message(msg).unwrap();
        match parsed {
            ParsedAcpEvent::ToolCall(tc) => {
                assert_eq!(tc.toolCallId, "call_42");
                assert_eq!(tc.name, "Bash");
                assert_eq!(
                    tc.arguments["command"].as_str().unwrap(),
                    "echo hello"
                );
            }
            _ => panic!("Expected ToolCall"),
        }
    }

    #[test]
    fn test_parse_message_chunk() {
        // 真实 Claude Code ACP: session/update 中的 agent_message_chunk
        let msg = r#"{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "test-sess",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "Hello, world!"}
                }
            }
        }"#;

        let parsed = parse_acp_message(msg).unwrap();
        match parsed {
            ParsedAcpEvent::MessageChunk(text) => {
                assert_eq!(text, "Hello, world!");
            }
            _ => panic!("Expected MessageChunk, got {:?}", parsed),
        }
    }

    #[test]
    fn test_parse_final_response() {
        let msg = r#"{
            "jsonrpc": "2.0",
            "id": 3,
            "result": {
                "stopReason": "end_turn",
                "usage": {"inputTokens": 100, "outputTokens": 10, "totalTokens": 110}
            }
        }"#;

        let parsed = parse_acp_message(msg).unwrap();
        match parsed {
            ParsedAcpEvent::FinalMessage { usage } => {
                let u = usage.expect("should have usage data");
                assert_eq!(u.input_tokens, 100);
                assert_eq!(u.output_tokens, 10);
                assert_eq!(u.total_tokens, 110);
            }
            _ => panic!("Expected FinalMessage, got {:?}", parsed),
        }
    }

    #[test]
    fn test_build_acp_prompt() {
        let blocks = build_acp_prompt(
            "Previous: user asked about weather",
            &[],
            "What's the temperature?",
        );

        assert_eq!(blocks.len(), 2); // system + user
        match &blocks[0] {
            PromptBlock::Text { text } => {
                assert!(text.contains("Previous: user asked about weather"));
            }
            _ => panic!("Expected system block"),
        }
    }
}
