//! fixlet 消息路由循环
//!
//! fixlet 的核心：连接 fixus（WebSocket），管理 Agent 子进程（ACP stdio），
//! 双向路由消息。无状态——崩溃后重启即可。

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::acp::{self, AcpClient, ParsedAcpEvent};
use super::idempotency::TurnContext;

/// 流式消息累积器
struct MessageAccumulator {
    chunks: Vec<String>,
}

impl MessageAccumulator {
    fn new() -> Self {
        Self { chunks: Vec::new() }
    }

    fn append(&mut self, chunk: &str) {
        self.chunks.push(chunk.to_string());
    }

    fn finalize(&self) -> String {
        self.chunks.join("")
    }

    fn reset(&mut self) {
        self.chunks.clear();
    }
}

// ── 配置 ────────────────────────────────────────────────────────────────

/// fixlet 配置
#[derive(Debug, Clone)]
pub struct FixletConfig {
    /// fixus Gateway WebSocket URL
    pub fixus_url: String,
    /// 此 fixlet 服务的 agent_type(fixus 按它路由 execute_turn,不再绑定具体 session_id)
    pub agent_type: String,
    /// Agent 启动命令（例如 "claude-agent-acp"）
    pub agent_command: String,
    /// Agent 工作目录
    pub agent_cwd: Option<String>,
}

impl Default for FixletConfig {
    fn default() -> Self {
        Self {
            fixus_url: "ws://127.0.0.1:3000/ws/fixlet".into(),
            agent_type: "default".into(),
            agent_command: "claude-agent-acp".into(),
            agent_cwd: None,
        }
    }
}

// ── fixus Protocol 消息类型 ─────────────────────────────────────────────

/// fixus → fixlet 消息
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "type")]
pub enum FixusToFixlet {
    #[serde(rename = "execute_turn")]
    ExecuteTurn(ExecuteTurnMsg),
    #[serde(rename = "tool_result")]
    ToolResult(ToolResultMsg),
    #[serde(rename = "pong")]
    Pong { timestamp: String },
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ExecuteTurnMsg {
    pub session_id: String,
    pub turn_id: i64,
    pub input: TurnInputMsg,
    pub context: TurnContextMsg,
    #[serde(default)]
    pub tools: Vec<fixus::protocol::ToolDefinition>,
    pub redo_group: String,
    #[serde(default)]
    pub redo_count: i32,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TurnInputMsg {
    pub user_input: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TurnContextMsg {
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub messages: Vec<fixus::Message>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ToolResultMsg {
    pub step_id: String,
    pub tool_call_id: String,
    pub output: Value,
}

/// fixlet → fixus 消息
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type")]
pub enum FixletToFixus {
    #[serde(rename = "tool_invoked")]
    ToolInvoked {
        session_id: String,
        turn_id: i64,
        step_id: String,
        local_seq: i64,
        tool_name: String,
        tool_call_id: String,
        idempotency_key: String,
        input: Value,
    },
    #[serde(rename = "turn_execution_done")]
    TurnExecutionDone {
        session_id: String,
        turn_id: i64,
        max_local_seq: i64,
        final_output: String,
    },
    #[serde(rename = "turn_execution_error")]
    TurnExecutionError {
        session_id: String,
        turn_id: i64,
        error_type: String,
        error_message: String,
    },
    #[serde(rename = "llm_chunk")]
    LlmChunk {
        session_id: String,
        turn_id: i64,
        text: String,
    },
    #[serde(rename = "llm_completed")]
    LlmCompleted {
        session_id: String,
        turn_id: i64,
        model: String,
        input_tokens: i64,
        output_tokens: i64,
        total_tokens: i64,
        #[serde(default)]
        cached_read_tokens: i64,
        #[serde(default)]
        cached_write_tokens: i64,
    },
    #[serde(rename = "ping")]
    Ping { timestamp: String },
}

impl FixletToFixus {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

// ── Agent 进程管理 ──────────────────────────────────────────────────────

/// 启动 Agent 子进程，返回 stdin writer channel 和 stdout reader
fn spawn_agent(config: &FixletConfig) -> Option<(tokio::sync::mpsc::UnboundedSender<String>, tokio::sync::mpsc::UnboundedReceiver<String>, Child)> {
    let mut cmd = if cfg!(target_os = "windows") {
        let mut c = Command::new("cmd");
        c.args(["/C", &config.agent_command]);
        c
    } else {
        let mut c = Command::new("bash");
        c.args(["-c", &config.agent_command]);
        c
    };

    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::inherit());

    if let Some(ref cwd) = config.agent_cwd {
        cmd.current_dir(cwd);
    }

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to spawn agent process: {}", e);
            return None;
        }
    };

    let stdin = child.stdin.take()?;
    let stdout = child.stdout.take()?;

    // stdin writer task
    let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let mut writer = stdin;
        while let Some(line) = stdin_rx.recv().await {
            let mut msg = line;
            msg.push('\n');
            if writer.write_all(msg.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    // stdout reader task
    let (stdout_tx, stdout_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if stdout_tx.send(line).is_err() {
                break;
            }
        }
    });

    Some((stdin_tx, stdout_rx, child))
}

// ── 主路由循环 ──────────────────────────────────────────────────────────

/// fixlet 主循环
///
/// 连接 fixus WebSocket，处理 execute_turn，启动 Agent，双向路由消息。
pub async fn run(config: FixletConfig) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        tracing::info!("Connecting to fixus at {}", config.fixus_url);

        let ws = match connect_async(&config.fixus_url).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                tracing::error!("Failed to connect to fixus: {}. Retrying in 3s...", e);
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                continue;
            }
        };

        tracing::info!("Connected to fixus");

        let (mut ws_tx_raw, mut ws_rx) = ws.split();

        // 发送 register 消息，声明此 fixlet 服务的 agent_type
        let register_msg = serde_json::json!({
            "type": "register",
            "agent_type": config.agent_type,
        });
        if let Err(e) = ws_tx_raw.send(Message::Text(register_msg.to_string().into())).await {
            tracing::error!("Failed to send register message: {}", e);
            continue;
        }
        tracing::info!("Registered for agent_type {}", config.agent_type);

        let ws_tx = std::sync::Arc::new(tokio::sync::Mutex::new(ws_tx_raw));

        // 当前 activate Turn 上下文（每次 execute_turn 时替换）
        let mut active_turn: Option<TurnContext> = None;
        let mut agent_stdin: Option<tokio::sync::mpsc::UnboundedSender<String>> = None;
        let mut agent_stdout: Option<tokio::sync::mpsc::UnboundedReceiver<String>> = None;
        // Child 进程 handle（用于监控退出）
        let mut agent_child: Option<Child> = None;
        // 流式消息累积器
        let mut msg_accumulator = MessageAccumulator::new();

        // 发送一个初始 ping 告知 fixus fixlet 已就绪
        {
            let ping = FixletToFixus::Ping {
                timestamp: chrono::Utc::now().to_rfc3339(),
            };
            let mut tx = ws_tx.lock().await;
            let _ = tx.send(Message::Text(ping.to_json().into())).await;
        }

        loop {
            tokio::select! {
                // ── fixus → fixlet (WebSocket) ──
                msg = ws_rx.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(e) = handle_fixus_message(
                                &text,
                                &ws_tx,
                                &mut active_turn,
                                &mut agent_stdin,
                                &mut agent_stdout,
                                &mut agent_child,
                                &config,
                                &mut msg_accumulator,
                            ).await {
                                tracing::error!("Error handling fixus message: {}", e);
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            tracing::warn!("fixus WebSocket closed, reconnecting...");
                            break; // 跳出内层循环，重新连接
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let mut tx = ws_tx.lock().await;
                            let _ = tx.send(Message::Pong(data)).await;
                        }
                        _ => {}
                    }
                }

                // ── Agent → fixlet (stdout) ──
                agent_msg = async {
                    match &mut agent_stdout {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(text) = agent_msg {
                        if let Some(ref mut ctx) = active_turn {
                            if let Err(e) = handle_agent_message(
                                &text,
                                ctx,
                                &ws_tx,
                                agent_stdin.as_ref(),
                                &mut msg_accumulator,
                            ).await {
                                tracing::error!("Error handling agent message: {}", e);
                            }
                        }
                    }
                }

                // ── Agent 进程退出检测 ──
                _ = async {
                    match &mut agent_child {
                        Some(child) => {
                            let status = child.wait().await;
                            Some(status)
                        }
                        None => std::future::pending().await,
                    }
                } => {
                    tracing::warn!("Agent process exited unexpectedly");
                    if let Some(ref ctx) = active_turn {
                        let error_msg = FixletToFixus::TurnExecutionError {
                            session_id: ctx.task_id.clone(),
                            turn_id: ctx.turn_id,
                            error_type: "agent_process_exited".into(),
                            error_message: "Agent process exited unexpectedly".into(),
                        };
                        let mut tx = ws_tx.lock().await;
                        let _ = tx.send(Message::Text(error_msg.to_json().into())).await;
                    }
                    agent_stdin = None;
                    agent_stdout = None;
                    agent_child = None;
                    active_turn = None;
                }
            }
        }
    }
}

/// 处理来自 fixus 的消息
async fn handle_fixus_message(
    text: &str,
    ws_tx: &std::sync::Arc<tokio::sync::Mutex<impl SinkExt<Message> + Unpin>>,
    active_turn: &mut Option<TurnContext>,
    agent_stdin: &mut Option<tokio::sync::mpsc::UnboundedSender<String>>,
    agent_stdout: &mut Option<tokio::sync::mpsc::UnboundedReceiver<String>>,
    agent_child: &mut Option<Child>,
    config: &FixletConfig,
    msg_accumulator: &mut MessageAccumulator,
) -> Result<(), Box<dyn std::error::Error>> {
    let msg: FixusToFixlet = serde_json::from_str(text)?;

    match msg {
        FixusToFixlet::ExecuteTurn(et) => {
            tracing::info!("Received execute_turn: session={}, turn={}", et.session_id, et.turn_id);

            // 1. 创建 Turn 上下文，重置消息累积器
            msg_accumulator.reset();
            let mut ctx = TurnContext::new(
                et.session_id.clone(),
                et.turn_id,
                et.redo_group.clone(),
                et.redo_count,
            );
            *active_turn = Some(ctx.clone());

            // 2. 如果 redo_count > 0，告知 Agent 这是重做
            let redo_note = if et.redo_count > 0 {
                format!(
                    "\n[fixus] This is a retry (redo_count={}, redo_group={}). Idempotency keys are available for safe retry.",
                    et.redo_count, et.redo_group
                )
            } else {
                String::new()
            };

            // 3. 构建 ACP prompt
            let user_input = format!("{}{}", et.input.user_input, redo_note);
            let prompt_blocks = acp::build_acp_prompt(
                &et.context.summary,
                &et.context.messages,
                &user_input,
            );
            let acp_tools = acp::build_acp_tools(&et.tools);

            // 4. 启动新的 Agent 子进程（如果尚未启动或需要重启）
            if agent_child.is_none() {
                if let Some((stdin_tx, stdout_rx, child)) = spawn_agent(config) {
                    *agent_stdin = Some(stdin_tx.clone());
                    *agent_stdout = Some(stdout_rx);
                    *agent_child = Some(child);

                    // ACP 初始化握手
                    let mut acp = AcpClient::new(et.session_id.clone());
                    acp.set_stdin_tx(stdin_tx.clone());

                    acp.initialize();
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                    // session/new — 需要 mcpServers 参数，响应中包含真实 sessionId
                    let session_new_id = acp.next_req_id();
                    let session_new_msg = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": session_new_id,
                        "method": "session/new",
                        "params": {
                            "cwd": config.agent_cwd.clone().unwrap_or_else(|| "/tmp".into()),
                            "mcpServers": []
                        }
                    });
                    acp.send_raw(&session_new_msg.to_string());

                    // 等待 session/new 响应以获取真实 sessionId
                    let real_session_id = loop {
                        match agent_stdout.as_mut().unwrap().recv().await {
                            Some(line) => {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                                    if v.get("id").and_then(|i| i.as_i64()) == Some(session_new_id) {
                                        if let Some(sid) = v.get("result")
                                            .and_then(|r| r.get("sessionId"))
                                            .and_then(|s| s.as_str())
                                        {
                                            let model = v.get("result")
                                                .and_then(|r| r.get("models"))
                                                .and_then(|m| m.get("currentModelId"))
                                                .and_then(|s| s.as_str())
                                                .unwrap_or("");
                                            tracing::info!("ACP session/new: sessionId={} model={}", sid, model);
                                            ctx.model = model.to_string();
                                            *active_turn = Some(ctx.clone()); // 同步更新
                                            break Some(sid.to_string());
                                        }
                                    }
                                    // 忽略中间通知
                                }
                            }
                            None => { break None; }
                        }
                    };

                    let real_sid = real_session_id
                        .unwrap_or_else(|| format!("fixus:{}:turn_{}", et.session_id, et.turn_id));

                    acp.session_prompt(&real_sid, prompt_blocks, acp_tools);
                } else {
                    let error_msg = FixletToFixus::TurnExecutionError {
                        session_id: et.session_id,
                        turn_id: et.turn_id,
                        error_type: "agent_spawn_failed".into(),
                        error_message: "Failed to spawn agent process".into(),
                    };
                    let mut tx = ws_tx.lock().await;
                    let _ = tx.send(Message::Text(error_msg.to_json().into())).await;
                }
            } else {
                // Agent 已在运行，直接发 prompt
                let mut acp = AcpClient::new(et.session_id.clone());
                acp.set_stdin_tx(agent_stdin.as_ref().unwrap().clone());
                acp.session_prompt(
                    &format!("{}:turn_{}", et.session_id, et.turn_id),
                    prompt_blocks,
                    acp_tools,
                );
            }
        }

        FixusToFixlet::ToolResult(tr) => {
            tracing::info!(
                "Received tool_result: step_id={}, tool_call_id={}",
                tr.step_id,
                tr.tool_call_id
            );

            // 将结果转发给 Agent（ACP tool_result）
            if let Some(ref stdin_tx) = agent_stdin {
                let result_json = serde_json::to_string(&tr.output).unwrap_or_default();
                let mut acp = AcpClient::new(
                    active_turn
                        .as_ref()
                        .map(|c| c.task_id.clone())
                        .unwrap_or_default(),
                );
                acp.set_stdin_tx(stdin_tx.clone());

                let session_id = active_turn
                    .as_ref()
                    .map(|c| format!("{}:turn_{}", c.task_id, c.turn_id))
                    .unwrap_or_default();

                acp.tool_result(&session_id, &tr.tool_call_id, &result_json);
            }
        }

        FixusToFixlet::Pong { .. } => {
            // 心跳 pong，无需处理
        }
    }

    Ok(())
}

/// 处理来自 Agent 的消息
async fn handle_agent_message(
    text: &str,
    ctx: &mut TurnContext,
    ws_tx: &std::sync::Arc<tokio::sync::Mutex<impl SinkExt<Message> + Unpin>>,
    _agent_stdin: Option<&tokio::sync::mpsc::UnboundedSender<String>>,
    msg_acc: &mut MessageAccumulator,
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed = match acp::parse_acp_message(text) {
        Some(p) => p,
        None => return Ok(()),
    };

    match parsed {
        // 流式消息片段 — 累积
        ParsedAcpEvent::MessageChunk(chunk) => {
            msg_acc.append(&chunk);

            // 实时转发每个 token 给 fixus（用于 SSE 流式输出）
            let chunk_msg = FixletToFixus::LlmChunk {
                session_id: ctx.task_id.clone(),
                turn_id: ctx.turn_id,
                text: chunk.clone(),
            };
            let mut tx = ws_tx.lock().await;
            let _ = tx.send(Message::Text(chunk_msg.to_json().into())).await;

            tracing::debug!("agent message chunk: {} chars", chunk.len());
        }

        // 流式思考 — 忽略（可记录到 debug 日志）
        ParsedAcpEvent::ThoughtChunk(_) => {
            // 不处理思考过程
        }

        ParsedAcpEvent::ToolCall(tc) => {
            // Agent 请求执行 Tool → 转发给 fixus
            let meta = ctx.prepare_tool_call(&tc.name, &tc.toolCallId, &tc.arguments);

            tracing::info!(
                "Agent tool_call: {} (step_id={}, local_seq={})",
                tc.name,
                meta.step_id,
                meta.local_seq
            );

            let msg = FixletToFixus::ToolInvoked {
                session_id: ctx.task_id.clone(),
                turn_id: ctx.turn_id,
                step_id: meta.step_id,
                local_seq: meta.local_seq,
                tool_name: tc.name,
                tool_call_id: tc.toolCallId,
                idempotency_key: meta.idempotency_key,
                input: tc.arguments,
            };

            let mut tx = ws_tx.lock().await;
            let _ = tx.send(Message::Text(msg.to_json().into())).await;
        }

        ParsedAcpEvent::FinalMessage { usage } => {
            let final_text = msg_acc.finalize();

            // 先发送 llm_completed（含 usage + model 数据）
            if let Some(ref u) = usage {
                let llm_msg = FixletToFixus::LlmCompleted {
                    session_id: ctx.task_id.clone(),
                    turn_id: ctx.turn_id,
                    model: ctx.model.clone(),
                    input_tokens: u.input_tokens,
                    output_tokens: u.output_tokens,
                    total_tokens: u.total_tokens,
                    cached_read_tokens: u.cached_read_tokens,
                    cached_write_tokens: u.cached_write_tokens,
                };
                let mut tx = ws_tx.lock().await;
                let _ = tx.send(Message::Text(llm_msg.to_json().into())).await;
                tracing::info!(
                    "LLM completed: {} input + {} output = {} total tokens",
                    u.input_tokens, u.output_tokens, u.total_tokens
                );
            }

            tracing::info!(
                "Agent final message: {} chars ({} chunks), max_local_seq={}",
                final_text.len(),
                msg_acc.chunks.len(),
                ctx.local_seq.current()
            );

            let done_msg = FixletToFixus::TurnExecutionDone {
                session_id: ctx.task_id.clone(),
                turn_id: ctx.turn_id,
                max_local_seq: ctx.local_seq.current(),
                final_output: final_text,
            };

            let mut tx = ws_tx.lock().await;
            let _ = tx.send(Message::Text(done_msg.to_json().into())).await;
        }

        ParsedAcpEvent::Error(err) => {
            tracing::warn!("Agent error: {}", err);
        }

        ParsedAcpEvent::Other(_) => {
            // 忽略未识别的事件
        }
    }

    Ok(())
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixlet_to_fixus_serialization() {
        let msg = FixletToFixus::ToolInvoked {
            session_id: "sess_1".into(),
            turn_id: 1,
            step_id: "step_1".into(),
            local_seq: 3,
            tool_name: "Bash".into(),
            tool_call_id: "call_42".into(),
            idempotency_key: "sess_1:rg_abc:Bash:abc123".into(),
            input: serde_json::json!({"command": "echo hello"}),
        };

        let json = msg.to_json();
        let parsed: Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["type"], "tool_invoked");
        assert_eq!(parsed["session_id"], "sess_1");
        assert_eq!(parsed["step_id"], "step_1");
        assert_eq!(parsed["local_seq"], 3);
        assert_eq!(parsed["tool_name"], "Bash");
    }

    #[test]
    fn test_deserialize_execute_turn() {
        let json = r#"{
            "type": "execute_turn",
            "session_id": "sess_1",
            "turn_id": 1,
            "input": {"user_input": "hello"},
            "context": {"summary": "", "messages": []},
            "tools": [],
            "redo_group": "rg_abc",
            "redo_count": 0
        }"#;

        let msg: FixusToFixlet = serde_json::from_str(json).unwrap();
        match msg {
            FixusToFixlet::ExecuteTurn(et) => {
                assert_eq!(et.session_id, "sess_1");
                assert_eq!(et.turn_id, 1);
                assert_eq!(et.redo_group, "rg_abc");
                assert_eq!(et.input.user_input, "hello");
            }
            _ => panic!("Expected ExecuteTurn"),
        }
    }

    #[test]
    fn test_llm_chunk_serialization() {
        let msg = FixletToFixus::LlmChunk {
            session_id: "sess_1".into(),
            turn_id: 1,
            text: "hello".into(),
        };
        let json = msg.to_json();
        let parsed: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], "llm_chunk");
        assert_eq!(parsed["text"], "hello");
    }
}
