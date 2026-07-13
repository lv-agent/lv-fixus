//! fixlet — 纯 broker 客户端
//!
//! 只连 broker,不连 fixus。通过 broker 接收 execute_turn,启动 Agent 子进程,
//! 通过 broker 回传 lifecycle 事件。无状态——崩溃后重启即可。

use std::sync::Arc;

use logdb_client::broker::BrokerProducer;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

use super::acp::{self, AcpClient, ParsedAcpEvent};
use super::backend::{self, AgentBackend};
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
#[derive(Clone)]
pub struct FixletConfig {
    /// 此 fixlet 服务的 task_type(决定订阅哪条 `task-begin-{task_type}` stream)
    pub task_type: String,
    /// 可插拔 agent backend(CR-9):spawn 规格 + model 提取。
    pub backend: Arc<dyn AgentBackend>,
    /// Agent 工作目录
    pub agent_cwd: Option<String>,
}


// ── Agent 进程管理 ──────────────────────────────────────────────────────

/// 启动 Agent 子进程，返回 stdin writer channel 和 stdout reader
fn spawn_agent(config: &FixletConfig) -> Option<(tokio::sync::mpsc::UnboundedSender<String>, tokio::sync::mpsc::UnboundedReceiver<String>, Child)> {
    let spec = config.backend.spawn_spec();
    let mut cmd = if cfg!(target_os = "windows") {
        let mut c = Command::new("cmd");
        c.args(["/C", &spec.command]);
        c
    } else {
        let mut c = Command::new("bash");
        c.args(["-c", &spec.command]);
        c
    };

    // backend 额外 env(ClaudeCode 为空;GenericAcpBackend 等可注入)
    for (k, v) in &spec.env {
        cmd.env(k, v);
    }

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

/// 后台 broker 认领(pull-based):竞争消费 `task-begin-{task_type}` stream,
/// 认领到 turn.begin(execute_turn)后转交主循环执行。fixlet 不再被动接收 fixus 推送。
///
/// 用**稳定 group** `fixlets-{task_type}`:offset 服务端持久化,fixlet 重启后续点,
/// 不重放历史 turn.begin(unique-per-instance group 每次重启从 earliest 读会重跑所有历史 turn)。
/// 重启瞬间旧实例的 stale member 由 logdbd session timeout 清理 + 外层 retry 兜底
/// (前置条件:logdbd shards > 1,否则 consumers > shards 报错)。
/// fixlet 执行中崩溃:turn 不经 broker 重投,由 fixus turn 级 redo 兜底(发新 turn.begin)。
async fn turn_claim_subscriber(
    config: FixletConfig,
    turn_tx: tokio::sync::mpsc::UnboundedSender<serde_json::Value>,
) {
    let broker_addr = std::env::var("BROKER_ADDR").unwrap_or_else(|_| "127.0.0.1:5100".into());
    let namespace = std::env::var("LOGDBD_NAMESPACE").unwrap_or_else(|_| "default".into());
    let sanitized = config.task_type.replace('.', "-");
    let stream = format!("task-begin-{}", sanitized);
    let group = format!("fixlets-{}", sanitized);
    let instance_id = uuid::Uuid::now_v7().to_string().replace('-', "");
    let consumer_id = format!("fixlet-{}", instance_id);

    tracing::info!(
        "turn claim subscriber starting: broker={} stream={} group={} consumer={}",
        broker_addr, stream, group, consumer_id
    );

    loop {
        match claim_turns(&broker_addr, &namespace, &stream, &group, &consumer_id, &turn_tx).await {
            Ok(()) => tracing::info!("turn claim subscriber ended normally"),
            Err(e) => {
                tracing::error!("turn claim subscriber error: {}; retrying in 1s", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn claim_turns(
    broker_addr: &str,
    namespace: &str,
    stream: &str,
    group: &str,
    consumer_id: &str,
    turn_tx: &tokio::sync::mpsc::UnboundedSender<serde_json::Value>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use logdb_client::broker::GroupConsumer;
    use logdb_broker_proto::pb::consume_response::Payload;
    use tokio_stream::StreamExt as TokioStreamExt;

    let mut consumer = GroupConsumer::join(
        format!("http://{}", broker_addr),
        namespace,
        stream,
        group,
        consumer_id,
    ).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    tracing::info!("turn claim joined {} ({}), shards: {:?}", group, consumer_id, consumer.assigned_shards());

    let mut frames = consumer.consume_frames().await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    let mut consecutive_errors: u32 = 0;
    while let Some(item) = TokioStreamExt::next(&mut frames).await {
        let frame = match item {
            Ok(f) => { consecutive_errors = 0; f }
            Err(e) => {
                consecutive_errors += 1;
                tracing::error!("turn claim consume error (consecutive={}): {}", consecutive_errors, e);
                if consecutive_errors >= 3 {
                    return Err(format!("{} consecutive errors, rejoining", consecutive_errors).into());
                }
                continue;
            }
        };
        match frame.payload {
            Some(Payload::Record(rec)) => {
                if rec.event_type != "execute_turn" { continue; }
                let payload: serde_json::Value = serde_json::from_slice(&rec.content).unwrap_or_default();
                let task_id = payload.get("session_id").and_then(|v| v.as_str())
                    .or_else(|| payload.get("task_id").and_then(|v| v.as_str()))
                    .unwrap_or("");
                tracing::info!("turn claim: received turn.begin task={} — committing and forwarding", task_id);
                // commit 后转主循环执行;崩溃由 fixus redo 兜底(turn 级恢复)
                let _ = consumer.commit_shard(rec.shard_id, rec.seq).await;
                let _ = turn_tx.send(payload);
            }
            Some(Payload::CaughtUp(_)) | Some(Payload::Rebalance(_)) | Some(Payload::Assignment(_)) => {}
            None => {}
        }
    }
    consumer.leave().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    Ok(())
}

pub async fn run(config: FixletConfig) -> Result<(), Box<dyn std::error::Error>> {
    // turn claim subscriber ↔ 主循环 通道: subscriber 认领 turn.begin 后传给主循环执行
    let (turn_tx, mut turn_rx) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();

    // 启动 turn 认领订阅(后台 pull-based,竞争消费 task-begin-{task_type})
    let claim_config = config.clone();
    let claim_turn_tx = turn_tx.clone();
    tokio::spawn(async move {
        turn_claim_subscriber(claim_config, claim_turn_tx).await;
    });

    // broker lifecycle producer: turn_execution_done 等事件通过 broker 回传 fixus
    let broker_addr = std::env::var("BROKER_ADDR").unwrap_or_else(|_| "127.0.0.1:5100".into());
    let namespace = std::env::var("LOGDBD_NAMESPACE").unwrap_or_else(|_| "default".into());
    let lifecycle_producer = Arc::new(tokio::sync::Mutex::new(
        BrokerProducer::connect(format!("http://{}", broker_addr)).await
            .map_err(|e| format!("broker producer: {}", e))?,
    ));

    // Redis connection for llm_chunk streaming (bypass fixus WS)
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let redis_conn: Arc<tokio::sync::Mutex<Option<redis::aio::ConnectionManager>>> =
        match redis::Client::open(redis_url.clone()) {
            Ok(client) => match client.get_connection_manager().await {
                Ok(conn) => {
                    tracing::info!("fixlet Redis connected for token streaming: {}", redis_url);
                    Arc::new(tokio::sync::Mutex::new(Some(conn)))
                }
                Err(e) => {
                    tracing::warn!("fixlet Redis unavailable ({}), token streaming disabled", e);
                    Arc::new(tokio::sync::Mutex::new(None))
                }
            },
            Err(e) => {
                tracing::warn!("fixlet Redis client error ({}), token streaming disabled", e);
                Arc::new(tokio::sync::Mutex::new(None))
            }
        };

    let lifecycle_producer_clone = lifecycle_producer.clone();
    let lifecycle_namespace = namespace.clone();
    let redis_conn_clone = redis_conn.clone();

    // Agent 状态（在多次 execute_turn 间复用）
    let mut active_turn: Option<TurnContext> = None;
    let mut agent_stdin: Option<tokio::sync::mpsc::UnboundedSender<String>> = None;
    let mut agent_stdout: Option<tokio::sync::mpsc::UnboundedReceiver<String>> = None;
    let mut agent_child: Option<Child> = None;
    let mut msg_accumulator = MessageAccumulator::new();

    loop {
        tokio::select! {
            // ── turn claim subscriber → turn.begin 到达 ──
            Some(et_payload) = turn_rx.recv() => {
                let task_id = et_payload.get("session_id").and_then(|v| v.as_str()).unwrap_or("?");
                tracing::info!("turn.begin received via broker: task={}", task_id);

                if let Err(e) = handle_execute_turn_from_broker(
                    &et_payload,
                    &mut active_turn,
                    &mut agent_stdin,
                    &mut agent_stdout,
                    &mut agent_child,
                    &config,
                    &mut msg_accumulator,
                    &lifecycle_producer_clone,
                    &lifecycle_namespace,
                ).await {
                    tracing::error!("Error handling execute_turn: {}", e);
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
                            agent_stdin.as_ref(),
                            &mut msg_accumulator,
                            &redis_conn_clone,
                            &lifecycle_producer_clone,
                            &lifecycle_namespace,
                        ).await {
                            tracing::error!("Error handling agent message: {}", e);
                        }
                    }
                }
            }

            // ── Agent 进程退出检测 ──
            _ = async {
                match &mut agent_child {
                    Some(child) => { let status = child.wait().await; Some(status) }
                    None => std::future::pending().await,
                }
            } => {
                tracing::warn!("Agent process exited unexpectedly");
                if let Some(ref ctx) = active_turn {
                    let err_payload = serde_json::json!({
                        "task_id": ctx.task_id,
                        "turn_id": ctx.turn_id,
                        "error_type": "agent_process_exited",
                        "error_message": "Agent process exited unexpectedly",
                        "event_type": "turn_execution_error",
                    });
                    let content = serde_json::to_vec(&err_payload).unwrap_or_default();
                    let mut lp = lifecycle_producer_clone.lock().await;
                    if let Err(e) = lp.produce_full(
                        &lifecycle_namespace, "task-end", "turn_execution_error", &content,
                        Some(&ctx.task_id), 0, "application/json", &std::collections::HashMap::from([
                            ("task_id".into(), ctx.task_id.clone()),
                            ("event_type".into(), "turn_execution_error".into()),
                        ]),
                    ).await {
                        tracing::error!("broker produce turn_execution_error failed: {}", e);
                    }
                    drop(lp);
                }
                agent_stdin = None;
                agent_stdout = None;
                agent_child = None;
                active_turn = None;
            }
        }
    }
}

/// 处理来自 broker `task-begin-{task_type}` stream 的 turn.begin(execute_turn) payload
async fn handle_execute_turn_from_broker(
    payload: &serde_json::Value,
    
    active_turn: &mut Option<TurnContext>,
    agent_stdin: &mut Option<tokio::sync::mpsc::UnboundedSender<String>>,
    agent_stdout: &mut Option<tokio::sync::mpsc::UnboundedReceiver<String>>,
    agent_child: &mut Option<Child>,
    config: &FixletConfig,
    msg_accumulator: &mut MessageAccumulator,
    lifecycle_producer: &Arc<tokio::sync::Mutex<BrokerProducer>>,
    lifecycle_namespace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let task_id = payload["session_id"].as_str().unwrap_or("");
    let turn_id = payload["turn_id"].as_i64().unwrap_or(0);
    let user_input = payload["input"]["user_input"].as_str().unwrap_or("");
    let redo_group = payload["redo_group"].as_str().unwrap_or("");
    let redo_count = payload["redo_count"].as_i64().unwrap_or(0) as i32;
    let summary = payload["context"]["summary"].as_str().unwrap_or("");
    let messages: Vec<fixus::Message> = serde_json::from_value(
        payload["context"]["messages"].clone()
    ).unwrap_or_default();

    if task_id.is_empty() || turn_id == 0 {
        return Err("missing session_id or turn_id".into());
    }

    tracing::info!("execute_turn: session={} turn={} redo_count={}", task_id, turn_id, redo_count);

    msg_accumulator.reset();
    let mut ctx = TurnContext::new(task_id.to_string(), turn_id, redo_group.to_string(), redo_count);
    *active_turn = Some(ctx.clone());

    let redo_note = if redo_count > 0 {
        format!("\n[fixus] This is a retry (redo_count={}, redo_group={}).", redo_count, redo_group)
    } else { String::new() };

    let full_input = format!("{}{}", user_input, redo_note);
    let prompt_blocks = acp::build_acp_prompt(summary, &messages, &full_input);

    // ── 1. 确保 agent 进程存在（整个 fixlet 生命周期内只 spawn 一次；复用进程省启动开销）──
    if agent_child.is_none() {
        match spawn_agent(config) {
            Some((stdin_tx, stdout_rx, child)) => {
                *agent_stdin = Some(stdin_tx.clone());
                *agent_stdout = Some(stdout_rx);
                *agent_child = Some(child);

                let mut acp = AcpClient::new(task_id.to_string());
                acp.set_stdin_tx(stdin_tx.clone());
                acp.initialize();
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            None => {
                tracing::error!("Failed to spawn agent process for task {}", task_id);
                let err = serde_json::json!({
                    "task_id": task_id, "turn_id": turn_id,
                    "error_type": "agent_spawn_failed",
                    "error_message": "Failed to spawn agent process",
                    "event_type": "turn_execution_error",
                });
                let mut lp = lifecycle_producer.lock().await;
                let _ = lp.produce_full(lifecycle_namespace, "task-end", "turn_execution_error",
                    &serde_json::to_vec(&err).unwrap_or_default(), Some(task_id), 0,
                    "application/json", &std::collections::HashMap::from([
                        ("task_id".into(), task_id.to_string()),
                        ("event_type".into(), "turn_execution_error".into()),
                    ])).await;
                drop(lp);
                return Ok(());
            }
        }
    }

    // ── 2. 每个 turn 起新 session ──
    // fixus 每 turn 重放全量上下文(context.messages=完整历史)。若复用 session,
    // agent 自身存的历史会与 fixus 重放的历史重叠(上一轮内容讲两遍)。故每 turn 起新 session:
    // 复用 agent 进程(省 spawn+initialize),不复用 session(避免重复 + 天然多 task,
    // 每个 session/new 绑各自 task_id 到 tools-bank MCP header)。
    let tools_bank_url = std::env::var("TOOLS_BANK_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3001/mcp".into());
    tracing::info!("session/new for task {} turn {}: tools-bank={}", task_id, turn_id, tools_bank_url);

    let mut acp = AcpClient::new(task_id.to_string());
    acp.set_stdin_tx(agent_stdin.as_ref().unwrap().clone());

    let session_new_id = acp.next_req_id();
    let cwd = config.agent_cwd.clone().unwrap_or_else(|| "/tmp".into());
    let params = backend::build_session_new_params(
        config.backend.as_ref(),
        task_id,
        &cwd,
        &tools_bank_url,
    );
    acp.send_raw(
        &backend::build_session_new_request(session_new_id, params).to_string(),
    );

    let real_session_id = loop {
        match agent_stdout.as_mut().unwrap().recv().await {
            Some(line) => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                    if v.get("id").and_then(|i| i.as_i64()) == Some(session_new_id) {
                        if let Some(sid) = v.get("result").and_then(|r| r.get("sessionId")).and_then(|s| s.as_str()) {
                            // model 提取走 backend(CR-9):ClaudeCode = result.models.currentModelId
                            let result = v.get("result").cloned().unwrap_or_default();
                            let model = config.backend.extract_model(&result).unwrap_or_default();
                            tracing::info!("ACP session/new: sessionId={} model={}", sid, model);
                            ctx.model = model;
                            *active_turn = Some(ctx.clone());
                            break Some(sid.to_string());
                        }
                    }
                }
            }
            None => { break None; }
        }
    };

    // session/new 期间 agent stdout 关闭 ⇒ agent 进程已退出。清空进程句柄以便下个 turn 重新 spawn,
    // 并回 turn_execution_error(不再用编造的 session id 硬发 prompt——那只会得到 Session not found)。
    let real_sid = match real_session_id {
        Some(sid) => sid,
        None => {
            tracing::error!("session/new failed for task {} (agent closed stdout) — agent likely dead", task_id);
            *agent_stdin = None;
            *agent_stdout = None;
            *agent_child = None;
            let err = serde_json::json!({
                "task_id": task_id, "turn_id": turn_id,
                "error_type": "session_create_failed",
                "error_message": "Agent closed stdout during session/new",
                "event_type": "turn_execution_error",
            });
            let mut lp = lifecycle_producer.lock().await;
            let _ = lp.produce_full(lifecycle_namespace, "task-end", "turn_execution_error",
                &serde_json::to_vec(&err).unwrap_or_default(), Some(task_id), 0,
                "application/json", &std::collections::HashMap::from([
                    ("task_id".into(), task_id.to_string()),
                    ("event_type".into(), "turn_execution_error".into()),
                ])).await;
            drop(lp);
            return Ok(());
        }
    };
    acp.session_prompt(&real_sid, prompt_blocks, vec![]);

    Ok(())
}

/// 处理来自 Agent 的消息
async fn handle_agent_message(
    text: &str,
    ctx: &mut TurnContext,
    
    _agent_stdin: Option<&tokio::sync::mpsc::UnboundedSender<String>>,
    msg_acc: &mut MessageAccumulator,
    redis_conn: &Arc<tokio::sync::Mutex<Option<redis::aio::ConnectionManager>>>,
    lifecycle_producer: &Arc<tokio::sync::Mutex<BrokerProducer>>,
    lifecycle_namespace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed = match acp::parse_acp_message(text) {
        Some(p) => p,
        None => return Ok(()),
    };

    match parsed {
        // 流式消息片段 — 累积
        ParsedAcpEvent::MessageChunk(chunk) => {
            msg_acc.append(&chunk);

            // 实时转发每个 token 到 Redis（fixus-stream SSE 消费），绕过 fixus WS
            if let Some(ref mut conn) = *redis_conn.lock().await {
                let payload = serde_json::json!({
                    "task_id": ctx.task_id,
                    "turn_id": ctx.turn_id,
                    "text": chunk,
                });
                let _ = redis::cmd("PUBLISH")
                    .arg(format!("fixus:token:{}", ctx.task_id))
                    .arg(serde_json::to_string(&payload).unwrap_or_default())
                    .query_async::<()>(conn)
                    .await;
            }

            tracing::debug!("agent message chunk: {} chars", chunk.len());
        }

        // 流式思考 — 忽略（可记录到 debug 日志）
        ParsedAcpEvent::ThoughtChunk(_) => {
            // 不处理思考过程
        }

        ParsedAcpEvent::ToolCall(_tc) => {
            // Agent tool calls 现在由 claude-agent-acp 直接通过 MCP 调用 tools-bank,
            // 不再经 fixus WS 转发。此处仅记录。
            tracing::debug!(
                "Agent tool_call: {} (id={}) — handled by MCP",
                _tc.name, _tc.toolCallId
            );
        }

        ParsedAcpEvent::FinalMessage { usage } => {
            let final_text = msg_acc.finalize();

            // llm_completed → broker lifecycle (替代 WS)
            if let Some(ref u) = usage {
                let llm_payload = serde_json::json!({
                    "task_id": ctx.task_id,
                    "turn_id": ctx.turn_id,
                    "model": ctx.model,
                    "input_tokens": u.input_tokens,
                    "output_tokens": u.output_tokens,
                    "total_tokens": u.total_tokens,
                    "event_type": "llm_completed",
                });
                let content = serde_json::to_vec(&llm_payload).unwrap_or_default();
                let mut lp = lifecycle_producer.lock().await;
                if let Err(e) = lp.produce_full(
                    &lifecycle_namespace, "task-end", "llm_completed", &content,
                    Some(&ctx.task_id), 0, "application/json", &std::collections::HashMap::from([
                        ("task_id".into(), ctx.task_id.clone()),
                        ("event_type".into(), "llm_completed".into()),
                    ]),
                ).await {
                    tracing::error!("broker produce llm_completed failed: {}", e);
                }
                drop(lp);

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

            // Produce turn_execution_done to broker (替代 WS send —— fixlet 独立性)
            let done_payload = serde_json::json!({
                "task_id": ctx.task_id,
                "turn_id": ctx.turn_id,
                "max_local_seq": ctx.local_seq.current(),
                "final_output": final_text,
                "event_type": "turn_execution_done",
            });
            let content = serde_json::to_vec(&done_payload).unwrap_or_default();
            let mut meta = std::collections::HashMap::new();
            meta.insert("task_id".into(), ctx.task_id.clone());
            meta.insert("event_type".into(), "turn_execution_done".into());

            let mut lp = lifecycle_producer.lock().await;
            if let Err(e) = lp.produce_full(
                &lifecycle_namespace, "task-end", "turn_execution_done", &content,
                Some(&ctx.task_id), 0, "application/json", &meta,
            ).await {
                tracing::error!("broker produce turn_execution_done failed: {}", e);
            }
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
