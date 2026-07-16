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

/// 由 task_type 算认领 stream 名 + 稳定 group 名。
///
/// 点号转义:`.` 在 stream 名里非法(logdbd 限制,见 memory dev-stack-startup),
/// 故 `task_type` 里的 `.` 换成 `-`。抽成纯函数便于测试。
fn stream_and_group_for(task_type: &str) -> (String, String) {
    let sanitized = task_type.replace('.', "-");
    (
        format!("task-begin-{}", sanitized),
        format!("fixlets-{}", sanitized),
    )
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
    let (stream, group) = stream_and_group_for(&config.task_type);
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
                    // close the llm pair if session/prompt already fired (step_id set)。
                    // 本分支持 ref ctx(不可变),故用 current()+1 而非 next()(&mut);
                    // turn 已结束,计数器即将丢弃,local_seq 仅信息性。
                    if let Some(step_id) = ctx.step_id.as_deref() {
                        emit_lifecycle(
                            &lifecycle_producer_clone, &lifecycle_namespace, &ctx.task_id, "llm_failed",
                            llm_failed_payload(&ctx.task_id, ctx.turn_id, step_id, "agent_process_exited", "Agent process exited unexpectedly", ctx.local_seq.current() + 1),
                        ).await;
                    }
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

/// 解析后的 execute_turn(fixus → fixlet 的 turn.begin 契约)。
struct ParsedExecuteTurn {
    task_id: String,
    turn_id: i64,
    user_input: String,
    redo_group: String,
    redo_count: i32,
    summary: String,
    messages: Vec<fixus::Message>,
    /// 透传的 effective policy(opaque JSON,fixlet 不解析)。fixus 经
    /// turn-begin payload 下发,fixlet 注入 `X-Fixus-Policy` header 给 tools-bank。
    effective_policy: Option<serde_json::Value>,
}

/// 解析 execute_turn payload(fixus → fixlet 的 turn.begin 契约)。
///
/// 字段约定:
/// - `session_id` → task_id(fixus 历史字段名;`task_id` 也接受)
/// - `turn_id` / `redo_group` / `redo_count` — turn 标识与重做计数
/// - `input.user_input` — 本 turn 用户输入
/// - `context.summary` / `context.messages` — LLM 上下文(摘要 + 增量)
///
/// 校验:`session_id` 空 或 `turn_id == 0` → `Err`(缺关键标识)。
/// 抽成纯函数便于测试 fixus↔fixlet 线契约。
fn parse_execute_turn_payload(
    payload: &serde_json::Value,
) -> Result<ParsedExecuteTurn, &'static str> {
    let task_id = payload
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let turn_id = payload.get("turn_id").and_then(|v| v.as_i64()).unwrap_or(0);
    if task_id.is_empty() || turn_id == 0 {
        return Err("missing session_id or turn_id");
    }
    Ok(ParsedExecuteTurn {
        task_id,
        turn_id,
        user_input: payload
            .get("input")
            .and_then(|v| v.get("user_input"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        redo_group: payload.get("redo_group").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        redo_count: payload.get("redo_count").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
        summary: payload
            .get("context")
            .and_then(|v| v.get("summary"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        messages: serde_json::from_value(
            payload.get("context").and_then(|v| v.get("messages")).cloned().unwrap_or_default(),
        )
        .unwrap_or_default(),
        effective_policy: payload.get("effective_policy").cloned(),
    })
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
    let parsed = parse_execute_turn_payload(payload)?;
    let ParsedExecuteTurn { task_id, turn_id, user_input, redo_group, redo_count, summary, messages, effective_policy } = parsed;
    // 影子绑定为 &str —— 保持下游用法(Some(task_id)、build_acp_prompt(summary,…))类型不变,
    // 底层 String 由上面的解构绑定持有,活到函数末尾。
    let task_id = task_id.as_str();
    let user_input = user_input.as_str();
    let redo_group = redo_group.as_str();
    let summary = summary.as_str();

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
    let policy_str = effective_policy.as_ref().map(|v| v.to_string());
    let params = backend::build_session_new_params(
        config.backend.as_ref(),
        task_id,
        &cwd,
        &tools_bank_url,
        turn_id,
        policy_str,
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
    // ── 3. mint step_id + emit llm_invoked(在 session/prompt 发送前)──
    // fixlet 是 LLM 交互的唯一观察者。step_id 在本 turn 的 invoked/completed/failed
    // 三事件间共享(配对)。local_seq.next() 从此激活(此前 LocalSeqCounter 是死代码)。
    let step_id = uuid::Uuid::now_v7().to_string();
    ctx.step_id = Some(step_id.clone());
    let invoked_seq = ctx.local_seq.next();
    *active_turn = Some(ctx.clone());
    emit_lifecycle(
        lifecycle_producer,
        &lifecycle_namespace,
        &ctx.task_id,
        "llm_invoked",
        llm_invoked_payload(
            &ctx.task_id,
            ctx.turn_id,
            &step_id,
            &ctx.model,
            &messages,
            invoked_seq,
        ),
    )
    .await;

    acp.session_prompt(&real_sid, prompt_blocks, vec![]);

    Ok(())
}

// ── step-event payload builders + lifecycle emit helper ─────────────────
//
// fixlet 是 LLM 交互的唯一观察者(session/prompt + FinalMessage{usage})。
// 这里构造 step-event 对(llm_invoked / llm_completed / llm_failed)的 payload,
// emit 到 broker task-end,由 fixus step-events 消费侧消费。
// 消费侧契约:flat token 字段(input_tokens/output_tokens/total_tokens)用于
// llm_completed;messages 用于 llm_invoked —— 这里 payload shape 必须匹配。

fn llm_invoked_payload(
    task_id: &str,
    turn_id: i64,
    step_id: &str,
    model: &str,
    messages: &[fixus::Message],
    local_seq: i64,
) -> Value {
    serde_json::json!({
        "task_id": task_id,
        "turn_id": turn_id,
        "step_id": step_id,
        "model": model,
        "messages": messages,
        "local_seq": local_seq,
        "event_type": "llm_invoked",
    })
}

fn llm_completed_payload(
    task_id: &str,
    turn_id: i64,
    step_id: &str,
    model: &str,
    usage: Option<(i64, i64, i64)>,
    local_seq: i64,
) -> Value {
    let (input_tokens, output_tokens, total_tokens) = usage.unwrap_or((0, 0, 0));
    serde_json::json!({
        "task_id": task_id,
        "turn_id": turn_id,
        "step_id": step_id,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "local_seq": local_seq,
        "event_type": "llm_completed",
    })
}

fn llm_failed_payload(
    task_id: &str,
    turn_id: i64,
    step_id: &str,
    error_type: &str,
    error_message: &str,
    local_seq: i64,
) -> Value {
    serde_json::json!({
        "task_id": task_id,
        "turn_id": turn_id,
        "step_id": step_id,
        "error_type": error_type,
        "error_message": error_message,
        "local_seq": local_seq,
        "event_type": "llm_failed",
    })
}

/// 把 step-event produce 到 broker task-end stream。封装 produce_full + meta;
/// 失败仅 warn(step-event 是 best-effort 观测,不影响 turn 主流程)。
//
// 注:既有 turn_execution_done / turn_execution_error 仍用内联 produce_full(早于本 helper,
// 为控制 diff 范围未迁移);新 LLM step 事件统一走 emit_lifecycle。
async fn emit_lifecycle(
    producer: &tokio::sync::Mutex<BrokerProducer>,
    namespace: &str,
    task_id: &str,
    event_type: &str,
    payload: Value,
) {
    let content = serde_json::to_vec(&payload).unwrap_or_default();
    let meta = std::collections::HashMap::from([
        ("task_id".into(), task_id.to_string()),
        ("event_type".into(), event_type.to_string()),
    ]);
    let mut lp = producer.lock().await;
    if let Err(e) = lp
        .produce_full(
            namespace,
            "task-end",
            event_type,
            &content,
            Some(task_id),
            0,
            "application/json",
            &meta,
        )
        .await
    {
        tracing::warn!("fixlet: broker produce {} failed: {}", event_type, e);
    }
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

            // llm_completed → broker lifecycle(step-events:与 llm_invoked 配对,共享 step_id)。
            // 不再 gate on usage —— 始终 emit(无 usage 时 tokens 默认 0),保证 invoked/completed 配对完整。
            if ctx.step_id.is_some() {
                let usage_tuple = usage.as_ref().map(|u| (u.input_tokens, u.output_tokens, u.total_tokens));
                let completed_seq = ctx.local_seq.next();              // &mut first, borrow ends (NLL)
                let step_id = ctx.step_id.as_deref().unwrap();         // & second — drop the .clone()
                emit_lifecycle(
                    lifecycle_producer,
                    &lifecycle_namespace,
                    &ctx.task_id,
                    "llm_completed",
                    llm_completed_payload(&ctx.task_id, ctx.turn_id, step_id, &ctx.model, usage_tuple, completed_seq),
                )
                .await;
            }
            if let Some(ref u) = usage {
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
            // llm_failed → broker lifecycle(仅当本 turn 已发过 llm_invoked,即 step_id 存在)。
            // spawn/session-new 失败发生在 session/prompt 前(无 step_id)→ 走各自的 turn_execution_error,不在此 emit。
            if ctx.step_id.is_some() {
                let failed_seq = ctx.local_seq.next();                // &mut first, borrow ends (NLL)
                let step_id = ctx.step_id.as_deref().unwrap();        // & second — drop the .clone()
                emit_lifecycle(
                    lifecycle_producer,
                    &lifecycle_namespace,
                    &ctx.task_id,
                    "llm_failed",
                    llm_failed_payload(&ctx.task_id, ctx.turn_id, step_id, "agent_error", &err, failed_seq),
                )
                .await;
            }
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
    use serde_json::json;

    // ── MessageAccumulator ──────────────────────────────────────────

    #[test]
    fn accumulator_new_finalizes_empty() {
        let acc = MessageAccumulator::new();
        assert_eq!(acc.finalize(), "");
    }

    #[test]
    fn accumulator_appends_in_order() {
        let mut acc = MessageAccumulator::new();
        acc.append("Hello, ");
        acc.append("world");
        acc.append("!");
        assert_eq!(acc.finalize(), "Hello, world!");
    }

    #[test]
    fn accumulator_reset_clears() {
        let mut acc = MessageAccumulator::new();
        acc.append("keep?");
        acc.reset();
        assert_eq!(acc.finalize(), "");
        // reset 后可继续累积
        acc.append("again");
        assert_eq!(acc.finalize(), "again");
    }

    // ── stream_and_group_for(认领路由,点号转义)────────────────────

    #[test]
    fn stream_and_group_normal_task_type() {
        let (s, g) = stream_and_group_for("claude");
        assert_eq!(s, "task-begin-claude");
        assert_eq!(g, "fixlets-claude");
    }

    #[test]
    fn stream_and_group_dots_are_sanitized() {
        // 点号在 stream 名非法 → 换成 -(memory dev-stack-startup 的坑)
        let (s, g) = stream_and_group_for("acp.claude.v2");
        assert_eq!(s, "task-begin-acp-claude-v2");
        assert_eq!(g, "fixlets-acp-claude-v2");
    }

    #[test]
    fn stream_and_group_empty_task_type() {
        let (s, g) = stream_and_group_for("");
        assert_eq!(s, "task-begin-");
        assert_eq!(g, "fixlets-");
    }

    // ── parse_execute_turn_payload(fixus→fixlet 线契约)─────────────

    fn full_payload() -> serde_json::Value {
        json!({
            "session_id": "task-1",
            "turn_id": 5,
            "input": { "user_input": "hello" },
            "redo_group": "rg-1",
            "redo_count": 2,
            "context": {
                "summary": "prior summary",
                "messages": [
                    { "role": "user", "content": "hi" },
                    { "role": "assistant", "content": "hey" }
                ]
            }
        })
    }

    #[test]
    fn parse_full_valid_payload() {
        let p = parse_execute_turn_payload(&full_payload()).expect("应解析成功");
        assert_eq!(p.task_id, "task-1");
        assert_eq!(p.turn_id, 5);
        assert_eq!(p.user_input, "hello");
        assert_eq!(p.redo_group, "rg-1");
        assert_eq!(p.redo_count, 2);
        assert_eq!(p.summary, "prior summary");
        assert_eq!(p.messages.len(), 2);
        assert_eq!(p.messages[0].role, "user");
        assert_eq!(p.messages[1].content, "hey");
    }

    #[test]
    fn parse_missing_session_id_errors() {
        let mut p = full_payload();
        p["session_id"] = json!(null);
        assert!(parse_execute_turn_payload(&p).is_err());
    }

    #[test]
    fn parse_empty_session_id_errors() {
        let mut p = full_payload();
        p["session_id"] = json!("");
        assert!(parse_execute_turn_payload(&p).is_err());
    }

    #[test]
    fn parse_turn_id_zero_errors() {
        let mut p = full_payload();
        p["turn_id"] = json!(0);
        assert!(parse_execute_turn_payload(&p).is_err());
    }

    #[test]
    fn parse_missing_turn_id_defaults_to_zero_then_errors() {
        let mut p = full_payload();
        p_object_remove(&mut p, "turn_id");
        assert!(parse_execute_turn_payload(&p).is_err(), "缺 turn_id 默认 0 → 应 Err");
    }

    #[test]
    fn parse_uses_session_id_not_task_id_alias() {
        // 当前契约只认 session_id;仅有 task_id(无 session_id)→ Err。
        // 文档化此行为(若未来要加 task_id 别名,更新此测试)。
        let p = json!({ "task_id": "task-1", "turn_id": 5 });
        assert!(parse_execute_turn_payload(&p).is_err());
    }

    #[test]
    fn parse_optional_fields_default_empty() {
        // 只有关键字段 → 可选字段兜底(user_input="", summary="", messages=[])
        let p = json!({ "session_id": "t", "turn_id": 1 });
        let parsed = parse_execute_turn_payload(&p).expect("关键字段齐全应成功");
        assert_eq!(parsed.task_id, "t");
        assert_eq!(parsed.turn_id, 1);
        assert_eq!(parsed.user_input, "");
        assert_eq!(parsed.redo_group, "");
        assert_eq!(parsed.redo_count, 0);
        assert_eq!(parsed.summary, "");
        assert!(parsed.messages.is_empty());
    }

    #[test]
    fn parse_malformed_messages_default_empty() {
        // context.messages 不是合法 Message 数组 → 兜底空(不 panic)
        let mut p = full_payload();
        p["context"]["messages"] = json!("not an array");
        let parsed = parse_execute_turn_payload(&p).expect("不应因 messages 畸形失败");
        assert!(parsed.messages.is_empty());
    }

    #[test]
    fn parse_extracts_effective_policy() {
        // turn-begin payload 带 effective_policy → 解析为 Some(opaque JSON)
        let p = json!({
            "session_id": "task-1",
            "turn_id": 1,
            "input": {"user_input": ""},
            "effective_policy": {
                "fs": {"read_paths": [], "write_paths": []},
                "net": {"egress": []},
                "agent_role": "reader"
            }
        });
        let parsed = parse_execute_turn_payload(&p).unwrap();
        let eff = parsed.effective_policy.expect("effective_policy 应解析为 Some");
        assert_eq!(eff["agent_role"], "reader");
        assert!(eff["net"]["egress"].is_array());
    }

    #[test]
    fn parse_effective_policy_absent_is_none() {
        // 无 effective_policy 字段 → None(向后兼容旧 payload)
        let p = json!({ "session_id": "t", "turn_id": 1 });
        let parsed = parse_execute_turn_payload(&p).unwrap();
        assert!(parsed.effective_policy.is_none());
    }

    // serde_json::Map 没有 pub remove 便利,用 as_object_mut
    fn p_object_remove(v: &mut serde_json::Value, key: &str) {
        if let Some(obj) = v.as_object_mut() {
            obj.remove(key);
        }
    }

    // ── step-event payload builders(step-events Phase 3)─────────────

    #[test]
    fn llm_invoked_payload_shape() {
        let msgs = vec![fixus::Message { role: "user".into(), content: "hi".into() }];
        let p = llm_invoked_payload("t1", 5, "llm-s1", "claude-sonnet-5", &msgs, 1);
        assert_eq!(p["task_id"], "t1");
        assert_eq!(p["turn_id"], 5);
        assert_eq!(p["step_id"], "llm-s1");
        assert_eq!(p["model"], "claude-sonnet-5");
        assert_eq!(p["messages"][0]["role"], "user");
        assert_eq!(p["local_seq"], 1);
    }

    #[test]
    fn llm_completed_payload_shape_with_usage() {
        let p = llm_completed_payload("t1", 5, "llm-s1", "claude-sonnet-5", Some((100, 20, 120)), 2);
        assert_eq!(p["step_id"], "llm-s1");
        assert_eq!(p["input_tokens"], 100);
        assert_eq!(p["output_tokens"], 20);
        assert_eq!(p["total_tokens"], 120);
        assert_eq!(p["local_seq"], 2);
    }

    #[test]
    fn llm_completed_payload_shape_without_usage() {
        let p = llm_completed_payload("t1", 5, "llm-s1", "claude-sonnet-5", None, 2);
        assert_eq!(p["step_id"], "llm-s1");
        assert_eq!(p["input_tokens"], 0);
    }

    #[test]
    fn llm_failed_payload_shape() {
        let p = llm_failed_payload("t1", 5, "llm-s1", "agent_error", "boom", 3);
        assert_eq!(p["step_id"], "llm-s1");
        assert_eq!(p["error_type"], "agent_error");
        assert_eq!(p["error_message"], "boom");
        assert_eq!(p["local_seq"], 3);
    }
}
