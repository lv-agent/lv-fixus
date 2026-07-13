//! tools-bank — 独立工具注册中心与代理
//!
//! 职责:
//! - MCP server: tools/list (工具目录) + tools/call (工具调用)
//! - Tool registry: 内置 6 工具 + 可扩展注册 API
//! - Broker dispatch: tools/call → broker produce → 等 sandbox 结果 → MCP response
//!
//! 不依赖 fixus。只与 broker 对话。

use std::collections::HashMap;
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::Json, routing::{get, post}, Router};
use clap::Parser;
use logdb_client::broker::{BrokerProducer, GroupConsumer};
use logdb_broker_proto::pb::consume_response::Payload as ConsumePayload;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex};
use tokio_stream::StreamExt;
use uuid::Uuid;

// ── CLI ──

#[derive(Parser)]
#[command(name = "tools-bank", version)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:5100")]
    broker_addr: String,

    #[arg(long, default_value = "default")]
    namespace: String,

    #[arg(long, default_value = "default")]
    region: String,

    #[arg(long, default_value = "3001")]
    port: u16,
}

// ── Tool Registry ──

#[derive(Debug, Clone, Serialize)]
struct ToolDef {
    name: String,
    description: String,
    #[serde(rename = "inputSchema")]
    input_schema: serde_json::Value,
}

struct ToolRegistry {
    tools: Vec<ToolDef>,
}

impl ToolRegistry {
    fn with_builtins() -> Self {
        Self {
            tools: vec![
                ToolDef {
                    name: "fixus_bash".into(),
                    description: "Execute a shell command (via fixus sandbox)".into(),
                    input_schema: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "command": {"type": "string", "description": "The command to execute"},
                            "description": {"type": "string", "description": "Brief description"}
                        },
                        "required": ["command"]
                    }),
                },
                ToolDef {
                    name: "fixus_read".into(),
                    description: "Read a file (via fixus sandbox)".into(),
                    input_schema: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string", "description": "Path to file (relative or absolute, scoped to workspace)"},
                            "offset": {"type": "integer", "description": "Line number to start reading from"},
                            "limit": {"type": "integer", "description": "Number of lines to read"}
                        },
                        "required": ["file_path"]
                    }),
                },
                ToolDef {
                    name: "fixus_write".into(),
                    description: "Write to a file (via fixus sandbox)".into(),
                    input_schema: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string", "description": "Path to file (relative or absolute, scoped to workspace)"},
                            "content": {"type": "string", "description": "Content to write"}
                        },
                        "required": ["file_path", "content"]
                    }),
                },
                ToolDef {
                    name: "fixus_edit".into(),
                    description: "Edit a file by replacing a string (via fixus sandbox)".into(),
                    input_schema: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string", "description": "Path to file (relative or absolute, scoped to workspace)"},
                            "old_string": {"type": "string", "description": "String to replace"},
                            "new_string": {"type": "string", "description": "Replacement string"}
                        },
                        "required": ["file_path", "old_string", "new_string"]
                    }),
                },
                ToolDef {
                    name: "fixus_glob".into(),
                    description: "Find files matching a pattern (via fixus sandbox)".into(),
                    input_schema: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "pattern": {"type": "string", "description": "Glob pattern (e.g. *.rs)"}
                        },
                        "required": ["pattern"]
                    }),
                },
                ToolDef {
                    name: "fixus_grep".into(),
                    description: "Search for a pattern in files (via fixus sandbox)".into(),
                    input_schema: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "pattern": {"type": "string", "description": "Pattern to search for"},
                            "path": {"type": "string", "description": "Directory or file to search in"}
                        },
                        "required": ["pattern"]
                    }),
                },
            ],
        }
    }

    fn list(&self) -> &[ToolDef] { &self.tools }
}

// ── MCP types ──

#[derive(Debug, Deserialize)]
struct McpRequest {
    jsonrpc: String,
    id: Option<i64>,
    method: String,
    #[serde(default)]
    params: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct McpResponse {
    jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<McpErrorBody>,
}

#[derive(Debug, Serialize)]
struct McpErrorBody {
    code: i64,
    message: String,
}

// ── Tool result ──

#[derive(Debug)]
struct PendingToolResult {
    success: bool,
    output: serde_json::Value,
    error: Option<String>,
    duration_ms: u64,
}

// ── App state ──

struct AppState {
    registry: ToolRegistry,
    producer: Arc<Mutex<BrokerProducer>>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<PendingToolResult>>>>,
    namespace: String,
    region: String,
}

// ── Idempotency helpers ──

fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let items: Vec<String> = keys.iter().map(|k| {
                format!("{}:{}", serde_json::to_string(k).unwrap(), canonical_json(&map[*k]))
            }).collect();
            format!("{{{}}}", items.join(","))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(canonical_json).collect();
            format!("[{}]", items.join(","))
        }
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".into()),
    }
}

fn build_key(task_id: &str, tool_name: &str, args: &serde_json::Value) -> String {
    use sha2::{Digest, Sha256};
    let canonical = canonical_json(args);
    let hash = hex::encode(Sha256::digest(canonical.as_bytes()).as_slice());
    format!("{}:bank:{}:{}", task_id, tool_name, &hash[..16])
}

fn default_timeout(tool_name: &str) -> u64 {
    let name = tool_name.strip_prefix("fixus_").unwrap_or(tool_name).to_lowercase();
    match name.as_str() {
        "bash" => 30,
        "read" | "write" | "edit" | "glob" | "grep" => 15,
        _ => 30,
    }
}

// ── MCP handlers ──

fn mcp_ok(id: Option<i64>, result: serde_json::Value) -> McpResponse {
    McpResponse { jsonrpc: "2.0".into(), id, result: Some(result), error: None }
}

fn mcp_err(id: Option<i64>, code: i64, message: &str) -> McpResponse {
    McpResponse { jsonrpc: "2.0".into(), id, result: None, error: Some(McpErrorBody { code, message: message.to_string() }) }
}

fn tools_list(registry: &ToolRegistry, id: Option<i64>) -> McpResponse {
    mcp_ok(id, serde_json::json!({ "tools": registry.list() }))
}

async fn tools_call(
    state: &AppState,
    task_id: &str,
    tool_name: &str,
    args: &serde_json::Value,
    id: Option<i64>,
) -> McpResponse {
    let step_id = Uuid::now_v7().to_string();
    let tool_call_id = Uuid::now_v7().to_string();
    let idempotency_key = build_key(task_id, tool_name, args);
    let timeout_secs = default_timeout(tool_name);
    let sandbox_timeout_ms = timeout_secs.saturating_sub(5).max(5) * 1000;
    let stream = format!("tool-invoke-{}", state.region);

    // work_dir 不再由 tools-bank 决定:传 task_id 作 session_id,
    // sandbox-server 据此 get_or_create → base_dir/{task_id} 做 per-task 隔离。
    let payload = serde_json::json!({
        "step_type": "tool_call",
        "tool_name": tool_name,
        "tool_call_id": tool_call_id,
        "idempotency_key": idempotency_key,
        "input": args,
        "local_seq": 0,
        "session_id": task_id,
        "timeout_ms": sandbox_timeout_ms,
    });

    tracing::info!("tools-bank: dispatching {} step_id={}", tool_name, step_id);

    // Register oneshot BEFORE produce (race fix)
    let (tx, mut rx) = oneshot::channel::<PendingToolResult>();
    state.pending.lock().await.insert(step_id.clone(), tx);

    // Produce to broker
    let content = serde_json::to_vec(&payload).unwrap_or_default();
    let mut meta = HashMap::new();
    meta.insert("task_id".into(), task_id.to_string());
    meta.insert("step_id".into(), step_id.clone());
    meta.insert("event_type".into(), "tool_invoked".into());

    let mut prod = state.producer.lock().await;
    if let Err(e) = prod.produce_full(
        &state.namespace, &stream, "tool_invoked", &content,
        Some(task_id), 0, "application/json", &meta,
    ).await {
        state.pending.lock().await.remove(&step_id);
        return mcp_err(id, -32603, &format!("broker produce failed: {}", e));
    }
    drop(prod);

    // Wait for sandbox result
    let timeout_dur = std::time::Duration::from_secs(timeout_secs);
    match tokio::time::timeout(timeout_dur, &mut rx).await {
        Ok(Ok(r)) => {
            let text = serde_json::to_string(&r.output).unwrap_or_default();
            mcp_ok(id, serde_json::json!({
                "content": [{"type": "text", "text": text}],
                "isError": !r.success,
                "_meta": {"duration_ms": r.duration_ms}
            }))
        }
        Ok(Err(_)) => {
            state.pending.lock().await.remove(&step_id);
            mcp_err(id, -32603, "pending channel closed")
        }
        Err(_) => {
            state.pending.lock().await.remove(&step_id);
            mcp_err(id, -32603, &format!("sandbox timeout after {}s", timeout_secs))
        }
    }
}

async fn handle_mcp(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<McpRequest>,
) -> Result<Json<McpResponse>, (StatusCode, Json<McpResponse>)> {
    let sid = headers.get("X-Fixus-Session-Id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("(none)");
    tracing::debug!("MCP request: method={} id={:?} session={}", body.method, body.id, sid);

    match body.method.as_str() {
        "initialize" => {
            let resp = mcp_ok(body.id, serde_json::json!({
                "protocolVersion": "2024-11-05",
                "serverInfo": {"name": "tools-bank", "version": "0.1.0"},
                "capabilities": {"tools": {}}
            }));
            Ok(Json(resp))
        }
        "tools/list" => {
            Ok(Json(tools_list(&state.registry, body.id)))
        }
        "tools/call" => {
            let params = body.params.as_ref();
            let tool_name = params.and_then(|p| p.get("name")).and_then(|v| v.as_str()).unwrap_or("?");
            let task_id = headers.get("X-Fixus-Session-Id")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown");
            let args = params.and_then(|p| p.get("arguments")).cloned().unwrap_or(serde_json::Value::Null);

            tracing::info!("tools-bank: tools/call task={} tool={}", task_id, tool_name);
            Ok(Json(tools_call(&state, task_id, tool_name, &args, body.id).await))
        }
        _ => {
            Ok(Json(mcp_err(body.id, -32601, &format!("unknown method: {}", body.method))))
        }
    }
}

// ── Result consumer ──

async fn run_result_consumer(
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<PendingToolResult>>>>,
    broker_addr: &str,
    namespace: &str,
    region: &str,
) {
    let stream = format!("tool-result-{}", region);
    let instance_id = Uuid::now_v7().to_string().replace('-', "");
    let group = format!("tools-bank-results-{}", &instance_id[..12]);
    let consumer_id = format!("tools-bank-{}", instance_id);

    tracing::info!("result consumer: broker={} stream={} group={}", broker_addr, stream, group);

    loop {
        match try_consume_results(&pending, broker_addr, namespace, &stream, &group, &consumer_id).await {
            Ok(()) => tracing::info!("result consumer ended"),
            Err(e) => {
                tracing::error!("result consumer error: {}; retrying in 1s", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn try_consume_results(
    pending: &Arc<Mutex<HashMap<String, oneshot::Sender<PendingToolResult>>>>,
    broker_addr: &str,
    namespace: &str,
    stream: &str,
    group: &str,
    consumer_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut consumer = GroupConsumer::join(
        format!("http://{}", broker_addr),
        namespace, stream, group, consumer_id,
    ).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    tracing::info!("result consumer joined {} ({}), shards: {:?}", group, consumer_id, consumer.assigned_shards());

    let mut frames = consumer.consume_frames().await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    let mut consecutive_errors: u32 = 0;
    while let Some(item) = frames.next().await {
        let frame = match item {
            Ok(f) => { consecutive_errors = 0; f }
            Err(e) => {
                consecutive_errors += 1;
                if consecutive_errors >= 3 {
                    return Err(format!("{} consecutive errors, rejoining", consecutive_errors).into());
                }
                continue;
            }
        };
        match frame.payload {
            Some(ConsumePayload::Record(rec)) => {
                if rec.event_type != "tool_result" { continue; }
                let result: serde_json::Value = serde_json::from_slice(&rec.content).unwrap_or_default();
                let step_id = result["step_id"].as_str().unwrap_or("");
                if step_id.is_empty() { continue; }

                let pr = PendingToolResult {
                    success: result["success"].as_bool().unwrap_or(false),
                    output: result["output"].clone(),
                    error: result["error"].as_str().map(|s| s.to_string()),
                    duration_ms: result["duration_ms"].as_u64().unwrap_or(0),
                };

                if let Some(tx) = pending.lock().await.remove(step_id) {
                    let _ = tx.send(pr);
                }
                let _ = consumer.commit_shard(rec.shard_id, rec.seq).await;
            }
            _ => {}
        }
    }
    consumer.leave().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    Ok(())
}

// ── Main ──

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "tools_bank=info".into()))
        .init();

    let cli = Cli::parse();

    let producer = BrokerProducer::connect(format!("http://{}", cli.broker_addr)).await
        .expect("broker producer connect");

    let state = Arc::new(AppState {
        registry: ToolRegistry::with_builtins(),
        producer: Arc::new(Mutex::new(producer)),
        pending: Arc::new(Mutex::new(HashMap::new())),
        namespace: cli.namespace.clone(),
        region: cli.region.clone(),
    });

    // Background result consumer
    let pending = state.pending.clone();
    let broker_addr = cli.broker_addr.clone();
    let namespace = cli.namespace.clone();
    let region = cli.region.clone();
    tokio::spawn(async move {
        run_result_consumer(pending, &broker_addr, &namespace, &region).await;
    });

    // MCP HTTP server
    let app = Router::new()
        .route("/mcp", post(handle_mcp))
        .route("/health", get(|| async { Json(serde_json::json!({"status":"ok"})) }))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cli.port);
    tracing::info!("tools-bank starting on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
}