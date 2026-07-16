//! tools-bank — 独立工具注册中心与代理
//!
//! 职责:
//! - MCP server: tools/list (工具目录) + tools/call (工具调用)
//! - Tool registry: 多源分发器(CR-11)—— sandbox builtins + 外部 HTTP adapter
//! - Broker dispatch: builtin 工具 → broker produce → 等 sandbox 结果 → MCP response
//!
//! 不依赖 fixus。只与 broker 对话。

mod adapter;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use adapter::{
    build_key, parse_http_adapters, CallCtx, HttpActionAdapter, InvokeError,
    PendingMap, SandboxAdapter, ToolRegistry,
};
use axum::{
    extract::State, http::StatusCode, response::Json, routing::{get, post}, Router,
};
use clap::Parser;
use logdb_client::broker::{BrokerProducer, GroupConsumer};
use logdb_broker_proto::pb::consume_response::Payload as ConsumePayload;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
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

    /// operator extras catalog file (shared with sandbox-server); tools beyond
    /// the 8 builtins. Missing/unreadable → builtins only (not fatal); malformed
    /// → log + builtins only.
    #[arg(long)]
    extra_tools: Option<PathBuf>,
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

// ── App state ──

struct AppState {
    registry: ToolRegistry,
    /// 共享 broker producer:tools_call 顶层发 task-end 步事件对,
    /// SandboxAdapter 发 tool-invoke→sandbox。两者复用同一 Arc<Mutex>。
    task_end_producer: Arc<Mutex<BrokerProducer>>,
    /// task-end 步事件 produce 的 namespace(= cli.namespace)。
    lifecycle_namespace: String,
    /// per-process 单调步事件序号(task-end local_seq)。
    seq: AtomicU64,
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
    turn_id: Option<i64>,
    tool_name: &str,
    args: &serde_json::Value,
    effective_policy: Option<serde_json::Value>,
    id: Option<i64>,
) -> McpResponse {
    let idempotency_key = build_key(task_id, tool_name, args);
    let step_id = uuid::Uuid::now_v7().to_string();
    let ctx = CallCtx {
        task_id: task_id.to_string(),
        idempotency_key,
        effective_policy,
        step_id,
        turn_id,
    };

    tracing::info!("tools-bank: tools/call task={} tool={} step={}", task_id, tool_name, ctx.step_id);

    match state.registry.invoke(tool_name, args, &ctx).await {
        Ok(r) => {
            // success 时回灌 output;失败时把 error 语义拼进 text(让 agent 看到原因)。
            let text = if !r.success {
                match &r.error {
                    Some(e) if r.output.is_null() => e.clone(),
                    Some(e) => format!("{}: {}", e, serde_json::to_string(&r.output).unwrap_or_default()),
                    None => serde_json::to_string(&r.output).unwrap_or_default(),
                }
            } else {
                serde_json::to_string(&r.output).unwrap_or_default()
            };
            mcp_ok(id, serde_json::json!({
                "content": [{"type": "text", "text": text}],
                "isError": !r.success,
                "_meta": {"duration_ms": r.duration_ms}
            }))
        }
        Err(InvokeError::NotFound) => {
            mcp_err(id, -32602, &format!("unknown tool: {}", tool_name))
        }
        Err(InvokeError::Adapter(msg)) => {
            mcp_err(id, -32603, &msg)
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
            // X-Fixus-Turn-Id:fixlet 按轮注入;直接 MCP 调用(无 turn)→ None。
            let turn_id = headers.get("X-Fixus-Turn-Id")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<i64>().ok());
            // X-Fixus-Policy:fixlet 注入的 effective policy JSON 字符串 → opaque Value。
            // 缺 header 或非法 JSON → None(sandbox 侧 fail-closed 严默认)。
            let effective_policy = headers.get("X-Fixus-Policy")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
            let args = params.and_then(|p| p.get("arguments")).cloned().unwrap_or(serde_json::Value::Null);

            Ok(Json(tools_call(&state, task_id, turn_id, tool_name, &args, effective_policy, body.id).await))
        }
        _ => {
            Ok(Json(mcp_err(body.id, -32601, &format!("unknown method: {}", body.method))))
        }
    }
}

// ── Result consumer ──

async fn run_result_consumer(
    pending: PendingMap,
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
    pending: &PendingMap,
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
            Err(_) => {
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

                let pr = adapter::PendingToolResult {
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

// ── 构造 registry:sandbox builtins + operator extras + env 外部 HTTP adapter ──

fn build_registry(
    producer: Arc<Mutex<BrokerProducer>>,
    pending: PendingMap,
    namespace: String,
    region: String,
    extras: Vec<fixus_tool_catalog::ToolSpec>,
) -> ToolRegistry {
    let mut registry = ToolRegistry::new();

    let sandbox = SandboxAdapter {
        producer: producer.clone(),
        pending: pending.clone(),
        namespace: namespace.clone(),
        region: region.clone(),
        extras,
    };
    registry
        .register(Box::new(sandbox))
        .expect("register sandbox adapter");

    // 外部 HTTP adapter(env TOOLS_BANK_HTTP_ADAPTERS);撞名记 warn 跳过。
    let http_env = std::env::var("TOOLS_BANK_HTTP_ADAPTERS").unwrap_or_default();
    for cfg in parse_http_adapters(&http_env) {
        let adapter_name = cfg.name.clone();
        let tool_count = cfg.tools.len();
        let timeout_secs = cfg.timeout_secs;
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(
                    "skip http adapter {}: client build failed: {}",
                    adapter_name,
                    e
                );
                continue;
            }
        };
        match registry.register(Box::new(HttpActionAdapter { cfg, client })) {
            Ok(()) => tracing::info!(
                "registered http adapter `{}`: {} tool(s), timeout={}s",
                adapter_name,
                tool_count,
                timeout_secs
            ),
            Err(dup) => tracing::warn!(
                "skip http adapter `{}`: tool `{}` already owned by `{}`",
                adapter_name,
                dup.tool,
                dup.existing_adapter
            ),
        }
    }

    tracing::info!(
        "registry: {} adapter(s), {} tool(s)",
        registry.adapter_count(),
        registry.list().len()
    );
    registry
}

// ── Main ──

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "tools_bank=info".into()))
        .init();

    let cli = Cli::parse();

    let producer = Arc::new(tokio::sync::Mutex::new(
        BrokerProducer::connect(format!("http://{}", cli.broker_addr)).await
            .expect("broker producer connect"),
    ));

    // pending 共享:sandbox adapter(注册工具时) + result consumer(回灌结果)
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));

    // Load operator extras catalog (shared with sandbox-server). CLI flag 优先于
    // FIXUS_TOOLS_CATALOG_FILE env;missing/unreadable → builtins only(not fatal);
    // malformed → log + builtins only(spec §7:extras 永不让一个坏 catalog 打挂 tools-bank)。
    let extra_path = cli.extra_tools.clone()
        .or_else(|| std::env::var("FIXUS_TOOLS_CATALOG_FILE").ok().map(PathBuf::from));
    let extras: Vec<fixus_tool_catalog::ToolSpec> = match extra_path.as_ref().and_then(|p| std::fs::read_to_string(p).ok()) {
        Some(s) => match fixus_tool_catalog::parse_extra_catalog(&s) {
            Ok(v) => { tracing::info!("loaded {} extra tool(s) from {:?}", v.len(), extra_path); v }
            Err(e) => { tracing::error!("extra-tools parse failed ({:?}): {}; continuing with builtins only", extra_path, e); vec![] }
        },
        None => vec![],
    };
    // Drop extras colliding with a builtin name (spec §6.3: warn + skip the extra,
    // keep builtins + other extras). ToolRegistry's cross-adapter dup-check can't
    // see an intra-batch duplicate (builtins ∪ extras arrive as one adapter batch).
    let before = extras.len();
    let extras = fixus_tool_catalog::filter_collisions(&extras, fixus_tool_catalog::builtins());
    if extras.len() < before {
        tracing::warn!(
            "skipped {} extra tool(s) whose name collides with a builtin",
            before - extras.len()
        );
    }
    tracing::info!(
        "tool catalog for sandbox adapter: {} total ({} builtins + {} extras)",
        fixus_tool_catalog::builtins().len() + extras.len(),
        fixus_tool_catalog::builtins().len(),
        extras.len()
    );

    let registry = build_registry(
        producer.clone(),
        pending.clone(),
        cli.namespace.clone(),
        cli.region.clone(),
        extras,
    );

    let state = Arc::new(AppState {
        registry,
        task_end_producer: producer.clone(),
        lifecycle_namespace: cli.namespace.clone(),
        seq: AtomicU64::new(0),
    });

    // Background result consumer
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
