//! sandbox-server v2 — broker GroupConsumer pull-worker (Plan D).
//!
//! 旧版是 passive HTTP server,新版是 consumer loop:
//!   1. join broker consumer group "sandboxes-<region>"
//!   2. consume_frames() → 接收 tools:region:<region> 流的 tool_invoked 事件
//!   3. executor 执行(幂等缓存按 idempotency_key 去重)
//!   4. HTTP POST 结果回 fixus
//!   5. commit_shard offset

mod executor;
mod landlock;
mod sandbox_core;
mod session;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use serde::Deserialize;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

use logdb_client::broker::GroupConsumer;
use logdb_broker_proto::pb::consume_response::Payload;

use crate::session::SessionManager;

// ── CLI ──

#[derive(Parser)]
#[command(name = "sandbox-server", version)]
struct Cli {
    /// broker 地址 (127.0.0.1:5100)
    #[arg(long, default_value = "127.0.0.1:5100")]
    broker_addr: String,

    /// logdbd namespace
    #[arg(long, default_value = "default")]
    namespace: String,

    /// 此 sandbox 服务的 region
    #[arg(long, default_value = "default")]
    region: String,

    /// fixus 的 HTTP 地址(用于 POST /api/v1/tools/result)
    #[arg(long, default_value = "http://127.0.0.1:3000")]
    fixus_url: String,

    /// session 目录
    #[arg(long, default_value = "/tmp/sandbox-sessions")]
    session_dir: PathBuf,

    /// consumer group 名称
    #[arg(long, default_value = "sandboxes")]
    group: String,
}

// ── Idempotence Cache ──

struct IdempotentCache {
    cache: RwLock<HashMap<String, ToolResult>>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ToolResult {
    success: bool,
    output: serde_json::Value,
    error: Option<String>,
    duration_ms: u64,
}

impl IdempotentCache {
    fn new() -> Self { Self { cache: RwLock::new(HashMap::new()) } }
    async fn get(&self, key: &str) -> Option<ToolResult> { self.cache.read().await.get(key).cloned() }
    async fn put(&self, key: String, result: ToolResult) { self.cache.write().await.insert(key, result); }
}

// ── Main ──

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let session_mgr = Arc::new(session::SessionManager::new(cli.session_dir.clone()));
    let cache = IdempotentCache::new();
    let stream = format!("tools:region:{}", cli.region);
    let consumer_id = format!("sandbox-{}", uuid::Uuid::now_v7().to_string().replace('-', ""));

    tracing::info!("sandbox pull-worker starting: broker={} stream={} group={} consumer={} fixus={}",
        cli.broker_addr, stream, cli.group, consumer_id, cli.fixus_url);

    loop {
        match run_consumer(&cli, &stream, &consumer_id, &session_mgr, &cache).await {
            Ok(()) => tracing::info!("consumer loop ended normally"),
            Err(e) => {
                tracing::error!("consumer error: {}; retrying in 1s", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn run_consumer(
    cli: &Cli,
    stream: &str,
    consumer_id: &str,
    session_mgr: &SessionManager,
    cache: &IdempotentCache,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("http://{}", cli.broker_addr);
    let mut consumer = GroupConsumer::join(addr, &cli.namespace, stream, &cli.group, consumer_id).await?;
    tracing::info!("joined group {} ({}), assigned shards: {:?}", cli.group, consumer_id, consumer.assigned_shards());

    let mut frames = consumer.consume_frames().await?;
    let fixus_client = reqwest::Client::new();

    while let Some(item) = frames.next().await {
        let frame = match item {
            Ok(f) => f,
            Err(e) => { tracing::error!("consume error: {}", e); continue; }
        };
        match frame.payload {
            Some(Payload::Record(rec)) => {
                if rec.event_type != "tool_invoked" { continue; }
                let payload: serde_json::Value = serde_json::from_slice(&rec.content).unwrap_or_default();
                let tool_name = payload["tool_name"].as_str().unwrap_or("?").to_string();
                let tool_call_id = payload["tool_call_id"].as_str().unwrap_or("?").to_string();
                let idempotency_key = payload["idempotency_key"].as_str().unwrap_or("?").to_string();
                let step_id = rec.metadata.get("step_id").cloned().unwrap_or_default();
                let task_id = rec.metadata.get("task_id").cloned().unwrap_or_default();

                // Idempotency check
                let result = if let Some(cached) = cache.get(&idempotency_key).await {
                    tracing::info!("cache hit: {}", idempotency_key);
                    cached
                } else {
                    // Execute
                    let t0 = std::time::Instant::now();
                    let work_dir = session_mgr.get_or_create(&step_id);
                    let tool_name_fix = tool_name.strip_prefix("fixus_").unwrap_or(&tool_name);
                    let (success, output, error) = match execute_tool(tool_name_fix, &payload, &work_dir).await {
                        Ok(o) => (true, o, None),
                        Err(e) => (false, serde_json::Value::Null, Some(e)),
                    };
                    let dur = t0.elapsed().as_millis() as u64;
                    let r = ToolResult { success, output, error, duration_ms: dur };
                    cache.put(idempotency_key.clone(), r.clone()).await;
                    r
                };

                // POST result to fixus
                let result_body = serde_json::json!({
                    "step_id": step_id, "task_id": task_id, "tool_call_id": tool_call_id,
                    "success": result.success, "output": result.output, "error": result.error, "duration_ms": result.duration_ms,
                });
                match fixus_client.post(format!("{}/api/v1/tools/result", cli.fixus_url)).json(&result_body).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            let _ = consumer.commit_shard(rec.shard_id, rec.seq).await;
                        } else {
                            tracing::error!("fixus rejected result: {}", resp.status());
                        }
                    }
                    Err(e) => tracing::error!("fixus POST failed: {}", e),
                }
            }
            Some(Payload::CaughtUp(_)) | Some(Payload::Rebalance(_)) | Some(Payload::Assignment(_)) => {}
            None => {}
        }
    }
    consumer.leave().await?;
    Ok(())
}

// ── Tool dispatch ──


async fn execute_tool(
    tool_name: &str,
    payload: &serde_json::Value,
    work_dir: &Path,
) -> Result<serde_json::Value, String> {
    let input = payload.get("input").cloned().unwrap_or(serde_json::Value::Null);
    let timeout_ms = payload.get("timeout_ms").and_then(|v| v.as_u64()).unwrap_or(120_000);
    let timeout = (timeout_ms / 1000).max(1).min(600); // 1-600s

    // MVP: fixus orchestrator 当前只发 Bash/Read/Write 等,统一转发给 executor
    let name = tool_name.strip_prefix("fixus_").unwrap_or(tool_name);
    let code = match name {
        "Bash" | "bash" => input.get("command").or(input.get("code")).and_then(|v| v.as_str()).unwrap_or("echo 'no command'").to_string(),
        "Read" | "read" | "Write" | "write" | "Edit" | "edit" | "Glob" | "glob" | "Grep" | "grep" => {
            // 这些工具在旧 sandbox 里是 fixus lib 实现的;当前 sandbox-server 独立运行
            // 暂时不支持(Bash only for MVP)
            return Err(format!("tool {} not supported (Bash only)", name));
        }
        _ => return Err(format!("unknown tool {}", name)),
    };

    let exec_result = crate::executor::execute(&code, work_dir, None, timeout).await.map_err(|e| format!("{}", e))?;
    Ok(serde_json::json!({
        "stdout": exec_result.stdout, "stderr": exec_result.stderr, "exit_code": exec_result.exit_code,
    }))
}

