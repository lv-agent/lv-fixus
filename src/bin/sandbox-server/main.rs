//! sandbox-server v2 — broker GroupConsumer pull-worker (Plan D).
//!
//! 旧版是 passive HTTP server,新版是 consumer loop:
//!   1. join broker consumer group "sandboxes-<region>"
//!   2. consume_frames() → 接收 tools-region-<region> 流的 tool_invoked 事件
//!   3. executor 执行(幂等缓存按 idempotency_key 去重)
//!   4. HTTP POST 结果回 fixus
//!   5. commit_shard offset

mod executor;
mod landlock;
mod sandbox_core;
mod session;
mod tools;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use tokio::sync::{RwLock, Semaphore};
use tokio_stream::StreamExt;

use logdb_client::broker::{BrokerProducer, GroupConsumer};
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
    let cache = Arc::new(IdempotentCache::new());
    let stream = format!("tools-region-{}", cli.region);
    let consumer_id = format!("sandbox-{}", uuid::Uuid::now_v7().to_string().replace('-', ""));

    tracing::info!("sandbox pull-worker starting: broker={} stream={} group={} consumer={}",
        cli.broker_addr, stream, cli.group, consumer_id);

    let result_stream = format!("tool-results-region-{}", cli.region);

    loop {
        let result_producer = match BrokerProducer::connect(format!("http://{}", cli.broker_addr)).await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("failed to connect result producer: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        match run_consumer(&cli, &stream, &result_stream, &consumer_id, &session_mgr, &cache, result_producer).await {
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
    result_stream: &str,
    consumer_id: &str,
    session_mgr: &SessionManager,
    cache: &Arc<IdempotentCache>,
    result_producer: BrokerProducer,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("http://{}", cli.broker_addr);
    let mut consumer = GroupConsumer::join(addr, &cli.namespace, stream, &cli.group, consumer_id).await?;
    tracing::info!("joined group {} ({}), assigned shards: {:?}", cli.group, consumer_id, consumer.assigned_shards());

    let mut frames = consumer.consume_frames().await?;

    // 并发控制:最多 4 个工具同时执行,通过 semaphore + spawn 实现
    let semaphore = Arc::new(Semaphore::new(4));
    let producer = Arc::new(tokio::sync::Mutex::new(result_producer));
    // commit 通过 mpsc 串行化回主循环(避免多线程争用 GroupConsumer)
    let (commit_tx, mut commit_rx) = tokio::sync::mpsc::unbounded_channel::<(u32, u64)>();

    let mut consecutive_errors: u32 = 0;
    let mut in_flight: usize = 0;

    while let Some(item) = frames.next().await {
        // 先处理已完成的 commit(非阻塞)
        while let Ok((shard_id, seq)) = commit_rx.try_recv() {
            let _ = consumer.commit_shard(shard_id, seq).await;
            in_flight = in_flight.saturating_sub(1);
        }

        let frame = match item {
            Ok(f) => {
                consecutive_errors = 0;
                f
            }
            Err(e) => {
                consecutive_errors += 1;
                tracing::error!("consume error (consecutive={}): {}", consecutive_errors, e);
                if consecutive_errors >= 3 {
                    return Err(format!("{} consecutive consume errors, rejoining", consecutive_errors).into());
                }
                continue;
            }
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
                let shard_id = rec.shard_id;
                let seq = rec.seq;

                // Idempotency check
                if let Some(cached) = cache.get(&idempotency_key).await {
                    tracing::info!("cache hit: {}", idempotency_key);
                    // 缓存命中:直接 produce + commit(无需 spawn)
                    let result_payload = serde_json::json!({
                        "step_id": step_id, "task_id": task_id, "tool_call_id": tool_call_id,
                        "tool_name": tool_name,
                        "success": cached.success, "output": cached.output, "error": cached.error, "duration_ms": cached.duration_ms,
                    });
                    let content = serde_json::to_vec(&result_payload).unwrap_or_default();
                    let mut meta = HashMap::new();
                    meta.insert("step_id".into(), step_id.clone());
                    meta.insert("task_id".into(), task_id.clone());
                    meta.insert("event_type".into(), "tool_result".into());
                    let mut p = producer.lock().await;
                    if let Err(e) = p.produce_full(&cli.namespace, result_stream, "tool_result", &content, Some(&step_id), 0, "application/json", &meta).await {
                        tracing::error!("broker produce result failed: {}", e);
                    }
                    let _ = consumer.commit_shard(shard_id, seq).await;
                    continue;
                }

                // Spawn 并发执行:acquire semaphore → execute → produce → commit
                let sem = semaphore.clone();
                let prod = producer.clone();
                let tx = commit_tx.clone();
                let ns = cli.namespace.clone();
                let rs = result_stream.to_string();
                let cache_clone = cache.clone();
                let work_dir = payload
                    .get("work_dir")
                    .and_then(|v| v.as_str())
                    .map(PathBuf::from)
                    .unwrap_or_else(|| session_mgr.get_or_create(&step_id));
                let tool_name_fix = tool_name.strip_prefix("fixus_").unwrap_or(&tool_name).to_string();

                in_flight += 1;
                tokio::spawn(async move {
                    let _permit = sem.acquire().await.expect("semaphore closed");
                    let t0 = std::time::Instant::now();

                    let (success, output, error) = match execute_tool(&tool_name_fix, &payload, &work_dir).await {
                        Ok(o) => (true, o, None),
                        Err(e) => (false, serde_json::Value::Null, Some(e)),
                    };
                    let dur = t0.elapsed().as_millis() as u64;
                    tracing::info!(
                        "executed {} in work_dir={} success={} exit_code={} duration_ms={} error={:?}",
                        tool_name_fix, work_dir.display(), success,
                        output.get("exit_code").and_then(|v| v.as_i64()).unwrap_or(-1),
                        dur, error
                    );

                    let r = ToolResult { success, output, error, duration_ms: dur };
                    cache_clone.put(idempotency_key.clone(), r.clone()).await;

                    // Produce result to broker
                    let result_payload = serde_json::json!({
                        "step_id": step_id, "task_id": task_id, "tool_call_id": tool_call_id,
                        "tool_name": tool_name,
                        "success": r.success, "output": r.output, "error": r.error, "duration_ms": r.duration_ms,
                    });
                    let content = serde_json::to_vec(&result_payload).unwrap_or_default();
                    let mut meta = HashMap::new();
                    meta.insert("step_id".into(), step_id.clone());
                    meta.insert("task_id".into(), task_id.clone());
                    meta.insert("event_type".into(), "tool_result".into());

                    let mut p = prod.lock().await;
                    if let Err(e) = p.produce_full(&ns, &rs, "tool_result", &content, Some(&step_id), 0, "application/json", &meta).await {
                        tracing::error!("broker produce result failed: {}", e);
                    }
                    drop(p);

                    // 通知主循环 commit offset
                    let _ = tx.send((shard_id, seq));
                });
            }
            Some(Payload::CaughtUp(_)) | Some(Payload::Rebalance(_)) | Some(Payload::Assignment(_)) => {}
            None => {}
        }
    }

    // Drain remaining commits and in-flight tasks
    drop(commit_tx);
    while let Some((shard_id, seq)) = commit_rx.recv().await {
        let _ = consumer.commit_shard(shard_id, seq).await;
        in_flight = in_flight.saturating_sub(1);
    }
    tracing::info!("all tasks completed, leaving consumer group");
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
    let timeout_secs = (timeout_ms / 1000).max(1).min(600); // 1-600s
    let timeout_dur = std::time::Duration::from_secs(timeout_secs);

    // fixus_ 前缀由 sandbox-server 统一 strip
    let name = tool_name.strip_prefix("fixus_").unwrap_or(tool_name);
    match name {
        // Bash 走 executor(Landlock 子进程 + ulimit)
        "Bash" | "bash" => {
            let code = input
                .get("command")
                .or(input.get("code"))
                .and_then(|v| v.as_str())
                .unwrap_or("echo 'no command'")
                .to_string();
            let exec_result = crate::executor::execute(&code, work_dir, None, timeout_secs)
                .await
                .map_err(|e| format!("{}", e))?;
            Ok(serde_json::json!({
                "stdout": exec_result.stdout, "stderr": exec_result.stderr, "exit_code": exec_result.exit_code,
            }))
        }
        // 文件工具(sandbox-server 进程内,路径校验限定 work_dir)
        "Read" | "read" => crate::tools::execute_read(&input, work_dir).await,
        "Write" | "write" => crate::tools::execute_write(&input, work_dir).await,
        "Edit" | "edit" => crate::tools::execute_edit(&input, work_dir).await,
        "Glob" | "glob" => crate::tools::execute_glob(&input, work_dir).await,
        "Grep" | "grep" => crate::tools::execute_grep(&input, work_dir, timeout_dur).await,
        _ => Err(format!("unknown tool {}", name)),
    }
}

