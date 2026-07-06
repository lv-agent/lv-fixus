//! fixus-stream — SSE 流式网关(logdbd 事件流 + Redis token 流 fan-in)
//!
//! 两条通道合并转 SSE 推给客户端:
//! - **事件级**(turn/llm/tool 生命周期):logdbd `Subscribe` RPC
//! - **token 级**(`llm_chunk`,逐字流式):Redis `SUBSCRIBE`(ephemeral 快路径,
//!   token 频率太高不入 append-only 事件库)。REDIS_URL 未设置时该通道关闭。
//!
//! ## 环境变量
//! - `LOGDBD_ADDR` — logdbd gRPC 地址(默认 127.0.0.1:50051)
//! - `LOGDBD_NAMESPACE` — logdbd namespace(默认 "default")
//! - `REDIS_URL` — Redis 地址(可选;未设置则无 token 逐字流式)
//! - `PORT` — 监听端口(默认 8081)

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    response::sse::{Event, Sse},
    routing::get,
    Router,
};
use futures_util::stream::Stream;
use futures_util::StreamExt;
use logdb_client::Client;
use tokio::sync::{mpsc, Mutex};

// ── 应用状态 ──

struct AppState {
    client: Arc<Mutex<Client>>,
    namespace: String,
    /// Redis 地址;Some 时启用 token 逐字流式
    redis_url: Option<String>,
}

// ── Terminal event types(收到即结束 SSE)──

const TERMINAL_EVENTS: &[&str] = &[
    "turn_completed", "turn_failed", "turn_canceled", "turn_blocked",
];

fn is_terminal(et: &str) -> bool {
    TERMINAL_EVENTS.contains(&et)
}

// ── Subscribe 的事件类型(Turn 级 + Step 级)──

const SUBSCRIBE_EVENT_TYPES: &[&str] = &[
    "turn_started", "turn_completed", "turn_failed", "turn_canceled", "turn_blocked",
    "llm_invoked", "llm_completed", "llm_failed",
    "tool_invoked", "tool_completed", "tool_failed",
];

// ── Token 订阅循环(Redis fan-in)──

/// 订阅 Redis `turn:{session}:{turn}` 通道,把每个 token chunk 作为 `llm_chunk`
/// SSE 事件转发。连不上 Redis / 客户端断连时退出。
async fn token_loop(
    redis_url: String,
    session_id: String,
    turn_id: i64,
    tx: mpsc::Sender<Result<Event, axum::Error>>,
) {
    let client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("token_loop: redis open failed: {}", e);
            return;
        }
    };
    let mut pubsub = match client.get_async_pubsub().await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("token_loop: redis pubsub connect failed: {}", e);
            return;
        }
    };
    let channel = format!("turn:{}:{}", session_id, turn_id);
    if let Err(e) = pubsub.subscribe(&channel).await {
        tracing::warn!("token_loop: subscribe {} failed: {}", channel, e);
        return;
    }
    tracing::debug!("token_loop: subscribed {}", channel);

    let mut msg_stream = pubsub.into_on_message();
    while let Some(msg) = msg_stream.next().await {
        let payload: String = msg.get_payload().unwrap_or_default();
        if tx
            .send(Ok(Event::default().event("llm_chunk").data(payload)))
            .await
            .is_err()
        {
            return; // SSE 客户端断连
        }
    }
}

// ── SSE 端点 ──

async fn stream_handler(
    State(state): State<Arc<AppState>>,
    Path((session_id, turn_id)): Path<(String, i64)>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let (tx, rx) = mpsc::channel::<Result<Event, axum::Error>>(256);
    let client = state.client.clone();
    let namespace = state.namespace.clone();
    let redis_url = state.redis_url.clone();

    tokio::spawn(async move {
        let consumer_id = uuid::Uuid::now_v7().to_string();

        // logdbd Subscribe — 事件级流式
        let mut stream = {
            let mut c = client.lock().await;
            match c
                .subscribe(
                    &namespace,
                    &session_id,
                    SUBSCRIBE_EVENT_TYPES.iter().map(|s| s.to_string()).collect(),
                    "fixus-stream",
                    &consumer_id,
                )
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("subscribe failed: {}", e);
                    let _ = tx
                        .send(Ok(Event::default().event("error").data(
                            serde_json::json!({"error": e.to_string()}).to_string(),
                        )))
                        .await;
                    return;
                }
            }
        };

        tracing::debug!(
            "session={} turn={} consumer={}: subscribed",
            session_id,
            turn_id,
            consumer_id
        );

        // Redis token 订阅 — fan-in(若配置了 REDIS_URL)
        let token_task = redis_url.as_ref().map(|url| {
            let tx2 = tx.clone();
            tokio::spawn(token_loop(url.clone(), session_id.clone(), turn_id, tx2))
        });

        // 事件循环:消费 logdbd Record,过滤 turn_id,转 SSE
        let mut sent_done = false;
        while let Some(record) = stream.next().await {
            let rec = match record {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("subscribe stream error: {}", e);
                    break;
                }
            };

            let rec_turn_id: Option<i64> = rec
                .metadata
                .get("turn_id")
                .and_then(|s| s.parse().ok());
            if rec_turn_id != Some(turn_id) {
                continue;
            }

            let payload_str = String::from_utf8_lossy(&rec.content).to_string();
            if tx
                .send(Ok(Event::default().event(&rec.event_type).data(&payload_str)))
                .await
                .is_err()
            {
                break; // 客户端断连
            }

            // Terminal 事件 → 发 done 并结束
            if is_terminal(&rec.event_type) {
                let _ = tx
                    .send(Ok(Event::default().event("done").data("{}")))
                    .await;
                sent_done = true;
                break;
            }
        }

        // Turn 结束(或 logdbd 流断开)→ 停 token 订阅
        if let Some(h) = token_task {
            h.abort();
        }
        if !sent_done {
            let _ = tx
                .send(Ok(Event::default().event("done").data("{}")))
                .await;
        }
    });

    Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx))
}

// ── 健康检查 ──

async fn health() -> &'static str {
    "ok"
}

// ── 主函数 ──

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr = std::env::var("LOGDBD_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".into());
    let namespace = std::env::var("LOGDBD_NAMESPACE").unwrap_or_else(|_| "default".into());
    let redis_url = std::env::var("REDIS_URL")
        .ok()
        .filter(|s| !s.is_empty());
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8081);

    let client = Client::connect(&addr).await?;
    tracing::info!("Connected to logdbd at {}", addr);
    if redis_url.is_some() {
        tracing::info!("Token streaming enabled (Redis)");
    } else {
        tracing::info!("REDIS_URL not set — token streaming disabled (event-level only)");
    }

    let state = Arc::new(AppState {
        client: Arc::new(Mutex::new(client)),
        namespace,
        redis_url,
    });

    let app = Router::new()
        .route(
            "/sessions/{session_id}/turns/{turn_id}/stream",
            get(stream_handler),
        )
        .route("/health", get(health))
        .with_state(state);

    let bind_addr = format!("0.0.0.0:{}", port);
    tracing::info!("fixus-stream listening on {}", bind_addr);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
