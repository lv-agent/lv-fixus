//! fixus-stream — logdbd Subscribe → SSE 流式网关
//!
//! 通过 logdbd Subscribe RPC 获取实时事件流，转为 SSE 推给客户端。
//! Subscribe 自动处理历史回放 + 实时推送 + consumer offset 追踪。
//!
//! ## 环境变量
//! - `LOGDBD_ADDR` — logdbd gRPC 地址（默认 127.0.0.1:50051）
//! - `LOGDBD_NAMESPACE` — logdbd namespace（默认 "default"）
//! - `PORT` — 监听端口（默认 8081）

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
}

// ── Terminal event types ──

const TERMINAL_EVENTS: &[&str] = &[
    "turn_completed", "turn_failed", "turn_canceled", "turn_blocked",
];

fn is_terminal(et: &str) -> bool {
    TERMINAL_EVENTS.contains(&et)
}

// ── Subscribe 的事件类型（Turn 级 + Step 级） ──

const SUBSCRIBE_EVENT_TYPES: &[&str] = &[
    "turn_started", "turn_completed", "turn_failed", "turn_canceled", "turn_blocked",
    "llm_invoked", "llm_completed", "llm_failed",
    "tool_invoked", "tool_completed", "tool_failed",
];

// ── SSE 端点 ──

async fn stream_handler(
    State(state): State<Arc<AppState>>,
    Path((session_id, turn_id)): Path<(String, i64)>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let (tx, rx) = mpsc::channel::<Result<Event, axum::Error>>(256);
    let client = state.client.clone();
    let namespace = state.namespace.clone();

    tokio::spawn(async move {
        let consumer_id = uuid::Uuid::now_v7().to_string();

        // Subscribe — logdbd 自动处理历史回放 + 实时推送
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

        // 消费 Record 流，过滤 turn_id，转 SSE
        while let Some(record) = stream.next().await {
            let rec = match record {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("subscribe stream error: {}", e);
                    break;
                }
            };

            // 过滤：只推送目标 turn 的事件
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
                return; // 客户端断连
            }

            // Terminal 事件 → 发送 done 并结束
            if is_terminal(&rec.event_type) {
                let _ = tx
                    .send(Ok(Event::default().event("done").data("{}")))
                    .await;
                return;
            }
        }

        // stream 结束（logdbd 连接断开等）
        let _ = tx
            .send(Ok(Event::default().event("done").data("{}")))
            .await;
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

    let addr = std::env::var("LOGDBD_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".into());
    let namespace = std::env::var("LOGDBD_NAMESPACE")
        .unwrap_or_else(|_| "default".into());
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8081);

    let client = Client::connect(&addr).await?;
    tracing::info!("Connected to logdbd at {}", addr);

    let state = Arc::new(AppState {
        client: Arc::new(Mutex::new(client)),
        namespace,
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
