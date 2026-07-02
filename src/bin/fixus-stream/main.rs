//! fixus-stream — Redis → SSE 流式网关
//!
//! 将 fixus Turn 事件以 SSE 格式推给客户端。
//! 晚加入的客户端先从 DB 补发历史事件，再切到实时流。
//!
//! ## 实时流模式
//! - Redis 可用时：SUBSCRIBE turn:{session_id}:{turn_id}，实时推送
//! - Redis 不可用时：降级为 DB 轮询（200ms）
//!
//! ## 环境变量
//! - `REDIS_URL` — Redis 地址（实时流，可选）
//! - `DATABASE_URL` — fixus SQLite 数据库路径
//! - `PORT` — 监听端口（默认 8081）

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    response::sse::{Event, Sse},
    routing::get,
    Router,
};
use futures_util::stream::Stream;
use futures_util::StreamExt;
use sqlx::Row;
use tokio::sync::mpsc;

// ── Redis 订阅器 ──────────────────────────────────────────────────

/// Redis 订阅封装。
///
/// 每个 SSE 连接调用 `subscribe_turn()` 创建独立的 PubSub 连接。
/// PubSub 需要独占连接，不能复用 ConnectionManager。
struct RedisSubscriber {
    client: Option<redis::Client>,
}

impl RedisSubscriber {
    /// 创建订阅器。REDIS_URL 未设置时返回空。
    async fn new() -> Self {
        let redis_url = match std::env::var("REDIS_URL") {
            Ok(url) if !url.is_empty() => url,
            _ => {
                tracing::info!("REDIS_URL not set — real-time streaming disabled, DB polling only");
                return Self { client: None };
            }
        };

        match redis::Client::open(redis_url.as_str()) {
            Ok(client) => {
                tracing::info!("Redis subscriber ready ({})", redis_url);
                Self { client: Some(client) }
            }
            Err(e) => {
                tracing::warn!("Redis client open failed ({}), falling back to DB polling", e);
                Self { client: None }
            }
        }
    }

    fn is_available(&self) -> bool {
        self.client.is_some()
    }
}

// ── 应用状态 ──

struct AppState {
    db_pool: sqlx::SqlitePool,
    redis: RedisSubscriber,
}

// ── Terminal event types ──

const TERMINAL_EVENTS: &[&str] = &[
    "turn_completed",
    "turn_failed",
    "turn_canceled",
    "turn_blocked",
];

fn is_terminal_event(event_type: &str) -> bool {
    TERMINAL_EVENTS.contains(&event_type)
}

// ── SSE 端点 ──

#[derive(serde::Deserialize)]
struct StreamQuery {
    #[serde(default)]
    last_seq: Option<i64>,
}

async fn stream_handler(
    State(state): State<Arc<AppState>>,
    Path(turn_id): Path<i64>,
    Query(query): Query<StreamQuery>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let (tx, rx) = mpsc::channel::<Result<Event, axum::Error>>(256);
    let pool = state.db_pool.clone();
    let redis = state.redis.client.clone();
    let last_seq = query.last_seq.unwrap_or(0);
    let mut next_seq = last_seq + 1;

    tokio::spawn(async move {
        // ── Phase 1: DB 历史补发 ──
        let mut session_id: Option<String> = None;

        let rows = sqlx::query(
            "SELECT session_id, seq, event_type, payload FROM agent_events
             WHERE turn_id = ?1 AND seq >= ?2
             ORDER BY seq LIMIT 100",
        )
        .bind(turn_id)
        .bind(next_seq)
        .fetch_all(&pool)
        .await;

        match rows {
            Ok(events) => {
                for row in &events {
                    let sid: String = row.get("session_id");
                    let seq: i64 = row.get("seq");
                    let event_type: String = row.get("event_type");
                    let payload: String = row.get("payload");

                    // 记住 session_id 供 Redis 订阅用
                    if session_id.is_none() {
                        session_id = Some(sid);
                    }

                    if tx
                        .send(Ok(Event::default().event(&event_type).data(&payload)))
                        .await
                        .is_err()
                    {
                        return; // 客户端断连
                    }
                    next_seq = seq + 1;
                }
            }
            Err(e) => {
                tracing::warn!("DB history query failed: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }

        // 检查 Turn 是否已结束
        let done = sqlx::query(
            "SELECT COUNT(*) as cnt FROM agent_events
             WHERE turn_id = ?1 AND event_type IN ('turn_completed', 'turn_failed', 'turn_canceled', 'turn_blocked')",
        )
        .bind(turn_id)
        .fetch_one(&pool)
        .await
        .map(|r| {
            let cnt: i64 = r.get("cnt");
            cnt > 0
        })
        .unwrap_or(false);

        if done {
            let _ = tx.send(Ok(Event::default().event("done").data("{}"))).await;
            return;
        }

        // ── Phase 2: 实时流 ──

        // 尝试从已发送的历史事件中获取 session_id
        // 如果没有历史事件，从 DB 单独查
        let session_id = match session_id {
            Some(sid) => sid,
            None => {
                match sqlx::query("SELECT session_id FROM agent_events WHERE turn_id = ?1 LIMIT 1")
                    .bind(turn_id)
                    .fetch_optional(&pool)
                    .await
                {
                    Ok(Some(row)) => row.get::<String, _>("session_id"),
                    Ok(None) => {
                        tracing::warn!("Turn {} has no events, cannot determine session_id", turn_id);
                        // 降级到 DB 轮询等事件出现
                        fallthrough_db_polling(&pool, turn_id, next_seq, tx).await;
                        return;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to query session_id for turn {}: {}", turn_id, e);
                        fallthrough_db_polling(&pool, turn_id, next_seq, tx).await;
                        return;
                    }
                }
            }
        };

        // 尝试 Redis 实时流
        if let Some(client) = redis {
            match client.get_async_pubsub().await {
                Ok(mut pubsub) => {
                    let channel = format!("turn:{}:{}", session_id, turn_id);
                    match pubsub.subscribe(&channel).await {
                        Ok(()) => {
                            tracing::debug!("Turn {}: streaming via Redis SUBSCRIBE {}", turn_id, channel);
                            let mut msg_stream = pubsub.on_message();

                            while let Some(msg) = msg_stream.next().await {
                                let payload: String = match msg.get_payload() {
                                    Ok(p) => p,
                                    Err(e) => {
                                        tracing::warn!("Redis message parse error: {}", e);
                                        continue;
                                    }
                                };

                                // 解析事件类型以判断是否 terminal
                                let event_type = extract_event_type(&payload);
                                let event_name = event_type
                                    .as_deref()
                                    .unwrap_or("unknown");

                                if tx
                                    .send(Ok(Event::default().event(event_name).data(&payload)))
                                    .await
                                    .is_err()
                                {
                                    return; // 客户端断连
                                }

                                if event_type
                                    .as_deref()
                                    .map(|t| is_terminal_event(t))
                                    .unwrap_or(false)
                                {
                                    let _ = tx
                                        .send(Ok(Event::default().event("done").data("{}")))
                                        .await;
                                    return;
                                }
                            }

                            // Redis 流结束（连接断开），降级到 DB 轮询
                            tracing::warn!("Turn {}: Redis stream ended, falling back to DB polling", turn_id);
                            fallthrough_db_polling(&pool, turn_id, next_seq, tx).await;
                            return;
                        }
                        Err(e) => {
                            tracing::warn!("Redis subscribe failed for {}: {}, falling back to DB polling", channel, e);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Redis PubSub connection failed: {}, falling back to DB polling", e);
                }
            }
        }

        // Redis 不可用或连接失败 → DB 轮询
        fallthrough_db_polling(&pool, turn_id, next_seq, tx).await;
    });

    Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx))
}

/// DB 轮询模式（原有逻辑，作为 Redis 不可用时的降级方案）
async fn fallthrough_db_polling(
    pool: &sqlx::SqlitePool,
    turn_id: i64,
    mut next_seq: i64,
    tx: mpsc::Sender<Result<Event, axum::Error>>,
) {
    loop {
        let rows = sqlx::query(
            "SELECT seq, event_type, payload FROM agent_events
             WHERE turn_id = ?1 AND seq >= ?2
             ORDER BY seq LIMIT 10",
        )
        .bind(turn_id)
        .bind(next_seq)
        .fetch_all(pool)
        .await;

        match rows {
            Ok(events) => {
                for row in &events {
                    let seq: i64 = row.get("seq");
                    let event_type: String = row.get("event_type");
                    let payload: String = row.get("payload");
                    if tx
                        .send(Ok(Event::default().event(&event_type).data(&payload)))
                        .await
                        .is_err()
                    {
                        return; // 客户端断连
                    }
                    next_seq = seq + 1;
                }
            }
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }

        // 检查 Turn 是否已结束
        let done = sqlx::query(
            "SELECT COUNT(*) as cnt FROM agent_events
             WHERE turn_id = ?1 AND event_type IN ('turn_completed', 'turn_failed', 'turn_canceled', 'turn_blocked')",
        )
        .bind(turn_id)
        .fetch_one(pool)
        .await
        .map(|r| {
            let cnt: i64 = r.get("cnt");
            cnt > 0
        })
        .unwrap_or(false);

        if done {
            let _ = tx.send(Ok(Event::default().event("done").data("{}"))).await;
            return;
        }

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// 从 JSON payload 中提取 "type" 字段值
fn extract_event_type(payload: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|v| v.get("type")?.as_str().map(|s| s.to_string()))
}

// ── 健康检查 ──

async fn health() -> &'static str { "ok" }

// ── 主函数 ──

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:fixus.db?mode=rwc".into());
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8081);

    let db_pool = sqlx::SqlitePool::connect(&db_url).await?;
    tracing::info!("Connected to DB {}", db_url);

    let redis = RedisSubscriber::new().await;
    tracing::info!(
        "Real-time streaming: {}",
        if redis.is_available() { "Redis SUBSCRIBE" } else { "DB polling only" }
    );

    let state = Arc::new(AppState { db_pool, redis });

    let app = Router::new()
        .route("/turns/{turn_id}/stream", get(stream_handler))
        .route("/health", get(health))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("fixus-stream listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
