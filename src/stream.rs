//! 流式发布 — Redis pub/sub 加速通道
//!
//! 如果 `REDIS_URL` 环境变量已设置，WAL 写入后额外发布到 Redis。
//! 发布失败不影响核心功能——fire & forget。

use redis::aio::ConnectionManager;

/// 流式发布器（可选）
#[derive(Clone)]
pub struct StreamPublisher {
    conn: Option<ConnectionManager>,
}

impl StreamPublisher {
    /// 创建发布器。REDIS_URL 未设置时返回空（不启用流式）。
    pub async fn new() -> Self {
        let redis_url = match std::env::var("REDIS_URL") {
            Ok(url) if !url.is_empty() => url,
            _ => {
                tracing::info!("REDIS_URL not set — streaming disabled");
                return Self { conn: None };
            }
        };

        match redis::Client::open(redis_url.clone()) {
            Ok(client) => match client.get_tokio_connection_manager().await {
                Ok(conn) => {
                    tracing::info!("Streaming enabled via Redis {}", redis_url);
                    Self { conn: Some(conn) }
                }
                Err(e) => {
                    tracing::warn!("Redis connection failed ({}), streaming disabled", e);
                    Self { conn: None }
                }
            },
            Err(e) => {
                tracing::warn!("Redis client error ({}), streaming disabled", e);
                Self { conn: None }
            }
        }
    }

    /// 发布事件到 Redis。失败静默忽略。
    pub async fn publish(&self, session_id: &str, turn_id: i64, event_json: &str) {
        if let Some(ref conn) = self.conn {
            let channel = format!("turn:{}:{}", session_id, turn_id);
            let mut c = conn.clone();
            let _: Result<(), _> = redis::cmd("PUBLISH")
                .arg(&channel)
                .arg(event_json)
                .query_async(&mut c)
                .await;
        }
    }
}
