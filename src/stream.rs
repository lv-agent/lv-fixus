//! Token 流式发布 — Redis pub/sub ephemeral 快路径
//!
//! **仅用于 llm_chunk**(LLM 逐字 token):频率太高,不适合进 append-only 事件库
//! (会爆 WAL + 索引)。事件级流式(turn/llm/tool 生命周期)走 fixus-stream 的
//! logdbd Subscribe,不经此处。
//!
//! REDIS_URL 未设置时返回空 publisher(不启用 token 流式),不影响核心功能。
//! fixus-stream 侧对应地 SUBSCRIBE 同一通道,与 logdbd 事件流 fan-in 后转 SSE。

use redis::aio::ConnectionManager;

/// Token 流式发布器(可选)
#[derive(Clone)]
pub struct TokenPublisher {
    conn: Option<ConnectionManager>,
}

impl TokenPublisher {
    /// 创建发布器。REDIS_URL 未设置或连接失败时返回空(降级,不启用 token 流式)。
    pub async fn new() -> Self {
        let redis_url = match std::env::var("REDIS_URL") {
            Ok(url) if !url.is_empty() => url,
            _ => {
                tracing::info!("REDIS_URL not set — token streaming disabled");
                return Self { conn: None };
            }
        };

        match redis::Client::open(redis_url.clone()) {
            Ok(client) => match client.get_tokio_connection_manager().await {
                Ok(conn) => {
                    tracing::info!("Token streaming enabled via Redis {}", redis_url);
                    Self { conn: Some(conn) }
                }
                Err(e) => {
                    tracing::warn!("Redis connection failed ({}), token streaming disabled", e);
                    Self { conn: None }
                }
            },
            Err(e) => {
                tracing::warn!("Redis client error ({}), token streaming disabled", e);
                Self { conn: None }
            }
        }
    }

    /// 发布一个 token chunk 到 Redis(fire & forget)。未启用时为空操作。
    pub async fn publish(&self, session_id: &str, turn_id: i64, chunk_json: &str) {
        if let Some(ref conn) = self.conn {
            let channel = format!("turn:{}:{}", session_id, turn_id);
            let mut c = conn.clone();
            let _: Result<(), _> = redis::cmd("PUBLISH")
                .arg(&channel)
                .arg(chunk_json)
                .query_async(&mut c)
                .await;
        }
    }
}
