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

// ── 历史回放(E3:晚 attach / 重连补回放)──

/// `plan_history_replay` 的结果。
struct ReplayPlan {
    /// 按 seq 升序、已过滤本 turn 的 (event_type, payload_utf8)。
    events: Vec<(String, String)>,
    /// 历史中是否已出现终态 → turn 早已完成,发完即结束、无需再 tail。
    hit_terminal: bool,
    /// 本 turn 历史的最大 seq(含),用于 live 流去重。
    last_seq: i64,
}

/// 从 scan_all 历史快照中挑出本 turn 事件,按 seq 升序生成 SSE 事件;
/// 命中终态即截断(重连已完成 turn 的场景)。纯函数,可单测。
fn plan_history_replay(history: &[logdb_client::Record], turn_id: i64) -> ReplayPlan {
    let mut matching: Vec<&logdb_client::Record> = history
        .iter()
        .filter(|rec| {
            rec.metadata
                .get("turn_id")
                .and_then(|s| s.parse::<i64>().ok())
                == Some(turn_id)
        })
        .collect();
    matching.sort_by_key(|rec| rec.seq);

    let mut events: Vec<(String, String)> = Vec::new();
    let mut hit_terminal = false;
    let mut last_seq: i64 = 0;
    for rec in matching {
        last_seq = last_seq.max(rec.seq as i64);
        events.push((
            rec.event_type.clone(),
            String::from_utf8_lossy(&rec.content).into_owned(),
        ));
        if is_terminal(&rec.event_type) {
            hit_terminal = true;
            break;
        }
    }

    ReplayPlan {
        events,
        hit_terminal,
        last_seq,
    }
}

/// live 流去重:seq <= 已回放的历史最大 seq → 历史已发,跳过。
fn should_emit_live(rec_seq: i64, last_history_seq: i64) -> bool {
    rec_seq > last_history_seq
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

        // ── 历史回放(E3):subscribe 是纯 live tail,attach 前写入的事件(如
        // turn_started)会丢。晚 attach / 重连时先 scan_all 回放本 turn 已提交事件。
        // subscribe 已在 scan 前 attach,故 (last_seq, ∞) 仍由 live 流覆盖;
        // live 端 seq <= last_seq 的事件去重跳过(should_emit_live)。
        // 历史读失败不致命 → 退化为纯 live(旧行为)。
        let mut last_history_seq: i64 = 0;
        let mut history_hit_terminal = false;
        {
            let mut c = client.lock().await;
            match c.scan_all(&namespace, &session_id, 1).await {
                Ok(history) => {
                    drop(c);
                    let plan = plan_history_replay(&history, turn_id);
                    last_history_seq = plan.last_seq;
                    for (et, payload) in plan.events {
                        if tx
                            .send(Ok(Event::default().event(et).data(payload)))
                            .await
                            .is_err()
                        {
                            if let Some(h) = token_task {
                                h.abort();
                            }
                            return; // 客户端断连
                        }
                    }
                    history_hit_terminal = plan.hit_terminal;
                }
                Err(e) => {
                    drop(c);
                    tracing::warn!("scan_all history failed: {}", e);
                }
            }
        }

        // 历史中已出现终态 → turn 早已完成(重连查看历史),发 done 结束。
        if history_hit_terminal {
            let _ = tx
                .send(Ok(Event::default().event("done").data("{}")))
                .await;
            if let Some(h) = token_task {
                h.abort();
            }
            return;
        }

        // ── live 事件循环:subscribe,seq <= last_history_seq 去重。
        let mut sent_done = false;
        while let Some(record) = stream.next().await {
            let rec = match record {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("subscribe stream error: {}", e);
                    break;
                }
            };

            if !should_emit_live(rec.seq as i64, last_history_seq) {
                continue; // 历史已发,去重
            }

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
    // 与 sandbox-server / fixlet 一致:builder + EnvFilter(RUST_LOG 优先,缺省
    // fixus_stream=info)。原裸 fmt::init() 默认 ERROR 级别,会吞掉本 bin 的 info 日志。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fixus_stream=info".into()),
        )
        .init();

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

#[cfg(test)]
mod tests {
    //! E3 历史回放的核心逻辑单测(纯函数,不依赖 logdbd)。
    use super::*;
    use std::collections::HashMap;

    fn rec(seq: u64, turn_id: i64, etype: &str, content: &str) -> logdb_client::Record {
        let mut md = HashMap::new();
        md.insert("turn_id".into(), turn_id.to_string());
        logdb_client::Record {
            seq,
            event_type: etype.into(),
            content: content.as_bytes().to_vec(),
            metadata: md,
            ..Default::default()
        }
    }

    #[test]
    fn plan_replay_filters_by_turn_and_orders_by_seq() {
        // 故意乱序:turn 1 有 seq 1/3,turn 2 有 seq 2,验证过滤 + seq 排序。
        let history = vec![
            rec(3, 1, "llm_invoked", r#""c3""#),
            rec(1, 1, "turn_started", "{}"),
            rec(2, 2, "turn_started", "{}"),
        ];
        let plan = plan_history_replay(&history, 1);
        assert!(!plan.hit_terminal);
        assert_eq!(plan.last_seq, 3);
        assert_eq!(plan.events.len(), 2);
        assert_eq!(plan.events[0].0, "turn_started"); // seq 1
        assert_eq!(plan.events[1].0, "llm_invoked"); // seq 3
    }

    #[test]
    fn plan_replay_stops_at_terminal_and_includes_it() {
        let history = vec![
            rec(1, 1, "turn_started", "{}"),
            rec(2, 1, "llm_invoked", r#""c2""#),
            rec(3, 1, "turn_completed", r#"{"output":"done"}"#),
            rec(4, 1, "llm_invoked", r#""after-terminal""#),
        ];
        let plan = plan_history_replay(&history, 1);
        assert!(plan.hit_terminal);
        assert_eq!(plan.last_seq, 3);
        assert_eq!(plan.events.len(), 3); // 含终态本身;其后的事件不发
        assert_eq!(plan.events.last().unwrap().0, "turn_completed");
    }

    #[test]
    fn plan_replay_empty_for_unknown_turn() {
        let history = vec![rec(1, 9, "turn_started", "{}")];
        let plan = plan_history_replay(&history, 1);
        assert!(plan.events.is_empty());
        assert!(!plan.hit_terminal);
        assert_eq!(plan.last_seq, 0);
    }

    #[test]
    fn should_emit_live_dedups_by_seq_threshold() {
        assert!(!should_emit_live(3, 5)); // 历史 max=5,seq 3 已发
        assert!(!should_emit_live(5, 5)); // 边界:等于历史 max → 已发
        assert!(should_emit_live(6, 5)); // 新事件
        assert!(should_emit_live(1, 0)); // 无历史(last=0)→ 全发
    }
}
