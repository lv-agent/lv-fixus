//! Axum HTTP 路由 — fixus Gateway API
//!
//! 实现 fixus Protocol 的 HTTP 端点：
//! - Session 管理（创建、查询、结束）
//! - Turn 管理（开始、完成、失败、查询）
//! - 事件记录（单个、批量）
//! - 上下文构建
//! - 恢复管理

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use serde::Serialize;
use crate::error::AppError;
use crate::orchestrator::Orchestrator;
use crate::protocol::*;
use crate::task_registry::TaskRegistry;
use crate::storage::EventStore;
use crate::broker_store::BrokerEventStore;
use crate::{context, recovery, service};

// ── 应用状态 ────────────────────────────────────────────────────────────

/// 共享应用状态
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn EventStore>,
    pub registry: Arc<TaskRegistry>,
    pub token_publisher: Arc<crate::stream::TokenPublisher>,
    /// 共享的 Orchestrator 实例 —— 必须全进程唯一,否则 pending_results(oneshot map)
    /// 会因 execute_tool 与 tool_result_handler 拿到不同实例而无法兑现,导致工具结果丢失。
    pub orchestrator: Arc<Orchestrator>,
}

// ── 错误转换 ────────────────────────────────────────────────────────────

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::TaskNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            AppError::TaskAlreadyExists(_) => (StatusCode::CONFLICT, self.to_string()),
            AppError::TaskAlreadyEnded(_) => (StatusCode::GONE, self.to_string()),
            AppError::TurnNotFound { .. } => (StatusCode::NOT_FOUND, self.to_string()),
            AppError::TurnAlreadyTerminal { .. } => (StatusCode::CONFLICT, self.to_string()),
            AppError::StepAlreadyTerminal { .. } => (StatusCode::CONFLICT, self.to_string()),
            AppError::LifecycleInvariant(_) => (StatusCode::CONFLICT, self.to_string()),
            AppError::InvalidTaskStateTransition { .. } => (StatusCode::CONFLICT, self.to_string()),
            AppError::Validation(_) | AppError::PayloadValidation { .. } => {
                (StatusCode::BAD_REQUEST, self.to_string())
            }
            AppError::InvalidEventType(_) | AppError::InvalidPayload(_) => {
                (StatusCode::BAD_REQUEST, self.to_string())
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        let body = Json(ApiResponse::<()>::err(message));
        (status, body).into_response()
    }
}

// ── 服务器启动 ──────────────────────────────────────────────────────────

/// 启动 fixus HTTP 服务器
pub async fn start() -> Result<(), AppError> {
    // broker 地址（默认 localhost:5100,与 logdbd 不同端口）
    let broker_addr = std::env::var("BROKER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:5100".into());
    let namespace = std::env::var("LOGDBD_NAMESPACE")
        .unwrap_or_else(|_| "default".into());

    let db = BrokerEventStore::connect(&broker_addr, &namespace)
        .await
        .map_err(|e| AppError::Internal(format!("broker connect: {}", e)))?;
    let store: Arc<dyn EventStore> = Arc::new(db);

    let registry = TaskRegistry::new();
    let token_publisher = Arc::new(crate::stream::TokenPublisher::new().await);
    let orchestrator = Arc::new(Orchestrator::new(
        store.clone(),
        registry.clone(),
        (*token_publisher).clone(),
    ));
    // 启动 broker result consumer(对称架构:sandbox→broker→fixus,无 HTTP 直连)
    let region = std::env::var("SANDBOX_REGION").unwrap_or_else(|_| "default".into());
    orchestrator.spawn_result_consumer(&broker_addr, &namespace, &region);
    // 启动 broker lifecycle consumer(fixlet turn_execution_done → broker → fixus)
    orchestrator.spawn_lifecycle_consumer(&broker_addr, &namespace);

    let state = AppState {
        store,
        registry,
        token_publisher,
        orchestrator,
    };
    let app = build_router(state);

    let port = std::env::var("FIXUS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("fixus server starting on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.map_err(|e| {
        AppError::Internal(format!("Failed to bind to {}: {}", addr, e))
    })?;

    axum::serve(listener, app).await.map_err(|e| {
        AppError::Internal(format!("Server error: {}", e))
    })?;

    Ok(())
}

/// 流式端点 URL（环境变量 FIXUS_STREAM_URL，未设置时流式不可用）
fn stream_url_for(task_id: &str, turn_id: i64) -> Option<String> {
    std::env::var("FIXUS_STREAM_URL").ok().map(|base| {
        format!(
            "{}/sessions/{}/turns/{}/stream",
            base.trim_end_matches('/'),
            task_id,
            turn_id
        )
    })
}

/// 从 AppState 获取共享的 Orchestrator(全进程唯一实例,pending_results 跨 handler 共享)
fn orchestrator(state: &AppState) -> Arc<Orchestrator> {
    state.orchestrator.clone()
}

/// 构建 Axum Router
pub fn build_router(state: AppState) -> Router {
    Router::new()
        // Session
        .route("/api/v1/sessions", post(create_session_handler))
        .route("/api/v1/sessions/{task_id}", get(get_session_handler))
        .route("/api/v1/sessions/{task_id}/end", post(end_session_handler))
        .route("/api/v1/sessions/{task_id}/ready", post(mark_ready_handler))
        .route("/api/v1/sessions/{task_id}/state", get(get_task_state_handler))
        // Turn
        .route(
            "/api/v1/sessions/{task_id}/turns",
            post(start_turn_handler),
        )
        .route(
            "/api/v1/sessions/{task_id}/turns/{turn_id}",
            get(get_turn_handler),
        )
        .route(
            "/api/v1/sessions/{task_id}/turns/{turn_id}/complete",
            post(complete_turn_handler),
        )
        .route(
            "/api/v1/sessions/{task_id}/turns/{turn_id}/fail",
            post(fail_turn_handler),
        )
        .route(
            "/api/v1/sessions/{task_id}/turns/{turn_id}/cancel",
            post(cancel_turn_handler),
        )
        // Events
        .route(
            "/api/v1/sessions/{task_id}/events",
            post(record_event_handler),
        )
        .route(
            "/api/v1/sessions/{task_id}/events/batch",
            post(record_events_batch_handler),
        )
        // Context
        .route(
            "/api/v1/sessions/{task_id}/context",
            get(get_context_handler),
        )
        .route(
            "/api/v1/sessions/{task_id}/turns/{turn_id}/context",
            get(get_turn_context_handler),
        )
        // Recovery
        .route(
            "/api/v1/sessions/{task_id}/recovery",
            get(get_recovery_handler),
        )
        .route(
            "/api/v1/sessions/{task_id}/recovery/apply",
            post(apply_recovery_handler),
        )
        // Summary
        .route(
            "/api/v1/sessions/{task_id}/summary",
            post(write_summary_handler),
        )
        // Token stats
        .route(
            "/api/v1/sessions/{task_id}/token-usage",
            get(get_token_usage_handler),
        )
        // Health
        .route("/health", get(health_handler))
        .with_state(state)
}

// ── Session Handlers ────────────────────────────────────────────────────

async fn create_session_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<CreateSessionRequest>,
) -> Result<Json<ApiResponse<CreateSessionResponse>>, AppError> {
    let tenant_id = headers.get("X-Fixus-Tenant-Id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("default");
    let user_id = headers.get("X-Fixus-User-Id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let prov = crate::models::Provenance {
        source_channel: "api".into(),
        source_session_id: None,
        source_user_id: Some(user_id.to_string()),
        source_tenant_id: Some(tenant_id.to_string()),
        source_message_id: None,
        created_at: chrono::Utc::now(),
        created_by: "api".into(),
    };
    let task_type = req.task_type.as_deref().unwrap_or(&req.agent_type);
    // fixus 分配 task_id(spec §8.4);req.session_id(client 指定)被忽略。
    let (task_id, event) = service::create_task(
        &*state.store,
        task_type,
        &prov,
        req.body.as_ref(),
    )
    .await?;

    Ok(Json(ApiResponse::ok(CreateSessionResponse {
        session_id: task_id,

        seq: event.seq,
    })))
}

async fn get_session_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<SessionInfo>>, AppError> {
    let session = service::get_task_info(&*state.store, &task_id)
        .await?
        .ok_or_else(|| AppError::TaskNotFound(task_id.clone()))?;

    let is_ended = service::is_task_ended(&*state.store, &task_id).await?;
    let max_turn = service::get_max_turn_id(&*state.store, &task_id).await?;
    let max_seq = service::get_max_seq(&*state.store, &task_id).await?;

    Ok(Json(ApiResponse::ok(SessionInfo {
        task_id: session.task_id,
        tenant_id: session.tenant_id,
        user_id: session.user_id,
        agent_type: session.task_type,
        state: session.state.as_str().to_string(),
        created_at: session.created_at.to_rfc3339(),
        metadata: session.metadata,
        is_ended,
        turn_count: max_turn,
        event_count: max_seq,
    })))
}

#[derive(Debug, Clone, Serialize)]
struct SessionInfo {
    task_id: String,
    tenant_id: String,
    user_id: String,
    agent_type: String,
    state: String,
    created_at: String,
    metadata: Option<serde_json::Value>,
    is_ended: bool,
    turn_count: i64,
    event_count: i64,
}

async fn end_session_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let reason = body
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("client_requested");

    let event = service::cancel_task(&*state.store, &task_id, reason).await?;

    Ok(Json(ApiResponse::ok(serde_json::json!({
        "task_id": event.task_id,
        "seq": event.seq,
        "reason": reason,
    }))))
}

/// POST /sessions/{task_id}/ready — nuntius 标记 readiness 通过(created → ready)
///
/// 写 task_ready 后入 claim 队列,等待执行器认领(spec §4.1 语义 gate + §8.3 claim)。
async fn mark_ready_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    // task_ready 退回纯 WAL 状态事件:turn 认领已改 broker pull-based(认领的是 turn,不是 task),
    // 不再需要 broker stream 也不需要内存 claim 队列。
    let event = service::mark_task_ready(&*state.store, &task_id).await?;

    Ok(Json(ApiResponse::ok(serde_json::json!({
        "task_id": event.task_id,
        "seq": event.seq,
        "state": "ready",
    }))))
}

/// GET /sessions/{task_id}/state — 查询 Task 当前状态(事件投影)
async fn get_task_state_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let st = state
        .store
        .get_task_state(&task_id)
        .await?
        .ok_or_else(|| AppError::TaskNotFound(task_id.clone()))?;
    Ok(Json(ApiResponse::ok(serde_json::json!({
        "task_id": task_id,
        "state": st.as_str(),
    }))))
}

// ── Turn Handlers ───────────────────────────────────────────────────────

async fn start_turn_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Json(req): Json<StartTurnRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let orch = orchestrator(&state);

    // 异步启动:写 turn_started 后立即返回 turn_id + stream_url,执行在后台进行。
    // 客户端凭 stream_url 连 fixus-stream SSE,实时看事件 + token 流式。
    match orch
        .start_turn_async(&task_id, &req.user_input, req.redo_group.as_deref())
        .await?
    {
        crate::orchestrator::AsyncTurnStart::Started { turn_id } => Ok(Json(ApiResponse::ok(
            serde_json::json!({
                "turn_id": turn_id,
                "stream_url": stream_url_for(&task_id, turn_id),
            }),
        ))),
        crate::orchestrator::AsyncTurnStart::RecoveryTriggered { incomplete_count } => {
            Ok(Json(ApiResponse::ok(serde_json::json!({
                "turn_id": 0,
                "stream_url": serde_json::Value::Null,
                "recovery": format!(
                    "{} incomplete turn(s) detected; retry after recovery",
                    incomplete_count
                ),
            }))))
        }
    }
}

async fn get_turn_handler(
    State(state): State<AppState>,
    Path((task_id, turn_id)): Path<(String, i64)>,
) -> Result<Json<ApiResponse<TurnInfo>>, AppError> {
    let events = service::get_turn_events(&*state.store, &task_id, turn_id).await?;
    if events.is_empty() {
        return Err(AppError::TurnNotFound {
            task_id,
            turn_id,
        });
    }

    let steps = service::get_turn_steps(&*state.store, &task_id, turn_id).await?;

    let terminal = events
        .iter()
        .find(|e| e.event_type.is_turn_terminal())
        .map(|e| e.event_type.as_str().to_string());

    Ok(Json(ApiResponse::ok(TurnInfo {
        turn_id,
        event_count: events.len() as i64,
        terminal_event: terminal,
        steps,
    })))
}

#[derive(Debug, Clone, Serialize)]
struct TurnInfo {
    turn_id: i64,
    event_count: i64,
    terminal_event: Option<String>,
    steps: Vec<crate::models::StepExecution>,
}

async fn complete_turn_handler(
    State(state): State<AppState>,
    Path((task_id, turn_id)): Path<(String, i64)>,
    Json(req): Json<CompleteTurnRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let event =
        service::complete_turn(&*state.store, &task_id, turn_id, &req.final_output).await?;

    Ok(Json(ApiResponse::ok(serde_json::json!({
        "turn_id": turn_id,
        "seq": event.seq,
    }))))
}

async fn cancel_turn_handler(
    State(state): State<AppState>,
    Path((task_id, turn_id)): Path<(String, i64)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let reason = body.get("reason").and_then(|v| v.as_str()).unwrap_or("user_canceled");
    let event = service::cancel_turn(&*state.store, &task_id, turn_id, reason).await?;
    Ok(Json(ApiResponse::ok(serde_json::json!({
        "turn_id": turn_id, "seq": event.seq, "state": "canceled"
    }))))
}

async fn fail_turn_handler(
    State(state): State<AppState>,
    Path((task_id, turn_id)): Path<(String, i64)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let error_type = body
        .get("error_type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let error_message = body
        .get("error_message")
        .and_then(|v| v.as_str())
        .unwrap_or("No error message");
    let stack_trace = body
        .get("stack_trace")
        .and_then(|v| v.as_str());

    let event = service::fail_turn(
        &*state.store,
        &task_id,
        turn_id,
        error_type,
        error_message,
        stack_trace,
    )
    .await?;

    Ok(Json(ApiResponse::ok(serde_json::json!({
        "turn_id": turn_id,
        "seq": event.seq,
    }))))
}

// ── Event Handlers ──────────────────────────────────────────────────────

async fn record_event_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Json(req): Json<RecordEventRequest>,
) -> Result<Json<ApiResponse<RecordEventResponse>>, AppError> {
    let event_type = crate::models::EventType::from_str(&req.event_type)
        .ok_or_else(|| AppError::InvalidEventType(req.event_type.clone()))?;

    let seq = service::record_event(
        &*state.store,
        &task_id,
        req.turn_id,
        Some(&req.step_id),
        event_type,
        req.payload,
    )
    .await?;

    Ok(Json(ApiResponse::ok(RecordEventResponse { seq })))
}

async fn record_events_batch_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Json(req): Json<RecordEventsBatchRequest>,
) -> Result<Json<ApiResponse<RecordEventsBatchResponse>>, AppError> {
    let mut events = Vec::with_capacity(req.events.len());

    for report in &req.events {
        let event_type = crate::models::EventType::from_str(&report.event_type)
            .ok_or_else(|| AppError::InvalidEventType(report.event_type.clone()))?;

        let event = crate::models::AgentEvent::new(
            task_id.clone(),
            Some(report.turn_id),
            Some(report.step_id.clone()),
            event_type,
            report.payload.clone(),
        );
        events.push(event);
    }

    let seqs = service::write_events_batch(&*state.store, &events).await?;

    Ok(Json(ApiResponse::ok(RecordEventsBatchResponse { seqs })))
}

// ── Context Handlers ────────────────────────────────────────────────────

async fn get_context_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<ContextResponse>>, AppError> {
    let ctx = context::build_llm_context(&*state.store, &task_id).await?;

    Ok(Json(ApiResponse::ok(ContextResponse {
        summary: ctx.summary,
        summarized_up_to_seq: ctx.summarized_up_to_seq,
        summarized_up_to_turn_id: ctx.summarized_up_to_turn_id,
        messages: ctx.messages,
    })))
}

async fn get_turn_context_handler(
    State(state): State<AppState>,
    Path((task_id, turn_id)): Path<(String, i64)>,
) -> Result<Json<ApiResponse<Vec<crate::models::Message>>>, AppError> {
    let messages = context::build_turn_context(&*state.store, &task_id, turn_id).await?;
    Ok(Json(ApiResponse::ok(messages)))
}

// ── Recovery Handlers ───────────────────────────────────────────────────

async fn get_recovery_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<RecoveryStatusResponse>>, AppError> {
    let rec_state = recovery::check_session_recovery(&*state.store, &task_id).await?;

    let mut redo_queue = Vec::new();
    for incomplete_turn in &rec_state.incomplete_turns {
        let decision =
            recovery::decide_turn_recovery(&*state.store, &task_id, incomplete_turn).await?;

        match decision {
            recovery::RecoveryDecision::SafeToRedo {
                turn_id,
                redo_group,
                redo_count: _,
                ..
            } => {
                if let Some(ctx) =
                    recovery::build_redo_context(&*state.store, &task_id, incomplete_turn)
                        .await?
                {
                    redo_queue.push(RedoInfo {
                        turn_id,
                        redo_group,
                        redo_count: ctx.redo_count,
                        user_input: ctx.user_input,
                    });
                }
            }
            _ => {}
        }
    }

    Ok(Json(ApiResponse::ok(RecoveryStatusResponse {
        session_id: rec_state.task_id.clone(),
        incomplete_turns: rec_state.incomplete_turns,
        redo_queue,
    })))
}

async fn apply_recovery_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<Vec<RedoInfo>>>, AppError> {
    let redo_queue = recovery::recover_task(&*state.store, &task_id).await?;

    let redo_infos: Vec<RedoInfo> = redo_queue
        .into_iter()
        .map(|ctx| RedoInfo {
            turn_id: ctx.turn_id,
            redo_group: ctx.redo_group,
            redo_count: ctx.redo_count,
            user_input: ctx.user_input,
        })
        .collect();

    Ok(Json(ApiResponse::ok(redo_infos)))
}

// ── Summary Handler ─────────────────────────────────────────────────────

async fn write_summary_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let summarized_up_to_turn_id = body
        .get("summarized_up_to_turn_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| AppError::Validation("summarized_up_to_turn_id is required".into()))?;

    let summarized_up_to_seq = body
        .get("summarized_up_to_seq")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| AppError::Validation("summarized_up_to_seq is required".into()))?;

    let summary = body
        .get("summary")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Validation("summary is required".into()))?;

    let covered_event_count = body
        .get("covered_event_count")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let event = service::write_summary_marker(
        &*state.store,
        &task_id,
        summarized_up_to_turn_id,
        summarized_up_to_seq,
        summary,
        covered_event_count,
    )
    .await?;

    Ok(Json(ApiResponse::ok(serde_json::json!({
        "seq": event.seq,
        "summarized_up_to_turn_id": summarized_up_to_turn_id,
        "summarized_up_to_seq": summarized_up_to_seq,
    }))))
}

// ── Token Usage Handler ─────────────────────────────────────────────────

async fn get_token_usage_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<ApiResponse<Vec<crate::models::TokenUsageStats>>>, AppError> {
    let stats = service::get_token_usage_stats(&*state.store, &task_id).await?;
    Ok(Json(ApiResponse::ok(stats)))
}

// ── Health Handler ──────────────────────────────────────────────────────

async fn health_handler(
    State(state): State<AppState>,
) -> Json<ApiResponse<crate::orchestrator::HealthStatus>> {
    let orch = orchestrator(&state);
    let health = orch.health().await;
    Json(ApiResponse::ok(health))
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Provenance;
    use crate::storage::LogdbdEventStore;

    use logdbd::catalog::Catalog;
    use logdbd::consumer::ConsumerTracker;
    use logdbd::pb::log_db_service_server::LogDbServiceServer;
    use logdbd::service::LogDbServiceImpl;
    use logdbd::storage::Storage;
    use logdbd::subscribe::SubscribeHub;
    use logdb::Config as LogdbConfig;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    fn test_provenance() -> Provenance {
        Provenance {
            source_channel: "api".into(),
            source_session_id: None,
            source_user_id: Some("u".into()),
            source_tenant_id: Some("t".into()),
            source_message_id: None,
            created_at: chrono::Utc::now(),
            created_by: "test".into(),
        }
    }

    async fn setup() -> (AppState, Arc<TaskRegistry>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = LogdbConfig::default();
        cfg.data_dir = dir.path().to_path_buf();
        cfg.durability_mode = logdb::DurabilityMode::Sync;
        cfg.ring_size = 256;
        cfg.shards = 1;
        cfg.flush_timeout = Duration::from_secs(5);
        let db = logdb::LogDb::open(cfg).unwrap();
        let storage = Arc::new(Storage::new(db, 1));
        let catalog = Arc::new(Catalog::open(dir.path()).unwrap());
        let subscribe_hub = Arc::new(SubscribeHub::new());
        let consumer_tracker = Arc::new(ConsumerTracker::new(None));
        let svc = LogDbServiceImpl::new(
            Arc::clone(&storage),
            Arc::clone(&catalog),
            Arc::clone(&consumer_tracker),
            Arc::clone(&subscribe_hub),
            "test-node".into(),
            "primary".into(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            Server::builder()
                .add_service(LogDbServiceServer::new(svc))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let store: Arc<dyn EventStore> = Arc::new(LogdbdEventStore::connect(&addr, "fixus-srv-test").await.unwrap());
        let registry = TaskRegistry::new();
        let token_publisher = Arc::new(crate::stream::TokenPublisher::new().await);
        let orchestrator = Arc::new(Orchestrator::new(
            store.clone(),
            registry.clone(),
            (*token_publisher).clone(),
        ));
        let state = AppState {
            store,
            registry: registry.clone(),
            token_publisher,
            orchestrator,
        };
        (state, registry, dir)
    }

    async fn wait_seq(state: &AppState, sid: &str, expected: i64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(v) = state.store.get_max_seq(sid).await {
                if v >= expected {
                    return;
                }
            }
            if std::time::Instant::now() >= deadline {
                panic!("seq {} not reached for {}", expected, sid);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// 直调 handler(不经 HTTP 栈),验证 /ready 端到端:写 task_ready 事件 + 投影到 ready
    #[tokio::test]
    async fn mark_ready_handler_writes_event() {
        let (state, _registry, _d) = setup().await;
        let prov = test_provenance();
        let (tid, _) = service::create_task(&*state.store, "db.repair", &prov, None)
            .await
            .unwrap();
        wait_seq(&state, &tid, 1).await;
        assert_eq!(
            state.store.get_task_state(&tid).await.unwrap(),
            Some(crate::models::TaskState::Created)
        );

        // 调 /ready handler
        let resp = mark_ready_handler(State(state.clone()), Path(tid.clone())).await;
        let Json(api) = resp.unwrap();
        assert!(api.success);
        assert_eq!(api.data.as_ref().unwrap()["state"], "ready");

        // state 投影到 ready
        wait_seq(&state, &tid, 2).await;
        assert_eq!(
            state.store.get_task_state(&tid).await.unwrap(),
            Some(crate::models::TaskState::Ready)
        );
    }

    /// 直调 /state handler
    #[tokio::test]
    async fn get_task_state_handler_returns_projection() {
        let (state, _reg, _d) = setup().await;
        let prov = test_provenance();
        let (tid, _) = service::create_task(&*state.store, "db.repair", &prov, None)
            .await
            .unwrap();
        wait_seq(&state, &tid, 1).await;

        // created
        let Json(api) = get_task_state_handler(State(state.clone()), Path(tid.clone()))
            .await
            .unwrap();
        assert!(api.success);
        assert_eq!(api.data.unwrap()["state"], "created");

        // mark_ready → ready
        service::mark_task_ready(&*state.store, &tid).await.unwrap();
        wait_seq(&state, &tid, 2).await;
        let Json(api) = get_task_state_handler(State(state.clone()), Path(tid.clone()))
            .await
            .unwrap();
        assert_eq!(api.data.unwrap()["state"], "ready");
    }

    /// /ready 对不存在的 Task → TaskNotFound(Err)
    #[tokio::test]
    async fn mark_ready_handler_missing_task_errors() {
        let (state, _reg, _d) = setup().await;
        let err = mark_ready_handler(State(state), Path("task_nonexistent".into())).await;
        assert!(matches!(err, Err(AppError::TaskNotFound(_))));
    }
}
