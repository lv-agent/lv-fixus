mod executor;
mod landlock;
mod sandbox_core;
mod session;

use std::path::PathBuf;
use std::sync::Arc;

use axum::{Json, Router, extract::Path, extract::State, http::StatusCode, routing};
use clap::Parser;
use serde::{Deserialize, Serialize};
use session::SessionManager;

#[derive(Parser)]
#[command(name = "sandbox-server", version)]
struct Cli {
    /// HTTP 监听端口
    #[arg(long, default_value = "8485")]
    port: u16,

    /// Session 工作目录的父目录
    #[arg(long, default_value = "/tmp/sandbox-sessions")]
    session_dir: PathBuf,

    /// 最大执行超时（秒）
    #[arg(long, default_value = "600")]
    max_timeout: u64,
}

#[derive(Clone)]
struct AppState {
    sessions: Arc<SessionManager>,
    max_timeout: u64,
}

// ── Request/Response ──

#[derive(Deserialize)]
struct ExecRequest {
    code: String,
    #[serde(default)]
    s3_path: Option<String>,
    #[serde(default)]
    env: Option<std::collections::HashMap<String, String>>,
    #[serde(default = "default_timeout")]
    timeout_secs: u64,
}

fn default_timeout() -> u64 { 120 }

#[derive(Serialize)]
struct ExecResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    stdout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stderr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    runtimes: Vec<&'static str>,
}

#[derive(Serialize)]
struct CleanedResponse {
    status: String,
}

// ── Handlers ──

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        runtimes: vec!["python3", "node", "bash"],
    })
}

async fn exec(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(req): Json<ExecRequest>,
) -> (StatusCode, Json<ExecResponse>) {
    let timeout = req.timeout_secs.min(state.max_timeout);
    let work_dir = state.sessions.get_or_create(&session_id);

    match executor::execute(&req.code, &work_dir, req.env, timeout).await {
        Ok(output) => (
            StatusCode::OK,
            Json(ExecResponse {
                stdout: Some(output.stdout),
                stderr: Some(output.stderr),
                exit_code: Some(output.exit_code),
                error: None,
                detail: None,
            }),
        ),
        Err(e) => {
            let (status, error) = match &e {
                executor::ExecError::Timeout => (StatusCode::REQUEST_TIMEOUT, "timeout"),
                executor::ExecError::Spawn(_) => (StatusCode::INTERNAL_SERVER_ERROR, "spawn_error"),
            };
            (
                status,
                Json(ExecResponse {
                    stdout: None,
                    stderr: None,
                    exit_code: None,
                    error: Some(error.into()),
                    detail: Some(e.to_string()),
                }),
            )
        }
    }
}

async fn cleanup(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Json<CleanedResponse> {
    state.sessions.cleanup(&session_id);
    Json(CleanedResponse { status: "cleaned".into() })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let state = AppState {
        sessions: Arc::new(SessionManager::new(cli.session_dir.clone())),
        max_timeout: cli.max_timeout,
    };

    let app = Router::new()
        .route("/health", routing::get(health))
        .route("/session/{session_id}/exec", routing::post(exec))
        .route("/session/{session_id}", routing::delete(cleanup))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cli.port);
    println!("🛡️  Sandbox server listening on {addr}");
    println!("   Session dir: {}", cli.session_dir.display());
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
