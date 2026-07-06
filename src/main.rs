//! fixus CLI — Agent Session Event Store 服务入口
//!
//! 子命令：
//! - `fixus serve`  启动 HTTP/WebSocket 服务（默认）

#[tokio::main]
async fn main() {
    // 初始化 tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fixus=info,tower_http=info".into()),
        )
        .init();

    if let Err(e) = fixus::server::start().await {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
