//! fixus CLI — Agent Session Event Store 服务入口
//!
//! 子命令：
//! - `fixus serve`  启动 HTTP/WebSocket 服务
//! - `fixus migrate` 执行数据库迁移

use std::env;

#[tokio::main]
async fn main() {
    // 初始化 tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fixus=info,tower_http=info".into()),
        )
        .init();

    let args: Vec<String> = env::args().collect();
    let command = args.get(1).map(|s| s.as_str()).unwrap_or("serve");

    match command {
        "migrate" => {
            if let Err(e) = fixus::storage::run_migrations().await {
                eprintln!("Migration failed: {}", e);
                std::process::exit(1);
            }
            println!("Migrations completed successfully.");
        }
        "serve" | _ => {
            if let Err(e) = fixus::server::start().await {
                eprintln!("Server error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
