//! fixlet — 无状态协议网关
//!
//! 连接 fixus Gateway，管理 Agent 子进程（ACP stdio），
//! 双向路由消息。没有数据库连接，不执行工具，不存储状态。
//!
//! ## 用法
//!
//! ```bash
//! FIXUS_URL=ws://127.0.0.1:3000/ws/fixlet \
//! FIXUS_AGENT_TYPE="database.repair" \
//! AGENT_COMMAND="npx claude-agent-acp" \
//! cargo run --bin fixlet
//! ```
//!
//! ## 环境变量
//!
//! | 变量 | 默认值 | 说明 |
//! |------|--------|------|
//! | `FIXUS_URL` | `ws://127.0.0.1:3000/ws/fixlet` | fixus WebSocket 地址 |
//! | `FIXUS_AGENT_TYPE` | `default` | 此 fixlet 服务的 agent_type(fixus 按它路由 execute_turn) |
//! | `AGENT_COMMAND` | `claude-agent-acp` | Agent 启动命令 |
//! | `AGENT_CWD` | (none) | Agent 工作目录 |

mod acp;
mod idempotency;
mod router;

use router::{run, FixletConfig};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fixlet=info".into()),
        )
        .init();

    let config = FixletConfig {
        fixus_url: std::env::var("FIXUS_URL")
            .unwrap_or_else(|_| "ws://127.0.0.1:3000/ws/fixlet".into()),
        agent_type: std::env::var("FIXUS_AGENT_TYPE").unwrap_or_else(|_| "default".into()),
        agent_command: std::env::var("AGENT_COMMAND")
            .unwrap_or_else(|_| "claude-agent-acp".into()),
        agent_cwd: std::env::var("AGENT_CWD").ok(),
    };

    tracing::info!("fixlet starting");
    tracing::info!("  fixus_url: {}", config.fixus_url);
    tracing::info!("  agent_type: {}", config.agent_type);
    tracing::info!("  agent_command: {}", config.agent_command);

    if let Err(e) = run(config).await {
        tracing::error!("fixlet exited with error: {}", e);
        std::process::exit(1);
    }
}
