//! fixlet — 无状态协议网关(broker 消费者)
//!
//! 经 broker(logdbd)pull-based 认领 turn:消费 `task-begin-{task_type}` stream
//! (稳定 group `fixlets-{task_type}`),跑 ACP agent,把结果写回 broker `task-end`、
//! 流式 token 写 Redis。不连 fixus,无数据库连接,不执行工具,不存储状态。
//!
//! ## 用法
//!
//! ```bash
//! FIXUS_AGENT_TYPE="database.repair" \
//! AGENT_COMMAND="npx claude-agent-acp" \
//! BROKER_ADDR=127.0.0.1:5100 \
//! TOOLS_BANK_URL=http://127.0.0.1:3001/mcp \
//! cargo run --bin fixlet
//! ```
//!
//! ## 环境变量
//!
//! | 变量 | 默认值 | 说明 |
//! |------|--------|------|
//! | `FIXUS_AGENT_TYPE` | `default` | 此 fixlet 服务的 task_type(订阅 `task-begin-{task_type}`;env 名保留 back-compat) |
//! | `AGENT_COMMAND` | `claude-agent-acp` | Agent 启动命令 |
//! | `AGENT_CWD` | (none) | Agent 工作目录 |
//! | `BROKER_ADDR` | `127.0.0.1:5100` | logdbd broker 地址 |
//! | `TOOLS_BANK_URL` | `http://127.0.0.1:3001/mcp` | tools-bank MCP URL(session/new 注入给 agent) |

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

        task_type: std::env::var("FIXUS_AGENT_TYPE").unwrap_or_else(|_| "default".into()),
        agent_command: std::env::var("AGENT_COMMAND")
            .unwrap_or_else(|_| "claude-agent-acp".into()),
        agent_cwd: std::env::var("AGENT_CWD").ok(),
    };

    tracing::info!("fixlet starting");
    tracing::info!("  task_type: {}", config.task_type);
    tracing::info!("  agent_command: {}", config.agent_command);

    if let Err(e) = run(config).await {
        tracing::error!("fixlet exited with error: {}", e);
        std::process::exit(1);
    }
}
