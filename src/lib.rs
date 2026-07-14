//! fixus — Agent Session Event Store
//!
//! 不可变事件存储与可靠执行运行时。
//! 拉丁语"固定、不可移"—Events 不可变，状态由它锚定。

pub mod error;
pub mod models;
pub mod storage;
pub mod service;
pub mod recovery;
pub mod retry;
pub mod dispatcher;
pub mod metrics;
pub mod context;
pub mod protocol;
pub mod server;
pub mod task_registry;
pub mod orchestrator;
pub mod stream;
pub mod projection;
pub mod broker_store;
pub mod policy;

// 重新导出核心类型
pub use error::{AppError, Result};
pub use models::{
    validate_payload_required_fields, AgentEvent, EventScope, EventType, IncompleteStep,
    IncompleteTurn, LlmCompletedPayload, LlmFailedPayload, LlmInvokedPayload, Message, Task,
    SessionEndedPayload, SessionStartedPayload, StepExecution, SummaryMarkerPayload,
    TokenUsageStats, ToolCall, ToolCompletedPayload, ToolFailedPayload, ToolInvokedPayload,
    TurnCompletedPayload, TurnFailedPayload, TurnStartedPayload, Usage,
};
