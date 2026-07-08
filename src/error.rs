/// fixus 统一错误类型
///
/// 所有 pub API 返回 `Result<T, AppError>`，不 panic。
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("database migration error: {0}")]
    Migration(String),

    // ── 序列化错误 ──
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    // ── 会话错误 ──
    #[error("session not found: {0}")]
    TaskNotFound(String),

    #[error("session already exists: {0}")]
    TaskAlreadyExists(String),

    #[error("session already ended: {0}")]
    TaskAlreadyEnded(String),

    // ── Turn 错误 ──
    #[error("turn not found: session={task_id}, turn={turn_id}")]
    TurnNotFound { task_id: String, turn_id: i64 },

    #[error("turn already has terminal event: session={task_id}, turn={turn_id}")]
    TurnAlreadyTerminal { task_id: String, turn_id: i64 },

    #[error("turn has no started event: session={task_id}, turn={turn_id}")]
    TurnNotStarted { task_id: String, turn_id: i64 },

    // ── Step 错误 ──
    #[error("step not found: step_id={step_id}")]
    StepNotFound { step_id: String },

    #[error("step already has terminal event: step_id={step_id}")]
    StepAlreadyTerminal { step_id: String },

    #[error("step start/terminal type mismatch: expected {expected:?}, got {got:?}")]
    StepTypeMismatch { expected: String, got: String },

    // ── 生命周期不变量违反 ──
    #[error("lifecycle invariant violation: {0}")]
    LifecycleInvariant(String),

    #[error("seq continuity gap detected: session={task_id}, missing after seq={seq}")]
    SeqGap { task_id: String, seq: i64 },

    // ── 校验错误 ──
    #[error("validation error: {0}")]
    Validation(String),

    #[error("payload validation error for event {event_type}: missing required field '{field}'")]
    PayloadValidation { event_type: String, field: String },

    // ── 恢复错误 ──
    #[error("recovery skipped: non-idempotent tool {tool_name}")]
    RecoverySkippedNonIdempotent { tool_name: String },

    #[error("recovery error: {0}")]
    Recovery(String),

    // ── 协议错误 ──
    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("invalid event type: {0}")]
    InvalidEventType(String),

    #[error("invalid payload: {0}")]
    InvalidPayload(String),

    // ── 上下文构建错误 ──
    #[error("context build error: {0}")]
    ContextBuild(String),

    // ── 摘要错误 ──
    #[error("summary error: {0}")]
    Summary(String),

    // ── 内部错误 ──
    #[error("internal error: {0}")]
    Internal(String),
}

/// 便捷 Result 类型别名
pub type Result<T> = std::result::Result<T, AppError>;
