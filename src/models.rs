//! 数据模型 — AgentEvent, EventType, Task 等核心数据结构
//!
//! 类型命名与设计文档 8.1 节 Rust 侧定义保持一致。
//!
//! 设计原则：状态是 Events 的派生产物。不直接持久化 Agent 的当前状态，
//! 而是持久化所有产生该状态的 Events，状态通过重放 Events 重建。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── EventType 枚举 ────────────────────────────────────────────────────────

/// Agent 事件类型枚举
///
/// 命名规则：全部 snake_case，全部小写，动词使用过去式。
/// - invoked   → Agent 主动发起的调用
/// - completed → 调用正常完成
/// - failed    → 调用失败
/// - started   → 有生命周期的容器开始
/// - ended     → 容器正常结束
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    // Session 级别事件 (turn_id = NULL, step_id = NULL)
    SessionStarted,
    SessionEnded,
    SummaryMarker,

    // Task 级别事件 (turn_id = NULL, step_id = NULL) — Task 生命周期状态机(spec §8.2)
    TaskCreated,
    TaskReady,
    TaskClaimed,
    TaskBlocked,
    TaskSucceeded,
    TaskFailed,
    TaskCanceled,

    // Turn 级别事件 (turn_id NOT NULL, step_id = NULL)
    TurnPending,   // 排队等待
    TurnStarted,
    TurnCompleted,
    TurnFailed,    // Agent 崩溃等，可 redo
    TurnCanceled,  // 用户取消，不可 redo
    TurnBlocked,   // 非幂等 Tool 悬空，需人工

    // Step 级别事件 — LLM (step_id NOT NULL)
    LlmInvoked,
    LlmCompleted,
    LlmFailed,

    // Step 级别事件 — Tool (step_id NOT NULL)
    ToolInvoked,
    ToolCompleted,
    ToolFailed,
}

impl EventType {
    /// 从 snake_case 字符串解析事件类型
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "session_started" => Some(Self::SessionStarted),
            "session_ended" => Some(Self::SessionEnded),
            "summary_marker" => Some(Self::SummaryMarker),
            "task_created" => Some(Self::TaskCreated),
            "task_ready" => Some(Self::TaskReady),
            "task_claimed" => Some(Self::TaskClaimed),
            "task_blocked" => Some(Self::TaskBlocked),
            "task_succeeded" => Some(Self::TaskSucceeded),
            "task_failed" => Some(Self::TaskFailed),
            "task_canceled" => Some(Self::TaskCanceled),
            "turn_pending" => Some(Self::TurnPending),
            "turn_started" => Some(Self::TurnStarted),
            "turn_completed" => Some(Self::TurnCompleted),
            "turn_failed" => Some(Self::TurnFailed),
            "turn_canceled" => Some(Self::TurnCanceled),
            "turn_blocked" => Some(Self::TurnBlocked),
            "llm_invoked" => Some(Self::LlmInvoked),
            "llm_completed" => Some(Self::LlmCompleted),
            "llm_failed" => Some(Self::LlmFailed),
            "tool_invoked" => Some(Self::ToolInvoked),
            "tool_completed" => Some(Self::ToolCompleted),
            "tool_failed" => Some(Self::ToolFailed),
            _ => None,
        }
    }

    /// 转为 snake_case 字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SessionStarted => "session_started",
            Self::SessionEnded => "session_ended",
            Self::SummaryMarker => "summary_marker",
            Self::TaskCreated => "task_created",
            Self::TaskReady => "task_ready",
            Self::TaskClaimed => "task_claimed",
            Self::TaskBlocked => "task_blocked",
            Self::TaskSucceeded => "task_succeeded",
            Self::TaskFailed => "task_failed",
            Self::TaskCanceled => "task_canceled",
            Self::TurnPending => "turn_pending",
            Self::TurnStarted => "turn_started",
            Self::TurnCompleted => "turn_completed",
            Self::TurnFailed => "turn_failed",
            Self::TurnCanceled => "turn_canceled",
            Self::TurnBlocked => "turn_blocked",
            Self::LlmInvoked => "llm_invoked",
            Self::LlmCompleted => "llm_completed",
            Self::LlmFailed => "llm_failed",
            Self::ToolInvoked => "tool_invoked",
            Self::ToolCompleted => "tool_completed",
            Self::ToolFailed => "tool_failed",
        }
    }

    /// 是否为 Task 级别事件 (turn_id = NULL, step_id = NULL) — Task 生命周期
    pub fn is_task_level(&self) -> bool {
        matches!(
            self,
            Self::TaskCreated
                | Self::TaskReady
                | Self::TaskClaimed
                | Self::TaskBlocked
                | Self::TaskSucceeded
                | Self::TaskFailed
                | Self::TaskCanceled
        )
    }

    /// 是否为 Session 级别事件 (turn_id = NULL, step_id = NULL)
    pub fn is_session_level(&self) -> bool {
        matches!(
            self,
            Self::SessionStarted | Self::SessionEnded | Self::SummaryMarker
        )
    }

    /// 是否为 Turn 级别事件 (turn_id NOT NULL, step_id = NULL)
    pub fn is_turn_level(&self) -> bool {
        matches!(
            self,
            Self::TurnPending
                | Self::TurnStarted
                | Self::TurnCompleted
                | Self::TurnFailed
                | Self::TurnCanceled
                | Self::TurnBlocked
        )
    }

    /// 是否为 Step 级别事件 (step_id NOT NULL)
    pub fn is_step_level(&self) -> bool {
        matches!(
            self,
            Self::LlmInvoked
                | Self::LlmCompleted
                | Self::LlmFailed
                | Self::ToolInvoked
                | Self::ToolCompleted
                | Self::ToolFailed
        )
    }

    /// 是否为 Step 启动事件
    pub fn is_step_start(&self) -> bool {
        matches!(self, Self::LlmInvoked | Self::ToolInvoked)
    }

    /// 是否为 Step 终止事件
    pub fn is_step_terminal(&self) -> bool {
        matches!(
            self,
            Self::LlmCompleted | Self::LlmFailed | Self::ToolCompleted | Self::ToolFailed
        )
    }

    /// 是否为 Turn 终止事件（不需要 redo 的终态）
    pub fn is_turn_terminal(&self) -> bool {
        matches!(
            self,
            Self::TurnCompleted | Self::TurnFailed | Self::TurnCanceled | Self::TurnBlocked
        )
    }

    /// 是否可以在恢复时跳过（不 redo）
    pub fn is_turn_skip_on_recovery(&self) -> bool {
        matches!(self, Self::TurnPending | Self::TurnCanceled | Self::TurnBlocked)
    }

    /// 获取事件所属的作用域级别
    pub fn scope(&self) -> EventScope {
        if self.is_task_level() {
            EventScope::Task
        } else if self.is_session_level() {
            EventScope::Session
        } else if self.is_turn_level() {
            EventScope::Turn
        } else {
            EventScope::Step
        }
    }

    /// 检查 start/terminal 类型匹配
    /// llm_invoked  → llm_completed 或 llm_failed
    /// tool_invoked → tool_completed 或 tool_failed
    pub fn matches_start_type(start: &Self, terminal: &Self) -> bool {
        matches!(
            (start, terminal),
            (Self::LlmInvoked, Self::LlmCompleted)
                | (Self::LlmInvoked, Self::LlmFailed)
                | (Self::ToolInvoked, Self::ToolCompleted)
                | (Self::ToolInvoked, Self::ToolFailed)
        )
    }

    /// 对应给定 start 类型的合法 terminal 事件类型列表
    pub fn valid_terminals_for(start: &Self) -> &[Self] {
        match start {
            Self::LlmInvoked => &[Self::LlmCompleted, Self::LlmFailed],
            Self::ToolInvoked => &[Self::ToolCompleted, Self::ToolFailed],
            _ => &[],
        }
    }

    /// 所有事件类型的字符串列表（用于 CHECK 约束）
    pub fn all_str_variants() -> &'static [&'static str] {
        &[
            "session_started",
            "session_ended",
            "summary_marker",
            "task_created",
            "task_ready",
            "task_claimed",
            "task_blocked",
            "task_succeeded",
            "task_failed",
            "task_canceled",
            "turn_pending",
            "turn_started",
            "turn_completed",
            "turn_failed",
            "turn_canceled",
            "turn_blocked",
            "llm_invoked",
            "llm_completed",
            "llm_failed",
            "tool_invoked",
            "tool_completed",
            "tool_failed",
        ]
    }
}

/// 事件作用域级别
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventScope {
    /// Task 级别 — turn_id = NULL, step_id = NULL(Task 生命周期事件)
    Task,
    /// Session 级别 — turn_id = NULL, step_id = NULL
    Session,
    /// Turn 级别 — turn_id NOT NULL, step_id = NULL
    Turn,
    /// Step 级别 — step_id NOT NULL
    Step,
}

impl EventScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Session => "session",
            Self::Turn => "turn",
            Self::Step => "step",
        }
    }
}

// ── AgentEvent ──────────────────────────────────────────────────────────

/// Agent 不可变事件 — 一切状态的唯一来源
///
/// 每个 Event 通过 `turn_id` 和 `step_id` 标注自己的归属。
/// `seq` 是 Task 内的全局单调递增序号。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvent {
    /// Task 命名空间
    pub task_id: String,
    /// Task 内全局单调递增序号（物理坐标）
    pub seq: i64,
    /// Turn 归属标签（Turn 级和 Step 级事件有值）
    pub turn_id: Option<i64>,
    /// Step 归属标签（Task 内全局唯一，Step 级事件有值）
    pub step_id: Option<String>,
    /// 事件语义类型
    pub event_type: EventType,
    /// payload 结构版本号
    pub schema_version: i32,
    /// 事件内容（JSON）
    pub payload: serde_json::Value,
    /// 数据库写入时间
    pub created_at: DateTime<Utc>,
}

impl AgentEvent {
    /// 创建新的事件（seq 和 created_at 由数据库填充）
    pub fn new(
        task_id: String,
        turn_id: Option<i64>,
        step_id: Option<String>,
        event_type: EventType,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            task_id,
            seq: 0, // 由数据库分配
            turn_id,
            step_id,
            event_type,
            schema_version: 1,
            payload,
            created_at: Utc::now(), // 会被数据库覆盖
        }
    }

    /// 校验事件的作用域约束
    ///
    /// 对应数据库 CONSTRAINT chk_event_scope：
    /// - Session 级别：turn_id = NULL, step_id = NULL
    /// - Turn 级别：turn_id NOT NULL, step_id = NULL
    /// - Step 级别：step_id NOT NULL
    pub fn validate_scope(&self) -> Result<(), String> {
        match self.event_type.scope() {
            EventScope::Task | EventScope::Session => {
                if self.turn_id.is_some() {
                    return Err(format!(
                        "{}-level event {} must have turn_id = NULL",
                        self.event_type.scope().as_str(),
                        self.event_type.as_str()
                    ));
                }
                if self.step_id.is_some() {
                    return Err(format!(
                        "{}-level event {} must have step_id = NULL",
                        self.event_type.scope().as_str(),
                        self.event_type.as_str()
                    ));
                }
            }
            EventScope::Turn => {
                if self.turn_id.is_none() {
                    return Err(format!(
                        "Turn-level event {} must have turn_id NOT NULL",
                        self.event_type.as_str()
                    ));
                }
                if self.step_id.is_some() {
                    return Err(format!(
                        "Turn-level event {} must have step_id = NULL",
                        self.event_type.as_str()
                    ));
                }
            }
            EventScope::Step => {
                if self.step_id.is_none() {
                    return Err(format!(
                        "Step-level event {} must have step_id NOT NULL",
                        self.event_type.as_str()
                    ));
                }
                // turn_id 可为 NULL（Session 级后台 Step）
            }
        }
        Ok(())
    }
}

// ── TaskState ──────────────────────────────────────────────────────────

/// Task 状态机(spec §4)
///
/// 8 态:`created → ready → claimed → executing → (blocked ⇄ ready) → succeeded | failed`
/// 任意活态 → `canceled`(终态)。
///
/// 状态是事件的投影(spec §4.4):本枚举只描述合法迁移,实际状态由
/// `storage::get_task_state` 从 Task 级事件流派生。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Created,
    Ready,
    Claimed,
    Executing,
    Blocked,
    Succeeded,
    Failed,
    Canceled,
}

impl TaskState {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "created" => Some(Self::Created),
            "ready" => Some(Self::Ready),
            "claimed" => Some(Self::Claimed),
            "executing" => Some(Self::Executing),
            "blocked" => Some(Self::Blocked),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Ready => "ready",
            Self::Claimed => "claimed",
            Self::Executing => "executing",
            Self::Blocked => "blocked",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    /// 是否终态(不可再迁出)
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }

    /// `from → to` 是否合法迁移(spec §4 状态机)
    pub fn can_transition(from: Self, to: Self) -> bool {
        use TaskState::*;
        if from.is_terminal() {
            return false;
        }
        matches!(
            (from, to),
            (Created, Ready)
                | (Ready, Claimed)
                | (Ready, Canceled)
                | (Claimed, Executing)
                | (Claimed, Canceled)
                | (Executing, Blocked)
                | (Executing, Succeeded)
                | (Executing, Failed)
                | (Executing, Canceled)
                | (Blocked, Ready)
                | (Blocked, Canceled)
                | (Created, Canceled)
        )
    }
}

// ── FailureReason(CR-3 失败分类法)──────────────────────────────────────
// 详见 docs/superpowers/plans/2026-07-13-cr3-failure-taxonomy-retry-budget.md

/// 失败原因分类法(CR-3)。
///
/// 区分「基础设施类(瞬态,预算内重试)」与「应用/终态类(不重试)」。
/// `retry::RetryPolicy` 据此 + 预算决定 Retry / Fail。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureReason {
    // ── 基础设施类:retryable ──
    AgentSpawnFailed,
    SessionCreateFailed,
    AgentProcessExited,
    RedoDispatchFailed,
    BrokerError,
    SandboxTimeout,
    AgentUnresponsive,
    // ── 应用/终态类:不重试 ──
    ApplicationError,
    Policy,
    Canceled,
    // ── 兜底:按 retryable 处理(预算内重试),避免新错误类型静默杀 task ──
    Unknown,
}

impl FailureReason {
    /// snake_case 字面量(用于事件负载序列化 / 审计)。
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AgentSpawnFailed => "agent_spawn_failed",
            Self::SessionCreateFailed => "session_create_failed",
            Self::AgentProcessExited => "agent_process_exited",
            Self::RedoDispatchFailed => "redo_dispatch_failed",
            Self::BrokerError => "broker_error",
            Self::SandboxTimeout => "sandbox_timeout",
            Self::AgentUnresponsive => "agent_unresponsive",
            Self::ApplicationError => "application_error",
            Self::Policy => "policy",
            Self::Canceled => "canceled",
            Self::Unknown => "unknown",
        }
    }

    /// 是否值得在预算内重试。
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::AgentSpawnFailed
                | Self::SessionCreateFailed
                | Self::AgentProcessExited
                | Self::RedoDispatchFailed
                | Self::BrokerError
                | Self::SandboxTimeout
                | Self::AgentUnresponsive
                | Self::Unknown
        )
    }
}

/// 从 error_type(+ error_message 辅助)推断失败分类。纯函数,无副作用。
///
/// 未知 error_type → [`FailureReason::Unknown`](按可重试处理,由预算兜底)。
pub fn classify_failure(error_type: &str, _error_message: &str) -> FailureReason {
    match error_type {
        "agent_spawn_failed" => FailureReason::AgentSpawnFailed,
        "session_create_failed" => FailureReason::SessionCreateFailed,
        "agent_process_exited" => FailureReason::AgentProcessExited,
        "redo_dispatch_failed" => FailureReason::RedoDispatchFailed,
        "broker_error" => FailureReason::BrokerError,
        "sandbox_timeout" => FailureReason::SandboxTimeout,
        "agent_unresponsive" => FailureReason::AgentUnresponsive,
        "application_error" => FailureReason::ApplicationError,
        "policy" => FailureReason::Policy,
        "canceled" => FailureReason::Canceled,
        _ => FailureReason::Unknown,
    }
}

/// write chokepoint 的 task 迁移合法性 guard(CR-7 defense-in-depth)。
///
/// 非任务态事件(llm/tool/turn-非 started)→ 直接 Ok(不关心 task 态)。
/// 任务态事件 → 校验 `current → target` 合法(复用 [`TaskState::can_transition`]),
/// **特判 `turn_started`**:pull-based 可从 `Ready|Claimed` 直入 `Executing`(跳过 claimed)。
/// `current=None`(冷缓存/首事件)→ 仅 `task_created` 合法。
///
/// 设计见 `docs/superpowers/plans/2026-07-13-cr7-write-invariant-guard.md`。
pub fn validate_task_event_transition(
    current: Option<TaskState>,
    event_type: EventType,
) -> crate::error::Result<()> {
    use TaskState::*;
    let target: Option<TaskState> = match event_type {
        EventType::TaskCreated => Some(Created),
        EventType::TaskReady => Some(Ready),
        EventType::TaskClaimed => Some(Claimed),
        EventType::TaskBlocked => Some(Blocked),
        EventType::TaskSucceeded => Some(Succeeded),
        EventType::TaskFailed => Some(Failed),
        EventType::TaskCanceled => Some(Canceled),
        EventType::TurnStarted => Some(Executing),
        _ => return Ok(()), // 非任务态事件:不改变 task 态,不校验
    };
    let target = target.unwrap();
    match current {
        None => {
            if event_type == EventType::TaskCreated {
                Ok(())
            } else {
                Err(crate::error::AppError::LifecycleInvariant(format!(
                    "{:?}:无当前态(首事件必须是 task_created)", event_type
                )))
            }
        }
        Some(from) => {
            let legal = if event_type == EventType::TurnStarted {
                // pull-based:Ready 或 Claimed → Executing(可跳 claimed)
                matches!(from, Ready | Claimed)
            } else {
                TaskState::can_transition(from, target)
            };
            if legal {
                Ok(())
            } else {
                Err(crate::error::AppError::LifecycleInvariant(format!(
                    "非法 task 迁移:{:?} → {:?}(事件 {:?})",
                    from, target, event_type
                )))
            }
        }
    }
}

#[cfg(test)]
mod failure_reason_tests {
    use super::*;

    #[test]
    fn classify_known_types() {
        assert_eq!(classify_failure("agent_spawn_failed", ""), FailureReason::AgentSpawnFailed);
        assert_eq!(classify_failure("session_create_failed", ""), FailureReason::SessionCreateFailed);
        assert_eq!(classify_failure("agent_process_exited", ""), FailureReason::AgentProcessExited);
        assert_eq!(classify_failure("redo_dispatch_failed", ""), FailureReason::RedoDispatchFailed);
        assert_eq!(classify_failure("broker_error", ""), FailureReason::BrokerError);
        assert_eq!(classify_failure("sandbox_timeout", ""), FailureReason::SandboxTimeout);
        assert_eq!(classify_failure("agent_unresponsive", ""), FailureReason::AgentUnresponsive);
        assert_eq!(classify_failure("application_error", ""), FailureReason::ApplicationError);
        assert_eq!(classify_failure("policy", ""), FailureReason::Policy);
        assert_eq!(classify_failure("canceled", ""), FailureReason::Canceled);
    }

    #[test]
    fn classify_unknown_falls_back() {
        assert_eq!(classify_failure("something_new", ""), FailureReason::Unknown);
        assert_eq!(classify_failure("", ""), FailureReason::Unknown);
    }

    #[test]
    fn infra_and_unknown_are_retryable() {
        assert!(FailureReason::AgentSpawnFailed.is_retryable());
        assert!(FailureReason::SessionCreateFailed.is_retryable());
        assert!(FailureReason::AgentProcessExited.is_retryable());
        assert!(FailureReason::RedoDispatchFailed.is_retryable());
        assert!(FailureReason::BrokerError.is_retryable());
        assert!(FailureReason::SandboxTimeout.is_retryable());
        assert!(FailureReason::AgentUnresponsive.is_retryable());
        assert!(FailureReason::Unknown.is_retryable());
    }

    #[test]
    fn application_and_terminal_not_retryable() {
        assert!(!FailureReason::ApplicationError.is_retryable());
        assert!(!FailureReason::Policy.is_retryable());
        assert!(!FailureReason::Canceled.is_retryable());
    }

    #[test]
    fn as_str_snake_case() {
        assert_eq!(FailureReason::AgentProcessExited.as_str(), "agent_process_exited");
        assert_eq!(FailureReason::RedoDispatchFailed.as_str(), "redo_dispatch_failed");
        assert_eq!(FailureReason::ApplicationError.as_str(), "application_error");
        assert_eq!(FailureReason::Unknown.as_str(), "unknown");
    }
}

#[cfg(test)]
mod write_invariant_tests {
    use super::*;
    use crate::error::AppError;

    #[test]
    fn guard_allows_legal_task_transitions() {
        // Created → Ready
        assert!(validate_task_event_transition(Some(TaskState::Created), EventType::TaskReady).is_ok());
        // Ready → Claimed
        assert!(validate_task_event_transition(Some(TaskState::Ready), EventType::TaskClaimed).is_ok());
        // Claimed → Executing(经 turn_started)
        assert!(validate_task_event_transition(Some(TaskState::Claimed), EventType::TurnStarted).is_ok());
        // Executing → Succeeded / Failed / Canceled
        assert!(validate_task_event_transition(Some(TaskState::Executing), EventType::TaskSucceeded).is_ok());
        assert!(validate_task_event_transition(Some(TaskState::Executing), EventType::TaskFailed).is_ok());
        assert!(validate_task_event_transition(Some(TaskState::Executing), EventType::TaskCanceled).is_ok());
        // Executing → Blocked;Blocked → Ready
        assert!(validate_task_event_transition(Some(TaskState::Executing), EventType::TaskBlocked).is_ok());
        assert!(validate_task_event_transition(Some(TaskState::Blocked), EventType::TaskReady).is_ok());
        // Created → Canceled;Ready → Canceled
        assert!(validate_task_event_transition(Some(TaskState::Created), EventType::TaskCanceled).is_ok());
        assert!(validate_task_event_transition(Some(TaskState::Ready), EventType::TaskCanceled).is_ok());
    }

    #[test]
    fn guard_rejects_illegal_task_transitions() {
        // Created → Failed(跳过 ready/executing)
        assert!(matches!(
            validate_task_event_transition(Some(TaskState::Created), EventType::TaskFailed),
            Err(AppError::LifecycleInvariant(_))
        ));
        // 终态迁出:Succeeded → Ready
        assert!(matches!(
            validate_task_event_transition(Some(TaskState::Succeeded), EventType::TaskReady),
            Err(AppError::LifecycleInvariant(_))
        ));
        // Created → Claimed(跳过 ready)
        assert!(matches!(
            validate_task_event_transition(Some(TaskState::Created), EventType::TaskClaimed),
            Err(AppError::LifecycleInvariant(_))
        ));
        // 重复 task_created(已有当前态)
        assert!(matches!(
            validate_task_event_transition(Some(TaskState::Created), EventType::TaskCreated),
            Err(AppError::LifecycleInvariant(_))
        ));
    }

    #[test]
    fn guard_turn_started_special_from_ready() {
        // pull-based:turn_started 从 Ready 合法(跳 claimed);从 Executing/Blocked/Created 非法
        assert!(validate_task_event_transition(Some(TaskState::Ready), EventType::TurnStarted).is_ok());
        assert!(validate_task_event_transition(Some(TaskState::Claimed), EventType::TurnStarted).is_ok());
        assert!(matches!(
            validate_task_event_transition(Some(TaskState::Executing), EventType::TurnStarted),
            Err(AppError::LifecycleInvariant(_))
        ));
        assert!(matches!(
            validate_task_event_transition(Some(TaskState::Blocked), EventType::TurnStarted),
            Err(AppError::LifecycleInvariant(_))
        ));
        assert!(matches!(
            validate_task_event_transition(Some(TaskState::Created), EventType::TurnStarted),
            Err(AppError::LifecycleInvariant(_))
        ));
    }

    #[test]
    fn guard_none_current_only_allows_task_created() {
        assert!(validate_task_event_transition(None, EventType::TaskCreated).is_ok());
        assert!(matches!(
            validate_task_event_transition(None, EventType::TaskReady),
            Err(AppError::LifecycleInvariant(_))
        ));
        assert!(matches!(
            validate_task_event_transition(None, EventType::TurnStarted),
            Err(AppError::LifecycleInvariant(_))
        ));
    }

    #[test]
    fn guard_ignores_non_task_events() {
        // llm/tool/turn(非 started)事件不改变 task 态 → Ok
        assert!(validate_task_event_transition(Some(TaskState::Executing), EventType::LlmInvoked).is_ok());
        assert!(validate_task_event_transition(Some(TaskState::Executing), EventType::ToolInvoked).is_ok());
        assert!(validate_task_event_transition(Some(TaskState::Executing), EventType::TurnCompleted).is_ok());
        assert!(validate_task_event_transition(None, EventType::LlmCompleted).is_ok());
    }

    // ── 性能(#[ignore],cargo test --lib -- --ignored perf_validate --nocapture)──
    // guard 纯函数 O(1) match,write chokepoint 每个任务态事件调一次。测 ns/次。

    fn report_perf(name: &str, unit: &str, mut samples: Vec<u64>) {
        samples.sort_unstable();
        let n = samples.len();
        if n == 0 {
            println!("[perf] {}: no samples", name);
            return;
        }
        let p = |q: usize| samples[(q * n / 100).min(n.saturating_sub(1))];
        let sum: u64 = samples.iter().sum();
        println!(
            "[perf] {:<34} n={:>6}  p50={:>7}{}  p95={:>7}{}  p99={:>7}{}  avg={:>7}{}",
            name, n, p(50), unit, p(95), unit, p(99), unit, sum / n as u64, unit
        );
    }

    #[test]
    #[ignore]
    fn perf_validate_task_event_transition() {
        let cases = [
            (Some(TaskState::Created), EventType::TaskReady),
            (Some(TaskState::Executing), EventType::TaskFailed),
            (Some(TaskState::Ready), EventType::TurnStarted),
            (Some(TaskState::Executing), EventType::LlmInvoked), // 非任务态快路径
            (None, EventType::TaskCreated),
        ];
        for _ in 0..10_000 {
            for (s, e) in &cases {
                let _ = validate_task_event_transition(*s, e.clone());
            }
        }
        let n = 50_000;
        let mut ns = Vec::with_capacity(n);
        for i in 0..n {
            let (s, e) = &cases[i % cases.len()];
            let t0 = std::time::Instant::now();
            let _ = validate_task_event_transition(*s, e.clone());
            ns.push(t0.elapsed().as_nanos() as u64);
        }
        report_perf("validate_task_event_transition", "ns", ns);
    }
}

// ── Task ─────────────────────────────────────────────────────────────────

/// Tenant — 多租户隔离单元
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// Task — 唯一有独立存储的实体(spec §3 head)
///
/// head 字段:task_id / task_type / state / provenance。
/// body(fixus opaque)整体存 `body: Option<Value>`。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub task_id: String,
    pub tenant_id: String,
    pub user_id: String,
    /// 路由键(原 agent_type,spec §8.3 改名)
    pub task_type: String,
    /// 当前状态(事件投影,由 storage::get_task_state 派生后填入)
    pub state: TaskState,
    /// 溯源
    pub provenance: Provenance,
    /// body(opaque):contract / schema_ref / task_brief / acceptance_result
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    /// 优先级(CR-1):大者优先派发。默认 0。
    #[serde(default)]
    pub priority: i32,
}

impl Task {
    pub fn new(
        task_id: String,
        tenant_id: String,
        user_id: String,
        task_type: String,
        state: TaskState,
        provenance: Provenance,
        body: Option<serde_json::Value>,
    ) -> Self {
        Self {
            task_id,
            tenant_id,
            user_id,
            task_type,
            state,
            provenance,
            body,
            created_at: Utc::now(),
            metadata: None,
            priority: 0,
        }
    }
}

// ── 类型化 Payload ──────────────────────────────────────────────────────

/// session_started 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStartedPayload {
    pub agent_type: String,
    #[serde(default)]
    pub initial_config: serde_json::Value,
}

/// session_ended 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEndedPayload {
    pub reason: String,
}

/// Task 溯源元数据(spec §3 head.provenance)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    /// 下发渠道:nuntius-chat / api / schedule / derived
    pub source_channel: String,
    /// nuntius chat session(下发对话)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_tenant_id: Option<String>,
    /// 触发提交的那条对话消息/澄清轮(精确溯源)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_message_id: Option<String>,
    pub created_at: DateTime<Utc>,
    /// 下发系统标识(如 "nuntius")
    pub created_by: String,
}

/// task_created 的 payload(spec §3 head: task_type + provenance + body)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCreatedPayload {
    pub task_type: String,
    pub provenance: Provenance,
    /// body(fixus opaque 透传):contract / schema_ref / task_brief / acceptance_result
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
}

/// Task 迁移事件的 payload(ready/claimed/blocked/succeeded/failed/canceled 通用)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTransitionPayload {
    /// 迁移原因(自由文本)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// 认领/执行该 Task 的执行器标识(claimed 时填,用于 preferred_claimant)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimant: Option<String>,
    /// 失败分类(CR-3);仅 failed 迁移填,task_failed 事件可直接查失败原因
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<FailureReason>,
}

/// summary_marker 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryMarkerPayload {
    /// 摘要覆盖到的业务语义边界 Turn
    pub summarized_up_to_turn_id: i64,
    /// 摘要覆盖到的物理边界 seq
    pub summarized_up_to_seq: i64,
    /// 摘要文本
    pub summary: String,
    /// 本次摘要覆盖的业务事件数量（非累计）
    pub covered_event_count: i64,
}

/// turn_started 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnStartedPayload {
    pub user_input: String,
    /// Turn 幂等锚点，首次创建时生成，重做时复用
    pub redo_group: String,
    /// 第几次重做，0 = 首次
    #[serde(default)]
    pub redo_count: i32,
}

/// turn_completed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnCompletedPayload {
    pub final_output: String,
}

/// turn_failed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnFailedPayload {
    pub error_type: String,
    pub error_message: String,
    /// 失败分类(CR-3);终态失败时填,便于审计/按因统计
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<FailureReason>,
    #[serde(default)]
    pub stack_trace: Option<String>,
}

/// llm_invoked 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmInvokedPayload {
    pub step_type: String, // "llm_call"
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(default)]
    pub tools: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub temperature: Option<f64>,
    /// fixlet 侧的本地递增序号
    pub local_seq: i64,
}

/// llm_completed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmCompletedPayload {
    /// 冗余写入 model，便于审计
    pub model: String,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(default)]
    pub usage: Option<Usage>,
    #[serde(default)]
    pub finish_reason: Option<String>,
    pub local_seq: i64,
}

/// llm_failed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmFailedPayload {
    pub error_type: String,
    pub error_message: String,
    /// 失败分类(CR-3);替代旧的 retryable:bool
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<FailureReason>,
    #[serde(default)]
    pub attempt: i32,
    pub local_seq: i64,
}

/// tool_invoked 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInvokedPayload {
    pub step_type: String, // "tool_call"
    pub tool_name: String,
    pub tool_call_id: String,
    /// 幂等键：{session_id}:{redo_group}:{tool_name}:{call_signature_hash}
    pub idempotency_key: String,
    pub input: serde_json::Value,
    /// 父 Step ID（支持嵌套 Step）
    #[serde(default)]
    pub parent_step_id: Option<String>,
    pub local_seq: i64,
}

/// tool_completed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCompletedPayload {
    pub tool_call_id: String,
    pub output: serde_json::Value,
    #[serde(default)]
    pub is_error: bool,
    pub local_seq: i64,
}

/// tool_failed 的 payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolFailedPayload {
    pub tool_call_id: String,
    pub error_type: String,
    pub error_message: String,
    /// 失败分类(CR-3);替代旧的 retryable:bool
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<FailureReason>,
    #[serde(default)]
    pub attempt: i32,
    pub local_seq: i64,
}

// ── 辅助类型 ────────────────────────────────────────────────────────────

/// LLM 对话消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

/// LLM Tool Call
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub call_type: String,
    pub function: FunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

/// Token 使用量
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
}

// ── 查询结果类型 ─────────────────────────────────────────────────────────

/// 未完成 Turn（恢复查询结果）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncompleteTurn {
    pub turn_id: i64,
    pub redo_group: String,
    pub redo_count: i32,
    pub turn_started_at: DateTime<Utc>,
}

/// 未完成 Step（诊断查询结果）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncompleteStep {
    pub seq: i64,
    pub turn_id: i64,
    pub step_id: String,
    pub start_event_type: String,
    pub payload: serde_json::Value,
    pub started_at: DateTime<Utc>,
}

/// Step 执行信息（含耗时）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepExecution {
    pub step_id: String,
    pub step_type: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub end_event: Option<String>,
    pub duration_ms: Option<f64>,
}

/// Token 消耗统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsageStats {
    pub model: String,
    pub call_count: i64,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
}

// ── 校验工具 ────────────────────────────────────────────────────────────

/// 对 payload JSON 进行关键字段非空校验
///
/// 根据设计文档 9.5 节的要求，部分 event_type 的 payload 关键字段
/// 必须在应用层强校验。
pub fn validate_payload_required_fields(
    event_type: &EventType,
    payload: &serde_json::Value,
) -> Result<(), crate::error::AppError> {
    let required: &[&str] = match event_type {
        EventType::LlmInvoked => &["model", "messages", "local_seq"],
        EventType::LlmCompleted => &["model", "local_seq"],
        EventType::ToolInvoked => &["tool_name", "tool_call_id", "idempotency_key", "local_seq"],
        EventType::SummaryMarker => &[
            "summarized_up_to_seq",
            "summarized_up_to_turn_id",
            "summary",
        ],
        _ => return Ok(()),
    };

    for field in required {
        match payload.get(field) {
            None | Some(serde_json::Value::Null) => {
                return Err(crate::error::AppError::PayloadValidation {
                    event_type: event_type.as_str().to_string(),
                    field: field.to_string(),
                });
            }
            _ => {}
        }
    }

    // llm_completed 额外检查 usage 子字段
    if *event_type == EventType::LlmCompleted {
        if let Some(usage) = payload.get("usage") {
            for sub_field in &["prompt_tokens", "completion_tokens"] {
                if usage.get(sub_field).is_none()
                    || usage.get(sub_field) == Some(&serde_json::Value::Null)
                {
                    return Err(crate::error::AppError::PayloadValidation {
                        event_type: event_type.as_str().to_string(),
                        field: format!("usage.{}", sub_field),
                    });
                }
            }
        }
    }

    Ok(())
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_serde_roundtrip() {
        let cases = vec![
            (EventType::SessionStarted, "session_started"),
            (EventType::SessionEnded, "session_ended"),
            (EventType::SummaryMarker, "summary_marker"),
            (EventType::TurnStarted, "turn_started"),
            (EventType::TurnCompleted, "turn_completed"),
            (EventType::TurnFailed, "turn_failed"),
            (EventType::LlmInvoked, "llm_invoked"),
            (EventType::LlmCompleted, "llm_completed"),
            (EventType::LlmFailed, "llm_failed"),
            (EventType::ToolInvoked, "tool_invoked"),
            (EventType::ToolCompleted, "tool_completed"),
            (EventType::ToolFailed, "tool_failed"),
        ];

        for (variant, s) in cases {
            assert_eq!(variant.as_str(), s);
            assert_eq!(EventType::from_str(s), Some(variant.clone()));

            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{}\"", s));
            let parsed: EventType = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_event_type_scope() {
        // Session 级别
        assert!(EventType::SessionStarted.is_session_level());
        assert!(EventType::SessionEnded.is_session_level());
        assert!(EventType::SummaryMarker.is_session_level());
        assert_eq!(EventType::SessionStarted.scope(), EventScope::Session);

        // Turn 级别
        assert!(EventType::TurnStarted.is_turn_level());
        assert!(EventType::TurnCompleted.is_turn_level());
        assert!(EventType::TurnFailed.is_turn_level());
        assert_eq!(EventType::TurnStarted.scope(), EventScope::Turn);

        // Step 级别
        assert!(EventType::LlmInvoked.is_step_level());
        assert!(EventType::ToolCompleted.is_step_level());
        assert_eq!(EventType::LlmInvoked.scope(), EventScope::Step);
    }

    #[test]
    fn test_step_start_terminal_matching() {
        assert!(EventType::matches_start_type(
            &EventType::LlmInvoked,
            &EventType::LlmCompleted
        ));
        assert!(EventType::matches_start_type(
            &EventType::LlmInvoked,
            &EventType::LlmFailed
        ));
        assert!(EventType::matches_start_type(
            &EventType::ToolInvoked,
            &EventType::ToolCompleted
        ));
        assert!(EventType::matches_start_type(
            &EventType::ToolInvoked,
            &EventType::ToolFailed
        ));

        // 不匹配的情况
        assert!(!EventType::matches_start_type(
            &EventType::LlmInvoked,
            &EventType::ToolCompleted
        ));
        assert!(!EventType::matches_start_type(
            &EventType::ToolInvoked,
            &EventType::LlmFailed
        ));
    }

    #[test]
    fn test_agent_event_scope_validation() {
        // 正确的 Session 级事件
        let e = AgentEvent::new(
            "sess_1".into(),
            None,
            None,
            EventType::SessionStarted,
            serde_json::json!({"agent_type": "test"}),
        );
        assert!(e.validate_scope().is_ok());

        // 错误的 Session 级事件：turn_id 有值
        let e = AgentEvent::new(
            "sess_1".into(),
            Some(1),
            None,
            EventType::SessionStarted,
            serde_json::json!({}),
        );
        assert!(e.validate_scope().is_err());

        // 正确的 Turn 级事件
        let e = AgentEvent::new(
            "sess_1".into(),
            Some(1),
            None,
            EventType::TurnStarted,
            serde_json::json!({"user_input": "hi", "redo_group": "rg_1"}),
        );
        assert!(e.validate_scope().is_ok());

        // 错误的 Turn 级事件：turn_id 为 None
        let e = AgentEvent::new(
            "sess_1".into(),
            None,
            None,
            EventType::TurnStarted,
            serde_json::json!({}),
        );
        assert!(e.validate_scope().is_err());

        // 正确的 Step 级事件
        let e = AgentEvent::new(
            "sess_1".into(),
            Some(1),
            Some("step_1".into()),
            EventType::LlmInvoked,
            serde_json::json!({"model": "gpt-4", "messages": [], "local_seq": 1}),
        );
        assert!(e.validate_scope().is_ok());

        // Session 级后台 Step：turn_id = NULL, step_id NOT NULL
        let e = AgentEvent::new(
            "sess_1".into(),
            None,
            Some("step_s1".into()),
            EventType::LlmInvoked,
            serde_json::json!({"model": "gpt-4", "messages": [], "local_seq": 1}),
        );
        assert!(e.validate_scope().is_ok());

        // 错误的 Step 级事件：step_id 为 None
        let e = AgentEvent::new(
            "sess_1".into(),
            Some(1),
            None,
            EventType::LlmInvoked,
            serde_json::json!({}),
        );
        assert!(e.validate_scope().is_err());
    }

    #[test]
    fn test_payload_validation() {
        // llm_invoked 必须有 model, messages, local_seq
        let payload = serde_json::json!({"model": "gpt-4", "messages": [], "local_seq": 1});
        assert!(validate_payload_required_fields(&EventType::LlmInvoked, &payload).is_ok());

        // 缺少 model
        let payload = serde_json::json!({"messages": [], "local_seq": 1});
        assert!(validate_payload_required_fields(&EventType::LlmInvoked, &payload).is_err());

        // summary_marker 必须字段
        let payload = serde_json::json!({
            "summarized_up_to_seq": 100,
            "summarized_up_to_turn_id": 5,
            "summary": "test summary"
        });
        assert!(validate_payload_required_fields(&EventType::SummaryMarker, &payload).is_ok());
    }

    #[test]
    fn test_turn_started_payload_serde() {
        let payload = TurnStartedPayload {
            user_input: "hello".into(),
            redo_group: "rg_001".into(),
            redo_count: 0,
        };
        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["user_input"], "hello");
        assert_eq!(json["redo_group"], "rg_001");
        assert_eq!(json["redo_count"], 0);
    }

    #[test]
    fn test_all_event_types_have_str_repr() {
        let all = EventType::all_str_variants();
        assert_eq!(all.len(), 22, "Expected 22 event types");
        for s in all {
            assert!(EventType::from_str(s).is_some(), "failed for: {}", s);
        }
    }

    #[test]
    fn test_task_state_serde_roundtrip() {
        for (variant, s) in [
            (TaskState::Created, "created"),
            (TaskState::Ready, "ready"),
            (TaskState::Claimed, "claimed"),
            (TaskState::Executing, "executing"),
            (TaskState::Blocked, "blocked"),
            (TaskState::Succeeded, "succeeded"),
            (TaskState::Failed, "failed"),
            (TaskState::Canceled, "canceled"),
        ] {
            assert_eq!(variant.as_str(), s);
            assert_eq!(TaskState::from_str(s), Some(variant));
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{}\"", s));
        }
    }

    #[test]
    fn test_task_state_legal_transitions() {
        use TaskState::*;
        // spec §4 状态机
        let legal = [
            (Created, Ready),
            (Ready, Claimed),
            (Claimed, Executing),
            (Executing, Blocked),
            (Blocked, Ready),
            (Executing, Succeeded),
            (Executing, Failed),
        ];
        for (from, to) in legal {
            assert!(
                TaskState::can_transition(from, to),
                "{:?}→{:?} should be legal",
                from,
                to
            );
        }
    }

    #[test]
    fn test_task_state_illegal_transitions() {
        use TaskState::*;
        let illegal = [
            (Created, Claimed),   // 必须 ready→claimed,跳过 ready 非法
            (Ready, Executing),   // 必须 claimed→executing
            (Succeeded, Ready),   // 终态不可迁出
            (Failed, Created),
            (Canceled, Ready),
            (Blocked, Succeeded), // blocked→ready→claimed→executing→succeeded,不可直达
        ];
        for (from, to) in illegal {
            assert!(
                !TaskState::can_transition(from, to),
                "{:?}→{:?} should be illegal",
                from,
                to
            );
        }
    }

    #[test]
    fn test_task_state_terminal() {
        use TaskState::*;
        assert!(Succeeded.is_terminal());
        assert!(Failed.is_terminal());
        assert!(Canceled.is_terminal());
        assert!(!Created.is_terminal());
        assert!(!Executing.is_terminal());
        assert!(!Blocked.is_terminal());
    }

    #[test]
    fn test_task_event_type_roundtrip() {
        let cases = [
            (EventType::TaskCreated, "task_created"),
            (EventType::TaskReady, "task_ready"),
            (EventType::TaskClaimed, "task_claimed"),
            (EventType::TaskBlocked, "task_blocked"),
            (EventType::TaskSucceeded, "task_succeeded"),
            (EventType::TaskFailed, "task_failed"),
            (EventType::TaskCanceled, "task_canceled"),
        ];
        for (variant, s) in cases {
            assert_eq!(variant.as_str(), s);
            assert_eq!(EventType::from_str(s), Some(variant.clone()));
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{}\"", s));
            // Task 级事件:turn_id=NULL, step_id=NULL
            assert_eq!(variant.scope(), EventScope::Task);
        }
    }

    #[test]
    fn test_task_event_scope_validation() {
        // Task 级事件必须 turn_id=NULL, step_id=NULL
        let e = AgentEvent::new(
            "t_1".into(),
            None,
            None,
            EventType::TaskReady,
            serde_json::json!({}),
        );
        assert!(e.validate_scope().is_ok());

        // 带 turn_id 非法
        let e = AgentEvent::new(
            "t_1".into(),
            Some(1),
            None,
            EventType::TaskReady,
            serde_json::json!({}),
        );
        assert!(e.validate_scope().is_err());
    }

    #[test]
    fn test_all_event_types_count_after_task_add() {
        // 原 15 + 新 7 Task 级 = 22
        assert_eq!(EventType::all_str_variants().len(), 22);
    }

    #[test]
    fn test_task_struct_has_task_type_and_state() {
        let prov = Provenance {
            source_channel: "nuntius-chat".into(),
            source_session_id: Some("chat_1".into()),
            source_user_id: Some("u1".into()),
            source_tenant_id: Some("t1".into()),
            source_message_id: None,
            created_at: Utc::now(),
            created_by: "nuntius".into(),
        };
        let task = Task::new(
            "t_abc".into(),
            "t1".into(),
            "u1".into(),
            "database.repair".into(),
            TaskState::Created,
            prov.clone(),
            None,
        );
        assert_eq!(task.task_type, "database.repair");
        assert_eq!(task.state, TaskState::Created);
        assert_eq!(task.provenance.source_channel, "nuntius-chat");
        assert!(task.body.is_none());
    }
}
