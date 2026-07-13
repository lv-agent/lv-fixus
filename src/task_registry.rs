//! Session Registry — 进程内 Turn 协调
//!
//! broker 架构后,fixlet 不再 WS 连 fixus:turn 派发走 broker `task-begin-{type}`,
//! turn 完成走 broker `task-end`。turn 认领由 fixlet 竞争消费 broker stream 完成,
//! fixus 进程内不再维护 ready 队列。本注册表只剩**进程内**协调:
//! - task_id → PendingTurn(Turn 完成时兑现 oneshot,通知 dispatch 后台任务)
//!
//! 线程安全：使用 RwLock，写少读多。

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};

// ── TurnOutcome ─────────────────────────────────────────────────────────

/// Turn 执行结果
#[derive(Debug, Clone)]
pub enum TurnOutcome {
    Completed {
        final_output: String,
        turn_id: i64,
        event_count: i64,
    },
    Failed {
        turn_id: i64,
        error_type: String,
        error_message: String,
    },
    Timeout {
        turn_id: i64,
    },
}

// ── PendingTurn ─────────────────────────────────────────────────────────

/// 一个正在等待结果的 Turn
pub struct PendingTurn {
    pub task_id: String,
    pub turn_id: i64,
    pub redo_group: String,
    /// Turn 完成时通知 HTTP handler
    pub result_tx: oneshot::Sender<TurnOutcome>,
}

impl PendingTurn {
    pub fn new(
        task_id: String,
        turn_id: i64,
        redo_group: String,
    ) -> (Self, oneshot::Receiver<TurnOutcome>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                task_id,
                turn_id,
                redo_group,
                result_tx: tx,
            },
            rx,
        )
    }
}

// ── TaskRegistry ─────────────────────────────────────────────────────

/// 进程内 Turn 协调(Turn 完成时兑现 oneshot,通知 dispatch 后台任务)
pub struct TaskRegistry {
    /// task_id → 当前活跃的 PendingTurn
    active_turns: RwLock<HashMap<String, PendingTurn>>,
}

impl TaskRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            active_turns: RwLock::new(HashMap::new()),
        })
    }

    // ── PendingTurn 管理 ──

    /// 注册一个等待结果的 Turn。
    ///
    /// 如果该 session 已有 pending turn（前一轮尚未清理），
    /// 先向旧的 oneshot 发送 Failed 通知，避免 HTTP handler 收到无意义的 RecvError。
    pub async fn register_pending_turn(&self, task_id: &str, turn: PendingTurn) {
        let mut turns = self.active_turns.write().await;
        if let Some(old) = turns.insert(task_id.to_string(), turn) {
            tracing::warn!(
                "session {}: replacing existing pending turn (turn_id={})",
                task_id, old.turn_id
            );
            // 通知旧 HTTP handler：Turn 被新请求取代
            let _ = old.result_tx.send(TurnOutcome::Failed {
                turn_id: old.turn_id,
                error_type: "replaced".into(),
                error_message: "Turn replaced by new request".into(),
            });
        }
        tracing::debug!("session {}: registered pending turn", task_id);
    }

    /// 取出并移除 PendingTurn
    pub async fn take_pending_turn(&self, task_id: &str) -> Option<PendingTurn> {
        let mut turns = self.active_turns.write().await;
        turns.remove(task_id)
    }

    /// 完成一个 PendingTurn 并通知 HTTP handler
    pub async fn complete_pending_turn(
        &self,
        task_id: &str,
        outcome: TurnOutcome,
    ) -> Result<(), String> {
        let pending = self
            .take_pending_turn(task_id)
            .await
            .ok_or_else(|| format!("No pending turn for session {}", task_id))?;

        // 通过 oneshot 发送结果给 HTTP handler
        if pending.result_tx.send(outcome).is_err() {
            tracing::warn!(
                "session {}: HTTP handler already dropped (client disconnected?)",
                task_id
            );
        }

        Ok(())
    }
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pending_turn_lifecycle() {
        let registry = TaskRegistry::new();

        let (pending, mut rx) =
            PendingTurn::new("sess_1".into(), 1, "rg_001".into());
        registry.register_pending_turn("sess_1", pending).await;

        // 完成 Turn
        registry
            .complete_pending_turn(
                "sess_1",
                TurnOutcome::Completed {
                    final_output: "done".into(),
                    turn_id: 1,
                    event_count: 5,
                },
            )
            .await
            .unwrap();

        // HTTP handler 收到结果
        match rx.try_recv().unwrap() {
            TurnOutcome::Completed { final_output, .. } => {
                assert_eq!(final_output, "done");
            }
            _ => panic!("Expected Completed"),
        }

        // PendingTurn 已清理
        assert!(registry.take_pending_turn("sess_1").await.is_none());
    }

}
