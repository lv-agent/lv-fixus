//! Session Registry — 活跃 Session 与 fixlet 连接管理
//!
//! 维护两个映射：
//! - session_id → fixlet WebSocket sender（路由消息给 fixlet）
//! - session_id → PendingTurn（Turn 完成时通知 HTTP handler）
//!
//! 线程安全：使用 RwLock，写少读多。

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};

// ── 类型别名 ────────────────────────────────────────────────────────────

/// WebSocket 消息发送通道
pub type WsSender = tokio::sync::mpsc::UnboundedSender<String>;

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
    pub session_id: String,
    pub turn_id: i64,
    pub redo_group: String,
    /// Turn 完成时通知 HTTP handler
    pub result_tx: oneshot::Sender<TurnOutcome>,
}

impl PendingTurn {
    pub fn new(
        session_id: String,
        turn_id: i64,
        redo_group: String,
    ) -> (Self, oneshot::Receiver<TurnOutcome>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                session_id,
                turn_id,
                redo_group,
                result_tx: tx,
            },
            rx,
        )
    }
}

// ── SessionRegistry ─────────────────────────────────────────────────────

/// 全局 Session → fixlet 连接注册表
pub struct SessionRegistry {
    /// session_id → fixlet WebSocket sender
    sessions: RwLock<HashMap<String, WsSender>>,
    /// session_id → 当前活跃的 PendingTurn
    active_turns: RwLock<HashMap<String, PendingTurn>>,
}

impl SessionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            sessions: RwLock::new(HashMap::new()),
            active_turns: RwLock::new(HashMap::new()),
        })
    }

    // ── fixlet 连接管理 ──

    /// fixlet 连接时注册（一个 fixlet 服务一个 session）
    pub async fn register_fixlet(&self, session_id: &str, sender: WsSender) {
        let mut sessions = self.sessions.write().await;
        let old = sessions.insert(session_id.to_string(), sender);
        if old.is_some() {
            tracing::info!(
                "session {}: fixlet reconnected (old connection replaced)",
                session_id
            );
        } else {
            tracing::info!("session {}: fixlet registered", session_id);
        }
    }

    /// fixlet 断开时注销
    pub async fn unregister_fixlet(&self, session_id: &str) {
        let mut sessions = self.sessions.write().await;
        sessions.remove(session_id);
        tracing::info!("session {}: fixlet unregistered", session_id);

        // 清理该 session 的 pending turn（会 dropped oneshot → HTTP 返回错误）
        let mut turns = self.active_turns.write().await;
        turns.remove(session_id);
    }

    /// 查找 session 对应的 fixlet 发送通道
    pub async fn get_fixlet_sender(&self, session_id: &str) -> Option<WsSender> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).cloned()
    }

    /// 向指定 session 的 fixlet 发送消息
    pub async fn send_to_fixlet(&self, session_id: &str, msg: &str) -> Result<(), String> {
        let sender = self
            .get_fixlet_sender(session_id)
            .await
            .ok_or_else(|| format!("No fixlet connected for session {}", session_id))?;

        sender.send(msg.to_string()).map_err(|e| {
            format!(
                "Failed to send message to fixlet for session {}: {}",
                session_id, e
            )
        })
    }

    // ── PendingTurn 管理 ──

    /// 注册一个等待结果的 Turn。
    ///
    /// 如果该 session 已有 pending turn（前一轮尚未清理），
    /// 先向旧的 oneshot 发送 Failed 通知，避免 HTTP handler 收到无意义的 RecvError。
    pub async fn register_pending_turn(&self, session_id: &str, turn: PendingTurn) {
        let mut turns = self.active_turns.write().await;
        if let Some(old) = turns.insert(session_id.to_string(), turn) {
            tracing::warn!(
                "session {}: replacing existing pending turn (turn_id={})",
                session_id, old.turn_id
            );
            // 通知旧 HTTP handler：Turn 被新请求取代
            let _ = old.result_tx.send(TurnOutcome::Failed {
                turn_id: old.turn_id,
                error_type: "replaced".into(),
                error_message: "Turn replaced by new request".into(),
            });
        }
        tracing::debug!("session {}: registered pending turn", session_id);
    }

    /// 取出并移除 PendingTurn
    pub async fn take_pending_turn(&self, session_id: &str) -> Option<PendingTurn> {
        let mut turns = self.active_turns.write().await;
        turns.remove(session_id)
    }

    /// 完成一个 PendingTurn 并通知 HTTP handler
    pub async fn complete_pending_turn(
        &self,
        session_id: &str,
        outcome: TurnOutcome,
    ) -> Result<(), String> {
        let pending = self
            .take_pending_turn(session_id)
            .await
            .ok_or_else(|| format!("No pending turn for session {}", session_id))?;

        // 通过 oneshot 发送结果给 HTTP handler
        if pending.result_tx.send(outcome).is_err() {
            tracing::warn!(
                "session {}: HTTP handler already dropped (client disconnected?)",
                session_id
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
    async fn test_register_and_send() {
        let registry = SessionRegistry::new();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register_fixlet("sess_1", tx).await;

        registry
            .send_to_fixlet("sess_1", "hello")
            .await
            .unwrap();

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, "hello");
    }

    #[tokio::test]
    async fn test_send_to_unknown_session_fails() {
        let registry = SessionRegistry::new();
        let result = registry.send_to_fixlet("nonexistent", "msg").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unregister_cleanup() {
        let registry = SessionRegistry::new();

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register_fixlet("sess_1", tx).await;
        assert!(registry.get_fixlet_sender("sess_1").await.is_some());

        registry.unregister_fixlet("sess_1").await;
        assert!(registry.get_fixlet_sender("sess_1").await.is_none());
    }

    #[tokio::test]
    async fn test_pending_turn_lifecycle() {
        let registry = SessionRegistry::new();

        let (pending, mut rx) = PendingTurn::new("sess_1".into(), 1, "rg_001".into());
        registry
            .register_pending_turn("sess_1", pending)
            .await;

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

    #[tokio::test]
    async fn test_unregister_drops_pending_turn() {
        let registry = SessionRegistry::new();

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register_fixlet("sess_1", tx).await;

        let (pending, mut rx) = PendingTurn::new("sess_1".into(), 1, "rg_001".into());
        registry
            .register_pending_turn("sess_1", pending)
            .await;

        // fixlet 断开 → pending turn 被清理
        registry.unregister_fixlet("sess_1").await;

        // oneshot 被 dropped
        assert!(rx.try_recv().is_err());
    }
}
