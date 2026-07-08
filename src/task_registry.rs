//! Session Registry — fixlet 连接 + 活跃 Turn 管理
//!
//! 维护两个映射：
//! - **agent_type → fixlet WebSocket sender**(按 agent_type 路由 execute_turn 给 fixlet)
//! - task_id → PendingTurn(Turn 完成时通知 HTTP handler;turn 属 session,仍按 task_id)
//!
//! 为什么按 agent_type 而非 task_id:上游(如 nuntius)每次创建随机 task_id,
//! 但 fixlet 实例按它服务的 agent_type 注册。按 agent_type 路由让任意同类型 session
//! 都能命中对应 fixlet,无需 task_id 协调。
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
    pub task_id: String,
    /// 该 turn 所属 session 的 agent_type(fixlet 断连时按它快速失败同类型 pending turn)
    pub agent_type: String,
    pub turn_id: i64,
    pub redo_group: String,
    /// Turn 完成时通知 HTTP handler
    pub result_tx: oneshot::Sender<TurnOutcome>,
}

impl PendingTurn {
    pub fn new(
        task_id: String,
        agent_type: String,
        turn_id: i64,
        redo_group: String,
    ) -> (Self, oneshot::Receiver<TurnOutcome>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                task_id,
                agent_type,
                turn_id,
                redo_group,
                result_tx: tx,
            },
            rx,
        )
    }
}

// ── TaskRegistry ─────────────────────────────────────────────────────

/// 全局 fixlet 连接注册表(按 agent_type 路由)+ 活跃 Turn
pub struct TaskRegistry {
    /// agent_type → fixlet WebSocket sender(一种 agent_type 一个 fixlet;worker-pool 待扩展)
    by_agent_type: RwLock<HashMap<String, WsSender>>,
    /// task_id → 当前活跃的 PendingTurn
    active_turns: RwLock<HashMap<String, PendingTurn>>,
}

impl TaskRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            by_agent_type: RwLock::new(HashMap::new()),
            active_turns: RwLock::new(HashMap::new()),
        })
    }

    // ── fixlet 连接管理(按 agent_type)──

    /// fixlet 连接时注册它服务的 agent_type(一种 agent_type 一个 fixlet)
    pub async fn register_fixlet(&self, agent_type: &str, sender: WsSender) {
        let mut map = self.by_agent_type.write().await;
        let old = map.insert(agent_type.to_string(), sender);
        if old.is_some() {
            tracing::info!(
                "agent_type {}: fixlet reconnected (old connection replaced)",
                agent_type
            );
        } else {
            tracing::info!("agent_type {}: fixlet registered", agent_type);
        }
    }

    /// fixlet 断开时注销其 agent_type,并快速失败该类型下所有 pending turn
    /// (fixlet 已断,继续等只会拖到 turn 超时;主动通知 HTTP handler)
    pub async fn unregister_fixlet(&self, agent_type: &str) {
        self.by_agent_type.write().await.remove(agent_type);

        let mut turns = self.active_turns.write().await;
        let to_fail: Vec<String> = turns
            .iter()
            .filter(|(_, p)| p.agent_type == agent_type)
            .map(|(sid, _)| sid.clone())
            .collect();
        for sid in to_fail {
            if let Some(p) = turns.remove(&sid) {
                let _ = p.result_tx.send(TurnOutcome::Failed {
                    turn_id: p.turn_id,
                    error_type: "fixlet_disconnected".into(),
                    error_message: format!("fixlet for agent_type {} disconnected", agent_type),
                });
            }
        }
        tracing::info!("agent_type {}: fixlet unregistered", agent_type);
    }

    /// 查找服务于指定 agent_type 的 fixlet 发送通道
    pub async fn get_fixlet_for_agent_type(&self, agent_type: &str) -> Option<WsSender> {
        let map = self.by_agent_type.read().await;
        map.get(agent_type).cloned()
    }

    /// 向服务于指定 agent_type 的 fixlet 发送消息
    pub async fn send_to_fixlet_for_agent_type(
        &self,
        agent_type: &str,
        msg: &str,
    ) -> Result<(), String> {
        let sender = self
            .get_fixlet_for_agent_type(agent_type)
            .await
            .ok_or_else(|| format!("No fixlet connected for agent_type {}", agent_type))?;

        sender.send(msg.to_string()).map_err(|e| {
            format!(
                "Failed to send message to fixlet for agent_type {}: {}",
                agent_type, e
            )
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
    async fn test_register_and_send() {
        let registry = TaskRegistry::new();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register_fixlet("database.repair", tx).await;

        registry
            .send_to_fixlet_for_agent_type("database.repair", "hello")
            .await
            .unwrap();

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, "hello");
    }

    #[tokio::test]
    async fn test_send_to_unknown_agent_type_fails() {
        let registry = TaskRegistry::new();
        let result = registry
            .send_to_fixlet_for_agent_type("nonexistent", "msg")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unregister_cleanup() {
        let registry = TaskRegistry::new();

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register_fixlet("database.repair", tx).await;
        assert!(registry.get_fixlet_for_agent_type("database.repair").await.is_some());

        registry.unregister_fixlet("database.repair").await;
        assert!(registry.get_fixlet_for_agent_type("database.repair").await.is_none());
    }

    #[tokio::test]
    async fn test_pending_turn_lifecycle() {
        let registry = TaskRegistry::new();

        let (pending, mut rx) =
            PendingTurn::new("sess_1".into(), "database.repair".into(), 1, "rg_001".into());
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

    #[tokio::test]
    async fn test_unregister_fails_pending_turns_of_agent_type() {
        let registry = TaskRegistry::new();

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register_fixlet("database.repair", tx).await;

        // 两个同 agent_type 的 session 都有 pending turn
        let (p1, mut rx1) =
            PendingTurn::new("sess_a".into(), "database.repair".into(), 1, "rg1".into());
        let (p2, mut rx2) =
            PendingTurn::new("sess_b".into(), "database.repair".into(), 1, "rg2".into());
        // 另一种 agent_type 的 pending turn 不应被动
        let (p3, mut rx3) =
            PendingTurn::new("sess_c".into(), "deploy.service".into(), 1, "rg3".into());
        registry.register_pending_turn("sess_a", p1).await;
        registry.register_pending_turn("sess_b", p2).await;
        registry.register_pending_turn("sess_c", p3).await;

        // database.repair 的 fixlet 断开 → 该类型 pending turn 快速失败,deploy.service 不受影响
        registry.unregister_fixlet("database.repair").await;

        match rx1.try_recv().unwrap() {
            TurnOutcome::Failed { error_type, .. } => assert_eq!(error_type, "fixlet_disconnected"),
            other => panic!("Expected Failed for sess_a, got {:?}", other),
        }
        assert!(matches!(
            rx2.try_recv().unwrap(),
            TurnOutcome::Failed { .. }
        ));
        // deploy.service 的 turn 仍在等待
        assert!(rx3.try_recv().is_err());
        assert!(registry.take_pending_turn("sess_c").await.is_some());
    }
}
