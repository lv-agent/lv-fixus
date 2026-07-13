//! local_seq 管理
//!
//! Tool 执行已迁至 tools-bank MCP（claude-agent-acp 直接调用），
//! fixlet 不再生成 idempotency_key 或转发 tool_call。仅保留 local_seq 计数器。
//!
//! ## local_seq 规则
//!
//! - fixlet 收到 execute_turn 时初始化为 0
//! - 每次 Agent 产生事件（tool_call, final_message）时递增
//! - turn_execution_done 中报告 max_local_seq
//! - local_seq 只在当前 Turn 内有意义

// ── local_seq 管理 ──────────────────────────────────────────────────────

/// Turn 内的 local_seq 计数器
///
/// 由 fixlet 维护，Turn 结束后丢弃。
#[derive(Debug, Clone)]
pub struct LocalSeqCounter {
    current: i64,
}

impl LocalSeqCounter {
    pub fn new() -> Self {
        Self { current: 0 }
    }

    /// 获取当前值
    pub fn current(&self) -> i64 {
        self.current
    }

    /// 递增并返回新值
    pub fn next(&mut self) -> i64 {
        self.current += 1;
        self.current
    }

    /// 重置计数器（新 Turn 开始时调用）
    pub fn reset(&mut self) {
        self.current = 0;
    }
}

impl Default for LocalSeqCounter {
    fn default() -> Self {
        Self::new()
    }
}

// ── Turn 上下文 ─────────────────────────────────────────────────────────

/// fixlet 在单次 Turn 执行期间维护的上下文
///
/// Turn 结束后丢弃，不持久化。
#[derive(Debug, Clone)]
pub struct TurnContext {
    pub task_id: String,
    pub turn_id: i64,
    pub redo_group: String,
    pub redo_count: i32,
    pub local_seq: LocalSeqCounter,
    /// Agent 当前使用的 model（从 ACP session/new 响应中提取）
    pub model: String,
}

impl TurnContext {
    pub fn new(
        task_id: String,
        turn_id: i64,
        redo_group: String,
        redo_count: i32,
    ) -> Self {
        Self {
            task_id,
            turn_id,
            redo_group,
            redo_count,
            local_seq: LocalSeqCounter::new(),
            model: String::new(),
        }
    }
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_seq_counter() {
        let mut c = LocalSeqCounter::new();
        assert_eq!(c.current(), 0);
        assert_eq!(c.next(), 1);
        assert_eq!(c.next(), 2);
        assert_eq!(c.current(), 2);
        c.reset();
        assert_eq!(c.current(), 0);
    }

    #[test]
    fn test_turn_context_stores_model() {
        let mut ctx = TurnContext::new("sess_1".into(), 1, "rg_abc".into(), 0);
        assert_eq!(ctx.model, "");  // 初始为空

        ctx.model = "deepseek:deepseek-v4-pro".to_string();
        assert_eq!(ctx.model, "deepseek:deepseek-v4-pro");
    }
}
