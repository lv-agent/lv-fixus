//! idempotency_key 和 local_seq 管理
//!
//! ## idempotency_key 生成规则
//!
//! ```
//! idempotency_key = "{task_id}:{redo_group}:{tool_name}:{canonical_input_hash}"
//! ```
//!
//! Turn 重做时，redo_group 不变，相同 Tool 对相同参数产生相同的 key。
//! Tool 实现侧检查此 key，若已执行过则返回缓存结果或幂等成功。
//!
//! ## local_seq 规则
//!
//! - fixlet 收到 execute_turn 时初始化为 0
//! - 每次 Agent 产生事件（tool_call, final_message）时递增
//! - turn_execution_done 中报告 max_local_seq
//! - local_seq 只在当前 Turn 内有意义

use sha2::{Digest, Sha256};
use uuid::Uuid;

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

// ── step_id 生成 ────────────────────────────────────────────────────────

/// 生成新的 step_id（ULID）
///
/// step_id 在 Session 内全局唯一，使用 UUID v7 保证排序。
pub fn generate_step_id() -> String {
    Uuid::now_v7().to_string()
}

// ── idempotency_key 生成 ────────────────────────────────────────────────

/// 构建 idempotency_key
///
/// 格式: `{task_id}:{redo_group}:{tool_name}:{canonical_hash}`
///
/// canonical_hash 是规范化输入参数的 SHA256 前 16 位（hex）。
pub fn build_idempotency_key(
    task_id: &str,
    redo_group: &str,
    tool_name: &str,
    input: &serde_json::Value,
) -> String {
    let hash = canonical_input_hash(input);
    format!("{}:{}:{}:{}", task_id, redo_group, tool_name, hash)
}

/// 计算输入参数的规范化哈希
///
/// 使用 serde_json 的排序序列化保证确定性。
fn canonical_input_hash(input: &serde_json::Value) -> String {
    let canonical = canonical_json(input);
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let result = hasher.finalize();
    hex::encode(&result[..8]) // SHA256 前 16 位 hex
}

/// 将 JSON 规范化为确定性的字符串形式
///
/// 按 key 排序、无多余空格。
fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let items: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        canonical_json(&map[*k])
                    )
                })
                .collect();
            format!("{{{}}}", items.join(","))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(canonical_json).collect();
            format!("[{}]", items.join(","))
        }
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".into()),
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

    /// 为下一个事件生成 step_id 和 idempotency_key
    pub fn prepare_tool_call(
        &mut self,
        tool_name: &str,
        tool_call_id: &str,
        arguments: &serde_json::Value,
    ) -> ToolCallMeta {
        let step_id = generate_step_id();
        let local_seq = self.local_seq.next();
        let idempotency_key = build_idempotency_key(
            &self.task_id,
            &self.redo_group,
            tool_name,
            arguments,
        );

        ToolCallMeta {
            step_id,
            local_seq,
            idempotency_key,
            tool_call_id: tool_call_id.to_string(),
            tool_name: tool_name.to_string(),
        }
    }
}

/// Tool 调用元数据（fixlet 在收到 ACP tool_call 时生成）
#[derive(Debug, Clone)]
pub struct ToolCallMeta {
    pub step_id: String,
    pub local_seq: i64,
    pub idempotency_key: String,
    pub tool_call_id: String,
    pub tool_name: String,
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
    fn test_step_id_uniqueness() {
        let id1 = generate_step_id();
        let id2 = generate_step_id();
        assert_ne!(id1, id2);
        // UUID v7 格式校验：应该是 36 字符的 UUID
        assert_eq!(id1.len(), 36);
    }

    #[test]
    fn test_canonical_json_deterministic() {
        // 相同内容不同 key 顺序应产生相同结果
        let a = serde_json::json!({"b": 1, "a": 2});
        let b = serde_json::json!({"a": 2, "b": 1});
        assert_eq!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn test_idempotency_key_deterministic() {
        let input = serde_json::json!({"command": "echo hello"});

        let key1 = build_idempotency_key("sess_1", "rg_abc", "Bash", &input);
        let key2 = build_idempotency_key("sess_1", "rg_abc", "Bash", &input);

        assert_eq!(key1, key2);
        assert!(key1.starts_with("sess_1:rg_abc:Bash:"));
    }

    #[test]
    fn test_idempotency_key_differs_per_input() {
        let input1 = serde_json::json!({"command": "echo hello"});
        let input2 = serde_json::json!({"command": "echo world"});

        let key1 = build_idempotency_key("sess_1", "rg_abc", "Bash", &input1);
        let key2 = build_idempotency_key("sess_1", "rg_abc", "Bash", &input2);

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_idempotency_key_stable_across_redo() {
        // 重做时 redo_group 不变，key 不变
        let input = serde_json::json!({"order_id": "12345"});

        // 首次
        let key1 = build_idempotency_key("sess_1", "rg_abc", "create_order", &input);
        // 重做（redo_group 相同）
        let key2 = build_idempotency_key("sess_1", "rg_abc", "create_order", &input);

        assert_eq!(key1, key2);
    }

    #[test]
    fn test_canonical_json_handles_nested() {
        let a = serde_json::json!({
            "outer": {
                "inner": [1, 2, 3],
                "name": "test"
            }
        });
        let b = serde_json::json!({
            "outer": {
                "name": "test",
                "inner": [1, 2, 3]
            }
        });
        assert_eq!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn test_turn_context_prepare_tool_call() {
        let mut ctx = TurnContext::new(
            "sess_1".into(),
            1,
            "rg_abc".into(),
            0,
        );

        let meta = ctx.prepare_tool_call(
            "Bash",
            "call_42",
            &serde_json::json!({"command": "echo hello"}),
        );

        assert_eq!(meta.local_seq, 1);
        assert_eq!(meta.tool_name, "Bash");
        assert_eq!(meta.tool_call_id, "call_42");
        assert!(meta.idempotency_key.starts_with("sess_1:rg_abc:Bash:"));
        assert!(!meta.step_id.is_empty());
    }

    #[test]
    fn test_turn_context_stores_model() {
        let mut ctx = TurnContext::new("sess_1".into(), 1, "rg_abc".into(), 0);
        assert_eq!(ctx.model, "");  // 初始为空

        ctx.model = "deepseek:deepseek-v4-pro".to_string();
        assert_eq!(ctx.model, "deepseek:deepseek-v4-pro");
    }
}
