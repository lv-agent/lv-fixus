//! 可插拔 agent backend(CR-9)。
//!
//! fixlet 已说通用 ACP(标准 JSON-RPC over stdio);"backend" 退化为少数变异点:
//! **spawn(命令/env)+ model 提取**。本模块用薄 trait 隔离这些变异点,ACP 协议/解析层
//! (`acp.rs`)共享不变。设计见 `docs/superpowers/plans/2026-07-13-cr9-pluggable-agent-backend.md`。

use serde_json::Value;

/// 子进程启动规格。
#[derive(Debug, Clone)]
pub struct SpawnSpec {
    /// 启动命令字符串(经 `bash -c`/`cmd /C` 执行,保持与现状一致)。
    pub command: String,
    /// 额外环境变量(并入子进程 env)。
    pub env: std::collections::HashMap<String, String>,
}

/// 可插拔 agent backend。ACP 协议/解析层共享;backend 只隔离真变异点。
pub trait AgentBackend: Send + Sync {
    /// backend 名(如 "claude-code"、"generic")。
    fn name(&self) -> &str;

    /// 子进程启动规格。
    fn spawn_spec(&self) -> SpawnSpec;

    /// session/new 的 backend 特定参数(合并进 fixus 的 cwd + mcpServers 外壳)。默认空。
    fn session_new_extra(&self) -> Value {
        Value::Null
    }

    /// 从 session/new 的 result 提取 model id(backend 特定 JSON 路径)。
    fn extract_model(&self, session_new_result: &Value) -> Option<String>;
}

// ── ClaudeCodeBackend(CR-9a:提取现状)─────────────────────────────────

/// Claude Code(`claude-agent-acp`)backend。行为与重构前逐字节等价(parity)。
pub struct ClaudeCodeBackend {
    command: String,
}

impl ClaudeCodeBackend {
    pub fn new(command: String) -> Self {
        Self { command }
    }

    /// 从 env 构造:`AGENT_COMMAND`(默认 `claude-agent-acp`)。
    pub fn from_env() -> Self {
        let command = std::env::var("AGENT_COMMAND").unwrap_or_else(|_| "claude-agent-acp".into());
        Self::new(command)
    }
}

impl AgentBackend for ClaudeCodeBackend {
    fn name(&self) -> &str {
        "claude-code"
    }

    fn spawn_spec(&self) -> SpawnSpec {
        // env(ANTHROPIC_API_KEY 等)由子进程继承 fixlet env(现状如此),不显式注入。
        SpawnSpec { command: self.command.clone(), env: Default::default() }
    }

    fn extract_model(&self, r: &Value) -> Option<String> {
        r.get("models")
            .and_then(|m| m.get("currentModelId"))
            .and_then(|s| s.as_str())
            .map(String::from)
    }
}

// ── GenericAcpBackend(CR-9b:配置驱动,证明可插拔)──────────────────────

/// 通用 ACP backend:命令 + model JSON 路径全由 env 配置。
/// 加任何 ACP-speaking agent(Hermes/Codex/Cursor…)不写代码,只配 env:
///   FIXLET_BACKEND=generic AGENT_COMMAND=hermes-acp MODEL_JSON_PATH=models.currentModelId
pub struct GenericAcpBackend {
    command: String,
    /// model id 在 session/new result 里的 dotted 路径(默认 `models.currentModelId`)。
    model_path: String,
}

impl GenericAcpBackend {
    pub fn new(command: String, model_path: String) -> Self {
        Self { command, model_path }
    }

    /// 从 env:`AGENT_COMMAND` + `MODEL_JSON_PATH`(默认 `models.currentModelId`)。
    pub fn from_env() -> Self {
        let command = std::env::var("AGENT_COMMAND").unwrap_or_else(|_| "claude-agent-acp".into());
        let model_path = std::env::var("MODEL_JSON_PATH")
            .unwrap_or_else(|_| "models.currentModelId".into());
        Self::new(command, model_path)
    }
}

impl AgentBackend for GenericAcpBackend {
    fn name(&self) -> &str {
        "generic"
    }

    fn spawn_spec(&self) -> SpawnSpec {
        SpawnSpec { command: self.command.clone(), env: Default::default() }
    }

    fn extract_model(&self, r: &Value) -> Option<String> {
        json_path_str(r, &self.model_path)
    }
}

/// 按 dotted path(`a.b.c`)从 JSON Value 取字符串值。
fn json_path_str(value: &Value, path: &str) -> Option<String> {
    let mut cur = value;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    cur.as_str().map(String::from)
}

// ── 工厂 ────────────────────────────────────────────────────────────────

/// 按 backend 名选(纯函数,便于单测)。`backend_from_env` 的可测核心。
pub fn select_backend(name: &str) -> Box<dyn AgentBackend> {
    match name {
        "generic" => Box::new(GenericAcpBackend::from_env()),
        _ => Box::new(ClaudeCodeBackend::from_env()),
    }
}

/// 从 env `FIXLET_BACKEND`(默认 `claude-code`)选 backend。
pub fn backend_from_env() -> Box<dyn AgentBackend> {
    let name = std::env::var("FIXLET_BACKEND").unwrap_or_else(|_| "claude-code".into());
    select_backend(&name)
}

// ── session/new 构造(fixus 外壳 + backend 额外参数)─────────────────────

/// 构造 session/new 的 `params`。fixus 外壳(cwd + tools-bank mcpServers 注入,
/// X-Fixus-Session-Id per-task 路由)所有 backend 共享;backend 的 `session_new_extra`
/// 合并进来(ClaudeCode = 空 → 与重构前等价)。
pub fn build_session_new_params(
    backend: &dyn AgentBackend,
    task_id: &str,
    cwd: &str,
    tools_bank_url: &str,
) -> Value {
    let mut params = serde_json::json!({
        "cwd": cwd,
        "mcpServers": [{
            "type": "http",
            "name": "fixus",
            "url": tools_bank_url,
            "headers": [{"name": "X-Fixus-Session-Id", "value": task_id}]
        }]
    });
    let extra = backend.session_new_extra();
    if let Some(obj) = extra.as_object() {
        if let Some(p) = params.as_object_mut() {
            for (k, v) in obj {
                p.insert(k.clone(), v.clone());
            }
        }
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_backend_extract_model_path() {
        let b = ClaudeCodeBackend::new("claude-agent-acp".into());
        let r = serde_json::json!({"models": {"currentModelId": "claude-sonnet-5"}});
        assert_eq!(b.extract_model(&r).as_deref(), Some("claude-sonnet-5"));
        // 缺字段
        assert_eq!(b.extract_model(&serde_json::json!({"foo": 1})), None);
        assert_eq!(b.extract_model(&serde_json::json!({"models": {}})), None);
    }

    #[test]
    fn claude_backend_spawn_spec_command() {
        let b = ClaudeCodeBackend::new("npx claude-agent-acp".into());
        assert_eq!(b.spawn_spec().command, "npx claude-agent-acp");
        assert!(b.spawn_spec().env.is_empty());
    }

    #[test]
    fn claude_backend_name() {
        let b = ClaudeCodeBackend::new("x".into());
        assert_eq!(b.name(), "claude-code");
    }

    #[test]
    fn session_new_extra_default_null() {
        let b = ClaudeCodeBackend::new("x".into());
        assert_eq!(b.session_new_extra(), Value::Null);
    }

    #[test]
    fn select_backend_default_is_claude_code() {
        assert_eq!(select_backend("claude-code").name(), "claude-code");
        assert_eq!(select_backend("unknown").name(), "claude-code"); // 未知 → 默认 claude-code
    }

    // ── GenericAcpBackend(CR-9b)──

    #[test]
    fn generic_backend_default_model_path() {
        let b = GenericAcpBackend::new("hermes-acp".into(), "models.currentModelId".into());
        let r = serde_json::json!({"models": {"currentModelId": "hermes-4"}});
        assert_eq!(b.extract_model(&r).as_deref(), Some("hermes-4"));
        assert_eq!(b.name(), "generic");
        assert_eq!(b.spawn_spec().command, "hermes-acp");
    }

    #[test]
    fn generic_backend_custom_dotted_model_path() {
        // Codex 风格:model 在 data.model
        let b = GenericAcpBackend::new("codex --acp".into(), "data.model".into());
        let r = serde_json::json!({"data": {"model": "gpt-5"}});
        assert_eq!(b.extract_model(&r).as_deref(), Some("gpt-5"));
        // 路径不存在 → None
        assert_eq!(b.extract_model(&serde_json::json!({"foo": 1})), None);
    }

    #[test]
    fn json_path_str_walks_segments() {
        let v = serde_json::json!({"a": {"b": {"c": "deep"}}});
        assert_eq!(json_path_str(&v, "a.b.c").as_deref(), Some("deep"));
        assert_eq!(json_path_str(&v, "a.x").as_deref(), None); // 中段缺失
        assert_eq!(json_path_str(&v, "a.b.c.d").as_deref(), None); // 终段非对象
    }

    #[test]
    fn select_backend_generic_branch() {
        // 注:select_backend("generic") 内部走 from_env(读 AGENT_COMMAND);此处仅验分支命中
        let b = select_backend("generic");
        assert_eq!(b.name(), "generic");
    }

    /// parity(CR-9a 核心):build_session_new_params 对 ClaudeCode backend 必须与
    /// 重构前 router.rs 硬编码的 session/new params 逐键等价(零回归)。
    #[test]
    fn build_session_new_params_matches_legacy_claude() {
        let b = ClaudeCodeBackend::new("claude-agent-acp".into());
        let got = build_session_new_params(&b, "task_abc", "/tmp/work", "http://127.0.0.1:3001/mcp");

        // 重构前 router.rs:440 硬编码的等价 params(legacy oracle)
        let legacy = serde_json::json!({
            "cwd": "/tmp/work",
            "mcpServers": [{
                "type": "http",
                "name": "fixus",
                "url": "http://127.0.0.1:3001/mcp",
                "headers": [{"name": "X-Fixus-Session-Id", "value": "task_abc"}]
            }]
        });
        assert_eq!(got, legacy, "session/new params 必须与重构前等价");
    }

    /// session_new_extra 非 null 时合并进 params(为 CR-9b/后续 backend 验证扩展点)。
    #[test]
    fn build_session_new_params_merges_extra() {
        struct WithExtra;
        impl AgentBackend for WithExtra {
            fn name(&self) -> &str { "test-extra" }
            fn spawn_spec(&self) -> SpawnSpec { SpawnSpec { command: "x".into(), env: Default::default() } }
            fn session_new_extra(&self) -> Value { serde_json::json!({"model": "gpt-5", "mode": "plan"}) }
            fn extract_model(&self, _: &Value) -> Option<String> { None }
        }
        let got = build_session_new_params(&WithExtra, "t", "/tmp", "http://tb");
        assert_eq!(got["model"], "gpt-5");
        assert_eq!(got["mode"], "plan");
        assert_eq!(got["mcpServers"][0]["headers"][0]["value"], "t"); // 外壳仍在
    }
}
