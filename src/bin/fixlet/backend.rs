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

/// Claude Code 内置工具全集(`claude_code` preset)。session/new 经
/// `_meta.claudeCode.options.disallowedTools` 全部 deny → 这些工具从模型上下文移除,
/// 模型只剩 `mcp__fixus__*`(8 工具),必走 fixus 工具链(tools-bank→broker→sandbox)。
///
/// 多列无害(SDK 语义:deny 一个不存在的工具是 no-op)。`AskUserQuestion` 由 ACP 自身
/// 基线 deny(`acp-agent.js` createSession),此处不重复。
const NATIVE_CLAUDE_TOOLS: &[&str] = &[
    "Bash", "Read", "Write", "Edit", "MultiEdit", "Glob", "Grep",
    "NotebookEdit", "TodoWrite", "WebSearch", "WebFetch",
    "Task", "Agent", "BashOutput", "KillShell", "ExitPlanMode",
];

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

    /// session/new 注入 `_meta.claudeCode.options.disallowedTools = NATIVE_CLAUDE_TOOLS`
    /// → claude-agent-acp 把这些内置工具名(Read/Write/Edit/Bash/Glob/Grep/Task/…)从模型
    /// 上下文移除(SDK `disallowedTools` 语义:removed from the model's context)。preset
    /// 仍加载、`mcpServers`(fixus)正常发现,于是模型只看到 `mcp__fixus__*`(8 工具),必走
    /// tools-bank→broker→sandbox-broker→lv-sandbox 工具链。bypassPermissions 默认开
    /// (`ALLOW_BYPASS`),fixus 工具免审批不卡死。
    ///
    /// 修的是"agent 永远优先用内置工具"的经典坑:仅加 `fixus_` 前缀只消除名字碰撞,不足以让
    /// agent 放弃原生工具。此处用 `disallowedTools` 把原生工具从上下文剔除,agent 别无选择。
    ///
    /// **为何不用 `_meta.disableBuiltInTools=true`(→ SDK `tools=[]`)**:实测会让 agent 进
    /// 退化态——MCP 工具不再被发现(tools-bank 0 请求)、model 调用不发起(agent 无 socket)、
    /// turn 无限卡死。`tools=[]` 这条路径在本 agent(model=glm-5.2 经 claude-agent-acp 0.23.1)
    /// 下不可用。`disallowedTools` 走另一条路径(canUseTool 拒绝 + 上下文移除),preset 正常
    /// 加载、MCP 正常发现,避开了退化态。消费点:`acp-agent.js` createSession line ~985 合并
    /// `userProvidedOptions.disallowedTools`。
    fn session_new_extra(&self) -> Value {
        serde_json::json!({
            "_meta": {
                "claudeCode": {
                    "options": { "disallowedTools": NATIVE_CLAUDE_TOOLS }
                }
            }
        })
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
    turn_id: i64,
    effective_policy: Option<String>,
) -> Value {
    // headers 动态构造:
    // - X-Fixus-Session-Id(per-task 路由)恒在
    // - X-Fixus-Turn-Id(本 turn 标识,tools-bank 用它关联 tool 事件到 turn)恒在
    // - X-Fixus-Policy(effective policy JSON 字符串)仅当 policy 存在时注入
    let mut headers = vec![
        serde_json::json!({"name": "X-Fixus-Session-Id", "value": task_id}),
        serde_json::json!({"name": "X-Fixus-Turn-Id", "value": turn_id.to_string()}),
    ];
    if let Some(p) = effective_policy {
        headers.push(serde_json::json!({"name": "X-Fixus-Policy", "value": p}));
    }
    let mut params = serde_json::json!({
        "cwd": cwd,
        "mcpServers": [{
            "type": "http",
            "name": "fixus",
            "url": tools_bank_url,
            "headers": headers
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

/// 构造完整的 session/new JSON-RPC 请求(envelope + params)。抽出来便于
/// parity 测试(完整消息字节级回归)。
pub fn build_session_new_request(req_id: i64, params: Value) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "session/new",
        "params": params,
    })
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

    /// ClaudeCode backend 在 session/new 注入 `_meta.claudeCode.options.disallowedTools`
    /// (把内置工具从模型上下文剔除,强制走 fixus 工具链)。见 ClaudeCodeBackend::session_new_extra。
    #[test]
    fn claude_backend_session_new_extra_disables_builtins() {
        let b = ClaudeCodeBackend::new("x".into());
        let extra = b.session_new_extra();
        let denied = extra["_meta"]["claudeCode"]["options"]["disallowedTools"]
            .as_array()
            .expect("disallowedTools 应为数组");
        // 关键内置工具都在 deny 列表里(Bash/Read/Write/Edit/Glob/Grep)
        for must_deny in ["Bash", "Read", "Write", "Edit", "Glob", "Grep"] {
            assert!(
                denied.iter().any(|v| v.as_str() == Some(must_deny)),
                "{must_deny} 应在 disallowedTools 中"
            );
        }
        // 确认与常量一致
        assert_eq!(denied.len(), NATIVE_CLAUDE_TOOLS.len());
    }

    /// trait 默认 session_new_extra 仍为 Null:GenericAcpBackend 等不注入 disableBuiltInTools
    /// (那是 claude-agent-acp 特定的 `_meta` 字段,通用 ACP agent 无此语义)。
    #[test]
    fn session_new_extra_default_null_for_generic() {
        let b = GenericAcpBackend::new("x".into(), "models.currentModelId".into());
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

    // ── 性能测试(#[ignore],cargo test --bin fixlet -- --ignored perf_ --nocapture)──
    // backend 层是 per-turn 一次性(spawn_spec + build_session_new_params + extract_model),
    // 非热路径、无锁。测一 turn 的 backend 总开销,证明新抽象层相对秒级 agent 执行可忽略。

    fn report(name: &str, unit: &str, mut samples: Vec<u64>) {
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
    fn perf_backend_per_turn_overhead() {
        let b = ClaudeCodeBackend::new("claude-agent-acp".into());
        let result = serde_json::json!({"sessionId": "s1", "models": {"currentModelId": "claude-sonnet-5"}});
        // warm-up
        for _ in 0..1000 {
            let _ = b.spawn_spec();
            let p = build_session_new_params(&b, "t", "/tmp", "http://tb/mcp", 1, None);
            let _ = build_session_new_request(1, p);
            let _ = b.extract_model(&result);
        }
        let n = 20_000;
        let mut ns = Vec::with_capacity(n);
        for i in 0..n {
            let t0 = std::time::Instant::now();
            let _ = b.spawn_spec();
            let p = build_session_new_params(&b, &format!("t{i}"), "/tmp", "http://tb/mcp", 1, None);
            let _ = build_session_new_request(i as i64, p);
            let m = b.extract_model(&result);
            ns.push(t0.elapsed().as_nanos() as u64);
            assert_eq!(m.as_deref(), Some("claude-sonnet-5")); // 功能正确
        }
        report("backend per-turn (spawn+new+model)", "ns", ns);
    }

    #[test]
    #[ignore]
    fn perf_generic_json_path_extract() {
        // GenericAcpBackend 的 dotted 路径解析(model 提取)—— 自定义路径下性能。
        let b = GenericAcpBackend::new("x".into(), "data.response.model".into());
        let result = serde_json::json!({"data": {"response": {"model": "gpt-5", "meta": {"x": 1}}}});
        for _ in 0..1000 {
            let _ = b.extract_model(&result);
        }
        let n = 50_000;
        let mut ns = Vec::with_capacity(n);
        for _ in 0..n {
            let t0 = std::time::Instant::now();
            let m = b.extract_model(&result);
            ns.push(t0.elapsed().as_nanos() as u64);
            assert_eq!(m.as_deref(), Some("gpt-5"));
        }
        report("generic extract_model (3-seg path)", "ns", ns);
    }

    /// 形状锁定(原 CR-9a parity):build_session_new_params 对 ClaudeCode backend 产出的
    /// session/new params —— cwd + mcpServers 外壳 + `_meta.claudeCode.options.disallowedTools`
    /// (禁内置工具,见 ClaudeCodeBackend::session_new_extra)。锁住 wire 形状防漂移。
    #[test]
    fn build_session_new_params_matches_legacy_claude() {
        let b = ClaudeCodeBackend::new("claude-agent-acp".into());
        let got = build_session_new_params(&b, "task_abc", "/tmp/work", "http://127.0.0.1:3001/mcp", 42, None);

        // 外壳 + headers(X-Fixus-Session-Id + X-Fixus-Turn-Id)+ 禁内置工具 disallowedTools
        assert_eq!(got["cwd"], "/tmp/work");
        assert_eq!(got["mcpServers"][0]["name"], "fixus");
        assert_eq!(got["mcpServers"][0]["url"], "http://127.0.0.1:3001/mcp");
        assert_eq!(got["mcpServers"][0]["headers"][0]["name"], "X-Fixus-Session-Id");
        assert_eq!(got["mcpServers"][0]["headers"][0]["value"], "task_abc");
        assert_eq!(got["mcpServers"][0]["headers"][1]["name"], "X-Fixus-Turn-Id");
        assert_eq!(got["mcpServers"][0]["headers"][1]["value"], "42");
        // 禁内置工具:_meta.claudeCode.options.disallowedTools == NATIVE_CLAUDE_TOOLS
        assert_eq!(
            got["_meta"]["claudeCode"]["options"]["disallowedTools"],
            serde_json::Value::from(NATIVE_CLAUDE_TOOLS),
            "session/new 应注入 disallowedTools"
        );
    }

    /// 形状锁定(完整消息):build_session_new_request 包出的完整 JSON-RPC envelope
    /// (jsonrpc/id/method/params),params 含 disallowedTools(见 session_new_extra)。
    #[test]
    fn build_session_new_request_matches_legacy_envelope() {
        let b = ClaudeCodeBackend::new("claude-agent-acp".into());
        let params = build_session_new_params(&b, "task_x", "/tmp", "http://tb/mcp", 7, None);
        let got = build_session_new_request(7, params);

        assert_eq!(got["jsonrpc"], "2.0");
        assert_eq!(got["id"], 7);
        assert_eq!(got["method"], "session/new");
        assert_eq!(got["params"]["cwd"], "/tmp");
        assert_eq!(got["params"]["mcpServers"][0]["name"], "fixus");
        assert_eq!(got["params"]["mcpServers"][0]["headers"][0]["name"], "X-Fixus-Session-Id");
        assert_eq!(got["params"]["mcpServers"][0]["headers"][0]["value"], "task_x");
        assert_eq!(got["params"]["mcpServers"][0]["headers"][1]["name"], "X-Fixus-Turn-Id");
        assert_eq!(got["params"]["mcpServers"][0]["headers"][1]["value"], "7");
        assert_eq!(
            got["params"]["_meta"]["claudeCode"]["options"]["disallowedTools"],
            serde_json::Value::from(NATIVE_CLAUDE_TOOLS)
        );
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
        let got = build_session_new_params(&WithExtra, "t", "/tmp", "http://tb", 1, None);
        assert_eq!(got["model"], "gpt-5");
        assert_eq!(got["mode"], "plan");
        assert_eq!(got["mcpServers"][0]["headers"][0]["value"], "t"); // 外壳仍在
    }

    // ── X-Fixus-Turn-Id header(step-events Phase 3)──

    #[test]
    fn session_new_params_include_turn_id_header() {
        let backend = select_backend("claude-code");
        let params = build_session_new_params(backend.as_ref(), "task-123", "/cwd", "http://tools-bank", 42, None);
        let headers = params["mcpServers"][0]["headers"].as_array().unwrap();
        let turn_id_hdr = headers.iter().find(|h| h["name"] == "X-Fixus-Turn-Id").unwrap();
        assert_eq!(turn_id_hdr["value"], "42");
    }

    // ── X-Fixus-Policy header 注入(Part C2)──

    #[test]
    fn session_new_params_include_policy_header_when_present() {
        let b = ClaudeCodeBackend::new("claude-agent-acp".into());
        let policy = serde_json::json!({
            "fs": {"read_paths": [], "write_paths": []},
            "net": {"egress": []},
            "agent_role": "reader"
        });
        let params = build_session_new_params(
            &b,
            "task_x",
            "/tmp",
            "http://tb/mcp",
            1,
            Some(policy.to_string()),
        );
        let headers = params["mcpServers"][0]["headers"].as_array().unwrap();
        // X-Fixus-Session-Id 仍在
        assert!(headers.iter().any(|h| h["name"] == "X-Fixus-Session-Id"));
        // X-Fixus-Policy 注入,值 = policy 序列化字符串
        let policy_hdr = headers
            .iter()
            .find(|h| h["name"] == "X-Fixus-Policy")
            .expect("X-Fixus-Policy header 应存在");
        assert_eq!(policy_hdr["value"], policy.to_string());
    }

    #[test]
    fn session_new_params_omit_policy_header_when_absent() {
        let b = ClaudeCodeBackend::new("claude-agent-acp".into());
        let params = build_session_new_params(&b, "task_x", "/tmp", "http://tb/mcp", 1, None);
        let headers = params["mcpServers"][0]["headers"].as_array().unwrap();
        assert!(!headers.iter().any(|h| h["name"] == "X-Fixus-Policy"));
    }
}
