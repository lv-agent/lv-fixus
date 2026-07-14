//! Tool adapter 抽象(CR-11)。
//!
//! 把原"6 builtins + 单一 broker 路径"重构为多源分发器:
//! - [`ActionAdapter`] trait:每个 adapter 拥有一组工具 + 自带执行路径。
//! - [`ToolRegistry`]:跨 adapter 扁平列工具、按名路由调用、撞名拒绝。
//! - [`SandboxAdapter`]:原 6 builtins,broker→sandbox 路径(逐字节 parity)。
//! - [`HttpActionAdapter`]:config-driven 外部 webhook adapter(扩展点;指向任意
//!   webhook / composio gateway)。composio 全家桶留 N1。
//!
//! 设计见 `docs/superpowers/plans/2026-07-13-cr11-external-action-adapter.md`。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use fixus_tool_catalog::{builtins, find, ToolDef, ToolSpec};
use logdb_client::broker::BrokerProducer;
use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, Mutex};

// ── 工具元数据 / 结果 / 上下文 ───────────────────────────────────────────

// `ToolDef`(MCP-facing:`{name, description, input_schema}` + serde rename
// `inputSchema`)来自 `fixus_tool_catalog` —— 它现在是工具身份的 single source
// of truth(见 `crates/tool-catalog/src/lib.rs`)。本 crate 不再自带 ToolDef,
// 避免与 catalog 出现两份逐字节漂移的定义。tools/list 直接序列化此结构,故其
// 字段名/顺序 = MCP 契约(由 catalog 保证)。

/// 统一工具执行结果。`success` 驱动 MCP `isError`;adapter 级 infra 故障用
/// [`AdapterError`] 表达(→ MCP -32603),区别于"工具执行了但失败"(→ isError)。
#[derive(Debug, Clone)]
pub struct ToolResult {
    pub success: bool,
    pub output: serde_json::Value,
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// 调用上下文(跨 adapter 契约:task_id + 幂等键)。
#[derive(Debug, Clone)]
pub struct CallCtx {
    pub task_id: String,
    pub idempotency_key: String,
    /// 透传的 effective policy(opaque JSON,tools-bank 不解析)。
    /// 来自 HTTP `X-Fixus-Policy` header,由 SandboxAdapter 写入 tool-invoke event
    /// metadata 供 sandbox-server 消费(CallCtx 本身不序列化,故无需 serde 属性)。
    pub effective_policy: Option<serde_json::Value>,
}

/// Adapter 级 infra 故障(无法到达执行器:broker down / 连接拒绝 / 超时)。
/// 与"执行器返回了结果(可能失败)"对立 —— 后者是 [`ToolResult`]。
#[derive(Debug)]
pub struct AdapterError(pub String);

/// registry.invoke 的统一错误:未知工具(-32602)/ adapter infra 故障(-32603)。
#[derive(Debug)]
pub enum InvokeError {
    NotFound,
    Adapter(String),
}

// ── ActionAdapter trait ─────────────────────────────────────────────────

/// 工具源接缝。新增外部工具 = 实现 trait + register,不动 MCP 层。
#[async_trait]
pub trait ActionAdapter: Send + Sync {
    /// adapter 名(诊断 / 撞名报错用)。
    fn name(&self) -> &str;
    /// 该 adapter 拥有的工具集。
    fn tools(&self) -> Vec<ToolDef>;
    /// 执行工具。`Ok` = 执行器应答(成功或工具级失败);`Err` = infra 故障。
    async fn invoke(
        &self,
        tool: &str,
        args: &serde_json::Value,
        ctx: &CallCtx,
    ) -> Result<ToolResult, AdapterError>;
}

// ── ToolRegistry ────────────────────────────────────────────────────────

/// 撞名信息(哪个工具已在哪个 adapter 注册)。
#[derive(Debug)]
pub struct DupTool {
    pub tool: String,
    pub existing_adapter: String,
}

/// 多源工具分发器。
///
/// `register` 时建 `tool 名 → adapter idx` 索引,故 [`ToolRegistry::find`]
/// (tools/call hot-path)是 O(1) HashMap 查;[`ToolRegistry::list`]
/// (tools/list,低频)仍遍历 adapter 取最新 tools()。工具集 per-adapter 静态
/// (sandbox builtins / http config 声明),索引不会失同步。
pub struct ToolRegistry {
    adapters: Vec<Box<dyn ActionAdapter>>,
    index: HashMap<String, usize>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            adapters: Vec::new(),
            index: HashMap::new(),
        }
    }

    /// 注册 adapter;若其任一工具名已被既有 adapter 占用,返回 [`DupTool`] 不注册。
    pub fn register(&mut self, adapter: Box<dyn ActionAdapter>) -> Result<(), DupTool> {
        let new_tools = adapter.tools();
        for t in &new_tools {
            if let Some(&existing_idx) = self.index.get(&t.name) {
                return Err(DupTool {
                    tool: t.name.clone(),
                    existing_adapter: self.adapters[existing_idx].name().to_string(),
                });
            }
        }
        let idx = self.adapters.len();
        for t in &new_tools {
            self.index.insert(t.name.clone(), idx);
        }
        self.adapters.push(adapter);
        Ok(())
    }

    /// 跨 adapter 扁平化所有工具(顺序按注册、按各 adapter.tools())。
    pub fn list(&self) -> Vec<ToolDef> {
        self.adapters.iter().flat_map(|a| a.tools()).collect()
    }

    /// 按工具名找归属 adapter(hot-path,O(1) 索引查)。
    pub fn find(&self, tool: &str) -> Option<&dyn ActionAdapter> {
        self.index.get(tool).map(|&i| self.adapters[i].as_ref())
    }

    /// 路由 + 执行。未知工具 → [`InvokeError::NotFound`];adapter infra 故障透传。
    pub async fn invoke(
        &self,
        tool: &str,
        args: &serde_json::Value,
        ctx: &CallCtx,
    ) -> Result<ToolResult, InvokeError> {
        match self.find(tool) {
            Some(a) => a
                .invoke(tool, args, ctx)
                .await
                .map_err(|e| InvokeError::Adapter(e.0)),
            None => Err(InvokeError::NotFound),
        }
    }

    /// 注册的 adapter 数(诊断)。
    pub fn adapter_count(&self) -> usize {
        self.adapters.len()
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── 纯函数:idempotency / timeout / payload ──────────────────────────────

/// canonical JSON(键排序)—— 幂等键稳定性。
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

/// 幂等键 = sha256(canonical_json(args)) 前 16 hex,带 task/tool 命名空间。
pub fn build_key(task_id: &str, tool_name: &str, args: &serde_json::Value) -> String {
    let canonical = canonical_json(args);
    let hash = hex::encode(Sha256::digest(canonical.as_bytes()).as_slice());
    format!("{}:bank:{}:{}", task_id, tool_name, &hash[..16])
}

/// per-tool 默认超时(秒)。查 catalog(已合并 builtins + extras),按全名查找;
/// 命中 → `default_timeout_secs`,未命中(外部工具 / 未知)→ 30s 兜底。
///
/// **不剥离 `fixus_` 前缀** —— catalog 用全名键(`fixus_bash` / `fixus_jq` / ...)。
/// 与原硬编码表逐项 parity:bash=30,read/write/edit/glob/grep=15(均为 catalog
/// 内置值);新增 jq/rg=15;未知工具仍 30s 兜底。
pub fn default_timeout(tool_name: &str, all_tools: &[ToolSpec]) -> u64 {
    find(all_tools, tool_name)
        .map(|s| s.default_timeout_secs)
        .unwrap_or(30)
}

/// 构造 sandbox payload(纯函数,便于 parity 快照测试)。
/// 必须与原 `tools_call` 内联构造逐字节相等(见 `sandbox_payload_byte_identical`)。
/// 注:region 不进 payload —— 它只用于 stream 名(`tool-invoke-{region}`),在
/// [`SandboxAdapter::invoke`] 里构造 stream 时用。
pub fn sandbox_payload(
    task_id: &str,
    tool_name: &str,
    tool_call_id: &str,
    args: &serde_json::Value,
    idempotency_key: &str,
    timeout_secs: u64,
) -> serde_json::Value {
    let sandbox_timeout_ms = timeout_secs.saturating_sub(5).max(5) * 1000;
    serde_json::json!({
        "step_type": "tool_call",
        "tool_name": tool_name,
        "tool_call_id": tool_call_id,
        "idempotency_key": idempotency_key,
        "input": args,
        "local_seq": 0,
        "session_id": task_id,
        "timeout_ms": sandbox_timeout_ms,
    })
}

/// 构造 tool-invoke event 的 metadata(含 effective_policy,若有)。
///
/// `task_id` / `step_id` / `event_type` 恒在;`effective_policy` 仅当 `Some` 时
/// 以序列化 JSON 字符串写入(sandbox-server 侧 `serde_json::from_str` 反序列化)。
/// 抽纯函数便于单测 + 让 `SandboxAdapter::invoke` 聚焦副作用(broker produce)。
pub fn build_invoke_meta(
    task_id: &str,
    step_id: &str,
    effective_policy: &Option<serde_json::Value>,
) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert("task_id".into(), task_id.to_string());
    meta.insert("step_id".into(), step_id.to_string());
    meta.insert("event_type".into(), "tool_invoked".into());
    if let Some(p) = effective_policy {
        meta.insert("effective_policy".into(), serde_json::to_string(p).unwrap_or_default());
    }
    meta
}

// ── PendingToolResult(sandbox oneshot 载荷;consumer 侧构造)─────────────

/// sandbox 结果帧的反序列化形态。由 main.rs 的 result consumer 构造、经
/// oneshot 喂给 [`SandboxAdapter`]。
#[derive(Debug)]
pub struct PendingToolResult {
    pub success: bool,
    pub output: serde_json::Value,
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// pending map 类型别名(consumer 与 SandboxAdapter 共享)。
pub type PendingMap = Arc<Mutex<HashMap<String, oneshot::Sender<PendingToolResult>>>>;

// ── SandboxAdapter(parity)──────────────────────────────────────────────

/// 原 8 builtins(6 originals + jq + rg)与 operator extras 的归属:broker→
/// sandbox 路径。行为与重构前对 builtins 逐字节等价(身份现由 catalog 提供)。
/// `extras` 是从 `--extra-tools` / `FIXUS_TOOLS_CATALOG_FILE` 加载的 operator
/// 外部二进制工具,与 builtins 合并后同时出现在 `tools/list` 与 timeout 查找中。
pub struct SandboxAdapter {
    pub producer: Arc<Mutex<BrokerProducer>>,
    pub pending: PendingMap,
    pub namespace: String,
    pub region: String,
    /// operator extras(与 builtins 同一 `ToolSpec` 形态,经 broker 同路径派发)。
    pub extras: Vec<ToolSpec>,
}

/// 内置工具定义(catalog 来源)。返回 8 个 ToolDef(6 originals + fixus_jq +
/// fixus_rg)。保留独立函数供 parity 快照测试用(无 broker 依赖的纯路径);
/// `SandboxAdapter::tools()` 在 extras=∅ 时与此等价。仅测试用 —— 生产路径走
/// `SandboxAdapter::tools()`(builtins ∪ extras)。
#[cfg(test)]
pub fn builtin_tools() -> Vec<ToolDef> {
    builtins().iter().map(ToolDef::from).collect()
}

#[async_trait]
impl ActionAdapter for SandboxAdapter {
    fn name(&self) -> &str {
        "sandbox"
    }

    fn tools(&self) -> Vec<ToolDef> {
        builtins()
            .iter()
            .chain(self.extras.iter())
            .map(ToolDef::from)
            .collect()
    }

    async fn invoke(
        &self,
        tool: &str,
        args: &serde_json::Value,
        ctx: &CallCtx,
    ) -> Result<ToolResult, AdapterError> {
        let step_id = uuid::Uuid::now_v7().to_string();
        let tool_call_id = uuid::Uuid::now_v7().to_string();
        // timeout 查 builtins ∪ 本 adapter 的 extras(全名查找,无前缀剥离)。
        let all: Vec<ToolSpec> = builtins().iter().chain(self.extras.iter()).cloned().collect();
        let timeout_secs = default_timeout(tool, &all);
        let stream = format!("tool-invoke-{}", self.region);
        let payload = sandbox_payload(
            &ctx.task_id,
            tool,
            &tool_call_id,
            args,
            &ctx.idempotency_key,
            timeout_secs,
        );

        tracing::info!(
            "tools-bank: dispatching {} step_id={}",
            tool,
            step_id
        );

        // Register oneshot BEFORE produce (race fix)
        let (tx, mut rx) = oneshot::channel::<PendingToolResult>();
        self.pending.lock().await.insert(step_id.clone(), tx);

        // Produce to broker
        let content = serde_json::to_vec(&payload).unwrap_or_default();
        let meta = build_invoke_meta(&ctx.task_id, &step_id, &ctx.effective_policy);

        let mut prod = self.producer.lock().await;
        if let Err(e) = prod
            .produce_full(
                &self.namespace,
                &stream,
                "tool_invoked",
                &content,
                Some(ctx.task_id.as_str()),
                0,
                "application/json",
                &meta,
            )
            .await
        {
            self.pending.lock().await.remove(&step_id);
            return Err(AdapterError(format!("broker produce failed: {}", e)));
        }
        drop(prod);

        // Wait for sandbox result
        let timeout_dur = Duration::from_secs(timeout_secs);
        match tokio::time::timeout(timeout_dur, &mut rx).await {
            Ok(Ok(r)) => Ok(ToolResult {
                success: r.success,
                output: r.output,
                error: r.error,
                duration_ms: r.duration_ms,
            }),
            Ok(Err(_)) => {
                self.pending.lock().await.remove(&step_id);
                Err(AdapterError("pending channel closed".into()))
            }
            Err(_) => {
                self.pending.lock().await.remove(&step_id);
                Err(AdapterError(format!("sandbox timeout after {}s", timeout_secs)))
            }
        }
    }
}

// ── HttpActionAdapter(外部扩展点,G2)──────────────────────────────────

/// HTTP adapter 单个外部工具的声明(schema 最小化:外部工具的真实 schema 由
/// webhook 自管,这里只给 `{"type":"object"}` 占位)。
#[derive(Debug, Clone)]
pub struct ToolAdapterTool {
    pub name: String,
    pub description: String,
}

/// HTTP adapter 配置(env 解析产物)。
#[derive(Debug, Clone)]
pub struct HttpAdapterConfig {
    pub name: String,
    pub base_url: String,
    /// 完整 Authorization 头值(如 `Bearer xxx` / `ApiKey xxx`);None 不发。
    pub auth_header: Option<String>,
    pub timeout_secs: u64,
    pub tools: Vec<ToolAdapterTool>,
}

/// config-driven 外部 webhook adapter。
///
/// `invoke`:`POST {base_url}/{tool}`,body `{tool, arguments, task_id,
/// idempotency_key}`。拿到任意 HTTP 响应 → [`ToolResult`](2xx=success,
/// 非 2xx=工具级失败);连接拒绝/超时 → [`AdapterError`](infra)。
pub struct HttpActionAdapter {
    pub cfg: HttpAdapterConfig,
    pub client: reqwest::Client,
}

#[async_trait]
impl ActionAdapter for HttpActionAdapter {
    fn name(&self) -> &str {
        &self.cfg.name
    }

    fn tools(&self) -> Vec<ToolDef> {
        self.cfg
            .tools
            .iter()
            .map(|t| ToolDef {
                name: t.name.clone(),
                description: t.description.clone(),
                input_schema: serde_json::json!({"type": "object"}),
            })
            .collect()
    }

    async fn invoke(
        &self,
        tool: &str,
        args: &serde_json::Value,
        ctx: &CallCtx,
    ) -> Result<ToolResult, AdapterError> {
        let url = format!(
            "{}/{}",
            self.cfg.base_url.trim_end_matches('/'),
            tool
        );
        let body = serde_json::json!({
            "tool": tool,
            "arguments": args,
            "task_id": ctx.task_id,
            "idempotency_key": ctx.idempotency_key,
        });
        let started = Instant::now();
        let mut req = self.client.post(&url).json(&body);
        if let Some(auth) = &self.cfg.auth_header {
            req = req.header("Authorization", auth);
        }
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let duration_ms = started.elapsed().as_millis() as u64;
                let success = status.is_success();
                let text = resp.text().await.unwrap_or_default();
                let output = serde_json::from_str::<serde_json::Value>(&text)
                    .unwrap_or_else(|_| serde_json::Value::String(text.clone()));
                let error = if success {
                    None
                } else {
                    Some(format!("http {}", status.as_u16()))
                };
                Ok(ToolResult {
                    success,
                    output,
                    error,
                    duration_ms,
                })
            }
            Err(e) => {
                let kind = if e.is_timeout() {
                    "timeout"
                } else if e.is_connect() {
                    "connect"
                } else {
                    "request"
                };
                Err(AdapterError(format!("http {} failed: {}", kind, e)))
            }
        }
    }
}

// ── config 解析(env TOOLS_BANK_HTTP_ADAPTERS)───────────────────────────
//
// 格式(分号分多条;每条管道分字段):
//   name|base_url|tool1,tool2[,..][|auth=<header value>][|timeout=<secs>]
// 例:
//   slack|http://localhost:9000|post_message,lookup|auth=Bearer x|timeout=10
//   ;lark|http://localhost:9001|send

/// 解析 env 字符串为 0..N 个 [`HttpAdapterConfig`]。非法条目跳过(不整体失败)。
pub fn parse_http_adapters(s: &str) -> Vec<HttpAdapterConfig> {
    s.split(';')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .filter_map(parse_one_http_adapter)
        .collect()
}

fn parse_one_http_adapter(entry: &str) -> Option<HttpAdapterConfig> {
    let parts: Vec<&str> = entry.split('|').map(str::trim).collect();
    if parts.len() < 3 {
        return None;
    }
    let name = parts[0].to_string();
    let base_url = parts[1].to_string();
    if name.is_empty() || base_url.is_empty() {
        return None;
    }
    let tools: Vec<ToolAdapterTool> = parts[2]
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| ToolAdapterTool {
            name: t.to_string(),
            description: format!("External tool {} via {}", t, base_url),
        })
        .collect();
    if tools.is_empty() {
        return None;
    }
    let mut auth_header = None;
    let mut timeout_secs = 15u64;
    for p in &parts[3..] {
        if let Some(v) = p.strip_prefix("auth=") {
            auth_header = Some(v.to_string());
        } else if let Some(v) = p.strip_prefix("timeout=") {
            timeout_secs = v.parse().unwrap_or(15);
        }
    }
    Some(HttpAdapterConfig {
        name,
        base_url,
        auth_header,
        timeout_secs,
        tools,
    })
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    // ── stub adapter(registry 测试用)──────────────────────────────────

    struct StubAdapter {
        name: &'static str,
        tool_names: Vec<&'static str>,
        invoke_marker: Arc<AtomicU32>,
    }

    #[async_trait]
    impl ActionAdapter for StubAdapter {
        fn name(&self) -> &str {
            self.name
        }
        fn tools(&self) -> Vec<ToolDef> {
            self.tool_names
                .iter()
                .map(|n| ToolDef {
                    name: (*n).into(),
                    description: format!("stub {}", n),
                    input_schema: serde_json::json!({"type": "object"}),
                })
                .collect()
        }
        async fn invoke(
            &self,
            tool: &str,
            _args: &serde_json::Value,
            _ctx: &CallCtx,
        ) -> Result<ToolResult, AdapterError> {
            self.invoke_marker.fetch_add(1, Ordering::Relaxed);
            Ok(ToolResult {
                success: true,
                output: serde_json::json!({"by": self.name, "tool": tool}),
                error: None,
                duration_ms: 0,
            })
        }
    }

    fn stub(name: &'static str, tools: Vec<&'static str>) -> (StubAdapter, Arc<AtomicU32>) {
        let m = Arc::new(AtomicU32::new(0));
        (
            StubAdapter {
                name,
                tool_names: tools,
                invoke_marker: m.clone(),
            },
            m,
        )
    }

    // ── CallCtx(Part C3)──

    #[test]
    fn call_ctx_carries_effective_policy() {
        // effective_policy 为 Some 时携带(opaque JSON,tools-bank 不解析)
        let ctx = CallCtx {
            task_id: "t".into(),
            idempotency_key: "k".into(),
            effective_policy: Some(serde_json::json!({"agent_role": "reader"})),
        };
        assert!(ctx.effective_policy.is_some());
        assert_eq!(ctx.effective_policy.as_ref().unwrap()["agent_role"], "reader");
        // None 也合法(缺 policy header 的旧路径)
        let ctx_none = CallCtx {
            task_id: "t".into(),
            idempotency_key: "k".into(),
            effective_policy: None,
        };
        assert!(ctx_none.effective_policy.is_none());
    }

    // ── §4.1 registry ──────────────────────────────────────────────────

    #[test]
    fn register_two_adapters_list_flattens() {
        let mut reg = ToolRegistry::new();
        reg.register(Box::new(stub("a", vec!["a1", "a2"]).0))
            .unwrap();
        reg.register(Box::new(stub("b", vec!["b1", "b2"]).0))
            .unwrap();
        let names: Vec<String> = reg.list().into_iter().map(|t| t.name).collect();
        assert_eq!(names, vec!["a1", "a2", "b1", "b2"]);
        assert_eq!(reg.adapter_count(), 2);
    }

    #[test]
    fn find_routes_to_owning_adapter() {
        let mut reg = ToolRegistry::new();
        reg.register(Box::new(stub("a", vec!["t_a"]).0)).unwrap();
        reg.register(Box::new(stub("b", vec!["t_b"]).0)).unwrap();
        assert_eq!(reg.find("t_a").map(|a| a.name()), Some("a"));
        assert_eq!(reg.find("t_b").map(|a| a.name()), Some("b"));
        assert!(reg.find("nope").is_none());
    }

    #[test]
    fn register_rejects_duplicate_tool_name() {
        let mut reg = ToolRegistry::new();
        reg.register(Box::new(stub("a", vec!["dup", "x"]).0))
            .unwrap();
        let err = reg
            .register(Box::new(stub("b", vec!["dup"]).0))
            .expect_err("dup must be rejected");
        assert_eq!(err.tool, "dup");
        assert_eq!(err.existing_adapter, "a");
        // 被拒后 b 未注册:仍只有 a 的工具
        let names: Vec<String> = reg.list().into_iter().map(|t| t.name).collect();
        assert_eq!(names, vec!["dup", "x"]);
    }

    #[tokio::test]
    async fn invoke_returns_not_found_for_unknown() {
        let reg = ToolRegistry::new();
        let err = reg
            .invoke("ghost", &serde_json::Value::Null, &CallCtx {
                task_id: "t".into(),
                idempotency_key: "k".into(),
                effective_policy: None,
            })
            .await
            .expect_err("unknown tool");
        assert!(matches!(err, InvokeError::NotFound));
    }

    #[tokio::test]
    async fn invoke_routes_to_owning_adapter() {
        let mut reg = ToolRegistry::new();
        let (a, mark_a) = stub("a", vec!["t_a"]);
        let (b, mark_b) = stub("b", vec!["t_b"]);
        reg.register(Box::new(a)).unwrap();
        reg.register(Box::new(b)).unwrap();
        let r = reg
            .invoke("t_b", &serde_json::Value::Null, &CallCtx {
                task_id: "t".into(),
                idempotency_key: "k".into(),
                effective_policy: None,
            })
            .await
            .unwrap();
        assert_eq!(r.output["by"], "b");
        assert_eq!(mark_a.load(Ordering::Relaxed), 0);
        assert_eq!(mark_b.load(Ordering::Relaxed), 1);
    }

    // ── §4.2 SandboxAdapter parity ────────────────────────────────────

    #[test]
    fn build_invoke_meta_includes_policy_when_present() {
        let meta = build_invoke_meta("t", "s", &Some(serde_json::json!({"x": 1})));
        assert_eq!(meta.get("task_id").unwrap(), "t");
        assert_eq!(meta.get("step_id").unwrap(), "s");
        assert_eq!(meta.get("event_type").unwrap(), "tool_invoked");
        // effective_policy 以序列化 JSON 字符串写入
        assert_eq!(meta.get("effective_policy").unwrap(), r#"{"x":1}"#);
    }

    #[test]
    fn build_invoke_meta_omits_policy_when_absent() {
        let meta = build_invoke_meta("t", "s", &None);
        assert_eq!(meta.get("task_id").unwrap(), "t");
        assert_eq!(meta.get("step_id").unwrap(), "s");
        // effective_policy 缺席(None)→ 不写入 key
        assert!(!meta.contains_key("effective_policy"));
    }


    #[test]
    fn sandbox_tools_list_catalog_sourced() {
        // tools/list 现以 catalog 为 source of truth:`SandboxAdapter`(无 extras)
        // 暴露 builtins()—— 6 originals + fixus_jq + fixus_rg = 8 个。无 broker
        // 依赖的纯路径直接测 `builtin_tools()`(它就是 `SandboxAdapter::tools()`
        // 在 extras=∅ 时的结果)。identity 由 catalog 保证 byte-parity。
        let tools = builtin_tools();
        assert_eq!(tools.len(), 8, "expected 6 originals + jq + rg");
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        for expected in [
            "fixus_bash",
            "fixus_read",
            "fixus_write",
            "fixus_edit",
            "fixus_glob",
            "fixus_grep",
            "fixus_jq",
            "fixus_rg",
        ] {
            assert!(names.contains(&expected), "missing builtin {}", expected);
        }
        // 每个 tool 的关键字段非空 + schema 形状(MCP 契约)
        for t in &tools {
            assert!(!t.description.is_empty(), "{}", t.name);
            assert_eq!(t.input_schema["type"], "object", "{}", t.name);
        }
    }

    #[test]
    fn sandbox_payload_byte_identical() {
        // 与原 tools_call 内联构造逐字节相等的快照。
        let args = serde_json::json!({"command": "echo hi"});
        let p = sandbox_payload(
            "task-1",
            "fixus_bash",
            "callid-abc",
            &args,
            "task-1:bank:fixus_bash:deadbeef",
            30,
        );
        assert_eq!(p["step_type"], "tool_call");
        assert_eq!(p["tool_name"], "fixus_bash");
        assert_eq!(p["tool_call_id"], "callid-abc");
        assert_eq!(p["idempotency_key"], "task-1:bank:fixus_bash:deadbeef");
        assert_eq!(p["input"], args);
        assert_eq!(p["local_seq"], 0);
        assert_eq!(p["session_id"], "task-1");
        // timeout_ms = (30 - 5) * 1000 = 25000
        assert_eq!(p["timeout_ms"], 25000);
        // 小超时:floor 到 5s
        let p2 = sandbox_payload("t", "x", "c", &args, "k", 3);
        assert_eq!(p2["timeout_ms"], 5000);
    }

    #[test]
    fn default_timeout_table_preserved() {
        // catalog 为 source:bash=30,read/write/edit/glob/grep=15,新增 jq/rg=15,
        // 未知工具 30s 兜底。无 `fixus_` 前缀剥离 —— 全名查找。
        let all: Vec<ToolSpec> = builtins().to_vec();
        assert_eq!(default_timeout("fixus_bash", &all), 30);
        assert_eq!(default_timeout("fixus_read", &all), 15);
        assert_eq!(default_timeout("fixus_write", &all), 15);
        assert_eq!(default_timeout("fixus_edit", &all), 15);
        assert_eq!(default_timeout("fixus_glob", &all), 15);
        assert_eq!(default_timeout("fixus_grep", &all), 15);
        // jq / rg 是新 builtin(均 15s)
        assert_eq!(default_timeout("fixus_jq", &all), 15);
        assert_eq!(default_timeout("fixus_rg", &all), 15);
        // 非 builtin / 未知工具默认 30s 兜底(全名不命中)
        assert_eq!(default_timeout("ext_slack", &all), 30);
        assert_eq!(default_timeout("BASH", &all), 30); // 大写不命中 → 30
    }

    #[test]
    fn build_key_stable_and_namespaced() {
        let a = serde_json::json!({"b": 1, "a": 2}); // 键序无关
        let b = serde_json::json!({"a": 2, "b": 1});
        assert_eq!(
            build_key("t1", "fixus_bash", &a),
            build_key("t1", "fixus_bash", &b)
        );
        assert!(build_key("t1", "fixus_bash", &a).starts_with("t1:bank:fixus_bash:"));
        // 不同 task / tool → 不同键
        assert_ne!(
            build_key("t1", "fixus_bash", &a),
            build_key("t2", "fixus_bash", &a)
        );
        assert_ne!(
            build_key("t1", "fixus_bash", &a),
            build_key("t1", "fixus_read", &a)
        );
    }

    // ── §4.3 HttpActionAdapter(in-process axum mock)─────────────────

    async fn spawn_mock(router: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        format!("http://{}", addr)
    }

    fn http_adapter(base_url: &str, tools: &[&str], timeout_secs: u64) -> HttpActionAdapter {
        let cfg = HttpAdapterConfig {
            name: "mock".into(),
            base_url: base_url.into(),
            auth_header: None,
            timeout_secs,
            tools: tools
                .iter()
                .map(|t| ToolAdapterTool {
                    name: (*t).into(),
                    description: "mock".into(),
                })
                .collect(),
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .build()
            .unwrap();
        HttpActionAdapter { cfg, client }
    }

    #[tokio::test]
    async fn http_invoke_round_trip() {
        use axum::extract::Json;
        use axum::http::StatusCode;
        use std::sync::Mutex;

        let received: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
        let received_c = received.clone();
        let router = axum::Router::new().route(
            "/{tool}",
            axum::routing::post(
                move |axum::extract::Path(tool): axum::extract::Path<String>,
                      Json(body): Json<serde_json::Value>| {
                    let received_c = received_c.clone();
                    async move {
                        {
                            let mut g = received_c.lock().unwrap();
                            *g = Some(serde_json::json!({"tool": tool, "body": body}));
                        }
                        (
                            StatusCode::OK,
                            Json(serde_json::json!({"ok": true, "echo_tool": tool})),
                        )
                    }
                },
            ),
        );
        let base = spawn_mock(router).await;

        let adapter = http_adapter(&base, &["ping"], 5);
        // tools() 声明
        let names: Vec<String> = adapter.tools().into_iter().map(|t| t.name).collect();
        assert_eq!(names, vec!["ping"]);

        let r = adapter
            .invoke(
                "ping",
                &serde_json::json!({"msg": "hi"}),
                &CallCtx {
                    task_id: "t1".into(),
                    idempotency_key: "k1".into(),
                    effective_policy: None,
                },
            )
            .await
            .expect("2xx → Ok");

        assert!(r.success);
        assert_eq!(r.output["ok"], true);
        assert_eq!(r.output["echo_tool"], "ping");
        assert!(r.duration_ms > 0 || r.duration_ms == 0); // 不断言阈值,只确认非缺省畸形
        // 服务端收到的 body 字段
        let recv = received.lock().unwrap().clone().unwrap();
        assert_eq!(recv["tool"], "ping");
        assert_eq!(recv["body"]["tool"], "ping");
        assert_eq!(recv["body"]["arguments"]["msg"], "hi");
        assert_eq!(recv["body"]["task_id"], "t1");
        assert_eq!(recv["body"]["idempotency_key"], "k1");
    }

    #[tokio::test]
    async fn http_invoke_non_2xx_is_tool_failure() {
        use axum::http::StatusCode;
        use axum::response::IntoResponse;

        let router = axum::Router::new().route(
            "/{tool}",
            axum::routing::post(
                move |axum::extract::Path(_tool): axum::extract::Path<String>| async move {
                    (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response()
                },
            ),
        );
        let base = spawn_mock(router).await;

        let adapter = http_adapter(&base, &["flaky"], 5);
        let r = adapter
            .invoke(
                "flaky",
                &serde_json::Value::Null,
                &CallCtx {
                    task_id: "t".into(),
                    idempotency_key: "k".into(),
                    effective_policy: None,
                },
            )
            .await
            .expect("拿到 HTTP 响应 → Ok(非 Err)");
        assert!(!r.success);
        assert!(r.error.as_deref().unwrap().contains("500"));
    }

    #[tokio::test]
    async fn http_invoke_timeout_is_adapter_error() {
        use axum::extract::Json;
        // 服务端睡 800ms,client 超时 200ms → reqwest 超时错误 → Err(AdapterError)
        let router = axum::Router::new().route(
            "/{tool}",
            axum::routing::post(
                move |axum::extract::Path(_tool): axum::extract::Path<String>| async move {
                    tokio::time::sleep(Duration::from_millis(800)).await;
                    Json(serde_json::json!({"ok": true}))
                },
            ),
        );
        let base = spawn_mock(router).await;

        let cfg = HttpAdapterConfig {
            name: "mock".into(),
            base_url: base,
            auth_header: None,
            timeout_secs: 1,
            tools: vec![ToolAdapterTool {
                name: "slow".into(),
                description: "".into(),
            }],
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .unwrap();
        let adapter = HttpActionAdapter { cfg, client };
        let err = adapter
            .invoke(
                "slow",
                &serde_json::Value::Null,
                &CallCtx {
                    task_id: "t".into(),
                    idempotency_key: "k".into(),
                    effective_policy: None,
                },
            )
            .await
            .expect_err("超时 → Err(AdapterError)");
        let msg = err.0.to_lowercase();
        assert!(
            msg.contains("timed out") || msg.contains("timeout") || msg.contains("elapsed"),
            "unexpected err msg: {}",
            err.0
        );
    }

    // ── §4.4 config 解析 ──────────────────────────────────────────────

    #[test]
    fn parse_empty_yields_no_adapters() {
        assert!(parse_http_adapters("").is_empty());
        assert!(parse_http_adapters("   ").is_empty());
        assert!(parse_http_adapters(" ; ; ").is_empty());
    }

    #[test]
    fn parse_one_adapter_with_tools() {
        let cfgs = parse_http_adapters(
            "slack|http://localhost:9000|post_message,lookup|auth=Bearer x|timeout=5",
        );
        assert_eq!(cfgs.len(), 1);
        let c = &cfgs[0];
        assert_eq!(c.name, "slack");
        assert_eq!(c.base_url, "http://localhost:9000");
        assert_eq!(c.auth_header.as_deref(), Some("Bearer x"));
        assert_eq!(c.timeout_secs, 5);
        let names: Vec<&str> = c.tools.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["post_message", "lookup"]);
    }

    #[test]
    fn parse_multiple_semicolon_separated() {
        let cfgs = parse_http_adapters(
            "slack|http://a:9000|a1;lark|http://b:9001|b1,b2|timeout=10",
        );
        assert_eq!(cfgs.len(), 2);
        assert_eq!(cfgs[0].name, "slack");
        assert_eq!(cfgs[1].name, "lark");
        assert_eq!(cfgs[1].timeout_secs, 10);
        assert_eq!(cfgs[1].tools.len(), 2);
    }

    #[test]
    fn parse_skips_malformed_entries() {
        // 字段不足 / 空 tool 列表 → 跳过,不影响其它合法条目
        let cfgs = parse_http_adapters("bad|only-two;lark|http://b:9001|b1");
        assert_eq!(cfgs.len(), 1);
        assert_eq!(cfgs[0].name, "lark");
    }

    // ── §4.5 registry + sandbox/http 共存(路由决策)─────────────────

    #[test]
    fn registry_mixed_adapters_route_correctly() {
        let mut reg = ToolRegistry::new();
        // sandbox(无需真 broker —— 只测路由,不 invoke)
        // 用 stub 模拟 sandbox 占住 6 builtins 名字
        reg.register(Box::new(
            stub(
                "sandbox",
                vec![
                    "fixus_bash",
                    "fixus_read",
                    "fixus_write",
                    "fixus_edit",
                    "fixus_glob",
                    "fixus_grep",
                ],
            )
            .0,
        ))
        .unwrap();
        // http adapter 占外部工具
        let http = HttpActionAdapter {
            cfg: HttpAdapterConfig {
                name: "ext".into(),
                base_url: "http://unused".into(),
                auth_header: None,
                timeout_secs: 5,
                tools: vec![
                    ToolAdapterTool {
                        name: "ext_slack_post".into(),
                        description: "".into(),
                    },
                    ToolAdapterTool {
                        name: "ext_lark_send".into(),
                        description: "".into(),
                    },
                ],
            },
            client: reqwest::Client::new(),
        };
        reg.register(Box::new(http)).unwrap();

        // list = 6 sandbox + 2 http
        assert_eq!(reg.list().len(), 8);
        // 路由:builtin → sandbox,external → ext
        assert_eq!(reg.find("fixus_bash").map(|a| a.name()), Some("sandbox"));
        assert_eq!(reg.find("ext_slack_post").map(|a| a.name()), Some("ext"));
        assert_eq!(reg.find("ext_lark_send").map(|a| a.name()), Some("ext"));
    }

    // ── 性能测试(#[ignore])──────────────────────────────────────────
    // registry.find() 是 tools/list + tools/call 的 hot-path(每次线性扫 adapter
    // × 各 adapter.tools())。量化大工具集下的查找成本。不断言阈值,数字供人读。

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
    fn perf_registry_find_at_scale() {
        // 1 sandbox(1 工具占位)+ 50 http adapter × 各 10 工具 = 501 工具,
        // 量化 hot-path find()(每次线性扫 adapter × 各 tools() 分配)成本。
        let mut reg = ToolRegistry::new();
        reg.register(Box::new(stub("sandbox", vec!["fixus_bash"]).0))
            .unwrap();
        for i in 0..50u32 {
            let names: Vec<&'static str> = (0..10).map(|j| leak_tool(i * 10 + j)).collect();
            let (a, _) = stub(leak_name(i), names);
            reg.register(Box::new(a)).unwrap();
        }
        // warm-up
        for _ in 0..20 {
            let _ = reg.find(leak_tool(499));
        }
        let n = 50_000;
        let mut ns = Vec::with_capacity(n);
        for i in 0..n {
            let t = std::time::Instant::now();
            let _ = reg.find(leak_tool((i % 500) as u32));
            ns.push(t.elapsed().as_nanos() as u64);
        }
        report("registry.find (51 adapters, ~501 tools)", "ns", ns);
    }

    fn leak_name(i: u32) -> &'static str {
        // 少量固定名,够 perf 用
        Box::leak(format!("a{}", i).into_boxed_str())
    }
    fn leak_tool(i: u32) -> &'static str {
        Box::leak(format!("tool{}", i).into_boxed_str())
    }
}
