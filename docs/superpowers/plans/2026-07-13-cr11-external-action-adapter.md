# CR-11:工具面广度(外部 action adapter)

> 日期:2026-07-13
> 来源:[`veps/TODO.md`](../../veps/TODO.md) CR-11(工具面广度,Tier 3,**L**)
> 纪律:**先 CR → 再 TDD 写测试 → 最后实施 → §5 perf**

---

## 1. recon 后的真实状态

### 1.1 tools-bank 当前**无任何分发抽象**

`src/bin/tools-bank/main.rs`(单文件 468 行):

- `ToolRegistry` = `Vec<ToolDef>`,`with_builtins()` 硬编码 6 工具(`fixus_bash/read/write/edit/glob/grep`)。
- `tools_call(state, task_id, tool_name, args, id)` 是**唯一执行路径**:对所有工具构造同一种 payload → produce 到 `tool-invoke-{region}` → 等 `tool-result-{region}` oneshot。**无 per-tool 路由,无 tool kind,无扩展点**。
- `ToolDef { name, description, input_schema }` 是纯元数据,**没有 handler / 没有 source 字段**。唯一的 per-tool 差异是 `default_timeout()`(硬编码 match)。
- 唯一对"外部世界"的开口是 broker(往内发,不是往外)。

⇒ agent 想动外部世界(Slack/GitHub/任意 webhook)无路径:tools-bank 只会把请求丢给本地 sandbox。

### 1.2 可借的基础设施

- `async-trait = "0.1"`(storage.rs/broker_store.rs 已用 `#[async_trait]`)—— trait 可直接用。
- `reqwest = "0.12"`(json feature)—— HTTP adapter 不需新依赖。
- tools-bank **零测试**(本 CR 补首批)。
- tools-bank "不依赖 fixus lib crate" —— trait 留在本 binary(`main.rs` 或 `bin/tools-bank/` 兄弟模块)。

### 1.3 parity 锚点(不可破坏)

SandboxAdapter 必须产出与现状**逐字节相同**的 payload(否则破坏 sandbox-server 契约):

```jsonc
{
  "step_type": "tool_call",
  "tool_name": "<tool>",
  "tool_call_id": "<uuid v7>",
  "idempotency_key": "<build_key>",
  "input": <args>,
  "local_seq": 0,
  "session_id": "<task_id>",
  "timeout_ms": <sandbox_timeout_ms>
}
```

+ produce meta(`task_id`/`step_id`/`event_type=tool_invoked`)+ oneshot 注册先于 produce(race fix)+ `default_timeout` 表。

---

## 2. 目标 / 非目标

### 目标(本 pass 落地,不接 composio)

- **G1 registry 抽象**:`ActionAdapter` trait + `ToolRegistry` 多源分发器。6 builtins 迁入 `SandboxAdapter`(parity:逐字节同 payload + 同超时表)。`tools/list` 跨 adapter 扁平化,`tools/call` 按工具名路由到归属 adapter。**内部重构,外部行为不变**。
- **G2 外部 action adapter 扩展点**:config-driven `HttpActionAdapter` —— `POST {base_url}/{tool}`,body `{tool, arguments, task_id, idempotency_key}`,返回体 = `output`。这是真实的"动外部世界"接缝(指向任意 webhook / composio gateway),无需引入 composio 依赖/账号。

### 非目标(留后续)

- **N1 composio 全家桶**:OAuth 托管 + 数百工具自动发现 —— 需 composio 账号/密钥,留后续。HttpActionAdapter 已是 composio gateway 的接入点。
- **N2 动态工具发现**(`GET {base}/.tools` 自动列工具):本 pass adapter 的工具集静态声明(per-adapter config);动态发现留后续。
- **N3 工具级鉴权/配额**:多租户下外部工具的 per-tenant 凭证路由,留 P3 鉴权。

---

## 3. 设计

### 3.1 类型

```rust
/// 工具元数据(+ 归属 adapter 名,serde 跳过,仅内部路由用)。
#[derive(Clone, Serialize)]
struct ToolDef {
    name: String,
    description: String,
    #[serde(rename = "inputSchema")] input_schema: serde_json::Value,
}

/// 统一工具执行结果(adapter 返回;MCP handler 再包 content/isError/_meta)。
struct ToolResult { success: bool, output: serde_json::Value,
                    error: Option<String>, duration_ms: u64 }

/// 调用上下文(透传 task_id + 幂等键)。
struct CallCtx { task_id: String, idempotency_key: String }

/// 接缝:每个 adapter 拥有一组工具,自带执行路径。
#[async_trait]
trait ActionAdapter: Send + Sync {
    fn name(&self) -> &str;
    fn tools(&self) -> Vec<ToolDef>;
    async fn invoke(&self, tool: &str, args: &Value, ctx: &CallCtx) -> ToolResult;
}
```

### 3.2 ToolRegistry —— 多源分发器

```rust
struct ToolRegistry { adapters: Vec<Box<dyn ActionAdapter>> }
impl ToolRegistry {
    fn new() -> Self;
    fn register(&mut self, a: Box<dyn ActionAdapter>) -> Result<(), DupTool>;  // 工具名撞名报错
    fn list(&self) -> Vec<ToolDef>;                 // 跨 adapter 扁平化
    fn find(&self, tool: &str) -> Option<&dyn ActionAdapter>;  // hot-path 路由
    async fn invoke(&self, tool, args, ctx) -> Result<ToolResult, ToolNotFound>;
}
```

路由:线性扫 adapters(数量小,实测 §5);首个声明该 tool 名的 adapter 命中。撞名在 `register` 时即拒(返回 `DupTool`,main 记 warn 跳过)。

### 3.3 SandboxAdapter(parity)

持有 broker 句柄(`Arc<Mutex<BrokerProducer>>`、`pending`、`namespace`、`region`)。`tools()` 返回原 6 builtins。`invoke()` = 原 `tools_call` 核心逻辑(oneshot 注册 → produce → 等/超时),payload 构造抽成纯函数 `sandbox_payload(task_id, tool, tool_call_id, args, region, idem) -> Value` 以便快照测试。`default_timeout`/`build_key`/`canonical_json` 归其使用。

### 3.4 HttpActionAdapter(扩展点)

config:`HttpAdapterConfig { name, base_url, auth_header: Option<String>, timeout_secs: u64, tools: Vec<ToolDef> }`。
- `invoke`:`POST {base_url}/{tool}`,header `Content-Type: application/json`(+ 可选 Authorization),body `{ "tool", "arguments", "task_id", "idempotency_key" }`。
- 2xx → `ToolResult { success: true, output: <body json or text>, .. }`;非 2xx → `success: false, error: "http <code>"`;超时/连接 → `success: false, error: <msg>`。
- 启动注册:env `TOOLS_BANK_HTTP_ADAPTERS` 解析(`name|base_url|tool1,tool2,...[|auth=...][|timeout=...]`,分号分隔多条)。空则不注册。

### 3.5 MCP 层薄化

`handle_mcp` 的 `tools/call` 改为:解析 tool_name/args/task_id → `registry.invoke(...)` → 包成 MCP response(成功 `content`+`isError`+`_meta`;未知工具 `-32602`;adapter 内部错误 `-32603`)。`build_key` 仍在调用前算(幂等键是 cross-adapter 的契约)。

---

## 4. TDD

### 4.1 registry(G1)

- [ ] `register_two_adapters_list_flattens`:注册两个 stub adapter(各 2 工具)→ `list()` 返回 4 个,顺序按注册。
- [ ] `find_routes_to_owning_adapter`:adapter A 声明 `t_a`、B 声明 `t_b` → `find("t_a")` == A,`find("t_b")` == B,`find("nope")` == None。
- [ ] `register_rejects_duplicate_tool_name`:A、B 都声明 `dup` → 第二个 `register` 返回 `Err(DupTool)`。
- [ ] `invoke_returns_not_found_for_unknown`:`invoke("ghost", ...)` → `Err(ToolNotFound)`。

### 4.2 SandboxAdapter parity(G1)

- [ ] `sandbox_tools_are_the_six_builtins`:`tools()` 返回恰好 6 个,name 集合 = `{fixus_bash, fixus_read, fixus_write, fixus_edit, fixus_glob, fixus_grep}`,且 description/schema 与原硬编码快照逐字段相等。
- [ ] `sandbox_payload_byte_identical`:`sandbox_payload(...)` 产出与原 `tools_call` 内联构造**逐字节相等**的 JSON(关键字段全在 + local_seq=0 + session_id=task_id)。
- [ ] `default_timeout_table_preserved`:bash=30 / read|write|edit|glob|grep=15 / 其它=30。

### 4.3 HttpActionAdapter(G2)

- [ ] `http_invoke_round_trip`:起 in-process axum mock webhook(记收到的 body)→ adapter.invoke → 收到 POST,body 含 tool/arguments/task_id/idempotency_key;返回体回灌 `output`;success=true;duration_ms>0。
- [ ] `http_invoke_non_2xx_is_failure`:mock 返 500 → success=false,error 含 "500"。
- [ ] `http_invoke_timeout_is_failure`:base_url 指向不可达端口 + 小 timeout → success=false,error 含超时/连接语义。
- [ ] `http_adapter_lists_declared_tools`:config 声明 2 工具 → `tools()` 返回这 2 个。

### 4.4 config 解析

- [ ] `parse_empty_yields_no_adapters`:`""` → `vec![]`。
- [ ] `parse_one_adapter_with_tools`:`name|http://x:9|a,b|auth=Bearer t|timeout=5` → 1 个 config(name/base_url/tools=[a,b]/auth=Some/timeout=5)。
- [ ] `parse_multiple_semicolon_separated`:两条用 `;` 分 → 2 个 config。

---

## 5. 实施步骤

- [ ] **CR-11a(G1)**:类型(trait/ToolRegistry/CallCtx/ToolResult/DupTool/ToolNotFound)+ SandboxAdapter(payload 纯函数 + 原 invoke 逻辑)+ MCP 层接线。§4.1+§4.2 测试。
- [ ] **CR-11b(G2)**:HttpActionAdapter + config 解析(env 注入)。§4.3+§4.4 测试。
- [ ] **CR-11c**:perf(registry `find` hot-path @ 大工具集;HttpActionAdapter 往返 @ 本地 mock)+ 全量 build/test + 勾 TODO + 提交。

---

## 6. 证据附录

### 6.1 落地范围(G1+G2;composio 留 N1)

- **G1 ✅ registry 抽象**:`adapter.rs` 新模块。`ActionAdapter` trait(`#[async_trait]`,
  `name/tools/invoke`)、`ToolRegistry` 多源分发器、`CallCtx`/`ToolResult`/`AdapterError`/
  `InvokeError`/`DupTool`。6 builtins 迁入 `SandboxAdapter`(持有 broker 句柄,`invoke` =
  原 `tools_call` 核心逻辑,payload 抽成纯函数 `sandbox_payload` 做 parity 快照)。
  MCP 层瘦化:`tools/call` → `registry.invoke` → 包 MCP(未知工具 -32602 / infra -32603 /
  `success` 驱动 `isError`);`build_key` 提到 MCP 层算(cross-adapter 幂等键)。
- **G1+ ✅ find 索引**:perf 测试发现 `find()` 每次 re-alloc 各 adapter 的 `tools()` Vec
  (p50=174µs/51 adapters)。改 `register` 时建 `tool 名 → adapter idx` HashMap,`find` O(1)
  → **p50=488ns(~350×)**。`list`(低频)仍遍历 adapter.tools()。
- **G2 ✅ HttpActionAdapter**:config-driven 外部 webhook adapter。`POST {base}/{tool}`,
  body `{tool, arguments, task_id, idempotency_key}`,可选 `Authorization` 头。
  拿到 HTTP 响应 → `ToolResult`(2xx=success / 非 2xx=工具级失败 isError);
  连接拒绝/超时 → `AdapterError`(infra,-32603)。reqwest 错误按 `is_timeout`/`is_connect`
  分桶进消息。env `TOOLS_BANK_HTTP_ADAPTERS` 解析(`name|base_url|t1,t2[|auth=..|timeout=..]`,
  分号分多条,非法条跳过)。
- **N1 ⏸ composio 全家桶**:OAuth + 数百工具自动发现需 composio 账号/密钥,留后续。
  HttpActionAdapter 已是 composio gateway / 任意 webhook 的接入点(见 §6.4)。

### 6.2 测试(全绿)

tools-bank **首批测试**(原 0 → 17 passed,1 ignored perf):

- registry §4.1(5):扁平 list / find 路由 / 撞名拒绝 / 未知工具 NotFound / invoke 路由。
- SandboxAdapter parity §4.2(4):6 builtins 名集合 / `sandbox_payload` 逐字段快照 /
  `default_timeout` 表 / `build_key` 稳定+命名空间。
- HttpActionAdapter §4.3(3,in-process axum mock):往返(body 字段 + output 回灌)/
  非 2xx→isError / 超时→AdapterError。
- config §4.4(4):空串 / 单条全字段 / 多条分号 / 非法跳过。
- 混合路由 §4.5(1):6 sandbox + 2 http 共存,路由正确。

全量 lib(跳 broker_store):**87 passed, 0 failed, 7 ignored**(lib 未触及,基线不变;
+17 在 tools-bank bin)。

### 6.3 构建

`cargo build --release` 成功(全 5 二进制)。

### 6.4 composio 接入路径(如何对接 N1)

HttpActionAdapter 是 webhook 级接缝。接 composio 两条路:

1. **gateway 模式(零代码,推荐起步)**:部署一个薄 gateway(composio SDK 转 HTTP),
   `TOOLS_BANK_HTTP_ADAPTERS=slack|http://gateway:9000|send_msg,search|auth=ApiKey $KEY`。
   agent 即获得 Slack 等工具,tools-bank 不改一行。
2. **原生 adapter 模式(N1)**:实现 `ComposioAdapter`(`tools()` 从 composio `GET /actions`
   动态拉;`invoke` 调 composio `POST /actions/execute`),`#[async_trait]` impl trait 后
   `register`。trait 已留好入口,无需碰 MCP 层 —— 这正是 CR-11 "留扩展点" 的目的。

N2 动态工具发现(`GET {base}/.tools`)同样可加进 HttpActionAdapter(周期或首次 `tools()`
时拉),不改 trait。

---

## 7. 风险

- **R1 线性扫路由**:adapter 数小(内置 1 + 少量 HTTP),线性扫够;若未来 N task-type × M external,改 HashMap<name, idx>(§5 perf 量化)。
- **R2 外部工具无沙箱**:HttpActionAdapter 把 args 直接外发 —— 由调用方 webhook 自行鉴权/限流;tools-bank 不假定外部可信。文档明示。
- **R3 工具名撞名**:两 adapter 声明同名 → `register` 拒,main 记 warn 跳过(不让后注册者静默遮蔽前者)。
- **R4 composio 不在本 pass**:HttpActionAdapter 是 webhook 级接缝;真 composio SDK(OAuth + 数百工具自动列)留 N1,本 pass 已为其留好 trait 入口。
