# CR-9:可插拔 agent backend 抽象

> 日期:2026-07-13
> 来源:[`veps/TODO.md`](../../veps/TODO.md) CR-9(可插拔 agent backend 抽象,Tier 3,**L**)
> 范围:`src/bin/fixlet/`(acp.rs / router.rs / main.rs + 新 backend 模块)
> 纪律:**先 CR → 再 TDD 写测试 → 最后实施**(本文是第一步)

---

## 1. 问题(代码取证后的真实状态)

### 1.1 fixlet 已说通用 ACP,"backend" 退化成一个字符串

取证(`src/bin/fixlet/` 全 4 文件):

- `FixletConfig { task_type, agent_command, agent_cwd }`(`router.rs:43`),`agent_command` 默认 `"claude-agent-acp"`(`main.rs:46`)。
- `spawn_agent`(`router.rs:66`):`bash -c <agent_command>` + stdio piped。**完全通用**——任何 stdio agent 都能跑。
- ACP 客户端(`acp.rs`):标准 JSON-RPC(initialize / session/new / session/prompt / session/cancel / ping)+ `parse_acp_message` 处理 `session/update` 事件。**协议层本就通用**。
- 多 backend 现状:**进程级**——一个 fixlet = 一个 `task_type` = 一个 `agent_command`。跑 Claude + Hermes = 起两个 fixlet 进程。代码里**零** backend 分支、**零** Hermes/Codex 痕迹(`grep -rn hermes|codex src/` 全空)。

⇒ "ACP 偏 Claude Code" 不是协议层偏,而是**少数 Claude 特定点硬编码在通用路径里**。

### 1.2 真正的 Claude 耦合点(仅 3 处)

| 位置 | Claude 特定 | 通用化方式 |
|------|------------|-----------|
| `acp.rs:410` `session_new` 读 `ANTHROPIC_MODEL` | model env 名 | 实际 live 路径(`router.rs:440` 发裸 session/new)**根本没走这个**——已是死代码;移到 backend |
| `router.rs:457` model 提取 `result.models.currentModelId` | JSON 路径 | 进 backend `extract_model` |
| `router.rs:440` session/new 的 `mcpServers`(tools-bank + `X-Fixus-Session-Id`) | **非 backend 特定**——是 fixus per-task 工具路由,所有 agent 都要 | 留在 router(fixus 外壳),不进 backend |

`parse_acp_message` 的事件名(`agent_message_chunk`/`tool_call`/`stopReason`/`usage.inputTokens`)是 **ACP 规范名**,合规 agent 应一致;**不进 backend**(否则每个 backend 重写解析,违背"加 agent = 加一个文件")。

### 1.3 结论:CR-9 是"抽薄 trait + 提取 Claude + 配置化通用 backend",不是"造 18 个 backend"

multica `pkg/agent/*.go` 18 个 backend 是因为它接 18 家**协议各异**的 CLI。fixus 走 ACP 统一协议,变异点只有 **spawn(命令/env)+ model 提取**,trait 极薄。诚实交付 = 抽 trait + 提 ClaudeCodeBackend + 加一个**配置驱动的通用 backend**(证明"加 agent = env 配置,不动核心")。

---

## 2. 目标 / 非目标

### 目标

- **G1 `AgentBackend` trait**:隔离真变异点(spawn 规格 + model 提取 + session/new 额外参数),ACP 协议/解析层共享不变。
- **G2 提取 `ClaudeCodeBackend`**:当前行为零改变地搬进一个 backend 模块(parity 测试保证)。
- **G3 `GenericAcpBackend`(配置驱动)**:命令/env/model 路径全由 env 配置——加任何 ACP agent 不写代码,只配 env。直接交付 CR-9"加 agent = 一个文件/配置"。
- **G4 backend 选择**:env `FIXLET_BACKEND`(默认 `claude-code`|`generic`);`FixletConfig` 持 `Arc<dyn AgentBackend>`。
- **G5 TDD parity**:提取 ClaudeCodeBackend 后,router 的 session/new 构造、model 提取、spawn 命令与现状逐字节等价(行为零回归)。

### 非目标(显式排除)

- **N1 不接真实第二 agent**:Hermes/Codex 的 ACP 具体差异未知(代码里无痕迹);通用性靠 GenericAcpBackend + 测试证明,不靠猜一个 Codex backend。真实第二 backend = 后续 CR-9c(需可测的真 agent)。
- **N2 不做 per-task_type 多 backend 同进程**:一 fixlet 一 backend(同进程多 backend 需大改 router 的 agent 进程复用模型,且与 pull-based 单 stream 订阅冲突)。多 backend 仍进程级。
- **N3 不改 ACP 协议层 / parse_acp_message**:事件解析保持通用;不为单 backend 加分支(防"每 backend 重写解析"反模式)。
- **N4 不动 tools-bank mcpServers 注入**:那是 fixus per-task 工具路由,所有 backend 共享,留 router。
- **N5 不做 backend 热加载 / 运行时切换**:启动时按 env 选一次。

---

## 3. 设计

### 3.1 `AgentBackend` trait(新 `backend.rs`)

```rust
use std::collections::HashMap;
use serde_json::Value;

/// 子进程启动规格。
#[derive(Debug, Clone)]
pub struct SpawnSpec {
    /// 启动命令字符串(经 `bash -c` 执行,保持与现状一致)。
    pub command: String,
    /// 额外环境变量(并入子进程 env;不覆盖 fixlet 自身 env 除非显式)。
    pub env: HashMap<String, String>,
}

/// 可插拔 agent backend。ACP 协议/解析层共享;backend 只隔离真变异点。
pub trait AgentBackend: Send + Sync {
    /// backend 名(如 "claude-code"、"generic")。
    fn name(&self) -> &str;

    /// 子进程启动规格。
    fn spawn_spec(&self) -> SpawnSpec;

    /// session/new 的 backend 特定参数(合并进 fixus 的 cwd + mcpServers 外壳)。
    /// 默认空(无额外参数)。
    fn session_new_extra(&self) -> Value { Value::Null }

    /// 从 session/new 的 result 提取 model id(backend 特定 JSON 路径)。
    fn extract_model(&self, session_new_result: &Value) -> Option<String>;
}
```

**为何这么薄**:ACP 协议通用(§1.2),变异只有 spawn + model 提取。`session_new_extra` 留扩展点(如某 backend 要传 `mode`/`permissionMode`)。

### 3.2 `ClaudeCodeBackend`(提取现状)

```rust
pub struct ClaudeCodeBackend {
    command: String,   // 默认 "claude-agent-acp",可 env 覆盖
    cwd: Option<String>,
}

impl AgentBackend for ClaudeCodeBackend {
    fn name(&self) -> &str { "claude-code" }
    fn spawn_spec(&self) -> SpawnSpec {
        SpawnSpec { command: self.command.clone(), env: HashMap::new() }
        // env: ANTHROPIC_API_KEY 等由子进程继承 fixlet env(现状如此),不显式注入
    }
    fn extract_model(&self, r: &Value) -> Option<String> {
        r.get("models").and_then(|m| m.get("currentModelId")).and_then(|s| s.as_str()).map(String::from)
    }
}
```

router 的 session/new 构造(mcpServers 外壳)留在 router;ClaudeCodeBackend 不带 `session_new_extra`(空)。**spawn_spec.command = agent_command(env AGENT_COMMAND,默认 claude-agent-acp)** —— 与现状逐字节等价。

### 3.3 `GenericAcpBackend`(配置驱动,证明可插拔)

```rust
pub struct GenericAcpBackend {
    command: String,              // env AGENT_COMMAND(必填)
    model_path: String,           // env MODEL_JSON_PATH,默认 "models.currentModelId"
}

impl AgentBackend for GenericAcpBackend {
    fn name(&self) -> &str { "generic" }
    fn spawn_spec(&self) -> SpawnSpec { SpawnSpec { command: self.command.clone(), env: HashMap::new() } }
    fn extract_model(&self, r: &Value) -> Option<String> {
        // 按 dotted path 取(默认 models.currentModelId)
        json_path_str(r, &self.model_path)
    }
}
```

加新 agent = `FIXLET_BACKEND=generic AGENT_COMMAND=hermes-acp MODEL_JSON_PATH=...`。**零代码**。

### 3.4 工厂 + 配置

```rust
// backend.rs
pub fn backend_from_env() -> Box<dyn AgentBackend> {
    match std::env::var("FIXLET_BACKEND").unwrap_or_else(|_| "claude-code".into()).as_str() {
        "generic" => Box::new(GenericAcpBackend::from_env()),
        _ => Box::new(ClaudeCodeBackend::from_env()),  // 默认 claude-code(back-compat)
    }
}
```

`FixletConfig` 改:

```rust
pub struct FixletConfig {
    pub task_type: String,
    pub agent_cwd: Option<String>,
    pub backend: Arc<dyn AgentBackend>,   // ← 替代 agent_command: String
}
```

`spawn_agent` 改用 `config.backend.spawn_spec()`(command + env);session/new 的 model 提取改用 `config.backend.extract_model(result)`;session/new params 的 `mcpServers` 外壳留 router(注入 tools-bank + X-Fixus-Session-Id),`model`/`cwd` 不变。

### 3.5 分阶段(L 级拆成可验证的步)

- **CR-9a(本波,M)**:trait + `ClaudeCodeBackend` 提取 + `FixletConfig.backend` + factory + router 改用 trait + **parity 测试**(session/new 构造/model 提取/spawn 命令字节级等价)。零行为变化。
- **CR-9b(本波,M)**:`GenericAcpBackend` + env 工厂分支 + 配置单测(model_path dotted 解析、backend 选择)。交付"加 agent = env"。
- **CR-9c(后续,需真 agent)**:接一个真实第二 agent(Hermes 或 Codex)端到端验证;被阻塞于"有可测的真 agent"。

---

## 4. TDD 测试清单(先写,跑红)

### 4.1 `backend.rs` 单测(纯单元)

- [ ] **`claude_backend_extract_model_path`**:`extract_model` 在 `{"models":{"currentModelId":"claude-sonnet-5"}}` 上返回 `Some("claude-sonnet-5")`;缺字段返回 `None`。
- [ ] **`claude_backend_spawn_spec_defaults`**:`ClaudeCodeBackend::from_env`(无 env)→ `spawn_spec().command == "claude-agent-acp"`。
- [ ] **`generic_backend_model_path_dotted`**:`MODEL_JSON_PATH=data.model` 时 `extract_model({"data":{"model":"gpt-5"}})` → `Some("gpt-5")`;默认路径 `models.currentModelId`。
- [ ] **`factory_selects_by_env`**:`FIXLET_BACKEND=generic` → `name()=="generic"`;默认 → `"claude-code"`。
- [ ] **`session_new_extra_default_null`**:两 backend 默认 `session_new_extra()==Value::Null`。

### 4.2 parity(router 行为零回归)

- [ ] **`router_session_new_shell_unchanged`**:用 ClaudeCodeBackend 构造的 session/new params 与现状硬编码逐键等价(`cwd`、`mcpServers[0]` 的 type/name/url/headers)。提一个 helper `build_session_new(backend, task_id, cwd, tools_bank_url) -> Value`,既给 router 用又给测试断言。
- [ ] **`spawn_command_equals_legacy`**:`spawn_agent` 用的 command(经 backend)与旧 `agent_command` 等价(同样 `bash -c <cmd>` 形)。

### 4.3 router 集成(回归)

- [ ] **`router_parity_session_new_payload`**:端到端构造 execute_turn payload → 走 `handle_execute_turn_from_broker` 的 session/new 分支(用测试 double 的 stdout 喂回 sessionId)→ 断言发给 agent stdin 的 session/new JSON 与重构前一致。

> router 当前无测试(`router.rs:628` 测试 mod 空);parity 测试用 `build_session_new` helper 把"构造"从"发 stdout"中拆出来,单测构造层,绕过真 agent 进程。

---

## 5. 实施步骤

- [ ] **CR-9a**:新建 `src/bin/fixlet/backend.rs`(trait + SpawnSpec + ClaudeCodeBackend + from_env + factory);`main.rs` 加 `mod backend`;`FixletConfig.backend: Arc<dyn AgentBackend>`;router `spawn_agent`/session-new-model-提取改用 backend;提 `build_session_new` helper。先写 §4.1/§4.2 测试(红)→ 实现(绿)→ parity 断言。`cargo build --bin fixlet`。
- [ ] **CR-9b**:`GenericAcpBackend`(dotted model path 解析)+ factory `generic` 分支 + §4.1 generic 测试。env 文档更新(main.rs header)。
- [ ] 全量 `cargo build --release` + `cargo test --bin fixlet`;勾掉 TODO CR-9(标 9a/9b 落地、9c 后续)。

---

## 6. 证据附录

### 6.1 落地范围

- **CR-9a ✅**:`AgentBackend` trait + `SpawnSpec` + `ClaudeCodeBackend`(提取现状)+ factory + `build_session_new_params` helper(parity)。`FixletConfig.backend: Arc<dyn AgentBackend>`;router `spawn_agent`/session-new model 提取改用 backend。
- **CR-9b ✅**:`GenericAcpBackend`(command + dotted `MODEL_JSON_PATH`)+ factory `generic` 分支。
- **CR-9c ⏸ 后续**:真实第二 agent(Hermes/Codex)端到端 —— 被阻塞于"有可测的真 agent"。可插拔性由构造 + 单测证明(非真实 E2E)。

### 6.2 测试(全绿)

`cargo test --bin fixlet` —— **20 passed**(基线 9 → +11 backend):

| 测试 | 验证 |
|------|------|
| `build_session_new_params_matches_legacy_claude` | **parity**:重构后 session/new params 与重构前硬编码逐键等价 |
| `build_session_new_params_merges_extra` | session_new_extra 合并进外壳(扩展点) |
| `claude_backend_*`(3) | extract_model 路径 / spawn_spec / name |
| `session_new_extra_default_null` | 默认不带额外参数 |
| `select_backend_default_is_claude_code` | 未知名 → 默认 claude-code |
| `generic_backend_default_model_path` | 默认 `models.currentModelId` |
| `generic_backend_custom_dotted_model_path` | 自定义 `data.model` 等 dotted 路径 |
| `json_path_str_walks_segments` | dotted 路径解析(含中段缺失/终段非对象) |
| `select_backend_generic_branch` | factory `generic` 分支命中 |

原有 9 个 acp 测试全绿(零回归)。

### 6.3 构建

`cargo build --release` 成功(48s)。

### 6.4 用法(back-compat)

- 既有部署零改动:`FIXLET_BACKEND` 不设 → 默认 `claude-code` → 读 `AGENT_COMMAND`(默认 `claude-agent-acp`),与重构前完全一致。
- 加新 agent:`FIXLET_BACKEND=generic AGENT_COMMAND=<cmd> MODEL_JSON_PATH=<path>` —— 零代码。

---

## 7. 风险与权衡

- **R1 提取破坏 Claude Code live 路径**:最高风险。缓解:parity 测试字节级断言 session/new + spawn;先重构不改行为(9a),再加重 backend(9b)。
- **R2 trait 太薄显得过度设计**:trade-off——薄 trait 恰好覆盖真变异(spawn + model);不把 ACP 解析塞进 trait(N3),避免反模式。若未来某 agent 真有协议差异,再扩 `session_new_extra` 或加 `parse_override` 钩子。
- **R3 无真第二 backend 验证**:9c 被阻塞;9b 的 GenericAcpBackend 用配置 + 单测证明可插拔,但没跑过真 Hermes/Codex。诚实标注:pluggability proven by construction, not by a real second-agent E2E。
- **R4 `bash -c` 包装**:保留现状(跨平台 cmd/bash 分支)。若某 backend 需直 exec(不经 shell),扩 SpawnSpec 加 `shell: bool` 或 `args: Vec<String>`,留后续。
- **R5 env back-compat**:`AGENT_COMMAND` 仍生效(ClaudeCodeBackend::from_env 读它);`FIXLET_BACKEND` 不设默认走 claude-code ⇒ 既有部署零改动。
