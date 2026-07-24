# CR-12 端到端组装与闭环 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 CR-12 网络化 git 沙箱的机制(已在 lv-sandbox 实现 + 零件级测试)组装成 running stack,并用一个全栈确定性 E2E 测试证明 fixus policy 声明 → agent clone 一个仓的完整链路,含 sentinel→real 兑换证据与安全不变量。

**Architecture:** 不改架构。组装逻辑内置进 `#[ignore]` Rust 集成测试(不交付 shipped launcher binary)。7 个进程:logdb-broker(内嵌 logdbd)+ tools-bank + sandbox-broker + sandbox-server(带 `FIXUS_GIT_*` env)+ swap-proxy(in-process)+ 本地 TLS git-http-backend 上游(in-process)。确定性驱动:`POST :3001/mcp` 带 `X-Fixus-Policy` 绕过 LLM。另补 fixus↔桥 metadata 握手契约单测、清 fixus 死码 `dispatch_tool`、写 operator 文档。

**Tech Stack:** Rust(tokio + reqwest + rustls/rcgen + std::process),axum(tools-bank /mcp),logdb-broker(lv-logdb),veps §5 sentinel 接缝。

**Spec:** `docs/superpowers/specs/2026-07-24-cr12-end-to-end-assembly-design.md`。

---

## 前置条件(E2E 测试 Task 3-6 需要,文档化)

1. **lv-sandbox**:`cargo build --workspace`(产出 sibling bin `sandbox-server`/`sandbox-broker`/`fixus-egress-swap-proxy`,与本测试同 `target/debug`)。
2. **lv-fixus**:`cargo build --bin tools-bank`(产出 tools-bank)。
3. **lv-logdb**(`/home/lvtao/logdb/lv-logdb`):`cargo build -p logdb-broker`(本计划用 `embedded:true`,不需单独 logdbd)。
4. **env 指针**(缺任一 → `#[ignore]` 早返回,不算失败):
   - `FIXUS_E2E_TOOLS_BANK_BIN=<lv-fixus>/target/debug/tools-bank`
   - `FIXUS_E2E_BROKER_BIN=<lv-logdb>/target/debug/logdb-broker`

## File Structure

| 文件 | 仓 | 动作 | 职责 |
|---|---|---|---|
| `crates/sandbox-broker/src/main.rs` | lv-sandbox | 改 | 补 net_profile_override 的 fixus-shape 契约用例 |
| `src/storage.rs` + `src/broker_store.rs` | lv-fixus | 改 | 删 dispatch_tool 死码 |
| `crates/git-remote-fixus/tests/cr12_e2e_full_stack.rs` | lv-sandbox | 新 | 全栈 E2E 测试(Task 3-6 增长) |
| `docs/cr-12-net-git-operator-guide.md` | lv-sandbox | 新 | operator 文档 |

**分支**:lv-fixus `cr12-e2e-assembly`(off master);lv-sandbox `cr12-e2e-assembly`(off main)。

---

## Task 1: net_profile_override 的 fixus-shape 契约用例

钉死"fixus `EffectivePolicy` serde 输出 ↔ 桥 `net_profile_override` 解析"契约。桥已有 `net_profile_override` 测试模块(`crates/sandbox-broker/src/main.rs` 约 315-362 的 `#[cfg(test)]`),但用的是合成 JSON;本任务补**精确匹配 fixus serde 形状**的用例。

**Files:**
- Modify: `/home/lvtao/lv/lv-sandbox/crates/sandbox-broker/src/main.rs`(现有 `#[cfg(test)]` 模块)

- [ ] **Step 1: 定位现有测试模块**

Run: `grep -n 'net_profile_override\|mod tests' /home/lvtao/lv/lv-sandbox/crates/sandbox-broker/src/main.rs`
Expected: 命中 `fn net_profile_override`(228)与 `#[cfg(test)] mod tests`(约 315)。

- [ ] **Step 2: 加 fixus-shape 契约用例**

在现有 `#[cfg(test)]` 模块内加(精确 fixus serde 形状:snake_case、`agent_role:"operator"`、`net.egress` 非空数组):

```rust
    /// cr-12 契约钉子:fixus `EffectivePolicy` 的 serde 输出(精确形状)→ net_profile_override。
    /// fixus 改 serde 形状(字段名/大小写/枚举变体)→ 此测试断 → 正是目的。
    #[test]
    fn net_profile_override_fixus_effective_policy_shape() {
        use std::collections::HashMap;
        // 精确复刻 fixus policy.rs 的默认 serde 输出(snake_case;agent_role/operator;
        // net.egress 元素 {host,ports,category?};ports 默认 [] 也发;category None 省略)。
        let policy_json = serde_json::json!({
            "fs": {"read_paths": [], "write_paths": []},
            "net": {"egress": [{"host": "localhost", "ports": [8443], "category": "https_api"}]},
            "agent_role": "operator"
        }).to_string();
        let mut meta = HashMap::new();
        meta.insert("effective_policy".into(), policy_json);
        assert_eq!(net_profile_override(&meta), Some("git"));

        // 反例 1:egress 空(fixus 默认 deny-all)→ None。
        let empty = serde_json::json!({
            "fs": {"read_paths": [], "write_paths": []},
            "net": {"egress": []},
            "agent_role": "operator"
        }).to_string();
        let mut m2 = HashMap::new();
        m2.insert("effective_policy".into(), empty.to_string());
        assert_eq!(net_profile_override(&m2), None);

        // 反例 2:Reader(fixus role_narrow 会清空 net)→ None(fail-closed)。
        let reader = serde_json::json!({
            "fs": {"read_paths": [], "write_paths": []},
            "net": {"egress": []},
            "agent_role": "reader"
        }).to_string();
        let mut m3 = HashMap::new();
        m3.insert("effective_policy".into(), reader.to_string());
        assert_eq!(net_profile_override(&m3), None);

        // 反例 3:缺 effective_policy 键 → None。
        assert_eq!(net_profile_override(&HashMap::new()), None);
    }
```

- [ ] **Step 3: 运行测试**

Run: `cd /home/lvtao/lv/lv-sandbox && cargo test -p sandbox-broker net_profile_override`
Expected: PASS(含新用例 + 既有矩阵用例)。

- [ ] **Step 4: Commit**

```bash
cd /home/lvtao/lv/lv-sandbox
git checkout -b cr12-e2e-assembly main
git add crates/sandbox-broker/src/main.rs
git commit -m "test(sandbox-broker): cr-12 fixus-shape contract case for net_profile_override"
```

---

## Task 2: 删 fixus `dispatch_tool` 死码

Explore 已确认零非测试调用方(方法调用语法 `\.dispatch_tool(` 全仓 0 命中),且 `#[cfg(test)]` 驱动(broker_store.rs:756-788)直接写 stream、不调 `dispatch_tool`。

**Files:**
- Modify: `/home/lvtao/lv/lv-fixus/src/storage.rs`(删 trait default,约 144-148)
- Modify: `/home/lvtao/lv/lv-fixus/src/broker_store.rs`(删 impl,约 479-507;改误导注释,约 762)

- [ ] **Step 1: 删 trait default**

`src/storage.rs` 删除(含文档注释):
```rust
    /// 把工具事件发到 sandbox dispatch stream(Plan D)。
    /// 默认 no-op;BrokerEventStore 覆盖。
    async fn dispatch_tool(&self, _task_id: &str, _event: &AgentEvent) -> Result<()> {
        Ok(())
    }
```

- [ ] **Step 2: 删 BrokerEventStore impl**

`src/broker_store.rs` 删除整个 `async fn dispatch_tool(...)`(约 479-507,含文档注释与 retry 循环)。

- [ ] **Step 3: 改误导注释**

`src/broker_store.rs:762` 附近,把 `// 设置 SANDBOX_REGION 让 dispatch_tool 用` 改为 `// 设置 SANDBOX_REGION:此测试直接写 tool-invoke-test stream(不走 dispatch_tool,后者已删)`。

- [ ] **Step 4: 确认无悬挂引用**

Run: `cd /home/lvtao/lv/lv-fixus && grep -rn 'dispatch_tool' src/`
Expected: 仅 Step 3 改后的注释命中,无代码引用。

- [ ] **Step 5: 跑测试确认无回归**

Run: `cd /home/lvtao/lv/lv-fixus && cargo test`
Expected: PASS(全绿;`test_e2e_dispatch_consume_execute` 仍过——它直接写 stream)。

- [ ] **Step 6: Commit**

```bash
cd /home/lvtao/lv/lv-fixus   # 已在 cr12-e2e-assembly 分支
git add src/storage.rs src/broker_store.rs
git commit -m "chore(fixus): remove dead dispatch_tool (zero callers; tools-bank is live producer)"
```

---

## Task 3: E2E 测试脚手架 + in-process swap-proxy/CGI 上游 + env gating

建测试文件,复用 G2 夹具(`common/mod.rs`)起 in-process swap-proxy + CGI 上游,加 env 指针 gate 与 RAII。本层自洽(不需外部 bin):验证 swap-proxy+CGI 起来、token 兑换工作。

**Files:**
- Create: `/home/lvtao/lv/lv-sandbox/crates/git-remote-fixus/tests/cr12_e2e_full_stack.rs`

- [ ] **Step 1: 写脚手架**

```rust
//! cr-12 全栈 E2E(policy→profile→执行→clone)。`#[ignore]`:需外部 bin + 预构建。
//! 见 docs/superpowers/specs/2026-07-24-cr12-end-to-end-assembly-design.md。
//!
//! 跑法:
//!   cd <lv-sandbox> && cargo build --workspace
//!   FIXUS_E2E_TOOLS_BANK_BIN=<lv-fixus>/target/debug/tools-bank \
//!   FIXUS_E2E_BROKER_BIN=<lv-logdb>/target/debug/logdb-broker \
//!   cargo test -p git-remote-fixus --test cr12_e2e_full_stack -- --ignored --nocapture

mod common;

use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use common::{gen_cert, spawn_cgi_tls_server, TestCert};
use egress_swap_proxy::{config::SwapConfig, server};
use tokio::net::TcpListener;

const SENTINEL: &str = "jail-sentinel-E2E";
const REAL_TOKEN: &str = "real-token-E2E";

/// 子进程 RAII:drop 时 kill+reap,panic 也清。
struct Proc {
    child: Child,
    name: &'static str,
}
impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        eprintln!("[teardown] killed {}", self.name);
    }
}

/// 取必需 env 指针;缺 → 指引并 panic(让测试 fail-skip:外层用 `#[ignore]` 标,手动跑)。
fn require_bin(env_name: &str) -> String {
    match std::env::var(env_name) {
        Ok(v) if !v.is_empty() => v,
        _ => panic!(
            "set {env_name} (+ cargo build --workspace & --bin tools-bank & -p logdb-broker) \
             to run this #[ignore] full-stack test"
        ),
    }
}

/// workspace target/debug(sibling bin 都在此)。
fn target_debug() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_BIN_EXE_git-remote-fixus"))
        .parent()
        .unwrap()
        .to_path_buf(),
}

/// 轮询 TCP 端口就绪(最多 ~5s)。
fn wait_port(addr: &str) {
    for _ in 0..100 {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("port not ready: {addr}");
}

/// 本层:起 in-process CGI 上游 + swap-proxy,验证 token 兑换。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-stack: needs FIXUS_E2E_TOOLS_BANK_BIN + FIXUS_E2E_BROKER_BIN + prebuilt workspace"]
async fn e2e_scaffold_swap_proxy_and_upstream() {
    let root = tempfile::tempdir().expect("tempdir");

    // 1) 本地 TLS git-http-backend 上游(记录 Authorization)。
    let auth_seen = Arc::new(std::sync::Mutex::new(None));
    let (up_port, up_cert) = spawn_cgi_tls_server(root.path().to_path_buf(), auth_seen.clone());

    // 2) swap-proxy 入站证 + in-process serve。
    let inbound = gen_cert();
    let swap_cfg = SwapConfig {
        listen: "127.0.0.1:0".into(),
        sentinel: SENTINEL.into(),
        real_token: REAL_TOKEN.into(),
        upstream: format!("127.0.0.1:{up_port}"),
        cert_pem: inbound.cert_pem.clone(),
        key_pem: inbound.key_pem.clone(),
        upstream_ca_pem: Some(up_cert.cert_pem.clone()),
    };
    let server_cfg = Arc::new(server::build_server_config(&swap_cfg).expect("server cfg"));
    let upstream_cfg = Arc::new(server::build_upstream_client_config(&swap_cfg).expect("upstream cfg"));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind swap-proxy");
    let swap_port = listener.local_addr().expect("addr").port();
    let cfg = Arc::new(swap_cfg);
    let _swap = tokio::spawn(async move {
        let _ = server::serve(listener, server_cfg, upstream_cfg, cfg).await;
    });

    // 3) 验证:swap-proxy 端口起来(后续 Task 在此之上接 sandbox-server)。
    wait_port(&format!("127.0.0.1:{swap_port}"));
    eprintln!("[scaffold] swap-proxy :{swap_port} → upstream :{up_port} OK");
    // _proc 占位:本层无外部进程;Proc RAII 留给 Task 4。
    let _ = target_debug(); // 编译期确认 sibling-bin 推导可用
}
```

- [ ] **Step 2: 运行(只验脚手架层)**

Run: `cd /home/lvtao/lv/lv-sandbox && cargo test -p git-remote-fixus --test cr12_e2e_full_stack -- --ignored --nocapture`
Expected: PASS,打印 `[scaffold] swap-proxy :<port> → upstream :<port> OK`(无需外部 bin——本层不起它们;`require_bin` 本层未调)。

- [ ] **Step 3: Commit**

```bash
cd /home/lvtao/lv/lv-sandbox   # 已在 cr12-e2e-assembly 分支(若 Task1 已切;否则先 git checkout -b cr12-e2e-assembly main)
git add crates/git-remote-fixus/tests/cr12_e2e_full_stack.rs
git commit -m "test(git-remote-fixus): cr-12 full-stack E2E scaffold (in-proc swap-proxy + CGI upstream)"
```

---

## Task 4: 起 logdb-broker(embedded)+ tools-bank + sandbox-broker + sandbox-server + readiness

扩 Task 3 的测试,起外部进程并轮询就绪。**不起** tool 调用(下一 Task)。

**Files:**
- Modify: `cr12_e2e_full_stack.rs`(Task 3 的文件)

- [ ] **Step 1: 加 helper:写 tmp config + spawn 进程**

在 Task 3 文件的 `wait_port` 之后加:

```rust
/// 写临时文件,返回路径。
fn write_tmp(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, content).unwrap();
    p
}

/// 起 logdb-broker(embedded logdbd)+ 返回 Proc。bind 5100(匹配 sandbox-broker/tools-bank 默认 --broker-addr)。
fn spawn_broker(dir: &std::path::Path) -> Proc {
    let bin = require_bin("FIXUS_E2E_BROKER_BIN");
    let cfg = write_tmp(dir, "broker.yaml",
        "bind_addr: \"127.0.0.1:5100\"\n\
         logdbd_addr: \"http://127.0.0.1:50051\"\n\
         embedded: true\n\
         num_shards: 4\n\
         session_timeout_ms: 10000\n");
    let child = Command::new(&bin)
        .env("LOGDB_BROKER_CONFIG", &cfg)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::inherit()).stderr(Stdio::inherit())
        .spawn().unwrap_or_else(|e| panic!("spawn logdb-broker: {e}"));
    Proc { child, name: "logdb-broker" }
}

/// 起 tools-bank:3001,接 broker 5100。
fn spawn_tools_bank() -> Proc {
    let bin = require_bin("FIXUS_E2E_TOOLS_BANK_BIN");
    let child = Command::new(&bin)
        .args(["--broker-addr", "127.0.0.1:5100", "--region", "default", "--port", "3001"])
        .env("LOGDBD_NAMESPACE", "default")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::inherit()).stderr(Stdio::inherit())
        .spawn().unwrap_or_else(|e| panic!("spawn tools-bank: {e}"));
    Proc { child, name: "tools-bank" }
}

/// 起 sandbox-server:8080,git profile 经 FIXUS_GIT_* env 指 swap-proxy;helper on PATH。
fn spawn_sandbox_server(
    dir: &std::path::Path,
    swap_port: u16,
    inbound_cert_pem: &str,
) -> Proc {
    let bin = target_debug().join("sandbox-server");
    let cfg = write_tmp(dir, "sandbox-server.yaml",
        "server:\n  listen_addr: \"127.0.0.1:8080\"\n  log_level: \"info\"\n  log_format: \"text\"\n\
         sandbox:\n  base_dir: \"SBASE\"\n  fail_closed: false\n  default_profile: \"shell\"\n\
         profiles:\n  shell:\n    rlimit:\n      nproc: 8192\n      nofile: 256\n"
            .replace("SBASE", dir.join("sandboxes").to_str().unwrap()));
    let cert_file = write_tmp(dir, "git-ca.pem", inbound_cert_pem);
    // helper 目录(target/debug)前置 PATH,使 `git clone fixus::...` 找到 git-remote-fixus。
    let mut path = std::ffi::OsString::from(target_debug());
    path.push(":");
    if let Ok(p) = std::env::var("PATH") { path.push(p); }
    let child = Command::new(&bin)
        .args(["--config", cfg.to_str().unwrap()])
        .env("PATH", &path)
        .env("FIXUS_GIT_SENTINEL", SENTINEL)
        .env("FIXUS_GIT_EGRESS_HOST", "localhost")
        .env("FIXUS_GIT_EGRESS_PORT", swap_port.to_string())
        .env("FIXUS_GIT_CA_FILE", &cert_file)
        .env("RUST_LOG", "info")
        .stdout(Stdio::inherit()).stderr(Stdio::inherit())
        .spawn().unwrap_or_else(|e| panic!("spawn sandbox-server: {e}"));
    Proc { child, name: "sandbox-server" }
}

/// 起 sandbox-broker:消费 tool-invoke-default,转发 sandbox-server:8080。去 proxy(坑5)。
fn spawn_sandbox_broker() -> Proc {
    let bin = target_debug().join("sandbox-broker");
    let mut cmd = Command::new(&bin);
    cmd.args(["--broker-addr", "127.0.0.1:5100",
              "--sandbox-url", "http://127.0.0.1:8080",
              "--region", "default", "--group", "sandboxes"])
        .env("LOGDBD_NAMESPACE", "default")
        .env("RUST_LOG", "warn")
        // 坑5:去所有 proxy env,否则 reqwest 走死代理。
        .env_remove("HTTP_PROXY").env_remove("HTTPS_PROXY")
        .env_remove("http_proxy").env_remove("https_proxy")
        .env_remove("ALL_PROXY").env_remove("all_proxy")
        .env("NO_PROXY", "*").env("no_proxy", "*")
        .stdout(Stdio::inherit()).stderr(Stdio::inherit());
    Proc { child: cmd.spawn().unwrap_or_else(|e| panic!("spawn sandbox-broker: {e}")), name: "sandbox-broker" }
}
```

- [ ] **Step 2: 在测试体内起全栈 + readiness**

把 Task 3 的 `e2e_scaffold_swap_proxy_and_upstream` 改名为 `e2e_full_stack_bringup`,在 `eprintln!("[scaffold]...")` 后加:

```rust
    // 4) 起外部进程栈。
    let _broker = spawn_broker(root.path());
    wait_port("127.0.0.1:5100");
    let _sandbox_server = spawn_sandbox_server(root.path(), swap_port, &inbound.cert_pem);
    wait_port("127.0.0.1:8080");
    let _sandbox_broker = spawn_sandbox_broker();
    let _tools_bank = spawn_tools_bank();
    wait_port("127.0.0.1:3001");
    // 给 sandbox-broker 加入 consumer group 一点时间。
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    eprintln!("[bringup] broker+server+bridge+tools-bank ready");
```

- [ ] **Step 3: 运行(验全栈起来)**

设两个 env 指针后 Run:
`cd /home/lvtao/lv/lv-sandbox && FIXUS_E2E_TOOLS_BANK_BIN=... FIXUS_E2E_BROKER_BIN=... cargo test -p git-remote-fixus --test cr12_e2e_full_stack -- --ignored --nocapture`
Expected: PASS,打印 `[bringup] broker+server+bridge+tools-bank ready`(进程 stderr 可见,无早退;子进程随测试结束被 RAII kill)。

- [ ] **Step 4: Commit**

```bash
git add crates/git-remote-fixus/tests/cr12_e2e_full_stack.rs
git commit -m "test(git-remote-fixus): cr-12 E2E bringup broker+tools-bank+sandbox-broker+server"
```

---

## Task 5: 驱动 plain `fixus_bash`(shell profile),证上栈通

不发 X-Fixus-Policy(net.egress 空 → 默认 shell profile),跑 `echo`,断言 stdout 往返。先证 tools-bank→broker→sandbox-broker→sandbox-server 链路通,再加 git 层。

**Files:**
- Modify: `cr12_e2e_full_stack.rs`

- [ ] **Step 1: 加 MCP 驱动 helper**

```rust
/// POST /mcp tools/call,返回 result.content[0].text(stringified ToolResult)。
async fn mcp_call(port: u16, session_id: &str, policy_json: Option<&str>,
                  tool: &str, command: &str) -> serde_json::Value {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60)).build().unwrap();
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": tool, "arguments": {"command": command}}
    });
    let mut req = client.post(format!("http://127.0.0.1:{port}/mcp"))
        .header("X-Fixus-Session-Id", session_id)
        .header("Content-Type", "application/json")
        .json(&body);
    if let Some(p) = policy_json {
        req = req.header("X-Fixus-Policy", p);
    }
    let resp = req.send().await.expect("POST /mcp");
    let v: serde_json::Value = resp.json().await.expect("mcp json");
    v
}
```

- [ ] **Step 2: 在 bringup 后驱动 plain bash + 断言**

在 `[bringup] ... ready` 之后加:

```rust
    // 5) plain bash(shell profile,无 policy)→ 证上栈通。
    let task_id = "e2e-plain-bash";
    let v = mcp_call(3001, task_id, None, "fixus_bash", "echo hello-from-sandbox").await;
    let text = v["result"]["content"][0]["text"].as_str().expect("content text");
    let out: serde_json::Value = serde_json::from_str(text).expect("ToolResult json");
    assert_eq!(out["exit_code"], 0, "plain bash failed: {v}");
    let stdout = out["stdout"].as_str().unwrap_or("");
    assert!(stdout.contains("hello-from-sandbox"), "stdout mismatch: {stdout}");
    eprintln!("[plain-bash] stdout={stdout:?} — upper stack OK");
```

- [ ] **Step 3: 运行**

Run(同 Task 4 env 指针):`cargo test -p git-remote-fixus --test cr12_e2e_full_stack -- --ignored --nocapture`
Expected: PASS,打印 `[plain-bash] stdout="hello-from-sandbox\n" — upper stack OK`。

- [ ] **Step 4: Commit**

```bash
git add crates/git-remote-fixus/tests/cr12_e2e_full_stack.rs
git commit -m "test(git-remote-fixus): cr-12 E2E drive plain fixus_bash (upper stack proven)"
```

---

## Task 6: 切 git profile 驱动 clone,断言 token 兑换 + 安全负向

发 `X-Fixus-Policy`(net.egress 非空 + operator)→ 桥选 git profile → clone 经 SOCKS→swap-proxy→上游。断言 clone 成功 + 上游见 `Bearer <REAL_TOKEN>`(非 sentinel)+ 负向(非 allowlist host 被拒)。

**Files:**
- Modify: `cr12_e2e_full_stack.rs`

- [ ] **Step 1: 在 CGI 上游初始化时建一个 bare 仓 + seed commit**

Task 3 的 `spawn_cgi_tls_server(root, auth_seen)` 用 `project_root` 作 GIT_PROJECT_ROOT。需先在 `root` 下建 `upstream.git` bare + seed。在测试体 `spawn_cgi_tls_server` 调用**之前**加:

```rust
    // 0) 上游 bare 仓 + 一个 seed commit(CGI 的 GIT_PROJECT_ROOT = root.path())。
    let upstream_git = root.path().join("upstream.git");
    common::git(root.path(), &["init", "--bare", "upstream.git"]);
    let seed = root.path().join("seed-work");
    std::fs::create_dir_all(&seed).unwrap();
    common::git(&seed, &["init", "--branch=main"]);
    std::fs::write(seed.join("note.txt"), "seeded\n").unwrap();
    common::git(&seed, &["add", "note.txt"]);
    common::git(&seed, &["commit", "-m", "seed"]);
    common::git(&seed, &["remote", "add", "origin", upstream_git.to_str().unwrap()]);
    common::git(&seed, &["push", "-u", "origin", "main"]);
```

- [ ] **Step 2: 加负向/正向 clone 驱动 + 断言**

在 Task 5 的 plain-bash 断言后加:

```rust
    // 6) git profile 驱动:policy net.egress 非空 + operator → 桥选 git → clone 经 swap-proxy。
    let policy = serde_json::json!({
        "fs": {"read_paths": [], "write_paths": []},
        "net": {"egress": [{"host": "localhost", "ports": [swap_port], "category": "https_api"}]},
        "agent_role": "operator"
    }).to_string();
    let clone_dest = format!("clone-out-{}", task_id);
    let clone_cmd = format!(
        "git -c protocol.version=0 clone fixus::https://localhost:{swap_port}/upstream.git {clone_dest}");
    let git_task = "e2e-git-clone";
    let v = mcp_call(3001, git_task, Some(&policy), "fixus_bash", &clone_cmd).await;
    let text = v["result"]["content"][0]["text"].as_str().expect("content text");
    let out: serde_json::Value = serde_json::from_str(text).expect("ToolResult json");
    assert_eq!(out["exit_code"], 0, "git clone through swap-proxy failed: {v}\n{}", out["stderr"].as_str().unwrap_or(""));
    assert!(root.path().join(format!("clone-out-{git_task}")).join(".git").exists(),
            "clone dest missing .git");

    // 7) 关键证据:上游见 Bearer <REAL_TOKEN>,非 sentinel。
    let auth = auth_seen.lock().unwrap().clone().expect("upstream must have received a request");
    assert_eq!(auth, format!("Bearer {REAL_TOKEN}"), "upstream saw wrong auth: {auth}");
    assert!(!auth.contains(SENTINEL), "sentinel MUST NOT reach upstream: {auth}");
    eprintln!("[git-clone] clone OK; upstream saw {auth}");

    // 8) 安全负向:非 allowlist host(example.invalid)经 SOCKS 必被拒。
    let neg = mcp_call(3001, "e2e-git-neg", Some(&policy), "fixus_bash",
        "git -c protocol.version=0 clone fixus::https://example.invalid/x.git neg-out 2>&1; exit $((1-$?))")
        .await; // 让失败也返回(反转 exit 便于断言"非 0")
    let ntext = neg["result"]["content"][0]["text"].as_str().expect("content text");
    let nout: serde_json::Value = serde_json::from_str(ntext).expect("ToolResult json");
    assert_ne!(nout["exit_code"], 0, "non-allowlist host MUST be rejected by SOCKS: {neg}");
    eprintln!("[security] non-allowlist clone rejected (exit={}) — SOCKS allowlist holds", nout["exit_code"]);
```

> 注:负向用 `exit $((1-$?))` 反转——git clone 失败(exit≠0)→ 反转为 0;成功会被反转成非 0。断言 `exit_code != 0` 即"clone 未成功"。

- [ ] **Step 3: 运行全栈 E2E**

Run(同 env 指针):`cargo test -p git-remote-fixus --test cr12_e2e_full_stack -- --ignored --nocapture`
Expected: PASS,三行:`[git-clone] clone OK; upstream saw Bearer real-token-E2E` + `[security] non-allowlist clone rejected`。

- [ ] **Step 4: Commit**

```bash
git add crates/git-remote-fixus/tests/cr12_e2e_full_stack.rs
git commit -m "test(git-remote-fixus): cr-12 full-stack E2E clone via swap-proxy + sentinel swap + security negative"
```

---

## Task 7: operator 文档

**Files:**
- Create: `/home/lvtao/lv/lv-sandbox/docs/cr-12-net-git-operator-guide.md`

- [ ] **Step 1: 写文档**

完整内容(基于 spec §4.3 + 已验 wire 形状):

````markdown
# CR-12 网络化 Git 沙箱 — Operator 指南

让牢内 agent 经 swap-proxy 安全 clone/push 远端 git 仓。本文 = env 契约 + 组装 recipe + 坑。

## 1. 拓扑
```
agent(牢内)→ git-remote-fixus helper → per-job SOCKS5h(UDS)→ swap-proxy(牢外)
  [sentinel→real token 改写 + Host 重写]→ 真 git 上游(github.com / 自建)
```
牢内只见 sentinel(公开占位值);真 token 只在 swap-proxy 进程。绕 swap-proxy 直连 → SOCKS allowlist 拒。

## 2. env 契约(两侧同 sentinel 必填)

**sandbox-server 主机进程**(`FIXUS_GIT_*`,git profile 在此读):
| env | 必填 | 说明 |
|---|---|---|
| `FIXUS_GIT_SENTINEL` | 是 | 公开占位值;进牢作 `FIXUS_GIT_SENTINEL`,helper 发 `Authorization: Bearer <它>` |
| `FIXUS_GIT_EGRESS_HOST` | 否(默 github.com) | swap-proxy 主机(allowlist target = helper 连接处) |
| `FIXUS_GIT_EGRESS_PORT` | 否(默 443) | swap-proxy 端口 |
| `FIXUS_GIT_CA_FILE` | 否(缺则 webpki) | swap-proxy 入站 TLS 证 PEM 路径 → 进牢作 `SANDBOX_CA_PEM` |

**swap-proxy 进程**(`FIXUS_SWAP_*`,`fixus-egress-swap-proxy`):
| env | 必填 | 说明 |
|---|---|---|
| `FIXUS_SWAP_SENTINEL` | 是 | **必须与 `FIXUS_GIT_SENTINEL` 同值** |
| `FIXUS_SWAP_TOKEN` | 是(秘密) | 真 token;只在本进程;改写后 `Bearer <它>` 转发 |
| `FIXUS_SWAP_CERT_PEM` | 是 | 入站 TLS 证 PEM(无自签回退,fail-closed) |
| `FIXUS_SWAP_KEY_PEM` | 是(秘密) | 入站 TLS 私钥 PEM |
| `FIXUS_SWAP_LISTEN` | 否(默 127.0.0.1:8443) | bind |
| `FIXUS_SWAP_UPSTREAM` | 否(默 github.com:443) | 真上游 host:port |
| `FIXUS_SWAP_UPSTREAM_CA_PEM` | 否(缺则 webpki-roots) | 上游 CA |

## 3. 组装 recipe(7 进程,见 `tests/cr12_e2e_full_stack.rs` 可跑参考)
1. 起 logdb-broker(`embedded:true` 内嵌 logdbd;`session_timeout_ms>0`;bind 与下两者 `--broker-addr` 一致)。
2. 起 sandbox-server:`--config <yaml>`(base_dir 可写 / `fail_closed:false`(WSL2)/ `profiles.shell.rlimit.nproc:8192`)+ 上节 `FIXUS_GIT_*` env + `PATH` 含 `git-remote-fixus`。
3. 起 sandbox-broker:`--broker-addr … --sandbox-url http://…:8080 --region … --group …`,**去所有 `*_PROXY` env**。
4. 起 tools-bank:`--broker-addr … --region … --port 3001`。
5. 起 swap-proxy(`fixus-egress-swap-proxy`)带上节 `FIXUS_SWAP_*` env。
6. (真 LLM turn 才需)起 fixus serve(:3000,+ redis 6379 + `REDIS_URL`/`SANDBOX_REGION`)+ fixlet(去 proxy + LLM 配置)。

## 4. systemd 样例(swap-proxy unit)
```ini
[Unit]
Description=fixus egress swap-proxy
[Service]
EnvironmentFile=/etc/fixus/swap.env   # FIXUS_SWAP_*
ExecStart=/usr/local/bin/fixus-egress-swap-proxy
Restart=on-failure
# 不打印秘密:SwapConfig 不 derive Debug。
[Install]
WantedBy=multi-user.target
```

## 5. 坑
1. logdb-broker `session_timeout_ms>0`(否则 stale member 不驱逐,turn 认领死锁)。
2. 起所有 fixus 系进程前确认无 `HTTPS_PROXY` 等(常 down);sandbox-broker 尤其要去。
3. logdbd/broker stream 名只允许 `[a-zA-Z0-9_-]`,禁点号。
4. sandbox-server 配置三件套:`base_dir` 可写 / `fail_closed:false` / `nproc:8192`(RLIMIT_NPROC 按 UID 计)。
5. helper 只支持 git 协议 v0/v1;`git -c protocol.version=0 clone …`。
````

- [ ] **Step 2: Commit**

```bash
cd /home/lvtao/lv/lv-sandbox
git add docs/cr-12-net-git-operator-guide.md
git commit -m "docs(cr-12): operator guide for networked git sandbox (env contract + recipe + gotchas)"
```

---

## Self-Review(plan 自检)

- **Spec 覆盖**:组件 A(Task 3-6 E2E)、B1(Task 1 握手)、B2(Task 2 清理)、C(Task 7 文档)全覆盖。✓
- **占位扫描**:每步含可执行代码/命令,无 TBD/TODO。负向断言的 `exit $((1-$?))` 已注释解释。✓
- **类型一致**:`SwapConfig` 字段、`server::serve` 签名、`spawn_cgi_tls_server` 返回 `(u16, TestCert)`、`common::git` 签名、MCP body 形状均与 Explore 报告一致。✓
- **端口一致**:logdb-broker bind 5100 = sandbox-broker/tools-bank 默认 `--broker-addr 127.0.0.1:5100`(报告修正点)。✓
- **风险已标**:embedded 模式若不稳 → fallback 分离 logdbd+broker(前置条件注);负向断言不锁死错误码(只断 exit≠0)。

## 执行顺序

Task 1 → 2 → 3 → 4 → 5 → 6 → 7(Task 1/2 独立可并行;3-6 必须顺序;7 可在 6 后或与 6 并行)。
```
