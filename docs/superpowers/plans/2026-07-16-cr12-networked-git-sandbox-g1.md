# CR-12 G1 — 网络化 Git 沙箱(最小闭环)Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 agent 在沙箱内对一个真实 git 仓库完成 `clone` + `commit` + `push`,全程零特权(AF_UNIX-only + SOCKS5h UDS 代理 + allowlist),凭据进牢笼但为占位假凭据(真凭据兑换在出口代理,本 plan 外)。

**Architecture:** 复用 lv-sandbox 已有的 cr-019 出口机制(seccomp AF_UNIX-only + per-job SOCKS5h-over-UDS 代理 + `AllowlistMatcher`)。"开网络" = 给一个 `git` profile 填 `egress_allowlist`。git 在 jail 里发不出 TCP,故新增 `git-remote-fixus` remote helper:它自己拨 UDS→SOCKS5h→TLS→git smart-HTTP。桥按 task 的 net hint 选 profile(关掉 `sandbox-broker/main.rs:140-143` 的 warn)。fixus `CapabilityPolicy.net` 已建模,只需把 net hint 透传进 tool-invoke metadata。

**Tech Stack:** Rust / Tokio;lv-sandbox(`sandbox-core` / `sandbox-broker` + 新 `git-remote-fixus` bin);fixus(`tools-bank` / `policy`);rustls(SOCKS5h→TLS);git remote-helper 协议。

**Design spec:** `veps/cr-12-networked-git-sandbox-design.md`(权威)。

> **进度(2026-07-16):** Task 0 ✓(gix via `Http` trait)| Task 1 ✓ `b61115a`(git profile)| Task 2 ✓ `6d852801` + 修复 `02c52eb5`(桥 per-task profile + **fail-closed** net 检测:egress 非空,非键存在 —— fixus 的 `net` 字段恒在)| Task 3 ✓ `4918913`..`3a5b2b0` + review-fix `b542a27`(`git-remote-fixus` helper:dialer+SOCKS5h+TLS / gix `Http` impl / 手写 smart-HTTP / E2E 真 clone+push;两轮 review 过 + nits 修)| Task 4 ✓ `f678fca`(fixus 侧 net 链路**无代码缺口** —— 全套接线 create_task policy→resolve_and_validate→effective_policy→turn_begin→X-Fixus-Policy→tool-invoke metadata→桥 均来自 Phase 1;仅加 G1 契约测试 pin 住"三 scope 同声明 + operator role → net 存活,任一缺/Reader → fail-closed 清空")| **Task 5 ✓ + Task 6 ✓**(sandbox-server-direct live E2E:牢内 clone+commit+push 全通,上游 bare 仓独立核对;§5 两条安全不变量 PASS;CA 注入 `e2d931e` + `/dev/null` 写 `33dab6e`;桥 net 翻译由 6 单测覆盖)。**G1 完成。** 证据见设计 §5.1。
>
> **已知 G1 缺口(留 G2/v2):** ① git 协议 **v2 不支持**(E2E 用 v0;真 github 默认 v2 —— 可能向后兼容回 v0 但未验证);② push POST 全缓冲(大 push OOM 风险,需流式);③ 边缘测试(push `ng` 拒绝、side-band band3、malformed pkt-line)。**契约已定**:桥认 `effective_policy.net.egress` 非空 → git profile。

**Cross-repo:** 本 plan 覆盖 lv-sandbox(`lv-sandbox`)+ fixus(`lv-fixus`)两仓。plan 文档居 fixus,实现按仓推进。

---

## 关键约束(已核实,代码事实)

- seccomp `deny_network()`(profile.rs:191)只放 `AF_UNIX` ⇒ **jail 建不出任何 TCP socket**。git HTTPS 必须经 UDS 代理,且需自定义 remote helper(标准 git/libcurl 不支持 SOCKS5-over-UDS)。
- cr-019 出口机制已建好且测齐:`egress.rs`(`EgressRule{host,port}` + `AllowlistMatcher`,默认拒)+ `proxy.rs`(SOCKS5h-over-UDS,`.proxy.sock`,强制 DOMAIN、拒 IPv4 字面量)+ `sandbox_context.rs` run_in_workspace(profile.egress_allowlist 非空则起 `JobProxy` 并注入 `SANDBOX_PROXY_SOCK`)。
- `create_session(profile, metadata, timeout)` 只收 **profile 名字**(`lv_client.rs:112`),egress 绑 profile。`SessionMap` 当前全局一个 profile(`session_map.rs:12`)⇒ 桥需按 task 选 profile。
- `SandboxProfile::shell()` 的 rlimit(`cpu_seconds(2)`/`fsize_mb(10)`)对 git clone/push 太紧 ⇒ `git` profile 须放宽。

---

## File Structure

**lv-sandbox**
- Modify: `crates/sandbox-core/src/profile.rs` — 加 `SandboxProfile::git()` + 注册进 `ProfileRegistry`(与 shell/python/node 同处)
- Modify: `crates/sandbox-broker/src/session_map.rs` — `get_or_create` 支持 per-task profile override
- Modify: `crates/sandbox-broker/src/translate.rs` — `execute` 透传 profile override 到 `get_or_create`
- Modify: `crates/sandbox-broker/src/main.rs` — 从 tool-invoke metadata 读 net hint → 选 profile(关 :140-143 warn)
- Create: `crates/git-remote-fixus/` — 新 bin(git remote helper)

**lv-fixus**
- Modify: `src/bin/tools-bank/adapter.rs` / `main.rs` — tool-invoke metadata 加 profile/net hint(随 effective_policy 同路径)
- Verify: `src/policy.rs` — `CapabilityPolicy.net` 已建模 + 可序列化透传
- Modify(若需):`src/protocol.rs` create_task — 接受 net 声明

---

## Task 0: Spike — `git-remote-fixus` 传输方案(先定方案再实现)

**Why:** git smart-HTTP 不是裸流协议(info/refs GET + service POST 请求/响应),`connect` capability 不适用;helper 必须自己做 HTTP 包装。手写 git upload-pack/receive-pack 协商易错,须先定方案。

**Files:** `crates/git-remote-fixus/`(新);spike 笔记记入本 plan 末尾"Task 0 结论"。

- [ ] **Step 0.1: 评估 `gix` crate 自定义 transport/dialer 可行性**

调研 `gix`(pure-Rust git)能否注册自定义 HTTP transport,其 dialer 钩子接到我们的 SOCKS5h-over-UDS+TLS。看 `gix-transport` / `gix-protocol` 的 custom connect API。
预期产出:可行 / 不可行 + 理由。

- [ ] **Step 0.2: 若 gix 不可行,定手写最小 smart-HTTP 方案**

手写范围:`info/refs?service=git-upload-pack|git-receive-pack`(GET)+ `git-<service>`(POST,`Content-Type: application/x-git-upload-pack-request`), pkt-line 最小解析(want/have/NAK + pack 流透传)。fetch/push 走 remote-helper 的 `fetch`/`push` 命令(非 `connect`)。
产出:helper 模块分解(dialer / http / pktline / helper-protocol)+ 风险点。

- [ ] **Step 0.3: 写最小验证 —— 经 UDS 代理 `clone` 一个本地 repo**

起一个本地 `git http-backend`(或 `git daemon` http)作上游;起 cr-019 proxy(allowlist 含上游 host);用选定方案的 dialer 拨 UDS→SOCKS5h→TLS(本地可先明文/自签)→ 拉 `info/refs` 成功。
预期:能拿到 `info/refs` 应答(证明 UDS+SOCKS5h+HTTP 通)。失败 → 回 Step 0.1/0.2。
```bash
# 上游:本地 git http(参考实现细节由 spike 定)
# 验证:dialer 能经 proxy 取到 refs
```

- [ ] **Step 0.4: 记 Task 0 结论**(方案选 gix / 手写 + 模块分解),据此展开 Task 3 的具体 TDD 步骤。

> Task 3 的细化步骤在 Task 0 结论后补写于此 plan。在结论前不实现 Task 3 主体。

---

## Task 1 (lv-sandbox): `git` profile preset + 注册

**Files:**
- Modify: `crates/sandbox-core/src/profile.rs`(加 `git()`;在 `ProfileRegistry` 注册处与 shell/python/node 并列注册)

- [ ] **Step 1.1: 写失败测试 —— `git()` profile 有非空 egress + 放宽 rlimit**

`crates/sandbox-core/src/profile.rs` tests:
```rust
#[test]
fn git_profile_has_egress_and_relaxed_rlimits() {
    let p = SandboxProfile::git();
    assert_eq!(p.name, "git");
    assert!(!p.egress_allowlist.is_empty(), "git profile must allow egress");
    assert!(p.egress_allowlist.iter().any(|r| r.port == Some(443)));
    // rlimit 须比 shell 宽(git clone/push 吃 CPU/fs/fd)
    assert!(p.rlimit.fsize_mb > shell_ref().rlimit.fsize_mb);
    assert!(p.rlimit.cpu_seconds > shell_ref().rlimit.cpu_seconds);
    assert!(p.rlimit.nofile > shell_ref().rlimit.nofile);
}
fn shell_ref() -> SandboxProfile { SandboxProfile::shell() }
```
Run: `cargo test -p sandbox-core git_profile_has_egress_and_relaxed_rlimits`
Expected: FAIL(无 `git()`)。

- [ ] **Step 1.2: 实现 `SandboxProfile::git()`**

`crates/sandbox-core/src/profile.rs`(在 `node()` 后):
```rust
/// cr-012: git 代码-dev profile —— 开 allowlist 出口 + 放宽 rlimit/超时。
/// egress 目标默认 github.com:443,可由 env `FIXUS_GIT_EGRESS_HOST` 覆盖(指向凭据出口代理)。
pub fn git() -> Self {
    let mut p = Self::shell();
    p.name = "git".into();
    p.rlimit = RlimitConfig::new()
        .cpu_seconds(120)      // clone/push 长
        .nofile(256)
        .nproc(64)
        .fsize_mb(1024)        // 仓库 + pack
        .core_disabled()
        .stack_mb(16)
        .memlock_disabled();
    p.default_timeout = Duration::from_secs(300);
    p.egress_allowlist = vec![crate::egress::EgressRule {
        host: std::env::var("FIXUS_GIT_EGRESS_HOST")
            .unwrap_or_else(|_| "github.com".to_string()),
        port: Some(443),
    }];
    p
}
```

- [ ] **Step 1.3: 注册进 `ProfileRegistry`**

在 `ProfileRegistry` 构造处(与 shell/python/node 注册同处)加 `git`。Run profile 注册相关测试。

- [ ] **Step 1.4: 跑测试确认通过**
Run: `cargo test -p sandbox-core git_profile` → PASS。

- [ ] **Step 1.5: Commit**
```bash
git add crates/sandbox-core/src/profile.rs
git commit -m "feat(sandbox): cr-12 git profile (egress allowlist + relaxed rlimit)"
```

---

## Task 2 (lv-sandbox bridge): 按 task 选 profile(关 warn)

**Files:**
- Modify: `crates/sandbox-broker/src/session_map.rs`(`get_or_create` 加 profile override)
- Modify: `crates/sandbox-broker/src/translate.rs`(`execute` 透传 override)
- Modify: `crates/sandbox-broker/src/main.rs`(读 metadata net hint → 选 profile)

- [ ] **Step 2.1: 写失败测试 —— per-task profile override**

`session_map.rs` tests 加:首次见 task-A 带 override="git" → `create_session` 收到 profile="git";task-B 无 override → 收默认("shell")。用 `CountingMock` 扩展记录传给 create_session 的 profile 字符串。
```rust
#[tokio::test]
async fn per_task_profile_override() {
    // mock 记录每次 create_session 的 profile 参数
    // task-A override=Some("git") → profile "git"
    // task-B override=None → profile "shell"(默认)
}
```
Run: `cargo test -p sandbox-broker per_task_profile_override` → FAIL(签名不支持 override)。

- [ ] **Step 2.2: 改 `SessionMap::get_or_create` 支持 override**

```rust
pub async fn get_or_create(
    &self,
    http: &Arc<dyn SandboxHttp>,
    task_id: &str,
    profile_override: Option<&str>,
) -> Result<String, BridgeError> {
    let mut map = self.map.lock().await;
    if let Some(sid) = map.get(task_id).cloned() { return Ok(sid); }
    let profile = profile_override.unwrap_or(&self.profile);
    let mut metadata = HashMap::new();
    metadata.insert("fixus_task_id".to_string(), task_id.to_string());
    let sid = http.create_session(profile, metadata, self.timeout_secs).await?;
    map.insert(task_id.to_string(), sid.clone());
    Ok(sid)
}
```
更新现有测试调用处(补 `None`)。

- [ ] **Step 2.3: `translate::execute` 透传 override**

`execute` 加参数 `profile_override: Option<&str>`,传给 `get_or_create`。更新 mock 测试调用处。

- [ ] **Step 2.4: `main.rs` 读 net hint → 选 profile(关 :140-143 warn)**

在 `run_consumer` 解析 frame 处,把现有的 effective_policy warn 替换为:若 metadata 的 effective_policy 含 `net`(或专用 `net_profile` hint)→ profile_override=Some("git")。
```rust
// 替换 main.rs:140-143 的 warn 块
let profile_override = rec.metadata.get("effective_policy")
    .and_then(|v| serde_json::from_str::<serde_json::Value>(v).ok())
    .and_then(|p| p.get("net"))
    .map(|_| "git");  // net 声明 → git profile(G1:固定 git;G2:按 allowlist 细分)
```
传给 `translate::execute(..., profile_override)`。

- [ ] **Step 2.5: 跑测试**
Run: `cargo test -p sandbox-broker` → PASS。

- [ ] **Step 2.6: Commit**
```bash
git add crates/sandbox-broker/src/session_map.rs crates/sandbox-broker/src/translate.rs crates/sandbox-broker/src/main.rs
git commit -m "feat(sandbox-broker): cr-12 per-task profile from net hint (closes effective_policy warn)"
```

---

## Task 3 (lv-sandbox): `git-remote-fixus` helper(依赖 Task 0 结论)

**Files:** Create `crates/git-remote-fixus/`(Cargo.toml + `src/{main.rs, dialer.rs, http.rs}`)

> 方案见"Task 0 结论":gix + 自实现 `Http` trait。blocking。实现时读 `gix-transport/src/client/blocking_io/http/mod.rs` 确认 `Http`/`Transport`/`GetResponse`/`PostResponse` 的确切签名。

- [ ] **Step 3.1: 建 crate** —— `crates/git-remote-fixus/Cargo.toml`(bin `git-remote-fixus`;deps:`gix` features `blocking-network-client`+`http-client`、`rustls`+`webpki-roots`);加入 workspace `members`。`cargo build -p git-remote-fixus`(空 main)通过。

- [ ] **Step 3.2: dialer + 测试** —— `src/dialer.rs::connect(proxy_sock: &Path, host: &str, port: u16) -> rustls TlsStream<UnixStream>`。blocking UDS + SOCKS5h 握手(字节照抄 `sandbox-core/src/proxy.rs` 测试 `socks5h_connect`)+ rustls client 握手。
  测试:起 loopback TLS echo + 一个最小 SOCKS5h-over-TCP 代理(复刻 cr-019 逻辑或直接用 `sandbox-core::proxy`)→ connect 成功,echo 往返;非 allowlist host → `Err`。

- [ ] **Step 3.3: `Http` trait impl + 测试** —— `src/http.rs`:impl `gix_transport::client::blocking_io::http::Http`(`get`/`post`),HTTP/1.1 over dialer 的 TLS 流。**契约(docs.rs 明示):401→`io::Error(PermissionDenied)`,非 2xx→`io::Error(Other)`**;返回 `GetResponse`/`PostResponse`(body reader)。
  测试:loopback HTTP+TLS 上游 → GET/POST roundtrip,断言响应 body + 401/500 的错误映射。

- [ ] **Step 3.4: remote-helper main** —— `src/main.rs`:stdin 命令循环(`capabilities`→报 `fetch`+`push`;`list`;`fetch`/`push`),剥 `fixus::` 前缀(`fixus::https://<host>/<repo>.git`→`https://...`),用我们的 `Http`-backed `Transport` 构造 gix fetch/push,按 helper 协议把 refs/pack 写 stdout。

- [ ] **Step 3.5: 集成测试** —— 本地 `git http-backend`(或 `git daemon --http`)上游 + cr-019 proxy(allowlist 含上游 host)→ `git clone fixus::https://<upstream>/<repo>` 经 helper 成功;`git push` 成功(断言远端收到 commit)。

- [ ] **Step 3.6: Commit**(按子步骤分多次提交)。

---

## Task 4 (lv-fixus): net 声明 → tool-invoke metadata profile hint

**Files:**
- Verify: `src/policy.rs`(`CapabilityPolicy.net` 已建模)
- Modify: `src/bin/tools-bank/adapter.rs` / `main.rs`(tool-invoke metadata 带 net hint)

- [ ] **Step 4.1: 核实 `CapabilityPolicy.net` 存在且可序列化**

Read `src/policy.rs`;确认 `net` 字段 + serde。若 net 已是布尔/枚举,确认 enabled 时能产出 `{"net": ...}` JSON 片段塞进 effective_policy(已是 opaque 透传,见 adapter.rs `build_invoke_meta`)。

- [ ] **Step 4.2: 写失败测试 —— task 声明 net 时 tool-invoke metadata 含 net**

`adapter.rs` tests:CallCtx.effective_policy = Some({"net": {...}}) → `build_invoke_meta` 产出的 metadata 经序列化含 net 字段 → 桥侧(Step 2.4)能识别。
Run: 相关测试 → 据现状 FAIL/PASS。

- [ ] **Step 4.3: 确保 effective_policy 透传 net**

若 `build_invoke_meta` 已把整个 effective_policy 序列化进 metadata(adapter.rs:250-263 已是此模式)→ 无需改,net 随之而过。只需确认 `CapabilityPolicy` 在 task 声明 net 时产出含 `net` key 的 JSON。

- [ ] **Step 4.4: create_task 接受 net 声明(若 protocol 未暴露)**

`src/protocol.rs` create_task body 加 `net: Option<NetCapability>`(或复用现有);流入 task 的 effective_policy。

- [ ] **Step 4.5: 跑测试 + Commit**
```bash
cargo test --lib -- --test-threads=1 policy tools_bank
git commit -m "feat(tools-bank): cr-12 net capability → tool-invoke metadata (profile hint)"
```

---

## Task 5: 部署 helper + 端到端(clone + push)

- [x] **Step 5.1: 装 helper 进 jail PATH** ✓(`sudo install -m 0755` → `/usr/bin/git-remote-fixus`;jail PATH=`/usr/bin:/bin`,helper 可见)

`git-remote-fixus` 二进制装到宿主机 `/usr/bin/git-remote-fixus`(jail env PATH=`/usr/bin:/bin`,env.rs:24,只读挂载)。部署脚本/文档记入。

- [~] **Step 5.2: 起凭据出口代理** — N/A(G1 直连本地 TLS 上游;代理兑换本 CR 外,§5)

- [x] **Step 5.3: E2E —— live clone + push**(pivot:sandbox-server-direct 2 进程,非全 9 进程 broker 栈;证明牢内 CR-12 交付物 —— git profile + CA 注入 + cr-019 代理 + 真 jail + helper clone/push)

按 `dev-stack-startup` memory 起 fixus 栈;create_task 声明 net + repo_url;agent 执行:`git clone fixus::https://github.com/<org>/<repo>` → 改文件 → `git commit` → `git push origin feat/cr12-test`。
预期:clone 成功、push 到 feature branch、turn `steps[]` 含 tool 事件。

- [x] **Step 5.4: 记录 E2E 证据** ✓(push `27e2554..f13ebb9` → 上游 bare 仓 `main` 独立核对;见设计 §5.1)。

---

## Task 6: 安全验收(占位凭据不变真)

- [x] **Step 6.1: 测试 —— in-jail 凭据绕过代理直连 github 必失败** ✓(牢内 `/dev/tcp` 建 AF_INET socket → seccomp `deny_network` KillProcess,exec `status=Killed`)

jail 内用占位凭据直连(不经代理,直接对 `github.com` 发 TLS)→ 因 seccomp AF_UNIX-only 建不出 TCP socket,必失败(连不上)。这是"占位不伪装成真"的硬保证的物理体现。
```bash
# jail 内:git -c http.proxy= -c credential.helper= clone https://github.com/<repo>
# 预期:失败(socket() 被 seccomp KILL)
```

- [x] **Step 6.2: 测试 —— 非白名单 host 被代理拒** ✓(`git ls-remote fixus::https://example.com/x` → `socks5 connect rejected: REP=2`)

agent 尝试 `git clone fixus::https://evil.com/x`(经代理,evil.com 不在 allowlist)→ 代理 reply 0x02 拒绝(已有 `non_allowlisted_denied` 测试覆盖代理侧;这里做端到端)。

- [x] **Step 6.3: 记验收结论入设计文档 §5。** ✓(设计 §5.1 + 本 plan Task 5/6 结论)

---

## Self-Review(写完后自查)

- **Spec 覆盖:** 设计 §3(allowlist)→ Task 1;§4.2(桥翻译 net)→ Task 2 + Task 4;§4.4(agent 自助 clone/push)→ Task 3+5;§5(占位凭据不变真)→ Task 6;snapshot 降级(§6)不在 G1(留 G3)。✓
- **占位扫描:** Task 3 主体依赖 Task 0 结论,显式标"结论后补写"——这是 spike,非占位;其余步骤有具体代码/命令。✓
- **类型一致:** `get_or_create(..., Option<&str>)` 在 session_map/translate/main 三处签名一致;`CapabilityPolicy.net` / effective_policy / metadata net key 跨 fixus↔桥命名一致(Step 2.4 读 `effective_policy.net`,Step 4 产 `net` key)。✓
- **硬依赖:** Task 3 ← Task 0;Task 5 ← Task 1-4;Task 6 ← Task 5。✓

---

## Task 0 结论(已定,2026-07-16)

**决定:用 `gix`,经其 `Http` trait 注入自定义 HTTP 层。**

依据(Step 0.1 核实):`gix-transport::client::blocking_io::http` 暴露 **`Http` trait** ——
"abstract the HTTP operations needed to power all git interactions: read via GET, write via POST",
内置 curl / reqwest 两实现。**自实现 `Http`** = 我们掌控实际 GET/POST(拨 UDS→SOCKS5h→TLS→HTTP/1.1),
gix 负责其上全部 git 协议(协商 / pkt-line / packfile)。**无需手写 git smart-HTTP 协议。**

设计(自底向上,4 层):
1. **dialer**(自写,小):blocking UDS(`std::os::unix::net::UnixStream`)+ SOCKS5h 握手(字节照抄
   `sandbox-core/src/proxy.rs` 测试 `socks5h_connect`)+ rustls TLS 握手 → 返回加密流。
2. **`Http` trait impl**(自写,中等,核心):HTTP/1.1 GET/POST over dialer 的 TLS 流;请求 body 写入 +
   响应 body reader;401→`io::Error(PermissionDenied)`、非 2xx→`io::Error(Other)` 映射;返回 gix 期望的
   `GetResponse` / `PostResponse`。
3. **gix 协议**(零代码):fetch/push 协商与 pack 全交给 gix(在我们 `Http`-backed Transport 之上)。
4. **remote-helper main**(薄):stdin 命令循环(`capabilities`/`list`/`fetch`/`push`),剥 `fixus::`
   前缀(`fixus::https://<host>/<repo>.git` → `https://...`),用我们的 `Http` transport 跑 gix
   fetch/push,按 helper 协议写 refs/pack 到 stdout。

blocking vs async:helper 是 git 拉起的子进程(stdin/stdout 顺序请求/响应),用 **blocking**(无 tokio
runtime)。dialer = blocking UDS + blocking rustls(`rustls::Stream` 包 blocking UnixStream)。

**权衡 —— gix 是大依赖**(gitoxide 全家桶,~数十 transitive crates)。但 helper 是独立 crate
(`crates/git-remote-fixus`),gix **只污染这一个 bin**,不进 sandbox-core/server/broker(核心保持精简),
编译成本隔离。对照手写 ~500 行 smart-HTTP(协议 v0/v2、capabilities、shallows、thin pack、push 报告)
正确性风险高 —— **选 gix**。(若日后要去 gix:dialer + HTTP/1.1 层不变,只把 gix 协议层换成手写。)

**残留风险**:自实现 `Http` trait 的契约细节(body 流式、错误映射、redirect)—— Task 3 TDD 用真本地
git http 上游兜底验证。

(Step 0.3 的"经代理拉 info/refs 最小验证"并入 Task 3.2 的首个测试,不单列。)

---

## Task 4 结论(已定,2026-07-16)

**fixus 侧 net 链路无代码缺口 —— 全套接线已由 sandbox-boundary Phase 1 建好。**

核实(代码事实):
- `create_session_handler` 收 `req.policy: Option<TaskPolicyRequest>`(`protocol.rs:67`),其中
  `TaskPolicyRequest { agent_role(默认 Reader), policy: Option<CapabilityPolicy> }`(`models.rs:889-895`)。
- `resolve_and_validate(operator, tenant_policy, task_policy, agent_role)`(`server.rs:336-340`)算
  effective,写入 task_created event(`broker_store.rs:243` / `models.rs:852`)。
- orchestrator turn_begin payload 带 effective_policy(`orchestrator.rs:786-801`);fixlet 注入
  `X-Fixus-Policy` header(`router.rs:513`);tools-bank `build_invoke_meta` 把 effective_policy
  序列化进 tool-invoke metadata(`adapter.rs:250-263`);桥 `net_profile_override` 读
  `effective_policy.net.egress` 非空 → git profile(commit `02c52eb5`)。**全链已通。**

**G1 配置配方**(三 scope 同声明 + operator role —— 因 `resolve_effective` 是 Operator∩Tenant∩Task
三重 `intersect_net`,只留同时出现在两边的规则;`role_narrow` 在 Reader 下清空 net):

1. **Operator**(env `FIXUS_OPERATOR_POLICY_FILE` TOML):
   ```toml
   [[net.egress]]
   host = "github.com"   # 或凭据出口代理 host
   ports = [443]
   ```
2. **Tenant**(`PUT /api/v1/tenants/{id}/policy`):同上 egress(缺省 = 空 → 交集清空,net 失效)。
3. **Task**(create_session body):
   ```json
   "policy": { "agent_role": "operator",
               "policy": { "net": { "egress": [{ "host": "github.com", "ports": [443] }] } } }
   ```

→ effective_policy.net.egress 非空 + agent_role=operator → git profile → 沙箱开 allowlist 出口。
契约 pin 在 `policy.rs::g1_git_profile_contract_three_scopes_net_survives_with_operator_role`。

**G1 待 live 验证(Task 5)**:三 scope 配齐后,真起全栈确认桥选到 git profile + jail 起代理 + agent clone/push 成功。
