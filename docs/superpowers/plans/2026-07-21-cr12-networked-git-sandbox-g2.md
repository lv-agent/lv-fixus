# CR-12 G2 — 凭据接缝正式化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 CR-12 §5 的"占位假凭据(sentinel)→ 出口代理 fake→real 兑换"从纸面 spec 落成可验证的接缝:牢里 git-remote-fixus helper 每请求带 `Authorization: Bearer <sentinel>`,牢外 reference swap-proxy TLS 终结、识别 sentinel、改写真 token、转发上游。真 token 只在 swap-proxy 进程内;绕过代理直连必败(seccomp,G1 已验)。

**Architecture:** sentinel 走 git profile 既有 `env` 接缝(紧邻 `SANDBOX_CA_PEM`)进牢 → helper(`FixusHttp::with_default_roots`)从自身 env 读 `FIXUS_GIT_SENTINEL`、在 `get`/`post` 层把 `Authorization` 头插进每个请求 → 牢内 UDS→SOCKS5h(allowlist 收口到 swap-proxy host)→ rustls TLS → swap-proxy(新 crate,牢笼外,TLS 终结点 = 信任边界)解析头部块、把 sentinel 行改写成真 token、TLS 连真上游(github.com:443)转发。在 repo 的 SOCKS proxy(`proxy.rs`)**零改动**(透明中继,符合 `docs/security.md:122-123`)。

**Tech Stack:** Rust / Tokio(swap-proxy)/ blocking rustls(helper,现状);新 crate `crates/egress-swap-proxy` = `tokio` + `tokio-rustls` + `rustls` 0.23 + `rustls-pemfile` + `webpki-roots`(dev:`rcgen`)。

**Design spec:** `veps/cr-12-networked-git-sandbox-design.md` §5(权威)。G1 plan:`docs/superpowers/plans/2026-07-16-cr12-networked-git-sandbox-g1.md`。

**范围决策(已与用户确认):**
- sentinel 注入机制 = **helper Authorization 头**(非 git credential helper)。理由:env 接缝已存在、helper 已 own transport、零新文件接缝、sentinel 全程在 TLS 隧道内。
- G2 = **只做凭据接缝**。G1 遗留硬化(git 协议 v2 / 流式 push / 边缘测试)留 G1.5。reference swap-proxy E2E 用**本地受控上游**(hermetic,不对真 github,故不卡 v2)。

**fixus 侧:无改动。** G1 plan Task 4 已确认 net 声明→tool-invoke metadata 全链由 Phase 1 接好;sentinel 是 operator→lv-sandbox profile.env 的事,不经过 fixus。本 plan 纯 lv-sandbox。

**Cross-repo:** 本 plan 文档居 lv-fixus;实现全在 lv-sandbox(`/home/lvtao/lv/lv-sandbox/`)。

**安全不变量(本 plan 验收对象):**
1. in-jail 凭据绕过代理直连 github 必败(seccomp `deny_network`,G1 已验;G2 复确认 sentinel 在场不变结论)。
2. 真 token 只存在于 swap-proxy 进程内(helper 只发 sentinel;sentinel 非密可公开)。
3. sentinel 错误/缺失 → swap-proxy 401(不转发)。

---

## 关键约束(已核实,代码事实)

- `git-remote-fixus/src/main.rs:38` = `FixusHttp::with_default_roots(proxy_sock)`。⇒ sentinel 在 `with_default_roots` 内读 env = **零 main.rs 改动**。
- `git-remote-fixus/src/http.rs:549`(get)/ `:584`(post):`headers` 入参被收成 `hdrs: Vec<String>`。Authorization 在这两处 `hdrs.insert(0, ...)` 即覆盖所有请求(smart.rs 三个调用点 list/fetch/push 无需改)。
- `sandbox-core/src/profile.rs:196-198`:`SANDBOX_CA_PEM` 注入 `p.env` 的模式 —— sentinel 照抄,走同一接缝(`build_sanitized_env` `env.rs:40-45` 透传非 PROTECTED key)。
- profile 测试有 `GIT_ENV_LOCK` mutex(`profile.rs:319-322`)序列化所有动 `FIXUS_GIT_*` 进程 env 的测试 —— sentinel env 胶水测试必须加入此互斥集。
- helper 是 **blocking**(无 tokio runtime);swap-proxy 是 **async**(tokio)。两者不同 crate,不混。
- helper 每请求新拨一条连接 + 发 `Connection: close`(`http.rs:175-177`)⇒ swap-proxy 可**一请求一连接**,做"头部改写中继":读到 `\r\n\r\n` 为头部块,改写 Authorization 行,其余字节(请求体)透传。无需完整 HTTP 解析。
- `spawn_tls_http` 测试 helper(`http.rs:641-705`)的 responder 收到原始请求字节 `req: &[u8]` ⇒ 可直接据 `req.contains(b"Authorization: Bearer ...")` 分支返回,无需额外捕获设施。
- workspace `Cargo.toml` 有 `tokio`(全特性)、`axum 0.8`、`reqwest 0.12`;**无** `rustls`/`tokio-rustls` 于 workspace deps(git-remote-fixus 在 crate 级引 `rustls 0.23`)。新 swap-proxy crate 须在 workspace deps 加 `tokio-rustls` + `rustls`。

---

## File Structure

**lv-sandbox(`/home/lvtao/lv/lv-sandbox/`)**
- Modify: `crates/sandbox-core/src/profile.rs` — `git_inner` 加 `sentinel` 入参 + `git()` 读 `FIXUS_GIT_SENTINEL` env + 3 个测试
- Modify: `crates/git-remote-fixus/src/http.rs` — `FixusHttp` 加 `auth_header` 字段 + `new`/`with_default_roots`/test ctor + get/post 注入 + 2 个测试
- Create: `crates/egress-swap-proxy/` — 新 crate(`Cargo.toml` + `src/{main.rs, swap.rs, config.rs}` + `tests/swap_e2e.rs`)
- Modify: `Cargo.toml`(workspace `members` 加 `crates/egress-swap-proxy` + `[workspace.dependencies]` 加 `tokio-rustls`/`rustls`/`rustls-pemfile`/`webpki-roots`/`rcgen`)
- Modify: `docs/security.md` / `docs/usage.md` / `docs/network-isolation.md` + `docs/zh/*` 镜像 — sentinel 接缝正式化 + reference proxy 说明

**lv-fixus**:无改动(plan 文档除外)。

---

## Task 1 (sandbox-core): git profile 注入 sentinel env

**Files:**
- Modify: `crates/sandbox-core/src/profile.rs`(`git_inner` 签名 + `git()` env 读 + tests)

- [ ] **Step 1.1: 写失败测试 —— `git_inner` 注入 sentinel**

`crates/sandbox-core/src/profile.rs` tests 末尾(`mod tests` 内,`git_profile_no_ca_when_absent` 后)加:
```rust
#[test]
fn git_profile_injects_sentinel_when_provided() {
    // 纯 seam:sentinel 走 profile.env,与 SANDBOX_CA_PEM 同接缝。
    let g = SandboxProfile::git_inner("github.com", 443, None, Some("sentinel-XYZ"));
    assert_eq!(
        g.env.get("FIXUS_GIT_SENTINEL").map(String::as_str),
        Some("sentinel-XYZ"),
        "sentinel provided → FIXUS_GIT_SENTINEL injected verbatim"
    );
}

#[test]
fn git_profile_no_sentinel_when_absent() {
    let g = SandboxProfile::git_inner("github.com", 443, None, None);
    assert!(!g.env.contains_key("FIXUS_GIT_SENTINEL"), "no sentinel → no injection");
}
```
Run: `cargo test -p sandbox-core git_profile_injects_sentinel git_profile_no_sentinel`
Expected: FAIL(`git_inner` 无第 4 参,编译错)。

- [ ] **Step 1.2: `git_inner` 加 sentinel 入参 + 注入**

`crates/sandbox-core/src/profile.rs:178`,签名 + body(`SANDBOX_CA_PEM` 注入块 `:196-198` 之后):
```rust
fn git_inner(host: &str, port: u16, ca_pem: Option<&str>, sentinel: Option<&str>) -> Self {
    let mut p = Self::shell();
    // ...（现有 rlimit / timeout / egress / CA 注入不变）...
    // cr-12 CA 注入(见 read_git_ca_pem)。env 通道免 jail fs 依赖。
    if let Some(pem) = ca_pem {
        p.env.insert("SANDBOX_CA_PEM".to_string(), pem.to_string());
    }
    // cr-12 G2: sentinel 占位凭据进牢(helper 据此加 Authorization 头;出口代理 fake→real 兑换)。
    // 非密可公开;真 token 只在牢外 swap-proxy 进程内。与 CA 同走 env 接缝。
    if let Some(s) = sentinel {
        p.env.insert("FIXUS_GIT_SENTINEL".to_string(), s.to_string());
    }
    // ...（现有 /dev/null extra_writable 不变）...
    p
}
```

- [ ] **Step 1.3: `git()` 读 `FIXUS_GIT_SENTINEL` env**

`crates/sandbox-core/src/profile.rs:163-174`,在 `git()` 内读 sentinel 并传入:
```rust
pub fn git() -> Self {
    let host = std::env::var("FIXUS_GIT_EGRESS_HOST")
        .unwrap_or_else(|_| "github.com".to_string());
    let port = std::env::var("FIXUS_GIT_EGRESS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(443);
    let ca_pem = read_git_ca_pem();
    let sentinel = std::env::var("FIXUS_GIT_SENTINEL")
        .ok()
        .filter(|s| !s.trim().is_empty()); // trim:空白串不注入(与 read_ca_pem_from_path 一致;Step 1.4 测试要求)
    Self::git_inner(&host, port, ca_pem.as_deref(), sentinel.as_deref())
}
```

- [ ] **Step 1.4: env 胶水测试(加入 GIT_ENV_LOCK 互斥集)**

`crates/sandbox-core/src/profile.rs` tests 加(紧邻 `git_reads_fixus_git_ca_file_env`,共用 `lock_git_env`):
```rust
#[test]
fn git_reads_fixus_git_sentinel_env() {
    // env → 入参 胶水冒烟;与其它 FIXUS_GIT_* env 测试互斥(mutex 序列化)。
    let _g = lock_git_env();
    std::env::set_var("FIXUS_GIT_SENTINEL", "env-sentinel-ABC");
    let g = SandboxProfile::git();
    std::env::remove_var("FIXUS_GIT_SENTINEL");
    assert_eq!(
        g.env.get("FIXUS_GIT_SENTINEL").map(String::as_str),
        Some("env-sentinel-ABC"),
        "FIXUS_GIT_SENTINEL env → profile.env wiring"
    );

    std::env::set_var("FIXUS_GIT_SENTINEL", "   ");
    let g = SandboxProfile::git();
    std::env::remove_var("FIXUS_GIT_SENTINEL");
    assert!(
        !g.env.contains_key("FIXUS_GIT_SENTINEL"),
        "blank sentinel → not injected"
    );
}
```

- [ ] **Step 1.5: 跑测试 + 更新既有调用处**

`git_inner` 签名变了 → 修所有调用点(grep `git_inner`):`git()` 已改;tests 里 `git_profile_injects_ca_pem_when_provided`、`git_profile_no_ca_when_absent`、`git_profile_egress_port_overridable`(各传一个 `None`/`Some(..)` 第 4 参)。
Run: `cargo test -p sandbox-core git_` → 全 PASS。

- [ ] **Step 1.6: Commit**
```bash
git add crates/sandbox-core/src/profile.rs
git commit -m "feat(sandbox-core): cr-12 G2 git profile injects sentinel credential (FIXUS_GIT_SENTINEL env)"
```

---

## Task 2 (git-remote-fixus): helper 每请求带 Authorization 头

**Files:**
- Modify: `crates/git-remote-fixus/src/http.rs`(`FixusHttp` 结构 + ctor + get/post 注入 + tests)

- [ ] **Step 2.1: 写失败测试 —— 设了 sentinel 时请求带 Bearer**

`crates/git-remote-fixus/src/http.rs` tests 末尾加。responder 据请求是否含正确 Bearer 返回 200/401:
```rust
#[test]
fn get_sends_bearer_when_auth_set() {
    // 上游只在见到正确 Bearer 时返回 200;helper 设了 auth_header ⇒ 应 200。
    let sentinel = "test-sentinel-123";
    let port = spawn_tls_http(move |_m, _p, req| {
        if req.windows(15).any(|w| w == b"Authorization: ") {
            let want = format!("Authorization: Bearer {sentinel}");
            if req.windows(want.len()).any(|w| w == want.as_bytes()) {
                return http_200("ok-bearer");
            }
        }
        http_status(401, "Unauthorized")
    });
    thread::sleep(std::time::Duration::from_millis(50));
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join(".proxy.sock");
    let _proxy = spawn_proxy(sock.clone());
    wait_sock(&sock);

    let auth = format!("Authorization: Bearer {sentinel}");
    let mut http = FixusHttp::with_insecure_and_auth(sock, Some(auth));
    let url = format!("https://localhost:{port}/info/refs");
    let mut resp = http.get(&url, "", std::iter::empty::<&str>()).expect("get ok");
    drop(resp.headers);
    let mut body = String::new();
    resp.body.read_to_string(&mut body).unwrap();
    assert_eq!(body, "ok-bearer", "auth_header set → upstream saw bearer → 200");
}

#[test]
fn get_no_bearer_when_absent_is_401() {
    // 同一上游,helper 未设 auth_header ⇒ 无 Bearer ⇒ 401(PermissionDenied)。
    let port = spawn_tls_http(|_m, _p, req| {
        if req.windows(15).any(|w| w == b"Authorization: ") {
            return http_200("should-not-happen");
        }
        http_status(401, "Unauthorized")
    });
    thread::sleep(std::time::Duration::from_millis(50));
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join(".proxy.sock");
    let _proxy = spawn_proxy(sock.clone());
    wait_sock(&sock);

    let mut http = FixusHttp::with_insecure(sock); // 无 auth
    let url = format!("https://localhost:{port}/x");
    let mut resp = http.get(&url, "", std::iter::empty::<&str>()).expect("get returns response");
    drop(resp.headers);
    let mut sink = String::new();
    let err = resp.body.read_to_string(&mut sink).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied, "no bearer → 401");
}
```
Run: `cargo test -p git-remote-fixus --lib get_sends_bearer_when_auth_set get_no_bearer_when_absent_is_401`
Expected: FAIL(`FixusHttp` 无 `auth_header` 字段 / `with_insecure_and_auth` 不存在,编译错)。

- [ ] **Step 2.2: `FixusHttp` 加 `auth_header` 字段 + ctor**

`crates/git-remote-fixus/src/http.rs:28-49`,改 struct + 三个 ctor:
```rust
pub struct FixusHttp {
    proxy_sock: PathBuf,
    config: Arc<ClientConfig>,
    /// cr-12 G2:每个请求注入的 Authorization 头(由 `FIXUS_GIT_SENTINEL` env 构造)。
    /// None = 不加(匿名上游,G1 兼容)。sentinel 非密;真 token 在牢外 swap-proxy。
    auth_header: Option<String>,
}

impl FixusHttp {
    pub fn new(
        proxy_sock: PathBuf,
        config: Arc<ClientConfig>,
        auth_header: Option<String>,
    ) -> Self {
        Self { proxy_sock, config, auth_header }
    }

    /// CA 信任(优先级 SANDBOX_CA_PEM > SANDBOX_CA_FILE > webpki-roots)
    /// + sentinel(`FIXUS_GIT_SENTINEL`)→ Authorization 头。两者均从 jail env 读
    /// (由 git profile 注入)。⇒ main.rs:38 的 `with_default_roots` 调用无需改。
    pub fn with_default_roots(proxy_sock: PathBuf) -> Self {
        let auth_header = std::env::var("FIXUS_GIT_SENTINEL")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| format!("Authorization: Bearer {s}"));
        Self::new(proxy_sock, dialer::env_client_config(), auth_header)
    }

    #[cfg(test)]
    pub(crate) fn with_insecure(proxy_sock: PathBuf) -> Self {
        Self::new(proxy_sock, dialer::insecure_client_config(), None)
    }

    #[cfg(test)]
    pub(crate) fn with_insecure_and_auth(
        proxy_sock: PathBuf,
        auth_header: Option<String>,
    ) -> Self {
        Self::new(proxy_sock, dialer::insecure_client_config(), auth_header)
    }
}
```

- [ ] **Step 2.3: `get` 注入 Authorization**

`crates/git-remote-fixus/src/http.rs:549`(get 内,`let hdrs: Vec<String> = ...` 处):
```rust
        let mut hdrs: Vec<String> = headers.into_iter().map(|h| h.as_ref().to_string()).collect();
        if let Some(auth) = &self.auth_header {
            hdrs.insert(0, auth.clone());
        }
        conn.send_request("GET", &path, &host, &hdrs, None)
            .map_err(io_err)?;
```

- [ ] **Step 2.4: `post` 注入 Authorization**

`crates/git-remote-fixus/src/http.rs:584`(post 内,`let hdrs: Vec<String> = ...` 处,注意 `hdrs` 之后移入 `PostBody.headers`):
```rust
        let mut hdrs: Vec<String> = headers.into_iter().map(|h| h.as_ref().to_string()).collect();
        if let Some(auth) = &self.auth_header {
            hdrs.insert(0, auth.clone());
        }
        let shared = Arc::new(Mutex::new(conn));
```
(`hdrs` 随后照原样移入 `PostBody { headers: hdrs, .. }`,Authorization 一并被 drop 时发出。)

- [ ] **Step 2.5: 跑测试 + 既有测试无回归**

Run: `cargo test -p git-remote-fixus`
Expected: 新 2 测试 PASS + 既有(get_returns_body / get_401_maps... / post_writes_body...)全 PASS(无 auth 时 `auth_header=None`,行为不变)。

- [ ] **Step 2.6: Commit**
```bash
git add crates/git-remote-fixus/src/http.rs
git commit -m "feat(git-remote-fixus): cr-12 G2 inject Authorization: Bearer <sentinel> per request"
```

---

## Task 3 (新 crate): `egress-swap-proxy` —— 牢外 sentinel→real 兑换代理

**Files:** Create `crates/egress-swap-proxy/`(`Cargo.toml` + `src/{main.rs, config.rs, swap.rs}` + `tests/swap_e2e.rs`);Modify workspace `Cargo.toml`。

设计 = **头部改写中继**(非完整 HTTP 解析):TLS 终结入站 → 读到 `\r\n\r\n` 取头部块 → 纯函数改写 Authorization 行 → TLS 连真上游 → 发改写后头部 + 透传请求体 / 回传响应。helper 每请求新连接 + `Connection: close` ⇒ 一请求一连接。

- [ ] **Step 3.1: workspace deps + crate 骨架**

`Cargo.toml` `[workspace.dependencies]` 加(webpki-roots/rcgen 版本对齐 git-remote-fixus 的 rustls 0.23):
```toml
rustls = { version = "0.23", features = ["ring", "tls12", "std"] }
tokio-rustls = "0.26"
rustls-pemfile = "2"
webpki-roots = "1"
rcgen = "0.13"
```
`Cargo.toml` `[workspace] members` 加 `"crates/egress-swap-proxy"`。

新建 `crates/egress-swap-proxy/Cargo.toml`:
```toml
[package]
name = "egress-swap-proxy"
version.workspace = true
edition.workspace = true
license.workspace = true
rust-version.workspace = true

[dependencies]
tokio = { workspace = true }
tokio-rustls = { workspace = true }
rustls = { workspace = true }
rustls-pemfile = { workspace = true }
webpki-roots = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }

[dev-dependencies]
rcgen = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread", "net"] }

[[bin]]
name = "fixus-egress-swap-proxy"
path = "src/main.rs"
```
`cargo build -p egress-swap-proxy`(空 main)通过。

- [ ] **Step 3.2: 写失败测试 —— 纯函数 `rewrite_authorization`**

新建 `crates/egress-swap-proxy/src/swap.rs`:
```rust
//! cr-12 G2:头部改写核心。纯函数,便于单测。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SwapError {
    #[error("missing Authorization header")]
    MissingAuthorization,
    #[error("sentinel mismatch")]
    SentinelMismatch,
    #[error("malformed header block (no CRLF CRLF terminator)")]
    MalformedHeaderBlock,
}

/// 在 HTTP/1.1 头部块(到 `\r\n\r\n` 为止,不含终止空行)里:
/// 找 `authorization: bearer <sentinel>`(大小写不敏感),改写成 `Authorization: Bearer <real_token>`。
/// 返回改写后的头部块(原样保留其余行与字节序,仅替换该行)。
pub fn rewrite_authorization(
    header_block: &[u8],
    sentinel: &str,
    real_token: &str,
) -> Result<Vec<u8>, SwapError> {
    let block = std::str::from_utf8(header_block).map_err(|_| SwapError::MalformedHeaderBlock)?;
    // 首行是 request line;其余每行一个 header。
    let mut lines: Vec<&str> = block.split("\r\n").collect();
    // request line(行 0)不动;从行 1 找 Authorization。
    let mut found = false;
    for line in lines.iter_mut().skip(1) {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("authorization") {
                let v = value.trim();
                let want = format!("Bearer {sentinel}");
                if v.eq_ignore_ascii_case(&want) {
                    *line = format!("Authorization: Bearer {real_token}");
                    found = true;
                    break;
                } else {
                    return Err(SwapError::SentinelMismatch);
                }
            }
        }
    }
    if !found {
        return Err(SwapError::MissingAuthorization);
    }
    Ok(lines.join("\r\n").into_bytes())
}
```
`src/swap.rs` tests:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn block(req_line: &str, auth: Option<&str>) -> Vec<u8> {
        let mut lines: Vec<String> = vec![req_line.to_string()];
        if let Some(a) = auth {
            lines.push(format!("Authorization: {a}"));
        }
        lines.push("Host: x".to_string());
        lines.join("\r\n").into_bytes()
    }

    #[test]
    fn rewrites_matching_sentinel() {
        let b = block("GET /info/refs HTTP/1.1", Some("Bearer sent-XYZ"));
        let out = rewrite_authorization(&b, "sent-XYZ", "real-TOKEN").unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("Authorization: Bearer real-TOKEN"), "{s}");
        assert!(!s.contains("sent-XYZ"), "sentinel must not survive: {s}");
        assert!(s.contains("GET /info/refs HTTP/1.1"), "request line preserved");
        assert!(s.contains("Host: x"), "other headers preserved");
    }

    #[test]
    fn case_insensitive_header_name_and_value() {
        let b = block("POST /g HTTP/1.1", Some("bearer sent-XYZ"));
        let out = rewrite_authorization(&b, "sent-XYZ", "real").unwrap();
        assert!(String::from_utf8(out).unwrap().contains("Authorization: Bearer real"));
    }

    #[test]
    fn mismatched_sentinel_errors() {
        let b = block("GET / HTTP/1.1", Some("Bearer wrong"));
        assert!(matches!(
            rewrite_authorization(&b, "sent-XYZ", "real"),
            Err(SwapError::SentinelMismatch)
        ));
    }

    #[test]
    fn missing_authorization_errors() {
        let b = block("GET / HTTP/1.1", None);
        assert!(matches!(
            rewrite_authorization(&b, "sent-XYZ", "real"),
            Err(SwapError::MissingAuthorization)
        ));
    }
}
```
Run: `cargo test -p egress-swap-proxy --lib rewrites_matching_sentinel`
Expected: FAIL(crate 空主体)。然后实现 `swap.rs`(Step 3.2 已含实现)→ PASS 全 4。

- [ ] **Step 3.3: 实现 `swap.rs`**(代码见 Step 3.2,一并落地)→ 跑 `cargo test -p egress-swap-proxy --lib` → 4 PASS。

- [ ] **Step 3.4: config.rs —— env 读取**

新建 `crates/egress-swap-proxy/src/config.rs`:
```rust
//! cr-12 G2:swap-proxy 配置(全 env)。reference 实现 = 单 sentinel → 单 real token。

#[derive(Debug, Clone)]
pub struct SwapConfig {
    pub listen: String,        // FIXUS_SWAP_LISTEN, 默认 "127.0.0.1:8443"
    pub sentinel: String,      // FIXUS_SWAP_SENTINEL(必填)
    pub real_token: String,    // FIXUS_SWAP_TOKEN(必填)
    pub upstream: String,      // FIXUS_SWAP_UPSTREAM host:port, 默认 "github.com:443"
    pub cert_pem: Option<String>,  // FIXUS_SWAP_CERT_PEM(内容);缺则自签(测试用)
    pub key_pem: Option<String>,   // FIXUS_SWAP_KEY_PEM(内容)
    pub upstream_ca_pem: Option<String>, // FIXUS_SWAP_UPSTREAM_CA_PEM;缺则 webpki-roots
}

impl SwapConfig {
    pub fn from_env() -> Result<Self, String> {
        let sentinel = std::env::var("FIXUS_SWAP_SENTINEL")
            .map_err(|_| "FIXUS_SWAP_SENTINEL required".to_string())?;
        let real_token = std::env::var("FIXUS_SWAP_TOKEN")
            .map_err(|_| "FIXUS_SWAP_TOKEN required".to_string())?;
        Ok(Self {
            listen: std::env::var("FIXUS_SWAP_LISTEN")
                .unwrap_or_else(|_| "127.0.0.1:8443".to_string()),
            sentinel,
            real_token,
            upstream: std::env::var("FIXUS_SWAP_UPSTREAM")
                .unwrap_or_else(|_| "github.com:443".to_string()),
            cert_pem: std::env::var("FIXUS_SWAP_CERT_PEM").ok().filter(|s| !s.is_empty()),
            key_pem: std::env::var("FIXUS_SWAP_KEY_PEM").ok().filter(|s| !s.is_empty()),
            upstream_ca_pem: std::env::var("FIXUS_SWAP_UPSTREAM_CA_PEM").ok().filter(|s| !s.is_empty()),
        })
    }
}
```
(单元测试:`from_env_requires_sentinel_and_token` —— set/unset env 断言 Ok/Err。)

- [ ] **Step 3.5: main.rs —— TLS 终结 + 头部改写 + 转发中继**

新建 `crates/egress-swap-proxy/src/main.rs`(骨架,关键逻辑齐全):
```rust
//! cr-12 G2 reference swap-proxy:牢外 sentinel→real 兑换。
//! 牢内 helper → UDS→SOCKS5h(allowlist 收口到本代理)→ TLS → 本代理。
//! 本代理 TLS 终结(信任边界),改写 Authorization,再 TLS 连真上游转发。
//! 一请求一连接(helper 每请求新拨 + Connection: close)。

mod config;
mod swap;

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::rustls;
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into()),
    ).init();

    let cfg = config::SwapConfig::from_env().map_err(|e| {
        eprintln!("egress-swap-proxy config error: {e}");
        std::process::exit(2);
    })?;

    let server_cfg = build_server_config(&cfg)?;
    let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
    let upstream_cfg = Arc::new(build_upstream_client_config(&cfg)?);

    let listener = TcpListener::bind(&cfg.listen).await?;
    info!(listen = %cfg.listen, upstream = %cfg.upstream, "egress-swap-proxy ready");

    let cfg = Arc::new(cfg);
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => { warn!(error=%e, "accept failed"); continue; }
        };
        let acceptor = acceptor.clone();
        let cfg = cfg.clone();
        let up = upstream_cfg.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(tcp, acceptor, cfg, up).await {
                warn!(%peer, error=%e, "conn handler");
            }
        });
    }
}

async fn handle_conn(
    tcp: tokio::net::TcpStream,
    acceptor: TlsAcceptor,
    cfg: Arc<config::SwapConfig>,
    upstream_cfg: Arc<rustls::ClientConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut tls = acceptor.accept(tcp).await?;

    // 1) 读到头部块结束(\r\n\r\n)。
    let mut buf = Vec::with_capacity(2048);
    loop {
        let mut tmp = [0u8; 4096];
        let n = tls.read(&mut tmp).await?;
        if n == 0 { return Ok(()); } // 客户端先关,无完整请求
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") { break; }
        if buf.len() > 64 * 1024 {
            tls.write_all(b"HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Length: 0\r\n\r\n").await?;
            return Ok(());
        }
    }
    let hdr_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let header_block = &buf[..hdr_end];          // 不含尾 \r\n\r\n
    let body_so_far = &buf[hdr_end + 4..];       // 头之后已读到的请求体字节

    // 2) 改写 Authorization。
    let rewritten = match swap::rewrite_authorization(header_block, &cfg.sentinel, &cfg.real_token) {
        Ok(b) => b,
        Err(e) => {
            tls.write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n").await?;
            info!(error=%e, "rejected (sentinel missing/mismatch)");
            return Ok(());
        }
    };

    // 3) 连真上游(TLS)。
    let (up_host, up_port) = split_host_port(&cfg.upstream);
    let up_tcp = tokio::net::TcpStream::connect((up_host.as_str(), up_port)).await?;
    let connector = tokio_rustls::TlsConnector::from(upstream_cfg);
    let mut up = connector.connect(rustls::pki_types::ServerName::try_from(up_host.clone())?, up_tcp).await?;

    // 4) 发改写后头部 + 头之后已读体。
    up.write_all(&rewritten).await?;
    up.write_all(b"\r\n\r\n").await?;
    if !body_so_far.is_empty() {
        up.write_all(body_so_far).await?;
    }
    up.flush().await?;

    // 5) 双向中继:客户端→上游(剩余请求体)+ 上游→客户端(响应)。
    let mut tls_r = tls;
    let (mut rio, mut wio) = tls_r.split();
    let c2u = tokio::io::copy(&mut rio, &mut up);
    tokio::pin!(c2u);
    // 上游→客户端
    let mut up = up;
    let u2c = tokio::io::copy(&mut up, &mut wio);
    tokio::pin!(u2c);
    let _ = tokio::time::timeout(Duration::from_secs(300), async {
        tokio::select! {
            _ = &mut c2u => {}
            _ = &mut u2c => {}
        }
    }).await;
    let _ = wio.shutdown().await;
    Ok(())
}

fn split_host_port(s: &str) -> (String, u16) {
    match s.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(443)),
        None => (s.to_string(), 443),
    }
}

fn build_server_config(cfg: &config::SwapConfig)
    -> Result<rustls::ServerConfig, Box<dyn std::error::Error>>
{
    use rustls::ServerConfig;
    let (cert_chain, key_der) = match (&cfg.cert_pem, &cfg.key_pem) {
        (Some(cert), Some(key)) => (load_certs(cert)?, load_key(key)?),
        _ => {
            // 测试回退:运行期自签(production 必须由 operator 提供证)。
            warn!("FIXUS_SWAP_CERT_PEM/KEY_PEM absent → generating ephemeral self-signed (test only)");
            ephemeral_self_signed()?
        }
    };
    Ok(ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key_der)?)
}

fn build_upstream_client_config(cfg: &config::SwapConfig)
    -> Result<rustls::ClientConfig, Box<dyn std::error::Error>>
{
    let root_store = match &cfg.upstream_ca_pem {
        Some(pem) => {
            let mut s = rustls::RootCertStore::empty();
            for c in rustls_pemfile::certs(&mut pem.as_bytes()) {
                s.add(c?)?;
            }
            s
        }
        None => rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
        },
    };
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}

fn load_certs(pem: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, Box<dyn std::error::Error>> {
    let mut v = Vec::new();
    for c in rustls_pemfile::certs(&mut pem.as_bytes()) { v.push(c?.into_owned()); }
    Ok(v)
}
fn load_key(pem: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>, Box<dyn std::error::Error>> {
    rustls_pemfile::private_key(&mut pem.as_bytes())
        .map_err(|e| e.into())
        .and_then(|k| k.ok_or("no private key in PEM".into()))
}
fn ephemeral_self_signed() -> Result<(Vec<rustls::pki_types::CertificateDer<'static>>, rustls::pki_types::PrivateKeyDer<'static>), Box<dyn std::error::Error>> {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
    let cert = rustls::pki_types::CertificateDer::from(ck.cert.der().to_vec());
    let key = rustls::pki_types::PrivateKeyDer::try_from(ck.signing_key.serialize_der())?;
    Ok((vec![cert], key))
}
```
> **注:** `load_key`/`split` 等 API 细节(rustls 0.23 + rcgen 0.13 的确切类型转换、`pem.as_byteers()` 笔误等)在实现时以 `cargo check` 修正为准 —— 上述为结构骨架,核心控制流(读头→改写→转发→中继)齐全且无占位。`tls.split()` 需要 `tokio::io::AsyncWrite`/`AsyncRead` 分裂;若 0.26 API 不支持半分裂,改为 spawn 两个 task(一 copy c2u、一 copy u2c)join。

- [ ] **Step 3.6: `cargo check -p egress-swap-proxy`** 修 API 类型错(rustls 0.23 / rcgen 0.13 / tokio-rustls 0.26 的确切签名)。空跑 `cargo run -p egress-swap-proxy`(缺 env → 退出码 2 + 配置错误信息)。

- [ ] **Step 3.7: Commit**
```bash
git add Cargo.toml Cargo.lock crates/egress-swap-proxy
git commit -m "feat(egress-swap-proxy): cr-12 G2 reference sentinel→real swap proxy"
```

---

## Task 4: E2E 不变量验证(hermetic 集成测试)

**Files:** `crates/egress-swap-proxy/tests/swap_e2e.rs`

栈(全本地,无真 github):rcgen 造 CA+证 → 起"要求 real-token"的 fake 上游(tokio TLS HTTP,记录收到的 Authorization)→ 起 swap-proxy(FIXUS_SWAP_SENTINEL/ TOKEN/ UPSTREAM= fake 上游)→ 用牢 helper 协议(GET info/refs 带 sentinel)经 UDS→SOCKS5h→swap-proxy→fake 上游,断言上游收到的是 real-token。

- [ ] **Step 4.1: 写 E2E 测试**

`crates/egress-swap-proxy/tests/swap_e2e.rs`:
```rust
//! cr-12 G2 E2E:helper 带 sentinel → swap-proxy 改写 → 上游只见 real-token。
//! 复用 git-remote-fixus 的 spawn_tls_http / spawn_proxy 模式(此处内联最小版,避免跨 crate test-dep)。

use std::sync::Arc;
use std::sync::Mutex;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn swap_proxy_replaces_sentinel_with_real_token_end_to_end() {
    // 1) 记录上游收到的 Authorization。
    let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    // 2) 起 fake 上游:仅 Authorization: Bearer real-TOKEN 放行,并记录值。
    let upstream_port = spawn_fake_upstream(seen.clone(), "real-TOKEN").await;
    // 3) 起 swap-proxy(独立线程 / 子进程),UPSTREAM=127.0.0.1:upstream_port。
    let (proxy_port, _cert_pem, ca_pem) =
        run_swap_proxy("sent-SENT", "real-TOKEN", upstream_port).await;
    // 4) helper 经 UDS→SOCKS5h(allowlist=127.0.0.1:proxy_port)→ TLS(信 ca_pem)
    //    带 Authorization: Bearer sent-SENT 发 GET /info/refs。
    //    (此处用 FixusHttp::with_insecure_and_auth 直测 http 层,或经 git-remote-fixus
    //     binary 子进程;最小验证用 http 层 roundtrip。)
    let got = http_get_via_proxy(proxy_port, &ca_pem, "Authorization: Bearer sent-SENT").await;
    assert_eq!(got, "upstream-ok", "request with sentinel must reach upstream (swapped)");

    let captured = seen.lock().unwrap().clone();
    assert_eq!(captured.as_deref(), Some("Bearer real-TOKEN"),
        "upstream must see REAL token, not sentinel");
}
```
> 实现细节(`spawn_fake_upstream` / `run_swap_proxy` 用 in-process tokio task 还是子进程;`http_get_via_proxy` 用 rustls client)在 Step 4.1 落地时按 swap-proxy 的 public test seam 写。**不变量断言已明确**:上游 `seen == Bearer real-TOKEN`(非 sentinel)。

- [ ] **Step 4.2: 加不变量 2/3 测试**

同文件加:
- `wrong_sentinel_returns_401`:helper 带 `Bearer wrong` → 经 swap-proxy → 401(上游未被调用,`seen` 仍 None)。
- `real_token_never_reaches_jail_side`:断言 helper 发出的字节只含 `sent-SENT`,不含 `real-TOKEN`(real-token 串只在 swap-proxy 进程内 + 上游侧)。可在 `http_get_via_proxy` 捕获发出的请求字节断言。

Run: `cargo test -p egress-swap-proxy --test swap_e2e`
Expected: 全 PASS。

- [ ] **Step 4.3: Commit**
```bash
git add crates/egress-swap-proxy/tests/swap_e2e.rs
git commit -m "test(egress-swap-proxy): cr-12 G2 E2E sentinel→real swap + invariants"
```

---

## Task 5: 文档更新(sentinel 接缝正式化)

**Files:** `docs/security.md` / `docs/usage.md` / `docs/network-isolation.md` + `docs/zh/{security,usage,network-isolation}.md`

- [ ] **Step 5.1: security.md `Git egress & sentinel credential model` 段(`:76-114`)**

把 `:108` "The swap-proxy body and the sentinel scheme are out of CR-12 scope (operator-implemented)." 改写为:
> **G2(2026-07-21):sentinel 接缝正式化。** 牢内 `git-remote-fixus` helper 每请求带 `Authorization: Bearer <sentinel>`(sentinel 由 git profile 经 `FIXUS_GIT_SENTINEL` env 注入,非密可公开)。reference swap-proxy 见 `crates/egress-swap-proxy`(bin `fixus-egress-swap-proxy`):TLS 终结、识别 sentinel、改写真 token、转发上游;真 token 只在该进程内。生产 swap-proxy 仍可由 operator 实现(同接缝契约)。不变量:绕代理直连必败(seccomp);sentinel 错/缺 → 401。

- [ ] **Step 5.2: usage.md 凭据段(`:416-428`)**

加运行 reference swap-proxy 的 env 配方(`FIXUS_SWAP_LISTEN/SENTINEL/TOKEN/UPSTREAM/CERT_PEM/KEY_PEM`)+ 牢侧配方(operator `FIXUS_GIT_SENTINEL` + `FIXUS_GIT_EGRESS_HOST` 指向 swap-proxy + `FIXUS_GIT_CA_FILE` 信 swap-proxy 证)。注明三 scope policy 仍取最严交集。

- [ ] **Step 5.3: network-isolation.md + 三份 ZH 镜像** 同步同样语义(reference proxy 已存在;契约固定)。

- [ ] **Step 5.4: Commit**
```bash
git add docs/security.md docs/usage.md docs/network-isolation.md docs/zh/
git commit -m "docs: cr-12 G2 sentinel seam formalized + reference swap-proxy"
```

---

## Task 6: live E2E + memory

- [ ] **Step 6.1: live 三进程验证**(sandbox-server-direct 栈 + swap-proxy,非全 broker)
  - 起 reference swap-proxy(`FIXUS_SWAP_SENTINEL`/`TOKEN`/`UPSTREAM`= 本地 git-http-backend 要求 real-token)。
  - 牢侧 `FIXUS_GIT_SENTINEL` + `FIXUS_GIT_EGRESS_HOST`=swap-proxy + `FIXUS_GIT_CA_FILE`=swap-proxy CA。
  - 牢内 `git clone fixus::https://... → commit → push`,断言经 swap-proxy 兑换后上游接受。
  - 复验 §5 不变量 1(牢内 `/dev/tcp` 直连 → seccomp KillProcess,sentinel 在场不变结论)。

- [ ] **Step 6.2: 记录证据 + 更新 memory**
  - 证据入 design `veps/cr-12-networked-git-sandbox-design.md` §5(G2 段)。
  - 更新 memory `cr-12-networked-git-sandbox-g1` → 标 G2 完成(或新建 `cr-12-networked-git-sandbox-g2`):sentinel 接缝 + reference swap-proxy crate + env 配方 + gotcha。

---

## Self-Review(写完后自查)

- **Spec 覆盖:** design §5 sentinel 模型 → Task 1(注入)+ Task 2(helper 头)+ Task 3(swap-proxy);不变量 1(bypass)→ Task 6.1 复验;不变量 2(real 只在 proxy)→ Task 4.2;不变量 3(错 sentinel 401)→ Task 4.2。✓
- **占位扫描:** Task 3.5 的 rustls/rcgen 类型转换显式标"以 cargo check 修正为准"—— 这是 API 版本对齐,非设计占位;控制流齐全。Task 4.1 的 test seam 显式标"按 swap-proxy public seam 写"—— 不变量断言已硬定。无 TBD/TODO。✓
- **类型一致:** `FIXUS_GIT_SENTINEL`(牢侧 env,Task 1)/ `FIXUS_SWAP_SENTINEL`(proxy 侧 env,Task 3)二者值由 operator 配齐 —— 命名区分"牢内携带"vs"代理期望",文档(Task 5)钉契约。`FixusHttp::new(.., auth_header: Option<String>)` 三处 ctor 一致。`rewrite_authorization(header_block, sentinel, real_token)` 跨 Task 3/4 签名一致。✓
- **硬依赖:** Task 2 ← Task 1(helper 读 FIXUS_GIT_SENTINEL,profile 注入);Task 3 ← workspace deps(3.1);Task 4 ← Task 3;Task 6 ← Task 1-5。✓
- **安全:** sentinel 非密 ✓;真 token 只在 swap-proxy(Task 4.2 验)✓;绕过必败(seccomp)✓;allowlist 收口到 swap-proxy host(`FIXUS_GIT_EGRESS_HOST`)✓;`proxy.rs` 零改动(透明中继)✓。

---

## 执行顺序

Task 1 → Task 2 → Task 3(3.1 deps → 3.2/3.3 swap.rs → 3.4 config → 3.5/3.6 main)→ Task 4 → Task 5 → Task 6。Task 1/2 小而独立;Task 3 是大头(新 crate);Task 4 依赖 3。
