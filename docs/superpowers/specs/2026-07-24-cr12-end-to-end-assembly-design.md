# CR-12 端到端组装与闭环 设计

> **状态**:设计已与用户确认(2026-07-24)。下一步:writing-plans 出实现计划。
> **关联**:`veps/cr-12-networked-git-sandbox-design.md` §5(G2 凭据接缝)、`docs/superpowers/plans/2026-07-21-cr12-networked-git-sandbox-g2.md`(G2 已实施)、memory `cr-12-networked-git-sandbox-g1/g2`、`dev-stack-startup`(全栈起法 + 5 坑)。

## 1. 背景与动机

CR-12「网络化 Git 沙箱」的机制(G1:SOCKS5h-over-UDS + seccomp net-off + git-remote-fixus helper;G2:sentinel 凭据接缝 + reference swap-proxy)已全部在 **lv-sandbox** 实现并通过三套对抗式测试。但所有测试都是**零件级**(helper→SOCKS→swap-proxy→上游),没有任何测试驱动过**完整链路**:

```
fixus policy 声明 → tools-bank → broker → sandbox-broker 选 git profile
  → sandbox-server 按 git profile 执行 → helper → swap-proxy → 上游 clone
```

Explore 测绘确认的真空:

1. **无人组装 running stack** —— swap-proxy + sandbox-server(带 `FIXUS_GIT_*` env)+ sandbox-broker 从没作为一组服务被拉起并互通。两仓零处 spawn swap-proxy。
2. **sentinel 无铸造/同步** —— sentinel 必须同值出现在 sandbox-server env(`FIXUS_GIT_SENTINEL`)与 swap-proxy env(`FIXUS_SWAP_SENTINEL`);今天只能 operator 手搓。
3. **无端到端证据** —— fixus 声明任务 → 全栈 → agent 真 clone 一个仓,从未跑通。

**架构前提(不可违背)**:fixus 按设计隔离于 sandbox 内部 —— 它对网络化 git 的唯一贡献是声明能力天花板(`effective_policy.net.egress` 非空 + `agent_role=Operator`),桥(sandbox-broker)据此选 `git` profile。fixus **不** spawn swap-proxy、**不** 注入 sandbox-server host env(那些在 sandbox-server 主机进程,fixus 碰不到)。swap-proxy 按设计文档(§5)是 **operator 拥有**的 out-of-process 服务,G2 只交付树内 reference 实现。因此本设计的集成真空落在 **lv-sandbox / operator 侧**,不在 fixus 侧。

## 2. 目标 / 非目标

**目标**:
- G1:一个确定可重复的**全栈 E2E 测试**,证明净 git 链路(policy→profile→执行→clone)端到端跑通,含 sentinel→real 兑换证据 + 安全不变量。
- G2:把组装逻辑(铸 sentinel + 自签入站证 + 印双侧 env + 起 swap-proxy)**内置进测试**(不交付 shipped launcher binary,忠实 G2「reference impl,operator 自实现 prod」哲学)。
- G3:补 **fixus 侧契约钉子** —— resolve_effective 序列化的 `EffectivePolicy` JSON ↔ 桥 `net_profile_override` 解析的握手,今天两端各自单测、未联合验证。
- G4:**operator 文档** —— env 契约表 + 可读 recipe + systemd/compose 样例 + 5 个坑。
- G5:清 fixus 侧死码 `EventStore::dispatch_tool`(零非测试调用方)。

**非目标(YAGNI,显式留作后续)**:
- per-task 定向(桥铸 per-task sentinel / 按 `net.egress[].host` 定向 per-task swap-proxy)—— 架构深化。
- 真 LLM turn 自动化(非确定,只文档化手动 recipe,不进自动测试)。
- git 协议 v2 / push streaming(G1.5)。
- shipped launcher binary(已选测试内置)。

## 3. 架构

### 3.1 要证明的链路

```
fixus policy(三 scope net.egress=[localhost] + agent_role=Operator)
  → resolve_effective → EffectivePolicy JSON
  → tools-bank POST :3001/mcp 带 X-Fixus-Policy header
  → broker stream tool-invoke-{region}(metadata.effective_policy)
  → sandbox-broker net_profile_override(net.egress 非空 → profile="git")
  → sandbox-server 按 git profile 跑 job
      (起 per-job SOCKS5h-over-UDS + 注 FIXUS_GIT_SENTINEL env + helper on PATH)
  → bash 跑 `git clone fixus::https://localhost:<swap-port>/<path>`
  → git-remote-fixus helper → SOCKS5h → swap-proxy(sentinel→real 改写 + Host 重写)
  → 本地 TLS git-http-backend 上游(记录 Authorization)→ clone 成功
```

### 3.2 全栈确定性测试 = 7 进程

绕过 fixus serve / fixlet / redis / LLM(tools-bank 直接产 tool-invoke,确定可重复)。fixus 的 resolve_effective 由独立快单测覆盖(G3);真 LLM turn 走文档化手动 recipe(G4)。

| # | 进程 | 来源仓 | 测试中如何起 |
|---|---|---|---|
| 1 | logdbd | lv-logdb(`~/logdb/lv-logdb`) | env 指针 `FIXUS_E2E_LOGDBD_BIN`,带 tmp `logdbd.yaml`(`shards:4`) |
| 2 | logdb-broker | lv-logdb | env 指针 `FIXUS_E2E_BROKER_BIN`,带 tmp `broker.yaml`(`session_timeout_ms:10000` —— 坑1) |
| 3 | tools-bank | **lv-fixus** | env 指针 `FIXUS_E2E_TOOLS_BANK_BIN`,`--broker-addr :5100 --region default --port 3001` |
| 4 | sandbox-broker | lv-sandbox | sibling bin(见 §3.3),`env -u *_PROXY NO_PROXY=*`(坑5) |
| 5 | sandbox-server | lv-sandbox | sibling bin,tmp `server.yaml`(base_dir 可写 / fail_closed:false / nproc 8192 —— 坑:配置三件套)+ `FIXUS_GIT_*` env + helper on PATH |
| 6 | swap-proxy | lv-sandbox(egress-swap-proxy) | **in-process** `server::serve`(复用 G2 测试夹具) |
| 7 | 本地 TLS git-http-backend 上游 | — | **in-process** 复用 `tests/common/mod.rs::spawn_cgi_tls_server` |

env 指针(1/2/3)缺任一 → `#[ignore]` 早返回(套 lv-sandbox 既有 `SANDBOX_BRIDGE_TEST_URL` gate 先例),不算失败、不进默认 CI。

### 3.3 sibling bin 定位(lv-sandbox 内 4 个 bin)

lv-sandbox workspace 的 sandbox-server / sandbox-broker / fixus-egress-swap-proxy / git-remote-fixus 同属一个 `target/debug`。测试用:
```rust
let target_debug = Path::new(env!("CARGO_BIN_EXE_git-remote-fixus")).parent().unwrap();
let sandbox_server = target_debug.join("sandbox-server");
let sandbox_broker = target_debug.join("sandbox-broker");
```
robust,不硬编码路径。**前置**(文档化):`cargo build --workspace`(测试假设 sibling bin 已构建;`cargo test -p git-remote-fixus` 不构建 sibling 包)。

## 4. 组件

### 4.1 组件 A:全栈 E2E `#[ignore]` 测试

- **文件**:`lv-sandbox/crates/git-remote-fixus/tests/cr12_e2e_full_stack.rs`。
- **理由**:与 G2 测试同 crate → **直接共享 `tests/common/mod.rs`**(rcgen 自签 localhost 证 `gen_cert()` / TLS git-http-backend CGI 上游 `spawn_cgi_tls_server` / SOCKS5h `spawn_proxy` / swap-proxy `server::serve` / `path_with_helper()`)。最大化复用,零夹具迁移。
- **组装逻辑(内置)**:
  1. `gen_cert()` → swap-proxy 入站证(localhost SAN)。
  2. sentinel = 固定 `jail-sentinel-E2E`(测试用固定值,确定可重复,免 `rand` 依赖;sentinel 本就是公开占位值,无需保密)。
  3. real_token = 固定 `real-token-E2E`(dummy;上游 CGI 记录它以取证)。
  4. 起 in-process CGI 上游(记录 Authorization 到 `Arc<Mutex<Option<String>>>`)→ 拿其 `:port` + 证。
  5. 起 in-process swap-proxy `server::serve`,`SwapConfig{ sentinel, real_token, upstream=<CGI host:port>, cert_pem/key_pem=入站证, upstream_ca_pem=CGI 证 }` → 拿 swap-proxy `:port`。
  6. 写 tmp config:`logdbd.yaml`(shards:4)、`broker.yaml`(session_timeout_ms:10000)、`sandbox-server.yaml`(base_dir=tmp 可写、fail_closed:false、profiles.shell.rlimit.nproc:8192)。
  7. spawn 4/5(sandbox-server,sibling bin)env:`SANDBOX_CONFIG` + `FIXUS_GIT_SENTINEL=<sentinel>` + `FIXUS_GIT_EGRESS_HOST=localhost` + `FIXUS_GIT_EGRESS_PORT=<swap-port>` + `FIXUS_GIT_CA_FILE=<tmp 入站证 pem>` + `PATH=<helper dir>:$PATH`(helper dir = `target_debug`)。
  8. spawn sandbox-broker(sibling bin,`env -u HTTP_PROXY HTTPS_PROXY ... NO_PROXY=*`)`--broker-addr 127.0.0.1:5100 --sandbox-url 127.0.0.1:8080 --region default --group sandboxes`。
  9. spawn tools-bank(env 指针)`--broker-addr 127.0.0.1:5100 --region default --port 3001`。
  10. spawn logdbd + broker(env 指针,各自 tmp config)。
  11. readiness 轮询:broker :5100 / sandbox-server :8080 / tools-bank :3001。
- **驱动**:reqwest `POST http://127.0.0.1:3001/mcp`:
  - header `X-Fixus-Session-Id: <task-id>`
  - header `X-Fixus-Policy: <JSON>` —— JSON 形状须匹配 fixus `EffectivePolicy` serde 输出(plan 钉死精确字段:`net.egress=[{host:"localhost", ports:[<swap-port>], category:null}]` + `agent_role:"Operator"`)
  - body:`fixus_bash` 工具,`input.command = "git clone --depth 1 fixus::https://localhost:<swap-port>/<bare-repo-path> <dest>"`
- **断言**:
  1. tool 结果 success(clone exit 0)。
  2. `<dest>/.git` 存在。
  3. **关键证据**:上游 CGI 抓到 `Authorization: Bearer <real_token>`(精确大小写,非 sentinel)。
  4. **安全不变量**:负向 clone —— 再发一个 `git clone fixus::https://<非 allowlist host>/...`;SOCKS allowlist 只放 localhost → 非 localhost host 在 SOCKS 层被拒(连接失败)。证「绕 swap-proxy 直连外部必败」的等价面。
- **清理**:所有 subprocess 包成 RAII(`Drop` → `kill`),scopeguard 兜底 panic 也清;drive 带超时。

### 4.2 组件 B:fixus 侧契约测试 + 死码清理

- **B1 metadata 握手单测** —— 落 **lv-sandbox**(挨着消费者 `net_profile_override`):
  - 构造**匹配 fixus `EffectivePolicy` serde 输出**的 JSON → 喂 `net_profile_override` → 断言 `Some("git")`。
  - 反例:`net.egress=[]` 或缺 → `None`;`agent_role:Reader` → (取决于桥实现)断言符合 fail-closed。
  - 这是「生产者序列化 ↔ 消费者解析」契约钉子:fixus 若改 serde 形状 → 此测试断 → 正是目的。
  - **plan 须先核**:fixus `EffectivePolicy` 的精确 serde 字段名(camelCase?snake?`net` vs `network`?)+ 桥 `net_profile_override` 的精确解析路径(lv-sandbox sandbox-broker `main.rs`)。
- **B2 死码清理** —— 落 **lv-fixus**:删 `EventStore::dispatch_tool`(`src/broker_store.rs:481`)+ trait default(`src/storage.rs:146`)+ `#[cfg(test)]` 驱动(`src/broker_store.rs:762`)。Explore 已确认零非测试调用方。机械;删后跑 `cargo test -p fixus`(或相应包)确认无回归。

### 4.3 组件 C:operator 文档

- **文件**:`lv-sandbox/docs/cr-12-net-git-operator-guide.md`。
- **内容**:
  1. **env 契约表** —— sandbox-server host 的 `FIXUS_GIT_*`(`SENTINEL`/`EGRESS_HOST`/`EGRESS_PORT`/`CA_FILE`)↔ swap-proxy 的 `FIXUS_SWAP_*`(`SENTINEL` 同值/`TOKEN` 真 token/`UPSTREAM`/`CERT_PEM`/`KEY_PEM`/`UPSTREAM_CA_PEM`)。**强调:同 sentinel 必须双填**。
  2. **7 进程组装 recipe** —— 从 §4.1 测试 setup 派生的人可读步骤。
  3. **扩到真 LLM turn** —— 加 fixus serve(:3000,+ redis 6379 + `REDIS_URL`/`SANDBOX_REGION`)+ fixlet(+ 去 proxy + MiniMax `CLAUDE_CONFIG_DIR` + bypassPermissions)+ LLM 的接法(引 `dev-stack-startup` memory)。
  4. **systemd/compose 样例** —— swap-proxy + sandbox-server 两 unit,env-file 注入。
  5. **5 个坑**逐条:broker `session_timeout_ms>0` / 去所有 `*_PROXY` / stream 名禁点号 / sandbox-server 配置三件套(base_dir+fail_closed:false+nproc 8192)/ sandbox-broker 也要去 proxy。

## 5. 数据 / env 流(sentinel 同步是核心不变量)

```
测试/operator 铸造: sentinel S, real_token R, 入站证 C
  ├─ sandbox-server 进程 env:
  │    FIXUS_GIT_SENTINEL=S        ──┐
  │    FIXUS_GIT_EGRESS_HOST=localhost │ git profile 读这些(profile.rs:163)
  │    FIXUS_GIT_EGRESS_PORT=<swap>   │ → 注牢 env:FIXUS_GIT_SENTINEL=S, SANDBOX_CA_PEM=C
  │    FIXUS_GIT_CA_FILE=C.pem      ──┘
  └─ swap-proxy 进程 env(或 in-process SwapConfig):
       FIXUS_SWAP_SENTINEL=S       ← 必须同 S
       FIXUS_SWAP_TOKEN=R          (只在本进程)
       FIXUS_SWAP_CERT_PEM/KEY_PEM=C
       FIXUS_SWAP_UPSTREAM=<真上游>
```
helper 只见 `FIXUS_GIT_SENTINEL=S`;R 永不进牢。swap-proxy 收 `Bearer S` → 改写为 `Bearer R` 转发。

## 6. 错误处理 / 健壮性

- env 指针 gate:缺 → 测试 `eprintln` 指引 + 早返回(非失败)。
- readiness:端口轮询(≤ ~5s),起不来 → 带各进程 stderr 的清晰 panic。
- 清理:RAII + scopeguard(panic 也清子进程)。
- drive 超时(reqwest timeout)。
- 回归:既有 G2 三套测试(swap 单元 / swap E2E / 真 git 集成)+ bridge live 测试不变。

## 7. 测试策略汇总

| 测试 | 位置 | 类型 | 覆盖 |
|---|---|---|---|
| 全栈 E2E | lv-sandbox `git-remote-fixus/tests/cr12_e2e_full_stack.rs` | `#[ignore]`(需外部 bin) | policy→profile→执行→clone 全链 + sentinel 兑换 + 安全不变量 |
| metadata 握手 | lv-sandbox(挨着 `net_profile_override`) | 普通 `#[test]` | fixus 序列化 ↔ 桥解析 契约 |
| 死码清理后回归 | lv-fixus | `cargo test` | 删 dispatch_tool 无回归 |

## 8. 落点与分支

- **lv-fixus**:分支 `cr12-e2e-assembly`(off master)—— 本 spec + B1 的 fixus 序列化形状核对 + B2 死码清理。
- **lv-sandbox**:分支 `cr12-e2e-assembly`(off main)—— 组件 A E2E 测试 + B1 握手测试(挨消费者)+ 组件 C 文档。

## 9. 风险 / 待 plan 钉死

- **fixus `EffectivePolicy` serde 精确形状**(B1 / §4.1 驱动 header)—— plan 须读 fixus `policy.rs` + `adapter.rs` 核对字段名/大小写,否则握手测试与驱动 header 会写错。
- **sibling bin 前置 build** —— `cargo build --workspace` 是 `#[ignore]` 测试的文档化前置;若 CI 要跑需额外编排(本设计不含 CI 编排)。
- **logdbd / broker 的 tmp config 精确字段** —— plan 须核对 lv-logdb 的 yaml schema(引 `dev-stack-startup` memory 的 `/tmp/logdbd-dev.yaml` shards:4 / `broker-dev.yaml` session_timeout_ms:10000)。
- **tools-bank `fixus_bash` 工具的 MCP body 精确形状** —— plan 须核对 tools-bank `handle_mcp` 的 JSON-RPC 字段。
- **负向 clone 断言(§4.1 断言④)** —— SOCKS allowlist 拒非 localhost 的确切失败形态(连接 reset / timeout)须在实现时确认,断言写"非 success"即可,不锁死错误码。
