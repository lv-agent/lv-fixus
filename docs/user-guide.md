# fixus 用户指南

fixus 是 Agent 可靠执行运行时。本指南面向**部署/运维 fixus 全栈**的工程师:如何配置、启动、调用 HTTP API、理解 Task 生命周期、接入 ACP agent、以及使用 CR-12 网络化 git 沙箱。

安全模型单独成文:[安全设计白皮书](security-whitepaper.md)。沙箱执行细节以 [lv-sandbox 文档](https://github.com/lv-agent/lv-sandbox) 为准。

---

## 1. 架构总览

```
                ┌─────────── HTTP 网关 ───────────┐
   client ──HTTP──▶  fixus (Event Store + Orchestrator)
                          │  (logdbd 客户端;stream 名 = task_id)
                          ▼
                     ┌─ logdbd broker ─┐  ← 星型中心(append-only log)
                     └─────────────────┘
            task-begin-{type} / task-end          tool-invoke-{region} / tool-result-{region}
                  ▼                                        ▼
              ┌────────┐  ACP(stdio)              ┌────────────┐   spawn     ┌──────────────┐
              │ fixlet │◀────────▶ Claude/Hermes   │ tools-bank │────────────▶│ sandbox-server│
              └────────┘  (turn 认领 + Agent 控制) └────────────┘  (MCP 入口) └──────────────┘
                                                                          │
                                          token 流:Redis pub/sub + SSE ◀── fixus-stream
```

- **fixus**:不可变事件存储 + Turn 编排引擎 + HTTP 网关。是 logdbd 的客户端;每个 task 一条 stream(`stream 名 = task_id`)。
- **logdbd broker**:append-only log,星型中心。所有服务间通信经它(fixus↔fixlet 走 `task-begin/task-end`;tools-bank↔sandbox-server 走 `tool-invoke/tool-result`)。
- **fixlet**:broker 消费者,竞争认领 turn(`task-begin-{type}`),起 ACP agent(Claude Code / Hermes)执行,把 LLM/工具事件回写 task stream。
- **tools-bank**:agent 工具入口(MCP / JSON-RPC),把工具调用经 broker 派发到 sandbox-server。
- **sandbox-server**:broker 消费者,landlock + seccomp + cgroup 沙箱里执行工具。CR-12 起支持 allowlisted 网络化 `git` profile。
- **fixus-stream**:SSE 网关,Redis SUBSCRIBE token 流 + DB 轮询 fallback。

> **分层不变量**:server 必须经 service 层访问 storage,不直接调 `storage::*`。

---

## 2. 前置依赖

| 依赖 | 用途 | 默认 |
|------|------|------|
| **logdbd** broker | append-only log(星型中心) | `127.0.0.1:5100` |
| **Redis** | token 流 pub/sub(SSE) | 本地默认 |

构建:Rust stable,`cargo build --workspace`。

---

## 3. 配置(环境变量)

fixus 主服务:

| 变量 | 默认 | 说明 |
|------|------|------|
| `FIXUS_PORT` | `3000` | HTTP 监听端口 |
| `BROKER_ADDR` | `127.0.0.1:5100` | logdbd broker 地址 |
| `LOGDBD_NAMESPACE` | `default` | logdbd namespace |
| `FIXUS_OPERATOR_POLICY_FILE` | 空 | Operator policy TOML 路径;空 = 严默认;非法 → **fail-closed 拒启动** |
| `FIXUS_MAX_RETRY_ATTEMPTS` | `2` | CR-3 retry 预算;负值/非法 → 默认 |

其余二进制(fixlet / tools-bank / sandbox-server / fixus-stream)各有自己的 broker 订阅地址、region、Redis 等环境变量;见各二进制 `--help` / 启动脚本。沙箱侧 `git` profile 用的 `FIXUS_GIT_*` 见 [§7 CR-12](#7-cr-12-网络化-git-沙箱) 与 lv-sandbox 文档。

---

## 4. 启动全栈

```bash
# 0. 起 logdbd broker + Redis(见各自文档)

# 1. fixus(Event Store + 网关)
FIXUS_PORT=3000 BROKER_ADDR=127.0.0.1:5100 cargo run --bin fixus

# 2. fixlet(认领 turn + 起 ACP agent)
cargo run --bin fixlet   # 按 task_type 订阅 task-begin-{type}

# 3. tools-bank + sandbox-server(工具执行)
cargo run --bin tools-bank
cargo run --bin sandbox-server

# 4. fixus-stream(SSE,可选)
cargo run --bin fixus-stream
```

健康检查:`GET /health`。指标:`GET /metrics`(Prometheus,CR-4 业务指标)。

---

## 5. HTTP API

基址 `http://localhost:3000`。请求/响应 JSON(tagged serde)。**API 鉴权为 P3,当前不强制**(见白皮书)。

### Session / Task

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/sessions` | 创建 task(body: `agent_type`(必填)、`task_type`(缺省回退 `agent_type`)、`session_id`(可选,自生成)、`body`、`metadata`、`priority`、`policy`) |
| `GET`  | `/api/v1/sessions/{task_id}` | 查询 session |
| `POST` | `/api/v1/sessions/{task_id}/ready` | 标记 ready(`created → ready`) |
| `POST` | `/api/v1/sessions/{task_id}/end` | 结束 session |
| `GET`  | `/api/v1/sessions/{task_id}/state` | Task 当前状态(事件投影) |

### Turn

| 方法 | 路径 |
|------|------|
| `POST` | `/api/v1/sessions/{task_id}/turns` |
| `GET`  | `/api/v1/sessions/{task_id}/turns/{turn_id}` |
| `POST` | `/api/v1/sessions/{task_id}/turns/{turn_id}/complete` |
| `POST` | `/api/v1/sessions/{task_id}/turns/{turn_id}/fail` |
| `POST` | `/api/v1/sessions/{task_id}/turns/{turn_id}/cancel` |

### 事件 / 上下文 / 恢复

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/sessions/{task_id}/events` | 记录单个事件 |
| `POST` | `/api/v1/sessions/{task_id}/events/batch` | 批量记录(回传 paired step 事件) |
| `GET`  | `/api/v1/sessions/{task_id}/context` | 构建 LLM 上下文(Events → Messages) |
| `GET`  | `/api/v1/sessions/{task_id}/turns/{turn_id}/context` | 单 turn 上下文 |
| `GET`  | `/api/v1/sessions/{task_id}/recovery` | 查崩溃恢复计划 |
| `POST` | `/api/v1/sessions/{task_id}/recovery/apply` | 应用恢复 |
| `GET`  | `/api/v1/sessions/{task_id}/summary` | 摘要 |
| `GET`  | `/api/v1/sessions/{task_id}/token-usage` | token 用量 |

### 租户策略

| 方法 | 路径 | 说明 |
|------|------|------|
| `PUT` | `/api/v1/tenants/{tenant_id}/policy` | 设置 tenant policy(校验 `tenant ⊆ operator`,越权 → 400) |

### 示例:创建并推进一个 task

```bash
# 创建 → 返回 session_id(= task_id)= logdbd stream 名
curl -s localhost:3000/api/v1/sessions -H 'content-type: application/json' \
  -d '{"agent_type":"claude","task_type":"code-dev","priority":5}'
# → {"session_id":"<task_id>","seq":1}

# 标记 ready → fixlet 订阅 task-begin-code-dev 会竞争认领
curl -s -X POST localhost:3000/api/v1/sessions/<task_id>/ready

# 查状态
curl -s localhost:3000/api/v1/sessions/<task_id>/state
```

---

## 6. Task 生命周期(8 态状态机)

```
created → ready → claimed → executing → succeeded
                     │            │
                     ▼            ▼
                   blocked ⇄ ready        failed
                                          canceled
```

- 状态 = 事件投影(7 个 Task 级事件驱动迁移)。
- **pull-based 认领**:fixlet 竞争消费 `task-begin-{task_type}`;stable group `fixlets-{task_type}`;`preferred_claimant` 优先。
- 终态(`succeeded`/`failed`/`canceled`)事件唯一:同一 `task_id`/`turn_id`/`step_id` 至多一个 terminal 事件(storage 校验)。
- 崩溃恢复:`redo_group` + 幂等键;**非幂等写工具阻塞 turn**(不盲目重放,见白皮书)。

spec 细节见 `docs/superpowers/specs/2026-07-07-task-model-design.md`。

---

## 7. CR-12 网络化 git 沙箱

默认沙箱**零出站**(seccomp `AF_UNIX`-only)。CR-12 给一个 **`git` profile**:开 allowlisted 出口,让 agent 自己 `git clone`/`commit`/`push`,无需 fixus 侧 harvest 编排。

用法(沙箱侧配置;详见 [lv-sandbox 文档](https://github.com/lv-agent/lv-sandbox)):

- 选 `profile: git`。
- 出口 host/port:env `FIXUS_GIT_EGRESS_HOST`(默认 `github.com`)、`FIXUS_GIT_EGRESS_PORT`(默认 `443`)。
- 自签/内网 CA:env `FIXUS_GIT_CA_FILE`(PEM 文件路径)→ 注入牢笼 `SANDBOX_CA_PEM`,helper dialer 据此信任。
- clone 用 `fixus::https://<host>/<path>` remote scheme(走 `git-remote-fixus` helper → SOCKS5h UDS 代理)。

**凭据是占位假凭据(sentinel)**:真凭据 + fake→real 兑换在出口代理(牢笼外,使用方实现)。安全不变量见 [白皮书 §CR-12](security-whitepaper.md#cr-12-凭据模型sentinel)。

---

## 8. ACP agent 集成

fixlet 是 ACP 协议桥:认领 turn 后起 ACP agent(stdio),对接 Claude Code / Hermes。backend 可插拔(CR-9)。

- agent 工具**必须**经 session/new 的 `mcpServers` 注入(走 tools-bank),且需 `bypassPermissions` 否则卡死。
- LLM 上下文由 fixus 重建(Events → Messages),经 ACP 注入 agent。
- LLM/tool 事件以 paired step 事件回写 task stream(step-events;header value 必须 string)。

---

## 9. 可观测性

- `GET /metrics` — Prometheus 业务指标(CR-4:task/turn 状态、retry、token 用量等)。
- `GET /health` — 存活。
- tracing:`RUST_LOG` / `fixus=info,tower_http=info` 默认。
- token 流:SSE(fixus-stream),Redis pub/sub 主路径 + DB 轮询 fallback。
