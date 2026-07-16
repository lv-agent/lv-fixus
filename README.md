# fixus — Agent Session Event Store

**FIXUS** — 拉丁语「固定、不可移」。Agent 可靠执行运行时:**不可变事件存储 + Turn 级崩溃恢复 + 多 Agent(ACP)+ 沙箱工具执行**。

fixus 把「agent 的一次执行」建模成一条不可变的事件流(stream 名 = `task_id`),状态是事件的投影,崩溃后从事件重放恢复。服务间全走 broker(logdbd 星型拓扑),工具执行落在独立的网络化沙箱。

> 完整操作说明见 [docs/user-guide.md](docs/user-guide.md);安全模型见 [docs/security-whitepaper.md](docs/security-whitepaper.md)。

## 能力速览

- **不可变事件存储** — append-only log(logdbd,gRPC);事务内 seq 取号、无 gap;终态事件唯一。
- **Task 一等实体 + 8 态状态机** — `created→ready→claimed→executing→blocked⇄ready→succeeded/failed/canceled`(状态 = 事件投影)。
- **pull-based turn 认领** — fixlet 经 broker 竞争消费 `task-begin-{type}` 认领 turn(stable group `fixlets-{type}`,preferred_claimant 优先)。
- **Turn 六状态机 + Step 生命周期** — 22 种 EventType(Task 7 + Session/Turn/Step 15)。
- **Turn 级崩溃恢复** — `redo_group` + 幂等键 + LLM 缓存注入;非幂等写阻塞而非重放。
- **LLM 上下文重建** — Events → Messages(摘要 + 增量 + 全量回放)。
- **ACP 多 Agent 桥** — fixlet 对接 Claude Code / Hermes(可插拔 backend)。
- **沙箱工具执行** — 独立 sandbox-server(landlock + seccomp + cgroup),经 broker `tool-invoke/tool-result` 派发;CR-12 起 allowlisted 网络化 `git` profile。
- **流式 token 转发** — Redis pub/sub + SSE,DB 轮询 fallback。
- **多租户字段** — `tenant_id` / `user_id`(API 鉴权为 P3,见白皮书)。

## 二进制(5 个,松耦合)

| 二进制 | 职责 |
|--------|------|
| `fixus` | 主服务 — Event Store + Orchestrator + HTTP 网关(`fixus` 直接起 HTTP 服务) |
| `fixlet` | ACP 协议桥(broker 消费者)— 认领 turn + Agent 控制 |
| `sandbox-server` | 工具执行沙箱(broker 消费者)— Landlock + seccomp + ulimit/cgroup |
| `tools-bank` | 独立 MCP 服务 — agent 工具入口,broker 化派发到 sandbox-server |
| `fixus-stream` | SSE 流式网关 — Redis SUBSCRIBE + DB fallback |

fixlet / sandbox-server / tools-bank / fixus-stream 不依赖 lib crate,未来可拆分独立 repo。

## 技术栈

Rust · Tokio · Axum · logdbd(append-only log,gRPC)· Redis(pub/sub + SSE)· MCP(JSON-RPC)· ACP(stdio)。

## 快速开始

```bash
# 1. 起依赖:logdbd broker(默认 127.0.0.1:5100)+ Redis
# 2. 起 fixus
FIXUS_PORT=3000 BROKER_ADDR=127.0.0.1:5100 cargo run --bin fixus
# 3. 创建一个 task(返回 session_id = task_id,stream 名)
curl -s localhost:3000/api/v1/sessions -H 'content-type: application/json' \
  -d '{"agent_type":"claude","task_type":"code-dev"}'
```

详细启动、HTTP API、Task 生命周期、ACP 集成、CR-12 git 沙箱用法见 [用户指南](docs/user-guide.md)。

## 相关仓库

- **[lv-sandbox](https://github.com/lv-agent/lv-sandbox)** — 沙箱执行器(seccomp/landlock/egress/git profile),工具执行的实际落点。沙箱细节以 lv-sandbox 文档为准。
- **logdbd** — append-only log 数据库(broker 星型中心)。

## 许可证

见仓库根 `Cargo.toml`。
