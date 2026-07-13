# 外部 Action Adapter 使用指南

> 让 agent 调用**外部世界**的工具(GitHub / Slack / 任意 HTTP 服务)。
> 对应实现:CR-11(`src/bin/tools-bank/adapter.rs`)。

## 1. 这是什么

fixus 自带 6 个**本地沙箱工具**(`fixus_bash/read/write/edit/glob/grep`)—— agent 用它们操作本地工作区。但 agent 调不了外部服务:不能去 GitHub 建 issue、不能发 Slack、不能查 Jira。

**外部 Action Adapter** 解决这个问题:你写一个(或部署一个)HTTP webhook 桥接到目标服务,在 tools-bank 注册一行配置,agent 就能像调 `fixus_bash` 一样调你的外部工具。**tools-bank 不需要改一行代码**。

本文以一个**真实可跑的 GitHub issue 工具**为例(代码见 [`examples/external-adapters/github_webhook.py`](../../examples/external-adapters/github_webhook.py)),讲清楚从启动到 agent 调用的全链路。

## 2. 工作原理

```
  agent (Claude Code / Hermes)
        │  ACP tools/call "gh_create_issue"
        ▼
  fixlet ──► tools-bank (MCP server)
                  │  按 tool 名路由:
                  │   fixus_bash ─► broker ─► sandbox-server  (本地)
                  │   gh_*        ─► HttpActionAdapter
                  ▼                          │
            HttpActionAdapter                │ POST http://127.0.0.1:9000/gh_create_issue
                  │                          ▼
                  │                   你的 webhook (github_webhook.py)
                  │                          │ Bearer $GITHUB_TOKEN
                  │                          ▼
                  │                   GitHub REST API
                  │                          │
                  │                          ▼ 响应
                  ◄──────────────────────────┘
                  │
                  ▼  把 GitHub 响应包成 MCP 工具结果
            agent 看到 issue 创建结果
```

**两层认证,职责分明**:

| 路径 | 认证 | 谁配 |
|------|------|------|
| tools-bank → webhook | 可选 `auth=`(webhook 自身入口鉴权) | tools-bank env `TOOLS_BANK_HTTP_ADAPTERS` |
| webhook → GitHub | `Bearer $GITHUB_TOKEN`(**只留 webhook 侧**) | webhook 进程 env |

> ⚠️ **安全关键**:目标服务的 token(如 GitHub PAT)**永远只在 webhook 进程内持有**,agent 和 tools-bank 都不经手。agent 只传业务参数(repo / title),不传凭证。

## 3. 完整样例:GitHub issue 工具(逐步跑通)

### 3.1 准备 GitHub token

在 https://github.com/settings/tokens 生成一个 PAT(fine-grained 推荐),给 `Issues: Read and Write` 权限(限定到你想操作的仓库)。

```bash
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
```

### 3.2 启动 webhook

```bash
cd /home/lvtao/lv/lv-fixus
GITHUB_TOKEN=$GITHUB_TOKEN python3 examples/external-adapters/github_webhook.py
# github-webhook on 127.0.0.1:9000  api=https://api.github.com  tools=['gh_create_issue', ...]
```

验证 webhook 自身工作(不经过 tools-bank):

```bash
curl -s http://127.0.0.1:9000/health
# {"service": "github-webhook", "tools": [...], "token_configured": true}
```

直接测一个工具(模拟 tools-bank 会发的 body):

```bash
curl -s http://127.0.0.1:9000/gh_list_issues \
  -H 'Content-Type: application/json' \
  -d '{"tool":"gh_list_issues","arguments":{"repo":"octocat/Hello-World","limit":3},
       "task_id":"manual-test","idempotency_key":"x"}' | python3 -m json.tool
# 返回 octocat/Hello-World 仓库最近的 3 个 issue
```

### 3.3 注册到 tools-bank 并启动

```bash
# 先起 broker(memory dev-stack-startup 有全栈启动方法)
# 然后:
TOOLS_BANK_HTTP_ADAPTERS="github|http://127.0.0.1:9000|gh_create_issue,gh_list_issues,gh_get_issue,gh_add_comment|timeout=20" \
  ./target/release/tools-bank
```

启动日志确认注册:

```
registered http adapter `github`: 4 tool(s), timeout=20s
registry: 2 adapter(s), 10 tool(s)    # 6 sandbox + 4 github
```

### 3.4 验证 MCP 端点(不依赖 agent)

```bash
# 1. tools/list —— agent 会看到全部 10 个工具,含 4 个 gh_*
curl -s localhost:3001/mcp \
  -H 'Content-Type: application/json' -H 'X-Fixus-Session-Id: t1' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \
  | python3 -c "import sys,json;print([t['name'] for t in json.load(sys.stdin)['result']['tools']])"
# ['fixus_bash', 'fixus_read', 'fixus_write', 'fixus_edit', 'fixus_glob', 'fixus_grep',
#  'gh_create_issue', 'gh_list_issues', 'gh_get_issue', 'gh_add_comment']

# 2. 通过 tools-bank 真实创建一个 issue
curl -s localhost:3001/mcp \
  -H 'Content-Type: application/json' -H 'X-Fixus-Session-Id: t1' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call",
       "params":{"name":"gh_create_issue",
                 "arguments":{"repo":"YOUR/REPO","title":"from fixus agent","body":"created via external action adapter"}}}' \
  | python3 -m json.tool
# result.content[0].text 里是 GitHub 返回的 issue JSON(含 number、url)
```

### 3.5 agent 怎么用上

tools-bank 本来就是 agent 的 MCP 工具源(fixlet 在 ACP `session/new` 的 `mcpServers` 里声明它 —— 这部分**已经在用**,6 个 sandbox 工具就这么暴露的)。**新加的 `gh_*` 工具自动一起出现**,agent 无需任何额外配置。

agent 侧的体验:它看到一个叫 `gh_create_issue` 的工具,schema 是 `{"type":"object"}`(见 §6 说明),会按描述尝试调用。为了让 agent 更准地用,建议在 prompt 或工具描述里告诉它参数格式(见 §6 改进)。

## 4. 配置参考

### 4.1 `TOOLS_BANK_HTTP_ADAPTERS` 格式

```
名称|base_url|工具1,工具2,...[|auth=头值][|timeout=秒][;名称2|...]
```

| 字段 | 必填 | 说明 |
|------|------|------|
| 名称 | ✓ | adapter 诊断名(日志、撞名报错用),任意 |
| base_url | ✓ | webhook 根地址,工具调用会 POST 到 `{base_url}/{工具名}` |
| 工具列表 | ✓ | 逗号分隔的工具名;这些名字会暴露给 agent,**必须全库唯一**(撞名见 §7) |
| `auth=` | | 整个 `Authorization` 头值(如 `Bearer xxx`、`ApiKey xxx`)。webhook 自身鉴权用 |
| `timeout=` | | HTTP 调用超时秒数,默认 15 |

**多条**用 `;` 分隔。例:

```bash
TOOLS_BANK_HTTP_ADAPTERS="github|http://127.0.0.1:9000|gh_create_issue,gh_list_issues,gh_get_issue,gh_add_comment|timeout=20;slack|http://127.0.0.1:9001|post_message,lookup_user|auth=Bearer x|timeout=10"
```

非法条目会被跳过(不影响其它条目),日志有 warn。

### 4.2 CLI 参数(tools-bank)

```bash
tools-bank --broker-addr 127.0.0.1:5100 --namespace default --region default --port 3001
```
(均为默认值,通常可省)

## 5. 编写你自己的 adapter(契约)

webhook 是一个普通 HTTP 服务,只需遵守下列契约。

### 5.1 请求(tools-bank → 你的 webhook)

```
POST {base_url}/{tool_name}
Content-Type: application/json
[Authorization: <auth= 的值>]      # 仅当配置了 auth=

{
  "tool": "<工具名>",
  "arguments": <agent 传的任意 JSON>,
  "task_id": "<fixus task id>",
  "idempotency_key": "<tools-bank 算的幂等键>"
}
```

- `arguments` 是 agent 调工具时传的参数,你自由定义结构(你的 webhook 怎么解析,agent 就该怎么传)。
- `idempotency_key` 形如 `task:bank:tool:hash`,含 task_id + 工具名 + 参数哈希。**同一个 turn 里相同参数重试会得到相同 key** —— 你的 webhook 可以据此去重(对非幂等副作用尤其重要,见 §6)。

### 5.2 响应(你的 webhook → tools-bank)

tools-bank 用 **HTTP 状态码**区分成败,用 **body** 作工具输出:

| 你返回 | tools-bank 判定 | agent 看到 |
|--------|----------------|------------|
| 2xx + 任意 body | `success` | body 作为工具输出(`isError:false`) |
| 非 2xx + body | 工具失败 | `"http <code>: <body>"`(`isError:true`) |
| 连接拒绝 / 超时 | infra 故障 | MCP 错误 `-32603`(agent 知道工具不可达) |

> **最佳实践**:把目标服务的成败**透传**成你的状态码(如 GitHub 返 422,你也返 422 + GitHub 错误 body)。这样 agent 既看到是 HTTP 失败,又看到目标服务的错误详情。`github_webhook.py` 就是这么做的。

### 5.3 最小 webhook 骨架

```python
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        tool = self.path.strip("/")
        env = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)) or b"{}"))
        args = env.get("arguments", {})
        try:
            # 你的业务逻辑:根据 tool 名操作外部服务
            result = do_your_thing(tool, args)
            self._reply(200, {"ok": True, "data": result})
        except YourBusinessError as e:
            self._reply(400, {"ok": False, "error": str(e)})  # → agent isError
        except Exception as e:
            self._reply(500, {"error": f"crashed: {e}"})

    def _reply(self, status, obj):
        data = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

ThreadingHTTPServer(("127.0.0.1", 9000), H).serve_forever()
```

## 6. 最佳实践

### 6.1 token 只留 webhook 侧

永远不要让 agent 或 tools-bank 经手目标服务的凭证。token 从 webhook 进程 env 读(见 `github_webhook.py` 顶部)。多租户场景下,按 `task_id` 在 webhook 内路由到不同凭证(留作扩展)。

### 6.2 给 agent 更好的工具描述

当前 `HttpActionAdapter` 给每个外部工具声明的是最小 schema `{"type":"object"}`(真实参数结构只有你的 webhook 知道)。agent 看不到字段名,可能盲传。

两个改善方向(任选):

- **轻量(推荐起步)**:在系统 prompt 里告诉 agent 这几个工具的参数。例:"`gh_create_issue` 接受 `{repo, title, body?, labels?}`,repo 是 `owner/repo`"。
- **进阶**:扩展 `HttpActionAdapter` 支持从 webhook `GET {base_url}/.tools` 拉取每个工具的真实 JSON Schema(CR-11 N2,留作后续;见 plan §6.4)。

### 6.3 幂等性

tools-bank 会在 turn 级崩溃恢复时**重放**同一 `idempotency_key`。对非幂等副作用(建 issue、发消息):

- 理想:webhook 按 `idempotency_key` 去重(存最近 N 个 key,命中则返上次结果)。
- 最低限度:webhook 容忍重复调用(如 GitHub 对完全相同的 issue 内容会创建多条 —— 这是当前 `github_webhook.py` 的行为,生产环境应加去重)。

### 6.4 超时

`timeout=` 要**大于**目标服务的最慢正常响应(GitHub API 偶尔慢,建议 ≥ 20s)。超时会被 tools-bank 当 infra 故障(-32603),agent 会看到"工具不可达"而非"工具失败",语义不同。

### 6.5 可观测

webhook 应打印每次调用的 `tool` / `task_id` / 关键参数(见 `github_webhook.py` 的 stderr 日志)。出问题时,task_id 是贯穿 fixus 事件流的追踪键。

## 7. 故障排查

| 现象 | 原因 / 处理 |
|------|------------|
| 启动日志没有 "registered http adapter" | env 格式错(字段 < 3、工具列表空、base_url 空)—— 见 §4.1,该条被跳过 |
| `tools/list` 看不到外部工具 | 同上;或工具名撞了 builtin(日志有 "skip http adapter ... already owned by") |
| 调用返 `-32603 "http timeout failed"` | webhook 没起 / 地址错 / 超时太短(§6.4) |
| 调用返 `isError:true "http 401"` | webhook 侧目标服务凭证无效(如 `GITHUB_TOKEN` 过期) |
| 调用返 `isError:true "http 422"` | 目标服务业务校验失败(如 issue 标题为空)—— 看 body 详情 |
| agent 不调用外部工具 | 工具 schema 是 `{"type":"object"}`,agent 不知参数 —— 在 prompt 里描述(§6.2) |

直接绕过 tools-bank 测 webhook(§3.2 的 curl)能快速定位问题在 webhook 还是 tools-bank。

## 8. 参考

- 实现:`src/bin/tools-bank/adapter.rs`(`HttpActionAdapter`、`parse_http_adapters`)
- CR 设计:`docs/superpowers/plans/2026-07-13-cr11-external-action-adapter.md`
- GitHub 样例:`examples/external-adapters/github_webhook.py`
- 全栈启动(broker / fixlet / tools-bank 依赖关系):见 memory `dev-stack-startup`

### 8.1 MCP 错误码对照

| 码 | 含义 | 触发 |
|----|------|------|
| `-32602` | 未知工具 | `tools/call` 的工具名不在任何 adapter |
| `-32603` | infra 故障 | webhook 连接拒绝 / 超时;sandbox broker produce 失败 / 超时 |
| `isError:true` | 工具执行了但失败 | webhook 返非 2xx;sandbox 返 `success:false` |
