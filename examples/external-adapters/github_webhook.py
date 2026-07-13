#!/usr/bin/env python3
"""github_webhook.py — tools-bank 外部 action adapter 样例:GitHub issue 工具。

把 agent 的工具调用桥接到 GitHub REST API。tools-bank 用 HttpActionAdapter
POST 到本服务,本服务按工具名分发到对应 GitHub 端点。

工具(在 tools-bank 配置里声明,见 README/指南):
  gh_create_issue    arguments: {repo, title, body?, labels?}
  gh_list_issues     arguments: {repo, state?, limit?}
  gh_get_issue       arguments: {repo, issue_number}
  gh_add_comment     arguments: {repo, issue_number, body}

契约(tools-bank → webhook):
  POST /<tool_name>
  body: {"tool", "arguments", "task_id", "idempotency_key"}
  可选 Authorization 头(若 tools-bank 配了 auth=)

环境变量:
  GITHUB_TOKEN         必填,GitHub Personal Access Token(仅在此服务内持有,
                       agent 永不经手 —— 这是安全关键)。
  GITHUB_API_BASE      可选,默认 https://api.github.com(GitHub Enterprise 改这里)。
  PORT                 可选,默认 9000。

返回语义(对齐 tools-bank HttpActionAdapter):
  GitHub 2xx    → 本服务 2xx + GitHub 原始 body         → agent success
  GitHub 非 2xx → 本服务 同状态码 + GitHub 错误 body      → agent isError,
                  tools-bank 把 "http <code>" + body 拼进 text,agent 看到详情
  本地异常       → 500 + {"error": "..."}               → agent isError
"""
import json
import os
import sys
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_API_BASE = os.environ.get("GITHUB_API_BASE", "https://api.github.com").rstrip("/")
PORT = int(os.environ.get("PORT", "9000"))

TOOLS = ["gh_create_issue", "gh_list_issues", "gh_get_issue", "gh_add_comment"]


def gh(method, path, body=None):
    """调 GitHub API,返回 (status, json_body)。GitHub HTTP 错误透传(状态码+body)。"""
    url = f"{GITHUB_API_BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, method=method, data=data, headers={
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        # GitHub 业务错误(404/422/403 等):透传状态码 + body,让 agent 看到。
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"error": raw}


def dispatch(tool, args):
    """按工具名分发到 GitHub 端点。返回 (status, body)。"""
    repo = args.get("repo")
    if not repo or "/" not in str(repo):
        return 400, {"error": "arguments.repo 必须为 'owner/repo' 形式"}
    base = f"/repos/{repo}"

    if tool == "gh_create_issue":
        title = args.get("title")
        if not title:
            return 400, {"error": "arguments.title 必填"}
        payload = {
            "title": title,
            "body": args.get("body", ""),
            "labels": args.get("labels", []) or [],
        }
        return gh("POST", f"{base}/issues", payload)

    if tool == "gh_list_issues":
        state = args.get("state", "open")
        limit = int(args.get("limit", 30))
        return gh("GET", f"{base}/issues?state={state}&per_page={limit}"
                         "&sort=created&direction=desc")

    if tool == "gh_get_issue":
        n = args.get("issue_number")
        if not n:
            return 400, {"error": "arguments.issue_number 必填"}
        return gh("GET", f"{base}/issues/{n}")

    if tool == "gh_add_comment":
        n = args.get("issue_number")
        text = args.get("body")
        if not n:
            return 400, {"error": "arguments.issue_number 必填"}
        if not text:
            return 400, {"error": "arguments.body 必填"}
        return gh("POST", f"{base}/issues/{n}/comments", {"body": text})

    return 404, {"error": f"unknown tool: {tool}"}


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, obj):
        data = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        tool = self.path.strip("/").split("/")[0]
        try:
            n = int(self.headers.get("Content-Length", 0))
            envelope = json.loads(self.rfile.read(n) or b"{}")
        except Exception as e:
            self._send(400, {"error": f"bad request body: {e}"})
            return
        args = envelope.get("arguments", {}) or {}
        task_id = envelope.get("task_id", "?")
        idem = envelope.get("idempotency_key", "?")
        print(f"[{tool}] task={task_id} idem={idem[:24]} repo={args.get('repo')}",
              file=sys.stderr)
        try:
            status, body = dispatch(tool, args)
        except Exception as e:
            self._send(500, {"error": f"webhook crashed: {e}"})
            return
        self._send(status, body)

    def do_GET(self):
        # 健康检查 + 工具自描述(排错用)
        if self.path in ("/", "/health"):
            self._send(200, {
                "service": "github-webhook",
                "tools": TOOLS,
                "api_base": GITHUB_API_BASE,
                "token_configured": bool(GITHUB_TOKEN),
            })
            return
        self._send(404, {"error": "GET only supported at / or /health"})

    def log_message(self, fmt, *a):  # 用上面自定义 print,静默默认访问日志
        pass


if __name__ == "__main__":
    if not GITHUB_TOKEN:
        print("WARN: GITHUB_TOKEN 未设置 —— GitHub 调用将返回 401", file=sys.stderr)
    print(f"github-webhook on 127.0.0.1:{PORT}  api={GITHUB_API_BASE}  tools={TOOLS}",
          file=sys.stderr)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
