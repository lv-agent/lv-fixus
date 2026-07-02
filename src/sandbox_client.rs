//! Sandbox Client — 调用独立的 sandbox-server 执行工具
//!
//! 如果 `SANDBOX_URL` 环境变量设置，通过 HTTP 调用远端 sandbox-server；
//! 否则 fallback 到本地进程内执行（当前 sandbox.rs）。
//!
//! sandbox-server API:
//!   POST /session/{id}/exec  {code, timeout_secs}  → {stdout, stderr, exit_code}
//!   DELETE /session/{id}                            → cleanup

use serde::{Deserialize, Serialize};

use crate::error::AppError;

// ── Sandbox 执行接口 ────────────────────────────────────────────────────

/// 统一的 Sandbox 执行结果
pub struct SandboxExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub duration_ms: u64,
}

/// 统一的 Sandbox 执行接口
#[async_trait::async_trait]
pub trait SandboxExecutor: Send + Sync {
    async fn exec(
        &self,
        session_id: &str,
        code: &str,
        timeout_secs: u64,
    ) -> Result<SandboxExecResult, AppError>;

    async fn cleanup(&self, session_id: &str);
}

// ── SandboxClient（自动选择 Remote 或 Local）────────────────────────────

pub struct SandboxClient {
    inner: Box<dyn SandboxExecutor>,
}

impl SandboxClient {
    pub fn new() -> Self {
        if let Ok(url) = std::env::var("SANDBOX_URL") {
            tracing::info!("Using remote sandbox at {}", url);
            Self { inner: Box::new(RemoteSandbox::new(&url)) }
        } else {
            tracing::info!("Using local sandbox (no SANDBOX_URL set)");
            Self { inner: Box::new(LocalSandbox) }
        }
    }

    pub async fn exec(
        &self,
        session_id: &str,
        code: &str,
        timeout_secs: u64,
    ) -> Result<SandboxExecResult, AppError> {
        self.inner.exec(session_id, code, timeout_secs).await
    }

    pub async fn cleanup(&self, session_id: &str) {
        self.inner.cleanup(session_id).await;
    }
}

// ── RemoteSandbox（HTTP 调用 sandbox-server）────────────────────────────

struct RemoteSandbox {
    base_url: String,
    client: reqwest::Client,
}

impl RemoteSandbox {
    fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }
}

#[derive(Serialize)]
struct ExecRequest<'a> {
    code: &'a str,
    #[serde(default = "default_timeout")]
    timeout_secs: u64,
}

fn default_timeout() -> u64 { 120 }

#[derive(Deserialize)]
struct ExecResponse {
    stdout: Option<String>,
    stderr: Option<String>,
    exit_code: Option<i32>,
    #[allow(dead_code)]
    error: Option<String>,
}

#[async_trait::async_trait]
impl SandboxExecutor for RemoteSandbox {
    async fn exec(
        &self,
        session_id: &str,
        code: &str,
        timeout_secs: u64,
    ) -> Result<SandboxExecResult, AppError> {
        let url = format!("{}/session/{}/exec", self.base_url, session_id);
        let start = std::time::Instant::now();

        let resp = self.client
            .post(&url)
            .json(&ExecRequest { code, timeout_secs })
            .timeout(std::time::Duration::from_secs(timeout_secs + 10))
            .send()
            .await
            .map_err(|e| AppError::Internal(format!("sandbox HTTP error: {}", e)))?;

        let duration_ms = start.elapsed().as_millis() as u64;

        if !resp.status().is_success() {
            return Err(AppError::Internal(format!(
                "sandbox returned {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }

        let result: ExecResponse = resp.json().await
            .map_err(|e| AppError::Internal(format!("sandbox response parse error: {}", e)))?;

        Ok(SandboxExecResult {
            stdout: result.stdout.unwrap_or_default(),
            stderr: result.stderr.unwrap_or_default(),
            exit_code: result.exit_code.unwrap_or(-1),
            duration_ms,
        })
    }

    async fn cleanup(&self, session_id: &str) {
        let url = format!("{}/session/{}", self.base_url, session_id);
        let _ = self.client.delete(&url).send().await;
    }
}

// ── LocalSandbox（fallback，进程内执行）─────────────────────────────────

struct LocalSandbox;

#[async_trait::async_trait]
impl SandboxExecutor for LocalSandbox {
    async fn exec(
        &self,
        _session_id: &str,
        code: &str,
        timeout_secs: u64,
    ) -> Result<SandboxExecResult, AppError> {
        let result = crate::sandbox::execute_tool(crate::sandbox::ExecuteRequest {
            tool_name: "Bash".into(),
            tool_call_id: "local".into(),
            idempotency_key: "local".into(),
            input: serde_json::json!({"command": code}),
            timeout_ms: timeout_secs * 1000,
        }).await;

        let stdout = result.output.get("stdout")
            .and_then(|v| v.as_str()).unwrap_or("").to_string();
        let stderr = result.output.get("stderr")
            .and_then(|v| v.as_str()).unwrap_or("").to_string();
        let exit_code = result.output.get("exit_code")
            .and_then(|v| v.as_i64()).unwrap_or(-1) as i32;

        Ok(SandboxExecResult {
            stdout,
            stderr,
            exit_code,
            duration_ms: result.duration_ms,
        })
    }

    async fn cleanup(&self, _session_id: &str) {
        // local sandbox has no persistent state to clean up
    }
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_sandbox_exec() {
        let client = SandboxClient::new(); // will use LocalSandbox (no SANDBOX_URL)
        let result = client.exec("test", "echo hello-sandbox", 10).await.unwrap();
        assert!(result.stdout.contains("hello-sandbox"));
        assert_eq!(result.exit_code, 0);
    }

    #[tokio::test]
    async fn test_local_sandbox_timeout() {
        let client = SandboxClient::new();
        let result = client.exec("test", "sleep 30", 1).await;
        // 1 second timeout on sleep 30 should fail
        // Local sandbox might not enforce timeout perfectly, so just check it doesn't crash
        match result {
            Ok(r) => assert!(r.exit_code != 0 || r.stderr.contains("Terminated")),
            Err(_) => {} // timeout is also acceptable
        }
    }
}
