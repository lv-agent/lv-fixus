//! Runtime Sandbox — 安全执行工具调用的隔离环境
//!
//! 提供通用的 Tool Execution API。fixus 在写入 tool_invoked 后，
//! 通过此模块执行实际的工具调用，然后写入 tool_completed/tool_failed。
//!
//! # 安全模型
//! - 开发阶段：tokio::process::Command 子进程
//! - 生产阶段：可替换为 Docker/Firecracker/K8s Job
//!
//! # 职责边界
//! - Sandbox 不知道 Event Store 的存在
//! - Sandbox 不管理幂等性（idempotency_key 只传递给 Tool，由 Tool 自身判断）
//! - Sandbox 不决定重试策略（由 fixus recovery.rs 负责）

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;

use crate::error::{AppError, Result};

// ── 执行请求/响应 ──────────────────────────────────────────────────────

/// Tool 执行请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteRequest {
    pub tool_name: String,
    pub tool_call_id: String,
    /// 幂等键（由 fixus 传递，Tool 实现侧检查）
    pub idempotency_key: String,
    /// Tool 输入参数
    pub input: serde_json::Value,
    /// 超时毫秒（默认 10_000）
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    10_000
}

/// Tool 执行结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteResult {
    pub tool_call_id: String,
    pub success: bool,
    pub output: serde_json::Value,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ── Sandbox 执行器 ──────────────────────────────────────────────────────

/// Sandbox 执行上下文
#[derive(Clone)]
pub struct Sandbox {
    /// 默认超时
    pub default_timeout: Duration,
    /// 允许的最大输出大小（字节）
    pub max_output_size: usize,
    /// 工作空间根目录 — Read/Write/Edit 仅允许操作此目录内的文件。
    /// 从 FIXUS_WORKSPACE_ROOT 环境变量读取，未设置时默认为当前工作目录。
    pub workspace_root: std::path::PathBuf,
}

impl Default for Sandbox {
    fn default() -> Self {
        let workspace_root = std::env::var("FIXUS_WORKSPACE_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")));

        Self {
            default_timeout: Duration::from_secs(10),
            max_output_size: 1024 * 1024, // 1MB
            workspace_root,
        }
    }
}

impl Sandbox {
    pub fn new() -> Self {
        Self::default()
    }

    /// 校验路径是否在 workspace 根目录内
    ///
    /// 防止路径遍历攻击（如 `../../etc/shadow`）。
    /// Bash 工具不经过此校验（由 sandbox-server 的 Landlock 负责隔离）。
    fn validate_path_in_workspace(&self, file_path: &str) -> Result<()> {
        let path = std::path::Path::new(file_path);

        // 解析为绝对路径（handle .., symlinks 等）
        let canonical = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.workspace_root.join(path)
        };

        // 不要求目标必须存在（Write 场景），只检查解析后的路径前缀
        let normalized = normalize_path(&canonical);

        if !normalized.starts_with(&self.workspace_root) {
            return Err(AppError::Validation(format!(
                "Path '{}' is outside workspace root '{}'. Access denied.",
                file_path,
                self.workspace_root.display()
            )));
        }
        Ok(())
    }

    /// 执行一个 Tool 调用
    pub async fn execute(&self, req: ExecuteRequest) -> ExecuteResult {
        let start = std::time::Instant::now();
        let timeout_dur = Duration::from_millis(req.timeout_ms);

        let result = self.execute_tool_impl(&req, timeout_dur).await;

        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(output) => ExecuteResult {
                tool_call_id: req.tool_call_id,
                success: true,
                output,
                duration_ms,
                error: None,
            },
            Err(e) => ExecuteResult {
                tool_call_id: req.tool_call_id,
                success: false,
                output: serde_json::Value::Null,
                duration_ms,
                error: Some(e.to_string()),
            },
        }
    }

    /// 执行 Bash 命令
    async fn execute_bash(
        &self,
        req: &ExecuteRequest,
        timeout_dur: Duration,
    ) -> Result<serde_json::Value> {
        let command = req
            .input
            .get("command")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::Validation("Bash requires 'command' field".into()))?;

        let output_future = Command::new("bash")
            .arg("-c")
            .arg(command)
            .output();

        let output = timeout(timeout_dur, output_future)
            .await
            .map_err(|_| AppError::Internal("Bash command timed out".into()))?
            .map_err(|e| AppError::Internal(format!("Bash execution failed: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        // 截断过大输出
        let stdout = truncate(&stdout, self.max_output_size);
        let stderr = truncate(&stderr, self.max_output_size);

        Ok(serde_json::json!({
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": output.status.code().unwrap_or(-1),
        }))
    }

    /// 读取文件
    async fn execute_read(
        &self,
        req: &ExecuteRequest,
        _timeout: Duration,
    ) -> Result<serde_json::Value> {
        let file_path = req
            .input
            .get("file_path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::Validation("Read requires 'file_path' field".into()))?;

        self.validate_path_in_workspace(file_path)?;

        let offset = req
            .input
            .get("offset")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;

        let limit = req
            .input
            .get("limit")
            .and_then(|v| v.as_i64());

        let content = tokio::fs::read_to_string(file_path).await.map_err(|e| {
            AppError::Internal(format!("Read failed: {}", e))
        })?;

        // 支持偏移和行数限制
        let lines: Vec<&str> = content.lines().collect();
        let start = offset.min(lines.len());
        let end = match limit {
            Some(l) if l > 0 => (start + l as usize).min(lines.len()),
            _ => lines.len(),
        };
        let result = lines[start..end].join("\n");

        Ok(serde_json::json!({
            "content": truncate(&result, self.max_output_size),
            "total_lines": lines.len(),
            "lines_returned": end - start,
            "offset": start,
        }))
    }

    /// 写入文件
    async fn execute_write(
        &self,
        req: &ExecuteRequest,
        _timeout: Duration,
    ) -> Result<serde_json::Value> {
        let file_path = req
            .input
            .get("file_path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::Validation("Write requires 'file_path' field".into()))?;

        self.validate_path_in_workspace(file_path)?;

        let content = req
            .input
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::Validation("Write requires 'content' field".into()))?;

        tokio::fs::write(file_path, content).await.map_err(|e| {
            AppError::Internal(format!("Write failed: {}", e))
        })?;

        Ok(serde_json::json!({
            "bytes_written": content.len(),
            "file_path": file_path,
        }))
    }

    /// 编辑文件（字符串替换）
    async fn execute_edit(
        &self,
        req: &ExecuteRequest,
        _timeout: Duration,
    ) -> Result<serde_json::Value> {
        let file_path = req
            .input
            .get("file_path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::Validation("Edit requires 'file_path' field".into()))?;

        self.validate_path_in_workspace(file_path)?;

        let old_string = req
            .input
            .get("old_string")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::Validation("Edit requires 'old_string' field".into()))?;

        let new_string = req
            .input
            .get("new_string")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let content = tokio::fs::read_to_string(file_path).await.map_err(|e| {
            AppError::Internal(format!("Edit read failed: {}", e))
        })?;

        if !content.contains(old_string) {
            return Err(AppError::Validation(format!(
                "old_string not found in {}",
                file_path
            )));
        }

        let new_content = content.replacen(old_string, new_string, 1);
        tokio::fs::write(file_path, &new_content).await.map_err(|e| {
            AppError::Internal(format!("Edit write failed: {}", e))
        })?;

        Ok(serde_json::json!({
            "file_path": file_path,
            "replaced": true,
            "bytes_written": new_content.len(),
        }))
    }

    /// Glob 模式匹配文件
    async fn execute_glob(
        &self,
        req: &ExecuteRequest,
        _timeout: Duration,
    ) -> Result<serde_json::Value> {
        let pattern = req
            .input
            .get("pattern")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::Validation("Glob requires 'pattern' field".into()))?;

        let path = req
            .input
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or(".");

        // 用 find 命令实现 glob（简单但有效）
        let full_pattern = format!("{}/{}", path, pattern);
        let output = Command::new("find")
            .arg(path)
            .arg("-name")
            .arg(pattern)
            .arg("-type").arg("f")
            .output().await
            .map_err(|e| AppError::Internal(format!("Glob find failed: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let files: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();

        Ok(serde_json::json!({
            "files": files,
            "count": files.len(),
            "pattern": pattern,
        }))
    }

    /// Grep 搜索文件内容
    async fn execute_grep(
        &self,
        req: &ExecuteRequest,
        timeout_dur: Duration,
    ) -> Result<serde_json::Value> {
        let pattern = req
            .input
            .get("pattern")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AppError::Validation("Grep requires 'pattern' field".into()))?;

        let path = req
            .input
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or(".");

        let output_future = Command::new("grep")
            .arg("-rn")
            .arg("-I")  // 忽略二进制
            .arg("--color=never")
            .arg(pattern)
            .arg(path)
            .output();

        let output = timeout(timeout_dur, output_future)
            .await
            .map_err(|_| AppError::Internal("Grep timed out".into()))?
            .map_err(|e| AppError::Internal(format!("Grep failed: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let results: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();

        Ok(serde_json::json!({
            "matches": truncate(&results.join("\n"), self.max_output_size),
            "count": results.len(),
            "pattern": pattern,
        }))
    }

    /// 执行工具（路由到具体实现）
    async fn execute_tool_impl(
        &self,
        req: &ExecuteRequest,
        timeout_dur: Duration,
    ) -> Result<serde_json::Value> {
        match req.tool_name.as_str() {
            "Bash" | "bash" => self.execute_bash(req, timeout_dur).await,
            "Read" | "read" => self.execute_read(req, timeout_dur).await,
            "Write" | "write" => self.execute_write(req, timeout_dur).await,
            "Edit" | "edit" => self.execute_edit(req, timeout_dur).await,
            "Glob" | "glob" => self.execute_glob(req, timeout_dur).await,
            "Grep" | "grep" => self.execute_grep(req, timeout_dur).await,
            _ => self.execute_unknown(req).await,
        }
    }

    /// 未知 Tool 的默认处理
    async fn execute_unknown(&self, req: &ExecuteRequest) -> Result<serde_json::Value> {
        Err(AppError::Validation(format!(
            "Unknown tool '{}'. Register it in the sandbox or add a custom executor.",
            req.tool_name
        )))
    }
}

/// 截断超长字符串
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!(
            "{}...\n[truncated: {} bytes total, showing first {}]",
            &s[..max_len / 2],
            s.len(),
            max_len / 2
        )
    }
}

/// 路径规范化（不要求文件存在）
///
/// 处理 `.`、`..`、多余分隔符，返回不含 `..` 的规范化路径。
fn normalize_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                parts.pop();
            }
            _ => {
                parts.push(component);
            }
        }
    }
    parts.iter().collect()
}

// ── 便捷函数 ────────────────────────────────────────────────────────────

/// 用默认 Sandbox 执行一个 Tool 调用
pub async fn execute_tool(req: ExecuteRequest) -> ExecuteResult {
    Sandbox::default().execute(req).await
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_execute_echo() {
        let sandbox = Sandbox::default();
        let result = sandbox
            .execute(ExecuteRequest {
                tool_name: "Bash".into(),
                tool_call_id: "call_001".into(),
                idempotency_key: "test:rg:bash:echo".into(),
                input: serde_json::json!({"command": "echo hello"}),
                timeout_ms: 5000,
            })
            .await;

        assert!(result.success);
        assert_eq!(result.tool_call_id, "call_001");
        assert!(result
            .output
            .get("stdout")
            .and_then(|v| v.as_str())
            .unwrap()
            .contains("hello"));
        assert_eq!(
            result
                .output
                .get("exit_code")
                .and_then(|v| v.as_i64())
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_execute_nonzero_exit() {
        let sandbox = Sandbox::default();
        let result = sandbox
            .execute(ExecuteRequest {
                tool_name: "Bash".into(),
                tool_call_id: "call_002".into(),
                idempotency_key: "test:rg:bash:fail".into(),
                input: serde_json::json!({"command": "exit 1"}),
                timeout_ms: 5000,
            })
            .await;

        assert!(result.success); // 命令执行完成就算 success
        assert_eq!(
            result
                .output
                .get("exit_code")
                .and_then(|v| v.as_i64())
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn test_execute_timeout() {
        let sandbox = Sandbox::default();
        let result = sandbox
            .execute(ExecuteRequest {
                tool_name: "Bash".into(),
                tool_call_id: "call_003".into(),
                idempotency_key: "test:rg:bash:slow".into(),
                input: serde_json::json!({"command": "sleep 10"}),
                timeout_ms: 100, // 100ms 超时
            })
            .await;

        assert!(!result.success);
        assert!(result.error.unwrap().contains("timed out"));
    }

    #[tokio::test]
    async fn test_unknown_tool() {
        let sandbox = Sandbox::default();
        let result = sandbox
            .execute(ExecuteRequest {
                tool_name: "UnknownTool".into(),
                tool_call_id: "call_004".into(),
                idempotency_key: "test:rg:unknown:{}".into(),
                input: serde_json::json!({}),
                timeout_ms: 5000,
            })
            .await;

        assert!(!result.success);
        assert!(result.error.unwrap().contains("Unknown tool"));
    }

    #[tokio::test]
    async fn test_execute_read() {
        let sandbox = Sandbox::default();
        // 读 Cargo.toml
        let result = sandbox
            .execute(ExecuteRequest {
                tool_name: "Read".into(),
                tool_call_id: "call_r1".into(),
                idempotency_key: "test:rg:read:1".into(),
                input: serde_json::json!({"file_path": "Cargo.toml", "limit": 3}),
                timeout_ms: 5000,
            })
            .await;

        assert!(result.success);
        let content = result.output.get("content").and_then(|v| v.as_str()).unwrap();
        assert!(content.contains("[package]"));
    }

    #[tokio::test]
    async fn test_execute_write_and_read() {
        let sandbox = Sandbox::default();
        let tmp_dir = format!("target/test-tmp-{}", std::process::id());
        let _ = std::fs::create_dir_all(&tmp_dir);
        let tmp = format!("{}/fixus_sandbox_test.txt", tmp_dir);

        // Write
        let r = sandbox.execute(ExecuteRequest {
            tool_name: "Write".into(),
            tool_call_id: "call_w1".into(),
            idempotency_key: "test:rg:write:1".into(),
            input: serde_json::json!({"file_path": &tmp, "content": "hello sandbox"}),
            timeout_ms: 5000,
        }).await;
        assert!(r.success, "Write failed: {:?}", r.error);

        // Read back
        let r = sandbox.execute(ExecuteRequest {
            tool_name: "Read".into(),
            tool_call_id: "call_r2".into(),
            idempotency_key: "test:rg:read:2".into(),
            input: serde_json::json!({"file_path": &tmp}),
            timeout_ms: 5000,
        }).await;
        assert!(r.success, "Read failed: {:?}", r.error);
        assert!(r.output.get("content").and_then(|v| v.as_str()).unwrap().contains("hello sandbox"));

        // Cleanup
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_dir(&tmp_dir);
    }

    #[tokio::test]
    async fn test_execute_edit() {
        let sandbox = Sandbox::default();
        let tmp_dir = format!("target/test-tmp-{}", std::process::id());
        let _ = std::fs::create_dir_all(&tmp_dir);
        let tmp = format!("{}/fixus_edit_test.txt", tmp_dir);
        std::fs::write(&tmp, "line1\nline2\nline3").unwrap();

        let r = sandbox.execute(ExecuteRequest {
            tool_name: "Edit".into(),
            tool_call_id: "call_e1".into(),
            idempotency_key: "test:rg:edit:1".into(),
            input: serde_json::json!({"file_path": &tmp, "old_string": "line2", "new_string": "modified"}),
            timeout_ms: 5000,
        }).await;
        assert!(r.success, "Edit failed: {:?}", r.error);

        let content = std::fs::read_to_string(&tmp).unwrap();
        assert!(content.contains("modified"));
        assert!(!content.contains("line2"));

        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_dir(&tmp_dir);
    }

    #[tokio::test]
    async fn test_path_traversal_blocked() {
        let sandbox = Sandbox::default();

        // Read outside workspace should fail
        let r = sandbox.execute(ExecuteRequest {
            tool_name: "Read".into(),
            tool_call_id: "call_tr1".into(),
            idempotency_key: "test:rg:read:tr".into(),
            input: serde_json::json!({"file_path": "/etc/passwd"}),
            timeout_ms: 5000,
        }).await;
        assert!(!r.success, "Read /etc/passwd should be blocked");
        assert!(r.error.unwrap().contains("outside workspace"));

        // Write outside workspace should fail
        let r = sandbox.execute(ExecuteRequest {
            tool_name: "Write".into(),
            tool_call_id: "call_tr2".into(),
            idempotency_key: "test:rg:write:tr".into(),
            input: serde_json::json!({"file_path": "/tmp/fixus_traversal_test.txt", "content": "hack"}),
            timeout_ms: 5000,
        }).await;
        assert!(!r.success, "Write /tmp should be blocked");
        assert!(r.error.unwrap().contains("outside workspace"));

        // Path traversal via .. should fail
        let r = sandbox.execute(ExecuteRequest {
            tool_name: "Read".into(),
            tool_call_id: "call_tr3".into(),
            idempotency_key: "test:rg:read:tr3".into(),
            input: serde_json::json!({"file_path": "../../../etc/shadow"}),
            timeout_ms: 5000,
        }).await;
        assert!(!r.success, "Path traversal should be blocked");
        assert!(r.error.unwrap().contains("outside workspace"));
    }

    #[tokio::test]
    async fn test_execute_glob() {
        let sandbox = Sandbox::default();
        let result = sandbox.execute(ExecuteRequest {
            tool_name: "Glob".into(),
            tool_call_id: "call_g1".into(),
            idempotency_key: "test:rg:glob:1".into(),
            input: serde_json::json!({"pattern": "*.rs", "path": "src"}),
            timeout_ms: 5000,
        }).await;

        assert!(result.success);
        let count = result.output.get("count").and_then(|v| v.as_i64()).unwrap();
        assert!(count > 0, "Should find .rs files in src/");
    }

    #[tokio::test]
    async fn test_execute_grep() {
        let sandbox = Sandbox::default();
        let result = sandbox.execute(ExecuteRequest {
            tool_name: "Grep".into(),
            tool_call_id: "call_gr1".into(),
            idempotency_key: "test:rg:grep:1".into(),
            input: serde_json::json!({"pattern": "pub struct", "path": "src"}),
            timeout_ms: 5000,
        }).await;

        assert!(result.success);
        let count = result.output.get("count").and_then(|v| v.as_i64()).unwrap();
        assert!(count > 0, "Should find 'pub struct' in src/");
    }

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("short", 100), "short");
        let long = "a".repeat(200);
        let truncated = truncate(&long, 100);
        assert!(truncated.len() < 200);
        assert!(truncated.contains("truncated"));
    }
}
