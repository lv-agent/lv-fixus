//! 文件工具(Read/Write/Edit/Glob/Grep)—— 从 fixus 进程内沙箱移植而来。
//!
//! 这些工具在 sandbox-server 进程内执行,路径校验限定在当次调用的 `work_dir`
//! (`base_dir/<step_id>`)内。Bash 仍由 `executor::execute` 经 Landlock 子进程执行,
//! 这里只处理文件类工具。
//!
//! 注:文件工具目前仅应用层路径校验(与原进程内沙箱平级),未套 Landlock;
//! 给文件工具也加内核级隔离是后续加固项。

use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;

const MAX_OUTPUT_SIZE: usize = 1024 * 1024; // 1MB

/// 校验路径在 work_dir 内,返回规范化后的绝对路径。
///
/// 防止路径遍历(如 `../../etc/shadow`)。相对路径相对 work_dir 解析。
fn validate_path_in_workspace(file_path: &str, work_dir: &Path) -> Result<PathBuf, String> {
    let path = Path::new(file_path);
    let canonical = if path.is_absolute() {
        path.to_path_buf()
    } else {
        work_dir.join(path)
    };
    // 不要求目标必须存在(Write 场景),只检查解析后的路径前缀
    let normalized = normalize_path(&canonical);
    if !normalized.starts_with(work_dir) {
        return Err(format!(
            "Path '{}' is outside work_dir '{}'. Access denied.",
            file_path,
            work_dir.display()
        ));
    }
    Ok(normalized)
}

/// 解析 `SANDBOX_READ_ALLOWED_HOST_PATHS` env 值（逗号分隔绝对路径）为 normalize 后的 PathBuf Vec。
/// 空 / 全空白 → 空 Vec（不放宽任何路径）。**相对路径 entry 被忽略**（白名单语义要求绝对路径）。
/// 入口归一化一次（main.rs 启动时调一次）。
pub fn parse_allowed_read_paths(env_value: &str) -> Vec<PathBuf> {
    env_value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| {
            let p = PathBuf::from(s);
            if p.is_absolute() {
                Some(normalize_path(&p))
            } else {
                None
            }
        })
        .collect()
}

/// 校验读类工具（R/Glob/Grep）的路径，允许两类位置：
/// 1. 在 work_dir 内（原行为）
/// 2. 绝对路径，且规范化后落在任一 `allowed_read_paths[i]` 前缀下（白名单）
///
/// 返回规范化后的绝对路径。**白名单只对绝对路径生效**；相对路径一律相对 work_dir。
/// W/E 仍用 `validate_path_in_workspace`（写类不允许白名单放宽，保 work_dir 隔离严格）。
fn validate_read_path(
    file_path: &str,
    work_dir: &Path,
    allowed_read_paths: &[PathBuf],
) -> Result<PathBuf, String> {
    let path = Path::new(file_path);
    let canonical = if path.is_absolute() {
        path.to_path_buf()
    } else {
        work_dir.join(path)
    };
    let normalized = normalize_path(&canonical);
    // 1) 白名单分支（仅绝对路径）
    if canonical.is_absolute() {
        for allowed in allowed_read_paths {
            if normalized.starts_with(allowed) {
                return Ok(normalized);
            }
        }
    }
    // 2) 原 work_dir 检查
    if !normalized.starts_with(work_dir) {
        return Err(format!(
            "Path '{}' is outside work_dir '{}'. Access denied.",
            file_path,
            work_dir.display()
        ));
    }
    Ok(normalized)
}

/// 路径规范化(不要求文件存在):处理 `.`、`..`、多余分隔符。
fn normalize_path(path: &Path) -> PathBuf {
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

/// 读取文件
pub async fn execute_read(input: &serde_json::Value, work_dir: &Path, allowed_read_paths: &[PathBuf]) -> Result<serde_json::Value, String> {
    let file_path = input
        .get("file_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Read requires 'file_path' field".to_string())?;
    let abs = validate_read_path(file_path, work_dir, allowed_read_paths)?;

    let offset = input.get("offset").and_then(|v| v.as_i64()).unwrap_or(0) as usize;
    let limit = input.get("limit").and_then(|v| v.as_i64());

    let content = tokio::fs::read_to_string(&abs)
        .await
        .map_err(|e| format!("Read failed: {}", e))?;

    let lines: Vec<&str> = content.lines().collect();
    let start = offset.min(lines.len());
    let end = match limit {
        Some(l) if l > 0 => (start + l as usize).min(lines.len()),
        _ => lines.len(),
    };
    let result = lines[start..end].join("\n");

    Ok(serde_json::json!({
        "content": truncate(&result, MAX_OUTPUT_SIZE),
        "total_lines": lines.len(),
        "lines_returned": end - start,
        "offset": start,
    }))
}

/// 写入文件
pub async fn execute_write(input: &serde_json::Value, work_dir: &Path) -> Result<serde_json::Value, String> {
    let file_path = input
        .get("file_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Write requires 'file_path' field".to_string())?;
    let abs = validate_path_in_workspace(file_path, work_dir)?;

    let content = input
        .get("content")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Write requires 'content' field".to_string())?;

    // 确保父目录存在
    if let Some(parent) = abs.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    tokio::fs::write(&abs, content)
        .await
        .map_err(|e| format!("Write failed: {}", e))?;

    Ok(serde_json::json!({
        "bytes_written": content.len(),
        "file_path": file_path,
    }))
}

/// 编辑文件(字符串替换)
pub async fn execute_edit(input: &serde_json::Value, work_dir: &Path) -> Result<serde_json::Value, String> {
    let file_path = input
        .get("file_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Edit requires 'file_path' field".to_string())?;
    let abs = validate_path_in_workspace(file_path, work_dir)?;

    let old_string = input
        .get("old_string")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Edit requires 'old_string' field".to_string())?;
    let new_string = input.get("new_string").and_then(|v| v.as_str()).unwrap_or("");

    let content = tokio::fs::read_to_string(&abs)
        .await
        .map_err(|e| format!("Edit read failed: {}", e))?;

    if !content.contains(old_string) {
        return Err(format!("old_string not found in {}", file_path));
    }

    let new_content = content.replacen(old_string, new_string, 1);
    tokio::fs::write(&abs, &new_content)
        .await
        .map_err(|e| format!("Edit write failed: {}", e))?;

    Ok(serde_json::json!({
        "file_path": file_path,
        "replaced": true,
        "bytes_written": new_content.len(),
    }))
}

/// Glob 模式匹配文件
pub async fn execute_glob(input: &serde_json::Value, work_dir: &Path, allowed_read_paths: &[PathBuf]) -> Result<serde_json::Value, String> {
    let pattern = input
        .get("pattern")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Glob requires 'pattern' field".to_string())?;
    let raw_path = input.get("path").and_then(|v| v.as_str()).unwrap_or(".");
    let abs = validate_read_path(raw_path, work_dir, allowed_read_paths)?;

    let output = Command::new("find")
        .arg(&abs)
        .arg("-name")
        .arg(pattern)
        .arg("-type")
        .arg("f")
        .output()
        .await
        .map_err(|e| format!("Glob find failed: {}", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let files: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();

    Ok(serde_json::json!({
        "files": files,
        "count": files.len(),
        "pattern": pattern,
    }))
}

/// Grep 搜索文件内容
pub async fn execute_grep(input: &serde_json::Value, work_dir: &Path, allowed_read_paths: &[PathBuf], timeout_dur: Duration) -> Result<serde_json::Value, String> {
    let pattern = input
        .get("pattern")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Grep requires 'pattern' field".to_string())?;
    let raw_path = input.get("path").and_then(|v| v.as_str()).unwrap_or(".");
    let abs = validate_read_path(raw_path, work_dir, allowed_read_paths)?;

    let output_future = Command::new("grep")
        .arg("-rn")
        .arg("-I") // 忽略二进制
        .arg("--color=never")
        .arg(pattern)
        .arg(&abs)
        .output();

    let output = timeout(timeout_dur, output_future)
        .await
        .map_err(|_| "Grep timed out".to_string())?
        .map_err(|e| format!("Grep failed: {}", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let results: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();

    Ok(serde_json::json!({
        "matches": truncate(&results.join("\n"), MAX_OUTPUT_SIZE),
        "count": results.len(),
        "pattern": pattern,
    }))
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create temp dir")
    }

    #[test]
    fn normalize_removes_current_dir() {
        assert_eq!(normalize_path(Path::new("/a/./b")), PathBuf::from("/a/b"));
    }

    #[test]
    fn normalize_removes_parent_dir() {
        assert_eq!(normalize_path(Path::new("/a/b/../c")), PathBuf::from("/a/c"));
    }

    #[test]
    fn normalize_removes_extra_separators() {
        let result = normalize_path(Path::new("/a//b")).to_string_lossy().to_string();
        assert!(!result.contains("//"));
    }

    #[test]
    fn truncate_short_string() {
        assert_eq!(truncate("hello", 100), "hello");
    }

    #[test]
    fn truncate_long_string() {
        let long = "x".repeat(2000);
        let result = truncate(&long, 1000);
        assert!(result.len() < 1500);
        assert!(result.contains("truncated"));
    }

    #[test]
    fn validate_path_in_workspace_rejects_outside() {
        let work_dir = Path::new("/tmp/work");
        assert!(validate_path_in_workspace("/etc/passwd", work_dir).is_err());
        assert!(validate_path_in_workspace("../../etc/passwd", work_dir).is_err());
    }

    #[test]
    fn validate_path_in_workspace_allows_inside() {
        let work_dir = Path::new("/tmp/work");
        assert!(validate_path_in_workspace("/tmp/work/file.txt", work_dir).is_ok());
        assert!(validate_path_in_workspace("subdir/file.txt", work_dir).is_ok());
    }

    #[tokio::test]
    async fn execute_read_reads_file() {
        let dir = tmp_dir();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "line1\nline2\nline3").unwrap();

        let input = serde_json::json!({"file_path": file_path.to_string_lossy()});
        let result = execute_read(&input, dir.path(), &[]).await.unwrap();

        assert_eq!(result["content"].as_str().unwrap(), "line1\nline2\nline3");
        assert_eq!(result["total_lines"].as_u64().unwrap(), 3);
        assert_eq!(result["lines_returned"].as_u64().unwrap(), 3);
    }

    #[tokio::test]
    async fn execute_read_respects_offset_and_limit() {
        let dir = tmp_dir();
        let file_path = dir.path().join("lines.txt");
        std::fs::write(&file_path, "a\nb\nc\nd\ne").unwrap();

        let input = serde_json::json!({"file_path": file_path.to_string_lossy(), "offset": 1, "limit": 2});
        let result = execute_read(&input, dir.path(), &[]).await.unwrap();

        assert_eq!(result["content"].as_str().unwrap(), "b\nc");
        assert_eq!(result["lines_returned"].as_u64().unwrap(), 2);
    }

    #[tokio::test]
    async fn execute_read_rejects_path_outside_work_dir() {
        let dir = tmp_dir();
        let input = serde_json::json!({"file_path": "/etc/hosts"});
        assert!(execute_read(&input, dir.path(), &[]).await.is_err());
    }

    #[tokio::test]
    async fn execute_write_creates_and_writes_file() {
        let dir = tmp_dir();
        let input = serde_json::json!({"file_path": "out.txt", "content": "test content"});
        let result = execute_write(&input, dir.path()).await.unwrap();

        assert_eq!(result["bytes_written"].as_u64().unwrap(), 12);
        let written = std::fs::read_to_string(dir.path().join("out.txt")).unwrap();
        assert_eq!(written, "test content");
    }

    #[tokio::test]
    async fn execute_write_rejects_path_outside_work_dir() {
        let dir = tmp_dir();
        let input = serde_json::json!({"file_path": "/tmp/evil.txt", "content": "bad"});
        assert!(execute_write(&input, dir.path()).await.is_err());
    }

    #[tokio::test]
    async fn execute_edit_replaces_string() {
        let dir = tmp_dir();
        let file_path = dir.path().join("edit.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let input = serde_json::json!({
            "file_path": &file_path.to_string_lossy(),
            "old_string": "world",
            "new_string": "rust"
        });
        let result = execute_edit(&input, dir.path()).await.unwrap();
        assert_eq!(result["replaced"].as_bool().unwrap(), true);

        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "hello rust");
    }

    #[test]
    fn parse_allowed_read_paths_empty_and_whitespace() {
        assert_eq!(parse_allowed_read_paths(""), Vec::<PathBuf>::new());
        assert_eq!(parse_allowed_read_paths(",,"), Vec::<PathBuf>::new());
        assert_eq!(parse_allowed_read_paths("   "), Vec::<PathBuf>::new());
    }

    #[test]
    fn parse_allowed_read_paths_skips_relative() {
        let r = parse_allowed_read_paths("/abs,relative,./foo,/another");
        assert_eq!(r, vec![PathBuf::from("/abs"), PathBuf::from("/another")]);
    }

    #[test]
    fn parse_allowed_read_paths_normalizes() {
        let r = parse_allowed_read_paths("/a/./b/../c");
        assert_eq!(r, vec![PathBuf::from("/a/c")]);
    }

    #[test]
    fn validate_read_path_allows_workdir_when_no_whitelist() {
        let work_dir = Path::new("/tmp/work");
        let allowed: Vec<PathBuf> = vec![];
        assert!(validate_read_path("/tmp/work/file.txt", work_dir, &allowed).is_ok());
        assert!(validate_read_path("subdir/file.txt", work_dir, &allowed).is_ok());
    }

    #[test]
    fn validate_read_path_allows_whitelisted_absolute_path() {
        let work_dir = Path::new("/tmp/work");
        let allowed = vec![PathBuf::from("/home/lvtao/codeagent")];
        assert!(validate_read_path("/home/lvtao/codeagent/multica/go.mod", work_dir, &allowed).is_ok());
        assert!(validate_read_path("/home/lvtao/codeagent/foo/bar.go", work_dir, &allowed).is_ok());
    }

    #[test]
    fn validate_read_path_rejects_outside_both() {
        let work_dir = Path::new("/tmp/work");
        let allowed = vec![PathBuf::from("/home/lvtao/codeagent")];
        assert!(validate_read_path("/etc/passwd", work_dir, &allowed).is_err());
        assert!(validate_read_path("/home/lvtao/other/foo.go", work_dir, &allowed).is_err());
    }

    #[test]
    fn validate_read_path_rejects_relative_path_escape_attempt() {
        let work_dir = Path::new("/tmp/work");
        let allowed = vec![PathBuf::from("/home/lvtao/codeagent")];
        // 相对路径 "../etc/passwd" 不走白名单分支(只在绝对路径生效),落 work_dir 校验被拒
        assert!(validate_read_path("../etc/passwd", work_dir, &allowed).is_err());
    }

    #[tokio::test]
    async fn execute_edit_errors_when_old_string_not_found() {
        let dir = tmp_dir();
        let file_path = dir.path().join("edit.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let input = serde_json::json!({
            "file_path": &file_path.to_string_lossy(),
            "old_string": "nonexistent",
            "new_string": "rust"
        });
        assert!(execute_edit(&input, dir.path()).await.is_err());
    }
}
