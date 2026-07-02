#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Stdio;

#[derive(Debug)]
pub struct ExecOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

/// Execute code in the given working directory with resource limits and
/// Landlock filesystem restrictions.
pub async fn execute(
    code: &str,
    work_dir: &Path,
    env: Option<std::collections::HashMap<String, String>>,
    timeout_secs: u64,
) -> Result<ExecOutput, ExecError> {
    let cpu_limit = timeout_secs + 5;
    let wrapped_code = format!(
        "ulimit -t {} -v 2097152 -u 50 -f 524288 2>/dev/null; {}",
        cpu_limit, code
    );

    let work_dir = work_dir.to_path_buf();

    let output = tokio::task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new("bash");
        cmd.args(["-c", &wrapped_code])
            .current_dir(&work_dir)
            .env("HOME", &work_dir)
            .env("TMPDIR", &work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Apply Landlock in the child process (after fork, before exec)
        #[cfg(target_os = "linux")]
        {
            let work_dir_clone = work_dir.clone();
            unsafe {
                cmd.pre_exec(move || {
                    crate::landlock::apply(&work_dir_clone, &[]);
                    Ok(())
                });
            }
        }

        if let Some(ref env_vars) = env {
            for (k, v) in env_vars {
                cmd.env(k, v);
            }
        }

        cmd.output().map_err(|e| ExecError::Spawn(e.to_string()))
    });

    let output = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs + 5),
        output,
    )
    .await
    .map_err(|_| ExecError::Timeout)?
    .map_err(|e| ExecError::Spawn(e.to_string()))??;

    Ok(ExecOutput {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        exit_code: output.status.code().unwrap_or(-1),
    })
}

#[derive(Debug)]
pub enum ExecError {
    Timeout,
    Spawn(String),
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::Timeout => write!(f, "执行超时"),
            ExecError::Spawn(s) => write!(f, "进程启动失败: {}", s),
        }
    }
}
