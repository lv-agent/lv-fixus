#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Stdio;

use crate::policy::EffectivePolicy;

#[derive(Debug)]
pub struct ExecOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

/// Execute bash code in the given working directory with resource limits,
/// policy-driven Landlock fs isolation, and (D4) seccomp net-off.
///
/// `eff` drives the Landlock ruleset (work_dir rw + eff.fs read/write + infra ro).
/// **fail-closed**:Landlock/seccomp setup failure aborts the child (returns
/// `ExecError::Spawn`) unless `SANDBOX_ALLOW_NO_LANDLOCK=true`(opt-in degraded)。
pub async fn execute(
    code: &str,
    work_dir: &Path,
    env: Option<std::collections::HashMap<String, String>>,
    eff: &EffectivePolicy,
    timeout_secs: u64,
) -> Result<ExecOutput, ExecError> {
    let cpu_limit = timeout_secs + 5;
    let wrapped_code = format!(
        "ulimit -t {} -v 2097152 -u 50 -f 524288 2>/dev/null; {}",
        cpu_limit, code
    );

    let work_dir = work_dir.to_path_buf();
    let eff = eff.clone();

    let output = tokio::task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new("bash");
        cmd.args(["-c", &wrapped_code])
            .current_dir(&work_dir)
            .env("HOME", &work_dir)
            .env("TMPDIR", &work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Apply Landlock + seccomp in the child (after fork, before exec).
        // pre_exec is unix-only; on non-Linux, sandbox_core::apply returns Err
        // → fail-closed (refuse to exec unsandboxed) unless SANDBOX_ALLOW_NO_LANDLOCK=true.
        #[cfg(unix)]
        {
            let work_dir_clone = work_dir.clone();
            let eff_clone = eff.clone();
            unsafe {
                cmd.pre_exec(move || {
                    // ── Landlock(policy-driven)── fail-closed ──
                    if let Err(e) = crate::sandbox_core::apply(&work_dir_clone, &eff_clone) {
                        if std::env::var("SANDBOX_ALLOW_NO_LANDLOCK").as_deref() == Ok("true") {
                            tracing::warn!("landlock disabled (opt-in degraded): {}", e);
                        } else {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("sandbox enforce failed (fail-closed): {}", e),
                            ));
                        }
                    }
                    // ── seccomp net-off(linux only)── fail-closed ──
                    // net_off(egress 空)→ 拒 socket(AF_INET/AF_INET6)。filter apply 失败 → 拒 exec
                    // (绝不放行一个本应 net-off 却未强制的进程)。
                    #[cfg(target_os = "linux")]
                    {
                        if crate::sandbox_core::should_block_net(&eff_clone) {
                            if let Err(e) = crate::sandbox_core::apply_net_filter_block() {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("seccomp net filter failed (fail-closed): {}", e),
                                ));
                            }
                        }
                    }
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
