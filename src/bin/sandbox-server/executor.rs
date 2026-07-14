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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{AgentRole, FsPolicy, PathScope, TrustLevel};
    use std::path::PathBuf;

    /// 构造 net-off(egress 空)、仅授权给定 read scope 的 eff(Operator)。
    /// work_dir 由 execute 隐式 rw,无需在此声明。
    fn strict_eff(read: &[&str]) -> EffectivePolicy {
        EffectivePolicy {
            fs: FsPolicy {
                read_paths: read
                    .iter()
                    .map(|p| PathScope {
                        path: PathBuf::from(p),
                        trust: TrustLevel::Host,
                    })
                    .collect(),
                write_paths: vec![],
            },
            net: Default::default(),
            agent_role: AgentRole::Operator,
        }
    }

    /// 判定一次 execute 是否构成"越权被拒":
    /// - `Ok` 且 (非零 exit 或 stderr/stdout 含 denied/permission) → 进程跑了但被内核拒;
    /// - `Err(Spawn)` → fail-closed 拒 exec(Landlock/seccomp 装载失败时绝不放行);
    /// - `Err(Timeout)` → 不算被拒(可能是真挂起)。
    fn blocked(r: Result<ExecOutput, ExecError>) -> bool {
        match r {
            Ok(o) => {
                let combined = format!("{} {}", o.stdout, o.stderr).to_lowercase();
                o.exit_code != 0
                    || combined.contains("denied")
                    || combined.contains("permission")
            }
            Err(ExecError::Spawn(_)) => true,
            Err(ExecError::Timeout) => false,
        }
    }

    /// E1 回归测:锁定 Phase 1 沙箱 enforcement —— Landlock fs 隔离 + seccomp net-off。
    ///
    /// `#[ignore]`:需真 fork + Landlock/seccomp 内核支持,非默认 CI。手动跑:
    ///   cargo test --bin sandbox-server -- --ignored sandbox_enforcement --nocapture
    ///
    /// 断言:
    /// (a) 读 policy+infra 之外的路径(/etc 不在 infra_ro)→ Landlock 拒(EACCES / 非零 exit);
    /// (b) bash `/dev/tcp` 走 socket(AF_INET) → seccomp 拒(EACCES → "Permission denied")。
    ///
    /// 环境说明:WSL2(Linux 6.x)通常支持 Landlock;若某内核不支持,Landlock apply 会 Err,
    /// executor 的 fail-closed 直接拒 exec(返回 `ExecError::Spawn`)——同样表明 enforcement
    /// 生效(进程未越权运行),`blocked()` 把 Spawn 也计为"被拒"。故本测在不支持 Landlock 的
    /// 内核上仍应通过(以 fail-closed 形式),只有在"放行越权访问"时才失败。
    #[tokio::test]
    #[cfg(target_os = "linux")]
    #[ignore]
    async fn sandbox_enforcement_blocks_outside_policy_and_net() {
        // 显式确保不被 opt-out env 干扰(SANDBOX_ALLOW_NO_LANDLOCK=true 会降级放行)。
        std::env::remove_var("SANDBOX_ALLOW_NO_LANDLOCK");

        let work = tempfile::tempdir().expect("temp work_dir");
        // net off + 仅授权一个无意义 read scope;/etc 与外网均越界。
        let eff = strict_eff(&["/tmp/fixus-sandbox-test-allowed-marker"]);

        // (a) 读 /etc/hostname(/etc 不在 infra_ro)→ Landlock 拒。
        let r_a = execute(
            "cat /etc/hostname",
            work.path(),
            None,
            &eff,
            5,
        )
        .await;
        assert!(
            blocked(r_a),
            "(a) 读 /etc 应被 Landlock 拒绝(EACCES 或 fail-closed Spawn)"
        );

        // (b) /dev/tcp → socket(AF_INET) → seccomp 拒(net off 时套 filter)。
        let r_b = execute(
            "echo test > /dev/tcp/127.0.0.1/1",
            work.path(),
            None,
            &eff,
            5,
        )
        .await;
        assert!(
            blocked(r_b),
            "(b) /dev/tcp 应被 seccomp 拒(socket AF_INET → EACCES 或 fail-closed Spawn)"
        );
    }
}
