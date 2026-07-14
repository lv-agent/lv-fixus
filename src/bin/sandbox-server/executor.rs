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

/// rlimit 元组(纯函数,linux-only;逐字节 parity 旧 bash `ulimit`)。
///
/// | 资源 | 旧 bash ulimit | 本函数 setrlimit 值 |
/// |------|---------------|-------------------|
/// | CPU 秒 | `-t <timeout+5>` | `timeout_secs + 5`(RLIMIT_CPU) |
/// | 虚拟内存 | `-v 2097152`(KiB) | `2_097_152 * 1024` bytes = 2 GiB(RLIMIT_AS) |
/// | 进程数 | `-u 50` | `50`(RLIMIT_NPROC) |
/// | 文件大小 | `-f 524288`(512B 块) | `524_288 * 512` bytes = 256 MiB(RLIMIT_FSIZE) |
#[cfg(target_os = "linux")]
pub(crate) fn rlimit_tuple(timeout_secs: u64) -> (u64, u64, u64, u64) {
    (timeout_secs + 5, 2_097_152 * 1024, 50, 524_288 * 512)
}

/// 在 `pre_exec`(fork 后、exec 前)上下文装 rlimit。
///
/// soft == hard(与旧 `ulimit` 行为一致:ulimit 同时设两者)。
/// **fail-closed**:任一 `setrlimit` 失败 → `Err`(调用方拒 exec,绝不放行一个无 rlimit 的进程;
/// 无 opt-out —— rlimit 无法被"安全降级")。
#[cfg(target_os = "linux")]
fn apply_rlimits(timeout_secs: u64) -> Result<(), String> {
    use libc::{rlimit, setrlimit, RLIMIT_AS, RLIMIT_CPU, RLIMIT_FSIZE, RLIMIT_NPROC};
    let (cpu, as_bytes, nproc, fsize) = rlimit_tuple(timeout_secs);

    // 用 4 个显式 unsafe 块(closure 借用 setrlimit/常量无必要,显式更清晰且零歧义)。
    unsafe {
        let rl = rlimit {
            rlim_cur: cpu,
            rlim_max: cpu,
        };
        if setrlimit(RLIMIT_CPU, &rl) != 0 {
            return Err(format!(
                "setrlimit RLIMIT_CPU failed: {}",
                std::io::Error::last_os_error()
            ));
        }
    }
    unsafe {
        let rl = rlimit {
            rlim_cur: as_bytes,
            rlim_max: as_bytes,
        };
        if setrlimit(RLIMIT_AS, &rl) != 0 {
            return Err(format!(
                "setrlimit RLIMIT_AS failed: {}",
                std::io::Error::last_os_error()
            ));
        }
    }
    unsafe {
        let rl = rlimit {
            rlim_cur: nproc,
            rlim_max: nproc,
        };
        if setrlimit(RLIMIT_NPROC, &rl) != 0 {
            return Err(format!(
                "setrlimit RLIMIT_NPROC failed: {}",
                std::io::Error::last_os_error()
            ));
        }
    }
    unsafe {
        let rl = rlimit {
            rlim_cur: fsize,
            rlim_max: fsize,
        };
        if setrlimit(RLIMIT_FSIZE, &rl) != 0 {
            return Err(format!(
                "setrlimit RLIMIT_FSIZE failed: {}",
                std::io::Error::last_os_error()
            ));
        }
    }
    Ok(())
}

/// 核心:spawn 任意 argv,在 `pre_exec` 装 rlimit + Landlock + seccomp。
///
/// 泛化自旧 bash-only `execute`:外部二进制工具(jq/rg,T7)复用同一硬隔离。
/// rlimit 机制已从 bash `ulimit` 迁移到 `pre_exec` 的 `libc::setrlimit`(对任意 spawned 进程生效)。
///
/// `eff` 驱动 Landlock 规则集(work_dir rw + eff.fs read/write + infra ro)。
/// **fail-closed**:setrlimit/Landlock/seccomp setup 失败中止子进程(返回 `ExecError::Spawn`),
/// 唯一例外是 Landlock 在 `SANDBOX_ALLOW_NO_LANDLOCK=true` 时 opt-in 降级(setrlimit/seccomp 无 opt-out)。
pub async fn execute_argv(
    argv: &[String],
    work_dir: &Path,
    env: Option<std::collections::HashMap<String, String>>,
    eff: &EffectivePolicy,
    timeout_secs: u64,
) -> Result<ExecOutput, ExecError> {
    let work_dir = work_dir.to_path_buf();
    let eff = eff.clone();
    let argv: Vec<String> = argv.to_vec();

    let output = tokio::task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .current_dir(&work_dir)
            .env("HOME", &work_dir)
            .env("TMPDIR", &work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // 在子进程(fork 后、exec 前)装 rlimit + Landlock + seccomp。
        // pre_exec 是 unix-only;非-Linux 上 sandbox_core::apply 返回 Err
        // → fail-closed(拒 exec 无沙箱进程),除非 SANDBOX_ALLOW_NO_LANDLOCK=true。
        #[cfg(unix)]
        {
            let work_dir_clone = work_dir.clone();
            let eff_clone = eff.clone();
            unsafe {
                cmd.pre_exec(move || {
                    // ── 1. setrlimit(linux only)── fail-closed,无 opt-out ──
                    #[cfg(target_os = "linux")]
                    {
                        if let Err(e) = apply_rlimits(timeout_secs) {
                            return Err(std::io::Error::other(format!(
                                "rlimit failed (fail-closed): {}",
                                e
                            )));
                        }
                    }
                    // ── 2. Landlock(policy-driven)── fail-closed ──
                    if let Err(e) = crate::sandbox_core::apply(&work_dir_clone, &eff_clone) {
                        if std::env::var("SANDBOX_ALLOW_NO_LANDLOCK").as_deref() == Ok("true") {
                            tracing::warn!("landlock disabled (opt-in degraded): {}", e);
                        } else {
                            return Err(std::io::Error::other(format!(
                                "sandbox enforce failed (fail-closed): {}",
                                e
                            )));
                        }
                    }
                    // ── 3. seccomp net-off(linux only)── fail-closed ──
                    // net_off(egress 空)→ 拒 socket(AF_INET/AF_INET6)。filter apply 失败 → 拒 exec
                    // (绝不放行一个本应 net-off 却未强制的进程)。
                    #[cfg(target_os = "linux")]
                    {
                        if crate::sandbox_core::should_block_net(&eff_clone) {
                            if let Err(e) = crate::sandbox_core::apply_net_filter_block() {
                                return Err(std::io::Error::other(format!(
                                    "seccomp net filter failed (fail-closed): {}",
                                    e
                                )));
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

/// 执行 bash 代码的薄包装:`bash -c <code>` → `execute_argv`。
///
/// **rlimit 机制迁移**:`code` 原样透传(不再注入 `ulimit -t ... -v ... -u ... -f ...` 前缀);
/// rlimit 现由 `execute_argv` 的 `pre_exec` 经 `libc::setrlimit` 装载(数值逐字节 parity 旧 ulimit)。
/// Landlock + seccomp net-off 语义不变(Phase 1)。
pub async fn execute_bash(
    code: &str,
    work_dir: &Path,
    env: Option<std::collections::HashMap<String, String>>,
    eff: &EffectivePolicy,
    timeout_secs: u64,
) -> Result<ExecOutput, ExecError> {
    let argv = vec!["bash".to_string(), "-c".to_string(), code.to_string()];
    execute_argv(&argv, work_dir, env, eff, timeout_secs).await
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

    /// rlimit 元组 parity:锁定旧 bash `ulimit` → 新 `setrlimit` 数值精确转换。
    /// (KiB→bytes for AS;512B-blocks→bytes for FSIZE)。
    #[cfg(target_os = "linux")]
    #[test]
    fn rlimit_values_match_bash_ulimit_parity() {
        let (cpu, asb, nproc, fsize) = rlimit_tuple(10);
        assert_eq!(cpu, 15); // timeout+5
        assert_eq!(asb, 2_097_152 * 1024); // -v 2097152 KiB → bytes (2 GiB)
        assert_eq!(nproc, 50); // -u 50
        assert_eq!(fsize, 524_288 * 512); // -f 524288 × 512B-blocks → bytes (256 MiB)
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
        let r_a = execute_bash(
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
        let r_b = execute_bash(
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
