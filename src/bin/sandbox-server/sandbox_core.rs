//! Policy-driven Landlock sandbox for sandbox-server bash execution.
//!
//! 规则集来自 EffectivePolicy(work_dir rw + read_ro + write_rw + infra_ro;default deny)。
//! Phase 1 fail-closed:Ruleset 创建/加规则/限制任一失败 → 返回 Err(调用方拒 exec,绝不静默降级)。
//! 非Linux / 老内核(Landlock 不可用):apply 返回 Err → 经调用方 fail-closed 默认拒,
//! 仅当显式设置 `SANDBOX_ALLOW_NO_LANDLOCK=true` 时降级为"仅应用层"(在 executor 里判定)。
//!
//! 必须在 `pre_exec`(fork 后、exec 前)上下文调用。

use std::path::{Path, PathBuf};

use crate::policy::EffectivePolicy;

/// 生成的 Landlock 规则描述(纯数据,全平台可测;`apply_impl` 据此建 PathBeneath)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LandlockRules {
    /// 工作目录:rw(及子树)。
    pub work_dir: PathBuf,
    /// `effective.fs.read_paths`(Host)→ ro。
    pub read_ro: Vec<PathBuf>,
    /// `effective.fs.write_paths`(Host/WorkDir)→ rw。
    pub write_rw: Vec<PathBuf>,
    /// 固定基础设施:binary/exec 所需 `/usr /bin /lib /lib64`(`/etc` 不再 blanket ro)。
    pub infra_ro: Vec<PathBuf>,
}

/// 从 effective policy + work_dir 生成 Landlock 规则描述(纯函数,全平台可测)。
/// 路径经 `tools::normalize_path` 规范化;去重(保留首次出现)。
pub fn build_landlock_rules(work_dir: &Path, eff: &EffectivePolicy) -> LandlockRules {
    let mut read_ro: Vec<PathBuf> = Vec::new();
    for s in &eff.fs.read_paths {
        let p = crate::tools::normalize_path(&s.path);
        if !read_ro.contains(&p) {
            read_ro.push(p);
        }
    }
    let mut write_rw: Vec<PathBuf> = Vec::new();
    for s in &eff.fs.write_paths {
        let p = crate::tools::normalize_path(&s.path);
        if !write_rw.contains(&p) {
            write_rw.push(p);
        }
    }
    LandlockRules {
        work_dir: crate::tools::normalize_path(work_dir),
        read_ro,
        write_rw,
        infra_ro: vec![
            PathBuf::from("/usr"),
            PathBuf::from("/bin"),
            PathBuf::from("/lib"),
            PathBuf::from("/lib64"),
        ],
    }
}

/// Apply Landlock from effective policy.
///
/// Phase 1 fail-closed:任何失败 → `Err`(调用方拒 exec,绝不静默放行)。
pub fn apply(work_dir: &Path, eff: &EffectivePolicy) -> Result<(), String> {
    apply_impl(work_dir, eff)
}

#[cfg(target_os = "linux")]
fn apply_impl(work_dir: &Path, eff: &EffectivePolicy) -> Result<(), String> {
    use landlock::{
        make_bitflags, AccessFs, BitFlags, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset,
        RulesetAttr, RulesetCreatedAttr,
    };

    let rules = build_landlock_rules(work_dir, eff);

    let access_rw = make_bitflags!(AccessFs::{
        Execute | ReadFile | ReadDir | WriteFile
            | RemoveDir | RemoveFile | MakeDir | MakeReg | MakeSock | MakeFifo | MakeSym | Truncate
    });
    let access_ro = make_bitflags!(AccessFs::{Execute | ReadFile | ReadDir});
    // /dev/null /dev/zero /dev/random /dev/urandom:最小 rw(无 ReadDir,不暴露设备树)。
    let access_dev = make_bitflags!(AccessFs::{ReadFile | WriteFile});

    // 单路径入栈:打不开(不存在 / 无权限)→ 跳过该路径(某些 infra 路径在容器里可能缺失,
    // 跳过单条不整体失败)。整体 fail-closed 由下面 Ruleset 链保证。
    fn push_path(
        v: &mut Vec<PathBeneath<PathFd>>,
        p: &Path,
        flags: BitFlags<AccessFs>,
    ) {
        match PathFd::new(p) {
            Ok(fd) => v.push(PathBeneath::new(fd, flags)),
            Err(e) => tracing::warn!(
                "landlock: skip path {} ({}); not enforced for it",
                p.display(),
                e
            ),
        }
    }

    let mut path_rules: Vec<PathBeneath<PathFd>> = Vec::new();
    push_path(&mut path_rules, &rules.work_dir, access_rw);
    for p in &rules.read_ro {
        push_path(&mut path_rules, p, access_ro);
    }
    for p in &rules.write_rw {
        push_path(&mut path_rules, p, access_rw);
    }
    for p in &rules.infra_ro {
        push_path(&mut path_rules, p, access_ro);
    }
    // /dev 收紧:仅 null/zero/random/urandom(不再全 /dev rw)。
    for d in ["/dev/null", "/dev/zero", "/dev/random", "/dev/urandom"] {
        push_path(&mut path_rules, Path::new(d), access_dev);
    }

    let path_rules: Vec<Result<PathBeneath<PathFd>, landlock::RulesetError>> =
        path_rules.into_iter().map(Ok).collect();

    // fail-closed:BestEffort(老 kernel 降级能力集),但 create/add_rules/restrict_self
    // 任一失败 → Err(不静默 warn-and-skip)。
    Ruleset::default()
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(access_rw | access_ro | access_dev)
        .and_then(|r| r.create())
        .and_then(|r| r.add_rules(path_rules))
        .and_then(|r| r.restrict_self())
        .map_err(|e| format!("landlock enforce failed: {}", e))?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn apply_impl(_work_dir: &Path, _eff: &EffectivePolicy) -> Result<(), String> {
    Err("landlock unavailable on non-linux".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{AgentRole, FsPolicy, PathScope, TrustLevel};

    fn eff(read: &[&str], write: &[&str]) -> EffectivePolicy {
        EffectivePolicy {
            fs: FsPolicy {
                read_paths: read
                    .iter()
                    .map(|p| PathScope {
                        path: PathBuf::from(p),
                        trust: TrustLevel::Host,
                    })
                    .collect(),
                write_paths: write
                    .iter()
                    .map(|p| PathScope {
                        path: PathBuf::from(p),
                        trust: TrustLevel::Host,
                    })
                    .collect(),
            },
            net: Default::default(),
            agent_role: AgentRole::Operator,
        }
    }

    #[test]
    fn build_rules_from_policy() {
        let e = eff(&["/home/x/proj"], &["/var/out"]);
        let r = build_landlock_rules(Path::new("/tmp/wd"), &e);
        // read paths → read_ro(normalized)
        assert_eq!(r.read_ro, vec![PathBuf::from("/home/x/proj")]);
        // write paths → write_rw
        assert_eq!(r.write_rw, vec![PathBuf::from("/var/out")]);
        // work_dir normalized
        assert_eq!(r.work_dir, PathBuf::from("/tmp/wd"));
        // infra:/usr /bin /lib /lib64
        assert!(r.infra_ro.contains(&PathBuf::from("/usr")));
        assert!(r.infra_ro.contains(&PathBuf::from("/lib64")));
        // /etc NOT in infra(收紧——旧版 blanket ro 已移除)
        assert!(!r.infra_ro.contains(&PathBuf::from("/etc")));
    }

    #[test]
    fn build_rules_normalizes_paths() {
        let e = eff(&["/home/x/proj/../proj/./sub"], &[]);
        let r = build_landlock_rules(Path::new("/tmp/wd/.."), &e);
        assert_eq!(r.read_ro, vec![PathBuf::from("/home/x/proj/sub")]);
        assert_eq!(r.work_dir, PathBuf::from("/tmp"));
    }

    #[test]
    fn build_rules_dedups() {
        let e = eff(&["/a", "/a", "/b"], &[]);
        let r = build_landlock_rules(Path::new("/wd"), &e);
        assert_eq!(r.read_ro, vec![PathBuf::from("/a"), PathBuf::from("/b")]);
    }

    #[test]
    fn build_rules_empty_policy_still_has_infra() {
        let e = EffectivePolicy::default();
        let r = build_landlock_rules(Path::new("/wd"), &e);
        assert!(r.read_ro.is_empty());
        assert!(r.write_rw.is_empty());
        assert!(
            !r.infra_ro.is_empty(),
            "infra 始终在(二进制执行所需)"
        );
    }
}
