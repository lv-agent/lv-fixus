//! CapabilityPolicy — 沙箱边界声明式策略模型 + resolver。
//!
//! fixus = resolver(算 effective = Operator ∩ Tenant ∩ Task + role 收窄)。
//! sandbox-server = enforcer(只认 EffectivePolicy)。spec:`veps/sandbox-boundary-redesign.md`。

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 单个 scope 的策略声明(Operator / Tenant / Task 各一份)。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CapabilityPolicy {
    pub fs: FsPolicy,
    pub net: NetPolicy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct FsPolicy {
    /// 可读根(R/Glob/Grep)。work_dir 隐式在内,resolver 不重复列。
    #[serde(default)]
    pub read_paths: Vec<PathScope>,
    /// 可写根(W/E)。work_dir 隐式在内。
    #[serde(default)]
    pub write_paths: Vec<PathScope>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PathScope {
    pub path: PathBuf,
    pub trust: TrustLevel,
}

/// trust 分层(dim 2):影响审计 + agent_role 门槛。work_dir 隐式 WorkDir。
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    WorkDir,
    #[default] // 严默认:PathScope 缺省 trust
    Host,
    Remote,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct NetPolicy {
    /// 出口规则;空 = deny-all(严默认)。
    #[serde(default)]
    pub egress: Vec<EgressRule>,
}

/// Phase 1:host 为字符串(domain/glob/CIDR),per-host 强制留 Phase 2(建模占位)。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EgressRule {
    pub host: String,
    #[serde(default)]
    pub ports: Vec<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<NetCategory>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NetCategory {
    PackageManager,
    HttpsApi,
    Dns,
    PrivateLan,
}

/// agent 信任级(dim 5)。task 携带,在 effective 内再收窄。
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    #[default] // 严默认
    Reader,
    Operator,
}

/// resolver 输出:随 broker event 透传给 sandbox 的有效策略(已交集 + role 收窄)。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EffectivePolicy {
    pub fs: FsPolicy,
    pub net: NetPolicy,
    pub agent_role: AgentRole,
}

/// 路径规范化(不要求存在):处理 `.`、`..`、多余分隔符。
pub fn normalize_path(path: &std::path::Path) -> PathBuf {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                parts.pop();
            }
            other => {
                parts.push(other);
            }
        }
    }
    parts.iter().collect()
}

/// `p` 是否落在 `scope` 的递归 beneath 内(按路径段前缀匹配)。
/// 双方先 normalize,再用 `starts_with`(Path starts_with 按段匹配,非字符串前缀)。
pub fn path_within(p: &std::path::Path, scope: &PathScope) -> bool {
    let norm_p = normalize_path(p);
    let norm_scope = normalize_path(&scope.path);
    norm_p == norm_scope || norm_p.starts_with(&norm_scope)
}

/// 两 allowlist 的交集:取每个重叠对里更窄(more-specific)的 scope。
/// 路径段前缀语义下,两 scope 要么包含要么不相交,故交集 = 重叠对中的更深者。
pub fn intersect_fs(a: &FsPolicy, b: &FsPolicy) -> FsPolicy {
    fn intersect_set(a: &[PathScope], b: &[PathScope]) -> Vec<PathScope> {
        let mut out: Vec<PathScope> = Vec::new();
        let push_if_new = |out: &mut Vec<PathScope>, s: PathScope| {
            if !out.iter().any(|e| e.path == s.path) {
                out.push(s);
            }
        };
        for sa in a {
            if let Some(sb) = b.iter().find(|sb| path_within(&sa.path, sb) || path_within(&sb.path, sa)) {
                let tighter = if path_within(&sa.path, sb) { sa.clone() } else { sb.clone() };
                push_if_new(&mut out, tighter);
            }
        }
        out
    }
    FsPolicy {
        read_paths: intersect_set(&a.read_paths, &b.read_paths),
        write_paths: intersect_set(&a.write_paths, &b.write_paths),
    }
}

/// Phase 1:精确匹配(host+ports+category 全等)交集。
/// host 重叠/glob 合并留 Phase 2(net 本就只开关级强制)。
pub fn intersect_net(a: &NetPolicy, b: &NetPolicy) -> NetPolicy {
    let egress = a
        .egress
        .iter()
        .filter(|ra| b.egress.iter().any(|rb| rb == *ra))
        .cloned()
        .collect();
    NetPolicy { egress }
}

/// agent_role 再收窄:Reader → 砍 net + Host-trust write(保 WorkDir write + 所有 read)。
pub fn role_narrow(mut eff: EffectivePolicy, role: AgentRole) -> EffectivePolicy {
    eff.agent_role = role;
    match role {
        AgentRole::Operator => {}
        AgentRole::Reader => {
            eff.net.egress.clear();
            eff.fs.write_paths.retain(|s| s.trust == TrustLevel::WorkDir);
        }
    }
    eff
}

/// effective = Operator ∩ Tenant ∩ Task(fs + net),再 role 收窄。
/// 永远安全(只收窄,不放权)。越权检测在信任边界由 `validate_subset` 显式做。
pub fn resolve_effective(
    operator: &CapabilityPolicy,
    tenant: &CapabilityPolicy,
    task: &CapabilityPolicy,
    role: AgentRole,
) -> EffectivePolicy {
    let fs = intersect_fs(&operator.fs, &tenant.fs);
    let fs = intersect_fs(&fs, &task.fs);
    let net = intersect_net(&operator.net, &tenant.net);
    let net = intersect_net(&net, &task.net);
    role_narrow(
        EffectivePolicy { fs, net, agent_role: role },
        role,
    )
}

/// 越权检测结果:列出 child 中不被 parent 覆盖的 scope / egress host。
#[derive(Debug, Clone, PartialEq)]
pub struct SubsetViolation {
    pub fs_read_violations: Vec<PathBuf>,
    pub fs_write_violations: Vec<PathBuf>,
    pub net_violations: Vec<String>,
}

impl SubsetViolation {
    pub fn is_empty(&self) -> bool {
        self.fs_read_violations.is_empty()
            && self.fs_write_violations.is_empty()
            && self.net_violations.is_empty()
    }
}

/// 校验 `child ⊆ parent`(fs read/write 路径段前缀 + net host 精确)。
/// 用于信任边界:task⊆tenant(创建时)、tenant⊆operator(API 设置时)。
/// 越权 → Err(列出违规项),由调用方硬失败(400)。
pub fn validate_subset(child: &CapabilityPolicy, parent: &CapabilityPolicy) -> Result<(), SubsetViolation> {
    let check_paths = |child_scopes: &[PathScope], parent_scopes: &[PathScope]| -> Vec<PathBuf> {
        child_scopes
            .iter()
            .filter(|c| !parent_scopes.iter().any(|p| path_within(&c.path, p)))
            .map(|c| normalize_path(&c.path))
            .collect()
    };
    let v = SubsetViolation {
        fs_read_violations: check_paths(&child.fs.read_paths, &parent.fs.read_paths),
        fs_write_violations: check_paths(&child.fs.write_paths, &parent.fs.write_paths),
        net_violations: child
            .net
            .egress
            .iter()
            .filter(|cr| !parent.net.egress.iter().any(|pr| pr == *cr))
            .map(|r| r.host.clone())
            .collect(),
    };
    if v.is_empty() { Ok(()) } else { Err(v) }
}

/// 解析 Operator policy TOML(部署期 fixus host 文件)。
/// 空/缺字段 → 默认(严:仅 work_dir,无外网)。非法 TOML → Err(fixus 启动 fail-closed)。
pub fn parse_operator_toml(s: &str) -> Result<CapabilityPolicy, String> {
    if s.trim().is_empty() {
        return Ok(CapabilityPolicy::default());
    }
    let policy: CapabilityPolicy = toml::from_str(s).map_err(|e| format!("operator policy TOML: {}", e))?;
    let norm = |scopes: Vec<PathScope>| -> Vec<PathScope> {
        scopes.into_iter().map(|mut s| { s.path = normalize_path(&s.path); s }).collect()
    };
    Ok(CapabilityPolicy {
        fs: FsPolicy { read_paths: norm(policy.fs.read_paths), write_paths: norm(policy.fs.write_paths) },
        net: policy.net,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(p: &str) -> PathScope {
        PathScope { path: PathBuf::from(p), trust: TrustLevel::Host }
    }

    fn rule(h: &str) -> EgressRule {
        EgressRule { host: h.into(), ports: vec![], category: None }
    }

    fn policy(read: &[&str], write: &[&str], egress: &[&str]) -> CapabilityPolicy {
        CapabilityPolicy {
            fs: FsPolicy {
                read_paths: read.iter().map(|p| scope(p)).collect(),
                write_paths: write.iter().map(|p| scope(p)).collect(),
            },
            net: NetPolicy { egress: egress.iter().map(|h| rule(h)).collect() },
        }
    }

    #[test]
    fn capability_policy_serde_roundtrip() {
        let p = CapabilityPolicy {
            fs: FsPolicy {
                read_paths: vec![PathScope { path: "/home/x/proj".into(), trust: TrustLevel::Host }],
                write_paths: vec![],
            },
            net: NetPolicy { egress: vec![EgressRule { host: "pypi.org".into(), ports: vec![443], category: Some(NetCategory::PackageManager) }] },
        };
        let j = serde_json::to_string(&p).unwrap();
        let back: CapabilityPolicy = serde_json::from_str(&j).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn empty_policy_default_is_strict() {
        let p = CapabilityPolicy::default();
        assert!(p.fs.read_paths.is_empty());
        assert!(p.fs.write_paths.is_empty());
        assert!(p.net.egress.is_empty(), "空 net = deny-all");
    }

    #[test]
    fn agent_role_default_is_reader() {
        assert_eq!(AgentRole::default(), AgentRole::Reader);
    }

    #[test]
    fn normalize_handles_dotdot() {
        assert_eq!(normalize_path(std::path::Path::new("/a/b/../c")), PathBuf::from("/a/c"));
    }

    #[test]
    fn path_within_prefix_recursive() {
        let scope = PathScope { path: PathBuf::from("/home/x/proj"), trust: TrustLevel::Host };
        assert!(path_within(std::path::Path::new("/home/x/proj"), &scope));
        assert!(path_within(std::path::Path::new("/home/x/proj/sub/f.go"), &scope));
        assert!(!path_within(std::path::Path::new("/home/x/other"), &scope));
        assert!(!path_within(std::path::Path::new("/home/x/proj-evil"), &scope), "前缀必须按路径段,不是字符串前缀");
    }

    #[test]
    fn path_within_normalizes() {
        let scope = PathScope { path: PathBuf::from("/a/b"), trust: TrustLevel::Host };
        assert!(path_within(std::path::Path::new("/a/b/../b/c"), &scope));
    }

    #[test]
    fn intersect_fs_takes_tighter_overlap() {
        let a = FsPolicy { read_paths: vec![scope("/home/x")], write_paths: vec![] };
        let b = FsPolicy { read_paths: vec![scope("/home/x/proj")], write_paths: vec![] };
        let r = intersect_fs(&a, &b);
        assert_eq!(r.read_paths, vec![scope("/home/x/proj")]);
    }

    #[test]
    fn intersect_fs_disjoint_yields_empty() {
        let a = FsPolicy { read_paths: vec![scope("/a")], write_paths: vec![] };
        let b = FsPolicy { read_paths: vec![scope("/b")], write_paths: vec![] };
        assert!(intersect_fs(&a, &b).read_paths.is_empty());
    }

    #[test]
    fn intersect_fs_write_paths_intersected_too() {
        let a = FsPolicy { read_paths: vec![], write_paths: vec![scope("/repo")] };
        let b = FsPolicy { read_paths: vec![], write_paths: vec![scope("/repo/sub")] };
        let r = intersect_fs(&a, &b);
        assert_eq!(r.write_paths, vec![scope("/repo/sub")]);
    }

    #[test]
    fn intersect_fs_dedups_identical() {
        let a = FsPolicy { read_paths: vec![scope("/x")], write_paths: vec![] };
        let b = FsPolicy { read_paths: vec![scope("/x")], write_paths: vec![] };
        assert_eq!(intersect_fs(&a, &b).read_paths.len(), 1);
    }

    #[test]
    fn intersect_net_keeps_rules_in_both() {
        let a = NetPolicy { egress: vec![rule("pypi.org"), rule("github.com")] };
        let b = NetPolicy { egress: vec![rule("github.com"), rule("npm.org")] };
        let r = intersect_net(&a, &b);
        assert_eq!(r.egress, vec![rule("github.com")]);
    }

    #[test]
    fn intersect_net_empty_if_either_empty() {
        let a = NetPolicy { egress: vec![rule("x")] };
        assert!(intersect_net(&a, &NetPolicy::default()).egress.is_empty());
    }

    #[test]
    fn role_narrow_reader_kills_net_and_host_write() {
        let mut eff = EffectivePolicy {
            fs: FsPolicy {
                read_paths: vec![scope("/host/read")],
                write_paths: vec![
                    PathScope { path: "/host/write".into(), trust: TrustLevel::Host },
                    PathScope { path: "/tmp/wd".into(), trust: TrustLevel::WorkDir },
                ],
            },
            net: NetPolicy { egress: vec![rule("pypi.org")] },
            agent_role: AgentRole::Operator,
        };
        eff = role_narrow(eff, AgentRole::Reader);
        assert!(eff.net.egress.is_empty(), "Reader 无外网");
        assert_eq!(eff.fs.write_paths.len(), 1);
        assert_eq!(eff.fs.write_paths[0].trust, TrustLevel::WorkDir);
        assert_eq!(eff.fs.read_paths.len(), 1);
    }

    #[test]
    fn role_narrow_operator_keeps_all() {
        let eff = EffectivePolicy {
            fs: FsPolicy { read_paths: vec![scope("/h")], write_paths: vec![scope("/h")] },
            net: NetPolicy { egress: vec![rule("x")] },
            agent_role: AgentRole::Operator,
        };
        let r = role_narrow(eff.clone(), AgentRole::Operator);
        assert_eq!(r, eff);
    }

    #[test]
    fn resolve_effective_intersects_three_scopes() {
        let op = policy(&["/home"], &[], &[]);
        let tenant = policy(&["/home/x/proj"], &[], &[]);
        let task = policy(&["/home/x/proj/src"], &[], &[]);
        let eff = resolve_effective(&op, &tenant, &task, AgentRole::Operator);
        assert_eq!(eff.fs.read_paths, vec![scope("/home/x/proj/src")]);
        assert!(eff.net.egress.is_empty());
    }

    #[test]
    fn resolve_effective_empty_all_yields_strict() {
        let eff = resolve_effective(&CapabilityPolicy::default(), &CapabilityPolicy::default(), &CapabilityPolicy::default(), AgentRole::Operator);
        assert!(eff.fs.read_paths.is_empty());
        assert!(eff.fs.write_paths.is_empty());
        assert!(eff.net.egress.is_empty(), "空 = deny-all");
    }

    #[test]
    fn resolve_effective_applies_role() {
        let op = policy(&[], &["/host"], &["pypi.org"]);
        let eff = resolve_effective(&op, &CapabilityPolicy::default(), &CapabilityPolicy::default(), AgentRole::Reader);
        assert!(eff.net.egress.is_empty());
        assert!(eff.fs.write_paths.is_empty());
    }

    #[test]
    fn validate_subset_child_within_parent_ok() {
        let parent = policy(&["/home/x"], &[], &[]);
        let child = policy(&["/home/x/proj"], &[], &[]);
        assert!(validate_subset(&child, &parent).is_ok());
    }

    #[test]
    fn validate_subset_child_exceeds_parent_err() {
        let parent = policy(&["/home/x/proj"], &[], &[]);
        let child = policy(&["/home/x/other"], &[], &[]); // ⊄ parent
        let err = validate_subset(&child, &parent).unwrap_err();
        assert!(err.fs_read_violations.iter().any(|p| p == &PathBuf::from("/home/x/other")));
    }

    #[test]
    fn validate_subset_net_rule_not_in_parent_err() {
        let parent = policy(&[], &[], &["pypi.org"]);
        let child = policy(&[], &[], &["pypi.org", "evil.com"]);
        let err = validate_subset(&child, &parent).unwrap_err();
        assert_eq!(err.net_violations, vec!["evil.com".to_string()]);
    }

    #[test]
    fn parse_operator_toml_basic() {
        let toml = r#"
[fs]
[[fs.read_paths]]
path = "/home/lvtao/codeagent"
trust = "host"

[net]
egress = []
"#;
        let p = parse_operator_toml(toml).unwrap();
        assert_eq!(p.fs.read_paths[0].path, PathBuf::from("/home/lvtao/codeagent"));
        assert_eq!(p.fs.read_paths[0].trust, TrustLevel::Host);
        assert!(p.net.egress.is_empty());
    }

    #[test]
    fn parse_operator_toml_empty_is_strict() {
        let p = parse_operator_toml("").unwrap();
        assert!(p.fs.read_paths.is_empty());
        assert!(p.net.egress.is_empty());
    }

    #[test]
    fn parse_operator_toml_malformed_err() {
        assert!(parse_operator_toml("not = valid = toml = {{{").is_err());
    }
}
