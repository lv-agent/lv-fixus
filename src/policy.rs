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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    WorkDir,
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
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

impl Default for AgentRole {
    fn default() -> Self {
        AgentRole::Reader // 严默认
    }
}

impl Default for TrustLevel {
    fn default() -> Self {
        TrustLevel::Host
    }
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
        let mut push_if_new = |out: &mut Vec<PathScope>, s: PathScope| {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(p: &str) -> PathScope {
        PathScope { path: PathBuf::from(p), trust: TrustLevel::Host }
    }

    fn rule(h: &str) -> EgressRule {
        EgressRule { host: h.into(), ports: vec![], category: None }
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
}
