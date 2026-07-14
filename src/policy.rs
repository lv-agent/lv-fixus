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

#[cfg(test)]
mod tests {
    use super::*;

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
}
