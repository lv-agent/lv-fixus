//! EffectivePolicy 镜像(sandbox-server 不依赖 lib crate)。字段与 src/policy.rs serde 兼容。
//!
//! Phase 1 sandbox 只需知道:fs read/write scope + net 是否空(on/off)+ agent_role。
//! egress 规则以 `serde_json::Value` 透传(只看是否空),不解析 host —— per-host 强制留 Phase 2。

use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct EffectivePolicy {
    #[serde(default)]
    pub fs: FsPolicy,
    #[serde(default)]
    pub net: NetPolicy,
    #[serde(default)]
    pub agent_role: AgentRole,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct FsPolicy {
    #[serde(default)]
    pub read_paths: Vec<PathScope>,
    #[serde(default)]
    pub write_paths: Vec<PathScope>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PathScope {
    pub path: PathBuf,
    #[serde(default)]
    pub trust: TrustLevel,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct NetPolicy {
    /// Phase 1:只看是否空(空 = net off / deny-all),不解析 host。
    #[serde(default)]
    pub egress: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    WorkDir,
    Host,
    Remote,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Reader,
    Operator,
}

impl Default for TrustLevel {
    fn default() -> Self {
        TrustLevel::Host
    }
}

impl Default for AgentRole {
    fn default() -> Self {
        AgentRole::Reader // 严默认
    }
}

impl EffectivePolicy {
    /// net 是否关闭(egress 空 = deny-all)。
    pub fn net_off(&self) -> bool {
        self.net.egress.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_effective_policy() {
        let j = r#"{"fs":{"read_paths":[{"path":"/x","trust":"host"}],"write_paths":[]},"net":{"egress":[]},"agent_role":"reader"}"#;
        let p: EffectivePolicy = serde_json::from_str(j).unwrap();
        assert_eq!(p.fs.read_paths.len(), 1);
        assert_eq!(p.fs.read_paths[0].path, PathBuf::from("/x"));
        assert_eq!(p.fs.read_paths[0].trust, TrustLevel::Host);
        assert!(p.net_off(), "empty egress => net off");
        assert_eq!(p.agent_role, AgentRole::Reader);
    }

    #[test]
    fn empty_default_is_strict() {
        // 缺 effective_policy → unwrap_or_default() → 严默认(仅 work_dir, net off, Reader)
        let p: EffectivePolicy = serde_json::from_str("{}").unwrap_or_default();
        assert!(p.fs.read_paths.is_empty());
        assert!(p.fs.write_paths.is_empty());
        assert!(p.net_off(), "空 net = deny-all");
        assert_eq!(p.agent_role, AgentRole::Reader, "严默认 role");
    }
}
