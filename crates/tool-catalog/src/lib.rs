//! fixus-tool-catalog — tool identity single source of truth.
//!
//! This crate is the single source of truth for tool identity (name / input
//! schema / default timeout / executor kind). It is consumed by two otherwise
//! independent binaries:
//!
//! - `tools-bank` (MCP-facing): serializes [`ToolDef`] for `tools/list` and
//!   routes invocations by tool name.
//! - `sandbox-server` (execution): dispatches by [`ExecutorKind`] and renders
//!   argv from a parsed [`ToolSpec`].
//!
//! ## Why a separate crate (pure data)
//!
//! Both `tools-bank` and `sandbox-server` deliberately do **not** depend on the
//! `fixus` lib crate, so they don't link the runtime (storage / service /
//! orchestrator + sqlx etc.). Cargo dependencies are crate-granular — you
//! cannot depend on "just one module" of the lib — so tool identity lives here,
//! in its own crate. This crate MUST stay pure-data: no runtime, HTTP, DB, or
//! async-runtime dependencies. Keeping it dependency-light preserves the
//! "bins can be split into separate repos" property.
//!
//! See `veps/tool-registry-consolidation-design.md` §3 for the data model.

use serde::{Deserialize, Serialize};

/// MCP-facing tool metadata. `tools/list` serializes this directly
/// (field names/order = MCP contract).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: serde_json::Value,
}

/// Full tool spec (single source of truth). Builtins + parsed extras are both
/// this type.
#[derive(Debug, Clone)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    pub default_timeout_secs: u64,
    pub executor: ExecutorKind,
}

#[derive(Debug, Clone)]
pub enum ExecutorKind {
    Bash,
    FileRead,
    FileWrite,
    FileEdit,
    Glob,
    Grep,
    Bin(BinSpec),
}

#[derive(Debug, Clone)]
pub struct BinSpec {
    pub binary: String,
    pub argv: Vec<ArgvPart>,
    pub io: BinIo,
    pub path_args: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinIo {
    Read,
    Write,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgvPart {
    Literal(String),
    Arg(String),
    Flag(String, String),
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("malformed catalog: {0}")]
    Malformed(String),
    #[error("duplicate tool name in catalog: {0}")]
    DuplicateName(String),
    #[error("unknown argv placeholder: {0}")]
    UnknownPlaceholder(String),
}

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("missing required arg: {0}")]
    MissingArg(String),
    #[error("arg type mismatch for: {0}")]
    ArgTypeMismatch(String),
}

// ── builtins catalog (single source of truth) ───────────────────────────

/// The 8 builtin tools: 6 originals (byte-identical to
/// `src/bin/tools-bank/adapter.rs::builtin_tools()` — same name /
/// description / input_schema) plus `fixus_jq` and `fixus_rg`. Only
/// `default_timeout_secs` is new metadata layered on top.
///
/// Returns `&'static [ToolSpec]` via a `OnceLock` so callers get a stable
/// slice without re-allocating on every call.
pub fn builtins() -> &'static [ToolSpec] {
    static CATALOG: std::sync::OnceLock<Vec<ToolSpec>> = std::sync::OnceLock::new();
    CATALOG.get_or_init(|| {
        vec![
            // ── 6 originals: byte-parity with adapter.rs::builtin_tools() ──
            ToolSpec {
                name: "fixus_bash".into(),
                description: "Execute a shell command (via fixus sandbox)".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "command": {"type": "string", "description": "The command to execute"},
                        "description": {"type": "string", "description": "Brief description"}
                    },
                    "required": ["command"]
                }),
                default_timeout_secs: 30,
                executor: ExecutorKind::Bash,
            },
            ToolSpec {
                name: "fixus_read".into(),
                description: "Read a file (via fixus sandbox)".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "file_path": {"type": "string", "description": "Path to file (relative or absolute, scoped to workspace)"},
                        "offset": {"type": "integer", "description": "Line number to start reading from"},
                        "limit": {"type": "integer", "description": "Number of lines to read"}
                    },
                    "required": ["file_path"]
                }),
                default_timeout_secs: 15,
                executor: ExecutorKind::FileRead,
            },
            ToolSpec {
                name: "fixus_write".into(),
                description: "Write to a file (via fixus sandbox)".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "file_path": {"type": "string", "description": "Path to file (relative or absolute, scoped to workspace)"},
                        "content": {"type": "string", "description": "Content to write"}
                    },
                    "required": ["file_path", "content"]
                }),
                default_timeout_secs: 15,
                executor: ExecutorKind::FileWrite,
            },
            ToolSpec {
                name: "fixus_edit".into(),
                description: "Edit a file by replacing a string (via fixus sandbox)".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "file_path": {"type": "string", "description": "Path to file (relative or absolute, scoped to workspace)"},
                        "old_string": {"type": "string", "description": "String to replace"},
                        "new_string": {"type": "string", "description": "Replacement string"}
                    },
                    "required": ["file_path", "old_string", "new_string"]
                }),
                default_timeout_secs: 15,
                executor: ExecutorKind::FileEdit,
            },
            ToolSpec {
                name: "fixus_glob".into(),
                description: "Find files matching a pattern (via fixus sandbox)".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "pattern": {"type": "string", "description": "Glob pattern (e.g. *.rs)"}
                    },
                    "required": ["pattern"]
                }),
                default_timeout_secs: 15,
                executor: ExecutorKind::Glob,
            },
            ToolSpec {
                name: "fixus_grep".into(),
                description: "Search for a pattern in files (via fixus sandbox)".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "pattern": {"type": "string", "description": "Pattern to search for"},
                        "path": {"type": "string", "description": "Directory or file to search in"}
                    },
                    "required": ["pattern"]
                }),
                default_timeout_secs: 15,
                executor: ExecutorKind::Grep,
            },
            // ── jq (new; spec §5.5) ──
            ToolSpec {
                name: "fixus_jq".into(),
                description: "Run jq (JSON query) on a file".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "filter": {"type": "string"},
                        "file": {"type": "string"},
                        "raw": {"type": "boolean"}
                    },
                    "required": ["filter", "file"]
                }),
                default_timeout_secs: 15,
                executor: ExecutorKind::Bin(BinSpec {
                    binary: "jq".into(),
                    argv: vec![
                        ArgvPart::Literal("jq".into()),
                        ArgvPart::Flag("raw".into(), "-r".into()),
                        ArgvPart::Arg("filter".into()),
                        ArgvPart::Arg("file".into()),
                    ],
                    io: BinIo::Read,
                    path_args: vec!["file".into()],
                }),
            },
            // ── rg (new; spec §5.5) ──
            ToolSpec {
                name: "fixus_rg".into(),
                description: "Recursively search file contents (ripgrep)".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "pattern": {"type": "string"},
                        "path": {"type": "string"}
                    },
                    "required": ["pattern"]
                }),
                default_timeout_secs: 15,
                executor: ExecutorKind::Bin(BinSpec {
                    binary: "rg".into(),
                    argv: vec![
                        ArgvPart::Literal("rg".into()),
                        ArgvPart::Arg("pattern".into()),
                        ArgvPart::Arg("path".into()),
                    ],
                    io: BinIo::Read,
                    path_args: vec!["path".into()],
                }),
            },
        ]
    })
}

/// Linear search for a tool by full name. Returns `None` if not found.
pub fn find<'a>(all: &'a [ToolSpec], full_name: &str) -> Option<&'a ToolSpec> {
    all.iter().find(|s| s.name == full_name)
}

/// Convert a full [`ToolSpec`] into the MCP-facing [`ToolDef`] (drops
/// executor / timeout metadata; renames `input_schema` → `inputSchema`
/// via the serde attribute on `ToolDef`).
impl From<&ToolSpec> for ToolDef {
    fn from(s: &ToolSpec) -> Self {
        ToolDef {
            name: s.name.clone(),
            description: s.description.clone(),
            input_schema: s.input_schema.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_has_six_originals_plus_jq_rg() {
        let names: Vec<&str> = builtins().iter().map(|s| s.name.as_str()).collect();
        for orig in [
            "fixus_bash",
            "fixus_read",
            "fixus_write",
            "fixus_edit",
            "fixus_glob",
            "fixus_grep",
        ] {
            assert!(names.contains(&orig), "missing original builtin {}", orig);
        }
        assert!(names.contains(&"fixus_jq"));
        assert!(names.contains(&"fixus_rg"));
    }

    #[test]
    fn builtins_schemas_valid_and_timeouts() {
        for s in builtins() {
            assert_eq!(s.input_schema["type"], "object", "{}", s.name);
            assert!(!s.description.is_empty(), "{}", s.name);
            assert!(s.default_timeout_secs > 0, "{}", s.name);
        }
        assert_eq!(
            find(builtins(), "fixus_bash").unwrap().default_timeout_secs,
            30
        );
        assert_eq!(
            find(builtins(), "fixus_read").unwrap().default_timeout_secs,
            15
        );
        assert_eq!(
            find(builtins(), "fixus_write").unwrap().default_timeout_secs,
            15
        );
        assert_eq!(
            find(builtins(), "fixus_edit").unwrap().default_timeout_secs,
            15
        );
        assert_eq!(
            find(builtins(), "fixus_glob").unwrap().default_timeout_secs,
            15
        );
        assert_eq!(
            find(builtins(), "fixus_grep").unwrap().default_timeout_secs,
            15
        );
    }

    #[test]
    fn builtins_originals_match_adapter_byte_for_byte() {
        // Spot-check the 6 originals' schema shapes match the MCP contract.
        // fixus_bash must require ["command"]; fixus_read must require ["file_path"]; etc.
        let bash = find(builtins(), "fixus_bash").unwrap();
        assert_eq!(bash.input_schema["required"], serde_json::json!(["command"]));
        assert_eq!(bash.input_schema["properties"]["command"]["type"], "string");
        let read = find(builtins(), "fixus_read").unwrap();
        assert_eq!(
            read.input_schema["required"],
            serde_json::json!(["file_path"])
        );
        let write = find(builtins(), "fixus_write").unwrap();
        assert_eq!(
            write.input_schema["required"],
            serde_json::json!(["file_path", "content"])
        );
    }

    #[test]
    fn builtins_jq_and_rg_specs() {
        let jq = find(builtins(), "fixus_jq").unwrap();
        assert!(matches!(jq.executor, ExecutorKind::Bin(_)));
        assert_eq!(jq.default_timeout_secs, 15);
        if let ExecutorKind::Bin(b) = &jq.executor {
            assert_eq!(b.binary, "jq");
            assert!(matches!(b.io, crate::BinIo::Read));
            assert!(b.path_args.contains(&"file".to_string()));
        }
        let rg = find(builtins(), "fixus_rg").unwrap();
        assert!(matches!(rg.executor, ExecutorKind::Bin(_)));
        if let ExecutorKind::Bin(b) = &rg.executor {
            assert_eq!(b.binary, "rg");
            assert!(matches!(b.io, crate::BinIo::Read));
        }
    }

    #[test]
    fn find_returns_none_for_unknown() {
        assert!(find(builtins(), "fixus_nope").is_none());
    }

    #[test]
    fn tooldef_from_spec_renames_input_schema() {
        let s = find(builtins(), "fixus_bash").unwrap();
        let d: ToolDef = s.into();
        let j = serde_json::to_value(&d).unwrap();
        assert!(j.get("inputSchema").is_some(), "must serialize as inputSchema");
        assert!(
            j.get("input_schema").is_none(),
            "must NOT serialize as input_schema"
        );
        assert_eq!(d.name, "fixus_bash");
        assert_eq!(d.description, s.description);
        assert_eq!(d.input_schema, s.input_schema);
    }
}
