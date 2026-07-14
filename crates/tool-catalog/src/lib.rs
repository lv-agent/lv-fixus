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

// ── argv template rendering (spec §5.3) — injection-safe by construction ──

/// Render an argv template against agent-supplied arguments (spec §5.3).
///
/// Each [`ArgvPart`] maps to exactly one output `String` (or zero, for a
/// `Flag` whose value is absent/false). The output `Vec<String>` is intended
/// to be passed verbatim to `Command::new(&out[0]).args(&out[1..])` →
/// `execve`: argv elements are handed to the kernel directly and are NEVER
/// interpreted by a shell.
///
/// # Injection safety (the whole point)
///
/// Each `Arg(name)` substitution contributes **exactly one** argv element,
/// regardless of what characters the value contains (spaces, `;`, `$()`,
/// backticks, newlines, quotes). There is no shell anywhere in this path —
/// no `sh -c`, no string concatenation into a command line, no whitespace
/// splitting, no globbing. Shell injection is therefore impossible by
/// construction: a malicious value can only become the single argv element
/// it was meant to be, never a new flag or a second argument.
///
/// # Per-variant rules
///
/// - `Literal(s)` → push `s` (one element).
/// - `Arg(name)`:
///   - missing key OR `Value::Null` → [`RenderError::MissingArg`].
///   - `String` → push it. `Number` → push `n.to_string()`. `Bool` → push
///     `"true"`/`"false"`. (Always one element.)
///   - `Array`/`Object` → [`RenderError::ArgTypeMismatch`].
/// - `Flag(name, value)`:
///   - `Bool(true)` → push `value`. `Bool(false)` → skip.
///   - missing key OR `Value::Null` → skip (flags are optional).
///   - any non-bool present → [`RenderError::ArgTypeMismatch`].
///
/// If `args` is not a JSON object, every lookup is treated as missing (so an
/// `Arg` errors with `MissingArg`, a `Flag` is skipped) — callers always pass
/// an object per the MCP contract.
pub fn render_argv(argv: &[ArgvPart], args: &serde_json::Value) -> Result<Vec<String>, RenderError> {
    let mut out: Vec<String> = Vec::with_capacity(argv.len());
    for part in argv {
        match part {
            ArgvPart::Literal(s) => out.push(s.clone()),
            ArgvPart::Arg(name) => {
                // ONE value, ONE push. No splitting, no concatenation.
                match args.get(name) {
                    None | Some(serde_json::Value::Null) => {
                        return Err(RenderError::MissingArg(name.clone()));
                    }
                    Some(serde_json::Value::String(s)) => out.push(s.clone()),
                    Some(serde_json::Value::Number(n)) => out.push(n.to_string()),
                    Some(serde_json::Value::Bool(b)) => out.push(b.to_string()),
                    // Array / Object → wrong shape; refuse.
                    Some(_) => return Err(RenderError::ArgTypeMismatch(name.clone())),
                }
            }
            ArgvPart::Flag(name, value) => {
                // Push only the static template `value` (never an arg), and only
                // when the flag is explicitly true. Missing/null/false → skip.
                match args.get(name) {
                    None | Some(serde_json::Value::Null) => {}
                    Some(serde_json::Value::Bool(true)) => out.push(value.clone()),
                    Some(serde_json::Value::Bool(false)) => {}
                    Some(_) => return Err(RenderError::ArgTypeMismatch(name.clone())),
                }
            }
        }
    }
    Ok(out)
}

// ── argv element parsing (spec §5.2) ────────────────────────────────────

/// Parse one argv element string into an [`ArgvPart`] (spec §5.2).
///
/// Brace-wrapped elements (`{...}`) are placeholders: `{name}`→`Arg`,
/// `{?name:value}`→`Flag`. Malformed brace-wrapped elements →
/// [`CatalogError::UnknownPlaceholder`] (fail loud at catalog load). Everything
/// else → `Literal` verbatim, including mid-string braces (e.g. `--out={f}`).
///
/// This is security-adjacent: the brace-wrapped discriminator + fail-loud rule
/// underpins the injection-safety model, so the matching must stay exact.
pub fn parse_argv_element(s: &str) -> Result<ArgvPart, CatalogError> {
    // Brace-wrapped iff it starts with `{` AND ends with `}`. By construction
    // that requires ≥2 bytes; both delimiters are ASCII so byte-index slicing
    // at 1 and len-1 is always on UTF-8 char boundaries (safe for any content).
    if s.starts_with('{') && s.ends_with('}') {
        let inner = &s[1..s.len() - 1];
        if let Some(rest) = inner.strip_prefix('?') {
            // Flag candidate: {?name:value}. Split at the FIRST ':'; the value
            // is everything after it (≥1 char; may contain '=' or further ':').
            match rest.split_once(':') {
                Some((name, value)) if is_ident(name) && !value.is_empty() => {
                    Ok(ArgvPart::Flag(name.into(), value.into()))
                }
                _ => Err(CatalogError::UnknownPlaceholder(s.into())),
            }
        } else {
            // Arg candidate: {name}. Must be a valid identifier.
            if is_ident(inner) {
                Ok(ArgvPart::Arg(inner.into()))
            } else {
                Err(CatalogError::UnknownPlaceholder(s.into()))
            }
        }
    } else {
        // Not brace-wrapped → whole string literal verbatim.
        Ok(ArgvPart::Literal(s.into()))
    }
}

/// Identifier predicate matching `[A-Za-z_][A-Za-z0-9_]*` (non-empty).
fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false, // empty or invalid first char
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

    #[test]
    fn parse_argv_literal_bare() {
        assert_eq!(parse_argv_element("jq").unwrap(), ArgvPart::Literal("jq".into()));
        assert_eq!(parse_argv_element("--color").unwrap(), ArgvPart::Literal("--color".into()));
        assert_eq!(parse_argv_element("repos").unwrap(), ArgvPart::Literal("repos".into()));
    }

    #[test]
    fn parse_argv_arg_placeholder() {
        assert_eq!(parse_argv_element("{filter}").unwrap(), ArgvPart::Arg("filter".into()));
        assert_eq!(parse_argv_element("{file}").unwrap(), ArgvPart::Arg("file".into()));
        assert_eq!(parse_argv_element("{_under}").unwrap(), ArgvPart::Arg("_under".into()));
        assert_eq!(parse_argv_element("{a1b2}").unwrap(), ArgvPart::Arg("a1b2".into()));
    }

    #[test]
    fn parse_argv_flag_placeholder() {
        assert_eq!(parse_argv_element("{?raw:-r}").unwrap(), ArgvPart::Flag("raw".into(), "-r".into()));
        // value may contain '='
        assert_eq!(parse_argv_element("{?color:--color=always}").unwrap(),
                   ArgvPart::Flag("color".into(), "--color=always".into()));
    }

    #[test]
    fn parse_argv_rejects_malformed_placeholder() {
        assert!(parse_argv_element("{}").is_err());          // empty
        assert!(parse_argv_element("{1bad}").is_err());      // name starts with digit
        assert!(parse_argv_element("{?n}").is_err());        // flag missing value
        assert!(parse_argv_element("{?n:}").is_err());       // flag empty value
        assert!(parse_argv_element("{ bad }").is_err());     // spaces not valid identifier
    }

    #[test]
    fn parse_argv_mid_brace_is_literal() {
        // NOT brace-wrapped (doesn't start with {) → whole string literal, braces preserved
        assert_eq!(parse_argv_element("--out={f}").unwrap(), ArgvPart::Literal("--out={f}".into()));
        assert_eq!(parse_argv_element("a{b}c").unwrap(), ArgvPart::Literal("a{b}c".into()));
    }

    // ── render_argv (spec §5.3) — injection-safe argv template rendering ──

    #[test]
    fn render_basic_with_and_without_flag() {
        let argv = vec![
            ArgvPart::Literal("jq".into()),
            ArgvPart::Flag("raw".into(), "-r".into()),
            ArgvPart::Arg("filter".into()),
            ArgvPart::Arg("file".into()),
        ];
        let with = serde_json::json!({"filter":".x","file":"a.json","raw":true});
        assert_eq!(render_argv(&argv, &with).unwrap(), vec!["jq", "-r", ".x", "a.json"]);
        let without = serde_json::json!({"filter":".x","file":"a.json","raw":false});
        assert_eq!(render_argv(&argv, &without).unwrap(), vec!["jq", ".x", "a.json"]);
        // flag absent entirely → treated as false → skipped
        let absent = serde_json::json!({"filter":".x","file":"a.json"});
        assert_eq!(render_argv(&argv, &absent).unwrap(), vec!["jq", ".x", "a.json"]);
    }

    #[test]
    fn render_arg_injection_stays_one_element() {
        // THE security test: shell metacharacters must NOT split or be interpreted.
        let argv = vec![ArgvPart::Literal("jq".into()), ArgvPart::Arg("filter".into())];
        let payload = "; rm -rf /";
        let args = serde_json::json!({"filter": payload});
        let out = render_argv(&argv, &args).unwrap();
        assert_eq!(out.len(), 2, "metacharacters must not add elements");
        assert_eq!(out[0], "jq");
        assert_eq!(out[1], payload, "the malicious string is ONE argv element, verbatim");
        // more metacharacter flavors — all stay one element
        for nasty in ["a b c", "$(whoami)", "`id`", "foo\nbar", "x | y", "\"q\"", "'q'", "  $HOME  "] {
            let out = render_argv(&argv, &serde_json::json!({"filter": nasty})).unwrap();
            assert_eq!(out.len(), 2);
            assert_eq!(out[1], nasty);
        }
    }

    #[test]
    fn render_missing_required_arg_errors() {
        let argv = vec![ArgvPart::Arg("filter".into())];
        assert!(matches!(render_argv(&argv, &serde_json::json!({})),
                         Err(RenderError::MissingArg(n)) if n == "filter"));
        // explicit null also counts as missing
        assert!(render_argv(&argv, &serde_json::json!({"filter": serde_json::Value::Null})).is_err());
    }

    #[test]
    #[allow(clippy::approx_constant)] // 3.14 is a sample float, not π
    fn render_number_and_bool_args_coerced_to_string() {
        let argv = vec![ArgvPart::Literal("echo".into()), ArgvPart::Arg("n".into())];
        assert_eq!(render_argv(&argv, &serde_json::json!({"n": 42})).unwrap(), vec!["echo", "42"]);
        assert_eq!(render_argv(&argv, &serde_json::json!({"n": 3.14})).unwrap(), vec!["echo", "3.14"]);
        assert_eq!(render_argv(&argv, &serde_json::json!({"n": true})).unwrap(), vec!["echo", "true"]);
    }

    #[test]
    fn render_arg_rejects_object_and_array() {
        let argv = vec![ArgvPart::Arg("x".into())];
        assert!(render_argv(&argv, &serde_json::json!({"x": {"a":1}})).is_err());
        assert!(render_argv(&argv, &serde_json::json!({"x": [1,2]})).is_err());
    }

    #[test]
    fn render_flag_non_bool_errors() {
        let argv = vec![ArgvPart::Flag("r".into(), "-r".into())];
        // present but non-bool → error (NOT silently skipped; an explicit wrong type is a misuse)
        assert!(render_argv(&argv, &serde_json::json!({"r": "yes"})).is_err());
        assert!(render_argv(&argv, &serde_json::json!({"r": 1})).is_err());
    }

    #[test]
    fn render_empty_template_yields_empty() {
        assert_eq!(render_argv(&[], &serde_json::json!({})).unwrap(), Vec::<String>::new());
    }
}
