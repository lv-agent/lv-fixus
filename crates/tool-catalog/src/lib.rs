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

#![allow(dead_code)] // skeleton: types only; builtins/find/render/parse come in later tasks

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
