//! Unified Landlock sandbox rules for lv-agent.
//!
//! Used by both agentworker (Worker execution) and sandbox-server (remote execution).
//!
//! On Linux: enforces filesystem access restrictions.
//! On non-Linux / old kernels: no-op (safe fallback).
//!
//! Must be called from `pre_exec` — after fork, before exec.

use std::path::Path;

/// Apply Landlock filesystem restrictions.
///
/// * `work_dir` — full read/write access to this directory and its children.
/// * `extra_paths` — additional paths with read/execute access (e.g., tool binaries).
pub fn apply(work_dir: &Path, extra_paths: &[&str]) {
    apply_impl(work_dir, extra_paths)
}

#[cfg(target_os = "linux")]
fn apply_impl(work_dir: &Path, extra_paths: &[&str]) {
    use landlock::{
        make_bitflags, AccessFs, CompatLevel, Compatible, PathBeneath, PathFd,
        Ruleset, RulesetAttr, RulesetCreatedAttr,
    };

    let access_rw = make_bitflags!(AccessFs::{
        Execute | ReadFile | ReadDir | WriteFile
        | RemoveDir | RemoveFile | MakeDir | MakeReg | MakeSock | MakeFifo | MakeSym | Truncate
    });
    let access_ro = make_bitflags!(AccessFs::{Execute | ReadFile | ReadDir});
    let access_dev = make_bitflags!(AccessFs::{ReadFile | ReadDir | WriteFile});

    let mut rules: Vec<PathBeneath<PathFd>> = Vec::new();

    // Allow full access to work directory
    if let Ok(fd) = PathFd::new(work_dir) {
        rules.push(PathBeneath::new(fd, access_rw));
    }

    // Allow read+execute on system paths
    for sys_path in extra_paths.iter().chain(&["/usr", "/bin", "/lib", "/lib64", "/etc", "/opt"]) {
        if let Ok(fd) = PathFd::new(sys_path) {
            rules.push(PathBeneath::new(fd, access_ro));
        }
    }

    // Allow basic device access
    if let Ok(fd) = PathFd::new("/dev") {
        rules.push(PathBeneath::new(fd, access_dev));
    }

    let rules: Vec<Result<PathBeneath<PathFd>, landlock::RulesetError>> =
        rules.into_iter().map(Ok).collect();

    if let Err(e) = Ruleset::default()
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(access_rw | access_ro | access_dev)
        .and_then(|r| r.create())
        .and_then(|r| r.add_rules(rules))
        .and_then(|r| r.restrict_self())
    {
        tracing::warn!("Landlock sandbox not enforced: {}", e);
    }
}

#[cfg(not(target_os = "linux"))]
fn apply_impl(_work_dir: &Path, _extra_paths: &[&str]) {
    // no-op on non-Linux
}
