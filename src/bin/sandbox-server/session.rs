use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct SessionManager {
    base_dir: PathBuf,
    sessions: Mutex<HashMap<String, SessionState>>,
}

struct SessionState {
    work_dir: PathBuf,
    /// 最近访问时刻(CR-5):get_or_create 命中时刷新;GC idle 判旧依据。
    last_accessed: Instant,
}

/// GC 策略(CR-5)。
#[derive(Clone, Debug)]
pub struct GcPolicy {
    /// session 最近访问超过此时长 → 淘汰(map 内)。
    pub max_idle: Duration,
    /// 活跃 session 数上限,超限按 LRU(最久未访问)淘汰。
    pub max_sessions: usize,
}

/// 一次 sweep 的淘汰计数(CR-5)。
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// 从活跃 map 淘汰的 session 数(并删其 work_dir)。
    pub evicted_sessions: usize,
    /// 盘上 orphan 目录数(不在活跃 map、mtime 过期)。
    pub removed_orphans: usize,
}

impl SessionManager {
    pub fn new(base_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&base_dir).ok();
        Self { base_dir, sessions: Mutex::new(HashMap::new()) }
    }

    /// Get or create a session's working directory. 命中时刷新 `last_accessed`。
    pub fn get_or_create(&self, session_id: &str) -> PathBuf {
        let mut sessions = self.sessions.lock().unwrap();
        let now = Instant::now();
        if let Some(state) = sessions.get_mut(session_id) {
            state.last_accessed = now;
            return state.work_dir.clone();
        }
        let work_dir = self.base_dir.join(session_id);
        std::fs::create_dir_all(&work_dir).ok();
        sessions.insert(
            session_id.to_string(),
            SessionState { work_dir: work_dir.clone(), last_accessed: now },
        );
        work_dir
    }

    /// Clean up a session and remove its working directory.
    pub fn cleanup(&self, session_id: &str) -> bool {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(state) = sessions.remove(session_id) {
            let _ = std::fs::remove_dir_all(&state.work_dir);
            true
        } else {
            false
        }
    }

    /// List active session IDs.
    #[allow(dead_code)]
    pub fn list(&self) -> Vec<String> {
        self.sessions.lock().unwrap().keys().cloned().collect()
    }

    /// 执行一次 GC(CR-5):① idle 淘汰 ② LRU 超容淘汰 ③ 盘上 orphan 扫描。
    ///
    /// 三步共享一次锁;orphan 扫描用「活跃 map 名集合」排除,不会误删/重复删活跃 session。
    pub fn sweep(&self, policy: &GcPolicy) -> SweepReport {
        let now = Instant::now();
        let mut evicted = 0usize;
        let mut orphans = 0usize;
        let mut sessions = self.sessions.lock().unwrap();

        // ① idle 淘汰:last_accessed 早于 (now - max_idle) 的,删 map + remove_dir_all。
        let idle_ids: Vec<String> = sessions
            .iter()
            .filter(|(_, s)| now.duration_since(s.last_accessed) > policy.max_idle)
            .map(|(id, _)| id.clone())
            .collect();
        for id in idle_ids {
            if let Some(state) = sessions.remove(&id) {
                let _ = std::fs::remove_dir_all(&state.work_dir);
                evicted += 1;
            }
        }

        // ② LRU 超容淘汰:若仍 > max_sessions,按 last_accessed 最旧者淘汰至 == max_sessions。
        if sessions.len() > policy.max_sessions {
            let mut by_age: Vec<(String, Instant)> =
                sessions.iter().map(|(k, v)| (k.clone(), v.last_accessed)).collect();
            by_age.sort_by_key(|(_, t)| *t); // 最旧(最早 Instant)在前
            let to_evict = sessions.len() - policy.max_sessions;
            for (id, _) in by_age.into_iter().take(to_evict) {
                if let Some(state) = sessions.remove(&id) {
                    let _ = std::fs::remove_dir_all(&state.work_dir);
                    evicted += 1;
                }
            }
        }

        // ③ orphan 扫盘:base_dir 下子目录,名 ∉ 活跃 map 且 mtime > max_idle → remove_dir_all。
        let active: std::collections::HashSet<&String> = sessions.keys().collect();
        if let Ok(entries) = std::fs::read_dir(&self.base_dir) {
            for ent in entries.flatten() {
                let path = ent.path();
                if !path.is_dir() {
                    continue;
                }
                let name = match path.file_name().and_then(|n| n.to_str()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };
                if active.contains(&name) {
                    continue; // 活跃 session 的目录,跳过
                }
                let mtime = ent.metadata().and_then(|m| m.modified()).ok();
                if let Some(mt) = mtime {
                    if let Ok(age) = mt.elapsed() {
                        if age > policy.max_idle {
                            let _ = std::fs::remove_dir_all(&path);
                            orphans += 1;
                        }
                    }
                }
            }
        }

        SweepReport { evicted_sessions: evicted, removed_orphans: orphans }
    }

    /// 测试专用:把某 session 的 last_accessed 回溯 age(Instant 回溯,无需 sleep)。
    #[cfg(test)]
    fn backdate_last_access(&self, session_id: &str, age: Duration) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(state) = sessions.get_mut(session_id) {
            state.last_accessed = Instant::now() - age;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::SystemTime;

    fn tmp_mgr() -> (SessionManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SessionManager::new(dir.path().to_path_buf());
        (mgr, dir)
    }

    /// 回溯一个目录的 mtime(用于 orphan 测试,无需 sleep)。
    /// 目录须以只读打开(`set_modified` 用 futimens,对目录 fd 生效)。
    fn backdate_mtime(path: &PathBuf, age: Duration) {
        let f = std::fs::File::open(path).unwrap();
        let past = SystemTime::now() - age;
        f.set_modified(past).unwrap();
    }

    #[test]
    fn sweep_evicts_idle_sessions() {
        let (mgr, _d) = tmp_mgr();
        let a = mgr.get_or_create("task_a");
        let b = mgr.get_or_create("task_b");
        // A 回溯 1h 前 → idle;B 保持新
        mgr.backdate_last_access("task_a", Duration::from_secs(3600));

        let report = mgr.sweep(&GcPolicy { max_idle: Duration::from_secs(60), max_sessions: 100 });
        assert!(report.evicted_sessions >= 1, "应淘汰 idle session A");
        assert!(!a.exists(), "A 的 work_dir 应被删");
        assert!(b.exists(), "B 的 work_dir 应保留(未 idle)");
        assert_eq!(mgr.list().len(), 1, "map 应只剩 B");
        assert_eq!(mgr.list()[0], "task_b");
    }

    #[test]
    fn sweep_lru_when_over_capacity() {
        let (mgr, _d) = tmp_mgr();
        mgr.get_or_create("a");
        mgr.get_or_create("b");
        mgr.get_or_create("c");
        // 让 a 最旧
        mgr.backdate_last_access("a", Duration::from_secs(30));
        mgr.backdate_last_access("b", Duration::from_secs(10));
        // c 保持最新

        // max_idle 很大(不触发 idle),max_sessions=2 → LRU 淘汰 a
        let report = mgr.sweep(&GcPolicy {
            max_idle: Duration::from_secs(3600),
            max_sessions: 2,
        });
        assert!(report.evicted_sessions >= 1, "超容应 LRU 淘汰");
        assert_eq!(mgr.list().len(), 2, "应剩 2 个");
        assert!(!mgr.list().contains(&"a".to_string()), "最旧的 a 应被淘汰");
    }

    #[test]
    fn sweep_keeps_active_session_dir() {
        let (mgr, _d) = tmp_mgr();
        let dir = mgr.get_or_create("active");
        // idle 阈值很大 → 不淘汰活跃 session;也不该被当 orphan 删
        let report = mgr.sweep(&GcPolicy {
            max_idle: Duration::from_secs(3600),
            max_sessions: 100,
        });
        assert_eq!(report.evicted_sessions, 0);
        assert!(dir.exists(), "活跃 session 的目录必须保留");
    }

    #[test]
    fn sweep_removes_disk_orphans() {
        let (mgr, dir) = tmp_mgr();
        // 建一个活跃 session(在 map)
        let _active = mgr.get_or_create("active");
        // 建一个 orphan 目录(不在 map),回溯 mtime 1h 前
        let orphan = dir.path().join("crash_leftover");
        fs::create_dir_all(&orphan).unwrap();
        backdate_mtime(&orphan, Duration::from_secs(3600));

        let report = mgr.sweep(&GcPolicy { max_idle: Duration::from_secs(60), max_sessions: 100 });
        assert!(report.removed_orphans >= 1, "应删除盘上 orphan");
        assert!(!orphan.exists(), "orphan 目录应被删");
    }

    #[test]
    fn sweep_report_counts() {
        let (mgr, dir) = tmp_mgr();
        // 1 个 idle session(map 内)+ 1 个 orphan(盘上)
        let idle = mgr.get_or_create("idle");
        mgr.backdate_last_access("idle", Duration::from_secs(3600));
        let orphan = dir.path().join("orphan_dir");
        fs::create_dir_all(&orphan).unwrap();
        backdate_mtime(&orphan, Duration::from_secs(3600));

        let report = mgr.sweep(&GcPolicy { max_idle: Duration::from_secs(60), max_sessions: 100 });
        assert_eq!(report.evicted_sessions, 1, "idle 淘汰计数");
        assert_eq!(report.removed_orphans, 1, "orphan 删除计数");
        assert!(!idle.exists());
        assert!(!orphan.exists());
    }

    #[test]
    fn get_or_create_refreshes_last_access() {
        let (mgr, _d) = tmp_mgr();
        mgr.get_or_create("s");
        // 模拟很久之前创建后,再次 get_or_create 应刷新 last_accessed
        mgr.backdate_last_access("s", Duration::from_secs(3600));
        mgr.get_or_create("s"); // 命中 → 刷新为 now

        // 现在 sweep(max_idle=60s)不该淘汰它(刚被刷新)
        let report = mgr.sweep(&GcPolicy { max_idle: Duration::from_secs(60), max_sessions: 100 });
        assert_eq!(report.evicted_sessions, 0, "get_or_create 刷新后不应被判 idle");
        assert_eq!(mgr.list().len(), 1);
    }
}
