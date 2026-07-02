use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

pub struct SessionManager {
    base_dir: PathBuf,
    sessions: Mutex<HashMap<String, SessionState>>,
}

struct SessionState {
    work_dir: PathBuf,
}

impl SessionManager {
    pub fn new(base_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&base_dir).ok();
        Self { base_dir, sessions: Mutex::new(HashMap::new()) }
    }

    /// Get or create a session's working directory.
    pub fn get_or_create(&self, session_id: &str) -> PathBuf {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(state) = sessions.get(session_id) {
            return state.work_dir.clone();
        }
        let work_dir = self.base_dir.join(session_id);
        std::fs::create_dir_all(&work_dir).ok();
        sessions.insert(session_id.to_string(), SessionState { work_dir: work_dir.clone() });
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
}
