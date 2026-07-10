//! Task 内存投射 — consume 事件流 → O(1) 更新所有衍生字段(spec §4.4 projection)。
//!
//! `TaskProjection::apply` 是核心:每条事件进入,更新 head(state/task_type/provenance/body)、
//! 事件索引(by_seq/by_turn_id)、活跃 Turn/Step、摘要、Token 累积。不依赖任何 broker/logdbd proto
//! 类型——收基本类型(seq/event_type/content/metadata)。

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::error::{AppError, Result};
use crate::models::{
    AgentEvent, EventType, Provenance, StepExecution, Task, TaskState, TokenUsageStats,
    IncompleteStep, IncompleteTurn,
};


// ── TaskProjection ───────────────────────────────────────────────────────

/// Task 的内存投射——事件流 → O(1) 更新。
///
/// 每次 `apply` 一条事件,无需 replay。head 字段(state/task_type/provenance/body)
/// 从对应 Task 级事件析出;Turn/Step 索引从 metadata(turn_id/step_id)维护。
pub struct TaskProjection {
    pub task_id: String,

    // head(从 task_created 析出后不变)
    pub task_type: String,
    pub provenance: Provenance,
    pub body: Option<serde_json::Value>,

    // 投影 state
    pub state: TaskState,

    // 序列追踪
    pub max_seq: i64,
    pub max_turn_id: i64,

    // 事件索引
    by_seq: HashMap<i64, AgentEvent>,
    by_turn_id: HashMap<i64, Vec<i64>>, // turn_id → [seq], 有序

    // 活跃 Turn(有 turn_started 无 terminal)
    pub active_turns: HashSet<i64>,
    // 活跃 Step(有 start 无 terminal): step_id → (seq, start_event)
    pub active_steps: HashMap<String, (i64, AgentEvent)>,
    // 已完成 Step: step_id → (start_seq, start_event, terminal_type, terminal_seq)
    completed_steps: HashMap<String, (i64, AgentEvent, String, i64)>,

    // 摘要
    pub latest_summary: Option<serde_json::Value>,
    pub summarized_up_to_seq: i64,
    pub summarized_up_to_turn_id: i64,

    // Token 累积
    pub token_stats: HashMap<String, TokenUsageStats>,

    pub recent_turn_ids: VecDeque<i64>,
}

impl TaskProjection {
    pub fn new(task_id: &str) -> Self {
        Self {
            task_id: task_id.to_string(),
            task_type: String::new(),
            provenance: Provenance {
                source_channel: String::new(),
                source_session_id: None,
                source_user_id: None,
                source_tenant_id: None,
                source_message_id: None,
                created_at: chrono::Utc::now(),
                created_by: String::new(),
            },
            body: None,
            state: TaskState::Created,
            max_seq: 0,
            max_turn_id: 0,
            by_seq: HashMap::new(),
            by_turn_id: HashMap::new(),
            active_turns: HashSet::new(),
            active_steps: HashMap::new(),
            completed_steps: HashMap::new(),
            latest_summary: None,
            summarized_up_to_seq: 0,
            summarized_up_to_turn_id: 0,
            token_stats: HashMap::new(),
            recent_turn_ids: VecDeque::new(),
        }
    }

    /// 应用一条事件,更新投射。O(1)——不进索引线性扫描。
    pub fn apply(
        &mut self,
        seq: u64,
        event_type: &str,
        content: &[u8],
        metadata: &HashMap<String, String>,
    ) -> Result<()> {
        let seq = seq as i64;
        let et = EventType::from_str(event_type)
            .ok_or_else(|| AppError::InvalidEventType(event_type.to_string()))?;
        let payload: serde_json::Value =
            serde_json::from_slice(content).unwrap_or_default();

        let turn_id: Option<i64> = metadata
            .get("turn_id")
            .and_then(|v| v.parse().ok());
        let step_id = metadata.get("step_id").cloned();

        // 追踪 seq
        self.max_seq = self.max_seq.max(seq);
        if let Some(tid) = turn_id {
            self.max_turn_id = self.max_turn_id.max(tid);
        }

        // 构建 AgentEvent(仅用于索引,不完整)
        let ae = AgentEvent {
            task_id: self.task_id.clone(),
            seq,
            turn_id,
            step_id: step_id.clone(),
            event_type: et.clone(),
            schema_version: 1,
            payload: payload.clone(),
            created_at: chrono::Utc::now(),
        };

        // 索引
        self.by_seq.insert(seq, ae.clone());
        if let Some(tid) = turn_id {
            self.by_turn_id.entry(tid).or_default().push(seq);
            self.recent_turn_ids.push_back(tid);
        }

        // 按 event_type 更新 head
        match et {
            EventType::TaskCreated => {
                self.task_type = payload["task_type"]
                    .as_str()
                    .unwrap_or("?")
                    .to_string();
                if let Some(p) = payload.get("provenance") {
                    if let Ok(prov) = serde_json::from_value(p.clone()) {
                        self.provenance = prov;
                    }
                }
                self.body = payload.get("body").filter(|v| !v.is_null()).cloned();
                self.state = TaskState::Created;
            }
            EventType::TaskReady => self.state = TaskState::Ready,
            EventType::TaskClaimed => self.state = TaskState::Claimed,
            EventType::TaskBlocked => self.state = TaskState::Blocked,
            EventType::TaskSucceeded => self.state = TaskState::Succeeded,
            EventType::TaskFailed => self.state = TaskState::Failed,
            EventType::TaskCanceled => self.state = TaskState::Canceled,

            EventType::TurnStarted => {
                if self.state == TaskState::Claimed {
                    self.state = TaskState::Executing;
                }
                if let Some(tid) = turn_id {
                    self.active_turns.insert(tid);
                }
            }
            EventType::TurnCompleted
            | EventType::TurnFailed
            | EventType::TurnCanceled
            | EventType::TurnBlocked => {
                if let Some(tid) = turn_id {
                    self.active_turns.remove(&tid);
                }
            }

            EventType::LlmInvoked | EventType::ToolInvoked => {
                if let Some(ref sid) = step_id {
                    self.active_steps.insert(sid.clone(), (seq, ae));
                }
            }
            EventType::LlmCompleted
            | EventType::LlmFailed
            | EventType::ToolCompleted
            | EventType::ToolFailed => {
                if let Some(ref sid) = step_id {
                    if let Some((start_seq, start_ev)) = self.active_steps.remove(sid) {
                        self.completed_steps.insert(
                            sid.clone(),
                            (start_seq, start_ev, et.as_str().to_string(), seq),
                        );
                    }
                }
                // Token 累积
                if et == EventType::LlmCompleted {
                    if let Some(usage) = payload.get("usage") {
                        let model = payload["model"].as_str().unwrap_or("?").to_string();
                        let entry = self.token_stats.entry(model.clone()).or_insert_with(
                            || TokenUsageStats {
                                model: model.clone(),
                                call_count: 0,
                                prompt_tokens: 0,
                                completion_tokens: 0,
                            },
                        );
                        entry.call_count += 1;
                        entry.prompt_tokens += usage["prompt_tokens"].as_i64().unwrap_or(0);
                        entry.completion_tokens += usage["completion_tokens"].as_i64().unwrap_or(0);
                    }
                }
            }

            EventType::SummaryMarker => {
                self.latest_summary = Some(payload.clone());
                self.summarized_up_to_seq =
                    payload["summarized_up_to_seq"].as_i64().unwrap_or(seq);
                self.summarized_up_to_turn_id = payload["summarized_up_to_turn_id"]
                    .as_i64()
                    .unwrap_or(self.summarized_up_to_turn_id);
            }

            // Session 级事件(legacy):不更新 Task head,仅索引
            _ => {}
        }

        Ok(())
    }

    // ── 查询方法(供 BrokerEventStore 转发)──────────────────────────────

    pub fn by_seq(&self, seq: i64) -> Option<&AgentEvent> {
        self.by_seq.get(&seq)
    }

    pub fn turn_seqs(&self, turn_id: i64) -> &[i64] {
        self.by_turn_id.get(&turn_id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn after_seq(&self, after_seq: i64) -> Vec<&AgentEvent> {
        self.by_seq
            .iter()
            .filter(|(s, _)| **s > after_seq)
            .map(|(_, e)| e)
            .collect()
    }

    pub fn incomplete_turns(&self) -> Vec<IncompleteTurn> {
        self.active_turns
            .iter()
            .filter_map(|tid| {
                self.by_turn_id.get(tid).and_then(|seqs| {
                    seqs.first().and_then(|s| self.by_seq.get(s)).map(|e| {
                        let rg = e.payload["redo_group"].as_str().unwrap_or("?").to_string();
                        let rc = e.payload["redo_count"].as_i64().unwrap_or(0) as i32;
                        IncompleteTurn {
                            turn_id: *tid,
                            redo_group: rg,
                            redo_count: rc,
                            turn_started_at: e.created_at,
                        }
                    })
                })
            })
            .collect()
    }

    pub fn incomplete_steps(&self) -> Vec<IncompleteStep> {
        self.active_steps
            .iter()
            .map(|(sid, (seq, ev))| IncompleteStep {
                seq: *seq,
                turn_id: ev.turn_id.unwrap_or(0),
                step_id: sid.clone(),
                start_event_type: ev.event_type.as_str().to_string(),
                payload: ev.payload.clone(),
                started_at: ev.created_at,
            })
            .collect()
    }

    pub fn turn_steps(&self, turn_id: i64) -> Vec<StepExecution> {
        let mut steps: Vec<StepExecution> = Vec::new();
        // 查 turn_id 下的所有 seq
        if let Some(seqs) = self.by_turn_id.get(&turn_id) {
            for s in seqs {
                if let Some(ev) = self.by_seq.get(s) {
                    if let Some(ref sid) = ev.step_id {
                        if ev.event_type.is_step_start() {
                            // 启动事件:查 completed 对应
                            let (ended_at, end_event, duration_ms) =
                                self.completed_steps.get(sid).map(|(_, _, term_type, term_seq)| {
                                    let term_ev = self.by_seq.get(term_seq);
                                    let dur = term_ev.map(|t| {
                                        (t.created_at - ev.created_at).num_milliseconds()
                                            as f64
                                    });
                                    (term_ev.map(|t| t.created_at), Some(term_type.clone()), dur)
                                }).unwrap_or((None, None, None));
                            steps.push(StepExecution {
                                step_id: sid.clone(),
                                step_type: ev.payload.get("step_type").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                started_at: ev.created_at,
                                ended_at,
                                end_event,
                                duration_ms,
                            });
                        }
                    }
                }
            }
        }
        steps.sort_by_key(|s| s.started_at);
        steps
    }

    pub fn llm_payloads_after_seq(&self, after_seq: i64) -> Vec<String> {
        self.by_seq
            .iter()
            .filter(|(s, _)| **s > after_seq)
            .filter(|(_, e)| e.event_type == EventType::LlmCompleted)
            .map(|(_, e)| serde_json::to_string(&e.payload).unwrap_or_default())
            .collect()
    }
}

// ── ProjectionCache(LRU)───────────────────────────────────────────────

/// broker consume → 内存投射 LRU。容量限制,线程安全(RwLock)。
pub struct ProjectionCache {
    inner: tokio::sync::RwLock<LruMap>,
}

struct LruMap {
    map: HashMap<String, Arc<tokio::sync::RwLock<TaskProjection>>>,
    order: VecDeque<String>,
    cap: usize,
}

/// 投射引用(Arc+RwLock),调用方拿到后可读可更新。
pub type ProjectionRef = Arc<tokio::sync::RwLock<TaskProjection>>;

impl ProjectionCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: tokio::sync::RwLock::new(LruMap {
                map: HashMap::new(),
                order: VecDeque::new(),
                cap: capacity,
            }),
        }
    }

    /// 本地缓存命中(不调 broker),返回投射引用。
    pub async fn get(&self, task_id: &str) -> Option<ProjectionRef> {
        let mut m = self.inner.write().await;
        let proj = m.map.get(task_id).cloned();
        if proj.is_some() {
            m.order.retain(|id| id != task_id);
            m.order.push_back(task_id.to_string());
        }
        proj
    }

    /// 从 broker consume 新事件,建/更新投射。
    /// `broker_addr` 不带 scheme("127.0.0.1:PORT")
    pub async fn catch_up(
        &self,
        task_id: &str,
        broker_addr: &str,
        namespace: &str,
    ) -> std::result::Result<ProjectionRef, logdb_client::broker::BrokerError> {
        use logdb_client::broker::GroupConsumer;
        use logdb_broker_proto::pb::consume_response::Payload;
        use tokio_stream::StreamExt;

        let addr = format!("http://{}", broker_addr);
        let mut consumer = GroupConsumer::join(addr, namespace, task_id, "fixus-reads", "singleton").await?;
        let all_shards: HashSet<u32> = consumer.assigned_shards().iter().copied().collect();
        let mut stream = consumer.consume_frames().await?;
        let mut proj_guard = self.inner.write().await;
        let proj = proj_guard
            .map
            .entry(task_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(TaskProjection::new(task_id))));
        let proj_clone = proj.clone();
        drop(proj_guard); // 释放写锁,消费 async 不持锁

        let mut caught_up: HashSet<u32> = HashSet::new();
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() { break; }
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(frame))) => {
                    match frame.payload {
                        Some(Payload::Record(rec)) => {
                            let mut p = proj_clone.write().await;
                            let _ = p.apply(rec.seq, &rec.event_type, &rec.content, &rec.metadata);
                        }
                        Some(Payload::CaughtUp(c)) => { caught_up.insert(c.shard_id); }
                        Some(Payload::Rebalance(_)) | Some(Payload::Assignment(_)) => {}
                        None => {}
                    }
                    if caught_up == all_shards { break; }
                }
                Ok(Some(Err(e))) => return Err(e),
                _ => break,
            }
        }
        consumer.leave().await?;

        // 更新 LRU 顺序
        let mut m = self.inner.write().await;
        m.order.push_back(task_id.to_string());
        while m.order.len() > m.cap {
            if let Some(old) = m.order.pop_front() {
                m.map.remove(&old);
            }
        }
        Ok(proj_clone)
    }
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn meta_turn(turn_id: i64) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("turn_id".to_string(), turn_id.to_string());
        m
    }

    fn meta_step(step_id: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("step_id".to_string(), step_id.to_string());
        m
    }

    fn meta_turn_step(turn_id: i64, step_id: &str) -> HashMap<String, String> {
        let mut m = meta_turn(turn_id);
        m.insert("step_id".to_string(), step_id.to_string());
        m
    }

    #[test]
    fn test_projection_lifecycle_head_and_state() {
        let mut p = TaskProjection::new("t1");

        // task_created
        p.apply(1, "task_created", serde_json::to_vec(&json!({
            "task_type": "db.repair",
            "provenance": {"source_channel":"api","source_user_id":"u1","source_tenant_id":"t1","created_at":"2026-01-01T00:00:00Z","created_by":"nuntius"},
            "body": {"task_brief": "fix db1"}
        })).unwrap().as_slice(), &HashMap::new()).unwrap();

        assert_eq!(p.task_type, "db.repair");
        assert_eq!(p.state, TaskState::Created);
        assert_eq!(p.max_seq, 1);
        assert!(p.body.is_some());

        // task_ready
        p.apply(2, "task_ready", b"{}", &HashMap::new()).unwrap();
        assert_eq!(p.state, TaskState::Ready);

        // task_claimed → Claimed
        p.apply(3, "task_claimed", serde_json::to_vec(&json!({"claimant":"f1"})).unwrap().as_slice(), &HashMap::new()).unwrap();
        assert_eq!(p.state, TaskState::Claimed);

        // turn_started → Executing
        let ts = serde_json::to_vec(&json!({"user_input":"hi","redo_group":"rg1","redo_count":0})).unwrap();
        p.apply(4, "turn_started", &ts, &meta_turn(1)).unwrap();
        assert_eq!(p.state, TaskState::Executing);
        assert!(p.active_turns.contains(&1));
        assert_eq!(p.max_turn_id, 1);

        // turn_completed → 移出 active_turns
        p.apply(5, "turn_completed", serde_json::to_vec(&json!({"final_output":"done"})).unwrap().as_slice(), &meta_turn(1)).unwrap();
        assert!(!p.active_turns.contains(&1));

        // task_succeeded → 终态
        p.apply(6, "task_succeeded", b"{}", &HashMap::new()).unwrap();
        assert_eq!(p.state, TaskState::Succeeded);
        assert!(p.state.is_terminal());
    }

    #[test]
    fn test_projection_active_steps_and_token_accumulation() {
        let mut p = TaskProjection::new("t2");
        // task_created
        p.apply(1, "task_created", serde_json::to_vec(&json!({"task_type":"x","provenance":{"source_channel":"api","created_by":"t","created_at":"2026-01-01T00:00:00Z"}})).unwrap().as_slice(), &HashMap::new()).unwrap();

        // llm_invoked
        p.apply(2, "llm_invoked", serde_json::to_vec(&json!({"step_type":"llm_call","model":"gpt-4","messages":[],"local_seq":1})).unwrap().as_slice(), &meta_turn_step(1, "step-a")).unwrap();
        assert_eq!(p.active_steps.len(), 1);

        // llm_completed(带 usage)
        p.apply(3, "llm_completed", serde_json::to_vec(&json!({"model":"gpt-4","local_seq":1,"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}})).unwrap().as_slice(), &meta_turn_step(1, "step-a")).unwrap();
        assert_eq!(p.active_steps.len(), 0);
        assert_eq!(p.token_stats["gpt-4"].call_count, 1);
        assert_eq!(p.token_stats["gpt-4"].prompt_tokens, 10);
        assert_eq!(p.token_stats["gpt-4"].completion_tokens, 5);

        // tool_invoked → tool_completed
        p.apply(4, "tool_invoked", serde_json::to_vec(&json!({"step_type":"tool_call","tool_name":"Bash","tool_call_id":"c1","idempotency_key":"k","input":{},"local_seq":2})).unwrap().as_slice(), &meta_turn_step(1, "step-b")).unwrap();
        p.apply(5, "tool_completed", serde_json::to_vec(&json!({"tool_call_id":"c1","output":{},"is_error":false,"local_seq":2})).unwrap().as_slice(), &meta_turn_step(1, "step-b")).unwrap();
        assert_eq!(p.active_steps.len(), 0);

        // get_turn_steps
        let steps = p.turn_steps(1);
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].step_id, "step-a");
        assert_eq!(steps[1].step_id, "step-b");
    }

    #[test]
    fn test_projection_incomplete_turns_and_summary() {
        let mut p = TaskProjection::new("t3");
        p.apply(1, "task_created", serde_json::to_vec(&json!({"task_type":"x","provenance":{"source_channel":"api","created_by":"t","created_at":"2026-01-01T00:00:00Z"}})).unwrap().as_slice(), &HashMap::new()).unwrap();

        // turn 7 started(无 terminal → incomplete)
        p.apply(2, "turn_started", serde_json::to_vec(&json!({"user_input":"x","redo_group":"rg-abc","redo_count":2})).unwrap().as_slice(), &meta_turn(7)).unwrap();
        let inc = p.incomplete_turns();
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].turn_id, 7);
        assert_eq!(inc[0].redo_group, "rg-abc");
        assert_eq!(inc[0].redo_count, 2);

        // summary_marker
        p.apply(3, "summary_marker", serde_json::to_vec(&json!({"summarized_up_to_seq":2,"summarized_up_to_turn_id":7,"summary":"sum text","covered_event_count":2})).unwrap().as_slice(), &HashMap::new()).unwrap();
        assert!(p.latest_summary.is_some());
        assert_eq!(p.summarized_up_to_seq, 2);
    }
}
