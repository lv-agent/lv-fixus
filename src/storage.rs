//! Event Store 持久化层
//!
//! 定义 `EventStore` trait 及其 logdbd 实现。
//!
//! 设计目标：
//! - 不可变 append-only 事件存储
//! - 简单读走 gRPC Read/Scan（零序列化开销）
//! - 过滤/聚合查询走 logdbd 原生结构化 QueryRequest（直读 segment,cr-027）

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use logdb_client::{
    Client, QueryRequest, QueryResponse, QueryResult, query_response,
};
#[cfg(test)]
use logdbd_proto::pb::{AbsentMatch, AppendRequest, MetadataFilter};
use tokio::sync::Mutex;

use crate::error::{AppError, Result};
use crate::models::{
    AgentEvent, EventType, IncompleteStep, IncompleteTurn, Provenance, StepExecution, Task,
    TaskState, TokenUsageStats,
};

// ── EventStore Trait ────────────────────────────────────────────────────────

/// Event Store 抽象 trait。
///
/// 当前实现：`LogdbdEventStore`（通过 gRPC 连接 logdbd）。
#[async_trait]
pub trait EventStore: Send + Sync {
    // ── Session ─────────────────────────────────────────────────────────

    async fn create_task(
        &self,
        task_type: &str,
        tenant_id: &str,
        user_id: &str,
        provenance: &Provenance,
        body: Option<&serde_json::Value>,
    ) -> Result<(String, AgentEvent)>;

    async fn get_task(&self, task_id: &str) -> Result<Option<Task>>;
    async fn task_exists(&self, task_id: &str) -> Result<bool>;
    async fn is_task_ended(&self, task_id: &str) -> Result<bool>;

    /// 派生 Task 当前状态(从 Task 级事件流投影,spec §4.4)。
    /// 不存在 task_created → 返回 None(调用方判 TaskNotFound)。
    async fn get_task_state(&self, task_id: &str) -> Result<Option<TaskState>>;

    // ── Seq ─────────────────────────────────────────────────────────────

    async fn get_max_seq(&self, task_id: &str) -> Result<i64>;
    async fn get_max_turn_id(&self, task_id: &str) -> Result<i64>;

    // ── Write ───────────────────────────────────────────────────────────

    async fn write_event(&self, event: &AgentEvent) -> Result<i64>;
    async fn write_events_batch(&self, events: &[AgentEvent]) -> Result<Vec<i64>>;

    // ── Read ────────────────────────────────────────────────────────────

    async fn get_event(&self, task_id: &str, seq: i64) -> Result<Option<AgentEvent>>;
    async fn get_turn_events(&self, task_id: &str, turn_id: i64)
        -> Result<Vec<AgentEvent>>;
    async fn get_events_after_seq(
        &self,
        task_id: &str,
        after_seq: i64,
    ) -> Result<Vec<AgentEvent>>;
    async fn get_latest_summary(&self, task_id: &str) -> Result<Option<AgentEvent>>;
    async fn get_turn_steps(
        &self,
        task_id: &str,
        turn_id: i64,
    ) -> Result<Vec<StepExecution>>;

    // ── Recovery ────────────────────────────────────────────────────────

    async fn get_incomplete_turns(&self, task_id: &str)
        -> Result<Vec<IncompleteTurn>>;
    async fn get_incomplete_steps(
        &self,
        task_id: &str,
    ) -> Result<Vec<IncompleteStep>>;
    async fn detect_seq_gaps(&self, task_id: &str) -> Result<Vec<i64>>;
    async fn is_turn_seq_continuous(&self, task_id: &str, turn_id: i64)
        -> Result<bool>;

    // ── Stats ───────────────────────────────────────────────────────────

    async fn get_token_usage_stats(
        &self,
        task_id: &str,
    ) -> Result<Vec<TokenUsageStats>>;

    // ── Summary helpers ─────────────────────────────────────────────────

    async fn count_turns_after_seq(&self, task_id: &str, after_seq: i64) -> Result<i64>;
    async fn count_events_after_seq(&self, task_id: &str, after_seq: i64) -> Result<i64>;
    async fn get_llm_payloads_after_seq(
        &self,
        task_id: &str,
        after_seq: i64,
    ) -> Result<Vec<String>>;
    async fn get_recent_turn_ids(&self, task_id: &str, limit: i64) -> Result<Vec<i64>>;

    // ── Dispatch ────────────────────────────────────────────────────────

    /// 把 ready task 发布到 broker stream `tasks-{task_type}`,供 fixlet 订阅。
    /// 默认 no-op;BrokerEventStore 覆盖。
    async fn publish_ready_task(&self, _task_id: &str, _task_type: &str, _task_brief: &str, _preferred_claimant: Option<&str>) -> Result<()> {
        Ok(())
    }

    /// 把工具事件发到 sandbox dispatch stream(Plan D)。
    /// 默认 no-op;BrokerEventStore 覆盖。
    async fn dispatch_tool(&self, _task_id: &str, _event: &AgentEvent) -> Result<()> {
        Ok(())
    }

    // ── Archive ─────────────────────────────────────────────────────────

    async fn archive_events_before_seq(
        &self,
        task_id: &str,
        before_seq: i64,
    ) -> Result<ArchiveResult>;
}

// ── 归档结果 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ArchiveResult {
    pub archived: usize,
    pub path: String,
}

// ── LogdbdEventStore ────────────────────────────────────────────────────────

/// logdbd-backed Event Store。
///
/// 通过 gRPC 与 logdbd 通信。
/// - namespace = self.namespace(单 namespace;tenant_id 仅作 metadata 字段,查询时按字段过滤)
/// - stream   = task_id
/// - 简单读：`client.read()` / `client.scan_all()` — content 直接是 `Vec<u8>`
/// - 过滤查询：`client.query(QueryRequest)` — 原生结构化谓词,直读 segment(cr-027)
pub struct LogdbdEventStore {
    client: Arc<Mutex<Client>>,
    namespace: String,
}

#[cfg(test)]
impl LogdbdEventStore {
    /// 连接到 logdbd 服务器。
    pub async fn connect(addr: &str, namespace: &str) -> Result<Self> {
        let client = Client::connect(addr)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd connect: {}", e)))?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            namespace: namespace.to_string(),
        })
    }

    /// 执行一次结构化查询。调用方构造请求体(谓词 + result 形态);本方法填入
    /// `namespace = self.namespace`、`stream = stream`,并统一映射 gRPC 错误。
    async fn run_query(
        &self,
        client: &mut Client,
        stream: &str,
        mut req: QueryRequest,
    ) -> Result<QueryResponse> {
        req.namespace = self.namespace.clone();
        req.stream = stream.to_string();
        client
            .query(req)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))
    }

    /// 校验 terminal 事件唯一性:同一 session/turn/step 最多一个 terminal 事件。
    /// append 前调用,违反则返回 `LifecycleInvariant`。
    /// 覆盖:session_ended / 4 种 turn terminal / step llm terminal / step tool terminal。
    async fn check_terminal_uniqueness(
        &self,
        client: &mut Client,
        event: &AgentEvent,
    ) -> Result<()> {
        use EventType::*;
        let (conflict_types, scope): (Vec<&str>, &str) = match event.event_type {
            SessionEnded => (vec!["session_ended"], "session"),
            TaskSucceeded | TaskFailed | TaskCanceled => (
                vec!["task_succeeded", "task_failed", "task_canceled"],
                "task",
            ),
            TurnCompleted | TurnFailed | TurnCanceled | TurnBlocked => (
                vec!["turn_completed", "turn_failed", "turn_canceled", "turn_blocked"],
                "turn",
            ),
            LlmCompleted | LlmFailed => (vec!["llm_completed", "llm_failed"], "step"),
            ToolCompleted | ToolFailed => (vec!["tool_completed", "tool_failed"], "step"),
            // 非 terminal 事件不校验唯一性
            _ => return Ok(()),
        };

        // scope → metadata 等值过滤(session 级由 stream 本身限定,无需 metadata)。
        let mut metadata = Vec::new();
        match scope {
            "turn" => {
                let tid = event.turn_id.ok_or_else(|| {
                    AppError::LifecycleInvariant(format!(
                        "{} 缺 turn_id",
                        event.event_type.as_str()
                    ))
                })?;
                metadata.push(MetadataFilter {
                    key: "turn_id".into(),
                    value: tid.to_string(),
                });
            }
            "step" => {
                let sid = event.step_id.as_ref().ok_or_else(|| {
                    AppError::LifecycleInvariant(format!(
                        "{} 缺 step_id",
                        event.event_type.as_str()
                    ))
                })?;
                metadata.push(MetadataFilter {
                    key: "step_id".into(),
                    value: sid.clone(),
                });
            }
            _ => {} // session 级:stream 已限定 session
        }

        let resp = self
            .run_query(
                client,
                &event.task_id,
                QueryRequest {
                    event_types: conflict_types.into_iter().map(String::from).collect(),
                    metadata,
                    result: QueryResult::Count as i32,
                    ..Default::default()
                },
            )
            .await?;

        let count = match resp.result {
            Some(query_response::Result::Count(n)) => n,
            _ => 0,
        };
        if count > 0 {
            return Err(AppError::LifecycleInvariant(format!(
                "{} 已存在同类 terminal 事件(session={}, turn={:?}, step={:?})",
                event.event_type.as_str(),
                event.task_id,
                event.turn_id,
                event.step_id,
            )));
        }
        Ok(())
    }

    /// 一次查询读回全部 Task 级事件记录(7 种,有界 ≤~8 条)。
    /// 调用方据此判定存在性(task_created 是否在结果中)+ 派生状态,
    /// 避免为"存在性"和"head 事实"分别打 round-trip。
    async fn read_task_records(
        &self,
        client: &mut Client,
        task_id: &str,
    ) -> Result<Vec<logdb_client::Record>> {
        let task_events = [
            "task_created",
            "task_ready",
            "task_claimed",
            "task_blocked",
            "task_succeeded",
            "task_failed",
            "task_canceled",
        ];
        let resp = self
            .run_query(
                client,
                task_id,
                QueryRequest {
                    event_types: task_events.iter().map(|s| s.to_string()).collect(),
                    result: QueryResult::Records as i32,
                    ..Default::default()
                },
            )
            .await?;
        Ok(match resp.result {
            Some(query_response::Result::Records(r)) => r.records,
            _ => vec![],
        })
    }

    /// 从已读回的 Task 级记录派生状态(spec §4.4 projection)。
    ///
    /// - 最新 Task 迁移事件 → 基础态
    /// - 基础态 == Claimed 且存在 task_claimed 之后的 turn_started → Executing
    ///   (仅此分支触发一次条件 turn_started Max 查询;其余状态零额外查询)
    async fn derive_task_state(
        &self,
        client: &mut Client,
        records: &[logdb_client::Record],
        task_id: &str,
    ) -> Result<TaskState> {
        // 最新迁移事件(seq 最大者)
        let latest = records.iter().max_by_key(|r| r.seq);
        let base = match latest.map(|r| r.event_type.as_str()) {
            Some("task_created") => TaskState::Created,
            Some("task_ready") => TaskState::Ready,
            Some("task_claimed") => TaskState::Claimed,
            Some("task_blocked") => TaskState::Blocked,
            Some("task_succeeded") => TaskState::Succeeded,
            Some("task_failed") => TaskState::Failed,
            Some("task_canceled") => TaskState::Canceled,
            _ => TaskState::Created,
        };

        // Claimed → 检查是否有之后的 turn_started(派生 Executing)
        if base == TaskState::Claimed {
            let claimed_seq = latest.map(|r| r.seq).unwrap_or(0);
            let resp = self
                .run_query(
                    client,
                    task_id,
                    QueryRequest {
                        event_types: vec!["turn_started".into()],
                        result: QueryResult::Max as i32,
                        ..Default::default()
                    },
                )
                .await?;
            if let Some(query_response::Result::Max(ts_seq)) = resp.result {
                if (ts_seq as i64) > (claimed_seq as i64) {
                    return Ok(TaskState::Executing);
                }
            }
        }

        Ok(base)
    }
}

// ── 辅助函数 ────────────────────────────────────────────────────────────────

/// AgentEvent → logdbd metadata HashMap
fn event_meta(event: &AgentEvent) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("task_id".into(), event.task_id.clone());
    if let Some(tid) = event.turn_id {
        m.insert("turn_id".into(), tid.to_string());
    }
    if let Some(ref sid) = event.step_id {
        m.insert("step_id".into(), sid.clone());
    }
    m.insert("schema_version".into(), event.schema_version.to_string());
    m
}

/// proto Record → AgentEvent（content 是原始 bytes，无 hex 编解码）
fn event_from_record(rec: &logdb_client::Record, task_id: &str) -> Result<AgentEvent> {
    let event_type =
        EventType::from_str(&rec.event_type).ok_or_else(|| {
            AppError::InvalidEventType(rec.event_type.clone())
        })?;

    let payload: serde_json::Value = if rec.content.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&rec.content).unwrap_or_default()
    };

    let turn_id = rec
        .metadata
        .get("turn_id")
        .and_then(|s| s.parse().ok());
    let step_id = rec.metadata.get("step_id").cloned();
    let schema_version = rec
        .metadata
        .get("schema_version")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let created_at = Utc.timestamp_nanos(rec.timestamp_ns as i64);

    Ok(AgentEvent {
        task_id: task_id.to_string(),
        seq: rec.seq as i64,
        turn_id,
        step_id,
        event_type,
        schema_version,
        payload,
        created_at,
    })
}

// ── EventStore for LogdbdEventStore ─────────────────────────────────────────

#[async_trait]
#[cfg(test)]
impl EventStore for LogdbdEventStore {
    // ── Session ─────────────────────────────────────────────────────────

    async fn create_task(
        &self,
        task_type: &str,
        tenant_id: &str,
        user_id: &str,
        provenance: &Provenance,
        body: Option<&serde_json::Value>,
    ) -> Result<(String, AgentEvent)> {
        // fixus 分配 task_id(UUIDv7,全局唯一单调)— spec §8.4
        let task_id = format!(
            "task_{}",
            uuid::Uuid::now_v7().to_string().replace('-', "")
        );

        let payload = serde_json::json!({
            "task_type": task_type,
            "provenance": provenance,
            "body": body.cloned().unwrap_or(serde_json::Value::Null),
        });
        let content = serde_json::to_vec(&payload)
            .map_err(|e| AppError::Internal(format!("json: {}", e)))?;

        let mut meta = HashMap::new();
        meta.insert("task_id".into(), task_id.clone());
        meta.insert("tenant_id".into(), tenant_id.to_string());
        meta.insert("user_id".into(), user_id.to_string());
        meta.insert("task_type".into(), task_type.to_string());

        let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        let mut client = self.client.lock().await;
        let resp = client
            .append_full(
                &self.namespace,
                &task_id,
                "task_created",
                "application/json",
                &meta,
                ts_ns,
                &content,
            )
            .await
            .map_err(|e| AppError::Internal(format!("logdbd append: {}", e)))?;

        let event = AgentEvent {
            task_id: task_id.clone(),
            seq: resp.seq as i64,
            turn_id: None,
            step_id: None,
            event_type: EventType::TaskCreated,
            schema_version: 1,
            payload,
            created_at: Utc::now(),
        };

        Ok((task_id, event))
    }

    async fn get_task(&self, task_id: &str) -> Result<Option<Task>> {
        let mut client = self.client.lock().await;

        // 一次查询读回全部 Task 级事件(head 事实 + 派生状态共用,省 round-trip)
        let records = self.read_task_records(&mut client, task_id).await?;
        let Some(rec) = records.iter().find(|r| r.event_type == "task_created") else {
            return Ok(None);
        };

        let payload: serde_json::Value = if rec.content.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&rec.content).unwrap_or_default()
        };

        let provenance: Provenance = serde_json::from_value(
            payload
                .get("provenance")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        )
        .unwrap_or_else(|_| Provenance {
            source_channel: "unknown".into(),
            source_session_id: None,
            source_user_id: None,
            source_tenant_id: None,
            source_message_id: None,
            created_at: Utc::now(),
            created_by: "unknown".into(),
        });

        let tenant_id = rec
            .metadata
            .get("tenant_id")
            .cloned()
            .or_else(|| provenance.source_tenant_id.clone())
            .unwrap_or_else(|| "default".into());
        let user_id = rec
            .metadata
            .get("user_id")
            .cloned()
            .or_else(|| provenance.source_user_id.clone())
            .unwrap_or_default();

        // 派生当前 state(spec §4.4 projection;复用同一批 records)
        let state = self.derive_task_state(&mut client, &records, task_id).await?;

        Ok(Some(Task {
            task_id: task_id.to_string(),
            tenant_id,
            user_id,
            task_type: payload["task_type"]
                .as_str()
                .unwrap_or("unknown")
                .to_string(),
            state,
            provenance,
            body: payload.get("body").filter(|v| !v.is_null()).cloned(),
            created_at: Utc::now(),
            metadata: None,
        }))
    }

    async fn task_exists(&self, task_id: &str) -> Result<bool> {
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec!["task_created".into()],
                    result: QueryResult::Exists as i32,
                    ..Default::default()
                },
            )
            .await?;
        Ok(matches!(
            resp.result,
            Some(query_response::Result::Exists(true))
        ))
    }

    async fn is_task_ended(&self, task_id: &str) -> Result<bool> {
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec![
                        "task_succeeded".into(),
                        "task_failed".into(),
                        "task_canceled".into(),
                    ],
                    result: QueryResult::Exists as i32,
                    ..Default::default()
                },
            )
            .await?;
        Ok(matches!(
            resp.result,
            Some(query_response::Result::Exists(true))
        ))
    }

    async fn get_task_state(&self, task_id: &str) -> Result<Option<TaskState>> {
        let mut client = self.client.lock().await;
        // 一次查询:存在性(task_created 在结果中)+ 派生状态,共用同一批 records
        let records = self.read_task_records(&mut client, task_id).await?;
        if !records.iter().any(|r| r.event_type == "task_created") {
            return Ok(None);
        }
        self.derive_task_state(&mut client, &records, task_id)
            .await
            .map(Some)
    }

    // ── Seq ─────────────────────────────────────────────────────────────

    async fn get_max_seq(&self, task_id: &str) -> Result<i64> {
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    result: QueryResult::Max as i32, // aggregate_field="" ⇒ seq
                    ..Default::default()
                },
            )
            .await?;
        Ok(match resp.result {
            Some(query_response::Result::Max(n)) => n as i64,
            _ => 0,
        })
    }

    async fn get_max_turn_id(&self, task_id: &str) -> Result<i64> {
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    aggregate_field: "turn_id".into(),
                    result: QueryResult::Max as i32,
                    ..Default::default()
                },
            )
            .await?;
        Ok(match resp.result {
            Some(query_response::Result::Max(n)) => n as i64,
            _ => 0,
        })
    }

    // ── Write ───────────────────────────────────────────────────────────

    async fn write_event(&self, event: &AgentEvent) -> Result<i64> {
        event
            .validate_scope()
            .map_err(|msg| AppError::LifecycleInvariant(msg))?;
        crate::models::validate_payload_required_fields(
            &event.event_type,
            &event.payload,
        )?;

        let content = serde_json::to_vec(&event.payload)
            .map_err(|e| AppError::Internal(format!("json: {}", e)))?;
        let meta = event_meta(event);
        let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        let mut client = self.client.lock().await;
        self.check_terminal_uniqueness(&mut client, event).await?;

        let resp = client
            .append_full(
                &self.namespace,
                &event.task_id,
                event.event_type.as_str(),
                "application/json",
                &meta,
                ts_ns,
                &content,
            )
            .await
            .map_err(|e| AppError::Internal(format!("logdbd append: {}", e)))?;

        Ok(resp.seq as i64)
    }

    async fn write_events_batch(
        &self,
        events: &[AgentEvent],
    ) -> Result<Vec<i64>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        let mut requests = Vec::with_capacity(events.len());
        let mut client = self.client.lock().await;

        // 校验 + 构造请求(per-event 校验仍查 DB)
        for event in events {
            event
                .validate_scope()
                .map_err(|msg| AppError::LifecycleInvariant(msg))?;
            crate::models::validate_payload_required_fields(
                &event.event_type,
                &event.payload,
            )?;
            self.check_terminal_uniqueness(&mut client, event).await?;

            let content = serde_json::to_vec(&event.payload)
                .map_err(|e| AppError::Internal(format!("json: {}", e)))?;
            let meta = event_meta(event);
            let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

            requests.push(AppendRequest {
                namespace: self.namespace.clone(),
                stream: event.task_id.clone(),
                shard_key: Some(event.task_id.clone()),
                event_type: event.event_type.as_str().to_string(),
                content_type: "application/json".to_string(),
                metadata: meta,
                timestamp_ns: ts_ns,
                content,
            });
        }

        // 原子批写:全部成功或全部失败(logdbd batch_append)
        let resp = client
            .append_batch(requests)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd append_batch: {}", e)))?;

        if let Some(err) = resp.error.as_ref() {
            return Err(AppError::Internal(format!(
                "logdbd append_batch error: {} — {} (retryable={})",
                err.code, err.message, err.retryable
            )));
        }

        Ok(resp.records.iter().map(|r| r.seq as i64).collect())
    }

    // ── Read（优化：简单读走原生 gRPC，零 hex 开销）───────────────────

    async fn get_event(
        &self,
        task_id: &str,
        seq: i64,
    ) -> Result<Option<AgentEvent>> {
        let mut client = self.client.lock().await;
        match client
            .read(&self.namespace, task_id, seq as u64)
            .await
        {
            Ok(Some(rec)) => Ok(Some(event_from_record(&rec, task_id)?)),
            Ok(None) => Ok(None),
            Err(e) => Err(AppError::Internal(format!(
                "logdbd read: {}",
                e
            ))),
        }
    }

    /// 原生 gRPC scan — 不需要 SQL，content 直接是 bytes
    async fn get_events_after_seq(
        &self,
        task_id: &str,
        after_seq: i64,
    ) -> Result<Vec<AgentEvent>> {
        let from = (after_seq + 1).max(1) as u64;
        let mut client = self.client.lock().await;
        let records = client
            .scan_all(&self.namespace, task_id, from)
            .await
            .map_err(|e| {
                AppError::Internal(format!("logdbd scan: {}", e))
            })?;

        // 客户端过滤：只保留 Turn 级和 Step 级业务事件
        const KEEP_TYPES: &[&str] = &[
            "turn_started",
            "turn_completed",
            "turn_failed",
            "turn_canceled",
            "turn_blocked",
            "llm_invoked",
            "llm_completed",
            "llm_failed",
            "tool_invoked",
            "tool_completed",
            "tool_failed",
        ];

        records
            .iter()
            .filter(|r| KEEP_TYPES.contains(&r.event_type.as_str()))
            .map(|r| event_from_record(r, task_id))
            .collect()
    }

    // ── Read（过滤查询走 SQL）──────────────────────────────────────────

    async fn get_turn_events(
        &self,
        task_id: &str,
        turn_id: i64,
    ) -> Result<Vec<AgentEvent>> {
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    metadata: vec![MetadataFilter {
                        key: "turn_id".into(),
                        value: turn_id.to_string(),
                    }],
                    result: QueryResult::Records as i32,
                    ..Default::default() // ascending by seq
                },
            )
            .await?;
        let recs = match resp.result {
            Some(query_response::Result::Records(r)) => r.records,
            _ => vec![],
        };
        recs.iter().map(|r| event_from_record(r, task_id)).collect()
    }

    async fn get_latest_summary(
        &self,
        task_id: &str,
    ) -> Result<Option<AgentEvent>> {
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec!["summary_marker".into()],
                    result: QueryResult::Records as i32,
                    limit: 1,
                    descending: true, // latest
                    ..Default::default()
                },
            )
            .await?;
        match resp.result {
            Some(query_response::Result::Records(r)) => match r.records.into_iter().next() {
                Some(rec) => Ok(Some(event_from_record(&rec, task_id)?)),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }

    async fn get_turn_steps(
        &self,
        task_id: &str,
        turn_id: i64,
    ) -> Result<Vec<StepExecution>> {
        // 原实现是 records 自连接(按 step_id 把 invoked 配对到 completed/failed)。
        // 结构化查询无 JOIN,故拉取该 turn 全部相关记录后客户端配对。
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    metadata: vec![MetadataFilter {
                        key: "turn_id".into(),
                        value: turn_id.to_string(),
                    }],
                    event_types: vec![
                        "llm_invoked".into(),
                        "tool_invoked".into(),
                        "llm_completed".into(),
                        "llm_failed".into(),
                        "tool_completed".into(),
                        "tool_failed".into(),
                    ],
                    result: QueryResult::Records as i32,
                    ..Default::default() // ascending by seq
                },
            )
            .await?;
        let recs = match resp.result {
            Some(query_response::Result::Records(r)) => r.records,
            _ => vec![],
        };

        // terminal(completion/failure)按 step_id 索引;原 SQL 是 INNER JOIN,
        // 故只保留有 terminal 配对的 invoke(未配对的 invoke 见 get_incomplete_steps)。
        let mut terminals: HashMap<String, &logdb_client::Record> = HashMap::new();
        for r in &recs {
            if matches!(
                r.event_type.as_str(),
                "llm_completed" | "llm_failed" | "tool_completed" | "tool_failed"
            ) {
                if let Some(sid) = r.metadata.get("step_id") {
                    terminals.entry(sid.clone()).or_insert(r);
                }
            }
        }

        let mut steps = Vec::new();
        for r in &recs {
            if !matches!(r.event_type.as_str(), "llm_invoked" | "tool_invoked") {
                continue;
            }
            let step_id = r.metadata.get("step_id").cloned().unwrap_or_default();
            let Some(end) = terminals.get(&step_id) else {
                continue; // INNER JOIN:无 terminal 配对则丢弃
            };

            // step_type 在 invoke 的 payload 里(metadata 不含它)。
            let payload: serde_json::Value = if r.content.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_slice(&r.content).unwrap_or_default()
            };
            let step_type = payload
                .get("step_type")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());

            let started_ts = r.timestamp_ns as i64;
            let ended_ts = end.timestamp_ns as i64;
            let duration_ms = if ended_ts > started_ts {
                Some(((ended_ts - started_ts) as f64) / 1_000_000.0)
            } else {
                None
            };
            steps.push(StepExecution {
                step_id,
                step_type,
                started_at: Utc.timestamp_nanos(started_ts),
                ended_at: Some(Utc.timestamp_nanos(ended_ts)),
                end_event: Some(end.event_type.clone()),
                duration_ms,
            });
        }
        Ok(steps)
    }

    // ── Recovery ────────────────────────────────────────────────────────

    async fn get_incomplete_turns(
        &self,
        task_id: &str,
    ) -> Result<Vec<IncompleteTurn>> {
        // 反连接:turn_started 且同 turn_id 无任何 terminal(turn_completed/…/blocked)。
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec!["turn_started".into()],
                    absent: Some(AbsentMatch {
                        peer_event_types: vec![
                            "turn_completed".into(),
                            "turn_failed".into(),
                            "turn_canceled".into(),
                            "turn_blocked".into(),
                        ],
                        join_key: "turn_id".into(),
                    }),
                    result: QueryResult::Records as i32,
                    ..Default::default()
                },
            )
            .await?;
        let recs = match resp.result {
            Some(query_response::Result::Records(r)) => r.records,
            _ => vec![],
        };

        let mut turns = Vec::new();
        for r in &recs {
            let turn_id = r
                .metadata
                .get("turn_id")
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            // redo_group/redo_count 在 turn_started 的 payload 里。
            let payload: serde_json::Value = if r.content.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_slice(&r.content).unwrap_or_default()
            };
            turns.push(IncompleteTurn {
                turn_id,
                redo_group: payload["redo_group"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_string(),
                redo_count: payload["redo_count"].as_i64().unwrap_or(0) as i32,
                turn_started_at: Utc.timestamp_nanos(r.timestamp_ns as i64),
            });
        }
        // 原 SQL:ORDER BY CAST(tid AS INTEGER) ASC。
        turns.sort_by_key(|t| t.turn_id);
        Ok(turns)
    }

    async fn get_incomplete_steps(
        &self,
        task_id: &str,
    ) -> Result<Vec<IncompleteStep>> {
        // 反连接:llm/tool_invoked 且同 step_id 无任何 terminal。
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec!["llm_invoked".into(), "tool_invoked".into()],
                    absent: Some(AbsentMatch {
                        peer_event_types: vec![
                            "llm_completed".into(),
                            "llm_failed".into(),
                            "tool_completed".into(),
                            "tool_failed".into(),
                        ],
                        join_key: "step_id".into(),
                    }),
                    result: QueryResult::Records as i32,
                    ..Default::default() // ascending by seq
                },
            )
            .await?;
        let recs = match resp.result {
            Some(query_response::Result::Records(r)) => r.records,
            _ => vec![],
        };

        recs.iter()
            .map(|r| {
                let payload: serde_json::Value = if r.content.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&r.content).unwrap_or_default()
                };
                Ok(IncompleteStep {
                    seq: r.seq as i64,
                    turn_id: r
                        .metadata
                        .get("turn_id")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0),
                    step_id: r.metadata.get("step_id").cloned().unwrap_or_default(),
                    start_event_type: r.event_type.clone(),
                    payload,
                    started_at: Utc.timestamp_nanos(r.timestamp_ns as i64),
                })
            })
            .collect()
    }

    async fn detect_seq_gaps(
        &self,
        task_id: &str,
    ) -> Result<Vec<i64>> {
        // 找 seq 空洞:对每个 seq < MAX(seq),若 seq+1 不存在则报告 seq+1。
        // 结构化查询无此谓词,拉取全部记录后客户端计算。
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    result: QueryResult::Records as i32,
                    ..Default::default()
                },
            )
            .await?;
        let recs = match resp.result {
            Some(query_response::Result::Records(r)) => r.records,
            _ => vec![],
        };

        let mut seqs: Vec<u64> = recs.iter().map(|r| r.seq).collect();
        seqs.sort_unstable();
        let set: std::collections::HashSet<u64> = seqs.iter().copied().collect();
        let max_seq = *seqs.last().unwrap_or(&0);

        let mut gaps = Vec::new();
        for &s in seqs.iter().filter(|&&s| s < max_seq) {
            if !set.contains(&(s + 1)) {
                gaps.push((s + 1) as i64);
            }
        }
        Ok(gaps)
    }

    async fn is_turn_seq_continuous(
        &self,
        task_id: &str,
        turn_id: i64,
    ) -> Result<bool> {
        let gaps = self.detect_seq_gaps(task_id).await?;
        let mut client = self.client.lock().await;
        let meta = vec![MetadataFilter {
            key: "turn_id".into(),
            value: turn_id.to_string(),
        }];
        // 该 turn 的 seq 上下界(原 SQL 一次 MIN/MAX;结构化查询每次一个聚合,发两次)。
        let lo = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    metadata: meta.clone(),
                    result: QueryResult::Min as i32,
                    ..Default::default()
                },
            )
            .await?;
        let hi = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    metadata: meta,
                    result: QueryResult::Max as i32,
                    ..Default::default()
                },
            )
            .await?;
        let lo = match lo.result {
            Some(query_response::Result::Min(n)) => n as i64,
            _ => 0,
        };
        let hi = match hi.result {
            Some(query_response::Result::Max(n)) => n as i64,
            _ => 0,
        };
        // turn 无记录 ⇒ lo=hi=0;seq 从 1 起,gap 不会落入 [0,0] ⇒ 自然返回 true。
        Ok(!gaps.iter().any(|g| *g >= lo && *g <= hi))
    }

    // ── Stats ───────────────────────────────────────────────────────────

    async fn get_token_usage_stats(
        &self,
        task_id: &str,
    ) -> Result<Vec<TokenUsageStats>> {
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec!["llm_completed".into()],
                    result: QueryResult::Records as i32,
                    ..Default::default()
                },
            )
            .await?;
        let recs = match resp.result {
            Some(query_response::Result::Records(r)) => r.records,
            _ => vec![],
        };

        // 按 model 客户端聚合(SQL 原本只拉 content 行,聚合也在客户端)。
        let mut map: HashMap<String, TokenUsageStats> = HashMap::new();
        for r in &recs {
            if r.content.is_empty() {
                continue;
            }
            let payload: serde_json::Value =
                serde_json::from_slice(&r.content).unwrap_or_default();
            let model = payload
                .get("model")
                .and_then(|x| x.as_str())
                .unwrap_or("unknown")
                .to_string();
            let usage = payload.get("usage");
            let prompt = usage
                .and_then(|u| u.get("prompt_tokens"))
                .and_then(|x| x.as_i64())
                .unwrap_or(0);
            let completion = usage
                .and_then(|u| u.get("completion_tokens"))
                .and_then(|x| x.as_i64())
                .unwrap_or(0);

            let e = map.entry(model.clone()).or_insert_with(|| {
                TokenUsageStats {
                    model: model.clone(),
                    call_count: 0,
                    prompt_tokens: 0,
                    completion_tokens: 0,
                }
            });
            e.call_count += 1;
            e.prompt_tokens += prompt;
            e.completion_tokens += completion;
        }
        let mut stats: Vec<_> = map.into_values().collect();
        stats.sort_by(|a, b| a.model.cmp(&b.model));
        Ok(stats)
    }

    // ── Summary helpers ─────────────────────────────────────────────────

    async fn count_turns_after_seq(
        &self,
        task_id: &str,
        after_seq: i64,
    ) -> Result<i64> {
        // SQL `seq > N` ⇒ from_seq = N+1(引擎 from_seq 含端点);turn_id 非空由
        // 聚合跳过缺该字段的记录保证。COUNT(DISTINCT turn_id) where turn_started。
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec!["turn_started".into()],
                    from_seq: Some((after_seq + 1).max(1) as u64),
                    aggregate_field: "turn_id".into(),
                    result: QueryResult::CountDistinct as i32,
                    ..Default::default()
                },
            )
            .await?;
        Ok(match resp.result {
            Some(query_response::Result::CountDistinct(n)) => n as i64,
            _ => 0,
        })
    }

    async fn count_events_after_seq(
        &self,
        task_id: &str,
        after_seq: i64,
    ) -> Result<i64> {
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec![
                        "turn_pending".into(),
                        "turn_started".into(),
                        "turn_completed".into(),
                        "turn_failed".into(),
                        "turn_canceled".into(),
                        "turn_blocked".into(),
                        "llm_invoked".into(),
                        "llm_completed".into(),
                        "llm_failed".into(),
                        "tool_invoked".into(),
                        "tool_completed".into(),
                        "tool_failed".into(),
                    ],
                    from_seq: Some((after_seq + 1).max(1) as u64),
                    result: QueryResult::Count as i32,
                    ..Default::default()
                },
            )
            .await?;
        Ok(match resp.result {
            Some(query_response::Result::Count(n)) => n as i64,
            _ => 0,
        })
    }

    async fn get_llm_payloads_after_seq(
        &self,
        task_id: &str,
        after_seq: i64,
    ) -> Result<Vec<String>> {
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec!["llm_completed".into()],
                    from_seq: Some((after_seq + 1).max(1) as u64),
                    result: QueryResult::Records as i32,
                    ..Default::default()
                },
            )
            .await?;
        let recs = match resp.result {
            Some(query_response::Result::Records(r)) => r.records,
            _ => vec![],
        };
        Ok(recs
            .iter()
            .filter(|r| !r.content.is_empty())
            .map(|r| String::from_utf8_lossy(&r.content).to_string())
            .collect())
    }

    async fn get_recent_turn_ids(
        &self,
        task_id: &str,
        limit: i64,
    ) -> Result<Vec<i64>> {
        // 引擎 DISTINCT_VALUES 按 seq 返回字符串;原 SQL 要数值 DESC LIMIT n,故客户端排序截断。
        let mut client = self.client.lock().await;
        let resp = self
            .run_query(
                &mut client,
                task_id,
                QueryRequest {
                    event_types: vec!["turn_started".into()],
                    aggregate_field: "turn_id".into(),
                    result: QueryResult::DistinctValues as i32,
                    ..Default::default()
                },
            )
            .await?;
        let values = match resp.result {
            Some(query_response::Result::DistinctValues(d)) => d.values,
            _ => vec![],
        };
        let mut ids: Vec<i64> = values
            .iter()
            .filter_map(|s| s.parse::<i64>().ok())
            .collect();
        ids.sort_unstable_by(|a, b| b.cmp(a)); // DESC
        ids.truncate(limit.max(0) as usize);
        Ok(ids)
    }

    // ── Archive ─────────────────────────────────────────────────────────

    async fn archive_events_before_seq(
        &self,
        _task_id: &str,
        before_seq: i64,
    ) -> Result<ArchiveResult> {
        tracing::info!(
            "logdbd: archive before_seq={} (no-op — retention managed by logdbd)",
            before_seq
        );
        Ok(ArchiveResult {
            archived: 0,
            path: String::new(),
        })
    }
}

// ── 集成测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod logdbd_tests {
    //! LogdbdEventStore 集成测试 — 起真实 logdbd(内嵌 lib)验证 EventStore 契约。
    //!
    //! cr-027 后 query() 直读 segment 的 committed cursor(无 SQLite/无 Indexer)。
    //! append 返回后 committed 游标在亚毫秒级推进,故每次 append 后用 `wait_seq`
    //! 轮询 get_max_seq 追平,再断言/写下一个依赖事件。

    use super::{EventStore, LogdbdEventStore};
    use crate::error::AppError;
    use crate::models::{AgentEvent, EventType};

    use logdbd::catalog::Catalog;
    use logdbd::consumer::ConsumerTracker;
    use logdbd::pb::log_db_service_server::LogDbServiceServer;
    use logdbd::service::LogDbServiceImpl;
    use logdbd::storage::Storage;
    use logdbd::subscribe::SubscribeHub;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    const NS: &str = "fixus-test";

    fn test_storage(dir: &std::path::Path) -> Storage {
        let mut cfg = logdb::Config::default();
        cfg.data_dir = dir.to_path_buf();
        cfg.durability_mode = logdb::DurabilityMode::Sync;
        cfg.ring_size = 256;
        cfg.shards = 1;
        cfg.flush_timeout = Duration::from_secs(5);
        let db = logdb::LogDb::open(cfg).unwrap();
        Storage::new(db, 1)
    }

    /// 起一个真实 logdbd gRPC server(cr-027 后无 Indexer/SQLite;query 直读 segment)。
    /// 返回 (addr, tempdir);tempdir 必须在测试期间存活(持有 data_dir)。
    async fn start_server() -> (String, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();

        let storage = Arc::new(test_storage(dir.path()));
        let catalog = Arc::new(Catalog::open(dir.path()).unwrap());
        let subscribe_hub = Arc::new(SubscribeHub::new());
        let consumer_tracker = Arc::new(ConsumerTracker::new(None));

        let svc = LogDbServiceImpl::new(
            Arc::clone(&storage),
            Arc::clone(&catalog),
            Arc::clone(&consumer_tracker),
            Arc::clone(&subscribe_hub),
            "test-node".into(),
            "primary".into(),
        );
        let svc = LogDbServiceServer::new(svc);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        (addr, dir)
    }

    async fn setup() -> (LogdbdEventStore, tempfile::TempDir) {
        let (addr, dir) = start_server().await;
        let store = LogdbdEventStore::connect(&addr, NS).await.unwrap();
        (store, dir)
    }

    /// 轮询直到 committed cursor 的 max_seq >= expected。
    /// append 返回后 committed 游标推进有亚毫秒级延迟,这是测试与 logdbd 的同步点。
    async fn wait_seq(store: &LogdbdEventStore, sid: &str, expected: i64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match store.get_max_seq(sid).await {
                Ok(v) if v >= expected => return,
                Ok(_) => {}
                Err(_) => {} // committed 游标尚未推进 → 继续等
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "cache did not catch up to seq {} for {} (last={:?})",
                    expected,
                    sid,
                    store.get_max_seq(sid).await
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn test_provenance() -> crate::models::Provenance {
        crate::models::Provenance {
            source_channel: "api".into(),
            source_session_id: None,
            source_user_id: Some("u".into()),
            source_tenant_id: Some("t".into()),
            source_message_id: None,
            created_at: chrono::Utc::now(),
            created_by: "test".into(),
        }
    }

    /// 创建测试 Task,返回 fixus 分配的 task_id(create_task 新签名:fixus 分配 id)。
    async fn create_test_task(store: &LogdbdEventStore, task_type: &str) -> String {
        let prov = test_provenance();
        let (tid, _ev) = store
            .create_task(task_type, "t", "u", &prov, None)
            .await
            .unwrap();
        tid
    }

    // ── Session 生命周期 ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_task_round_trip() {
        let (store, _dir) = setup().await;
        let prov = crate::models::Provenance {
            source_channel: "api".into(),
            source_session_id: None,
            source_user_id: Some("user-1".into()),
            source_tenant_id: Some("tenant-a".into()),
            source_message_id: None,
            created_at: chrono::Utc::now(),
            created_by: "test".into(),
        };
        let (tid, ev) = store
            .create_task("claude-code", "tenant-a", "user-1", &prov, None)
            .await
            .unwrap();
        assert_eq!(ev.event_type, EventType::TaskCreated);
        assert_eq!(ev.seq, 1);
        assert_eq!(ev.task_id, tid);
        assert!(tid.starts_with("task_"), "fixus-assigned id: {}", tid);

        wait_seq(&store, &tid, 1).await;
        assert!(store.task_exists(&tid).await.unwrap());

        let s = store.get_task(&tid).await.unwrap().unwrap();
        assert_eq!(s.task_id, tid);
        assert_eq!(s.tenant_id, "tenant-a");
        assert_eq!(s.user_id, "user-1");
        assert_eq!(s.task_type, "claude-code");
        assert_eq!(s.state, crate::models::TaskState::Created);
        assert!(!store.is_task_ended(&tid).await.unwrap());
    }

    #[tokio::test]
    async fn task_terminal_detected() {
        let (store, _dir) = setup().await;
        let sid = create_test_task(&store, "a").await;
        let sid = sid.as_str();
        wait_seq(&store, sid, 1).await;
        assert!(!store.is_task_ended(sid).await.unwrap());

        // task_canceled(终态)→ is_task_ended true
        let end = AgentEvent::new(
            sid.into(),
            None,
            None,
            EventType::TaskCanceled,
            serde_json::json!({"reason": "done"}),
        );
        assert_eq!(store.write_event(&end).await.unwrap(), 2);
        wait_seq(&store, sid, 2).await;
        assert!(store.is_task_ended(sid).await.unwrap());
    }

    // ── 终态唯一性(B2)──────────────────────────────────────────────────────

    #[tokio::test]
    async fn terminal_uniqueness_rejects_duplicate_turn_terminal() {
        let (store, _dir) = setup().await;
        let sid = create_test_task(&store, "a").await;
        let sid = sid.as_str();
        wait_seq(&store, sid, 1).await;

        let ts = AgentEvent::new(
            sid.into(),
            Some(1),
            None,
            EventType::TurnStarted,
            serde_json::json!({"user_input": "hi", "redo_group": "rg1", "redo_count": 0}),
        );
        store.write_event(&ts).await.unwrap();
        wait_seq(&store, sid, 2).await;

        // 首个 turn terminal(completed)写入成功
        let tc = AgentEvent::new(
            sid.into(),
            Some(1),
            None,
            EventType::TurnCompleted,
            serde_json::json!({"final_output": "done"}),
        );
        store.write_event(&tc).await.unwrap();
        wait_seq(&store, sid, 3).await;

        // 第二个 turn terminal(failed,同 turn_id)→ LifecycleInvariant
        let tf = AgentEvent::new(
            sid.into(),
            Some(1),
            None,
            EventType::TurnFailed,
            serde_json::json!({"error_type": "boom", "error_message": "x"}),
        );
        let err = store.write_event(&tf).await.unwrap_err();
        assert!(
            matches!(err, AppError::LifecycleInvariant(_)),
            "expected LifecycleInvariant, got {:?}",
            err
        );
    }

    #[tokio::test]
    async fn terminal_uniqueness_rejects_duplicate_step_terminal() {
        let (store, _dir) = setup().await;
        let sid = create_test_task(&store, "a").await;
        let sid = sid.as_str();
        wait_seq(&store, sid, 1).await;

        let inv = AgentEvent::new(
            sid.into(),
            Some(1),
            Some("step-1".into()),
            EventType::LlmInvoked,
            serde_json::json!({"step_type":"llm_call","model":"gpt-4","messages":[],"local_seq":1}),
        );
        store.write_event(&inv).await.unwrap();
        wait_seq(&store, sid, 2).await;

        let comp = AgentEvent::new(
            sid.into(),
            Some(1),
            Some("step-1".into()),
            EventType::LlmCompleted,
            serde_json::json!({"model":"gpt-4","local_seq":1,"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}),
        );
        store.write_event(&comp).await.unwrap();
        wait_seq(&store, sid, 3).await;

        // llm_failed 与已写入的 llm_completed 同 step_id → LifecycleInvariant
        let fail = AgentEvent::new(
            sid.into(),
            Some(1),
            Some("step-1".into()),
            EventType::LlmFailed,
            serde_json::json!({"error_type":"x","error_message":"y","local_seq":1}),
        );
        let err = store.write_event(&fail).await.unwrap_err();
        assert!(
            matches!(err, AppError::LifecycleInvariant(_)),
            "expected LifecycleInvariant, got {:?}",
            err
        );
    }

    // ── get_turn_steps(回归:SQL 曾误引 records.step_id 列,见审计 E2)────────

    #[tokio::test]
    async fn get_turn_steps_pairs_invoked_completed() {
        let (store, _dir) = setup().await;
        let sid = create_test_task(&store, "a").await;
        let sid = sid.as_str();
        wait_seq(&store, sid, 1).await;

        // step-1: llm_invoked → llm_completed
        let inv1 = AgentEvent::new(
            sid.into(),
            Some(1),
            Some("step-1".into()),
            EventType::LlmInvoked,
            serde_json::json!({"step_type":"llm_call","model":"gpt-4","messages":[],"local_seq":1}),
        );
        store.write_event(&inv1).await.unwrap();
        wait_seq(&store, sid, 2).await;
        let comp1 = AgentEvent::new(
            sid.into(),
            Some(1),
            Some("step-1".into()),
            EventType::LlmCompleted,
            serde_json::json!({"model":"gpt-4","local_seq":1,"content":"ok"}),
        );
        store.write_event(&comp1).await.unwrap();
        wait_seq(&store, sid, 3).await;

        // step-2: tool_invoked → tool_completed
        let inv2 = AgentEvent::new(
            sid.into(),
            Some(1),
            Some("step-2".into()),
            EventType::ToolInvoked,
            serde_json::json!({"step_type":"tool_call","tool_name":"Bash","tool_call_id":"c1","idempotency_key":"k","input":{},"local_seq":2}),
        );
        store.write_event(&inv2).await.unwrap();
        wait_seq(&store, sid, 4).await;
        let comp2 = AgentEvent::new(
            sid.into(),
            Some(1),
            Some("step-2".into()),
            EventType::ToolCompleted,
            serde_json::json!({"tool_call_id":"c1","output":{},"is_error":false,"local_seq":2}),
        );
        store.write_event(&comp2).await.unwrap();
        wait_seq(&store, sid, 5).await;

        let steps = store.get_turn_steps(sid, 1).await.unwrap();
        assert_eq!(steps.len(), 2, "应有 2 个 step(1 llm + 1 tool),got {}", steps.len());
        // ORDER BY e_start.seq ASC → step-1 在前
        assert_eq!(steps[0].step_id, "step-1");
        assert_eq!(steps[0].end_event.as_deref(), Some("llm_completed"));
        assert_eq!(steps[0].step_type.as_deref(), Some("llm_call"));
        assert!(steps[0].ended_at.is_some(), "ended_at 应存在");
        assert_eq!(steps[1].step_id, "step-2");
        assert_eq!(steps[1].end_event.as_deref(), Some("tool_completed"));
        assert_eq!(steps[1].step_type.as_deref(), Some("tool_call"));

        // 过滤 turn_id:查不存在的 turn 应返回空
        assert!(store.get_turn_steps(sid, 99).await.unwrap().is_empty());
    }

    // ── Seq 连续性 ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn seq_monotonic_and_no_gaps() {
        let (store, _dir) = setup().await;
        let prov = test_provenance();
        let (sid, ev) = store
            .create_task("a", "t", "u", &prov, None)
            .await
            .unwrap();
        assert_eq!(ev.seq, 1, "task_created seq must be 1");
        let sid = sid.as_str();

        for tid in 1..=3i64 {
            let e = AgentEvent::new(
                sid.into(),
                Some(tid),
                None,
                EventType::TurnStarted,
                serde_json::json!({
                    "user_input": format!("t{}", tid),
                    "redo_group": format!("rg{}", tid),
                    "redo_count": 0,
                }),
            );
            let seq = store.write_event(&e).await.unwrap();
            assert_eq!(seq, tid + 1, "seq must be contiguous");
        }

        wait_seq(&store, sid, 4).await;
        assert_eq!(store.get_max_seq(sid).await.unwrap(), 4);
        assert_eq!(store.get_max_turn_id(sid).await.unwrap(), 3);
        let gaps = store.detect_seq_gaps(sid).await.unwrap();
        assert!(gaps.is_empty(), "unexpected seq gaps: {:?}", gaps);
    }

    // ── 崩溃恢复:redo_group/redo_count 解析(B3)──────────────────────────

    #[tokio::test]
    async fn incomplete_turns_parse_redo_group() {
        let (store, _dir) = setup().await;
        let sid = create_test_task(&store, "a").await;
        let sid = sid.as_str();
        wait_seq(&store, sid, 1).await;

        // turn 7 started(带 redo_group/redo_count),未终止 → 应出现在 incomplete 列表
        let ts = AgentEvent::new(
            sid.into(),
            Some(7),
            None,
            EventType::TurnStarted,
            serde_json::json!({
                "user_input": "x",
                "redo_group": "rg-abc",
                "redo_count": 2,
            }),
        );
        store.write_event(&ts).await.unwrap();
        wait_seq(&store, sid, 2).await;

        let incomplete = store.get_incomplete_turns(sid).await.unwrap();
        assert_eq!(incomplete.len(), 1, "turn 7 should be incomplete");
        let t = &incomplete[0];
        assert_eq!(t.turn_id, 7);
        assert_eq!(t.redo_group, "rg-abc");
        assert_eq!(t.redo_count, 2);
    }

    // ── 批写原子性(B4)──────────────────────────────────────────────────────

    #[tokio::test]
    async fn write_events_batch_returns_contiguous_seqs() {
        let (store, _dir) = setup().await;
        let sid = create_test_task(&store, "a").await;
        let sid = sid.as_str();
        wait_seq(&store, sid, 1).await;

        let events = vec![
            AgentEvent::new(
                sid.into(),
                Some(1),
                None,
                EventType::TurnStarted,
                serde_json::json!({"user_input":"hi","redo_group":"rg1","redo_count":0}),
            ),
            AgentEvent::new(
                sid.into(),
                Some(1),
                Some("s1".into()),
                EventType::LlmInvoked,
                serde_json::json!({"step_type":"llm_call","model":"gpt-4","messages":[],"local_seq":1}),
            ),
            AgentEvent::new(
                sid.into(),
                Some(1),
                Some("s1".into()),
                EventType::LlmCompleted,
                serde_json::json!({"model":"gpt-4","local_seq":1,"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}),
            ),
        ];
        let seqs = store.write_events_batch(&events).await.unwrap();
        assert_eq!(seqs, vec![2, 3, 4]);
        wait_seq(&store, sid, 4).await;

        let tev = store.get_turn_events(sid, 1).await.unwrap();
        assert_eq!(tev.len(), 3, "turn events read back via query");
    }

    // ── gRPC read 路径 ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_event_reads_back_via_grpc() {
        let (store, _dir) = setup().await;
        let sid = create_test_task(&store, "a").await;
        let sid = sid.as_str();
        wait_seq(&store, sid, 1).await;

        // get_event 走原生 gRPC read(不经 query cache)
        let ev = store.get_event(sid, 1).await.unwrap().unwrap();
        assert_eq!(ev.event_type, EventType::TaskCreated);
        assert_eq!(ev.seq, 1);
        assert_eq!(ev.payload["task_type"], "a");
        assert_eq!(ev.payload["provenance"]["source_user_id"], "u");

        // 不存在的 seq → None
        assert!(store.get_event(sid, 999).await.unwrap().is_none());
    }

    // ── Task 状态机投影(spec §4.4)──────────────────────────────────────────

    #[tokio::test]
    async fn get_task_state_lifecycle_projection() {
        let (store, _dir) = setup().await;
        let prov = test_provenance();
        let (tid, _) = store
            .create_task("db.repair", "t", "u", &prov, None)
            .await
            .unwrap();
        wait_seq(&store, &tid, 1).await;

        // created
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(crate::models::TaskState::Created)
        );

        // created → ready
        store.write_event(&AgentEvent::new(
            tid.clone(), None, None,
            EventType::TaskReady, serde_json::json!({}),
        )).await.unwrap();
        wait_seq(&store, &tid, 2).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(crate::models::TaskState::Ready)
        );

        // ready → claimed
        store.write_event(&AgentEvent::new(
            tid.clone(), None, None,
            EventType::TaskClaimed, serde_json::json!({"claimant":"fixlet-1"}),
        )).await.unwrap();
        wait_seq(&store, &tid, 3).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(crate::models::TaskState::Claimed)
        );

        // claimed + turn_started → executing(派生态)
        store.write_event(&AgentEvent::new(
            tid.clone(), Some(1), None,
            EventType::TurnStarted,
            serde_json::json!({"user_input":"x","redo_group":"rg1","redo_count":0}),
        )).await.unwrap();
        wait_seq(&store, &tid, 4).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(crate::models::TaskState::Executing)
        );

        // executing → succeeded(终态)
        store.write_event(&AgentEvent::new(
            tid.clone(), None, None,
            EventType::TaskSucceeded, serde_json::json!({"reason":"done"}),
        )).await.unwrap();
        wait_seq(&store, &tid, 5).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(crate::models::TaskState::Succeeded)
        );

        // 不存在的 task → None
        assert_eq!(store.get_task_state("nope").await.unwrap(), None);
    }

    // ── 性能测试(#[ignore],cargo test -- --ignored --nocapture 看)──────
    //
    // 测量 Task 热路径:create_task(append)/ get_task_state(投影)/ get_task(head 读)。
    // 只断言功能正确,不断言时间阈值(WSL2 I/O 抖动会 flake);数字供人读。
    // 跑法:cargo test --lib -- --ignored perf_ --nocapture

    fn report(name: &str, unit: &str, mut samples: Vec<u64>) {
        samples.sort_unstable();
        let n = samples.len();
        if n == 0 {
            println!("[perf] {}: no samples", name);
            return;
        }
        let p = |q: usize| samples[(q * n / 100).min(n.saturating_sub(1))];
        let sum: u64 = samples.iter().sum();
        println!(
            "[perf] {:<28} n={:>5}  p50={:>8}{}  p95={:>8}{}  p99={:>8}{}  avg={:>8}{}",
            name, n, p(50), unit, p(95), unit, p(99), unit, sum / n as u64, unit
        );
    }

    #[tokio::test]
    #[ignore]
    async fn perf_create_task_append() {
        let (store, _d) = setup().await;
        let prov = test_provenance();
        // warm-up( primes logdbd 连接 / segment)
        for _ in 0..20 {
            let (tid, _) = store
                .create_task("db.repair", "t", "u", &prov, None)
                .await
                .unwrap();
            wait_seq(&store, &tid, 1).await;
        }
        let n = 200;
        let mut us = Vec::with_capacity(n);
        for _ in 0..n {
            let t0 = std::time::Instant::now();
            let (tid, _) = store
                .create_task("db.repair", "t", "u", &prov, None)
                .await
                .unwrap();
            us.push(t0.elapsed().as_micros() as u64);
            wait_seq(&store, &tid, 1).await; // ensure append durable before next
        }
        report("create_task (append)", "µs", us);
    }

    #[tokio::test]
    #[ignore]
    async fn perf_get_task_state_projection() {
        let (store, _d) = setup().await;
        let prov = test_provenance();
        let (tid, _) = store
            .create_task("db.repair", "t", "u", &prov, None)
            .await
            .unwrap();
        wait_seq(&store, &tid, 1).await;
        // 构造完整生命周期 → Executing(get_task_state 需扫 Task 事件 + 查 turn_started Max)
        store
            .write_event(&AgentEvent::new(
                tid.clone(), None, None, EventType::TaskReady, serde_json::json!({}),
            ))
            .await
            .unwrap();
        store
            .write_event(&AgentEvent::new(
                tid.clone(), None, None, EventType::TaskClaimed, serde_json::json!({"claimant":"f1"}),
            ))
            .await
            .unwrap();
        store
            .write_event(&AgentEvent::new(
                tid.clone(), Some(1), None, EventType::TurnStarted,
                serde_json::json!({"user_input":"x","redo_group":"rg1","redo_count":0}),
            ))
            .await
            .unwrap();
        wait_seq(&store, &tid, 4).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(crate::models::TaskState::Executing)
        );

        // warm-up
        for _ in 0..20 {
            let _ = store.get_task_state(&tid).await.unwrap();
        }
        let n = 200;
        let mut us = Vec::with_capacity(n);
        for _ in 0..n {
            let t0 = std::time::Instant::now();
            let s = store.get_task_state(&tid).await.unwrap();
            us.push(t0.elapsed().as_micros() as u64);
            assert_eq!(s, Some(crate::models::TaskState::Executing));
        }
        report("get_task_state (projection)", "µs", us);
    }

    #[tokio::test]
    #[ignore]
    async fn perf_get_task_head_read() {
        let (store, _d) = setup().await;
        let prov = test_provenance();
        let body = serde_json::json!({"task_brief":"fix db1","contract":{"x":1}});
        let (tid, _) = store
            .create_task("db.repair", "t", "u", &prov, Some(&body))
            .await
            .unwrap();
        wait_seq(&store, &tid, 1).await;
        // warm-up
        for _ in 0..20 {
            let _ = store.get_task(&tid).await.unwrap();
        }
        let n = 200;
        let mut us = Vec::with_capacity(n);
        for _ in 0..n {
            let t0 = std::time::Instant::now();
            let t = store.get_task(&tid).await.unwrap().unwrap();
            us.push(t0.elapsed().as_micros() as u64);
            assert_eq!(t.task_type, "db.repair");
        }
        report("get_task (head + state)", "µs", us);
    }
}

