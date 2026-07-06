//! Event Store 持久化层
//!
//! 定义 `EventStore` trait 及其 logdbd 实现。
//!
//! 设计目标：
//! - 不可变 append-only 事件存储
//! - 简单读走 gRPC Read/Scan（零序列化开销）
//! - 过滤/聚合查询走 logdbd SQL query cache

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use logdb_client::{Client, RecordExt};
use logdbd_proto::pb::AppendRequest;
use tokio::sync::Mutex;

use crate::error::{AppError, Result};
use crate::models::{
    AgentEvent, EventType, IncompleteStep, IncompleteTurn, Session, StepExecution, TokenUsageStats,
};

// ── EventStore Trait ────────────────────────────────────────────────────────

/// Event Store 抽象 trait。
///
/// 当前实现：`LogdbdEventStore`（通过 gRPC 连接 logdbd）。
#[async_trait]
pub trait EventStore: Send + Sync {
    // ── Session ─────────────────────────────────────────────────────────

    async fn create_session(
        &self,
        session_id: &str,
        tenant_id: &str,
        user_id: &str,
        agent_type: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<AgentEvent>;

    async fn get_session(&self, session_id: &str) -> Result<Option<Session>>;
    async fn session_exists(&self, session_id: &str) -> Result<bool>;
    async fn is_session_ended(&self, session_id: &str) -> Result<bool>;

    // ── Seq ─────────────────────────────────────────────────────────────

    async fn get_max_seq(&self, session_id: &str) -> Result<i64>;
    async fn get_max_turn_id(&self, session_id: &str) -> Result<i64>;

    // ── Write ───────────────────────────────────────────────────────────

    async fn write_event(&self, event: &AgentEvent) -> Result<i64>;
    async fn write_events_batch(&self, events: &[AgentEvent]) -> Result<Vec<i64>>;

    // ── Read ────────────────────────────────────────────────────────────

    async fn get_event(&self, session_id: &str, seq: i64) -> Result<Option<AgentEvent>>;
    async fn get_turn_events(&self, session_id: &str, turn_id: i64)
        -> Result<Vec<AgentEvent>>;
    async fn get_events_after_seq(
        &self,
        session_id: &str,
        after_seq: i64,
    ) -> Result<Vec<AgentEvent>>;
    async fn get_latest_summary(&self, session_id: &str) -> Result<Option<AgentEvent>>;
    async fn get_turn_steps(
        &self,
        session_id: &str,
        turn_id: i64,
    ) -> Result<Vec<StepExecution>>;

    // ── Recovery ────────────────────────────────────────────────────────

    async fn get_incomplete_turns(&self, session_id: &str)
        -> Result<Vec<IncompleteTurn>>;
    async fn get_incomplete_steps(
        &self,
        session_id: &str,
    ) -> Result<Vec<IncompleteStep>>;
    async fn detect_seq_gaps(&self, session_id: &str) -> Result<Vec<i64>>;
    async fn is_turn_seq_continuous(&self, session_id: &str, turn_id: i64)
        -> Result<bool>;

    // ── Stats ───────────────────────────────────────────────────────────

    async fn get_token_usage_stats(
        &self,
        session_id: &str,
    ) -> Result<Vec<TokenUsageStats>>;

    // ── Summary helpers ─────────────────────────────────────────────────

    async fn count_turns_after_seq(&self, session_id: &str, after_seq: i64) -> Result<i64>;
    async fn count_events_after_seq(&self, session_id: &str, after_seq: i64) -> Result<i64>;
    async fn get_llm_payloads_after_seq(
        &self,
        session_id: &str,
        after_seq: i64,
    ) -> Result<Vec<String>>;
    async fn get_recent_turn_ids(&self, session_id: &str, limit: i64) -> Result<Vec<i64>>;

    // ── Archive ─────────────────────────────────────────────────────────

    async fn archive_events_before_seq(
        &self,
        session_id: &str,
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
/// - stream   = session_id
/// - 简单读：`client.read()` / `client.scan_all()` — content 直接是 `Vec<u8>`
/// - 过滤查询：`client.query()` — SQL SELECT against logdbd's per-stream SQLite cache
pub struct LogdbdEventStore {
    client: Arc<Mutex<Client>>,
    namespace: String,
}

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

    /// 校验 terminal 事件唯一性:同一 session/turn/step 最多一个 terminal 事件。
    /// append 前调用,违反则返回 `LifecycleInvariant`。
    /// 覆盖:session_ended / 4 种 turn terminal / step llm terminal / step tool terminal。
    async fn check_terminal_uniqueness(
        &self,
        client: &mut Client,
        event: &AgentEvent,
    ) -> Result<()> {
        use EventType::*;
        let (conflict_in, scope) = match event.event_type {
            SessionEnded => ("('session_ended')", "session"),
            TurnCompleted | TurnFailed | TurnCanceled | TurnBlocked => {
                ("('turn_completed','turn_failed','turn_canceled','turn_blocked')", "turn")
            }
            LlmCompleted | LlmFailed => ("('llm_completed','llm_failed')", "step"),
            ToolCompleted | ToolFailed => ("('tool_completed','tool_failed')", "step"),
            // 非 terminal 事件不校验唯一性
            _ => return Ok(()),
        };

        let where_scope = match scope {
            "turn" => {
                let tid = event.turn_id.ok_or_else(|| {
                    AppError::LifecycleInvariant(format!(
                        "{} 缺 turn_id",
                        event.event_type.as_str()
                    ))
                })?;
                format!(
                    " AND json_extract(metadata_json,'$.turn_id') = {}",
                    sql_quote(&tid.to_string())
                )
            }
            "step" => {
                let sid = event.step_id.as_ref().ok_or_else(|| {
                    AppError::LifecycleInvariant(format!(
                        "{} 缺 step_id",
                        event.event_type.as_str()
                    ))
                })?;
                format!(
                    " AND json_extract(metadata_json,'$.step_id') = {}",
                    sql_quote(sid)
                )
            }
            _ => String::new(), // session 级:stream_id 已限定 session
        };

        let sql = format!(
            "SELECT COUNT(*) AS c FROM records WHERE event_type IN {}{}",
            conflict_in, where_scope
        );
        let rows = client
            .query(&self.namespace, &event.session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;

        if let Some(row) = rows.first() {
            let v: serde_json::Value = serde_json::from_str(row).unwrap_or_default();
            if v["c"].as_i64().unwrap_or(0) > 0 {
                return Err(AppError::LifecycleInvariant(format!(
                    "{} 已存在同类 terminal 事件(session={}, turn={:?}, step={:?})",
                    event.event_type.as_str(),
                    event.session_id,
                    event.turn_id,
                    event.step_id,
                )));
            }
        }
        Ok(())
    }
}

// ── 辅助函数 ────────────────────────────────────────────────────────────────

/// AgentEvent → logdbd metadata HashMap
fn event_meta(event: &AgentEvent) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("session_id".into(), event.session_id.clone());
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
fn event_from_record(rec: &logdb_client::Record, session_id: &str) -> Result<AgentEvent> {
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
        session_id: session_id.to_string(),
        seq: rec.seq as i64,
        turn_id,
        step_id,
        event_type,
        schema_version,
        payload,
        created_at,
    })
}

/// SQL query row (JSON string) → AgentEvent
fn event_from_query_row(row: &str) -> Result<AgentEvent> {
    let v: serde_json::Value =
        serde_json::from_str(row).map_err(|e| {
            AppError::Internal(format!("parse query row: {}", e))
        })?;

    let seq = v["seq"].as_i64().unwrap_or(0);
    let event_type_str = v["event_type"].as_str().unwrap_or("");
    let event_type =
        EventType::from_str(event_type_str).ok_or_else(|| {
            AppError::InvalidEventType(event_type_str.into())
        })?;

    let meta_str = v["metadata_json"].as_str().unwrap_or("{}");
    let meta: serde_json::Value =
        serde_json::from_str(meta_str).unwrap_or_default();
    let turn_id = meta["turn_id"].as_str().and_then(|s| s.parse().ok());
    let step_id = meta["step_id"].as_str().map(|s| s.to_string());
    let schema_version = meta["schema_version"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let session_id =
        meta["session_id"].as_str().unwrap_or("").to_string();

    // content 在 query 中为 hex 字符串（logdbd 内部 blob→hex）
    let content_hex = v["content"].as_str().unwrap_or("");
    let payload: serde_json::Value =
        if content_hex.is_empty() || content_hex == "null" {
            serde_json::Value::Null
        } else {
            let bytes = hex::decode(content_hex).map_err(|e| {
                AppError::Internal(format!("decode content hex: {}", e))
            })?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        };

    let ts_ns = v["ts_ns"].as_i64().unwrap_or(0);
    let created_at = Utc.timestamp_nanos(ts_ns);

    Ok(AgentEvent {
        session_id,
        seq,
        turn_id,
        step_id,
        event_type,
        schema_version,
        payload,
        created_at,
    })
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

// ── EventStore for LogdbdEventStore ─────────────────────────────────────────

#[async_trait]
impl EventStore for LogdbdEventStore {
    // ── Session ─────────────────────────────────────────────────────────

    async fn create_session(
        &self,
        session_id: &str,
        tenant_id: &str,
        user_id: &str,
        agent_type: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<AgentEvent> {
        let payload = serde_json::json!({
            "agent_type": agent_type,
            "user_id": user_id,
            "initial_config": metadata.unwrap_or(serde_json::Value::Null),
        });
        let content = serde_json::to_vec(&payload)
            .map_err(|e| AppError::Internal(format!("json: {}", e)))?;

        let mut meta = HashMap::new();
        meta.insert("session_id".into(), session_id.to_string());
        meta.insert("tenant_id".into(), tenant_id.to_string());
        meta.insert("user_id".into(), user_id.to_string());

        let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        let mut client = self.client.lock().await;
        let resp = client
            .append_full(
                &self.namespace,
                session_id,
                "session_started",
                "application/json",
                &meta,
                ts_ns,
                &content,
            )
            .await
            .map_err(|e| AppError::Internal(format!("logdbd append: {}", e)))?;

        Ok(AgentEvent {
            session_id: session_id.to_string(),
            seq: resp.seq as i64,
            turn_id: None,
            step_id: None,
            event_type: EventType::SessionStarted,
            schema_version: 1,
            payload,
            created_at: Utc::now(),
        })
    }

    async fn get_session(&self, session_id: &str) -> Result<Option<Session>> {
        let sql = format!(
            "SELECT content, metadata_json FROM records WHERE event_type = {} ORDER BY seq LIMIT 1",
            sql_quote("session_started")
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;

        if rows.is_empty() {
            return Ok(None);
        }
        let v: serde_json::Value =
            serde_json::from_str(&rows[0]).unwrap_or_default();
        let meta_str = v["metadata_json"].as_str().unwrap_or("{}");
        let meta: serde_json::Value =
            serde_json::from_str(meta_str).unwrap_or_default();

        let content_hex = v["content"].as_str().unwrap_or("");
        let payload: serde_json::Value =
            if content_hex.is_empty() || content_hex == "null" {
                serde_json::Value::Null
            } else {
                let bytes = hex::decode(content_hex).unwrap_or_default();
                serde_json::from_slice(&bytes).unwrap_or_default()
            };

        Ok(Some(Session {
            session_id: session_id.to_string(),
            tenant_id: meta["tenant_id"]
                .as_str()
                .unwrap_or("default")
                .to_string(),
            user_id: meta["user_id"].as_str().unwrap_or("").to_string(),
            agent_type: payload["agent_type"]
                .as_str()
                .unwrap_or("unknown")
                .to_string(),
            created_at: Utc::now(),
            metadata: payload.get("initial_config").cloned(),
        }))
    }

    async fn session_exists(&self, session_id: &str) -> Result<bool> {
        let sql = format!(
            "SELECT COUNT(*) AS cnt FROM records WHERE event_type = {}",
            sql_quote("session_started")
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        if rows.is_empty() {
            return Ok(false);
        }
        let v: serde_json::Value =
            serde_json::from_str(&rows[0]).unwrap_or_default();
        Ok(v["cnt"].as_i64().unwrap_or(0) > 0)
    }

    async fn is_session_ended(&self, session_id: &str) -> Result<bool> {
        let sql = format!(
            "SELECT COUNT(*) AS cnt FROM records WHERE event_type = {}",
            sql_quote("session_ended")
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        if rows.is_empty() {
            return Ok(false);
        }
        let v: serde_json::Value =
            serde_json::from_str(&rows[0]).unwrap_or_default();
        Ok(v["cnt"].as_i64().unwrap_or(0) > 0)
    }

    // ── Seq ─────────────────────────────────────────────────────────────

    async fn get_max_seq(&self, session_id: &str) -> Result<i64> {
        let sql = "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM records";
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        if rows.is_empty() {
            return Ok(0);
        }
        let v: serde_json::Value =
            serde_json::from_str(&rows[0]).unwrap_or_default();
        Ok(v["max_seq"].as_i64().unwrap_or(0))
    }

    async fn get_max_turn_id(&self, session_id: &str) -> Result<i64> {
        let sql = "SELECT COALESCE(MAX(CAST(json_extract(metadata_json, '$.turn_id') AS INTEGER)), 0) AS max_turn_id FROM records WHERE json_extract(metadata_json, '$.turn_id') IS NOT NULL";
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        if rows.is_empty() {
            return Ok(0);
        }
        let v: serde_json::Value =
            serde_json::from_str(&rows[0]).unwrap_or_default();
        Ok(v["max_turn_id"].as_i64().unwrap_or(0))
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
                &event.session_id,
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
                stream: event.session_id.clone(),
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
        session_id: &str,
        seq: i64,
    ) -> Result<Option<AgentEvent>> {
        let mut client = self.client.lock().await;
        match client
            .read(&self.namespace, session_id, seq as u64)
            .await
        {
            Ok(Some(rec)) => Ok(Some(event_from_record(&rec, session_id)?)),
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
        session_id: &str,
        after_seq: i64,
    ) -> Result<Vec<AgentEvent>> {
        let from = (after_seq + 1).max(1) as u64;
        let mut client = self.client.lock().await;
        let records = client
            .scan_all(&self.namespace, session_id, from)
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
            .map(|r| event_from_record(r, session_id))
            .collect()
    }

    // ── Read（过滤查询走 SQL）──────────────────────────────────────────

    async fn get_turn_events(
        &self,
        session_id: &str,
        turn_id: i64,
    ) -> Result<Vec<AgentEvent>> {
        let sql = format!(
            "SELECT * FROM records WHERE json_extract(metadata_json, '$.turn_id') = '{}' ORDER BY seq ASC",
            turn_id
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        rows.iter().map(|r| event_from_query_row(r)).collect()
    }

    async fn get_latest_summary(
        &self,
        session_id: &str,
    ) -> Result<Option<AgentEvent>> {
        let sql = format!(
            "SELECT * FROM records WHERE event_type = {} ORDER BY seq DESC LIMIT 1",
            sql_quote("summary_marker")
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(event_from_query_row(&rows[0])?))
    }

    async fn get_turn_steps(
        &self,
        session_id: &str,
        turn_id: i64,
    ) -> Result<Vec<StepExecution>> {
        let sql = format!(
            "SELECT e_start.step_id, e_start.metadata_json AS start_meta, e_start.ts_ns AS started_ts, e_end.ts_ns AS ended_ts, e_end.event_type AS end_event FROM records e_start JOIN records e_end ON json_extract(e_end.metadata_json, '$.step_id') = json_extract(e_start.metadata_json, '$.step_id') AND e_end.event_type IN ('llm_completed','llm_failed','tool_completed','tool_failed') WHERE json_extract(e_start.metadata_json, '$.turn_id') = '{tid}' AND e_start.event_type IN ('llm_invoked','tool_invoked') ORDER BY e_start.seq ASC",
            tid = turn_id
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;

        let mut steps = Vec::new();
        for row_str in &rows {
            let v: serde_json::Value =
                serde_json::from_str(row_str).unwrap_or_default();
            let start_meta_str =
                v["start_meta"].as_str().unwrap_or("{}");
            let start_meta: serde_json::Value =
                serde_json::from_str(start_meta_str).unwrap_or_default();
            let step_type = start_meta
                .get("step_type")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());
            let started_ts = v["started_ts"].as_i64().unwrap_or(0);
            let ended_ts = v["ended_ts"].as_i64().unwrap_or(0);
            let duration_ms = if ended_ts > started_ts {
                Some(((ended_ts - started_ts) as f64) / 1_000_000.0)
            } else {
                None
            };
            steps.push(StepExecution {
                step_id: v["step_id"].as_str().unwrap_or("").to_string(),
                step_type,
                started_at: Utc.timestamp_nanos(started_ts),
                ended_at: Some(Utc.timestamp_nanos(ended_ts)),
                end_event: v["end_event"].as_str().map(|s| s.to_string()),
                duration_ms,
            });
        }
        Ok(steps)
    }

    // ── Recovery ────────────────────────────────────────────────────────

    async fn get_incomplete_turns(
        &self,
        session_id: &str,
    ) -> Result<Vec<IncompleteTurn>> {
        let sql = "SELECT json_extract(metadata_json, '$.turn_id') AS tid, metadata_json, ts_ns, content FROM records WHERE event_type = 'turn_started' AND NOT EXISTS (SELECT 1 FROM records e2 WHERE json_extract(e2.metadata_json, '$.turn_id') = json_extract(records.metadata_json, '$.turn_id') AND e2.event_type IN ('turn_completed','turn_failed','turn_canceled','turn_blocked')) ORDER BY CAST(tid AS INTEGER) ASC";
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;

        let mut turns = Vec::new();
        for row_str in &rows {
            let v: serde_json::Value =
                serde_json::from_str(row_str).unwrap_or_default();
            let turn_id = v["tid"]
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            let ts_ns = v["ts_ns"].as_i64().unwrap_or(0);

            // 从 turn_started 的 content(hex)解 payload,取 redo_group/redo_count
            let content_hex = v["content"].as_str().unwrap_or("");
            let (redo_group, redo_count) = if content_hex.is_empty() || content_hex == "null" {
                ("unknown".to_string(), 0)
            } else {
                let payload: serde_json::Value = hex::decode(content_hex)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                    .unwrap_or_default();
                (
                    payload["redo_group"].as_str().unwrap_or("unknown").to_string(),
                    payload["redo_count"].as_i64().unwrap_or(0) as i32,
                )
            };

            turns.push(IncompleteTurn {
                turn_id,
                redo_group,
                redo_count,
                turn_started_at: Utc.timestamp_nanos(ts_ns),
            });
        }
        Ok(turns)
    }

    async fn get_incomplete_steps(
        &self,
        session_id: &str,
    ) -> Result<Vec<IncompleteStep>> {
        let sql = "SELECT e.seq, json_extract(e.metadata_json, '$.turn_id') AS tid, json_extract(e.metadata_json, '$.step_id') AS sid, e.event_type, e.content, e.ts_ns FROM records e WHERE e.event_type IN ('llm_invoked','tool_invoked') AND NOT EXISTS (SELECT 1 FROM records e2 WHERE json_extract(e2.metadata_json, '$.step_id') = json_extract(e.metadata_json, '$.step_id') AND e2.event_type IN ('llm_completed','llm_failed','tool_completed','tool_failed')) ORDER BY e.seq ASC";
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;

        let mut steps = Vec::new();
        for row_str in &rows {
            let v: serde_json::Value =
                serde_json::from_str(row_str).unwrap_or_default();
            let content_hex = v["content"].as_str().unwrap_or("");
            let payload: serde_json::Value =
                if content_hex.is_empty() || content_hex == "null" {
                    serde_json::Value::Null
                } else {
                    let bytes =
                        hex::decode(content_hex).unwrap_or_default();
                    serde_json::from_slice(&bytes).unwrap_or_default()
                };
            steps.push(IncompleteStep {
                seq: v["seq"].as_i64().unwrap_or(0),
                turn_id: v["tid"]
                    .as_str()
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0),
                step_id: v["sid"].as_str().unwrap_or("").to_string(),
                start_event_type: v["event_type"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                payload,
                started_at: Utc.timestamp_nanos(
                    v["ts_ns"].as_i64().unwrap_or(0),
                ),
            });
        }
        Ok(steps)
    }

    async fn detect_seq_gaps(
        &self,
        session_id: &str,
    ) -> Result<Vec<i64>> {
        let sql = "SELECT seq + 1 AS missing_seq FROM records e1 WHERE NOT EXISTS (SELECT 1 FROM records e2 WHERE e2.seq = e1.seq + 1) AND seq < (SELECT MAX(seq) FROM records)";
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let v: serde_json::Value =
                    serde_json::from_str(r).ok()?;
                v["missing_seq"].as_i64()
            })
            .collect())
    }

    async fn is_turn_seq_continuous(
        &self,
        session_id: &str,
        turn_id: i64,
    ) -> Result<bool> {
        let gaps = self.detect_seq_gaps(session_id).await?;
        let sql = format!(
            "SELECT MIN(seq) AS mn, MAX(seq) AS mx FROM records WHERE json_extract(metadata_json, '$.turn_id') = '{}'",
            turn_id
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        if rows.is_empty() {
            return Ok(true);
        }
        let v: serde_json::Value =
            serde_json::from_str(&rows[0]).unwrap_or_default();
        match (v["mn"].as_i64(), v["mx"].as_i64()) {
            (Some(lo), Some(hi)) => {
                Ok(!gaps.iter().any(|g| *g >= lo && *g <= hi))
            }
            _ => Ok(true),
        }
    }

    // ── Stats ───────────────────────────────────────────────────────────

    async fn get_token_usage_stats(
        &self,
        session_id: &str,
    ) -> Result<Vec<TokenUsageStats>> {
        let sql = format!(
            "SELECT content FROM records WHERE event_type = {}",
            sql_quote("llm_completed")
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;

        let mut map: HashMap<String, TokenUsageStats> = HashMap::new();
        for row_str in &rows {
            let v: serde_json::Value =
                serde_json::from_str(row_str).unwrap_or_default();
            let content_hex = v["content"].as_str().unwrap_or("");
            if content_hex.is_empty() || content_hex == "null" {
                continue;
            }
            let bytes = match hex::decode(content_hex) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let payload: serde_json::Value =
                serde_json::from_slice(&bytes).unwrap_or_default();
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
        session_id: &str,
        after_seq: i64,
    ) -> Result<i64> {
        let sql = format!(
            "SELECT COUNT(DISTINCT json_extract(metadata_json, '$.turn_id')) AS cnt FROM records WHERE seq > {} AND json_extract(metadata_json, '$.turn_id') IS NOT NULL AND event_type = {}",
            after_seq,
            sql_quote("turn_started")
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        if rows.is_empty() {
            return Ok(0);
        }
        let v: serde_json::Value =
            serde_json::from_str(&rows[0]).unwrap_or_default();
        Ok(v["cnt"].as_i64().unwrap_or(0))
    }

    async fn count_events_after_seq(
        &self,
        session_id: &str,
        after_seq: i64,
    ) -> Result<i64> {
        let sql = format!(
            "SELECT COUNT(*) AS cnt FROM records WHERE seq > {} AND event_type IN ({})",
            after_seq,
            "'turn_pending','turn_started','turn_completed','turn_failed','turn_canceled','turn_blocked','llm_invoked','llm_completed','llm_failed','tool_invoked','tool_completed','tool_failed'"
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        if rows.is_empty() {
            return Ok(0);
        }
        let v: serde_json::Value =
            serde_json::from_str(&rows[0]).unwrap_or_default();
        Ok(v["cnt"].as_i64().unwrap_or(0))
    }

    async fn get_llm_payloads_after_seq(
        &self,
        session_id: &str,
        after_seq: i64,
    ) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT content FROM records WHERE seq > {} AND event_type = {}",
            after_seq,
            sql_quote("llm_completed")
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let v: serde_json::Value =
                    serde_json::from_str(r).ok()?;
                let h = v["content"].as_str().unwrap_or("");
                if h.is_empty() || h == "null" {
                    return None;
                }
                let bytes = hex::decode(h).ok()?;
                Some(String::from_utf8_lossy(&bytes).to_string())
            })
            .collect())
    }

    async fn get_recent_turn_ids(
        &self,
        session_id: &str,
        limit: i64,
    ) -> Result<Vec<i64>> {
        let sql = format!(
            "SELECT DISTINCT CAST(json_extract(metadata_json, '$.turn_id') AS INTEGER) AS tid FROM records WHERE event_type = 'turn_started' ORDER BY tid DESC LIMIT {}",
            limit
        );
        let mut client = self.client.lock().await;
        let rows = client
            .query(&self.namespace, session_id, &sql)
            .await
            .map_err(|e| AppError::Internal(format!("logdbd query: {}", e)))?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let v: serde_json::Value =
                    serde_json::from_str(r).ok()?;
                v["tid"].as_i64()
            })
            .collect())
    }

    // ── Archive ─────────────────────────────────────────────────────────

    async fn archive_events_before_seq(
        &self,
        _session_id: &str,
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

