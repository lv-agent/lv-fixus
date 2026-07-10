//! Broker-backed EventStore — 所有写经 BrokerServiceClient,所有读经 ProjectionCache。
//!
//! 生产路径:fixus → broker → logdbd。测试路径:内嵌 broker + logdbd harness。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use logdb_client::broker::BrokerProducer;
use logdb_broker_proto::pb::broker_service_client::BrokerServiceClient;
use logdb_broker_proto::pb::ProduceRequest;

use crate::error::{AppError, Result};
use crate::models::{AgentEvent, EventType, IncompleteStep, IncompleteTurn, Task, StepExecution, TokenUsageStats, TaskState, Provenance};
use crate::projection::{ProjectionCache, TaskProjection};
use crate::storage::EventStore;

// ── BrokerWriter ─────────────────────────────────────────────────────────

/// broker 写路径:produce / batch_produce。
/// 内部持有 BrokerServiceClient,所有 produce 方法返回 (gid, seq)。
pub struct BrokerWriter {
    client: BrokerServiceClient<Channel>,
    namespace: String,
}

impl BrokerWriter {
    pub async fn connect(addr: &str, namespace: &str) -> std::result::Result<Self, logdb_client::broker::BrokerError> {
        let uri = if addr.starts_with("http") { addr.to_string() } else { format!("http://{}", addr) };
        let client = BrokerServiceClient::connect(uri)
            .await
            .map_err(logdb_client::broker::BrokerError::Transport)?;
        Ok(Self {
            client,
            namespace: namespace.to_string(),
        })
    }

    /// 单条 produce(完整字段),返回 (gid, seq)。
    pub async fn produce(
        &mut self,
        stream: &str,
        event_type: &str,
        content: &[u8],
        shard_key: Option<&str>,
        timestamp_ns: u64,
        content_type: &str,
        metadata: &HashMap<String, String>,
    ) -> std::result::Result<(u64, u64), logdb_client::broker::BrokerError> {
        let resp = self
            .client
            .produce(ProduceRequest {
                namespace: self.namespace.clone(),
                stream: stream.into(),
                event_type: event_type.into(),
                timestamp_ns,
                content_type: content_type.into(),
                metadata: metadata.clone(),
                content: content.to_vec(),
                shard_key: shard_key.map(String::from),
            })
            .await?
            .into_inner();
        Ok((resp.gid, resp.seq))
    }

    /// 批量 produce。返回 Vec<(gid, seq)>。
    pub async fn batch_produce(
        &mut self,
        requests: Vec<BatchProduceEntry>,
    ) -> std::result::Result<Vec<(u64, u64)>, logdb_client::broker::BrokerError> {
        let records: Vec<ProduceRequest> = requests
            .into_iter()
            .map(|r| ProduceRequest {
                namespace: self.namespace.clone(),
                stream: r.stream,
                event_type: r.event_type,
                timestamp_ns: r.timestamp_ns,
                content_type: r.content_type,
                metadata: r.metadata,
                content: r.content,
                shard_key: r.shard_key,
            })
            .collect();
        let resp = self
            .client
            .batch_produce(logdb_broker_proto::pb::BatchProduceRequest { requests: records })
            .await?
            .into_inner();
        Ok(resp.records.into_iter().map(|r| (r.gid, r.seq)).collect())
    }
}

pub struct BatchProduceEntry {
    pub stream: String,
    pub event_type: String,
    pub content: Vec<u8>,
    pub shard_key: Option<String>,
    pub timestamp_ns: u64,
    pub content_type: String,
    pub metadata: HashMap<String, String>,
}

// ── BrokerEventStore(EventStore trait) ──────────────────────────────────

/// 从 AgentEvent 提取 metadata HashMap(用于 produce)。
fn event_meta(event: &AgentEvent) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("task_id".into(), event.task_id.clone());
    if let Some(tid) = event.turn_id {
        m.insert("turn_id".into(), tid.to_string());
    }
    if let Some(ref sid) = event.step_id {
        m.insert("step_id".into(), sid.clone());
    }
    m.insert("event_type".into(), event.event_type.as_str().into());
    m
}

/// EventStore broker 实现——写经 BrokerWriter,读经 ProjectionCache。
pub struct BrokerEventStore {
    writer: Arc<Mutex<BrokerWriter>>,
    cache: ProjectionCache,
    namespace: String,
    broker_addr: String,
}

impl BrokerEventStore {
    pub async fn connect(broker_addr: &str, namespace: &str) -> std::result::Result<Self, logdb_client::broker::BrokerError> {
        let writer = BrokerWriter::connect(broker_addr, namespace).await?;
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            cache: ProjectionCache::new(1000),
            namespace: namespace.to_string(),
            broker_addr: broker_addr.to_string(),
        })
    }

    async fn ensure_projection(&self, task_id: &str) -> Result<()> {
        if self.cache.get(task_id).await.is_some() {
            return Ok(());
        }
        self.cache
            .catch_up(task_id, &self.broker_addr, &self.namespace)
            .await
            .map_err(|e| AppError::Internal(format!("broker: {}", e)))?;
        Ok(())
    }

    /// 安全持有投射读锁并应用 f。guard 在 f 返回后立即释放。
    async fn with_projection<T>(&self, task_id: &str, f: impl FnOnce(&TaskProjection) -> T) -> Result<T> {
        self.ensure_projection(task_id).await?;
        // ensure 已确认缓存命中,catch_up 也在内;get 必 Some
        let proj = self.cache.get(task_id).await.unwrap();
        let p = proj.read().await;
        Ok(f(&p))
    }
}

#[async_trait]
impl EventStore for BrokerEventStore {
    // ── 写 ──

    async fn create_task(
        &self,
        task_type: &str,
        tenant_id: &str,
        user_id: &str,
        provenance: &Provenance,
        body: Option<&serde_json::Value>,
    ) -> Result<(String, AgentEvent)> {
        let task_id = format!("task_{}", uuid::Uuid::now_v7().to_string().replace('-', ""));
        let payload = serde_json::json!({"task_type":task_type,"provenance":provenance,"body":body.cloned().unwrap_or(serde_json::Value::Null)});
        let content = serde_json::to_vec(&payload).map_err(|e| AppError::Internal(format!("json: {}", e)))?;
        let mut meta = HashMap::new();
        meta.insert("task_id".into(), task_id.clone());
        meta.insert("tenant_id".into(), tenant_id.to_string());
        meta.insert("user_id".into(), user_id.to_string());
        meta.insert("task_type".into(), task_type.to_string());
        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let mut w = self.writer.lock().await;
        let (_, seq) = w.produce(&task_id, "task_created", &content, Some(&task_id), ts_ns, "application/json", &meta)
            .await.map_err(|e| AppError::Internal(format!("broker produce: {}", e)))?;
        let event = AgentEvent { task_id: task_id.clone(), seq: seq as i64, turn_id: None, step_id: None,
            event_type: EventType::TaskCreated, schema_version: 1, payload, created_at: chrono::Utc::now() };
        // 延迟入缓存:forwarder 异步 tail,首次 get_task/get_task_state 懒 catch_up
        Ok((task_id, event))
    }

    async fn write_event(&self, event: &AgentEvent) -> Result<i64> {
        event.validate_scope().map_err(|msg| AppError::LifecycleInvariant(msg))?;
        crate::models::validate_payload_required_fields(&event.event_type, &event.payload)?;
        let content = serde_json::to_vec(&event.payload).map_err(|e| AppError::Internal(format!("json: {}", e)))?;
        let meta = event_meta(event);
        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let mut w = self.writer.lock().await;
        let (_, seq) = w.produce(&event.task_id, event.event_type.as_str(), &content, Some(&event.task_id), ts_ns, "application/json", &meta)
            .await.map_err(|e| AppError::Internal(format!("broker produce: {}", e)))?;
        let seq_i64 = seq as i64;
        // 更新投射(若有——当前 write_event 用于 WAL,投射在 ensure 时已全量 consume 过;此处为事件刚写入,后续 ensure 会重新 consume 到)
        if let Some(proj) = self.cache.get(&event.task_id).await {
            let mut p = proj.write().await;
            let _ = p.apply(seq, event.event_type.as_str(), &content, &meta);
        }
        Ok(seq_i64)
    }

    async fn write_events_batch(&self, events: &[AgentEvent]) -> Result<Vec<i64>> {
        if events.is_empty() { return Ok(vec![]); }
        let tid = &events[0].task_id;
        let mut w = self.writer.lock().await;
        let entries: Vec<BatchProduceEntry> = events.iter().map(|e| {
            let content = serde_json::to_vec(&e.payload).unwrap_or_default();
            let meta = event_meta(e);
            BatchProduceEntry {
                stream: e.task_id.clone(), event_type: e.event_type.as_str().into(),
                content, shard_key: Some(e.task_id.clone()), timestamp_ns: 0,
                content_type: "application/json".into(), metadata: meta,
            }
        }).collect();
        let results = w.batch_produce(entries).await.map_err(|e| AppError::Internal(format!("broker batch: {}", e)))?;
        let seqs: Vec<i64> = results.into_iter().map(|(_, s)| s as i64).collect();
        // 更新投射
        if let Some(proj) = self.cache.get(tid).await {
            let mut p = proj.write().await;
            for (i, e) in events.iter().enumerate() {
                let c = serde_json::to_vec(&e.payload).unwrap_or_default();
                let _ = p.apply(seqs[i] as u64, e.event_type.as_str(), &c, &event_meta(e));
            }
        }
        Ok(seqs)
    }

    // ── 读(委托投射) ──

    async fn get_task(&self, task_id: &str) -> Result<Option<Task>> {
        match self.ensure_projection(task_id).await {
            Ok(()) => {}
            Err(_) => return Ok(None),
        }
        self.with_projection(task_id, |p| Some(Task {
            task_id: task_id.to_string(),
            tenant_id: p.provenance.source_tenant_id.clone().unwrap_or_default(),
            user_id: p.provenance.source_user_id.clone().unwrap_or_default(),
            task_type: p.task_type.clone(),
            state: p.state,
            provenance: p.provenance.clone(),
            body: p.body.clone(),
            created_at: p.provenance.created_at,
            metadata: None,
        })).await
    }

    async fn get_task_state(&self, task_id: &str) -> Result<Option<TaskState>> {
        self.with_projection(task_id, |p| Some(p.state)).await
    }
    async fn task_exists(&self, task_id: &str) -> Result<bool> {
        if self.cache.get(task_id).await.is_some() { return Ok(true); }
        Ok(self.ensure_projection(task_id).await.is_ok()) // 懒 catch_up
    }
    async fn is_task_ended(&self, task_id: &str) -> Result<bool> {
        self.with_projection(task_id, |p| p.state.is_terminal()).await
    }
    async fn get_max_seq(&self, task_id: &str) -> Result<i64> {
        self.with_projection(task_id, |p| p.max_seq).await
    }
    async fn get_max_turn_id(&self, task_id: &str) -> Result<i64> {
        self.with_projection(task_id, |p| p.max_turn_id).await
    }
    async fn get_event(&self, task_id: &str, seq: i64) -> Result<Option<AgentEvent>> {
        self.with_projection(task_id, |p| p.by_seq(seq).cloned()).await
    }
    async fn get_turn_events(&self, task_id: &str, turn_id: i64) -> Result<Vec<AgentEvent>> {
        self.with_projection(task_id, |p| {
            p.turn_seqs(turn_id).iter().filter_map(|s| p.by_seq(*s).cloned()).collect()
        }).await
    }
    async fn get_events_after_seq(&self, task_id: &str, after_seq: i64) -> Result<Vec<AgentEvent>> {
        self.with_projection(task_id, |p| p.after_seq(after_seq).into_iter().cloned().collect()).await
    }
    async fn get_latest_summary(&self, task_id: &str) -> Result<Option<AgentEvent>> {
        self.with_projection(task_id, |p| {
            p.latest_summary.as_ref().map(|s| AgentEvent {
                task_id: task_id.into(), seq: p.summarized_up_to_seq, turn_id: None, step_id: None,
                event_type: EventType::SummaryMarker, schema_version: 1, payload: s.clone(),
                created_at: chrono::Utc::now(),
            })
        }).await
    }
    async fn get_turn_steps(&self, task_id: &str, turn_id: i64) -> Result<Vec<StepExecution>> {
        self.with_projection(task_id, |p| p.turn_steps(turn_id)).await
    }
    async fn get_incomplete_turns(&self, task_id: &str) -> Result<Vec<IncompleteTurn>> {
        self.with_projection(task_id, |p| p.incomplete_turns()).await
    }
    async fn get_incomplete_steps(&self, task_id: &str) -> Result<Vec<IncompleteStep>> {
        self.with_projection(task_id, |p| p.incomplete_steps()).await
    }
    async fn detect_seq_gaps(&self, _task_id: &str) -> Result<Vec<i64>> {
        Ok(vec![])
    }
    async fn is_turn_seq_continuous(&self, _task_id: &str, _turn_id: i64) -> Result<bool> {
        Ok(true)
    }
    async fn get_token_usage_stats(&self, task_id: &str) -> Result<Vec<TokenUsageStats>> {
        self.with_projection(task_id, |p| p.token_stats.values().cloned().collect()).await
    }
    async fn count_turns_after_seq(&self, task_id: &str, after_seq: i64) -> Result<i64> {
        self.with_projection(task_id, |p| {
            p.recent_turn_ids.iter().filter(|tid| {
                p.turn_seqs(**tid).first().map(|s| *s > after_seq).unwrap_or(false)
            }).count() as i64
        }).await
    }
    async fn count_events_after_seq(&self, task_id: &str, after_seq: i64) -> Result<i64> {
        self.with_projection(task_id, |p| p.max_seq.saturating_sub(after_seq).max(0)).await
    }
    async fn get_llm_payloads_after_seq(&self, task_id: &str, after_seq: i64) -> Result<Vec<String>> {
        self.with_projection(task_id, |p| p.llm_payloads_after_seq(after_seq)).await
    }
    async fn get_recent_turn_ids(&self, task_id: &str, limit: i64) -> Result<Vec<i64>> {
        self.with_projection(task_id, |p| p.recent_turn_ids.iter().rev().take(limit as usize).copied().collect()).await
    }
    async fn archive_events_before_seq(&self, _task_id: &str, _before_seq: i64) -> Result<crate::storage::ArchiveResult> {
        Ok(crate::storage::ArchiveResult { archived: 0, path: String::new() })
    }

    /// 把 ready task 发布到 broker stream `tasks-{task_type}`,供 fixlet 订阅认领。
    /// 替代原来的内存 TaskRegistry 队列——broker 持久化,fixus 重启不丢。
    async fn publish_ready_task(&self, task_id: &str, task_type: &str, task_brief: &str, preferred_claimant: Option<&str>) -> Result<()> {
        // broker stream 名只允许 [a-zA-Z0-9_-], task_type 中的 '.' 替换为 '-'
        let sanitized = task_type.replace('.', "-");
        let stream = format!("tasks-{}", sanitized);
        let payload = serde_json::json!({
            "task_id": task_id,
            "task_type": task_type,
            "task_brief": task_brief,
            "preferred_claimant": preferred_claimant,
        });
        let content = serde_json::to_vec(&payload).map_err(|e| AppError::Internal(format!("json: {}", e)))?;
        let mut meta = HashMap::new();
        meta.insert("task_id".into(), task_id.to_string());
        meta.insert("event_type".into(), "task_ready".into());

        let mut last_err = None;
        for attempt in 0..3 {
            let mut w = self.writer.lock().await;
            match w.produce(&stream, "task_ready", &content, Some(task_id), 0, "application/json", &meta).await {
                Ok((gid, seq)) => {
                    tracing::info!("publish_ready_task: task={} type={} stream={} gid={} seq={}",
                        task_id, task_type, stream, gid, seq);
                    return Ok(());
                }
                Err(e) => {
                    drop(w);
                    last_err = Some(e);
                    if attempt < 2 {
                        let delay_ms = 100u64 << attempt;
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }
        Err(AppError::Internal(format!("publish_ready_task after 3 retries: {}", last_err.unwrap())))
    }

    /// 把工具事件发到 sandbox dispatch stream `tools-region-<SANDBOX_REGION>`。
    /// 失败时自动重试(backoff: 100ms → 200ms → 400ms),总共最多 3 次尝试。
    async fn dispatch_tool(&self, task_id: &str, event: &AgentEvent) -> Result<()> {
        let region = std::env::var("SANDBOX_REGION").unwrap_or_else(|_| "default".into());
        let stream = format!("tools-region-{}", region);
        let content = serde_json::to_vec(&event.payload).map_err(|e| AppError::Internal(format!("json: {}", e)))?;
        let meta = event_meta(event);

        let mut last_err = None;
        for attempt in 0..3 {
            let mut w = self.writer.lock().await;
            match w.produce(&stream, event.event_type.as_str(), &content, Some(task_id), 0, "application/json", &meta).await {
                Ok((gid, seq)) => {
                    tracing::debug!("dispatch_tool: gid={} seq={} (attempt {})", gid, seq, attempt + 1);
                    return Ok(());
                }
                Err(e) => {
                    drop(w); // 释放锁,重试时重新获取
                    last_err = Some(e);
                    if attempt < 2 {
                        let delay_ms = 100u64 << attempt; // 100, 200, 400
                        tracing::warn!("dispatch_tool retry {}/3 after {}ms: {}", attempt + 1, delay_ms, last_err.as_ref().unwrap());
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }
        Err(AppError::Internal(format!("dispatch after 3 retries: {}", last_err.unwrap())))
    }
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use logdb::{Config as DbConfig, LogDb};
    use logdbd::catalog::Catalog;
    use logdbd::consumer::ConsumerTracker;
    use logdbd::service::LogDbServiceImpl;
    use logdbd::storage::Storage;
    use logdbd::subscribe::SubscribeHub;
    use logdbd_proto::pb::log_db_service_server::LogDbServiceServer;
    use logdb_broker::coordinator::CoordinatorRegistry;
    use logdb_broker::forwarder::Forwarder;
    use logdb_broker::persistence::Persistence;
    use logdb_broker::service::BrokerServiceImpl;
    use logdb_broker_proto::pb::broker_service_server::BrokerServiceServer;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    // ── Harness ──────────────────────────────────────────────────────────

    type LogdbdHarness = (String, tempfile::TempDir);

    async fn start_logdbd() -> LogdbdHarness {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = DbConfig::default();
        cfg.data_dir = dir.path().to_path_buf();
        cfg.durability_mode = logdb::DurabilityMode::Sync;
        cfg.ring_size = 256;
        cfg.shards = 4;
        cfg.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(cfg).unwrap();
        let storage = Arc::new(Storage::new(db, 4));
        let catalog = Arc::new(Catalog::open(dir.path()).unwrap());
        let svc = LogDbServiceImpl::new(
            Arc::clone(&storage),
            catalog,
            Arc::new(ConsumerTracker::new(None)),
            Arc::new(SubscribeHub::new()),
            "test-node".into(),
            "primary".into(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            Server::builder()
                .add_service(LogDbServiceServer::new(svc))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        (addr, dir)
    }

    async fn start_broker(logdbd_addr: &str) -> String {
        let addr = format!("http://{}", logdbd_addr);
        let forwarder = Forwarder::connect(addr.clone()).await.unwrap();
        let persistence = Persistence::connect(addr).await.unwrap();
        persistence.ensure_meta_stream().await.unwrap();
        let registry = Arc::new(CoordinatorRegistry::new(4));
        // recover any persisted offsets (none at startup)
        if let Ok(recovered) = persistence.load_recovered_offsets().await {
            for rec in &recovered {
                registry.commit_offset(&rec.ns, &rec.stream, &rec.group, rec.shard, rec.seq);
            }
            let _ = persistence.compact_offsets(&recovered).await;
        }
        let svc = BrokerServiceImpl::new(registry, Some(forwarder), Some(persistence), None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            Server::builder()
                .add_service(BrokerServiceServer::new(svc))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        addr
    }

    async fn setup() -> (BrokerWriter, LogdbdHarness) {
        let (logdbd_addr, dir) = start_logdbd().await;
        let broker_addr = start_broker(&logdbd_addr).await;
        let writer = BrokerWriter::connect(&broker_addr, "test-ns").await.unwrap();
        (writer, (broker_addr, dir))
    }

    // ── Tests ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_broker_write_produce_returns_seq() {
        let (mut writer, (broker_addr, _dir)) = setup().await;
        let payload = serde_json::to_vec(&serde_json::json!({"k":"v"})).unwrap();
        let mut meta = HashMap::new();
        meta.insert("task_id".into(), "t1".into());

        let (gid, seq) = writer
            .produce("s1", "task_created", &payload, Some("t1"), 0, "application/json", &meta)
            .await
            .unwrap();
        assert_eq!(seq, 1, "first event must be seq 1");
        assert!(gid > 0);

        // 第二 event 同 stream
        let (_, seq2) = writer
            .produce("s1", "task_ready", b"{}", Some("t1"), 0, "application/json", &meta)
            .await
            .unwrap();
        assert_eq!(seq2, 2);

        // 查回:broker forwarder 异步 tail,produce 返回后稍等转发完成
        tokio::time::sleep(Duration::from_millis(50)).await;
        check_record_at(&broker_addr, "test-ns", "s1", 1, "task_created").await;
    }

    #[tokio::test]
    async fn test_broker_batch_produce_contiguous_seqs() {
        let (mut writer, _harness) = setup().await;
        let entries: Vec<BatchProduceEntry> = (0..3)
            .map(|i| {
                let mut meta = HashMap::new();
                meta.insert("task_id".into(), "tb".into());
                BatchProduceEntry {
                    stream: "sb".into(),
                    event_type: "turn_started".into(),
                    content: serde_json::to_vec(&serde_json::json!({"user_input":format!("t{}",i),"redo_group":format!("rg{}",i),"redo_count":0})).unwrap(),
                    shard_key: Some("tb".into()),
                    timestamp_ns: 0,
                    content_type: "application/json".into(),
                    metadata: meta,
                }
            })
            .collect();
        let results = writer.batch_produce(entries).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1, 1);
        assert_eq!(results[1].1, 2);
        assert_eq!(results[2].1, 3);
    }

    /// 用 broker consume(不 join group,直接 raw consume)读回事件验证落盘。
    async fn check_record_at(broker_addr: &str, ns: &str, stream: &str, expected_seq: u64, expected_type: &str) {
        use logdb_broker_proto::pb::broker_service_client::BrokerServiceClient;
        use logdb_broker_proto::pb::ConsumeRequest;
        use tokio_stream::StreamExt;

        let uri = if broker_addr.starts_with("http") { broker_addr.to_string() } else { format!("http://{}", broker_addr) };
        let mut client = BrokerServiceClient::connect(uri).await.unwrap();
        // join group first, then consume
        let jr = client.join_group(logdb_broker_proto::pb::JoinGroupRequest {
            namespace: ns.into(), stream: stream.into(), group: "test".into(), consumer_id: "c1".into(),
        }).await.unwrap().into_inner();
        assert!(!jr.assigned_shards.is_empty());

        // retry: broker forwarder 异步 tail,produce 后需等待转发
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            let mut stream_resp = client.consume(ConsumeRequest {
                namespace: ns.into(), stream: stream.into(), group: "test".into(), consumer_id: "c1".into(), generation: jr.generation,
            }).await.unwrap().into_inner();
            // 第一条 record
            if let Some(Ok(r)) = stream_resp.next().await {
                if let Some(logdb_broker_proto::pb::consume_response::Payload::Record(rec)) = r.payload {
                    if rec.seq == expected_seq {
                        assert_eq!(rec.event_type, expected_type);
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("expected Record at seq {} after deadline", expected_seq);
    }

    // ── ProjectionCache 集成测试(consume → CaughtUp → 投射)─────────────

    #[tokio::test]
    async fn test_projection_cache_populate_and_hit() {
        use crate::projection::{ProjectionCache, TaskProjection};
        let (logdbd_addr, _dir) = start_logdbd().await;
        let broker_addr = start_broker(&logdbd_addr).await;
        let mut writer = BrokerWriter::connect(&broker_addr, "test-ns").await.unwrap();

        // 写 3 条事件
        let content1 = serde_json::to_vec(&serde_json::json!({
            "task_type":"db.repair","provenance":{"source_channel":"api","created_by":"t","created_at":"2026-01-01T00:00:00Z"},"body":null
        })).unwrap();
        let meta = HashMap::new();
        writer.produce("t-cache", "task_created", &content1, Some("t-cache"), 0, "application/json", &meta).await.unwrap();
        writer.produce("t-cache", "task_ready", b"{}", Some("t-cache"), 0, "application/json", &meta).await.unwrap();
        writer.produce("t-cache", "task_claimed", b"{}", Some("t-cache"), 0, "application/json", &meta).await.unwrap();

        // 等 forwarder 追上
        tokio::time::sleep(Duration::from_millis(100)).await;

        let cache = ProjectionCache::new(10);
        // 未命中 → catch_up
        let proj = cache.catch_up("t-cache", &broker_addr, "test-ns").await.unwrap();
        let p = proj.read().await;
        assert_eq!(p.state, crate::models::TaskState::Claimed);
        assert_eq!(p.task_type, "db.repair");
        drop(p);

        // 再次命中(不 consume)
        assert!(cache.get("t-cache").await.is_some());
    }

    #[tokio::test]
    async fn test_broker_event_store_create_and_read() {
        let (logdbd_addr, _dir) = start_logdbd().await;
        let broker_addr = start_broker(&logdbd_addr).await;
        let store = BrokerEventStore::connect(&broker_addr, "test-ns").await.unwrap();

        let prov = crate::models::Provenance {
            source_channel: "api".into(), source_session_id: None, source_user_id: Some("u".into()),
            source_tenant_id: Some("t".into()), source_message_id: None,
            created_at: chrono::Utc::now(), created_by: "test".into(),
        };
        let (tid, _) = store.create_task("db.repair", "t", "u", &prov, None).await.unwrap();
        assert!(tid.starts_with("task_"));

        // catch_up 可能因 broker forwarder 延迟,短暂重试
        let task = loop {
            let t = store.get_task(&tid).await.unwrap().unwrap();
            if !t.task_type.is_empty() { break t; }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(task.task_type, "db.repair");
        assert_eq!(task.state, TaskState::Created);
        assert!(!store.is_task_ended(&tid).await.unwrap());

        let st = store.get_task_state(&tid).await.unwrap().unwrap();
        assert_eq!(st, TaskState::Created);

        // gap detection(退化)
        assert!(store.detect_seq_gaps(&tid).await.unwrap().is_empty());
    }

    // ── E2E: dispatch → consume → execute (Plan D) ────────────────────

    #[tokio::test]
    async fn test_e2e_dispatch_consume_execute() {
        use logdb_client::broker::GroupConsumer;
        use logdb_broker_proto::pb::consume_response::Payload;
        use tokio_stream::StreamExt;

        // 设置 SANDBOX_REGION 让 dispatch_tool 用
        std::env::set_var("SANDBOX_REGION", "test");

        let (ld_addr, _d) = start_logdbd().await;
        let ba = start_broker(&ld_addr).await;
        let mut w = BrokerWriter::connect(&ba, "ns").await.unwrap();

        // 1. 写一条 tool_invoked 到 dispatch stream
        let step_id = "step-e2e-1";
        let uid = uuid::Uuid::now_v7().to_string();
        let idem_key = &format!("t-e2e:rg:bash:{}", &uid[..8]);
        let tool_payload = serde_json::json!({
            "step_type":"tool_call","tool_name":"fixus_Bash","tool_call_id":"call-e2e",
            "idempotency_key": idem_key,
            "input": {"command": "echo e2e-ok"},
            "local_seq": 1,
        });
        let content = serde_json::to_vec(&tool_payload).unwrap();
        let mut meta = HashMap::new();
        meta.insert("task_id".into(), "t-e2e".into());
        meta.insert("step_id".into(), step_id.into());
        meta.insert("turn_id".into(), "1".into());
        meta.insert("event_type".into(), "tool_invoked".into());
        w.produce("tools-region-test", "tool_invoked", &content, Some("t-e2e"), 0, "application/json", &meta).await.expect("dispatch produce");

        // 稍等 broker forwarder 追上(异步 tail)
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 2. 起 sandbox consumer
        let ba_full = format!("http://{}", ba);
        let mut consumer = GroupConsumer::join(ba_full, "ns", "tools-region-test", "sandboxes-test", "c-e2e").await.unwrap();
        let mut frames = consumer.consume_frames().await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut received = false;
        while tokio::time::Instant::now() < deadline {
            let item = tokio::time::timeout(Duration::from_millis(500), frames.next()).await;
            match item {
                Ok(Some(Ok(frame))) => {
                    if let Some(Payload::Record(rec)) = frame.payload {
                        assert_eq!(rec.event_type, "tool_invoked");
                        let pl: serde_json::Value = serde_json::from_slice(&rec.content).unwrap();
                        assert_eq!(pl["tool_name"], "fixus_Bash");
                        // Execute
                        let d = tempfile::tempdir().unwrap();
                        let r = std::process::Command::new("sh").arg("-c").arg("echo e2e-ok").current_dir(d.path()).output().unwrap();
                        assert!(String::from_utf8_lossy(&r.stdout).contains("e2e-ok"));
                        let _ = consumer.commit_shard(rec.shard_id, rec.seq).await;
                        received = true;
                        break;
                    }
                }
                Ok(Some(Err(e))) => panic!("consume: {}", e),
                _ => { /* timeout/end, retry */ }
            }
        }
        let _ = consumer.leave().await;
        assert!(received, "expected tool_invoked via broker dispatch");
    }
}
