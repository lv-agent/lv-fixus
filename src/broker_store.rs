//! Broker-backed EventStore — 所有写经 BrokerServiceClient,所有读经 ProjectionCache。
//!
//! 生产路径:fixus → broker → logdbd。测试路径:内嵌 broker + logdbd harness。

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::transport::Channel;

use logdb_client::broker::BrokerProducer;
use logdb_broker_proto::pb::broker_service_client::BrokerServiceClient;
use logdb_broker_proto::pb::ProduceRequest;

use crate::error::{AppError, Result};
use crate::models::AgentEvent;

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
}
