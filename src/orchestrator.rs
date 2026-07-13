//! Turn Orchestrator — 中心编排引擎
//!
//! 职责：
//! - execute_turn: 用户请求 → WAL → context构建 → 下发fixlet → 等待结果
//! - handle_turn_execution_done: fixlet done → WAL → 通知HTTP handler
//! - Tool 执行: tools-bank MCP → broker → sandbox-server (不再经 fixus)
//!
//! Turn 编排引擎。fixus 的中心组件:Turn 启动、Claim、恢复、健康检查。

use std::sync::Arc;
use std::time::Duration;

use crate::error::{AppError, Result};
use crate::models::AgentEvent;
use crate::task_registry::{PendingTurn, TaskRegistry, TurnOutcome};
use crate::storage::EventStore;
use crate::{context, recovery, service};

/// 依赖健康状态
#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthStatus {
    pub fixus: &'static str,
    pub broker: DependencyHealth,
    pub sandbox: DependencyHealth,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DependencyHealth {
    pub status: &'static str, // "ok" | "degraded" | "unknown"
    pub last_ok_secs_ago: Option<u64>,
}

/// Turn 编排器
pub struct Orchestrator {
    store: Arc<dyn EventStore>,
    registry: Arc<TaskRegistry>,
    turn_timeout: Duration,
    token_publisher: crate::stream::TokenPublisher,
    last_dispatch_ok: Arc<tokio::sync::Mutex<Option<std::time::Instant>>>,
    last_result_ok: Arc<tokio::sync::Mutex<Option<std::time::Instant>>>,
}

/// 异步 Turn 启动结果(`start_turn_async`)
#[derive(Debug)]
pub enum AsyncTurnStart {
    Started { turn_id: i64 },
    RecoveryTriggered { incomplete_count: usize },
}

impl Orchestrator {
    pub fn new(
        store: Arc<dyn EventStore>,
        registry: Arc<TaskRegistry>,
        token_publisher: crate::stream::TokenPublisher,
    ) -> Self {
        Self {
            store,
            registry,
            turn_timeout: Duration::from_secs(300),
            token_publisher,
            last_dispatch_ok: Arc::new(tokio::sync::Mutex::new(None)),
            last_result_ok: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }


    pub async fn health(&self) -> HealthStatus {
        let now = std::time::Instant::now();
        let dispatch_ago = self.last_dispatch_ok.lock().await.map(|t| now.duration_since(t).as_secs());
        let result_ago = self.last_result_ok.lock().await.map(|t| now.duration_since(t).as_secs());

        let broker_status = match dispatch_ago {
            None => "unknown",
            Some(s) if s < 120 => "ok",
            Some(_) => "degraded",
        };
        let sandbox_status = match result_ago {
            None => "unknown",
            Some(s) if s < 120 => "ok",
            Some(_) => "degraded",
        };

        HealthStatus {
            fixus: "ok",
            broker: DependencyHealth { status: broker_status, last_ok_secs_ago: dispatch_ago },
            sandbox: DependencyHealth { status: sandbox_status, last_ok_secs_ago: result_ago },
        }
    }

    pub fn spawn_result_consumer(
        self: &Arc<Self>,
        broker_addr: &str,
        namespace: &str,
        region: &str,
    ) {
        let orch = self.clone();
        let stream = format!("tool-result-{}", region);
        let broker_addr = broker_addr.to_string();
        let namespace = namespace.to_string();
        // 每次启动用唯一 group 名,避免旧实例的 stale consumer member 占 shard(TODO: 未来 fixus 需做 HA 时改为多成员共享 group)
        let instance_id = uuid::Uuid::now_v7().to_string().replace('-', "");
        let group = format!("fixus-results-{}", &instance_id[..12]);

        tokio::spawn(async move {
            let consumer_id = format!("fixus-result-{}", instance_id);
            tracing::info!(
                "result consumer starting: broker={} stream={} group={} consumer={}",
                broker_addr, stream, group, consumer_id
            );

            loop {
                match run_result_consumer(&orch, &broker_addr, &namespace, &stream, &group, &consumer_id)
                    .await
                {
                    Ok(()) => {
                        tracing::info!("result consumer ended normally");
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        tracing::error!("result consumer error: {}; retrying in 1s", msg);
                        drop(msg);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    async fn resolve_task_type(&self, task_id: &str) -> Result<String> {
        // broker forwarder tail 是异步的:create 写完 task_created 后,
        // catch_up 可能领先于 forwarder 发出 CaughtUp 信号却没看到该事件 → 投影里 task_type 空。
        // 失效缓存重试,给 forwarder 追上的时间。
        for attempt in 0..10u32 {
            let session = self
                .store
                .get_task(task_id)
                .await?
                .ok_or_else(|| AppError::TaskNotFound(task_id.to_string()))?;
            if !session.task_type.is_empty() {
                return Ok(session.task_type);
            }
            tracing::warn!(
                task_id, attempt,
                "task_type empty in projection (broker forwarder lag?) — invalidating + retry"
            );
            self.store.invalidate_projection(task_id).await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        Err(AppError::Internal(format!(
            "task_type unresolved for {} after 10 retries (projection never saw task_created?)",
            task_id
        )))
    }


    // ── Turn 执行入口 ────────────────────────────────────────────────

    pub async fn execute_turn(
        &self,
        task_id: &str,
        user_input: &str,
        redo_group: Option<&str>,
    ) -> Result<TurnOutcome> {
        // 1. 恢复检查 — 检测到未完成 Turn 时触发后台恢复，不阻塞当前请求
        let incomplete = self.store.get_incomplete_turns(task_id).await?;
        if !incomplete.is_empty() {
            let count = incomplete.len();
            tracing::warn!(
                "session {}: {} incomplete turns detected, triggering background recovery",
                task_id,
                count
            );
            self.spawn_background_recovery(task_id.to_string());

            return Ok(TurnOutcome::Completed {
                final_output: format!(
                    "Recovery triggered: {} incomplete turn(s) detected. Retry after recovery completes.",
                    count
                ),
                turn_id: 0,
                event_count: 0,
            });
        }

        // 2. 启动新 Turn（WAL: turn_started）
        let (turn_id, redo_group, _turn_started) =
            service::start_turn(&*self.store, task_id, user_input, redo_group).await?;
        tracing::info!(
            "session {}: turn {} started, redo_group={}",
            task_id,
            turn_id,
            redo_group
        );

        // 3-7. 执行体
        self.run_turn_to_completion(task_id, turn_id, user_input, &redo_group)
            .await
    }

    async fn run_turn_to_completion(
        &self,
        task_id: &str,
        turn_id: i64,
        user_input: &str,
        redo_group: &str,
    ) -> Result<TurnOutcome> {
        // 3. 构建 context（只构建一次，传递给 dispatch）
        let ctx = context::build_llm_context(&*self.store, task_id).await?;

        // 3b. 前置校验 task_type 可解析(不可解析则在此失败,避免注册一个派发不出去的 pending turn)
        self.resolve_task_type(task_id).await?;

        // 4. 创建 PendingTurn(含 oneshot channel,等待 broker task-end 兑现完成通知)
        let (pending, result_rx) = PendingTurn::new(
            task_id.to_string(),
            turn_id,
            redo_group.to_string(),
        );
        self.registry
            .register_pending_turn(task_id, pending)
            .await;

        // 5. 下发 turn.begin(execute_turn) 到 `task-begin-{task_type}`
        // pull-based: fixlet 竞争消费该 stream 认领 turn,无需 fixus 推送、无需 registry 检查。
        self.dispatch_execute_turn_with_ctx(
            task_id,
            turn_id,
            user_input,
            redo_group,
            0,
            &[],
            &ctx,
        )
        .await?;

        // 7. 等待 Turn 完成（超时保护）
        match tokio::time::timeout(self.turn_timeout, result_rx).await {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(_recv_err)) => {
                // oneshot sender 被 dropped（fixlet 断开等）
                tracing::error!(
                    "session {}: pending turn channel closed unexpectedly",
                    task_id
                );
                self.fail_turn_and_respond(
                    task_id,
                    turn_id,
                    "channel_closed",
                    "fixlet connection lost",
                )
                .await
            }
            Err(_elapsed) => {
                // Turn 超时
                tracing::warn!("session {}: turn {} timed out", task_id, turn_id);
                self.fail_turn_and_respond(
                    task_id,
                    turn_id,
                    "timeout",
                    &format!("Turn timed out after {}s", self.turn_timeout.as_secs()),
                )
                .await
            }
        }
    }

    pub async fn start_turn_async(
        &self,
        task_id: &str,
        user_input: &str,
        redo_group: Option<&str>,
    ) -> Result<AsyncTurnStart> {
        // 1. 恢复检查
        let incomplete = self.store.get_incomplete_turns(task_id).await?;
        if !incomplete.is_empty() {
            let count = incomplete.len();
            tracing::warn!(
                "session {}: {} incomplete turns, triggering background recovery",
                task_id,
                count
            );
            self.spawn_background_recovery(task_id.to_string());
            return Ok(AsyncTurnStart::RecoveryTriggered { incomplete_count: count });
        }

        // 2. 启动新 Turn（WAL: turn_started）— turn_id 在此确定
        let (turn_id, redo_group, _turn_started) =
            service::start_turn(&*self.store, task_id, user_input, redo_group).await?;
        tracing::info!(
            "session {}: turn {} started (async), redo_group={}",
            task_id,
            turn_id,
            redo_group
        );

        // 3-7. 后台执行(不阻塞 HTTP 响应)
        let orch = Orchestrator {
            store: self.store.clone(),
            registry: self.registry.clone(),
            turn_timeout: self.turn_timeout,
            token_publisher: self.token_publisher.clone(),
            last_dispatch_ok: Arc::new(tokio::sync::Mutex::new(None)),
            last_result_ok: Arc::new(tokio::sync::Mutex::new(None)),
        };
        let sid = task_id.to_string();
        let ui = user_input.to_string();
        tokio::spawn(async move {
            match orch.run_turn_to_completion(&sid, turn_id, &ui, &redo_group).await {
                Ok(_) => tracing::info!(
                    "session {}: turn {} background execution finished",
                    sid,
                    turn_id
                ),
                Err(e) => tracing::error!(
                    "session {}: turn {} background execution failed: {}",
                    sid,
                    turn_id,
                    e
                ),
            }
        });

        Ok(AsyncTurnStart::Started { turn_id })
    }

    fn spawn_background_recovery(&self, task_id: String) {
        let store = self.store.clone();
        let registry = self.registry.clone();
        let turn_timeout = self.turn_timeout;
        let token_publisher = self.token_publisher.clone();

        tokio::spawn(async move {
            tracing::info!("session {}: background recovery started", task_id);

            let redo_queue = match recovery::recover_task(&*store, &task_id).await {
                Ok(q) => q,
                Err(e) => {
                    tracing::error!("session {}: recovery failed: {}", task_id, e);
                    return;
                }
            };

            if redo_queue.is_empty() {
                tracing::info!("session {}: no turns to redo", task_id);
                return;
            }

            // 创建临时 Orchestrator 用于 dispatch
            let orch = Orchestrator {
                store: store.clone(),
                registry: registry.clone(),
                turn_timeout,
                token_publisher,
                last_dispatch_ok: Arc::new(tokio::sync::Mutex::new(None)),
                last_result_ok: Arc::new(tokio::sync::Mutex::new(None)),
            };

            let mut redo_success = 0;
            let mut redo_failed = 0;

            // 前置校验 task_type 可解析(循环外一次,派发前 fail-fast)
            if let Err(e) = orch.resolve_task_type(&task_id).await {
                tracing::error!(
                    "session {}: recovery cannot resolve task_type: {}",
                    task_id,
                    e
                );
                return;
            }

            for redo_ctx in &redo_queue {
                tracing::info!(
                    "session {}: redo turn {} redo_count={} redo_group={}",
                    task_id,
                    redo_ctx.turn_id,
                    redo_ctx.redo_count,
                    redo_ctx.redo_group
                );

                let (pending, result_rx) = PendingTurn::new(
                    task_id.clone(),
                    redo_ctx.turn_id,
                    redo_ctx.redo_group.clone(),
                );
                registry.register_pending_turn(&task_id, pending).await;

                let cached = orch
                    .get_cached_llm_responses(&task_id, redo_ctx.turn_id)
                    .await;
                if let Err(e) = orch
                    .dispatch_execute_turn(
                        &task_id,
                        redo_ctx.turn_id,
                        &redo_ctx.user_input,
                        &redo_ctx.redo_group,
                        redo_ctx.redo_count,
                        &cached,
                    )
                    .await
                {
                    tracing::error!(
                        "session {}: redo dispatch failed for turn {}: {}",
                        task_id,
                        redo_ctx.turn_id,
                        e
                    );
                    let _ = orch
                        .fail_turn_and_respond(
                            &task_id,
                            redo_ctx.turn_id,
                            "redo_dispatch_failed",
                            &e.to_string(),
                        )
                        .await;
                    redo_failed += 1;
                    continue;
                }

                match tokio::time::timeout(turn_timeout, result_rx).await {
                    Ok(Ok(TurnOutcome::Completed { turn_id, .. })) => {
                        tracing::info!(
                            "session {}: redo turn {} succeeded",
                            task_id,
                            turn_id
                        );
                        redo_success += 1;
                    }
                    Ok(Ok(TurnOutcome::Failed {
                        turn_id,
                        error_type,
                        ..
                    })) => {
                        tracing::error!(
                            "session {}: redo turn {} failed: {}",
                            task_id,
                            turn_id,
                            error_type
                        );
                        redo_failed += 1;
                    }
                    Ok(Ok(TurnOutcome::Timeout { .. })) | Err(_) => {
                        tracing::error!(
                            "session {}: redo turn {} timed out",
                            task_id,
                            redo_ctx.turn_id
                        );
                        let _ = orch
                            .fail_turn_and_respond(
                                &task_id,
                                redo_ctx.turn_id,
                                "redo_timeout",
                                "Redo timed out",
                            )
                            .await;
                        redo_failed += 1;
                    }
                    Ok(Err(_)) => {
                        let _ = orch
                            .fail_turn_and_respond(
                                &task_id,
                                redo_ctx.turn_id,
                                "channel_closed",
                                "fixlet connection lost during redo",
                            )
                            .await;
                        redo_failed += 1;
                    }
                }
            }

            tracing::info!(
                "session {}: background recovery finished — {} succeeded, {} failed",
                task_id,
                redo_success,
                redo_failed
            );
        });
    }

    async fn dispatch_execute_turn(
        &self,
        task_id: &str,
        turn_id: i64,
        user_input: &str,
        redo_group: &str,
        redo_count: i32,
        cached_llm_responses: &[String],
    ) -> Result<()> {
        let ctx = context::build_llm_context(&*self.store, task_id).await?;
        self.dispatch_execute_turn_with_ctx(
            task_id,
            turn_id,
            user_input,
            redo_group,
            redo_count,
            cached_llm_responses,
            &ctx,
        )
        .await
    }

    async fn dispatch_execute_turn_with_ctx(
        &self,
        task_id: &str,
        turn_id: i64,
        user_input: &str,
        redo_group: &str,
        redo_count: i32,
        cached_llm_responses: &[String],
        ctx: &context::LlmContext,
    ) -> Result<()> {
        // 注入缓存：将上次尝试的 LLM 响应作为 context 传给 Agent
        let user_input_with_cache = if cached_llm_responses.is_empty() {
            user_input.to_string()
        } else {
            let mut cached_input = String::from(
                "[CACHED FROM PREVIOUS ATTEMPT — reuse or adapt if applicable]\n",
            );
            for (i, resp) in cached_llm_responses.iter().enumerate() {
                if !resp.is_empty() {
                    cached_input.push_str(&format!(
                        "\nPrevious response {}: {}\n",
                        i + 1,
                        resp
                    ));
                }
            }
            cached_input.push_str(&format!("\n[END CACHE]\n\n{}", user_input));
            cached_input
        };

        let msg = serde_json::json!({
            "type": "execute_turn",
            "session_id": task_id,
            "turn_id": turn_id,
            "input": {"user_input": &user_input_with_cache},
            "context": {
                "summary": ctx.summary,
                "messages": ctx.messages,
            },
            "tools": [
                {"name":"fixus_Bash","description":"Execute a shell command (via fixus sandbox)","parameters":{"type":"object","properties":{"command":{"type":"string","description":"The command to execute"},"description":{"type":"string","description":"Brief description"}},"required":["command"]}},
                {"name":"fixus_Read","description":"Read a file (via fixus sandbox)","parameters":{"type":"object","properties":{"file_path":{"type":"string","description":"Absolute path to the file"},"offset":{"type":"integer"},"limit":{"type":"integer"}},"required":["file_path"]}},
                {"name":"fixus_Write","description":"Write to a file (via fixus sandbox)","parameters":{"type":"object","properties":{"file_path":{"type":"string"},"content":{"type":"string"}},"required":["file_path","content"]}},
                {"name":"fixus_Edit","description":"Edit a file by replacing a string","parameters":{"type":"object","properties":{"file_path":{"type":"string"},"old_string":{"type":"string"},"new_string":{"type":"string"}},"required":["file_path","old_string","new_string"]}},
                {"name":"fixus_Glob","description":"Find files matching a pattern","parameters":{"type":"object","properties":{"pattern":{"type":"string"}},"required":["pattern"]}},
                {"name":"fixus_Grep","description":"Search for a pattern in files","parameters":{"type":"object","properties":{"pattern":{"type":"string"},"path":{"type":"string"}},"required":["pattern"]}}
            ],
            "redo_group": redo_group,
            "redo_count": redo_count,
        });

        // publish turn.begin(execute_turn) 到 `task-begin-{task_type}`
        // pull-based: fixlet 竞争消费该 stream 认领 turn,消费后启动 agent。
        let task_type = self.resolve_task_type(task_id).await?;
        self.store.publish_turn_begin(task_id, &task_type, &msg).await
            .map_err(|e| AppError::Protocol(format!(
                "Failed to publish turn.begin to task-begin-{} for task {}: {}",
                task_type, task_id, e
            )))?;

        tracing::info!(
            "session {}: published turn.begin turn_id={} redo_count={} to task-begin-{}",
            task_id, turn_id, redo_count, task_type
        );

        Ok(())
    }

    // ── Tool 调用处理 ────────────────────────────────────────────────



    async fn get_cached_llm_responses(&self, task_id: &str, turn_id: i64) -> Vec<String> {
        match self.store.get_turn_events(task_id, turn_id).await {
            Ok(events) => events
                .iter()
                .filter(|e| e.event_type == crate::models::EventType::LlmCompleted)
                .filter_map(|e| e.payload.get("content").and_then(|v| v.as_str()))
                .map(|s| s.to_string())
                .collect(),
            Err(_) => vec![],
        }
    }

    // ── Turn 完成处理 ────────────────────────────────────────────────

    pub async fn handle_llm_chunk(
        &self,
        task_id: &str,
        turn_id: i64,
        text: &str,
    ) -> Result<()> {
        self.token_publisher
            .publish(
                task_id,
                turn_id,
                &serde_json::json!({ "type": "llm_chunk", "text": text }).to_string(),
            )
            .await;
        Ok(())
    }

    pub async fn handle_llm_completed(
        &self,
        task_id: &str,
        turn_id: i64,
        model: &str,
        input_tokens: i64,
        output_tokens: i64,
        total_tokens: i64,
    ) -> Result<()> {
        let step_id = uuid::Uuid::now_v7().to_string();
        let payload = serde_json::json!({
            "model": model,
            "usage": {
                "prompt_tokens": input_tokens,
                "completion_tokens": output_tokens,
                "total_tokens": total_tokens,
            },
            "local_seq": 0,
        });

        service::record_event(
            &*self.store,
            task_id,
            Some(turn_id),
            Some(&step_id),
            crate::models::EventType::LlmCompleted,
            payload,
        )
        .await?;

        tracing::info!(
            "session {}: llm_completed turn={} tokens(in={} out={} total={})",
            task_id,
            turn_id,
            input_tokens,
            output_tokens,
            total_tokens
        );

        Ok(())
    }

    pub async fn handle_turn_execution_done(
        &self,
        task_id: &str,
        turn_id: i64,
        max_local_seq: i64,
        final_output: &str,
    ) -> Result<()> {
        tracing::info!(
            "session {}: turn_execution_done turn_id={} max_local_seq={} output_len={}",
            task_id,
            turn_id,
            max_local_seq,
            final_output.len()
        );

        // 1. WAL: 写 turn_completed
        let _event =
            service::complete_turn(&*self.store, task_id, turn_id, final_output).await?;

        // 2. 统计该 Turn 的事件数量
        let turn_events = self
            .store
            .get_turn_events(task_id, turn_id)
            .await?;
        let event_count = turn_events.len() as i64;

        // 3. 通知 HTTP handler
        let outcome = TurnOutcome::Completed {
            final_output: final_output.to_string(),
            turn_id,
            event_count,
        };

        if let Err(e) = self
            .registry
            .complete_pending_turn(task_id, outcome)
            .await
        {
            tracing::warn!(
                "session {}: failed to complete pending turn: {} (client may have disconnected)",
                task_id,
                e
            );
        }

        tracing::info!(
            "session {}: turn {} completed, {} events",
            task_id,
            turn_id,
            event_count
        );

        Ok(())
    }

    pub async fn handle_turn_execution_error(
        &self,
        task_id: &str,
        turn_id: i64,
        error_type: &str,
        error_message: &str,
    ) -> Result<()> {
        tracing::error!(
            "session {}: turn_execution_error turn_id={} type={}: {} — attempting redo",
            task_id,
            turn_id,
            error_type,
            error_message
        );

        // 1. 读取原始 turn_started 获取 redo 上下文
        let turn_events = self
            .store
            .get_turn_events(task_id, turn_id)
            .await?;
        let turn_started = turn_events
            .iter()
            .find(|e| e.event_type == crate::models::EventType::TurnStarted);

        let (user_input, redo_group, redo_count) = match turn_started {
            Some(event) => {
                let input = event
                    .payload
                    .get("user_input")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let rg = event
                    .payload
                    .get("redo_group")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let rc = event
                    .payload
                    .get("redo_count")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i32;
                (input, rg, rc + 1)
            }
            None => {
                // 没有 turn_started，无法 redo
                self.fail_turn_and_respond(task_id, turn_id, error_type, error_message)
                    .await?;
                return Ok(());
            }
        };

        tracing::info!(
            "session {}: redo turn {} redo_count={} redo_group={}",
            task_id,
            turn_id,
            redo_count,
            redo_group
        );

        // 2. 查询上次尝试的 LLM 缓存，注入后分发 redo
        let cached = self.get_cached_llm_responses(task_id, turn_id).await;
        if let Err(e) = self
            .dispatch_execute_turn(
                task_id,
                turn_id,
                &user_input,
                &redo_group,
                redo_count,
                &cached,
            )
            .await
        {
            tracing::error!(
                "session {}: failed to dispatch redo: {}",
                task_id,
                e
            );
            self.fail_turn_and_respond(
                task_id,
                turn_id,
                "redo_dispatch_failed",
                &e.to_string(),
            )
            .await?;
        }

        Ok(())
    }

    // ── MCP Tool 执行 ─────────────────────────────────────────────────

    async fn fail_turn_and_respond(
        &self,
        task_id: &str,
        turn_id: i64,
        error_type: &str,
        error_message: &str,
    ) -> Result<TurnOutcome> {
        // WAL: 写 turn_failed
        if let Err(e) = service::fail_turn(
            &*self.store,
            task_id,
            turn_id,
            error_type,
            error_message,
            None,
        )
        .await
        {
            tracing::error!(
                "session {}: failed to write turn_failed: {}",
                task_id,
                e
            );
        }

        let outcome = TurnOutcome::Failed {
            turn_id,
            error_type: error_type.to_string(),
            error_message: error_message.to_string(),
        };

        // 通知 HTTP handler
        let _ = self
            .registry
            .complete_pending_turn(task_id, outcome.clone())
            .await;

        Ok(outcome)
    }

    pub fn spawn_lifecycle_consumer(
        self: &Arc<Self>,
        broker_addr: &str,
        namespace: &str,
    ) {
        let orch = self.clone();
        let broker_addr = broker_addr.to_string();
        let namespace = namespace.to_string();
        let instance_id = uuid::Uuid::now_v7().to_string().replace('-', "");
        let group = format!("fixus-lifecycle-{}", &instance_id[..12]);

        tokio::spawn(async move {
            let consumer_id = format!("fixus-lifecycle-{}", instance_id);
            tracing::info!(
                "lifecycle consumer starting: broker={} stream=task-end group={} consumer={}",
                broker_addr, group, consumer_id
            );
            loop {
                match run_lifecycle_consumer(&orch, &broker_addr, &namespace, &group, &consumer_id).await {
                    Ok(()) => tracing::info!("lifecycle consumer ended normally"),
                    Err(e) => {
                        let msg = e.to_string();
                        tracing::error!("lifecycle consumer error: {}; retrying in 1s", msg);
                        drop(msg);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }
}

// ── Broker Result Consumer ─────────────────────────────────────────────

/// 后台消费 `tool-result-<region>` stream,将 sandbox-server 回传的工具结果
/// 消费 `tool-result-<region>` stream,记录工具结果用于投影和健康检查。
async fn run_result_consumer(
    orch: &Arc<Orchestrator>,
    broker_addr: &str,
    namespace: &str,
    stream: &str,
    group: &str,
    consumer_id: &str,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use logdb_broker_proto::pb::consume_response::Payload;
    use logdb_client::broker::GroupConsumer;
    use tokio_stream::StreamExt;

    let mut consumer = GroupConsumer::join(
        format!("http://{}", broker_addr),
        namespace,
        stream,
        group,
        consumer_id,
    )
    .await
    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    tracing::info!(
        "result consumer joined {} ({}), assigned shards: {:?}",
        group,
        consumer_id,
        consumer.assigned_shards()
    );

    let mut frames = consumer
        .consume_frames()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    // 连续错误计数器:达到阈值时认为 stream 已断,跳出循环让外层 rejoin
    let mut consecutive_errors: u32 = 0;
    while let Some(item) = frames.next().await {
        let frame = match item {
            Ok(f) => {
                consecutive_errors = 0;
                f
            }
            Err(e) => {
                consecutive_errors += 1;
                tracing::error!("result consume error (consecutive={}): {}", consecutive_errors, e);
                if consecutive_errors >= 3 {
                    return Err(format!("result consumer: {} consecutive consume errors, rejoining", consecutive_errors).into());
                }
                continue;
            }
        };
        match frame.payload {
            Some(Payload::Record(rec)) => {
                if rec.event_type != "tool_result" {
                    continue;
                }
                let result: serde_json::Value =
                    serde_json::from_slice(&rec.content).unwrap_or_default();
                let step_id = result["step_id"].as_str().unwrap_or("");
                if step_id.is_empty() {
                    tracing::warn!("result record missing step_id");
                    continue;
                }

                let success = result["success"].as_bool().unwrap_or(false);
                let duration_ms = result["duration_ms"].as_u64().unwrap_or(0);
                tracing::info!(
                    "result consumer: step_id={} success={} duration_ms={}",
                    step_id, success, duration_ms
                );
                // 记录结果到达时间(健康检查用)
                *orch.last_result_ok.lock().await = Some(std::time::Instant::now());

                let _ = consumer.commit_shard(rec.shard_id, rec.seq).await;
            }
            Some(Payload::CaughtUp(_))
            | Some(Payload::Rebalance(_))
            | Some(Payload::Assignment(_)) => {}
            None => {}
        }
    }
    consumer.leave().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    Ok(())
}

// ── Broker Lifecycle Consumer (free fn) ────────────────────────────────

async fn run_lifecycle_consumer(
    orch: &Arc<Orchestrator>,
    broker_addr: &str,
    namespace: &str,
    group: &str,
    consumer_id: &str,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use logdb_broker_proto::pb::consume_response::Payload;
    use logdb_client::broker::GroupConsumer;
    use tokio_stream::StreamExt;

    let mut consumer = GroupConsumer::join(
        format!("http://{}", broker_addr),
        namespace,
        "task-end",
        group,
        consumer_id,
    )
    .await
    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    tracing::info!("lifecycle consumer joined {} ({}) stream=task-end, shards: {:?}", group, consumer_id, consumer.assigned_shards());

    let mut frames = consumer.consume_frames().await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    let mut consecutive_errors: u32 = 0;
    while let Some(item) = frames.next().await {
        let frame = match item {
            Ok(f) => { consecutive_errors = 0; f }
            Err(e) => {
                consecutive_errors += 1;
                tracing::error!("lifecycle consume error (consecutive={}): {}", consecutive_errors, e);
                if consecutive_errors >= 3 {
                    return Err(format!("{} consecutive lifecycle errors, rejoining", consecutive_errors).into());
                }
                continue;
            }
        };
        match frame.payload {
            Some(Payload::Record(rec)) => {
                let payload: serde_json::Value = serde_json::from_slice(&rec.content).unwrap_or_default();
                let task_id = payload["task_id"].as_str().unwrap_or("");
                if task_id.is_empty() { continue; }

                match rec.event_type.as_str() {
                    "turn_execution_done" => {
                        let turn_id = payload["turn_id"].as_i64().unwrap_or(0);
                        let max_local_seq = payload["max_local_seq"].as_i64().unwrap_or(0);
                        let final_output = payload["final_output"].as_str().unwrap_or("");
                        if turn_id == 0 { continue; }
                        tracing::info!("lifecycle: turn_execution_done task={} turn={}", task_id, turn_id);
                        let _ = orch.handle_turn_execution_done(task_id, turn_id, max_local_seq, final_output).await;
                    }
                    "llm_completed" => {
                        let turn_id = payload["turn_id"].as_i64().unwrap_or(0);
                        let model = payload["model"].as_str().unwrap_or("");
                        let input_tokens = payload.get("input_tokens").and_then(|v| v.as_i64()).unwrap_or(0);
                        let output_tokens = payload.get("output_tokens").and_then(|v| v.as_i64()).unwrap_or(0);
                        let total_tokens = payload.get("total_tokens").and_then(|v| v.as_i64()).unwrap_or(0);
                        if turn_id == 0 { continue; }
                        tracing::info!("lifecycle: llm_completed task={} turn={} tokens={}", task_id, turn_id, total_tokens);
                        let _ = orch.handle_llm_completed(task_id, turn_id, model, input_tokens, output_tokens, total_tokens).await;
                    }
                    "turn_execution_error" => {
                        let turn_id = payload["turn_id"].as_i64().unwrap_or(0);
                        let error_type = payload["error_type"].as_str().unwrap_or("unknown");
                        let error_message = payload["error_message"].as_str().unwrap_or("");
                        if turn_id == 0 { continue; }
                        tracing::info!("lifecycle: turn_execution_error task={} turn={} type={}", task_id, turn_id, error_type);
                        let _ = orch.handle_turn_execution_error(task_id, turn_id, error_type, error_message).await;
                    }
                    _ => {}
                }
                let _ = consumer.commit_shard(rec.shard_id, rec.seq).await;
            }
            _ => {}
        }
    }
    consumer.leave().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    Ok(())
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{EventType, Provenance, TaskState};
    use crate::storage::LogdbdEventStore;
    use crate::stream::TokenPublisher;
    use crate::task_registry::TaskRegistry;

    use logdbd::catalog::Catalog;
    use logdbd::consumer::ConsumerTracker;
    use logdbd::pb::log_db_service_server::LogDbServiceServer;
    use logdbd::service::LogDbServiceImpl;
    use logdbd::storage::Storage;
    use logdbd::subscribe::SubscribeHub;
    use logdb::Config as LogdbConfig;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    fn test_provenance() -> Provenance {
        Provenance {
            source_channel: "api".into(),
            source_session_id: None,
            source_user_id: Some("u".into()),
            source_tenant_id: Some("t".into()),
            source_message_id: None,
            created_at: chrono::Utc::now(),
            created_by: "test".into(),
        }
    }

    async fn setup() -> (LogdbdEventStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = LogdbConfig::default();
        cfg.data_dir = dir.path().to_path_buf();
        cfg.durability_mode = logdb::DurabilityMode::Sync;
        cfg.ring_size = 256;
        cfg.shards = 1;
        cfg.flush_timeout = Duration::from_secs(5);
        let db = logdb::LogDb::open(cfg).unwrap();
        let storage = Arc::new(Storage::new(db, 1));
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
        let store = LogdbdEventStore::connect(&addr, "fixus-orch-test").await.unwrap();
        (store, dir)
    }

    async fn wait_seq(store: &dyn EventStore, sid: &str, expected: i64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(v) = store.get_max_seq(sid).await {
                if v >= expected {
                    return;
                }
            }
            if std::time::Instant::now() >= deadline {
                panic!("seq {} not reached for {}", expected, sid);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn make_orch(
        store: Arc<dyn EventStore>,
        registry: Arc<TaskRegistry>,
        token_publisher: TokenPublisher,
    ) -> Orchestrator {
        Orchestrator::new(store, registry, token_publisher)
    }

    #[tokio::test]
    async fn health_status_all_unknown_when_fresh() {
        let (store, _d) = setup().await;
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(Arc::new(store), registry, tp);
        let h = orch.health().await;
        assert_eq!(h.fixus, "ok");
        assert_eq!(h.broker.status, "unknown");
        assert_eq!(h.sandbox.status, "unknown");
        assert!(h.broker.last_ok_secs_ago.is_none());
        assert!(h.sandbox.last_ok_secs_ago.is_none());
    }

    #[tokio::test]
    async fn health_status_transitions_after_tracking() {
        let (store, _d) = setup().await;
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(Arc::new(store), registry, tp);

        // Initially unknown
        let h = orch.health().await;
        assert_eq!(h.broker.status, "unknown");

        // Simulate successful dispatch
        *orch.last_dispatch_ok.lock().await = Some(std::time::Instant::now());
        let h = orch.health().await;
        assert_eq!(h.broker.status, "ok");
        assert!(h.broker.last_ok_secs_ago.unwrap() < 2);

        // Simulate result received
        *orch.last_result_ok.lock().await = Some(std::time::Instant::now());
        let h = orch.health().await;
        assert_eq!(h.sandbox.status, "ok");
    }

}
