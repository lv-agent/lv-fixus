//! Turn Orchestrator — 中心编排引擎
//!
//! 职责：
//! - execute_turn: 用户请求 → WAL → context构建 → 下发fixlet → 等待结果
//! - handle_tool_invoked: fixlet tool_call → WAL → sandbox → tool_result
//! - handle_turn_execution_done: fixlet done → WAL → 通知HTTP handler
//!
//! Turn 编排引擎。fixus 的中心组件:Turn 启动、Claim、恢复、健康检查。

use std::path::PathBuf;
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
    /// Turn 级超时（默认 5 分钟）
    turn_timeout: Duration,
    /// Token 逐字流式发布(Redis ephemeral 快路径,仅 llm_chunk)
    token_publisher: crate::stream::TokenPublisher,
    /// 项目工作目录(SANDBOX_WORKSPACE env 或 current_dir),注入 dispatch payload 供 sandbox-server 路径校验
    work_dir: PathBuf,
    /// 最近一次成功 dispatch 时间(用于健康检查)
    last_dispatch_ok: Arc<tokio::sync::Mutex<Option<std::time::Instant>>>,
    /// 最近一次收到 sandbox 结果的时间(用于健康检查)
    last_result_ok: Arc<tokio::sync::Mutex<Option<std::time::Instant>>>,
}

/// 异步 Turn 启动结果(`start_turn_async`)
#[derive(Debug)]
pub enum AsyncTurnStart {
    /// Turn 已启动,后台执行中;客户端凭 turn_id 连 fixus-stream SSE 看进度
    Started { turn_id: i64 },
    /// 检测到未完成 Turn,已触发后台恢复;本次无新 Turn
    RecoveryTriggered { incomplete_count: usize },
}

/// claim 处理结果(spec §8.3 pull-based 认领)
#[derive(Debug)]
pub enum ClaimOutcome {
    /// 认领成功:已写 task_claimed,下发 claim_granted 给执行器
    Granted {
        task_id: String,
        task_type: String,
        task_brief: String,
    },
    /// 认领拒绝:无 ready Task 或状态迁移失败
    Denied {
        reason: String,
    },
}

impl Orchestrator {
    pub fn new(
        store: Arc<dyn EventStore>,
        registry: Arc<TaskRegistry>,
        token_publisher: crate::stream::TokenPublisher,
    ) -> Self {
        let work_dir = std::env::var("SANDBOX_WORKSPACE")
            .ok()
            .map(PathBuf::from)
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        tracing::info!("orchestrator work_dir={}", work_dir.display());
        Self {
            store,
            registry,
            turn_timeout: Duration::from_secs(300),
            token_publisher,
            work_dir,
            last_dispatch_ok: Arc::new(tokio::sync::Mutex::new(None)),
            last_result_ok: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }


    /// 返回依赖健康状态。
    /// - broker: 基于最近一次成功 dispatch 的时间(>2min 未 dispatch → degraded)
    /// - sandbox: 基于最近一次收到结果的时间(>2min 未收到 → degraded)
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

    /// 启动 broker result consumer 后台任务。
    ///
    /// 消费 `tool-results-region-<region>` stream,用于投影更新和健康检查。
    /// tools-bank 独立处理 MCP 响应,fixus 只做事件消费(不再管理 oneshot)。
    pub fn spawn_result_consumer(
        self: &Arc<Self>,
        broker_addr: &str,
        namespace: &str,
        region: &str,
    ) {
        let orch = self.clone();
        let stream = format!("tool-results-region-{}", region);
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

    /// 解析 session 的 task_type(用于按 task_type 路由到 fixlet)。
    /// task_type 是 session 创建时落库的独立业务字段,非事件派生。
    async fn resolve_task_type(&self, task_id: &str) -> Result<String> {
        let session = self
            .store
            .get_task(task_id)
            .await?
            .ok_or_else(|| AppError::TaskNotFound(task_id.to_string()))?;
        Ok(session.task_type)
    }

    /// 处理 fixlet claim 请求(spec §8.3 pull-based 认领)。
    ///
    /// 1. 从 registry claim 队列匹配一个 ready Task(按 preferred_claimant 优先)
    /// 2. service 层写 task_claimed(校验状态不变量;ready → claimed)
    /// 3. 取 task_brief(body 编译产物),随 ClaimOutcome 返回,由 server 层下发 claim_granted
    ///
    /// 注:claim_granted 的 fixlet 侧执行(Turn 启动 + executing/succeeded 流转)在 Plan D 串联。
    pub async fn handle_claim(
        &self,
        task_type: &str,
        claimant: &str,
    ) -> Result<ClaimOutcome> {
        let Some(claimed) = self.registry.claim_next(task_type, claimant).await else {
            return Ok(ClaimOutcome::Denied {
                reason: format!("no ready task for task_type {}", task_type),
            });
        };

        if let Err(e) = service::claim_task(&*self.store, &claimed.task_id, claimant).await {
            tracing::warn!(
                "claim_task failed for {}: {} (state race? re-enqueue skipped)",
                claimed.task_id,
                e
            );
            return Ok(ClaimOutcome::Denied {
                reason: format!("claim transition failed: {}", e),
            });
        }

        let task = self
            .store
            .get_task(&claimed.task_id)
            .await?
            .ok_or_else(|| AppError::TaskNotFound(claimed.task_id.clone()))?;
        let task_brief = task
            .body
            .as_ref()
            .and_then(|b| b.get("task_brief"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        tracing::info!(
            "task {}: claimed by {} (task_type={})",
            claimed.task_id,
            claimant,
            claimed.task_type
        );

        Ok(ClaimOutcome::Granted {
            task_id: claimed.task_id,
            task_type: claimed.task_type,
            task_brief,
        })
    }

    // ── Turn 执行入口 ────────────────────────────────────────────────

    /// 执行一个完整的 Turn
    ///
    /// 这是用户 POST /turns 的入口。
    /// 自动处理恢复、context 构建、fixlet 下发、Sandbox 调度、结果返回。
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

    /// Turn 执行体(turn_started 已写入、turn_id 已知):
    /// 构建 context → 注册 PendingTurn → 检查 fixlet → 派发 → 等待完成(超时/失败处理)。
    /// 被 `execute_turn`(同步)与 `start_turn_async`(后台)复用。
    async fn run_turn_to_completion(
        &self,
        task_id: &str,
        turn_id: i64,
        user_input: &str,
        redo_group: &str,
    ) -> Result<TurnOutcome> {
        // 3. 构建 context（只构建一次，传递给 dispatch）
        let ctx = context::build_llm_context(&*self.store, task_id).await?;

        // 3b. 解析 task_type —— 按 task_type 路由到 fixlet(不再按 task_id)
        let task_type = self.resolve_task_type(task_id).await?;

        // 4. 创建 PendingTurn（含 oneshot channel，等待完成通知;记录 task_type 便于 fixlet 断连快速失败）
        let (pending, result_rx) = PendingTurn::new(
            task_id.to_string(),
            task_type.clone(),
            turn_id,
            redo_group.to_string(),
        );
        self.registry
            .register_pending_turn(task_id, pending)
            .await;

        // 5. 检查服务于该 task_type 的 fixlet 是否已连接
        if self
            .registry
            .get_fixlet_for_task_type(&task_type)
            .await
            .is_none()
        {
            self.fail_turn_and_respond(
                task_id,
                turn_id,
                "no_fixlet",
                &format!("No fixlet connected for task_type {}", task_type),
            )
            .await?;
            return Err(AppError::Protocol(format!(
                "No fixlet connected for task_type {} (session {})",
                task_type, task_id
            )));
        }

        // 6. 下发 execute_turn 给 fixlet（复用已构建的 context）
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

    /// 异步启动 Turn:写 turn_started 后**立即返回 turn_id**,执行体在后台进行。
    /// 客户端凭 turn_id 连 fixus-stream SSE,实时看事件 + token 流式。
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
            work_dir: PathBuf::from("/tmp"),
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

    /// 后台恢复：检测未完成 Turn，逐个 redo，不阻塞 HTTP 请求。
    ///
    /// 恢复完成后客户端可以重新发起 execute_turn。
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
                work_dir: PathBuf::from("/tmp"),
                last_dispatch_ok: Arc::new(tokio::sync::Mutex::new(None)),
                last_result_ok: Arc::new(tokio::sync::Mutex::new(None)),
            };

            let mut redo_success = 0;
            let mut redo_failed = 0;

            // task_type 是 session 级常量,循环外解析一次(按 task_type 路由 redo)
            let task_type = match orch.resolve_task_type(&task_id).await {
                Ok(at) => at,
                Err(e) => {
                    tracing::error!(
                        "session {}: recovery cannot resolve task_type: {}",
                        task_id,
                        e
                    );
                    return;
                }
            };

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
                    task_type.clone(),
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

    /// 下发 execute_turn 消息给 fixlet（redo 路径，需刷新 context）
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

    /// 下发 execute_turn（复用已构建的 context，避免重复查询）
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

        // 按 session 的 task_type 路由到对应 fixlet
        let task_type = self.resolve_task_type(task_id).await?;
        self.registry
            .send_to_fixlet_for_task_type(&task_type, &msg.to_string())
            .await
            .map_err(|e| {
                AppError::Protocol(format!("Failed to dispatch execute_turn: {}", e))
            })?;

        tracing::info!(
            "session {}: dispatched execute_turn turn_id={} redo_count={}",
            task_id,
            turn_id,
            redo_count
        );

        Ok(())
    }

    // ── Tool 调用处理 ────────────────────────────────────────────────



    /// 从 WAL 读取上次尝试的 LLM 缓存（同 turn_id 下所有 llm_completed）
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

    /// 回传 tool_result 到 fixlet
    // ── Turn 完成处理 ────────────────────────────────────────────────

    /// 处理 fixlet 上报的 llm_chunk（流式 token）
    ///
    /// token 频率太高,不入 append-only 事件库;走 Redis ephemeral 快路径,
    /// fixus-stream SUBSCRIBE 同一通道,与 logdbd 事件流 fan-in 转 SSE。
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

    /// 处理 fixlet 上报的 llm_completed
    ///
    /// 写入 llm_completed Event 到 WAL，包含 token 用量。
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

    /// 处理 fixlet 上报的 turn_execution_done
    ///
    /// 1. WAL: 写 turn_completed
    /// 2. 通知 HTTP handler（oneshot）
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

    /// 处理 fixlet 上报的 turn_execution_error
    ///
    /// Agent 进程异常退出时调用。
    /// 不立即 fail——先尝试 redo。只有 redo 也失败才写 turn_failed。
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

    /// 执行 MCP 工具调用（由 POST /mcp 触发）
    ///
    /// 1. WAL: tool_invoked
    /// 2. Sandbox 执行
    /// 3. WAL: tool_completed 或 tool_failed
    /// 4. 返回执行结果
    /// 写 turn_failed 并通知 HTTP handler
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

    /// 启动 `task-lifecycle` stream 消费者,接收 fixlet 的 turn_execution_done。
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
                "lifecycle consumer starting: broker={} stream=task-lifecycle group={} consumer={}",
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

/// 后台消费 `tool-results-region-<region>` stream,将 sandbox-server 回传的工具结果
/// 消费 `tool-results-region-<region>` stream,记录工具结果用于投影和健康检查。
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
        "task-lifecycle",
        group,
        consumer_id,
    )
    .await
    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    tracing::info!("lifecycle consumer joined {} ({}), shards: {:?}", group, consumer_id, consumer.assigned_shards());

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
                if rec.event_type != "turn_execution_done" { continue; }
                let payload: serde_json::Value = serde_json::from_slice(&rec.content).unwrap_or_default();
                let task_id = payload["task_id"].as_str().unwrap_or("");
                let turn_id = payload["turn_id"].as_i64().unwrap_or(0);
                let max_local_seq = payload["max_local_seq"].as_i64().unwrap_or(0);
                let final_output = payload["final_output"].as_str().unwrap_or("");
                if task_id.is_empty() || turn_id == 0 { continue; }
                tracing::info!("lifecycle: turn_execution_done task={} turn={}", task_id, turn_id);
                let _ = orch.handle_turn_execution_done(task_id, turn_id, max_local_seq, final_output).await;
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

    /// 构建一个接好 store + registry 的 Orchestrator(TokenPublisher 无 Redis 降级)
    fn make_orch(
        store: Arc<dyn EventStore>,
        registry: Arc<TaskRegistry>,
        token_publisher: TokenPublisher,
    ) -> Orchestrator {
        Orchestrator::new(store, registry, token_publisher)
    }

    #[tokio::test]
    async fn handle_claim_denied_when_no_ready_task() {
        let (store, _d) = setup().await;
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = make_orch(Arc::new(store), registry, tp);

        // 空队列 → Denied
        match orch.handle_claim("db.repair", "fixlet-1").await.unwrap() {
            ClaimOutcome::Denied { reason } => {
                assert!(reason.contains("no ready task"), "reason: {}", reason);
            }
            other => panic!("expected Denied, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn handle_claim_granted_writes_task_claimed_and_brief() {
        let (store, _d) = setup().await;
        let store_arc: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = make_orch(store_arc.clone(), registry.clone(), tp);

        // 创建 Task,body 带 task_brief
        let body = serde_json::json!({ "task_brief": "fix db1 deadlocks" });
        let prov = test_provenance();
        let (tid, _) = service::create_task(&*store_arc, "db.repair", &prov, Some(&body))
            .await
            .unwrap();
        wait_seq(&*store_arc, &tid, 1).await;

        // readiness 通过 + 入 claim 队列
        service::mark_task_ready(&*store_arc, &tid).await.unwrap();
        wait_seq(&*store_arc, &tid, 2).await;
        registry
            .enqueue_ready(tid.clone(), "db.repair".into(), None)
            .await;

        // claim → Granted
        match orch.handle_claim("db.repair", "fixlet-1").await.unwrap() {
            ClaimOutcome::Granted {
                task_id,
                task_type,
                task_brief,
            } => {
                assert_eq!(task_id, tid);
                assert_eq!(task_type, "db.repair");
                assert_eq!(task_brief, "fix db1 deadlocks");
            }
            other => panic!("expected Granted, got {:?}", other),
        }

        // task_claimed 已写入 → state == Claimed
        wait_seq(&*store_arc, &tid, 3).await;
        assert_eq!(
            store_arc.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Claimed)
        );

        // 队列已消费 → 再 claim 同类型 → Denied
        match orch.handle_claim("db.repair", "fixlet-2").await.unwrap() {
            ClaimOutcome::Denied { .. } => {}
            other => panic!("expected Denied after drain, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn handle_claim_denied_when_state_not_ready_invariant_guard() {
        // 队列里塞了一个尚未 ready(Created 态)的 Task → claim_task 应拒绝(不变量)
        let (store, _d) = setup().await;
        let store_arc: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = make_orch(store_arc.clone(), registry.clone(), tp);

        let prov = test_provenance();
        let (tid, _) = service::create_task(&*store_arc, "db.repair", &prov, None)
            .await
            .unwrap();
        wait_seq(&*store_arc, &tid, 1).await;
        // 故意不 mark_task_ready 就入队(模拟竞态/bug)
        registry
            .enqueue_ready(tid.clone(), "db.repair".into(), None)
            .await;

        match orch.handle_claim("db.repair", "fixlet-1").await.unwrap() {
            ClaimOutcome::Denied { reason } => {
                assert!(
                    reason.contains("claim transition failed"),
                    "reason: {}",
                    reason
                );
            }
            other => panic!("expected Denied (invariant), got {:?}", other),
        }
        // 状态仍是 Created(未被错误迁移)
        assert_eq!(
            store_arc.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Created)
        );
    }

    // ── #77: Health 相关测试 ────────────────────────────────────────────

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
