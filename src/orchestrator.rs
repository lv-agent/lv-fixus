//! Turn Orchestrator — 中心编排引擎
//!
//! 职责：
//! - execute_turn: 用户请求 → WAL → context构建 → 下发fixlet → 等待结果
//! - handle_turn_execution_done: fixlet done → WAL → 通知HTTP handler
//! - Tool 执行: tools-bank MCP → broker → sandbox-server (不再经 fixus)
//!
//! Turn 编排引擎。fixus 的中心组件:Turn 启动、Claim、恢复、健康检查。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{AppError, Result};
use crate::metrics::{Metrics, OUTCOME_FAILED, OUTCOME_SUCCESS};
use crate::models::{classify_failure, AgentEvent, FailureReason};
use crate::dispatcher::{Dispatcher, QueuedTurn};
use crate::retry::{RetryDecision, RetryPolicy};
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
    /// Timeout 自动延长次数(step 1,本轮改):每个 turn 最多延长 N 次再走 fail_turn_and_respond("timeout")。
    /// 总 wall clock 上限 = `initial * (1 + f + f² + ... + f^N)`(累计 deadline)。
    turn_max_extensions: u32,
    /// Timeout 延长因子(默认 1.5 = 50%)。`compute_extension_deadlines` 用它推导每次延长的 deadline。
    turn_extension_factor: f64,
    token_publisher: crate::stream::TokenPublisher,
    /// 失败重试预算(CR-3)
    retry_policy: RetryPolicy,
    /// 派发调度器(CR-1+2):per-task_type 优先队列 + 并发闸
    dispatcher: Arc<Dispatcher>,
    /// per-turn 重试计数器(in-memory,key = `{task_id}:{turn_id}`)。CR-3。
    ///
    /// **已知局限**:fixus 重启会重置;重启后由 `recovery.rs` 接管(独立有界路径)。
    /// 跨重启持久化预算 = 后续 CR-3c(加 `TurnRetryScheduled` 持久事件)。
    retry_attempts: Arc<tokio::sync::Mutex<HashMap<String, i32>>>,
    /// 业务指标(CR-4)。进程内默认实例;`server::start` 与 `/metrics` handler 共享同一 `Arc`。
    metrics: Arc<Metrics>,
    /// per-turn 首次派发时刻(key = `{task_id}:{turn_id}`),CR-4 turn 执行时长计时。
    /// 只在首次派发(`dispatch_pending`)写入;turn 级 Retry 不重置 → 终态观察到「首次派发→终态」总壁钟。
    /// 终态唯一(CR-3 保证),`remove` 恰好一次,无泄漏。跨重启丢失(同 retry_attempts 局限)。
    dispatch_times: Arc<tokio::sync::Mutex<HashMap<String, std::time::Instant>>>,
    last_dispatch_ok: Arc<tokio::sync::Mutex<Option<std::time::Instant>>>,
    last_result_ok: Arc<tokio::sync::Mutex<Option<std::time::Instant>>>,
}

/// 异步 Turn 启动结果(`start_turn_async`)
#[derive(Debug)]
pub enum AsyncTurnStart {
    Started { turn_id: i64 },
    RecoveryTriggered { incomplete_count: usize },
}

/// 解析 dispatch_times 的 key `{task_id}:{turn_id}` → (task_id, turn_id)。
/// task_id(task_<uuid>,无冒号)与 turn_id 之间用最后一个冒号分隔(防御)。
fn parse_turn_key(key: &str) -> Option<(String, i64)> {
    let (tid, turn) = key.rsplit_once(':')?;
    Some((tid.to_string(), turn.parse().ok()?))
}

/// 推导 cumulative wall-clock deadline 表(step 1 turn_timeout 延长机制)。
///
/// 返回从 turn 派发起算的累计 deadline 列表:`[initial, initial*f, initial*f², ...]`。
/// 共 `max_extensions + 1` 个 deadline。例:`(600s, 3, 1.5)` = `[600s, 900s, 1350s, 2025s]`,
/// total wall-clock ≈ 33.75 min。`run_turn_to_completion` 用此表做"到点延长"循环。
pub fn compute_extension_deadlines(
    initial: Duration,
    max_extensions: u32,
    factor: f64,
) -> Vec<Duration> {
    let mut deadlines = vec![initial];
    let mut current = initial;
    for _ in 0..max_extensions {
        current = Duration::from_secs_f64(current.as_secs_f64() * factor);
        deadlines.push(current);
    }
    deadlines
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
            // 默认 600s + 延长 3 次(1.5x),total wall clock ≈ 33.75min;env FIXUS_TURN_TIMEOUT_SECS 可配。
            turn_timeout: Duration::from_secs(600),
            turn_max_extensions: 3,
            turn_extension_factor: 1.5,
            token_publisher,
            retry_policy: RetryPolicy::default(),
            dispatcher: Arc::new(Dispatcher::new(6)),
            retry_attempts: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            metrics: Metrics::new(),
            dispatch_times: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            last_dispatch_ok: Arc::new(tokio::sync::Mutex::new(None)),
            last_result_ok: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// 覆盖默认 retry 预算(CR-3)。`max_attempts` = 一个 turn 最多重试次数(不含首跑)。
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// 覆盖默认 per-type 并发上限(CR-1+2)。默认 6。
    pub fn with_max_concurrent(mut self, max_concurrent: usize) -> Self {
        self.dispatcher = Arc::new(Dispatcher::new(max_concurrent));
        self
    }

    /// 覆盖默认 turn wall-clock 超时(step 1)。默认 600s;server.rs 用 env 注入。
    pub fn with_turn_timeout(mut self, timeout: Duration) -> Self {
        self.turn_timeout = timeout;
        self
    }

    /// 覆盖默认 turn timeout 延长策略(step 1)。`max=0` 退化为原一刀切;`factor > 1.0`。
    pub fn with_turn_extensions(mut self, max: u32, factor: f64) -> Self {
        assert!(factor > 1.0, "turn_extension_factor must be > 1.0");
        self.turn_max_extensions = max;
        self.turn_extension_factor = factor;
        self
    }

    /// 注入共享的业务指标实例(CR-4)。默认 `new()` 自建一个;`server::start` 让
    /// orchestrator 与 `/metrics` handler 共享同一 `Arc<Metrics>` 时用此注入同一实例。
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// 取业务指标句柄(CR-4),供 `/metrics` handler 渲染。
    pub fn metrics_handle(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// per-type `(task_type, pending, in_flight)` 快照(CR-4),供 Gauge pull。
    pub fn dispatcher_counts(&self) -> Vec<(String, usize, usize)> {
        self.dispatcher.snapshot()
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
        // 3. 前置校验 task_type 可解析(不可解析则在此失败,避免注册一个派发不出去的 pending turn)
        self.resolve_task_type(task_id).await?;

        // 4. 创建 PendingTurn(含 oneshot channel,等待 broker task-end 兑现完成通知)
        let (pending, mut result_rx) = PendingTurn::new(
            task_id.to_string(),
            turn_id,
            redo_group.to_string(),
        );
        self.registry
            .register_pending_turn(task_id, pending)
            .await;

        // 5. 经调度器入队 + 派发(CR-1+2:按 priority/容量;redo_count=0,空 cache)。
        //    pull-based: fixlet 竞争消费 `task-begin-{type}` 认领 turn。
        self.enqueue_and_dispatch(task_id, turn_id, user_input, redo_group, 0, vec![])
            .await?;

        // 7. 等待 Turn 完成（带"到点延长"超时保护，step 1）
        //    一刀切 wall-clock 对长任务误杀，本轮改为累计 deadline 表 + N 次延长。
        //    max_extensions=0 时退化为原行为（单次 deadline = turn_timeout）。
        let deadlines = compute_extension_deadlines(
            self.turn_timeout,
            self.turn_max_extensions,
            self.turn_extension_factor,
        );
        let turn_start = std::time::Instant::now();
        let total_attempts = deadlines.len();
        let mut attempt = 0;

        loop {
            // deadlines[attempt] 是从 turn_start 起算的累计 wall-clock 截止时间
            // (因 timeout() 接受"剩余时长"，这里用累计差值等价表达)。
            let elapsed = turn_start.elapsed();
            let remaining = deadlines[attempt].saturating_sub(elapsed);
            tracing::info!(
                "session {}: turn {} waiting (attempt {}/{}, elapsed={}s, next deadline=+{}s)",
                task_id,
                turn_id,
                attempt + 1,
                total_attempts,
                elapsed.as_secs(),
                deadlines[attempt].as_secs()
            );

            match tokio::time::timeout(remaining, &mut result_rx).await {
                Ok(Ok(outcome)) => return Ok(outcome),
                Ok(Err(_recv_err)) => {
                    // oneshot sender 被 dropped（fixlet 断开等）
                    tracing::error!(
                        "session {}: pending turn channel closed unexpectedly",
                        task_id
                    );
                    return self
                        .fail_turn_and_respond(
                            task_id,
                            turn_id,
                            "channel_closed",
                            "fixlet connection lost",
                        )
                        .await;
                }
                Err(_elapsed) => {
                    attempt += 1;
                    if attempt >= total_attempts {
                        // 最后一次延长也已耗尽 → kill
                        tracing::warn!(
                            "session {}: turn {} timed out after {} attempts (total {}s)",
                            task_id,
                            turn_id,
                            total_attempts,
                            turn_start.elapsed().as_secs()
                        );
                        return self
                            .fail_turn_and_respond(
                                task_id,
                                turn_id,
                                "timeout",
                                &format!(
                                    "Turn timed out after {} attempts ({}s total)",
                                    total_attempts,
                                    turn_start.elapsed().as_secs()
                                ),
                            )
                            .await;
                    }
                    tracing::info!(
                        "session {}: turn {} extending timeout (attempt {}/{}, next deadline=+{}s)",
                        task_id,
                        turn_id,
                        attempt + 1,
                        total_attempts,
                        deadlines[attempt].as_secs()
                    );
                    // 继续循环，下一次用 deadlines[attempt]（已是累计新 deadline）
                }
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
            turn_max_extensions: self.turn_max_extensions,
            turn_extension_factor: self.turn_extension_factor,
            token_publisher: self.token_publisher.clone(),
            retry_policy: self.retry_policy,
            dispatcher: self.dispatcher.clone(),
            retry_attempts: self.retry_attempts.clone(),
            metrics: self.metrics.clone(),
            dispatch_times: self.dispatch_times.clone(),
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
        let turn_max_extensions = self.turn_max_extensions;
        let turn_extension_factor = self.turn_extension_factor;
        let token_publisher = self.token_publisher.clone();
        let retry_policy = self.retry_policy;
        let retry_attempts = self.retry_attempts.clone();
        let dispatcher = self.dispatcher.clone();
        let metrics = self.metrics.clone();
        let dispatch_times = self.dispatch_times.clone();

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
                turn_max_extensions,
                turn_extension_factor,
                token_publisher,
                retry_policy,
                dispatcher,
                retry_attempts,
                metrics,
                dispatch_times,
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

                let (pending, mut result_rx) = PendingTurn::new(
                    task_id.clone(),
                    redo_ctx.turn_id,
                    redo_ctx.redo_group.clone(),
                );
                registry.register_pending_turn(&task_id, pending).await;

                let cached = orch
                    .get_cached_llm_responses(&task_id, redo_ctx.turn_id)
                    .await;
                // 经调度器入队 + 派发(CR-1+2);produce 失败由 dispatch_pending 内部终态收口,
                // 这里只兜 resolve/get_task 失败。
                if let Err(e) = orch
                    .enqueue_and_dispatch(
                        &task_id,
                        redo_ctx.turn_id,
                        &redo_ctx.user_input,
                        &redo_ctx.redo_group,
                        redo_ctx.redo_count,
                        cached,
                    )
                    .await
                {
                    tracing::error!(
                        "session {}: redo enqueue/dispatch failed for turn {}: {}",
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

                // 延长式 wall-clock 超时(同 run_turn_to_completion,step 1)
                let deadlines = compute_extension_deadlines(
                    turn_timeout,
                    turn_max_extensions,
                    turn_extension_factor,
                );
                let redo_start = std::time::Instant::now();
                let total_attempts = deadlines.len();
                let mut attempt = 0;

                loop {
                    let elapsed = redo_start.elapsed();
                    let remaining = deadlines[attempt].saturating_sub(elapsed);
                    match tokio::time::timeout(remaining, &mut result_rx).await {
                        Ok(Ok(TurnOutcome::Completed { turn_id, .. })) => {
                            tracing::info!(
                                "session {}: redo turn {} succeeded",
                                task_id,
                                turn_id
                            );
                            redo_success += 1;
                            break;
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
                            break;
                        }
                        Ok(Ok(TurnOutcome::Timeout { .. })) | Err(_) => {
                            attempt += 1;
                            if attempt >= total_attempts {
                                tracing::error!(
                                    "session {}: redo turn {} timed out after {} attempts (total {}s)",
                                    task_id,
                                    redo_ctx.turn_id,
                                    total_attempts,
                                    redo_start.elapsed().as_secs()
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
                                break;
                            }
                            tracing::info!(
                                "session {}: redo turn {} extending timeout (attempt {}/{}, next deadline=+{}s)",
                                task_id,
                                redo_ctx.turn_id,
                                attempt + 1,
                                total_attempts,
                                deadlines[attempt].as_secs()
                            );
                            // 继续循环
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
                            break;
                        }
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

        // 打点(CR-8):token 用量实时计数(cross-task 观测,按 task_type/model)。
        let tt = self
            .resolve_task_type(task_id)
            .await
            .unwrap_or_else(|_| "unknown".to_string());
        self.metrics
            .record_llm_tokens(&tt, model, input_tokens, output_tokens, total_tokens);

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

        // turn 成功完成 → 清 per-turn 重试计数器(CR-3)
        self.retry_attempts
            .lock()
            .await
            .remove(&format!("{}:{}", task_id, turn_id));

        // 打点(CR-4):turn 执行时长 + 终态计数(success)
        self.record_turn_terminal_metric(task_id, turn_id, OUTCOME_SUCCESS)
            .await;

        // turn 终态 → 释放调度器容量槽 + 续派队里下一个(CR-1+2)
        self.release_slot_and_redispatch(task_id).await;

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
        let reason = classify_failure(error_type, error_message);
        tracing::error!(
            "session {}: turn_execution_error turn_id={} type={} reason={:?}: {}",
            task_id,
            turn_id,
            error_type,
            reason,
            error_message
        );

        // 1. 读取原始 turn_started 获取 redo 上下文(user_input / redo_group)
        let turn_events = self.store.get_turn_events(task_id, turn_id).await?;
        let turn_started = turn_events
            .iter()
            .find(|e| e.event_type == crate::models::EventType::TurnStarted);

        let (user_input, redo_group) = match turn_started {
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
                (input, rg)
            }
            None => {
                // 没有 turn_started,无法 redo → 终态收口
                self.fail_task_with_reason(task_id, turn_id, reason, error_type, error_message)
                    .await?;
                return Ok(());
            }
        };

        // 2. 决策(CR-3):用 in-memory per-turn 重试计数器作 current。
        //    Retry ⇒ 计数器 +1 并重派;Fail ⇒ 清计数器并终态收口。
        let key = format!("{}:{}", task_id, turn_id);
        let decision = {
            let mut attempts = self.retry_attempts.lock().await;
            let current = attempts.get(&key).copied().unwrap_or(0);
            let d = self.retry_policy.decide(reason, current);
            match &d {
                RetryDecision::Retry { .. } => {
                    attempts.insert(key.clone(), current + 1);
                }
                RetryDecision::Fail { .. } => {
                    attempts.remove(&key);
                }
            }
            d
        };

        match decision {
            RetryDecision::Retry {
                next_redo_count, ..
            } => {
                tracing::info!(
                    "session {}: retry turn {} attempt={} reason={:?} redo_group={}",
                    task_id,
                    turn_id,
                    next_redo_count,
                    reason,
                    redo_group
                );
                // 打点(CR-4):retry 预算消耗计数。
                let retry_tt = self
                    .resolve_task_type(task_id)
                    .await
                    .unwrap_or_else(|_| "unknown".to_string());
                self.metrics.record_retry(&retry_tt);
                let cached = self.get_cached_llm_responses(task_id, turn_id).await;
                if let Err(e) = self
                    .dispatch_with_retry(
                        task_id,
                        turn_id,
                        &user_input,
                        &redo_group,
                        next_redo_count,
                        &cached,
                    )
                    .await
                {
                    // dispatch 反复失败(broker 不可达等)→ 终态收口
                    tracing::error!(
                        "session {}: retry dispatch exhausted for turn {}: {}",
                        task_id,
                        turn_id,
                        e
                    );
                    self.fail_task_with_reason(
                        task_id,
                        turn_id,
                        FailureReason::RedoDispatchFailed,
                        "redo_dispatch_failed",
                        &e.to_string(),
                    )
                    .await?;
                }
            }
            RetryDecision::Fail {
                budget_exhausted, ..
            } => {
                tracing::warn!(
                    "session {}: turn {} terminal fail reason={:?} budget_exhausted={}",
                    task_id,
                    turn_id,
                    reason,
                    budget_exhausted
                );
                self.fail_task_with_reason(task_id, turn_id, reason, error_type, error_message)
                    .await?;
                self.release_slot_and_redispatch(task_id).await;
            }
        }

        Ok(())
    }

    // ── 失败终态收口(CR-3)─────────────────────────────────────────────

    /// 打点 turn 终态(CR-4):取首次派发时刻算 `dispatch→terminal` 时长 + resolve task_type +
    /// 记 `fixus_turn_terminal_total` / `fixus_turn_duration_seconds`。
    ///
    /// `dispatch_time` 缺失(跨重启 / 兜底路径无 turn_started)→ duration 记 0.0,**终态计数仍记**
    /// (计数比精度重要)。终态唯一(CR-3),`remove` 恰好一次。
    async fn record_turn_terminal_metric(&self, task_id: &str, turn_id: i64, outcome: &str) {
        let key = format!("{}:{}", task_id, turn_id);
        let dur = self
            .dispatch_times
            .lock()
            .await
            .remove(&key)
            .map(|t0| t0.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        let tt = self
            .resolve_task_type(task_id)
            .await
            .unwrap_or_else(|_| "unknown".to_string());
        self.metrics.record_turn_terminal(&tt, outcome, dur);
    }

    /// 终态失败:写 `turn_failed`(带 failure_reason)+ task `Executing→Failed`(带 failure_reason)
    /// + 通知 HTTP handler。修复此前 turn_failed 后 task 永远卡 Executing 的 bug。
    async fn fail_task_with_reason(
        &self,
        task_id: &str,
        turn_id: i64,
        reason: FailureReason,
        error_type: &str,
        error_message: &str,
    ) -> Result<TurnOutcome> {
        // 清 per-turn 重试计数器
        self.retry_attempts
            .lock()
            .await
            .remove(&format!("{}:{}", task_id, turn_id));

        // 打点(CR-4):turn 执行时长 + 终态计数(failed)。所有失败路径的唯一漏斗。
        self.record_turn_terminal_metric(task_id, turn_id, OUTCOME_FAILED)
            .await;

        // WAL: turn_failed(带 failure_reason)
        if let Err(e) = service::fail_turn(
            &*self.store,
            task_id,
            turn_id,
            error_type,
            error_message,
            None,
            Some(reason),
        )
        .await
        {
            tracing::error!(
                "session {}: failed to write turn_failed: {}",
                task_id,
                e
            );
        }

        // WAL: task Executing → Failed(带 failure_reason)。task_failed 终态,不再卡 Executing。
        if let Err(e) = service::fail_task(
            &*self.store,
            task_id,
            &format!("{}: {}", reason.as_str(), error_type),
            Some(reason),
        )
        .await
        {
            tracing::error!(
                "session {}: failed to transition task to Failed: {}",
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

    /// 同步阻塞路径的终态失败(timeout / channel_closed / recovery 的 dispatch 失败)。
    ///
    /// 这些场景**不重试**(重试会延长阻塞的 HTTP 调用):分类后直接 [`Self::fail_task_with_reason`]。
    /// 异步 agent 崩溃路径([`Self::handle_turn_execution_error`])才走 retry 预算。
    async fn fail_turn_and_respond(
        &self,
        task_id: &str,
        turn_id: i64,
        error_type: &str,
        error_message: &str,
    ) -> Result<TurnOutcome> {
        let reason = classify_failure(error_type, error_message);
        let outcome = self
            .fail_task_with_reason(task_id, turn_id, reason, error_type, error_message)
            .await?;
        // 终态失败也释放槽 + 续派(超时 / channel_closed / recovery 兜底)
        self.release_slot_and_redispatch(task_id).await;
        Ok(outcome)
    }

    /// `dispatch_execute_turn` 的有界 in-process 重试(CR-3):broker produce 失败时,
    /// 短退避重试最多 `max_attempts` 次。与 turn 级 redo 预算正交(dispatch 失败不产生
    /// 新 turn_started、不递增 redo 计数),两层共用 `max_attempts` 上限。
    async fn dispatch_with_retry(
        &self,
        task_id: &str,
        turn_id: i64,
        user_input: &str,
        redo_group: &str,
        redo_count: i32,
        cached: &[String],
    ) -> Result<()> {
        let max = self.retry_policy.max_attempts.max(1);
        let mut last_err: Option<String> = None;
        for attempt in 0..max {
            match self
                .dispatch_execute_turn(task_id, turn_id, user_input, redo_group, redo_count, cached)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let msg = e.to_string();
                    tracing::warn!(
                        "session {}: dispatch attempt {}/{} failed for turn {}: {}",
                        task_id,
                        attempt + 1,
                        max,
                        turn_id,
                        msg
                    );
                    last_err = Some(msg);
                    if attempt + 1 < max {
                        tokio::time::sleep(Duration::from_millis(200u64 << (attempt.min(4) as u32)))
                            .await;
                    }
                }
            }
        }
        Err(AppError::Protocol(format!(
            "dispatch failed after {} attempts: {}",
            max,
            last_err.unwrap_or_default()
        )))
    }

    // ── 派发调度器接线(CR-1+2)──────────────────────────────────────────

    /// 把一个 turn 入队并尽量派发(按容量/优先级)。
    /// 用于**新 turn**派发(execute_turn / recovery redo);**不**用于 Retry(Retry 槽位已持有,
    /// 走 dispatch_with_retry 直接重派)。
    async fn enqueue_and_dispatch(
        &self,
        task_id: &str,
        turn_id: i64,
        user_input: &str,
        redo_group: &str,
        redo_count: i32,
        cached_llm: Vec<String>,
    ) -> Result<()> {
        let task_type = self.resolve_task_type(task_id).await?;
        let priority = self
            .store
            .get_task(task_id)
            .await?
            .map(|t| t.priority)
            .unwrap_or(0);
        self.metrics.record_turn_enqueued(&task_type);
        self.dispatcher.enqueue(QueuedTurn {
            task_type: task_type.clone(),
            task_id: task_id.to_string(),
            turn_id,
            user_input: user_input.to_string(),
            redo_group: redo_group.to_string(),
            redo_count,
            cached_llm,
            priority,
            enqueued_at: std::time::Instant::now(),
        });
        self.dispatch_pending(&task_type).await;
        Ok(())
    }

    /// 把该 type 队列里能派的 turn 都派出去(直到容量满或队列空)。
    /// `try_pop` 已 `in_flight++`;派发失败则 `on_turn_terminal` 释放 + 终态收口,继续下一个。
    async fn dispatch_pending(&self, task_type: &str) {
        loop {
            let turn = match self.dispatcher.try_pop(task_type) {
                Some(t) => t,
                None => return,
            };
            let tid = turn.task_id.clone();
            let turn_id = turn.turn_id;
            let tt = turn.task_type.clone();
            // 打点(CR-4):队列等待时长 + 记首次派发时刻(终态算 duration)。
            self.metrics
                .record_turn_dispatched(&tt, turn.enqueued_at.elapsed().as_secs_f64());
            self.dispatch_times
                .lock()
                .await
                .insert(format!("{}:{}", tid, turn_id), std::time::Instant::now());
            if let Err(e) = self
                .dispatch_with_retry(&tid, turn_id, &turn.user_input, &turn.redo_group, turn.redo_count, &turn.cached_llm)
                .await
            {
                tracing::error!(
                    "session {}: dispatch failed for turn {}: {} — releasing slot + terminal fail",
                    tid,
                    turn_id,
                    e
                );
                self.dispatcher.on_turn_terminal(&tt);
                let _ = self
                    .fail_task_with_reason(&tid, turn_id, FailureReason::RedoDispatchFailed, "redo_dispatch_failed", &e.to_string())
                    .await;
                continue;
            }
            // 派发成功:turn 在途,等终态事件(done/error)释放。继续填下一个槽。
        }
    }

    /// turn 终态(完成/失败/超时)后:释放一个容量槽 + 续派队里下一个。
    async fn release_slot_and_redispatch(&self, task_id: &str) {
        let task_type = match self.resolve_task_type(task_id).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    "release_slot: resolve_task_type failed for {}: {}",
                    task_id,
                    e
                );
                return;
            }
        };
        self.dispatcher.on_turn_terminal(&task_type);
        self.dispatch_pending(&task_type).await;
    }

    // ── turn 看门狗(CR-6)──────────────────────────────────────────────

    /// 扫描 `dispatch_times`,回收派发超 `lease` 未终态的 turn:触发
    /// [`Self::handle_turn_execution_error`](`agent_unresponsive`,CR-3 治理 retry/fail +
    /// release_slot),并刷新该 turn 的 dispatch_times(重计 lease 窗,避免每轮重复触发;
    /// CR-3 `max_attempts` 预算封顶收敛)。返回回收数。看门狗循环调此。
    pub async fn reclaim_stale_turns(&self, lease: Duration) -> usize {
        let now = std::time::Instant::now();
        let stale: Vec<String> = {
            let dt = self.dispatch_times.lock().await;
            dt.iter()
                .filter(|(_, t)| now.duration_since(**t) > lease)
                .map(|(k, _)| k.clone())
                .collect()
        };
        for key in &stale {
            let (task_id, turn_id) = match parse_turn_key(key) {
                Some(v) => v,
                None => continue,
            };
            let tt = self
                .resolve_task_type(&task_id)
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            tracing::warn!(
                "turn watchdog: reclaim {} turn {} (no terminal in {:?})",
                task_id,
                turn_id,
                lease
            );
            self.metrics.record_watchdog_reclaim(&tt);
            // 刷新 lease 窗;CR-3 预算封顶 ⇒ 最多 max_attempts+1 轮后 Failed
            self.dispatch_times
                .lock()
                .await
                .insert(key.clone(), std::time::Instant::now());
            let _ = self
                .handle_turn_execution_error(
                    &task_id,
                    turn_id,
                    "agent_unresponsive",
                    "no terminal event within turn_lease",
                )
                .await;
        }
        stale.len()
    }

    /// 启动后台看门狗:周期 `interval` 扫描,派发超 `lease` 未终态的 turn 回收。
    pub fn spawn_turn_watchdog(self: &Arc<Self>, lease: Duration, interval: Duration) {
        let orch = self.clone();
        tokio::spawn(async move {
            tracing::info!(
                "turn watchdog starting: lease={:?} interval={:?}",
                lease,
                interval
            );
            loop {
                tokio::time::sleep(interval).await;
                let reclaimed = orch.reclaim_stale_turns(lease).await;
                if reclaimed > 0 {
                    tracing::info!("turn watchdog reclaimed {} turn(s)", reclaimed);
                }
            }
        });
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

    // ── CR-3 §4.2 集成测试(失败分类 + retry 预算)──────────────────────

    /// 建一个 task 并推进到 Executing(create→ready→claim→start_turn)。
    /// 每步后 `wait_seq` 等 logdbd 异步写入可见。
    async fn cr3_setup_task_at_executing(
        store: &dyn EventStore,
    ) -> (String, i64, String) {
        let (tid, _) = service::create_task(store, "default", &test_provenance(), None, 0)
            .await
            .unwrap();
        wait_seq(store, &tid, 1).await;
        service::mark_task_ready(store, &tid).await.unwrap();
        wait_seq(store, &tid, 2).await;
        service::claim_task(store, &tid, "fixlet-test").await.unwrap();
        wait_seq(store, &tid, 3).await;
        let (turn_id, redo_group, _) =
            service::start_turn(store, &tid, "do work", None).await.unwrap();
        wait_seq(store, &tid, 4).await;
        (tid, turn_id, redo_group)
    }

    /// §4.2:agent 崩溃(`agent_process_exited`)超预算(max_attempts=2)⇒ task Failed,
    /// 且 `turn_failed` 带 `failure_reason`。此前行为:无限 redo + task 永远卡 Executing。
    #[tokio::test]
    async fn cr3_agent_crash_budget_exhausted_fails_task() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry.clone(), tp)
            .with_retry_policy(RetryPolicy { max_attempts: 2 });

        let (tid, turn_id, rg) = cr3_setup_task_at_executing(&*store).await;
        let (pending, _rx) = PendingTurn::new(tid.clone(), turn_id, rg);
        registry.register_pending_turn(&tid, pending).await;

        // 前 2 次 → Retry(dispatch 到 broker,LogdbdEventStore 的 publish_turn_begin 是 no-op Ok);
        // 第 3 次 → 预算耗尽 → Fail
        for i in 0..3 {
            orch.handle_turn_execution_error(&tid, turn_id, "agent_process_exited", "boom")
                .await
                .unwrap();
            if i < 2 {
                assert_ne!(
                    store.get_task_state(&tid).await.unwrap(),
                    Some(TaskState::Failed),
                    "attempt {}: 预算内不该 Failed",
                    i
                );
            }
        }
        // 第 3 次 Fail 写 turn_failed(seq5)+ task_failed(seq6);等可见再断言
        wait_seq(&*store, &tid, 6).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Failed)
        );

        // turn_failed 恰好 1 个(只在终态 Fail 时写),且带 failure_reason
        let events = store.get_turn_events(&tid, turn_id).await.unwrap();
        let tfs: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == EventType::TurnFailed)
            .collect();
        assert_eq!(tfs.len(), 1, "应只有 1 个 turn_failed(预算内 Retry 不写)");
        let fr = tfs[0]
            .payload
            .get("failure_reason")
            .and_then(|v| v.as_str());
        assert_eq!(fr, Some("agent_process_exited"));
    }

    /// §4.2:终态原因(`application_error`)立即 Fail —— 不消耗预算、不重试。
    #[tokio::test]
    async fn cr3_terminal_reason_fails_immediately() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry.clone(), tp)
            .with_retry_policy(RetryPolicy { max_attempts: 5 });

        let (tid, turn_id, rg) = cr3_setup_task_at_executing(&*store).await;
        let (pending, _rx) = PendingTurn::new(tid.clone(), turn_id, rg);
        registry.register_pending_turn(&tid, pending).await;

        // application_error 一次即 Fail(不可重试),即便预算=5
        orch.handle_turn_execution_error(&tid, turn_id, "application_error", "bad output")
            .await
            .unwrap();
        wait_seq(&*store, &tid, 6).await;

        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Failed)
        );
        let events = store.get_turn_events(&tid, turn_id).await.unwrap();
        let fr = events
            .iter()
            .find(|e| e.event_type == EventType::TurnFailed)
            .and_then(|e| e.payload.get("failure_reason").and_then(|v| v.as_str()));
        assert_eq!(fr, Some("application_error"));
    }

    /// §4.2:预算内 Retry 后 turn 成功完成 → 计数器清零,task 未 Failed。
    #[tokio::test]
    async fn cr3_retry_within_budget_then_success() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry.clone(), tp)
            .with_retry_policy(RetryPolicy { max_attempts: 2 });

        let (tid, turn_id, rg) = cr3_setup_task_at_executing(&*store).await;
        let (pending, _rx) = PendingTurn::new(tid.clone(), turn_id, rg);
        registry.register_pending_turn(&tid, pending).await;

        // 1 次 agent 崩溃 → Retry(计数=1),仍未 Failed
        orch.handle_turn_execution_error(&tid, turn_id, "agent_process_exited", "hiccup")
            .await
            .unwrap();
        assert_ne!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Failed)
        );

        // turn 成功完成 → 清计数器,写 turn_completed(seq5),task 未 Failed
        orch.handle_turn_execution_done(&tid, turn_id, 0, "ok")
            .await
            .unwrap();
        wait_seq(&*store, &tid, 5).await;
        assert_ne!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Failed)
        );
        let events = store.get_turn_events(&tid, turn_id).await.unwrap();
        assert!(
            events.iter().any(|e| e.event_type == EventType::TurnCompleted),
            "应有 turn_completed"
        );
    }

    // ── CR-1+2 §4.2 集成测试(派发调度器接线)──────────────────────────────
    //
    // 注:`execute_turn` 会阻塞在 result_rx(turn_timeout),且 LogdbdEventStore 的
    // publish_turn_begin 是 no-op,无法从 broker 侧观测派发序。故这里直接调
    // enqueue_and_dispatch / release_slot_and_redispatch(不阻塞),用 dispatcher 的
    // in_flight / pending_count 验证 orchestrator↔dispatcher 接线。
    // priority 顺序由 dispatcher 单测(§4.1)覆盖。

    /// §4.2:并发闸端到端 —— max=1 时第 2 个 turn 排队,首个终态后第 2 个被续派。
    #[tokio::test]
    async fn cr12_concurrency_cap_and_redispatch() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry, tp).with_max_concurrent(1);

        let prov = test_provenance();
        let (ta, _) = service::create_task(&*store, "default", &prov, None, 0).await.unwrap();
        let (tb, _) = service::create_task(&*store, "default", &prov, None, 0).await.unwrap();
        wait_seq(&*store, &ta, 1).await;
        wait_seq(&*store, &tb, 1).await;

        // A 入队 + 派发(max=1 → in_flight=1)
        orch.enqueue_and_dispatch(&ta, 1, "a", "rg_a", 0, vec![])
            .await
            .unwrap();
        assert_eq!(orch.dispatcher.in_flight("default"), 1);
        assert_eq!(orch.dispatcher.pending_count("default"), 0);

        // B 入队(max=1 满 → B 排队;in_flight 仍 1,pending=1)
        orch.enqueue_and_dispatch(&tb, 1, "b", "rg_b", 0, vec![])
            .await
            .unwrap();
        assert_eq!(orch.dispatcher.in_flight("default"), 1);
        assert_eq!(orch.dispatcher.pending_count("default"), 1);

        // A 终态 → 释放槽 → B 续派(in_flight 回到 1,pending=0)
        orch.release_slot_and_redispatch(&ta).await;
        assert_eq!(
            orch.dispatcher.in_flight("default"),
            1,
            "B 应被续派,占住释放的槽"
        );
        assert_eq!(orch.dispatcher.pending_count("default"), 0);
    }

    /// §4.2:priority 透传 —— 高优先 task 的 turn 入队时 priority 进队列(CR-1 数据通路)。
    #[tokio::test]
    async fn cr12_priority_threaded_through_task() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry, tp).with_max_concurrent(2);

        let prov = test_provenance();
        let (th, _) = service::create_task(&*store, "default", &prov, None, 9).await.unwrap();
        wait_seq(&*store, &th, 1).await;

        // 高优先 task 的 turn 入队;priority 从 task head 读出进 QueuedTurn(dispatch_pending 派发后 in_flight=1)
        orch.enqueue_and_dispatch(&th, 1, "hi", "rg", 0, vec![])
            .await
            .unwrap();
        assert_eq!(orch.dispatcher.in_flight("default"), 1);
        // 派出去的 turn 是 priority=9(由 task priority 透传;dispatch_pending 已取走,
        // 这里仅断言计数,顺序正确性见 dispatcher §4.1 pop_order_is_priority_desc)
    }

    // ── CR-4 §4.3 集成测试(业务指标打点)──────────────────────────────────

    /// §4.3:turn 完整生命周期(入队→派发→成功终态)→ render() 含全部 turn 级指标。
    #[tokio::test]
    async fn cr4_metrics_record_turn_lifecycle() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry.clone(), tp);

        let (tid, turn_id, rg) = cr3_setup_task_at_executing(&*store).await;
        let (pending, _rx) = PendingTurn::new(tid.clone(), turn_id, rg);
        registry.register_pending_turn(&tid, pending).await;

        // 入队 + 派发(LogdbdEventStore publish_turn_begin = no-op Ok)
        orch.enqueue_and_dispatch(&tid, turn_id, "do work", "rg", 0, vec![])
            .await
            .unwrap();
        // 成功终态
        orch.handle_turn_execution_done(&tid, turn_id, 0, "ok")
            .await
            .unwrap();
        wait_seq(&*store, &tid, 5).await;

        let out = orch.metrics_handle().render();
        assert!(
            out.contains("fixus_turn_enqueued_total{task_type=\"default\"} 1"),
            "enqueued:\n{}", out
        );
        assert!(
            out.contains("fixus_turn_dispatched_total{task_type=\"default\"} 1"),
            "dispatched:\n{}", out
        );
        assert!(
            out.contains("fixus_turn_queue_wait_seconds_count{task_type=\"default\"} 1"),
            "queue_wait:\n{}", out
        );
        assert!(
            out.contains("fixus_turn_terminal_total{outcome=\"success\",task_type=\"default\"} 1"),
            "terminal success:\n{}", out
        );
        assert!(
            out.contains("fixus_turn_duration_seconds_count{outcome=\"success\",task_type=\"default\"} 1"),
            "duration:\n{}", out
        );
    }

    /// §4.3:application_error(终态原因)→ outcome="failed" 计数。
    #[tokio::test]
    async fn cr4_metrics_terminal_failed_on_fail_path() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry.clone(), tp)
            .with_retry_policy(RetryPolicy { max_attempts: 5 });

        let (tid, turn_id, rg) = cr3_setup_task_at_executing(&*store).await;
        let (pending, _rx) = PendingTurn::new(tid.clone(), turn_id, rg);
        registry.register_pending_turn(&tid, pending).await;

        orch.handle_turn_execution_error(&tid, turn_id, "application_error", "bad output")
            .await
            .unwrap();
        wait_seq(&*store, &tid, 6).await;

        let out = orch.metrics_handle().render();
        assert!(
            out.contains("fixus_turn_terminal_total{outcome=\"failed\",task_type=\"default\"} 1"),
            "terminal failed:\n{}", out
        );
    }

    /// §4.3:retryable 原因 + 预算内 → Retry → retry_attempts_total 计数(CR-3 可观测)。
    #[tokio::test]
    async fn cr4_metrics_retry_counter() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry.clone(), tp)
            .with_retry_policy(RetryPolicy { max_attempts: 2 });

        let (tid, turn_id, rg) = cr3_setup_task_at_executing(&*store).await;
        let (pending, _rx) = PendingTurn::new(tid.clone(), turn_id, rg);
        registry.register_pending_turn(&tid, pending).await;

        // agent_process_exited 可重试 + 预算=2 → 第 1 次 Retry
        orch.handle_turn_execution_error(&tid, turn_id, "agent_process_exited", "boom")
            .await
            .unwrap();
        assert_ne!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Failed),
            "预算内不该 Failed"
        );

        let out = orch.metrics_handle().render();
        assert!(
            out.contains("fixus_retry_attempts_total{task_type=\"default\"} 1"),
            "retry counter:\n{}", out
        );
    }

    // ── CR-6 §4.2 集成测试(turn 看门狗)──────────────────────────────────

    #[test]
    fn parse_turn_key_splits_task_and_turn() {
        assert_eq!(parse_turn_key("task_abc:3"), Some(("task_abc".into(), 3)));
        assert_eq!(parse_turn_key("task_abc"), None); // 无冒号
        assert_eq!(parse_turn_key("task_abc:x"), None); // turn 非数字
    }

    /// §4.2:派发后无终态超 lease → 看门狗回收 → CR-3 治理(max_attempts=0 → 直接 Fail)。
    #[tokio::test]
    async fn cr6_watchdog_reclaims_stale_turn() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry.clone(), tp)
            .with_retry_policy(RetryPolicy { max_attempts: 0 }); // 不重试 → 直接 Fail

        let (tid, turn_id, rg) = cr3_setup_task_at_executing(&*store).await;
        let (pending, _rx) = PendingTurn::new(tid.clone(), turn_id, rg);
        registry.register_pending_turn(&tid, pending).await;

        orch.enqueue_and_dispatch(&tid, turn_id, "do work", "rg", 0, vec![])
            .await
            .unwrap();
        // backdate 到 120s 前(模拟派发后久无终态)
        orch.dispatch_times.lock().await.insert(
            format!("{}:{}", tid, turn_id),
            std::time::Instant::now() - std::time::Duration::from_secs(120),
        );

        let n = orch.reclaim_stale_turns(std::time::Duration::from_secs(60)).await;
        assert!(n >= 1, "应回收 stale turn");
        wait_seq(&*store, &tid, 6).await; // turn_failed(5) + task_failed(6)
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Failed),
            "max_attempts=0 + agent_unresponsive → 直接 Failed"
        );
        // watchdog 计数 +1
        let out = orch.metrics_handle().render();
        assert!(
            out.contains("fixus_turn_watchdog_reclaims_total{task_type=\"default\"}"),
            "watchdog reclaim 计数应有:{}", out
        );
    }

    /// §4.2:fresh turn(< lease)不被回收。
    #[tokio::test]
    async fn cr6_watchdog_skips_fresh_turn() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry.clone(), tp);

        let (tid, turn_id, rg) = cr3_setup_task_at_executing(&*store).await;
        let (pending, _rx) = PendingTurn::new(tid.clone(), turn_id, rg);
        registry.register_pending_turn(&tid, pending).await;
        orch.enqueue_and_dispatch(&tid, turn_id, "do work", "rg", 0, vec![])
            .await
            .unwrap();

        // lease=3600s,dispatch_times 是 fresh → 不回收
        let n = orch.reclaim_stale_turns(std::time::Duration::from_secs(3600)).await;
        assert_eq!(n, 0, "fresh turn 不该回收");
        assert_ne!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Failed),
            "fresh turn 状态不变"
        );
    }

    /// §4.2:看门狗 + CR-3 预算收敛(max_attempts=1 → 2 轮后 Failed,不无限循环)。
    #[tokio::test]
    async fn cr6_watchdog_converges_via_retry_budget() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry.clone(), tp)
            .with_retry_policy(RetryPolicy { max_attempts: 1 });

        let (tid, turn_id, rg) = cr3_setup_task_at_executing(&*store).await;
        let (pending, _rx) = PendingTurn::new(tid.clone(), turn_id, rg);
        registry.register_pending_turn(&tid, pending).await;
        orch.enqueue_and_dispatch(&tid, turn_id, "do work", "rg", 0, vec![])
            .await
            .unwrap();

        let lease = std::time::Duration::from_secs(60);
        let backdate = || std::time::Instant::now() - std::time::Duration::from_secs(120);

        // 第 1 轮:retry(attempts 0→1),未 Failed
        orch.dispatch_times
            .lock()
            .await
            .insert(format!("{}:{}", tid, turn_id), backdate());
        let _ = orch.reclaim_stale_turns(lease).await;
        assert_ne!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Failed),
            "预算内第 1 轮不该 Failed"
        );

        // 第 2 轮:attempts=1, 1<1 false → Fail(收敛,无无限循环)
        orch.dispatch_times
            .lock()
            .await
            .insert(format!("{}:{}", tid, turn_id), backdate());
        let _ = orch.reclaim_stale_turns(lease).await;
        wait_seq(&*store, &tid, 6).await;
        assert_eq!(
            store.get_task_state(&tid).await.unwrap(),
            Some(TaskState::Failed),
            "第 2 轮预算耗尽应 Failed"
        );
    }

    // ── 性能测试(#[ignore],cargo test --lib -- --ignored perf_ --nocapture)──
    // 看门狗每 interval 扫一次 dispatch_times(纯 HashMap 遍历 + lock)。测最常见
    // "无 stale" 扫描成本。不断言阈值,数字供人读。

    fn report_perf(name: &str, unit: &str, mut samples: Vec<u64>) {
        samples.sort_unstable();
        let n = samples.len();
        if n == 0 {
            println!("[perf] {}: no samples", name);
            return;
        }
        let p = |q: usize| samples[(q * n / 100).min(n.saturating_sub(1))];
        let sum: u64 = samples.iter().sum();
        println!(
            "[perf] {:<34} n={:>6}  p50={:>7}{}  p95={:>7}{}  p99={:>7}{}  avg={:>7}{}",
            name, n, p(50), unit, p(95), unit, p(99), unit, sum / n as u64, unit
        );
    }

    // ── CR-8 §4.2 集成测试(token metrics)────────────────────────────────

    /// §4.2:handle_llm_completed → token 计数进 metrics(render 含)。
    #[tokio::test]
    async fn cr8_llm_completed_records_token_metrics() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry, tp);

        let (tid, turn_id, _rg) = cr3_setup_task_at_executing(&*store).await;
        orch.handle_llm_completed(&tid, turn_id, "claude-sonnet-5", 100, 20, 120)
            .await
            .unwrap();
        wait_seq(&*store, &tid, 5).await; // setup 到 4,llm_completed = 5

        let out = orch.metrics_handle().render();
        assert!(
            out.contains("fixus_token_input_tokens_total{model=\"claude-sonnet-5\",task_type=\"default\"} 100"),
            "input tokens:\n{}", out
        );
        assert!(
            out.contains("fixus_token_output_tokens_total{model=\"claude-sonnet-5\",task_type=\"default\"} 20"),
            "output tokens:\n{}", out
        );
        assert!(
            out.contains("fixus_token_total_tokens_total{model=\"claude-sonnet-5\",task_type=\"default\"} 120"),
            "total tokens:\n{}", out
        );
    }

    #[tokio::test]
    #[ignore]
    async fn perf_watchdog_scan_at_scale() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry, tp);
        // 填 5000 条 dispatch_times(全 fresh)
        {
            let mut dt = orch.dispatch_times.lock().await;
            let now = std::time::Instant::now();
            for i in 0..5000u32 {
                dt.insert(format!("task_{i}:1"), now);
            }
        }
        let lease = std::time::Duration::from_secs(3600); // 全 fresh → 只扫不回收
        for _ in 0..5 {
            let _ = orch.reclaim_stale_turns(lease).await; // warm-up
        }
        let n = 50;
        let mut us = Vec::with_capacity(n);
        for _ in 0..n {
            let t0 = std::time::Instant::now();
            let r = orch.reclaim_stale_turns(lease).await;
            us.push(t0.elapsed().as_micros() as u64);
            assert_eq!(r, 0, "fresh 不该回收");
        }
        report_perf("watchdog scan (5000 in-flight)", "µs", us);
    }

    // ── compute_extension_deadlines (step 1,turn_timeout 延长机制) ──

    #[test]
    fn deadlines_zero_extensions_is_initial_only() {
        assert_eq!(
            compute_extension_deadlines(Duration::from_secs(300), 0, 1.5),
            vec![Duration::from_secs(300)],
            "max_extensions=0 退化为原一刀切"
        );
    }

    #[test]
    fn deadlines_three_extensions_compound_cumulative() {
        // 300 → 450 → 675 → 1012.5(300 * 1.5³ = 1012.5,保留小数)
        assert_eq!(
            compute_extension_deadlines(Duration::from_secs(300), 3, 1.5),
            vec![
                Duration::from_secs(300),
                Duration::from_secs(450),
                Duration::from_secs(675),
                Duration::from_secs_f64(1012.5),
            ],
            "300s 起步 + 3 次延长累计 deadline 表"
        );
    }

    #[test]
    fn deadlines_default_600s_three_extensions_total_33min() {
        // 用户拍板的默认值:600s + 3 次 1.5x → cumulative = [600, 900, 1350, 2025]
        // 最后 deadline 2025s ≈ 33.75min(从这里开始 fail)。
        let deadlines = compute_extension_deadlines(Duration::from_secs(600), 3, 1.5);
        assert_eq!(
            deadlines,
            vec![
                Duration::from_secs(600),
                Duration::from_secs(900),
                Duration::from_secs(1350),
                Duration::from_secs(2025),
            ]
        );
        assert_eq!(deadlines.last().unwrap().as_secs(), 2025);
    }

    #[test]
    fn deadlines_one_extension_compounds_by_factor() {
        assert_eq!(
            compute_extension_deadlines(Duration::from_secs(100), 1, 1.5),
            vec![Duration::from_secs(100), Duration::from_secs(150)],
            "单次延长 = 100 * 1.5 = 150"
        );
    }

    #[test]
    fn deadlines_len_equals_max_extensions_plus_one() {
        for max in 0..=5 {
            let d = compute_extension_deadlines(Duration::from_secs(60), max, 1.5);
            assert_eq!(d.len(), (max + 1) as usize, "max={}", max);
        }
    }

}
