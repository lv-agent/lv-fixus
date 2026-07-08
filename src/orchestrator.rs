//! Turn Orchestrator — 中心编排引擎
//!
//! 职责：
//! - execute_turn: 用户请求 → WAL → context构建 → 下发fixlet → 等待结果
//! - handle_tool_invoked: fixlet tool_call → WAL → sandbox → tool_result
//! - handle_turn_execution_done: fixlet done → WAL → 通知HTTP handler
//!
//! 这是把 fixus、fixlet、sandbox 串成完整闭环的中心组件。

use std::sync::Arc;
use std::time::Duration;

use crate::error::{AppError, Result};
use crate::task_registry::{PendingTurn, TaskRegistry, TurnOutcome};
use crate::storage::EventStore;
use crate::{context, recovery, sandbox, service};

// ── 辅助类型 ────────────────────────────────────────────────────────────

/// MCP tool 执行结果
#[derive(Debug, Clone, serde::Serialize)]
pub struct ToolExecuteResult {
    pub success: bool,
    pub output: serde_json::Value,
    pub duration_ms: u64,
}

/// 为 MCP tool 构建 idempotency_key
///
/// 格式: `{task_id}:mcp:{tool_name}:{canonical_hash}`
/// MCP tool 的 idempotency_key 与 Turn 无关（不包含 redo_group），
/// 因为 MCP tool 在 Turn 级别恢复时不参与重做。
fn build_tool_idempotency_key(
    task_id: &str,
    tool_name: &str,
    args: &serde_json::Value,
) -> String {
    use sha2::{Digest, Sha256};
    let canonical = canonical_json(args);
    let hash = hex::encode(Sha256::digest(canonical.as_bytes()).as_slice());
    format!("{}:mcp:{}:{}", task_id, tool_name, &hash[..16])
}

/// 规范化为确定性的 JSON 字符串
fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let items: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        canonical_json(&map[*k])
                    )
                })
                .collect();
            format!("{{{}}}", items.join(","))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(canonical_json).collect();
            format!("[{}]", items.join(","))
        }
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".into()),
    }
}

/// 首字母大写
fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

// ── Orchestrator ────────────────────────────────────────────────────────

/// Turn 编排器
pub struct Orchestrator {
    store: Arc<dyn EventStore>,
    registry: Arc<TaskRegistry>,
    /// Turn 级超时（默认 5 分钟）
    turn_timeout: Duration,
    /// Token 逐字流式发布(Redis ephemeral 快路径,仅 llm_chunk)
    token_publisher: crate::stream::TokenPublisher,
}

/// 异步 Turn 启动结果(`start_turn_async`)
#[derive(Debug)]
pub enum AsyncTurnStart {
    /// Turn 已启动,后台执行中;客户端凭 turn_id 连 fixus-stream SSE 看进度
    Started { turn_id: i64 },
    /// 检测到未完成 Turn,已触发后台恢复;本次无新 Turn
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
        }
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
            "task_id": task_id,
            "turn_id": turn_id,
            "input": {"user_input": &user_input_with_cache},
            "context": {
                "summary": ctx.summary,
                "messages": ctx.messages,
            },
            "tools": [],  // TODO: 从 session 配置或工具注册表获取
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

    /// 处理 fixlet 上报的 tool_invoked
    ///
    /// 1. WAL: 写 tool_invoked
    /// 2. 调度 Sandbox 执行
    /// 3. WAL: 写 tool_completed 或 tool_failed
    /// 4. 回传 tool_result 给 fixlet
    pub async fn handle_tool_invoked(
        &self,
        task_id: &str,
        turn_id: i64,
        step_id: &str,
        local_seq: i64,
        tool_name: &str,
        tool_call_id: &str,
        idempotency_key: &str,
        input: &serde_json::Value,
    ) -> Result<()> {
        tracing::info!(
            "session {}: tool_invoked {} step_id={} idempotency_key={}",
            task_id,
            tool_name,
            step_id,
            idempotency_key
        );

        // 1. WAL: 写 tool_invoked
        let _event = service::record_tool_invoked(
            &*self.store,
            task_id,
            Some(turn_id),
            step_id,
            tool_name,
            tool_call_id,
            idempotency_key,
            input,
            None, // parent_step_id
            local_seq,
        )
        .await?;

        // 2. 统一调度 Sandbox 执行（所有工具走同一 ExecuteRequest 路径）
        let native_tool = tool_name.strip_prefix("fixus_").unwrap_or(tool_name);
        let native_tool_cased = capitalize_first(native_tool);

        let exec_result = crate::sandbox::execute_tool(crate::sandbox::ExecuteRequest {
            tool_name: native_tool_cased,
            tool_call_id: tool_call_id.to_string(),
            idempotency_key: idempotency_key.to_string(),
            input: input.clone(),
            timeout_ms: 30_000,
        })
        .await;

        // 3. WAL: 写 tool_completed 或 tool_failed
        if exec_result.success {
            service::record_tool_completed(
                &*self.store,
                task_id,
                Some(turn_id),
                step_id,
                tool_call_id,
                &exec_result.output,
                false,
                local_seq,
            )
            .await?;
        } else {
            service::record_tool_failed(
                &*self.store,
                task_id,
                Some(turn_id),
                step_id,
                tool_call_id,
                "sandbox_execution_error",
                &exec_result.error.unwrap_or_default(),
                true,
                1,
                local_seq,
            )
            .await?;
        }

        // 4. 回传 tool_result 给 fixlet
        self.send_tool_result_to_fixlet(
            task_id,
            step_id,
            tool_call_id,
            &exec_result.output,
            exec_result.success,
            exec_result.duration_ms,
        )
        .await;

        Ok(())
    }

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
    async fn send_tool_result_to_fixlet(
        &self,
        task_id: &str,
        step_id: &str,
        tool_call_id: &str,
        output: &serde_json::Value,
        success: bool,
        duration_ms: u64,
    ) {
        let result_msg = serde_json::json!({
            "type": "tool_result",
            "step_id": step_id,
            "tool_call_id": tool_call_id,
            "output": output,
            "success": success,
            "duration_ms": duration_ms,
        });

        // tool_result 也按 task_type 路由回对应 fixlet
        let task_type = match self.resolve_task_type(task_id).await {
            Ok(at) => at,
            Err(e) => {
                tracing::error!(
                    "session {}: cannot resolve task_type for tool_result: {}",
                    task_id,
                    e
                );
                return;
            }
        };
        if let Err(e) = self
            .registry
            .send_to_fixlet_for_task_type(&task_type, &result_msg.to_string())
            .await
        {
            tracing::error!(
                "session {}: failed to send tool_result to fixlet (task_type {}): {}",
                task_id,
                task_type,
                e
            );
        }
    }

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
    pub async fn execute_tool(
        &self,
        task_id: &str,
        tool_name: &str, // "fixus_bash"
        tool_call_id: &str,
        args: &serde_json::Value,
    ) -> Result<ToolExecuteResult> {
        use uuid::Uuid;

        let step_id = Uuid::now_v7().to_string();
        let active_turn = self.store.get_max_turn_id(task_id).await?;

        // 将 fixus_* 映射为原生工具名
        let native_tool: String = tool_name
            .strip_prefix("fixus_")
            .map(|s| capitalize_first(s))
            .unwrap_or_else(|| tool_name.to_string());

        // 生成 idempotency_key
        let idempotency_key = build_tool_idempotency_key(task_id, tool_name, args);

        tracing::info!(
            "session {}: executing tool {} (native={}) idempotency_key={}",
            task_id,
            tool_name,
            native_tool,
            idempotency_key
        );

        // 1. WAL: tool_invoked
        let turn_id = if active_turn > 0 {
            Some(active_turn)
        } else {
            None
        };
        service::record_tool_invoked(
            &*self.store,
            task_id,
            turn_id,
            &step_id,
            tool_name,
            tool_call_id,
            &idempotency_key,
            args,
            None,
            0,
        )
        .await?;

        // 2. Sandbox 执行
        let exec_result = crate::sandbox::execute_tool(crate::sandbox::ExecuteRequest {
            tool_name: native_tool.to_string(),
            tool_call_id: tool_call_id.to_string(),
            idempotency_key: idempotency_key.clone(),
            input: args.clone(),
            timeout_ms: 30_000,
        })
        .await;

        // 3. WAL: tool_terminal
        if exec_result.success {
            service::record_tool_completed(
                &*self.store,
                task_id,
                turn_id,
                &step_id,
                tool_call_id,
                &exec_result.output,
                false,
                0,
            )
            .await?;
        } else {
            let error_msg = exec_result.error.clone().unwrap_or_default();
            service::record_tool_failed(
                &*self.store,
                task_id,
                turn_id,
                &step_id,
                tool_call_id,
                "sandbox_execution_error",
                &error_msg,
                true,
                1,
                0,
            )
            .await?;
        }

        Ok(ToolExecuteResult {
            success: exec_result.success,
            output: exec_result.output,
            duration_ms: exec_result.duration_ms,
        })
    }

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
}

// ── 测试 ────────────────────────────────────────────────────────────────
