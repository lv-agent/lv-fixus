# fixlet / tools-bank Step-Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every tool call and every LLM call inside a turn appear as a properly-paired step-event pair (`{invoked, terminal}` sharing one `step_id`) in the event store, visible through the existing fixus-stream SSE channel — by adding only the **producer** side (tools-bank + fixlet) and the **missing dispatch arms** on the fixus consumer.

**Architecture:** Producers (tools-bank for tool events, fixlet for LLM events) mint a `step_id` and publish two records to the broker `task-end` stream via the existing `BrokerProducer::produce_full` API. fixus's `run_lifecycle_consumer` gains 5 new `match` arms (plus a fix to the `llm_completed` arm) that parse the JSON payload and call the already-built `service::record_*` typed helpers → `store.write_event` → logdbd stream `{task_id}` → fixus-stream SSE. `step_id` pairs `invoked ↔ terminal` inside `projection`. No new EventType, no schema change, no new transport, no nuntius change.

**Tech Stack:** Rust, Tokio, Axum, logdbd broker (`logdb_client::broker::{BrokerProducer, produce_full}`), serde_json, uuid (v7).

**Spec:** `docs/superpowers/specs/2026-07-16-fixlet-step-events-design.md`

---

## File Structure (responsibilities locked here)

| File | Role in this plan |
|------|-------------------|
| `src/orchestrator.rs` | **Consumer.** Add 5 `handle_*` step handlers + fix `handle_llm_completed`; add 5 dispatch arms + fix the `llm_completed` arm in `run_lifecycle_consumer`. |
| `src/service.rs` | **No change.** `record_tool_invoked/completed/failed`, `record_llm_invoked/completed/failed` already exist with the signatures this plan calls. |
| `src/models.rs` | **No change.** 6 EventType variants + payload structs + `validate_payload_required_fields` + `validate_scope` (turn_id NULL ok for Step) already in place. |
| `src/bin/tools-bank/main.rs` | **Producer (tool).** Lift `BrokerProducer` to shared `Arc<Mutex<>>`; add `task_end_producer` + `seq: AtomicU64` to `AppState`; read `X-Fixus-Turn-Id` in `handle_mcp`; mint `step_id` + emit the tool pair in `tools_call`. |
| `src/bin/tools-bank/adapter.rs` | **Producer (tool).** Extend `CallCtx` with `step_id` + `turn_id`; `SandboxAdapter::invoke` reuses `ctx.step_id` instead of minting its own. |
| `src/bin/fixlet/router.rs` | **Producer (LLM).** Mint per-turn `step_id` + emit `llm_invoked` before `session/prompt`; emit `llm_completed` (carrying `step_id`, ungated) at `FinalMessage`; emit `llm_failed` at ACP `Error`; exercise `LocalSeqCounter::next()`. |
| `src/bin/fixlet/idempotency.rs` | **Producer (LLM).** Add `step_id: Option<String>` to `TurnContext`. |
| `src/bin/fixlet/backend.rs` | **Producer (LLM).** Add `turn_id` param to `build_session_new_params`; push `X-Fixus-Turn-Id` MCP header so the agent carries it on every tool call. |

**Key wire contract (all producers MUST match):** fixus dispatches on the broker **record's** `event_type` (3rd arg to `produce_full`), and requires the JSON body to carry string `task_id` (else the record is skipped). New arms additionally require `step_id` (non-empty). `turn_id` is `Option<i64>` (NULL ok for Step events).

---

## Phase 1 — fixus consumer (`src/orchestrator.rs`)

All handlers follow the established `handle_llm_completed` pattern: take parsed primitives, call a `service::record_*` helper, return `Result<()>`. Test fixture (verbatim from `orchestrator.rs:1683`/`1782`/`2237`): `setup()` → real in-process logdbd, `cr3_setup_task_at_executing(&*store)` → `(tid, turn_id, redo_group)` with task at Executing (max_seq == 4), `Orchestrator::new(store.clone(), registry, tp)`.

### Task 1: orchestrator step-event handlers (`tool_*`, `llm_invoked`, `llm_failed`)

**Files:**
- Modify: `src/orchestrator.rs` (add methods next to `handle_llm_completed` ~`855`; add tests in the `#[cfg(test)]` mod)

- [ ] **Step 1: Write the failing tests** (append to the test mod in `src/orchestrator.rs`)

```rust
    #[tokio::test]
    async fn handle_tool_invoked_writes_step_event() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let registry = TaskRegistry::new();
        let tp = TokenPublisher::new().await;
        let orch = Orchestrator::new(store.clone(), registry, tp);
        let (tid, turn_id, _rg) = cr3_setup_task_at_executing(&*store).await;

        orch.handle_tool_invoked(
            &tid, Some(turn_id), "step-tool-1", "read_file", "tcid-1",
            "tid:bank:read_file:abcd1234", &serde_json::json!({"path": "/a"}), 1,
        ).await.unwrap();
        wait_seq(&*store, &tid, 5).await;

        let evs = store.get_events_after_seq(&tid, 4).await.unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].event_type, EventType::ToolInvoked);
        assert_eq!(evs[0].step_id.as_deref(), Some("step-tool-1"));
        assert_eq!(evs[0].turn_id, Some(turn_id));
    }

    #[tokio::test]
    async fn handle_tool_completed_pairs_step_event() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let orch = Orchestrator::new(store.clone(), TaskRegistry::new(), TokenPublisher::new().await);
        let (tid, turn_id, _rg) = cr3_setup_task_at_executing(&*store).await;
        orch.handle_tool_invoked(&tid, Some(turn_id), "s1", "read_file", "tc1", "k", &json!({}), 1).await.unwrap();
        orch.handle_tool_completed(&tid, Some(turn_id), "s1", "tc1", &json!({"ok": true}), false, 2).await.unwrap();
        wait_seq(&*store, &tid, 6).await;

        let evs = store.get_events_after_seq(&tid, 4).await.unwrap();
        assert_eq!(evs.iter().filter(|e| e.event_type == EventType::ToolCompleted).count(), 1);
    }

    #[tokio::test]
    async fn handle_tool_failed_writes_terminal() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let orch = Orchestrator::new(store.clone(), TaskRegistry::new(), TokenPublisher::new().await);
        let (tid, turn_id, _rg) = cr3_setup_task_at_executing(&*store).await;
        orch.handle_tool_invoked(&tid, Some(turn_id), "s2", "write_file", "tc2", "k", &json!({}), 1).await.unwrap();
        orch.handle_tool_failed(&tid, Some(turn_id), "s2", "tc2", "infra", "broker down", 2).await.unwrap();
        wait_seq(&*store, &tid, 6).await;
        let evs = store.get_events_after_seq(&tid, 4).await.unwrap();
        assert!(evs.iter().any(|e| e.event_type == EventType::ToolFailed));
    }

    #[tokio::test]
    async fn handle_llm_invoked_writes_messages_and_model() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let orch = Orchestrator::new(store.clone(), TaskRegistry::new(), TokenPublisher::new().await);
        let (tid, turn_id, _rg) = cr3_setup_task_at_executing(&*store).await;
        let msgs = vec![crate::models::Message { role: "user".into(), content: "hi".into() }];
        orch.handle_llm_invoked(&tid, Some(turn_id), "llm-1", "claude-sonnet-5", &msgs, 1).await.unwrap();
        wait_seq(&*store, &tid, 5).await;
        let evs = store.get_events_after_seq(&tid, 4).await.unwrap();
        assert_eq!(evs[0].event_type, EventType::LlmInvoked);
        assert_eq!(evs[0].payload["model"], "claude-sonnet-5");
    }

    #[tokio::test]
    async fn handle_llm_failed_writes_terminal() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let orch = Orchestrator::new(store.clone(), TaskRegistry::new(), TokenPublisher::new().await);
        let (tid, turn_id, _rg) = cr3_setup_task_at_executing(&*store).await;
        orch.handle_llm_invoked(&tid, Some(turn_id), "llm-2", "claude-sonnet-5", &[], 1).await.unwrap();
        orch.handle_llm_failed(&tid, Some(turn_id), "llm-2", "agent_error", "boom", 2).await.unwrap();
        wait_seq(&*store, &tid, 6).await;
        let evs = store.get_events_after_seq(&tid, 4).await.unwrap();
        assert!(evs.iter().any(|e| e.event_type == EventType::LlmFailed));
    }

    #[tokio::test]
    async fn step_event_allows_null_turn_id() {
        let (store, _d) = setup().await;
        let store: Arc<dyn EventStore> = Arc::new(store);
        let orch = Orchestrator::new(store.clone(), TaskRegistry::new(), TokenPublisher::new().await);
        let (tid, _turn_id, _rg) = cr3_setup_task_at_executing(&*store).await;
        // turn_id = None must be accepted (Step scope)
        orch.handle_tool_invoked(&tid, None, "bg-1", "noop", "tc9", "k", &json!({}), 1).await.unwrap();
        wait_seq(&*store, &tid, 5).await;
        let evs = store.get_events_after_seq(&tid, 4).await.unwrap();
        assert_eq!(evs[0].event_type, EventType::ToolInvoked);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib handle_tool_invoked_writes_step_event handle_tool_completed_pairs handle_tool_failed_writes_terminal handle_llm_invoked_writes_messages handle_llm_failed_writes_terminal step_event_allows_null_turn_id`
Expected: FAIL — `no method named handle_tool_invoked` (etc.) on `Orchestrator`.

- [ ] **Step 3: Implement the 5 handlers** (add next to `handle_llm_completed`, `src/orchestrator.rs` ~`855`)

```rust
    pub async fn handle_tool_invoked(
        &self,
        task_id: &str,
        turn_id: Option<i64>,
        step_id: &str,
        tool_name: &str,
        tool_call_id: &str,
        idempotency_key: &str,
        input: &serde_json::Value,
        local_seq: i64,
    ) -> Result<()> {
        service::record_tool_invoked(
            &*self.store, task_id, turn_id, step_id, tool_name, tool_call_id,
            idempotency_key, input, None, local_seq, None, None,
        ).await?;
        tracing::info!(
            "session {}: tool_invoked turn={:?} step={} tool={}",
            task_id, turn_id, step_id, tool_name
        );
        Ok(())
    }

    pub async fn handle_tool_completed(
        &self,
        task_id: &str,
        turn_id: Option<i64>,
        step_id: &str,
        tool_call_id: &str,
        output: &serde_json::Value,
        is_error: bool,
        local_seq: i64,
    ) -> Result<()> {
        service::record_tool_completed(
            &*self.store, task_id, turn_id, step_id, tool_call_id, output, is_error, local_seq,
        ).await?;
        tracing::info!(
            "session {}: tool_completed turn={:?} step={} is_error={}",
            task_id, turn_id, step_id, is_error
        );
        Ok(())
    }

    pub async fn handle_tool_failed(
        &self,
        task_id: &str,
        turn_id: Option<i64>,
        step_id: &str,
        tool_call_id: &str,
        error_type: &str,
        error_message: &str,
        local_seq: i64,
    ) -> Result<()> {
        service::record_tool_failed(
            &*self.store, task_id, turn_id, step_id, tool_call_id,
            error_type, error_message, None, 0, local_seq,
        ).await?;
        tracing::info!(
            "session {}: tool_failed turn={:?} step={} type={}",
            task_id, turn_id, step_id, error_type
        );
        Ok(())
    }

    pub async fn handle_llm_invoked(
        &self,
        task_id: &str,
        turn_id: Option<i64>,
        step_id: &str,
        model: &str,
        messages: &[crate::models::Message],
        local_seq: i64,
    ) -> Result<()> {
        service::record_llm_invoked(
            &*self.store, task_id, turn_id, step_id, model, messages, None, None, local_seq,
        ).await?;
        tracing::info!(
            "session {}: llm_invoked turn={:?} step={} model={}",
            task_id, turn_id, step_id, model
        );
        Ok(())
    }

    pub async fn handle_llm_failed(
        &self,
        task_id: &str,
        turn_id: Option<i64>,
        step_id: &str,
        error_type: &str,
        error_message: &str,
        local_seq: i64,
    ) -> Result<()> {
        service::record_llm_failed(
            &*self.store, task_id, turn_id, step_id,
            error_type, error_message, None, 0, local_seq,
        ).await?;
        tracing::info!(
            "session {}: llm_failed turn={:?} step={} type={}",
            task_id, turn_id, step_id, error_type
        );
        Ok(())
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib handle_tool_invoked_writes_step_event handle_tool_completed_pairs handle_tool_failed_writes_terminal handle_llm_invoked_writes_messages handle_llm_failed_writes_terminal step_event_allows_null_turn_id`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add src/orchestrator.rs
git commit -m "feat(orchestrator): step-event handlers (tool_*/llm_invoked/llm_failed)"
```

---

### Task 2: fix `handle_llm_completed` — read `step_id` + `local_seq` from caller

The degenerate bug: `handle_llm_completed` mints `Uuid::now_v7()` and hardcodes `local_seq: 0`, so the `llm_invoked`/`llm_completed` pair never shares a `step_id`. Change it to receive both from the caller.

**Files:**
- Modify: `src/orchestrator.rs:855-903` (`handle_llm_completed` body) + the existing test `cr8_llm_completed_records_token_metrics` (`src/orchestrator.rs:2237-2264`)

- [ ] **Step 1: Update the existing test to pass `step_id` + `local_seq`**

In `cr8_llm_completed_records_token_metrics`, change the call:

```rust
        orch.handle_llm_completed(&tid, turn_id, "llm-step-1", "claude-sonnet-5", 100, 20, 120, 1)
            .await
            .unwrap();
```

Add a pairing assertion after `wait_seq`:

```rust
        let evs = store.get_events_after_seq(&tid, 4).await.unwrap();
        assert_eq!(evs[0].event_type, EventType::LlmCompleted);
        assert_eq!(evs[0].step_id.as_deref(), Some("llm-step-1"));
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib cr8_llm_completed_records_token_metrics`
Expected: FAIL — wrong number of args / `step_id` mismatch.

- [ ] **Step 3: Change the signature + body of `handle_llm_completed`**

```rust
    pub async fn handle_llm_completed(
        &self,
        task_id: &str,
        turn_id: i64,
        step_id: &str,
        model: &str,
        input_tokens: i64,
        output_tokens: i64,
        total_tokens: i64,
        local_seq: i64,
    ) -> Result<()> {
        let payload = serde_json::json!({
            "model": model,
            "usage": {
                "prompt_tokens": input_tokens,
                "completion_tokens": output_tokens,
                "total_tokens": total_tokens,
            },
            "local_seq": local_seq,
        });

        service::record_event(
            &*self.store,
            task_id,
            Some(turn_id),
            Some(step_id),
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
            "session {}: llm_completed turn={} step={} tokens(in={} out={} total={})",
            task_id, turn_id, step_id, input_tokens, output_tokens, total_tokens
        );

        Ok(())
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib cr8_llm_completed_records_token_metrics`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orchestrator.rs
git commit -m "fix(orchestrator): handle_llm_completed reuses caller step_id+local_seq (pairing)"
```

---

### Task 3: wire the 5 new dispatch arms + fix the `llm_completed` arm

`run_lifecycle_consumer`'s `match rec.event_type.as_str()` (`src/orchestrator.rs:1586`) only handles `turn_execution_done` / `llm_completed` / `turn_execution_error`; the 5 new types hit `_ => {}` and are silently dropped. Add the arms. These are mechanical glue (parse JSON → call the Task-1/2 handlers); their correctness is covered by handler unit tests (Tasks 1–2) + the Phase-4 E2E, so this task's verification is compile + lib-test regression + a focused dispatch-shape assertion.

**Files:**
- Modify: `src/orchestrator.rs:1586-1614` (the `match rec.event_type.as_str()` block)

- [ ] **Step 1: Add the new arms** (inside the existing `match rec.event_type.as_str() { ... }`, before the `_ => {}` arm at `:1613`)

```rust
                    "tool_invoked" => {
                        let turn_id = payload.get("turn_id").and_then(|v| v.as_i64());
                        let step_id = payload["step_id"].as_str().unwrap_or("");
                        let tool_name = payload["tool_name"].as_str().unwrap_or("");
                        let tool_call_id = payload["tool_call_id"].as_str().unwrap_or("");
                        let idempotency_key = payload["idempotency_key"].as_str().unwrap_or("");
                        let local_seq = payload.get("local_seq").and_then(|v| v.as_i64()).unwrap_or(0);
                        let input = payload.get("input").cloned().unwrap_or(serde_json::Value::Null);
                        if step_id.is_empty() { continue; }
                        tracing::info!("lifecycle: tool_invoked task={} step={}", task_id, step_id);
                        let _ = orch.handle_tool_invoked(
                            task_id, turn_id, step_id, tool_name, tool_call_id, idempotency_key, &input, local_seq,
                        ).await;
                    }
                    "tool_completed" => {
                        let turn_id = payload.get("turn_id").and_then(|v| v.as_i64());
                        let step_id = payload["step_id"].as_str().unwrap_or("");
                        let tool_call_id = payload["tool_call_id"].as_str().unwrap_or("");
                        let local_seq = payload.get("local_seq").and_then(|v| v.as_i64()).unwrap_or(0);
                        let output = payload.get("output").cloned().unwrap_or(serde_json::Value::Null);
                        let is_error = payload.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false);
                        if step_id.is_empty() { continue; }
                        tracing::info!("lifecycle: tool_completed task={} step={}", task_id, step_id);
                        let _ = orch.handle_tool_completed(
                            task_id, turn_id, step_id, tool_call_id, &output, is_error, local_seq,
                        ).await;
                    }
                    "tool_failed" => {
                        let turn_id = payload.get("turn_id").and_then(|v| v.as_i64());
                        let step_id = payload["step_id"].as_str().unwrap_or("");
                        let tool_call_id = payload["tool_call_id"].as_str().unwrap_or("");
                        let error_type = payload["error_type"].as_str().unwrap_or("unknown");
                        let error_message = payload["error_message"].as_str().unwrap_or("");
                        let local_seq = payload.get("local_seq").and_then(|v| v.as_i64()).unwrap_or(0);
                        if step_id.is_empty() { continue; }
                        tracing::info!("lifecycle: tool_failed task={} step={}", task_id, step_id);
                        let _ = orch.handle_tool_failed(
                            task_id, turn_id, step_id, tool_call_id, error_type, error_message, local_seq,
                        ).await;
                    }
                    "llm_invoked" => {
                        let turn_id = payload.get("turn_id").and_then(|v| v.as_i64());
                        let step_id = payload["step_id"].as_str().unwrap_or("");
                        let model = payload["model"].as_str().unwrap_or("");
                        let local_seq = payload.get("local_seq").and_then(|v| v.as_i64()).unwrap_or(0);
                        let messages: Vec<crate::models::Message> = payload
                            .get("messages")
                            .and_then(|v| serde_json::from_value(v.clone()).ok())
                            .unwrap_or_default();
                        if step_id.is_empty() { continue; }
                        tracing::info!("lifecycle: llm_invoked task={} step={}", task_id, step_id);
                        let _ = orch.handle_llm_invoked(
                            task_id, turn_id, step_id, model, &messages, local_seq,
                        ).await;
                    }
                    "llm_failed" => {
                        let turn_id = payload.get("turn_id").and_then(|v| v.as_i64());
                        let step_id = payload["step_id"].as_str().unwrap_or("");
                        let error_type = payload["error_type"].as_str().unwrap_or("unknown");
                        let error_message = payload["error_message"].as_str().unwrap_or("");
                        let local_seq = payload.get("local_seq").and_then(|v| v.as_i64()).unwrap_or(0);
                        if step_id.is_empty() { continue; }
                        tracing::info!("lifecycle: llm_failed task={} step={}", task_id, step_id);
                        let _ = orch.handle_llm_failed(
                            task_id, turn_id, step_id, error_type, error_message, local_seq,
                        ).await;
                    }
```

- [ ] **Step 2: Fix the existing `llm_completed` arm** to read `step_id` + `local_seq` from the payload and pass them through (`src/orchestrator.rs:1593-1602`)

```rust
                    "llm_completed" => {
                        let turn_id = payload["turn_id"].as_i64().unwrap_or(0);
                        let step_id = payload["step_id"].as_str().unwrap_or("");
                        let model = payload["model"].as_str().unwrap_or("");
                        let local_seq = payload.get("local_seq").and_then(|v| v.as_i64()).unwrap_or(0);
                        let input_tokens = payload.get("input_tokens").and_then(|v| v.as_i64()).unwrap_or(0);
                        let output_tokens = payload.get("output_tokens").and_then(|v| v.as_i64()).unwrap_or(0);
                        let total_tokens = payload.get("total_tokens").and_then(|v| v.as_i64()).unwrap_or(0);
                        if turn_id == 0 || step_id.is_empty() { continue; }
                        tracing::info!("lifecycle: llm_completed task={} turn={} step={} tokens={}", task_id, turn_id, step_id, total_tokens);
                        let _ = orch.handle_llm_completed(
                            task_id, turn_id, step_id, model, input_tokens, output_tokens, total_tokens, local_seq,
                        ).await;
                    }
```

- [ ] **Step 3: Compile + run the full lib suite (regression)**

Run: `cargo check --lib && cargo test --lib -q`
Expected: compiles; all lib tests PASS (no regressions; the consumer loop itself is not unit-tested — covered by Phase-4 E2E).

- [ ] **Step 4: Commit**

```bash
git add src/orchestrator.rs
git commit -m "feat(orchestrator): dispatch tool_*/llm_invoked/llm_failed in lifecycle consumer"
```

---

## Phase 2 — tools-bank producer (`src/bin/tools-bank/`)

### Task 4: lift `BrokerProducer` to a shared `Arc<Mutex<>>`

`BrokerProducer` is currently **moved** into `SandboxAdapter` inside `build_registry`, so `tools_call` (which lives at the `AppState` level, above the registry) cannot reach it. Wrap it in `Arc<Mutex<>>` **before** `build_registry`, clone once into `SandboxAdapter` and once into `AppState.task_end_producer`.

**Files:**
- Modify: `src/bin/tools-bank/main.rs` (`AppState` ~`86`, producer connect ~`349`, `build_registry` signature + `SandboxAdapter` construction ~`283-291`/`385-393`)

- [ ] **Step 1: Add `task_end_producer` + `seq` to `AppState`** (`src/bin/tools-bank/main.rs:86`)

```rust
struct AppState {
    registry: ToolRegistry,
    task_end_producer: Arc<tokio::sync::Mutex<BrokerProducer>>,
    lifecycle_namespace: String,
    seq: std::sync::atomic::AtomicU64,
}
```

(Add `use std::sync::atomic::{AtomicU64, Ordering};` at the top if not present.)

- [ ] **Step 2: Wrap the producer at connect time** (`src/bin/tools-bank/main.rs:349-350`)

```rust
    let producer = Arc::new(tokio::sync::Mutex::new(
        BrokerProducer::connect(format!("http://{}", cli.broker_addr)).await
            .expect("broker producer connect"),
    ));
```

- [ ] **Step 3: Change `build_registry` to borrow the shared producer** (find its definition ~`main.rs:385`)

Change the signature so it takes `producer: Arc<tokio::sync::Mutex<BrokerProducer>>` (clone) and `namespace: String` (clone) instead of owning + constructing them. Inside, change the `SandboxAdapter` construction:

```rust
    let sandbox = SandboxAdapter {
        producer: producer.clone(),
        pending: pending.clone(),
        namespace: namespace.clone(),
        region: region.clone(),
        extras,
    };
```

(Previously this site did `producer: Arc::new(Mutex::new(producer))` from an owned value — remove that inner wrap; the Arc<Mutex> is now passed in.) Keep the call site passing `producer.clone()` and `namespace.clone()`.

- [ ] **Step 4: Construct `AppState` with the shared handles** (`src/bin/tools-bank/main.rs:393`)

```rust
    let state = Arc::new(AppState {
        registry,
        task_end_producer: producer.clone(),
        lifecycle_namespace: namespace.clone(),
        seq: AtomicU64::new(0),
    });
```

- [ ] **Step 5: Compile + run tools-bank tests (regression)**

Run: `cargo check --bin tools-bank && cargo test --bin tools-bank -q`
Expected: compiles; existing tools-bank tests still PASS (the sandbox round-trip now shares the same producer handle, behavior unchanged).

- [ ] **Step 6: Commit**

```bash
git add src/bin/tools-bank/main.rs
git commit -m "refactor(tools-bank): lift BrokerProducer to shared Arc<Mutex> on AppState"
```

---

### Task 5: extend `CallCtx` (`step_id`, `turn_id`) + read `X-Fixus-Turn-Id`

`step_id` must move **up** into `tools_call` (today it is minted inside `SandboxAdapter::invoke` at `adapter.rs:317`, invisible to `tools_call`, and absent for `HttpActionAdapter`). `turn_id` is entirely new.

**Files:**
- Modify: `src/bin/tools-bank/adapter.rs` (`CallCtx` ~`42-49`, `SandboxAdapter::invoke` ~`311-383`)
- Modify: `src/bin/tools-bank/main.rs` (`handle_mcp` ~`148-189`, `tools_call` ~`104-146`)

- [ ] **Step 1: Extend `CallCtx`** (`src/bin/tools-bank/adapter.rs:42-49`)

```rust
#[derive(Clone)]
pub struct CallCtx {
    pub task_id: String,
    pub idempotency_key: String,
    pub effective_policy: Option<serde_json::Value>,
    /// task-end 配对 key(整个 tools/call 共享,由 tools_call 铸造)
    pub step_id: String,
    /// 来自 X-Fixus-Turn-Id;直接 MCP 调用(无 turn)时为 None
    pub turn_id: Option<i64>,
}
```

- [ ] **Step 2: Make `SandboxAdapter::invoke` reuse `ctx.step_id`** (`src/bin/tools-bank/adapter.rs:317`)

Delete the two local mints:
```rust
    let step_id = uuid::Uuid::now_v7().to_string();
    let tool_call_id = uuid::Uuid::now_v7().to_string();
```
Replace with:
```rust
    let step_id = ctx.step_id.clone();
    let tool_call_id = uuid::Uuid::now_v7().to_string();
```
(`tool_call_id` stays adapter-local — it correlates the sandbox round-trip; `step_id` now comes from `ctx` so the task-end pair shares it.) Everything downstream (`sandbox_payload(... &tool_call_id ...)`, `self.pending.insert(step_id.clone(), tx)`, `build_invoke_meta(&ctx.task_id, &step_id, ...)`) is unchanged.

- [ ] **Step 3: Read `X-Fixus-Turn-Id` in `handle_mcp`** (`src/bin/tools-bank/main.rs`, inside the `"tools/call"` arm ~`173`)

```rust
            let task_id = headers.get("X-Fixus-Session-Id")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown");
            let turn_id = headers.get("X-Fixus-Turn-Id")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<i64>().ok());
            // X-Fixus-Policy ...
            let effective_policy = headers.get("X-Fixus-Policy")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
            let args = params.and_then(|p| p.get("arguments")).cloned().unwrap_or(serde_json::Value::Null);

            Ok(Json(tools_call(&state, task_id, turn_id, tool_name, &args, effective_policy, body.id).await))
```

- [ ] **Step 4: Thread `turn_id` through `tools_call` + mint `step_id` into `CallCtx`** (`src/bin/tools-bank/main.rs:104`)

Change the signature and the `CallCtx` construction:

```rust
async fn tools_call(
    state: &AppState,
    task_id: &str,
    turn_id: Option<i64>,
    tool_name: &str,
    args: &serde_json::Value,
    effective_policy: Option<serde_json::Value>,
    id: Option<i64>,
) -> McpResponse {
    let idempotency_key = build_key(task_id, tool_name, args);
    let step_id = uuid::Uuid::now_v7().to_string();
    let ctx = CallCtx {
        task_id: task_id.to_string(),
        idempotency_key,
        effective_policy,
        step_id,
        turn_id,
    };

    tracing::info!("tools-bank: tools/call task={} tool={} step={}", task_id, tool_name, ctx.step_id);

    match state.registry.invoke(tool_name, args, &ctx).await {
        // ... unchanged Ok/Err arms ...
    }
}
```

- [ ] **Step 5: Compile**

Run: `cargo check --bin tools-bank`
Expected: compiles. (Existing `CallCtx` construction sites — if any in tests — need `step_id`/`turn_id` added; the compiler will point them out. Add `step_id: "test-step".into(), turn_id: None` to any test fixture the compiler flags.)

- [ ] **Step 6: Commit**

```bash
git add src/bin/tools-bank/adapter.rs src/bin/tools-bank/main.rs
git commit -m "feat(tools-bank): CallCtx carries step_id+turn_id; X-Fixus-Turn-Id header"
```

---

### Task 6: emit the `tool_invoked` + `tool_completed`/`tool_failed` pair from `tools_call`

Map outcomes: `Ok(r)` → `tool_completed` (`is_error = !r.success`, carrying `output`); `Err(InvokeError::NotFound | Adapter)` → `tool_failed` (infra error). Both share `ctx.step_id`. `local_seq` from the shared `AppState.seq` counter (`step_id` is the pairing key; `local_seq` only satisfies the schema + gives a monotonic hint).

**Files:**
- Modify: `src/bin/tools-bank/main.rs` (`tools_call` body + a new `emit_task_end` helper)

- [ ] **Step 1: Write the failing test for the payload builders** (add a `#[cfg(test)]` mod in `src/bin/tools-bank/main.rs` if none exists; else append)

```rust
#[cfg(test)]
mod step_event_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tool_invoked_payload_shape() {
        let p = tool_invoked_payload("t1", Some(7), "s1", "read_file", "tc1", "k", &json!({"a":1}), 3);
        assert_eq!(p["task_id"], "t1");
        assert_eq!(p["turn_id"], 7);
        assert_eq!(p["step_id"], "s1");
        assert_eq!(p["tool_name"], "read_file");
        assert_eq!(p["tool_call_id"], "tc1");
        assert_eq!(p["idempotency_key"], "k");
        assert_eq!(p["local_seq"], 3);
    }

    #[test]
    fn tool_completed_payload_shape() {
        let p = tool_completed_payload("t1", Some(7), "s1", "tc1", &json!({"ok":true}), true, 4);
        assert_eq!(p["step_id"], "s1");
        assert_eq!(p["is_error"], true);
        assert_eq!(p["local_seq"], 4);
    }

    #[test]
    fn tool_failed_payload_shape() {
        let p = tool_failed_payload("t1", Some(7), "s1", "tc1", "NotFound", "unknown tool: x", 5);
        assert_eq!(p["step_id"], "s1");
        assert_eq!(p["error_type"], "NotFound");
        assert_eq!(p["error_message"], "unknown tool: x");
        assert_eq!(p["local_seq"], 5);
    }

    #[test]
    fn tool_invoked_payload_null_turn_id() {
        let p = tool_invoked_payload("t1", None, "s1", "noop", "tc1", "k", &json!({}), 1);
        assert!(p.get("turn_id").is_none() || p["turn_id"].is_null());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --bin tools-bank step_event_tests -q`
Expected: FAIL — `cannot find function tool_invoked_payload`.

- [ ] **Step 3: Add the payload builders + `emit_task_end` helper** (top-level in `src/bin/tools-bank/main.rs`)

```rust
fn tool_invoked_payload(
    task_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    tool_name: &str,
    tool_call_id: &str,
    idempotency_key: &str,
    input: &serde_json::Value,
    local_seq: i64,
) -> serde_json::Value {
    let mut p = serde_json::json!({
        "task_id": task_id,
        "step_id": step_id,
        "tool_name": tool_name,
        "tool_call_id": tool_call_id,
        "idempotency_key": idempotency_key,
        "input": input,
        "local_seq": local_seq,
        "event_type": "tool_invoked",
    });
    if let Some(t) = turn_id {
        p["turn_id"] = serde_json::json!(t);
    }
    p
}

fn tool_completed_payload(
    task_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    tool_call_id: &str,
    output: &serde_json::Value,
    is_error: bool,
    local_seq: i64,
) -> serde_json::Value {
    let mut p = serde_json::json!({
        "task_id": task_id,
        "step_id": step_id,
        "tool_call_id": tool_call_id,
        "output": output,
        "is_error": is_error,
        "local_seq": local_seq,
        "event_type": "tool_completed",
    });
    if let Some(t) = turn_id {
        p["turn_id"] = serde_json::json!(t);
    }
    p
}

fn tool_failed_payload(
    task_id: &str,
    turn_id: Option<i64>,
    step_id: &str,
    tool_call_id: &str,
    error_type: &str,
    error_message: &str,
    local_seq: i64,
) -> serde_json::Value {
    let mut p = serde_json::json!({
        "task_id": task_id,
        "step_id": step_id,
        "tool_call_id": tool_call_id,
        "error_type": error_type,
        "error_message": error_message,
        "local_seq": local_seq,
        "event_type": "tool_failed",
    });
    if let Some(t) = turn_id {
        p["turn_id"] = serde_json::json!(t);
    }
    p
}

async fn emit_task_end(
    producer: &Arc<tokio::sync::Mutex<BrokerProducer>>,
    namespace: &str,
    task_id: &str,
    event_type: &str,
    payload: serde_json::Value,
) {
    let content = serde_json::to_vec(&payload).unwrap_or_default();
    let meta = std::collections::HashMap::from([
        ("task_id".into(), task_id.to_string()),
        ("event_type".into(), event_type.to_string()),
    ]);
    let mut lp = producer.lock().await;
    if let Err(e) = lp.produce_full(
        namespace, "task-end", event_type, &content,
        Some(task_id), 0, "application/json", &meta,
    ).await {
        tracing::warn!("tools-bank: broker produce {} failed: {}", event_type, e);
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --bin tools-bank step_event_tests -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Wire the emits into `tools_call`** (wrap the existing `match state.registry.invoke(...)`)

```rust
    let local_seq = state.seq.fetch_add(1, Ordering::Relaxed) as i64 + 1;
    emit_task_end(
        &state.task_end_producer, &state.lifecycle_namespace, task_id, "tool_invoked",
        tool_invoked_payload(task_id, ctx.turn_id, &ctx.step_id, tool_name, &ctx.tool_call_id_for_event(), &ctx.idempotency_key, args, local_seq),
    ).await;
    // NOTE: tool_call_id for the event = ctx.step_id's companion; see note below.

    match state.registry.invoke(tool_name, args, &ctx).await {
        Ok(r) => {
            let text = /* ...existing text-building unchanged... */;
            let local_seq2 = state.seq.fetch_add(1, Ordering::Relaxed) as i64 + 1;
            emit_task_end(
                &state.task_end_producer, &state.lifecycle_namespace, task_id,
                "tool_completed",
                tool_completed_payload(task_id, ctx.turn_id, &ctx.step_id, &ctx.step_id, &r.output, !r.success, local_seq2),
            ).await;
            mcp_ok(id, serde_json::json!({
                "content": [{"type": "text", "text": text}],
                "isError": !r.success,
                "_meta": {"duration_ms": r.duration_ms}
            }))
        }
        Err(InvokeError::NotFound) => {
            let local_seq2 = state.seq.fetch_add(1, Ordering::Relaxed) as i64 + 1;
            emit_task_end(
                &state.task_end_producer, &state.lifecycle_namespace, task_id,
                "tool_failed",
                tool_failed_payload(task_id, ctx.turn_id, &ctx.step_id, &ctx.step_id, "NotFound", &format!("unknown tool: {}", tool_name), local_seq2),
            ).await;
            mcp_err(id, -32602, &format!("unknown tool: {}", tool_name))
        }
        Err(InvokeError::Adapter(msg)) => {
            let local_seq2 = state.seq.fetch_add(1, Ordering::Relaxed) as i64 + 1;
            emit_task_end(
                &state.task_end_producer, &state.lifecycle_namespace, task_id,
                "tool_failed",
                tool_failed_payload(task_id, ctx.turn_id, &ctx.step_id, &ctx.step_id, "Adapter", &msg, local_seq2),
            ).await;
            mcp_err(id, -32603, &msg)
        }
    }
```

**Note on `tool_call_id` provenance:** tools-bank never sees claude's internal tool-use id. Use `ctx.step_id` as the `tool_call_id` value in the event payloads (it already uniquely identifies the call within the task). `step_id` — not `tool_call_id` — is what pairs `invoked ↔ terminal`, so this field is informational. Remove the placeholder `ctx.tool_call_id_for_event()` call above and use `ctx.step_id` directly (it was only written that way to make the note explicit):

```rust
        tool_invoked_payload(task_id, ctx.turn_id, &ctx.step_id, tool_name, &ctx.step_id, &ctx.idempotency_key, args, local_seq),
```

- [ ] **Step 6: Compile + run tools-bank tests**

Run: `cargo check --bin tools-bank && cargo test --bin tools-bank -q`
Expected: compiles; all tools-bank tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/bin/tools-bank/main.rs
git commit -m "feat(tools-bank): emit tool_invoked+tool_completed/failed pair to task-end"
```

---

## Phase 3 — fixlet producer (`src/bin/fixlet/`)

### Task 7: `TurnContext.step_id` + `X-Fixus-Turn-Id` MCP header

**Files:**
- Modify: `src/bin/fixlet/idempotency.rs` (`TurnContext` `56-83`)
- Modify: `src/bin/fixlet/backend.rs` (`build_session_new_params` `179-200`) + its tests
- Modify: `src/bin/fixlet/router.rs` (call site `505-511`)

- [ ] **Step 1: Add `step_id` to `TurnContext`** (`src/bin/fixlet/idempotency.rs:56-83`)

Add the field and initialize it in `new()`:

```rust
pub struct TurnContext {
    pub task_id: String,
    pub turn_id: i64,
    pub redo_group: String,
    pub redo_count: i32,
    pub local_seq: LocalSeqCounter,
    pub model: String,
    /// 本 turn 的 LLM step 配对 key;session_prompt 前 mint,FinalMessage/Error 复用
    pub step_id: Option<String>,
}
```
and in `new()`:
```rust
        Self {
            task_id,
            turn_id,
            redo_group,
            redo_count,
            local_seq: LocalSeqCounter::new(),
            model: String::new(),
            step_id: None,
        }
```

- [ ] **Step 2: Write the failing test for the turn-id header** (in `src/bin/fixlet/backend.rs` test mod)

```rust
    #[test]
    fn session_new_params_include_turn_id_header() {
        let backend = select_backend(&Default::default());
        let params = build_session_new_params(
            backend.as_ref(), "task-123", "/cwd", "http://tools-bank", 42, None,
        );
        let headers = params["mcpServers"][0]["headers"].as_array().unwrap();
        let turn_id_hdr = headers.iter().find(|h| h["name"] == "X-Fixus-Turn-Id").unwrap();
        assert_eq!(turn_id_hdr["value"], 42);
    }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --bin fixlet session_new_params_include_turn_id_header`
Expected: FAIL — `mismatched types` / wrong arg count on `build_session_new_params`.

- [ ] **Step 4: Add `turn_id` param + push the header** (`src/bin/fixlet/backend.rs:179-200`)

```rust
pub fn build_session_new_params(
    backend: &dyn AgentBackend,
    task_id: &str,
    cwd: &str,
    tools_bank_url: &str,
    turn_id: i64,
    effective_policy: Option<String>,
) -> Value {
    // headers 动态构造:X-Fixus-Session-Id(per-task 路由)+ X-Fixus-Turn-Id(每个 tool call 带回 tools-bank)恒在;
    // X-Fixus-Policy 仅当 policy 存在时注入。
    let mut headers = vec![
        serde_json::json!({"name": "X-Fixus-Session-Id", "value": task_id}),
        serde_json::json!({"name": "X-Fixus-Turn-Id", "value": turn_id}),
    ];
    if let Some(p) = effective_policy {
        headers.push(serde_json::json!({"name": "X-Fixus-Policy", "value": p}));
    }
    let mut params = serde_json::json!({
        "cwd": cwd,
        "mcpServers": [{
            "type": "http",
            "name": "fixus",
            "url": tools_bank_url,
            "headers": headers
        }]
    });
    // ...rest unchanged (backend-specific augment)...
    params
}
```

- [ ] **Step 5: Update the call site** (`src/bin/fixlet/router.rs:505-511`)

```rust
    let params = backend::build_session_new_params(
        config.backend.as_ref(),
        task_id,
        &cwd,
        &tools_bank_url,
        turn_id,
        policy_str,
    );
```

- [ ] **Step 6: Update the other existing `build_session_new_params` tests** in `backend.rs` (the compiler will list them — e.g. `build_session_new_params_matches_legacy_claude`, `session_new_params_include_policy_header_when_present`) to pass a `turn_id` argument (e.g. `1`). Add an assertion that `X-Fixus-Session-Id` is still present in the legacy test.

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test --bin fixlet -q`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/bin/fixlet/idempotency.rs src/bin/fixlet/backend.rs src/bin/fixlet/router.rs
git commit -m "feat(fixlet): TurnContext.step_id + X-Fixus-Turn-Id session header"
```

---

### Task 8: mint `step_id` + emit `llm_invoked` before `session/prompt`; activate `local_seq`

**Files:**
- Modify: `src/bin/fixlet/router.rs` (just before `acp.session_prompt(...)` at `563`; add an `emit_lifecycle` helper near the top of the file)

- [ ] **Step 1: Write the failing test for the `llm_invoked` payload** (append to `src/bin/fixlet/router.rs` test mod, or create one)

```rust
#[cfg(test)]
mod step_event_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn llm_invoked_payload_shape() {
        let msgs = vec![fixus::Message { role: "user".into(), content: "hi".into() }];
        let p = llm_invoked_payload("t1", 5, "llm-s1", "claude-sonnet-5", &msgs, 1);
        assert_eq!(p["task_id"], "t1");
        assert_eq!(p["turn_id"], 5);
        assert_eq!(p["step_id"], "llm-s1");
        assert_eq!(p["model"], "claude-sonnet-5");
        assert_eq!(p["messages"][0]["role"], "user");
        assert_eq!(p["local_seq"], 1);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --bin fixlet step_event_tests::llm_invoked_payload_shape`
Expected: FAIL — `cannot find function llm_invoked_payload`.

- [ ] **Step 3: Add the payload builder + `emit_lifecycle` helper** (top-level in `src/bin/fixlet/router.rs`)

```rust
fn llm_invoked_payload(
    task_id: &str,
    turn_id: i64,
    step_id: &str,
    model: &str,
    messages: &[fixus::Message],
    local_seq: i64,
) -> serde_json::Value {
    serde_json::json!({
        "task_id": task_id,
        "turn_id": turn_id,
        "step_id": step_id,
        "model": model,
        "messages": messages,
        "local_seq": local_seq,
        "event_type": "llm_invoked",
    })
}

async fn emit_lifecycle(
    producer: &tokio::sync::Mutex<BrokerProducer>,
    namespace: &str,
    task_id: &str,
    event_type: &str,
    payload: serde_json::Value,
) {
    let content = serde_json::to_vec(&payload).unwrap_or_default();
    let meta = std::collections::HashMap::from([
        ("task_id".into(), task_id.to_string()),
        ("event_type".into(), event_type.to_string()),
    ]);
    let mut lp = producer.lock().await;
    if let Err(e) = lp.produce_full(
        namespace, "task-end", event_type, &content,
        Some(task_id), 0, "application/json", &meta,
    ).await {
        tracing::warn!("fixlet: broker produce {} failed: {}", event_type, e);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --bin fixlet step_event_tests::llm_invoked_payload_shape`
Expected: PASS.

- [ ] **Step 5: Mint `step_id` + emit `llm_invoked` before the prompt dispatch** (`src/bin/fixlet/router.rs:563`, immediately before `acp.session_prompt(&real_sid, prompt_blocks, vec![]);`)

```rust
    // mint per-turn LLM step_id;FinalMessage 与 ACP Error 复用,保证 invoked↔terminal 配对
    let step_id = uuid::Uuid::now_v7().to_string();
    ctx.step_id = Some(step_id.clone());
    *active_turn = Some(ctx.clone());
    emit_lifecycle(
        &lifecycle_producer, &lifecycle_namespace, &ctx.task_id, "llm_invoked",
        llm_invoked_payload(&ctx.task_id, ctx.turn_id, &step_id, &ctx.model, &messages, ctx.local_seq.next()),
    ).await;

    acp.session_prompt(&real_sid, prompt_blocks, vec![]);

    Ok(())
}
```

(`&lifecycle_producer` is the `Arc<tokio::sync::Mutex<BrokerProducer>>` from `router.rs:240`; `&lifecycle_namespace` from `router.rs:266`; `messages` is the `Vec<fixus::Message>` destructured at `router.rs:435` and still in scope. `ctx.local_seq.next()` finally exercises `LocalSeqCounter`.)

- [ ] **Step 6: Compile**

Run: `cargo check --bin fixlet`
Expected: compiles.

- [ ] **Step 7: Commit**

```bash
git add src/bin/fixlet/router.rs
git commit -m "feat(fixlet): emit llm_invoked before session/prompt; activate local_seq"
```

---

### Task 9: emit `llm_completed` (paired, ungated) + `llm_failed`

- **`llm_completed`:** the existing block (`router.rs:620-680`) is gated on `if let Some(ref u) = usage`, so when `usage` is `None` nothing is emitted and the `llm_invoked` dangles. Remove the gate (always emit at `FinalMessage`), and add `step_id` + `local_seq`.
- **`llm_failed`:** emit at the ACP `Error` arm (`router.rs:682`). (Spawn / `session/new` failures happen *before* `session/prompt` — i.e. before any `llm_invoked` — so they are NOT `llm_failed`; they keep their existing `turn_execution_error`. An agent process that dies mid-response leaves a dangling `llm_invoked`, which is the documented crash-mid-step limitation.)

**Files:**
- Modify: `src/bin/fixlet/router.rs` (`FinalMessage` arm `620-680`; `ParsedAcpEvent::Error` arm `682-684`)

- [ ] **Step 1: Write failing tests for the two payload builders** (append to the `step_event_tests` mod added in Task 8)

```rust
    #[test]
    fn llm_completed_payload_shape_with_usage() {
        let p = llm_completed_payload("t1", 5, "llm-s1", "claude-sonnet-5", Some((100, 20, 120)), 2);
        assert_eq!(p["step_id"], "llm-s1");
        assert_eq!(p["input_tokens"], 100);
        assert_eq!(p["output_tokens"], 20);
        assert_eq!(p["total_tokens"], 120);
        assert_eq!(p["local_seq"], 2);
    }

    #[test]
    fn llm_completed_payload_shape_without_usage() {
        let p = llm_completed_payload("t1", 5, "llm-s1", "claude-sonnet-5", None, 2);
        assert_eq!(p["step_id"], "llm-s1");
        assert_eq!(p["input_tokens"], 0);
    }

    #[test]
    fn llm_failed_payload_shape() {
        let p = llm_failed_payload("t1", 5, "llm-s1", "agent_error", "boom", 3);
        assert_eq!(p["step_id"], "llm-s1");
        assert_eq!(p["error_type"], "agent_error");
        assert_eq!(p["error_message"], "boom");
        assert_eq!(p["local_seq"], 3);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --bin fixlet step_event_tests -q`
Expected: FAIL — `cannot find function llm_completed_payload` / `llm_failed_payload`.

- [ ] **Step 3: Add the two payload builders** (next to `llm_invoked_payload` from Task 8)

```rust
fn llm_completed_payload(
    task_id: &str,
    turn_id: i64,
    step_id: &str,
    model: &str,
    usage: Option<(i64, i64, i64)>,
    local_seq: i64,
) -> serde_json::Value {
    let (input_tokens, output_tokens, total_tokens) = usage.unwrap_or((0, 0, 0));
    serde_json::json!({
        "task_id": task_id,
        "turn_id": turn_id,
        "step_id": step_id,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "local_seq": local_seq,
        "event_type": "llm_completed",
    })
}

fn llm_failed_payload(
    task_id: &str,
    turn_id: i64,
    step_id: &str,
    error_type: &str,
    error_message: &str,
    local_seq: i64,
) -> serde_json::Value {
    serde_json::json!({
        "task_id": task_id,
        "turn_id": turn_id,
        "step_id": step_id,
        "error_type": error_type,
        "error_message": error_message,
        "local_seq": local_seq,
        "event_type": "llm_failed",
    })
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --bin fixlet step_event_tests -q`
Expected: PASS (4 tests incl. Task 8's).

- [ ] **Step 5: Replace the gated `llm_completed` block** in the `FinalMessage` arm (`router.rs:620-680`). Remove the `if let Some(ref u) = usage` gate; always emit, reading tokens from `usage` (defaulting to 0). Replace the existing inline `llm_payload` + `produce_full` block with:

```rust
        ParsedAcpEvent::FinalMessage { usage } => {
            let final_text = msg_acc.finalize();

            // llm_completed → broker lifecycle(与 llm_invoked 配对;usage 缺失也发,token 计 0)
            let usage_tuple = usage.as_ref().map(|u| (u.input_tokens, u.output_tokens, u.total_tokens));
            if let Some(step_id) = ctx.step_id.as_deref() {
                emit_lifecycle(
                    &lifecycle_producer, &lifecycle_namespace, &ctx.task_id, "llm_completed",
                    llm_completed_payload(&ctx.task_id, ctx.turn_id, step_id, &ctx.model, usage_tuple, ctx.local_seq.next()),
                ).await;
            }
            if let Some(ref u) = usage {
                tracing::info!(
                    "LLM completed: {} input + {} output = {} total tokens",
                    u.input_tokens, u.output_tokens, u.total_tokens
                );
            }

            tracing::info!(
                "Agent final message: {} chars ({} chunks), max_local_seq={}",
                final_text.len(),
                msg_acc.chunks.len(),
                ctx.local_seq.current()
            );

            // Produce turn_execution_done to broker (unchanged) ...
            // (keep the existing done_payload + produce_full block verbatim)
```

(Leave the existing `turn_execution_done` block that follows untouched.)

- [ ] **Step 6: Emit `llm_failed` at the ACP `Error` arm** (`router.rs:682-684`)

```rust
        ParsedAcpEvent::Error(err) => {
            tracing::warn!("Agent error: {}", err);
            if let Some(step_id) = ctx.step_id.as_deref() {
                emit_lifecycle(
                    &lifecycle_producer, &lifecycle_namespace, &ctx.task_id, "llm_failed",
                    llm_failed_payload(&ctx.task_id, ctx.turn_id, step_id, "agent_error", &err, ctx.local_seq.next()),
                ).await;
            }
        }
```

- [ ] **Step 7: Compile + run fixlet tests**

Run: `cargo check --bin fixlet && cargo test --bin fixlet -q`
Expected: compiles; all fixlet tests PASS.

- [ ] **Step 8: Commit**

```bash
git add src/bin/fixlet/router.rs
git commit -m "feat(fixlet): emit paired llm_completed (ungated) + llm_failed on agent error"
```

---

## Phase 4 — end-to-end validation (live 9-process stack)

### Task 10: confirm pairing + SSE delivery on a real tool-calling turn

This is the integration proof that the producer (Phases 2–3) and consumer (Phase 1) round-trip through the broker and land as paired step events visible over SSE. Follow the stack in memory `dev-stack-startup`.

- [ ] **Step 1: Build all binaries**

Run: `cargo build --bin fixus --bin fixlet --bin tools-bank --bin fixus-stream`
Expected: clean build.

- [ ] **Step 2: Bring up the 9-process stack** (broker session_timeout>0; fixlet without proxy; task_type projection race noted in `dev-stack-startup`). Use the `/tmp` startup scripts referenced in memory (do not delete them per `cleanup-test-residue`).

- [ ] **Step 3: Submit a turn that forces one tool call** (e.g. an agent prompt that uses a builtin like `read_file`/`jq`).

- [ ] **Step 4: Assert the 4 step events appear, paired, in the turn's event log**

Run: `curl -s http://127.0.0.1:<fixus-port>/turns/<turn_id>/events | jq '.[] | {type, step_id, turn_id}'`
Expected: among the lifecycle events, see `tool_invoked` + `tool_completed` (same `step_id`), and `llm_invoked` + `llm_completed` (same `step_id`). `local_seq` values are non-zero.

- [ ] **Step 5: Assert `turn_steps()` is populated** (projection pairing fired)

Run: `curl -s http://127.0.0.1:<fixus-port>/turns/<turn_id> | jq '.steps'` (or the projection field name the API exposes — confirm against `projection.rs:293`)
Expected: non-empty steps list; each step has matching invoked+terminal.

- [ ] **Step 6: Assert the events flow over SSE**

Connect to the turn's SSE stream (`/turns/<turn_id>/stream`) during the turn (or replay via the event API); confirm `tool_invoked`/`tool_completed`/`llm_invoked`/`llm_completed` frames appear.

- [ ] **Step 7: Tear down the stack + remove test residue** (sandbox/perf/e2e outputs only — per `cleanup-test-residue`; keep CLAUDE.md + `/tmp` startup scripts).

- [ ] **Step 8: Commit any final wiring** (if the E2E surfaced a needed tweak) and update memory `step-events-spec-pending` → mark done.

---

## Self-Review

**Spec coverage** (every spec Change → task):

| Spec section | Task |
|---|---|
| Change 1 — tools-bank emits tool-step pair | Tasks 4 (producer shared) → 5 (step_id up + turn_id header) → 6 (emit pair) |
| Change 2 — fixlet emits LLM-step pair + fixes degenerate | Tasks 7 (step_id + header) → 8 (llm_invoked) → 9 (llm_completed ungated + llm_failed) |
| Change 3 — fixus 5 new arms + fix llm_completed | Tasks 1 (handlers) → 2 (fix handle_llm_completed) → 3 (arms) |
| Pairing / ordering / safety (step_id, terminal-uniqueness, NULL turn_id) | Task 1 (`step_event_allows_null_turn_id`); enforced by existing storage/projection |
| `local_seq` finally exercised | Task 8 (fixlet `next()`) + Task 6 (tools-bank counter) |
| Testing — unit fixus/tools-bank/fixlet + live E2E | Tasks 1,2,6,7,8,9 (unit) + Task 10 (E2E) |

**Placeholder scan:** all code blocks contain real code derived from the on-disk signatures verified by the three exploration agents. The one explicit placeholder in Task 6 Step 5 (`ctx.tool_call_id_for_event()`) is called out and resolved to `ctx.step_id` in the same step. No "TBD"/"add error handling"/"similar to Task N" remain.

**Type consistency:** `step_id: &str` / `Option<String>` used uniformly (TurnContext stores `Option<String>`, handlers take `&str`, arms read `as_str()`); `turn_id: Option<i64>` on the step handlers / `i64` on `handle_llm_completed` (turn-scoped) — matches `service::record_*` signatures exactly; `tool_call_id` event field resolves to `ctx.step_id` consistently in both `tool_invoked` and `tool_completed`/`tool_failed`. Payload builder names (`tool_invoked_payload`, `llm_invoked_payload`, …) match between their test (Step 1) and definition (Step 3) in each task.

**Open detail resolutions (from spec §"Open details"):**
- *fixlet emit point for `llm_invoked`*: `router.rs:563` (before `acp.session_prompt`) — Task 8.
- *carry `step_id` to FinalMessage*: stashed on `TurnContext.step_id` — Task 7+8.
- *`messages` representation*: `&messages` (`Vec<fixus::Message>`, already in scope) serialized verbatim — Task 8.
- *`uuid` available to tools-bank*: yes (`Cargo.toml:29`, used `adapter.rs:317`) — Task 5.
- *`X-Fixus-Turn-Id` read path*: `handle_mcp` header read — Task 5; set via `build_session_new_params` — Task 7.
- *`tool_call_id` provenance*: tools-bank never sees claude's tool-use id → use `ctx.step_id` (informational; `step_id` pairs) — Task 6.

## Notes / known limitations (carried from spec)

- Crash mid-step leaves a dangling `invoked` (surfaces in `incomplete_steps`); on redo a new `step_id` is minted. Documented, acceptable.
- Live "tool started" granularity still deferred (needs shared call-id across fixlet↔tools-bank).
- Only the `claude-code`/ACP backend is instrumented.
