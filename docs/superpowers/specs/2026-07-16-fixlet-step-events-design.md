# fixlet / tools-bank → Event-Store Step Events

- **Date:** 2026-07-16
- **Status:** Approved (design); pending implementation plan
- **Owner:** runtime (fixus)

## Problem

Observers (nuntius, via fixus-stream SSE) see only coarse task/turn lifecycle —
~5 events per turn. The 31 tool calls and the LLM call inside a turn are
invisible, because **no component emits step-level events** (`tool_invoked` /
`tool_completed` / `tool_failed` / `llm_invoked` / `llm_completed` /
`llm_failed`) into the event store.

The receiving side is already built: the 6 step `EventType`s are defined, their
payload structs exist, and `storage` / `projection` / `context` / `recovery`
all consume them. `fixus-stream`'s `SUBSCRIBE_EVENT_TYPES` already lists all 6.
The `service::record_*` step helpers exist. **Only the producer side is
missing:**

- `record_llm_invoked / record_llm_completed / record_llm_failed /
  record_tool_invoked / record_tool_completed` have **zero production callers**.
  (`record_tool_failed` has one caller — `recovery.rs`.)
- The single end-to-end path today is degenerate: fixlet publishes an
  `llm_completed` *broker lifecycle string*, and `orchestrator::handle_llm_completed`
  synthesizes a lone `LlmCompleted` event with a **random `step_id`** and no
  companion `llm_invoked` — so projection pairing never fires.

## Goals

1. Every tool call and every LLM call inside a turn becomes a properly-paired
   step-event pair in the event store (`{task_id}` logdbd stream).
2. Observers see them through the **existing** fixus-stream SSE channel — no
   nuntius change, no new transport, no broker coupling for external readers.
3. Pairing is correct (`step_id`) so `projection` (`turn_steps`,
   `incomplete_steps`), `context` reconstruction, and `recovery` behave
   coherently — and the degenerate `llm_completed` is fixed.

## Non-goals

- Live "tool started" granularity (tool events appear when the call completes,
  not mid-flight). Deferred — would require a shared call-id across
  fixlet↔tools-bank.
- Changing the broker star topology, the event-store schema, or the 22
  `EventType` set.
- Making nuntius read broker streams directly (rejected: couples nuntius to
  fixus-internal wire formats).

## Constraints that drive the design

1. **Only fixus appends `AgentEvent`s.** fixlet / tools-bank have no
   `EventStore` handle. The sole channel is the `task-end` broker stream, which
   `run_lifecycle_consumer` (`orchestrator.rs:1586`) dispatches on `event_type`
   and routes through `service::record_event` → `store.write_event` → logdbd
   stream `task_id`.
2. **tools-bank is the sole holder of the complete tool record**: it receives
   the MCP `tools/call` (name, args) and returns the MCP response (output), and
   it already has `task_id` (from `X-Fixus-Session-Id`) and builds the
   `idempotency_key`. **fixlet sees tool *calls* but never tool *results*** —
   results flow over the MCP path (tools-bank → sandbox) out-of-band of
   fixlet's ACP stdio. ⇒ **tools-bank owns tool events; fixlet owns LLM events.**

## Design

### Change 1 — tools-bank emits a tool-step pair

Today `tools-bank` receives `tools/call`, dispatches via `tool-invoke-{region}`
to sandbox, awaits `tool-result-{region}`, and returns the MCP response — at
which point it holds the complete record.

After each call completes (success **or** error), tools-bank produces **two**
records to `task-end`, sharing one `step_id` (UUID v7, minted per call):

| Record (`event_type`) | Payload |
|---|---|
| `tool_invoked` | `task_id`, `turn_id`, `step_id`, `tool_name`, `tool_call_id`, `idempotency_key`, `input`, `local_seq` |
| `tool_completed` **or** `tool_failed` | `task_id`, `turn_id`, `step_id`, `tool_call_id`, `output`, `is_error` / `error_type`+`error_message`+`failure_reason`, `local_seq` |

Why a **pair** rather than one enriched event: `projection`'s `turn_steps()` is
derived from `completed_steps`, which is populated only by an
`invoked → terminal` transition keyed on `step_id`. A lone `tool_completed`
(no matching `tool_invoked`) leaves `completed_steps` empty. Both events are
required for the step to appear in API views as well as in the raw event log.

`turn_id` is plumbed via a new `X-Fixus-Turn-Id` MCP request header that fixlet
sets per turn. If absent (e.g. a direct MCP test call with no turn), `turn_id`
is `NULL` — permitted for step events (`models.rs`: `turn_id 可为 NULL`), though
such events may not appear in a turn-scoped SSE stream.

`local_seq`: tools-bank keeps a per-task `LocalSeqCounter` and increments it for
each event.

### Change 2 — fixlet emits an LLM-step pair (and fixes the degenerate one)

fixlet is the sole observer of LLM interaction (it drives the agent over ACP
and sees `FinalMessage{usage}`).

- On **prompt send** (after `session/new`, when `session/prompt` is dispatched
  to `claude-agent-acp`): mint a `step_id` (UUID v7), stash it on `TurnContext`,
  emit `llm_invoked` → `{task_id, turn_id, step_id, model, messages, local_seq}`.
- On **FinalMessage**: emit `llm_completed` with the **same `step_id`** (read
  back from `TurnContext`), usage tokens, `local_seq`.
- On **agent error** (the existing `turn_execution_error` paths): emit
  `llm_failed` with the same `step_id` + error fields + `local_seq`.

`local_seq` finally exercises the existing `LocalSeqCounter`
(`idempotency.rs`), whose `next()` is currently only called in tests (so
`max_local_seq` is always 0 today).

`messages`: fixlet receives the turn context (summary + messages + user input)
in the `execute_turn` payload and passes it to the agent; that is what it
includes in `llm_invoked`. (Required field per
`validate_payload_required_fields`.)

### Change 3 — fixus consumer: 5 new arms + fix `llm_completed`

In `run_lifecycle_consumer` dispatch (`orchestrator.rs:1586`), add arms that
parse the payload and call the existing typed helpers (which take `turn_id:
Option<i64>` + `step_id: &str`):

| `event_type` | Handler → `service::` helper |
|---|---|
| `tool_invoked` | `record_tool_invoked` |
| `tool_completed` | `record_tool_completed` |
| `tool_failed` | `record_tool_failed` |
| `llm_invoked` | `record_llm_invoked` |
| `llm_failed` | `record_llm_failed` |
| `llm_completed` *(modified)* | read `step_id` **from payload** instead of minting |

**Fix the degenerate `llm_completed`:** `handle_llm_completed`
(`orchestrator.rs:855`) currently allocates `step_id = Uuid::now_v7()` per call,
so its `invoked → completed` pairing never matches. Change it to read the
`step_id` that fixlet now sends, so the `llm_invoked`/`llm_completed` pair lands
under one `step_id`. Map the existing token fields (`input_tokens` /
`output_tokens` / `total_tokens`) into the `usage` object
(`prompt_tokens` / `completion_tokens` / `total_tokens`) the typed helper expects.

### What does NOT change

- **fixus-stream** — `SUBSCRIBE_EVENT_TYPES` (`fixus-stream/main.rs:101`)
  already lists all 6 step event strings; SSE carries them for free once they
  land in the `task_id` stream.
- **projection / storage / context / recovery** — already wired to consume
  step events; they become correct once producers send real `step_id`s.
- **nuntius** — zero changes; uses existing SSE + event/turn APIs.
- **EventType set, payload struct shapes, event-store schema, broker topology.**

## Data flow

**Tool path**
```
claude tool_use → claude-agent-acp → MCP tools/call → tools-bank
  tools-bank: mint step_id; dispatch tool-invoke→sandbox; await tool-result
  on completion:
    produce task-end {tool_invoked,  …, step_id, input}
    produce task-end {tool_completed|tool_failed, …, step_id, output}
  return MCP response → claude
task-end → fixus run_lifecycle_consumer → record_tool_invoked / record_tool_(in)complete
  → store.write_event → logdbd stream {task_id}
  → fixus-stream SSE → nuntius
```

**LLM path**
```
fixlet claims execute_turn → session/new + session/prompt to agent
  at prompt send:   mint step_id; produce task-end {llm_invoked, …, step_id, messages}
  at FinalMessage:  produce task-end {llm_completed, …, same step_id, usage}
  on agent error:   produce task-end {llm_failed, …, same step_id, error}
task-end → fixus run_lifecycle_consumer → record_llm_invoked / record_llm_(in)complete
  → store.write_event → logdbd stream {task_id}
  → fixus-stream SSE → nuntius
```

## Pairing, ordering, safety

- Broker assigns monotonic `seq` per `task_id` stream ⇒ append order preserved;
  `step_id` pairs `invoked ↔ terminal` inside `projection`.
- Storage's existing terminal-uniqueness check enforces ≤1
  `{tool_completed, tool_failed}` per `step_id` (and ≤1 `{llm_completed,
  llm_failed}`) — remains in force.
- Required-payload validation (`models.rs`) is satisfied: producers include
  `model`/`messages`/`local_seq` (LLM) and `tool_name`/`tool_call_id`/
  `idempotency_key`/`local_seq` (tool).
- **Crash mid-step** leaves a dangling `invoked` (surfaces in
  `incomplete_steps`). On redo, a new `step_id` is minted; the stale one
  remains incomplete. LLM steps are effectively idempotent; tool steps are
  already covered by `recovery`'s existing non-idempotent-dangling-write logic
  once both halves emit. Acceptable; noted as a known limitation.

## Testing

- **Unit — fixus:** `run_lifecycle_consumer` dispatch — given each new
  `event_type` payload, assert the correct `service::record_*` helper is called
  with the parsed `turn_id` + `step_id`; assert `llm_completed` reuses the
  payload `step_id` (regression for the degenerate bug).
- **Unit — tools-bank:** on a completed `tools/call`, assert it emits a
  `tool_invoked` + `tool_completed` pair sharing one `step_id`; on an errored
  call, `tool_invoked` + `tool_failed`.
- **Unit — fixlet:** assert `llm_invoked` and `llm_completed`/`llm_failed`
  share one `step_id` and `local_seq` advances.
- **Live E2E:** the 9-process stack + one tool-calling turn → confirm
  `tool_invoked / tool_completed / llm_invoked / llm_completed` appear in
  `GET /turns/{id}` events and in the `/turns/{id}/stream` SSE, with correct
  `step_id` pairing and `turn_steps()` populated.

## Open details (resolved during implementation planning, not design decisions)

- Exact emit point in fixlet for `llm_invoked` (the `session/prompt` dispatch
  site) and the mechanism for carrying `step_id` forward to `FinalMessage`
  (stash on `TurnContext`).
- The precise `messages` representation fixlet includes in `llm_invoked`.
- Confirm `uuid` is a dependency available to tools-bank (it already builds
  idempotency keys, so likely yes; add if not).
- `X-Fixus-Turn-Id` header read path in tools-bank alongside
  `X-Fixus-Session-Id`.
- `tool_call_id` provenance for tool events: tools-bank does not see claude's
  internal tool-use id. Use the MCP `tools/call` request id if it is a stable
  correlation id, otherwise fall back to the `idempotency_key` (which already
  uniquely identifies the call within the task). `step_id` — not `tool_call_id`
  — is what pairs the events, so this field is informational.

## Out of scope / future

- Live tool "started" signal (option 2 from brainstorming) — would need a
  shared call-id across fixlet↔tools-bank.
- Step-event emission from other backends (only the `claude-code`/ACP backend is
  in scope).
- Collapsing `llm_invoked`+`llm_completed` into a single event — rejected; the
  event model and projection both expect the pair.
