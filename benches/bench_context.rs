//! Benchmarks for the context-building hot path (events → LLM messages).
//!
//! 每个 turn,fixus 从 task 事件流重建 LLM 上下文([`fixus::context::events_to_messages`])。
//! 本 bench hermetically 测这条纯 CPU 路径 —— broker-backed append/forward 需活 logdbd,
//! 无法 hermetic bench;这两类热路径(turn 重建 + 状态投影)是 fixus 进程内 CPU 大头。
//!
//! 运行:`cargo bench -p fixus --bench bench_context`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fixus::context::events_to_messages;
use fixus::models::{AgentEvent, EventType};
use serde_json::json;

/// 构造一段真实形态的事件流:Session/Task 前缀 + N 个 turn(每个 turn =
/// TurnStarted[user] + LlmInvoked + LlmCompleted[assistant] + TurnCompleted)。
fn make_event_stream(turns: usize) -> Vec<AgentEvent> {
    let task_id = "bench-task".to_string();
    let mut events = Vec::with_capacity(turns * 4 + 4);
    events.push(AgentEvent::new(
        task_id.clone(),
        None,
        None,
        EventType::SessionStarted,
        json!({}),
    ));
    events.push(AgentEvent::new(
        task_id.clone(),
        None,
        None,
        EventType::TaskCreated,
        json!({ "agent_type": "claude" }),
    ));
    events.push(AgentEvent::new(
        task_id.clone(),
        None,
        None,
        EventType::TaskReady,
        json!({}),
    ));
    events.push(AgentEvent::new(
        task_id.clone(),
        None,
        None,
        EventType::TaskClaimed,
        json!({}),
    ));
    for t in 1..=turns {
        let tid = t as i64;
        events.push(AgentEvent::new(
            task_id.clone(),
            Some(tid),
            None,
            EventType::TurnStarted,
            json!({ "user_input": format!("turn {t}: please implement feature {t}") }),
        ));
        events.push(AgentEvent::new(
            task_id.clone(),
            Some(tid),
            Some(format!("s-{t}-llm")),
            EventType::LlmInvoked,
            json!({ "model": "claude-sonnet-5" }),
        ));
        events.push(AgentEvent::new(
            task_id.clone(),
            Some(tid),
            Some(format!("s-{t}-llm")),
            EventType::LlmCompleted,
            json!({ "content": format!("Sure — here is the implementation for feature {t} …") }),
        ));
        events.push(AgentEvent::new(
            task_id.clone(),
            Some(tid),
            None,
            EventType::TurnCompleted,
            json!({}),
        ));
    }
    events
}

fn bench_events_to_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("events_to_messages");
    group.sampling_mode(criterion::SamplingMode::Flat);
    for turns in [10_usize, 50, 200] {
        let events = make_event_stream(turns);
        group.bench_with_input(BenchmarkId::from_parameter(turns), &events, |b, ev| {
            b.iter(|| {
                let msgs = events_to_messages(black_box(ev));
                black_box(msgs);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_events_to_messages);
criterion_main!(benches);
