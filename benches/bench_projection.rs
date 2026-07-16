//! Benchmarks for the state-projection hot path (events → Task/Turn state).
//!
//! [`fixus::projection::TaskProjection::apply`] 是 broker forwarder 投影事件到状态的入口
//! (入参 = 原始 seq / 事件类型串 / JSON 内容字节 / metadata)。每个事件进系统都要过它,
//! 是 fixus 进程内另一条 CPU 热路径。本 bench hermetically 测(无需 broker)。
//!
//! 运行:`cargo bench -p fixus --bench bench_projection`

use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fixus::projection::TaskProjection;
use serde_json::json;

/// 原始事件 = (seq, event_type 字符串, JSON 内容字节, metadata)。
type RawEvent = (u64, String, Vec<u8>, HashMap<String, String>);

fn push(
    out: &mut Vec<RawEvent>,
    seq: &mut u64,
    et: &str,
    content: serde_json::Value,
    meta: HashMap<String, String>,
) {
    *seq += 1;
    out.push((
        *seq,
        et.to_string(),
        serde_json::to_vec(&content).unwrap(),
        meta,
    ));
}

fn meta(turn_id: Option<i64>, step_id: Option<&str>) -> HashMap<String, String> {
    let mut m = HashMap::new();
    if let Some(t) = turn_id {
        m.insert("turn_id".to_string(), t.to_string());
    }
    if let Some(s) = step_id {
        m.insert("step_id".to_string(), s.to_string());
    }
    m
}

/// 与 bench_context 同形的事件流,但序列化为 apply() 接受的原始形态。
fn make_raw_stream(turns: usize) -> Vec<RawEvent> {
    let mut out: Vec<RawEvent> = Vec::with_capacity(turns * 4 + 4);
    let mut seq = 0u64;
    push(&mut out, &mut seq, "session_started", json!({}), HashMap::new());
    push(
        &mut out,
        &mut seq,
        "task_created",
        json!({ "agent_type": "claude" }),
        HashMap::new(),
    );
    push(&mut out, &mut seq, "task_ready", json!({}), HashMap::new());
    push(&mut out, &mut seq, "task_claimed", json!({}), HashMap::new());
    for t in 1..=turns {
        let tid = t as i64;
        let step = format!("s-{t}-llm");
        push(
            &mut out,
            &mut seq,
            "turn_started",
            json!({ "user_input": format!("turn {t}") }),
            meta(Some(tid), None),
        );
        push(
            &mut out,
            &mut seq,
            "llm_invoked",
            json!({ "model": "claude-sonnet-5" }),
            meta(Some(tid), Some(&step)),
        );
        push(
            &mut out,
            &mut seq,
            "llm_completed",
            json!({ "content": format!("reply {t}") }),
            meta(Some(tid), Some(&step)),
        );
        push(
            &mut out,
            &mut seq,
            "turn_completed",
            json!({}),
            meta(Some(tid), None),
        );
    }
    out
}

fn bench_projection_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("projection_apply");
    group.sampling_mode(criterion::SamplingMode::Flat);
    for turns in [10_usize, 50, 200] {
        let stream = make_raw_stream(turns);
        group.bench_with_input(BenchmarkId::from_parameter(turns), &stream, |b, stream| {
            b.iter(|| {
                let mut p = TaskProjection::new("bench-task");
                for (seq, et, content, meta) in black_box(stream) {
                    let _ = p.apply(*seq, et, content, meta);
                }
                black_box(p);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_projection_apply);
criterion_main!(benches);
