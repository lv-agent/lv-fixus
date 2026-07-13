# CR-5:session/work_dir 自动 GC

> 日期:2026-07-13
> 来源:[`veps/TODO.md`](../../veps/TODO.md) CR-5(session/work_dir 自动 GC)
> 范围:`src/bin/sandbox-server/`(独立二进制,不走 fixus lib)
> 纪律:**先 CR → 再 TDD 写测试 → 最后实施**(本文是第一步)

---

## 1. 问题(代码取证后的真实状态)

### 1.1 SessionManager 只增不删,`/tmp/sandbox-sessions/` 无限膨胀

取证(`src/bin/sandbox-server/session.rs` 全文 48 行):

- `SessionManager` 持 `HashMap<String, SessionState>`,`SessionState { work_dir: PathBuf }` —— **无时间戳**。
- `get_or_create(session_id)`(`:21`):每次 tool invoke 都可能新建 `base_dir/{session_id}`(per-task 隔离,见 commit `e9a3de8`),插入 map。**永不删除**。
- `cleanup(session_id)`(`:33`):存在但**从不被自动调用**(`grep cleanup` 于 main.rs 全空),只做单 session 删除。
- `main.rs`(`:81` 起 session_mgr;`:201` get_or_create):consumer loop 无限创建,无任何回收。

⇒ 长跑 sandbox-server:`/tmp/sandbox-sessions/{task_id}` 目录数随 task 数线性增长,**靠手动清**(TODO 原文)。

### 1.2 两类垃圾

- **活跃 map 内的过期 session**:task 早已结束,但 work_dir 仍被 map 持有。
- **盘上 orphan 目录**:进程崩溃/重启后,map 重建为空,但旧 `base_dir/{task_id}` 留在盘上(map 不知情)。

两类都得治。

### 1.3 multica 参照

multica `internal/daemon/gc.go` 定期回收旧 work_dir(mtime)。fixus 抄其**定期 sweep + mtime 判旧**语义,实现走 Rust std(`Instant` + `fs::read_dir` + `fs::modified`)。

---

## 2. 目标 / 非目标

### 目标

- **G1 自动回收**:sandbox-server 后台定期 sweep,**无需手动清 `/tmp/sandbox-sessions/`**。
- **G2 三重淘汰策略**:① idle 超时(last_accessed > max_idle);② 容量上限超限 LRU(max_sessions);③ 盘上 orphan(mtime > max_idle 且不在活跃 map)。
- **G3 可配置 + 可观测**:CLI 调参;sweep 日志报告淘汰数(便于确认 GC 在工作)。
- **G4 TDD**:sweep 纯逻辑单测(无 sleep —— 用 `Instant` 回溯 + `File::set_modified` 回溯 mtime)。

### 非目标(显式排除)

- **N1 不做跨进程/跨主机协调**:单 sandbox-server 进程自扫自;多副本各自 GC 自己的 base_dir(若共享目录需外部协调,留后续)。
- **N2 不做优雅驱逐**:直接 `remove_dir_all`;不通知正在用该 work_dir 的 in-flight 工具(工具并发由 main.rs semaphore=4 保证,GC 间隔远大于工具时长,实际不冲突;若需严格,加 in-flight 集合排除,留后续)。
- **N3 不动 per-task 隔离语义**:GC 只删整目录,不改 `get_or_create` 的 `base_dir/{session_id}` 布局。
- **N4 不做配额(磁盘字节上限)**:只按目录数(idle + count cap);按字节配额需 du 扫描,留后续。

---

## 3. 设计

### 3.1 `session.rs` 数据模型 + sweep

```rust
use std::time::{Duration, Instant};

struct SessionState {
    work_dir: PathBuf,
    last_accessed: Instant,   // ← 新增:get_or_create 时更新为 now
}

/// GC 策略。
#[derive(Clone, Debug)]
pub struct GcPolicy {
    /// session 最近访问超过此时长 → 淘汰(map 内)。
    pub max_idle: Duration,
    /// 活跃 session 数上限,超限按 LRU(最久未访问)淘汰。
    pub max_sessions: usize,
}

/// sweep 结果计数(日志/观测)。
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub evicted_sessions: usize,   // 从 map 淘汰(并删 dir)
    pub removed_orphans: usize,    // 盘上 orphan dir(不在 map)
}

impl SessionManager {
    pub fn get_or_create(&self, session_id: &str) -> PathBuf {
        // 命中 → 更新 last_accessed = Instant::now();未命中 → 建目录 + 插入。
    }

    /// 执行一次 GC。返回淘汰计数。
    pub fn sweep(&self, policy: &GcPolicy) -> SweepReport {
        // 1. idle 淘汰:map 内 last_accessed 早于 (now - max_idle) 的,删 map + remove_dir_all。
        // 2. LRU 超容淘汰:若仍 > max_sessions,按 last_accessed 最旧者淘汰至 == max_sessions。
        // 3. orphan 扫盘:read_dir(base_dir),子目录名 ∉ 活跃 map 且 mtime > max_idle → remove_dir_all。
    }

    #[cfg(test)]
    fn backdate_last_access(&self, session_id: &str, age: Duration) {
        // 测试专用:把 last_accessed 设为 now - age(Instant 回溯,无需 sleep)。
    }
}
```

**关键不变量**:
- 活跃 map 内的 session,其 work_dir 在第 1/2 步被删后,第 3 步 orphan 扫描用「活跃 map 名集合」排除,**不会重复删/误删活跃 session 的目录**。
- orphan 只删**子目录**(work_dir),不动 base_dir 本身、不动文件(防御)。

### 3.2 `main.rs` 后台 sweep 任务 + CLI

CLI 加 3 个参数(env-friendly,默认值见下):

```rust
#[arg(long, default_value = "300")]
gc_interval_secs: u64,      // sweep 周期,默认 5 分钟

#[arg(long, default_value = "3600")]
gc_max_idle_secs: u64,      // idle 阈值,默认 1 小时

#[arg(long, default_value = "100")]
gc_max_sessions: usize,     // 活跃上限,默认 100
```

main 里 `session_mgr` 创建后,spawn 后台任务:

```rust
let policy = session::GcPolicy {
    max_idle: Duration::from_secs(cli.gc_max_idle_secs),
    max_sessions: cli.gc_max_sessions,
};
let mgr = session_mgr.clone();
tokio::spawn(async move {
    loop {
        tokio::time::sleep(Duration::from_secs(cli.gc_interval_secs)).await;
        let report = mgr.sweep(&policy);   // sync,brief(锁 + 少量 fs)
        if report.evicted_sessions > 0 || report.removed_orphans > 0 {
            tracing::info!(
                "session gc: evicted {} sessions, removed {} orphans",
                report.evicted_sessions, report.removed_orphans
            );
        }
    }
});
```

sweep 是 sync(std Mutex + fs),耗时极短(目录数有限);inline 调用即可,无需 spawn_blocking。

---

## 4. TDD 测试清单(先写,跑红)

### 4.1 `session.rs` 单测(用 tempfile 隔离目录)

- [ ] **`sweep_evicts_idle_sessions`**:建 2 session,A 回溯 last_accessed 1h 前、B 保持新;sweep(max_idle=60s)→ A 被删(map + dir)、B 保留。
- [ ] **`sweep_lru_when_over_capacity`**:建 3 session(都新),回溯最早一个;sweep(max_sessions=2, max_idle=很大)→ 最早那个被删,剩 2。
- [ ] **`sweep_keeps_active_session_dir`**:建 1 session;sweep(max_idle=很大)→ 其 work_dir 仍在盘上(不被当 orphan)。
- [ ] **`sweep_removes_disk_orphans`**:在 base_dir 建一个**不在 map** 的子目录,`File::set_modified` 回溯 mtime 1h 前;sweep(max_idle=60s)→ orphan dir 被删。
- [ ] **`sweep_report_counts`**:组合场景,断言 `SweepReport` 计数正确(evicted + orphan 分项)。
- [ ] **`get_or_create_updates_last_access`**:两次 get_or_create 同一 session,第二次后 last_accessed 更新(回溯验证:第二次后再回溯小的 age 不触发淘汰)。

> 不用 `tokio::time::sleep`:idle 用 `backdate_last_access`(Instant 回溯);orphan 用 `File::set_modified`(mtime 回溯)。确定性、快。

---

## 5. 实施步骤

- [ ] **CR-5a**:`session.rs` 加 `last_accessed` 字段 + `GcPolicy` + `SweepReport` + `sweep()` + `backdate_last_access(test)`;先写 §4.1 测试(跑红:类型/方法缺或 stub)→ 填实现(跑绿)。`get_or_create` 改为更新 `last_accessed`。
- [ ] **CR-5b**:`main.rs` CLI 加 3 个 `gc_*` 参数;spawn 后台 sweep task(§3.2);`cargo build` 验证。
- [ ] 全量 `cargo build --release` + 确认 sandbox-server 二进制可起;勾掉 TODO CR-5。

---

## 6. 证据附录

### 6.1 测试(全绿)

`cargo test --bin sandbox-server session::` —— 6/6:

| 测试 | 验证 |
|------|------|
| `sweep_evicts_idle_sessions` | idle(last_accessed 回溯 1h)> max_idle(60s) → 删 map + dir;新 session 保留 |
| `sweep_lru_when_over_capacity` | max_sessions=2、3 session → 最旧(LRU)淘汰 |
| `sweep_keeps_active_session_dir` | 活跃 session 的目录不被当 orphan 误删 |
| `sweep_removes_disk_orphans` | 不在 map 的 orphan dir(mtime 回溯)被删 |
| `sweep_report_counts` | evicted/orphan 计数分项正确 |
| `get_or_create_refreshes_last_access` | 命中刷新 last_accessed,不被误判 idle |

全 sandbox-server 测试:**20 passed, 0 failed**(基线 14 → +6)。

### 6.2 构建

`cargo build --release` 成功(54s)。

### 6.3 CLI(默认值)

```
--gc-interval-secs 300   # sweep 周期 5 分钟
--gc-max-idle-secs 3600  # idle 阈值 1 小时
--gc-max-sessions 100    # 活跃上限
```

后台 task 启动日志:`session gc task starting: interval=300s max_idle=3600s max_sessions=100`;有淘汰才记 `session gc: evicted N sessions, removed M orphans`。

---

## 7. 风险与权衡

- **R1 GC 删到正在用的 work_dir**:sandbox 工具并发上限 semaphore=4,GC 周期默认 300s 远大于工具时长;且 idle 阈值默认 1h,正在跑的 task 持续 get_or_create 会刷新 last_accessed,不会被判 idle。实际安全;严格隔离(in-flight 集合)留后续(N2)。
- **R2 orphan 扫盘误删**:只删 base_dir 下的**子目录**且 mtime 老 + 不在活跃 map;不动文件、不动 base_dir。命名碰撞(task_id 是 UUIDv7)概率为零。
- **R3 `File::set_modified` 平台差异**:Linux 支持(1.96 toolchain,API 稳定 1.75+);测试依赖它,CI 需 Linux(本项目本就 Linux-only:landlock)。
- **R4 sweep 持锁时长**:std Mutex 持有期间做 fs read_dir + remove_dir_all;目录数有限(≤ max_sessions + 少量 orphan),耗时 ms 级,不阻塞 get_or_create(get_or_create 是热路径但有 semaphore 限流)。可接受。
