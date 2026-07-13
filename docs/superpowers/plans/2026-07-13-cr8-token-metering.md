# CR-8:token 用量计量(草稿 —— 被架构阻塞,待决策)

> 日期:2026-07-13
> 来源:[`veps/TODO.md`](../../veps/TODO.md) CR-8(token 用量计量,Tier 3,**L**)
> 纪律:**先 CR → 再 TDD 写测试 → 最后实施**

---

## 1. recon 后的真实状态

### 1.1 per-task 维度**已存在**

- `EventStore::get_token_usage_stats(task_id)`(`storage.rs`):从 `llm_completed` 事件按 model 客户端聚合,返回 `Vec<TokenUsageStats{model, call_count, prompt_tokens, completion_tokens}>`。
- HTTP `GET /api/v1/sessions/{task_id}/token-usage`(`server.rs`)已暴露。
- `handle_llm_completed`(`orchestrator.rs:670`):lifecycle consumer 路由 fixlet 的 `llm_completed` → 写 `llm_completed` 事件(usage.{prompt,completion,total}_tokens)。**生产已通**。

⇒ "某 task 用了多少 token" 已可查(虽缺 total/cache 分项)。

### 1.2 cross-task 计费聚合**被 logdbd 架构阻塞**

multica:`runtime_usage`(runtime/日/model)+ `task_usage_hourly`(小时桶)+ backfill —— 全是**跨 task 聚合 + 物化持久化**。

fixus 阻塞点:
- logdbd **per-stream**(stream=task_id),**无跨 task 扫描 / 全局 task registry**(`EventStore` 所有方法都要 task_id;`grep list_tasks/scan` 全空)。
- 故"全部 task 的 token 总量 / 按 tenant / 按 task_type 聚合"**无法计算**(没有 task_id 列表可遍历)。
- 现有聚合是**on-the-fly 单 task 重放**(get_token_usage_stats),不能 scale 到跨 task。

⇒ 落库计量(billing 级)需要 fixus **目前没有**的物化存储层。

### 1.3 cache token 链路缺失(次要)

fixlet ACP 拿到 `cached_read_tokens`/`cached_write_tokens`(`acp.rs:226`)但 `router.rs:561` 只发 input/output/total ⇒ fixus 收不到 cache 分项。补全需 fixlet+fixus 两端协同改。

---

## 2. 目标 / 非目标(待决策锁定)

### 目标(本 pass 可达成,不依赖新存储)

- **G1 per-task 数据补全**:`TokenUsageStats` 加 `total_tokens`;`get_token_usage_stats` 解析之;`/token-usage` 响应加 per-task total rollup。
- **G2 cross-task 实时可观测(非持久化)**:借 CR-4 metrics 加 `fixus_token_{input,output,total}_tokens_total{task_type,model}` Counter,在 `handle_llm_completed` 累加。给运维/成本**实时**视角(reset on restart,非 billing)。

### 非目标(阻塞 / 待决策)

- **N1 cross-task 物化持久化(billing 级)**:被 logdbd per-stream 阻塞,需选物化存储(见 §3)。
- **N2 cache token 全链路**:fixlet 转发 + fixus 解析 + 落库,跨二进制,留后续。
- **N3 hourly 桶 / backfill**:依赖 N1 的物化层。

---

## 3. 阻塞点:cross-task 物化存储(架构决策)

fixus 要 billing 级跨 task 计量,需一个**能跨 task 聚合的物化存储**。选项:

| 方案 | 说明 | 代价 |
|------|------|------|
| **A. PostgreSQL 表** | `task_usage(task_id, task_type, model, tokens...)` + `task_usage_hourly`。fixus 在 llm_completed 时 upsert。跨 task = SQL 聚合。 | 引 PG 依赖(与 P3「PostgreSQL 支持」重合);fixus 部署拓扑变。 |
| **B. Redis 物化** | fixus 在 llm_completed 时 `HINCRBY usage:{task_type}:{model}` 等。跨 task = Redis 聚合。 | 已依赖 Redis(流式);但 Redis 非持久 billing 源(reset/AOF 仍非权威)。 |
| **C. logdbd catalog stream** | 若 logdbd 支持元数据流列举 task_id,可周期遍历 + 现有 get_token_usage_stats 聚合(无需新存储)。 | 依赖 logdbd 能力(待查);N task × 重放 = 慢,只宜周期批跑。 |
| **D. 推迟到 P3** | 等 fixus 迁 PostgreSQL(P3 路线),CR-8 随之落地。 | 计量延后;G1/G2 仍可先做。 |

**本 pass 不做 N1**(需用户拍板方案 A/B/C/D)。先做 G1/G2(不依赖决策),N1 待决。

---

## 4. TDD(本 pass G1/G2)

### 4.1 G1 per-task 补全

- [ ] `TokenUsageStats` 加 `total_tokens`;`get_token_usage_stats` 从 `usage.total_tokens` 解析。
- [ ] `/token-usage` 响应加 `total: { prompt, completion, total }` rollup。

### 4.2 G2 token metrics

- [ ] `metrics.rs` 加 `fixus_token_{input,output,total}_tokens_total{task_type,model}` + `record_llm_tokens(tt, model, in, out, total)`。
- [ ] `handle_llm_completed` 调 `record_llm_tokens`。
- [ ] catalog 测试 + 集成(cr8:发 llm_completed → render 含 token 计数)。

---

## 5. 实施步骤(G1/G2;N1 待决策)

- [ ] **CR-8a(G1)**:`TokenUsageStats.total_tokens` + 查询解析 + API rollup。§4.1 测试。
- [ ] **CR-8b(G2)**:token metrics + handle_llm_completed 打点。§4.2 测试 + perf(可选)。
- [ ] 全量验证 + 勾 TODO CR-8(标 G1/G2 落地、N1 待决)。

---

## 6. 证据附录

### 6.1 落地范围(决策:G1+G2 先做,N1 待 P3)

- **G1 ✅**:`TokenUsageStats.total_tokens`(serde default 向后兼容)+ `TokenUsageTotals` + `TokenUsageResponse{by_model, totals}`(from_by_model rollup);projection 与 LogdbdEventStore 均解析 `usage.total_tokens`;`/token-usage` 返回 rollup。
- **G2 ✅**:`fixus_token_{input,output,total}_tokens_total{task_type,model}` Counter + `record_llm_tokens`;`handle_llm_completed` 打点(cross-task 实时观测)。
- **N1 ⏸ P3**:cross-task 物化 billing 被 logdbd per-stream 阻塞,推迟到 PostgreSQL P3。

### 6.2 测试(全绿)

- `token_usage_tests`(G1):3/3 —— rollup 跨 model 求和 / 空 rollup / serde total + 旧数据 default 兼容。
- `cr8_llm_completed_records_token_metrics`(G2):1/1 —— handle_llm_completed → render 含 input/output/total 计数。

全量 lib(跳过 broker_store):**87 passed, 0 failed**, 7 ignored(基线 83 → +4)。

### 6.3 构建

`cargo build --release` 成功(69s)。

### 6.4 cache token 链路(仍未通,次要 TODO)

fixlet `router.rs` 只发 input/output/total(ACP 有 cached_read/write 但 drop)。补 cache 分项需 fixlet+fixus 协同;留后续。

---

## 7. 风险

- **R1 G2 metrics 非权威 billing**:Counter 进程内、reset on restart;只供实时观测,不能当计费账本。billing 需 N1(物化)。
- **R2 N1 拖到 P3**:计量能力延后;但 G1/G2 先把数据补全 + 实时视角立起来,N1 落地时直接灌。
- **R3 fixlet token 字段名**:fixlet 发 `input_tokens`/`output_tokens`,fixus 存 `prompt_tokens`/`completion_tokens`(handle_llm_completed 映射)。G2 metrics 用 fixus 侧语义。
