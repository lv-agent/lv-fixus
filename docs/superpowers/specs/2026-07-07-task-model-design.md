# Task 模型设计:Task 作为顶层实体

> 日期:2026-07-07
> 范围:跨 fixus + nuntius 两仓。Task 从「会话的附属品」提升为横跨两个世界(交互引擎造它、自治引擎消费它)的一等实体。
> 状态:设计已与 stakeholder 逐点确认,待 spec review。

---

## 1. 背景与动机

### 1.1 现状:Task 三段式

Task 目前**没有一等公民地位**,它被拆散成三处临时状态的拼凑:

- **定义** — `nuntius/backend/schemas/*.json`(4 个:`database.repair` / `config.change` / `deploy.service` / `log.analysis`)。每个 schema 含 `task_type`、`agent_types[]`、`required/optional_fields[]`、`auth`、`risk_level`。启动时 `Registry::load` 全读进内存。
- **创建** — `clarify.rs` + `POST /chat`:每轮 `detect_intent`(LLM 选 task_type)→ `extract_fields`(LLM 抽字段)→ `check_readiness`(静态遍历 required_fields 查缺)→ 齐了提示提交。Contract = 抽出的 fields JSON。
- **处理** — `POST /tasks`:校验 session → 生成 fixus_session_id(UUID v7)→ `fixus.create_session(agent_type=task_type, contract)` → `start_turn(contract_json)` → fixus 按 session.agent_type 路由到 fixlet → fixlet spawn claude,contract JSON 当 prompt。

### 1.2 现状的张力

1. **Task 没有自己的实体/状态机** — 它散落在 schema 文件 + 临时 fields JSON + fixus session 里。`list_tasks` 是从会话消息翻 `task_ref` 再反查 fixus 拼出来的;nuntius 侧 `task_ref.status` 永远写死 `"executing"`,从不更新。
2. **`schema.agent_types[]` 是死字段** — `submit_task` 直接拿 `task_type` 当 `agent_type` 传给 fixus(`database.repair`),从不读 schema 声明的 `["db-repair"]`。
3. **Contract → Agent 指令是隐式的** — fields JSON 直接 `to_string()` 塞进 turn 当 prompt,Agent 收到一坨 JSON 自己猜。
4. **readiness 是静态的** — `check_readiness` 只遍历 required_fields 查缺,判不了"描述够不够执行"(语义完备性),且每轮不累积字段(F1)。
5. **fixus 的 session 是事实上的 Task,却没正名** — Session 是 fixus 唯一有独立存储的实体,顶了"任务容器"的缺,但缺分发状态(created/ready/claimed/...)。

### 1.3 设计目标

把 Task 提升为一等实体,有完整的:生命周期状态机、结构化契约、Contract→Agent 指令编译层、可选产物验收。保持"一 Task = 一 Agent 一次执行"的扁平模型(不做 Task 编排/父子/多 Agent 协作 —— YAGNI)。

---

## 2. 核心决策

### 2.1 Task 是顶层实体,fixus session 退场

经核实,fixus 的 `Session` 是"唯一有独立存储的实体"(`models.rs:325`),承担:agent_type 载体(路由)、turn 命名空间、context 重建边界、多租户、塞 contract。这五条职责**全能上移到 Task**。

**决策:砍掉 fixus session 作为顶层,Task 取而代之。** turn 直接挂 Task 下。原 `session_id` 语义降级为 Task 的一个**溯源属性**(标记 Task 从哪个 nuntius chat session 下发),且只是溯源信息的一部分。

"保留 session"的唯一理由是 Task 1:N(重试开新 session),但这场景被 fixus 已有的 **turn 级崩溃恢复(redo_group)** 覆盖;真要"整个任务从头来"语义上是新 Task。故 1:N 价值不大,不保留 session 层。

### 2.2 head / body 分离(envelope/payload)

Task 实体内部分两层,fixus 只认 head:

| | head(fixus 拥有·理解·可索引) | body(nuntius 拥有·理解;fixus opaque 透传) |
|--|--|--|
| 性质 | 结构化骨架,fixus 能索引/路由/驱动状态机 | 业务语义,fixus 当不透明 blob 存 + 透传 |
| 内容 | task_id / task_type / state / provenance | contract / schema_ref / task_brief / judgments |

fixus 的"通用性"体现在:它对**任何 Task** 跑同一套机制(事件存储、崩溃恢复、token 流、工具沙箱、按 task_type 路由),head 是它的通用 schema,body 是黑盒。fixus 名副其实地"处理 Task"却不被业务污染。

### 2.3 执行调度是 pull-based(认领式),不是中心命令式

是否执行由**执行器自己决定**(可拿走不执行)。fixus/nuntius 都不能命令执行器开始 —— Task 进 `ready` 后,合资格的执行器主动 `claim`。`claimed` 是 fixus 可自动感知的状态(执行器经 WS 来 claim)。

---

## 3. Task 数据模型

```
Task
├─ head  (fixus 拥有·理解·可索引)
│  ├─ task_id            全局唯一,由 fixus 在 create_task 时分配
│  ├─ task_type          = agent_type,路由键
│  ├─ state              见 §4 状态机
│  └─ provenance         溯源:Task 从哪下发
│      ├─ source_channel     nuntius-chat / api / schedule / derived
│      ├─ source_session_id  nuntius chat session(下发对话)
│      ├─ source_user_id
│      ├─ source_tenant_id
│      ├─ source_message_id  触发提交的那条对话消息/澄清轮(精确溯源)
│      ├─ created_at
│      └─ created_by          = "nuntius"(下发系统标识)
│
└─ body  (nuntius 拥有·理解;fixus opaque 透传)
   ├─ contract              字段值(澄清产物,JSON)
   ├─ schema_ref            task_type → schema(隐式引用)
   ├─ task_brief            编译产物(§6),created→ready 时生成,之后不变
   └─ acceptance_result     acceptance 验收结果(§7,仅 done_criteria≠none 时有)
```

**body 的可变性**:created 态期间 body 可变(nuntius 随澄清更新 contract,fixus opaque 存储);`created→ready` 时 body 冻结、task_brief 编译完成,之后不变。

**readiness 判定不入 body** — readiness 是 nuntius 侧的 LLM 判定(它有 schema + 对话语境),其"通过"结论体现为 `task_ready` 事件(head),过程留在 nuntius chat 会话;只有 acceptance 的结果存进 body。

**provenance 设计要点**:source_channel 预留多渠道(nuntius-chat 现用;api/schedule/derived 未来);source_message_id 保留是因为"折腾自治引擎核心就是为了溯源"。provenance 是结构化元数据,归 head(fixus 可存可索引,如"查某 user 的全部 Task")。

**head 存储方式**:独立存储(非纯事件投影),与现状 Session 一致 —— 查询当前状态不用回放全部事件。这是 event-sourcing 纯粹性与查询实用性的折中,保持现状。

---

## 4. 状态机

```
created ─[nuntius: readiness 通过]─▶ ready
ready ─[executor claim]─▶ claimed ─[开跑]─▶ executing
executing ─[需人工]─▶ blocked ─[nuntius: 人工确认]─▶ ready(重走 claim)
executing ─[executor 报告]─▶ succeeded | failed
任意活态 ─[用户放弃/取消]─▶ canceled(终态;nuntius 触发,清理澄清中途放弃的 Task)
```

### 4.1 触发权(关键:谁迁移状态)

| 迁移 | 谁触发 | 性质 |
|------|--------|------|
| `created → ready` | **nuntius**(readiness 判定通过) | 语义 gate,body 判定,结论写成 head 事件 |
| `ready → claimed` | 执行器(fixus 自动感知 WS claim) | 运行时事实 |
| `claimed → executing` | 执行器(fixus 自动) | 运行时事实 |
| `executing → blocked` | 执行器(fixus 自动,执行器请求人工) | 运行时事实 |
| `blocked → ready` | **nuntius**(人工确认) | 语义 gate |
| `executing → succeeded/failed` | 执行器(fixus 自动,turn 终态) | 运行时事实 |

**规律**:head 状态机里,nuntius 只插手**两处语义 gate**(`created→ready`、`blocked→ready`),其余全是执行器驱动、fixus 自动迁移。fixus 永远不主动做语义迁移,只接受 nuntius 写入的事件。

### 4.2 blocked 的层次

fixus 现有 `TurnBlocked` 事件(`models.rs:35`,turn 级,非幂等工具悬空)。Task 级 `blocked` 是它的上层泛化:TurnBlocked 是 TaskBlocked 的**一个触发源**(工具悬空),Task 级 blocked 还有别的成因(Agent 主动请求授权、遇到歧义)。两者并存,TurnBlocked 可触发 TaskBlocked。

### 4.3 blocked → ready 的 claim 优先级

人工确认后 Task 回 ready 重新被认领:
- **优先原 claimant**(上下文连续):fixus 路由时带 hint `preferred_claimant=原执行器`,先 offer 给它。
- **允许其他 agent 认领**(liveness):原执行器超时/拒绝,开放给同 task_type 的其他执行器。换执行器时新执行器从 fixus 事件流重建上下文(`context.rs` 的 events→messages,fixus 本就能做)。

### 4.4 状态 vs 事件流(head 的两层)

| | head 状态(state) | head 事件流(events) |
|--|--|--|
| 粒度 | 粗:分发态 | 细:执行轨迹 |
| 内容 | created/ready/claimed/executing/blocked/succeeded/failed | Task 级迁移事件 + 运行事件(LlmInvoked/ToolInvoked/...) + token |
| 时刻 | 任一时刻一个 | 不可变、追加、时序 |
| 关系 | **是事件流的投影** | 事实本体 |

这正是 event sourcing:状态是事件的投影,事件是不可变事实。nuntius 呈现也分两层:Task 卡片(粗态)读当前 state;执行轨迹(细)订阅事件流。

**运行状态不重新定义** — fixus 现有 15 种 `EventType` 已全覆盖:`llm-call=LlmInvoked`、`llm-call-end=LlmCompleted`、`tool-call=ToolInvoked`、`tool-call-end=ToolCompleted`,还多出 `LlmFailed`/`ToolFailed`/`TurnPending`/`TurnCanceled`/`TurnBlocked`。nuntius 订阅事件流投影成 UI 即可。

---

## 5. readiness gate(created → ready)

完备性判定有三层,顺序执行:

```
Task 在 created 态,每轮 chat:
  ① intent 匹配 task_type                    (LLM)
  ② extract 字段 → 累积进 contract           (LLM,看全部上下文 → 治 F1)
  ③ 静态硬检查                                (代码,不调 LLM)
       required_fields 都填且非空?
       enum 字段值在 options 取值范围内?
       不过 → 追问缺的/越界字段(文案确定,schema 驱动)
  ④ LLM 动态语义判定                          (③ 过了才跑)
       [schema + 累积对话 + contract] → 够不够执行?
       不够 → 追问("描述到什么程度才能执行"的软判定)
  ⑤ 人确认 gate                              (④ 过了才到)
       requires_approval = true  → 等用户显式确认 → ready
       requires_approval = false → 直接 ready(自动)
```

- **③ 是硬骨架**("人不填这些,任务执行不下去"):required_fields 必填 + enum 取值范围。代码判,省 LLM。
- **④ 是软语义**("描述够不够执行"):LLM 判,因为完备性完全动态、每个实例需要的东西不同。累积全部上下文(治 F1)。
- **⑤ 是人确认**:不新增状态。③④ 过了,Task 仍停在 `created`,nuntius 内部知道"待人确认",UI 显示"信息已齐,确认提交?"。用户确认即置 `ready`。

### 5.1 schema 简化

- **砍掉 `risk_level`** — 它的唯一用途是"要不要人确认",而 `auth.requires_approval` 已表达此意,纯冗余。
- **"是否人确认"直接用现有 `auth.requires_approval`** — 不加新字段。硬规则:高风险(requires_approval=true)必须人确认;若不想让人确认,就别标 requires_approval。

---

## 6. Contract 编译(contract → task_brief)

### 6.1 问题

现状 contract JSON 直接塞 prompt,Agent 收到裸 JSON 自己猜。schema 定义了字段,但没定义字段怎么变成指令。

### 6.2 编译 = schema 模板 + contract 插值 → task_brief

编译产物 **task_brief(任务简报)** 是 body 的一部分,created→ready 时生成,fixus opaque 透传给执行器当初始输入。

**编译方式:静态模板插值,不靠 LLM**(与 readiness 靠 LLM 相反)。理由:
- 确定性、可审计(同 contract 永远出同一份简报)。
- 省一次 LLM 调用。
- claude 本身是 LLM,拿到结构化简报会自己理解、叙述化。

readiness 要 LLM 是因为"够不够执行"无确定答案;编译不需要 LLM,因为 schema 模板是确定的。

### 6.3 分工边界(简报不越界)

| | 谁定义 | 内容 |
|--|--|--|
| task_brief | nuntius,从 schema 编译 | 目标 / 参数 / 约束 / 期望产出 |
| 执行能力 | 自治引擎(fixlet 的 claude 自带) | 工具 / MCP / system prompt / 流程 |

**简报是"做什么",claude 能力是"能做什么"**,两者正交。task_type 决定路由到哪个 fixlet(哪种 claude 能力),task_brief 是该 Task 的具体输入。**简报不写执行步骤**("第一步 kill、第二步 rebuild"),步骤让 Agent 自主规划 —— 执行知识在自治引擎侧,塞进 nuntius 越界。

示例(database.repair):
```
目标:对 {{target}} 执行 {{scope}} 修复
故障现象:{{symptom}}
约束:用户{{已/未}}接受在线影响({{online_impact_accepted}})
完成标准:{{target}} 恢复正常,死锁消除/索引重建完成
```

### 6.4 schema 新增字段

```
schema += {
  instructions:   "模板字符串,带 {{field}} 占位符",  // 编译 task_brief
  done_criteria:  "完成标准描述",                     // 进简报(给 Agent)+ 给 acceptance verifier
  allowed_tools:  ["..."]?,                           // 可选,工具白名单;不写=claude 默认全部
  acceptance:     "none | auto | human"               // 验收形态(§7),默认 none
}
```

---

## 7. acceptance(可选验收)

succeeded 是执行事实(Agent 报告跑完),**不蕴含"达成 Task 意图"**。acceptance 是 body 的可选验收层,**不进 head 状态**(fixus head 到 succeeded/failed 终结)。

- 形态由 schema 声明(复用/新增字段):
  - `none`(默认)— succeeded 即终态。fire-and-forget 类 Task(分析日志给报告)。
  - `auto` — nuntius 用 LLM 对照原始意图 + fixus 执行事件流自动复核(独立于 Agent 自检,catch Agent 自以为完成但没达标)。
  - `human` — 人工 review gate(有副作用的 Task:改线上配置、kill 进程)。
- 结果(accepted/rejected)存 nuntius body,不写回 fixus head。
- 验收不过要重试 → nuntius 创建**新 Task**(新 task_id),body 带上"上次为什么没过"作上下文;原 Task head 停在 succeeded(执行事实不可变)。
- `done_criteria`(§6.4)一鱼两吃:进简报告诉 Agent"怎么算做完",也给 acceptance verifier 当验收依据。

---

## 8. fixus 侧改动

> **存储现实(纠正)**:fixus 持久化是 **logdbd append-only log**,不是 SQL 表(无 `sessions`/`events` 表,无 DDL)。`session_id` 是 fixus 传给 logdbd 的 **stream 名(值 = opaque ID)**。Task 复用同一 ID 值 → logdbd stream 名不变 → **logdbd 零感知**(它是通用底层,不该也不受 fixus 命名影响)。组件均未商用,无旧数据/兼容包袱。

### 8.1 Session → Task rename(核心改动,纯代码层)

- `models.rs`:`Session` struct → `Task`(+ `state` / `provenance` / `body` 字段);`session_id` 字段 → `task_id`(**值不变**)。`Session::new` 是死代码(无调用),顺手演进。
- `EventStore` trait(23 个方法):`create_session` → `create_task`,所有 `session_id: &str` 参数 → `task_id: &str`;`LogdbdEventStore` impl 同步。stream 名用 `task_id` 值(= 原 session_id 值,logdbd 无感)。
- `context.rs` / `recovery.rs` / `orchestrator.rs` / `service.rs` / `session_registry.rs` / `protocol.rs` / `error.rs`:所有 `session_id` → `task_id`、`Session` → `Task`。
- **wire 层 cosmetic(URL `/sessions`→`/tasks`、header `X-Fixus-Session-Id`→`X-Fixus-Task-Id`、ACP 前缀、注释)**:值不变,改不改都不影响功能。**留到后续 plan 顺手清** —— 因为本 plan 只动 fixus,改 fixus HTTP 路径会 break nuntius 调用,而 nuntius 改动在 Plan C/D。cosmetic 清理在那些 plan 里一并做。

### 8.2 新增 Task 级事件

fixus 现有 Session/Turn/Step 三级事件,无 Task 级。新增 Task 级事件(每个状态迁移显式化):

```
task_created, task_ready, task_claimed, task_blocked, task_succeeded, task_failed, task_canceled
```

运行事件(LlmInvoked/ToolInvoked/...)白用现成的。

### 8.3 routing 改 task_type

- `session_registry.rs` 现按 `session.agent_type` 路由 → 改按 `task.task_type` 路由。
- 新增 `claim` 协议:执行器经 WS 认领 ready 的 Task;带 `preferred_claimant` hint(blocked 恢复时优先原执行器)。
- Task 级状态机由 fixus 驱动(执行器行为)+ 接受 nuntius 写入的语义事件(created→ready、blocked→ready)。

### 8.4 create_task 接口

nuntius 调 `create_task(task_type, provenance, body)` → fixus 分配 task_id、存 head、发 task_created 事件。task_id 由 fixus 分配(保证唯一)。

---

## 9. nuntius 侧改动

### 9.1 Task 创建时序

Task 在 **nuntius 确认要澄清一个 Task 时**(意图匹配 ① 通过 + 进入澄清流程)就 `create_task(state=created)`,拿回 task_id。**不是 readiness 通过才创建** —— created 态先于 ready,readiness 是 created→ready 的推进过程。

- `create_task(task_type, provenance, body=初始 contract)` → fixus 分配 task_id、存 head、发 `task_created`。
- provenance 带全:source_channel=nuntius-chat、source_session_id、source_user_id、source_message_id(触发意图的那条消息)、created_at、created_by=nuntius。
- 澄清中途用户放弃 → nuntius 发 `task_canceled`,Task 进终态(不悬在 created)。
- readiness 通过(+ 人确认)→ 编译 task_brief → 发 `task_ready`(created→ready,body 冻结)。

### 9.2 clarify loop 重写(§5)

- `extract_fields` 改为**累积**(看全部上下文,治 F1)。
- `check_readiness` 升级为三层:静态硬检查(代码)+ LLM 动态语义 + 人确认 gate。
- readiness 通过 → 编译 task_brief(§6)→ 发 `task_ready`(created→ready)。Task 已在意图匹配时 create_task,这里只推进状态。

### 9.3 schema 加载 + 字段

- 加载 `instructions` / `done_criteria` / `allowed_tools`。
- 砍 `risk_level`;`requires_approval` 复用为人确认开关。

### 9.4 状态投影

- nuntius 订阅 fixus 事件流(SSE),投影 Task 状态到 UI。
- Task 卡片读 head 当前 state;执行轨迹订阅运行事件。
- `list_tasks` 不再反查拼凑,直接查 fixus task 列表(按 provenance.user_id 过滤)。

### 9.5 acceptance(可选,§7)

succeeded 后按 schema 形态(none/auto/human)做验收,结果存 body。

---

## 10. 未决与未来(本 spec 不实现)

- **动态 schema 声明** — schema 仍静态文件;未来自治引擎可经 gRPC 动态声明新能力(`registry.rs` 注释已预留)。
- **Task 编排** — 父子 Task / 依赖 / 多 Agent 协作。YAGNI,本设计保持扁平。
- **多渠道下发** — source_channel 预留字段,api/schedule/derived 渠道后续接入。
- **head 纯事件投影** — 现保持独立存储;未来若要纯 event sourcing 可改物化视图。

---

## 11. 验收标准

1. fixus 不再有 session 顶层;Task 是唯一顶层实体,turn 挂 Task 下。
2. Task head(task_id/task_type/state/provenance)+ body(contract/schema_ref/task_brief/judgments)结构落地。
3. 状态机六态 + 两处 nuntius gate(created→ready、blocked→ready),迁移触发权符合 §4.1 表。
4. readiness 三层(静态硬 + LLM 动态语义 + 人确认),clarify 累积上下文(F1 治愈)。
5. Contract 编译为 task_brief(schema 模板插值),Agent 收到结构化简报而非裸 JSON。
6. schema 砍 risk_level、加 instructions/done_criteria/allowed_tools,requires_approval 作人确认开关。
7. fixus 加 Task 级事件;运行状态复用现有 EventType。
8. 一个 task_type 端到端跑通(database.repair):chat 澄清 → readiness 过 → 编译 brief → create_task → 执行器 claim → 执行 → 事件流回投 → nuntius 呈现。
