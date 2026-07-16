# fixus 安全设计白皮书

fixus 是 Agent 可靠执行运行时。本文是**系统级**安全模型:事件存储完整性、崩溃恢复语义、broker 信任边界、沙箱隔离、CR-12 凭据模型、多租户治理。操作细节见 [用户指南](user-guide.md);沙箱机制细节以 [lv-sandbox 文档](https://github.com/lv-agent/lv-sandbox) 为准。

> 状态:**已实现并 live 验证**(Claude Code + Hermes,含 `kill -9` 崩溃恢复;CR-12 git 沙箱 E2E)。标注 **P3** 的为规划项,当前未实现。

---

## 1. 威胁模型

fixus 防御的核心威胁:

1. **执行崩溃导致状态损坏 / 重复副作用** — agent 执行中进程被杀、机器重启。→ 事件存储不可变 + Turn 级崩溃恢复 + 幂等键 + 非幂等写阻塞。
2. **Agent 工具的逃逸与外泄** — agent 跑不可信代码 / 读敏感文件 / 外联。→ 独立沙箱(seccomp + landlock + cgroup + allowlisted egress)+ 输出脱敏 + env allowlist。
3. **凭据泄露** — git 凭据进沙箱。→ CR-12 sentinel 占位凭据 + 出口代理兑换(真凭据不过边界)。
4. **越权** — tenant / task 声明超出其权限的能力。→ 三 scope 策略交集(Operator ∩ Tenant ∩ Task),最严者胜。

**非目标(当前)**:API 层鉴权(P3,未强制);同文件多 agent 协作合并;同机侧信道。

---

## 2. 信任边界

```
┌─ Operator(部署方)─ 给定 Operator policy(严默认,部署期 TOML)─ 信任 ceiling ─┐
│  ┌─ Tenant ─ tenant policy(⊆ operator,否则拒)─┐                          │
│  │  ┌─ Task ─ 创建者声明(⊆ tenant ⊆ operator)─┐                         │
│  │  │     有效能力 = Operator ∩ Tenant ∩ Task(最严交集)                  │
│  │  └────────────────────────────────────────┘                          │
│  └────────────────────────────────────────────┘                          │
└──────────────────────────────────────────────────────────────────────────┘
        │ 有效能力经 broker 透传到 sandbox-server 落地执行
        ▼
   牢笼(landlock + seccomp + cgroup;零出站默认)
```

- **Operator policy** 是全系统信任上限:env `FIXUS_OPERATOR_POLICY_FILE` 指向 TOML;**空 = 严默认;非法 = fail-closed 拒启动**。
- 任何能力(fs 读写、net egress)取 **三 scope 交集**;任何一 scope 不放行即不放行。
- fixus 只做策略 resolve + 校验;**落地强制在 sandbox-server**(seccomp/landlock 是内核级,无法绕过)。

---

## 3. 事件存储完整性

事件是不可变的事实来源;状态是投影。

- **append-only**:logdbd append-only log;事件只增不改不删(`archive` 是归档,非改写)。
- **WAL seq 无 gap**:事务内取 seq 号,写事件与 seq 自增同一事务;`detect_seq_gaps` / `is_turn_seq_continuous` 可审计连续性。
- **终态唯一**:同一 `task_id`/`turn_id`/`step_id` 至多一个 terminal 事件(storage 校验四种 Turn 终态 + Step 终态)。
- **CR-7 write guard**:写路径不变量守护(broker 投影缓存 write guard),防止越权 / 乱序写。
- **生命周期不变量**:service 层校验 Task 8 态迁移合法性;非法迁移 → `LifecycleInvariant`(HTTP 409)。
- **脏数据不 panic**:所有 pub API 返回 `Result`;解析点(`event_from_row` 等)遇脏数据返回错误而非 panic。

> 事件存储本身**不含机密**(payload 是 agent 可见的执行记录);凭据不存于事件(见 §CR-12)。

---

## 4. 崩溃恢复 & 幂等性

Turn 级崩溃恢复是可靠性的核心承诺。

- **redo_group**:崩溃后按 `redo_group` 重放该组事件。
- **幂等键**(`idempotency_key`):幂等工具可安全重放;重复执行产生相同结果,不重复副作用。
- **LLM 缓存注入**:重放时把已发生的 LLM 响应从事件注入上下文,避免重复付费调用。
- **非幂等写阻塞**(★ 安全关键):写副作用但非幂等的工具,崩溃后 **阻塞 turn**(`blocked`),**不盲目重放**。`recovery` 分类工具(读 / 幂等写 / 非幂等写):只有非幂等写阻塞,等人工 / 上层确认后 `blocked → ready` 续跑。
- **CR-3 失败分类 + retry 预算**:终态原因立即失败;可重试的受 `FIXUS_MAX_RETRY_ATTEMPTS`(默认 2)预算约束。

> 这意味着:**fixus 永不会因崩溃而把一个非幂等写工具执行两次**。这是相对「at-least-once 重放」的有意收紧。

---

## 5. broker 星型拓扑信任

服务间全走 logdbd broker(gRPC),无进程内直连、无点对点 WS。

- **星型中心 = logdbd**:fixus、fixlet、tools-bank、sandbox-server、fixus-stream 都是它的客户端。
- **stream 名 = task_id**:数据天然按 task 隔离;`task-begin-{type}`/`task-end`、`tool-invoke-{region}`/`tool-result-{region}` 是控制面 stream。
- **pull-based 认领**:fixlet 竞争消费(stable group `fixlets-{type}`,`preferred_claimant` 优先),无中心调度器单点。
- 信任假设:broker 与各服务同属可信内网(P3:broker 鉴权 / mTLS 待加)。**API 网关是唯一对客户端暴露的面**,其余服务不对外。

---

## 6. 沙箱隔离(概述;细节见 lv-sandbox)

工具执行落在独立 sandbox-server,牢笼级隔离(内核强制,agent 无法绕过):

| 维度 | 机制 | 默认 |
|------|------|------|
| **syscall** | seccomp denylist + `socket()` 限 `AF_UNIX`(`deny_network`) | 任何 `socket(domain != AF_UNIX)` → **KillProcess** |
| **fs** | landlock ReadWrite 仅限 task workspace;`/dev/null` 等 device ReadOnly(profile 可显式放宽) | 最小可写 |
| **资源** | cgroup v2 + rlimit(cpu / mem / fsize / nofile / nproc / timeout) | profile 定义 |
| **进程** | NoNewPrivs(禁提权)、setsid、fd 清理、**env allowlist**(运行方密钥不进牢笼) | 硬化 |
| **网络** | 默认**零出站**;opt-in allowlist 经 cr-019 SOCKS5h-over-UDS 代理(DOMAIN ATYP,真 DNS 在代理侧) | off |

- **allowlist 收口**:非白名单 host:port → 代理 `SOCKS5 REP=0x02` 拒绝。自动化测试:`proxy::non_allowlisted_denied`、`proxy::proxy_rejects_ipv4_literal`、git-remote-fixus `dialer::non_allowlisted_host_is_denied`。
- **输出脱敏**:stdout/stderr 返调用方前洗常见密钥模式(Bearer / AWS `AKIA` / GitHub token / PEM 私钥)。
- **`socket(AF_INET)` 被 kill 的 live 测试**:seccomp_tests 验证牢内开 INET socket → 进程被杀(CR-12 §5 不变量 1 的自动化证据)。

> landlock/seccomp 的精确 ruleset、profile 字段、cgroup 行为以 lv-sandbox 文档与代码为准。

---

## 7. CR-12 凭据模型(sentinel)

CR-12 让牢内 agent 能 `git clone`/`push`(`git` profile 开 allowlisted 出口)。凭据安全是核心:

**假设**:凭据**进牢笼,但牢笼里的是占位假凭据(sentinel)**。

- `git` profile 给牢笼一个 sentinel;git 走**标准凭据流程**(credential helper / `git config`)。
- sentinel 是**非密占位符**:被 exfiltration 也无用(可公开)。
- **真凭据 + fake→real 兑换在出口代理(牢笼外)**:allowlist 指向代理;代理识别 sentinel → 替换为真 token → 转发 `github.com:443`。**真 token 只在代理进程内**。
- 兑换机制(代理本体 + sentinel 方案)由**使用方实现**(CR-12 外);CR-12 只定义牢笼侧 allowlist 形态 + 标准 git 凭据流程。

**安全不变量(验收,均有自动化测试)**:

1. **牢内凭据绕代理直连 `github.com` 必失败** —— 证明 sentinel 不是「伪装的真 token」。自动化:`socket(domain != AF_UNIX)` 被 seccomp `deny_network` **KillProcess**(牢内无法开 INET socket)。
2. **真凭据只存在于代理进程内**(设计保证;代理本体 CR-12 外)。
3. **allowlist 收口**:非白名单 host 被代理 `REP=0x02` 拒。

> ⇒ 比「token 绝不过边界」更易验证:把牢内 sentinel 公开,它必须什么也干不了。

CR-12 设计全文见 `veps/cr-12-networked-git-sandbox-design.md`(本地)。

---

## 8. 多租户与策略治理

- 多租户字段:`tenant_id` / `user_id` 贯穿事件与 task。
- **三 scope 交集**:Operator ∩ Tenant ∩ Task,fs / net 各维度取最严。tenant policy 经 `PUT /api/v1/tenants/{id}/policy` 设置(校验 `tenant ⊆ operator`,越权 → 400)。
- **net 是正交 capability**(CR-12):可与 shell/python/node 等语言 runtime 正交叠加。
- 治理视角:开网络是 policy 决策,须经 Operator scope(默认禁网,显式放行)—— 见 `adr-2026-07-10-sandbox-broker-governance.md`。

---

## 9. 已知限制 & P3

诚实披露当前边界:

- **API 层无鉴权**(P3):HTTP 网关当前不校验身份;依赖网络层隔离。多租户字段已建模,强制待 P3。
- **broker 鉴权 / mTLS**(P3):服务间 gRPC 当前靠可信内网。
- **Session Fork**(未实现):`from_snapshot` 旁路(cr-027)在桥上未接通。
- **summary_marker 自动触发**(未串联):always-on 摘要 deferred;将来走 lazy on-demand。fixus 不自带 LLM 客户端。
- **`turn_pending`** 为死代码(待清)。
- **Event 导入端点**:缺(批量历史导入)。
- **同机侧信道 / 资源耗尽**:cgroup 限额缓解,非硬件级隔离。

---

## 10. 测试与验证

- **数据完整性**:WAL seq 无 gap、终态唯一、幂等分类、CR-7 write guard、turn 连续性 —— 均有自动化测试(lv-fixus 262 tests)。
- **沙箱隔离**:seccomp `deny_network`(INET → KillProcess,含 live 测试)、landlock、egress allowlist 拒绝非白名单 —— lv-sandbox 343 tests。
- **CR-12 §5 两不变量**:均有自动化测试 + live E2E(牢内 clone+push 经 SOCKS5h UDS 代理 + rustls TLS 全通;两不变量 live PASS)。
- **崩溃恢复**:`kill -9` 场景 live 验证(Claude Code + Hermes)。

性能基线:lv-sandbox 有 criterion benches(fork/exec 延迟、RSS、per-job 开销);fixus 纯计算层(事件→消息 `context`、事件→状态 `projection`)有 criterion benches(`benches/bench_{context,projection}.rs`)。
