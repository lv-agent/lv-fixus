//! Event Store 持久化层
//!
//! 实现设计文档第 3 节的数据库 schema、第 9 节的写入协议、
//! 第 11 节的关键查询。
//!
//! 开发阶段使用 SQLite（设计文档 16.10 节），部分唯一索引
//! 降级为应用层校验。
//!
//! # 数据库操作约定
//! - 全部走事务，不在事务外执行 seq 自增
//! - seq 取号与 INSERT 在同一数据库事务中完成

use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;

use crate::error::{AppError, Result};
use crate::models::{
    AgentEvent, EventType, IncompleteStep, IncompleteTurn, Session, StepExecution, TokenUsageStats,
};

/// 将 SQLite datetime 字符串转换为 DateTime<Utc>
///
/// SQLite `datetime('now')` 返回格式: "YYYY-MM-DD HH:MM:SS" (UTC，无时区标记)
fn parse_sqlite_datetime(s: &str) -> DateTime<Utc> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|naive| naive.and_utc().into())
        .unwrap_or(DateTime::<Utc>::MIN_UTC)
}

/// 默认数据库路径
const DEFAULT_DB_URL: &str = "sqlite:fixus.db?mode=rwc";

// ── 连接池 ──────────────────────────────────────────────────────────────

/// 创建 SQLite 连接池
pub async fn create_pool(database_url: Option<&str>) -> Result<SqlitePool> {
    let url = database_url.unwrap_or(DEFAULT_DB_URL);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(url)
        .await?;

    Ok(pool)
}

/// 从环境变量 FIXUS_DATABASE_URL 或默认路径创建连接池
pub async fn pool_from_env() -> Result<SqlitePool> {
    let url = std::env::var("FIXUS_DATABASE_URL").ok();
    create_pool(url.as_deref()).await
}

// ── 数据库迁移 ──────────────────────────────────────────────────────────

/// 执行数据库迁移
///
/// 创建 sessions、session_seq_counter、agent_events 三张表
/// 及其索引。
pub async fn run_migrations() -> Result<()> {
    let pool = pool_from_env().await?;
    run_migrations_on(&pool).await
}

/// 在指定连接池上执行迁移
pub async fn run_migrations_on(pool: &SqlitePool) -> Result<()> {
    // Tenants 表（多租户隔离）
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS tenants (
            id          TEXT        NOT NULL PRIMARY KEY,
            name        TEXT        NOT NULL,
            created_at  TEXT        NOT NULL DEFAULT (datetime('now'))
        );
        "#,
    )
    .execute(pool)
    .await?;

    // 默认租户（向后兼容）
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO tenants (id, name) VALUES ('default', 'Default Tenant');
        "#,
    )
    .execute(pool)
    .await?;

    // Sessions 表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sessions (
            session_id   TEXT        NOT NULL,
            tenant_id    TEXT        NOT NULL DEFAULT 'default',
            user_id      TEXT        NOT NULL DEFAULT '',
            agent_type   TEXT        NOT NULL,
            created_at   TEXT        NOT NULL DEFAULT (datetime('now')),
            metadata     TEXT,

            PRIMARY KEY (session_id),
            FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        );
        "#,
    )
    .execute(pool)
    .await?;

    // 迁移旧表：添加 tenant_id / user_id 列（如果不存在）
    // SQLite 不支持 IF NOT EXISTS for ALTER TABLE，忽略错误
    let _ = sqlx::query("ALTER TABLE sessions ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default'").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE sessions ADD COLUMN user_id TEXT NOT NULL DEFAULT ''").execute(pool).await;

    // sessions created_at 索引
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_sessions_created_at
            ON sessions (created_at);
        "#,
    )
    .execute(pool)
    .await?;

    // 租户隔离索引
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_sessions_tenant
            ON sessions (tenant_id, created_at);
        "#,
    )
    .execute(pool)
    .await?;

    // Seq Counter 表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS session_seq_counter (
            session_id  TEXT   NOT NULL PRIMARY KEY,
            last_seq    INTEGER NOT NULL DEFAULT 0,

            FOREIGN KEY (session_id)
                REFERENCES sessions (session_id)
                ON DELETE CASCADE
        );
        "#,
    )
    .execute(pool)
    .await?;

    // Agent Events 表 — 核心表
    // SQLite 不支持 CHECK 约束中的 IN 列表和部分唯一索引，
    // event_type 枚举、scope 约束、唯一性约束在应用层校验。
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS agent_events (
            session_id     TEXT    NOT NULL,
            seq            INTEGER NOT NULL,
            turn_id        INTEGER,
            step_id        TEXT,
            event_type     TEXT    NOT NULL,
            schema_version INTEGER NOT NULL DEFAULT 1,
            payload        TEXT    NOT NULL DEFAULT '{}',
            created_at     TEXT    NOT NULL DEFAULT (datetime('now')),

            PRIMARY KEY (session_id, seq),

            FOREIGN KEY (session_id)
                REFERENCES sessions (session_id)
                ON DELETE RESTRICT,

            CONSTRAINT chk_seq_positive
                CHECK (seq > 0),

            CONSTRAINT chk_turn_id_positive
                CHECK (turn_id IS NULL OR turn_id > 0)
        );
        "#,
    )
    .execute(pool)
    .await?;

    // Turn 内全量读取索引
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_events_turn
            ON agent_events (session_id, turn_id, seq ASC)
            WHERE turn_id IS NOT NULL;
        "#,
    )
    .execute(pool)
    .await?;

    // Step 级查询索引
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_events_step
            ON agent_events (session_id, step_id, seq ASC)
            WHERE step_id IS NOT NULL;
        "#,
    )
    .execute(pool)
    .await?;

    // 事件类型索引
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_events_type
            ON agent_events (session_id, event_type);
        "#,
    )
    .execute(pool)
    .await?;

    tracing::info!("Database migrations completed successfully.");
    Ok(())
}

// ── Session 操作 ────────────────────────────────────────────────────────

/// 创建 Session（含 session_started 事件写入）
///
/// 对应设计文档 9.2 节"创建 Session 的完整事务"。
/// 在一个事务中完成：
/// 1. INSERT INTO sessions
/// 2. INSERT INTO session_seq_counter
/// 3. seq = 1 写入 session_started
pub async fn create_session(
    pool: &SqlitePool,
    session_id: &str,
    tenant_id: &str,
    user_id: &str,
    agent_type: &str,
    metadata: Option<serde_json::Value>,
) -> Result<AgentEvent> {
    let metadata_str = metadata
        .as_ref()
        .map(|m| m.to_string())
        .unwrap_or_else(|| "{}".to_string());

    let payload = serde_json::json!({
        "agent_type": agent_type,
        "initial_config": metadata.as_ref().unwrap_or(&serde_json::Value::Null),
    });
    let payload_str = payload.to_string();

    let mut tx = pool.begin().await?;

    // 1. 插入 Session
    sqlx::query(
        r#"
        INSERT INTO sessions (session_id, tenant_id, user_id, agent_type, metadata)
        VALUES (?1, ?2, ?3, ?4, ?5)
        "#,
    )
    .bind(session_id)
    .bind(tenant_id)
    .bind(user_id)
    .bind(agent_type)
    .bind(&metadata_str)
    .execute(&mut *tx)
    .await?;

    // 2. 初始化 seq counter
    sqlx::query(
        r#"
        INSERT INTO session_seq_counter (session_id, last_seq)
        VALUES (?1, 0)
        "#,
    )
    .bind(session_id)
    .execute(&mut *tx)
    .await?;

    // 3. 取 seq = 1 并写入 session_started
    // SQLite 不直接支持 UPDATE ... RETURNING，用自增计数替代
    let _row = sqlx::query(
        r#"
        UPDATE session_seq_counter
        SET last_seq = last_seq + 1
        WHERE session_id = ?1
        "#,
    )
    .bind(session_id)
    .execute(&mut *tx)
    .await?;

    // SQLite 中直接用 last_insert_rowid 不可靠，使用子查询
    let seq_row = sqlx::query(
        r#"
        SELECT last_seq FROM session_seq_counter WHERE session_id = ?1
        "#,
    )
    .bind(session_id)
    .fetch_one(&mut *tx)
    .await?;
    let seq: i64 = seq_row.get("last_seq");

    let created_at = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    sqlx::query(
        r#"
        INSERT INTO agent_events (session_id, seq, turn_id, step_id, event_type, schema_version, payload, created_at)
        VALUES (?1, ?2, NULL, NULL, 'session_started', 1, ?3, ?4)
        "#,
    )
    .bind(session_id)
    .bind(seq)
    .bind(&payload_str)
    .bind(&created_at)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(AgentEvent {
        session_id: session_id.to_string(),
        seq,
        turn_id: None,
        step_id: None,
        event_type: EventType::SessionStarted,
        schema_version: 1,
        payload,
        created_at: parse_sqlite_datetime(&created_at)
            ,
    })
}

/// 查询 Session
pub async fn get_session(pool: &SqlitePool, session_id: &str) -> Result<Option<Session>> {
    let row = sqlx::query(
        r#"
        SELECT session_id, tenant_id, user_id, agent_type, created_at, metadata
        FROM sessions
        WHERE session_id = ?1
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(r) => {
            let created_at_str: String = r.get("created_at");
            let created_at = parse_sqlite_datetime(&created_at_str)
                ;

            let metadata_str: Option<String> = r.get("metadata");
            let metadata = metadata_str
                .filter(|s| s != "{}")
                .and_then(|s| serde_json::from_str(&s).ok());

            Ok(Some(Session {
                session_id: r.get("session_id"),
                tenant_id: r.get("tenant_id"),
                user_id: r.get("user_id"),
                agent_type: r.get("agent_type"),
                created_at,
                metadata,
            }))
        }
        None => Ok(None),
    }
}

/// 检查 Session 是否存在
pub async fn session_exists(pool: &SqlitePool, session_id: &str) -> Result<bool> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*) as cnt FROM sessions WHERE session_id = ?1
        "#,
    )
    .bind(session_id)
    .fetch_one(pool)
    .await?;

    let cnt: i64 = row.get("cnt");
    Ok(cnt > 0)
}

/// 检查 Session 是否已结束（已有 session_ended 事件）
pub async fn is_session_ended(pool: &SqlitePool, session_id: &str) -> Result<bool> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*) as cnt
        FROM agent_events
        WHERE session_id = ?1 AND event_type = 'session_ended'
        "#,
    )
    .bind(session_id)
    .fetch_one(pool)
    .await?;

    let cnt: i64 = row.get("cnt");
    Ok(cnt > 0)
}

// ── Seq 计数器操作 ──────────────────────────────────────────────────────

/// 获取当前最大 seq（用于恢复时重建 turn_id 计数器等）
pub async fn get_max_seq(pool: &SqlitePool, session_id: &str) -> Result<i64> {
    let row = sqlx::query(
        r#"
        SELECT COALESCE(MAX(seq), 0) as max_seq
        FROM agent_events
        WHERE session_id = ?1
        "#,
    )
    .bind(session_id)
    .fetch_one(pool)
    .await?;

    Ok(row.get("max_seq"))
}

/// 获取当前最大 turn_id（用于崩溃恢复时重建计数器）
pub async fn get_max_turn_id(pool: &SqlitePool, session_id: &str) -> Result<i64> {
    let row = sqlx::query(
        r#"
        SELECT COALESCE(MAX(turn_id), 0) as max_turn_id
        FROM agent_events
        WHERE session_id = ?1 AND turn_id IS NOT NULL
        "#,
    )
    .bind(session_id)
    .fetch_one(pool)
    .await?;

    Ok(row.get("max_turn_id"))
}

// ── Event 写入（Write-Ahead） ───────────────────────────────────────────

/// 在事务中取 seq 并写入事件（核心写入路径）
///
/// 对应设计文档 9.3 节"普通 Event 写入"。
/// seq 取号与 INSERT 在同一数据库事务中完成。
///
/// 返回分配到的 seq。
async fn next_seq_in_tx(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, session_id: &str) -> Result<i64> {
    sqlx::query(
        r#"
        UPDATE session_seq_counter
        SET last_seq = last_seq + 1
        WHERE session_id = ?1
        "#,
    )
    .bind(session_id)
    .execute(&mut **tx)
    .await?;

    let row = sqlx::query(
        r#"
        SELECT last_seq FROM session_seq_counter WHERE session_id = ?1
        "#,
    )
    .bind(session_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(row.get("last_seq"))
}

/// 写入单个 Event（在一个已开启的事务中）
///
/// ## 前置条件
/// - 调用方已开启数据库事务
/// - event_type 的 scope 与 turn_id/step_id 已完成应用层校验
///
/// ## 应用层校验（SQLite 缺少部分唯一索引的补偿）
/// - Session 级别唯一性：session_started/session_ended 各最多一条
/// - Turn 级别唯一性：同一 turn_id 最多一个 start/terminal
/// - Step 级别唯一性：同一 step_id 最多一个 start/terminal
async fn insert_event_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event: &AgentEvent,
) -> Result<i64> {
    let session_id = &event.session_id;
    let event_type_str = event.event_type.as_str();
    let payload_str = event.payload.to_string();
    let created_at = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

    // 1. 应用层生命周期不变量校验（补偿 SQLite 缺少部分唯一索引）
    validate_lifecycle_invariants(tx, &event.session_id, event.turn_id, event.step_id.as_deref(), &event.event_type).await?;

    // 2. 取 seq
    let seq = next_seq_in_tx(tx, session_id).await?;

    // 3. 写入
    sqlx::query(
        r#"
        INSERT INTO agent_events (session_id, seq, turn_id, step_id, event_type, schema_version, payload, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        "#,
    )
    .bind(session_id)
    .bind(seq)
    .bind(event.turn_id)
    .bind(&event.step_id)
    .bind(event_type_str)
    .bind(event.schema_version)
    .bind(&payload_str)
    .bind(&created_at)
    .execute(&mut **tx)
    .await?;

    Ok(seq)
}

/// 写入单个 Event 并返回 seq
///
/// 公共 API — 自动开启事务。
pub async fn write_event(pool: &SqlitePool, event: &AgentEvent) -> Result<i64> {
    // 先做作用域校验（不依赖数据库 CHECK 约束）
    event.validate_scope().map_err(|msg| AppError::LifecycleInvariant(msg))?;

    // payload 关键字段校验
    crate::models::validate_payload_required_fields(&event.event_type, &event.payload)?;

    let mut tx = pool.begin().await?;
    let seq = insert_event_in_tx(&mut tx, event).await?;
    tx.commit().await?;

    Ok(seq)
}

/// 批量写入 Event（在同一事务中）
///
/// 用于 fixlet 异步回传事件的批量落库（设计文档 16.3.2）。
pub async fn write_events_batch(
    pool: &SqlitePool,
    events: &[AgentEvent],
) -> Result<Vec<i64>> {
    if events.is_empty() {
        return Ok(vec![]);
    }

    let mut tx = pool.begin().await?;
    let mut seqs = Vec::with_capacity(events.len());

    for event in events {
        event.validate_scope().map_err(|msg| AppError::LifecycleInvariant(msg))?;
        crate::models::validate_payload_required_fields(&event.event_type, &event.payload)?;
        let seq = insert_event_in_tx(&mut tx, event).await?;
        seqs.push(seq);
    }

    tx.commit().await?;
    Ok(seqs)
}

// ── 生命周期不变量校验（应用层） ────────────────────────────────────────

/// 在写入前校验生命周期不变量
///
/// SQLite 不支持 PostgreSQL 的部分唯一索引（WHERE 子句），
/// 因此这些唯一性约束需要在应用层校验。
async fn validate_lifecycle_invariants(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    session_id: &str,
    turn_id: Option<i64>,
    step_id: Option<&str>,
    event_type: &EventType,
) -> Result<()> {
    match event_type {
        // Session 级别：session_started / session_ended 各最多一条
        EventType::SessionStarted => {
            let row = sqlx::query(
                "SELECT COUNT(*) as cnt FROM agent_events WHERE session_id = ?1 AND event_type = 'session_started'",
            )
            .bind(session_id)
            .fetch_one(&mut **tx)
            .await?;
            let cnt: i64 = row.get("cnt");
            if cnt > 0 {
                return Err(AppError::LifecycleInvariant(
                    "session_started already exists for this session".into(),
                ));
            }
        }
        EventType::SessionEnded => {
            let row = sqlx::query(
                "SELECT COUNT(*) as cnt FROM agent_events WHERE session_id = ?1 AND event_type = 'session_ended'",
            )
            .bind(session_id)
            .fetch_one(&mut **tx)
            .await?;
            let cnt: i64 = row.get("cnt");
            if cnt > 0 {
                return Err(AppError::SessionAlreadyEnded(session_id.to_string()));
            }
        }
        // Turn 级别：同一 turn_id 最多一个 start 和一个 terminal
        EventType::TurnStarted => {
            if let Some(tid) = turn_id {
                let row = sqlx::query(
                    "SELECT COUNT(*) as cnt FROM agent_events WHERE session_id = ?1 AND turn_id = ?2 AND event_type = 'turn_started'",
                )
                .bind(session_id)
                .bind(tid)
                .fetch_one(&mut **tx)
                .await?;
                let cnt: i64 = row.get("cnt");
                if cnt > 0 {
                    return Err(AppError::LifecycleInvariant(
                        format!("turn_started already exists for turn {}", tid),
                    ));
                }
            }
        }
        EventType::TurnCompleted | EventType::TurnFailed
        | EventType::TurnCanceled | EventType::TurnBlocked => {
            if let Some(tid) = turn_id {
                let row = sqlx::query(
                    "SELECT COUNT(*) as cnt FROM agent_events WHERE session_id = ?1 AND turn_id = ?2 AND event_type IN ('turn_completed', 'turn_failed', 'turn_canceled', 'turn_blocked')",
                )
                .bind(session_id)
                .bind(tid)
                .fetch_one(&mut **tx)
                .await?;
                let cnt: i64 = row.get("cnt");
                if cnt > 0 {
                    return Err(AppError::TurnAlreadyTerminal {
                        session_id: session_id.to_string(),
                        turn_id: tid,
                    });
                }
            }
        }
        // Step 级别：同一 step_id 最多一个 start 和一个 terminal
        EventType::LlmInvoked | EventType::ToolInvoked => {
            if let Some(sid) = step_id {
                let row = sqlx::query(
                    "SELECT COUNT(*) as cnt FROM agent_events WHERE session_id = ?1 AND step_id = ?2 AND event_type IN ('llm_invoked', 'tool_invoked')",
                )
                .bind(session_id)
                .bind(sid)
                .fetch_one(&mut **tx)
                .await?;
                let cnt: i64 = row.get("cnt");
                if cnt > 0 {
                    return Err(AppError::LifecycleInvariant(
                        format!("start event already exists for step {}", sid),
                    ));
                }
            }
        }
        EventType::LlmCompleted
        | EventType::LlmFailed
        | EventType::ToolCompleted
        | EventType::ToolFailed => {
            if let Some(sid) = step_id {
                let row = sqlx::query(
                    "SELECT COUNT(*) as cnt FROM agent_events WHERE session_id = ?1 AND step_id = ?2 AND event_type IN ('llm_completed', 'llm_failed', 'tool_completed', 'tool_failed')",
                )
                .bind(session_id)
                .bind(sid)
                .fetch_one(&mut **tx)
                .await?;
                let cnt: i64 = row.get("cnt");
                if cnt > 0 {
                    return Err(AppError::StepAlreadyTerminal {
                        step_id: sid.to_string(),
                    });
                }
            }
        }
        _ => {}
    }
    Ok(())
}

// ── Event 查询 ──────────────────────────────────────────────────────────

/// 从数据库行构建 AgentEvent
///
/// 遇到无法识别的 event_type 时返回错误而非 panic，
/// 防止脏数据导致整个服务进程崩溃。
fn event_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<AgentEvent> {
    let payload_str: String = row.get("payload");
    let payload: serde_json::Value =
        serde_json::from_str(&payload_str).unwrap_or(serde_json::Value::Null);

    let event_type_str: String = row.get("event_type");
    let event_type = EventType::from_str(&event_type_str)
        .ok_or_else(|| AppError::InvalidEventType(event_type_str.clone()))?;

    let created_at_str: String = row.get("created_at");
    let created_at = parse_sqlite_datetime(&created_at_str);

    Ok(AgentEvent {
        session_id: row.get("session_id"),
        seq: row.get("seq"),
        turn_id: row.get("turn_id"),
        step_id: row.get("step_id"),
        event_type,
        schema_version: row.get("schema_version"),
        payload,
        created_at,
    })
}

/// 读取某个 Turn 的完整执行过程
///
/// 对应设计文档 11.2 节。
pub async fn get_turn_events(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
) -> Result<Vec<AgentEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT session_id, seq, turn_id, step_id, event_type, schema_version, payload, created_at
        FROM agent_events
        WHERE session_id = ?1 AND turn_id = ?2
        ORDER BY seq ASC
        "#,
    )
    .bind(session_id)
    .bind(turn_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(event_from_row)
        .collect::<Result<Vec<_>>>()?)
}

/// 检测未完成的 Turn（恢复入口）
///
/// 对应设计文档 11.5 节。
pub async fn get_incomplete_turns(pool: &SqlitePool, session_id: &str) -> Result<Vec<IncompleteTurn>> {
    let rows = sqlx::query(
        r#"
        SELECT
            e_start.turn_id,
            e_start.payload,
            e_start.created_at
        FROM agent_events e_start
        WHERE e_start.session_id = ?1
          AND e_start.event_type = 'turn_started'
          AND NOT EXISTS (
              SELECT 1
              FROM agent_events e_end
              WHERE e_end.session_id = e_start.session_id
                AND e_end.turn_id    = e_start.turn_id
                AND e_end.event_type IN ('turn_completed', 'turn_failed', 'turn_canceled', 'turn_blocked')
          )
        ORDER BY e_start.turn_id ASC
        "#,
    )
    .bind(session_id)
    .fetch_all(pool)
    .await?;

    let mut turns = Vec::new();
    for row in &rows {
        let payload_str: String = row.get("payload");
        let payload: serde_json::Value =
            serde_json::from_str(&payload_str).unwrap_or_default();

        let turn_id: i64 = row.get("turn_id");
        let redo_group = payload
            .get("redo_group")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let redo_count = payload
            .get("redo_count")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;

        let created_at_str: String = row.get("created_at");
        let turn_started_at = parse_sqlite_datetime(&created_at_str);

        turns.push(IncompleteTurn {
            turn_id,
            redo_group,
            redo_count,
            turn_started_at,
        });
    }

    Ok(turns)
}

/// 检测未完成的 Step（Turn 内诊断）
///
/// 对应设计文档 11.6 节。
pub async fn get_incomplete_steps(
    pool: &SqlitePool,
    session_id: &str,
) -> Result<Vec<IncompleteStep>> {
    let rows = sqlx::query(
        r#"
        SELECT
            e_start.seq,
            e_start.turn_id,
            e_start.step_id,
            e_start.event_type,
            e_start.payload,
            e_start.created_at
        FROM agent_events e_start
        WHERE e_start.session_id = ?1
          AND e_start.event_type IN ('llm_invoked', 'tool_invoked')
          AND NOT EXISTS (
              SELECT 1
              FROM agent_events e_end
              WHERE e_end.session_id = e_start.session_id
                AND e_end.step_id    = e_start.step_id
                AND e_end.event_type IN ('llm_completed', 'llm_failed',
                                         'tool_completed', 'tool_failed')
          )
        ORDER BY e_start.seq ASC
        "#,
    )
    .bind(session_id)
    .fetch_all(pool)
    .await?;

    let mut steps = Vec::new();
    for row in &rows {
        let payload_str: String = row.get("payload");
        let payload: serde_json::Value =
            serde_json::from_str(&payload_str).unwrap_or_default();

        let created_at_str: String = row.get("created_at");
        let started_at = parse_sqlite_datetime(&created_at_str);

        steps.push(IncompleteStep {
            seq: row.get("seq"),
            turn_id: row.get("turn_id"),
            step_id: row.get("step_id"),
            start_event_type: row.get("event_type"),
            payload,
            started_at,
        });
    }

    Ok(steps)
}

/// Turn 内的 Step 列表（含耗时）
///
/// 对应设计文档 11.3 节。
pub async fn get_turn_steps(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
) -> Result<Vec<StepExecution>> {
    let rows = sqlx::query(
        r#"
        SELECT
            e_start.step_id,
            e_start.payload AS start_payload,
            e_start.created_at AS started_at,
            e_end.created_at AS ended_at,
            e_end.event_type AS end_event
        FROM agent_events e_start
        JOIN agent_events e_end
          ON  e_end.session_id = e_start.session_id
          AND e_end.turn_id    = e_start.turn_id
          AND e_end.step_id    = e_start.step_id
          AND e_end.event_type IN ('llm_completed', 'llm_failed',
                                   'tool_completed', 'tool_failed')
        WHERE e_start.session_id = ?1
          AND e_start.turn_id    = ?2
          AND e_start.event_type IN ('llm_invoked', 'tool_invoked')
        ORDER BY e_start.seq ASC
        "#,
    )
    .bind(session_id)
    .bind(turn_id)
    .fetch_all(pool)
    .await?;

    let mut steps = Vec::new();
    for row in &rows {
        let start_payload_str: String = row.get("start_payload");
        let start_payload: serde_json::Value =
            serde_json::from_str(&start_payload_str).unwrap_or_default();
        let step_type = start_payload
            .get("step_type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let started_at_str: String = row.get("started_at");
        let ended_at_str: String = row.get("ended_at");
        let end_event_str: String = row.get("end_event");

        let started_at = parse_sqlite_datetime(&started_at_str);
        let ended_at = parse_sqlite_datetime(&ended_at_str);

        let duration_ms = (ended_at - started_at).num_milliseconds() as f64;

        steps.push(StepExecution {
            step_id: row.get("step_id"),
            step_type,
            started_at,
            ended_at: Some(ended_at),
            end_event: Some(end_event_str),
            duration_ms: Some(duration_ms),
        });
    }

    Ok(steps)
}

/// 获取最新 summary_marker
///
/// 对应设计文档 11.1 Step 1。
pub async fn get_latest_summary(pool: &SqlitePool, session_id: &str) -> Result<Option<AgentEvent>> {
    let row = sqlx::query(
        r#"
        SELECT session_id, seq, turn_id, step_id, event_type, schema_version, payload, created_at
        FROM agent_events
        WHERE session_id = ?1 AND event_type = 'summary_marker'
        ORDER BY seq DESC
        LIMIT 1
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await?;

    row.as_ref()
        .map(event_from_row)
        .transpose()
}

/// 读取 seq 大于指定值的事件（用于上下文增量构建）
///
/// 对应设计文档 11.1 Step 2。
/// 只取 Turn 级和 Turn 内 Step 事件，排除 Session 级后台 Step。
pub async fn get_events_after_seq(
    pool: &SqlitePool,
    session_id: &str,
    after_seq: i64,
) -> Result<Vec<AgentEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT session_id, seq, turn_id, step_id, event_type, schema_version, payload, created_at
        FROM agent_events
        WHERE session_id = ?1
          AND seq > ?2
          AND (
              -- Turn 级事件
              (
                  turn_id IS NOT NULL
                  AND step_id IS NULL
                  AND event_type IN ('turn_started', 'turn_completed', 'turn_failed', 'turn_canceled', 'turn_blocked')
              )
              OR
              -- Turn 内 Step 事件
              (
                  turn_id IS NOT NULL
                  AND step_id IS NOT NULL
                  AND event_type IN (
                      'llm_invoked',  'llm_completed',  'llm_failed',
                      'tool_invoked', 'tool_completed', 'tool_failed'
                  )
              )
          )
        ORDER BY seq ASC
        "#,
    )
    .bind(session_id)
    .bind(after_seq)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(event_from_row)
        .collect::<Result<Vec<_>>>()?)
}

/// 检测 seq 连续性（运维用）
///
/// 对应设计文档 11.8 节。
pub async fn detect_seq_gaps(pool: &SqlitePool, session_id: &str) -> Result<Vec<i64>> {
    let rows = sqlx::query(
        r#"
        SELECT seq + 1 AS missing_seq
        FROM agent_events e1
        WHERE session_id = ?1
          AND NOT EXISTS (
              SELECT 1 FROM agent_events e2
              WHERE e2.session_id = ?1
                AND e2.seq = e1.seq + 1
          )
          AND seq < (SELECT MAX(seq) FROM agent_events WHERE session_id = ?1)
        "#,
    )
    .bind(session_id)
    .bind(session_id)
    .bind(session_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(|r| r.get("missing_seq")).collect())
}

/// LLM Token 消耗统计
///
/// 对应设计文档 11.7 节。
/// 读取所有 llm_completed 事件，在应用层按 model 聚合。
pub async fn get_token_usage_stats(
    pool: &SqlitePool,
    session_id: &str,
) -> Result<Vec<TokenUsageStats>> {
    let rows = sqlx::query(
        r#"
        SELECT payload
        FROM agent_events
        WHERE session_id = ?1 AND event_type = 'llm_completed'
        "#,
    )
    .bind(session_id)
    .fetch_all(pool)
    .await?;

    // 按 model 聚合统计
    use std::collections::HashMap;
    let mut stats_map: HashMap<String, TokenUsageStats> = HashMap::new();

    for row in &rows {
        let payload_str: String = row.get("payload");

        if let Ok(payload) = serde_json::from_str::<serde_json::Value>(&payload_str) {
            let model = payload
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let usage = payload.get("usage");
            let prompt_tokens = usage
                .and_then(|u| u.get("prompt_tokens"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let completion_tokens = usage
                .and_then(|u| u.get("completion_tokens"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            let entry = stats_map.entry(model.clone()).or_insert_with(|| {
                TokenUsageStats {
                    model: model.clone(),
                    call_count: 0,
                    prompt_tokens: 0,
                    completion_tokens: 0,
                }
            });
            entry.call_count += 1;
            entry.prompt_tokens += prompt_tokens;
            entry.completion_tokens += completion_tokens;
        }
    }

    let mut stats: Vec<TokenUsageStats> = stats_map.into_values().collect();
    stats.sort_by(|a, b| a.model.cmp(&b.model));
    Ok(stats)
}

/// 读取一条 Event（按 session_id + seq）
pub async fn get_event(
    pool: &SqlitePool,
    session_id: &str,
    seq: i64,
) -> Result<Option<AgentEvent>> {
    let row = sqlx::query(
        r#"
        SELECT session_id, seq, turn_id, step_id, event_type, schema_version, payload, created_at
        FROM agent_events
        WHERE session_id = ?1 AND seq = ?2
        "#,
    )
    .bind(session_id)
    .bind(seq)
    .fetch_optional(pool)
    .await?;

    row.as_ref()
        .map(event_from_row)
        .transpose()
}

/// 判断 Turn 内 seq 是否连续
pub async fn is_turn_seq_continuous(
    pool: &SqlitePool,
    session_id: &str,
    turn_id: i64,
) -> Result<bool> {
    let gaps = detect_seq_gaps(pool, session_id).await?;
    // 获取该 Turn 的 seq 范围
    let rows = sqlx::query(
        r#"
        SELECT MIN(seq) as min_seq, MAX(seq) as max_seq
        FROM agent_events
        WHERE session_id = ?1 AND turn_id = ?2
        "#,
    )
    .bind(session_id)
    .bind(turn_id)
    .fetch_one(pool)
    .await?;

    let min_seq: Option<i64> = rows.get("min_seq");
    let max_seq: Option<i64> = rows.get("max_seq");

    match (min_seq, max_seq) {
        (Some(min_s), Some(max_s)) => {
            // 检查 Turn 范围内是否有 gap
            let has_gap = gaps.iter().any(|g| *g >= min_s && *g <= max_s);
            Ok(!has_gap)
        }
        _ => Ok(true), // 空的 Turn 视为连续
    }
}

// ── Event 归档 ──────────────────────────────────────────────────────────

/// 归档结果
#[derive(Debug, Clone)]
pub struct ArchiveResult {
    pub archived: usize,
    pub path: String,
}

/// 将 seq 小于 before_seq 的 Event 导出到 JSONL 文件并从 DB 删除
pub async fn archive_events_before_seq(
    pool: &SqlitePool,
    session_id: &str,
    before_seq: i64,
) -> Result<ArchiveResult> {
    use tokio::io::AsyncWriteExt;

    // 1. 读取要归档的 Event
    let rows = sqlx::query(
        "SELECT session_id, seq, turn_id, step_id, event_type, schema_version, payload, created_at
         FROM agent_events
         WHERE session_id = ?1 AND seq < ?2
         ORDER BY seq",
    )
    .bind(session_id)
    .bind(before_seq)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(ArchiveResult { archived: 0, path: String::new() });
    }

    let count = rows.len();

    // 2. 写入 JSONL 文件
    let archive_dir = std::path::Path::new("archives").join(session_id);
    tokio::fs::create_dir_all(&archive_dir).await.map_err(|e| {
        AppError::Internal(format!("Failed to create archive dir: {}", e))
    })?;

    let archive_path = archive_dir.join(format!("events_1_{}.jsonl", before_seq));
    let mut file = tokio::fs::File::create(&archive_path).await.map_err(|e| {
        AppError::Internal(format!("Failed to create archive file: {}", e))
    })?;

    for row in &rows {
        let event = event_from_row(row)?;
        let mut line = serde_json::to_string(&event).unwrap_or_default();
        line.push('\n');
        file.write_all(line.as_bytes()).await.map_err(|e| {
            AppError::Internal(format!("Failed to write archive: {}", e))
        })?;
    }

    // 3. 从热存储删除
    sqlx::query(
        "DELETE FROM agent_events WHERE session_id = ?1 AND seq < ?2",
    )
    .bind(session_id)
    .bind(before_seq)
    .execute(pool)
    .await?;

    let path_str = archive_path.to_string_lossy().to_string();
    tracing::info!(
        "Archived {} events for session {} to {}",
        count, session_id, path_str
    );

    Ok(ArchiveResult {
        archived: count,
        path: path_str,
    })
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;

    async fn setup_test_db() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("Failed to create test DB");
        run_migrations_on(&pool).await.expect("Migration failed");
        pool
    }

    #[tokio::test]
    async fn test_create_session() {
        let pool = setup_test_db().await;
        let event = create_session(&pool, "sess_test_1", "default", "", "test_agent", None)
            .await
            .expect("Failed to create session");

        assert_eq!(event.seq, 1);
        assert_eq!(event.event_type, EventType::SessionStarted);
        assert_eq!(event.turn_id, None);
        assert_eq!(event.step_id, None);

        // 检查 session 存在
        let session = get_session(&pool, "sess_test_1")
            .await
            .expect("Failed to get session")
            .expect("Session not found");
        assert_eq!(session.agent_type, "test_agent");
    }

    #[tokio::test]
    async fn test_create_session_duplicate_fails() {
        let pool = setup_test_db().await;
        create_session(&pool, "sess_dup_1", "default", "", "test", None)
            .await
            .expect("First create should succeed");

        // 重复创建应失败（session_started 唯一性）
        let result = create_session(&pool, "sess_dup_1", "default", "", "test", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_event_and_read_back() {
        let pool = setup_test_db().await;
        create_session(&pool, "sess_rw_1", "default", "", "test", None)
            .await
            .unwrap();

        // 写入 turn_started
        let event = AgentEvent::new(
            "sess_rw_1".into(),
            Some(1),
            None,
            EventType::TurnStarted,
            serde_json::json!({"user_input": "hello", "redo_group": "rg_001", "redo_count": 0}),
        );
        let seq = write_event(&pool, &event).await.unwrap();
        assert_eq!(seq, 2);

        // 读回
        let read = get_event(&pool, "sess_rw_1", seq).await.unwrap().unwrap();
        assert_eq!(read.event_type, EventType::TurnStarted);
        assert_eq!(read.turn_id, Some(1));
        assert_eq!(read.payload["user_input"], "hello");
        assert_eq!(read.payload["redo_group"], "rg_001");
    }

    #[tokio::test]
    async fn test_duplicate_turn_start_fails() {
        let pool = setup_test_db().await;
        create_session(&pool, "sess_ts_1", "default", "", "test", None)
            .await
            .unwrap();

        let event = AgentEvent::new(
            "sess_ts_1".into(),
            Some(1),
            None,
            EventType::TurnStarted,
            serde_json::json!({"user_input": "hi", "redo_group": "rg_1", "redo_count": 0}),
        );
        write_event(&pool, &event).await.unwrap();

        // 重复写入 turn_started 应失败
        let result = write_event(&pool, &event).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_duplicate_session_ended_fails() {
        let pool = setup_test_db().await;
        create_session(&pool, "sess_se_1", "default", "", "test", None)
            .await
            .unwrap();

        let event = AgentEvent::new(
            "sess_se_1".into(),
            None,
            None,
            EventType::SessionEnded,
            serde_json::json!({"reason": "done"}),
        );
        write_event(&pool, &event).await.unwrap();

        let result = write_event(&pool, &event).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_step_lifecycle() {
        let pool = setup_test_db().await;
        create_session(&pool, "sess_step_1", "default", "", "test", None)
            .await
            .unwrap();

        // llm_invoked
        let invoked = AgentEvent::new(
            "sess_step_1".into(),
            Some(1),
            Some("step_001".into()),
            EventType::LlmInvoked,
            serde_json::json!({
                "step_type": "llm_call",
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "hi"}],
                "local_seq": 1
            }),
        );
        let seq1 = write_event(&pool, &invoked).await.unwrap();

        // 重复写 llm_invoked 应失败
        let result = write_event(&pool, &invoked).await;
        assert!(result.is_err());

        // llm_completed
        let completed = AgentEvent::new(
            "sess_step_1".into(),
            Some(1),
            Some("step_001".into()),
            EventType::LlmCompleted,
            serde_json::json!({
                "model": "gpt-4",
                "content": "Hello!",
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
                "finish_reason": "stop",
                "local_seq": 2
            }),
        );
        let seq2 = write_event(&pool, &completed).await.unwrap();
        assert!(seq2 > seq1);

        // 重复写 terminal 应失败
        let result = write_event(&pool, &completed).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_incomplete_turn_detection() {
        let pool = setup_test_db().await;
        create_session(&pool, "sess_inc_1", "default", "", "test", None)
            .await
            .unwrap();

        // 写入 turn_started 但不到 terminal
        let started = AgentEvent::new(
            "sess_inc_1".into(),
            Some(1),
            None,
            EventType::TurnStarted,
            serde_json::json!({"user_input": "hi", "redo_group": "rg_001", "redo_count": 0}),
        );
        write_event(&pool, &started).await.unwrap();

        let incomplete = get_incomplete_turns(&pool, "sess_inc_1")
            .await
            .unwrap();
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].turn_id, 1);
        assert_eq!(incomplete[0].redo_group, "rg_001");

        // 写入 turn_completed
        let completed = AgentEvent::new(
            "sess_inc_1".into(),
            Some(1),
            None,
            EventType::TurnCompleted,
            serde_json::json!({"final_output": "done"}),
        );
        write_event(&pool, &completed).await.unwrap();

        let incomplete = get_incomplete_turns(&pool, "sess_inc_1")
            .await
            .unwrap();
        assert!(incomplete.is_empty());
    }

    #[tokio::test]
    async fn test_batch_write() {
        let pool = setup_test_db().await;
        create_session(&pool, "sess_batch_1", "default", "", "test", None)
            .await
            .unwrap();

        let events: Vec<AgentEvent> = (0..3)
            .map(|i| {
                AgentEvent::new(
                    "sess_batch_1".into(),
                    Some(1),
                    Some(format!("step_{}", i)),
                    EventType::LlmInvoked,
                    serde_json::json!({
                        "step_type": "llm_call",
                        "model": "gpt-4",
                        "messages": [],
                        "local_seq": i + 1
                    }),
                )
            })
            .collect();

        let seqs = write_events_batch(&pool, &events).await.unwrap();
        assert_eq!(seqs.len(), 3);
        assert!(seqs[0] < seqs[1]);
        assert!(seqs[1] < seqs[2]);

        // 验证 sequential
        assert_eq!(seqs[1], seqs[0] + 1);
        assert_eq!(seqs[2], seqs[1] + 1);
    }

    #[tokio::test]
    async fn test_seq_no_gap_on_rollback() {
        let pool = setup_test_db().await;
        create_session(&pool, "sess_gap_1", "default", "", "test", None)
            .await
            .unwrap();

        // 写入一个有效事件
        let valid = AgentEvent::new(
            "sess_gap_1".into(),
            Some(1),
            None,
            EventType::TurnStarted,
            serde_json::json!({"user_input": "hi", "redo_group": "rg_1", "redo_count": 0}),
        );
        let seq1 = write_event(&pool, &valid).await.unwrap();

        // 尝试写入非法事件（会被事务回滚）
        let invalid = AgentEvent::new(
            "sess_gap_1".into(),
            None, // TurnStarted 必须有 turn_id，故意放错
            None,
            EventType::TurnStarted,
            serde_json::json!({"user_input": "bad", "redo_group": "rg_bad", "redo_count": 0}),
        );
        let result = write_event(&pool, &invalid).await;
        assert!(result.is_err());

        // 再写一个合法事件，seq 应连续
        let valid2 = AgentEvent::new(
            "sess_gap_1".into(),
            Some(2),
            None,
            EventType::TurnCompleted,
            serde_json::json!({"final_output": "ok"}),
        );
        let seq2 = write_event(&pool, &valid2).await.unwrap();
        assert_eq!(seq2, seq1 + 1, "seq should be continuous after rollback");

        // 检查无 gap
        let gaps = detect_seq_gaps(&pool, "sess_gap_1").await.unwrap();
        assert!(gaps.is_empty());
    }

    #[tokio::test]
    async fn test_token_usage_stats() {
        let pool = setup_test_db().await;
        create_session(&pool, "sess_tok_1", "default", "", "test", None)
            .await
            .unwrap();

        let event = AgentEvent::new(
            "sess_tok_1".into(),
            Some(1),
            Some("step_tok_1".into()),
            EventType::LlmCompleted,
            serde_json::json!({
                "model": "gpt-4",
                "content": "Hello",
                "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
                "local_seq": 2
            }),
        );
        write_event(&pool, &event).await.unwrap();

        let stats = get_token_usage_stats(&pool, "sess_tok_1")
            .await
            .unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].model, "gpt-4");
        assert_eq!(stats[0].call_count, 1);
        assert_eq!(stats[0].prompt_tokens, 100);
        assert_eq!(stats[0].completion_tokens, 50);
    }

    #[tokio::test]
    async fn test_archive_events() {
        let pool = setup_test_db().await;
        create_session(&pool, "sess_arch", "default", "", "test", None).await.unwrap();

        // 写入几个事件
        for i in 0..3 {
            let event = AgentEvent::new(
                "sess_arch".into(), Some(1), Some(format!("step_{}", i)),
                EventType::LlmCompleted,
                serde_json::json!({"model": "test", "content": format!("msg {}", i), "local_seq": i+1}),
            );
            write_event(&pool, &event).await.unwrap();
        }

        // 归档 seq < 3（保留 seq 1,2，删除 seq 1）
        // Wait, let me re-check — seq starts at 1 for session_started, then 2,3,4
        // Archive before_seq=3 → keeps seq>=3, archives seq<3 → archives seq 1,2
        let result = archive_events_before_seq(&pool, "sess_arch", 3).await.unwrap();
        assert_eq!(result.archived, 2);
        assert!(result.path.contains("sess_arch"));

        // 验证归档文件存在
        assert!(std::path::Path::new(&result.path).exists());

        // 验证热存储中只剩 seq >= 3
        let remaining = sqlx::query(
            "SELECT COUNT(*) as cnt FROM agent_events WHERE session_id = 'sess_arch'"
        ).fetch_one(&pool).await.unwrap();
        let cnt: i64 = remaining.get("cnt");
        assert_eq!(cnt, 2); // seq=3 + seq=4 (the 2 LlmCompleted events after session_started)
        // Actually: session_started seq=1, then 3 llm_completed seq=2,3,4
        // Archive before_seq=3 → removes seq=1,2, keeps seq=3,4 → 2 events

        // 清理
        let _ = std::fs::remove_file(&result.path);
    }

    #[tokio::test]
    async fn test_create_session_with_tenant() {
        let pool = setup_test_db().await;
        // 需要先创建租户
        sqlx::query("INSERT OR IGNORE INTO tenants (id, name) VALUES ('acme-corp', 'ACME Corp')")
            .execute(&pool).await.unwrap();
        let event = create_session(&pool, "sess_tenant_1", "acme-corp", "alice", "test_agent", None)
            .await
            .expect("Failed to create session");

        assert_eq!(event.seq, 1);
        let session = get_session(&pool, "sess_tenant_1").await.unwrap().unwrap();
        assert_eq!(session.tenant_id, "acme-corp");
        assert_eq!(session.user_id, "alice");
    }
}
