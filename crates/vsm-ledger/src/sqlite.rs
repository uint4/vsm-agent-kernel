use crate::{
    EventFilter, GenomeSnapshot, Ledger, LedgerError, LedgerEvent, StoredTrialRecord, TraceWindow,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::{path::Path, sync::Arc};
use tokio::sync::Mutex;
use vsm_core::{GenomeId, NodeId, OrganizationalGenome, SuggestionId, TaskTrace, TraceId};

#[derive(Clone)]
pub struct SqliteLedger {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteLedger {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, LedgerError> {
        let conn = Connection::open(path).map_err(LedgerError::from)?;
        let ledger = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        ledger.initialize().await?;
        Ok(ledger)
    }

    pub async fn in_memory() -> Result<Self, LedgerError> {
        let conn = Connection::open_in_memory().map_err(LedgerError::from)?;
        let ledger = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        ledger.initialize().await?;
        Ok(ledger)
    }

    async fn initialize(&self) -> Result<(), LedgerError> {
        let conn = self.conn.lock().await;
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS ledger_events (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                kind TEXT NOT NULL,
                genome_id TEXT,
                node_id TEXT,
                task_id TEXT,
                directive_id TEXT,
                suggestion_id TEXT,
                correlation_id TEXT,
                event_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_ledger_events_created_at
                ON ledger_events(created_at);
            CREATE INDEX IF NOT EXISTS idx_ledger_events_kind
                ON ledger_events(kind);
            CREATE INDEX IF NOT EXISTS idx_ledger_events_node
                ON ledger_events(node_id);
            CREATE INDEX IF NOT EXISTS idx_ledger_events_task
                ON ledger_events(task_id);
            CREATE INDEX IF NOT EXISTS idx_ledger_events_directive
                ON ledger_events(directive_id);
            CREATE INDEX IF NOT EXISTS idx_ledger_events_correlation
                ON ledger_events(correlation_id);

            CREATE TABLE IF NOT EXISTS task_traces (
                id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                genome_id TEXT NOT NULL,
                assigned_node_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                outcome_score REAL NOT NULL,
                token_total INTEGER NOT NULL,
                trace_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_task_traces_started_at
                ON task_traces(started_at);
            CREATE INDEX IF NOT EXISTS idx_task_traces_task
                ON task_traces(task_id);
            CREATE INDEX IF NOT EXISTS idx_task_traces_node
                ON task_traces(assigned_node_id);
            CREATE INDEX IF NOT EXISTS idx_task_traces_genome
                ON task_traces(genome_id);

            CREATE TABLE IF NOT EXISTS genome_snapshots (
                genome_id TEXT PRIMARY KEY,
                role TEXT NOT NULL,
                saved_at TEXT NOT NULL,
                genome_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_genome_snapshots_role
                ON genome_snapshots(role);
            CREATE INDEX IF NOT EXISTS idx_genome_snapshots_saved_at
                ON genome_snapshots(saved_at);

            CREATE TABLE IF NOT EXISTS champion_genomes (
                controller_node_id TEXT PRIMARY KEY,
                genome_id TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trial_records (
                trial_id TEXT PRIMARY KEY,
                controller_node_id TEXT NOT NULL,
                base_genome_id TEXT NOT NULL,
                candidate_genome_id TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                routed_tasks INTEGER NOT NULL,
                consumed_tokens INTEGER NOT NULL,
                trace_count INTEGER NOT NULL,
                total_score REAL NOT NULL,
                decision TEXT,
                record_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_trial_records_controller_status
                ON trial_records(controller_node_id, status);
            CREATE INDEX IF NOT EXISTS idx_trial_records_candidate
                ON trial_records(candidate_genome_id);
            "#,
        )
        .map_err(LedgerError::from)?;
        Ok(())
    }
}

#[async_trait]
impl Ledger for SqliteLedger {
    async fn append_event(&self, event: LedgerEvent) -> Result<(), LedgerError> {
        let event_json = serde_json::to_string(&event)?;
        let metadata_json = serde_json::to_string(&event.metadata)?;
        let id = event.id.clone();
        let created_at = event.created_at.to_rfc3339();
        let kind = event.kind.as_storage_key();
        let genome_id = event.genome_id.as_ref().map(ToString::to_string);
        let node_id = event.node_id.as_ref().map(ToString::to_string);
        let task_id = event.task_id.as_ref().map(ToString::to_string);
        let directive_id = event.directive_id.as_ref().map(ToString::to_string);
        let suggestion_id = event.suggestion_id.as_ref().map(ToString::to_string);
        let correlation_id = event.correlation_id.clone();

        let conn = self.conn.lock().await;
        conn.execute(
            r#"
            INSERT INTO ledger_events (
                id, created_at, kind, genome_id, node_id, task_id, directive_id,
                suggestion_id, correlation_id, event_json, metadata_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                id,
                created_at,
                kind,
                genome_id,
                node_id,
                task_id,
                directive_id,
                suggestion_id,
                correlation_id,
                event_json,
                metadata_json,
            ],
        )
        .map_err(LedgerError::from)?;
        Ok(())
    }

    async fn write_task_trace(&self, trace: TaskTrace) -> Result<(), LedgerError> {
        let trace_json = serde_json::to_string(&trace)?;
        let completed_at = trace.completed_at.as_ref().map(|ts| ts.to_rfc3339());
        let token_total = trace.token_total() as i64;
        let conn = self.conn.lock().await;
        conn.execute(
            r#"
            INSERT OR REPLACE INTO task_traces (
                id, task_id, genome_id, assigned_node_id, started_at, completed_at,
                outcome_score, token_total, trace_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            params![
                trace.id.to_string(),
                trace.task_id.to_string(),
                trace.genome_id.to_string(),
                trace.assigned_node_id.to_string(),
                trace.started_at.to_rfc3339(),
                completed_at,
                trace.outcome_score,
                token_total,
                trace_json,
            ],
        )
        .map_err(LedgerError::from)?;
        Ok(())
    }

    async fn get_trace(&self, trace_id: &TraceId) -> Result<Option<TaskTrace>, LedgerError> {
        let conn = self.conn.lock().await;
        let payload: Option<String> = conn
            .query_row(
                "SELECT trace_json FROM task_traces WHERE id = ?1",
                params![trace_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(LedgerError::from)?;
        payload
            .map(|json| serde_json::from_str(&json).map_err(LedgerError::from))
            .transpose()
    }

    async fn recent_events(&self, filter: EventFilter) -> Result<Vec<LedgerEvent>, LedgerError> {
        let limit = filter.limit.unwrap_or(500).max(1) as i64;
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT event_json
                FROM ledger_events
                ORDER BY created_at DESC
                LIMIT ?1
                "#,
            )
            .map_err(LedgerError::from)?;
        let rows = stmt
            .query_map(params![limit], |row| row.get::<_, String>(0))
            .map_err(LedgerError::from)?;

        let mut events = Vec::new();
        for row in rows {
            let json = row.map_err(LedgerError::from)?;
            let event: LedgerEvent = serde_json::from_str(&json)?;
            if filter_event(&event, &filter) {
                events.push(event);
            }
        }
        events.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        Ok(events)
    }

    async fn recent_task_traces(&self, window: TraceWindow) -> Result<Vec<TaskTrace>, LedgerError> {
        let limit = window.limit.unwrap_or(500).max(1) as i64;
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT trace_json
                FROM task_traces
                ORDER BY started_at DESC
                LIMIT ?1
                "#,
            )
            .map_err(LedgerError::from)?;
        let rows = stmt
            .query_map(params![limit], |row| row.get::<_, String>(0))
            .map_err(LedgerError::from)?;

        let mut traces = Vec::new();
        for row in rows {
            let json = row.map_err(LedgerError::from)?;
            let trace: TaskTrace = serde_json::from_str(&json)?;
            if window
                .since
                .as_ref()
                .map(|since| &trace.started_at >= since)
                .unwrap_or(true)
            {
                traces.push(trace);
            }
        }
        traces.sort_by(|a, b| a.started_at.cmp(&b.started_at));
        Ok(traces)
    }

    async fn save_genome_snapshot(&self, snapshot: GenomeSnapshot) -> Result<(), LedgerError> {
        let genome_json = serde_json::to_string(&snapshot.genome)?;
        let metadata_json = serde_json::to_string(&snapshot.metadata)?;
        let conn = self.conn.lock().await;
        conn.execute(
            r#"
            INSERT OR REPLACE INTO genome_snapshots (
                genome_id, role, saved_at, genome_json, metadata_json
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                snapshot.genome_id.to_string(),
                snapshot.role.as_storage_key(),
                snapshot.saved_at.to_rfc3339(),
                genome_json,
                metadata_json,
            ],
        )
        .map_err(LedgerError::from)?;
        Ok(())
    }

    async fn get_genome_snapshot(
        &self,
        genome_id: &GenomeId,
    ) -> Result<Option<GenomeSnapshot>, LedgerError> {
        let conn = self.conn.lock().await;
        let row: Option<(String, String, String, String)> = conn
            .query_row(
                "SELECT role, saved_at, genome_json, metadata_json FROM genome_snapshots WHERE genome_id = ?1",
                params![genome_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(LedgerError::from)?;

        row.map(|(role, saved_at, genome_json, metadata_json)| {
            let genome: OrganizationalGenome = serde_json::from_str(&genome_json)?;
            let metadata = serde_json::from_str(&metadata_json)?;
            let saved_at = DateTime::parse_from_rfc3339(&saved_at)
                .map_err(|err| LedgerError::Storage(err.to_string()))?
                .with_timezone(&Utc);
            Ok(GenomeSnapshot {
                genome_id: genome.id.clone(),
                role: crate::GenomeSnapshotRole::from_storage_key(&role),
                saved_at,
                genome,
                metadata,
            })
        })
        .transpose()
    }

    async fn set_champion_genome_id(
        &self,
        controller_node_id: &NodeId,
        genome_id: &GenomeId,
    ) -> Result<(), LedgerError> {
        let conn = self.conn.lock().await;
        conn.execute(
            r#"
            INSERT OR REPLACE INTO champion_genomes (
                controller_node_id, genome_id, updated_at
            ) VALUES (?1, ?2, ?3)
            "#,
            params![
                controller_node_id.to_string(),
                genome_id.to_string(),
                chrono::Utc::now().to_rfc3339(),
            ],
        )
        .map_err(LedgerError::from)?;
        Ok(())
    }

    async fn get_champion_genome_id(
        &self,
        controller_node_id: &NodeId,
    ) -> Result<Option<GenomeId>, LedgerError> {
        let conn = self.conn.lock().await;
        let genome_id: Option<String> = conn
            .query_row(
                "SELECT genome_id FROM champion_genomes WHERE controller_node_id = ?1",
                params![controller_node_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(LedgerError::from)?;
        Ok(genome_id.map(GenomeId::from_string))
    }

    async fn write_trial_record(&self, record: StoredTrialRecord) -> Result<(), LedgerError> {
        let record_json = serde_json::to_string(&record)?;
        let completed_at = record.completed_at.as_ref().map(|ts| ts.to_rfc3339());
        let decision = record
            .decision
            .as_ref()
            .map(crate::StoredTrialDecision::as_storage_key);
        let conn = self.conn.lock().await;
        conn.execute(
            r#"
            INSERT OR REPLACE INTO trial_records (
                trial_id, controller_node_id, base_genome_id, candidate_genome_id,
                status, started_at, updated_at, completed_at, routed_tasks,
                consumed_tokens, trace_count, total_score, decision, record_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
            params![
                record.trial_id.to_string(),
                record.controller_node_id.to_string(),
                record.base_genome_id.to_string(),
                record.candidate_genome_id.to_string(),
                record.status.as_storage_key(),
                record.started_at.to_rfc3339(),
                record.updated_at.to_rfc3339(),
                completed_at,
                record.routed_tasks as i64,
                record.consumed_tokens as i64,
                record.trace_count as i64,
                record.total_score,
                decision,
                record_json,
            ],
        )
        .map_err(LedgerError::from)?;
        Ok(())
    }

    async fn get_trial_record(
        &self,
        trial_id: &SuggestionId,
    ) -> Result<Option<StoredTrialRecord>, LedgerError> {
        let conn = self.conn.lock().await;
        let payload: Option<String> = conn
            .query_row(
                "SELECT record_json FROM trial_records WHERE trial_id = ?1",
                params![trial_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(LedgerError::from)?;
        payload
            .map(|json| serde_json::from_str(&json).map_err(LedgerError::from))
            .transpose()
    }

    async fn get_active_trial_record(
        &self,
        controller_node_id: &NodeId,
    ) -> Result<Option<StoredTrialRecord>, LedgerError> {
        let conn = self.conn.lock().await;
        let payload: Option<String> = conn
            .query_row(
                r#"
                SELECT record_json
                FROM trial_records
                WHERE controller_node_id = ?1 AND status = 'active'
                ORDER BY updated_at DESC
                LIMIT 1
                "#,
                params![controller_node_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(LedgerError::from)?;
        payload
            .map(|json| serde_json::from_str(&json).map_err(LedgerError::from))
            .transpose()
    }
}

fn filter_event(event: &LedgerEvent, filter: &EventFilter) -> bool {
    if !filter.kinds.is_empty() && !filter.kinds.contains(&event.kind) {
        return false;
    }
    if let Some(node_id) = &filter.node_id {
        if event.node_id.as_ref() != Some(node_id) {
            return false;
        }
    }
    if let Some(task_id) = &filter.task_id {
        if event.task_id.as_ref() != Some(task_id) {
            return false;
        }
    }
    if let Some(directive_id) = &filter.directive_id {
        if event.directive_id.as_ref() != Some(directive_id) {
            return false;
        }
    }
    if let Some(correlation_id) = &filter.correlation_id {
        if event.correlation_id.as_ref() != Some(correlation_id) {
            return false;
        }
    }
    if let Some(since) = filter.since.as_ref() {
        if &event.created_at < since {
            return false;
        }
    }
    true
}
