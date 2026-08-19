//! Interleaved watermark snapshot over SQL Server CDC.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, Result};
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{
    Checkpoint, CheckpointStore, DatabaseSchema, InterleavedSnapshotCheckpoint, Row, SyncManager,
    SyncPhase, TableDefinition, Value,
};
use surreal_sync_runtime::{
    run_interleaved_snapshot_with_resume_and_transforms, InterleavedSnapshotConfig,
    NoopCheckpointer, PkTuple, ReconciliationEvent, SnapshotCheckpointer, SnapshotSignal,
    SnapshotTransforms, TableSpec, WatermarkKind, WatermarkSource,
};
use tracing::info;
use uuid::Uuid;

use crate::from_mssql::catalog::{collect_database_schema, MssqlTableMeta, TableSyncKind};
use crate::from_mssql::cdc::{self, CdcChange, CdcOperation};
use crate::from_mssql::checkpoint::{MssqlCheckpoint, MssqlLsn};
use crate::from_mssql::client::MssqlClient;
use crate::from_mssql::regular;
use crate::from_mssql::schema::{database_schema, schemafull_extras, temporal_targets};
use crate::from_mssql::signal::{self, signal_qualified, SIGNAL_TABLE};
use crate::from_mssql::temporal;
use crate::from_mssql::SourceOpts;

#[derive(Default)]
struct WatermarkIds {
    low: Option<Uuid>,
    high: Option<Uuid>,
}

/// Options for [`MssqlWatermarkSource::connect_with_options`].
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    pub start_at: Option<MssqlCheckpoint>,
    pub tables_filter: Option<Vec<String>>,
    pub cancel: tokio_util::sync::CancellationToken,
}

impl ConnectOptions {
    pub fn with_cancel(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            start_at: None,
            tables_filter: None,
            cancel: tokio_util::sync::CancellationToken::new(),
        }
    }
}

/// SQL Server CDC + keyset snapshot backend for the interleaved watermark loop.
pub struct MssqlWatermarkSource {
    client: MssqlClient,
    db_schema: DatabaseSchema,
    metas: Vec<MssqlTableMeta>,
    by_target: HashMap<String, MssqlTableMeta>,
    tables: Vec<TableSpec>,
    relation_tables: Vec<String>,
    temporal: HashSet<String>,
    from_lsn: Option<MssqlLsn>,
    confirmed: MssqlLsn,
    watermarks: Mutex<WatermarkIds>,
    pending_watermarks: Mutex<HashSet<Uuid>>,
    cancel: tokio_util::sync::CancellationToken,
    schemafull: bool,
    dry_run: bool,
}

impl MssqlWatermarkSource {
    pub async fn connect(from_opts: &SourceOpts) -> Result<Self> {
        Self::connect_with_options(from_opts, ConnectOptions::default()).await
    }

    pub async fn connect_with_options(
        from_opts: &SourceOpts,
        options: ConnectOptions,
    ) -> Result<Self> {
        let client = crate::from_mssql::client::connect(&from_opts.connection_string).await?;
        cdc::ensure_cdc_enabled(&client).await?;
        signal::ensure_signal_table(&client).await?;

        let mut metas = collect_database_schema(&client, &from_opts.tables).await?;
        if let Some(filter) = &options.tables_filter {
            let allowed: HashSet<_> = filter.iter().cloned().collect();
            metas.retain(|m| allowed.contains(&m.target) || allowed.contains(&m.source.dotted()));
        }
        for meta in &metas {
            cdc::ensure_table_cdc(&client, &meta.source).await?;
        }

        let mut from_lsn = options.start_at.map(|c| c.lsn);
        if from_lsn.is_none() {
            // Wait until CDC has a max LSN so we do not replay an empty history.
            for _ in 0..50 {
                if let Some(lsn) = cdc::max_lsn(&client).await? {
                    from_lsn = Some(lsn);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        let confirmed = from_lsn.clone().unwrap_or_else(|| MssqlLsn(vec![0; 10]));

        let db_schema = database_schema(&metas);
        let temporal = temporal_targets(&metas);
        let tables = metas
            .iter()
            .map(|m| TableSpec::new(m.target.clone(), keyset_columns(m)))
            .collect();
        let by_target = metas
            .iter()
            .map(|m| (m.target.clone(), m.clone()))
            .collect();

        Ok(Self {
            client,
            db_schema,
            metas,
            by_target,
            tables,
            relation_tables: from_opts.relation_tables.clone(),
            temporal,
            from_lsn,
            confirmed,
            watermarks: Mutex::new(WatermarkIds::default()),
            pending_watermarks: Mutex::new(HashSet::new()),
            cancel: options.cancel,
            schemafull: from_opts.schemafull,
            dry_run: from_opts.dry_run,
        })
    }

    pub async fn resolve_snapshot_table_names(from_opts: &SourceOpts) -> Result<Vec<String>> {
        let client = crate::from_mssql::client::connect(&from_opts.connection_string).await?;
        let metas = collect_database_schema(&client, &from_opts.tables).await?;
        Ok(metas.into_iter().map(|m| m.target).collect())
    }

    pub fn start_checkpoint(&self) -> MssqlCheckpoint {
        MssqlCheckpoint::new(self.confirmed.clone())
    }

    pub fn with_cancel(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    fn table_def(&self, target: &str) -> Option<&TableDefinition> {
        self.db_schema.get_table(target)
    }

    async fn poll_cdc_once(&mut self) -> Result<Vec<ReconciliationEvent<MssqlLsn>>> {
        let Some(to) = cdc::max_lsn(&self.client).await? else {
            return Ok(Vec::new());
        };
        let mut events = Vec::new();
        for meta in &self.metas {
            let changes =
                cdc::poll_changes(&self.client, meta, self.from_lsn.as_ref(), &to).await?;
            for ch in changes {
                if let Some(ev) = self.change_to_event(meta, &ch)? {
                    events.push(ev);
                }
            }
        }
        let signal_changes = cdc::poll_signal_changes(
            &self.client,
            &signal_qualified(),
            self.from_lsn.as_ref(),
            &to,
        )
        .await?;
        for ch in signal_changes {
            if let Some(ev) = signal_event(&ch) {
                let (low, high) = {
                    let guard = self.watermarks.lock().expect("watermark lock");
                    (guard.low, guard.high)
                };
                let mut pending = self.pending_watermarks.lock().expect("pending lock");
                if let Some(id) = ev.pk.single_uuid() {
                    if Some(id) != low && Some(id) != high {
                        continue;
                    }
                    pending.remove(&id);
                }
                events.push(ev);
            }
        }
        Ok(events)
    }

    fn change_to_event(
        &self,
        meta: &MssqlTableMeta,
        change: &CdcChange,
    ) -> Result<Option<ReconciliationEvent<MssqlLsn>>> {
        if change.operation == CdcOperation::UpdateBefore && meta.kind == TableSyncKind::Regular {
            return Ok(None);
        }
        match meta.kind {
            TableSyncKind::Regular => match regular::apply_change(
                meta,
                change,
                self.table_def(&meta.target),
                &self.relation_tables,
                &self.temporal,
            )? {
                regular::RegularCdc::Row(c) => {
                    let pk = pk_from_change(&c, &meta.pk_columns);
                    Ok(Some(ReconciliationEvent {
                        position: change.start_lsn.clone(),
                        table: meta.target.clone(),
                        pk,
                        change: c,
                    }))
                }
                regular::RegularCdc::Relation(r) => {
                    let pk = PkTuple::new(vec![r.relation.id.clone()]);
                    let row_change = match r.operation {
                        surreal_sync_core::ChangeOp::Delete => {
                            surreal_sync_core::Change::delete(&meta.target, r.relation.id.clone())
                        }
                        op => surreal_sync_core::Change::new(
                            op,
                            &meta.target,
                            r.relation.id.clone(),
                            Some(regular::relation_fields(&r.relation)),
                        ),
                    };
                    Ok(Some(ReconciliationEvent {
                        position: change.start_lsn.clone(),
                        table: meta.target.clone(),
                        pk,
                        change: row_change,
                    }))
                }
            },
            TableSyncKind::Temporal => {
                let action = temporal::apply_change(
                    meta,
                    change,
                    self.table_def(&meta.target),
                    &self.temporal,
                )?;
                let Some(c) = action.new_version else {
                    return Ok(None);
                };
                let pk = pk_from_fields(&change.fields, &meta.pk_columns);
                Ok(Some(ReconciliationEvent {
                    position: change.start_lsn.clone(),
                    table: meta.target.clone(),
                    pk,
                    change: c,
                }))
            }
        }
    }
}

fn keyset_columns(meta: &MssqlTableMeta) -> Vec<String> {
    match meta.kind {
        TableSyncKind::Regular => meta.pk_columns.clone(),
        TableSyncKind::Temporal => {
            let mut c = meta.pk_columns.clone();
            c.push(
                meta.period_start
                    .clone()
                    .unwrap_or_else(|| "ValidFrom".into()),
            );
            c.push(meta.period_end.clone().unwrap_or_else(|| "ValidTo".into()));
            c
        }
    }
}

fn pk_from_change(change: &surreal_sync_core::Change, pk_columns: &[String]) -> PkTuple {
    if let Some(fields) = &change.fields {
        return pk_from_fields(fields, pk_columns);
    }
    PkTuple::new(vec![change.id.clone()])
}

fn pk_from_fields(fields: &HashMap<String, Value>, pk_columns: &[String]) -> PkTuple {
    PkTuple::new(
        pk_columns
            .iter()
            .map(|c| fields.get(c).cloned().unwrap_or(Value::Null))
            .collect(),
    )
}

fn signal_event(change: &CdcChange) -> Option<ReconciliationEvent<MssqlLsn>> {
    if change.operation != CdcOperation::Insert {
        return None;
    }
    let id = match change.fields.get("id") {
        Some(Value::Uuid(u)) => *u,
        _ => return None,
    };
    Some(ReconciliationEvent {
        position: change.start_lsn.clone(),
        table: SIGNAL_TABLE.to_string(),
        pk: PkTuple::new(vec![Value::Uuid(id)]),
        change: surreal_sync_core::Change::create(
            SIGNAL_TABLE,
            Value::Uuid(id),
            change.fields.clone(),
        ),
    })
}

#[async_trait::async_trait]
impl WatermarkSource for MssqlWatermarkSource {
    type Position = MssqlLsn;

    async fn snapshot_tables(&self) -> Result<Vec<TableSpec>> {
        Ok(self.tables.clone())
    }

    async fn read_chunk(
        &self,
        table: &TableSpec,
        after: Option<&PkTuple>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        let meta = self
            .by_target
            .get(&table.table)
            .ok_or_else(|| anyhow!("unknown snapshot table `{}`", table.table))?;
        let after_vals = after.map(|p| p.0.as_slice());
        match meta.kind {
            TableSyncKind::Regular => {
                let maps = regular::read_chunk(&self.client, meta, after_vals, limit).await?;
                Ok(regular::snapshot_rows(
                    meta,
                    maps,
                    0,
                    self.table_def(&meta.target),
                    &self.relation_tables,
                    &self.temporal,
                ))
            }
            TableSyncKind::Temporal => {
                let maps = temporal::read_chunk(&self.client, meta, after_vals, limit).await?;
                Ok(temporal::rows_from_maps(
                    meta,
                    maps,
                    0,
                    self.table_def(&meta.target),
                    &self.temporal,
                ))
            }
        }
    }

    async fn write_watermark(&self, kind: WatermarkKind, id: Uuid) -> Result<()> {
        let kind_str = match kind {
            WatermarkKind::Low => "low",
            WatermarkKind::High => "high",
        };
        signal::insert_watermark(&self.client, kind_str, id).await?;
        {
            let mut guard = self.watermarks.lock().expect("watermark lock");
            match kind {
                WatermarkKind::Low => guard.low = Some(id),
                WatermarkKind::High => guard.high = Some(id),
            }
        }
        self.pending_watermarks
            .lock()
            .expect("pending lock")
            .insert(id);
        Ok(())
    }

    async fn next_reconciliation_events(
        &mut self,
    ) -> Result<Vec<ReconciliationEvent<Self::Position>>> {
        for attempt in 0..40 {
            if self.cancel.is_cancelled() {
                return Err(anyhow!("interleaved snapshot cancelled"));
            }
            let events = self.poll_cdc_once().await?;
            let pending = self.pending_watermarks.lock().expect("pending lock").len();
            if !events.is_empty() || pending == 0 {
                if events.is_empty() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                return Ok(events);
            }
            // Wait for the Agent capture job to land the watermark insert.
            tokio::time::sleep(Duration::from_millis(150)).await;
            if attempt == 39 {
                anyhow::bail!(
                    "CDC did not capture watermark rows in time. {}",
                    "SQL Server Agent must be running (Linux containers: MSSQL_AGENT_ENABLED=true)."
                );
            }
        }
        Ok(Vec::new())
    }

    async fn current_position(&self) -> Result<Self::Position> {
        Ok(self.confirmed.clone())
    }

    async fn commit_reconciled(&mut self, position: Self::Position) -> Result<()> {
        self.from_lsn = Some(position.clone());
        self.confirmed = position;
        Ok(())
    }

    async fn read_signals(&mut self) -> Result<Vec<SnapshotSignal>> {
        Ok(Vec::new())
    }

    async fn resolve_tables(&self, names: &[String]) -> Result<Vec<TableSpec>> {
        Ok(self
            .tables
            .iter()
            .filter(|t| names.iter().any(|n| n == &t.table))
            .cloned()
            .collect())
    }
}

/// Options controlling interleaved snapshot restart and table selection.
#[derive(Clone, Debug)]
pub struct InterleavedFullSyncOptions {
    pub resume_progress: Option<InterleavedSnapshotCheckpoint>,
    pub tables_filter: Option<Vec<String>>,
    pub start_at: Option<MssqlCheckpoint>,
    pub emit_full_sync_start: bool,
}

impl Default for InterleavedFullSyncOptions {
    fn default() -> Self {
        Self {
            resume_progress: None,
            tables_filter: None,
            start_at: None,
            emit_full_sync_start: true,
        }
    }
}

/// Outcome of an interleaved snapshot full sync.
#[derive(Debug, Clone)]
pub struct InterleavedFullSyncOutcome {
    pub start: MssqlCheckpoint,
    pub end: MssqlCheckpoint,
    pub cancelled: bool,
}

pub async fn run_interleaved_snapshot_full_sync<S, St>(
    surreal: &S,
    from_opts: &SourceOpts,
    chunk_size: usize,
    cancel: tokio_util::sync::CancellationToken,
    manager: Option<&SyncManager<St>>,
    options: InterleavedFullSyncOptions,
) -> Result<InterleavedFullSyncOutcome>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let transforms = SnapshotTransforms::identity();
    run_interleaved_snapshot_full_sync_with_transforms(
        surreal,
        from_opts,
        chunk_size,
        cancel,
        manager,
        options,
        &transforms,
    )
    .await
}

pub async fn run_interleaved_snapshot_full_sync_with_transforms<S, St>(
    surreal: &S,
    from_opts: &SourceOpts,
    chunk_size: usize,
    cancel: tokio_util::sync::CancellationToken,
    manager: Option<&SyncManager<St>>,
    options: InterleavedFullSyncOptions,
    transforms: &SnapshotTransforms,
) -> Result<InterleavedFullSyncOutcome>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let connect_opts = ConnectOptions {
        start_at: options.start_at.clone(),
        tables_filter: options.tables_filter.clone(),
        cancel: cancel.clone(),
    };
    let mut source = MssqlWatermarkSource::connect_with_options(from_opts, connect_opts).await?;

    let extras = schemafull_extras(&source.metas, from_opts.relation_tables.clone());
    surreal_sync_core::maybe_emit_schemafull(
        surreal,
        &source.db_schema,
        &extras,
        source.schemafull,
        source.dry_run,
    )
    .await?;

    let start = if options.emit_full_sync_start {
        let start = source.start_checkpoint();
        if let Some(manager) = manager {
            manager
                .emit_checkpoint(&start, SyncPhase::FullSyncStart)
                .await?;
            info!(
                "Emitted interleaved snapshot start checkpoint: {}",
                start.to_cli_string()
            );
        }
        start
    } else {
        source.start_checkpoint()
    };

    let config = InterleavedSnapshotConfig { chunk_size };
    let resume_ref = options.resume_progress.as_ref();
    let snapshot_result = if let Some(manager) = manager {
        let mut checkpointer = ManagerRefCheckpointer { manager };
        run_interleaved_snapshot_with_resume_and_transforms(
            &mut source,
            surreal,
            &config,
            &mut checkpointer,
            resume_ref,
            transforms,
        )
        .await
    } else {
        run_interleaved_snapshot_with_resume_and_transforms(
            &mut source,
            surreal,
            &config,
            &mut NoopCheckpointer,
            resume_ref,
            transforms,
        )
        .await
    };

    match snapshot_result {
        Ok(result) => {
            let end = MssqlCheckpoint::new(result.final_position);
            info!(
                "SQL Server watermark snapshot complete (final LSN {}, peak buffered rows: {})",
                end.lsn, result.peak_buffered_rows
            );
            if let Some(manager) = manager {
                manager
                    .emit_checkpoint(&end, SyncPhase::FullSyncEnd)
                    .await?;
            }
            Ok(InterleavedFullSyncOutcome {
                start,
                end,
                cancelled: false,
            })
        }
        Err(e) if cancel.is_cancelled() => {
            info!("Interleaved snapshot cancelled ({e}); FullSyncStart remains the resume point");
            Ok(InterleavedFullSyncOutcome {
                end: start.clone(),
                start,
                cancelled: true,
            })
        }
        Err(e) => Err(e),
    }
}

struct ManagerRefCheckpointer<'a, St: CheckpointStore> {
    manager: &'a SyncManager<St>,
}

#[async_trait::async_trait]
impl<St: CheckpointStore> SnapshotCheckpointer for ManagerRefCheckpointer<'_, St> {
    async fn save_progress(&mut self, checkpoint: &InterleavedSnapshotCheckpoint) -> Result<()> {
        self.manager
            .emit_checkpoint(checkpoint, SyncPhase::SnapshotProgress)
            .await?;
        Ok(())
    }
}

/// Result of the initial interleaved snapshot planning step.
#[derive(Debug, Clone)]
pub struct InitialInterleavedOutcome {
    pub snapshot_skipped: bool,
    pub sync_outcome: Option<InterleavedFullSyncOutcome>,
}

pub async fn run_initial_interleaved_snapshot<S, St>(
    surreal: &S,
    from_opts: &SourceOpts,
    chunk_size: usize,
    cancel: tokio_util::sync::CancellationToken,
    manager: Option<&SyncManager<St>>,
) -> Result<InitialInterleavedOutcome>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let transforms = SnapshotTransforms::identity();
    run_initial_interleaved_snapshot_with_transforms(
        surreal,
        from_opts,
        chunk_size,
        cancel,
        manager,
        &transforms,
    )
    .await
}

pub async fn run_initial_interleaved_snapshot_with_transforms<S, St>(
    surreal: &S,
    from_opts: &SourceOpts,
    chunk_size: usize,
    cancel: tokio_util::sync::CancellationToken,
    manager: Option<&SyncManager<St>>,
    transforms: &SnapshotTransforms,
) -> Result<InitialInterleavedOutcome>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    if let Some(manager) = manager {
        if let Ok(progress) = manager
            .read_checkpoint::<InterleavedSnapshotCheckpoint>(SyncPhase::SnapshotProgress)
            .await
        {
            if !progress.all_done() {
                info!("Resuming interleaved snapshot from saved per-chunk progress");
                let start_at = resume_checkpoint_from_progress(&progress)?;
                let outcome = run_interleaved_snapshot_full_sync_with_transforms(
                    surreal,
                    from_opts,
                    chunk_size,
                    cancel,
                    Some(manager),
                    InterleavedFullSyncOptions {
                        resume_progress: Some(progress),
                        start_at: Some(start_at),
                        emit_full_sync_start: false,
                        ..InterleavedFullSyncOptions::default()
                    },
                    transforms,
                )
                .await?;
                return Ok(InitialInterleavedOutcome {
                    snapshot_skipped: false,
                    sync_outcome: Some(outcome),
                });
            }
        }
        if let Ok(end) = manager
            .read_checkpoint::<MssqlCheckpoint>(SyncPhase::FullSyncEnd)
            .await
        {
            info!(
                "Snapshot already complete; skipping snapshot and streaming from {}",
                end.to_cli_string()
            );
            return Ok(InitialInterleavedOutcome {
                snapshot_skipped: true,
                sync_outcome: Some(InterleavedFullSyncOutcome {
                    start: end.clone(),
                    end,
                    cancelled: false,
                }),
            });
        }
    }

    let outcome = run_interleaved_snapshot_full_sync_with_transforms(
        surreal,
        from_opts,
        chunk_size,
        cancel,
        manager,
        InterleavedFullSyncOptions {
            emit_full_sync_start: true,
            ..InterleavedFullSyncOptions::default()
        },
        transforms,
    )
    .await?;
    Ok(InitialInterleavedOutcome {
        snapshot_skipped: false,
        sync_outcome: Some(outcome),
    })
}

fn resume_checkpoint_from_progress(
    progress: &InterleavedSnapshotCheckpoint,
) -> Result<MssqlCheckpoint> {
    let lsn: MssqlLsn =
        serde_json::from_value(progress.reconciliation_pos.clone()).or_else(|_| {
            progress
                .reconciliation_pos
                .as_str()
                .ok_or_else(|| anyhow!("cannot parse snapshot LSN"))
                .and_then(MssqlLsn::from_hex)
        })?;
    Ok(MssqlCheckpoint::new(lsn))
}
