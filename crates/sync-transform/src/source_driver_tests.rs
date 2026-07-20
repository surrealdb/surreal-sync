//! SourceDriver / run_source_runtime tests.

use crate::test_support::{RecordingSink, ScriptedSourceDriver};
use crate::{
    run_source_runtime, run_source_runtime_with, write_relations, ApplyEvent, ApplyOpts,
    CheckpointPolicy, ControlSignal, Pipeline, PositionedEvent, RuntimeExit, SourceDriver,
    SourceRuntimeOpts, StopReason,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use sync_core::{
    UniversalChange, UniversalRelation, UniversalRelationChange, UniversalThingRef, UniversalValue,
};

fn change(id: i64) -> UniversalChange {
    let mut data = HashMap::new();
    data.insert(
        "name".to_string(),
        UniversalValue::VarChar {
            value: format!("row-{id}"),
            length: 64,
        },
    );
    UniversalChange::create("users", UniversalValue::Int64(id), data)
}

fn relation(id: i64) -> UniversalRelation {
    UniversalRelation::new(
        "follows",
        UniversalValue::Int64(id),
        UniversalThingRef::new("users", UniversalValue::Int64(id)),
        UniversalThingRef::new("users", UniversalValue::Int64(id + 1)),
        HashMap::new(),
    )
}

fn opts() -> ApplyOpts {
    ApplyOpts::default()
        .with_max_in_flight(1)
        .with_batch_size(1)
        .with_batch_max_wait(Duration::from_millis(20))
        .with_timeout(Duration::from_secs(5))
}

#[tokio::test]
async fn run_source_runtime_identity_change_feed_like() {
    let mut driver = ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::change(change(2), 20u64),
    ]);
    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();
    let runtime_opts = SourceRuntimeOpts::default();

    let exit = run_source_runtime(&mut driver, &sink, &pipeline, &apply_opts, &runtime_opts)
        .await
        .unwrap();
    assert_eq!(exit, RuntimeExit::Stopped(StopReason::Finished));
    assert_eq!(sink.applied().len(), 2);
    assert_eq!(driver.commits, vec![10, 20]);
    // Default policy persists after each sink-safe commit.
    assert_eq!(driver.persisted, vec![10, 20]);
}

#[tokio::test]
async fn persist_checkpoint_only_after_sink_commit_only_skips() {
    let mut driver = ScriptedSourceDriver::new(vec![PositionedEvent::change(change(1), 10u64)])
        .commit_only();
    assert_eq!(driver.policy, CheckpointPolicy::CommitOnly);

    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();

    run_source_runtime(
        &mut driver,
        &sink,
        &pipeline,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(driver.commits, vec![10]);
    assert!(
        driver.persisted.is_empty(),
        "CommitOnly must not call persist_checkpoint"
    );
    assert_eq!(sink.applied().len(), 1);
}

#[tokio::test]
async fn stop_reason_cancel_after_polls() {
    let mut driver = ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::change(change(2), 20u64),
        PositionedEvent::change(change(3), 30u64),
    ])
    .cancel_after_polls(2);
    // Don't finish on empty so cancel path is exercised with remaining work.
    driver.finished_when_empty = false;

    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();

    let exit = run_source_runtime(
        &mut driver,
        &sink,
        &pipeline,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(exit, RuntimeExit::Stopped(StopReason::Cancelled));
    // First item sunk+committed; cancel on 2nd poll may leave later unsunk.
    assert!(!driver.commits.is_empty());
    assert!(driver.commits[0] == 10);
    // persist only for sunk commits
    assert_eq!(driver.persisted, driver.commits);
}

#[tokio::test]
async fn runtime_opts_deadline_stops() {
    let mut driver = ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::change(change(2), 20u64),
    ]);
    driver.finished_when_empty = false;

    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();
    let runtime_opts = SourceRuntimeOpts::new().with_deadline(Instant::now());

    let exit = run_source_runtime(&mut driver, &sink, &pipeline, &apply_opts, &runtime_opts)
        .await
        .unwrap();
    assert_eq!(exit, RuntimeExit::Stopped(StopReason::Deadline));
}

#[tokio::test]
async fn between_events_schema_and_adhoc_hooks() {
    let mut driver = ScriptedSourceDriver::new(vec![PositionedEvent::change(change(1), 10u64)])
        .with_signals(vec![
            ControlSignal::SchemaRefresh,
            ControlSignal::AdHocSnapshot {
                tables: vec!["users".into(), "orders".into()],
            },
        ]);

    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();

    run_source_runtime(
        &mut driver,
        &sink,
        &pipeline,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(driver.schema_refresh_count, 1);
    assert_eq!(
        driver.adhoc_snapshots,
        vec![vec!["users".to_string(), "orders".to_string()]]
    );
    assert_eq!(sink.applied().len(), 1);
}

#[tokio::test]
async fn write_relations_identity() {
    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let opts = ApplyOpts::default();
    write_relations(&sink, &pipeline, vec![relation(1), relation(2)], &opts)
        .await
        .unwrap();
    assert_eq!(sink.relations_written().len(), 1);
    assert_eq!(sink.relations_written()[0].len(), 2);
}

#[tokio::test]
async fn mixed_relation_and_change_events_ordered() {
    let mut driver = ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::new(
            ApplyEvent::relation_change(UniversalRelationChange::create(relation(5))),
            20u64,
        ),
        PositionedEvent::change(change(2), 30u64),
    ]);
    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();

    run_source_runtime(
        &mut driver,
        &sink,
        &pipeline,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        sink.apply_order_tags(),
        vec![
            "change:1".to_string(),
            "relation:5".to_string(),
            "change:2".to_string(),
        ]
    );
    assert_eq!(driver.commits, vec![10, 20, 30]);
    assert_eq!(driver.persisted, vec![10, 20, 30]);
}

#[tokio::test]
async fn interval_when_drained_persists_once_after_commits() {
    // batch_size=1 + max_in_flight=1 ⇒ each item drains fully before the next.
    // IntervalWhenDrained with ZERO interval ⇒ persist on every drain (coalesced
    // to one persist per drained commit watermark).
    let mut driver = ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::change(change(2), 20u64),
    ])
    .interval_when_drained(Duration::ZERO);

    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();

    run_source_runtime(
        &mut driver,
        &sink,
        &pipeline,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(driver.commits, vec![10, 20]);
    // Each commit left the window drained with interval=0 ⇒ persist each time.
    assert_eq!(driver.persisted, vec![10, 20]);
    assert_eq!(sink.applied().len(), 2);
}

#[tokio::test]
async fn interval_when_drained_defers_until_window_empty() {
    // W=2 + slow transforms: both batches stay in-flight before either sinks.
    // Long interval ⇒ no mid-window persist; after both commit the window drains
    // and IntervalWhenDrained persists the sunk watermark promptly (coalesced
    // pending was 20). Force-flush on finish is a no-op if already persisted.
    use crate::test_support::{BatchScript, ScriptedTransformer};
    use std::sync::Arc;

    let mut driver = ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::change(change(2), 20u64),
    ])
    .interval_when_drained(Duration::from_secs(3600));
    driver.finished_when_empty = true;

    let sink = RecordingSink::new();
    let transformer = Arc::new(
        ScriptedTransformer::new(Pipeline::new())
            .on_batch(1, BatchScript::succeed_after(Duration::from_millis(40)))
            .on_batch(2, BatchScript::succeed_after(Duration::from_millis(40))),
    );
    let apply_opts = ApplyOpts::default()
        .with_max_in_flight(2)
        .with_batch_size(1)
        .with_batch_max_wait(Duration::from_millis(20))
        .with_timeout(Duration::from_secs(5));

    crate::run_source_runtime_with(
        &mut driver,
        &sink,
        transformer,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(driver.commits, vec![10, 20]);
    assert_eq!(
        driver.persisted,
        vec![20],
        "IntervalWhenDrained must not persist mid-window; only the last sunk watermark after drain"
    );
}

#[tokio::test]
async fn interval_when_drained_persists_sunk_promptly_when_already_drained() {
    // Identity / W=1: each commit leaves the window empty → persist promptly
    // (last-sunk-on-sink cadence), even with a long interval.
    let mut driver = ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::change(change(2), 20u64),
    ])
    .interval_when_drained(Duration::from_secs(3600));

    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();

    run_source_runtime(
        &mut driver,
        &sink,
        &pipeline,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(driver.commits, vec![10, 20]);
    assert_eq!(
        driver.persisted,
        vec![10, 20],
        "drained commits must persist sunk watermarks without waiting for interval"
    );
}

#[tokio::test]
async fn adhoc_snapshot_receives_apply_helpers_and_can_write() {
    use sync_core::UniversalRow;

    struct AdhocWriter {
        remaining: Vec<PositionedEvent<u64>>,
        commits: Vec<u64>,
        wrote_via_adhoc: bool,
    }

    #[async_trait::async_trait]
    impl SourceDriver for AdhocWriter {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<Self::Position>>> {
            if self.remaining.is_empty() {
                return Ok(Vec::new());
            }
            Ok(vec![self.remaining.remove(0)])
        }

        async fn commit(&mut self, position: Self::Position) -> anyhow::Result<()> {
            self.commits.push(position);
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.remaining.is_empty()
        }

        async fn between_events(&mut self) -> anyhow::Result<Vec<ControlSignal>> {
            if self.wrote_via_adhoc {
                return Ok(Vec::new());
            }
            Ok(vec![ControlSignal::AdHocSnapshot {
                tables: vec!["users".into()],
            }])
        }

        async fn on_adhoc_snapshot(
            &mut self,
            tables: &[String],
            apply: &dyn crate::AdhocApply,
        ) -> anyhow::Result<()> {
            assert_eq!(tables, &["users".to_string()]);
            let mut data = HashMap::new();
            data.insert(
                "name".to_string(),
                UniversalValue::VarChar {
                    value: "adhoc".into(),
                    length: 64,
                },
            );
            let row = UniversalRow::new("users", 0, UniversalValue::Int64(99), data);
            apply.write_rows(vec![row]).await?;
            self.wrote_via_adhoc = true;
            Ok(())
        }
    }

    let mut driver = AdhocWriter {
        remaining: vec![PositionedEvent::change(change(1), 10u64)],
        commits: Vec::new(),
        wrote_via_adhoc: false,
    };
    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();

    run_source_runtime(
        &mut driver,
        &sink,
        &pipeline,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert!(driver.wrote_via_adhoc);
    assert_eq!(sink.applied().len(), 1);
    assert_eq!(sink.rows_written().len(), 1);
    assert_eq!(sink.rows_written()[0][0].id, UniversalValue::Int64(99));
}

#[tokio::test]
async fn interval_when_drained_persists_filtered_read_progress() {
    // No work items (nothing to commit) but driver reports read progress — e.g.
    // filtered-only catch-up. IntervalWhenDrained must still persist when drained.
    let mut driver = ScriptedSourceDriver::new(Vec::<PositionedEvent<u64>>::new())
        .interval_when_drained(Duration::ZERO)
        .with_read_progress(99u64)
        .cancel_after_polls(3);
    driver.finished_when_empty = false;

    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();

    run_source_runtime(
        &mut driver,
        &sink,
        &pipeline,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert!(driver.commits.is_empty(), "filtered-only path must not commit");
    assert!(
        driver.persisted.contains(&99),
        "drained IntervalWhenDrained must persist read_progress; got {:?}",
        driver.persisted
    );
}

#[tokio::test]
async fn note_sunk_events_counts_after_sink_success() {
    let mut driver = ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::change(change(2), 20u64),
    ]);

    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = opts();

    run_source_runtime(
        &mut driver,
        &sink,
        &pipeline,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(driver.sunk_events, 2);
    assert_eq!(driver.commits, vec![10, 20]);
}

/// Regression: failure_policy=Skip must still call `note_sunk_events` so drivers
/// that gate slot/cursor advance on sunk counts (e.g. wal2json) do not stall.
#[tokio::test]
async fn failure_policy_skip_still_notes_sunk_events() {
    use crate::test_support::{BatchScript, ScriptedTransformer};
    use crate::FailurePolicy;
    use std::sync::Arc;

    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::fail_after(Duration::ZERO, "skip-me"))
        .on_batch(2, BatchScript::succeed_after(Duration::ZERO));

    let mut driver = ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::change(change(2), 20u64),
    ])
    .commit_only();

    let sink = RecordingSink::new();
    let apply_opts = opts().with_failure_policy(FailurePolicy::Skip);

    run_source_runtime_with(
        &mut driver,
        &sink,
        Arc::new(transformer),
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert!(
        sink.applied().is_empty() || sink.applied().len() == 1,
        "failed batch must not be sunk; successor may be: {:?}",
        sink.applied()
    );
    assert_eq!(
        sink.applied().len(),
        1,
        "successor batch should still sink"
    );
    assert_eq!(
        driver.sunk_events, 2,
        "Skip must note_sunk_events for the failed batch so advance is not stuck"
    );
    assert_eq!(driver.commits, vec![10, 20]);
}

/// wal2json-style gate: commit only advances once every emitted event is noted.
#[tokio::test]
async fn failure_policy_skip_unblocks_gated_commit() {
    use crate::test_support::{BatchScript, ScriptedTransformer};
    use crate::FailurePolicy;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct GatedDriver {
        remaining: Vec<PositionedEvent<u64>>,
        emitted: u64,
        sunk: u64,
        commits: Vec<u64>,
        finished: bool,
    }

    #[async_trait]
    impl SourceDriver for GatedDriver {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<u64>>> {
            if self.finished || self.remaining.is_empty() {
                self.finished = self.remaining.is_empty();
                return Ok(Vec::new());
            }
            // Emit the whole peek batch at once (wal2json-like).
            let events = std::mem::take(&mut self.remaining);
            self.emitted = events.len() as u64;
            self.sunk = 0;
            Ok(events)
        }

        async fn commit(&mut self, position: u64) -> anyhow::Result<()> {
            if self.sunk < self.emitted {
                // Not ready to advance — same stuck path as pre-fix wal2json.
                return Ok(());
            }
            self.commits.push(position);
            self.emitted = 0;
            self.sunk = 0;
            self.finished = true;
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn checkpoint_policy(&self) -> CheckpointPolicy {
            CheckpointPolicy::CommitOnly
        }

        fn note_sunk_events(&mut self, count: u64) {
            self.sunk = self.sunk.saturating_add(count);
        }
    }

    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::fail_after(Duration::ZERO, "skip-me"))
        .on_batch(2, BatchScript::succeed_after(Duration::ZERO));

    let mut driver = GatedDriver {
        remaining: vec![
            PositionedEvent::change(change(1), 100u64),
            PositionedEvent::change(change(2), 100u64), // shared peek LSN
        ],
        emitted: 0,
        sunk: 0,
        commits: Vec::new(),
        finished: false,
    };

    let sink = RecordingSink::new();
    let apply_opts = opts().with_failure_policy(FailurePolicy::Skip);

    run_source_runtime_with(
        &mut driver,
        &sink,
        Arc::new(transformer),
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        driver.commits,
        vec![100],
        "gated commit must advance after Skip notes sunk events"
    );
    assert_eq!(sink.applied().len(), 1);
}
