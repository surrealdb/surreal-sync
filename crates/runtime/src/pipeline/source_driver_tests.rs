//! SourceDriver / run_source_runtime tests.

use crate::pipeline::test_support::{RecordingSink, ScriptedSourceDriver};
use crate::pipeline::{
    run_source_runtime, run_source_runtime_with, write_relations, ApplyEvent, ApplyOpts,
    CheckpointPolicy, ControlSignal, Pipeline, PositionedEvent, RuntimeExit, SourceDriver,
    SourceRuntimeOpts, StopReason,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use surreal_sync_core::{Change, Relation, RelationChange, ThingRef, Value};

fn change(id: i64) -> Change {
    let mut data = HashMap::new();
    data.insert(
        "name".to_string(),
        Value::VarChar {
            value: format!("row-{id}"),
            length: 64,
        },
    );
    Change::create("users", Value::Int64(id), data)
}

fn relation(id: i64) -> Relation {
    Relation::new(
        "follows",
        Value::Int64(id),
        ThingRef::new("users", Value::Int64(id)),
        ThingRef::new("users", Value::Int64(id + 1)),
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
    assert_eq!(driver.advances, vec![10, 20]);
    // Default policy persists after each sink-safe watermark advance.
    assert_eq!(driver.persisted, vec![10, 20]);
}

#[tokio::test]
async fn persist_checkpoint_only_after_sink_advance_only_skips() {
    let mut driver =
        ScriptedSourceDriver::new(vec![PositionedEvent::change(change(1), 10u64)]).advance_only();
    assert_eq!(driver.policy, CheckpointPolicy::AdvanceOnly);

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

    assert_eq!(driver.advances, vec![10]);
    assert!(
        driver.persisted.is_empty(),
        "AdvanceOnly must not call persist_checkpoint"
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
    // First item sunk + watermark advanced; cancel on 2nd poll may leave later unsunk.
    assert!(!driver.advances.is_empty());
    assert!(driver.advances[0] == 10);
    // persist only for sunk advances
    assert_eq!(driver.persisted, driver.advances);
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
            ApplyEvent::relation_change(RelationChange::create(relation(5))),
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
    assert_eq!(driver.advances, vec![10, 20, 30]);
    assert_eq!(driver.persisted, vec![10, 20, 30]);
}

#[tokio::test]
async fn interval_when_drained_persists_once_after_advances() {
    // batch_size=1 + max_in_flight=1 ⇒ each item drains fully before the next.
    // IntervalWhenDrained with ZERO interval ⇒ persist on every drain (coalesced
    // to one persist per drained advance watermark).
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

    assert_eq!(driver.advances, vec![10, 20]);
    // Each advance left the window drained with interval=0 ⇒ persist each time.
    assert_eq!(driver.persisted, vec![10, 20]);
    assert_eq!(sink.applied().len(), 2);
}

#[tokio::test]
async fn interval_when_drained_defers_until_window_empty() {
    // W=2 + slow transforms: both batches stay in-flight before either sinks.
    // Long interval ⇒ no mid-window persist; after both advance the window drains
    // and IntervalWhenDrained persists the sunk watermark promptly (coalesced
    // pending was 20). Force-flush on finish is a no-op if already persisted.
    use crate::pipeline::test_support::{BatchScript, ScriptedTransformer};
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

    crate::pipeline::run_source_runtime_with(
        &mut driver,
        &sink,
        transformer,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(driver.advances, vec![10, 20]);
    assert_eq!(
        driver.persisted,
        vec![20],
        "IntervalWhenDrained must not persist mid-window; only the last sunk watermark after drain"
    );
}

#[tokio::test]
async fn interval_when_drained_persists_sunk_promptly_when_already_drained() {
    // Identity / W=1: each advance leaves the window empty → persist promptly
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

    assert_eq!(driver.advances, vec![10, 20]);
    assert_eq!(
        driver.persisted,
        vec![10, 20],
        "drained advances must persist sunk watermarks without waiting for interval"
    );
}

#[tokio::test]
async fn adhoc_snapshot_receives_apply_helpers_and_can_write() {
    use surreal_sync_core::Row;

    struct AdhocWriter {
        remaining: Vec<PositionedEvent<u64>>,
        advances: Vec<u64>,
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

        async fn advance_watermark(&mut self, position: Self::Position) -> anyhow::Result<()> {
            self.advances.push(position);
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
            apply: &dyn crate::pipeline::AdhocApply,
        ) -> anyhow::Result<()> {
            assert_eq!(tables, &["users".to_string()]);
            let mut data = HashMap::new();
            data.insert(
                "name".to_string(),
                Value::VarChar {
                    value: "adhoc".into(),
                    length: 64,
                },
            );
            let row = Row::new("users", 0, Value::Int64(99), data);
            apply.write_rows(vec![row]).await?;
            self.wrote_via_adhoc = true;
            Ok(())
        }
    }

    let mut driver = AdhocWriter {
        remaining: vec![PositionedEvent::change(change(1), 10u64)],
        advances: Vec::new(),
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
    assert_eq!(sink.rows_written()[0][0].id, Value::Int64(99));
}

#[tokio::test]
async fn interval_when_drained_persists_filtered_read_progress() {
    // No work items (nothing to advance_watermark) but driver reports read progress — e.g.
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

    assert!(
        driver.advances.is_empty(),
        "filtered-only path must not advance_watermark"
    );
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
    assert_eq!(driver.advances, vec![10, 20]);
}

/// Regression: failure_policy=Skip must still call `note_sunk_events` so drivers
/// that gate slot/cursor advance on sunk counts (e.g. wal2json) do not stall.
#[tokio::test]
async fn failure_policy_skip_still_notes_sunk_events() {
    use crate::pipeline::test_support::{BatchScript, ScriptedTransformer};
    use crate::pipeline::FailurePolicy;
    use std::sync::Arc;

    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::fail_after(Duration::ZERO, "skip-me"))
        .on_batch(2, BatchScript::succeed_after(Duration::ZERO));

    let mut driver = ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::change(change(2), 20u64),
    ])
    .advance_only();

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
    assert_eq!(sink.applied().len(), 1, "successor batch should still sink");
    assert_eq!(
        driver.sunk_events, 2,
        "Skip must note_sunk_events for the failed batch so advance is not stuck"
    );
    assert_eq!(driver.advances, vec![10, 20]);
}

/// wal2json-style gate: watermark advances only once every emitted event is noted.
#[tokio::test]
async fn failure_policy_skip_unblocks_gated_advance() {
    use crate::pipeline::test_support::{BatchScript, ScriptedTransformer};
    use crate::pipeline::FailurePolicy;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct GatedDriver {
        remaining: Vec<PositionedEvent<u64>>,
        emitted: u64,
        sunk: u64,
        advances: Vec<u64>,
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

        async fn advance_watermark(&mut self, position: u64) -> anyhow::Result<()> {
            if self.sunk < self.emitted {
                // Not ready to advance — same stuck path as pre-fix wal2json.
                return Ok(());
            }
            self.advances.push(position);
            self.emitted = 0;
            self.sunk = 0;
            self.finished = true;
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn checkpoint_policy(&self) -> CheckpointPolicy {
            CheckpointPolicy::AdvanceOnly
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
        advances: Vec::new(),
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
        driver.advances,
        vec![100],
        "gated advance_watermark must run after Skip notes sunk events"
    );
    assert_eq!(sink.applied().len(), 1);
}

/// Identity + max_in_flight≥2: poll continues while a slow sink holds the first batch.
#[tokio::test]
async fn identity_polls_while_slow_sink_in_flight() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    struct ObservingDriver {
        remaining: Vec<PositionedEvent<u64>>,
        poll_count: Arc<AtomicU64>,
        advances: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait::async_trait]
    impl SourceDriver for ObservingDriver {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<Self::Position>>> {
            self.poll_count.fetch_add(1, Ordering::SeqCst);
            if self.remaining.is_empty() {
                return Ok(Vec::new());
            }
            Ok(vec![self.remaining.remove(0)])
        }

        async fn advance_watermark(&mut self, position: Self::Position) -> anyhow::Result<()> {
            self.advances.lock().unwrap().push(position);
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.remaining.is_empty()
        }
    }

    let started = Arc::new(Notify::new());
    let gate = Arc::new(Notify::new());
    let sink = RecordingSink::new().with_apply_hold(Arc::clone(&started), Arc::clone(&gate));
    let poll_count = Arc::new(AtomicU64::new(0));
    let advances = Arc::new(Mutex::new(Vec::new()));

    let mut driver = ObservingDriver {
        remaining: vec![
            PositionedEvent::change(change(1), 10u64),
            PositionedEvent::change(change(2), 20u64),
            PositionedEvent::change(change(3), 30u64),
        ],
        poll_count: Arc::clone(&poll_count),
        advances: Arc::clone(&advances),
    };

    let pipeline = Pipeline::new();
    assert!(pipeline.is_identity());
    let apply_opts = ApplyOpts::default()
        .with_max_in_flight(2)
        .with_batch_size(1)
        .with_batch_max_wait(Duration::from_millis(5))
        .with_timeout(Duration::from_secs(5));
    let runtime_opts = SourceRuntimeOpts::default();

    let run = run_source_runtime(&mut driver, &sink, &pipeline, &apply_opts, &runtime_opts);
    tokio::pin!(run);

    tokio::select! {
        _ = started.notified() => {}
        _ = &mut run => panic!("runtime finished before sink started"),
    }

    for _ in 0..40 {
        tokio::task::yield_now().await;
        if poll_count.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(
        poll_count.load(Ordering::SeqCst) >= 2,
        "poll must continue while slow sink is in flight; poll_count={}",
        poll_count.load(Ordering::SeqCst)
    );
    assert!(
        advances.lock().unwrap().is_empty(),
        "must not advance_watermark before sink finishes: {:?}",
        advances.lock().unwrap()
    );

    // Release gated applies until runtime completes.
    let release = async {
        loop {
            gate.notify_waiters();
            tokio::time::sleep(Duration::from_millis(5)).await;
            if advances.lock().unwrap().len() >= 3 {
                // Keep releasing briefly for any in-flight apply.
                gate.notify_waiters();
                break;
            }
        }
    };
    tokio::select! {
        r = &mut run => { r.unwrap(); }
        _ = release => { gate.notify_waiters(); run.await.unwrap(); }
    }

    assert_eq!(*advances.lock().unwrap(), vec![10, 20, 30]);
    assert_eq!(
        sink.applied()
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]
    );
}

/// Non-identity: transforms overlap; sink stays ordered; watermark advances only after sink.
#[tokio::test]
async fn non_identity_transforms_overlap_sink_ordered_advance_after_sink() {
    use crate::pipeline::test_support::{BatchScript, ScriptedTransformer};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    struct ObservingDriver {
        remaining: Vec<PositionedEvent<u64>>,
        advances: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait::async_trait]
    impl SourceDriver for ObservingDriver {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<Self::Position>>> {
            if self.remaining.is_empty() {
                return Ok(Vec::new());
            }
            Ok(vec![self.remaining.remove(0)])
        }

        async fn advance_watermark(&mut self, position: Self::Position) -> anyhow::Result<()> {
            self.advances.lock().unwrap().push(position);
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.remaining.is_empty()
        }
    }

    let started = Arc::new(Notify::new());
    let gate = Arc::new(Notify::new());
    let sink = RecordingSink::new().with_apply_hold(Arc::clone(&started), Arc::clone(&gate));

    let transformer = Arc::new(
        ScriptedTransformer::new(Pipeline::new())
            .on_batch(1, BatchScript::succeed_after(Duration::from_millis(30)))
            .on_batch(2, BatchScript::succeed_after(Duration::from_millis(5))),
    );

    let advances = Arc::new(Mutex::new(Vec::new()));
    let mut driver = ObservingDriver {
        remaining: vec![
            PositionedEvent::change(change(1), 100u64),
            PositionedEvent::change(change(2), 200u64),
        ],
        advances: Arc::clone(&advances),
    };

    let apply_opts = ApplyOpts::default()
        .with_max_in_flight(2)
        .with_batch_size(1)
        .with_batch_max_wait(Duration::from_millis(20))
        .with_timeout(Duration::from_secs(5));
    let runtime_opts = SourceRuntimeOpts::default();

    let run = run_source_runtime_with(
        &mut driver,
        &sink,
        Arc::clone(&transformer),
        &apply_opts,
        &runtime_opts,
    );
    tokio::pin!(run);

    tokio::select! {
        _ = started.notified() => {}
        _ = &mut run => panic!("runtime finished before first sink"),
    }

    assert!(
        transformer.completed_order().contains(&2),
        "batch 2 transform should overlap and finish before/while batch 1 sinks: {:?}",
        transformer.completed_order()
    );
    assert!(
        advances.lock().unwrap().is_empty(),
        "advance_watermark must wait for sink: {:?}",
        advances.lock().unwrap()
    );
    assert!(sink.applied().is_empty(), "first apply still gated");

    let release = async {
        loop {
            gate.notify_waiters();
            tokio::time::sleep(Duration::from_millis(5)).await;
            if advances.lock().unwrap().len() >= 2 {
                gate.notify_waiters();
                break;
            }
        }
    };
    tokio::select! {
        r = &mut run => { r.unwrap(); }
        _ = release => { gate.notify_waiters(); run.await.unwrap(); }
    }

    assert_eq!(
        transformer.completed_order().first(),
        Some(&2),
        "transforms may complete out of order"
    );
    assert_eq!(
        sink.applied()
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(1), Value::Int64(2)],
        "sink must stay ordered"
    );
    assert_eq!(
        *advances.lock().unwrap(),
        vec![100, 200],
        "advance_watermark only after each sink"
    );
}

/// Oversized poll_work keeps all events (no silent drop).
#[tokio::test]
async fn oversized_poll_work_keeps_excess_in_buffer() {
    struct FatPollDriver {
        once: bool,
        advances: Vec<u64>,
    }

    #[async_trait::async_trait]
    impl SourceDriver for FatPollDriver {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<Self::Position>>> {
            if self.once {
                return Ok(Vec::new());
            }
            self.once = true;
            // One poll returns more than batch_size (2).
            Ok(vec![
                PositionedEvent::change(change(1), 10),
                PositionedEvent::change(change(2), 20),
                PositionedEvent::change(change(3), 30),
            ])
        }

        async fn advance_watermark(&mut self, position: Self::Position) -> anyhow::Result<()> {
            self.advances.push(position);
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.once
        }
    }

    let mut driver = FatPollDriver {
        once: false,
        advances: Vec::new(),
    };
    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::default()
        .with_max_in_flight(2)
        .with_batch_size(2)
        .with_batch_max_wait(Duration::from_millis(5))
        .with_timeout(Duration::from_secs(5));

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
        sink.applied().len(),
        3,
        "all oversized poll events must be sunk, not dropped"
    );
    assert_eq!(driver.advances, vec![20, 30]);
}

/// max_in_flight≥3 + slow sink: poll_count must keep rising across outer-loop
/// iterations while the sink stays gated (spare window capacity).
#[tokio::test]
async fn polls_keep_rising_while_slow_sink_with_spare_capacity() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    struct ObservingDriver {
        remaining: Vec<PositionedEvent<u64>>,
        poll_count: Arc<AtomicU64>,
        advances: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait::async_trait]
    impl SourceDriver for ObservingDriver {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<Self::Position>>> {
            self.poll_count.fetch_add(1, Ordering::SeqCst);
            if self.remaining.is_empty() {
                return Ok(Vec::new());
            }
            Ok(vec![self.remaining.remove(0)])
        }

        async fn advance_watermark(&mut self, position: Self::Position) -> anyhow::Result<()> {
            self.advances.lock().unwrap().push(position);
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.remaining.is_empty()
        }
    }

    let started = Arc::new(Notify::new());
    let gate = Arc::new(Notify::new());
    let sink = RecordingSink::new().with_apply_hold(Arc::clone(&started), Arc::clone(&gate));
    let poll_count = Arc::new(AtomicU64::new(0));
    let advances = Arc::new(Mutex::new(Vec::new()));

    // batch_size=2: after first sink batch launches, one more poll leaves a partial
    // buffer and spare window capacity — further polls must continue while gated.
    let mut driver = ObservingDriver {
        remaining: vec![
            PositionedEvent::change(change(1), 10u64),
            PositionedEvent::change(change(2), 20u64),
            PositionedEvent::change(change(3), 30u64),
            PositionedEvent::change(change(4), 40u64),
            PositionedEvent::change(change(5), 50u64),
            PositionedEvent::change(change(6), 60u64),
        ],
        poll_count: Arc::clone(&poll_count),
        advances: Arc::clone(&advances),
    };

    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::default()
        .with_max_in_flight(3)
        .with_batch_size(2)
        // Long max_wait so partial buffer does not flush without a second poll.
        .with_batch_max_wait(Duration::from_secs(30))
        .with_timeout(Duration::from_secs(5));
    let runtime_opts = SourceRuntimeOpts::default();

    let run = run_source_runtime(&mut driver, &sink, &pipeline, &apply_opts, &runtime_opts);
    tokio::pin!(run);

    tokio::select! {
        _ = started.notified() => {}
        _ = &mut run => panic!("runtime finished before sink started"),
    }

    // First sink batch consumed 2 polls; spare-capacity refill must keep polling
    // (at least through the next full batch) while apply stays gated.
    for _ in 0..80 {
        tokio::task::yield_now().await;
        if poll_count.load(Ordering::SeqCst) >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(
        poll_count.load(Ordering::SeqCst) >= 5,
        "poll must continue under spare capacity while slow sink holds; poll_count={}",
        poll_count.load(Ordering::SeqCst)
    );
    assert!(
        advances.lock().unwrap().is_empty(),
        "must not advance_watermark before sink finishes: {:?}",
        advances.lock().unwrap()
    );

    let release = async {
        loop {
            gate.notify_waiters();
            tokio::time::sleep(Duration::from_millis(5)).await;
            if advances.lock().unwrap().len() >= 3 {
                gate.notify_waiters();
                break;
            }
        }
    };
    tokio::select! {
        r = &mut run => { r.unwrap(); }
        _ = release => { gate.notify_waiters(); run.await.unwrap(); }
    }

    assert_eq!(sink.applied().len(), 6);
    assert_eq!(*advances.lock().unwrap(), vec![20, 40, 60]);
}

/// Slow multi-event sink + concurrent transform completion must not re-apply
/// (biased select dropping the sink future must not restart apply).
#[tokio::test]
async fn multi_event_sink_not_reapplied_when_transform_completes() {
    use crate::pipeline::test_support::{BatchScript, ScriptedTransformer};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    struct ObservingDriver {
        remaining: Vec<PositionedEvent<u64>>,
        advances: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait::async_trait]
    impl SourceDriver for ObservingDriver {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<Self::Position>>> {
            if self.remaining.is_empty() {
                return Ok(Vec::new());
            }
            // Emit two events so the first sink batch is multi-event.
            if self.remaining.len() >= 2 {
                let a = self.remaining.remove(0);
                let b = self.remaining.remove(0);
                return Ok(vec![a, b]);
            }
            Ok(vec![self.remaining.remove(0)])
        }

        async fn advance_watermark(&mut self, position: Self::Position) -> anyhow::Result<()> {
            self.advances.lock().unwrap().push(position);
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.remaining.is_empty()
        }
    }

    let started = Arc::new(Notify::new());
    let gate = Arc::new(Notify::new());
    let sink = RecordingSink::new().with_apply_hold(Arc::clone(&started), Arc::clone(&gate));

    // Batch 1 (events 1–2): fast transform → multi-event sink, gated on first apply.
    // Batch 2 (event 3): delayed transform that completes while sink is mid-apply,
    // winning biased select and cancelling the sink await.
    let transformer = Arc::new(
        ScriptedTransformer::new(Pipeline::new())
            .on_batch(1, BatchScript::succeed_after(Duration::ZERO))
            .on_batch(2, BatchScript::succeed_after(Duration::from_millis(40))),
    );

    let advances = Arc::new(Mutex::new(Vec::new()));
    let mut driver = ObservingDriver {
        remaining: vec![
            PositionedEvent::change(change(1), 10u64),
            PositionedEvent::change(change(2), 20u64),
            PositionedEvent::change(change(3), 30u64),
        ],
        advances: Arc::clone(&advances),
    };

    let apply_opts = ApplyOpts::default()
        .with_max_in_flight(2)
        .with_batch_size(2)
        .with_batch_max_wait(Duration::from_millis(5))
        .with_timeout(Duration::from_secs(5));
    let runtime_opts = SourceRuntimeOpts::default();

    let run = run_source_runtime_with(
        &mut driver,
        &sink,
        Arc::clone(&transformer),
        &apply_opts,
        &runtime_opts,
    );
    tokio::pin!(run);

    tokio::select! {
        _ = started.notified() => {}
        _ = &mut run => panic!("runtime finished before first sink apply"),
    }

    // Let batch 2 transform complete while first apply stays gated.
    for _ in 0..60 {
        tokio::task::yield_now().await;
        if transformer.completed_order().contains(&2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(
        transformer.completed_order().contains(&2),
        "batch 2 must complete while sink is mid-apply: {:?}",
        transformer.completed_order()
    );

    // Give select a chance to cancel/re-poll the sink future.
    for _ in 0..20 {
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let release = async {
        loop {
            gate.notify_waiters();
            tokio::time::sleep(Duration::from_millis(5)).await;
            if advances.lock().unwrap().len() >= 2 {
                gate.notify_waiters();
                break;
            }
        }
    };
    tokio::select! {
        r = &mut run => { r.unwrap(); }
        _ = release => { gate.notify_waiters(); run.await.unwrap(); }
    }

    let applied = sink.applied();
    let ids: Vec<_> = applied.iter().map(|c| c.id.clone()).collect();
    assert_eq!(
        ids,
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        "sink order must stay correct"
    );
    assert_eq!(
        sink.apply_attempts(),
        3,
        "each event must be applied exactly once (no cancel/re-apply); attempts={}",
        sink.apply_attempts()
    );
}

/// Filter transform: `note_sunk_events` must use pre-transform input count so
/// Kafka-style pending-acks and wal2json emitted/sunk gates still advance.
#[tokio::test]
async fn filter_transform_notes_input_count_for_kafka_and_wal2json() {
    use crate::pipeline::BatchTransformer;
    use anyhow::bail;
    use async_trait::async_trait;
    use std::sync::Arc;
    use surreal_sync_core::Change;

    /// Drops every other change (keeps odd ids) — length shrinks.
    struct FilterOddIds;

    #[async_trait]
    impl BatchTransformer for FilterOddIds {
        fn is_identity(&self) -> bool {
            false
        }

        async fn transform_changes(
            &self,
            _batch_id: u64,
            changes: Vec<Change>,
        ) -> anyhow::Result<Vec<Change>> {
            Ok(changes
                .into_iter()
                .filter(|c| match &c.id {
                    Value::Int64(id) => id % 2 == 1,
                    _ => true,
                })
                .collect())
        }

        async fn transform_rows(
            &self,
            _batch_id: u64,
            rows: Vec<surreal_sync_core::Row>,
        ) -> anyhow::Result<Vec<surreal_sync_core::Row>> {
            Ok(rows)
        }

        async fn transform_events(
            &self,
            batch_id: u64,
            events: Vec<ApplyEvent>,
        ) -> anyhow::Result<Vec<ApplyEvent>> {
            // Homogeneous change batches may filter (default transform_events
            // rejects length changes).
            if events.iter().all(|e| matches!(e, ApplyEvent::Change(_))) {
                let changes: Vec<_> = events
                    .into_iter()
                    .map(|e| match e {
                        ApplyEvent::Change(c) => c,
                        ApplyEvent::RelationChange(_) => unreachable!(),
                    })
                    .collect();
                let out = self.transform_changes(batch_id, changes).await?;
                return Ok(out.into_iter().map(ApplyEvent::Change).collect());
            }
            bail!("FilterOddIds expects homogeneous change batches")
        }
    }

    /// Kafka-like: pending → ready on note_sunk; advance_watermark drains ready (commit_batch).
    struct KafkaAckDriver {
        remaining: Vec<PositionedEvent<u64>>,
        pending_acks: std::collections::VecDeque<u64>,
        ready_to_commit: Vec<u64>,
        committed: Vec<u64>,
        processed_count: u64,
        finished: bool,
    }

    #[async_trait]
    impl SourceDriver for KafkaAckDriver {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<u64>>> {
            if self.finished || self.remaining.is_empty() {
                self.finished = self.remaining.is_empty();
                return Ok(Vec::new());
            }
            let ev = self.remaining.remove(0);
            self.pending_acks.push_back(ev.position);
            Ok(vec![ev])
        }

        async fn advance_watermark(&mut self, _position: Self::Position) -> anyhow::Result<()> {
            self.committed.append(&mut self.ready_to_commit);
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn checkpoint_policy(&self) -> CheckpointPolicy {
            CheckpointPolicy::AdvanceOnly
        }

        fn note_sunk_events(&mut self, count: u64) {
            for _ in 0..count {
                if let Some(p) = self.pending_acks.pop_front() {
                    self.ready_to_commit.push(p);
                }
            }
            self.processed_count = self.processed_count.saturating_add(count);
        }
    }

    /// wal2json-like: advance_watermark only when sunk >= emitted.
    struct Wal2jsonGateDriver {
        remaining: Vec<PositionedEvent<u64>>,
        emitted: u64,
        sunk: u64,
        advances: u64,
        finished: bool,
    }

    #[async_trait]
    impl SourceDriver for Wal2jsonGateDriver {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<u64>>> {
            if self.finished || self.remaining.is_empty() {
                self.finished = self.remaining.is_empty();
                return Ok(Vec::new());
            }
            // Emit a multi-event peek batch (like one WAL segment).
            let batch: Vec<_> = self.remaining.drain(..).collect();
            self.emitted = self.emitted.saturating_add(batch.len() as u64);
            Ok(batch)
        }

        async fn advance_watermark(&mut self, _position: Self::Position) -> anyhow::Result<()> {
            if self.sunk < self.emitted {
                return Ok(());
            }
            self.advances = self.advances.saturating_add(1);
            self.emitted = 0;
            self.sunk = 0;
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn checkpoint_policy(&self) -> CheckpointPolicy {
            CheckpointPolicy::AdvanceOnly
        }

        fn note_sunk_events(&mut self, count: u64) {
            self.sunk = self.sunk.saturating_add(count);
        }
    }

    let events = vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::change(change(2), 20u64),
        PositionedEvent::change(change(3), 30u64),
    ];
    let apply_opts = opts().with_batch_size(3);
    let transformer = Arc::new(FilterOddIds);

    // Kafka: filter 3→2 sunk writes, but all 3 pending acks must advance (commit_batch).
    let mut kafka = KafkaAckDriver {
        remaining: events.clone(),
        pending_acks: std::collections::VecDeque::new(),
        ready_to_commit: Vec::new(),
        committed: Vec::new(),
        processed_count: 0,
        finished: false,
    };
    let sink = RecordingSink::new();
    run_source_runtime_with(
        &mut kafka,
        &sink,
        Arc::clone(&transformer),
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();
    assert_eq!(sink.applied().len(), 2, "filter must sink only odd ids");
    assert_eq!(
        kafka.processed_count, 3,
        "note_sunk_events must count input messages, not filtered sink length"
    );
    assert_eq!(
        kafka.committed,
        vec![10, 20, 30],
        "Kafka pending-ack advance_watermark must include filtered-away messages"
    );
    assert!(
        kafka.pending_acks.is_empty(),
        "no pending acks must remain after filter sink"
    );

    // wal2json: same filter; slot must still advance (sunk matches emitted).
    let mut wal = Wal2jsonGateDriver {
        remaining: events,
        emitted: 0,
        sunk: 0,
        advances: 0,
        finished: false,
    };
    let sink2 = RecordingSink::new();
    run_source_runtime_with(
        &mut wal,
        &sink2,
        transformer,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();
    assert_eq!(sink2.applied().len(), 2);
    assert_eq!(
        wal.advances, 1,
        "wal2json slot must advance when sunk notes input count under filter"
    );
    assert_eq!(wal.emitted, 0, "emitted must reset after advance");
    assert_eq!(wal.sunk, 0, "sunk must reset after advance");
}

/// Fan-out transform: `note_sunk_events` must not over-count input so
/// max_messages / processed_count and wal2json gates do not premature-advance.
#[tokio::test]
async fn fan_out_transform_notes_input_count_not_output_length() {
    use crate::pipeline::BatchTransformer;
    use anyhow::bail;
    use async_trait::async_trait;
    use std::sync::Arc;
    use surreal_sync_core::Change;

    /// Duplicates each change — length grows.
    struct FanOutTwice;

    #[async_trait]
    impl BatchTransformer for FanOutTwice {
        fn is_identity(&self) -> bool {
            false
        }

        async fn transform_changes(
            &self,
            _batch_id: u64,
            changes: Vec<Change>,
        ) -> anyhow::Result<Vec<Change>> {
            let mut out = Vec::with_capacity(changes.len() * 2);
            for c in changes {
                out.push(c.clone());
                out.push(c);
            }
            Ok(out)
        }

        async fn transform_rows(
            &self,
            _batch_id: u64,
            rows: Vec<surreal_sync_core::Row>,
        ) -> anyhow::Result<Vec<surreal_sync_core::Row>> {
            Ok(rows)
        }

        async fn transform_events(
            &self,
            batch_id: u64,
            events: Vec<ApplyEvent>,
        ) -> anyhow::Result<Vec<ApplyEvent>> {
            if events.iter().all(|e| matches!(e, ApplyEvent::Change(_))) {
                let changes: Vec<_> = events
                    .into_iter()
                    .map(|e| match e {
                        ApplyEvent::Change(c) => c,
                        ApplyEvent::RelationChange(_) => unreachable!(),
                    })
                    .collect();
                let out = self.transform_changes(batch_id, changes).await?;
                return Ok(out.into_iter().map(ApplyEvent::Change).collect());
            }
            bail!("FanOutTwice expects homogeneous change batches")
        }
    }

    /// Kafka-like processed_count + max_messages stop.
    struct MaxMessagesDriver {
        remaining: Vec<PositionedEvent<u64>>,
        pending_acks: std::collections::VecDeque<u64>,
        ready_to_commit: Vec<u64>,
        committed: Vec<u64>,
        processed_count: u64,
        max_messages: u64,
        finished: bool,
    }

    #[async_trait]
    impl SourceDriver for MaxMessagesDriver {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<u64>>> {
            if self.stop_reason().is_some() || self.finished || self.remaining.is_empty() {
                if self.remaining.is_empty() {
                    self.finished = true;
                }
                return Ok(Vec::new());
            }
            let ev = self.remaining.remove(0);
            self.pending_acks.push_back(ev.position);
            Ok(vec![ev])
        }

        async fn advance_watermark(&mut self, _position: Self::Position) -> anyhow::Result<()> {
            self.committed.append(&mut self.ready_to_commit);
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn checkpoint_policy(&self) -> CheckpointPolicy {
            CheckpointPolicy::AdvanceOnly
        }

        fn stop_reason(&self) -> Option<StopReason> {
            if self.processed_count >= self.max_messages {
                Some(StopReason::Until)
            } else {
                None
            }
        }

        fn note_sunk_events(&mut self, count: u64) {
            for _ in 0..count {
                if let Some(p) = self.pending_acks.pop_front() {
                    self.ready_to_commit.push(p);
                }
            }
            self.processed_count = self.processed_count.saturating_add(count);
            if self.processed_count >= self.max_messages {
                self.finished = true;
            }
        }
    }

    /// wal2json-like gate that would premature-advance if fan-out over-counted.
    struct Wal2jsonGateDriver {
        remaining: Vec<PositionedEvent<u64>>,
        emitted: u64,
        sunk: u64,
        advances: u64,
        /// Second peek only after first advance (simulates awaiting watermark advance).
        can_emit_second: bool,
        finished: bool,
    }

    #[async_trait]
    impl SourceDriver for Wal2jsonGateDriver {
        type Position = u64;

        async fn poll_work(&mut self) -> anyhow::Result<Vec<PositionedEvent<u64>>> {
            if self.finished {
                return Ok(Vec::new());
            }
            if self.emitted > self.sunk {
                // Still awaiting sunk for the current peek — do not emit more.
                return Ok(Vec::new());
            }
            if self.remaining.is_empty() {
                self.finished = true;
                return Ok(Vec::new());
            }
            if !self.can_emit_second && self.advances == 0 && self.emitted > 0 {
                return Ok(Vec::new());
            }
            // Emit one event per peek.
            let ev = self.remaining.remove(0);
            self.emitted = self.emitted.saturating_add(1);
            self.can_emit_second = false;
            Ok(vec![ev])
        }

        async fn advance_watermark(&mut self, _position: Self::Position) -> anyhow::Result<()> {
            if self.sunk < self.emitted {
                return Ok(());
            }
            self.advances = self.advances.saturating_add(1);
            self.emitted = 0;
            self.sunk = 0;
            self.can_emit_second = true;
            Ok(())
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn checkpoint_policy(&self) -> CheckpointPolicy {
            CheckpointPolicy::AdvanceOnly
        }

        fn note_sunk_events(&mut self, count: u64) {
            self.sunk = self.sunk.saturating_add(count);
        }
    }

    let apply_opts = opts().with_batch_size(1);
    let transformer = Arc::new(FanOutTwice);

    // max_messages=2: two input messages → four sink writes; stop after 2 inputs.
    let mut kafka = MaxMessagesDriver {
        remaining: vec![
            PositionedEvent::change(change(1), 10u64),
            PositionedEvent::change(change(2), 20u64),
            PositionedEvent::change(change(3), 30u64),
        ],
        pending_acks: std::collections::VecDeque::new(),
        ready_to_commit: Vec::new(),
        committed: Vec::new(),
        processed_count: 0,
        max_messages: 2,
        finished: false,
    };
    let sink = RecordingSink::new();
    run_source_runtime_with(
        &mut kafka,
        &sink,
        Arc::clone(&transformer),
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        sink.applied().len(),
        4,
        "fan-out must write 2 outputs per input for the first 2 inputs"
    );
    assert_eq!(
        kafka.processed_count, 2,
        "processed_count / max_messages must use input count, not fan-out length"
    );
    assert_eq!(kafka.committed, vec![10, 20]);
    assert!(
        !kafka.committed.contains(&30),
        "third input must not be advanced after max_messages"
    );

    // wal2json: one input fans out to 2 sink writes; must not advance early
    // (sunk would hit emitted with post-transform count of 2 vs emitted 1).
    let mut wal = Wal2jsonGateDriver {
        remaining: vec![
            PositionedEvent::change(change(1), 10u64),
            PositionedEvent::change(change(2), 20u64),
        ],
        emitted: 0,
        sunk: 0,
        advances: 0,
        can_emit_second: true,
        finished: false,
    };
    let sink2 = RecordingSink::new();
    run_source_runtime_with(
        &mut wal,
        &sink2,
        transformer,
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();
    assert_eq!(sink2.applied().len(), 4, "two inputs × fan-out 2");
    assert_eq!(
        wal.advances, 2,
        "wal2json must advance once per input peek, not once per fan-out write"
    );
}

#[tokio::test]
async fn row_chunk_driver_streams_chunks_through_runtime() {
    use crate::pipeline::{RowChunkDriver, RowChunkSource};
    use async_trait::async_trait;
    use surreal_sync_core::Row;

    struct TwoChunks {
        n: usize,
    }

    #[async_trait]
    impl RowChunkSource for TwoChunks {
        async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<Row>>> {
            self.n += 1;
            if self.n > 2 {
                return Ok(None);
            }
            let id = self.n as i64;
            let mut fields = HashMap::new();
            fields.insert("v".to_string(), Value::Int64(id));
            Ok(Some(vec![Row::new(
                "t",
                id as u64,
                Value::Int64(id),
                fields,
            )]))
        }
    }

    let mut driver = RowChunkDriver::new(TwoChunks { n: 0 });
    let sink = RecordingSink::new();
    let apply_opts = ApplyOpts::identity().with_max_in_flight(2);
    run_source_runtime(
        &mut driver,
        &sink,
        &Pipeline::new(),
        &apply_opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();
    assert_eq!(driver.sunk_count(), 2);
    assert_eq!(sink.rows_written().len(), 2);
    assert!(sink.applied().is_empty());
}
