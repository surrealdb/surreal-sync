//! Phase 2 reliability tests: windowed apply, ordered sink/commit, fail/skip.

use crate::test_support::{
    BatchScript, RecordingSink, ScriptedChangeFeed, ScriptedTransformer, SinkFailWhen,
};
use crate::{
    run_change_feed, run_change_feed_with, write_rows, ApplyOpts, FailurePolicy, InPlaceTransform,
    Pipeline, PositionedChange,
};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use sync_core::{UniversalChange, UniversalRow, UniversalValue};

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

fn positioned(id: i64, pos: u64) -> PositionedChange<u64> {
    PositionedChange::new(change(id), pos)
}

fn row(id: i64) -> UniversalRow {
    UniversalRow::builder("users", id as u64, UniversalValue::Int64(id))
        .field(
            "name",
            UniversalValue::VarChar {
                value: format!("row-{id}"),
                length: 64,
            },
        )
        .build()
}

struct TagName;

impl InPlaceTransform for TagName {
    fn transform_row(&self, row: &mut UniversalRow) -> Result<()> {
        row.fields.insert(
            "name".to_string(),
            UniversalValue::VarChar {
                value: "tagged".to_string(),
                length: 64,
            },
        );
        Ok(())
    }

    fn transform_change(&self, change: &mut UniversalChange) -> Result<()> {
        if let Some(data) = change.data.as_mut() {
            data.insert(
                "name".to_string(),
                UniversalValue::VarChar {
                    value: "tagged".to_string(),
                    length: 64,
                },
            );
        }
        Ok(())
    }
}

fn opts_window(w: usize) -> ApplyOpts {
    ApplyOpts::default()
        .with_max_in_flight(w)
        .with_batch_size(1)
        .with_batch_max_wait(Duration::from_millis(50))
        .with_timeout(Duration::from_secs(5))
}

#[tokio::test]
async fn identity_pipeline_commits_track_sink() {
    let pipeline = Pipeline::new();
    assert!(pipeline.is_identity());

    let mut feed = ScriptedChangeFeed::new(vec![
        positioned(1, 10),
        positioned(2, 20),
        positioned(3, 30),
    ]);
    let sink = RecordingSink::new();
    let opts = opts_window(1);

    run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap();

    assert_eq!(sink.applied().len(), 3);
    assert_eq!(feed.commits, vec![10, 20, 30]);
    // Sink order matches feed order.
    assert_eq!(
        sink.applied()
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>(),
        vec![
            UniversalValue::Int64(1),
            UniversalValue::Int64(2),
            UniversalValue::Int64(3),
        ]
    );
}

#[tokio::test]
async fn write_rows_identity_no_stage_dispatch() {
    let pipeline = Pipeline::new();
    assert!(pipeline.is_identity());
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default();
    write_rows(&sink, &pipeline, vec![row(1), row(2)], &opts)
        .await
        .unwrap();
    let written = sink.rows_written();
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].len(), 2);
}

#[tokio::test]
async fn inplace_stages_mutate_before_sink() {
    let mut pipeline = Pipeline::new();
    pipeline.push_inplace(TagName);

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 1)]);
    let sink = RecordingSink::new();
    let opts = opts_window(1);

    run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap();

    let applied = sink.applied();
    assert_eq!(applied.len(), 1);
    assert_eq!(
        applied[0].data.as_ref().unwrap().get("name"),
        Some(&UniversalValue::VarChar {
            value: "tagged".to_string(),
            length: 64,
        })
    );
    assert_eq!(feed.commits, vec![1]);
}

/// Core reliability case: W≥2, former batch A fails AFTER later batch B's
/// transform succeeds (out-of-order completion). Sink must never apply B before
/// A; commit never past A; B discarded; retry works.
#[tokio::test]
async fn window_former_fails_after_later_transform_succeeds() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::fail_after(Duration::from_millis(80), "A transform failed"))
        .on_batch(2, BatchScript::succeed_after(Duration::from_millis(5)));

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 100), positioned(2, 200)]);
    let sink = RecordingSink::new();
    let opts = opts_window(2);

    let err = run_change_feed_with(&mut feed, &sink, Arc::new(transformer.clone()), &opts)
        .await
        .unwrap_err();
    let err_msg = format!("{err:#}");
    assert!(
        err_msg.contains("A transform failed"),
        "unexpected error: {err_msg}"
    );

    // B may complete first, but must never be sunk or committed.
    let completed = transformer.completed_order();
    assert!(
        completed.contains(&2),
        "B should have completed transform: {completed:?}"
    );
    assert!(sink.applied().is_empty(), "sink must not apply A or B");
    assert!(
        feed.commits.is_empty(),
        "commit must not advance past failed A: {:?}",
        feed.commits
    );

    // Retry / replay from uncommitted positions — both succeed.
    let transformer2 = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::succeed_after(Duration::from_millis(5)))
        .on_batch(2, BatchScript::succeed_after(Duration::from_millis(5)));
    let mut feed2 = ScriptedChangeFeed::from_remaining(vec![
        positioned(1, 100),
        positioned(2, 200),
    ]);
    let sink2 = RecordingSink::new();
    run_change_feed_with(&mut feed2, &sink2, Arc::new(transformer2), &opts)
        .await
        .unwrap();

    assert_eq!(
        sink2
            .applied()
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>(),
        vec![UniversalValue::Int64(1), UniversalValue::Int64(2)]
    );
    assert_eq!(feed2.commits, vec![100, 200]);
}

/// Same window: A sink fails after both A and B transformed. Neither commit past
/// A; B not applied.
#[tokio::test]
async fn window_sink_fails_after_both_transformed() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::succeed_after(Duration::from_millis(10)))
        .on_batch(2, BatchScript::succeed_after(Duration::from_millis(10)));

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 100), positioned(2, 200)]);
    let sink = RecordingSink::new().fail_when(vec![SinkFailWhen::ApplyIndex(0)]);
    let opts = opts_window(2);

    let err = run_change_feed_with(&mut feed, &sink, Arc::new(transformer.clone()), &opts)
        .await
        .unwrap_err();
    let err_msg = format!("{err:#}");
    assert!(
        err_msg.contains("RecordingSink scripted fail"),
        "unexpected error: {err_msg}"
    );

    // Both transforms should have completed (or at least A attempted sink).
    let completed = transformer.completed_order();
    assert!(
        completed.len() >= 1,
        "expected transform completions: {completed:?}"
    );
    assert!(
        sink.applied().is_empty(),
        "A sink failed so nothing applied: {:?}",
        sink.applied()
    );
    assert!(
        feed.commits.is_empty(),
        "must not commit past failed A: {:?}",
        feed.commits
    );
}

#[tokio::test]
async fn failure_policy_fail_leaves_commits_unchanged() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::fail_after(Duration::ZERO, "boom"));

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10), positioned(2, 20)]);
    let sink = RecordingSink::new();
    let opts = opts_window(1).with_failure_policy(FailurePolicy::Fail);

    let err = run_change_feed_with(&mut feed, &sink, Arc::new(transformer), &opts)
        .await
        .unwrap_err();
    let err_msg = format!("{err:#}");
    assert!(err_msg.contains("boom"), "unexpected error: {err_msg}");
    assert!(feed.commits.is_empty());
    assert!(sink.applied().is_empty());
}

#[tokio::test]
async fn failure_policy_skip_commits_past_failed_batch() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::fail_after(Duration::ZERO, "skip-me"))
        .on_batch(2, BatchScript::succeed_after(Duration::ZERO));

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10), positioned(2, 20)]);
    let sink = RecordingSink::new();
    let opts = opts_window(1).with_failure_policy(FailurePolicy::Skip);

    run_change_feed_with(&mut feed, &sink, Arc::new(transformer), &opts)
        .await
        .unwrap();

    // Failed batch not written; still committed past it; successor applied.
    assert_eq!(sink.applied().len(), 1);
    assert_eq!(sink.applied()[0].id, UniversalValue::Int64(2));
    assert_eq!(feed.commits, vec![10, 20]);
}

/// Simulated crash mid-window: recreate from last commit; uncommitted batches replay.
#[tokio::test]
async fn crash_replay_from_last_commit() {
    // First run: A succeeds, B's sink fails once (crash after A committed).
    let transformer = ScriptedTransformer::new(Pipeline::new());
    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 100), positioned(2, 200)]);
    let sink = RecordingSink::new()
        .fail_when(vec![SinkFailWhen::ChangeId("2".into())])
        .fail_once();
    let opts = opts_window(1);

    let err = run_change_feed_with(&mut feed, &sink, Arc::new(transformer), &opts)
        .await
        .unwrap_err();
    let err_msg = format!("{err:#}");
    assert!(
        err_msg.contains("RecordingSink"),
        "unexpected error: {err_msg}"
    );
    assert_eq!(feed.commits, vec![100]);
    assert_eq!(sink.applied().len(), 1);
    assert_eq!(sink.applied()[0].id, UniversalValue::Int64(1));

    // Replay from last commit: only uncommitted B remains.
    let mut feed2 = ScriptedChangeFeed::from_remaining(vec![positioned(2, 200)]);
    // Fresh sink that succeeds (simulating restart); prior A already durable.
    let sink2 = RecordingSink::new();
    let pipeline = Pipeline::new();
    run_change_feed(&mut feed2, &sink2, &pipeline, &opts)
        .await
        .unwrap();

    assert_eq!(feed2.commits, vec![200]);
    assert_eq!(sink2.applied().len(), 1);
    assert_eq!(sink2.applied()[0].id, UniversalValue::Int64(2));
}

#[tokio::test]
async fn ordered_sink_despite_out_of_order_transform_completion() {
    // B transforms much faster than A; both succeed. Sink order must stay A then B.
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::succeed_after(Duration::from_millis(60)))
        .on_batch(2, BatchScript::succeed_after(Duration::from_millis(5)));

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 100), positioned(2, 200)]);
    let sink = RecordingSink::new();
    let opts = opts_window(2);

    run_change_feed_with(&mut feed, &sink, Arc::new(transformer.clone()), &opts)
        .await
        .unwrap();

    let completed = transformer.completed_order();
    assert_eq!(
        completed.first(),
        Some(&2),
        "B should complete transform first: {completed:?}"
    );
    assert_eq!(
        sink.applied()
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>(),
        vec![UniversalValue::Int64(1), UniversalValue::Int64(2)]
    );
    assert_eq!(feed.commits, vec![100, 200]);
}
