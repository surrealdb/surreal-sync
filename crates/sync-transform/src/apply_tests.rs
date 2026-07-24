//! Phase 2 reliability tests: windowed apply, ordered sink / watermark advance, fail/skip.

use crate::test_support::{
    BatchScript, RecordingSink, ScriptedChangeFeed, ScriptedTransformer, SinkFailWhen,
};
use crate::{
    apply_changes, apply_relation_changes, run_change_feed, run_change_feed_with, write_rows,
    ApplyContext, ApplyOpts, FailurePolicy, InPlaceTransform, Pipeline, PositionedChange,
};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use sync_core::{Change, Relation, RelationChange, Row, ThingRef, Value};
use tokio::time::timeout;

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

fn positioned(id: i64, pos: u64) -> PositionedChange<u64> {
    PositionedChange::new(change(id), pos)
}

fn row(id: i64) -> Row {
    Row::builder("users", id as u64, Value::Int64(id))
        .field(
            "name",
            Value::VarChar {
                value: format!("row-{id}"),
                length: 64,
            },
        )
        .build()
}

struct TagName;

impl InPlaceTransform for TagName {
    fn transform(
        &self,
        _table: &str,
        _id: &mut Value,
        fields: Option<&mut HashMap<String, Value>>,
    ) -> Result<()> {
        if let Some(fields) = fields {
            fields.insert(
                "name".to_string(),
                Value::VarChar {
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
async fn identity_pipeline_advances_track_sink() {
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
    assert_eq!(feed.advances, vec![10, 20, 30]);
    // Sink order matches feed order.
    assert_eq!(
        sink.applied()
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3),]
    );
}

#[tokio::test]
async fn write_rows_identity_uses_apply_context() {
    // Omit `--transforms-config` → ApplyOpts::identity() (W=1, batch_size=1).
    // Every path goes through ApplyContext; identity upserts coalesce to
    // write_rows inside the ordered sink step (not a oneshot bypass).
    let pipeline = Pipeline::new();
    assert!(pipeline.is_identity());
    let sink = RecordingSink::new();
    let opts = ApplyOpts::identity();
    assert_eq!(opts.max_in_flight, 1);
    assert_eq!(opts.batch_size, 1);
    write_rows(&sink, &pipeline, vec![row(1), row(2)], &opts)
        .await
        .unwrap();
    // batch_size=1 → one coalesced write per row.
    let written = sink.rows_written();
    assert_eq!(written.len(), 2);
    assert_eq!(written[0].len(), 1);
    assert_eq!(written[1].len(), 1);
    assert_eq!(written[0][0].id, Value::Int64(1));
    assert_eq!(written[1][0].id, Value::Int64(2));
    assert!(
        sink.applied().is_empty(),
        "identity upserts coalesce to write_rows inside the window"
    );
}

#[tokio::test]
async fn write_rows_identity_windowed_when_max_in_flight_gt_1() {
    // W>1 still uses ApplyContext; upserts coalesce inside the sink step.
    let pipeline = Pipeline::new();
    assert!(pipeline.is_identity());
    let sink = RecordingSink::new();
    let opts = ApplyOpts::identity()
        .with_max_in_flight(2)
        .with_batch_size(1);
    write_rows(&sink, &pipeline, vec![row(1), row(2)], &opts)
        .await
        .unwrap();
    assert_eq!(sink.rows_written().len(), 2);
    assert!(
        sink.applied().is_empty(),
        "identity upserts coalesce to write_rows, not per-change apply"
    );
}

#[tokio::test]
async fn write_rows_coalesces_homogeneous_upsert_batch() {
    // Larger batch_size keeps a single bulk write_rows inside the window.
    let pipeline = Pipeline::new();
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default()
        .with_batch_size(10)
        .with_max_in_flight(1);
    write_rows(&sink, &pipeline, vec![row(1), row(2), row(3)], &opts)
        .await
        .unwrap();
    let written = sink.rows_written();
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].len(), 3);
    assert!(sink.applied().is_empty());
}

#[tokio::test]
async fn write_rows_honors_max_in_flight_window() {
    // Non-identity + W>1 must use ApplyContext; upserts coalesce to
    // write_rows inside the ordered sink (still windowed).
    let mut pipeline = Pipeline::new();
    pipeline.push_inplace(TagName);
    let sink = RecordingSink::new();
    let opts = opts_window(4).with_batch_size(1);
    write_rows(&sink, &pipeline, vec![row(1), row(2), row(3)], &opts)
        .await
        .unwrap();
    let written = sink.rows_written();
    assert_eq!(written.len(), 3);
    for batch in &written {
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch[0].fields.get("name"),
            Some(&Value::VarChar {
                value: "tagged".to_string(),
                length: 64,
            })
        );
    }
    assert!(
        sink.applied().is_empty(),
        "windowed write_rows upserts coalesce to write_rows"
    );
}

#[tokio::test]
async fn delete_and_mixed_batches_use_per_event_apply() {
    // Delete (and mixed Update+Delete) must not coalesce to write_rows.
    let pipeline = Pipeline::new();
    let opts = ApplyOpts::default()
        .with_batch_size(10)
        .with_max_in_flight(1);

    let sink = RecordingSink::new();
    apply_changes(
        &sink,
        &pipeline,
        vec![
            Change::delete("users", Value::Int64(1)),
            Change::delete("users", Value::Int64(2)),
        ],
        &opts,
    )
    .await
    .unwrap();
    assert_eq!(sink.applied().len(), 2);
    assert!(
        sink.rows_written().is_empty(),
        "Delete batches stay on apply_change, not write_rows"
    );

    let sink = RecordingSink::new();
    let mut update_data = HashMap::new();
    update_data.insert(
        "name".to_string(),
        Value::VarChar {
            value: "alice".to_string(),
            length: 64,
        },
    );
    apply_changes(
        &sink,
        &pipeline,
        vec![
            Change::update("users", Value::Int64(1), update_data),
            Change::delete("users", Value::Int64(2)),
        ],
        &opts,
    )
    .await
    .unwrap();
    assert_eq!(sink.applied().len(), 2);
    assert!(
        sink.rows_written().is_empty(),
        "mixed Update+Delete must not coalesce to write_rows"
    );
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
        applied[0].fields.as_ref().unwrap().get("name"),
        Some(&Value::VarChar {
            value: "tagged".to_string(),
            length: 64,
        })
    );
    assert_eq!(feed.advances, vec![1]);
}

/// Core reliability case: W≥2, former batch A fails AFTER later batch B's
/// transform succeeds (out-of-order completion). Sink must never apply B before
/// A; watermark never advances past A; B discarded; retry works.
#[tokio::test]
async fn window_former_fails_after_later_transform_succeeds() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(
            1,
            BatchScript::fail_after(Duration::from_millis(80), "A transform failed"),
        )
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

    // Force OOO: B must complete transform before A.
    let completed = transformer.completed_order();
    assert_eq!(
        completed.first(),
        Some(&2),
        "B should complete transform first (OOO): {completed:?}"
    );
    assert!(
        completed.contains(&1),
        "A should eventually complete (as Err): {completed:?}"
    );
    assert!(sink.applied().is_empty(), "sink must not apply A or B");
    assert!(
        feed.advances.is_empty(),
        "must not advance_watermark past failed A: {:?}",
        feed.advances
    );

    // Retry / replay from unadvanced positions — both succeed.
    let transformer2 = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::succeed_after(Duration::from_millis(5)))
        .on_batch(2, BatchScript::succeed_after(Duration::from_millis(5)));
    let mut feed2 =
        ScriptedChangeFeed::from_remaining(vec![positioned(1, 100), positioned(2, 200)]);
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
        vec![Value::Int64(1), Value::Int64(2)]
    );
    assert_eq!(feed2.advances, vec![100, 200]);
}

/// Same window: A sink fails after both A and B transformed (forced OOO).
/// Neither advance past A; B was waiting then discarded — never applied.
#[tokio::test]
async fn window_sink_fails_after_both_transformed() {
    // Force OOO: B finishes transform first; A finishes later then sink-fails.
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::succeed_after(Duration::from_millis(60)))
        .on_batch(2, BatchScript::succeed_after(Duration::from_millis(5)));

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

    let completed = transformer.completed_order();
    assert_eq!(
        completed.first(),
        Some(&2),
        "B should complete transform first: {completed:?}"
    );
    assert!(
        completed.contains(&1) && completed.contains(&2),
        "both A and B must have completed transform before A sink fail: {completed:?}"
    );
    assert!(
        sink.applied().is_empty(),
        "A sink failed so nothing applied (B discarded, never sunk): {:?}",
        sink.applied()
    );
    assert!(
        feed.advances.is_empty(),
        "must not advance_watermark past failed A: {:?}",
        feed.advances
    );
    // A was attempted (and failed); B must not have been attempted.
    assert_eq!(
        sink.apply_attempts(),
        1,
        "only A sink apply should be attempted; B must remain waiting then discarded"
    );
}

/// Direct discard-on-failure: after Fail, successors are cleared from the
/// window and the ApplyContext is poisoned (not merely empty via fail-stop exit).
#[tokio::test]
async fn discard_on_failure_poisons_and_clears_successors() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(
            1,
            BatchScript::fail_after(Duration::from_millis(80), "A boom"),
        )
        .on_batch(2, BatchScript::succeed_after(Duration::from_millis(5)));

    let sink = RecordingSink::new();
    let opts = opts_window(2);
    let mut ctx = ApplyContext::new(&sink, Arc::new(transformer.clone()), &opts);

    assert!(ctx.push_change(change(1), 100u64).await.unwrap().is_none());
    assert!(ctx.push_change(change(2), 200u64).await.unwrap().is_none());
    // Window full (W=2): third change stays in the unstarted buffer and must
    // be discarded with in-flight successors on Fail.
    assert!(ctx.push_change(change(3), 300u64).await.unwrap().is_none());
    assert_eq!(
        ctx.buffer_len(),
        1,
        "C should be buffered while A,B in flight"
    );

    // B completes first and sits in `completed` waiting for ordered A.
    // flush then hits A's failure → discard_successors clears B + buffer + poisons.
    let err = ctx.flush().await.unwrap_err();
    let err_msg = format!("{err:#}");
    assert!(err_msg.contains("A boom"), "unexpected error: {err_msg}");

    let completed = transformer.completed_order();
    assert_eq!(
        completed.first(),
        Some(&2),
        "B must have completed transform before A failed: {completed:?}"
    );
    assert!(
        completed.contains(&1),
        "A must have completed (as Err): {completed:?}"
    );

    assert!(ctx.is_poisoned(), "context must be poisoned after Fail");
    assert_eq!(
        ctx.in_flight_count(),
        0,
        "in-flight must be cleared by discard_successors"
    );
    assert_eq!(
        ctx.completed_waiting_count(),
        0,
        "completed successors (B) must be discarded, not left waiting"
    );
    assert_eq!(
        ctx.buffer_len(),
        0,
        "buffer must be cleared by discard_successors"
    );
    assert!(sink.applied().is_empty(), "B must never be sunk");

    // Further use fails immediately (no hang on missing next_to_apply).
    let reuse = timeout(
        Duration::from_millis(200),
        ctx.push_change(change(3), 300u64),
    )
    .await
    .expect("push_change must not hang after Fail");
    let reuse_err = format!("{:#}", reuse.unwrap_err());
    assert!(
        reuse_err.contains("poisoned"),
        "expected poison error, got: {reuse_err}"
    );

    let flush_reuse = timeout(Duration::from_millis(200), ctx.flush())
        .await
        .expect("flush must not hang after Fail");
    let flush_err = format!("{:#}", flush_reuse.unwrap_err());
    assert!(
        flush_err.contains("poisoned"),
        "expected poison error, got: {flush_err}"
    );
}

#[tokio::test]
async fn failure_policy_fail_leaves_advances_unchanged() {
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
    assert!(feed.advances.is_empty());
    assert!(sink.applied().is_empty());
}

#[tokio::test]
async fn failure_policy_skip_advances_past_failed_batch() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::fail_after(Duration::ZERO, "skip-me"))
        .on_batch(2, BatchScript::succeed_after(Duration::ZERO));

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10), positioned(2, 20)]);
    let sink = RecordingSink::new();
    let opts = opts_window(1).with_failure_policy(FailurePolicy::Skip);

    run_change_feed_with(&mut feed, &sink, Arc::new(transformer), &opts)
        .await
        .unwrap();

    // Failed batch not written; still advanced past it; successor applied.
    assert_eq!(sink.applied().len(), 1);
    assert_eq!(sink.applied()[0].id, Value::Int64(2));
    assert_eq!(feed.advances, vec![10, 20]);
}

/// Simulated crash mid-window: recreate from last watermark advance; unadvanced batches replay.
#[tokio::test]
async fn crash_replay_from_last_advance() {
    // First run: A succeeds, B's sink fails once (crash after A advanced).
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
    assert_eq!(feed.advances, vec![100]);
    assert_eq!(sink.applied().len(), 1);
    assert_eq!(sink.applied()[0].id, Value::Int64(1));

    // Replay from last watermark advance: only unadvanced B remains.
    let mut feed2 = ScriptedChangeFeed::from_remaining(vec![positioned(2, 200)]);
    // Fresh sink that succeeds (simulating restart); prior A already durable.
    let sink2 = RecordingSink::new();
    let pipeline = Pipeline::new();
    run_change_feed(&mut feed2, &sink2, &pipeline, &opts)
        .await
        .unwrap();

    assert_eq!(feed2.advances, vec![200]);
    assert_eq!(sink2.applied().len(), 1);
    assert_eq!(sink2.applied()[0].id, Value::Int64(2));
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
        vec![Value::Int64(1), Value::Int64(2)]
    );
    assert_eq!(feed.advances, vec![100, 200]);
}

/// ApplyContext::push_change / flush return sink-safe watermarks; Fail poisons;
/// Skip advances watermark without sinking the failed batch; W≥2 stays ordered.
#[tokio::test]
async fn apply_context_push_change_flush_watermarks() {
    let sink = RecordingSink::new();
    let opts = opts_window(2);
    let pipeline = Pipeline::new();
    let mut ctx = ApplyContext::new(&sink, Arc::new(pipeline), &opts);

    // batch_size=1 → each push can sink immediately (identity JoinSet path).
    let wm = ctx.push_change(change(1), 10u64).await.unwrap();
    assert_eq!(wm, Some(10), "identity push should return sunk watermark");
    assert_eq!(sink.applied().len(), 1);

    let wm = ctx.push_change(change(2), 20u64).await.unwrap();
    assert_eq!(wm, Some(20));
    assert_eq!(
        sink.applied()
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(1), Value::Int64(2)]
    );

    // Nothing buffered → flush returns None.
    assert_eq!(ctx.flush().await.unwrap(), None);
}

#[tokio::test]
async fn apply_context_take_sunk_change_count_tracks_batch_size() {
    let sink = RecordingSink::new();
    let opts = ApplyOpts::identity().with_batch_size(2);
    let pipeline = Pipeline::new();
    let mut ctx = ApplyContext::new(&sink, Arc::new(pipeline), &opts);

    assert_eq!(ctx.push_change(change(1), 10u64).await.unwrap(), None);
    assert_eq!(ctx.take_sunk_change_count(), 0);

    let wm = ctx.push_change(change(2), 20u64).await.unwrap();
    assert_eq!(wm, Some(20));
    assert_eq!(
        ctx.take_sunk_change_count(),
        2,
        "one sunk batch of size 2 must count as 2 changes, not 1"
    );
    assert_eq!(ctx.take_sunk_change_count(), 0, "take resets the counter");
}

#[tokio::test]
async fn apply_context_flush_partial_buffer_w2() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::succeed_after(Duration::from_millis(40)))
        .on_batch(2, BatchScript::succeed_after(Duration::from_millis(5)));

    let sink = RecordingSink::new();
    let opts = opts_window(2);
    let mut ctx = ApplyContext::new(&sink, Arc::new(transformer.clone()), &opts);

    // Push both without waiting; neither may be sunk yet (A slow).
    let _ = ctx.push_change(change(1), 100u64).await.unwrap();
    let _ = ctx.push_change(change(2), 200u64).await.unwrap();

    let wm = ctx.flush().await.unwrap();
    assert_eq!(wm, Some(200), "flush should sink through last position");
    assert_eq!(
        transformer.completed_order().first(),
        Some(&2),
        "W≥2 should allow B to complete first"
    );
    assert_eq!(
        sink.applied()
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(1), Value::Int64(2)],
        "sink must stay ordered A then B"
    );
}

/// W=1 + queued buffer: A holds the window, B stays buffered; flush must
/// start B after A drains (not stop after a single start/wait/drain pass).
#[tokio::test]
async fn flush_fully_drains_w1_queued_buffer() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::succeed_after(Duration::from_millis(40)))
        .on_batch(2, BatchScript::succeed_after(Duration::from_millis(5)));

    let sink = RecordingSink::new();
    let opts = opts_window(1);
    let mut ctx = ApplyContext::new(&sink, Arc::new(transformer), &opts);

    let _ = ctx.push_change(change(1), 100u64).await.unwrap();
    let _ = ctx.push_change(change(2), 200u64).await.unwrap();
    assert!(
        ctx.buffer_len() > 0,
        "B must remain buffered while A holds W=1"
    );

    let wm = ctx.flush().await.unwrap();
    assert_eq!(wm, Some(200), "flush must drain through queued B");
    assert_eq!(ctx.buffer_len(), 0);
    assert_eq!(ctx.in_flight_count(), 0);
    assert_eq!(ctx.completed_waiting_count(), 0);
    assert_eq!(
        sink.applied()
            .iter()
            .map(|c| c.id.clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(1), Value::Int64(2)]
    );
}

#[tokio::test]
async fn apply_context_fail_poisons_context() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::fail_after(Duration::ZERO, "ctx-fail"));

    let sink = RecordingSink::new();
    let opts = opts_window(1).with_failure_policy(FailurePolicy::Fail);
    let mut ctx = ApplyContext::new(&sink, Arc::new(transformer), &opts);

    let err = ctx.push_change(change(1), 10u64).await.unwrap_err();
    assert!(format!("{err:#}").contains("ctx-fail"));
    assert!(ctx.is_poisoned());
    assert!(sink.applied().is_empty());

    let reuse = ctx.push_change(change(2), 20u64).await.unwrap_err();
    assert!(format!("{reuse:#}").contains("poisoned"));
}

#[tokio::test]
async fn apply_context_skip_returns_watermark_without_sinking_failed() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::fail_after(Duration::ZERO, "skip-me"))
        .on_batch(2, BatchScript::succeed_after(Duration::ZERO));

    let sink = RecordingSink::new();
    let opts = opts_window(1).with_failure_policy(FailurePolicy::Skip);
    let mut ctx = ApplyContext::new(&sink, Arc::new(transformer), &opts);

    let wm = ctx.push_change(change(1), 10u64).await.unwrap();
    assert_eq!(
        wm,
        Some(10),
        "Skip should still return watermark for caller advance_watermark"
    );
    assert!(
        sink.applied().is_empty(),
        "failed batch must not be sunk under Skip"
    );
    assert!(!ctx.is_poisoned());

    let wm = ctx.push_change(change(2), 20u64).await.unwrap();
    assert_eq!(wm, Some(20));
    // flush clears any residual in-flight; watermark already advanced on push.
    assert_eq!(ctx.flush().await.unwrap(), None);
    assert_eq!(sink.applied().len(), 1);
    assert_eq!(sink.applied()[0].id, Value::Int64(2));
}

// --- Relations (first-class apply) ---

fn relation(id: i64) -> Relation {
    Relation::new(
        "follows",
        Value::Int64(id),
        ThingRef::new("users", Value::Int64(id)),
        ThingRef::new("users", Value::Int64(id + 1)),
        HashMap::new(),
    )
}

#[tokio::test]
async fn apply_context_push_flush_relations_ordered_with_changes() {
    let sink = RecordingSink::new();
    let opts = opts_window(1);
    let pipeline = Pipeline::new();
    let mut ctx = ApplyContext::new(&sink, Arc::new(pipeline), &opts);

    let wm = ctx.push_change(change(1), 10u64).await.unwrap();
    assert_eq!(wm, Some(10));

    let wm = ctx
        .push_relation_change(RelationChange::create(relation(5)), 20u64)
        .await
        .unwrap();
    assert_eq!(wm, Some(20));

    let wm = ctx.push_change(change(2), 30u64).await.unwrap();
    assert_eq!(wm, Some(30));

    assert_eq!(
        sink.apply_order_tags(),
        vec![
            "change:1".to_string(),
            "relation:5".to_string(),
            "change:2".to_string(),
        ]
    );
    assert_eq!(ctx.flush().await.unwrap(), None);
}

#[tokio::test]
async fn apply_context_relation_fail_poisons_like_changes() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(1, BatchScript::fail_after(Duration::ZERO, "rel-fail"));

    let sink = RecordingSink::new();
    let opts = opts_window(1).with_failure_policy(FailurePolicy::Fail);
    let mut ctx = ApplyContext::new(&sink, Arc::new(transformer), &opts);

    let err = ctx
        .push_relation_change(RelationChange::create(relation(1)), 10u64)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("rel-fail"));
    assert!(ctx.is_poisoned());
    assert!(sink.relations_applied().is_empty());
    assert!(sink.applied().is_empty());

    let reuse = ctx
        .push_relation_change(RelationChange::create(relation(2)), 20u64)
        .await
        .unwrap_err();
    assert!(format!("{reuse:#}").contains("poisoned"));
}

#[tokio::test]
async fn write_relations_via_apply_context() {
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default();
    let pipeline = Pipeline::new();
    let ctx: ApplyContext<'_, _, _, ()> = ApplyContext::new(&sink, Arc::new(pipeline), &opts);
    ctx.write_relations(vec![relation(1)]).await.unwrap();
    assert_eq!(sink.relations_written().len(), 1);
    assert_eq!(sink.relations_written()[0][0].id, Value::Int64(1));
}

#[tokio::test]
async fn apply_relation_changes_helper() {
    let sink = RecordingSink::new();
    let pipeline = Pipeline::new();
    let opts = ApplyOpts::default();
    apply_relation_changes(
        &sink,
        &pipeline,
        vec![RelationChange::create(relation(9))],
        &opts,
    )
    .await
    .unwrap();
    assert_eq!(sink.relations_applied().len(), 1);
    assert_eq!(sink.relations_applied()[0].relation.id, Value::Int64(9));
}

/// W≥2 discard with a mixed relation+change batch window: A (change) fails after
/// B (relation) already transformed; B must never sink; context poisons.
#[tokio::test]
async fn discard_on_failure_mixed_relation_and_change_w2() {
    let transformer = ScriptedTransformer::new(Pipeline::new())
        .on_batch(
            1,
            BatchScript::fail_after(Duration::from_millis(80), "A change boom"),
        )
        .on_batch(2, BatchScript::succeed_after(Duration::from_millis(5)));

    let sink = RecordingSink::new();
    let opts = opts_window(2);
    let mut ctx = ApplyContext::new(&sink, Arc::new(transformer.clone()), &opts);

    assert!(ctx.push_change(change(1), 100u64).await.unwrap().is_none());
    assert!(ctx
        .push_relation_change(RelationChange::create(relation(2)), 200u64)
        .await
        .unwrap()
        .is_none());
    // Third item buffered while A+B in flight.
    assert!(ctx.push_change(change(3), 300u64).await.unwrap().is_none());
    assert_eq!(ctx.buffer_len(), 1);

    let err = ctx.flush().await.unwrap_err();
    let err_msg = format!("{err:#}");
    assert!(
        err_msg.contains("A change boom"),
        "unexpected error: {err_msg}"
    );

    let completed = transformer.completed_order();
    assert_eq!(
        completed.first(),
        Some(&2),
        "relation batch B must complete transform first: {completed:?}"
    );
    assert!(
        completed.contains(&1),
        "change batch A must have completed (as Err): {completed:?}"
    );

    assert!(ctx.is_poisoned());
    assert_eq!(ctx.in_flight_count(), 0);
    assert_eq!(ctx.completed_waiting_count(), 0);
    assert_eq!(ctx.buffer_len(), 0);
    assert!(
        sink.applied().is_empty() && sink.relations_applied().is_empty(),
        "neither change nor relation successors may sink after Fail: applied={:?} rels={:?}",
        sink.applied(),
        sink.relations_applied()
    );
}
