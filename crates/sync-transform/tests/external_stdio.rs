//! Child-stdio fixture-worker integration tests (persistent + transient).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use sync_core::{UniversalChange, UniversalValue};
use sync_transform::test_support::{RecordingSink, ScriptedChangeFeed};
use sync_transform::{
    run_change_feed, write_rows, ApplyOpts, ChildStdioMode, ConfiguredStage, ExternalTransform,
    FramerKind, Pipeline, PositionedChange,
};

fn fixture_cmd(mode: &str) -> Vec<String> {
    let bin = env!("CARGO_BIN_EXE_sync-transform-fixture-worker");
    vec![bin.to_string(), mode.to_string()]
}

fn change(id: i64, name: &str) -> UniversalChange {
    let mut data = HashMap::new();
    data.insert(
        "name".to_string(),
        UniversalValue::VarChar {
            value: name.to_string(),
            length: 64,
        },
    );
    UniversalChange::create("users", UniversalValue::Int64(id), data)
}

fn positioned(id: i64, pos: u64) -> PositionedChange<u64> {
    PositionedChange::new(change(id, &format!("row-{id}")), pos)
}

#[tokio::test]
async fn persistent_echo_and_mutate() {
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        fixture_cmd("echo"),
        FramerKind::Ndjson,
    )
    .expect("spawn echo worker");
    let out = ext
        .exchange_changes(7, vec![change(1, "alice")])
        .await
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].id, UniversalValue::Int64(1));

    let ext_m = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        fixture_cmd("mutate"),
        FramerKind::Ndjson,
    )
    .expect("spawn mutate worker");
    let out = ext_m
        .exchange_changes(8, vec![change(2, "bob")])
        .await
        .unwrap();
    let name = out[0]
        .data
        .as_ref()
        .unwrap()
        .get("name")
        .expect("name field");
    match name {
        UniversalValue::VarChar { value, .. } => assert_eq!(value, "mutated"),
        other => panic!("expected VarChar, got {other:?}"),
    }
}

#[tokio::test]
async fn persistent_multiplex_batch_ids() {
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        fixture_cmd("echo"),
        FramerKind::Ndjson,
    )
    .expect("spawn");
    let a = {
        let ext = ext.clone();
        tokio::spawn(async move { ext.exchange_changes(1, vec![change(1, "a")]).await })
    };
    let b = {
        let ext = ext.clone();
        tokio::spawn(async move { ext.exchange_changes(2, vec![change(2, "b")]).await })
    };
    let out_a = a.await.unwrap().unwrap();
    let out_b = b.await.unwrap().unwrap();
    assert_eq!(out_a[0].id, UniversalValue::Int64(1));
    assert_eq!(out_b[0].id, UniversalValue::Int64(2));
}

#[tokio::test]
async fn transient_smoke() {
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Transient,
        fixture_cmd("echo"),
        FramerKind::Ndjson,
    )
    .expect("transient");
    let out = ext.exchange_changes(1, vec![change(9, "x")]).await.unwrap();
    assert_eq!(out[0].id, UniversalValue::Int64(9));

    // Second batch respawns.
    let out2 = ext
        .exchange_changes(2, vec![change(10, "y")])
        .await
        .unwrap();
    assert_eq!(out2[0].id, UniversalValue::Int64(10));
}

#[tokio::test]
async fn persistent_bad_batch_id_no_sink_no_advance() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(
            ChildStdioMode::Persistent,
            fixture_cmd("bad-batch-id"),
            FramerKind::Ndjson,
        )
        .unwrap(),
    );

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 100)]);
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default()
        .with_batch_size(1)
        .with_timeout(Duration::from_secs(3));

    let err = run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("batch_id"),
        "unexpected: {err:#}"
    );
    assert!(sink.applied().is_empty());
    assert!(feed.advances.is_empty());
}

/// W≥2 fixture-worker: first response forges `batch_id` to the next write's id.
/// PersistentChildStdio must fail closed — no sink, no watermark advance for either batch.
#[tokio::test]
async fn persistent_w2_colliding_batch_id_mismatch_no_sink_no_advance() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(
            ChildStdioMode::Persistent,
            fixture_cmd("bad-batch-id"),
            FramerKind::Ndjson,
        )
        .unwrap(),
    );

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 100), positioned(2, 200)]);
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default()
        .with_batch_size(1)
        .with_max_in_flight(2)
        .with_timeout(Duration::from_secs(5));

    let err = run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("batch_id") || msg.contains("mismatch"),
        "unexpected: {msg}"
    );
    assert!(
        sink.applied().is_empty(),
        "must not sink on colliding mismatch: {:?}",
        sink.applied()
    );
    assert!(
        feed.advances.is_empty(),
        "must not advance_watermark on colliding mismatch: {:?}",
        feed.advances
    );
}

#[tokio::test]
async fn persistent_missing_batch_id_no_sink_no_advance() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(
            ChildStdioMode::Persistent,
            fixture_cmd("missing-batch-id"),
            FramerKind::Ndjson,
        )
        .unwrap(),
    );

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 100)]);
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default()
        .with_batch_size(1)
        .with_timeout(Duration::from_secs(3));

    let err = run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("batch_id") || msg.contains("header") || msg.contains("missing field"),
        "unexpected: {msg}"
    );
    assert!(sink.applied().is_empty());
    assert!(feed.advances.is_empty());
}

#[tokio::test]
async fn persistent_via_run_change_feed_echo() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(
            ChildStdioMode::Persistent,
            fixture_cmd("echo"),
            FramerKind::Ndjson,
        )
        .unwrap(),
    );

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10), positioned(2, 20)]);
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default().with_batch_size(1);

    run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap();
    assert_eq!(sink.applied().len(), 2);
    assert_eq!(feed.advances, vec![10, 20]);
}

#[tokio::test]
async fn write_rows_through_external_mutate() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(
            ChildStdioMode::Persistent,
            fixture_cmd("mutate"),
            FramerKind::Ndjson,
        )
        .unwrap(),
    );
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default();
    let row = sync_core::UniversalRow::builder("users", 0, UniversalValue::Int64(1))
        .field(
            "name",
            UniversalValue::VarChar {
                value: "alice".into(),
                length: 64,
            },
        )
        .build();
    write_rows(&sink, &pipeline, vec![row], &opts)
        .await
        .unwrap();
    // write_rows → Update upserts coalesce to write_universal_rows inside the window.
    let written = sink.rows_written();
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].len(), 1);
    let name = written[0][0].fields.get("name").expect("name field");
    match name {
        UniversalValue::VarChar { value, .. } => assert_eq!(value, "mutated"),
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn pipeline_from_config_spawns_external() {
    let bin = env!("CARGO_BIN_EXE_sync-transform-fixture-worker");
    let toml = format!(
        r#"
[pipeline]
batch_size = 1

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "echo"]
"#
    );
    let cfg = sync_transform::parse_transforms_toml(&toml).unwrap();
    let opts = ApplyOpts::from_transforms_config(&cfg);
    assert_eq!(opts.batch_size, 1);
    let pipeline = Pipeline::from_config(&cfg).unwrap();
    assert!(!pipeline.is_identity());
    assert_eq!(pipeline.len(), 1);

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10)]);
    let sink = RecordingSink::new();
    run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap();
    assert_eq!(sink.applied().len(), 1);
    assert_eq!(feed.advances, vec![10]);
}

#[tokio::test]
async fn two_command_stages_daisy_chain_distinct_workers() {
    let bin = env!("CARGO_BIN_EXE_sync-transform-fixture-worker");
    let toml = format!(
        r#"
[pipeline]
batch_size = 1

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "echo"]
stdio.framer = "ndjson"

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "mutate"]
stdio.framer = "ndjson"
"#
    );
    let cfg = sync_transform::parse_transforms_toml(&toml).unwrap();
    assert_eq!(cfg.stages.len(), 2);
    let pipeline = Pipeline::from_config(&cfg).unwrap();
    assert_eq!(pipeline.len(), 2);
    let opts = ApplyOpts::from_transforms_config(&cfg);

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10)]);
    let sink = RecordingSink::new();
    run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap();
    assert_eq!(sink.applied().len(), 1);
    let applied = sink.applied();
    let name = applied[0]
        .data
        .as_ref()
        .unwrap()
        .get("name")
        .expect("name field");
    match name {
        UniversalValue::VarChar { value, .. } => assert_eq!(value, "mutated"),
        other => panic!("expected mutated name, got {other:?}"),
    }
    assert_eq!(feed.advances, vec![10]);
}

#[tokio::test]
async fn per_stage_retry_recovers_from_framed_error() {
    let bin = env!("CARGO_BIN_EXE_sync-transform-fixture-worker");
    let toml = format!(
        r#"
[pipeline]
batch_size = 1

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "error-once"]
stdio.framer = "ndjson"
retry.max_attempts = 3
retry.initial_backoff = "1ms"
retry.max_backoff = "5ms"
retry.jitter = false
"#
    );
    let cfg = sync_transform::parse_transforms_toml(&toml).unwrap();
    let pipeline = Pipeline::from_config(&cfg).unwrap();
    let opts = ApplyOpts::from_transforms_config(&cfg);

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10)]);
    let sink = RecordingSink::new();
    run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap();
    assert_eq!(sink.applied().len(), 1);
    assert_eq!(feed.advances, vec![10]);
}

#[tokio::test]
async fn per_stage_retry_exhausted_fails_batch() {
    let bin = env!("CARGO_BIN_EXE_sync-transform-fixture-worker");
    // bad-batch-id fails every exchange; retries should exhaust then fail.
    let toml = format!(
        r#"
[pipeline]
batch_size = 1
failure_policy = "fail"

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "bad-batch-id"]
retry.max_attempts = 2
retry.initial_backoff = "1ms"
retry.max_backoff = "2ms"
retry.jitter = false
"#
    );
    let cfg = sync_transform::parse_transforms_toml(&toml).unwrap();
    let pipeline = Pipeline::from_config(&cfg).unwrap();
    let opts = ApplyOpts::from_transforms_config(&cfg);

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10)]);
    let sink = RecordingSink::new();
    let err = run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("batch_id") || msg.contains("attempt"),
        "unexpected: {msg}"
    );
    assert!(sink.applied().is_empty());
    assert!(feed.advances.is_empty());
}

/// Two daisy-chained stages with different `retry.max_attempts` behave differently:
/// stage 1 (`error-once`) recovers only when its attempts allow a retry; stage 2
/// always fails (`bad-batch-id`) so a succeeding stage-1 must still fail the batch.
#[tokio::test]
async fn two_stages_different_max_attempts_behave_differently() {
    let bin = env!("CARGO_BIN_EXE_sync-transform-fixture-worker");

    // Stage 1 max_attempts=1: error-once fails immediately; never reaches stage 2.
    let fail_fast = format!(
        r#"
[pipeline]
batch_size = 1
failure_policy = "fail"

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "error-once"]
retry.max_attempts = 1
retry.jitter = false

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "echo"]
retry.max_attempts = 5
retry.jitter = false
"#
    );
    let cfg = sync_transform::parse_transforms_toml(&fail_fast).unwrap();
    let pipeline = Pipeline::from_config(&cfg).unwrap();
    let opts = ApplyOpts::from_transforms_config(&cfg);
    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10)]);
    let sink = RecordingSink::new();
    let err = run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("error") || msg.contains("attempt"),
        "stage1 with max_attempts=1 should fail: {msg}"
    );
    assert!(sink.applied().is_empty());
    assert!(feed.advances.is_empty());

    // Stage 1 max_attempts=3: error-once recovers; stage 2 bad-batch-id fails
    // (max_attempts=1) — proves per-stage retry wiring, not a shared policy.
    let recover_then_fail = format!(
        r#"
[pipeline]
batch_size = 1
failure_policy = "fail"

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "error-once"]
retry.max_attempts = 3
retry.initial_backoff = "1ms"
retry.max_backoff = "5ms"
retry.jitter = false

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "bad-batch-id"]
retry.max_attempts = 1
retry.jitter = false
"#
    );
    let cfg = sync_transform::parse_transforms_toml(&recover_then_fail).unwrap();
    assert_eq!(cfg.stages.len(), 2);
    let ConfiguredStage::Command(a) = &cfg.stages[0] else {
        panic!("expected Command stage");
    };
    let ConfiguredStage::Command(b) = &cfg.stages[1] else {
        panic!("expected Command stage");
    };
    assert_eq!(a.retry.max_attempts, 3);
    assert_eq!(b.retry.max_attempts, 1);

    let pipeline = Pipeline::from_config(&cfg).unwrap();
    let opts = ApplyOpts::from_transforms_config(&cfg);
    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 20)]);
    let sink = RecordingSink::new();
    let err = run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("batch_id") || msg.contains("mismatch") || msg.contains("attempt"),
        "stage2 should fail after stage1 recovered: {msg}"
    );
    assert!(sink.applied().is_empty());
    assert!(feed.advances.is_empty());

    // Same chain but stage 2 echoes: whole pipeline succeeds only when both
    // stages' retry policies allow progress.
    let both_ok = format!(
        r#"
[pipeline]
batch_size = 1

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "error-once"]
retry.max_attempts = 3
retry.initial_backoff = "1ms"
retry.max_backoff = "5ms"
retry.jitter = false

[[transforms]]
type = "command"
mode = "persistent"
command = [{bin:?}, "echo"]
retry.max_attempts = 1
"#
    );
    let cfg = sync_transform::parse_transforms_toml(&both_ok).unwrap();
    let pipeline = Pipeline::from_config(&cfg).unwrap();
    let opts = ApplyOpts::from_transforms_config(&cfg);
    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 30)]);
    let sink = RecordingSink::new();
    run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap();
    assert_eq!(sink.applied().len(), 1);
    assert_eq!(feed.advances, vec![30]);
}

// Silence unused when test-support feature shapes differ.
#[allow(dead_code)]
fn _arc_pipeline(p: Pipeline) -> Arc<Pipeline> {
    Arc::new(p)
}
