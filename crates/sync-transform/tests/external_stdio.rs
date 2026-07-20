//! Child-stdio fixture-worker integration tests (persistent + transient).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use sync_core::{UniversalChange, UniversalValue};
use sync_transform::test_support::{RecordingSink, ScriptedChangeFeed};
use sync_transform::{
    run_change_feed, write_rows, ApplyOpts, ChildStdioMode, ExternalTransform, Pipeline,
    PositionedChange,
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
    let ext = ExternalTransform::child_stdio(ChildStdioMode::Persistent, fixture_cmd("echo"))
        .expect("spawn echo worker");
    let out = ext
        .exchange_changes(7, vec![change(1, "alice")])
        .await
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].id, UniversalValue::Int64(1));

    let ext_m =
        ExternalTransform::child_stdio(ChildStdioMode::Persistent, fixture_cmd("mutate"))
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
    let ext = ExternalTransform::child_stdio(ChildStdioMode::Persistent, fixture_cmd("echo"))
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
    let ext = ExternalTransform::child_stdio(ChildStdioMode::Transient, fixture_cmd("echo"))
        .expect("transient");
    let out = ext
        .exchange_changes(1, vec![change(9, "x")])
        .await
        .unwrap();
    assert_eq!(out[0].id, UniversalValue::Int64(9));

    // Second batch respawns.
    let out2 = ext
        .exchange_changes(2, vec![change(10, "y")])
        .await
        .unwrap();
    assert_eq!(out2[0].id, UniversalValue::Int64(10));
}

#[tokio::test]
async fn persistent_bad_batch_id_no_sink_no_commit() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(ChildStdioMode::Persistent, fixture_cmd("bad-batch-id"))
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
    assert!(feed.commits.is_empty());
}

/// W≥2 fixture-worker: first response forges `batch_id` to the next write's id.
/// PersistentChildStdio must fail closed — no sink, no commit for either batch.
#[tokio::test]
async fn persistent_w2_colliding_batch_id_mismatch_no_sink_no_commit() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(ChildStdioMode::Persistent, fixture_cmd("bad-batch-id"))
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
        feed.commits.is_empty(),
        "must not commit on colliding mismatch: {:?}",
        feed.commits
    );
}

#[tokio::test]
async fn persistent_missing_batch_id_no_sink_no_commit() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(
            ChildStdioMode::Persistent,
            fixture_cmd("missing-batch-id"),
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
    assert!(feed.commits.is_empty());
}

#[tokio::test]
async fn persistent_via_run_change_feed_echo() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(ChildStdioMode::Persistent, fixture_cmd("echo")).unwrap(),
    );

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10), positioned(2, 20)]);
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default().with_batch_size(1);

    run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap();
    assert_eq!(sink.applied().len(), 2);
    assert_eq!(feed.commits, vec![10, 20]);
}

#[tokio::test]
async fn write_rows_through_external_mutate() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(ChildStdioMode::Persistent, fixture_cmd("mutate"))
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
    let written = sink.rows_written();
    assert_eq!(written.len(), 1);
    match written[0][0].get_field("name") {
        Some(UniversalValue::VarChar { value, .. }) => assert_eq!(value, "mutated"),
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn pipeline_from_config_spawns_external() {
    let bin = env!("CARGO_BIN_EXE_sync-transform-fixture-worker");
    let toml = format!(
        r#"
[[transforms]]
type = "external"
batch_size = 1
stdin.mode = "persistent"
stdin.command = [{bin:?}, "echo"]
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
    assert_eq!(feed.commits, vec![10]);
}

// Silence unused when test-support feature shapes differ.
#[allow(dead_code)]
fn _arc_pipeline(p: Pipeline) -> Arc<Pipeline> {
    Arc::new(p)
}
