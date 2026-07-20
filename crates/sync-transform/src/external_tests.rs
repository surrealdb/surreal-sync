//! Phase 3 external transform tests: scripted transport + apply wiring.

use crate::test_support::{
    ExternalBatchScript, RecordingSink, ScriptedChangeFeed, ScriptedExternalTransport,
};
use crate::{
    run_change_feed, ApplyOpts, ExternalTransform, Pipeline, PositionedChange,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use sync_core::{UniversalChange, UniversalValue};
use tokio::time::timeout;

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

#[tokio::test]
async fn scripted_external_echo_and_out_of_order() {
    let transport = ScriptedExternalTransport::new()
        .on_batch(1, ExternalBatchScript::echo_after(Duration::from_millis(40)))
        .on_batch(2, ExternalBatchScript::echo_after(Duration::from_millis(5)));

    let ext = ExternalTransform::with_transport(Arc::new(transport.clone()));

    let a = tokio::spawn({
        let ext = ext.clone();
        async move { ext.exchange_changes(1, vec![change(1)]).await }
    });
    let b = tokio::spawn({
        let ext = ext.clone();
        async move { ext.exchange_changes(2, vec![change(2)]).await }
    });

    let out_b = b.await.unwrap().unwrap();
    let out_a = a.await.unwrap().unwrap();
    assert_eq!(out_a[0].id, UniversalValue::Int64(1));
    assert_eq!(out_b[0].id, UniversalValue::Int64(2));
    assert_eq!(transport.write_order(), vec![1, 2]);
}

#[tokio::test]
async fn scripted_external_bad_batch_id_fails_exchange() {
    let transport = ScriptedExternalTransport::new()
        .on_batch(1, ExternalBatchScript::bad_batch_id_after(Duration::ZERO, 99));
    let ext = ExternalTransform::with_transport(Arc::new(transport));
    let err = ext
        .exchange_changes(1, vec![change(1)])
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("batch_id mismatch") || msg.contains("mismatched"),
        "unexpected: {msg}"
    );
}

#[tokio::test]
async fn mismatched_batch_id_no_sink_no_commit() {
    let transport = ScriptedExternalTransport::new()
        .on_batch(1, ExternalBatchScript::bad_batch_id_after(Duration::ZERO, 42));
    let mut pipeline = Pipeline::new();
    pipeline.push_external(ExternalTransform::with_transport(Arc::new(transport)));

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10), positioned(2, 20)]);
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default()
        .with_batch_size(1)
        .with_max_in_flight(1)
        .with_timeout(Duration::from_secs(2));

    let err = run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("batch_id") || msg.contains("mismatch"),
        "unexpected: {msg}"
    );
    assert!(sink.applied().is_empty(), "must not sink on bad batch_id");
    assert!(feed.commits.is_empty(), "must not commit on bad batch_id");
}

#[tokio::test]
async fn missing_batch_id_no_sink_no_commit() {
    let transport = ScriptedExternalTransport::new().on_batch(
        1,
        ExternalBatchScript {
            missing_batch_id: true,
            ..Default::default()
        },
    );
    let mut pipeline = Pipeline::new();
    pipeline.push_external(ExternalTransform::with_transport(Arc::new(transport)));

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10)]);
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default()
        .with_batch_size(1)
        .with_timeout(Duration::from_secs(2));

    let err = timeout(
        Duration::from_secs(3),
        run_change_feed(&mut feed, &sink, &pipeline, &opts),
    )
    .await
    .expect("should not hang")
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("batch_id") || msg.contains("header"),
        "unexpected: {msg}"
    );
    assert!(sink.applied().is_empty());
    assert!(feed.commits.is_empty());
}

#[tokio::test]
async fn scripted_external_happy_path_sinks_and_commits() {
    let transport = ScriptedExternalTransport::new();
    let mut pipeline = Pipeline::new();
    pipeline.push_external(ExternalTransform::with_transport(Arc::new(transport)));

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10), positioned(2, 20)]);
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default().with_batch_size(1);

    run_change_feed(&mut feed, &sink, &pipeline, &opts)
        .await
        .unwrap();
    assert_eq!(sink.applied().len(), 2);
    assert_eq!(feed.commits, vec![10, 20]);
}
