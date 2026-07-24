//! Phase 3 external transform tests: scripted transport + apply wiring.

use crate::pipeline::test_support::{
    ExternalBatchScript, RecordingSink, ScriptedChangeFeed, ScriptedExternalTransport,
};
use crate::pipeline::{run_change_feed, ApplyOpts, ExternalTransform, Pipeline, PositionedChange};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use surreal_sync_core::{Change, Value};
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

#[tokio::test]
async fn scripted_external_echo_and_out_of_order() {
    let transport = ScriptedExternalTransport::new()
        .on_batch(
            1,
            ExternalBatchScript::echo_after(Duration::from_millis(40)),
        )
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
    assert_eq!(out_a[0].id, Value::Int64(1));
    assert_eq!(out_b[0].id, Value::Int64(2));
    assert_eq!(transport.write_order(), vec![1, 2]);
}

#[tokio::test]
async fn scripted_external_bad_batch_id_fails_exchange() {
    let transport = ScriptedExternalTransport::new().on_batch(
        1,
        ExternalBatchScript::bad_batch_id_after(Duration::ZERO, 99),
    );
    let ext = ExternalTransform::with_transport(Arc::new(transport));
    let err = ext.exchange_changes(1, vec![change(1)]).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("batch_id mismatch") || msg.contains("mismatched"),
        "unexpected: {msg}"
    );
}

#[tokio::test]
async fn mismatched_batch_id_no_sink_no_advance() {
    let transport = ScriptedExternalTransport::new().on_batch(
        1,
        ExternalBatchScript::bad_batch_id_after(Duration::ZERO, 42),
    );
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
    assert!(
        feed.advances.is_empty(),
        "must not advance_watermark on bad batch_id"
    );
}

/// W≥2: a response that wrongly echoes another outstanding batch_id must not
/// be rebound onto that waiter (no sink / watermark advance for either batch).
#[tokio::test]
async fn colliding_mismatched_batch_id_w2_no_sink_no_advance() {
    let transport = ScriptedExternalTransport::new()
        // Batch 1 lies and claims batch_id=2 while 2 is also in flight.
        .on_batch(
            1,
            ExternalBatchScript::bad_batch_id_after(Duration::from_millis(5), 2),
        )
        .on_batch(
            2,
            ExternalBatchScript::echo_after(Duration::from_millis(30)),
        );

    let mut pipeline = Pipeline::new();
    pipeline.push_external(ExternalTransform::with_transport(Arc::new(transport)));

    let mut feed = ScriptedChangeFeed::new(vec![positioned(1, 10), positioned(2, 20)]);
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default()
        .with_batch_size(1)
        .with_max_in_flight(2)
        .with_timeout(Duration::from_secs(3));

    let err = timeout(
        Duration::from_secs(5),
        run_change_feed(&mut feed, &sink, &pipeline, &opts),
    )
    .await
    .expect("should not hang")
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("batch_id") || msg.contains("mismatch"),
        "unexpected: {msg}"
    );
    assert!(
        sink.applied().is_empty(),
        "must not sink either batch on colliding mismatch: {:?}",
        sink.applied()
    );
    assert!(
        feed.advances.is_empty(),
        "must not advance_watermark either batch on colliding mismatch: {:?}",
        feed.advances
    );
}

#[tokio::test]
async fn colliding_mismatched_batch_id_exchange_fails_closed() {
    let transport = ScriptedExternalTransport::new()
        .on_batch(
            1,
            ExternalBatchScript::bad_batch_id_after(Duration::from_millis(5), 2),
        )
        .on_batch(
            2,
            ExternalBatchScript::echo_after(Duration::from_millis(20)),
        );
    let ext = ExternalTransform::with_transport(Arc::new(transport));

    let a = tokio::spawn({
        let ext = ext.clone();
        async move { ext.exchange_changes(1, vec![change(1)]).await }
    });
    let b = tokio::spawn({
        let ext = ext.clone();
        async move { ext.exchange_changes(2, vec![change(2)]).await }
    });

    let err_a = a.await.unwrap().unwrap_err();
    let out_b = b.await.unwrap().unwrap();
    let msg = format!("{err_a:#}");
    assert!(
        msg.contains("batch_id mismatch") || msg.contains("mismatched"),
        "batch 1 must fail closed: {msg}"
    );
    // Batch 2 keeps its own request-keyed response (correct echo), proving we
    // did not deliver batch 1's payload under id 2.
    assert_eq!(out_b[0].id, Value::Int64(2));
}

#[tokio::test]
async fn missing_batch_id_no_sink_no_advance() {
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
    assert!(feed.advances.is_empty());
}

#[tokio::test]
async fn scripted_external_happy_path_sinks_and_advances() {
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
    assert_eq!(feed.advances, vec![10, 20]);
}

#[tokio::test]
async fn external_exchanges_relation_changes_over_wire() {
    use surreal_sync_core::{Relation, RelationChange, ThingRef};

    let transport = ScriptedExternalTransport::new();
    let ext = ExternalTransform::with_transport(Arc::new(transport.clone()));
    let rel = Relation::new(
        "follows",
        Value::Int64(7),
        ThingRef::new("users", Value::Int64(1)),
        ThingRef::new("users", Value::Int64(2)),
        HashMap::new(),
    );
    let out = ext
        .exchange_relation_changes(1, vec![RelationChange::create(rel)])
        .await
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].relation.id, Value::Int64(7));
    assert_eq!(
        transport.write_kinds(),
        vec![(1, crate::pipeline::WireItemKind::RelationChange)],
        "RelationChange must be recorded on the transport wire kind"
    );
}

#[tokio::test]
async fn external_pipeline_transforms_mixed_relation_and_change() {
    use crate::pipeline::{run_source_runtime, PositionedEvent, SourceRuntimeOpts};
    use surreal_sync_core::{Relation, RelationChange, ThingRef};

    let transport = ScriptedExternalTransport::new();
    let mut pipeline = Pipeline::new();
    pipeline.push_external(ExternalTransform::with_transport(Arc::new(transport)));

    let rel = Relation::new(
        "follows",
        Value::Int64(5),
        ThingRef::new("users", Value::Int64(5)),
        ThingRef::new("users", Value::Int64(6)),
        HashMap::new(),
    );
    let mut driver = crate::pipeline::test_support::ScriptedSourceDriver::new(vec![
        PositionedEvent::change(change(1), 10u64),
        PositionedEvent::relation_change(RelationChange::create(rel), 20u64),
    ]);
    let sink = RecordingSink::new();
    let opts = ApplyOpts::default()
        .with_batch_size(1)
        .with_max_in_flight(1)
        .with_timeout(Duration::from_secs(2));

    run_source_runtime(
        &mut driver,
        &sink,
        &pipeline,
        &opts,
        &SourceRuntimeOpts::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        sink.apply_order_tags(),
        vec!["change:1".to_string(), "relation:5".to_string()]
    );
    assert_eq!(driver.advances, vec![10, 20]);
}

/// One apply batch with both kinds: External must use distinct wire batch_ids
/// (plain for changes, high-bit for relations) and RelationChange kind.
#[tokio::test]
async fn mixed_external_batch_uses_distinct_wire_batch_ids_and_kinds() {
    use crate::pipeline::{relation_wire_batch_id, ApplyEvent, WireItemKind};
    use surreal_sync_core::{Relation, RelationChange, ThingRef};

    let transport = ScriptedExternalTransport::new();
    let mut pipeline = Pipeline::new();
    pipeline.push_external(ExternalTransform::with_transport(Arc::new(
        transport.clone(),
    )));

    let rel = Relation::new(
        "follows",
        Value::Int64(9),
        ThingRef::new("users", Value::Int64(1)),
        ThingRef::new("users", Value::Int64(2)),
        HashMap::new(),
    );
    let events = vec![
        ApplyEvent::Change(change(1)),
        ApplyEvent::relation_change(RelationChange::create(rel)),
    ];
    let out = pipeline.apply_events_async(42, events).await.unwrap();
    assert_eq!(out.len(), 2);
    assert!(out[0].is_change());
    assert!(out[1].is_relation_change());

    let kinds = transport.write_kinds();
    assert_eq!(
        kinds,
        vec![
            (42, WireItemKind::Change),
            (relation_wire_batch_id(42), WireItemKind::RelationChange),
        ],
        "mixed External must not reuse batch_id across change vs relation exchanges"
    );
    assert_ne!(kinds[0].0, kinds[1].0);
}
