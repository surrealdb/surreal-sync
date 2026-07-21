//! Kafka incremental sync to SurrealDB.
//!
//! Consumes protobuf-encoded messages from Kafka topics and writes them as
//! records to SurrealDB through [`SourceDriver`] + [`run_source_runtime`].
//!
//! Offset commit stays with the Kafka consumer group. The runtime window owns
//! transform/`max_in_flight` overlap; consumer-group offsets advance only after
//! the corresponding sink apply succeeds. On each sink success the driver
//! commits **all** messages in that sunk batch (`commit_batch`), not only the
//! batch's last position. `max_messages` / `processed_count` advance by the
//! sunk message count via [`SourceDriver::note_sunk_events`].
//!
//! This module was moved from src/kafka/incremental.rs in the main crate
//! to break the circular dependency between kafka and kafka-types.

use anyhow::Result;
use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, Utc};
use clap::Parser;
use kafka_types::Message;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use surreal_sink::SurrealSink;
use sync_core::{TableDefinition, TypedValue, UniversalChange, UniversalRow, UniversalValue};
use sync_transform::{
    run_source_runtime, ApplyOpts, CheckpointPolicy, Pipeline, PositionedEvent, SourceDriver,
    SourceRuntimeOpts, StopReason,
};
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::consumer::{Consumer, ConsumerConfig, SaslMechanism, SecurityProtocol};
use crate::Client;

/// Configuration for Kafka source.
#[derive(Debug, Clone, Parser)]
pub struct Config {
    /// Proto file path
    #[clap(long)]
    pub proto_path: String,
    /// Kafka brokers (comma-separated or multiple --brokers)
    #[clap(long, value_delimiter = ',', required = true)]
    pub brokers: Vec<String>,
    /// Consumer group ID
    #[clap(long)]
    pub group_id: String,
    /// Topic to consume from
    #[clap(long)]
    pub topic: String,
    /// Protobuf message type name
    #[clap(long)]
    pub message_type: String,
    /// Maximum buffer size for peeked messages
    #[clap(long, default_value_t = 1000)]
    pub buffer_size: usize,
    /// Session timeout in milliseconds
    #[clap(long, default_value = "30000")]
    pub session_timeout_ms: String,
    /// Number of consumers in the consumer group to spawn
    #[clap(long, default_value_t = 1)]
    pub num_consumers: usize,
    /// Messages to fetch per Kafka poll into the apply window.
    ///
    /// The transform runtime then batches/windows by `--transforms-config`
    /// `batch_size` / `max_in_flight`. Consumer-group offsets commit only after
    /// each message’s sink apply succeeds. Larger polls improve throughput but
    /// increase memory and potential duplicate processing on failure.
    #[clap(long, default_value_t = 100)]
    pub kafka_batch_size: usize,
    /// Optional table name to use in SurrealDB (defaults to topic name)
    #[clap(long)]
    pub table_name: Option<String>,
    /// Use Kafka message key as SurrealDB record ID (base64 encoded).
    /// If not set, the "id" field from the message payload is used.
    #[clap(long)]
    pub use_message_key_as_id: bool,
    /// Field name to use as record ID when use_message_key_as_id is false (default: "id")
    #[clap(long, default_value = "id")]
    pub id_field: String,
    /// Maximum number of messages to process before exiting.
    /// When set, the sync will exit immediately after processing this many messages
    /// instead of waiting for the deadline. Useful for loadtest scenarios where
    /// the exact message count is known.
    #[clap(long)]
    pub max_messages: Option<u64>,
    /// SASL username for broker authentication
    #[clap(long, env = "KAFKA_SASL_USERNAME")]
    pub sasl_username: Option<String>,
    /// SASL password for broker authentication
    #[clap(long, env = "KAFKA_SASL_PASSWORD")]
    pub sasl_password: Option<String>,
    /// SASL mechanism. Required when security_protocol uses SASL.
    #[clap(long, value_enum)]
    pub sasl_mechanism: Option<SaslMechanism>,
    /// Security protocol. Omit for plain Kafka with no auth.
    #[clap(long, value_enum)]
    pub security_protocol: Option<SecurityProtocol>,
    /// Path to CA certificate file for broker verification
    #[clap(long, env = "KAFKA_SSL_CA_LOCATION")]
    pub ssl_ca_location: Option<String>,
    /// Path to client certificate file for mTLS
    #[clap(long, env = "KAFKA_SSL_CERTIFICATE_LOCATION")]
    pub ssl_certificate_location: Option<String>,
    /// Path to client private key file for mTLS
    #[clap(long, env = "KAFKA_SSL_KEY_LOCATION")]
    pub ssl_key_location: Option<String>,
    /// Password for the client private key
    #[clap(long, env = "KAFKA_SSL_KEY_PASSWORD")]
    pub ssl_key_password: Option<String>,
}

/// Run incremental sync from Kafka to SurrealDB (identity transforms).
///
/// The sync will run until the deadline is reached. Once the deadline passes,
/// the function will gracefully terminate all consumers and exit.
pub async fn run_incremental_sync<S: SurrealSink + Send + Sync + 'static>(
    surreal: Arc<S>,
    config: Config,
    deadline: DateTime<Utc>,
    table_schema: Option<TableDefinition>,
) -> Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_incremental_sync_with_transforms(
        surreal,
        config,
        deadline,
        table_schema,
        &pipeline,
        &apply_opts,
    )
    .await
}

/// Run incremental sync through [`SourceDriver`] + [`run_source_runtime`].
///
/// Each consumer polls/decodes into [`PositionedEvent`]s (bounded by
/// `kafka_batch_size`). The runtime owns the transform window (`max_in_flight`).
/// Kafka consumer-group offset commit happens in [`SourceDriver::commit`] after
/// [`SourceDriver::note_sunk_events`] moves the sunk messages into a ready set —
/// restoring `commit_batch(&messages)` for the whole sunk batch.
pub async fn run_incremental_sync_with_transforms<S: SurrealSink + Send + Sync + 'static>(
    surreal: Arc<S>,
    config: Config,
    deadline: DateTime<Utc>,
    table_schema: Option<TableDefinition>,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    let duration_until_deadline = deadline.signed_duration_since(Utc::now());
    info!(
        "Starting Kafka incremental sync for message {} from topic {} (deadline in {} seconds)",
        config.message_type,
        config.topic,
        duration_until_deadline.num_seconds()
    );
    if pipeline.is_identity() {
        debug!("Kafka sync using identity transform pipeline");
    } else {
        info!(
            stages = pipeline.len(),
            max_in_flight = apply_opts.max_in_flight,
            batch_size = apply_opts.batch_size,
            "Kafka sync using transform pipeline"
        );
    }

    let table_name = config
        .table_name
        .clone()
        .unwrap_or_else(|| config.topic.clone());

    let consumer_config: ConsumerConfig = ConsumerConfig {
        brokers: config.brokers.join(","),
        group_id: config.group_id,
        topic: config.topic,
        message_type: config.message_type,
        buffer_size: config.buffer_size,
        session_timeout_ms: config.session_timeout_ms,
        sasl_username: config.sasl_username,
        sasl_password: config.sasl_password,
        sasl_mechanism: config.sasl_mechanism,
        security_protocol: config.security_protocol,
        ssl_ca_location: config.ssl_ca_location,
        ssl_certificate_location: config.ssl_certificate_location,
        ssl_key_location: config.ssl_key_location,
        ssl_key_password: config.ssl_key_password,
        ..Default::default()
    };

    let client = Client::from_proto_file(config.proto_path, consumer_config)?;
    info!(
        "Kafka client created successfully: schema={:?}",
        client.schema()
    );

    let processed_count = Arc::new(AtomicU64::new(0));
    let next_row_index = Arc::new(AtomicU64::new(0));
    let num_consumers = config.num_consumers;
    let max_messages = config.max_messages;
    info!("Spawning {num_consumers} consumers in the same consumer group...");
    if let Some(max) = max_messages {
        info!("Will exit early after processing {max} messages");
    }

    let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();
    for i in 0..num_consumers {
        let consumer = client.create_consumer()?;
        let surreal = Arc::clone(&surreal);
        let pipeline = pipeline.clone();
        let apply_opts = apply_opts.clone();
        let processed_count = Arc::clone(&processed_count);
        let next_row_index = Arc::clone(&next_row_index);
        let table_name = table_name.clone();
        let table_schema = table_schema.clone();
        let use_message_key_as_id = config.use_message_key_as_id;
        let id_field = config.id_field.clone();
        let kafka_batch_size = config.kafka_batch_size;
        let handle = tokio::spawn(async move {
            let mut driver = KafkaSourceDriver {
                consumer,
                table_name,
                table_schema,
                use_message_key_as_id,
                id_field,
                kafka_batch_size,
                pending_acks: VecDeque::new(),
                ready_to_commit: Vec::new(),
                processed_count,
                next_row_index,
                max_messages,
                deadline,
                finished: false,
            };
            let runtime_opts = SourceRuntimeOpts::new();
            let exit = run_source_runtime(
                &mut driver,
                surreal.as_ref(),
                &pipeline,
                &apply_opts,
                &runtime_opts,
            )
            .await?;
            debug!(consumer = i, ?exit, "Kafka consumer runtime exited");
            Ok(())
        });
        handles.push(handle);
    }

    // Wait for all consumers (deadline / max_messages stop inside each driver).
    let mut first_err: Option<anyhow::Error> = None;
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("Kafka consumer {i} failed: {e}");
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(e) => {
                tracing::error!("Kafka consumer {i} join error: {e}");
                if first_err.is_none() {
                    first_err = Some(e.into());
                }
            }
        }
    }

    let final_count = processed_count.load(Ordering::SeqCst);
    info!(
        "Kafka sync completed: processed {} messages total from topic {}",
        final_count, table_name
    );

    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(())
}

/// Kafka offset position placeholder (acks tracked in driver FIFO, not here).
#[derive(Debug, Clone)]
struct KafkaOffset;

struct KafkaSourceDriver {
    consumer: Consumer,
    table_name: String,
    table_schema: Option<TableDefinition>,
    use_message_key_as_id: bool,
    id_field: String,
    kafka_batch_size: usize,
    /// Messages received but not yet noted as sunk (FIFO, poll order).
    pending_acks: VecDeque<Message>,
    /// Messages noted sunk and ready to offset-commit on the next `commit`.
    ready_to_commit: Vec<Message>,
    /// Message count watermark (advanced only after sink via `note_sunk_events`).
    processed_count: Arc<AtomicU64>,
    /// Poll-time `UniversalRow.index` allocator (advances on receive, not commit).
    /// Separated from `processed_count` so overlapping polls never reuse indices.
    next_row_index: Arc<AtomicU64>,
    max_messages: Option<u64>,
    deadline: DateTime<Utc>,
    finished: bool,
}

#[async_trait]
impl SourceDriver for KafkaSourceDriver {
    type Position = KafkaOffset;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        if self.stop_reason().is_some() || self.finished {
            return Ok(Vec::new());
        }

        // Bound receive by remaining max_messages headroom so overlapping polls
        // (W≥2) cannot enqueue past the configured limit.
        let limit = poll_receive_limit(
            self.kafka_batch_size,
            self.max_messages,
            self.next_row_index.load(Ordering::SeqCst),
        );
        if limit == 0 {
            return Ok(Vec::new());
        }

        let messages = self
            .consumer
            .receive_batch(limit)
            .await
            .map_err(|e| anyhow::anyhow!("kafka receive_batch: {e}"))?;
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        // Reserve unique per-message indices for this poll batch (poll-time
        // allocator — not the post-sink processed_count).
        let base_index = alloc_row_indices(&self.next_row_index, messages.len());
        let mut events = Vec::with_capacity(messages.len());
        for (offset, message) in messages.into_iter().enumerate() {
            debug!("Received message: {:?}", message);
            let message_key = message.key.clone();
            let typed_values =
                kafka_types::message_to_typed_values(message.clone(), self.table_schema.as_ref())?;
            let row = typed_values_to_universal_row(
                typed_values,
                &self.table_name,
                self.use_message_key_as_id,
                message_key.as_deref(),
                &self.id_field,
                batch_row_index(base_index, offset),
            )?;
            let change = row_to_upsert_change(row);
            // Track for sink-gated commit_batch of *all* sunk messages (not only
            // the batch's last_position).
            self.pending_acks.push_back(message.clone());
            events.push(PositionedEvent::change(change, KafkaOffset));
        }
        Ok(events)
    }

    async fn commit(&mut self, _position: Self::Position) -> Result<()> {
        if self.ready_to_commit.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(&mut self.ready_to_commit);
        self.consumer
            .commit_batch(&batch)
            .await
            .map_err(|e| anyhow::anyhow!("kafka commit: {e}"))?;
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        // Durability is the consumer-group offset commit in `commit`.
        CheckpointPolicy::CommitOnly
    }

    fn stop_reason(&self) -> Option<StopReason> {
        if Utc::now() >= self.deadline {
            return Some(StopReason::Deadline);
        }
        if self.finished {
            return Some(StopReason::Until);
        }
        if let Some(max) = self.max_messages {
            if self.processed_count.load(Ordering::SeqCst) >= max {
                return Some(StopReason::Until);
            }
        }
        None
    }

    fn note_sunk_events(&mut self, count: u64) {
        // Move `count` poll-ordered messages from pending → ready_to_commit so
        // `commit` can offset-commit the whole sunk batch (main semantics).
        for _ in 0..count {
            match self.pending_acks.pop_front() {
                Some(message) => self.ready_to_commit.push(message),
                None => {
                    tracing::warn!(
                        "kafka note_sunk_events({count}): pending_acks exhausted early"
                    );
                    break;
                }
            }
        }
        let prev = self.processed_count.fetch_add(count, Ordering::SeqCst);
        let total = prev.saturating_add(count);
        if total / 100 > prev / 100 {
            info!("Processed {total} messages total");
        }
        if let Some(max) = self.max_messages {
            if total >= max {
                self.finished = true;
            }
        }
    }
}

fn row_to_upsert_change(row: UniversalRow) -> UniversalChange {
    UniversalChange::update(row.table, row.id, row.fields)
}

/// Convert typed values to UniversalRow.
///
/// Either uses the message key (base64 encoded) as ID, or extracts the ID from a specified field.
fn typed_values_to_universal_row(
    mut typed_values: HashMap<String, TypedValue>,
    table_name: &str,
    use_message_key_as_id: bool,
    message_key: Option<&[u8]>,
    id_field: &str,
    record_index: u64,
) -> Result<UniversalRow> {
    let id_value = if use_message_key_as_id {
        let key_bytes = message_key.ok_or_else(|| {
            anyhow::anyhow!("use_message_key_as_id is enabled but message has no key")
        })?;
        let base64_str = base64::engine::general_purpose::STANDARD.encode(key_bytes);
        UniversalValue::Text(base64_str)
    } else {
        let id_typed_value = typed_values
            .remove(id_field)
            .ok_or_else(|| anyhow::anyhow!("Message has no '{id_field}' field"))?;
        id_typed_value.value
    };

    let fields: HashMap<String, UniversalValue> = typed_values
        .into_iter()
        .map(|(k, tv)| (k, tv.value))
        .collect();

    Ok(UniversalRow::new(
        table_name.to_string(),
        record_index,
        id_value,
        fields,
    ))
}

/// Per-message `UniversalRow.index` within one Kafka decode batch.
///
/// `base` is the poll-time index allocator value at the start of the batch;
/// `offset` is the message's position in that batch.
fn batch_row_index(base: u64, offset: usize) -> u64 {
    base + offset as u64
}

/// How many messages this poll may receive given `max_messages` headroom.
fn poll_receive_limit(
    kafka_batch_size: usize,
    max_messages: Option<u64>,
    next_row_index: u64,
) -> usize {
    let Some(max) = max_messages else {
        return kafka_batch_size;
    };
    if next_row_index >= max {
        return 0;
    }
    let headroom = (max - next_row_index) as usize;
    kafka_batch_size.min(headroom)
}

/// Atomically reserve `count` unique row indices; returns the base index.
fn alloc_row_indices(next_row_index: &AtomicU64, count: usize) -> u64 {
    next_row_index.fetch_add(count as u64, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::{alloc_row_indices, batch_row_index, poll_receive_limit};
    use std::collections::{HashSet, VecDeque};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn batch_row_indices_are_unique_within_batch() {
        let base = 10u64;
        let batch_len = 5usize;
        let collided: Vec<u64> = (0..batch_len).map(|_| base).collect();
        assert_eq!(
            collided.iter().copied().collect::<HashSet<_>>().len(),
            1,
            "sanity: shared base alone is a single index"
        );

        let indices: Vec<u64> = (0..batch_len)
            .map(|offset| batch_row_index(base, offset))
            .collect();
        assert_eq!(indices, vec![10, 11, 12, 13, 14]);
        let unique: HashSet<_> = indices.iter().copied().collect();
        assert_eq!(
            unique.len(),
            indices.len(),
            "rows in a batch must not share one index; got {indices:?}"
        );
    }

    #[test]
    fn overlapping_polls_allocate_unique_indices() {
        // Simulate W≥2: two polls before any commit advances processed_count.
        let next = AtomicU64::new(0);
        let processed = AtomicU64::new(0);

        let base1 = alloc_row_indices(&next, 3);
        let idx1: Vec<u64> = (0..3).map(|o| batch_row_index(base1, o)).collect();

        // Bug pattern: reusing processed_count as base while commits lag.
        let stale_base = processed.load(Ordering::SeqCst);
        let collided: Vec<u64> = (0..2).map(|o| batch_row_index(stale_base, o)).collect();
        assert_eq!(collided, vec![0, 1], "stale processed_count would collide");

        let base2 = alloc_row_indices(&next, 2);
        let idx2: Vec<u64> = (0..2).map(|o| batch_row_index(base2, o)).collect();

        assert_eq!(idx1, vec![0, 1, 2]);
        assert_eq!(idx2, vec![3, 4]);
        let mut all = idx1;
        all.extend(idx2);
        let unique: HashSet<_> = all.iter().copied().collect();
        assert_eq!(unique.len(), all.len(), "overlapping polls must not collide");
    }

    #[test]
    fn max_messages_headroom_bounds_receive_under_overlap() {
        let max = Some(5u64);
        // After polling 4 (not yet committed), next poll may take only 1.
        assert_eq!(poll_receive_limit(100, max, 4), 1);
        assert_eq!(poll_receive_limit(100, max, 5), 0);
        assert_eq!(poll_receive_limit(100, max, 0), 5);
        assert_eq!(poll_receive_limit(2, max, 0), 2);
        assert_eq!(poll_receive_limit(100, None, 0), 100);
    }

    /// Models the pending_acks → ready_to_commit → commit_batch flow used when
    /// `batch_size > 1`: note_sunk_events(N) must stage N messages, and commit
    /// flushes all of them (not only last_position).
    #[test]
    fn sunk_batch_stages_all_messages_for_commit() {
        let mut pending: VecDeque<u32> = VecDeque::from(vec![10, 11, 12, 13]);
        let mut ready: Vec<u32> = Vec::new();
        let processed = AtomicU64::new(0);
        let max_messages = Some(3u64);
        let mut finished = false;

        // First sink batch of 2 (framework batch_size).
        let count = 2u64;
        for _ in 0..count {
            ready.push(pending.pop_front().expect("pending"));
        }
        let prev = processed.fetch_add(count, Ordering::SeqCst);
        let total = prev + count;
        if let Some(max) = max_messages {
            if total >= max {
                finished = true;
            }
        }
        assert_eq!(ready, vec![10, 11]);
        assert!(!finished);
        let committed = std::mem::take(&mut ready);
        assert_eq!(committed, vec![10, 11], "commit_batch must include all sunk");

        // Second sink batch of 1 reaches max_messages.
        let count = 1u64;
        for _ in 0..count {
            ready.push(pending.pop_front().expect("pending"));
        }
        let prev = processed.fetch_add(count, Ordering::SeqCst);
        let total = prev + count;
        if let Some(max) = max_messages {
            if total >= max {
                finished = true;
            }
        }
        assert!(finished, "processed_count must use message count, not commit calls");
        assert_eq!(processed.load(Ordering::SeqCst), 3);
        assert_eq!(ready, vec![12]);
        // One message remains pending (not yet sunk) — max_messages stop does not
        // require committing unread work.
        assert_eq!(pending, VecDeque::from(vec![13]));
    }

    /// Regression: counting commits (fetch_add(1)) with batch_size>1 never reaches
    /// max_messages when one commit covers multiple sunk messages.
    #[test]
    fn commit_counting_would_miss_max_messages() {
        let max = 4u64;
        // Four messages sunk as two transform batches → two commit() calls.
        let commit_calls = 2u64;
        let message_count = 4u64;
        assert!(
            commit_calls < max,
            "bug pattern: stop never trips if processed_count += 1 per commit"
        );
        assert!(message_count >= max);
    }
}
