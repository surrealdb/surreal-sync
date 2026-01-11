//! Kafka incremental sync to SurrealDB.
//!
//! Consumes protobuf-encoded messages from Kafka topics and writes
//! them as records to SurrealDB tables.
//!
//! This module was moved from src/kafka/incremental.rs in the main crate
//! to break the circular dependency between kafka and kafka-types.

use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;
use kafka_types::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use sync_core::TableDefinition;
use tokio::time::{sleep, Duration};
use tracing::{debug, info, warn};

use crate::consumer::ConsumerConfig;
use crate::Client;

/// SurrealDB connection options.
///
/// Defined locally to avoid dependency on main crate.
#[derive(Clone, Debug)]
pub struct SurrealOpts {
    /// SurrealDB endpoint URL
    pub surreal_endpoint: String,
    /// SurrealDB username
    pub surreal_username: String,
    /// SurrealDB password
    pub surreal_password: String,
}

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
    /// Number of messages to read from Kafka per batch before processing.
    /// Messages are read in batches, processed (written to SurrealDB one by one),
    /// then offsets are committed to Kafka. Larger batches improve throughput
    /// but increase memory usage and potential duplicate processing on failure.
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
}

/// Run incremental sync from Kafka to SurrealDB.
///
/// The sync will run until the deadline is reached. Once the deadline passes,
/// the function will gracefully terminate all consumers and exit.
pub async fn run_incremental_sync(
    config: Config,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    deadline: DateTime<Utc>,
    table_schema: Option<TableDefinition>,
) -> Result<()> {
    let duration_until_deadline = deadline.signed_duration_since(Utc::now());
    info!(
        "Starting Kafka incremental sync for message {} from topic {} (deadline in {} seconds)",
        config.message_type,
        config.topic,
        duration_until_deadline.num_seconds()
    );

    let surreal_conn_opts = surreal_sync_surreal::SurrealOpts {
        surreal_endpoint: to_opts.surreal_endpoint.clone(),
        surreal_username: to_opts.surreal_username.clone(),
        surreal_password: to_opts.surreal_password.clone(),
    };
    let surreal =
        surreal_sync_surreal::surreal_connect(&surreal_conn_opts, &to_namespace, &to_database)
            .await?;
    let surreal = Arc::new(surreal);

    // Determine table name: use configured table_name if provided, otherwise use topic name
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
        ..Default::default()
    };

    let client = Client::from_proto_file(config.proto_path, consumer_config)?;
    info!(
        "Kafka client created successfully: schema={:?}",
        client.schema()
    );

    // Shared counter for processed messages
    let processed_count = Arc::new(AtomicU64::new(0));

    let surreal = Arc::clone(&surreal);

    // Message processor function
    let use_message_key_as_id = config.use_message_key_as_id;
    let id_field = config.id_field.clone();
    let processor = {
        let counter = Arc::clone(&processed_count);
        let table_name = table_name.clone();
        let table_schema = table_schema.clone();
        move |messages: Vec<Message>| {
            let counter = Arc::clone(&counter);
            let surreal = Arc::clone(&surreal);
            let table_name = table_name.clone();
            let table_schema = table_schema.clone();
            let id_field = id_field.clone();
            async move {
                for message in messages {
                    debug!("Received message: {:?}", message);

                    // Put the message as a record into SurrealDB.
                    // The table name is either configured explicitly or defaults to topic name.
                    // The message fields become the record fields.
                    let message_key = message.key.clone();

                    // Use kafka-types for the TypedValue conversion path
                    let typed_values =
                        kafka_types::message_to_typed_values(message, table_schema.as_ref())?;

                    // Create record using appropriate ID strategy
                    let record = if use_message_key_as_id {
                        // Use message key as ID (base64 encoded to handle arbitrary bytes)
                        let key_bytes = message_key.ok_or_else(|| {
                            anyhow::anyhow!(
                                "use_message_key_as_id is enabled but message has no key"
                            )
                        })?;
                        surrealdb_types::typed_values_to_record_with_bytes_id(
                            typed_values,
                            &key_bytes,
                            &table_name,
                        )?
                    } else {
                        // Use specified field as ID (default: "id")
                        surrealdb_types::typed_values_to_record_using_field_as_id(
                            typed_values,
                            &id_field,
                            &table_name,
                        )?
                    };

                    surreal_sync_surreal::write_record(&surreal, &record).await?;

                    let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    if count % 100 == 0 {
                        info!("Processed {count} messages total");
                    }
                }

                Ok(())
            }
        }
    };

    let num_consumers = config.num_consumers;
    info!("Spawning {num_consumers} consumers in the same consumer group...");
    let handles = client.spawn_batch_consumer_group(
        config.num_consumers,
        config.kafka_batch_size,
        processor,
    )?;

    // Calculate how long to wait until deadline
    let duration_until_deadline = deadline.signed_duration_since(Utc::now());
    let timeout_duration = if duration_until_deadline.num_seconds() > 0 {
        Duration::from_secs(duration_until_deadline.num_seconds() as u64)
    } else {
        warn!("Deadline already passed, using minimal timeout");
        Duration::from_secs(1)
    };

    info!(
        "Consumers running for {} seconds until deadline...",
        timeout_duration.as_secs()
    );

    // Wait for deadline to pass
    sleep(timeout_duration).await;
    info!("Deadline reached, aborting consumer tasks");

    // Abort all consumer tasks
    for (i, handle) in handles.into_iter().enumerate() {
        handle.abort();
        debug!("Aborted consumer task {i}");
    }

    // Brief delay to allow cleanup
    sleep(Duration::from_millis(100)).await;

    let final_count = processed_count.load(Ordering::SeqCst);
    info!(
        "Kafka sync completed: processed {} messages total from topic {}",
        final_count, table_name
    );

    Ok(())
}
