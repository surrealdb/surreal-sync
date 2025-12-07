use crate::SurrealOpts;
use anyhow::Result;
use clap::Parser;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use surreal_sync_kafka::{ConsumerConfig, Message};
use sync_core::{TableDefinition, UniversalValue};
use tracing::{debug, error, info};

/// Convert a UniversalValue to a SurrealDB ID.
fn typed_value_to_surreal_id(value: &UniversalValue) -> Result<surrealdb::sql::Id> {
    match value {
        UniversalValue::Int32(i) => Ok(surrealdb::sql::Id::Number(*i as i64)),
        UniversalValue::Int64(i) => Ok(surrealdb::sql::Id::Number(*i)),
        UniversalValue::String(s) => Ok(surrealdb::sql::Id::String(s.clone())),
        UniversalValue::Uuid(u) => Ok(surrealdb::sql::Id::Uuid(surrealdb::sql::Uuid::from(*u))),
        other => anyhow::bail!("Cannot convert {other:?} to SurrealDB ID"),
    }
}

/// Configuration for Kafka source
#[derive(Debug, Clone, Parser)]
pub struct Config {
    /// Proto file path
    pub proto_path: String,
    /// Kafka brokers
    pub brokers: Vec<String>,
    /// Consumer group ID
    pub group_id: String,
    /// Topic to consume from
    pub topic: String,
    /// Protobuf message type name
    pub message_type: String,
    /// Maximum buffer size for peeked messages
    pub buffer_size: usize,
    /// Session timeout in milliseconds
    pub session_timeout_ms: String,
    /// Number of consumers in the consumer group to spawn
    #[clap(long, default_value_t = 1)]
    pub num_consumers: usize,
    /// Batch size for processing messages
    #[clap(long, default_value_t = 100)]
    pub batch_size: usize,
    /// Optional table name to use in SurrealDB (defaults to topic name)
    #[clap(long)]
    pub table_name: Option<String>,
}

/// Run incremental sync from Kafka to SurrealDB
pub async fn run_incremental_sync(
    config: Config,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    _deadline: chrono::DateTime<chrono::Utc>,
    table_schema: Option<TableDefinition>,
) -> Result<()> {
    info!(
        "Starting Kafka incremental sync for message {} from topic {}",
        config.message_type, config.topic
    );

    let surreal = crate::surreal::surreal_connect(&to_opts, &to_namespace, &to_database).await?;
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

    let client = surreal_sync_kafka::Client::from_proto_file(config.proto_path, consumer_config)?;
    info!(
        "Kafka client created successfully: schema={:?}",
        client.schema()
    );

    // Shared counter for processed messages
    let processed_count = Arc::new(AtomicU64::new(0));

    let surreal = Arc::clone(&surreal);

    // Message processor function
    let processor = {
        let counter = Arc::clone(&processed_count);
        let table_name = table_name.clone();
        let table_schema = table_schema.clone();
        move |messages: Vec<Message>| {
            let counter = Arc::clone(&counter);
            let surreal = Arc::clone(&surreal);
            let table_name = table_name.clone();
            let table_schema = table_schema.clone();
            async move {
                for message in messages {
                    debug!("Received message: {:?}", message);

                    // Put the message as a record into SurrealDB.
                    // The table name is either configured explicitly or defaults to topic name.
                    // The message fields become the record fields.
                    // Either the message key, or the "id" field in the message is used as the record ID.
                    let message_key = message.key.clone();

                    // Use kafka-types for the TypedValue conversion path
                    let mut typed_values =
                        kafka_types::message_to_typed_values(message, table_schema.as_ref())?;

                    // Extract ID from typed values
                    let surreal_id = if let Some(id_value) = typed_values.remove("id") {
                        typed_value_to_surreal_id(&id_value.value)?
                    } else if let Some(surreal_key) = message_key {
                        // Use message key as ID
                        surrealdb::sql::Id::String(
                            String::from_utf8_lossy(&surreal_key).to_string(),
                        )
                    } else {
                        anyhow::bail!("Message has no key and no 'id' field");
                    };

                    // Convert typed values to SurrealDB values
                    let surreal_values = surrealdb_types::typed_values_to_surreal_map(typed_values);

                    let r = crate::surreal::RecordWithSurrealValues {
                        id: surrealdb::sql::Thing::from((table_name.as_str(), surreal_id)),
                        data: surreal_values,
                    };

                    crate::surreal::write_record_with_surreal_values(&surreal, &r).await?;

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
    let handles =
        client.spawn_batch_consumer_group(config.num_consumers, config.batch_size, processor)?;

    info!("Consumers running. Press Ctrl+C to stop.");

    // Wait for all consumers (runs indefinitely until Ctrl+C)
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => info!("Consumer {i} finished successfully"),
            Ok(Err(e)) => error!("Consumer {i} error: {e}"),
            Err(e) => error!("Consumer {i} task error: {e}"),
        }
    }

    Ok(())
}
