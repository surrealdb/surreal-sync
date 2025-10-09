use crate::SurrealOpts;
use anyhow::Result;
use clap::Parser;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use surreal_sync_kafka::{ConsumerConfig, Message};
use tracing::{debug, info, warn};

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
}

/// Run incremental sync from Kafka to SurrealDB
pub async fn run_incremental_sync(
    config: Config,
    _to_namespace: String,
    _to_database: String,
    _to_opts: SurrealOpts,
    _deadline: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    info!("Starting Kafka incremental sync",);

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
    println!("Kafka client created successfully");

    // Shared counter for processed messages
    let processed_count = Arc::new(AtomicU64::new(0));

    // Message processor function
    let processor = {
        let counter = Arc::clone(&processed_count);
        move |messages: Vec<Message>| {
            let counter = Arc::clone(&counter);
            async move {
                for message in messages {
                    debug!("Received message: {:?}", message);

                    // Put the message as a record into SurrealDB.
                    // The topic name becomes the SurrealDB table name.
                    // The message fields become the record fields.
                    // Either the message key, or the "id" field in the message is used as the record ID.
                    warn!("SurrealDB integration not yet implemented for Kafka source");

                    let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    if count % 100 == 0 {
                        println!("Processed {count} messages total");
                    }
                }

                Ok(())
            }
        }
    };

    let num_consumers = config.num_consumers;
    println!("Spawning {num_consumers} consumers in the same consumer group...");
    let handles =
        client.spawn_batch_consumer_group(config.num_consumers, config.batch_size, processor)?;

    println!("Consumers running. Press Ctrl+C to stop.");

    // Wait for all consumers (runs indefinitely until Ctrl+C)
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => println!("Consumer {i} finished successfully"),
            Ok(Err(e)) => eprintln!("Consumer {i} error: {e}"),
            Err(e) => eprintln!("Consumer {i} task error: {e}"),
        }
    }

    Ok(())
}
