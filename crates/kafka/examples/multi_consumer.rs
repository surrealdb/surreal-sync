use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use surreal_sync_kafka::{Client, ConsumerConfig, Message};

/// Example demonstrating multiple consumers in a consumer group
///
/// This example shows how to:
/// 1. Parse a .proto file
/// 2. Create a Kafka client
/// 3. Spawn multiple consumers in the same consumer group
/// 4. Process messages with automatic offset commits
/// 5. Extract field values from decoded messages
///
/// To run this example:
/// 1. Start Kafka with Docker
///   docker run -d --name kafka -p 9092:9092 apache/kafka:latest
/// 2. Run tests
///   cargo test
/// 3. Run examples
///   cargo run --example multi_consumer

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    match run_main().await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Error: {e:?}");
            std::process::exit(1);
        }
    }
}

async fn run_main() -> anyhow::Result<()> {
    // Example .proto content
    let proto_content = r#"
        syntax = "proto3";

        package example;

        message UserEvent {
            string user_id = 1;
            string event_type = 2;
            int64 timestamp = 3;
            string data = 4;
        }
    "#;

    let config = ConsumerConfig {
        brokers: "localhost:9092".to_string(),
        group_id: "peek-example-group".to_string(),
        topic: "user-events".to_string(),
        message_type: "UserEvent".to_string(),
        buffer_size: 100,
        ..Default::default()
    };

    // Create Kafka client
    let client = Client::from_proto_string(proto_content, config)?;

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
                    // Extract fields from the decoded message
                    let user_id = message.message.get_string("user_id")?;
                    let event_type = message.message.get_string("event_type")?;
                    let timestamp = message.message.get_int64("timestamp")?;
                    let data = message.message.get_string("data")?;

                    println!(
                        "[Partition {}] User: {}, Event: {}, Timestamp: {}, Data: {}",
                        message.partition, user_id, event_type, timestamp, data
                    );

                    // Increment counter
                    let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    if count % 100 == 0 {
                        println!("Processed {count} messages total");
                    }
                }

                Ok(())
            }
        }
    };

    // Spawn 3 consumers in the same consumer group
    // Each consumer will process different partitions
    println!("Spawning 3 consumers in the same consumer group...");
    let handles = client.spawn_batch_consumer_group(3, 10, processor)?;

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
