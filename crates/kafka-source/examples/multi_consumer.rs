use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use surreal_sync_kafka_source::{Client, ConsumerConfig, Message, Payload, ProtoFieldValue};

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
                    let topic = message.topic;
                    let partition = message.partition;
                    let key = message.key;

                    println!("[Topic {topic} Partition {partition}] Message Key: {key:?}",);

                    match &message.payload {
                        Payload::Protobuf(proto_msg) => {
                            // Extract fields from the decoded message
                            for (field_name, field_value) in &proto_msg.fields {
                                match field_value {
                                    ProtoFieldValue::Bytes(v) => {
                                        println!("Field: {field_name}, Bytes Value: {v:?}");
                                    }
                                    ProtoFieldValue::Double(v) => {
                                        println!("Field: {field_name}, Double Value: {v}");
                                    }
                                    ProtoFieldValue::Float(v) => {
                                        println!("Field: {field_name}, Float Value: {v}");
                                    }
                                    ProtoFieldValue::Int32(v) => {
                                        println!("Field: {field_name}, Int32 Value: {v}");
                                    }
                                    ProtoFieldValue::Int64(v) => {
                                        println!("Field: {field_name}, Int64 Value: {v}");
                                    }
                                    ProtoFieldValue::Uint32(v) => {
                                        println!("Field: {field_name}, Uint32 Value: {v}");
                                    }
                                    ProtoFieldValue::Uint64(v) => {
                                        println!("Field: {field_name}, Uint64 Value: {v}");
                                    }
                                    ProtoFieldValue::String(v) => {
                                        println!("Field: {field_name}, String Value: {v}");
                                    }
                                    ProtoFieldValue::Bool(v) => {
                                        println!("Field: {field_name}, Bool Value: {v}");
                                    }
                                    ProtoFieldValue::Message(nested_msg) => {
                                        println!("Field: {field_name}, Nested Message:");
                                        for (nested_field, nested_value) in &nested_msg.fields {
                                            println!("  Nested Field: {nested_field}, Value: {nested_value:?}");
                                        }
                                    }
                                    ProtoFieldValue::Repeated(v) => {
                                        println!("Field: {field_name}, Repeated Values:");
                                        for (i, item) in v.iter().enumerate() {
                                            println!("  Item {i}: {item:?}");
                                        }
                                    }
                                    ProtoFieldValue::Null => {
                                        println!("Field: {field_name}, Value: null");
                                    }
                                }
                            }
                        }
                    }

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
