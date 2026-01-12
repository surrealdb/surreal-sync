use protobuf::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::Duration;

/// Example demonstrating how to produce UserEvent messages to Kafka
///
/// This example shows how to:
/// 1. Create a Kafka topic if it doesn't exist
/// 2. Encode protobuf messages manually
/// 3. Produce messages to a Kafka topic
/// 4. Publish multiple UserEvent messages for testing
///
/// To run this producer:
/// 1. Start Kafka with Docker:
///    docker run -d --name kafka -p 9092:9092 apache/kafka:latest
/// 2. Run the producer:
///    cargo run -p surreal-sync-kafka-producer
/// 3. Run the consumer in another terminal:
///    cd crates/kafka-source && cargo run --example multi_consumer
// Generated protobuf module
mod user_event;

use user_event::user_event::UserEvent;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    match run_main().await {
        Ok(_) => println!("Producer finished successfully"),
        Err(e) => {
            eprintln!("Error: {e:?}");
            std::process::exit(1);
        }
    }
}

async fn run_main() -> anyhow::Result<()> {
    let broker = "localhost:9092";
    let topic = "user-events";

    // Create topic if it doesn't exist
    println!("Creating topic '{topic}' if it doesn't exist...");
    create_topic_if_not_exists(broker, topic).await?;

    // Create producer
    println!("Creating Kafka producer...");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000")
        .create()?;

    println!("Publishing UserEvent messages to topic '{topic}'...");

    // Publish sample messages
    let messages = vec![
        ("user_001", "login", "User logged in from browser"),
        ("user_002", "signup", "New user registration"),
        ("user_001", "purchase", "Bought premium subscription"),
        ("user_003", "login", "User logged in from mobile app"),
        ("user_002", "profile_update", "Updated profile picture"),
        ("user_004", "signup", "New user registration"),
        ("user_003", "logout", "User logged out"),
        ("user_001", "view", "Viewed product page"),
        ("user_005", "login", "User logged in from browser"),
        ("user_002", "purchase", "Bought basic plan"),
    ];

    let mut count = 0;
    for (user_id, event_type, data) in messages {
        let timestamp = chrono::Utc::now().timestamp();

        // Create UserEvent message
        let mut event = UserEvent::new();
        event.user_id = user_id.to_string();
        event.event_type = event_type.to_string();
        event.timestamp = timestamp;
        event.data = data.to_string();

        // Encode to protobuf bytes
        let payload = event.write_to_bytes()?;

        // Publish to Kafka
        let record = FutureRecord::to(topic).key(user_id).payload(&payload);

        producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, _)| err)?;

        count += 1;
        println!(
            "Published message {count}: user={user_id}, event={event_type}, timestamp={timestamp}"
        );

        // Small delay between messages
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("\nSuccessfully published {count} messages to '{topic}'");

    Ok(())
}

async fn create_topic_if_not_exists(broker: &str, topic: &str) -> anyhow::Result<()> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .create()?;

    let new_topic = NewTopic::new(topic, 3, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

    match admin_client.create_topics(&[new_topic], &opts).await {
        Ok(results) => {
            for result in results {
                match result {
                    Ok(topic_name) => println!("Topic '{topic_name}' created successfully"),
                    Err((topic_name, err)) => {
                        // Topic already exists is not an error
                        if err.to_string().contains("already exists") {
                            println!("Topic '{topic_name}' already exists");
                        } else {
                            return Err(anyhow::anyhow!("Failed to create topic: {err}"));
                        }
                    }
                }
            }
        }
        Err(e) => return Err(anyhow::anyhow!("Failed to create topics: {e}")),
    }

    Ok(())
}
