//! Kafka incremental sync E2E test
//!
//! This test validates that Kafka incremental sync operations work correctly.
//! Unlike traditional database sources, Kafka:
//! - Does NOT require full sync before incremental sync (no snapshots)
//! - Does NOT use checkpoint files (Kafka manages consumer offsets internally)
//! - Supports incremental sync ONLY (streaming-only model)
//!
//! Test flow:
//! 1. Create Kafka topics
//! 2. Publish test messages (users, posts, relations) using protobuf encoding
//! 3. Run incremental sync to consume and sync messages to SurrealDB
//! 4. Verify synced data in SurrealDB using assert_synced

use chrono::Utc;
use std::time::Duration;
use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};
use surreal_sync::SurrealOpts;
use surreal_sync_kafka_producer::{
    publish_test_posts, publish_test_relations, publish_test_users, KafkaTestProducer,
};
use tokio::time::sleep;

/// Kafka broker address for testing
const KAFKA_BROKER: &str = "kafka:9092";

#[tokio::test]
async fn test_kafka_incremental_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,surreal_sync_kafka=debug")
        .try_init()
        .ok();

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    // Generate random topic names for this test run to avoid conflicts
    let users_topic = format!("test-users-{test_id}");
    let posts_topic = format!("test-posts-{test_id}");
    let relations_topic = format!("test-user-posts-{test_id}");

    tracing::info!(
        "Using topics: users={}, posts={}, relations={}",
        users_topic,
        posts_topic,
        relations_topic
    );

    // Setup SurrealDB connection
    let surreal_config = TestConfig::new(test_id, "kafka-incremental");
    let surreal = connect_surrealdb(&surreal_config).await?;

    // Clean up any existing test data in SurrealDB
    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;

    // Step 1: Setup Kafka producer and create topics
    tracing::info!("Setting up Kafka producer and topics...");
    let producer = KafkaTestProducer::new(KAFKA_BROKER).await?;

    // Create topics with appropriate partition counts
    producer.create_topic_if_not_exists(&users_topic, 3).await?;
    producer.create_topic_if_not_exists(&posts_topic, 3).await?;
    producer
        .create_topic_if_not_exists(&relations_topic, 3)
        .await?;

    // Give Kafka a moment to propagate topic metadata
    sleep(Duration::from_millis(500)).await;

    // Step 2: Publish test messages to Kafka
    tracing::info!("Publishing test messages to Kafka...");

    // Publish users
    publish_test_users(&producer, &users_topic).await?;

    // Publish posts
    publish_test_posts(&producer, &posts_topic).await?;

    // Publish relations
    publish_test_relations(&producer, &relations_topic).await?;

    tracing::info!("All test messages published to Kafka");

    // Give a small delay to ensure messages are committed
    sleep(Duration::from_millis(200)).await;

    // Step 3: Run incremental sync for users topic
    tracing::info!("Running Kafka incremental sync for users...");

    let surreal_opts = SurrealOpts {
        surreal_endpoint: surreal_config.surreal_endpoint.clone(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 100,
        dry_run: false,
    };

    // Create a temporary proto file for the user schema
    let proto_dir = tempfile::tempdir()?;
    let user_proto_path = proto_dir.path().join("user.proto");
    std::fs::write(
        &user_proto_path,
        include_str!("../../crates/kafka-producer/proto/user.proto"),
    )?;

    // Run sync for users topic with a deadline
    let user_config = surreal_sync::kafka::Config {
        proto_path: user_proto_path.to_string_lossy().to_string(),
        brokers: vec![KAFKA_BROKER.to_string()],
        group_id: format!("test-group-users-{test_id}"),
        topic: users_topic.clone(),
        message_type: "User".to_string(),
        buffer_size: 1000,
        session_timeout_ms: "6000".to_string(),
        num_consumers: 1,
        batch_size: 100,
        table_name: Some("all_types_users".to_string()),
    };

    // Run sync with a short deadline (just enough to consume existing messages)
    let deadline = Utc::now() + chrono::Duration::seconds(5);

    // Spawn sync task
    let sync_handle = tokio::spawn({
        let config = user_config.clone();
        let namespace = surreal_config.surreal_namespace.clone();
        let database = surreal_config.surreal_database.clone();
        let opts = surreal_opts.clone();
        async move {
            surreal_sync::kafka::run_incremental_sync(
                config, namespace, database, opts, deadline, None,
            )
            .await
        }
    });

    // Wait for sync to complete or timeout
    let sync_result = tokio::time::timeout(Duration::from_secs(10), sync_handle).await;

    match sync_result {
        Ok(Ok(Ok(()))) => tracing::info!("User sync completed successfully"),
        Ok(Ok(Err(e))) => tracing::warn!("User sync error (may be expected): {}", e),
        Ok(Err(e)) => tracing::warn!("User sync task error: {}", e),
        Err(_) => tracing::info!("User sync timeout (expected for test)"),
    }

    // Step 4: Run incremental sync for posts topic
    tracing::info!("Running Kafka incremental sync for posts...");

    let post_proto_path = proto_dir.path().join("post.proto");
    std::fs::write(
        &post_proto_path,
        include_str!("../../crates/kafka-producer/proto/post.proto"),
    )?;

    let post_config = surreal_sync::kafka::Config {
        proto_path: post_proto_path.to_string_lossy().to_string(),
        brokers: vec![KAFKA_BROKER.to_string()],
        group_id: format!("test-group-posts-{test_id}"),
        topic: posts_topic.clone(),
        message_type: "Post".to_string(),
        buffer_size: 1000,
        session_timeout_ms: "6000".to_string(),
        num_consumers: 1,
        batch_size: 100,
        table_name: Some("all_types_posts".to_string()),
    };

    let deadline = Utc::now() + chrono::Duration::seconds(5);

    let sync_handle = tokio::spawn({
        let config = post_config;
        let namespace = surreal_config.surreal_namespace.clone();
        let database = surreal_config.surreal_database.clone();
        let opts = surreal_opts.clone();
        async move {
            surreal_sync::kafka::run_incremental_sync(
                config, namespace, database, opts, deadline, None,
            )
            .await
        }
    });

    let sync_result = tokio::time::timeout(Duration::from_secs(10), sync_handle).await;

    match sync_result {
        Ok(Ok(Ok(()))) => tracing::info!("Post sync completed successfully"),
        Ok(Ok(Err(e))) => tracing::warn!("Post sync error (may be expected): {}", e),
        Ok(Err(e)) => tracing::warn!("Post sync task error: {}", e),
        Err(_) => tracing::info!("Post sync timeout (expected for test)"),
    }

    // Step 5: Run incremental sync for relations topic
    tracing::info!("Running Kafka incremental sync for relations...");

    let relation_proto_path = proto_dir.path().join("user_post_relation.proto");
    std::fs::write(
        &relation_proto_path,
        include_str!("../../crates/kafka-producer/proto/user_post_relation.proto"),
    )?;

    let relation_config = surreal_sync::kafka::Config {
        proto_path: relation_proto_path.to_string_lossy().to_string(),
        brokers: vec![KAFKA_BROKER.to_string()],
        group_id: format!("test-group-relations-{test_id}"),
        topic: relations_topic.clone(),
        message_type: "UserPostRelation".to_string(),
        buffer_size: 1000,
        session_timeout_ms: "6000".to_string(),
        num_consumers: 1,
        batch_size: 100,
        table_name: Some("authored_by".to_string()),
    };

    let deadline = Utc::now() + chrono::Duration::seconds(5);

    let sync_handle = tokio::spawn({
        let config = relation_config;
        let namespace = surreal_config.surreal_namespace.clone();
        let database = surreal_config.surreal_database.clone();
        let opts = surreal_opts;
        async move {
            surreal_sync::kafka::run_incremental_sync(
                config, namespace, database, opts, deadline, None,
            )
            .await
        }
    });

    let sync_result = tokio::time::timeout(Duration::from_secs(10), sync_handle).await;

    match sync_result {
        Ok(Ok(Ok(()))) => tracing::info!("Relation sync completed successfully"),
        Ok(Ok(Err(e))) => tracing::warn!("Relation sync error (may be expected): {}", e),
        Ok(Err(e)) => tracing::warn!("Relation sync task error: {}", e),
        Err(_) => tracing::info!("Relation sync timeout (expected for test)"),
    }

    // Step 6: Verify synced data in SurrealDB using standard test helper
    tracing::info!("Verifying synced data in SurrealDB...");
    surreal_sync::testing::surrealdb::assert_synced(&surreal, &dataset, "Kafka incremental sync")
        .await?;

    tracing::info!("✅ Kafka incremental sync test completed successfully");

    Ok(())
}
