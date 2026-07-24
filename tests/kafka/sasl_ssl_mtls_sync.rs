//! E2E test for Kafka incremental sync over SASL_SSL + SCRAM-SHA-256 + mTLS.

use chrono::Utc;
use std::{sync::Arc, time::Duration};
use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{
    generate_test_id, table::TestDataSet, users::create_users_table, SourceDatabase, TestConfig,
};
use surreal_sync_kafka::from_kafka::{
    Config as KafkaConfig, SaslMechanism as ConsumerSaslMechanism,
    SecurityProtocol as ConsumerSecurityProtocol,
};
use surreal_sync_kafka::producer::secure_container::SecureKafkaContainer;
use surreal_sync_kafka::producer::{
    publish_test_users, KafkaConnectionOptions, KafkaTestProducer, SaslMechanism, SecurityProtocol,
};
use tokio::time::sleep;

#[tokio::test]
async fn test_kafka_sasl_ssl_mtls_sync() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug,surreal_sync_kafka=debug")
        .try_init()
        .ok();

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();

    let mut kafka = SecureKafkaContainer::new("test-kafka-sasl-ssl-mtls")?;
    kafka.start()?;
    kafka.wait_until_ready(60).await?;

    let secrets = kafka.secrets();
    let kafka_broker = kafka.broker_address.clone();

    let test_id = generate_test_id();
    let dataset = TestDataSet::new().add_table(create_users_table());
    let users_topic = format!("test-users-sasl-ssl-{test_id}");

    tracing::info!("Using secured Kafka topic: {users_topic}");

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    let connection_options = KafkaConnectionOptions {
        brokers: kafka_broker.clone(),
        security_protocol: Some(SecurityProtocol::SaslSsl),
        sasl_mechanism: Some(SaslMechanism::ScramSha256),
        sasl_username: Some(secrets.sasl_username.clone()),
        sasl_password: Some(secrets.sasl_password.clone()),
        ssl_ca_location: Some(secrets.ca_pem.to_string_lossy().into_owned()),
        ssl_certificate_location: Some(secrets.client_cert_pem.to_string_lossy().into_owned()),
        ssl_key_location: Some(secrets.client_key_pem.to_string_lossy().into_owned()),
        ssl_key_password: None,
    };

    tracing::info!("Connecting secured Kafka producer...");
    let producer = KafkaTestProducer::connect(&connection_options).await?;
    producer.create_topic_if_not_exists(&users_topic, 3).await?;
    sleep(Duration::from_millis(500)).await;

    tracing::info!("Publishing users to secured Kafka topic...");
    publish_test_users(&producer, &users_topic).await?;
    sleep(Duration::from_millis(200)).await;

    let proto_dir = tempfile::tempdir()?;
    let user_proto_path = proto_dir.path().join("user.proto");
    std::fs::write(
        &user_proto_path,
        include_str!("../../crates/kafka/proto/user.proto"),
    )?;

    let user_config = KafkaConfig {
        proto_path: user_proto_path.to_string_lossy().to_string(),
        brokers: vec![kafka_broker],
        group_id: format!("test-group-sasl-ssl-{test_id}"),
        topic: users_topic,
        message_type: "User".to_string(),
        buffer_size: 1000,
        session_timeout_ms: "6000".to_string(),
        num_consumers: 1,
        kafka_batch_size: 100,
        table_name: Some("all_types_users".to_string()),
        use_message_key_as_id: false,
        id_field: "id".to_string(),
        id_columns: Vec::new(),
        max_messages: None,
        sasl_username: Some(secrets.sasl_username.clone()),
        sasl_password: Some(secrets.sasl_password.clone()),
        sasl_mechanism: Some(ConsumerSaslMechanism::ScramSha256),
        security_protocol: Some(ConsumerSecurityProtocol::SaslSsl),
        ssl_ca_location: Some(secrets.ca_pem.to_string_lossy().into_owned()),
        ssl_certificate_location: Some(secrets.client_cert_pem.to_string_lossy().into_owned()),
        ssl_key_location: Some(secrets.client_key_pem.to_string_lossy().into_owned()),
        ssl_key_password: None,
    };

    let deadline = Utc::now() + chrono::Duration::seconds(5);

    tracing::info!("Running secured Kafka incremental sync for users...");
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = Arc::new(surreal_sync_surreal::v2::Surreal2Sink::new(client.clone()));
            let sync_handle = tokio::spawn(async move {
                surreal_sync_kafka::from_kafka::run_incremental_sync(
                    sink,
                    user_config,
                    deadline,
                    None,
                )
                .await
            });

            let sync_result = tokio::time::timeout(Duration::from_secs(10), sync_handle).await;
            match sync_result {
                Ok(Ok(Ok(()))) => tracing::info!("User sync completed successfully"),
                Ok(Ok(Err(e))) => tracing::warn!("User sync error (may be expected): {e}"),
                Ok(Err(e)) => tracing::warn!("User sync task error: {e}"),
                Err(_) => tracing::info!("User sync timeout (expected for test)"),
            }
        }
        SurrealConnection::V3(client) => {
            let sink = Arc::new(surreal_sync_surreal::v3::Surreal3Sink::new(client.clone()));
            let sync_handle = tokio::spawn(async move {
                surreal_sync_kafka::from_kafka::run_incremental_sync(
                    sink,
                    user_config,
                    deadline,
                    None,
                )
                .await
            });

            let sync_result = tokio::time::timeout(Duration::from_secs(10), sync_handle).await;
            match sync_result {
                Ok(Ok(Ok(()))) => tracing::info!("User sync completed successfully"),
                Ok(Ok(Err(e))) => tracing::warn!("User sync error (may be expected): {e}"),
                Ok(Err(e)) => tracing::warn!("User sync task error: {e}"),
                Err(_) => tracing::info!("User sync timeout (expected for test)"),
            }
        }
    }

    tracing::info!("Verifying synced users in SurrealDB...");
    assert_synced_auto(
        &conn,
        &dataset,
        "Kafka SASL_SSL mTLS sync",
        SourceDatabase::Kafka,
    )
    .await?;

    tracing::info!("Kafka SASL_SSL mTLS sync test completed successfully");
    Ok(())
}
