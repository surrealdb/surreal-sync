//! Kafka populator for load testing.
//!
//! This module provides the KafkaPopulator which generates test data based on
//! a Schema, encodes it as protobuf messages, and publishes to Kafka topics.

use crate::encoder::{encode_row, get_message_key};
use crate::error::KafkaPopulatorError;
use crate::proto_gen::generate_proto_for_table;
use loadtest_generator::DataGenerator;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use sync_core::{Schema, UniversalRow};
use tempfile::TempDir;
use tracing::{debug, info};

/// Default batch size for message publishing.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Metrics from a populate operation.
#[derive(Debug, Clone, Default)]
pub struct PopulateMetrics {
    /// Number of messages published.
    pub messages_published: u64,
    /// Total time taken.
    pub total_duration: Duration,
    /// Time spent generating data.
    pub generation_duration: Duration,
    /// Time spent publishing messages.
    pub publish_duration: Duration,
    /// Number of batches executed.
    pub batch_count: u64,
}

impl PopulateMetrics {
    /// Calculate messages per second.
    pub fn messages_per_second(&self) -> f64 {
        if self.total_duration.as_secs_f64() > 0.0 {
            self.messages_published as f64 / self.total_duration.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Convert to aggregated metrics format for loadtest-distributed.
    pub fn to_aggregated(&self) -> loadtest_distributed::metrics::PopulateMetrics {
        loadtest_distributed::metrics::PopulateMetrics {
            rows_processed: self.messages_published,
            duration_ms: self.total_duration.as_millis() as u64,
            batch_count: self.batch_count,
            rows_per_second: self.messages_per_second(),
            bytes_written: None,
        }
    }
}

/// Kafka populator that generates and publishes test data.
///
/// The populator generates test data based on a Schema, encodes it as
/// protobuf messages, and publishes to Kafka topics. Each table in the schema
/// maps to a Kafka topic with the same name (or a configured name).
///
/// # Example
///
/// ```ignore
/// let schema = Schema::from_yaml(schema_yaml)?;
/// let mut populator = KafkaPopulator::new("localhost:9092", schema, 42).await?;
///
/// // Prepare and populate the users table
/// let proto_path = populator.prepare_table("users")?;
/// populator.create_topic("users").await?;
/// populator.populate("users", 1000).await?;
/// ```
pub struct KafkaPopulator {
    producer: FutureProducer,
    brokers: String,
    schema: Schema,
    generator: DataGenerator,
    batch_size: usize,
    proto_dir: TempDir,
}

impl KafkaPopulator {
    /// Create a new Kafka populator.
    ///
    /// # Arguments
    ///
    /// * `brokers` - Kafka broker addresses (e.g., "localhost:9092")
    /// * `schema` - Load test schema defining tables and field generators
    /// * `seed` - Random seed for deterministic generation
    pub async fn new(
        brokers: &str,
        schema: Schema,
        seed: u64,
    ) -> Result<Self, KafkaPopulatorError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "30000")
            .set("queue.buffering.max.messages", "100000")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("batch.size", "65536")
            .set("linger.ms", "5")
            .create()
            .map_err(KafkaPopulatorError::Kafka)?;

        let generator = DataGenerator::new(schema.clone(), seed);
        let proto_dir =
            TempDir::new().map_err(|e| KafkaPopulatorError::ProtoGeneration(e.to_string()))?;

        Ok(Self {
            producer,
            brokers: brokers.to_string(),
            schema,
            generator,
            batch_size: DEFAULT_BATCH_SIZE,
            proto_dir,
        })
    }

    /// Set the batch size for publish operations.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the starting index for generation (for incremental population).
    pub fn with_start_index(mut self, index: u64) -> Self {
        self.generator = std::mem::replace(
            &mut self.generator,
            DataGenerator::new(self.schema.clone(), 0),
        )
        .with_start_index(index);
        self
    }

    /// Get the current generation index.
    pub fn current_index(&self) -> u64 {
        self.generator.current_index()
    }

    /// Get a reference to the schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get the proto directory path.
    pub fn proto_dir(&self) -> &std::path::Path {
        self.proto_dir.path()
    }

    /// Prepare a table for population by generating its .proto file.
    ///
    /// Returns the path to the generated .proto file, which can be used
    /// by the Kafka consumer/sync process.
    pub fn prepare_table(&self, table_name: &str) -> Result<PathBuf, KafkaPopulatorError> {
        let table_schema = self
            .schema
            .get_table(table_name)
            .ok_or_else(|| KafkaPopulatorError::TableNotFound(table_name.to_string()))?;

        // Generate the .proto file content
        let proto_content = generate_proto_for_table(table_schema, "loadtest");

        // Write to temp directory
        let proto_path = self.proto_dir.path().join(format!("{table_name}.proto"));
        std::fs::write(&proto_path, &proto_content)?;

        info!(
            "Generated proto file for table '{}' at {:?}",
            table_name, proto_path
        );
        debug!("Proto content:\n{}", proto_content);

        Ok(proto_path)
    }

    /// Create a Kafka topic if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name to create
    /// * `partitions` - Number of partitions (default: 3)
    pub async fn create_topic(&self, topic: &str) -> Result<(), KafkaPopulatorError> {
        self.create_topic_with_partitions(topic, 3).await
    }

    /// Create a Kafka topic with specified partitions.
    pub async fn create_topic_with_partitions(
        &self,
        topic: &str,
        partitions: i32,
    ) -> Result<(), KafkaPopulatorError> {
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create()
            .map_err(KafkaPopulatorError::Kafka)?;

        let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

        match admin_client.create_topics(&[new_topic], &opts).await {
            Ok(results) => {
                for result in results {
                    match result {
                        Ok(topic_name) => {
                            info!("Topic '{}' created successfully", topic_name);
                        }
                        Err((topic_name, err)) => {
                            let err_str = err.to_string();
                            if err_str.contains("already exists")
                                || err_str.contains("TopicExistsException")
                            {
                                info!("Topic '{}' already exists", topic_name);
                            } else {
                                return Err(KafkaPopulatorError::TopicCreation(format!(
                                    "Failed to create topic {topic_name}: {err}"
                                )));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                return Err(KafkaPopulatorError::TopicCreation(format!(
                    "Failed to create topic: {e}"
                )));
            }
        }

        Ok(())
    }

    /// Populate a topic with the specified number of messages.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table (and topic) to populate
    /// * `count` - Number of messages to publish
    ///
    /// # Returns
    ///
    /// Metrics about the populate operation.
    pub async fn populate(
        &mut self,
        table_name: &str,
        count: u64,
    ) -> Result<PopulateMetrics, KafkaPopulatorError> {
        // Default: topic name = table name
        self.populate_to_topic(table_name, table_name, count).await
    }

    /// Populate a specific topic with messages for a table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table (for schema lookup)
    /// * `topic_name` - Name of the Kafka topic to publish to
    /// * `count` - Number of messages to publish
    ///
    /// # Returns
    ///
    /// Metrics about the populate operation.
    pub async fn populate_to_topic(
        &mut self,
        table_name: &str,
        topic_name: &str,
        count: u64,
    ) -> Result<PopulateMetrics, KafkaPopulatorError> {
        let start_time = Instant::now();
        let mut metrics = PopulateMetrics::default();

        let table_schema = self
            .schema
            .get_table(table_name)
            .ok_or_else(|| KafkaPopulatorError::TableNotFound(table_name.to_string()))?
            .clone();

        info!(
            "Populating topic '{}' with {} messages (batch size: {})",
            topic_name, count, self.batch_size
        );

        let mut remaining = count;
        let mut generation_time = Duration::ZERO;
        let mut publish_time = Duration::ZERO;

        while remaining > 0 {
            let batch_count = std::cmp::min(remaining, self.batch_size as u64);

            // Generate rows
            let gen_start = Instant::now();
            let rows: Vec<UniversalRow> = self
                .generator
                .internal_rows(table_name, batch_count)
                .map_err(|e| KafkaPopulatorError::Generator(e.to_string()))?
                .collect();
            generation_time += gen_start.elapsed();

            // Publish messages
            let publish_start = Instant::now();
            let published = self.publish_batch(topic_name, &table_schema, &rows).await?;
            publish_time += publish_start.elapsed();

            metrics.messages_published += published;
            metrics.batch_count += 1;
            remaining -= batch_count;

            debug!(
                "Batch {} complete: {} messages published, {} remaining",
                metrics.batch_count, published, remaining
            );
        }

        metrics.total_duration = start_time.elapsed();
        metrics.generation_duration = generation_time;
        metrics.publish_duration = publish_time;

        info!(
            "Population complete: {} messages in {:?} ({:.2} msg/sec)",
            metrics.messages_published,
            metrics.total_duration,
            metrics.messages_per_second()
        );

        Ok(metrics)
    }

    /// Publish a batch of messages to Kafka.
    async fn publish_batch(
        &self,
        topic: &str,
        table_schema: &sync_core::GeneratorTableDefinition,
        rows: &[UniversalRow],
    ) -> Result<u64, KafkaPopulatorError> {
        // First, encode all messages (key + payload pairs)
        let encoded_messages: Vec<(Vec<u8>, Vec<u8>)> = rows
            .iter()
            .map(|row| {
                let payload = encode_row(row, table_schema)?;
                let key = get_message_key(row);
                Ok((key, payload))
            })
            .collect::<Result<Vec<_>, KafkaPopulatorError>>()?;

        // Now send all messages - the encoded data lives long enough
        let mut futures = Vec::with_capacity(encoded_messages.len());
        for (key, payload) in &encoded_messages {
            let record = FutureRecord::to(topic).key(key).payload(payload);
            let future = self.producer.send(record, Duration::from_secs(30));
            futures.push(future);
        }

        // Wait for all messages to be delivered
        let mut published = 0u64;
        for future in futures {
            match future.await {
                Ok(_) => published += 1,
                Err((err, _)) => {
                    return Err(KafkaPopulatorError::Kafka(err));
                }
            }
        }

        Ok(published)
    }

    /// Populate incrementally (append messages to existing topic).
    ///
    /// This method continues from the current generator index, useful for
    /// testing incremental sync scenarios.
    pub async fn populate_incremental(
        &mut self,
        table_name: &str,
        count: u64,
    ) -> Result<PopulateMetrics, KafkaPopulatorError> {
        info!(
            "Incremental population starting at index {}",
            self.generator.current_index()
        );
        self.populate(table_name, count).await
    }

    /// Get all proto file paths for prepared tables.
    pub fn get_proto_paths(&self) -> Result<Vec<PathBuf>, KafkaPopulatorError> {
        let mut paths = Vec::new();
        for entry in std::fs::read_dir(self.proto_dir.path())? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|e| e == "proto") {
                paths.push(entry.path());
            }
        }
        Ok(paths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        let yaml = r#"
version: 1
seed: 42
tables:
  - name: users
    id:
      type: big_int
      generator:
        type: sequential
        start: 1
    fields:
      - name: email
        type:
          type: var_char
          length: 255
        generator:
          type: pattern
          pattern: "user_{index}@example.com"
      - name: age
        type: int
        generator:
          type: int_range
          min: 18
          max: 80
"#;
        Schema::from_yaml(yaml).unwrap()
    }

    #[test]
    fn test_metrics() {
        let metrics = PopulateMetrics {
            messages_published: 1000,
            total_duration: Duration::from_secs(10),
            generation_duration: Duration::from_secs(2),
            publish_duration: Duration::from_secs(8),
            batch_count: 10,
        };

        assert_eq!(metrics.messages_per_second(), 100.0);
    }

    #[test]
    fn test_schema_validation() {
        let schema = test_schema();
        assert!(schema.get_table("users").is_some());
        assert!(schema.get_table("nonexistent").is_none());
    }
}
