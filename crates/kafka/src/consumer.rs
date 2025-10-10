use crate::error::{Error, Result};
use crate::proto::decoder::{ProtoDecoder, ProtoMessage};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer as RdkafkaConsumer, StreamConsumer as RdkafkaStreamConsumer};
use rdkafka::message::{BorrowedMessage as RdkafkaBorrowedMessage, Message as RdkafkaMessage};
use rdkafka::{Offset, TopicPartitionList};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Configuration for Kafka consumer
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Kafka brokers (comma-separated list)
    pub brokers: String,
    /// Consumer group ID
    pub group_id: String,
    /// Topic to consume from
    ///
    /// All the messages' payloads must be of the same protobuf message type
    /// specified by the .proto schema and the message_type field.
    ///
    /// In case of multiple topics, use separate consumers for each topic.
    /// This is because we assume you may want to configure the proto schema file, and the proto message type in the
    /// proto schema, and the number of consumers in the consumer group, buffer size, and so on,
    /// per topic.
    pub topic: String,
    /// Protobuf message type name
    ///
    /// Must match a message defined in the provided .proto schema.
    pub message_type: String,
    /// Maximum buffer size for peeked messages
    ///
    /// The larger the buffer, the more messages can be peeked without blocking,
    /// but it also consumes more memory.
    /// Generally, a larger buffer is better for high-throughput scenarios,
    /// in exchange for higher memory usage and higher chance of duplicates on failure.
    pub buffer_size: usize,
    /// Auto offset reset strategy ("earliest" or "latest")
    ///
    /// "earliest" means the consumer will start from the beginning of the topic
    /// if no committed offsets are found for the consumer group.
    /// "latest" means the consumer will start from the end of the topic.
    ///
    /// Generally, "earliest" is preferred for CDC use cases to avoid missing messages = missing updates.
    pub auto_offset_reset: String,
    /// Session timeout in milliseconds
    pub session_timeout_ms: String,
    /// Enable auto commit (should be false for manual offset management)
    ///
    /// This is false by default, as surreal-sync manages offsets manually to ensure
    /// atomic processing and committing of batches.
    pub enable_auto_commit: bool,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            group_id: "surreal-sync-consumer".to_string(),
            topic: "".to_string(),
            message_type: "".to_string(),
            buffer_size: 100,
            auto_offset_reset: "earliest".to_string(),
            session_timeout_ms: "6000".to_string(),
            enable_auto_commit: false,
        }
    }
}

/// A Kafka message with decoded protobuf content
#[derive(Debug, Clone)]
pub struct Message {
    /// Decoded protobuf message content
    pub payload: Payload,
    /// Kafka topic
    pub topic: String,
    /// Kafka partition
    pub partition: i32,
    /// Kafka offset
    pub offset: i64,
    /// Message key (if any)
    pub key: Option<Vec<u8>>,
    /// Message timestamp (milliseconds since epoch)
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone)]
pub enum Payload {
    Protobuf(ProtoMessage),
}

/// Kafka consumer with peek buffer and manual offset management
pub struct Consumer {
    consumer: Arc<RdkafkaStreamConsumer>,
    decoder: Arc<ProtoDecoder>,
    config: ConsumerConfig,
    buffer: Arc<Mutex<VecDeque<Message>>>,
}

impl Consumer {
    /// Create a new Kafka consumer
    pub fn new(config: ConsumerConfig, decoder: ProtoDecoder) -> Result<Self> {
        let consumer: RdkafkaStreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", config.enable_auto_commit.to_string())
            .set("auto.offset.reset", &config.auto_offset_reset)
            .set("session.timeout.ms", &config.session_timeout_ms)
            .set("enable.partition.eof", "false")
            .create()
            .map_err(|e| Error::Consumer(format!("Failed to create consumer: {e}")))?;

        consumer
            .subscribe(&[&config.topic])
            .map_err(|e| Error::Consumer(format!("Failed to subscribe to topic: {e}")))?;

        Ok(Self {
            consumer: Arc::new(consumer),
            decoder: Arc::new(decoder),
            config,
            buffer: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    /// Peek at buffered messages without marking them as consumed
    /// This fetches messages into the buffer up to buffer_size
    pub async fn peek(&self, count: usize) -> Result<Vec<Message>> {
        let mut buffer = self.buffer.lock().await;

        // Fill buffer if needed
        while buffer.len() < count && buffer.len() < self.config.buffer_size {
            match tokio::time::timeout(Duration::from_millis(100), self.consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let kafka_msg = self.decode_message(&msg)?;
                    buffer.push_back(kafka_msg);
                }
                Ok(Err(e)) => return Err(Error::Consumer(format!("Error receiving message: {e}"))),
                Err(_) => break, // Timeout, no more messages available right now
            }
        }

        // Return uncommitted messages
        Ok(buffer.iter().take(count).cloned().collect::<Vec<_>>())
    }

    /// Receive multiple messages (blocks until at least one message is available)
    pub async fn receive_batch(&self, max_count: usize) -> Result<Vec<Message>> {
        let mut messages = Vec::new();
        let mut buffer = self.buffer.lock().await;

        // Drain from buffer first
        while let Some(buffered) = buffer.pop_front() {
            messages.push(buffered);
            if messages.len() >= max_count {
                return Ok(messages);
            }
        }

        drop(buffer); // Release lock before fetching

        // Fetch at least one message
        if messages.is_empty() {
            let msg = self
                .consumer
                .recv()
                .await
                .map_err(|e| Error::Consumer(format!("Error receiving message: {e}")))?;
            messages.push(self.decode_message(&msg)?);
        }

        // Try to fetch more with timeout
        while messages.len() < max_count {
            match tokio::time::timeout(Duration::from_millis(10), self.consumer.recv()).await {
                Ok(Ok(msg)) => messages.push(self.decode_message(&msg)?),
                _ => break,
            }
        }

        Ok(messages)
    }

    /// Commit multiple messages' offsets
    pub async fn commit_batch(&self, messages: &[Message]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        for message in messages {
            tpl.add_partition_offset(
                &message.topic,
                message.partition,
                Offset::Offset(message.offset + 1),
            )
            .map_err(|e| Error::Consumer(format!("Failed to add partition offset: {e}")))?;
        }

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| Error::Consumer(format!("Failed to commit offset: {e}")))?;

        self.clear_buffer().await;

        Ok(())
    }

    /// Clear the peek buffer
    async fn clear_buffer(&self) {
        let mut buffer = self.buffer.lock().await;
        buffer.clear();
    }

    fn decode_message(&self, msg: &RdkafkaBorrowedMessage) -> Result<Message> {
        let payload = msg
            .payload()
            .ok_or_else(|| Error::Consumer("Message has no payload".to_string()))?;

        let decoded = self.decoder.decode(&self.config.message_type, payload)?;

        Ok(Message {
            payload: Payload::Protobuf(decoded),
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            key: msg.key().map(|k| k.to_vec()),
            timestamp: msg.timestamp().to_millis(),
        })
    }

    /// Get the underlying consumer (for advanced use cases)
    pub fn inner(&self) -> &RdkafkaStreamConsumer {
        &self.consumer
    }
}

/// Clone support for spawning multiple consumer tasks
impl Clone for Consumer {
    fn clone(&self) -> Self {
        Self {
            consumer: Arc::clone(&self.consumer),
            decoder: Arc::clone(&self.decoder),
            config: self.config.clone(),
            buffer: Arc::clone(&self.buffer),
        }
    }
}
