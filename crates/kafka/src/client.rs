use crate::consumer::{Consumer, ConsumerConfig, Message};
use crate::error::Result;
use crate::proto::decoder::ProtoDecoder;
use crate::proto::parser::ProtoSchema;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Type alias for message processor functions
pub type MessageProcessor =
    Arc<dyn Fn(Message) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Kafka client for managing multiple consumers
pub struct Client {
    schema: Arc<ProtoSchema>,
    config: ConsumerConfig,
}

impl Client {
    /// Create a new Kafka client from a .proto file
    pub fn from_proto_file<P: AsRef<Path>>(proto_path: P, config: ConsumerConfig) -> Result<Self> {
        let schema = ProtoSchema::from_file(proto_path)?;
        Ok(Self {
            schema: Arc::new(schema),
            config,
        })
    }

    /// Create a new Kafka client from a .proto string
    pub fn from_proto_string(proto_content: &str, config: ConsumerConfig) -> Result<Self> {
        let schema = ProtoSchema::from_string(proto_content)?;
        Ok(Self {
            schema: Arc::new(schema),
            config,
        })
    }

    /// Create a decoder for this client's schema
    pub fn create_decoder(&self) -> ProtoDecoder {
        ProtoDecoder::new((*self.schema).clone())
    }

    /// Create a single consumer
    pub fn create_consumer(&self) -> Result<Consumer> {
        let decoder = self.create_decoder();
        Consumer::new(self.config.clone(), decoder)
    }

    /// Spawn a batch consumer task that processes messages in batches
    pub fn spawn_batch_consumer_task<F, Fut>(
        &self,
        batch_size: usize,
        processor: F,
    ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>>
    where
        F: Fn(Vec<Message>) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let consumer = self.create_consumer()?;

        let handle = tokio::spawn(async move {
            loop {
                // Receive a batch of messages
                let messages = consumer.receive_batch(batch_size).await?;

                if messages.is_empty() {
                    continue;
                }

                // Process the batch
                if let Err(e) = processor(messages.clone()).await {
                    tracing::error!("Error processing batch: {}", e);
                    // Don't commit on error - messages will be reprocessed
                    continue;
                }

                // Commit all messages in the batch after successful processing
                consumer.commit_batch(&messages).await?;
            }
        });

        Ok(handle)
    }

    /// Spawn multiple batch consumer tasks in the same consumer group
    ///
    /// When spawning multiple consumers:
    /// - All consumers join the same consumer group (same `group_id`)
    /// - Kafka assigns different partitions of the specified topic to each consumer
    /// - Each partition is processed by exactly one consumer
    pub fn spawn_batch_consumer_group<F, Fut>(
        &self,
        num_consumers: usize,
        batch_size: usize,
        processor: F,
    ) -> anyhow::Result<Vec<JoinHandle<anyhow::Result<()>>>>
    where
        F: Fn(Vec<Message>) -> Fut + Send + Clone + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let mut handles = Vec::new();

        for _ in 0..num_consumers {
            let handle = self.spawn_batch_consumer_task(batch_size, processor.clone())?;
            handles.push(handle);
        }

        Ok(handles)
    }

    /// Get the schema
    pub fn schema(&self) -> &ProtoSchema {
        &self.schema
    }

    /// Get the config
    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }
}
