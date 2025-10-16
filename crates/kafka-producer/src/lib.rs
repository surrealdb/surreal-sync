//! Kafka producer library for testing surreal-sync
//!
//! This library provides protobuf-encoded message publishing utilities for testing
//! database synchronization with Kafka as a source.
//!
//! ## Features
//!
//! - **Protobuf encoding**: Proper protobuf encoding for users, posts, and relations
//! - **Kafka producer**: Helper struct for publishing test messages to Kafka topics
//! - **Topic management**: Utilities for creating and managing Kafka topics
//!
//! ## Usage
//!
//! ```rust,no_run
//! use surreal_sync_kafka_producer::{KafkaTestProducer, UserMessage, PostMessage};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let producer = KafkaTestProducer::new("localhost:9092").await?;
//!
//!     // Create topic
//!     producer.create_topic_if_not_exists("users-topic", 3).await?;
//!
//!     // Publish a user message
//!     let user = UserMessage {
//!         id: "user_001".to_string(),
//!         name: "Alice Smith".to_string(),
//!         email: "alice@example.com".to_string(),
//!         age: 30,
//!         active: true,
//!         created_at: chrono::Utc::now().timestamp(),
//!         score: 95.7,
//!         // ... other fields
//!     };
//!
//!     producer.publish_user("users-topic", &user).await?;
//!     Ok(())
//! }
//! ```

use anyhow::{Context, Result};
use protobuf::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::Duration;

// Include generated protobuf code
mod protos;

// Test data helpers module
pub mod testdata;

// Re-export protobuf types for convenience
pub use protos::post::Post;
pub use protos::user::{Metadata, Preferences, Settings, User};
pub use protos::user_post_relation::UserPostRelation;

pub use testdata::{publish_test_posts, publish_test_relations, publish_test_users};

/// Kafka producer wrapper for testing
pub struct KafkaTestProducer {
    producer: FutureProducer,
    broker: String,
}

impl KafkaTestProducer {
    /// Create a new Kafka test producer
    pub async fn new(broker: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("message.timeout.ms", "5000")
            .create()
            .context("Failed to create Kafka producer")?;

        Ok(Self {
            producer,
            broker: broker.to_string(),
        })
    }

    /// Create Kafka topic if it doesn't exist
    pub async fn create_topic_if_not_exists(&self, topic: &str, partitions: i32) -> Result<()> {
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .create()
            .context("Failed to create admin client")?;

        let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

        match admin_client.create_topics(&[new_topic], &opts).await {
            Ok(results) => {
                for result in results {
                    match result {
                        Ok(topic_name) => {
                            tracing::info!("Topic '{topic_name}' created successfully");
                        }
                        Err((topic_name, err)) => {
                            if err.to_string().contains("already exists") {
                                tracing::info!("Topic '{topic_name}' already exists");
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

    /// Publish a user message to Kafka (protobuf encoded)
    pub async fn publish_user(&self, topic: &str, user: &UserMessage) -> Result<()> {
        let mut proto_user = User::new();
        proto_user.id = user.id.clone();
        proto_user.account_balance = user.account_balance;
        proto_user.validation_logic = user.validation_logic.clone();
        proto_user.reference_id = user.reference_id.clone();
        proto_user.name = user.name.clone();
        proto_user.email = user.email.clone();
        proto_user.age = user.age;
        proto_user.active = user.active;

        // Convert DateTime to protobuf Timestamp
        let mut timestamp = protobuf::well_known_types::timestamp::Timestamp::new();
        timestamp.seconds = user.created_at.timestamp();
        timestamp.nanos = user.created_at.timestamp_subsec_nanos() as i32;
        proto_user.created_at = Some(timestamp).into();

        proto_user.score = user.score;

        // Build metadata
        let mut metadata = Metadata::new();

        let mut preferences = Preferences::new();
        preferences.theme = user.metadata.preferences.theme.clone();
        preferences.language = user.metadata.preferences.language.clone();
        metadata.preferences = Some(preferences).into();

        metadata.tags = user.metadata.tags.clone();

        let mut settings = Settings::new();
        settings.notifications = user.metadata.settings.notifications;
        settings.privacy = user.metadata.settings.privacy.clone();
        metadata.settings = Some(settings).into();

        proto_user.metadata = Some(metadata).into();

        let payload = proto_user
            .write_to_bytes()
            .context("Failed to encode user message")?;
        let key = user.id.as_bytes();

        let record = FutureRecord::to(topic).key(key).payload(&payload);

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, _)| err)
            .context("Failed to send user message to Kafka")?;

        tracing::debug!("Published user message: {}", user.id);
        Ok(())
    }

    /// Publish a post message to Kafka (protobuf encoded)
    pub async fn publish_post(&self, topic: &str, post: &PostMessage) -> Result<()> {
        let mut proto_post = Post::new();
        proto_post.id = post.id.clone();
        proto_post.author_id = post.author_id.clone();
        proto_post.title = post.title.clone();
        proto_post.content = post.content.clone();
        proto_post.view_count = post.view_count;
        proto_post.published = post.published;
        proto_post.content_pattern = post.content_pattern.clone();
        proto_post.post_categories = post.post_categories.clone();

        // Convert DateTime to protobuf Timestamp for created_at
        let mut created_timestamp = protobuf::well_known_types::timestamp::Timestamp::new();
        created_timestamp.seconds = post.created_at.timestamp();
        created_timestamp.nanos = post.created_at.timestamp_subsec_nanos() as i32;
        proto_post.created_at = Some(created_timestamp).into();

        // Convert DateTime to protobuf Timestamp for updated_at
        let mut updated_timestamp = protobuf::well_known_types::timestamp::Timestamp::new();
        updated_timestamp.seconds = post.updated_at.timestamp();
        updated_timestamp.nanos = post.updated_at.timestamp_subsec_nanos() as i32;
        proto_post.updated_at = Some(updated_timestamp).into();

        let payload = proto_post
            .write_to_bytes()
            .context("Failed to encode post message")?;
        let key = post.id.as_bytes();

        let record = FutureRecord::to(topic).key(key).payload(&payload);

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, _)| err)
            .context("Failed to send post message to Kafka")?;

        tracing::debug!("Published post message: {}", post.id);
        Ok(())
    }

    /// Publish a user-post relation message to Kafka (protobuf encoded)
    pub async fn publish_relation(
        &self,
        topic: &str,
        relation: &UserPostRelationMessage,
    ) -> Result<()> {
        let mut proto_relation = UserPostRelation::new();
        proto_relation.user_id = relation.user_id.clone();
        proto_relation.post_id = relation.post_id.clone();

        // Convert DateTime to protobuf Timestamp
        let mut timestamp = protobuf::well_known_types::timestamp::Timestamp::new();
        timestamp.seconds = relation.relationship_created.timestamp();
        timestamp.nanos = relation.relationship_created.timestamp_subsec_nanos() as i32;
        proto_relation.relationship_created = Some(timestamp).into();

        let payload = proto_relation
            .write_to_bytes()
            .context("Failed to encode relation message")?;
        let key = format!("{}-{}", relation.user_id, relation.post_id);

        let record = FutureRecord::to(topic)
            .key(key.as_bytes())
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, _)| err)
            .context("Failed to send relation message to Kafka")?;

        tracing::debug!(
            "Published relation message: {} -> {}",
            relation.user_id,
            relation.post_id
        );
        Ok(())
    }
}

/// User message structure for Kafka
#[derive(Debug, Clone)]
pub struct UserMessage {
    pub id: String,
    pub account_balance: f64,
    pub metadata: UserMetadata,
    pub validation_logic: String,
    pub reference_id: String,
    pub name: String,
    pub email: String,
    pub age: i32,
    pub active: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct UserMetadata {
    pub preferences: UserPreferences,
    pub tags: Vec<String>,
    pub settings: UserSettings,
}

#[derive(Debug, Clone)]
pub struct UserPreferences {
    pub theme: String,
    pub language: String,
}

#[derive(Debug, Clone)]
pub struct UserSettings {
    pub notifications: bool,
    pub privacy: String,
}

/// Post message structure for Kafka
#[derive(Debug, Clone)]
pub struct PostMessage {
    pub id: String,
    pub author_id: String,
    pub title: String,
    pub content: String,
    pub view_count: i64,
    pub published: bool,
    pub content_pattern: String,
    pub post_categories: Vec<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// User-Post relation message structure for Kafka
#[derive(Debug, Clone)]
pub struct UserPostRelationMessage {
    pub user_id: String,
    pub post_id: String,
    pub relationship_created: chrono::DateTime<chrono::Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_message_encoding() {
        let user = UserMessage {
            id: "user_001".to_string(),
            account_balance: 12345.67890,
            metadata: UserMetadata {
                preferences: UserPreferences {
                    theme: "dark".to_string(),
                    language: "en".to_string(),
                },
                tags: vec!["premium".to_string(), "verified".to_string()],
                settings: UserSettings {
                    notifications: true,
                    privacy: "strict".to_string(),
                },
            },
            validation_logic: "function validate() { return true; }".to_string(),
            reference_id: "507f1f77bcf86cd799439011".to_string(),
            name: "Alice Smith".to_string(),
            email: "alice@example.com".to_string(),
            age: 30,
            active: true,
            created_at: chrono::DateTime::from_timestamp(1234567890, 0).unwrap(),
            score: 95.7,
        };

        let mut proto_user = User::new();
        proto_user.id = user.id.clone();
        proto_user.name = user.name.clone();

        let bytes = proto_user.write_to_bytes().unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_post_message_encoding() {
        let post = PostMessage {
            id: "post_001".to_string(),
            author_id: "user_001".to_string(),
            title: "Test Post".to_string(),
            content: "This is a test".to_string(),
            view_count: 150,
            published: true,
            content_pattern: "/test/i".to_string(),
            post_categories: vec!["tech".to_string()],
            created_at: chrono::DateTime::from_timestamp(1234567890, 0).unwrap(),
            updated_at: chrono::DateTime::from_timestamp(1234567890, 0).unwrap(),
        };

        let mut proto_post = Post::new();
        proto_post.id = post.id.clone();
        proto_post.title = post.title.clone();

        let bytes = proto_post.write_to_bytes().unwrap();
        assert!(!bytes.is_empty());
    }
}
