//! Test data publishing helpers for Kafka integration tests
//!
//! This module provides helper functions to publish test messages to Kafka topics
//! for testing database synchronization with realistic data.

use crate::{
    KafkaTestProducer, PostMessage, UserMessage, UserMetadata, UserPostRelationMessage,
    UserPreferences, UserSettings,
};
use chrono::Utc;

/// Publish test user messages to Kafka
pub async fn publish_test_users(
    producer: &KafkaTestProducer,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = Utc::now();

    // User 1
    let user1 = UserMessage {
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
        validation_logic: "function validate(input) { return input > threshold; }".to_string(),
        reference_id: "507f1f77bcf86cd799439011".to_string(),
        name: "Alice Smith".to_string(),
        email: "alice@example.com".to_string(),
        age: 30,
        active: true,
        created_at: now,
        score: 95.7,
    };

    producer.publish_user(topic, &user1).await?;
    tracing::debug!("Published user: {}", user1.id);

    // User 2
    let user2 = UserMessage {
        id: "user_002".to_string(),
        account_balance: 98765.43210,
        metadata: UserMetadata {
            preferences: UserPreferences {
                theme: "light".to_string(),
                language: "es".to_string(),
            },
            tags: vec!["standard".to_string(), "user".to_string()],
            settings: UserSettings {
                notifications: false,
                privacy: "public".to_string(),
            },
        },
        validation_logic: "function isValid() { return true; }".to_string(),
        reference_id: "507f191e810c19729de860ea".to_string(),
        name: "Bob Johnson".to_string(),
        email: "bob@example.com".to_string(),
        age: 25,
        active: false,
        created_at: now,
        score: 87.3,
    };

    producer.publish_user(topic, &user2).await?;
    tracing::debug!("Published user: {}", user2.id);

    Ok(())
}

/// Publish test post messages to Kafka
pub async fn publish_test_posts(
    producer: &KafkaTestProducer,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = Utc::now();

    // Post 1
    let post1 = PostMessage {
        id: "post_001".to_string(),
        author_id: "user_001".to_string(),
        title: "Database Testing".to_string(),
        content: "This post tests complex data types across databases".to_string(),
        view_count: 150,
        published: true,
        content_pattern: "/^[A-Za-z0-9\\s]+$/i".to_string(),
        post_categories: vec!["technology".to_string(), "tutorial".to_string()],
        created_at: now,
        updated_at: now,
    };

    producer.publish_post(topic, &post1).await?;
    tracing::debug!("Published post: {}", post1.id);

    // Post 2
    let post2 = PostMessage {
        id: "post_002".to_string(),
        author_id: "user_002".to_string(),
        title: "Advanced Sync Patterns".to_string(),
        content: "Exploring incremental sync strategies".to_string(),
        view_count: 89,
        published: false,
        content_pattern: "/^\\w+$/".to_string(),
        post_categories: vec!["news".to_string()],
        created_at: now,
        updated_at: now,
    };

    producer.publish_post(topic, &post2).await?;
    tracing::debug!("Published post: {}", post2.id);

    Ok(())
}

/// Publish test relation messages to Kafka
pub async fn publish_test_relations(
    producer: &KafkaTestProducer,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = Utc::now();

    // Relation 1: user_001 -> post_001
    let relation1 = UserPostRelationMessage {
        user_id: "user_001".to_string(),
        post_id: "post_001".to_string(),
        relationship_created: now,
    };

    producer.publish_relation(topic, &relation1).await?;
    tracing::debug!(
        "Published relation: {} -> {}",
        relation1.user_id,
        relation1.post_id
    );

    // Relation 2: user_002 -> post_002
    let relation2 = UserPostRelationMessage {
        user_id: "user_002".to_string(),
        post_id: "post_002".to_string(),
        relationship_created: now,
    };

    producer.publish_relation(topic, &relation2).await?;
    tracing::debug!(
        "Published relation: {} -> {}",
        relation2.user_id,
        relation2.post_id
    );

    Ok(())
}
