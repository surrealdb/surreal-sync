//! MongoDB incremental sync implementation using Change Streams
//!
//! This module provides incremental synchronization capabilities for MongoDB using
//! Change Streams, which provide real-time change notifications.

use crate::surreal::{surreal_connect, Change, ChangeOp};
use crate::sync::{ChangeStream, IncrementalSource, SourceDatabase, SyncCheckpoint};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bson::Document;
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use log::{debug, info, warn};
use mongodb::{
    change_stream::event::{ChangeStreamEvent, ResumeToken},
    options::{ChangeStreamOptions, FullDocumentType},
    Client,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Convert a BSON document directly to a SurrealValue map
fn bson_doc_to_keys_and_surreal_values(
    doc: Document,
) -> Result<HashMap<String, crate::SurrealValue>> {
    let mut map = HashMap::new();

    for (key, value) in doc {
        let v = crate::mongodb::convert_bson_to_surreal_value(value)?;
        map.insert(key, v);
    }

    Ok(map)
}

/// MongoDB implementation of incremental sync using Change Streams
#[derive(Debug)]
pub struct MongodbIncrementalSource {
    client: Client,
    database: String,
    resume_token: Arc<Mutex<Vec<u8>>>,
}

impl MongodbIncrementalSource {
    /// Create a new MongoDB incremental source with initial resume token
    pub async fn new(
        connection_string: &str,
        database: &str,
        initial_resume_token: Vec<u8>,
    ) -> Result<Self> {
        // Validate the resume token by trying to deserialize it
        bson::from_slice::<ResumeToken>(&initial_resume_token).map_err(|e| {
            anyhow::anyhow!(
                "Invalid resume token provided to MongoDB source constructor: {e}. \
                The token may be corrupted or from an incompatible MongoDB version.",
            )
        })?;

        // Connect to MongoDB
        let client = Client::with_uri_str(connection_string).await?;

        Ok(MongodbIncrementalSource {
            client,
            database: database.to_string(),
            resume_token: Arc::new(Mutex::new(initial_resume_token)),
        })
    }

    /// Get the current resume token from MongoDB
    ///
    /// This creates a change stream and immediately gets its resume token
    /// without consuming any events, providing a checkpoint for future resumption.
    #[allow(dead_code)]
    async fn get_current_resume_token(&self) -> Result<Vec<u8>> {
        let database = self.client.database(&self.database);

        // Create a change stream with no pipeline to get current token
        let options = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .build();

        let change_stream = database.watch().with_options(options).await?;

        // Get the resume token from the stream
        // The driver provides a resume_token() method that gives us the current position
        if let Some(token) = change_stream.resume_token() {
            let bytes = bson::to_vec(&token)?;
            return Ok(bytes);
        }

        // If no token is immediately available, we may need to wait for an event
        // For now, return an error to indicate we couldn't get a valid token
        Err(anyhow!("No resume token available from change stream"))
    }

    /// Start change stream from a specific checkpoint
    async fn start_change_stream(
        &self,
        checkpoint: Option<SyncCheckpoint>,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<Change>> + Send>>> {
        let database = self.client.database(&self.database);

        // Build change stream options
        let mut options = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .build();

        // If we have a checkpoint with a resume token, use it to resume the stream
        if let Some(checkpoint) = checkpoint {
            match checkpoint {
                SyncCheckpoint::MongoDB {
                    resume_token: token_bytes,
                    ..
                } => {
                    // Deserialize the token bytes back to a ResumeToken
                    // ResumeToken implements Deserialize, so we can deserialize it directly from BSON
                    let resume_token = bson::from_slice::<ResumeToken>(&token_bytes)
                        .map_err(|e| {
                            // We fail fast here to prevent silent data loss. If we cannot deserialize
                            // the resume token, starting from "current position" would skip all changes
                            // between the checkpoint time and now. This could result in missing critical
                            // data updates. By failing fast, we force operator intervention to either:
                            // 1. Provide a valid checkpoint
                            // 2. Explicitly start without a checkpoint (understanding the implications)
                            // 3. Perform a full sync to ensure consistency
                            anyhow!(
                                "Failed to deserialize resume token - refusing to start to prevent data loss. \
                                Error: {e}. The resume token may be corrupted or from an incompatible MongoDB version. \
                                Options: (1) Start without a checkpoint if data loss is acceptable, \
                                (2) Perform a full sync first, or (3) Provide a valid checkpoint.",
                            )
                        })?;

                    options.resume_after = Some(resume_token);
                    info!("Resuming change stream from saved checkpoint");
                }
                _ => {
                    // Invalid checkpoint type for MongoDB. We fail fast here because the operator
                    // explicitly provided a checkpoint that we cannot use. Starting from current
                    // position would lose all changes between the intended checkpoint and now.
                    // This is likely a configuration error that needs human intervention.
                    return Err(anyhow!(
                        "Invalid checkpoint type for MongoDB incremental sync. \
                        Expected MongoDB checkpoint, got {checkpoint:?}. \
                        This prevents resumption from the intended point and could cause data loss. \
                        Please provide a valid MongoDB checkpoint or start without one.",
                    ));
                }
            }
        }

        // Create the change stream
        let change_stream = database.watch().with_options(options).await?;
        let database_name = self.database.clone();
        let resume_token = self.resume_token.clone();

        // Convert MongoDB change stream to our ChangeEvent stream
        let stream = change_stream
            .map(move |result| {
                let database_name = database_name.clone();
                let resume_token = resume_token.clone();
                async move {
                    match result {
                        Ok(event) => {
                            Self::convert_change_event(event, &database_name, resume_token).await
                        }
                        Err(e) => Err(anyhow!("MongoDB change stream error: {e}")),
                    }
                }
            })
            .buffer_unordered(1);

        // Box the stream with Send bound
        let boxed_stream: std::pin::Pin<Box<dyn futures::Stream<Item = Result<Change>> + Send>> =
            Box::pin(stream);

        Ok(boxed_stream)
    }

    /// Convert MongoDB change event to our universal ChangeEvent
    async fn convert_change_event(
        event: ChangeStreamEvent<Document>,
        _database_name: &str,
        resume_token: Arc<Mutex<Vec<u8>>>,
    ) -> Result<Change> {
        // Extract and store the resume token from the event's _id field
        // The _id field contains the resume token for this specific event
        if let Ok(token_bytes) = bson::to_vec(&event.id) {
            *resume_token.lock().await = token_bytes;
        }

        // Determine operation type
        let operation = match event.operation_type {
            mongodb::change_stream::event::OperationType::Insert => ChangeOp::Create,
            mongodb::change_stream::event::OperationType::Update => ChangeOp::Update,
            mongodb::change_stream::event::OperationType::Replace => ChangeOp::Update,
            mongodb::change_stream::event::OperationType::Delete => ChangeOp::Delete,
            op => {
                // Skip other operation types (like invalidate, drop, etc.)
                return Err(anyhow!("Unsupported operation type: {op:?}"));
            }
        };

        // Get collection name
        let collection = event
            .ns
            .and_then(|ns| ns.coll)
            .unwrap_or_else(|| "unknown".to_string());

        // Get document ID
        let id = if let Some(id) = event.document_key {
            // Convert BSON document key to string ID
            if let Ok(oid) = id.get_object_id("_id") {
                surrealdb::sql::Thing::from((collection.clone(), oid.to_hex()))
            } else if let Ok(s) = id.get_str("_id") {
                surrealdb::sql::Thing::from((collection.clone(), s.to_string()))
            } else if let Ok(i) = id.get_i64("_id") {
                surrealdb::sql::Thing::from((collection.clone(), surrealdb::sql::Id::from(i)))
            } else {
                return Err(anyhow!(
                    "Unsupported _id type in document key: {:?}",
                    id.get("_id")
                ));
            }
        } else {
            return Err(anyhow!("No document key in change event"));
        };

        let data = match operation {
            ChangeOp::Delete => HashMap::new(),
            _ => {
                let d = event.full_document.unwrap();
                bson_doc_to_keys_and_surreal_values(d)?
            }
        };

        Ok(Change::record(operation, id, data))
    }
}

#[async_trait]
impl IncrementalSource for MongodbIncrementalSource {
    fn source_type(&self) -> SourceDatabase {
        SourceDatabase::MongoDB
    }

    async fn initialize(&mut self) -> Result<()> {
        // Source is already initialized via constructor - nothing to do
        Ok(())
    }

    async fn get_changes(&mut self) -> Result<Box<dyn ChangeStream>> {
        let checkpoint = Some(SyncCheckpoint::MongoDB {
            resume_token: self.resume_token.lock().await.clone(),
            timestamp: Utc::now(),
        });

        let stream = self.start_change_stream(checkpoint).await?;
        let initial_token = self.resume_token.lock().await.clone();

        Ok(Box::new(MongoChangeStream::new(stream, initial_token)))
    }

    async fn get_checkpoint(&self) -> Result<SyncCheckpoint> {
        Ok(SyncCheckpoint::MongoDB {
            resume_token: self.resume_token.lock().await.clone(),
            timestamp: Utc::now(),
        })
    }

    async fn cleanup(self) -> Result<()> {
        Ok(())
    }
}

// Type alias for complex MongoDB change stream type
type MongoStreamType =
    Arc<Mutex<std::pin::Pin<Box<dyn futures::Stream<Item = Result<Change>> + Send>>>>;

/// A change stream wrapper for MongoDB incremental sync
pub struct MongoChangeStream {
    // Wrap in Arc<Mutex> to make it Sync
    stream: MongoStreamType,
    current_checkpoint: Option<SyncCheckpoint>,
}

impl MongoChangeStream {
    pub fn new(
        stream: std::pin::Pin<Box<dyn futures::Stream<Item = Result<Change>> + Send>>,
        initial_resume_token: Vec<u8>,
    ) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
            current_checkpoint: Some(SyncCheckpoint::MongoDB {
                resume_token: initial_resume_token,
                timestamp: Utc::now(),
            }),
        }
    }
}

#[async_trait]
impl ChangeStream for MongoChangeStream {
    async fn next(&mut self) -> Option<Result<Change>> {
        let mut stream = self.stream.lock().await;
        stream.next().await
    }

    fn checkpoint(&self) -> Option<SyncCheckpoint> {
        self.current_checkpoint.clone()
    }
}

/// Run incremental sync from MongoDB to SurrealDB
///
/// This function implements the incremental sync logic:
/// 1. Connects to MongoDB and sets up Change Streams
/// 2. Reads changes from the specified checkpoint (resume token)
/// 3. Applies changes to SurrealDB
/// 4. Continues streaming changes until stopped
pub async fn run_incremental_sync(
    from_opts: crate::SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: crate::SurrealOpts,
    from_checkpoint: SyncCheckpoint,
    deadline: DateTime<Utc>,
    target_checkpoint: Option<SyncCheckpoint>,
) -> anyhow::Result<()> {
    info!(
        "Starting MongoDB incremental sync from checkpoint: {}",
        from_checkpoint.to_string()
    );

    // Extract MongoDB connection details from SourceOpts
    let connection_string = from_opts.source_uri.clone();
    let source_database = from_opts
        .source_database
        .clone()
        .ok_or_else(|| anyhow!("MongoDB source database name is required"))?;

    // Extract resume token from checkpoint using helper function
    let initial_resume_token = from_checkpoint.to_mongodb_resume_token()?;

    // Create MongoDB incremental source with resume token (already initialized)
    let mut source =
        MongodbIncrementalSource::new(&connection_string, &source_database, initial_resume_token)
            .await?;

    let surreal = surreal_connect(&to_opts, &to_namespace, &to_database).await?;

    // Authenticate
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &to_opts.surreal_username,
            password: &to_opts.surreal_password,
        })
        .await?;

    // Use namespace and database
    surreal.use_ns(&to_namespace).use_db(&to_database).await?;

    // Get change stream
    let mut stream = source.get_changes().await?;

    info!("Starting to consume MongoDB change stream...");

    let mut change_count = 0;
    let timeout_duration = std::time::Duration::from_secs(5);

    loop {
        let timeout_result = tokio::time::timeout(timeout_duration, stream.next()).await;
        let result = match timeout_result {
            Ok(Some(r)) => r,
            Ok(None) => {
                info!("Stream ended, stopping incremental sync");
                break;
            }
            Err(_) => {
                info!("Timeout waiting for changes, stopping incremental sync");
                break;
            }
        };
        match result {
            Ok(change) => {
                debug!("Received change: {change:?}");

                crate::apply_change(&surreal, &change).await?;

                change_count += 1;
                if change_count % 100 == 0 {
                    info!("Processed {change_count} changes");
                }

                // Check if we've reached the target checkpoint
                if let Some(ref target) = target_checkpoint {
                    let current = source.get_checkpoint().await?;
                    let reached = match (&current, target) {
                        (
                            SyncCheckpoint::MongoDB {
                                resume_token: current_token,
                                ..
                            },
                            SyncCheckpoint::MongoDB {
                                resume_token: target_token,
                                ..
                            },
                        ) => current_token >= target_token,
                        _ => false,
                    };

                    if reached {
                        info!(
                            "Reached target checkpoint: {}, stopping incremental sync",
                            target.to_string()
                        );
                        break;
                    }
                }

                let now = Utc::now();

                // Check if we've reached the deadline
                if now >= deadline {
                    info!("Reached deadline: {deadline}, stopping incremental sync");
                    break;
                }
            }
            Err(e) => {
                warn!("Error reading change stream: {e}");
                // Decide whether to continue or break based on error type
                // For now, we'll continue
            }
        }
    }

    info!("MongoDB incremental sync completed. Processed {change_count} changes");

    // Cleanup
    source.cleanup().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resume_token_checkpoint() {
        let token = vec![1, 2, 3, 4, 5];
        let checkpoint = SyncCheckpoint::MongoDB {
            resume_token: token.clone(),
            timestamp: Utc::now(),
        };

        match checkpoint {
            SyncCheckpoint::MongoDB { resume_token, .. } => {
                assert_eq!(resume_token, token);
            }
            _ => panic!("Wrong checkpoint type"),
        }
    }

    #[tokio::test]
    async fn test_mongodb_checkpoint() {
        let token = vec![1, 2, 3, 4, 5];
        let timestamp = Utc::now();
        let checkpoint = SyncCheckpoint::MongoDB {
            resume_token: token.clone(),
            timestamp,
        };

        match checkpoint {
            SyncCheckpoint::MongoDB {
                resume_token: parsed_token,
                timestamp: parsed_ts,
            } => {
                assert_eq!(parsed_token, token);
                assert_eq!(parsed_ts, timestamp);
            }
            _ => panic!("Wrong checkpoint type"),
        }
    }
}
