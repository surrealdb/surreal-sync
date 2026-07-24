//! MongoDB incremental sync via Change Streams and SourceDriver.
//!
//! Resume tokens are the source position. Idle-stop (no events for a timeout)
//! and wall-clock deadline match the earlier Change Streams incremental loop.

use crate::checkpoint::MongoDBCheckpoint;
use crate::{convert_bson_to_universal_value, SourceOpts};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bson::Document;
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use mongodb::{
    change_stream::event::{ChangeStreamEvent, ResumeToken},
    options::{ChangeStreamOptions, FullDocumentType},
    Client,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, ChangeOp, Value};
use surreal_sync_runtime::{
    ApplyOpts, CheckpointPolicy, Pipeline, PositionedEvent, SourceDriver, SourceRuntimeOpts,
    StopReason,
};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Default idle wait before stopping when the change stream has no events.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(5);

/// Options for the MongoDB change-stream replication tail.
#[derive(Clone, Debug)]
pub struct ReplicationTailOptions {
    /// Wall-clock stop for the stream phase.
    pub deadline: DateTime<Utc>,
    /// Optional resume-token stop bound (inclusive: the event that reaches
    /// the target is still applied, then the runtime stops).
    pub until: Option<MongoDBCheckpoint>,
    /// How long to wait for the next change before treating the stream as idle.
    pub idle_timeout: Duration,
}

impl ReplicationTailOptions {
    /// Build options from the historical `run_incremental_sync` arguments.
    pub fn stream(deadline: DateTime<Utc>, until: Option<MongoDBCheckpoint>) -> Self {
        Self {
            deadline,
            until,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
        }
    }
}

/// Trait for a stream of changes from MongoDB
#[async_trait]
pub trait ChangeStream: Send + Sync {
    /// Get the next change event from the stream
    async fn next(&mut self) -> Option<Result<Change>>;

    /// Get the current checkpoint of the stream
    fn checkpoint(&self) -> Option<MongoDBCheckpoint>;
}

/// Convert a BSON document directly to a Value map
fn bson_doc_to_universal_values(doc: Document) -> Result<HashMap<String, Value>> {
    let mut map = HashMap::new();

    for (key, value) in doc {
        // Skip _id field - it's used as the record ID
        if key == "_id" {
            continue;
        }
        let v = convert_bson_to_universal_value(value)?;
        map.insert(key, v);
    }

    Ok(map)
}

/// MongoDB implementation of incremental sync using Change Streams
#[derive(Debug)]
pub struct MongodbIncrementalSource {
    client: Client,
    database: String,
    /// Sink-safe resume token (advanced only after successful sink / watermark advance).
    resume_token: Arc<Mutex<Vec<u8>>>,
    /// Last token observed while fetching (may be ahead of [`Self::resume_token`]).
    seen_token: Arc<Mutex<Vec<u8>>>,
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
            resume_token: Arc::new(Mutex::new(initial_resume_token.clone())),
            seen_token: Arc::new(Mutex::new(initial_resume_token)),
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

    /// Initialize the incremental source
    pub async fn initialize(&mut self) -> Result<()> {
        // Source is already initialized via constructor - nothing to do
        Ok(())
    }

    /// Get a stream of changes
    pub async fn get_changes(&mut self) -> Result<Box<dyn ChangeStream>> {
        let checkpoint = MongoDBCheckpoint {
            resume_token: self.resume_token.lock().await.clone(),
            timestamp: Utc::now(),
        };

        let stream = self.start_change_stream(Some(checkpoint)).await?;
        let initial_token = self.resume_token.lock().await.clone();

        Ok(Box::new(MongoChangeStream::new(stream, initial_token)))
    }

    /// Get the current sink-safe checkpoint (token advanced only after sink).
    pub async fn get_checkpoint(&self) -> Result<MongoDBCheckpoint> {
        Ok(MongoDBCheckpoint {
            resume_token: self.resume_token.lock().await.clone(),
            timestamp: Utc::now(),
        })
    }

    /// Shared resume-token handle for sink-safe progress (updated on watermark advance).
    pub fn resume_token_handle(&self) -> Arc<Mutex<Vec<u8>>> {
        Arc::clone(&self.resume_token)
    }

    /// Last token observed while polling (may be ahead of the sink-safe handle).
    pub fn seen_token_handle(&self) -> Arc<Mutex<Vec<u8>>> {
        Arc::clone(&self.seen_token)
    }

    /// Cleanup resources
    pub async fn cleanup(self) -> Result<()> {
        Ok(())
    }

    /// Start change stream from a specific checkpoint
    async fn start_change_stream(
        &self,
        checkpoint: Option<MongoDBCheckpoint>,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<Change>> + Send>>> {
        let database = self.client.database(&self.database);

        // Build change stream options
        let mut options = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .build();

        // If we have a checkpoint with a resume token, use it to resume the stream
        if let Some(checkpoint) = checkpoint {
            // Deserialize the token bytes back to a ResumeToken
            // ResumeToken implements Deserialize, so we can deserialize it directly from BSON
            let resume_token = bson::from_slice::<ResumeToken>(&checkpoint.resume_token)
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

        // Create the change stream
        let change_stream = database.watch().with_options(options).await?;
        let database_name = self.database.clone();
        let seen_token = self.seen_token.clone();

        // Convert MongoDB change stream to our ChangeEvent stream
        let stream = change_stream
            .map(move |result| {
                let database_name = database_name.clone();
                let seen_token = seen_token.clone();
                async move {
                    match result {
                        Ok(event) => {
                            Self::convert_change_event(event, &database_name, seen_token).await
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

    /// Convert MongoDB change event to our Change
    async fn convert_change_event(
        event: ChangeStreamEvent<Document>,
        _database_name: &str,
        seen_token: Arc<Mutex<Vec<u8>>>,
    ) -> Result<Change> {
        // Track the fetch-time resume token separately from the sink-safe bookmark.
        if let Ok(token_bytes) = bson::to_vec(&event.id) {
            *seen_token.lock().await = token_bytes;
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

        // Get document ID as Value
        let id_value = if let Some(id) = event.document_key {
            // Convert BSON document key to Value
            if let Ok(oid) = id.get_object_id("_id") {
                Value::Text(oid.to_hex())
            } else if let Ok(s) = id.get_str("_id") {
                Value::Text(s.to_string())
            } else if let Ok(i) = id.get_i64("_id") {
                Value::Int64(i)
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
            ChangeOp::Delete => None,
            _ => {
                let d = event.full_document.unwrap();
                Some(bson_doc_to_universal_values(d)?)
            }
        };

        Ok(Change::new(operation, collection, id_value, data))
    }
}

// Type alias for complex MongoDB change stream type
type MongoStreamType =
    Arc<Mutex<std::pin::Pin<Box<dyn futures::Stream<Item = Result<Change>> + Send>>>>;

/// A change stream wrapper for MongoDB incremental sync.
///
/// Sink-safe resume tokens live on [`MongodbIncrementalSource`]'s
/// `resume_token` handle (advanced via the SourceDriver after sink). This
/// wrapper only yields decoded changes; [`ChangeStream::checkpoint`] always
/// returns `None` so callers cannot confuse an unread initial token for a
/// sunk watermark.
pub struct MongoChangeStream {
    // Wrap in Arc<Mutex> to make it Sync
    stream: MongoStreamType,
}

impl MongoChangeStream {
    pub fn new(
        stream: std::pin::Pin<Box<dyn futures::Stream<Item = Result<Change>> + Send>>,
        _initial_resume_token: Vec<u8>,
    ) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
        }
    }
}

#[async_trait]
impl ChangeStream for MongoChangeStream {
    async fn next(&mut self) -> Option<Result<Change>> {
        let mut stream = self.stream.lock().await;
        stream.next().await
    }

    fn checkpoint(&self) -> Option<MongoDBCheckpoint> {
        // Sink-safe resume is owned by MongodbChangeStreamDriver::resume_token
        // (advanced on advance_watermark). Do not report a stale fetch-time token.
        None
    }
}

/// Run incremental sync from MongoDB to SurrealDB (identity transforms).
pub async fn run_incremental_sync<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: MongoDBCheckpoint,
    deadline: DateTime<Utc>,
    target_checkpoint: Option<MongoDBCheckpoint>,
) -> anyhow::Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_incremental_sync_with_transforms(
        surreal,
        from_opts,
        from_checkpoint,
        ReplicationTailOptions::stream(deadline, target_checkpoint),
        &pipeline,
        &apply_opts,
    )
    .await
}

/// Incremental sync via `SourceDriver` + `run_source_runtime` (shared apply window).
///
/// Change stream events become [`PositionedEvent`]s with the resume token as
/// position; apply goes through [`surreal_sync_runtime::run_source_runtime_with`].
/// Idle-stop and deadline semantics match the earlier Change Streams loop.
pub async fn run_incremental_sync_with_transforms<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: MongoDBCheckpoint,
    options: ReplicationTailOptions,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> anyhow::Result<()> {
    use surreal_sync_core::Checkpoint;

    info!(
        "Starting MongoDB incremental sync from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    if pipeline.is_identity() {
        debug!("Incremental sync using identity transform pipeline");
    } else {
        info!(
            stages = pipeline.len(),
            max_in_flight = apply_opts.max_in_flight,
            batch_size = apply_opts.batch_size,
            "Incremental sync using transform pipeline"
        );
    }

    let connection_string = from_opts.source_uri.clone();
    let source_database = from_opts
        .source_database
        .clone()
        .ok_or_else(|| anyhow!("MongoDB source database name is required"))?;

    let mut source = MongodbIncrementalSource::new(
        &connection_string,
        &source_database,
        from_checkpoint.resume_token.clone(),
    )
    .await?;

    let stream = source.get_changes().await?;
    let resume_token = source.resume_token_handle();
    let seen_token = source.seen_token_handle();
    info!("Starting to consume MongoDB change stream...");

    let mut driver = MongodbChangeStreamDriver {
        stream,
        resume_token,
        seen_token,
        options: &options,
        until_reached: false,
        finished: false,
        total_changes: 0,
    };

    let runtime_opts = SourceRuntimeOpts::new();
    let transformer = Arc::new(pipeline.clone());
    let exit = surreal_sync_runtime::run_source_runtime_with(
        &mut driver,
        surreal,
        transformer,
        apply_opts,
        &runtime_opts,
    )
    .await?;

    match exit {
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Deadline) => {
            info!("Reached deadline, stopping incremental sync");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Until) => {
            info!("Reached target checkpoint, stopping incremental sync");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Finished) => {
            info!("MongoDB change stream idle or ended, stopping incremental sync");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Cancelled) => {
            info!("Cancellation requested, stopping incremental sync");
        }
    }

    info!(
        "MongoDB incremental sync completed. Processed {} changes",
        driver.total_changes
    );

    drop(driver);
    source.cleanup().await?;
    Ok(())
}

/// MongoDB change-stream CDC driver for [`surreal_sync_runtime::run_source_runtime_with`].
///
/// Position is the resume token for the emitted event. [`Self::advance_watermark`]
/// advances the sink-safe token handle; fetch-time tokens stay on `seen_token` only.
struct MongodbChangeStreamDriver<'a> {
    stream: Box<dyn ChangeStream>,
    /// Sink-safe bookmark (advanced in [`SourceDriver::advance_watermark`]).
    resume_token: Arc<Mutex<Vec<u8>>>,
    /// Last token observed while polling (source of PositionedEvent positions).
    seen_token: Arc<Mutex<Vec<u8>>>,
    options: &'a ReplicationTailOptions,
    until_reached: bool,
    finished: bool,
    total_changes: u64,
}

#[async_trait::async_trait]
impl SourceDriver for MongodbChangeStreamDriver<'_> {
    type Position = Vec<u8>;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        if self.stop_reason().is_some() || self.finished {
            return Ok(Vec::new());
        }

        let timeout_result =
            tokio::time::timeout(self.options.idle_timeout, self.stream.next()).await;
        let result = match timeout_result {
            Ok(Some(r)) => r,
            Ok(None) => {
                info!("Stream ended, stopping incremental sync");
                self.finished = true;
                return Ok(Vec::new());
            }
            Err(_) => {
                info!("Timeout waiting for changes, stopping incremental sync");
                self.finished = true;
                return Ok(Vec::new());
            }
        };

        match result {
            Ok(change) => {
                debug!("Received change: {change:?}");
                let token = self.seen_token.lock().await.clone();

                if let Some(ref target) = self.options.until {
                    if token >= target.resume_token {
                        use surreal_sync_core::Checkpoint;
                        info!(
                            "Reached target checkpoint: {}, stopping after this event",
                            target.to_cli_string()
                        );
                        self.until_reached = true;
                    }
                }

                Ok(vec![PositionedEvent::change(change, token)])
            }
            Err(e) => {
                warn!("Error reading change stream: {e}");
                // Continue on transient stream errors (same as earlier Change Streams loop).
                Ok(Vec::new())
            }
        }
    }

    async fn advance_watermark(&mut self, position: Self::Position) -> Result<()> {
        *self.resume_token.lock().await = position;
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        CheckpointPolicy::AdvanceOnly
    }

    fn stop_reason(&self) -> Option<StopReason> {
        if Utc::now() >= self.options.deadline {
            return Some(StopReason::Deadline);
        }
        if self.until_reached {
            return Some(StopReason::Until);
        }
        None
    }

    fn note_sunk_events(&mut self, count: u64) {
        self.total_changes = self.total_changes.saturating_add(count);
        if self.total_changes.is_multiple_of(100) {
            info!("Processed {} changes", self.total_changes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resume_token_checkpoint() {
        let token = vec![1, 2, 3, 4, 5];
        let checkpoint = MongoDBCheckpoint {
            resume_token: token.clone(),
            timestamp: Utc::now(),
        };

        assert_eq!(checkpoint.resume_token, token);
    }

    #[tokio::test]
    async fn test_mongodb_checkpoint() {
        let token = vec![1, 2, 3, 4, 5];
        let timestamp = Utc::now();
        let checkpoint = MongoDBCheckpoint {
            resume_token: token.clone(),
            timestamp,
        };

        assert_eq!(checkpoint.resume_token, token);
        assert_eq!(checkpoint.timestamp, timestamp);
    }
}
