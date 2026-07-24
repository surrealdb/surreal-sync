//! Implementation of SurrealSink trait for SurrealDB v2.

use anyhow::Result;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, Relation, RelationChange, Row, ZeroTemporalPolicy};
use surrealdb2::engine::any::Any;
use surrealdb2::Surreal;

use super::rows::{write_relations, write_rows};
use super::write::{apply_change, apply_relation_change};

/// Wrapper around Surreal<Any> that implements SurrealSink.
///
/// This wrapper provides the SurrealSink trait implementation for
/// SurrealDB v2 clients, allowing source crates to use generic
/// `<S: SurrealSink>` parameters.
pub struct Surreal2Sink {
    client: Surreal<Any>,
    zero_temporal: ZeroTemporalPolicy,
}

impl Surreal2Sink {
    /// Create a new Surreal2Sink from an existing Surreal connection.
    pub fn new(client: Surreal<Any>) -> Self {
        Self::with_zero_temporal_policy(client, ZeroTemporalPolicy::default())
    }

    /// Create a new Surreal2Sink with an explicit zero-temporal conversion policy.
    pub fn with_zero_temporal_policy(
        client: Surreal<Any>,
        zero_temporal: ZeroTemporalPolicy,
    ) -> Self {
        Self {
            client,
            zero_temporal,
        }
    }

    /// Get a reference to the underlying Surreal client.
    pub fn inner(&self) -> &Surreal<Any> {
        &self.client
    }

    /// Get a mutable reference to the underlying Surreal client.
    pub fn inner_mut(&mut self) -> &mut Surreal<Any> {
        &mut self.client
    }

    /// Consume self and return the underlying Surreal client.
    pub fn into_inner(self) -> Surreal<Any> {
        self.client
    }

    /// Zero-temporal conversion policy used when writing field values.
    pub fn zero_temporal_policy(&self) -> ZeroTemporalPolicy {
        self.zero_temporal
    }
}

#[async_trait::async_trait]
impl SurrealSink for Surreal2Sink {
    async fn write_rows(&self, rows: &[Row]) -> Result<()> {
        write_rows(&self.client, rows, self.zero_temporal).await
    }

    async fn write_relations(&self, relations: &[Relation]) -> Result<()> {
        write_relations(&self.client, relations, self.zero_temporal).await
    }

    async fn apply_change(&self, change: &Change) -> Result<()> {
        apply_change(&self.client, change, self.zero_temporal).await
    }

    async fn apply_relation_change(&self, change: &RelationChange) -> Result<()> {
        apply_relation_change(&self.client, change, self.zero_temporal).await
    }
}

#[async_trait::async_trait]
impl surreal_sync_core::SinkConnect for Surreal2Sink {
    async fn connect(config: &surreal_sync_core::SurrealConfig) -> anyhow::Result<Self> {
        let opts = super::SurrealOpts {
            surreal_endpoint: config.endpoint.clone(),
            surreal_username: config.username.clone(),
            surreal_password: config.password.clone(),
        };
        let client = super::surreal_connect(&opts, &config.namespace, &config.database).await?;
        Ok(Self::with_zero_temporal_policy(
            client,
            config.zero_temporal,
        ))
    }
}

impl surreal_sync_core::SinkWithCheckpoints for Surreal2Sink {
    type CheckpointStore = crate::v2::checkpoint::Surreal2Store;

    fn table_checkpoints(&self, table: String) -> Self::CheckpointStore {
        crate::v2::checkpoint::Surreal2Store::new(self.client.clone(), table)
    }
}
