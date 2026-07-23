//! Implementation of SurrealSink trait for SurrealDB v3.

use anyhow::Result;
use surreal_sink::SurrealSink;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use sync_core::{
    UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow, ZeroTemporalPolicy,
};

use crate::rows::{write_universal_relations, write_universal_rows};
use crate::write::{apply_universal_change, apply_universal_relation_change};

/// Wrapper around Surreal<Any> that implements SurrealSink.
///
/// This wrapper provides the SurrealSink trait implementation for
/// SurrealDB v3 clients, allowing source crates to use generic
/// `<S: SurrealSink>` parameters.
pub struct Surreal3Sink {
    client: Surreal<Any>,
    zero_temporal: ZeroTemporalPolicy,
}

impl Surreal3Sink {
    /// Create a new Surreal3Sink from an existing Surreal connection.
    pub fn new(client: Surreal<Any>) -> Self {
        Self::with_zero_temporal_policy(client, ZeroTemporalPolicy::default())
    }

    /// Create a new Surreal3Sink with an explicit zero-temporal conversion policy.
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
impl SurrealSink for Surreal3Sink {
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> Result<()> {
        write_universal_rows(&self.client, rows, self.zero_temporal).await
    }

    async fn write_universal_relations(&self, relations: &[UniversalRelation]) -> Result<()> {
        write_universal_relations(&self.client, relations, self.zero_temporal).await
    }

    async fn apply_universal_change(&self, change: &UniversalChange) -> Result<()> {
        apply_universal_change(&self.client, change, self.zero_temporal).await
    }

    async fn apply_universal_relation_change(
        &self,
        change: &UniversalRelationChange,
    ) -> Result<()> {
        apply_universal_relation_change(&self.client, change, self.zero_temporal).await
    }
}
