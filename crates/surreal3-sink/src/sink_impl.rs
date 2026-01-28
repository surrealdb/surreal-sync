//! Implementation of SurrealSink trait for SurrealDB v3.

use anyhow::Result;
use surreal_sink::SurrealSink;
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use sync_core::{UniversalChange, UniversalRelation, UniversalRow};

use crate::rows::{write_universal_relations, write_universal_rows};
use crate::write::apply_universal_change;

/// Wrapper around Surreal<Any> that implements SurrealSink.
///
/// This wrapper provides the SurrealSink trait implementation for
/// SurrealDB v3 clients, allowing source crates to use generic
/// `<S: SurrealSink>` parameters.
pub struct Surreal3Sink {
    client: Surreal<Any>,
}

impl Surreal3Sink {
    /// Create a new Surreal3Sink from an existing Surreal connection.
    pub fn new(client: Surreal<Any>) -> Self {
        Self { client }
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
}

#[async_trait::async_trait]
impl SurrealSink for Surreal3Sink {
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> Result<()> {
        write_universal_rows(&self.client, rows).await
    }

    async fn write_universal_relations(&self, relations: &[UniversalRelation]) -> Result<()> {
        write_universal_relations(&self.client, relations).await
    }

    async fn apply_universal_change(&self, change: &UniversalChange) -> Result<()> {
        apply_universal_change(&self.client, change).await
    }
}
