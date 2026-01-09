//! Neo4j client utilities
//!
//! This module provides utilities for creating and managing Neo4j Graph connections.

use crate::SourceOpts;
use anyhow::Result;
use neo4rs::{ConfigBuilder, Graph};

/// Create a new Neo4j Graph connection
pub async fn new_neo4j_client(opts: &SourceOpts) -> Result<Graph> {
    let config = ConfigBuilder::default()
        .uri(&opts.source_uri)
        .user(
            opts.source_username
                .clone()
                .unwrap_or_else(|| "neo4j".to_string()),
        )
        .password(
            opts.source_password
                .clone()
                .unwrap_or_else(|| "password".to_string()),
        )
        .db(opts
            .source_database
            .clone()
            .unwrap_or_else(|| "neo4j".to_string()))
        .build()?;

    let graph = Graph::connect(config)?;
    Ok(graph)
}
