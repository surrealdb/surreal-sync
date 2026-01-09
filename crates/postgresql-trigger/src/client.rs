//! PostgreSQL client utilities
//!
//! This module provides utilities for creating and managing PostgreSQL client connections.

use anyhow::Result;
use log::error;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

/// Create a new PostgreSQL client with connection handling
pub async fn new_postgresql_client(connection_string: &str) -> Result<Arc<Mutex<Client>>> {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL connection error: {e}");
        }
    });

    Ok(Arc::new(Mutex::new(client)))
}
