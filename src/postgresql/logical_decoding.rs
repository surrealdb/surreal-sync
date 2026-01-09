//! PostgreSQL logical decoding sync implementation
//!
//! This module handles the logical decoding-based synchronization from PostgreSQL to SurrealDB.
//! It uses the postgresql-replication crate to consume changes from PostgreSQL's logical
//! replication slot and applies them to SurrealDB.

use crate::SurrealOpts;
use anyhow::Result;
mod incremental;
mod initial;

/// Configuration for PostgreSQL logical decoding sync
#[derive(Clone)]
pub struct Config {
    /// PostgreSQL connection string
    pub connection_string: String,

    /// Replication slot name
    pub slot: String,

    /// Tables to sync (empty means all tables)
    pub tables: Vec<String>,

    /// PostgreSQL schema
    pub schema: String,

    /// Target SurrealDB namespace
    pub to_namespace: String,

    /// Target SurrealDB database
    pub to_database: String,

    /// Target SurrealDB options
    pub to_opts: SurrealOpts,
}

/// Run PostgreSQL logical decoding-based sync to SurrealDB
///
/// This function sets up a logical replication connection to PostgreSQL
/// and continuously streams changes to SurrealDB using the wal2json output plugin.
///
/// # Arguments
/// * `config` - Configuration for the sync operation
///
/// # Returns
/// Returns Ok(()) on successful completion, or an error if the sync fails
pub async fn sync(config: Config) -> Result<()> {
    let _pg_config = surreal_sync_postgresql_replication::Config::new(
        &config.connection_string,
        config.slot.clone(),
    )?;

    // This is a placeholder implementation
    tracing::info!("Starting PostgreSQL logical decoding sync");
    tracing::info!("Connection: {}", config.connection_string);
    tracing::info!("Slot: {}", config.slot);
    tracing::info!("Schema: {}", config.schema);
    tracing::info!("Tables: {:?}", config.tables);
    tracing::info!("Target: {}/{}", config.to_namespace, config.to_database);

    let surreal_conn_opts = surreal_sync_surreal::SurrealOpts {
        surreal_endpoint: config.to_opts.surreal_endpoint.clone(),
        surreal_username: config.to_opts.surreal_username.clone(),
        surreal_password: config.to_opts.surreal_password.clone(),
    };
    let surreal = surreal_sync_surreal::surreal_connect(
        &surreal_conn_opts,
        &config.to_namespace,
        &config.to_database,
    )
    .await?;
    let store = super::state::Store::new(surreal.clone());
    let id = super::state::StateID::from_connection_and_slot(
        &config.connection_string,
        &config.schema,
        &config.slot,
    )?;
    let state = store.get_state(&id).await?;
    let mut current = super::state::State::Pending;
    match state {
        None => {
            tracing::info!("No existing state found, starting fresh");
            store.transition(&id, super::state::State::Pending).await?;
        }
        _ => {
            tracing::info!("Existing state found: {:?}", state);
            current = state.unwrap();
        }
    }

    tracing::info!("Current state: {:?}", current);
    match current {
        super::state::State::Pending => {
            tracing::info!("Resuming from Pending state");

            tracing::warn!(
                "PostgreSQL logical decoding sync from Pending state is not yet implemented"
            );

            let pre_lsn = initial::sync(&surreal, &config).await?;

            store
                .transition(&id, super::state::State::Initial { pre_lsn })
                .await?;
        }
        super::state::State::Initial { pre_lsn } => {
            tracing::info!("Resuming from Initial state with pre_lsn: {}", pre_lsn);

            incremental::sync(&surreal, &config).await?;
        }
        super::state::State::Incremental => {
            tracing::info!("Resuming from Incremental state");
        }
    }

    tracing::warn!("PostgreSQL logical decoding sync is not yet implemented");

    Ok(())
}
