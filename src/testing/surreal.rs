//! Version-agnostic SurrealDB testing utilities
//!
//! This module provides version-agnostic wrappers around the v2 and v3 specific
//! SurrealDB testing functions. It auto-detects the server version and uses
//! the appropriate SDK.

use crate::testing::{
    surreal2, surreal3,
    table::{SourceDatabase, TestDataSet},
    test_helpers::TestConfig,
};

/// SurrealDB connection that can be either v2 or v3
pub enum SurrealConnection {
    V2(surrealdb2::Surreal<surrealdb2::engine::any::Any>),
    V3(surrealdb3::Surreal<surrealdb3::engine::any::Any>),
}

/// Auto-detect SurrealDB version from endpoint
pub async fn detect_version(
    endpoint: &str,
) -> Result<surreal_version::SurrealMajorVersion, Box<dyn std::error::Error>> {
    // Convert ws:// to http:// for version detection
    let http_endpoint = endpoint
        .replace("ws://", "http://")
        .replace("wss://", "https://");
    surreal_version::detect_server_version(&http_endpoint)
        .await
        .map_err(|e| e.into())
}

/// Connect to SurrealDB with auto-detection
pub async fn connect_auto(
    config: &TestConfig,
) -> Result<SurrealConnection, Box<dyn std::error::Error>> {
    match detect_version(&config.surreal_endpoint).await {
        Ok(surreal_version::SurrealMajorVersion::V2) => {
            tracing::info!(
                "Auto-detected SurrealDB v2 at {}, using v2 SDK",
                config.surreal_endpoint
            );
            let client = crate::testing::test_helpers::connect_surrealdb(config).await?;
            Ok(SurrealConnection::V2(client))
        }
        Ok(surreal_version::SurrealMajorVersion::V3) => {
            tracing::info!(
                "Auto-detected SurrealDB v3 at {}, using v3 SDK",
                config.surreal_endpoint
            );
            let client = surreal3::connect_surrealdb_v3(config).await?;
            Ok(SurrealConnection::V3(client))
        }
        Err(e) => {
            tracing::warn!(
                "Failed to detect SurrealDB version at {}: {}. Defaulting to v2 SDK.",
                config.surreal_endpoint,
                e
            );
            let client = crate::testing::test_helpers::connect_surrealdb(config).await?;
            Ok(SurrealConnection::V2(client))
        }
    }
}

/// Cleanup SurrealDB tables with auto-detection
pub async fn cleanup_auto(
    conn: &SurrealConnection,
    tables: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    match conn {
        SurrealConnection::V2(client) => {
            crate::testing::test_helpers::cleanup_surrealdb_test_data(client, tables).await
        }
        SurrealConnection::V3(client) => {
            surreal3::cleanup_surrealdb_test_data_v3(client, tables).await
        }
    }
}

/// Cleanup SurrealDB tables from a dataset with auto-detection
pub async fn cleanup_surrealdb_auto(
    conn: &SurrealConnection,
    dataset: &TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_names: Vec<&str> = dataset
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect();
    cleanup_auto(conn, &table_names).await
}

/// Assert synced data with auto-detection
pub async fn assert_synced_auto(
    conn: &SurrealConnection,
    dataset: &TestDataSet,
    test_prefix: &str,
    source: SourceDatabase,
) -> Result<(), Box<dyn std::error::Error>> {
    match conn {
        SurrealConnection::V2(client) => {
            surreal2::assert_synced(client, dataset, test_prefix, source).await
        }
        SurrealConnection::V3(client) => {
            surreal3::assert_synced_v3(client, dataset, test_prefix, source).await
        }
    }
}

/// Get the v2 client from a SurrealConnection
/// Returns None if the connection is v3
pub fn as_v2(
    conn: &SurrealConnection,
) -> Option<&surrealdb2::Surreal<surrealdb2::engine::any::Any>> {
    match conn {
        SurrealConnection::V2(client) => Some(client),
        SurrealConnection::V3(_) => None,
    }
}

/// Get the v3 client from a SurrealConnection
/// Returns None if the connection is v2
pub fn as_v3(
    conn: &SurrealConnection,
) -> Option<&surrealdb3::Surreal<surrealdb3::engine::any::Any>> {
    match conn {
        SurrealConnection::V2(_) => None,
        SurrealConnection::V3(client) => Some(client),
    }
}

/// Check if the connection is v2
pub fn is_v2(conn: &SurrealConnection) -> bool {
    matches!(conn, SurrealConnection::V2(_))
}

/// Check if the connection is v3
pub fn is_v3(conn: &SurrealConnection) -> bool {
    matches!(conn, SurrealConnection::V3(_))
}
