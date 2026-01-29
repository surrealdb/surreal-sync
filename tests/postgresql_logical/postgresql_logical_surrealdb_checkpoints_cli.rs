//! CLI Integration Tests for PostgreSQL Logical Replication with SurrealDB Checkpoints
//!
//! These tests verify that the surreal-sync CLI correctly stores and retrieves
//! checkpoints from SurrealDB (not filesystem) for PostgreSQL logical replication.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::postgresql::create_tables_and_indices;
use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};
use surreal_sync_postgresql_wal2json_source::testing::container::PostgresContainer;

/// Test PostgreSQL logical replication with SurrealDB checkpoint storage
#[tokio::test]
async fn test_postgresql_logical_surrealdb_checkpoints_cli(
) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    // Setup PostgreSQL container with wal2json
    const TEST_PORT: u16 = 15442; // Use unique port
    let container = PostgresContainer::new("test-logical-surrealdb-cli", TEST_PORT);
    container.build_image()?;
    container.start()?;
    container.wait_until_ready(30).await?;

    let test_id = generate_test_id();

    // Setup PostgreSQL with test data using container
    let connection_string = format!("postgresql://postgres:postgres@localhost:{TEST_PORT}/testdb");
    let (pg_client, pg_connection) =
        tokio_postgres::connect(&connection_string, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Create the empty table in PostgreSQL
    let dataset = create_unified_full_dataset();
    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    create_tables_and_indices(&pg_client, &dataset).await?;

    // Setup SurrealDB connection with auto-detection for validation
    let surreal_config = TestConfig::new(test_id, "postgresql-logical-surrealdb-test1");
    let conn = connect_auto(&surreal_config).await?;

    cleanup_surrealdb_auto(&conn, &dataset).await?;

    // Get table names from the dataset
    let table_names: Vec<String> = dataset.tables.iter().map(|t| t.name.clone()).collect();
    let tables_arg = table_names.join(",");

    let checkpoint_table = "surreal_sync_checkpoints";

    // Clean up any existing checkpoints from previous test runs
    match &conn {
        SurrealConnection::V2(client) => {
            client
                .query(format!("DELETE FROM {checkpoint_table}"))
                .await?;
        }
        SurrealConnection::V3(client) => {
            client
                .query(format!("DELETE FROM {checkpoint_table}"))
                .await?;
        }
    }

    // Execute CLI command for initial full sync WITH SurrealDB checkpoint storage
    // NOTE: The CLI subcommand is "postgresql" not "postgresql-wal2json"
    let args = [
        "from",
        "postgresql",
        "full",
        "--connection-string",
        &connection_string,
        "--slot",
        "surrealdb_test_slot",
        "--tables",
        &tables_arg,
        "--surreal-endpoint",
        &surreal_config.surreal_endpoint,
        "--to-namespace",
        &surreal_config.surreal_namespace,
        "--to-database",
        &surreal_config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--checkpoints-surreal-table",
        checkpoint_table,
    ];

    let output = execute_surreal_sync(&args)?;
    assert_cli_success(
        &output,
        "PostgreSQL logical full sync with SurrealDB checkpoints",
    );

    // Verify checkpoints were stored in SurrealDB (not filesystem)
    // V2 SDK uses serde::Deserialize, V3 SDK uses SurrealValue trait
    // We define two structs for compatibility with both SDK versions
    #[derive(serde::Deserialize, Debug)]
    struct StoredCheckpointV2 {
        #[allow(dead_code)]
        checkpoint_data: String,
        database_type: String,
        phase: String,
    }

    // For V3, we derive SurrealValue using the crate attribute to point to surrealdb3::types
    use surrealdb3::types::SurrealValue;
    #[derive(SurrealValue, Debug)]
    #[surreal(crate = "surrealdb3::types")]
    struct StoredCheckpointV3 {
        #[allow(dead_code)]
        checkpoint_data: String,
        database_type: String,
        phase: String,
    }

    // Common struct for assertions (converted from version-specific structs)
    #[derive(Debug)]
    struct StoredCheckpoint {
        #[allow(dead_code)]
        checkpoint_data: String,
        database_type: String,
        phase: String,
    }

    let checkpoints: Vec<StoredCheckpoint> = match &conn {
        SurrealConnection::V2(client) => {
            let mut response = client
                .query(format!("SELECT * FROM {checkpoint_table}"))
                .await?;
            let v2_checkpoints: Vec<StoredCheckpointV2> = response.take(0)?;
            v2_checkpoints
                .into_iter()
                .map(|c| StoredCheckpoint {
                    checkpoint_data: c.checkpoint_data,
                    database_type: c.database_type,
                    phase: c.phase,
                })
                .collect()
        }
        SurrealConnection::V3(client) => {
            let mut response = client
                .query(format!("SELECT * FROM {checkpoint_table}"))
                .await?;
            let v3_checkpoints: Vec<StoredCheckpointV3> = response.take(0)?;
            v3_checkpoints
                .into_iter()
                .map(|c| StoredCheckpoint {
                    checkpoint_data: c.checkpoint_data,
                    database_type: c.database_type,
                    phase: c.phase,
                })
                .collect()
        }
    };

    println!("Checkpoints in SurrealDB: {checkpoints:?}");

    // Should have t1 (FullSyncStart) and t2 (FullSyncEnd) checkpoints
    assert_eq!(
        checkpoints.len(),
        2,
        "Should have 2 checkpoints (t1 and t2) in SurrealDB"
    );

    // Verify checkpoint types
    let t1_checkpoint = checkpoints
        .iter()
        .find(|c| c.phase == "full_sync_start")
        .expect("Should have t1 (FullSyncStart) checkpoint");
    let t2_checkpoint = checkpoints
        .iter()
        .find(|c| c.phase == "full_sync_end")
        .expect("Should have t2 (FullSyncEnd) checkpoint");

    assert_eq!(t1_checkpoint.database_type, "postgresql-wal2json");
    assert_eq!(t2_checkpoint.database_type, "postgresql-wal2json");

    println!("t1 checkpoint: {t1_checkpoint:?}");
    println!("t2 checkpoint: {t2_checkpoint:?}");

    // Now insert some data for incremental sync to pick up
    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    // Execute CLI incremental sync command
    // KEY DIFFERENCE: No --incremental-from flag! Reads checkpoint from SurrealDB
    let incremental_args = [
        "from",
        "postgresql",
        "incremental",
        "--connection-string",
        &connection_string,
        "--slot",
        "surrealdb_test_slot",
        "--tables",
        &tables_arg,
        "--surreal-endpoint",
        &surreal_config.surreal_endpoint,
        "--to-namespace",
        &surreal_config.surreal_namespace,
        "--to-database",
        &surreal_config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--checkpoints-surreal-table",
        checkpoint_table,
        "--timeout",
        "30", // Short timeout for test
    ];

    let incremental_output = execute_surreal_sync(&incremental_args)?;
    assert_cli_success(
        &incremental_output,
        "PostgreSQL logical incremental sync with SurrealDB checkpoints",
    );

    println!("Standard Output:");
    println!("{}", String::from_utf8_lossy(&incremental_output.stdout));
    println!("Error Output:");
    println!("{}", String::from_utf8_lossy(&incremental_output.stderr));

    // Verify all data was synced correctly
    assert_synced_auto(
        &conn,
        &dataset,
        "PostgreSQL logical with SurrealDB checkpoints",
        SourceDatabase::PostgreSQL,
    )
    .await?;

    // Cleanup: drop the replication slot
    pg_client
        .execute(
            "SELECT pg_drop_replication_slot('surrealdb_test_slot')",
            &[],
        )
        .await
        .ok(); // Ignore errors if slot doesn't exist

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    // Cleanup checkpoints from SurrealDB
    match &conn {
        SurrealConnection::V2(client) => {
            client
                .query(format!("DELETE FROM {checkpoint_table}"))
                .await?;
        }
        SurrealConnection::V3(client) => {
            client
                .query(format!("DELETE FROM {checkpoint_table}"))
                .await?;
        }
    }

    // Cleanup: Stop container
    container.stop()?;

    Ok(())
}
