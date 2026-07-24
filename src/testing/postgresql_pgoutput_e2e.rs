//! Shared end-to-end sync test bodies for the PostgreSQL pgoutput WAL CDC source.

use crate::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use crate::testing::{create_unified_full_dataset, SourceDatabase, TestConfig};
use surreal_sync_postgresql::from_pgoutput::{
    capture_head_checkpoint, run_full_sync, run_replication_tail_with_checkpoints,
    PgoutputCheckpoint, ReplicationTailOptions, SourceOpts, SyncOpts,
};

/// WAL replication identifiers for an isolated test database.
#[derive(Clone, Debug)]
pub struct WalTestIds {
    pub db_name: String,
    pub slot_name: String,
    pub publication_name: String,
}

impl WalTestIds {
    pub fn new(test_id: u64) -> Self {
        Self {
            db_name: format!("test_{test_id}"),
            slot_name: format!("slot_{test_id}"),
            publication_name: format!("pub_{test_id}"),
        }
    }
}

pub fn wal_source_opts(test_conn_str: &str, ids: &WalTestIds) -> SourceOpts {
    SourceOpts {
        connection_string: test_conn_str.to_string(),
        schema: "public".to_string(),
        tables: vec![],
        slot_name: ids.slot_name.clone(),
        publication_name: ids.publication_name.clone(),
    }
}

/// Connect to PostgreSQL and spawn the connection driver task.
pub async fn connect_pg(
    conn_str: &str,
) -> Result<tokio_postgres::Client, Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });
    Ok(client)
}

/// Run a full-sync-only e2e test against PostgreSQL reachable at `test_conn_str`.
pub async fn run_postgresql_pgoutput_full_sync_e2e(
    test_conn_str: &str,
    test_id: u64,
    surreal_ws_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let dataset = create_unified_full_dataset();
    let ids = WalTestIds::new(test_id);

    let pg_client = connect_pg(test_conn_str).await?;

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, surreal_ws_endpoint);
    let conn = connect_auto(&surreal_config).await?;

    cleanup_surrealdb_auto(&conn, &dataset).await?;
    crate::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    crate::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;
    crate::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    let source_opts = wal_source_opts(test_conn_str, &ids);
    let sync_opts = SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal_sync_surreal::v2::Surreal2Sink::new(client.clone());
            run_full_sync::<_, surreal_sync_core::NullStore>(&sink, &source_opts, &sync_opts, None)
                .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal_sync_surreal::v3::Surreal3Sink::new(client.clone());
            run_full_sync::<_, surreal_sync_core::NullStore>(&sink, &source_opts, &sync_opts, None)
                .await?;
        }
    }

    assert_synced_auto(
        &conn,
        &dataset,
        "postgresql-pgoutput full sync only",
        SourceDatabase::PostgreSQL,
    )
    .await?;

    crate::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}

/// Run an incremental-sync-only e2e test: empty snapshot captures a WAL checkpoint,
/// rows are inserted, then the replication tail replays them.
pub async fn run_postgresql_pgoutput_incremental_e2e(
    test_conn_str: &str,
    test_id: u64,
    surreal_ws_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let checkpoint_dir = format!(".test-postgresql-pgoutput-incr-lib-checkpoints-{test_id}");
    crate::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let dataset = create_unified_full_dataset();
    let ids = WalTestIds::new(test_id);

    let pg_client = connect_pg(test_conn_str).await?;

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, surreal_ws_endpoint);
    let conn = connect_auto(&surreal_config).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    crate::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    crate::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;

    let source_opts = wal_source_opts(test_conn_str, &ids);
    let sync_opts = SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    let checkpoint_store =
        surreal_sync_runtime::checkpoint_fs::FilesystemStore::new(&checkpoint_dir);
    let sync_manager = surreal_sync_core::SyncManager::new(checkpoint_store);

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal_sync_surreal::v2::Surreal2Sink::new(client.clone());
            run_full_sync(&sink, &source_opts, &sync_opts, Some(&sync_manager)).await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal_sync_surreal::v3::Surreal3Sink::new(client.clone());
            run_full_sync(&sink, &source_opts, &sync_opts, Some(&sync_manager)).await?;
        }
    }

    crate::testing::checkpoint::verify_t1_t2_checkpoints(&checkpoint_dir)?;

    crate::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    let checkpoint_file = surreal_sync_runtime::checkpoint_fs::get_checkpoint_for_phase(
        &checkpoint_dir,
        surreal_sync_core::SyncPhase::FullSyncStart,
    )
    .await?;
    let wal_checkpoint: PgoutputCheckpoint = checkpoint_file.parse()?;

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal_sync_surreal::v2::Surreal2Sink::new(client.clone());
            run_replication_tail_with_checkpoints::<_, surreal_sync_core::NullStore>(
                &sink,
                source_opts,
                wal_checkpoint,
                ReplicationTailOptions::stream(
                    Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
                    None,
                ),
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal_sync_surreal::v3::Surreal3Sink::new(client.clone());
            run_replication_tail_with_checkpoints::<_, surreal_sync_core::NullStore>(
                &sink,
                source_opts,
                wal_checkpoint,
                ReplicationTailOptions::stream(
                    Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
                    None,
                ),
                None,
            )
            .await?;
        }
    }

    assert_synced_auto(
        &conn,
        &dataset,
        "postgresql-pgoutput incremental sync only",
        SourceDatabase::PostgreSQL,
    )
    .await?;

    crate::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}

/// Capture the current WAL head checkpoint (sets up publication when needed).
pub async fn capture_wal_head_checkpoint(
    test_conn_str: &str,
    ids: &WalTestIds,
) -> Result<PgoutputCheckpoint, Box<dyn std::error::Error>> {
    Ok(capture_head_checkpoint(&wal_source_opts(test_conn_str, ids)).await?)
}
