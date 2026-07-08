//! PostgreSQL connection helpers and pgoutput WAL client.

use std::sync::Arc;

use anyhow::Result;
use pgoutput_protocol::{
    parse_postgresql_uri, replication_connection_string, ConnectOptions as WalConnectOptions, Lsn,
    PgWalClient,
};
use surreal_sync_postgresql::new_postgresql_client;
use tokio::sync::Mutex;
use tokio_postgres::Client;
use tracing::info;

use crate::SourceOpts;

pub async fn new_sql_client(connection_string: &str) -> Result<Arc<Mutex<Client>>> {
    new_postgresql_client(connection_string).await
}

pub fn wal_connect_opts(from_opts: &SourceOpts, export_snapshot: bool) -> WalConnectOptions {
    WalConnectOptions {
        connection_string: replication_connection_string(&from_opts.connection_string),
        slot_name: from_opts.slot_name.clone(),
        publication_name: from_opts.publication_name.clone(),
        export_snapshot,
    }
}

pub async fn connect_wal_client(from_opts: &SourceOpts) -> Result<PgWalClient> {
    connect_wal_client_with_export(from_opts, false).await
}

pub async fn connect_wal_client_with_export(
    from_opts: &SourceOpts,
    export_snapshot: bool,
) -> Result<PgWalClient> {
    PgWalClient::connect(wal_connect_opts(from_opts, export_snapshot)).await
}

pub async fn resolve_schema(from_opts: &SourceOpts) -> String {
    if !from_opts.schema.is_empty() {
        return from_opts.schema.clone();
    }
    parse_connection_database(&from_opts.connection_string).unwrap_or_else(|_| "public".to_string())
}

pub async fn ensure_publication(
    client: &Client,
    publication_name: &str,
    schema: &str,
    tables: &[String],
) -> Result<()> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication_name],
        )
        .await?
        .get(0);

    if !exists {
        let sql = if tables.is_empty() {
            format!("CREATE PUBLICATION {publication_name} FOR ALL TABLES")
        } else {
            let qualified: Vec<String> = tables.iter().map(|t| format!("{schema}.{t}")).collect();
            format!(
                "CREATE PUBLICATION {publication_name} FOR TABLE {}",
                qualified.join(", ")
            )
        };
        client.batch_execute(&sql).await?;
        info!("Created publication {publication_name}");
        return Ok(());
    }

    for table in tables {
        let qualified = format!("{schema}.{table}");
        let exists: bool = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_tables
                    WHERE schemaname = $1 AND tablename = $2
                )",
                &[&schema, table],
            )
            .await?
            .get(0);
        if !exists {
            tracing::debug!(
                "Skipping publication add for missing table {qualified} (may have been renamed)"
            );
            continue;
        }
        let in_pub: bool = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_publication_tables
                    WHERE pubname = $1 AND schemaname = $2 AND tablename = $3
                )",
                &[&publication_name, &schema, table],
            )
            .await?
            .get(0);
        if !in_pub {
            client
                .batch_execute(&format!(
                    "ALTER PUBLICATION {publication_name} ADD TABLE {qualified}"
                ))
                .await?;
            info!("Added {qualified} to publication {publication_name}");
        }
    }
    Ok(())
}

pub async fn ensure_publication_for_source(
    client: &Client,
    from_opts: &SourceOpts,
    schema: &str,
) -> Result<()> {
    client
        .batch_execute(&crate::signal::create_signal_table_sql())
        .await?;
    if from_opts.tables.is_empty() {
        ensure_publication(client, &from_opts.publication_name, schema, &[]).await
    } else {
        let mut tables = from_opts.tables.clone();
        if !tables.iter().any(|t| t == crate::signal::SIGNAL_TABLE) {
            tables.push(crate::signal::SIGNAL_TABLE.to_string());
        }
        ensure_publication(client, &from_opts.publication_name, schema, &tables).await
    }
}

pub async fn get_current_wal_lsn(client: &Client) -> Result<Lsn> {
    let lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    Lsn::parse(&lsn)
}

pub async fn start_wal_from_checkpoint(
    client: &mut PgWalClient,
    checkpoint: &crate::PgoutputCheckpoint,
) -> Result<()> {
    client.start(Some(checkpoint.lsn)).await
}

pub async fn start_wal_at_end(client: &mut PgWalClient, sql: &Client) -> Result<()> {
    let lsn = get_current_wal_lsn(sql).await?;
    info!("Starting PostgreSQL WAL at current head: {lsn}");
    client.start(Some(lsn)).await
}

pub fn parse_connection_database(uri: &str) -> Result<String> {
    let (_, _, _, _, db) = parse_postgresql_uri(uri)?;
    Ok(db)
}
