//! Watermark / signal table on the SQL Server source.

use anyhow::Result;
use uuid::Uuid;

use crate::from_mssql::cdc::ensure_table_cdc;
use crate::from_mssql::client::{MssqlClient, SqlArg};
use crate::from_mssql::naming::QualifiedName;

/// Signal table used for interleaved watermark rows (`dbo.surreal_sync_signal`).
pub const SIGNAL_TABLE: &str = "surreal_sync_signal";

/// Qualified name of the signal table.
pub fn signal_qualified() -> QualifiedName {
    QualifiedName::new("dbo", SIGNAL_TABLE)
}

fn create_signal_table_sql() -> String {
    format!(
        "IF OBJECT_ID(N'dbo.{SIGNAL_TABLE}', N'U') IS NULL \
         CREATE TABLE dbo.{SIGNAL_TABLE} ( \
            id uniqueidentifier NOT NULL PRIMARY KEY, \
            kind nvarchar(32) NOT NULL, \
            tables nvarchar(max) NULL, \
            consumed bit NOT NULL CONSTRAINT DF_{SIGNAL_TABLE}_consumed DEFAULT (0) \
         );"
    )
}

/// Create the signal table and enable CDC on it.
pub async fn ensure_signal_table(client: &MssqlClient) -> Result<()> {
    client.simple_query(&create_signal_table_sql()).await?;
    ensure_table_cdc(client, &signal_qualified()).await?;
    Ok(())
}

/// Insert a watermark row. That insert must appear in CDC as a UUID primary key.
pub async fn insert_watermark(client: &MssqlClient, kind: &str, id: Uuid) -> Result<()> {
    client
        .execute(
            &format!("INSERT INTO dbo.{SIGNAL_TABLE} (id, kind, consumed) VALUES (@P1, @P2, 0)"),
            &[SqlArg::Uuid(id), SqlArg::String(kind.to_string())],
        )
        .await?;
    Ok(())
}
