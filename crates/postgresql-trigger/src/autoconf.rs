use anyhow::Result;
use tokio_postgres::Client;

/// Get list of user tables from PostgreSQL (excluding audit tables)
pub async fn get_user_tables(client: &Client, _database: &str) -> Result<Vec<String>> {
    let query = "
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename NOT LIKE 'surreal_sync_%'
        ORDER BY tablename
    ";

    let rows = client.query(query, &[]).await?;
    let tables: Vec<String> = rows
        .iter()
        .map(|row| row.get::<_, String>("tablename"))
        .collect();

    Ok(tables)
}
