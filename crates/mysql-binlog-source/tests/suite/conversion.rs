//! INSERT -> binlog -> `cdc_change_to_universal` over unified-dataset-style columns.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use binlog_protocol::{BinlogClient, CdcChange, EventBody, ReplicaOptions, SslMode};
use mysql_async::prelude::*;
use surreal_sync_mysql_binlog_source::cdc_change_to_universal;
use surreal_sync_mysql_trigger_source::json_columns::get_json_columns;
use sync_core::{
    ColumnDefinition, DatabaseSchema, TableDefinition, UniversalChangeOp, UniversalType,
};

async fn connect_client(
    conn_str: &str,
    flavor: binlog_protocol::Flavor,
    server_id: u32,
) -> Result<BinlogClient> {
    let binlog_conn = crate::shared::repl_connection_string(conn_str);
    let (host, port, user, pass, _) = crate::shared::parse_mysql_uri(&binlog_conn)?;
    BinlogClient::connect(ReplicaOptions {
        host,
        port,
        username: user,
        password: pass,
        server_id,
        ssl: SslMode::Disabled,
        blocking_poll: Duration::from_millis(200),
        flavor: Some(flavor),
        mariadb_flags: binlog_protocol::MariaDbDumpFlags {
            send_annotate_rows: true,
        },
    })
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))
}

async fn wait_for_users_insert(
    client: &mut BinlogClient,
) -> Result<(CdcChange, binlog_protocol::TableMapEvent)> {
    let mut table_maps: HashMap<u64, binlog_protocol::TableMapEvent> = HashMap::new();
    for _ in 0..80 {
        let events = client
            .next_events(32)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        for event in events {
            match event.body {
                EventBody::TableMap(tm) => {
                    table_maps.insert(tm.table_id, tm);
                }
                EventBody::Rows(rows) => {
                    let Some(table_map) = table_maps.get(&rows.table_id).cloned() else {
                        continue;
                    };
                    if table_map.table != "all_types_users" {
                        continue;
                    }
                    if let Some(row) = rows.rows.into_iter().next() {
                        let change = CdcChange {
                            position: client.current_position(),
                            database: table_map.database.clone(),
                            table: table_map.table.clone(),
                            operation: row,
                            xid: None,
                            gtid: None,
                        };
                        return Ok((change, table_map));
                    }
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("timed out waiting for INSERT on all_types_users")
}

fn users_schema() -> DatabaseSchema {
    let columns = vec![
        ColumnDefinition::new("name", UniversalType::VarChar { length: 255 }),
        ColumnDefinition::nullable("email", UniversalType::VarChar { length: 255 }),
        ColumnDefinition::nullable("age", UniversalType::Int32),
        ColumnDefinition::new("active", UniversalType::Bool),
        ColumnDefinition::new(
            "account_balance",
            UniversalType::Decimal {
                precision: 19,
                scale: 5,
            },
        ),
        ColumnDefinition::nullable("score", UniversalType::Float64),
        ColumnDefinition::nullable("metadata", UniversalType::Json),
        ColumnDefinition::nullable("created_at", UniversalType::LocalDateTime),
        ColumnDefinition::nullable("reference_id", UniversalType::VarChar { length: 255 }),
    ];
    DatabaseSchema::new(vec![TableDefinition::new(
        "all_types_users",
        ColumnDefinition::new("id", UniversalType::VarChar { length: 255 }),
        columns,
    )])
}

#[tokio::test]
async fn binlog_insert_converts_unified_users_columns() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let conn_str = crate::shared::create_test_db(container, "conv_users").await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "CREATE TABLE all_types_users (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255),
            age INT,
            active TINYINT(1) DEFAULT 1,
            account_balance DECIMAL(19, 5),
            score DOUBLE,
            metadata JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reference_id VARCHAR(255)
        )",
    )
    .await?;

    let schema = users_schema();
    let json_columns = get_json_columns(&mut conn, "conv_users").await?;

    let column_names: Vec<String> = conn
        .query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'all_types_users' \
                ORDER BY ORDINAL_POSITION",
        )
        .await?;
    drop(conn);

    let mut client = connect_client(&conn_str, container.flavor(), 9_001_001).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;

    let mut conn = pool.get_conn().await?;
    conn.exec_drop(
        "INSERT INTO all_types_users \
         (id, name, email, age, active, account_balance, score, metadata, reference_id) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "user_001",
            "Alice Example",
            "alice@example.com",
            30i32,
            1i8,
            "12345.67890",
            98.6f64,
            r#"{"settings":{"notifications":1}}"#,
            "ref-001",
        ),
    )
    .await?;

    let (change, table_map) = wait_for_users_insert(&mut client).await?;

    let universal =
        cdc_change_to_universal(&change, &table_map, &column_names, &schema, &json_columns)?;

    assert_eq!(universal.table, "all_types_users");
    assert_eq!(universal.operation, UniversalChangeOp::Create);
    let data = universal.data.context("expected row data")?;
    assert_eq!(
        data.get("name").and_then(|v| v.as_str()),
        Some("Alice Example")
    );
    assert_eq!(data.get("age").and_then(|v| v.as_i64()), Some(30));
    assert!(data.contains_key("metadata"));

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn mariadb_binlog_insert_converts_unified_users_columns() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mariadb_binlog().await;
    let conn_str = crate::shared::create_test_db(container, "conv_users_mdb").await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "CREATE TABLE all_types_users (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255),
            age INT,
            active TINYINT(1) DEFAULT 1,
            account_balance DECIMAL(19, 5),
            score DOUBLE,
            metadata LONGTEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reference_id VARCHAR(255)
        )",
    )
    .await?;

    let schema = users_schema();
    let json_columns = get_json_columns(&mut conn, "conv_users_mdb").await?;

    let column_names: Vec<String> = conn
        .query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'all_types_users' \
                ORDER BY ORDINAL_POSITION",
        )
        .await?;
    drop(conn);

    let mut client = connect_client(&conn_str, container.flavor(), 9_001_002).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;

    let mut conn = pool.get_conn().await?;
    conn.exec_drop(
        "INSERT INTO all_types_users \
         (id, name, email, age, active, account_balance, score, metadata, reference_id) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "user_001",
            "Bob Example",
            "bob@example.com",
            42i32,
            0i8,
            "99.99000",
            12.5f64,
            r#"{"tags":["a","b"]}"#,
            "ref-mdb",
        ),
    )
    .await?;

    let (change, table_map) = wait_for_users_insert(&mut client).await?;

    let universal =
        cdc_change_to_universal(&change, &table_map, &column_names, &schema, &json_columns)?;
    assert_eq!(universal.operation, UniversalChangeOp::Create);
    assert!(universal.data.is_some());

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}
