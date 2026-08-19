//! SQL Server field definitions, table injection, and unified-dataset assertions.

use crate::testing::{
    surreal::{assert_synced_auto, SurrealConnection},
    table::{SourceDatabase, TestDataSet, TestDoc, TestTable},
    value::{MssqlValue, SurrealDBValue},
};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::collections::HashSet;
use surreal_sync_mssql::from_mssql::testing::{MssqlClient, SqlArg};
use uuid::Uuid;

/// SQL Server-specific field representation.
#[derive(Debug, Clone)]
pub struct MssqlField {
    pub column_name: String,
    pub column_value: MssqlValue,
    pub data_type: String,
    pub precision: Option<u32>,
    pub scale: Option<u32>,
}

impl MssqlField {
    pub fn nvarchar(column: impl Into<String>, value: impl Into<String>) -> Self {
        let value = value.into();
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::NVarchar(value),
            data_type: "NVARCHAR(255)".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn ntext(column: impl Into<String>, value: impl Into<String>) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::NText(value.into()),
            data_type: "NVARCHAR(MAX)".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn xml(column: impl Into<String>, value: impl Into<String>) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::Xml(value.into()),
            data_type: "XML".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn int(column: impl Into<String>, value: i32) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::Int(value),
            data_type: "INT".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn bigint(column: impl Into<String>, value: i64) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::BigInt(value),
            data_type: "BIGINT".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn tinyint(column: impl Into<String>, value: u8) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::TinyInt(value),
            data_type: "TINYINT".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn bit(column: impl Into<String>, value: bool) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::Bit(value),
            data_type: "BIT".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn float(column: impl Into<String>, value: f64) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::Float(value),
            data_type: "FLOAT".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn decimal(
        column: impl Into<String>,
        value: impl Into<String>,
        precision: u32,
        scale: u32,
    ) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::Decimal {
                value: value.into(),
                precision: Some(precision),
                scale: Some(scale),
            },
            data_type: format!("DECIMAL({precision},{scale})"),
            precision: Some(precision),
            scale: Some(scale),
        }
    }

    pub fn money(column: impl Into<String>, value: impl Into<String>) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::Money(value.into()),
            data_type: "MONEY".to_string(),
            precision: Some(19),
            scale: Some(4),
        }
    }

    pub fn datetimeoffset(column: impl Into<String>, value: DateTime<Utc>) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::DateTimeOffset(value),
            data_type: "DATETIMEOFFSET".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn datetime2(column: impl Into<String>, value: NaiveDateTime) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::DateTime2(value),
            data_type: "DATETIME2".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn uniqueidentifier(column: impl Into<String>, value: Uuid) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::UniqueIdentifier(value),
            data_type: "UNIQUEIDENTIFIER".to_string(),
            precision: None,
            scale: None,
        }
    }

    pub fn varbinary(column: impl Into<String>, value: Vec<u8>) -> Self {
        MssqlField {
            column_name: column.into(),
            column_value: MssqlValue::VarBinary(value),
            data_type: "VARBINARY(64)".to_string(),
            precision: None,
            scale: None,
        }
    }
}

fn mssql_value_to_sql_arg(value: &MssqlValue) -> SqlArg {
    match value {
        MssqlValue::Null => SqlArg::Null,
        MssqlValue::Bit(b) => SqlArg::Bool(*b),
        MssqlValue::TinyInt(v) => SqlArg::I16(*v as i16),
        MssqlValue::SmallInt(v) => SqlArg::I16(*v),
        MssqlValue::Int(v) => SqlArg::I32(*v),
        MssqlValue::BigInt(v) => SqlArg::I64(*v),
        MssqlValue::Real(v) => SqlArg::F64(*v as f64),
        MssqlValue::Float(v) => SqlArg::F64(*v),
        MssqlValue::Decimal { value, .. } | MssqlValue::Money(value) => {
            SqlArg::String(value.clone())
        }
        MssqlValue::NChar(s)
        | MssqlValue::NVarchar(s)
        | MssqlValue::NText(s)
        | MssqlValue::Xml(s) => SqlArg::String(s.clone()),
        MssqlValue::VarBinary(b) => SqlArg::Bytes(b.clone()),
        MssqlValue::UniqueIdentifier(u) => SqlArg::Uuid(*u),
        MssqlValue::DateTime2(dt) => SqlArg::String(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
        MssqlValue::DateTimeOffset(dt) => SqlArg::String(dt.to_rfc3339()),
    }
}

fn placeholder_for(idx: usize, value: &MssqlValue) -> String {
    let p = format!("@P{}", idx + 1);
    match value {
        MssqlValue::TinyInt(_) => format!("CAST({p} AS TINYINT)"),
        MssqlValue::Decimal {
            precision, scale, ..
        } => {
            let prec = precision.unwrap_or(18);
            let scale = scale.unwrap_or(0);
            format!("CAST({p} AS DECIMAL({prec},{scale}))")
        }
        MssqlValue::Money(_) => format!("CAST({p} AS MONEY)"),
        MssqlValue::Xml(_) => format!("CAST({p} AS XML)"),
        MssqlValue::DateTime2(_) => format!("CAST({p} AS DATETIME2)"),
        MssqlValue::DateTimeOffset(_) => format!("CAST({p} AS DATETIMEOFFSET)"),
        MssqlValue::Real(_) => format!("CAST({p} AS REAL)"),
        _ => p,
    }
}

/// Drop unified dataset tables, disabling system-versioning when needed.
pub async fn cleanup_unified_dataset_tables(
    client: &MssqlClient,
) -> Result<(), Box<dyn std::error::Error>> {
    let tables = [
        "authored_by",
        "all_types_posts",
        "all_types_users",
        "people",
        "users",
        "posts",
        "authored",
        "Article",
        "Comment",
    ];
    for table in tables {
        let sql = format!(
            r#"
IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL
BEGIN
    IF OBJECTPROPERTY(OBJECT_ID(N'dbo.{table}'), 'TableTemporalType') = 2
        ALTER TABLE [dbo].[{table}] SET (SYSTEM_VERSIONING = OFF);
    IF OBJECT_ID(N'dbo.{table}_history', N'U') IS NOT NULL
        DROP TABLE [dbo].[{table}_history];
    DROP TABLE [dbo].[{table}];
END
"#
        );
        client.simple_query(&sql).await?;
    }
    Ok(())
}

/// Create unified tables (and indexes) in SQL Server.
///
/// `temporal_tables` names receive `PERIOD FOR SYSTEM_TIME` + `SYSTEM_VERSIONING`.
pub async fn create_tables_and_indices(
    client: &MssqlClient,
    dataset: &TestDataSet,
    temporal_tables: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let temporal: HashSet<&str> = temporal_tables.iter().copied().collect();
    for table in dataset.tables.iter().chain(dataset.relations.iter()) {
        let is_temporal = temporal.contains(table.name.as_str());
        let create_sql = table
            .schema
            .mssql
            .to_create_table_ddl(&table.name, is_temporal);
        client.simple_query(&create_sql).await.map_err(|e| {
            format!(
                "creating table {}: {e}\nSQL: {}",
                table.name,
                create_sql.replace('\n', " ")
            )
        })?;
        for index in &table.schema.mssql.indexes {
            let index_sql = table.schema.mssql.to_create_index_ddl(&table.name, index);
            client.simple_query(&index_sql).await.map_err(|e| {
                format!(
                    "creating index {} on {}: {e}\nSQL: {index_sql}",
                    index.name, table.name
                )
            })?;
        }
    }
    Ok(())
}

/// Insert every unified table and relation row.
pub async fn insert_rows(
    client: &MssqlClient,
    dataset: &TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in dataset.tables.iter().chain(dataset.relations.iter()) {
        inject_test_table_mssql(client, table).await?;
    }
    Ok(())
}

/// Insert rows for a single table.
pub async fn inject_test_table_mssql(
    client: &MssqlClient,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    for doc in &table.documents {
        let mssql_doc = doc.to_mssql_doc();
        if mssql_doc.is_empty() {
            continue;
        }
        let columns: Vec<String> = mssql_doc.keys().cloned().collect();
        let placeholders: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| placeholder_for(i, &mssql_doc[col]))
            .collect();
        let insert_sql = format!(
            "INSERT INTO [dbo].[{}] ({}) VALUES ({})",
            table.name,
            columns
                .iter()
                .map(|c| format!("[{c}]"))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders.join(", ")
        );
        let args: Vec<SqlArg> = columns
            .iter()
            .map(|col| mssql_value_to_sql_arg(&mssql_doc[col]))
            .collect();
        client
            .execute(&insert_sql, &args)
            .await
            .map_err(|e| format!("inserting into {}: {e}\nSQL: {insert_sql}", table.name))?;
    }
    Ok(())
}

fn business_pk_column(doc: &TestDoc) -> Option<String> {
    doc.get_field("id")
        .and_then(|f| f.mssql.as_ref())
        .map(|m| m.column_name.clone())
}

fn business_pk_value(doc: &TestDoc) -> Option<String> {
    match doc.get_field("id").map(|f| &f.value) {
        Some(SurrealDBValue::Thing { id, .. }) => match id.as_ref() {
            SurrealDBValue::String(s) => Some(s.clone()),
            SurrealDBValue::Int64(i) => Some(i.to_string()),
            SurrealDBValue::Int32(i) => Some(i.to_string()),
            _ => None,
        },
        _ => None,
    }
}

/// Ordinary (non-temporal) unified sync: every MSSQL-mapped field of every row.
pub async fn assert_synced_mssql(
    conn: &SurrealConnection,
    dataset: &TestDataSet,
    test_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_synced_auto(conn, dataset, test_prefix, SourceDatabase::Mssql).await?;
    for relation in &dataset.relations {
        assert_relation_rows(conn, relation, test_prefix, false, false).await?;
    }
    Ok(())
}

/// Mixed temporal graph: current versions of `temporal_tables` are matched
/// field-by-field to the unified dataset (version ids, not `Table:pk`).
/// Non-temporal unified tables still use PK record ids.
pub async fn assert_synced_mssql_temporal(
    conn: &SurrealConnection,
    dataset: &TestDataSet,
    test_prefix: &str,
    temporal_tables: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    crate::testing::surreal::assert_synced_mixed_temporal_auto(
        conn,
        dataset,
        test_prefix,
        SourceDatabase::Mssql,
        temporal_tables,
    )
    .await?;
    let users_temporal = temporal_tables.contains(&"all_types_users");
    let posts_temporal = temporal_tables.contains(&"all_types_posts");
    for relation in &dataset.relations {
        assert_relation_rows(conn, relation, test_prefix, users_temporal, posts_temporal).await?;
    }
    Ok(())
}

async fn assert_relation_rows(
    conn: &SurrealConnection,
    relation: &TestTable,
    test_prefix: &str,
    users_temporal: bool,
    posts_temporal: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let demoted_to_entity = users_temporal || posts_temporal;
    let count_sql = format!("SELECT count() FROM {} GROUP ALL", relation.name);
    let count = query_count(conn, &count_sql).await?;
    assert_eq!(
        count,
        relation.documents.len(),
        "{test_prefix}: relation '{}' count mismatch - expected {}, found {count}",
        relation.name,
        relation.documents.len()
    );

    for (doc_idx, expected_doc) in relation.documents.iter().enumerate() {
        let expected = expected_doc.to_surrealdb_doc_for_source(SourceDatabase::Mssql);
        let user_id = mssql_column_string(expected_doc, "in")
            .or_else(|| thing_id_string(expected.get("in")))
            .ok_or("authored_by in/user_id missing")?;
        let post_id = mssql_column_string(expected_doc, "out")
            .or_else(|| thing_id_string(expected.get("out")))
            .ok_or("authored_by out/post_id missing")?;

        let sql = if demoted_to_entity {
            let user_pred = if users_temporal {
                "user_id = $user_id".to_string()
            } else {
                "user_id = type::thing('all_types_users', $user_id)".to_string()
            };
            let post_pred = if posts_temporal {
                "post_id = $post_id".to_string()
            } else {
                "post_id = type::thing('all_types_posts', $post_id)".to_string()
            };
            format!(
                "SELECT * FROM {} WHERE {user_pred} AND {post_pred}",
                relation.name
            )
        } else {
            format!(
                "SELECT * FROM {} WHERE in = type::thing('all_types_users', $user_id) AND out = type::thing('all_types_posts', $post_id)",
                relation.name
            )
        };
        let dump = query_debug_bound(conn, &sql, &user_id, &post_id).await?;
        assert!(
            dump.contains(&user_id),
            "{test_prefix}: relation doc {} missing user endpoint {user_id}: {dump}",
            doc_idx + 1
        );
        assert!(
            dump.contains(&post_id),
            "{test_prefix}: relation doc {} missing post endpoint {post_id}: {dump}",
            doc_idx + 1
        );
        if users_temporal {
            assert!(
                !dump_has_record_link(&dump, "all_types_users"),
                "{test_prefix}: FK to temporal users must stay scalar: {dump}"
            );
        } else if !demoted_to_entity {
            assert!(
                dump_has_record_link(&dump, "all_types_users"),
                "{test_prefix}: relation doc {} missing in Thing: {dump}",
                doc_idx + 1
            );
        }
        if posts_temporal {
            assert!(
                !dump_has_record_link(&dump, "all_types_posts"),
                "{test_prefix}: FK to temporal posts must stay scalar: {dump}"
            );
        } else if !demoted_to_entity {
            assert!(
                dump_has_record_link(&dump, "all_types_posts"),
                "{test_prefix}: relation doc {} missing out Thing: {dump}",
                doc_idx + 1
            );
        }
        if expected.contains_key("relationship_created") {
            assert!(
                dump.contains("relationship_created"),
                "{test_prefix}: relation doc {} missing relationship_created: {dump}",
                doc_idx + 1
            );
        }
    }
    Ok(())
}

fn mssql_column_string(doc: &TestDoc, logical: &str) -> Option<String> {
    match doc.get_field(logical)?.mssql.as_ref()?.column_value {
        MssqlValue::NVarchar(ref s) | MssqlValue::NText(ref s) | MssqlValue::NChar(ref s) => {
            Some(s.clone())
        }
        _ => None,
    }
}

pub fn dump_has_record_link(dump: &str, table: &str) -> bool {
    dump.contains(&format!("tb: \"{table}\""))
        || dump.contains(&format!("table: \"{table}\""))
        || dump.contains(&format!("{table}:"))
}

fn thing_id_string(value: Option<&SurrealDBValue>) -> Option<String> {
    match value {
        Some(SurrealDBValue::Thing { id, .. }) => match id.as_ref() {
            SurrealDBValue::String(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

async fn query_count(
    conn: &SurrealConnection,
    sql: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    match conn {
        SurrealConnection::V2(db) => {
            let mut r = db.query(sql).await?;
            let count: Option<i64> = r.take((0, "count"))?;
            Ok(count.unwrap_or(0) as usize)
        }
        SurrealConnection::V3(db) => {
            let mut r = db.query(sql).await?;
            let count: Option<i64> = r.take((0, "count"))?;
            Ok(count.unwrap_or(0) as usize)
        }
    }
}

async fn query_debug_bound(
    conn: &SurrealConnection,
    sql: &str,
    user_id: &str,
    post_id: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    match conn {
        SurrealConnection::V2(db) => {
            let r = db
                .query(sql)
                .bind(("user_id", user_id.to_string()))
                .bind(("post_id", post_id.to_string()))
                .await?;
            Ok(format!("{r:?}"))
        }
        SurrealConnection::V3(db) => {
            let r = db
                .query(sql)
                .bind(("user_id", user_id.to_string()))
                .bind(("post_id", post_id.to_string()))
                .await?;
            Ok(format!("{r:?}"))
        }
    }
}

/// Qualified names for CLI `--tables`.
pub fn unified_table_args() -> Vec<String> {
    vec![
        "dbo.all_types_users".into(),
        "dbo.all_types_posts".into(),
        "dbo.authored_by".into(),
    ]
}

pub fn unified_entity_table_args() -> Vec<String> {
    vec!["dbo.all_types_users".into(), "dbo.all_types_posts".into()]
}

pub fn unified_surreal_tables() -> &'static [&'static str] {
    &["all_types_users", "all_types_posts", "authored_by"]
}

/// Used by temporal lookup; re-exported so surreal.rs can read PK column names.
pub fn doc_business_pk(doc: &TestDoc) -> Option<(String, String)> {
    Some((business_pk_column(doc)?, business_pk_value(doc)?))
}
