//! Full (one-shot) snapshot ingestion from BigQuery into SurrealDB.
//!
//! Modeled on the Snowflake full sync: rows stream through [`RowChunkDriver`] /
//! [`run_source_runtime_with`] so transform batching and `max_in_flight` overlap
//! apply within each table. BigQuery has no primary keys at all, so ID columns are
//! optional — like CSV, sequential ids are generated when omitted.
//!
//! Source reads are **page-bounded**: the REST API returns result pages; we keep
//! one page buffered and slice it into `sync_opts.batch_size` apply chunks (a
//! trailing chunk may be smaller when `page_len % batch_size != 0`). There is no
//! CDC and no durable source cursor — watermarks advance in-memory through the
//! shared apply path (`CheckpointPolicy::AdvanceOnly`), but there is nothing to
//! resume after a process restart.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::types::{convert_cell, FieldSchema};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Row, Value};
use surreal_sync_runtime::{
    run_source_runtime_with, ApplyOpts, Pipeline, RowChunkDriver, RowChunkSource, SourceRuntimeOpts,
};

use super::client::{BigQueryClient, QueryResult, QueryStream};
use super::{autoconf, SourceOpts, SyncOpts};

/// Ingest every selected table with an identity transform pipeline.
///
/// Returns the total number of rows sunk (0 in dry-run after a scan).
pub async fn run_full_sync<S: SurrealSink>(
    client: &BigQueryClient,
    sink: &S,
    opts: &SourceOpts,
    sync_opts: &SyncOpts,
) -> Result<usize> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_full_sync_with_transforms(client, sink, opts, sync_opts, &pipeline, &apply_opts).await
}

/// Ingest every selected table through the shared transform/apply path.
///
/// When `opts.tables` is empty, all base tables in the dataset are discovered and
/// ingested; otherwise the listed tables are used verbatim. BigQuery identifiers
/// are case-sensitive, so — unlike Snowflake — names are never case-folded.
pub async fn run_full_sync_with_transforms<S: SurrealSink>(
    client: &BigQueryClient,
    sink: &S,
    opts: &SourceOpts,
    sync_opts: &SyncOpts,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<usize> {
    if pipeline.is_identity() {
        tracing::debug!("BigQuery full sync using identity transform pipeline");
    } else {
        tracing::info!(
            stages = pipeline.len(),
            "BigQuery full sync using transform pipeline"
        );
    }

    let tables: Vec<String> = if opts.tables.is_empty() {
        autoconf::list_tables(client).await?
    } else {
        opts.tables
            .iter()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect()
    };

    if tables.is_empty() {
        tracing::warn!(
            "No tables found in {}.{} — nothing to ingest",
            opts.project_id,
            opts.dataset
        );
        return Ok(0);
    }

    tracing::info!("Ingesting {} table(s) from BigQuery", tables.len());

    let mut total = 0;
    for table in &tables {
        let count = migrate_table_with_transforms(
            client, sink, table, opts, sync_opts, pipeline, apply_opts,
        )
        .await?;
        tracing::info!("Ingested {count} row(s) from table '{table}'");
        total += count;
    }

    tracing::info!("BigQuery ingestion complete: {total} total row(s)");
    Ok(total)
}

/// Ingest a single table with an identity pipeline.
pub async fn migrate_table<S: SurrealSink>(
    client: &BigQueryClient,
    sink: &S,
    table: &str,
    opts: &SourceOpts,
    sync_opts: &SyncOpts,
) -> Result<usize> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    migrate_table_with_transforms(client, sink, table, opts, sync_opts, &pipeline, &apply_opts)
        .await
}

/// Ingest a single table through [`RowChunkDriver`], streaming BigQuery result
/// pages and slicing each into `sync_opts.batch_size` apply chunks.
pub async fn migrate_table_with_transforms<S: SurrealSink>(
    client: &BigQueryClient,
    sink: &S,
    table: &str,
    opts: &SourceOpts,
    sync_opts: &SyncOpts,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<usize> {
    validate_identifier(table, "table")?;
    validate_identifier(&opts.project_id, "project id")?;
    validate_identifier(&opts.dataset, "dataset")?;

    let sql = format!(
        "SELECT * FROM `{}`.`{}`.`{}`",
        opts.project_id, opts.dataset, table
    );

    let stream = client.execute_query_stream(&sql).await?;
    apply_query_stream_with_transforms(sink, table, stream, opts, sync_opts, pipeline, apply_opts)
        .await
}

/// Reject identifiers that are not plain names.
///
/// There is no parameter binding for identifiers in BigQuery SQL, so the table,
/// dataset, and project names are interpolated into the statement. Backticks alone
/// are not a safe quote (a name containing one would close the quote), and every
/// legal unquoted BigQuery identifier matches this pattern anyway.
fn validate_identifier(name: &str, kind: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        bail!("invalid BigQuery {kind} '{name}': expected letters, digits, '_' or '-'");
    }
    Ok(())
}

/// Push a live [`QueryStream`] through the shared apply path (production path).
pub async fn apply_query_stream_with_transforms<S: SurrealSink>(
    sink: &S,
    table: &str,
    mut stream: QueryStream<'_>,
    opts: &SourceOpts,
    sync_opts: &SyncOpts,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<usize> {
    let schema = stream.schema().to_vec();
    let id_indices = resolve_id_columns(&schema, &opts.id_columns)?;
    let id_index_set: HashSet<usize> = id_indices.iter().copied().collect();
    let batch_size = sync_opts.batch_size.max(1);

    if sync_opts.dry_run {
        let mut converted = 0usize;
        while let Some(raw_batch) = stream.next_batch(batch_size).await? {
            for row in raw_batch {
                convert_row(table, converted, &row, &schema, &id_indices, &id_index_set)?;
                converted += 1;
            }
        }
        tracing::info!("Dry-run scanned table '{table}': {converted} row(s)");
        return Ok(converted);
    }

    struct BigQueryPageChunks<'a> {
        stream: QueryStream<'a>,
        schema: Vec<FieldSchema>,
        table: String,
        id_indices: Vec<usize>,
        id_index_set: HashSet<usize>,
        batch_size: usize,
        next_row_index: usize,
    }

    #[async_trait]
    impl RowChunkSource for BigQueryPageChunks<'_> {
        async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<Row>>> {
            let Some(raw_batch) = self.stream.next_batch(self.batch_size).await? else {
                return Ok(None);
            };
            let mut batch = Vec::with_capacity(raw_batch.len());
            for row in raw_batch {
                let row_index = self.next_row_index;
                batch.push(convert_row(
                    &self.table,
                    row_index,
                    &row,
                    &self.schema,
                    &self.id_indices,
                    &self.id_index_set,
                )?);
                self.next_row_index = self.next_row_index.saturating_add(1);
            }
            Ok(Some(batch))
        }
    }

    let chunks = BigQueryPageChunks {
        stream,
        schema,
        table: table.to_string(),
        id_indices,
        id_index_set,
        batch_size,
        next_row_index: 0,
    };
    let mut driver = RowChunkDriver::new(chunks);
    let transformer = Arc::new(pipeline.clone());
    let runtime_opts = SourceRuntimeOpts::new();
    run_source_runtime_with(&mut driver, sink, transformer, apply_opts, &runtime_opts).await?;
    Ok(driver.sunk_count() as usize)
}

/// Convert an in-memory [`QueryResult`] and push it through the shared apply path.
///
/// Exposed for tests that build result sets without talking to BigQuery.
pub async fn apply_query_result_with_transforms<S: SurrealSink>(
    sink: &S,
    table: &str,
    result: &QueryResult,
    opts: &SourceOpts,
    sync_opts: &SyncOpts,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<usize> {
    let id_indices = resolve_id_columns(&result.schema, &opts.id_columns)?;
    let id_index_set: HashSet<usize> = id_indices.iter().copied().collect();
    let batch_size = sync_opts.batch_size.max(1);

    if sync_opts.dry_run {
        let mut converted = 0usize;
        for (row_index, row) in result.rows.iter().enumerate() {
            convert_row(
                table,
                row_index,
                row,
                &result.schema,
                &id_indices,
                &id_index_set,
            )?;
            converted += 1;
        }
        tracing::info!("Dry-run scanned table '{table}': {converted} row(s)");
        return Ok(converted);
    }

    struct InMemoryRowChunks<'a> {
        rows: &'a [Vec<JsonValue>],
        schema: &'a [FieldSchema],
        table: String,
        id_indices: Vec<usize>,
        id_index_set: HashSet<usize>,
        batch_size: usize,
        next_index: usize,
    }

    #[async_trait]
    impl RowChunkSource for InMemoryRowChunks<'_> {
        async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<Row>>> {
            if self.next_index >= self.rows.len() {
                return Ok(None);
            }
            let end = (self.next_index + self.batch_size).min(self.rows.len());
            let mut batch = Vec::with_capacity(end - self.next_index);
            for row_index in self.next_index..end {
                batch.push(convert_row(
                    &self.table,
                    row_index,
                    &self.rows[row_index],
                    self.schema,
                    &self.id_indices,
                    &self.id_index_set,
                )?);
            }
            self.next_index = end;
            Ok(Some(batch))
        }
    }

    let chunks = InMemoryRowChunks {
        rows: &result.rows,
        schema: &result.schema,
        table: table.to_string(),
        id_indices,
        id_index_set,
        batch_size,
        next_index: 0,
    };
    let mut driver = RowChunkDriver::new(chunks);
    let transformer = Arc::new(pipeline.clone());
    let runtime_opts = SourceRuntimeOpts::new();
    run_source_runtime_with(&mut driver, sink, transformer, apply_opts, &runtime_opts).await?;
    Ok(driver.sunk_count() as usize)
}

fn convert_row(
    table: &str,
    row_index: usize,
    row: &[JsonValue],
    schema: &[FieldSchema],
    id_indices: &[usize],
    id_index_set: &HashSet<usize>,
) -> Result<Row> {
    if row.len() != schema.len() {
        return Err(anyhow!(
            "row {row_index} of table '{table}' has {} cells but {} columns were declared",
            row.len(),
            schema.len()
        ));
    }

    let mut fields: HashMap<String, Value> = HashMap::new();
    for (i, field) in schema.iter().enumerate() {
        // When ID columns are explicit, keep them out of the field map so the
        // record ID is not duplicated as a field (matches the PostgreSQL PK behavior).
        if id_index_set.contains(&i) {
            continue;
        }
        let value = convert_cell(&row[i], field)?;
        fields.insert(field.name.clone(), value);
    }

    let id = build_record_id(row, row_index, id_indices);
    Ok(Row::new(table.to_string(), row_index as u64, id, fields))
}

/// Map the configured ID column names to their positions in the result set.
///
/// BigQuery identifiers are case-sensitive, so an exact match is tried first; a
/// case-insensitive match is accepted as a fallback so `--id-columns ID` still
/// finds an `id` column rather than failing on a technicality.
fn resolve_id_columns(schema: &[FieldSchema], id_columns: &[String]) -> Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(id_columns.len());
    for name in id_columns {
        let wanted = name.trim();
        let idx = schema
            .iter()
            .position(|f| f.name == wanted)
            .or_else(|| {
                schema
                    .iter()
                    .position(|f| f.name.eq_ignore_ascii_case(wanted))
            })
            .ok_or_else(|| anyhow!("id column '{name}' not found in result set"))?;
        indices.push(idx);
    }
    Ok(indices)
}

/// Build the SurrealDB record ID for a row.
///
/// - No ID columns: sequential per-table index (`Int64`). Deterministic within a
///   run, but not stable across re-runs.
/// - Single ID column: the cell as `Int64` when it parses as an integer, else `Text`.
/// - Composite ID columns: an [`Value::Array`] of scalar parts (Surreal
///   array record ID). Use the `flatten_id` transform to restore colon-joined Text.
fn build_record_id(row: &[JsonValue], row_index: usize, id_indices: &[usize]) -> Value {
    match id_indices {
        [] => Value::Int64(row_index as i64),
        [only] => cell_to_id_scalar(&row[*only]),
        many => {
            let parts: Vec<Value> = many.iter().map(|&i| cell_to_id_scalar(&row[i])).collect();
            Value::Array {
                elements: parts,
                element_type: Box::new(surreal_sync_core::Type::Text),
            }
        }
    }
}

fn cell_to_id_scalar(cell: &JsonValue) -> Value {
    let s = cell_to_string(cell);
    match s.parse::<i64>() {
        Ok(n) => Value::Int64(n),
        Err(_) => Value::Text(s),
    }
}

fn cell_to_string(cell: &JsonValue) -> String {
    match cell {
        JsonValue::String(s) => s.clone(),
        JsonValue::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn auto_generated_id_is_row_index() {
        let row = vec![json!("x")];
        assert_eq!(build_record_id(&row, 7, &[]), Value::Int64(7));
    }

    #[test]
    fn single_integer_id_column_is_int64() {
        let row = vec![json!("42"), json!("name")];
        assert_eq!(build_record_id(&row, 0, &[0]), Value::Int64(42));
    }

    #[test]
    fn single_non_integer_id_column_is_text() {
        let row = vec![json!("abc"), json!("name")];
        assert_eq!(
            build_record_id(&row, 0, &[0]),
            Value::Text("abc".to_string())
        );
    }

    #[test]
    fn composite_id_is_array() {
        let row = vec![json!("a"), json!("b"), json!("c")];
        assert_eq!(
            build_record_id(&row, 0, &[0, 2]),
            Value::Array {
                elements: vec![Value::Text("a".to_string()), Value::Text("c".to_string()),],
                element_type: Box::new(surreal_sync_core::Type::Text),
            }
        );
    }

    #[test]
    fn id_columns_match_case_sensitively_first() {
        // Two columns differing only in case: the exact spelling must win, because
        // BigQuery treats them as distinct columns.
        let schema = vec![
            FieldSchema::new("ID", "STRING"),
            FieldSchema::new("id", "STRING"),
        ];
        assert_eq!(
            resolve_id_columns(&schema, &["id".to_string()]).unwrap(),
            vec![1]
        );
        assert_eq!(
            resolve_id_columns(&schema, &["ID".to_string()]).unwrap(),
            vec![0]
        );
    }

    #[test]
    fn id_columns_fall_back_to_case_insensitive() {
        let schema = vec![FieldSchema::new("id", "INTEGER")];
        assert_eq!(
            resolve_id_columns(&schema, &["ID".to_string()]).unwrap(),
            vec![0]
        );
    }

    #[test]
    fn unknown_id_column_is_an_error() {
        let schema = vec![FieldSchema::new("id", "INTEGER")];
        assert!(resolve_id_columns(&schema, &["missing".to_string()]).is_err());
    }

    #[test]
    fn identifiers_with_sql_metacharacters_are_rejected() {
        assert!(validate_identifier("orders", "table").is_ok());
        assert!(validate_identifier("my-project_1", "project id").is_ok());
        assert!(validate_identifier("orders`; DROP TABLE x; --", "table").is_err());
        assert!(validate_identifier("a.b", "table").is_err());
        assert!(validate_identifier("", "table").is_err());
    }
}
