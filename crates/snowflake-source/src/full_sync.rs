//! Full (one-shot) snapshot ingestion from Snowflake into SurrealDB.
//!
//! Modeled on `crates/postgresql/src/full_sync.rs`, but — like the CSV source —
//! it does not require a primary key, because Snowflake primary keys are not
//! enforced and are frequently absent.

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use serde_json::Value as JsonValue;
use snowflake_types::convert_cell;
use surreal_sink::SurrealSink;
use sync_core::{UniversalRow, UniversalValue};

use crate::client::{QueryResult, SnowflakeClient};
use crate::{autoconf, SourceOpts, SyncOpts};

/// Ingest every selected table. Returns the total number of rows written.
///
/// When `opts.tables` is empty, all base tables in the schema are discovered and
/// ingested; otherwise the listed tables are used verbatim (upper-cased to match
/// Snowflake's unquoted-identifier casing).
pub async fn run_full_sync<S: SurrealSink>(
    client: &SnowflakeClient,
    sink: &S,
    opts: &SourceOpts,
    sync_opts: &SyncOpts,
) -> Result<usize> {
    let tables: Vec<String> = if opts.tables.is_empty() {
        autoconf::list_tables(client, &opts.database, &opts.schema).await?
    } else {
        opts.tables
            .iter()
            .map(|t| t.trim().to_ascii_uppercase())
            .collect()
    };

    if tables.is_empty() {
        tracing::warn!(
            "No tables found in {}.{} — nothing to ingest",
            opts.database,
            opts.schema
        );
        return Ok(0);
    }

    tracing::info!("Ingesting {} table(s) from Snowflake", tables.len());

    let mut total = 0;
    for table in &tables {
        let count = migrate_table(client, sink, table, opts, sync_opts).await?;
        tracing::info!("Ingested {count} row(s) from table '{table}'");
        total += count;
    }

    tracing::info!("Snowflake ingestion complete: {total} total row(s)");
    Ok(total)
}

/// Ingest a single table. Returns the number of rows written.
pub async fn migrate_table<S: SurrealSink>(
    client: &SnowflakeClient,
    sink: &S,
    table: &str,
    opts: &SourceOpts,
    sync_opts: &SyncOpts,
) -> Result<usize> {
    let db = opts.database.to_ascii_uppercase();
    let schema = opts.schema.to_ascii_uppercase();
    let table_upper = table.to_ascii_uppercase();
    let sql = format!("SELECT * FROM \"{db}\".\"{schema}\".\"{table_upper}\"");

    let result = client.execute_query(&sql).await?;

    if result.rows.is_empty() {
        tracing::info!("Table '{table}' is empty, skipping");
        return Ok(0);
    }

    // Resolve the (optional) ID columns once; empty means auto-generate.
    let id_indices = resolve_id_columns(&result, &opts.id_columns)?;
    let id_index_set: HashSet<usize> = id_indices.iter().copied().collect();

    let mut batch: Vec<UniversalRow> = Vec::new();
    let mut written = 0;

    for (row_index, row) in result.rows.iter().enumerate() {
        if row.len() != result.columns.len() {
            return Err(anyhow!(
                "row {row_index} of table '{table}' has {} cells but {} columns were declared",
                row.len(),
                result.columns.len()
            ));
        }

        let mut fields: HashMap<String, UniversalValue> = HashMap::new();
        for (i, col) in result.columns.iter().enumerate() {
            // When ID columns are explicit, keep them out of the field map so the
            // record ID is not duplicated as a field (matches the PostgreSQL PK behavior).
            if id_index_set.contains(&i) {
                continue;
            }
            let value = convert_cell(&row[i], col)?;
            fields.insert(col.name.clone(), value);
        }

        let id = build_record_id(row, row_index, &id_indices);
        batch.push(UniversalRow::new(
            table_upper.clone(),
            row_index as u64,
            id,
            fields,
        ));

        if batch.len() >= sync_opts.batch_size {
            written += flush(sink, &mut batch, sync_opts.dry_run).await?;
        }
    }

    written += flush(sink, &mut batch, sync_opts.dry_run).await?;
    Ok(written)
}

async fn flush<S: SurrealSink>(
    sink: &S,
    batch: &mut Vec<UniversalRow>,
    dry_run: bool,
) -> Result<usize> {
    if batch.is_empty() {
        return Ok(0);
    }
    let n = batch.len();
    if dry_run {
        tracing::debug!("Dry-run: would write {n} row(s)");
    } else {
        sink.write_universal_rows(batch).await?;
    }
    batch.clear();
    Ok(n)
}

/// Map the configured ID column names to their positions in the result set.
/// Names are matched case-insensitively (Snowflake upper-cases identifiers).
fn resolve_id_columns(result: &QueryResult, id_columns: &[String]) -> Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(id_columns.len());
    for name in id_columns {
        let wanted = name.trim().to_ascii_uppercase();
        let idx = result
            .columns
            .iter()
            .position(|c| c.name.to_ascii_uppercase() == wanted)
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
/// - Composite ID columns: the cells joined with `:` into `Text`.
///
/// All produced variants are legal SurrealDB record-ID types
/// (see `crates/surreal3-sink/src/rows.rs`).
fn build_record_id(row: &[JsonValue], row_index: usize, id_indices: &[usize]) -> UniversalValue {
    match id_indices {
        [] => UniversalValue::Int64(row_index as i64),
        [only] => cell_to_id_scalar(&row[*only]),
        many => {
            let joined = many
                .iter()
                .map(|&i| cell_to_string(&row[i]))
                .collect::<Vec<_>>()
                .join(":");
            UniversalValue::Text(joined)
        }
    }
}

fn cell_to_id_scalar(cell: &JsonValue) -> UniversalValue {
    let s = cell_to_string(cell);
    match s.parse::<i64>() {
        Ok(n) => UniversalValue::Int64(n),
        Err(_) => UniversalValue::Text(s),
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
        assert_eq!(build_record_id(&row, 7, &[]), UniversalValue::Int64(7));
    }

    #[test]
    fn single_integer_id_column_is_int64() {
        let row = vec![json!("42"), json!("name")];
        assert_eq!(build_record_id(&row, 0, &[0]), UniversalValue::Int64(42));
    }

    #[test]
    fn single_non_integer_id_column_is_text() {
        let row = vec![json!("abc"), json!("name")];
        assert_eq!(
            build_record_id(&row, 0, &[0]),
            UniversalValue::Text("abc".to_string())
        );
    }

    #[test]
    fn composite_id_joins_with_colon() {
        let row = vec![json!("a"), json!("b"), json!("c")];
        assert_eq!(
            build_record_id(&row, 0, &[0, 2]),
            UniversalValue::Text("a:c".to_string())
        );
    }
}
