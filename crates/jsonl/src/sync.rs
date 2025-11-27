// ! JSONL synchronization logic

use crate::conversion::ConversionRule;
use crate::surreal::{Record, SourceOpts, SurrealOpts, SurrealValue};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use surrealdb::sql::Thing;

/// Migrate data from JSONL files to SurrealDB
///
/// This function reads all .jsonl files from a directory and imports them into SurrealDB.
/// The filename (without .jsonl extension) is used as the table name.
///
/// # Arguments
/// * `from_opts` - Source directory containing .jsonl files
/// * `to_namespace` - Target SurrealDB namespace
/// * `to_database` - Target SurrealDB database
/// * `to_opts` - SurrealDB connection options
/// * `id_field` - Field to use as record ID (default: "id")
/// * `conversion_rules` - Rules for converting JSON objects to Thing references
pub async fn migrate_from_jsonl(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    id_field: String,
    conversion_rules: Vec<String>,
) -> Result<()> {
    tracing::info!("Starting JSONL migration");

    // Parse conversion rules
    let mut rules = Vec::new();
    for rule_str in &conversion_rules {
        rules.push(ConversionRule::parse(rule_str)?);
    }
    tracing::debug!("Parsed {} conversion rules", rules.len());

    // Connect to SurrealDB
    let surreal = crate::surreal::surreal_connect(&to_opts, &to_namespace, &to_database).await?;
    tracing::info!("Connected to SurrealDB");

    // Process JSONL files from directory
    let dir_path = Path::new(&from_opts.source_uri);
    if !dir_path.is_dir() {
        return Err(anyhow!(
            "Source path must be a directory containing JSONL files"
        ));
    }

    let mut total_migrated = 0;

    // Read all .jsonl files in the directory
    for entry in std::fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
            let file_name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| anyhow!("Invalid file name"))?;

            tracing::info!(
                "Processing JSONL file: {} -> table: {}",
                path.display(),
                file_name
            );

            let file = File::open(&path)?;
            let reader = BufReader::new(file);

            let mut batch: Vec<Record> = Vec::new();
            let mut line_count = 0;

            for line in reader.lines() {
                let line = line?;
                line_count += 1;

                if line.trim().is_empty() {
                    continue;
                }

                // Parse JSON line
                let json_value: Value = serde_json::from_str(&line)
                    .map_err(|e| anyhow!("Error parsing JSON at line {line_count}: {e}"))?;

                // Convert to surreal record
                let record = convert_json_to_record(&json_value, file_name, &id_field, &rules)?;

                batch.push(record);

                // Process batch when it reaches the batch size
                if batch.len() >= to_opts.batch_size {
                    if !to_opts.dry_run {
                        crate::surreal::write_records(&surreal, file_name, &batch).await?;
                    }
                    total_migrated += batch.len();
                    tracing::debug!("Migrated batch of {} documents", batch.len());
                    batch.clear();
                }
            }

            // Process remaining documents
            if !batch.is_empty() {
                if !to_opts.dry_run {
                    crate::surreal::write_records(&surreal, file_name, &batch).await?;
                }
                total_migrated += batch.len();
                tracing::debug!("Migrated final batch of {} documents", batch.len());
            }

            tracing::info!(
                "Completed migration of {} documents from {}",
                line_count,
                file_name
            );
        }
    }

    tracing::info!(
        "JSONL migration completed: {} total documents migrated",
        total_migrated
    );
    Ok(())
}

fn convert_json_to_record(
    value: &Value,
    table_name: &str,
    id_field: &str,
    rules: &[ConversionRule],
) -> Result<Record> {
    let mut id: Option<surrealdb::sql::Id> = None;

    if let Value::Object(obj) = value {
        let mut data = HashMap::new();

        for (key, val) in obj {
            if key == id_field {
                // Extract ID for the record
                if let Value::String(s) = val {
                    id = Some(surrealdb::sql::Id::from(s));
                } else if let Value::Number(n) = val {
                    if let Some(i) = n.as_i64() {
                        id = Some(surrealdb::sql::Id::from(i));
                    } else if let Some(u) = n.as_u64() {
                        id = Some(surrealdb::sql::Id::from(u));
                    } else {
                        anyhow::bail!("ID field number must be an integer: {n}");
                    }
                } else {
                    return Err(anyhow!("ID field must be a string or number"));
                }
            } else {
                // Convert the value, applying rules if applicable
                let v = convert_value_with_rules(val, rules)?;
                data.insert(key.clone(), v);
            }
        }

        // Create proper SurrealDB Thing for the record
        let id = match id {
            Some(id) => surrealdb::sql::Thing::from((table_name.to_string(), id)),
            None => return Err(anyhow!("Missing ID field: {id_field}")),
        };

        Ok(Record { id, data })
    } else {
        Err(anyhow!("JSONL line must be a JSON object"))
    }
}

fn convert_value_with_rules(value: &Value, rules: &[ConversionRule]) -> Result<SurrealValue> {
    match value {
        Value::Null => Ok(SurrealValue::Null),
        Value::Bool(b) => Ok(SurrealValue::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(SurrealValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(SurrealValue::Float(f))
            } else {
                Err(anyhow!("Unsupported number type"))
            }
        }
        Value::String(s) => Ok(SurrealValue::String(s.clone())),
        Value::Array(arr) => {
            let mut values = Vec::new();
            for item in arr {
                values.push(convert_value_with_rules(item, rules)?);
            }
            Ok(SurrealValue::Array(values))
        }
        Value::Object(obj) => {
            // Check if this object matches any conversion rule
            if let Some(type_value) = obj.get("type").and_then(|v| v.as_str()) {
                for rule in rules {
                    if rule.type_value == type_value {
                        // This object matches the rule, convert to Thing
                        if let Some(id_value) = obj.get(&rule.id_field).and_then(|v| v.as_str()) {
                            let thing =
                                Thing::from((rule.target_table.clone(), id_value.to_string()));
                            return Ok(SurrealValue::Thing(thing));
                        }
                    }
                }
            }

            // No matching rule, convert as regular object
            let mut kvs = HashMap::new();
            for (key, val) in obj {
                kvs.insert(key.clone(), convert_value_with_rules(val, rules)?);
            }
            Ok(SurrealValue::Object(kvs))
        }
    }
}
