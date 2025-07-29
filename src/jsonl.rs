use crate::{BindableValue, SourceOpts, SurrealOpts};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use surrealdb::engine::any::connect;
use surrealdb::sql::Thing;

#[derive(Debug, Clone)]
pub struct ConversionRule {
    pub type_field: String,
    pub type_value: String,
    pub id_field: String,
    pub target_table: String,
}

impl ConversionRule {
    pub fn parse(rule_str: &str) -> Result<Self> {
        // Split by comma but only on the first two commas to handle spaces in the target part
        let parts: Vec<&str> = rule_str.splitn(3, ',').collect();
        if parts.len() < 2 {
            return Err(anyhow!(
                "Invalid rule format. Expected: 'type=\"value\",id_field table:id_field'"
            ));
        }

        let type_part = parts[0].trim();
        let remainder = parts[1].trim();

        // Split the remainder by space to separate id_field and target
        let remainder_parts: Vec<&str> = remainder.splitn(2, ' ').collect();
        if remainder_parts.len() != 2 {
            return Err(anyhow!(
                "Invalid rule format. Expected: 'type=\"value\",id_field table:id_field'"
            ));
        }

        let id_field = remainder_parts[0].trim();
        let target_part = remainder_parts[1].trim();

        // Parse type="value"
        if !type_part.starts_with("type=\"") || !type_part.ends_with('"') {
            return Err(anyhow!(
                "Invalid type specification. Expected: type=\"value\""
            ));
        }
        let type_value = type_part[6..type_part.len() - 1].to_string();

        // Parse table:id_field
        let target_parts: Vec<&str> = target_part.split(':').collect();
        if target_parts.len() != 2 {
            return Err(anyhow!(
                "Invalid target specification. Expected: table:id_field"
            ));
        }
        let target_table = target_parts[0].to_string();
        let target_id_field = target_parts[1].to_string();

        // Validate that id_field matches target_id_field
        if id_field != target_id_field {
            return Err(anyhow!(
                "ID field mismatch: {} != {}",
                id_field,
                target_id_field
            ));
        }

        Ok(ConversionRule {
            type_field: "type".to_string(),
            type_value,
            id_field: id_field.to_string(),
            target_table,
        })
    }
}

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
    let surreal_endpoint = to_opts
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    tracing::debug!("Connecting to SurrealDB at: {}", surreal_endpoint);
    let surreal = connect(surreal_endpoint).await?;

    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &to_opts.surreal_username,
            password: &to_opts.surreal_password,
        })
        .await?;

    surreal.use_ns(&to_namespace).use_db(&to_database).await?;
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

            let mut batch = Vec::new();
            let mut line_count = 0;

            for line in reader.lines() {
                let line = line?;
                line_count += 1;

                if line.trim().is_empty() {
                    continue;
                }

                // Parse JSON line
                let json_value: Value = serde_json::from_str(&line)
                    .map_err(|e| anyhow!("Error parsing JSON at line {}: {}", line_count, e))?;

                // Convert to bindable document
                let (record_id, document) =
                    convert_json_to_bindable(&json_value, file_name, &id_field, &rules)?;

                batch.push((record_id, document));

                // Process batch when it reaches the batch size
                if batch.len() >= to_opts.batch_size {
                    if !to_opts.dry_run {
                        crate::migrate_batch(&surreal, file_name, &batch).await?;
                    }
                    total_migrated += batch.len();
                    tracing::debug!("Migrated batch of {} documents", batch.len());
                    batch.clear();
                }
            }

            // Process remaining documents
            if !batch.is_empty() {
                if !to_opts.dry_run {
                    crate::migrate_batch(&surreal, file_name, &batch).await?;
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

fn convert_json_to_bindable(
    value: &Value,
    table_name: &str,
    id_field: &str,
    rules: &[ConversionRule],
) -> Result<(String, HashMap<String, BindableValue>)> {
    if let Value::Object(obj) = value {
        let mut document = HashMap::new();
        let mut record_id = None;

        for (key, val) in obj {
            if key == id_field {
                // Extract ID for the record
                if let Value::String(id) = val {
                    record_id = Some(format!("{table_name}:{id}"));
                } else if let Value::Number(n) = val {
                    record_id = Some(format!("{table_name}:{n}"));
                } else {
                    return Err(anyhow!("ID field must be a string or number"));
                }
            } else {
                // Convert the value, applying rules if applicable
                let bindable_value = convert_value_with_rules(val, rules)?;
                document.insert(key.clone(), bindable_value);
            }
        }

        let record_id = record_id.ok_or_else(|| anyhow!("Missing ID field: {}", id_field))?;
        Ok((record_id, document))
    } else {
        Err(anyhow!("JSONL line must be a JSON object"))
    }
}

fn convert_value_with_rules(value: &Value, rules: &[ConversionRule]) -> Result<BindableValue> {
    match value {
        Value::Null => Ok(BindableValue::Null),
        Value::Bool(b) => Ok(BindableValue::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(BindableValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(BindableValue::Float(f))
            } else {
                Err(anyhow!("Unsupported number type"))
            }
        }
        Value::String(s) => Ok(BindableValue::String(s.clone())),
        Value::Array(arr) => {
            let mut bindables = Vec::new();
            for item in arr {
                bindables.push(convert_value_with_rules(item, rules)?);
            }
            Ok(BindableValue::Array(bindables))
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
                            return Ok(BindableValue::Thing(thing));
                        }
                    }
                }
            }

            // No matching rule, convert as regular object
            let mut bindables = HashMap::new();
            for (key, val) in obj {
                bindables.insert(key.clone(), convert_value_with_rules(val, rules)?);
            }
            Ok(BindableValue::Object(bindables))
        }
    }
}
