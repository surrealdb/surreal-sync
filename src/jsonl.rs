use crate::surreal::Record;
use crate::{SourceOpts, SurrealOpts, SurrealValue};
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
                "ID field mismatch: {id_field} != {target_id_field}"
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
                        crate::write_records(&surreal, file_name, &batch).await?;
                    }
                    total_migrated += batch.len();
                    tracing::debug!("Migrated batch of {} documents", batch.len());
                    batch.clear();
                }
            }

            // Process remaining documents
            if !batch.is_empty() {
                if !to_opts.dry_run {
                    crate::write_records(&surreal, file_name, &batch).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests parsing a well-formed conversion rule with all components present.
    /// Rule format: type="user",user_id users:user_id
    /// Means: Convert objects with type="user" into Thing references to the users table using user_id.
    #[test]
    fn test_parse_valid_rule() {
        let rule_str = r#"type="user",user_id users:user_id"#;
        let rule = ConversionRule::parse(rule_str).unwrap();

        assert_eq!(rule.type_field, "type");
        assert_eq!(rule.type_value, "user");
        assert_eq!(rule.id_field, "user_id");
        assert_eq!(rule.target_table, "users");
    }

    /// Tests that the parser correctly handles extra whitespace around delimiters.
    /// The parser should trim whitespace and extract the same values as without whitespace.
    #[test]
    fn test_parse_with_extra_whitespace() {
        let rule_str = r#"  type="user"  ,  user_id   users:user_id  "#;
        let rule = ConversionRule::parse(rule_str).unwrap();

        assert_eq!(rule.type_field, "type");
        assert_eq!(rule.type_value, "user");
        assert_eq!(rule.id_field, "user_id");
        assert_eq!(rule.target_table, "users");
    }

    /// Tests parsing rules with different type values and identifier patterns.
    /// Each case represents a different entity type that might need conversion:
    /// - organization: Multi-word type value with corresponding table
    /// - product: Standard entity type
    /// - admin_user: Underscore-separated type with simple "id" field name
    #[test]
    fn test_parse_different_type_values() {
        let test_cases = vec![
            (
                r#"type="organization",org_id organizations:org_id"#,
                "organization",
                "org_id",
                "organizations",
            ),
            (
                r#"type="product",product_id products:product_id"#,
                "product",
                "product_id",
                "products",
            ),
            (
                r#"type="admin_user",id admins:id"#,
                "admin_user",
                "id",
                "admins",
            ),
        ];

        for (rule_str, expected_type, expected_id, expected_table) in test_cases {
            let rule = ConversionRule::parse(rule_str).unwrap();
            assert_eq!(rule.type_value, expected_type);
            assert_eq!(rule.id_field, expected_id);
            assert_eq!(rule.target_table, expected_table);
        }
    }

    /// Tests that parsing fails when the comma separator between type and id_field is missing.
    /// Without the comma, the parser cannot distinguish between the type specification and id field.
    #[test]
    fn test_parse_missing_comma() {
        let rule_str = r#"type="user" user_id users:user_id"#;
        let result = ConversionRule::parse(rule_str);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid rule format"));
    }

    /// Tests that parsing fails when the space between id_field and target is missing.
    /// The space is required to separate the source id field from the target table:id mapping.
    #[test]
    fn test_parse_missing_space() {
        let rule_str = r#"type="user",user_idusers:user_id"#;
        let result = ConversionRule::parse(rule_str);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid rule format"));
    }

    /// Tests that parsing fails when type value is not wrapped in quotes.
    /// The parser expects: type="value" not type=value
    #[test]
    fn test_parse_invalid_type_format_no_quotes() {
        let rule_str = r#"type=user,user_id users:user_id"#;
        let result = ConversionRule::parse(rule_str);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid type specification"));
    }

    /// Tests that parsing fails when the opening quote is missing from the type value.
    /// The parser requires both opening and closing quotes around the type value.
    #[test]
    fn test_parse_invalid_type_format_missing_opening_quote() {
        let rule_str = r#"type=user",user_id users:user_id"#;
        let result = ConversionRule::parse(rule_str);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid type specification"));
    }

    /// Tests that parsing fails when the closing quote is missing from the type value.
    /// Without proper quote closure, the parser cannot determine where the type value ends.
    #[test]
    fn test_parse_invalid_type_format_missing_closing_quote() {
        let rule_str = r#"type="user,user_id users:user_id"#;
        let result = ConversionRule::parse(rule_str);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid type specification"));
    }

    /// Tests that parsing fails when the colon separator is missing from the target specification.
    /// The parser expects: table:id_field not table_id_field
    #[test]
    fn test_parse_missing_colon_in_target() {
        let rule_str = r#"type="user",user_id users_user_id"#;
        let result = ConversionRule::parse(rule_str);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid target specification"));
    }

    /// Tests that parsing fails when there are multiple colons in the target specification.
    /// The parser expects exactly one colon to separate table from id_field.
    #[test]
    fn test_parse_too_many_colons_in_target() {
        let rule_str = r#"type="user",user_id users:extra:user_id"#;
        let result = ConversionRule::parse(rule_str);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid target specification"));
    }

    /// Tests that parsing fails when the source id_field doesn't match the target id_field.
    /// This validation ensures consistency: if the rule says "user_id", both sides must use "user_id".
    /// This prevents mistakes like: type="user",user_id users:different_id
    #[test]
    fn test_parse_id_field_mismatch() {
        let rule_str = r#"type="user",user_id users:different_id"#;
        let result = ConversionRule::parse(rule_str);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("ID field mismatch"));
        assert!(err_msg.contains("user_id"));
        assert!(err_msg.contains("different_id"));
    }

    /// Tests that parsing fails when given an empty string.
    /// An empty rule has no components to parse.
    #[test]
    fn test_parse_empty_string() {
        let rule_str = "";
        let result = ConversionRule::parse(rule_str);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid rule format"));
    }

    /// Tests that parsing fails when only the type part is provided without id and target.
    /// A complete rule requires all three parts: type specification, id field, and target mapping.
    #[test]
    fn test_parse_only_type() {
        let rule_str = r#"type="user""#;
        let result = ConversionRule::parse(rule_str);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid rule format"));
    }

    /// Tests parsing a rule with an empty type value (empty string between quotes).
    /// While unusual, the parser should accept type="" as a valid empty type value.
    #[test]
    fn test_parse_empty_type_value() {
        let rule_str = r#"type="",user_id users:user_id"#;
        let rule = ConversionRule::parse(rule_str).unwrap();

        assert_eq!(rule.type_value, "");
        assert_eq!(rule.id_field, "user_id");
        assert_eq!(rule.target_table, "users");
    }

    /// Tests that type values can contain spaces.
    /// Example: type="admin user" is valid and should preserve the space in the type value.
    #[test]
    fn test_parse_type_value_with_spaces() {
        let rule_str = r#"type="admin user",admin_id admins:admin_id"#;
        let rule = ConversionRule::parse(rule_str).unwrap();

        assert_eq!(rule.type_value, "admin user");
        assert_eq!(rule.id_field, "admin_id");
        assert_eq!(rule.target_table, "admins");
    }

    /// Tests that type values can contain special characters like hyphens and underscores.
    /// Example: type="user-profile_v2" should be parsed correctly with all special chars preserved.
    #[test]
    fn test_parse_type_value_with_special_chars() {
        let rule_str = r#"type="user-profile_v2",id profiles:id"#;
        let rule = ConversionRule::parse(rule_str).unwrap();

        assert_eq!(rule.type_value, "user-profile_v2");
        assert_eq!(rule.id_field, "id");
        assert_eq!(rule.target_table, "profiles");
    }
}
