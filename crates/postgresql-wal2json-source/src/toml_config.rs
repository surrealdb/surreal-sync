use serde::Deserialize;
use std::path::PathBuf;

/// Source config for `from postgresql full -c <file>`.
#[derive(Debug, Deserialize)]
pub struct Wal2jsonFullSource {
    pub postgresql: Wal2jsonFullConfig,
}

/// All settings for a wal2json-based full sync, flattened under `[source.postgresql]`.
#[derive(Debug, Deserialize)]
pub struct Wal2jsonFullConfig {
    pub connection_string: String,

    #[serde(default = "default_slot")]
    pub slot: String,

    #[serde(default)]
    pub tables: Vec<String>,

    #[serde(default = "default_schema")]
    pub schema: String,

    pub schema_file: Option<PathBuf>,

    pub checkpoint_dir: Option<String>,

    pub checkpoints_surreal_table: Option<String>,
}

/// Source config for `from postgresql incremental -c <file>`.
#[derive(Debug, Deserialize)]
pub struct Wal2jsonIncrementalSource {
    pub postgresql: Wal2jsonIncrementalConfig,
}

/// All settings for a wal2json-based incremental sync, flattened under `[source.postgresql]`.
#[derive(Debug, Deserialize)]
pub struct Wal2jsonIncrementalConfig {
    pub connection_string: String,

    #[serde(default = "default_slot")]
    pub slot: String,

    #[serde(default)]
    pub tables: Vec<String>,

    #[serde(default = "default_schema")]
    pub schema: String,

    pub schema_file: Option<PathBuf>,

    pub checkpoints_surreal_table: Option<String>,

    pub incremental_from: Option<String>,

    pub incremental_to: Option<String>,

    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_slot() -> String {
    "surreal_sync_slot".to_string()
}

fn default_schema() -> String {
    "public".to_string()
}

fn default_timeout() -> u64 {
    3600
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal2json_full_config() {
        let toml_str = r#"
[postgresql]
connection_string = "postgresql://user:pass@localhost:5432/mydb"
tables = ["users", "orders"]
checkpoint_dir = "./checkpoints"
"#;
        let config: Wal2jsonFullSource = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.postgresql.connection_string,
            "postgresql://user:pass@localhost:5432/mydb"
        );
        assert_eq!(config.postgresql.slot, "surreal_sync_slot");
        assert_eq!(config.postgresql.schema, "public");
        assert_eq!(config.postgresql.tables, vec!["users", "orders"]);
        assert_eq!(
            config.postgresql.checkpoint_dir.as_deref(),
            Some("./checkpoints")
        );
    }

    #[test]
    fn test_wal2json_incremental_config() {
        let toml_str = r#"
[postgresql]
connection_string = "postgresql://user:pass@localhost:5432/mydb"
slot = "my_slot"
schema = "myschema"
checkpoints_surreal_table = "surreal_sync_checkpoints"
incremental_from = "0/1949850"
timeout = 7200
"#;
        let config: Wal2jsonIncrementalSource = toml::from_str(toml_str).unwrap();
        assert_eq!(config.postgresql.slot, "my_slot");
        assert_eq!(config.postgresql.schema, "myschema");
        assert_eq!(
            config.postgresql.checkpoints_surreal_table.as_deref(),
            Some("surreal_sync_checkpoints")
        );
        assert_eq!(
            config.postgresql.incremental_from.as_deref(),
            Some("0/1949850")
        );
        assert_eq!(config.postgresql.timeout, 7200);
    }
}
