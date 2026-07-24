use serde::Deserialize;
use std::path::PathBuf;

/// Source config for `from postgresql-trigger full -c <file>`.
#[derive(Debug, Deserialize)]
pub struct TriggerFullSource {
    pub postgresql: TriggerFullConfig,
}

/// All settings for a trigger-based full sync, flattened under `[source.postgresql]`.
#[derive(Debug, Deserialize)]
pub struct TriggerFullConfig {
    pub connection_string: String,

    #[serde(default)]
    pub tables: Vec<String>,

    pub schema_file: Option<PathBuf>,

    pub checkpoint_dir: Option<String>,

    pub checkpoints_surreal_table: Option<String>,
}

/// Source config for `from postgresql-trigger incremental -c <file>`.
#[derive(Debug, Deserialize)]
pub struct TriggerIncrementalSource {
    pub postgresql: TriggerIncrementalConfig,
}

/// All settings for a trigger-based incremental sync, flattened under `[source.postgresql]`.
#[derive(Debug, Deserialize)]
pub struct TriggerIncrementalConfig {
    pub connection_string: String,

    #[serde(default)]
    pub tables: Vec<String>,

    pub schema_file: Option<PathBuf>,

    pub checkpoints_surreal_table: Option<String>,

    pub incremental_from: Option<String>,

    pub incremental_to: Option<String>,

    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_timeout() -> u64 {
    3600
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigger_full_config() {
        let toml_str = r#"
[postgresql]
connection_string = "postgresql://user:pass@localhost:5432/mydb"
tables = ["users", "orders"]
checkpoint_dir = "./checkpoints"
"#;
        let config: TriggerFullSource = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.postgresql.connection_string,
            "postgresql://user:pass@localhost:5432/mydb"
        );
        assert_eq!(config.postgresql.tables, vec!["users", "orders"]);
        assert_eq!(
            config.postgresql.checkpoint_dir.as_deref(),
            Some("./checkpoints")
        );
        assert!(config.postgresql.checkpoints_surreal_table.is_none());
        assert!(config.postgresql.schema_file.is_none());
    }

    #[test]
    fn test_trigger_incremental_config() {
        let toml_str = r#"
[postgresql]
connection_string = "postgresql://user:pass@localhost:5432/mydb"
checkpoints_surreal_table = "surreal_sync_checkpoints"
incremental_from = "postgresql:sequence:123"
"#;
        let config: TriggerIncrementalSource = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.postgresql.checkpoints_surreal_table.as_deref(),
            Some("surreal_sync_checkpoints")
        );
        assert_eq!(
            config.postgresql.incremental_from.as_deref(),
            Some("postgresql:sequence:123")
        );
        assert!(config.postgresql.incremental_to.is_none());
        assert_eq!(config.postgresql.timeout, 3600);
    }
}
