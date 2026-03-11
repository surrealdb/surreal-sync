use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::path::Path;

/// Generic config file structure. `S` is the source-specific config type,
/// determined by which subcommand is being run.
#[derive(Debug, Deserialize)]
pub struct ConfigFile<S> {
    pub source: S,
    pub sink: SinkConfig,
}

/// Sink configuration -- currently only SurrealDB is supported.
#[derive(Debug, Deserialize)]
pub struct SinkConfig {
    pub surrealdb: SurrealDBSinkConfig,
}

/// SurrealDB sink connection and behavior settings.
#[derive(Debug, Deserialize)]
pub struct SurrealDBSinkConfig {
    #[serde(default = "default_surreal_endpoint")]
    pub endpoint: String,

    #[serde(default = "default_surreal_username")]
    pub username: String,

    #[serde(default = "default_surreal_password")]
    pub password: String,

    pub namespace: String,

    pub database: String,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default)]
    pub dry_run: bool,

    pub sdk_version: Option<String>,
}

fn default_surreal_endpoint() -> String {
    "http://localhost:8000".to_string()
}

fn default_surreal_username() -> String {
    "root".to_string()
}

fn default_surreal_password() -> String {
    "root".to_string()
}

fn default_batch_size() -> usize {
    1000
}

/// Load and parse a TOML config file into `ConfigFile<S>`.
pub fn load_config<S: DeserializeOwned>(path: &Path) -> Result<ConfigFile<S>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    let config: ConfigFile<S> =
        toml::from_str(&contents).with_context(|| format!("Failed to parse {}", path.display()))?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize)]
    struct DummySource {
        name: String,
    }

    #[test]
    fn test_load_minimal_config() {
        let toml_str = r#"
[source]
name = "test"

[sink.surrealdb]
namespace = "ns"
database = "db"
"#;
        let config: ConfigFile<DummySource> = toml::from_str(toml_str).unwrap();
        assert_eq!(config.source.name, "test");
        assert_eq!(config.sink.surrealdb.namespace, "ns");
        assert_eq!(config.sink.surrealdb.database, "db");
        assert_eq!(config.sink.surrealdb.endpoint, "http://localhost:8000");
        assert_eq!(config.sink.surrealdb.username, "root");
        assert_eq!(config.sink.surrealdb.batch_size, 1000);
        assert!(!config.sink.surrealdb.dry_run);
    }
}
