use tokio_postgres::Config as PostgresConfig;

/// Configuration for PostgreSQL connection and sync settings
#[derive(Debug, Clone)]
pub struct Config {
    /// PostgreSQL connection configuration
    pub postgres_config: PostgresConfig,

    /// Tables to sync (empty means all tables)
    pub tables: Vec<String>,

    /// PostgreSQL schema (default: "public")
    pub schema: String,

    /// Replication slot name for logical replication (default: "surreal_sync_slot")
    pub slot: String,
}

impl Config {
    /// Creates a new PostgreSQL config from a connection string
    ///
    /// # Arguments
    /// * `connection_string` - PostgreSQL connection string (e.g., "host=localhost user=postgres password=postgres dbname=testdb")
    /// * `slot` - Replication slot name
    ///
    /// # Errors
    /// Returns an error if the connection string cannot be parsed
    pub fn new(connection_string: &str, slot: String) -> Result<Self, tokio_postgres::Error> {
        let postgres_config = connection_string.parse::<PostgresConfig>()?;

        Ok(Self {
            postgres_config,
            tables: Vec::new(),
            schema: "public".to_string(),
            slot,
        })
    }

    /// Returns a reference to the PostgreSQL connection config
    pub fn postgres_config(&self) -> &PostgresConfig {
        &self.postgres_config
    }
}

impl Default for Config {
    fn default() -> Self {
        let postgres_config = "host=localhost user=postgres password=postgres dbname=postgres"
            .parse::<PostgresConfig>()
            .expect("Default PostgreSQL config should be valid");

        Self {
            postgres_config,
            tables: Vec::new(),
            schema: "public".to_string(),
            slot: "surreal_sync_slot".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_from_connection_string() {
        let config = Config::new(
            "host=localhost user=testuser password=testpass dbname=testdb",
            "test_slot".to_string(),
        )
        .unwrap();

        let host = &config.postgres_config.get_hosts()[0];
        let tokio_postgres::config::Host::Tcp(host) = host else {
            panic!("Expected TCP host");
        };
        assert_eq!(host.as_str(), "localhost");
        assert_eq!(config.postgres_config.get_user(), Some("testuser"));
        assert_eq!(config.postgres_config.get_dbname(), Some("testdb"));
        assert_eq!(config.slot, "test_slot");
        assert_eq!(config.schema, "public");
        assert!(config.tables.is_empty());
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();

        let host = &config.postgres_config.get_hosts()[0];
        let tokio_postgres::config::Host::Tcp(host) = host else {
            panic!("Expected TCP host");
        };
        assert_eq!(host.as_str(), "localhost");
        assert_eq!(config.postgres_config.get_user(), Some("postgres"));
        assert_eq!(config.postgres_config.get_dbname(), Some("postgres"));
        assert_eq!(config.schema, "public");
        assert_eq!(config.slot, "surreal_sync_slot");
        assert!(config.tables.is_empty());
    }

    #[test]
    fn test_invalid_connection_string() {
        let result = Config::new("invalid connection string", "test_slot".to_string());
        assert!(result.is_err());
    }
}
