//! Configuration types for distributed load testing.

use serde::{Deserialize, Serialize};

/// Supported orchestration platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Platform {
    DockerCompose,
    Kubernetes,
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Platform::DockerCompose => write!(f, "docker-compose"),
            Platform::Kubernetes => write!(f, "kubernetes"),
        }
    }
}

/// Supported source database types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    MySQL,
    PostgreSQL,
    /// PostgreSQL with WAL-based logical replication (requires wal2json extension)
    PostgreSQLLogical,
    MongoDB,
    Neo4j,
    Kafka,
    Csv,
    Jsonl,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::MySQL => write!(f, "mysql"),
            SourceType::PostgreSQL => write!(f, "postgresql"),
            SourceType::PostgreSQLLogical => write!(f, "postgresql-logical"),
            SourceType::MongoDB => write!(f, "mongodb"),
            SourceType::Neo4j => write!(f, "neo4j"),
            SourceType::Kafka => write!(f, "kafka"),
            SourceType::Csv => write!(f, "csv"),
            SourceType::Jsonl => write!(f, "jsonl"),
        }
    }
}

impl std::str::FromStr for SourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mysql" => Ok(SourceType::MySQL),
            "postgresql" | "postgres" | "postgresql-trigger" => Ok(SourceType::PostgreSQL),
            "postgresql-logical" | "postgresql-wal" => Ok(SourceType::PostgreSQLLogical),
            "mongodb" | "mongo" => Ok(SourceType::MongoDB),
            "neo4j" => Ok(SourceType::Neo4j),
            "kafka" => Ok(SourceType::Kafka),
            "csv" => Ok(SourceType::Csv),
            "jsonl" => Ok(SourceType::Jsonl),
            _ => Err(format!("Unknown source type: {s}")),
        }
    }
}

/// Resource limits for a container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// CPU limit (e.g., "1.0", "2.5")
    pub cpu_limit: String,
    /// Memory limit (e.g., "1Gi", "512Mi")
    pub memory_limit: String,
    /// Optional CPU request (for Kubernetes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_request: Option<String>,
    /// Optional memory request (for Kubernetes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_request: Option<String>,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            cpu_limit: "1.0".to_string(),
            memory_limit: "1Gi".to_string(),
            cpu_request: None,
            memory_request: None,
        }
    }
}

/// tmpfs mount configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TmpfsConfig {
    /// Size of the tmpfs mount (e.g., "1Gi", "512Mi")
    pub size: String,
    /// Mount path (default: /data)
    #[serde(default = "default_tmpfs_path")]
    pub path: String,
}

fn default_tmpfs_path() -> String {
    "/data".to_string()
}

impl Default for TmpfsConfig {
    fn default() -> Self {
        Self {
            size: "1Gi".to_string(),
            path: default_tmpfs_path(),
        }
    }
}

/// Configuration for a single populate/verify container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerConfig {
    /// Unique container identifier (e.g., "populate-1")
    pub id: String,
    /// Source database type
    pub source_type: SourceType,
    /// Tables assigned to this container
    pub tables: Vec<String>,
    /// Number of rows to generate per table
    pub row_count: u64,
    /// Random seed for deterministic data generation
    pub seed: u64,
    /// Resource limits
    pub resources: ResourceLimits,
    /// Optional tmpfs configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tmpfs: Option<TmpfsConfig>,
    /// Connection string for the source database
    pub connection_string: String,
    /// Batch size for inserts
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
}

fn default_batch_size() -> u64 {
    1000
}

/// Shared volume configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeConfig {
    /// Volume name
    pub name: String,
    /// Mount path in containers
    pub mount_path: String,
    /// Use tmpfs for the volume
    #[serde(default)]
    pub tmpfs: bool,
    /// Size if tmpfs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
}

impl Default for VolumeConfig {
    fn default() -> Self {
        Self {
            name: "loadtest-results".to_string(),
            mount_path: "/results".to_string(),
            tmpfs: false,
            size: None,
        }
    }
}

/// Database service configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database type
    pub source_type: SourceType,
    /// Container image
    pub image: String,
    /// Resource limits
    pub resources: ResourceLimits,
    /// Use tmpfs for database storage
    pub tmpfs_storage: bool,
    /// tmpfs size if enabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tmpfs_size: Option<String>,
    /// Database name
    pub database_name: String,
    /// Additional environment variables
    #[serde(default)]
    pub environment: Vec<(String, String)>,
    /// Additional command arguments
    #[serde(default)]
    pub command_args: Vec<String>,
}

/// SurrealDB target configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurrealDbConfig {
    /// Container image
    #[serde(default = "default_surrealdb_image")]
    pub image: String,
    /// Resource limits
    pub resources: ResourceLimits,
    /// Use memory storage
    #[serde(default = "default_true")]
    pub memory_storage: bool,
    /// Namespace
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// Database
    #[serde(default = "default_database")]
    pub database: String,
}

fn default_surrealdb_image() -> String {
    "surrealdb/surrealdb:latest".to_string()
}

fn default_true() -> bool {
    true
}

fn default_namespace() -> String {
    "loadtest".to_string()
}

fn default_database() -> String {
    "test".to_string()
}

impl Default for SurrealDbConfig {
    fn default() -> Self {
        Self {
            image: default_surrealdb_image(),
            resources: ResourceLimits {
                cpu_limit: "4.0".to_string(),
                memory_limit: "4Gi".to_string(),
                cpu_request: None,
                memory_request: None,
            },
            memory_storage: true,
            namespace: default_namespace(),
            database: default_database(),
        }
    }
}

/// Complete cluster configuration for distributed load testing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Target platform
    pub platform: Platform,
    /// Source database type
    pub source_type: SourceType,
    /// Container configurations (populate and verify containers)
    pub containers: Vec<ContainerConfig>,
    /// Shared results volume
    pub results_volume: VolumeConfig,
    /// Source database configuration
    pub database: DatabaseConfig,
    /// SurrealDB target configuration
    pub surrealdb: SurrealDbConfig,
    /// Schema file path
    pub schema_path: String,
    /// Network name
    #[serde(default = "default_network_name")]
    pub network_name: String,
    /// Dry-run mode (populate/verify won't write data)
    #[serde(default)]
    pub dry_run: bool,
}

fn default_network_name() -> String {
    "loadtest-network".to_string()
}

use crate::partitioner::partition_tables;
use crate::preset::{Preset, PresetSize};
use anyhow::Result;
use std::path::PathBuf;

/// Build a complete cluster configuration from CLI arguments.
#[allow(clippy::too_many_arguments)]
pub fn build_cluster_config(
    preset_size: PresetSize,
    source_type: SourceType,
    platform: Platform,
    workers_override: Option<usize>,
    cpu_limit_override: Option<String>,
    memory_limit_override: Option<String>,
    tmpfs_size_override: Option<String>,
    row_count_override: Option<u64>,
    batch_size_override: Option<u64>,
    schema_path: Option<PathBuf>,
    tables: &[String],
    dry_run: bool,
) -> Result<ClusterConfig> {
    // Get base preset
    let preset = Preset::by_size(preset_size).with_overrides(
        workers_override,
        cpu_limit_override.clone(),
        memory_limit_override.clone(),
        tmpfs_size_override.clone(),
        row_count_override,
        batch_size_override,
    );

    // Build container configurations
    let connection_string = get_default_connection_string(source_type);

    let containers = partition_tables(
        tables,
        preset.num_containers,
        source_type,
        preset.row_count,
        42, // base seed
        &preset.container_resources,
        Some(&preset.container_tmpfs),
        &connection_string,
        preset.batch_size,
    );

    // Build database configuration
    let database = DatabaseConfig {
        source_type,
        image: get_default_database_image(source_type),
        resources: preset.database_resources.clone(),
        tmpfs_storage: tmpfs_size_override.is_some(),
        tmpfs_size: tmpfs_size_override,
        database_name: "loadtest".to_string(),
        environment: vec![],
        command_args: vec![],
    };

    // Build SurrealDB configuration
    let surrealdb = SurrealDbConfig {
        resources: preset.surrealdb_resources,
        ..Default::default()
    };

    Ok(ClusterConfig {
        platform,
        source_type,
        containers,
        results_volume: VolumeConfig::default(),
        database,
        surrealdb,
        schema_path: schema_path
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "schema.yaml".to_string()),
        network_name: default_network_name(),
        dry_run,
    })
}

/// Get default connection string for a source type.
fn get_default_connection_string(source_type: SourceType) -> String {
    match source_type {
        SourceType::MySQL => "mysql://root:root@mysql:3306/loadtest".to_string(),
        SourceType::PostgreSQL | SourceType::PostgreSQLLogical => {
            "postgresql://postgres:postgres@postgresql:5432/loadtest".to_string()
        }
        SourceType::MongoDB => {
            "mongodb://root:root@mongodb:27017/loadtest?authSource=admin".to_string()
        }
        SourceType::Neo4j => "bolt://neo4j:password@neo4j:7687".to_string(),
        SourceType::Kafka => "kafka:9092".to_string(),
        SourceType::Csv => "/data/csv".to_string(),
        SourceType::Jsonl => "/data/jsonl".to_string(),
    }
}

/// Get default Docker image for a source type.
fn get_default_database_image(source_type: SourceType) -> String {
    match source_type {
        SourceType::MySQL => "mysql:8.0".to_string(),
        SourceType::PostgreSQL => "postgres:16".to_string(),
        // PostgreSQL logical replication requires wal2json extension
        // Using debezium/postgres image which includes wal2json
        SourceType::PostgreSQLLogical => "debezium/postgres:16".to_string(),
        SourceType::MongoDB => "mongo:7".to_string(),
        SourceType::Neo4j => "neo4j:5".to_string(),
        SourceType::Kafka => "apache/kafka:latest".to_string(),
        SourceType::Csv | SourceType::Jsonl => "busybox:latest".to_string(),
    }
}
