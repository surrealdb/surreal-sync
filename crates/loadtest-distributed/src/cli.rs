//! CLI argument definitions for surreal-loadtest.

use crate::config::{Platform, SourceType};
use crate::preset::PresetSize;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// Container-based distributed load testing for surreal-sync.
#[derive(Parser)]
#[command(name = "surreal-loadtest")]
#[command(about = "Container-based distributed load testing for surreal-sync")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

/// Available commands.
#[derive(Subcommand)]
pub enum Commands {
    /// Generate Docker Compose or Kubernetes configurations
    Generate(GenerateArgs),

    /// Generate and run (Docker Compose only)
    Run(GenerateArgs),

    /// Aggregate results from completed containers
    Aggregate(AggregateArgs),
}

/// Output format for aggregation.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Json,
    Table,
    Markdown,
}

/// Platform choice for CLI.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PlatformChoice {
    DockerCompose,
    Kubernetes,
    Both,
}

impl PlatformChoice {
    pub fn to_platforms(&self) -> Vec<Platform> {
        match self {
            PlatformChoice::DockerCompose => vec![Platform::DockerCompose],
            PlatformChoice::Kubernetes => vec![Platform::Kubernetes],
            PlatformChoice::Both => vec![Platform::DockerCompose, Platform::Kubernetes],
        }
    }
}

/// Source type for CLI.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SourceChoice {
    Mysql,
    /// MySQL trigger-based incremental sync (audit table with triggers)
    /// CLI: `from mysql incremental`
    #[value(name = "mysql-trigger-incremental")]
    MysqlTriggerIncremental,
    Postgresql,
    /// PostgreSQL trigger-based incremental sync (audit table with triggers)
    /// CLI: `from postgresql-trigger incremental`
    #[value(name = "postgresql-trigger-incremental")]
    PostgresqlTriggerIncremental,
    /// PostgreSQL logical replication incremental sync (WAL-based with wal2json)
    /// CLI: `from postgresql incremental` (uses WAL2JSON plugin)
    #[value(name = "postgresql-wal2json-incremental")]
    PostgresqlWal2JsonIncremental,
    Mongodb,
    /// MongoDB change stream incremental sync
    /// CLI: `from mongodb incremental`
    #[value(name = "mongodb-incremental")]
    MongodbIncremental,
    Neo4j,
    /// Neo4j timestamp-based incremental sync
    /// CLI: `from neo4j incremental`
    #[value(name = "neo4j-incremental")]
    Neo4jIncremental,
    Kafka,
    Csv,
    Jsonl,
}

impl From<SourceChoice> for SourceType {
    fn from(choice: SourceChoice) -> Self {
        match choice {
            SourceChoice::Mysql => SourceType::MySQL,
            SourceChoice::MysqlTriggerIncremental => SourceType::MySQLTriggerIncremental,
            SourceChoice::Postgresql => SourceType::PostgreSQL,
            SourceChoice::PostgresqlTriggerIncremental => SourceType::PostgreSQLTriggerIncremental,
            SourceChoice::PostgresqlWal2JsonIncremental => {
                SourceType::PostgreSQLWal2JsonIncremental
            }
            SourceChoice::Mongodb => SourceType::MongoDB,
            SourceChoice::MongodbIncremental => SourceType::MongoDBIncremental,
            SourceChoice::Neo4j => SourceType::Neo4j,
            SourceChoice::Neo4jIncremental => SourceType::Neo4jIncremental,
            SourceChoice::Kafka => SourceType::Kafka,
            SourceChoice::Csv => SourceType::Csv,
            SourceChoice::Jsonl => SourceType::Jsonl,
        }
    }
}

/// Preset size for CLI.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PresetChoice {
    Small,
    Medium,
    Large,
}

impl From<PresetChoice> for PresetSize {
    fn from(choice: PresetChoice) -> Self {
        match choice {
            PresetChoice::Small => PresetSize::Small,
            PresetChoice::Medium => PresetSize::Medium,
            PresetChoice::Large => PresetSize::Large,
        }
    }
}

/// Arguments for the generate command.
#[derive(Args, Clone)]
pub struct GenerateArgs {
    /// Resource preset (small, medium, large)
    #[arg(long, short = 'p', default_value = "medium")]
    pub preset: PresetChoice,

    /// Target platform
    #[arg(long, default_value = "docker-compose")]
    pub platform: PlatformChoice,

    /// Source database type
    #[arg(long, short = 's')]
    pub source: SourceChoice,

    /// Number of containers (overrides preset)
    #[arg(long, short = 'n', alias = "workers")]
    pub num_containers: Option<usize>,

    /// Output directory for generated files
    #[arg(long, short = 'o', default_value = "./loadtest-output")]
    pub output_dir: PathBuf,

    /// Enable tmpfs/ramdisk for data directories
    #[arg(long)]
    pub tmpfs: bool,

    /// tmpfs size (e.g., "2g", "512m")
    #[arg(long)]
    pub tmpfs_size: Option<String>,

    /// CPU limit per container (e.g., "2.0")
    #[arg(long)]
    pub cpu_limit: Option<String>,

    /// Memory limit per container (e.g., "4Gi")
    #[arg(long)]
    pub memory_limit: Option<String>,

    /// Path to loadtest schema YAML file
    #[arg(long)]
    pub schema: PathBuf,

    /// Number of rows per table
    #[arg(long)]
    pub row_count: Option<u64>,

    /// Batch size for database inserts
    #[arg(long)]
    pub batch_size: Option<u64>,

    /// Base random seed (containers use seed, seed+1, seed+2, ...)
    #[arg(long, default_value = "42")]
    pub seed: u64,

    /// Database connection string (for source database)
    #[arg(long)]
    pub connection_string: Option<String>,

    /// Enable tmpfs for database storage (experimental)
    #[arg(long)]
    pub database_tmpfs: bool,

    /// Generate configs in dry-run mode (populate/verify won't write data)
    #[arg(long)]
    pub dry_run: bool,

    /// SurrealDB Docker image (e.g., "surrealdb/surrealdb:v2.0.0")
    #[arg(long)]
    pub surrealdb_image: Option<String>,
}

/// Arguments for the aggregate command (file-based - deprecated in favor of aggregate-server).
#[derive(Args)]
pub struct AggregateArgs {
    /// Directory containing container result JSON files
    #[arg(long, short = 'd')]
    pub results_dir: PathBuf,

    /// Output format
    #[arg(long, short = 'f', default_value = "table")]
    pub output_format: OutputFormat,

    /// Output file (optional, defaults to stdout)
    #[arg(long, short = 'o')]
    pub output: Option<PathBuf>,
}

/// Arguments for the aggregate-server command (HTTP-based metrics collection).
#[derive(Args, Clone)]
pub struct AggregateServerArgs {
    /// Address to listen on (e.g., "0.0.0.0:9090")
    #[arg(long, default_value = "0.0.0.0:9090")]
    pub listen: String,

    /// Number of containers expected to report metrics
    #[arg(long, alias = "expected-workers")]
    pub expected_containers: usize,

    /// Timeout duration (e.g., "30m", "1h")
    #[arg(long, default_value = "30m")]
    pub timeout: String,

    /// Output format for the final report
    #[arg(long, short = 'f', default_value = "table")]
    pub output_format: OutputFormat,
}
