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

    /// Run a single task (used inside containers) - DEPRECATED, use populate/verify subcommands instead
    Worker(TaskArgs),
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
    Postgresql,
    Mongodb,
    Neo4j,
    Kafka,
    Csv,
    Jsonl,
}

impl From<SourceChoice> for SourceType {
    fn from(choice: SourceChoice) -> Self {
        match choice {
            SourceChoice::Mysql => SourceType::MySQL,
            SourceChoice::Postgresql => SourceType::PostgreSQL,
            SourceChoice::Mongodb => SourceType::MongoDB,
            SourceChoice::Neo4j => SourceType::Neo4j,
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

/// Arguments for the task command (populate/verify inside containers).
#[derive(Args)]
pub struct TaskArgs {
    /// Task/container identifier
    #[arg(long, env = "CONTAINER_ID", alias = "worker-id")]
    pub container_id: String,

    /// Path to loadtest schema YAML file
    #[arg(long)]
    pub schema: PathBuf,

    /// Source database type
    #[arg(long, short = 's')]
    pub source: SourceChoice,

    /// Tables to process (comma-separated)
    #[arg(long, short = 't', value_delimiter = ',')]
    pub tables: Vec<String>,

    /// Number of rows per table
    #[arg(long)]
    pub row_count: u64,

    /// Random seed for deterministic data generation
    #[arg(long)]
    pub seed: u64,

    /// Path to write metrics output JSON
    #[arg(long, env = "METRICS_OUTPUT")]
    pub metrics_output: PathBuf,

    /// Database connection string
    #[arg(long)]
    pub connection_string: String,

    /// Batch size for inserts
    #[arg(long, default_value = "1000")]
    pub batch_size: u64,

    /// Run verification instead of populate
    #[arg(long)]
    pub verify: bool,

    /// SurrealDB endpoint (for verification)
    #[arg(long)]
    pub surreal_endpoint: Option<String>,

    /// SurrealDB namespace
    #[arg(long, default_value = "loadtest")]
    pub surreal_namespace: String,

    /// SurrealDB database
    #[arg(long, default_value = "test")]
    pub surreal_database: String,
}

impl TaskArgs {
    pub fn source_type(&self) -> SourceType {
        self.source.into()
    }
}
