use clap::{Parser, Subcommand, ValueEnum};
use surreal_sync::{migrate_from_mongodb, migrate_from_neo4j, SourceOpts, SurrealOpts};

#[derive(Parser)]
#[command(name = "surreal-sync")]
#[command(about = "A tool for migrating Neo4j and MongoDB data to SurrealDB")]
#[command(long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Migrate data from source database to SurrealDB
    Sync {
        /// Source database type
        #[arg(value_enum)]
        from: SourceDatabase,

        /// Source database connection options
        #[command(flatten)]
        from_opts: SourceOpts,

        /// Target SurrealDB namespace
        #[arg(long)]
        to_namespace: String,

        /// Target SurrealDB database
        #[arg(long)]
        to_database: String,

        /// Target SurrealDB options
        #[command(flatten)]
        to_opts: SurrealOpts,
    },
}

#[derive(Clone, Debug, ValueEnum)]
enum SourceDatabase {
    /// MongoDB database
    MongoDB,
    /// Neo4j graph database
    Neo4j,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Sync {
            from,
            from_opts,
            to_namespace,
            to_database,
            to_opts,
        } => {
            tracing::info!("Starting migration from {:?} to SurrealDB", from);
            tracing::info!("Target: {}/{}", to_namespace, to_database);

            if to_opts.dry_run {
                tracing::info!("Running in dry-run mode - no data will be written");
            }

            match from {
                SourceDatabase::MongoDB => {
                    migrate_from_mongodb(from_opts, to_namespace, to_database, to_opts).await?;
                }
                SourceDatabase::Neo4j => {
                    migrate_from_neo4j(from_opts, to_namespace, to_database, to_opts).await?;
                }
            }

            tracing::info!("Migration completed successfully");
        }
    }

    Ok(())
}
