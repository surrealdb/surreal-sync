# SurrealSync

A command-line tool for migrating data from Neo4j and MongoDB databases to SurrealDB.

## Features

- **MongoDB Migration**: Migrate document-based data from MongoDB to SurrealDB
- **Neo4j Migration**: Migrate graph data from Neo4j to SurrealDB
- **Configurable Batching**: Control migration batch sizes for optimal performance
- **Dry Run Mode**: Test migrations without writing data
- **Environment Variable Support**: Configure connections via environment variables

## Installation

### Build from Source

```bash
cargo build --release
```

The binary will be available at `target/release/surreal-sync`.

## Usage

### Basic Command Structure

```bash
surreal-sync sync <SOURCE_DATABASE> [OPTIONS] --to-namespace <NAMESPACE> --to-database <DATABASE>
```

### MongoDB Migration

```bash
surreal-sync sync mongo-db \
  --source-uri "mongodb://user:pass@localhost:27017" \
  --source-database "mydb" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --batch-size 500
```

### Neo4j Migration

```bash
surreal-sync sync neo4j \
  --source-uri "bolt://localhost:7687" \
  --source-username "neo4j" \
  --source-password "password" \
  --to-namespace "production" \
  --to-database "graph_data" \
  --dry-run
```

## Environment Variables

You can set these environment variables instead of using command-line flags:

- `SOURCE_URI`: Source database connection string
- `SOURCE_DATABASE`: Source database name
- `SOURCE_USERNAME`: Source database username
- `SOURCE_PASSWORD`: Source database password
- `SURREAL_ENDPOINT`: SurrealDB endpoint (default: http://localhost:8000)
- `SURREAL_USERNAME`: SurrealDB username (default: root)
- `SURREAL_PASSWORD`: SurrealDB password (default: root)

## Command Line Options

### Source Database Options

- `--source-uri`: Connection string/URI for the source database
- `--source-database`: Name of the source database (optional for some sources)
- `--source-username`: Username for source database authentication
- `--source-password`: Password for source database authentication

### Target SurrealDB Options

- `--to-namespace`: Target SurrealDB namespace (required)
- `--to-database`: Target SurrealDB database (required)
- `--surreal-endpoint`: SurrealDB server endpoint (default: http://localhost:8000)
- `--surreal-username`: SurrealDB username (default: root)
- `--surreal-password`: SurrealDB password (default: root)

### Migration Options

- `--batch-size`: Number of records to process in each batch (default: 1000)
- `--dry-run`: Run migration without actually writing data to SurrealDB

## Development

This project is designed to work with the provided devcontainer that includes:
- SurrealDB server (localhost:8000)
- MongoDB server (localhost:27017)
- Neo4j server (localhost:7474/7687)

To use the devcontainer:

1. Open the project in VS Code/Cursor
2. Select "Reopen in Container" when prompted
3. All services will start automatically
