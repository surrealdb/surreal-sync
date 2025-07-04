# SurrealSync

A command-line tool for migrating data from Neo4j and MongoDB databases to SurrealDB.

## Features

- **MongoDB Migration**: Migrate document-based data from MongoDB to SurrealDB
- **Neo4j Migration**: Migrate graph data from Neo4j to SurrealDB

See [Supported MongoDB Data Types](/docs/mongodb-data-types.md) and [Supported Neo4j Data Types](/docs/neo4j-data-types.md) for more details.

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
SOURCE_URL=mongodb://user:pass@localhost:27017 \
surreal-sync sync mongo-db \
  --source-database "mydb" \
  --to-namespace "production" \
  --to-database "migrated_data"
```

### Neo4j Migration

```bash
surreal-sync sync neo4j \
  --source-uri "bolt://localhost:7687" \
  --source-username "neo4j" \
  --source-password "password" \
  --neo4j-timezone "America/New_York" \
  --to-namespace "production" \
  --to-database "graph_data"
```

By default, Neo4j local datetime and time values are assumed to be in UTC. You can specify a different timezone using the `--neo4j-timezone` option or the `NEO4J_TIMEZONE` environment variable.

## Environment Variables

You can set these environment variables instead of using command-line flags:

- `SOURCE_URI`: Source database connection string
- `SOURCE_DATABASE`: Source database name
- `SOURCE_USERNAME`: Source database username
- `SOURCE_PASSWORD`: Source database password
- `NEO4J_TIMEZONE`: Timezone for Neo4j local datetime/time values (default: UTC)
- `SURREAL_ENDPOINT`: SurrealDB endpoint (default: http://localhost:8000)
- `SURREAL_USERNAME`: SurrealDB username (default: root)
- `SURREAL_PASSWORD`: SurrealDB password (default: root)

## Command Line Options

### Source Database Options

- `--source-uri`: Connection string/URI for the source database
- `--source-database`: Name of the source database (optional for some sources)
- `--source-username`: Username for source database authentication
- `--source-password`: Password for source database authentication
- `--neo4j-timezone`: Timezone for Neo4j local datetime/time values (default: UTC). Use IANA timezone names like "America/New_York", "Europe/London", etc.

### Target SurrealDB Options

- `--to-namespace`: Target SurrealDB namespace (required)
- `--to-database`: Target SurrealDB database (required)
- `--surreal-endpoint`: SurrealDB server endpoint (default: http://localhost:8000)
- `--surreal-username`: SurrealDB username (default: root)
- `--surreal-password`: SurrealDB password (default: root)

## Development

This project is designed to work with the provided devcontainer that includes:
- SurrealDB server (localhost:8000)
- MongoDB server (localhost:27017)
- Neo4j server (localhost:7474/7687)

To use the devcontainer:

1. Open the project in VS Code/Cursor
2. Select "Reopen in Container" when prompted
3. All services will start automatically

## Roadmap

- [x] Add support for time zones assumed when converting local datetime and time from Neo4j
- [ ] Explore batching writes to SurrealDB for potentially better performance
- [ ] Add support for MongoDB `$regularExpression` to SurrealDB `regex` type conversion once SurrealDB v3 gets released
- [ ] Explore option to use BSON insted JSON Extended Format v2 for MongoDB
