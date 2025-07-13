# surreal-sync

`surreal-sync` is a command-line tool complementes SurrealDB's `surreal` command for migrating data from various databases to SurrealDB.

ToC:

- [Sources](#sources)
- [Installation](#installation)
- [Usage](#usage)
- [Development](#development)
- [Roadmap](#roadmap)

## Sources

- **MongoDB**: Migrate documents from MongoDB to SurrealDB ([SupportedData Types](/docs/mongodb-data-types.md))
- **Neo4j**: Migrate nodes and relationships from Neo4j to SurrealDB ([Supported Data Types](/docs/neo4j-data-types.md))
- **JSONL**: Migrate data from JSON Lines files to SurrealDB ([Usage](/docs/jsonl.md))

See  and  for more details.

## Installation

- [Pre-built binaries](#install-pre-built-binaries)
  - [Latest Release](#latest-release)
  - [Install Script](#install-script)
- [Build from source](#build-from-source)

### Install Pre-built Binaries

#### Latest Release

Install the latest stable release for your platform:

```bash
# Linux (x86_64)
curl -L https://github.com/surrealdb/surreal-sync/releases/latest/download/surreal-sync-x86_64-unknown-linux-gnu.tar.gz | tar xz
sudo mv surreal-sync /usr/local/bin/

# macOS (Apple Silicon)
curl -L https://github.com/surrealdb/surreal-sync/releases/latest/download/surreal-sync-aarch64-apple-darwin.tar.gz | tar xz
sudo mv surreal-sync /usr/local/bin/

# Linux (ARM64)
curl -L https://github.com/surrealdb/surreal-sync/releases/latest/download/surreal-sync-aarch64-unknown-linux-gnu.tar.gz | tar xz
sudo mv surreal-sync /usr/local/bin/
```

#### Install Script

You can also use this one-liner to automatically detect and install the appropriate binary:

```bash
# Latest stable release
curl -sSL https://raw.githubusercontent.com/surrealdb/surreal-sync/main/scripts/install.sh | bash

# Specific release
curl -sSL https://raw.githubusercontent.com/surrealdb/surreal-sync/main/scripts/install.sh | bash -s -- --version v0.1.0
```

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

See [devcontainer.md](docs/devcontainer.md).

## Roadmap

- [x] Add support for time zones assumed when converting local datetime and time from Neo4j
- [x] Add support for MongoDB `$regularExpression` to SurrealDB `regex` type conversion once SurrealDB v2.3 gets released
- [x] Explore option to use BSON insted JSON Extended Format v2 for MongoDB
- [ ] Explore batching writes to SurrealDB for potentially better performance
- [ ] More paralleism on the surreal-sync side for better performance
