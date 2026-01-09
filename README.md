# Surreal Sync

A command-line tool for migrating data from various database sources to SurrealDB with support for both full and incremental synchronization.

> Surreal Sync is currently in [active development](https://github.com/surrealdb/surreal-sync/releases) and is not yet stable. We are looking forward to any and all feedback on the tool, either via raising an issue or PR, or anywhere else in the [SurrealDB community](https://surrealdb.com/community).

## Installation

```bash
# Build from source
cargo build --release
sudo cp target/release/surreal-sync /usr/local/bin/

# Or install from releases
curl -L https://github.com/surrealdb/surreal-sync/releases/latest/download/surreal-sync-x86_64-unknown-linux-gnu.tar.gz | tar xz
sudo mv surreal-sync /usr/local/bin/
```

## Usage

```bash
surreal-sync from <SOURCE> <COMMAND> \
  --connection-string [CONNECTION STRING] \
  [SOURCE OPTIONS] \
  --surreal-endpoint [SURREAL ENDPOINT] \
  --surreal-username [SURREAL USERNAME] \
  --surreal-password [SURREAL PASSWORD] \
  --to-namespace <NS> \
  --to-database <DB>
```

See source-specific guides for more details:

- **[MongoDB](docs/mongodb.md)**: Full and incremental sync using change streams
- **[MySQL](docs/mysql.md)**: Full and incremental sync using trigger-based CDC + sequence checkpoints
- **[PostgreSQL (Trigger-based)](docs/postgresql.md)**: Full and incremental sync using trigger-based CDC + sequence checkpoints
- **[Neo4j](docs/neo4j.md)**: Full and incremental sync using timestamp-based tracking
- **[JSONL](docs/jsonl.md)**: Bulk import from JSON Lines files
- **[Kafka](docs/kafka.md)**: Kafka consumer that subscribes to a topic, importing Kafka message payloads into SurrealDB with optional deduplication

## Development

See [devcontainer.md](docs/devcontainer.md) for development.
