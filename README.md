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

PostgreSQL sources also support a TOML config file to avoid long flag lists:

```bash
surreal-sync from postgresql-trigger full -c surreal-sync.toml
surreal-sync from postgresql incremental -c surreal-sync.toml
```

## Full Sync Strategies (PostgreSQL & MySQL)

For PostgreSQL (both the trigger and wal2json sources) and MySQL, full sync offers two strategies, selected with `--strategy`:

- **`snapshot-stream` (default)** — A DBLog-style watermark snapshot interleaved with the change stream. Tables are copied in primary-key-ordered, resumable chunks (`--chunk-size`, default 1024) while changes stream concurrently. The target converges to a **consistent image at the end position (≈ t2), then tracks live** — not a frozen t1 snapshot. It uses **bounded memory** (O(chunk_size), independent of table size) and **bounded retention** (the source change log is freed as it is consumed, rather than pinned for the whole snapshot). It also supports ad-hoc / add-table re-snapshots while streaming.
- **`bulk`** — The original monolithic `SELECT *` per table with t1/t2 checkpoints, followed by a separate `incremental` run from t1. The source must retain the entire change log from t1 until catch-up.

Use the combined `sync` command to run the snapshot and hand off to incremental in one process, and the `snapshot` command to request an ad-hoc snapshot of additional tables against a running `sync`:

```bash
# One-process snapshot-stream + incremental
surreal-sync from postgresql sync --connection-string ... --to-namespace ns --to-database db

# Ask a running `sync` to also snapshot more tables
surreal-sync from postgresql snapshot --connection-string ... --tables new_table
```

**Recommendation:** use `snapshot-stream` whenever the source supports it. Opt out with `--strategy bulk` for tables without a usable primary key, or when writing watermark rows to the source is not permitted.

> **Behavior change:** `snapshot-stream` is now the **default** for PostgreSQL and MySQL. Compared to the previous behavior it creates a `surreal_sync_signal` table on the source and requires a primary key on every selected table. Pass `--strategy bulk` to keep the previous full-sync behavior. See [docs/design.md](docs/design.md#full-sync-strategies) for details.

See source-specific guides for more details:

- **[MongoDB](docs/mongodb.md)**: Full and incremental sync using change streams
- **[MySQL](docs/mysql.md)**: Full and incremental sync using trigger-based CDC + sequence checkpoints
- **[PostgreSQL (Trigger-based)](docs/postgresql.md)**: Full and incremental sync using trigger-based CDC + sequence checkpoints
- **[PostgreSQL (wal2json)](docs/postgresql-wal2json-source.md)**: Full and incremental sync using logical replication with wal2json plugin
- **[Neo4j](docs/neo4j.md)**: Full and incremental sync using timestamp-based tracking
- **[JSONL](docs/jsonl.md)**: Bulk import from JSON Lines files
- **[Kafka](docs/kafka.md)**: Kafka consumer that subscribes to a topic, importing Kafka message payloads into SurrealDB with optional deduplication

## Development

See [devcontainer.md](docs/devcontainer.md) for development.
