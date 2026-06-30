# Surreal-Sync for PostgreSQL (Trigger-Based)

`surreal-sync from postgresql-trigger` as a sub-command to `surreal-sync` that exports PostgreSQL tables to SurrealDB tables.

It supports inconsistent full syncs and consistent incremental syncs, and together provides ability to reproduce consistent snapshots from the source PostgreSQL tables onto the target SurrealDB tables.

## How It Works

`surreal-sync from postgresql-trigger` supports `full`, `incremental`, a combined `sync`, and an ad-hoc `snapshot` command.

Change capture is trigger-based: an audit table (`surreal_sync_changes`) populated by per-table triggers provides resumable, sequence-based checkpointing. A potential alternative to this approach is to read and parse WAL and replay changes recorded in the WAL (see the [wal2json source](postgresql-wal2json-source.md)). We opted for the trigger-based approach believing it's more reliable from the application perspective because you don't need to rely on less mature WAL reading/parsing libraries.

Full sync offers two strategies, selected with `--strategy`:

- **`snapshot-stream` (default)** — A DBLog-style watermark snapshot interleaved with the trigger change stream. Tables are copied in primary-key-ordered, resumable chunks (`--chunk-size`) while the audit-table stream is consumed concurrently; watermark windows reconcile snapshot reads against live changes (the log event wins). The result converges to a consistent image at the end position (≈ t2) and then keeps tracking live — it is **not** a frozen t1 snapshot. It uses bounded memory (O(chunk_size)) and prunes consumed rows from `surreal_sync_changes` as it goes, so the audit table does not grow for the whole snapshot. It requires a primary key on every selected table and writes a small `surreal_sync_signal` table to the source.
- **`bulk`** — The original strategy: a monolithic `SELECT *` per table bracketed by t1/t2 checkpoints. The dump can mix row versions between t1 and t2, so a separate `incremental` run from t1 is required to make the target consistent at t2. Use this when a table has no usable primary key, or when writing the signal table to the source is not permitted.

See [Full Sync Strategies](design.md#full-sync-strategies) for the side-by-side comparison and the consistency guarantee.

**Behavior change for existing users:** `snapshot-stream` is now the default. Compared to the previous behavior, it creates a `surreal_sync_signal` table on the source and requires every selected table to have a primary key. Pass `--strategy bulk` to keep the previous monolithic full-sync behavior.

## Prerequisites

You need appropriate permissions to create triggers and tables in the PostgreSQL database for incremental syncs.
That's because the incremental sync relies on database triggers to capture changes.

## Full Sync

You can start a full sync via the `surreal-sync from postgresql-trigger full` command like below:

```bash
# Note: Database name must be included in the connection string (e.g., /myapp at the end)
export CONNECTION_STRING="postgresql://postgres:postgres@postgresql:5432/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

# Full sync (snapshot-stream by default; automatically sets up triggers for incremental sync)
surreal-sync from postgresql-trigger full \
  # Source = PostgreSQL settings
  --connection-string "$CONNECTION_STRING" \
  # Target = SurrealDB settings
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

By default this runs the `snapshot-stream` strategy. The emitted checkpoint is the consistent end position; an `incremental` run started from it continues live tracking. You can tune the keyset chunk with `--chunk-size <N>` (default 1024).

To use the original monolithic strategy instead, add `--strategy bulk`:

```bash
surreal-sync from postgresql-trigger full \
  --connection-string "$CONNECTION_STRING" \
  --strategy bulk \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

For the bulk strategy the emitted checkpoints follow the t1/t2 model described below.

Checkpoint emission is enabled by providing a checkpoint store — either `--checkpoint-dir <DIR>` (filesystem) or `--checkpoints-surreal-table <TABLE>` (stored in SurrealDB). It is necessary when you want to start incremental syncs after the full sync, so the command knows where to continue the sync.

A `surreal-sync` with a checkpoint store configured will produce logs like the below:

```
INFO surreal_sync::postgresql_trigger_source: Emitted full sync start checkpoint (t1): 0
INFO surreal_sync::postgresql_trigger_source: Emitted full sync end checkpoint (t2): 123
```

The checkpoint format is just the sequence ID number (e.g., `"123"`), not a prefixed string.

With the **bulk** strategy you must specify t1 (not t2) as the starting point for incremental sync. This corresponds to the fact that bulk full sync produces an inconsistent snapshot of the PostgreSQL tables, due to the nature of PostgreSQL's isolation guarantee. By reading and applying changes made since t1 instead of t2, when the incremental sync writes all the changes up to t2, the target SurrealDB tables can be viewed as consistent with the source tables at t2.

With the **snapshot-stream** strategy the start and end checkpoints both record the consistent end position (≈ t2), because the snapshot is already reconciled against the live stream via watermarks. Start incremental sync from either; it continues live tracking from that consistent point. Prefer the combined [`sync`](#combined-snapshotstream-sync) command, which performs this handoff in one process.

## Incremental Sync

You must run full sync first to generate the checkpoint and set up the necessary triggers - incremental sync needs this infrastructure.

```bash
# Find the checkpoint file
ls ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json

# Extract sequence ID for incremental sync
# The checkpoint file structure is:
# {
#   "database_type": "postgresql",
#   "checkpoint": { "sequence_id": 123, "timestamp": "..." },
#   "phase": "FullSyncStart",
#   "created_at": "..."
# }
CHECKPOINT=$(cat ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json | jq -r '.checkpoint.sequence_id')
```

With the proper checkpoint, an incremental sync can be triggered via `surreal-sync from postgresql-trigger incremental`:

```bash
surreal-sync from postgresql-trigger incremental \
  # Source = PostgreSQL settings
  --connection-string "$CONNECTION_STRING" \
  # Target = SurrealDB settings
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  # Using the checkpoint from full sync (just the sequence ID number)
  --incremental-from "$CHECKPOINT" \
  --timeout 60
```

## Combined snapshot+stream sync

With the `snapshot-stream` strategy you usually don't need two separate runs. The `sync` command runs the watermark snapshot and then continues incremental sync from the handed-off end position, in a single process:

```bash
surreal-sync from postgresql-trigger sync \
  --connection-string "$CONNECTION_STRING" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --tables "users,orders" \
  --chunk-size 1024 \
  --timeout 3600
```

- `--chunk-size <N>`: rows read per keyset chunk during the snapshot phase (default 1024).
- `--timeout <SECONDS>`: how long the incremental phase runs after the snapshot completes (default 3600).
- `--tables`: comma-separated tables (empty means all tables). Every selected table must have a primary key.

## Ad-hoc Snapshots (Signalling)

While a `sync` is streaming, you can ask it to snapshot additional tables on the fly. The `snapshot` command inserts an `execute-snapshot` signal row into the `surreal_sync_signal` table on the source; the running `sync` picks it up and snapshots the requested tables while streaming continues (resumable via the snapshot checkpoint):

```bash
surreal-sync from postgresql-trigger snapshot \
  --connection-string "$CONNECTION_STRING" \
  --tables "new_table,another_table"
```

- `--tables` (required): comma-separated tables to snapshot.

### Config File

Instead of passing many flags, you can use a TOML config file with `-c` / `--config-file`:

```bash
surreal-sync from postgresql-trigger full -c surreal-sync.toml
surreal-sync from postgresql-trigger incremental -c surreal-sync.toml
```

Example `surreal-sync.toml`:

```toml
[source.postgresql]
connection_string = "postgresql://postgres:postgres@postgresql:5432/myapp"
tables = ["users", "orders"]
schema_file = "schema.yaml"

# Full sync checkpoint settings
checkpoint_dir = ".surreal-sync-checkpoints"
# Or use SurrealDB for checkpoints:
# checkpoints_surreal_table = "surreal_sync_checkpoints"

# Incremental sync settings (ignored by full sync)
# incremental_from = "123"
# timeout = 3600

[sink.surrealdb]
endpoint = "ws://localhost:8000"
username = "root"
password = "root"
namespace = "production"
database = "migrated_data"
```

CLI flags take precedence over config file values when both are provided. The same config file can be shared between `full` and `incremental` subcommands -- irrelevant fields are ignored.

### Command-Line Options

#### Full Sync Options

- `--connection-string`: PostgreSQL connection string (must include database, e.g., `postgresql://user:pass@host:5432/mydb`)
- `--tables`: Comma-separated tables to sync (empty means all tables)
- `--to-namespace`: Target SurrealDB namespace
- `--to-database`: Target SurrealDB database
- `--strategy`: Full-sync strategy, `snapshot-stream` (default) or `bulk`
- `--chunk-size`: Rows read per keyset chunk for the `snapshot-stream` strategy (default: 1024)
- `--checkpoint-dir`: Directory to write checkpoint files (mutually exclusive with `--checkpoints-surreal-table`)
- `--checkpoints-surreal-table`: SurrealDB table to store checkpoints (mutually exclusive with `--checkpoint-dir`)
- `--schema-file`: Optional schema file for type-aware conversion
- `-c` / `--config-file`: Load defaults from a TOML config file
- Plus standard SurrealDB options: `--surreal-endpoint`, `--surreal-username`, `--surreal-password`, `--batch-size`, `--dry-run`

#### Combined `sync` Options

- `--connection-string`, `--tables`, `--to-namespace`, `--to-database`, and standard SurrealDB options
- `--chunk-size`: Rows read per keyset chunk during the snapshot phase (default: 1024)
- `--timeout`: Seconds to run the incremental phase after the snapshot (default: 3600)
- `--schema-file`: Optional schema file for type-aware conversion

#### Ad-hoc `snapshot` Options

- `--connection-string`: PostgreSQL connection string (must include database)
- `--tables` (required): Comma-separated tables to snapshot

#### Incremental Sync Options

- `--connection-string`: PostgreSQL connection string (must include database)
- `--to-namespace`: Target SurrealDB namespace
- `--to-database`: Target SurrealDB database
- `--incremental-from`: Start checkpoint (sequence ID number, e.g., `"123"`)
- `--incremental-to`: Optional stop checkpoint (sequence ID number)
- `--timeout`: Maximum time to run in seconds (default: 3600 = 1 hour)
- `--schema-file`: Optional schema file for type-aware conversion
- Plus standard SurrealDB options

### Understanding Timeouts

The `--incremental-from` specifies the t1 checkpoint explained previously, and `--timeout` specifies when the incremental sync should stop.

The `--timeout` parameter is in **seconds** (default: 3600 = 1 hour). It's necessary when you want to run incremental sync in batches, or run it periodically rather than in a persistent process. Depending on how you want to keep incremental sync running, you should put surreal-sync under a process manager or under a container orchestration system that handles automatic retries, with or without the specific `timeout`.

While the incremental sync is running, your application can continue writing to PostgreSQL.

Doing incremental sync does not necessarily incur downtime to your application, as long as the source PostgreSQL database can serve the entire workloads.

## Troubleshooting

### Missing Triggers

If incremental sync is not capturing changes, ensure triggers were created during full sync:

```sql
-- Check for triggers
SELECT trigger_name, event_object_table
FROM information_schema.triggers
WHERE trigger_name LIKE 'surreal_sync_%';
```

### Permissions Issues

Ensure the user has sufficient permissions:

```sql
-- Grant necessary permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO sync_user;
GRANT CREATE ON SCHEMA public TO sync_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO sync_user;
```

### Audit Table Cleanup

The `snapshot-stream` strategy prunes consumed rows from `surreal_sync_changes` automatically as the stream is applied, keeping it bounded. For the `bulk` strategy (or long-running standalone `incremental` runs) the audit table may grow over time, so consider periodic cleanup:

```sql
-- Check audit table size
SELECT COUNT(*) FROM surreal_sync_changes;

-- Clean up old records (after they've been synced)
DELETE FROM surreal_sync_changes WHERE changed_at < NOW() - INTERVAL '30 days';
```

## Data Type Support

See [PostgreSQL Data Types](postgresql-data-types.md) for data type mapping information.
