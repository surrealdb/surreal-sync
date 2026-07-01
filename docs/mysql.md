# Surreal-Sync for MySQL

`surreal-sync from mysql` is a sub-command to `surreal-sync` that exports MySQL tables to SurrealDB tables.

It supports inconsistent full syncs and consistent incremental syncs, and together provides ability to reproduce consistent snapshots from the source MySQL tables onto the target SurrealDB tables.

## How It Works

`surreal-sync from mysql` supports `full`, `incremental`, a combined `sync`, and an ad-hoc `snapshot` command.

Change capture is trigger-based: an audit table (`surreal_sync_changes`) populated by per-table triggers provides resumable, sequence-based checkpointing. A potential alternative would be to read the binlog; `surreal-sync` may add a binlog-based backend in the future.

Full sync offers two strategies, selected with `--strategy`:

- **`interleaved-snapshot` (default)** — A DBLog-style watermark snapshot copied concurrently/interleaved with the trigger change stream. Tables are copied in primary-key-ordered, resumable chunks (`--chunk-size`, default 1024) while the audit-table stream is consumed concurrently; watermark windows reconcile snapshot reads against live changes (the log event wins). The result converges to a consistent image at the end position (≈ t2) and then keeps tracking live — it is **not** a frozen t1 snapshot. It uses bounded memory (O(chunk_size)) and prunes consumed rows from `surreal_sync_changes` as it goes, so the audit table does not grow for the whole snapshot (bounded retention). It requires a primary key on every selected table and writes a small `surreal_sync_signal` table to the source.
- **`sequential-snapshot`** — The original strategy: a monolithic `SELECT *` per table bracketed by t1/t2 checkpoints, then a separate replay of the [t1,t2] change log via an `incremental` run from t1 to make the target consistent at t2 (the audit log is pinned for the whole snapshot: unbounded retention). Use this when a table has no usable primary key, or when writing the signal table to the source is not permitted.

See [Full Sync Strategies](design/full-sync-strategies.md) for the side-by-side comparison and the consistency guarantee.

**Behavior change for existing users:** `interleaved-snapshot` is now the default. Compared to the previous behavior, it creates a `surreal_sync_signal` table on the source and requires every selected table to have a primary key. Pass `--strategy sequential-snapshot` to keep the previous monolithic full-sync behavior. (The previous strategy values `snapshot-stream` and `bulk` have been renamed to `interleaved-snapshot` and `sequential-snapshot`.)

## Prerequisites

You need appropriate permissions to create triggers and tables in the MySQL database for incremental syncs, because the incremental sync relies on database triggers to capture changes.

## Full Sync

You can start a full sync via the `surreal-sync from mysql full` command like below:

```bash
export CONNECTION_STRING="mysql://root:root@mysql:3306/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

# Full sync (interleaved-snapshot by default; automatically sets up triggers for incremental sync)
surreal-sync from mysql full \
  # Source = MySQL settings
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  # Target = SurrealDB settings
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

By default this runs the `interleaved-snapshot` strategy (tune the chunk with `--chunk-size <N>`, default 1024). Add `--strategy sequential-snapshot` to use the original monolithic strategy instead.

Checkpoint emission is enabled by providing a checkpoint store — either `--checkpoint-dir <DIR>` (filesystem) or `--checkpoints-surreal-table <TABLE>` (stored in SurrealDB). It is necessary when you want to start incremental syncs after the full sync, so the command knows where to continue the sync.

A `surreal-sync` with a checkpoint store configured will produce logs like those below:

```
INFO surreal_sync::mysql: Emitted full sync start checkpoint (t1): mysql:sequence:0
INFO surreal_sync::mysql: Emitted full sync end checkpoint (t2): mysql:sequence:123
```

With the **sequential-snapshot** strategy you must specify t1 (not t2) as the starting point for incremental sync. This corresponds to the fact that sequential-snapshot full sync may produce an inconsistent snapshot of the MySQL tables, depending on the MySQL isolation guarantee you chose. By reading and applying changes made since t1 instead of t2, when the incremental sync writes all the changes up to t2, the target SurrealDB tables can be viewed as consistent with the source tables at t2.

With the **interleaved-snapshot** strategy the start and end checkpoints both record the consistent end position (≈ t2), because the snapshot is already reconciled against the live stream via watermarks. Start incremental sync from either, or prefer the combined [`sync`](#combined-interleaved-snapshot-sync) command, which performs the handoff in one process.

## Incremental Sync

You must run full sync first to generate the checkpoint and set up the necessary triggers - incremental sync needs this infrastructure.

```bash
# Find the checkpoint file
ls ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json

# Extract sequence ID for incremental sync
NUM=$(cat ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json | jq -r '.checkpoint.MySQL.sequence')
CHECKPOINT="mysql:sequence:$NUM"
```

With the proper checkpoint, an incremental sync can be triggered via `surreal-sync from mysql incremental`:

```bash
surreal-sync from mysql incremental \
  # Source = MySQL settings
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  # Target = SurrealDB settings
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  # Using the checkpoint from full sync
  --incremental-from "$CHECKPOINT" \
  --timeout 1m
```

The `incremental-from` specifies the t1 checkpoint explained previously, and `timeout` specifies when the incremental sync should stop.

The `timeout` is necessary when you want to run incremental sync in batches, or run it periodically rather than in a persistent process. Depending on how you want to keep incremental sync running, you should put surreal-sync under a process manager or under a container orchestration system that handles automatic retries, with or without the specific `timeout`.

While the incremental sync is running, your application can continue writing to MySQL.

Doing incremental sync does not necessarily incur downtime to your application, as long as the source MySQL database can serve the entire workloads.

## Combined interleaved snapshot sync

With the `interleaved-snapshot` strategy you usually don't need two separate runs. The `sync` command runs the watermark snapshot and then continues incremental sync from the handed-off end position, in a single process:

```bash
surreal-sync from mysql sync \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
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
surreal-sync from mysql snapshot \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --tables "new_table,another_table"
```

- `--tables` (required): comma-separated tables to snapshot.

## Troubleshooting

### Missing Triggers

If incremental sync is not capturing changes, ensure triggers are created during full sync:

```sql
-- Check for triggers
SHOW TRIGGERS LIKE 'surreal_sync_%';
```

### Audit Table Cleanup

The `interleaved-snapshot` strategy prunes consumed rows from `surreal_sync_changes` automatically as the stream is applied, keeping it bounded. For the `sequential-snapshot` strategy (or long-running standalone `incremental` runs) the audit table may grow over time, so consider periodic cleanup:

```sql
-- Check audit table size
SELECT COUNT(*) FROM surreal_sync_changes;

-- Clean up old records (after they've been synced)
DELETE FROM surreal_sync_changes WHERE changed_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
```

## Data Type Support

See [MySQL Data Types](mysql-data-types.md) for data type mapping information.
