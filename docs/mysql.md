# Surreal-Sync for MySQL (trigger-based)

> **Start with binlog CDC.** For most MySQL/MariaDB migrations, use [`surreal-sync from mysql-binlog sync`](mysql-binlog.md) — no triggers or audit tables, GTID-safe resume, and a long-lived follower.

This guide covers **trigger-based** sync (`surreal-sync from mysql`) when binlog is not an option: no replication privileges, managed MySQL without binlog access, cannot enable ROW-format binlog, or policy constraints on the source. MariaDB uses the same sub-command; see [Surreal-Sync for MariaDB](mariadb.md).

Optional transforms: pass `--transforms-config` with a TOML file. Omit the flag to leave rows unchanged. Details: [How sync works](sync-pipeline.md).

> **Other strategies:** For the legacy sequential-snapshot workflow (inconsistent monolithic snapshot plus a separate incremental replay from t1), see [MySQL Legacy Full Sync](mysql/legacy.md).

## How It Works

`surreal-sync from mysql` supports `full`, `incremental`, a combined `sync`, and an ad-hoc `snapshot` command.

Per-table triggers write to `surreal_sync_changes` for resumable, sequence-based checkpointing. The default **interleaved-snapshot** workflow copies tables in resumable chunks while consuming the trigger change stream; watermark reconciliation aligns each chunk with live changes (the log event wins), converging to a **consistent image at the end position**. Use `sync` for snapshot plus incremental handoff in one process.

See [Full Sync Strategies](design/full-sync-strategies.md) for the consistency guarantee and strategy comparison.

## Prerequisites

You need appropriate permissions to create triggers and tables in the MySQL database, because sync relies on database triggers to capture changes.

Every table you select for sync must have a usable primary key. `surreal-sync` also writes a small `surreal_sync_signal` table on the source for watermark signalling.

## Combined sync

The `sync` command runs the watermark snapshot and continues incremental sync from the handed-off end position in one process — no separate incremental replay pass is needed to reach consistency.

```bash
export CONNECTION_STRING="mysql://root:root@mysql:3306/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

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
- `--tables`: comma-separated tables (empty means all tables).

## Full Sync

Use `full` when you want a one-time snapshot without immediately continuing incremental sync — for example, to bulk-load data and run `incremental` on a schedule later. Interleaved-snapshot is the default (`--chunk-size`, default 1024).

```bash
export CONNECTION_STRING="mysql://root:root@mysql:3306/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

surreal-sync from mysql full \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

Provide a checkpoint store (`--checkpoint-dir` or `--checkpoints-surreal-table`) so `incremental` can resume from the end position.

Example log output:

```
INFO surreal_sync::mysql: Emitted full sync start checkpoint (t1): mysql:sequence:0
INFO surreal_sync::mysql: Emitted full sync end checkpoint (t2): mysql:sequence:123
```

The `(t1)` / `(t2)` labels are log names for the bracketing positions. With interleaved-snapshot, **both checkpoints record the same consistent end position** — start `incremental` from either file.

## Incremental Sync

Prefer [`sync`](#combined-sync) when starting a new migration. If you already ran `full`, use `incremental` to continue live tracking from the end position (not a replay pass to fix inconsistency):

```bash
NUM=$(cat ./.surreal-sync-checkpoints/checkpoint_full_sync_end_*.json | jq -r '.checkpoint.MySQL.sequence')
CHECKPOINT="mysql:sequence:$NUM"

surreal-sync from mysql incremental \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --incremental-from "$CHECKPOINT" \
  --timeout 1m
```

`--incremental-from` is the end-position checkpoint; `--timeout` controls how long the run continues (useful for batched or scheduled runs).

While incremental sync is running, your application can continue writing to MySQL without downtime, as long as the source can serve the workload.

## Ad-hoc Snapshots (Signalling)

While a `sync` is streaming, you can snapshot additional tables on the fly. The `snapshot` command inserts an `execute-snapshot` signal row into `surreal_sync_signal`; the running `sync` picks it up and snapshots the requested tables while streaming continues:

```bash
surreal-sync from mysql snapshot \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --tables "new_table,another_table"
```

- `--tables` (required): comma-separated tables to snapshot.

## Troubleshooting

### Missing Triggers

If incremental sync is not capturing changes, ensure triggers were created during `full` or `sync`:

```sql
SHOW TRIGGERS LIKE 'surreal_sync_%';
```

### Audit Table Growth

Interleaved-snapshot prunes consumed rows from `surreal_sync_changes` as the stream is applied, keeping retention bounded during snapshot and streaming. A long-lived standalone `incremental` process can still let the table grow — monitor size and prune synced rows if needed.

## Data Type Support

See [MySQL Data Types](mysql-data-types.md) for data type mapping information.
