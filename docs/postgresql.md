# Surreal-Sync for PostgreSQL (Trigger-Based)

`surreal-sync from postgresql-trigger` is a sub-command to `surreal-sync` that exports PostgreSQL tables to SurrealDB tables.

Optional transforms: pass `--transforms-config` with a TOML file. Omit the flag to leave rows unchanged. Details: [How sync works](sync-pipeline.md).

The default **interleaved-snapshot** workflow copies tables in resumable, primary-key-ordered chunks while continuously consuming the trigger-based change stream. Watermark reconciliation aligns each chunk with live changes (the log event wins), so the target converges to a **consistent image at the end position** and can keep tracking live — it is not an inconsistent monolithic dump. Use the combined `sync` command for a single-process snapshot plus incremental handoff.

> **Other strategies:** For the legacy sequential-snapshot workflow (inconsistent monolithic snapshot plus a separate incremental replay from t1), see [PostgreSQL Legacy Full Sync (Trigger)](postgresql/legacy.md).

## How It Works

`surreal-sync from postgresql-trigger` supports `full`, `incremental`, a combined `sync`, and an ad-hoc `snapshot` command.

Change capture is trigger-based: an audit table (`surreal_sync_changes`) populated by per-table triggers provides resumable, sequence-based checkpointing. A potential alternative is logical replication with wal2json (see the [wal2json source](postgresql-wal2json-source.md)).

See [Full Sync Strategies](design/full-sync-strategies.md) for the consistency guarantee and strategy comparison.

## Prerequisites

You need appropriate permissions to create triggers and tables in the PostgreSQL database, because sync relies on database triggers to capture changes.

Every table you select for sync must have a usable primary key (single- or multi-column). Composite PKs become SurrealDB array record IDs (`table:[k1, k2]`) by default; join-table relations stay colon-flattened. Optional `flatten_id` / custom workers: [How sync works — Record IDs](sync-pipeline.md#record-ids-and-composite-primary-keys). `surreal-sync` also writes a small `surreal_sync_signal` table on the source for watermark signalling.

The database name must be included in the connection string (for example, `postgresql://user:pass@host:5432/myapp`).

## Combined sync (recommended)

The `sync` command is the standard entry point. It runs the watermark snapshot and continues incremental sync from the handed-off end position in one process — no separate incremental replay pass is needed to reach consistency.

```bash
export CONNECTION_STRING="postgresql://postgres:postgres@postgresql:5432/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

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
- `--tables`: comma-separated tables (empty means all tables).

## Full Sync

Use `full` when you want a one-time snapshot without immediately continuing incremental sync — for example, to bulk-load data and run `incremental` on a schedule later. Interleaved-snapshot is the default (`--chunk-size`, default 1024).

```bash
export CONNECTION_STRING="postgresql://postgres:postgres@postgresql:5432/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

surreal-sync from postgresql-trigger full \
  --connection-string "$CONNECTION_STRING" \
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
INFO surreal_sync::postgresql_trigger_source: Emitted full sync start checkpoint (t1): 0
INFO surreal_sync::postgresql_trigger_source: Emitted full sync end checkpoint (t2): 123
```

The `(t1)` / `(t2)` labels are log names for the bracketing positions. With interleaved-snapshot, **both checkpoints record the same consistent end position** — start `incremental` from either file. The checkpoint value is the sequence ID number (for example, `"123"`).

## Incremental Sync

Prefer [`sync`](#combined-sync-recommended) when starting a new migration. If you already ran `full`, use `incremental` to continue live tracking from the end position (not a replay pass to fix inconsistency):

```bash
CHECKPOINT=$(cat ./.surreal-sync-checkpoints/checkpoint_full_sync_end_*.json | jq -r '.checkpoint_data | fromjson | .sequence_id')

surreal-sync from postgresql-trigger incremental \
  --connection-string "$CONNECTION_STRING" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --incremental-from "$CHECKPOINT" \
  --timeout 3600
```

`--incremental-from` is the end-position checkpoint; `--timeout` controls how long the run continues (useful for batched or scheduled runs).

While incremental sync is running, your application can continue writing to PostgreSQL without downtime, as long as the source can serve the workload.

## Ad-hoc Snapshots (Signalling)

While a `sync` is streaming, you can snapshot additional tables on the fly. The `snapshot` command inserts an `execute-snapshot` signal row into `surreal_sync_signal`; the running `sync` picks it up and snapshots the requested tables while streaming continues:

```bash
surreal-sync from postgresql-trigger snapshot \
  --connection-string "$CONNECTION_STRING" \
  --tables "new_table,another_table"
```

- `--tables` (required): comma-separated tables to snapshot.

## Troubleshooting

### Missing Triggers

If incremental sync is not capturing changes, ensure triggers were created during `full` or `sync`:

```sql
SELECT trigger_name, event_object_table
FROM information_schema.triggers
WHERE trigger_name LIKE 'surreal_sync_%';
```

### Permissions Issues

Ensure the user has sufficient permissions:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO sync_user;
GRANT CREATE ON SCHEMA public TO sync_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO sync_user;
```

### Audit Table Growth

Interleaved-snapshot prunes consumed rows from `surreal_sync_changes` as the stream is applied, keeping retention bounded during snapshot and streaming. A long-lived standalone `incremental` process can still let the table grow — monitor size and prune synced rows if needed.

## Data Type Support

See [PostgreSQL Data Types](postgresql-data-types.md) for data type mapping information.
