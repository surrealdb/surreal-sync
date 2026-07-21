# PostgreSQL wal2json Source

`surreal-sync from postgresql` exports PostgreSQL tables to SurrealDB using logical replication with the [wal2json](https://github.com/eulerto/wal2json) output plugin.

Apply into SurrealDB goes through the [`sync-transform`](transforms.md) framework (`--transforms-config` optional; omit for identity).

The default **interleaved-snapshot** workflow copies tables in resumable, primary-key-ordered chunks while continuously consuming the WAL change stream. Watermark reconciliation aligns each chunk with live changes (the log event wins), so the target converges to a **consistent image at the end LSN** and can keep tracking live — it is not an inconsistent monolithic dump. The replication slot advances as changes are consumed, keeping WAL retention bounded. Use the combined `sync` command for a single-process snapshot plus incremental handoff.

> **Other strategies:** For the legacy sequential-snapshot workflow (inconsistent monolithic snapshot plus a separate incremental replay from t1), see [PostgreSQL wal2json Legacy Full Sync](postgresql-wal2json/legacy.md).

## How It Works

`surreal-sync from postgresql` supports `full`, `incremental`, a combined `sync`, and an ad-hoc `snapshot` command.

Changes are read via PostgreSQL logical replication functions (`pg_logical_slot_peek_changes`, `pg_replication_slot_advance`) over a regular SQL connection. `surreal-sync` creates the replication slot if it does not exist. Processing uses a peek-process-advance pattern with **at-least-once delivery** — ensure your target handling is idempotent.

See [Full Sync Strategies](design/full-sync-strategies.md) for the consistency guarantee and strategy comparison.

## Prerequisites

- PostgreSQL 10 or later (tested with PostgreSQL 16)
- wal2json extension 2.0 or later
- `wal_level = logical` in PostgreSQL configuration
- A database user with permission to create and use logical replication slots
- Every table you select for sync must have a usable primary key
- `surreal-sync` writes a small `surreal_sync_signal` table on the source for watermark signalling (captured by the slot)

Verify wal2json is available:

```sql
SELECT * FROM pg_available_extensions WHERE name = 'wal2json';
```

## Combined sync (recommended)

The `sync` command is the standard entry point. It runs the watermark snapshot and continues incremental sync from the handed-off end LSN in one process — no separate incremental replay pass is needed to reach consistency.

```bash
export CONNECTION_STRING="postgresql://user:password@host:5432/database"
export SURREAL_ENDPOINT="ws://localhost:8000"

surreal-sync from postgresql sync \
  --connection-string "$CONNECTION_STRING" \
  --slot "surreal_sync_slot" \
  --schema "public" \
  --tables "users,orders,products" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "myapp" \
  --chunk-size 1024 \
  --timeout 3600
```

- `--slot`: replication slot name (default `surreal_sync_slot`; created automatically if missing)
- `--chunk-size <N>`: rows read per keyset chunk during the snapshot phase (default 1024)
- `--timeout <SECONDS>`: how long the incremental phase runs after the snapshot completes (default 3600)
- `--tables`: comma-separated tables (empty means all user tables in the schema)

## Full Sync

Use `full` when you want a one-time snapshot without immediately continuing incremental sync. Interleaved-snapshot is the default (`--chunk-size`, default 1024).

```bash
surreal-sync from postgresql full \
  --connection-string "$CONNECTION_STRING" \
  --slot "surreal_sync_slot" \
  --schema "public" \
  --tables "users,orders,products" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "myapp" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

Provide a checkpoint store (`--checkpoint-dir` or `--checkpoints-surreal-table`) so `incremental` can resume from the end position.

With interleaved-snapshot, **both start and end checkpoints record the same consistent end LSN** — start `incremental` from either file. LSNs use the `segment/offset` format (for example, `0/1949850`).

## Incremental Sync

Prefer [`sync`](#combined-sync-recommended) when starting a new migration. If you already ran `full`, use `incremental` to continue live tracking from the end position (not a replay pass to fix inconsistency):

```bash
LSN=$(cat ./.surreal-sync-checkpoints/checkpoint_full_sync_end_*.json | jq -r '.checkpoint_data | fromjson | .lsn')

surreal-sync from postgresql incremental \
  --connection-string "$CONNECTION_STRING" \
  --slot "surreal_sync_slot" \
  --schema "public" \
  --incremental-from "$LSN" \
  --timeout 3600 \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "myapp"
```

`--incremental-from` is the end-position LSN; `--timeout` controls how long the run continues (useful for batched or scheduled runs). Optional `--incremental-to` stops at a specific LSN.

While incremental sync is running, your application can continue writing to PostgreSQL without downtime, as long as the source can serve the workload.

## Ad-hoc Snapshots (Signalling)

While a `sync` is streaming, you can snapshot additional tables on the fly. The `snapshot` command inserts an `execute-snapshot` signal row into `surreal_sync_signal`; the running `sync` picks it up and snapshots the requested tables while streaming continues:

```bash
surreal-sync from postgresql snapshot \
  --connection-string "$CONNECTION_STRING" \
  --slot "surreal_sync_slot" \
  --schema "public" \
  --tables "new_table,another_table"
```

- `--tables` (required): comma-separated tables to snapshot.

## Troubleshooting

### Replication Slot Lag

Interleaved-snapshot advances the slot as changes are consumed, keeping WAL retention bounded during snapshot and streaming. Monitor lag on long-running standalone `incremental` processes:

```sql
SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name = 'surreal_sync_slot';
```

Delete unused slots when migration is complete:

```sql
SELECT pg_drop_replication_slot('surreal_sync_slot');
```

### Timeout During Incremental Sync

Incremental sync runs for the specified `--timeout` then exits. Increase the value for longer runs, or use a scheduler for periodic sync. Avoid wrapping `surreal-sync` in a shell loop — signal handling becomes difficult.

### No New Changes Available

When there are no new committed transactions, incremental sync may appear idle. Long-running uncommitted transactions remain invisible until they commit — this is normal PostgreSQL logical decoding behavior, not an error.

## Data Type Support

See [PostgreSQL Data Types](postgresql-data-types.md) for data type mapping information. wal2json-specific parsing details and limitations (for example, array parsing) are documented in the [source crate README](../crates/postgresql-wal2json-source/README.md).

## References

- [wal2json GitHub Repository](https://github.com/eulerto/wal2json)
- [PostgreSQL Logical Replication Documentation](https://www.postgresql.org/docs/current/logical-replication.html)
- Implementation: [crates/postgresql-wal2json-source/](../crates/postgresql-wal2json-source/)
