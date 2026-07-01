# PostgreSQL wal2json — Sequential Snapshot (Legacy)

This guide covers the **sequential-snapshot** full-sync strategy for the PostgreSQL wal2json source. It is the original two-phase workflow: a monolithic `SELECT *` per table produces an **inconsistent** snapshot (row versions can mix anywhere between t1 and t2), and a **separate** incremental run from t1 replays the [t1,t2] WAL to make the target consistent at t2.

For the standard interleaved-snapshot workflow, see [PostgreSQL wal2json Source](../postgresql-wal2json-source.md). For the design rationale, see [Full Sync Strategies](../design/full-sync-strategies.md).

## When to use sequential-snapshot

Use `--strategy sequential-snapshot` when:

- A selected table has **no usable primary key** (interleaved-snapshot requires one on every selected table).
- Writing the **`surreal_sync_signal` watermark/signal table** to the source is not permitted.

**Behavior change for existing users:** `interleaved-snapshot` is now the default. Pass `--strategy sequential-snapshot` to keep the previous monolithic full-sync behavior. (The previous strategy values `snapshot-stream` and `bulk` have been renamed to `interleaved-snapshot` and `sequential-snapshot`.)

## Full Sync

```bash
export CONNECTION_STRING="postgresql://user:password@host:5432/database"
export SURREAL_ENDPOINT="ws://localhost:8000"

surreal-sync from postgresql full \
  --connection-string "$CONNECTION_STRING" \
  --slot "surreal_sync_slot" \
  --schema "public" \
  --strategy sequential-snapshot \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "myapp" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

Provide a checkpoint store (`--checkpoint-dir` or `--checkpoints-surreal-table`).

Checkpoints follow the t1/t2 model:

- **t1** — LSN captured *before* the snapshot begins. Use this as `--incremental-from` (not t2).
- **t2** — LSN captured *after* the snapshot finishes. Reference only; the dump itself is inconsistent until incremental replay catches up.

Because a plain `SELECT *` is not a database-wide consistent read, the dump can mix row versions seen anywhere between t1 and t2. LSNs use the `segment/offset` format (for example, `0/1949850`).

## Incremental Sync (required for consistency)

You must run full sync first to create the replication slot and emit checkpoints. Then run incremental **from t1** to replay WAL and reach consistency at t2:

```bash
LSN=$(cat ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json | jq -r '.checkpoint_data | fromjson | .lsn')

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

By reading and applying WAL from t1, incremental sync writes all changes up to t2; the target then equals the source at t2. While incremental sync runs, your application can continue writing to PostgreSQL.

There is no combined `sync` command for this strategy — consistency requires the two separate runs above.

## WAL retention

Sequential-snapshot pins the replication slot at t1 for the whole snapshot duration (**unbounded retention**). PostgreSQL retains WAL from t1 until the incremental replay catches up to t2, so disk usage can grow significantly on large or slow snapshots. Long-running standalone `incremental` runs have the same effect if the slot does not advance quickly.

Monitor slot lag:

```sql
SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name = 'surreal_sync_slot';
```

Delete unused slots when migration is complete:

```sql
SELECT pg_drop_replication_slot('surreal_sync_slot');
```

With the default interleaved-snapshot strategy, the slot advances continuously as changes are consumed — see [Troubleshooting](../postgresql-wal2json-source.md#replication-slot-lag) in the standard guide.
