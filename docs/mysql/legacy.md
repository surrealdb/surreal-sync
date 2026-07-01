# MySQL — Sequential Snapshot (Legacy)

This guide covers the **sequential-snapshot** full-sync strategy for MySQL. It is the original two-phase workflow: a monolithic `SELECT *` per table bracketed by t1/t2 checkpoints, followed by a separate incremental replay of the [t1,t2] change log to make the target consistent at t2.

For the standard workflow, see [Surreal-Sync for MySQL](../mysql.md).

See [Full Sync Strategies](../design/full-sync-strategies.md) for the design rationale and side-by-side comparison with interleaved-snapshot.

## When to use sequential-snapshot

Use `--strategy sequential-snapshot` when:

- A selected table has **no usable primary key** (interleaved-snapshot requires one on every selected table).
- Writing the **`surreal_sync_signal` watermark/signal table** to the source is not permitted.

**Behavior change for existing users:** `interleaved-snapshot` is now the default. Compared to the previous behavior, the default creates a `surreal_sync_signal` table on the source and requires every selected table to have a primary key. Pass `--strategy sequential-snapshot` to keep the previous monolithic full-sync behavior. (The previous strategy values `snapshot-stream` and `bulk` have been renamed to `interleaved-snapshot` and `sequential-snapshot`.)

## How it differs from the default

| | Sequential snapshot (this guide) | Interleaved snapshot ([standard](../mysql.md)) |
|---|---|---|
| Invocation | Two runs: `full` then `incremental --incremental-from <t1>` | Usually one `sync` run, or `full` then `incremental` from the end position |
| Snapshot vs stream | Sequential — replay after the snapshot | Concurrent — snapshot interleaved with the stream |
| Memory | Whole table materialized per `SELECT *` | Bounded to one chunk, O(chunk_size) |
| Audit log retention | Pinned for the whole snapshot (unbounded) | Drained continuously (bounded) |
| Primary key | Not required | Required on every selected table |
| Signal table | Not written | `surreal_sync_signal` written to source |

## Full Sync

```bash
export CONNECTION_STRING="mysql://root:root@mysql:3306/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

surreal-sync from mysql full \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --strategy sequential-snapshot \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

Checkpoint emission is enabled by providing a checkpoint store — either `--checkpoint-dir <DIR>` (filesystem) or `--checkpoints-surreal-table <TABLE>` (stored in SurrealDB).

A `surreal-sync` with a checkpoint store configured will produce logs like those below:

```
INFO surreal_sync::mysql: Emitted full sync start checkpoint (t1): mysql:sequence:0
INFO surreal_sync::mysql: Emitted full sync end checkpoint (t2): mysql:sequence:123
```

The emitted checkpoints follow the t1/t2 model:

- **t1** is captured *before* the snapshot begins.
- **t2** is captured *after* the snapshot finishes.

Because a plain `SELECT *` is not a database-wide consistent read, the dump can mix row versions seen anywhere between t1 and t2. Consistency is recovered in a separate incremental run (below).

## Incremental Sync

You must run full sync first to generate the checkpoint and set up the necessary triggers — incremental sync needs this infrastructure.

With the **sequential-snapshot** strategy you must specify **t1 (not t2)** as the starting point for incremental sync. By reading and applying changes made since t1 instead of t2, when the incremental sync writes all the changes up to t2, the target SurrealDB tables can be viewed as consistent with the source tables at t2.

```bash
# Find the t1 checkpoint file
ls ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json

# Extract sequence ID for incremental sync
NUM=$(cat ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json | jq -r '.checkpoint.MySQL.sequence')
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

The `incremental-from` specifies the t1 checkpoint, and `timeout` specifies when the incremental sync should stop.

While the incremental sync is running, your application can continue writing to MySQL.

## Audit table cleanup

The sequential-snapshot strategy pins the audit log for the whole snapshot duration, so `surreal_sync_changes` may grow over time. Long-running standalone `incremental` runs can have the same effect. Consider periodic cleanup:

```sql
-- Check audit table size
SELECT COUNT(*) FROM surreal_sync_changes;

-- Clean up old records (after they've been synced)
DELETE FROM surreal_sync_changes WHERE changed_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
```

With the default interleaved-snapshot strategy, consumed rows are pruned automatically — see [Troubleshooting](../mysql.md#audit-table-growth) in the standard guide.
