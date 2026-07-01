# PostgreSQL (Trigger) — Sequential Snapshot (Legacy)

This guide covers the **sequential-snapshot** full-sync strategy for the PostgreSQL trigger source. It is the original two-phase workflow: a monolithic `SELECT *` per table produces an **inconsistent** snapshot (row versions can mix anywhere between t1 and t2), and a **separate** incremental run from t1 replays the [t1,t2] change log to make the target consistent at t2.

For the standard interleaved-snapshot workflow, see [Surreal-Sync for PostgreSQL (Trigger-Based)](../postgresql.md). For the design rationale, see [Full Sync Strategies](../design/full-sync-strategies.md).

## When to use sequential-snapshot

Use `--strategy sequential-snapshot` when:

- A selected table has **no usable primary key** (interleaved-snapshot requires one on every selected table).
- Writing the **`surreal_sync_signal` watermark/signal table** to the source is not permitted.

**Behavior change for existing users:** `interleaved-snapshot` is now the default. Pass `--strategy sequential-snapshot` to keep the previous monolithic full-sync behavior. (The previous strategy values `snapshot-stream` and `bulk` have been renamed to `interleaved-snapshot` and `sequential-snapshot`.)

## Full Sync

```bash
export CONNECTION_STRING="postgresql://postgres:postgres@postgresql:5432/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

surreal-sync from postgresql-trigger full \
  --connection-string "$CONNECTION_STRING" \
  --strategy sequential-snapshot \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

Provide a checkpoint store (`--checkpoint-dir` or `--checkpoints-surreal-table`).

Example log output:

```
INFO surreal_sync::postgresql_trigger_source: Emitted full sync start checkpoint (t1): 0
INFO surreal_sync::postgresql_trigger_source: Emitted full sync end checkpoint (t2): 123
```

Checkpoints follow the t1/t2 model:

- **t1** — captured *before* the snapshot begins. Use this as `--incremental-from` (not t2).
- **t2** — captured *after* the snapshot finishes. Reference only; the dump itself is inconsistent until incremental replay catches up.

Because a plain `SELECT *` is not a database-wide consistent read, the dump can mix row versions seen anywhere between t1 and t2. The checkpoint value is the sequence ID number (for example, `"123"`).

## Incremental Sync (required for consistency)

You must run full sync first to set up triggers and emit checkpoints. Then run incremental **from t1** to replay changes and reach consistency at t2:

```bash
CHECKPOINT=$(cat ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json | jq -r '.checkpoint_data | fromjson | .sequence_id')

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

By reading and applying changes since t1, incremental sync writes all changes up to t2; the target then equals the source at t2. While incremental sync runs, your application can continue writing to PostgreSQL.

There is no combined `sync` command for this strategy — consistency requires the two separate runs above.

## Audit table cleanup

Sequential-snapshot pins the audit log for the whole snapshot duration (**unbounded retention**), so `surreal_sync_changes` may grow significantly. Long-running standalone `incremental` runs have the same effect. Consider periodic cleanup:

```sql
SELECT COUNT(*) FROM surreal_sync_changes;

DELETE FROM surreal_sync_changes WHERE changed_at < NOW() - INTERVAL '30 days';
```

With the default interleaved-snapshot strategy, consumed rows are pruned automatically — see [Troubleshooting](../postgresql.md#audit-table-growth) in the standard guide.
