# Surreal-Sync for MySQL

`surreal-sync from mysql` as a sub-command to `surreal-sync` that exports MySQL tables to SurrealDB tables.

It supports inconsistent full syncs and consistent incremental syncs, and together provides ability to reproduce consistent snapshots from the source MySQL tables onto the target SurrealDB tables.

## How It Works

`surreal-sync from mysql` supports two types of syncs, `full` and `incremental`.

The full sync uses standard MySQL queries to dump the table rows. As you might already know,
it does not guarantee something like "snapshot isolation at the table or the database level".
A full sync result can contain various versions of rows contained in the source MySQL tables, from the starting time to the ending time of the full sync.

The incremental sync uses a trigger-based approach with an audit table to capture changes. It provides a resumable change capture by tracking changes in a separate table and using sequence-based checkpointing.
A potential alternative would be to read binlog- `surreal-sync` may potentially support alternative incremental sync backend that relies on binlog reading and parsing.

## Prerequisites

You need appropriate permissions to create triggers and tables in the MySQL database for incremental syncs.
That's because the incremental sync relies on database triggers to capture changes.

## Full Sync

You can start a full sync via the `surreal-sync from mysql full` command like below:

```bash
export CONNECTION_STRING="mysql://root:root@mysql:3306/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

# Full sync (automatically sets up triggers for incremental sync)
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
  --emit-checkpoints
```

`--emit-checkpoints` is optional but necessary when you want to start incremental syncs after the full sync to enable the command to know "where to continue the sync".

A `surreal-sync` with the `emit-checkpoints` flag will produce logs like the below:

```
INFO surreal_sync::mysql: Emitted full sync start checkpoint (t1): mysql:sequence:0
INFO surreal_sync::mysql: Emitted full sync end checkpoint (t2): mysql:sequence:123
```

To continue incremental sync after this full sync, you need to specify t1 (not t2) as the starting point for incremental sync.

This corresponds to the fact that the `surreal-sync from mysql`'s full sync may produce inconsistent snapshot of the MySQL tables, depending on the nature of the MySQL isolation guarantee you chose.

By reading and applying changes made since t1 instead of t2, when the incremental sync writes all the changes up to t2, the target SurrealDB tables can be viewed as consistent with the source tables at t2.

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

## Troubleshooting

### Missing Triggers

If incremental sync is not capturing changes, ensure triggers were created during full sync:

```sql
-- Check for triggers
SHOW TRIGGERS LIKE 'surreal_sync_%';
```

### Audit Table Cleanup

The `surreal_sync_changes` audit table may grow over time. Consider periodic cleanup:

```sql
-- Check audit table size
SELECT COUNT(*) FROM surreal_sync_changes;

-- Clean up old records (after they've been synced)
DELETE FROM surreal_sync_changes WHERE changed_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
```

## Data Type Support

See [MySQL Data Types](mysql-data-types.md) for data type mapping information.
