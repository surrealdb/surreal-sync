# Surreal-Sync for PostgreSQL (Trigger-Based)

`surreal-sync from postgresql-trigger` as a sub-command to `surreal-sync` that exports PostgreSQL tables to SurrealDB tables.

It supports inconsistent full syncs and consistent incremental syncs, and together provides ability to reproduce consistent snapshots from the source PostgreSQL tables onto the target SurrealDB tables.

## How It Works

`surreal-sync from postgresql-trigger` supports two types of syncs, `full` and `incremental`.

The full sync uses standard PostgreSQL queries to dump the table rows. As you might already know,
it does not guarantee something like "snapshot isolation at the table or the database level".
A full sync result can contain various versions of rows contained in the source PostgreSQL tables, from the starting time to the ending time of the full sync.

The incremental sync uses a trigger-based approach with an audit table to capture changes. It provides a resumable change capture by tracking changes in a separate table and using sequence-based checkpointing.
A potential alternative to this approach is to read and parse WAL and reply changes recorded in the WAL.
We opted to use the trigger-based approach believing it's more reliable from the application perspective because
you don't need to explore less mature WAL reading/parsing libraries/code/etc.
But we may build an alternative PostgreSQL incremental sync backend that relies on WAL.

## Prerequisites

You need appropriate permissions to create triggers and tables in the PostgreSQL database for incremental syncs.
That's because the incremental sync relies on database triggers to capture changes.

## Full Sync

You can start a full sync via the `surreal-sync from postgresql-trigger full` command like below:

```bash
# Note: Database name must be included in the connection string (e.g., /myapp at the end)
export CONNECTION_STRING="postgresql://postgres:postgres@postgresql:5432/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

# Full sync (automatically sets up triggers for incremental sync)
surreal-sync from postgresql-trigger full \
  # Source = PostgreSQL settings
  --connection-string "$CONNECTION_STRING" \
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
INFO surreal_sync::postgresql_trigger_source: Emitted full sync start checkpoint (t1): 0
INFO surreal_sync::postgresql_trigger_source: Emitted full sync end checkpoint (t2): 123
```

The checkpoint format is just the sequence ID number (e.g., `"123"`), not a prefixed string.

To continue incremental sync after this full sync, you need to specify t1 (not t2) as the starting point for incremental sync.

This corresponds to the fact that the `surreal-sync from postgresql-trigger`'s full sync produces inconsistent snapshot of the PostgreSQL tables, due to the nature of PostgreSQL's isolation guarantee.

By reading and applying changes made since t1 instead of t2, when the incremental sync writes all the changes up to t2, the target SurrealDB tables can be viewed as consistent with the source tables at t2.

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

### Command-Line Options

#### Full Sync Options

- `--connection-string`: PostgreSQL connection string (must include database, e.g., `postgresql://user:pass@host:5432/mydb`)
- `--to-namespace`: Target SurrealDB namespace
- `--to-database`: Target SurrealDB database
- `--emit-checkpoints`: Emit checkpoint files for coordinating with incremental sync (boolean flag)
- `--checkpoint-dir`: Directory to write checkpoint files (default: `.surreal-sync-checkpoints`)
- `--schema-file`: Optional schema file for type-aware conversion
- Plus standard SurrealDB options: `--surreal-endpoint`, `--surreal-username`, `--surreal-password`, `--batch-size`, `--dry-run`

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

The `surreal_sync_changes` audit table may grow over time. Consider periodic cleanup:

```sql
-- Check audit table size
SELECT COUNT(*) FROM surreal_sync_changes;

-- Clean up old records (after they've been synced)
DELETE FROM surreal_sync_changes WHERE changed_at < NOW() - INTERVAL '30 days';
```

## Data Type Support

See [PostgreSQL Data Types](postgresql-data-types.md) for data type mapping information.
