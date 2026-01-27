# Surreal-Sync for MongoDB

`surreal-sync from mongodb` as a sub-command to `surreal-sync` that exports MongoDB collections to SurrealDB tables.

It supports inconsistent full syncs and consistent incremental syncs, and together provides ability to reproduce consistent snapshots from the source MongoDB collections onto the target SurrealDB tables.

## How It Works

`surreal-sync from mongodb` supports two types of syncs, `full` and `incremental`.

The full sync uses standard MongoDB queries to dump the collection items. As you might already know, it does not guarantee something like "snapshot isolation at the collection or the database level".

A full sync result can contain various versions of items contained in the source MongoDB collection, from the starting time to the ending time of the full sync.

The incremental sync uses MongoDB change streams, a MongoDB feature that provides real-time change notifications. As it provides a resumable change capture without a lot of configuration, we opted to rely on it for building incremental sync.

## Prerequisites

As change streams require a replica set, you must first initialize a replica set on MongoDB node(s) to do incremental syncs. 

## Full Sync

You can start a full sync via the `surreal-sync from mongodb full` command like below:

```bash
export CONNECTION_STRING="mongodb://root:root@mongodb:27017"
export SURREAL_ENDPOINT="ws://localhost:8000"

# Full sync (automatically verifies change streams)
surreal-sync from mongodb full \
  # Source = MongoDB settings
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  # Target = SurrealDB settings
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root"
  --to-namespace "production" \
  --to-database "migrated_data" \
  --emit-checkpoints
```

`--emit-checkpoints` is optional but necessary when you want to start incremental syncs after the full sync to enable the command to know where to continue the sync.

A `surreal-sync` with the `emit-checkpoints` flag will produce logs like the below:

```
INFO surreal_sync::mongodb: Emitted full sync start checkpoint (t1): mongodb::2024-01-15T10:30:00Z
INFO surreal_sync::mongodb: Emitted full sync end checkpoint (t2): mongodb::2024-01-15T10:35:00Z
```

To continue incremental sync after this full sync, you need to specify t1 (not t2) as the starting point for incremental sync.

This corresponds to the fact that the `surreal-sync from mongodb`'s full sync produces an inconsistent snapshot of the MongoDB collections, due to the nature of MongoDB's isolation guarantee.

By reading and applying changes made since t1 instead of t2, when the incremental sync writes all the changes up to t2, the target SurrealDB tables can be viewed as consistent with the source collections at t2.

## Incremental Sync

You must run a full sync first to generate the checkpoint, as incremental sync requires this starting point.

```bash
# Find the checkpoint file
ls ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json

# Extract resume token for incremental sync
RESUME_TOKEN=$(cat ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json | jq -r '.checkpoint.MongoDB.resume_token')
CHECKPOINT="mongodb:$RESUME_TOKEN"
```

With the proper checkpoint, an incremental sync can be triggered via `surreal-sync from mongodb incremental`:

```bash
surreal-sync from mongodb incremental \
  # Source = MongoDB settings
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

While the incremental sync is running, your application can continue writing to MongoDB.

Doing incremental sync does not necesarily incur downtime to your application, as long as the source MongoDB node/cluster can serve the entire workloads.

## Troubleshooting

If you don't see expected changes synced to the target SurrealDB when using incremental sync, ensure that the MongoDB oplog size is configured appropriately. If the retention period is too short, the change may already be nowhere to be found in the change stream when the incremental sync is run.

## Data Type Support

See [MongoDB Data Types](mongodb-data-types.md) for data type mapping information.
