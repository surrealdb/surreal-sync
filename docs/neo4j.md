# Surreal-Sync for Neo4j

`surreal-sync from neo4j` as a sub-command to `surreal-sync` that exports Neo4j nodes and relationships to SurrealDB tables.

It supports inconsistent full syncs and consistent incremental syncs, and together provides ability to reproduce consistent snapshots from the source Neo4j graph onto the target SurrealDB tables.

## How It Works

`surreal-sync from neo4j` supports two types of syncs, `full` and `incremental`.

The full sync uses standard Neo4j Cypher queries to dump the nodes and relationships. As you might already know,
it does not guarantee something like "snapshot isolation at the graph or the database level".
A full sync result can contain various versions of nodes and relationships contained in the source Neo4j graph, from the starting time to the ending time of the full sync.

The incremental sync uses timestamp-based tracking to capture changes. It queries for nodes and relationships that have been updated after a specific timestamp. This approach requires that your Neo4j data includes timestamp properties (like `updated_at`) on nodes and relationships.

## Prerequisites

You need to ensure your Neo4j nodes and relationships have timestamp properties for incremental syncs.
That's because the incremental sync relies on timestamp tracking to identify changed data.

Neo4j incremental sync cannot detect deletions for now. Deleted nodes and relationships will remain in SurrealDB after incremental sync. Consider using soft deletes in your application and Neo4j nodes, or periodic full syncs to handle deletions (although the periodic cleanup and re-full-sync may make sense only when you are using the target SurrealDB intermittently)

## Full Sync

You can start a full sync via the `surreal-sync from neo4j full` command like below:

```bash
export CONNECTION_STRING="bolt://neo4j:7687"
export USERNAME="neo4j"
export PASSWORD="password"
export SURREAL_ENDPOINT="ws://localhost:8000"

# Full sync (captures baseline timestamp)
surreal-sync from neo4j full \
  # Source = Neo4j settings
  --connection-string "$CONNECTION_STRING" \
  --username "$USERNAME" \
  --password "$PASSWORD" \
  --timezone "UTC" \
  # Target = SurrealDB settings
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "graph_data" \
  --emit-checkpoints
```

`--emit-checkpoints` is optional but necessary when you want to start incremental syncs after the full sync to enable the command to know "where to continue the sync".

A `surreal-sync` with the `emit-checkpoints` flag will produce logs like the below:

```
INFO surreal_sync::neo4j: Emitted full sync start checkpoint (t1): neo4j:2024-01-15T10:30:00Z
INFO surreal_sync::neo4j: Emitted full sync end checkpoint (t2): neo4j:2024-01-15T10:35:00Z
```

To continue incremental sync after this full sync, you need to specify t1 (not t2) as the starting point for incremental sync.

This corresponds to the fact that the `surreal-sync from neo4j`'s full sync produces inconsistent snapshot of the Neo4j graph, due to the nature of Neo4j's isolation guarantee.

By reading and applying changes made since t1 instead of t2, when the incremental sync writes all the changes up to t2, the target SurrealDB tables can be viewed as consistent with the source graph at t2.

## Incremental Sync

You must run full sync first to generate the checkpoint - incremental sync needs this starting timestamp.

Before running incremental sync, ensure your Neo4j data has timestamp fields (`updated_at` by default, but can be configurable via a `surreal-sync` flag).

```bash
# Find the checkpoint file
ls ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json

# Extract timestamp for incremental sync
TIMESTAMP=$(cat ./.surreal-sync-checkpoints/checkpoint_full_sync_start_*.json | jq -r '.checkpoint.Timestamp')
CHECKPOINT="neo4j:$TIMESTAMP"
```

With the proper checkpoint, an incremental sync can be triggered via `surreal-sync from neo4j incremental`:

```bash
surreal-sync from neo4j incremental \
  # Source = Neo4j settings
  --connection-string "$CONNECTION_STRING" \
  --username "$USERNAME" \
  --password "$PASSWORD" \
  --timezone "UTC" \
  # Target = SurrealDB settings
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "graph_data" \
  # Using the checkpoint from full sync
  --incremental-from "$CHECKPOINT" \
  --timeout 1m
```

The `incremental-from` specifies the t1 checkpoint explained previously, and `timeout` specifies when the incremental sync should stop.

The `timeout` is necessary when you want to run incremental sync in batches, or run it periodically rather than in a persistent process. Depending on how you want to keep incremental sync running, you should put surreal-sync under a process manager or under a container orchestration system that handles automatic retries, with or without the specific `timeout`.

While the incremental sync is running, your application can continue writing to Neo4j.
Doing incremental sync does not necessarily incur downtime to your application, as long as the source Neo4j database can serve the entire workloads.

Remember that incremental sync will NOT sync deletions. Deleted nodes and relationships will remain in SurrealDB.

## Troubleshooting

### Missing Timestamps

If incremental sync is not capturing changes, ensure all nodes have timestamp fields:

```cypher
// Find nodes without timestamp tracking
MATCH (n)
WHERE NOT EXISTS(n.updated_at)
RETURN labels(n) as label, count(n) as count
ORDER BY count DESC;
```

### Deletion Limitation

Neo4j incremental sync cannot detect deletions. Consider these workarounds:

1. Soft Deletes
```cypher
// Instead of DELETE, use soft delete in your application
MATCH (n:User {id: 123})
SET n.deleted_at = datetime(), n.deleted = true
```

2. Periodic Full Syncs
  - Schedule weekly or monthly clean ups and full syncs to handle deletions and ensure data consistency.
    This makes sense only when you use the target SurrealDB only intermittently.

### Performance Optimization

For large graphs, create indexes on timestamp fields:

```cypher
// Create index on timestamp field for better query performance
CREATE INDEX FOR (n:User) ON (n.updated_at);
CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.updated_at);
```

## Data Type Support

See [Neo4j Data Types](neo4j-data-types.md) for data type mapping information.
