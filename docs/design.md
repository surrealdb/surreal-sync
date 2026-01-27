# Surreal-Sync Design

This document explains the design principles and architecture of surreal-sync for both full and incremental synchronization.

## What is surreal-sync?

`surreal-sync` is a database synchronization tool that enables both full and incremental data synchronization from various source databases to SurrealDB.

## Overview

`surreal-sync` coordinates inconsistent full sync and consistent incremental sync with checkpoints to ensure no data loss during synchronization:

- Full Sync: Bulk transfers all existing data
- Incremental Sync: Captures and syncs ongoing changes
- Checkpoint: Prevents gaps and data loss between full and incremental sync using checkpoints (t1, t2, t3)

```
t1 ════════════════════ t2 ──────── t3 ──────→
│                        │          │
└── Full Sync ───────────┘          │
│                        │          │
└── Incremental Sync ────┘──────────┘──────...
```

1. **t1**: Full sync begins + incremental sync start
2. **t2**: Full sync completes + initial sync ending checkpoint
3. **t3**: Incremental sync continues with new changes

The user can treat the target SurrealDB as consistent with the source DB
since t2.

## Consistency Guarantee

`surreal-sync` is designed to provide consistent sync results even when the source database is not even configured to use Snapshot Isolation or greater.

If the source database is not SI or greater, the full sync dump would contain inconsistent snapshot of the source that mixes data between t1 and t2. Even in this case, `surreal-sync` can provide consistent results with t2 and later.

It does so by setting up the incremental sync infrastructure BEFORE the initial full sync begins, attempting to incrementally sync changes from t1 until t2, to provide the consistent result at t2.

Note that t1 will occasionally occur before the start of a full sync because it is not always possible to obtain the exact starting time (for example, the GTID of the transaction) of the full sync.

Similarly, t2 will occasionally occur after the termination of full sync because it is not always possible to obtain the exact ending time of the full sync.

## Database-Specific Implementations

Each database uses the most appropriate change detection method:

### Native CDC

`surreal-sync` uses a native CDC feature provided in the source database if possible/implemented.

Currently, our MongoDB source uses MongoDB Change Streams for incremental syncs.

### Trigger-Based CDC

`surreal-sync` uses trigger-based approaches whenever a native CDC feature is nonexistent, or considered not appropriate for various reasons.

Currently, our PostgreSQL and MySQL sources use triggers and audit tables updated via the triggers. It turned out to be sufficient to provide sequence-based checkpoints to full and incremental sync results and reproduce consistent snapshots onto the target SurrealDB tables.

### Timestamp-Based

`surreal-sync` uses a logical, timestamp-based approach whenever any of the above is not possible.

This refers to when the original data in the source database has timestamp-fields such as `updated_at` populated by the defaulting or the application code.

This is the approach currently used with our Neo4j source. This is mainly because Neo4j's CDC feature seems to be present only with an Enterprise subscription which we didn't want to rely on for now.

## Source-Specific Details

See the source-specific documentation for implementation details:

- **[MongoDB](mongodb.md)**: Change streams and resume token management
- **[MySQL](mysql.md)**: Trigger-based CDC with sequence checkpointing
- **[PostgreSQL](postgresql.md)**: Trigger-based CDC with sequence checkpointing
- **[Neo4j](neo4j.md)**: Timestamp-based tracking with deletion limitations
- **[JSONL](jsonl.md)**: File-based bulk import (no incremental sync)
