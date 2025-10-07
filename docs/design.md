# Surreal-Sync Design

This document explains the design principles and architecture of surreal-sync for both full and incremental synchronization.

## What's surreal-sync?

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

`surreal-sync` is designed to provide consistent sync results even though the source database is not even configured to use Snapshot Isolation or greater.

If the source database is not SI or greater, the full sync dump would
contain inconsistent snapshot of the source that mixes data between t1 and t2. Even so, `surreal-sync` can provide consistent result with t2 and later.

It does so by setting up the incremental sync infrastructure BEFORE the initial full sync begins, and tries to incremental-sync changes from t1 until t2, to provide the consistent result at t2.

Note that t1 is occasionally "before full sync start" because it is not always possible to obtain the exact starting time (for example GTID of the transaction) of the full sync.

Similarly, t2 is occasionally "after full sync end" because it is not always possible to obtain the exact ending time of the full sync.

## Database-Specific Implementations

Each database uses the most appropriate change detection method:

### Native CDC

`surreal-sync` uses a native CDC feature provided in the source database if possible/implemented.

Currently, our MongoDB source uses MongoDB Change Streams for incremental syncs.

### Trigger-Based CDC

`surreal-sync` uses trigger-based approaches whenever a native CDC feature is nowhere, or considered not appropriate for various reasons.

Currently, our PostgreSQL and MySQL sources use triggers and audit tables updated via the triggers. It turned out to be enough for providing sequence-based checkpoints to full and incremental sync results and reproduce consistent snapshots onto the target SurrealDB tables.

### Timestamp-Based

`surreal-sync` uses logical, timestamp-based approach whenever any of the above is not possible.

This basically means that the original data in the source database has timestamp-fields like `updated_at` populated by the defaulting or the application code.

Currently, our Neo4j source does this. It's mainly because Neo4j's CDC feature seems to be present only with Enterprise subscription which we didn't want to rely on "for now".

## Source-Specific Details

See the source-specific documentation for implementation details:

- **[MongoDB](mongodb.md)**: Change streams and resume token management
- **[MySQL](mysql.md)**: Trigger-based CDC with sequence checkpointing
- **[PostgreSQL](postgresql.md)**: Trigger-based CDC with sequence checkpointing
- **[Neo4j](neo4j.md)**: Timestamp-based tracking with deletion limitations
- **[JSONL](jsonl.md)**: File-based bulk import (no incremental sync)
