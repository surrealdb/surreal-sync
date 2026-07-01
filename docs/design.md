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

## Full Sync Strategies

For PostgreSQL and MySQL, `surreal-sync` offers two full-sync strategies that reach the same end state — a target consistent with the source — but differ in how the snapshot relates to the change stream. The **interleaved snapshot** strategy (the default) copies the table snapshot concurrently with the change stream, giving bounded memory and bounded log retention. The **sequential snapshot** strategy takes a monolithic snapshot first and then replays the whole change log on top, which pins the source log for the entire snapshot (unbounded retention).

See [Full Sync Strategies](design/full-sync-strategies.md) for the detailed side-by-side comparison, the consistency guarantee, and the bounded-memory/bounded-retention analysis.

## Consistency Guarantee

`surreal-sync` is designed to provide consistent sync results even when the source database is not even configured to use Snapshot Isolation or greater.

With the **sequential snapshot** strategy, the full sync dump would contain an inconsistent snapshot of the source that mixes data between t1 and t2 if the source is not SI or greater. Even in this case, `surreal-sync` can provide consistent results with t2 and later.

It does so by setting up the incremental sync infrastructure BEFORE the initial full sync begins, attempting to incrementally sync changes from t1 until t2, to provide the consistent result at t2.

Note that t1 will occasionally occur before the start of a full sync because it is not always possible to obtain the exact starting time (for example, the GTID of the transaction) of the full sync.

Similarly, t2 will occasionally occur after the termination of full sync because it is not always possible to obtain the exact ending time of the full sync.

With the **interleaved snapshot** strategy, consistency does not depend on the source's isolation level at all: the snapshot is reconciled against the live stream via watermarks, so the target converges to a consistent image at the end position (≈ t2) and then tracks live. See [Consistency guarantee](design/full-sync-strategies.md#consistency-guarantee-consistent-at-the-end-then-live) for details.

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
- **[MySQL](mysql.md)**: Trigger-based CDC with sequence checkpointing ([legacy sequential-snapshot guide](mysql/legacy.md))
- **[PostgreSQL](postgresql.md)**: Trigger-based CDC with sequence checkpointing ([legacy sequential-snapshot guide](postgresql/legacy.md))
- **[PostgreSQL (wal2json)](postgresql-wal2json-source.md)**: Logical replication with wal2json ([legacy sequential-snapshot guide](postgresql-wal2json/legacy.md))
- **[Neo4j](neo4j.md)**: Timestamp-based tracking with deletion limitations
- **[JSONL](jsonl.md)**: File-based bulk import (no incremental sync)
