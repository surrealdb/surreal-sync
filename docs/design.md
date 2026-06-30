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

For PostgreSQL and MySQL, `surreal-sync` offers two full-sync strategies. They reach the same end state — a target consistent with the source — but differ in how the snapshot relates to the change stream, and in their operational properties.

### Bulk (two-phase t1/t2)

The original strategy. Full sync reads each table with a monolithic `SELECT *`, bracketed by two checkpoints:

- **t1** is captured *before* the snapshot begins.
- **t2** is captured *after* the snapshot finishes.

Because a plain `SELECT *` is not a database-wide consistent read, the dump can mix row versions seen anywhere between t1 and t2. Consistency is recovered in a *separate* incremental run that replays the entire change log from t1 on top of the snapshot using idempotent UPSERTs; once it reaches t2 the target equals the source as of t2.

- Invocation: two CLI runs — `full` (`--strategy bulk`) then `incremental --incremental-from <t1>`.
- Snapshot vs stream: sequential. The stream is replayed *after* the snapshot.
- Memory: the whole table is materialized per `SELECT *`.
- Source change-log retention: the source must retain the entire change log from t1 until the incremental run catches up (the replication slot pins WAL; trigger audit rows pile up). Backlog grows with the full snapshot duration.
- Requirements: no primary key required; no extra table is written to the source.

### Snapshot-stream (watermark, the default)

The DBLog/Debezium-style incremental snapshot. The change stream is consumed *continuously*, and the table snapshot is interleaved into that same ordered stream in primary-key-ordered, resumable chunks. Each chunk is bracketed by a low and a high watermark written to a small `surreal_sync_signal` table on the source; rows read by the chunk are buffered, keyed by primary key, and reconciled against the live stream inside the watermark window (see the consistency guarantee below).

- Invocation: a single combined orchestrator `from <source> sync`, or `full` (default `--strategy snapshot-stream`) followed by `incremental` from the handed-off position. `--chunk-size <N>` controls the keyset chunk (default 1024, Debezium's default).
- Snapshot vs stream: concurrent, in one loop — there is no separate replay phase.
- Resumability: per-chunk checkpoint (stream position + per-table last primary key), so a crash resumes at the last completed chunk instead of restarting the whole table.
- Memory: bounded to one chunk buffer, O(chunk_size), independent of table size.
- Source change-log retention: drained continuously as it is consumed, so the log is never held from t1 to t2.
- Ad-hoc / add-table re-snapshot while streaming: supported via the signal table.
- Requirements: every selected table needs a usable primary key, and `surreal-sync` writes a `surreal_sync_signal` table (and watermark rows) to the source.

`snapshot-stream` is the **default** for PostgreSQL (both the wal2json and trigger sources) and MySQL. Use it whenever the source supports it. Opt out with `--strategy bulk` when a selected table has no usable primary key, or when writing the watermark/signal table to the source is not permitted.

#### Consistency guarantee (consistent at the end, then live)

The watermark snapshot does **not** produce a frozen point-in-time image at t1. Streaming never stops; the snapshot is interleaved into the same ordered change stream. The guarantee is:

> Once the snapshot has finished and streaming has applied the log up to position `P`, the target equals the source as of `P`.

If you stop when the snapshot ends, `P` is approximately t2, so you get a consistent image of the source at **t2 (the end), not t1** — and it keeps tracking live afterward. Every row's final value resolves to its t2 value, via the watermark window dedup:

- **Rows changed during the snapshot**: the dedup discards the snapshot read and keeps the **log events** (applied in commit order), so the row ends at its latest value.
- **Rows not changed during the snapshot**: the chunk `SELECT` value is emitted; because the row did not change across its window it is stable and still equals the t2 value.

In short, when primary keys collide inside an open watermark window the **log event wins**, and unchanged rows are stable. The t1 marker only tells the run where to begin draining the log; it does not freeze data at t1.

#### Bounded memory and bounded retention

These are the headline properties of snapshot-stream over bulk:

- **Bounded memory — O(chunk_size), event-based.** The only buffered state is one chunk (at most `chunk_size` keys), held between the low and high watermark and then flushed. The buffer only ever grows at chunk load, which is structurally capped by the `LIMIT chunk_size` read, so peak buffered rows equal `chunk_size` regardless of table size.
- **Bounded retention — the log is freed as it is consumed.** Because streaming is drained continuously, the source change log is advanced/freed throughout the snapshot rather than pinned for its whole duration: the wal2json backend advances the replication slot (`restart_lsn`) so WAL can be reclaimed, and the trigger backends prune consumed rows from the `surreal_sync_changes` audit table. Nothing is freed past the resumable checkpoint position, so resume safety is preserved. Net result: log retention is O(streaming lag), independent of total snapshot time.

By contrast, bulk holds the whole table in memory per `SELECT *` and pins the source change log from t1 until catch-up.

## Consistency Guarantee

`surreal-sync` is designed to provide consistent sync results even when the source database is not even configured to use Snapshot Isolation or greater.

With the **bulk** strategy, the full sync dump would contain an inconsistent snapshot of the source that mixes data between t1 and t2 if the source is not SI or greater. Even in this case, `surreal-sync` can provide consistent results with t2 and later.

It does so by setting up the incremental sync infrastructure BEFORE the initial full sync begins, attempting to incrementally sync changes from t1 until t2, to provide the consistent result at t2.

Note that t1 will occasionally occur before the start of a full sync because it is not always possible to obtain the exact starting time (for example, the GTID of the transaction) of the full sync.

Similarly, t2 will occasionally occur after the termination of full sync because it is not always possible to obtain the exact ending time of the full sync.

With the **snapshot-stream** strategy, consistency does not depend on the source's isolation level at all: the snapshot is reconciled against the live stream via watermarks, so the target converges to a consistent image at the end position (≈ t2) and then tracks live. See [Consistency guarantee](#consistency-guarantee-consistent-at-the-end-then-live) above.

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
