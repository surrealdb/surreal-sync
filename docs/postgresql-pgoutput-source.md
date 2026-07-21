# PostgreSQL pgoutput WAL Source

`surreal-sync from postgresql-pgoutput` replicates PostgreSQL tables into SurrealDB using native logical replication (`pgoutput`). It creates a replication slot and publication, reads row-level WAL changes, and applies them to SurrealDB — no triggers or wal2json plugin on the source.

Apply into SurrealDB goes through the [`sync-transform`](sync-pipeline.md) framework (`--transforms-config` optional; omit for identity).

The end-to-end model mirrors the MySQL binlog and wal2json sources:

1. **Snapshot** the selected tables into SurrealDB.
2. **Stream** WAL changes and apply them, converging the target to a consistent image.
3. **Resume** from a durable checkpoint store after any restart.

> **Alternatives:** For trigger-based capture see [Surreal-Sync for PostgreSQL](postgresql.md). For the wal2json logical-replication plugin see [PostgreSQL wal2json source](postgresql-wal2json-source.md).

## Schema changes

surreal-sync tolerates most online schema evolution while syncing. How a change is handled depends on whether it is observed on the **stream** (the streaming phase of `sync`, or `sync --snapshot-mode never`) or during a **snapshot** (`sync --snapshot-mode only`, or the snapshot phase of interleaved `sync`).

### Schema changes during streaming

surreal-sync observes DDL on the pgoutput stream (`Relation` events that describe new column layouts, and table renames detected when a known relation OID arrives with a new name) and refreshes its cached table schema and column metadata **before** applying subsequent row events. This keeps column-name and `ENUM` label resolution correct after an online `ALTER`. A `RENAME` of a synced table is followed automatically: the synced-table set is rewritten to the new name so post-rename row events are still applied (not silently filtered out).

Row events are never silently dropped: if a row event arrives without the preceding `Relation` metadata that describes its layout (which can only happen on a protocol violation or an unhandled schema edge case), surreal-sync fails loudly instead of skipping the change.

### Schema changes during full / interleaved snapshot

The snapshot copies each table in primary-key-ordered chunks. While that runs, the interleaved strategy is **also** consuming the live WAL, so DDL that happens during the snapshot is observed on the stream side and refreshes the cached schema/label metadata exactly as during steady-state streaming. Because each chunk is bracketed by low/high **watermarks** and overlapping changes are reconciled with the live log event winning, an `ALTER` that lands mid-snapshot is reconciled by the streamed post-`ALTER` row image rather than the stale chunk read.

Practical guidance:

- **Additive / label changes** (add column, add enum label) are safe during a snapshot; the streamed image carries the new shape.
- **Breaking changes** (drop the primary key, rename a table away from the synced set, or otherwise change the keyset ordering) during a large snapshot are best run in a maintenance window: pause `sync`, apply the DDL, then restart (the interleaved handoff guarantees no streamed change is missed across the restart).
- The **sequential** strategy dumps each table in a single `SELECT` pass with no live refresh *during that pass*; if you must run breaking DDL against a huge table, prefer the interleaved strategy or a maintenance window.

### Behavior matrix

Expected behavior for common DDL, for the streaming path and the snapshot path (`full` / `sync` snapshot):

| DDL | Streaming (`sync` stream / `never`) | Snapshot (`only` / interleaved) |
|-----|-------------------------------------|----------------------------------|
| **Add column** | Continue; schema refreshed, new column appears on later rows. | Continue; streamed post-DDL image carries the column. |
| **Drop column** | Continue; dropped column stops appearing. | Continue. |
| **Rename column** | Continue; new name used for later rows (old values remain in the target under the old field until overwritten). | Continue. |
| **Rename table** (synced) | Continue; synced set follows the rename to the new name. | Continue (streamed); coordinate for very large tables. |
| **Change enum labels** | Continue; labels re-resolved from refreshed metadata. Out-of-range indexes keep the raw value and emit a `WARN` (data-quality signal). | Continue. |
| **Drop primary key** | Clear error — a keyable primary key is required; fix schema or re-snapshot. | Clear error at schema collection for the affected synced table. |
| **Create new table** | Not synced automatically; use [`snapshot`](#ad-hoc-snapshots-signalling) to add it to a running follower. | Snapshot only covers `--tables` selected at start; use `snapshot` signalling to add more. |

**Primary keys.** Every synced table must have a usable primary key. A synced table without one is rejected with a clear error on the snapshot path, and its change events cannot be keyed on the streaming path.

## Protocol limitations

surreal-sync uses PostgreSQL's built-in `pgoutput` plugin (not wal2json). Two categories of WAL traffic are decoded but **not** turned into row changes today:

### Two-phase commit (2PC)

PostgreSQL logical decoding protocol v3+ can emit prepare/commit-prepared/rollback-prepared messages for two-phase transactions (`PREPARE TRANSACTION`, etc.). surreal-sync maps these control messages to no-ops during steady-state streaming. **Row changes from 2PC workflows are not replicated** until the prepared transaction commits and the changes appear as ordinary insert/update/delete events. If your application relies on reading uncommitted prepared state from the replication stream, use a different capture mechanism or avoid 2PC on synced tables.

### Parallel streaming (protocol v4+)

PostgreSQL 16+ can stream large transactions using `streaming = parallel` (protocol version 4), splitting one transaction across multiple sub-streams with `StreamStart` / `StreamStop` / `StreamCommit` / `StreamAbort` framing. surreal-sync currently consumes these as control events and applies only the final committed row images once the stream completes. **In-flight sub-stream rows are not applied incrementally** during an open parallel stream; convergence happens at commit (or the stream is discarded on abort). For most OLTP workloads this matches committed-row CDC semantics, but very long parallel-stream transactions may appear on the target only after commit.

### What this means in practice

- Normal single-phase `BEGIN` … `COMMIT` transactions are fully supported.
- `TRUNCATE` on synced tables is logged but does not automatically delete rows in SurrealDB (operators should treat truncate as a manual reconciliation event).
- Enable `track_commit_timestamp=on` on the source (recommended in server config) for watermark signalling used by interleaved snapshot.

## Prerequisites

| Setting | Value | Why |
|---------|-------|-----|
| `wal_level` | `logical` | Logical decoding must be enabled. |
| `max_wal_senders` | ≥ number of consumers | Each follower holds a replication connection. |
| `max_replication_slots` | ≥ number of consumers | Each follower needs a durable slot. |
| `track_commit_timestamp` | `on` | Watermark signalling for interleaved snapshot. |

The replication user needs `REPLICATION` privilege and `SELECT` on synced tables (plus `CREATE` on the database for the `surreal_sync_signal` watermark table).

```sql
CREATE USER surreal_sync WITH REPLICATION PASSWORD 'change_me';
GRANT CONNECT ON DATABASE myapp TO surreal_sync;
GRANT USAGE ON SCHEMA public TO surreal_sync;
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON ALL TABLES IN SCHEMA public TO surreal_sync;
```

Every synced table must have a usable primary key. surreal-sync creates a publication (default `surreal_sync_pub`) and logical replication slot (default `surreal_sync_slot`) on first run.

## Quick start

```bash
export CONNECTION_STRING="postgresql://surreal_sync:change_me@postgresql:5432/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

surreal-sync from postgresql-pgoutput sync \
  --connection-string "$CONNECTION_STRING" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --tables "users,orders" \
  --chunk-size 1024 \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

Use `--snapshot-mode only` followed by `--snapshot-mode never` to schedule snapshot and stream separately. Checkpoints record WAL **LSN** positions; resume by restarting the same command with the same `--checkpoint-dir`.

## Automated tests

```bash
make build-debug && cargo test -p surreal-sync --test postgresql_pgoutput -- --nocapture
cargo test -p surreal-sync-postgresql-pgoutput-source --test suite -- --nocapture
```

Integration tests use stock `postgres:16` with `wal_level=logical` (no wal2json image build).

## References

- Implementation: [crates/postgresql-pgoutput-source/](../crates/postgresql-pgoutput-source/) and [crates/pgoutput-protocol/](../crates/pgoutput-protocol/)
- [Full Sync Strategies](design/full-sync-strategies.md)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
