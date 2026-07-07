# MySQL/MariaDB Binlog Source

`surreal-sync from mysql-binlog` replicates MySQL and MariaDB tables into SurrealDB using the binary log (binlog) as a change-data-capture (CDC) stream. It registers as a replica, reads row-format binlog events, and applies them to SurrealDB — no triggers or audit tables on the source. The same subcommand works against both engines; surreal-sync auto-detects the flavor from `SELECT @@version` (override with `--flavor mysql` or `--flavor mariadb`).

The end-to-end model mirrors a mature CDC pipeline (Debezium-style **snapshot → stream → resume**), grounded in this project's commands and traits:

1. **Snapshot** the selected tables into SurrealDB.
2. **Stream** binlog changes and apply them, converging the target to a consistent image.
3. **Resume** from a durable checkpoint store after any restart, failover, or repointing.

### Terminology

- **Reconciliation** — interleaved snapshot binlog consumption: watermark-window dedup while tables are copied during the initial snapshot phase.
- **Replication tail** — post-handoff steady CDC: continuous binlog apply after snapshot handoff completes (`FullSyncEnd` / `CatchUpProgress`).

> **Trigger-based alternative:** If you cannot enable binlog replication or prefer audit triggers, see [Surreal-Sync for MySQL](mysql.md) and [Surreal-Sync for MariaDB](mariadb.md).

## The happy path

For a new migration that should also keep tracking live changes, run `sync` (default `--snapshot-mode initial`):

```bash
export CONNECTION_STRING="mysql://surreal_sync:change_me@mysql:3306/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

surreal-sync from mysql-binlog sync \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --tables "users,orders" \
  --chunk-size 1024 \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

This performs an interleaved snapshot, hands off to streaming in the same process, and persists checkpoints to your store as it goes. Stop with `SIGINT`/`SIGTERM` and **restart the same command** — surreal-sync reads the latest position from the store.

The sections below cover modes, prerequisites, and operations behind that command.

## How it works

`surreal-sync from mysql-binlog` exposes two subcommands:

| Command | Purpose |
|---------|---------|
| `sync` | Snapshot and/or stream in one process (recommended). |
| `snapshot` | Signal a running `sync` to snapshot extra tables on the fly. |

Use `--snapshot-mode` on `sync` to choose the phase mix:

| `--snapshot-mode` | Behavior |
|-------------------|----------|
| `initial` (default) | Interleaved snapshot, then continuous stream. |
| `never` | Stream only from the checkpoint store (optional `--from` override). |
| `only` | Snapshot only, emit checkpoint, exit (no stream). |

Changes are read by registering as a binlog replica (`COM_REGISTER_SLAVE` followed by `COM_BINLOG_DUMP`, or `COM_BINLOG_DUMP_GTID` on MySQL 8). Row-format events (`WRITE_ROWS`, `UPDATE_ROWS`, `DELETE_ROWS`, and MySQL `PARTIAL_UPDATE_ROWS`) are decoded and applied to SurrealDB. MySQL partial JSON row updates (`binlog_row_value_options=PARTIAL_JSON`) are reconstructed into the final JSON document before the change is applied, so normal MySQL 8 partial JSON logging can stay enabled. Every event's CRC32 checksum is verified before it is parsed, so a corrupt or truncated event fails loudly rather than being silently mis-decoded.

Processing uses an apply-then-commit pattern with **at-least-once delivery** — design SurrealDB writes to be idempotent (upsert by primary key).

## Lifecycle: start, stop, resume, cancel

Every `sync` run — in any `--snapshot-mode` — is designed to be **started, stopped, and restarted at any time** with no data loss beyond the at-least-once boundary. The unifying idea is the durable checkpoint: surreal-sync always knows a binlog position from which replaying every later change (and nothing earlier) reconstructs a correct target.

### Start

| You want to… | Command | Where it starts |
|--------------|---------|-----------------|
| Bulk-load once, stream later | `sync --snapshot-mode only` → `sync --snapshot-mode never` (same checkpoint store) | Snapshot from an empty target; stream picks up the persisted end checkpoint. |
| Migrate **and** keep tracking in one process | `sync` (default `initial`) | Snapshot, then hand off to streaming at the snapshot's consistent end. |
| Stream only, from the current head | `sync --snapshot-mode never` with an empty checkpoint store | Server's current position — only changes **after** start are applied. |
| Stream only, explicit start (advanced) | `sync --snapshot-mode never --from <checkpoint>` | Override the store with an explicit position (`head` or a checkpoint string). |

### Stop / cancel (graceful, at any time)

`SIGINT` (Ctrl-C) or `SIGTERM` stops **any** mode cleanly and **flushes a resumable checkpoint on the way out**:

- **Replication tail phase** (`initial` after snapshot, or `never`) — finishes the in-flight read, persists the latest streamed position, and exits `0`.
- **`only` snapshot** — stops before the next table/chunk boundary. It deliberately does **not** emit `FullSyncEnd`, so the persisted `FullSyncStart` (the master position captured before any rows were copied) remains the resume point.
- **`initial` snapshot** — emits `FullSyncStart` (the streaming lower bound) *before* copying any rows, so a cancel during the snapshot leaves a checkpoint that guarantees no streamed change is missed on the next run.

There is no data loss on cancel: worst case, a restart re-does an unfinished snapshot and re-applies a bounded window of already-applied changes (idempotent by primary key).

### Restart / resume

- **Stream phase** resumes from the checkpoint store (`--checkpoint-dir` or `--checkpoints-surreal-table`). Surreal-sync auto-captures GTID or file+offset positions as it runs; GTID checkpoints also survive failover/repointing (see [Failover and repointing](#failover-and-repointing)).
- **`only` that completed** emits `FullSyncEnd`; `sync --snapshot-mode never` from that checkpoint continues with no gap. A snapshot that was cancelled/crashed mid-run is re-run from scratch, but because `FullSyncStart` was captured **before** the snapshot, no committed change between start and the eventual streaming handoff is lost.
- **`initial` restart** reads `CatchUpProgress` to learn which tables already completed snapshot handoff. Tables still in the effective sync set that are not yet recorded are snapshotted; already-covered tables are skipped (idempotent upserts would be safe, but skipping saves work). If every table in the effective set is already covered, the snapshot phase is skipped and streaming resumes from the latest position (`CatchUpProgress` or `FullSyncEnd`, whichever is ahead). Mid-snapshot crashes resume from `SnapshotProgress` per-chunk checkpoints when available. Ad-hoc snapshots requested during streaming (`snapshot` CLI / execute-snapshot signal) append completed table names to `CatchUpProgress` immediately after each batch — those tables remain in the effective sync set on restart even if you do not add them to `--tables` again.
- **Expanded `--tables` after stop** — restart with additional tables in `--tables`; only the new tables are snapshotted, then streaming continues from the stored position.

Because the streaming lower bound (`FullSyncStart`) is persisted before the snapshot copies rows, changes that happened *during* a snapshot that was later interrupted are still streamed after restart — the interleaved handoff never leaves a gap.

### Consistency & idempotency guarantees

- **Delivery:** at-least-once. A crash between apply and commit may replay a bounded set of changes on restart.
- **Idempotency:** make SurrealDB writes idempotent (upsert by primary key, keyed deletes). Every synced table must have a usable primary key.
- **Interleaved snapshot** converges to a **consistent image at the streaming position** (live log event wins over an overlapping chunk read), not an inconsistent point-in-time dump.
- **No silent downgrade:** a non-empty but unparseable GTID (from a checkpoint string *or* from the server's `@@global.gtid_executed` / `@@global.gtid_binlog_pos` at snapshot time) is a hard error, never a silent fall back to file+offset.

## Checkpoints

Checkpoints record the binlog position surreal-sync has consumed. Resuming replays every change after that position and nothing before it.

### Checkpoint store (default)

Pass exactly one of:

- `--checkpoint-dir <DIR>` — JSON checkpoint files on disk.
- `--checkpoints-surreal-table <TABLE>` — checkpoints in a SurrealDB table (good for containers and supervised services).

Checkpoints are written at snapshot start (`FullSyncStart`), snapshot handoff completion (`FullSyncEnd` — immutable t2 boundary, written once), and during streaming via `CatchUpProgress` (position updated on `--checkpoint-interval`, default 10 seconds; table coverage merged when ad-hoc snapshot batches complete). **Restart the same `sync` command** after any stop — surreal-sync reads the latest position from `CatchUpProgress` when present, otherwise `FullSyncEnd`, otherwise `FullSyncStart`; when both `CatchUpProgress` and `FullSyncEnd` exist, the furthest-ahead position wins.

### Position formats (auto-captured)

Surreal-sync captures and persists positions automatically; operators rarely type these. Optional `--from` accepts an explicit override (`head`, or a checkpoint string). Internally:

- **GTID** — rotation- and failover-safe when GTID mode is enabled (preferred).
- **File + byte offset** — per-server; does not survive failover or purged logs.

Malformed checkpoint strings are hard errors — no silent downgrade from GTID to file+offset.

## Stream stop bounds

By default the stream phase runs until `SIGINT`/`SIGTERM` or a fatal error — there is no hidden timeout.

Optional bounds (mutually exclusive):

- `--stop-after <DURATION>` — wall-clock stop, e.g. `3600s`, `30m`, `300` (bare number = seconds).
- `--stop-at <CHECKPOINT>` — stop once the stream reaches an exact GTID/file position.

## Debezium comparison (brief)

| Concept | Debezium | surreal-sync `mysql-binlog` |
|---------|----------|----------------------------|
| Initial load + CDC | `snapshot.mode=initial` | `sync` (default `--snapshot-mode initial`) |
| CDC only | `snapshot.mode=never` | `sync --snapshot-mode never` |
| Snapshot only | `snapshot.mode=initial_only` / one-shot snapshot | `sync --snapshot-mode only` |
| Ad-hoc table snapshot | `execute-snapshot` signalling | `snapshot` subcommand + signal polling during stream |
| Resume offset | Kafka Connect offsets | Checkpoint store; optional `--from` override |

## Prerequisites

Configure the source server before connecting surreal-sync. Settings are grouped into **required** (CDC is incorrect or impossible without them) and **recommended** (needed for correct type fidelity and robust resume).

### Required — MySQL and MariaDB

| Setting | Value | Why |
|---------|-------|-----|
| `log_bin` | `ON` | Binary logging must be enabled. |
| `binlog_format` | `ROW` | Row-level before/after images; statement/mixed cannot be decoded into row changes. |
| `binlog_row_image` | `FULL` | Emit every column for inserts/updates/deletes so the target row is complete and deletes can be keyed. |
| `server_id` (source) | unique | The source must have a non-zero, unique server id. |
| `server_id` (consumer) | unique | Each surreal-sync consumer needs its own unique replica id (`--server-id`; a random high id is chosen if omitted). Two consumers sharing an id will disconnect each other. |

Minimal privileges for the replication user:

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `REPLICATION SLAVE` | `*.*` | Read the binlog stream. |
| `REPLICATION CLIENT` | `*.*` | `SHOW MASTER STATUS` / `SHOW BINLOG STATUS` to capture start/end positions. |
| `SELECT` | synced tables | Read rows during the snapshot. |
| `INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX` | sync database | Create and manage the `surreal_sync_signal` table (and its index) used for watermarks and ad-hoc snapshots. |

```sql
CREATE USER IF NOT EXISTS 'surreal_sync'@'%' IDENTIFIED BY 'change_me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'surreal_sync'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX
  ON `myapp`.* TO 'surreal_sync'@'%';
FLUSH PRIVILEGES;
```

Every table you select for sync must have a usable primary key. surreal-sync creates a small `surreal_sync_signal` table on the source for watermark signalling; its changes are captured directly from the binlog (no triggers).

### Recommended — MySQL and MariaDB

| Setting | Value | Why |
|---------|-------|-----|
| `binlog_row_metadata` | `FULL` | Include column names and `ENUM`/`SET` label definitions in `TABLE_MAP` events, so surreal-sync resolves enum/set values to their string labels straight from the stream. Without it, label mapping relies on `information_schema` lookups and can drift after `ALTER TABLE`. |
| GTID mode | on | Enables rotation-safe, failover-safe checkpoints (see below). |
| binlog retention | cover downtime | Keep enough binlog history (`binlog_expire_logs_seconds` / `expire_logs_days`) to cover the longest expected gap between runs, otherwise resume fails with a purged-log error. |

### MySQL 8 — GTID mode

```ini
# my.cnf / mysqld
gtid_mode=ON
enforce_gtid_consistency=ON
binlog_row_metadata=FULL
binlog_row_image=FULL
```

With `gtid_mode=ON`, surreal-sync resumes via `COM_BINLOG_DUMP_GTID` and emits MySQL GTID checkpoints during snapshot and streaming.

### MariaDB — GTID mode

MariaDB always assigns GTIDs when binary logging is on; there is no separate "GTID mode" toggle. surreal-sync resumes by GTID the MariaDB-native way (see [MariaDB notes](#mariadb-notes)).

```ini
# my.cnf / mariadbd
binlog_format=ROW
binlog_row_metadata=FULL
binlog_row_image=FULL
gtid_strict_mode=ON   # recommended
```

### TLS for the replication connection

Provide `--tls-mode` to encrypt the replication connection when the server is reachable only over an untrusted network:

- `--tls-mode disabled` (default) — plaintext; use only on a trusted network or VPN.
- `--tls-mode preferred` — use TLS if the server offers it.
- `--tls-mode required` — require TLS; fail if unavailable.
- `--tls-ca <PATH>` / `--tls-cert <PATH>` / `--tls-key <PATH>` — verify the server and/or present a client certificate.

### Binlog compression

surreal-sync transparently decodes MySQL 8's compressed transaction payloads (`TRANSACTION_PAYLOAD`, zstd). If you use per-column compression or a compression scheme surreal-sync does not yet decode, it fails with a clear error rather than importing corrupt data; disable that feature or open an issue:

```sql
SHOW VARIABLES LIKE 'binlog_transaction_compression';
```

## Initial snapshot → streaming handoff

surreal-sync offers two strategies to move from an empty target to a live-tracking one.

### Interleaved snapshot (default, recommended)

The default strategy copies tables in resumable, primary-key-ordered chunks **while** consuming the live binlog (**reconciliation**). It brackets each chunk with low/high **watermarks** written to `surreal_sync_signal` and reconciles overlapping changes (the live log event wins). The result converges to a **consistent image at the handoff position** — not an inconsistent point-in-time dump — and the same process enters the **replication tail** afterward. This is the Debezium "incremental snapshot" model.

Use `sync` for the all-in-one experience, or `sync --snapshot-mode only` followed by `sync --snapshot-mode never` if you want to schedule the two phases separately.

### Sequential snapshot

The sequential strategy captures the master position, dumps each table in one pass, then replays the binlog from the captured start position. It is simpler but produces a monolithic snapshot that is only consistent once the replay catches up. Choose it with `--strategy sequential-snapshot` on `sync`. Both start and end checkpoints record the **real** master position captured via `SHOW MASTER STATUS` (or GTID), so streaming resumes correctly.

See [Full Sync Strategies](design/full-sync-strategies.md) for the consistency guarantee and a full comparison.

### Recommendation

Use the default interleaved `sync` for the happy path. Reach for sequential snapshot only when you specifically want the snapshot and replay decoupled, or for very large tables where a single-pass dump is operationally simpler.

## Running as a long-lived service

With a checkpoint store, surreal-sync is a supervised daemon. Give each instance a stable, unique `--server-id`, persist checkpoints to a mounted volume or a SurrealDB table, and let your supervisor restart it; it resumes from the last checkpoint.

**systemd:**

```ini
[Unit]
Description=surreal-sync mysql-binlog follower
After=network-online.target

[Service]
ExecStart=/usr/local/bin/surreal-sync from mysql-binlog sync \
  --connection-string ${CONNECTION_STRING} \
  --database myapp --tables users,orders \
  --surreal-endpoint ${SURREAL_ENDPOINT} \
  --surreal-username root --surreal-password root \
  --to-namespace production --to-database migrated_data \
  --server-id 100001 \
  --checkpoints-surreal-table sync_checkpoints
Restart=always
RestartSec=5
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
```

**Kubernetes:** run as a single-replica `Deployment` (or `StatefulSet` for a stable identity), set a fixed `--server-id`, store checkpoints in SurrealDB or a `PersistentVolume`, and rely on `terminationGracePeriodSeconds` so the pod can flush a final checkpoint on `SIGTERM`.

## `sync` flags (reference)

| Flag | Default | Purpose |
|------|---------|---------|
| `--snapshot-mode` | `initial` | `initial` \| `never` \| `only` |
| `--checkpoint-dir` / `--checkpoints-surreal-table` | (required for resume) | Durable checkpoint store |
| `--from` | store | Advanced: explicit stream start (`head` or checkpoint string) |
| `--stop-after` | (none) | Wall-clock stream stop (`30m`, `3600s`, `300`) |
| `--stop-at` | (none) | Exact binlog/GTID stop bound |
| `--strategy` | `interleaved-snapshot` | Snapshot algorithm |
| `--chunk-size` | `1024` | Rows per keyset chunk during snapshot |
| `--checkpoint-interval` | `10` | Seconds between stream checkpoint writes |
| `--tables` | all tables | Comma-separated table filter |
| `--server-id` | random | Unique replica id |
| `--flavor` | auto-detect | `mysql` or `mariadb` |

### Snapshot-only example

```bash
surreal-sync from mysql-binlog sync \
  --snapshot-mode only \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --tables "users,orders" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

### Stream-only example

Resume from the checkpoint store (same flags as the happy path, plus `--snapshot-mode never`):

```bash
surreal-sync from mysql-binlog sync \
  --snapshot-mode never \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

Advanced: `--from head` or `--from <checkpoint-string>` overrides the store when you need an explicit start position.

Use `--stop-after` or `--stop-at` for bounded catch-up jobs; omit both for continuous streaming until shutdown.

While streaming, your application can keep writing to MySQL/MariaDB without downtime, as long as binlog retention covers the sync lag.

## Failover and repointing

GTID checkpoints are what make failover safe. Because a GTID identifies a transaction independently of which server or binlog file holds it, you can repoint surreal-sync at a promoted primary or a different replica and resume from the same GTID checkpoint — the new server serves the transactions after that GTID from its own binlog.

- **With GTID:** stop surreal-sync, change `--connection-string` to the new host, and restart with the same checkpoint store. Streaming resumes with no gap or duplication beyond the usual at-least-once boundary.
- **With file+offset only:** a checkpoint like `mysql-bin.000003:195` is meaningless on a different server (binlog file names and offsets are per-server). Repointing requires a fresh snapshot from the new server. Enable GTID mode to avoid this.

## Ad-hoc snapshots (signalling)

While a `sync` follower is streaming, snapshot additional tables on the fly. The `snapshot` command inserts an `execute-snapshot` signal row into `surreal_sync_signal`; the running follower polls for signals during steady-state streaming and snapshots the requested tables while streaming continues:

```bash
surreal-sync from mysql-binlog snapshot \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --tables "new_table,another_table"
```

- `--tables` (required): comma-separated tables to snapshot.

### Standard workflow: add tables without expanding `--tables` on restart

1. Start sync with an initial scope, e.g. `--tables users,orders` and a checkpoint store.
2. While streaming, add more tables with `snapshot --tables products` (no sync restart needed).
3. Stop or crash; restart sync with the **same** `--tables users,orders` (you do not need to remember to add `products`).
4. surreal-sync reads `CatchUpProgress.covered_tables`, sees `products` already snapshotted, skips re-snapshot, and **streams `products` in the replication tail** along with `users` and `orders`.

`--tables` sets the **initial** sync scope. Ad-hoc snapshots extend the **durable** scope via `CatchUpProgress` — on restart the effective sync set is the union of `--tables` and covered tables from the checkpoint. To **stop** syncing a table that is still recorded in `CatchUpProgress`, remove it from `--tables` **and** edit or reset the checkpoint entry (or start a fresh checkpoint store); removing `--tables` alone does not drop tables that were added via ad-hoc snapshot.

## Type fidelity

surreal-sync decodes MySQL/MariaDB types into their natural SurrealDB representations:

| MySQL/MariaDB type | SurrealDB result |
|--------------------|------------------|
| `ENUM` | the string label (not the ordinal), resolved from `binlog_row_metadata=FULL` |
| `SET` | an array/set of string labels |
| `JSON` | a nested object/array (see [MariaDB notes](#mariadb-notes) for MariaDB's `LONGTEXT` alias) |
| `DATE`, `TIME`, `DATETIME`, `TIMESTAMP` | temporal values (UTC-normalized for `TIMESTAMP`) |
| `DECIMAL`/`NUMERIC` | exact decimal (no float rounding) |
| `BIT` | integer/bit value |
| Geometry (`POINT`, `GEOMETRY`, …) | geometry value |
| `TINYINT(1)` | boolean (configurable) |

Keep `binlog_row_metadata=FULL` so `ENUM`/`SET` labels resolve directly from the stream and stay correct across `ALTER TABLE`.

See [MySQL Data Types](mysql-data-types.md) for the complete mapping; MariaDB uses the same mappings.

## Schema changes

surreal-sync tolerates most online schema evolution while syncing. How a change is handled depends on whether it is observed on the **stream** (`incremental`, and the streaming phase of `sync`) or during a **snapshot** (`full`, and the snapshot phase of interleaved `sync`).

### Schema changes during streaming

surreal-sync observes DDL in the stream (`QUERY` events such as `ALTER TABLE` and `RENAME TABLE`) and refreshes its cached table schema and column metadata **before** applying subsequent row events. This keeps column-name and `ENUM`/`SET` label resolution correct after an online `ALTER`. A `RENAME TABLE` of a synced table is followed automatically: the synced-table set is rewritten to the new name so post-rename row events are still applied (not silently filtered out).

Row events are never silently dropped: if a row event arrives without the preceding `TABLE_MAP` that describes its layout (which can only happen on a protocol violation or an unhandled schema edge case), surreal-sync fails loudly instead of skipping the change.

### Schema changes during full / interleaved snapshot

The snapshot copies each table in primary-key-ordered chunks. While that runs, the interleaved strategy is **also** consuming the live binlog, so DDL that happens during the snapshot is observed on the stream side and refreshes the cached schema/label metadata exactly as during steady-state streaming. Because each chunk is bracketed by low/high **watermarks** and overlapping changes are reconciled with the live log event winning, an `ALTER` that lands mid-snapshot is reconciled by the streamed post-`ALTER` row image rather than the stale chunk read.

Practical guidance:

- **Additive / label changes** (add column, add enum/set label) are safe during a snapshot; the streamed image carries the new shape.
- **Breaking changes** (drop the primary key, rename a table away from the synced set, or otherwise change the keyset ordering) during a large snapshot are best run in a maintenance window: pause `sync`, apply the DDL, then restart (the interleaved handoff guarantees no streamed change is missed across the restart).
- The **sequential** strategy dumps each table in a single `SELECT` pass with no live refresh *during that pass*; if you must run breaking DDL against a huge table, prefer the interleaved strategy or a maintenance window.

### Behavior matrix

Expected behavior for common DDL, for the streaming path (`incremental` / `sync` streaming) and the snapshot path (`full` / `sync` snapshot):

| DDL | Streaming (`incremental` / `sync`) | Snapshot (`full` / interleaved) |
|-----|-------------------------------------|----------------------------------|
| **Add column** | Continue; schema refreshed, new column appears on later rows. | Continue; streamed post-DDL image carries the column. |
| **Drop column** | Continue; dropped column stops appearing. | Continue. |
| **Rename column** | Continue; new name used for later rows (old values remain in the target under the old field until overwritten). | Continue. |
| **Rename table** (synced) | Continue; synced set follows the rename to the new name. | Continue (streamed); coordinate for very large tables. |
| **Change enum/set labels** | Continue; labels re-resolved from refreshed metadata. Out-of-range indexes keep the raw value and emit a `WARN` (data-quality signal). | Continue. |
| **Drop primary key** | Clear error — a keyable primary key is required; fix schema or re-snapshot. | Clear error at schema collection for the affected synced table. |
| **Create new table** | Not synced automatically; use [`snapshot`](#ad-hoc-snapshots-signalling) to add it to a running follower. | Snapshot only covers `--tables` selected at start; use `snapshot` signalling to add more. |

**Primary keys.** Every synced table must have a usable primary key. A synced table without one is rejected with a clear error on the snapshot path (`full`/interleaved), and its change events cannot be keyed on the streaming path. (Internally, PK-less tables that are merely *present* in the database but not synced do not block a run.)

## Operational concerns

- **Monitoring lag.** Compare the consumer's committed position against the server head (`SHOW MASTER STATUS` / `SHOW BINLOG STATUS`, or `gtid_executed` / `@@global.gtid_binlog_pos`). In follow mode the persisted checkpoint is the authoritative "how far have we gotten" marker.
- **Binlog rotation / expiry gaps.** If the server purges a binlog file the checkpoint still needs, resume fails. Size retention to cover downtime; use GTID so rotation itself never invalidates a checkpoint.
- **`server_id` uniqueness.** Every consumer needs a distinct id; collisions cause replicas to drop each other. Pin `--server-id` for long-lived services.
- **Restart semantics (at-least-once).** On restart surreal-sync resumes from the last committed checkpoint and may re-apply the in-flight transaction. Make SurrealDB writes idempotent (upsert by primary key, keyed deletes).
- **TLS.** Use `--tls-mode required` for replication over untrusted networks.

## MariaDB notes

MariaDB behaves like MySQL for surreal-sync binlog CDC, with these nuances:

**JSON columns.** MariaDB implements `JSON` as an **alias for `LONGTEXT`** (with an automatic `json_valid(...)` `CHECK` constraint) rather than a native type. In binlog ROW events, JSON values arrive as text. surreal-sync **detects JSON columns** on both engines — for MySQL via `information_schema.COLUMNS` (`DATA_TYPE = 'json'`), and for MariaDB via the `json_valid(...)` `CHECK` constraints — and parses them into nested objects on every read path (full snapshot, interleaved snapshot, and the binlog stream). Nested JSON therefore syncs to real SurrealDB objects on MariaDB just as on MySQL.

**Position tracking on MariaDB 11.4+.** Transaction events on MariaDB 11.4+ may report `log_pos = 0` in event headers. surreal-sync computes durable positions from file offset and event size instead, so `binlog_legacy_event_pos=ON` is **not** required.

**GTID resume.** MariaDB does not implement MySQL's `COM_BINLOG_DUMP_GTID`. surreal-sync resumes the MariaDB-native way: it reads the current position from `@@global.gtid_binlog_pos`, sets the session variables `@slave_connect_state` and `@slave_gtid_ignore_duplicates`, then issues a plain `COM_BINLOG_DUMP` with an empty filename and position 4 — the server streams from the connect state. Runtime checkpoints accumulate as MariaDB GTID lists (e.g. `gtid:0-1-270`); multi-domain positions round-trip as comma-separated lists (`gtid:0-1-270,1-7-42`) through the CLI and checkpoint store. `@slave_gtid_strict_mode` is left at the server default unless `--mariadb-gtid-strict-mode on` or `--mariadb-gtid-strict-mode off` is provided.

## Manual smoke test

Quick end-to-end check with Docker. Image tags match `scripts/test-images.env` (same as `make test` and CI). Health checks and SQL use **`docker exec`** — host `mysql`/`mysqladmin` are not required. Smoke runs use bounded **`--stop-after`**; omit stop flags for long-lived replication.

### Shared setup

```bash
set -a && source scripts/test-images.env && set +a
cargo build
export SYNC=./target/debug/surreal-sync

docker rm -f surreal-smoke mysql-binlog-smoke mariadb-binlog-smoke 2>/dev/null
docker run --name surreal-smoke -p 8000:8000 -d surrealdb/surrealdb:v2.6.5 \
  start --user root --pass root memory

export SURREAL_URL=http://127.0.0.1:8000
export CHECKPOINT_DIR=.surreal-sync-checkpoints-smoke
export DB=myapp
```

### MySQL 8.0 (port 3306)

```bash
docker run --name mysql-binlog-smoke \
  -e MYSQL_ROOT_PASSWORD=testpass \
  -e MYSQL_DATABASE=myapp \
  -p 3306:3306 -d "$MYSQL_BINLOG_IMAGE" \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --gtid-mode=ON \
  --enforce-gtid-consistency=ON \
  --server-id=1 \
  --log-slave-updates=ON \
  --binlog-row-value-options=

until docker exec mysql-binlog-smoke mysqladmin ping -h127.0.0.1 -uroot -ptestpass --silent 2>/dev/null; do sleep 1; done

docker exec -i mysql-binlog-smoke mysql -uroot -ptestpass myapp <<'SQL'
CREATE TABLE IF NOT EXISTS users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL
);
INSERT INTO users (name) VALUES ('alice');
CREATE USER IF NOT EXISTS 'surreal_sync'@'%'
  IDENTIFIED WITH mysql_native_password BY 'surreal_sync_pass';
SET GLOBAL binlog_row_value_options = '';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'surreal_sync'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX
  ON myapp.* TO 'surreal_sync'@'%';
FLUSH PRIVILEGES;
SQL

export MYSQL_URL=mysql://surreal_sync:surreal_sync_pass@127.0.0.1:3306/myapp

rm -rf "$CHECKPOINT_DIR"
$SYNC from mysql-binlog sync \
  --snapshot-mode only \
  --connection-string "$MYSQL_URL" \
  --database "$DB" --tables users \
  --surreal-endpoint "$SURREAL_URL" --surreal-username root --surreal-password root \
  --to-namespace test --to-database mysql_binlog \
  --server-id 100001 --checkpoint-dir "$CHECKPOINT_DIR"

docker exec -i mysql-binlog-smoke mysql -uroot -ptestpass myapp \
  -e "INSERT INTO users (name) VALUES ('bob');"

$SYNC from mysql-binlog sync \
  --snapshot-mode never \
  --connection-string "$MYSQL_URL" \
  --database "$DB" --tables users \
  --surreal-endpoint "$SURREAL_URL" --surreal-username root --surreal-password root \
  --to-namespace test --to-database mysql_binlog \
  --server-id 100001 --checkpoint-dir "$CHECKPOINT_DIR" \
  --stop-after 25s
```

### MariaDB 11.4 (port 3307)

MariaDB images ship `mariadb-admin` / `mariadb` instead of `mysqladmin` / `mysql`.

```bash
docker run --name mariadb-binlog-smoke \
  -e MYSQL_ROOT_PASSWORD=testpass \
  -e MYSQL_DATABASE=myapp \
  -p 3307:3306 -d "$MARIADB_BINLOG_IMAGE" \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --server-id=1 \
  --gtid-strict-mode=ON

until docker exec mariadb-binlog-smoke mariadb-admin ping -h127.0.0.1 -uroot -ptestpass --silent 2>/dev/null; do sleep 1; done

docker exec -i mariadb-binlog-smoke mariadb -uroot -ptestpass myapp <<'SQL'
CREATE TABLE IF NOT EXISTS users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL
);
INSERT INTO users (name) VALUES ('alice');
CREATE USER IF NOT EXISTS 'surreal_sync'@'%' IDENTIFIED BY 'surreal_sync_pass';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'surreal_sync'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX
  ON myapp.* TO 'surreal_sync'@'%';
FLUSH PRIVILEGES;
SQL

export MARIADB_URL=mysql://surreal_sync:surreal_sync_pass@127.0.0.1:3307/myapp

rm -rf "$CHECKPOINT_DIR"
$SYNC from mysql-binlog sync \
  --snapshot-mode only \
  --connection-string "$MARIADB_URL" \
  --database "$DB" --tables users \
  --flavor mariadb --mariadb-gtid-strict-mode on \
  --surreal-endpoint "$SURREAL_URL" --surreal-username root --surreal-password root \
  --to-namespace test --to-database mariadb_binlog \
  --server-id 100002 --checkpoint-dir "$CHECKPOINT_DIR"

docker exec -i mariadb-binlog-smoke mariadb -uroot -ptestpass myapp \
  -e "INSERT INTO users (name) VALUES ('bob');"

$SYNC from mysql-binlog sync \
  --snapshot-mode never \
  --connection-string "$MARIADB_URL" \
  --database "$DB" --tables users \
  --flavor mariadb --mariadb-gtid-strict-mode on \
  --surreal-endpoint "$SURREAL_URL" --surreal-username root --surreal-password root \
  --to-namespace test --to-database mariadb_binlog \
  --server-id 100002 --checkpoint-dir "$CHECKPOINT_DIR" \
  --stop-after 25s
```

### Verify (optional)

```bash
curl -s -X POST "$SURREAL_URL/sql" -u root:root \
  -H "Accept: application/json" -H "NS: test" -H "DB: mysql_binlog" \
  --data "SELECT * FROM users;"
```

Automated e2e: `make build-debug && cargo test -p surreal-sync --test mysql_binlog -- --nocapture`

## Troubleshooting

### Binlog consumer cannot connect

Verify grants and that binary logging is enabled:

```sql
SHOW GRANTS FOR 'surreal_sync'@'%';
SHOW MASTER STATUS;      -- or SHOW BINLOG STATUS on newer MariaDB
```

### Checkpoint ahead of available binlog

If resume fails because the binlog file was purged, take a fresh `full`/`sync` from the current position and increase `binlog_expire_logs_seconds` / `expire_logs_days` to cover expected downtime. GTID checkpoints avoid file-rotation failures but not purge-past-retention failures.

### Enum/set values look wrong after an ALTER

Ensure `binlog_row_metadata=FULL` so labels ship with the stream. surreal-sync refreshes schema on observed DDL; if you bypass the stream (out-of-band restore), restart the sync so it re-reads the schema.

### Duplicate or replayed changes

Binlog CDC is at-least-once. If a batch fails after apply but before commit, events may replay on restart. Make SurrealDB writes idempotent (upsert by primary key).

## Data type support

See [MySQL Data Types](mysql-data-types.md); MariaDB uses the same mappings (including the JSON handling in [MariaDB notes](#mariadb-notes)).

## References

- Implementation: [crates/mysql-binlog-source/](../crates/mysql-binlog-source/) and [crates/binlog-protocol/](../crates/binlog-protocol/)
- [Full Sync Strategies](design/full-sync-strategies.md)
- [MySQL Replication Documentation](https://dev.mysql.com/doc/refman/8.0/en/replication.html)
- [MariaDB Binary Log Documentation](https://mariadb.com/kb/en/binary-log/)
