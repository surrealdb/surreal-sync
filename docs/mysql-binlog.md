# MySQL/MariaDB Binlog Source

`surreal-sync from mysql-binlog` replicates MySQL and MariaDB tables into SurrealDB using the binary log (binlog) as a change-data-capture (CDC) stream. It registers as a replica, reads row-format binlog events, and applies them to SurrealDB — no triggers or audit tables on the source. The same subcommand works against both engines; surreal-sync auto-detects the flavor from `SELECT @@version` (override with `--flavor mysql` or `--flavor mariadb`).

The end-to-end model mirrors a mature CDC pipeline (Debezium-style **snapshot → stream → resume**), grounded in this project's commands and traits:

1. **Snapshot** the selected tables into SurrealDB.
2. **Stream** binlog changes and apply them, converging the target to a consistent image.
3. **Resume** from a durable checkpoint (GTID or file+offset) after any restart, failover, or repointing.

> **Trigger-based alternative:** If you cannot enable binlog replication or prefer audit triggers, see [Surreal-Sync for MySQL](mysql.md) and [Surreal-Sync for MariaDB](mariadb.md).

## The happy path

For a new migration that should also keep tracking live changes, run the combined `sync` command in continuous **follow** mode:

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
  --follow \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

This performs an interleaved snapshot, hands off to streaming in the same process, persists checkpoints as it goes, and keeps running until it receives a shutdown signal. Point it at a checkpoint store (`--checkpoint-dir` or `--checkpoints-surreal-table`) so it can resume where it left off.

Everything else in this document explains the pieces behind that command and the operational modes around it.

## How it works

`surreal-sync from mysql-binlog` exposes four subcommands:

| Command | Purpose |
|---------|---------|
| `sync` | Snapshot **and** stream in one process (recommended). |
| `full` | One-time snapshot; emits a resumable end checkpoint. |
| `incremental` | Stream changes starting from a checkpoint. |
| `snapshot` | Signal a running `sync`/`incremental` to snapshot extra tables on the fly. |

Changes are read by registering as a binlog replica (`COM_REGISTER_SLAVE` followed by `COM_BINLOG_DUMP`, or `COM_BINLOG_DUMP_GTID` on MySQL 8). Row-format events (`WRITE_ROWS`, `UPDATE_ROWS`, `DELETE_ROWS`, and MySQL `PARTIAL_UPDATE_ROWS`) are decoded and applied to SurrealDB. MySQL partial JSON row updates (`binlog_row_value_options=PARTIAL_JSON`) are reconstructed into the final JSON document before the change is applied, so normal MySQL 8 partial JSON logging can stay enabled. Every event's CRC32 checksum is verified before it is parsed, so a corrupt or truncated event fails loudly rather than being silently mis-decoded.

Processing uses an apply-then-commit pattern with **at-least-once delivery** — design SurrealDB writes to be idempotent (upsert by primary key).

## Lifecycle: start, stop, resume, cancel

Every mode — `full`, `incremental`, and the combined `sync` — is designed to be **started, stopped, and restarted at any time** with no data loss beyond the at-least-once boundary. The unifying idea is the durable checkpoint: surreal-sync always knows a binlog position from which replaying every later change (and nothing earlier) reconstructs a correct target.

### Start

| You want to… | Command | Where it starts |
|--------------|---------|-----------------|
| Bulk-load once, stream later | `full` (writes an end checkpoint) → `incremental --incremental-from <end>` | Snapshot from an empty target; incremental from the snapshot's end position. |
| Migrate **and** keep tracking in one process | `sync` (interleaved snapshot → streaming) | Snapshot, then hand off to streaming at the snapshot's consistent end. |
| Stream only, from the current head | `incremental --incremental-from head` (or an empty checkpoint store) | The server's current position — only changes **after** start are applied. |
| Stream only, from a known point | `incremental --incremental-from <gtid\|file:pos>` | Exactly that checkpoint. |

### Stop / cancel (graceful, at any time)

`SIGINT` (Ctrl-C) or `SIGTERM` stops **any** mode cleanly and **flushes a resumable checkpoint on the way out**:

- **`incremental` / streaming phase of `sync`** — finishes the in-flight read, persists the latest streamed position, and exits `0`.
- **`full` snapshot** — stops before the next table/chunk boundary. It deliberately does **not** emit `FullSyncEnd`, so the persisted `FullSyncStart` (the master position captured before any rows were copied) remains the resume point.
- **Interleaved `sync` snapshot** — emits `FullSyncStart` (the streaming lower bound) *before* copying any rows, so a cancel during the snapshot leaves a checkpoint that guarantees no streamed change is missed on the next run.

There is no data loss on cancel: worst case, a restart re-does an unfinished snapshot and re-applies a bounded window of already-applied changes (idempotent by primary key).

### Restart / resume

- **`incremental`** resumes from the persisted GTID/file checkpoint (from `--checkpoints-surreal-table`/`--checkpoint-dir`, or an explicit `--incremental-from`). GTID checkpoints also survive failover/repointing (see [Failover and repointing](#failover-and-repointing)).
- **`full`** that completed emits `FullSyncEnd`; re-running `incremental` from that checkpoint continues with no gap. A `full` that was cancelled/crashed mid-snapshot is re-run from scratch, but because `FullSyncStart` was captured **before** the snapshot, no committed change between start and the eventual streaming handoff is lost.
- **`sync`** restart re-runs the snapshot (idempotent upserts) and resumes streaming from the checkpoint. Because the streaming lower bound (`FullSyncStart`) is persisted before the snapshot copies rows, changes that happened *during* a snapshot that was later interrupted are still streamed after restart — the interleaved handoff never leaves a gap.

### Consistency & idempotency guarantees

- **Delivery:** at-least-once. A crash between apply and commit may replay a bounded set of changes on restart.
- **Idempotency:** make SurrealDB writes idempotent (upsert by primary key, keyed deletes). Every synced table must have a usable primary key.
- **Interleaved snapshot** converges to a **consistent image at the streaming position** (live log event wins over an overlapping chunk read), not an inconsistent point-in-time dump.
- **No silent downgrade:** a non-empty but unparseable GTID (from a checkpoint string *or* from the server's `@@global.gtid_executed` / `@@global.gtid_binlog_pos` at snapshot time) is a hard error, never a silent fall back to file+offset.

## Checkpoints

A checkpoint is a durable record of the exact binlog position surreal-sync has consumed. Resuming from a checkpoint replays every change after that position and nothing before it. Two forms exist:

- **File + byte offset** (`file:mysql-bin.000003:195`) — works on both MySQL and MariaDB. Tied to a specific binlog file on a specific server; it does **not** survive failover to a different server, and breaks if the referenced file has been purged.
- **GTID** (global transaction identifier) — server-independent and rotation-safe, so it is preferred whenever GTID mode is enabled:
  - **MySQL** uses a UUID-based GTID set: `gtid:d4c17f0c-8c11-11e1-9ed1-0800270a0001:1-107`.
  - **MariaDB** uses `domain-server-sequence` form: `gtid:0-1-270`, and a comma-separated list `gtid:0-1-270,1-7-42` for multiple replication domains.

surreal-sync emits GTID checkpoints during both snapshot and streaming whenever the server runs in GTID mode, and file+offset checkpoints otherwise. GTID and file+offset checkpoints are interchangeable inputs to `incremental --incremental-from` and to a checkpoint store.

### Checkpoint stores

Pass exactly one of:

- `--checkpoint-dir <DIR>` — JSON checkpoint files on disk.
- `--checkpoints-surreal-table <TABLE>` — checkpoints stored in a SurrealDB table (convenient for long-lived services and containers).

Checkpoints are written at snapshot start (`t1`), snapshot end (`t2`), and periodically while streaming (see [Continuous vs batch operation](#continuous-vs-batch-operation)).

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

The default strategy copies tables in resumable, primary-key-ordered chunks **while** consuming the live binlog. It brackets each chunk with low/high **watermarks** written to `surreal_sync_signal` and reconciles overlapping changes (the live log event wins). The result converges to a **consistent image at the streaming position** — not an inconsistent point-in-time dump — and the same process keeps tracking afterward. This is the Debezium "incremental snapshot" model.

Use `sync` for the all-in-one experience, or `full` (default strategy) followed by `incremental` if you want to schedule the two phases separately.

### Sequential snapshot

The sequential strategy captures the master position, dumps each table in one pass, then replays the binlog from the captured start position. It is simpler but produces a monolithic snapshot that is only consistent once the replay catches up. Choose it with `--strategy sequential-snapshot` on `full`. Both start and end checkpoints record the **real** master position captured via `SHOW MASTER STATUS` (or GTID), so `incremental` resumes correctly.

See [Full Sync Strategies](design/full-sync-strategies.md) for the consistency guarantee and a full comparison.

### Recommendation

Use the default interleaved `sync` for the happy path. Reach for sequential snapshot only when you specifically want the snapshot and replay decoupled, or for very large tables where a single-pass dump is operationally simpler.

## Continuous vs batch operation

surreal-sync distinguishes two explicit operating modes for `sync` and `incremental`, rather than relying on an implicit idle timeout:

### Follow mode (continuous / daemon)

`--follow` streams **indefinitely**. It blocks on the binlog waiting for new events, applies them as they arrive, and persists checkpoints periodically (`--checkpoint-interval <SECONDS>`, default 10) and on graceful shutdown. It exits only on `SIGINT`/`SIGTERM` (flushing a final checkpoint first) or on a fatal error. This is the mode for running surreal-sync as a long-lived service.

### Batch mode (bounded catch-up)

`--batch` streams until it reaches a bound, then exits cleanly — ideal for scheduled/cron catch-up jobs:

- `--until <checkpoint>` — stop once the given GTID/position is reached (exact, deterministic bound).
- `--timeout <SECONDS>` — stop after a wall-clock budget.

If neither `--follow` nor `--batch` is given, `sync` defaults to `--follow` and `incremental` defaults to `--batch` with the configured timeout.

### Running as a long-lived service

In follow mode with a checkpoint store, surreal-sync is a supervised daemon. Give each instance a stable, unique `--server-id`, persist checkpoints to a mounted volume or a SurrealDB table, and let your supervisor restart it; it resumes from the last checkpoint.

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
  --server-id 100001 --follow \
  --checkpoints-surreal-table sync_checkpoints
Restart=always
RestartSec=5
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
```

**Kubernetes:** run as a single-replica `Deployment` (or `StatefulSet` for a stable identity), set a fixed `--server-id`, store checkpoints in SurrealDB or a `PersistentVolume`, and rely on `terminationGracePeriodSeconds` so the pod can flush a final checkpoint on `SIGTERM`.

## Full sync

Use `full` for a one-time snapshot without immediately continuing to stream — for example, to bulk-load and run `incremental` on a schedule later.

```bash
surreal-sync from mysql-binlog full \
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

Provide a checkpoint store so `incremental` can resume from the end position. Example log:

```
INFO checkpoint: Emitted FullSyncEnd checkpoint: gtid:d4c17f0c-8c11-11e1-9ed1-0800270a0001:1-107
```

With the interleaved strategy, **both start and end checkpoints record the same consistent end position** — start `incremental` from either. With the sequential strategy, the start checkpoint is the real master position captured before the dump and the end checkpoint is the position after it.

Flags: `--chunk-size <N>` (rows per keyset chunk, default 1024), `--strategy interleaved-snapshot|sequential-snapshot`, `--server-id <N>`, `--flavor mysql|mariadb`, `--mariadb-gtid-strict-mode server-default|on|off`.

## Incremental sync

Prefer [`sync`](#the-happy-path) for a new migration. If you already ran `full`, use `incremental` to continue tracking from the end position:

```bash
CHECKPOINT="gtid:d4c17f0c-8c11-11e1-9ed1-0800270a0001:1-107"

surreal-sync from mysql-binlog incremental \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --incremental-from "$CHECKPOINT" \
  --follow
```

Or read the checkpoint from a SurrealDB table instead of `--incremental-from`:

```bash
surreal-sync from mysql-binlog incremental \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --checkpoints-surreal-table "sync_checkpoints" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --follow
```

`--incremental-from` accepts these forms (the `mysql-binlog:` prefix is implied by the subcommand and optional):

| Form | Example | Engine |
|------|---------|--------|
| File + offset | `file:mysql-bin.000003:195` | MySQL and MariaDB |
| MySQL GTID set | `gtid:d4c17f0c-8c11-11e1-9ed1-0800270a0001:1-107` | MySQL 8 (GTID mode) |
| MariaDB GTID | `gtid:0-1-270` | MariaDB |
| MariaDB multi-domain GTID | `gtid:0-1-270,1-7-42` | MariaDB |

In addition, `--incremental-from head` starts streaming from the server's **current** position (only changes after start are applied). The same "start at head" behavior applies when a checkpoint store is configured but empty (first run against a fresh store): surreal-sync captures the current master position and streams forward from there.

A malformed checkpoint string is a **hard error** — surreal-sync never silently downgrades a GTID checkpoint to file+offset, because that could skip or replay changes. An empty or `head` value explicitly means **start at the current server head** (captured via GTID when GTID mode is on, otherwise file+offset), not a silent no-op.

Use `--follow` for continuous tracking or `--batch` with `--until <checkpoint>` / `--timeout <SECONDS>` for a bounded run.

While incremental sync runs, your application can keep writing to MySQL/MariaDB without downtime, as long as binlog retention covers the sync lag.

## Failover and repointing

GTID checkpoints are what make failover safe. Because a GTID identifies a transaction independently of which server or binlog file holds it, you can repoint surreal-sync at a promoted primary or a different replica and resume from the same GTID checkpoint — the new server serves the transactions after that GTID from its own binlog.

- **With GTID:** stop surreal-sync, change `--connection-string` to the new host, and restart with the same checkpoint store. Streaming resumes with no gap or duplication beyond the usual at-least-once boundary.
- **With file+offset only:** a checkpoint like `mysql-bin.000003:195` is meaningless on a different server (binlog file names and offsets are per-server). Repointing requires a fresh snapshot from the new server. Enable GTID mode to avoid this.

## Ad-hoc snapshots (signalling)

While a `sync`/`incremental` follower is streaming, snapshot additional tables on the fly. The `snapshot` command inserts an `execute-snapshot` signal row into `surreal_sync_signal`; the running follower picks it up and snapshots the requested tables while streaming continues:

```bash
surreal-sync from mysql-binlog snapshot \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --tables "new_table,another_table"
```

- `--tables` (required): comma-separated tables to snapshot.

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
