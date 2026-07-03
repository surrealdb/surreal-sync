# MySQL/MariaDB Binlog Source

`surreal-sync from mysql-binlog` exports MySQL and MariaDB tables to SurrealDB using binlog-based logical replication. The same subcommand works against both engines; surreal-sync auto-detects the flavor from `SELECT @@version` (override with `--flavor mysql` or `--flavor mariadb` when needed).

The default **interleaved-snapshot** workflow copies tables in resumable, primary-key-ordered chunks while continuously consuming the binlog change stream. Watermark reconciliation aligns each chunk with live changes (the log event wins), so the target converges to a **consistent image at the end position** and can keep tracking live — it is not an inconsistent monolithic dump. Use the combined `sync` command for a single-process snapshot plus incremental handoff.

> **Trigger-based alternative:** If you cannot enable binlog replication or prefer audit triggers, see [Surreal-Sync for MySQL](mysql.md) and [Surreal-Sync for MariaDB](mariadb.md).

> **Other strategies:** For the legacy sequential-snapshot workflow (inconsistent monolithic snapshot plus a separate incremental replay from t1), pass `--strategy sequential-snapshot` to `full`. See [Full Sync Strategies](design/full-sync-strategies.md).

## How It Works

`surreal-sync from mysql-binlog` supports `full`, `incremental`, a combined `sync`, and an ad-hoc `snapshot` command.

Changes are read by registering as a binlog replica (`COM_REGISTER_SLAVE` / `COM_BINLOG_DUMP` or GTID-based dump on MySQL 8). Row-format binlog events (`WRITE_ROWS`, `UPDATE_ROWS`, `DELETE_ROWS`) are decoded and applied to SurrealDB. Processing uses an apply-then-commit pattern with **at-least-once delivery** — ensure your target handling is idempotent.

Checkpoints record a resumable binlog position:

- **File + byte offset** (`file:mysql-bin.000003:195`) — works on both MySQL and MariaDB.
- **GTID** — MySQL uses a UUID-based GTID set (`gtid:uuid:1-107`); MariaDB uses domain-server-sequence form (`gtid:0-1-270`, and a comma-separated list `gtid:0-1-270,1-7-42` for multiple domains). When GTID mode is enabled on the server, GTID checkpoints are preferred because they survive binlog file rotation cleanly. On MariaDB, surreal-sync captures the runtime position from `@@global.gtid_binlog_pos` and emits GTID checkpoints by default (see [MariaDB notes](#mariadb-notes)).

See [Full Sync Strategies](design/full-sync-strategies.md) for the consistency guarantee and strategy comparison.

## Prerequisites

Configure the source server before connecting surreal-sync.

### Both MySQL and MariaDB

- `log_bin=ON` — binary logging enabled.
- `binlog_format=ROW` — row-level change events (required for correct CDC).
- A **unique `server_id`** on each binlog consumer (`--server-id`; surreal-sync picks a random id if omitted). The source server also needs its own unique `server_id`.
- A replication-capable database user with at least:
  - `REPLICATION SLAVE` and `REPLICATION CLIENT` on `*.*`
  - `SELECT` on tables you sync (plus `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP` on the database for the signal table and schema operations during full sync)
- Every table you select for sync must have a usable primary key.
- `surreal-sync` writes a small `surreal_sync_signal` table on the source for watermark signalling (captured directly from binlog ROW events — no triggers required).

Example replication user (adjust database name and host as needed):

```sql
CREATE USER IF NOT EXISTS 'surreal_sync'@'%' IDENTIFIED BY 'change_me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'surreal_sync'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX
  ON `myapp`.* TO 'surreal_sync'@'%';
FLUSH PRIVILEGES;
```

Verify binlog settings:

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'server_id';
```

### MySQL 8

GTID mode is recommended for production:

```sql
-- my.cnf / mysqld flags
gtid_mode=ON
enforce_gtid_consistency=ON
```

Binlog compression (MySQL 8.0.20+) may require additional handling; disable it for v1 if you encounter `TRANSACTION_PAYLOAD` events that cannot be decoded:

```sql
SHOW VARIABLES LIKE 'binlog_transaction_compression';
-- set binlog_transaction_compression=OFF if needed
```

### MariaDB 11.4+

MariaDB 11.4 changed transaction event headers so `log_pos` in event headers may be zero. surreal-sync tracks position using file offset and event size instead — **`binlog_legacy_event_pos=ON` is not required**.

GTID strict mode is recommended:

```sql
-- my.cnf / mariadbd flags
gtid_strict_mode=ON
```

### Limitations

- **TLS/SSL** to the binlog stream is not supported in v1; connect over a trusted network or VPN.
- The consumer must stay connected or resume from a persisted checkpoint; binlog retention on the server must cover any gap between runs.

## Combined sync (recommended)

The `sync` command is the standard entry point. It runs the watermark snapshot and continues incremental sync from the handed-off end position in one process — no separate incremental replay pass is needed to reach consistency.

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
  --timeout 3600
```

- `--chunk-size <N>`: rows read per keyset chunk during the snapshot phase (default 1024).
- `--timeout <SECONDS>`: how long the incremental phase runs after the snapshot completes (default 3600).
- `--tables`: comma-separated tables (empty means all user tables in the database).
- `--server-id <N>`: unique replica id for this consumer (optional; random if omitted).
- `--flavor mysql|mariadb`: override auto-detected engine flavor (optional).

## Full Sync

Use `full` when you want a one-time snapshot without immediately continuing incremental sync — for example, to bulk-load data and run `incremental` on a schedule later. Interleaved-snapshot is the default (`--chunk-size`, default 1024).

```bash
export CONNECTION_STRING="mysql://surreal_sync:change_me@mysql:3306/myapp"
export SURREAL_ENDPOINT="ws://localhost:8000"

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

Provide a checkpoint store (`--checkpoint-dir` or `--checkpoints-surreal-table`) so `incremental` can resume from the end position.

Example log output:

```
INFO checkpoint: Emitted FullSyncEnd checkpoint: file:mysql-bin.000003:195
```

With interleaved-snapshot, **both start and end checkpoints record the same consistent end position** — start `incremental` from either file.

## Incremental Sync

Prefer [`sync`](#combined-sync-recommended) when starting a new migration. If you already ran `full`, use `incremental` to continue live tracking from the end position (not a replay pass to fix inconsistency):

```bash
# Use the checkpoint string from the full sync log (file: or gtid: form)
CHECKPOINT="file:mysql-bin.000003:195"

surreal-sync from mysql-binlog incremental \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --incremental-from "$CHECKPOINT" \
  --timeout 3600
```

Alternatively, read the checkpoint from a SurrealDB table instead of `--incremental-from`:

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
  --timeout 3600
```

`--incremental-from` accepts these checkpoint forms (the `mysql-binlog` database type prefix is implied by the subcommand):

| Form | Example | Engine |
|------|---------|--------|
| File + offset | `file:mysql-bin.000003:195` | MySQL and MariaDB |
| MySQL GTID set | `gtid:d4c17f0c-8c11-11e1-9ed1-0800270a0001:1-107` | MySQL 8 (GTID mode) |
| MariaDB GTID | `gtid:0-1-270` | MariaDB |

`--incremental-to` stops at a specific checkpoint (optional). `--timeout` controls how long the run continues (useful for batched or scheduled runs).

While incremental sync is running, your application can continue writing to MySQL or MariaDB without downtime, as long as the source can serve the workload and binlog retention covers the sync lag.

## Ad-hoc Snapshots (Signalling)

While a `sync` is streaming, you can snapshot additional tables on the fly. The `snapshot` command inserts an `execute-snapshot` signal row into `surreal_sync_signal`; the running `sync` picks it up and snapshots the requested tables while streaming continues:

```bash
surreal-sync from mysql-binlog snapshot \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --tables "new_table,another_table"
```

- `--tables` (required): comma-separated tables to snapshot.

## MariaDB notes

MariaDB behaves identically to MySQL for surreal-sync binlog CDC with two nuances:

**JSON columns.** MariaDB implements the `JSON` type as an **alias for `LONGTEXT`** (with an automatic `json_valid(...)` `CHECK` constraint) rather than as a native JSON type. In binlog ROW events, JSON column values arrive as text. surreal-sync **detects JSON columns** on both engines — for MySQL via `information_schema.COLUMNS` (`DATA_TYPE = 'json'`), and for MariaDB via the `json_valid(...)` `CHECK` constraints recorded in `information_schema` — and parses those columns into nested objects on every read path (full snapshot, interleaved snapshot, and the binlog incremental stream). Nested JSON therefore syncs to real SurrealDB objects on MariaDB just as it does on MySQL, with no extra configuration.

**MariaDB 11.4+ position tracking.** Transaction events on MariaDB 11.4+ may report `log_pos = 0` in event headers. surreal-sync uses file offset and event size to compute durable checkpoints instead, so you do not need `binlog_legacy_event_pos=ON`.

**MariaDB GTID resume.** MariaDB does not implement MySQL's `COM_BINLOG_DUMP_GTID` command. surreal-sync resumes by GTID the MariaDB-native way: it captures the current position from `@@global.gtid_binlog_pos`, sets the session variables `@slave_connect_state`, `@slave_gtid_strict_mode`, and `@slave_gtid_ignore_duplicates`, and then issues a plain `COM_BINLOG_DUMP` with an empty filename and position 4 — the server streams from the connect state. GTID positions accumulate as transactions are applied, so runtime checkpoints are emitted as MariaDB GTID lists (e.g. `gtid:0-1-270`). Multi-domain positions are preserved as a comma-separated list. The last-known file+byte offset is still tracked underneath as a fallback, but the GTID list is authoritative for MariaDB GTID checkpoints. If `@@global.gtid_binlog_pos` is empty (fresh server) or GTID cannot be parsed, surreal-sync falls back to a file+offset checkpoint.

## Troubleshooting

### Binlog consumer cannot connect

Verify replication grants and that binary logging is enabled:

```sql
SHOW GRANTS FOR 'surreal_sync'@'%';
SHOW MASTER STATUS;
```

On MariaDB, `SHOW MASTER STATUS` is equivalent; use `SHOW BINLOG STATUS` on newer versions if available.

### Checkpoint ahead of available binlog

If incremental sync fails because the binlog file was purged, you need a fresh `full` or `sync` from the current position. Increase `expire_logs_days` / `binlog_expire_logs_seconds` on the server to cover expected sync downtime.

### Timeout during incremental sync

Incremental sync runs for the specified `--timeout` then exits. Increase the value for longer runs, or use a scheduler for periodic sync. Avoid wrapping `surreal-sync` in a shell loop — signal handling becomes difficult.

### Duplicate or replayed changes

Binlog CDC is at-least-once. If a batch fails after apply but before commit, events may be replayed on restart. Design SurrealDB writes to be idempotent (for example, upsert by primary key).

## Data Type Support

See [MySQL Data Types](mysql-data-types.md) for data type mapping information; MariaDB uses the same mappings (including the JSON handling described in [MariaDB notes](#mariadb-notes)).

## References

- Implementation: [crates/mysql-binlog-source/](../crates/mysql-binlog-source/) and [crates/binlog-protocol/](../crates/binlog-protocol/)
- [MySQL Replication Documentation](https://dev.mysql.com/doc/refman/8.0/en/replication.html)
- [MariaDB Binary Log Documentation](https://mariadb.com/kb/en/binary-log/)
