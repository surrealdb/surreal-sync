# Captured binlog wire fixtures

Binary fixtures in this directory are **real post-`COM_BINLOG_DUMP` replication
stream bytes** recorded from stock Docker images. Replay tests decode them with
`BinlogBytesReader` — no containers at test time.

| File | Server | Description |
|------|--------|-------------|
| `mysql_8_basic.bin` | `mysql:8.0` | INSERT/UPDATE/DELETE on `wire_fixture` with composite PK and broad type coverage |
| `mariadb_11_4_basic.bin` | `mariadb:11.4` | Same DML plus a multi-statement transaction (intra-txn `log_pos=0`) |

Each `.bin` file has a sibling `.meta.json` with server image, flavor,
checksum flag, and the exact SQL used during capture.

## Regenerating fixtures

Requires Docker. From the repo root:

```bash
CAPTURE_BINLOG_FIXTURES=1 cargo test -p surreal-sync-mysql capture_binlog_fixtures -- --nocapture
```

This overwrites `*.bin` and `*.meta.json` in this directory. Review the diff,
accept updated `insta` snapshots if decode output changed intentionally:

```bash
cargo test -p surreal-sync-mysql replay_captured -- --nocapture
INSTA_UPDATE=1 cargo test -p surreal-sync-mysql replay_captured
```

Normal CI and local runs use `cargo test -p surreal-sync-mysql` only (no Docker).

## Capture SQL (MySQL)

See `mysql_8_basic.meta.json` for the full script. Schema highlights:

- Composite PK `(pk1, pk2)`
- `JSON`, `DECIMAL(19,5)`, `TIME(3)`, `DATETIME(6)`, `TIMESTAMP(3)`, `ENUM`, `SET`, `BIT(8)`, `YEAR`, `VARBINARY`, `GEOMETRY`
- One INSERT, one UPDATE, one DELETE transaction

MariaDB uses `LONGTEXT` instead of native `JSON` and adds a `START TRANSACTION … COMMIT` block with two INSERTs and an UPDATE.
