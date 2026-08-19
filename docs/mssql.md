# SQL Server Source

`surreal-sync from mssql sync` copies SQL Server tables into SurrealDB using **native SQL Server CDC** (change tables filled by the Agent capture job). There is no `full` / `incremental` split: one `sync` command snapshots, then stays on the CDC stream until you stop it.

Optional transforms: pass `--transforms-config` with a TOML file. Omit the flag to leave rows unchanged. Details: [How sync works](sync-pipeline.md).

## The happy path

```bash
surreal-sync from mssql sync \
  --connection-string 'Server=tcp:localhost,1433;User=sa;Password=...;Database=App;TrustServerCertificate=true;Encrypt=true' \
  --tables dbo.Article,sales.Order \
  --surreal-endpoint ws://localhost:8000 \
  --surreal-username root \
  --surreal-password root \
  --to-namespace prod \
  --to-database app \
  --checkpoints-surreal-table sync_checkpoints
```

Default `--strategy interleaved-snapshot` copies each table in primary-key order **while CDC is running**, using a signal table so live writes and the copy cannot disagree. After the snapshot it keeps applying CDC until `SIGINT`/`SIGTERM` or `--timeout`.

Restart the same command with the same checkpoint store to resume.

## Prerequisites

1. **SQL Server 2016+** (Developer, Standard, or Enterprise — **not Express**; Express has no CDC).
2. **CDC enabled on the database:**

```sql
EXEC sys.sp_cdc_enable_db;
```

surreal-sync enables CDC on each selected table when the login can (`db_owner`). If it cannot, the error includes the `sp_cdc_enable_table` statement a DBA must run.

3. **SQL Server Agent running.** CDC capture jobs do not populate change tables without it. Linux containers:

```bash
docker run --name mssql --platform linux/amd64 \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD='Surreal_Sync1' \
  -e MSSQL_PID=Developer \
  -e MSSQL_AGENT_ENABLED=true \
  -p 1433:1433 \
  -d mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04
```

`ACCEPT_EULA=Y` is required by Microsoft’s image ([SQL Server EULA](https://go.microsoft.com/fwlink/?linkid=2143497)). Developer edition is free for development and test; it is not a production license.

4. Every synced table needs a **primary key**.
5. Windows Integrated Auth (`IntegratedSecurity=true`) works only on Windows. Linux and CI use SQL authentication.

surreal-sync does **not** use Change Tracking or trigger/audit tables.

## Snapshot modes

| `--snapshot-mode` | Behavior |
|-------------------|----------|
| `initial` (default) | Snapshot, then continuous CDC. |
| `never` | CDC only from the checkpoint store. |
| `only` | Snapshot only, emit checkpoint, exit. |

## Strategies

| `--strategy` | When to use |
|--------------|-------------|
| `interleaved-snapshot` (default) | Normal path. Needs CDC, Agent, and writes to `surreal_sync_signal` (created and CDC-enabled by surreal-sync). |
| `sequential-snapshot` | One `SET TRANSACTION ISOLATION LEVEL SNAPSHOT` read of each table, then CDC from that LSN. **Writers are not locked.** Needs `ALLOW_SNAPSHOT_ISOLATION`. Long transactions version rows in **tempdb** — size tempdb for the dump. |

If snapshot isolation is off:

```sql
ALTER DATABASE [YourDb] SET ALLOW_SNAPSHOT_ISOLATION ON
```

## What gets copied

- `dbo.Article` becomes Surreal table `Article`. Other schemas become `sales__Order`.
- Composite primary keys become array record ids.
- Foreign keys between ordinary tables become `record<Target>` links. Join tables listed in `--relation-tables` become Surreal relations.
- Default is **schemaless**. Pass `--schemafull` to emit `DEFINE TABLE` / `FIELD` / `INDEX` before copying.

### `--schemafull` indexes

Ordinary tables: unique and non-unique btree indexes whose columns we sync are copied (`UNIQUE` when the source index is unique). Filtered indexes, INCLUDE columns, XML/spatial/full-text/columnstore, and indexes on skipped columns are omitted with a warning.

Temporal tables: source UNIQUE/PK indexes are **not** copied onto the unified table (history reuses the business key). surreal-sync emits query indexes for `is_current`, the business key, business key + `is_current` (not unique), and the period start/end columns. Uniqueness of “one current row per key” stays SQL Server’s job — SurrealDB has no partial unique index.

### Types we will not copy

`sql_variant`, `hierarchyid`, `geography` / `geometry`, CLR user-defined types, `cursor` / `table`, and `rowversion` / `timestamp` (binary, not datetime) fail before export.

## Temporal tables

System-versioned tables are detected from the catalog. There is no `--temporal` flag. Sync the **current** table (`dbo.Article`); selecting the history table alone is an error.

Every version lands in **one** Surreal table, plus `is_current`. The record id is the version (business key + period + content hash), not `Article:1`. Foreign keys **to** a temporal table stay scalars so they do not point at a single version.

Period columns stay visible (`ValidFrom` / `ValidTo`, or whatever SQL Server named them), including when they are `HIDDEN` in SQL Server.

| SQL Server | SurrealQL after sync |
|---|---|
| `SELECT * FROM dbo.Article` (current only) | `SELECT * FROM Article WHERE is_current` |
| `… WHERE Id = 1` | `SELECT * FROM Article WHERE Id = 1 AND is_current` |
| `FOR SYSTEM_TIME ALL` | `SELECT * FROM Article` |
| `FOR SYSTEM_TIME AS OF $t` | `SELECT * FROM Article WHERE ValidFrom <= $t AND ValidTo > $t` |
| `FOR SYSTEM_TIME FROM $t1 TO $t2` | `SELECT * FROM Article WHERE ValidFrom < $t2 AND ValidTo > $t1` |
| `FOR SYSTEM_TIME CONTAINED IN ($t1, $t2)` | `SELECT * FROM Article WHERE ValidFrom >= $t1 AND ValidTo <= $t2` |
| `SELECT * FROM dbo.ArticleHistory` | `SELECT * FROM Article WHERE is_current = false` |
| `JOIN … ON Order.ArticleId = Article.Id` (current) | `WHERE Order.ArticleId = Article.Id AND Article.is_current` |

SurrealDB cannot time-travel a graph edge the way `FOR SYSTEM_TIME AS OF` time-travels a join. Filter versions in `WHERE`.

Live CDC is enabled on the current table. An UPDATE writes a new current version and clears `is_current` on the previous one.

## Embed

```rust
use surreal_sync_mssql::{run, FlattenId, InPlaceTransform, Value};
use surreal_sync_surreal::Surreal3Sink;

run::<Surreal3Sink>([
    Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>,
]).await
```

Argv is `sync` plus the same flags as the stock CLI (no `from mssql` prefix). See `examples/from-mssql`.
