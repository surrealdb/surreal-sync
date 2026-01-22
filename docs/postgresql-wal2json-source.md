# PostgreSQL wal2json Source

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
  - [PostgreSQL Version](#postgresql-version)
  - [Verifying wal2json Extension](#verifying-wal2json-extension)
- [Architecture](#architecture)
  - [Core Components](#core-components)
  - [How It Works Under the Hood](#how-it-works-under-the-hood)
- [CLI Usage](#cli-usage)
  - [Full Sync Command](#full-sync-command)
  - [Incremental Sync Command](#incremental-sync-command)
  - [Example Workflow: Initial Sync + Ongoing Changes](#example-workflow-initial-sync--ongoing-changes)
- [Data Type Support](#data-type-support)
  - [Numeric Types](#numeric-types)
  - [String Types](#string-types)
  - [Boolean](#boolean)
  - [Binary Data](#binary-data)
  - [UUID](#uuid)
  - [Date/Time Types](#datetime-types)
  - [JSON Types](#json-types)
  - [Arrays](#arrays)
  - [NULL Values](#null-values)
- [Primary Key Handling](#primary-key-handling)
  - [Extraction from wal2json](#extraction-from-wal2json)
  - [Composite Keys](#composite-keys)
  - [Fallback Strategy](#fallback-strategy)
  - [DELETE Operation Handling](#delete-operation-handling)
- [Checkpoints](#checkpoints)
  - [LSN Format](#lsn-format)
  - [Checkpoint File Structure](#checkpoint-file-structure)
  - [Checkpoint Files Location](#checkpoint-files-location)
  - [Using Checkpoints for Incremental Sync](#using-checkpoints-for-incremental-sync)
- [Limitations and Considerations](#limitations-and-considerations)
  - [No Streaming Replication Protocol Support](#no-streaming-replication-protocol-support)
  - [Array Parsing Limitations](#array-parsing-limitations)
  - [Long-Running and Large Transactions](#long-running-and-large-transactions)
  - [At-Least-Once Delivery Semantics](#at-least-once-delivery-semantics)
  - [Schema Changes During Sync](#schema-changes-during-sync)
  - [Replication Slot Management](#replication-slot-management)
  - [Performance Considerations](#performance-considerations)
  - [Parallel Processing and Table Partitioning](#parallel-processing-and-table-partitioning)
- [Examples](#examples)
  - [Example 1: Basic Full Sync](#example-1-basic-full-sync)
  - [Example 2: Selective Table Sync](#example-2-selective-table-sync)
  - [Example 3: Incremental Sync from Checkpoint](#example-3-incremental-sync-from-checkpoint)
  - [Example 4: Selective Incremental Table Sync](#example-4-selective-incremental-table-sync)
- [Troubleshooting](#troubleshooting)
  - [Timeout During Incremental Sync](#timeout-during-incremental-sync)
  - [No New Changes Available](#no-new-changes-available)
- [References](#references)
  - [External Documentation](#external-documentation)
  - [Source Code](#source-code)
  - [Test Examples](#test-examples)
  - [Demo Application](#demo-application)

## Introduction

The PostgreSQL wal2json source is a Change Data Capture (CDC) implementation for surreal-sync that enables real-time data synchronization from PostgreSQL databases to SurrealDB using PostgreSQL's logical replication capabilities with the wal2json output plugin.

**Key Capabilities:**
- **Full Sync**: Complete database migration with schema discovery and data transfer
- **Incremental Sync**: Real-time change streaming for inserts, updates, and deletes
- **At-Least-Once Delivery**: Guarantees no data loss through peek-process-advance pattern
- **Transaction Support**: Respects PostgreSQL ACID transactions
- **LSN-Based Checkpointing**: Enables resumable incremental sync from any point in the WAL

## Prerequisites

### PostgreSQL Version
- PostgreSQL 10 or later (tested with PostgreSQL 16)
- wal2json extension version 2.0 or later

### Verifying wal2json Extension

Check if wal2json is installed and available on your PostgreSQL instance:

```sql
-- Check if wal2json extension is available
SELECT * FROM pg_available_extensions WHERE name = 'wal2json';

-- Expected output if available:
--   name    | default_version | installed_version | comment
-- ----------+-----------------+-------------------+---------
--  wal2json | 2.6            | 2.6               | ...
```

## Architecture

### Core Components

The PostgreSQL wal2json source consists of several key components:

1. ([Client](../crates/postgresql-wal2json-source/src/logical_replication.rs)) that manages PostgreSQL connections and replication slot lifecycle
2. Slot that handles WAL change streaming using peek and advance operations
3. ([wal2json Parser](../crates/postgresql-wal2json-source/src/wal2json.rs)) that parses JSON output from the wal2json plugin
4. ([Change Converter](../crates/postgresql-wal2json-source/src/change.rs)) that transforms wal2json output into strongly-typed Action enums
5. ([Value Converters](../crates/postgresql-wal2json-source/src/value/)) that provides PostgreSQL-specific type converters for timestamps, UUIDs, intervals, etc.

### How It Works Under the Hood

For implementation details on the connection model, transaction handling, at-least-once delivery pattern, and LSN-based checkpointing, see the documentation in [logical_replication.rs](../crates/postgresql-wal2json-source/src/logical_replication.rs#L1-L78).

**Key Points for Users:**
- **Full sync creates two checkpoints** - t1 checkpoint is captured BEFORE the full sync snapshot (use this for incremental sync), t2 checkpoint is captured AFTER the full sync completes (for reference only).
- **Incremental sync always starts from t1, not t2** - This ensures no changes are missed regardless of isolation level. See [the test implementation](../tests/postgresql_logical/postgresql_logical_incremental_sync_only_cli.rs#L88-L95) for reference.
- **At-least-once delivery** - Changes may be replayed if processing fails, so ensure your processing logic is idempotent.
- **Transaction consistency** - All changes within a PostgreSQL transaction are processed as a batch.

## CLI Usage

### Full Sync Command

```bash
surreal-sync from postgresql full \
  --connection-string "postgresql://user:password@host:5432/database" \
  --slot "surreal_sync_slot" \
  --tables "users,orders,products" \
  --schema "public" \
  --to-namespace "production" \
  --to-database "myapp" \
  --surreal-endpoint "ws://localhost:8000" \
  --surreal-username "root" \
  --surreal-password "root" \
  --emit-checkpoints \
  --checkpoint-dir ".checkpoints"
```

**Parameters:**
- `--connection-string` (required): PostgreSQL connection URL
- `--slot`: Replication slot name (default: `"surreal_sync_slot"`)
- `--tables`: Comma-separated table names (empty = sync all user tables)
- `--schema`: PostgreSQL schema to sync (default: `"public"`)
- `--to-namespace`: Target SurrealDB namespace
- `--to-database`: Target SurrealDB database
- `--surreal-endpoint`: SurrealDB WebSocket endpoint
- `--surreal-username`: SurrealDB username
- `--surreal-password`: SurrealDB password
- `--emit-checkpoints`: Enable checkpoint file emission for incremental sync
- `--checkpoint-dir`: Directory to store checkpoint files

### Incremental Sync Command

```bash
surreal-sync from postgresql incremental \
  --connection-string "postgresql://user:password@host:5432/database" \
  --slot "surreal_sync_slot" \
  --tables "users,orders,products" \
  --schema "public" \
  --to-namespace "production" \
  --to-database "myapp" \
  --surreal-endpoint "ws://localhost:8000" \
  --surreal-username "root" \
  --surreal-password "root" \
  --incremental-from "0/1949850" \
  --incremental-to "0/2000000" \
  --timeout 3600
```

**Incremental-Specific Parameters:**
- `--incremental-from` (required): Starting LSN checkpoint (from t1 checkpoint file)
- `--incremental-to`: Optional stopping LSN checkpoint
- `--timeout`: Maximum runtime in seconds (default: 3600 = 1 hour)

### Example Workflow: Initial Sync + Ongoing Changes

```bash
# Step 1: Run full sync with checkpoint emission
surreal-sync from postgresql full \
  --connection-string "postgresql://localhost/mydb" \
  --emit-checkpoints \
  --checkpoint-dir ".checkpoints" \
  --to-namespace "prod" \
  --to-database "myapp" \
  --surreal-endpoint "ws://localhost:8000" \
  --surreal-username "root" \
  --surreal-password "root"

# Step 2: Make some changes in PostgreSQL
psql -d mydb -c "INSERT INTO users (name, email) VALUES ('John', 'john@example.com');"

# Step 3: Run incremental sync from the t1 checkpoint
surreal-sync from postgresql incremental \
  --connection-string "postgresql://localhost/mydb" \
  --incremental-from "$(cat .checkpoints/postgresql_t1.json | jq -r '.lsn')" \
  --timeout 60 \
  --to-namespace "prod" \
  --to-database "myapp" \
  --surreal-endpoint "ws://localhost:8000" \
  --surreal-username "root" \
  --surreal-password "root"
```

## Data Type Support

All PostgreSQL types are supported in incremental sync through the type conversion helper function at [incremental_sync.rs:convert_postgres_value_to_surreal()](../crates/postgresql-wal2json-source/src/incremental_sync.rs#L234-L308).

### Numeric Types

| PostgreSQL Type | SurrealDB Type | Notes |
|----------------|----------------|-------|
| `smallint`, `int2` | `int` | Converted from i16 to 64-bit integer |
| `integer`, `int4` | `int` | Converted from i32 to 64-bit integer |
| `bigint`, `int8` | `int` | Native 64-bit integer |
| `real`, `float4` | `float` | Converted from 32-bit to 64-bit float |
| `double precision`, `float8` | `float` | Native 64-bit float |
| `numeric`, `decimal` | `decimal` | High-precision decimal. Falls back to `string` if parsing fails. |

### String Types

| PostgreSQL Type | SurrealDB Type |
|----------------|----------------|
| `text` | `string` |
| `varchar`, `character varying` | `string` |
| `char`, `character` | `string` |

### Boolean

| PostgreSQL Type | SurrealDB Type |
|----------------|----------------|
| `boolean`, `bool` | `bool` |

### Binary Data

| PostgreSQL Type | SurrealDB Type | Notes |
|----------------|----------------|-------|
| `bytea` | `bytes` | Binary data hex-decoded from wal2json output |

### UUID

| PostgreSQL Type | SurrealDB Type | Notes |
|----------------|----------------|-------|
| `uuid` | `uuid` | Converted to native UUID type (RFC 4122). Falls back to `string` only if UUID parsing fails. |

### Date/Time Types

| PostgreSQL Type | SurrealDB Type | Notes |
|----------------|----------------|-------|
| `timestamp`, `timestamp without time zone` | `datetime` | Parsed using PostgreSQL timestamp formats. Falls back to `string` if parsing fails. |
| `timestamptz`, `timestamp with time zone` | `datetime` | Parsed using PostgreSQL timestamptz formats. Falls back to `string` if parsing fails. |
| `date` | `string` | Format: `"2024-01-15"` |
| `time`, `time without time zone` | `string` | Format: `"10:30:00"`, `"15:37:16.123456"` |
| `timetz`, `time with time zone` | `string` | Format: `"10:30:00+00"` |
| `interval` | `duration` | Converted to duration type. Falls back to `string` if parsing fails. |

### JSON Types

| PostgreSQL Type | SurrealDB Type | Notes |
|----------------|----------------|-------|
| `json` | `object` / `array` | Recursively converted using [json_to_surreal()](../crates/postgresql-wal2json-source/src/incremental_sync.rs#L311-L339) |
| `jsonb` | `object` / `array` | Recursively converted using [json_to_surreal()](../crates/postgresql-wal2json-source/src/incremental_sync.rs#L311-L339) |

**Note**: While wal2json sends JSON/JSONB as strings (see [wal2json issue #221](https://github.com/eulerto/wal2json/issues/221)), our parser successfully converts them to nested structures, which are then recursively converted to SurrealDB objects and arrays with proper type preservation for nested values.

### Arrays

| PostgreSQL Type | SurrealDB Type | Supported Element Types |
|----------------|----------------|------------------------|
| `type[]` | `array` | `integer[]`, `bigint[]`, `text[]`, `boolean[]`, `varchar[]` |

**Array Parsing Limitations:**
- Does NOT support nested arrays (e.g., `integer[][]`)
- Does NOT handle quoted strings with embedded commas in text arrays
- Uses simple comma-splitting algorithm

### NULL Values

| PostgreSQL Value | SurrealDB Type |
|-----------------|----------------|
| `NULL` | `null` |

## Primary Key Handling

### Extraction from wal2json

The wal2json output includes a `pk` field with primary key information:

```json
{
  "pk": {
    "pknames": ["id"],
    "pktypes": ["integer"]
  }
}
```

Primary key values are extracted and used as SurrealDB [record IDs](https://surrealdb.com/docs/surrealql/datamodel/ids).

### Single Primary Keys

Single primary keys map directly to record IDs:

| PostgreSQL Primary Key | SurrealDB Record ID | Example |
|------------------------|---------------------|---------|
| `id INTEGER = 123` | `tablename:123` | Numeric ID |
| `id TEXT = 'user_abc'` | `tablename:'user_abc'` | String ID |
| `id UUID = '550e8400...'` | `tablename:u"550e8400-e29b-41d4-a716-446655440000"` | UUID ID |

### Composite Primary Keys

Tables with composite primary keys use SurrealDB's array-based record IDs:

```json
{
  "pk": {
    "pknames": ["user_id", "order_id"],
    "pktypes": ["integer", "integer"]
  }
}
```

**Example Record IDs:**
- PostgreSQL: `user_id = 123, order_id = 456`
- SurrealDB: `tablename:[123, 456]`

**Another Example:**
- PostgreSQL: `tenant = 'acme', user_id = 789`
- SurrealDB: `tablename:['acme', 789]`

The composite key values are converted to a SurrealDB array and used as the record ID, preserving the order and types of the original primary key columns

### Fallback Strategy

If no primary key information is available in the wal2json output:
1. First tries to extract `id` column
2. If `id` column doesn't exist, returns an error

### DELETE Operation Handling

DELETE operations in wal2json include the old row data in two possible fields:
- `identity`: Primary key values
- `columns`: All column values (if `REPLICA IDENTITY FULL` is set)

The implementation tries `identity` first, then falls back to `columns`.

## Checkpoints

### LSN Format

PostgreSQL LSNs (Log Sequence Numbers) are represented as `segment/offset`:
- Example: `0/1949850`
- Segment: 8-byte identifier
- Offset: Position within the segment

### Checkpoint File Structure

**t1 Checkpoint** (before full sync):
```json
{
  "lsn": "0/1949850",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**t2 Checkpoint** (after full sync):
```json
{
  "lsn": "0/1950120",
  "timestamp": "2024-01-15T10:35:00Z"
}
```

### Checkpoint Files Location

When using `--checkpoint-dir .checkpoints`:
```
.checkpoints/
├── postgresql_t1.json    # Starting checkpoint
└── postgresql_t2.json    # Ending checkpoint
```

### Using Checkpoints for Incremental Sync

```bash
# Extract LSN from t1 checkpoint
LSN=$(cat .checkpoints/postgresql_t1.json | jq -r '.lsn')

# Use LSN for incremental sync
surreal-sync from postgresql incremental \
  --incremental-from "$LSN" \
  ...
```

## Limitations and Considerations

### No Streaming Replication Protocol Support

This implementation uses **regular SQL connections** via `tokio-postgres`, not the PostgreSQL replication protocol. This is due to library limitations but provides good compatibility.

**Implications:**
- Changes are retrieved in batches via SQL queries
- Slightly higher latency compared to streaming replication
- More compatible with connection poolers and proxies

### Array Parsing Limitations

The array parser uses a simple comma-splitting algorithm for PostgreSQL array columns (e.g., `integer[]`, `text[]`, `boolean[]`):
- ✅ Supports: `{1,2,3}`, `{true,false}`, `{hello,world}`
- ❌ Does NOT support: Nested arrays `{{1,2},{3,4}}`
- ❌ Does NOT support: Quoted strings with commas `{"hello, world","foo"}`

See implementation: [change.rs:423-470](../crates/postgresql-wal2json-source/src/change.rs#L423-L470)

### Long-Running and Large Transactions

**How Transactions Are Handled:**

According to [PostgreSQL documentation](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-REPLICATION), `pg_logical_slot_peek_changes()` **only returns committed transactions**. Uncommitted/in-progress transactions are completely invisible.

**Long-Running Transactions:**
- Remain **invisible** while in progress (no errors, no retries, no blocking)
- Appear **atomically** when they commit (all changes at once)
- No impact on sync performance while running
- Sync continues processing other committed transactions normally

**Very Large Transactions:**
- Retrieved completely in one `peek()` call after commit
- All changes (BEGIN + data + COMMIT) returned together
- Can cause **memory spike** when processing huge transactions
- Processed as one atomic batch (at-least-once delivery)

**Streaming Behavior:**
Our implementation streams **transactions** continuously (one after another), but cannot stream **within a single large transaction**:

- ✅ **Streams transactions**: Processes committed transactions one by one as they become available
- ❌ **No intra-transaction streaming**: Cannot see parts of a large transaction before it commits
- ❌ **All-or-nothing for large transactions**: A transaction with millions of rows appears atomically

**Why?**
This is a limitation of using the SQL function interface (`pg_logical_slot_peek_changes()`), not wal2json itself. The SQL function only returns committed transactions, regardless of how wal2json formats the output.

**Could this be enhanced?**
Yes, by switching to the [streaming replication protocol](https://www.postgresql.org/docs/current/logicaldecoding-streaming.html) instead of SQL functions:
- Would use `pg_recvlogical` or similar streaming connection
- PostgreSQL would call wal2json's streaming callbacks (`stream_start_cb`, `stream_change_cb`, etc.)
- Large uncommitted transactions could be processed incrementally
- Requires significant implementation changes (not currently supported)

**Current Best Practices**: For systems with very large transactions (millions of rows):
- Increase available memory for the sync process to handle atomic transaction processing
- Break large batch operations into smaller transactions at the source (e.g., commit every 10,000 rows)
- Monitor WAL growth and replication lag during large operations

### At-Least-Once Delivery Semantics

The peek-process-advance pattern guarantees **at-least-once delivery**:
- No changes are lost (data safety)
- Changes may be re-delivered if processing fails (requires idempotent handling)

**Best Practice**: Ensure your processing logic is idempotent or handle duplicate change events.

### Schema Changes During Sync

Logical replication captures the schema state at the time of slot creation. Schema changes (ALTER TABLE) during sync may cause:
- Type mismatches
- Missing columns in change events
- Sync failures

**Best Practice**: Avoid schema changes during active sync. If needed:
1. Stop incremental sync
2. Apply schema changes
3. Run new full sync
4. Resume incremental sync from new checkpoint

### Replication Slot Management

Replication slots are persistent and consume disk space:
- WAL files are retained until the slot is advanced
- Inactive slots can fill up disk space
- Slots must be manually deleted when no longer needed

**Best Practice**: Monitor slot lag and delete unused slots:
```sql
-- Check slot lag
SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots;

-- Delete unused slot
SELECT pg_drop_replication_slot('surreal_sync_slot');
```

### Performance Considerations

**Batch Size**: The implementation retrieves changes in batches. Larger transactions = larger batches = higher memory usage.

**Checkpoint Frequency**: Advancing the slot only after successful processing can cause:
- Higher WAL retention if processing is slow
- Better data safety guarantees

**Network Latency**: Each peek and advance operation requires a round-trip to PostgreSQL.

### Parallel Processing and Table Partitioning

**Can I partition tables across multiple processes for parallel sync?**

Yes, but with important caveats:

**How It Works:**
- Table filtering happens **client-side** after reading WAL changes (see [implementation](../crates/postgresql-wal2json-source/src/logical_replication.rs#L468-L490))
- All changes are read from the WAL regardless of the `--tables` filter
- Each process needs its own replication slot (different `--slot` names)

**Trade-offs:**
- ✅ **Benefit**: Parallel processing of different tables
- ❌ **Cost**: WAL data is read multiple times (once per slot)
- ❌ **Cost**: Increased WAL retention (each slot holds WAL data until it advances)
- ❌ **Cost**: More disk I/O on PostgreSQL server

**Example:**
```bash
# Process 1: Users and orders (slot: sync_slot_1)
surreal-sync from postgresql incremental \
  --slot "sync_slot_1" \
  --tables "users,orders" \
  ...

# Process 2: Products and inventory (slot: sync_slot_2)
surreal-sync from postgresql incremental \
  --slot "sync_slot_2" \
  --tables "products,inventory" \
  ...
```

**Best Practice**: Only partition if processing (not reading) is the bottleneck. Otherwise, a single process reading all tables is more efficient.

## Examples

### Example 1: Basic Full Sync

```bash
# Sync all tables from the public schema (one-time migration)
surreal-sync from postgresql full \
  --connection-string "postgresql://postgres:postgres@localhost:5432/mydb" \
  --to-namespace "production" \
  --to-database "myapp" \
  --surreal-endpoint "ws://localhost:8000" \
  --surreal-username "root" \
  --surreal-password "root"
```

**For incremental sync later**: Add `--slot "surreal_sync_slot" --emit-checkpoints --checkpoint-dir ".checkpoints"` to emit checkpoint files that enable incremental sync.

### Example 2: Selective Table Sync

```bash
# Sync only specific tables (full sync)
surreal-sync from postgresql full \
  --connection-string "postgresql://postgres:postgres@localhost:5432/mydb" \
  --tables "users,orders,products" \
  --to-namespace "production" \
  --to-database "myapp" \
  --surreal-endpoint "ws://localhost:8000" \
  --surreal-username "root" \
  --surreal-password "root"
```

### Example 3: Incremental Sync from Checkpoint

```bash
# First: Run full sync with checkpoints (as shown in Example Workflow)
# Then: Read t1 checkpoint and run incremental sync
LSN=$(cat .checkpoints/postgresql_t1.json | jq -r '.lsn')

surreal-sync from postgresql incremental \
  --connection-string "postgresql://postgres:postgres@localhost:5432/mydb" \
  --slot "surreal_sync_slot" \
  --incremental-from "$LSN" \
  --timeout 3600 \
  --to-namespace "production" \
  --to-database "myapp" \
  --surreal-endpoint "ws://localhost:8000" \
  --surreal-username "root" \
  --surreal-password "root"
```

### Example 4: Selective Incremental Table Sync

```bash
# Sync only specific tables during incremental sync
LSN=$(cat .checkpoints/postgresql_t1.json | jq -r '.lsn')

surreal-sync from postgresql incremental \
  --connection-string "postgresql://postgres:postgres@localhost:5432/mydb" \
  --slot "surreal_sync_slot" \
  --tables "users,orders,products" \
  --incremental-from "$LSN" \
  --timeout 3600 \
  --to-namespace "production" \
  --to-database "myapp" \
  --surreal-endpoint "ws://localhost:8000" \
  --surreal-username "root" \
  --surreal-password "root"
```

**Note on Table Filtering in Incremental Sync**: The `--tables` flag filters changes **client-side** after reading WAL (see [implementation](../crates/postgresql-wal2json-source/src/logical_replication.rs#L468-L477)). This means:
- All table changes are read from the WAL regardless of the `--tables` filter
- Filtering happens in-memory by checking each change's table name
- **Multiple processes with different table filters will read the SAME WAL data**
- For parallel processing, you need separate replication slots (one per process) using different `--slot` names
- Each slot maintains its own position in the WAL, so WAL data is read multiple times (once per slot)

## Troubleshooting

### Timeout During Incremental Sync

**Symptom:**
```
Incremental sync timed out after 3600 seconds
```

**Solution:**
1. This is expected behavior - incremental sync runs for the specified timeout
2. Increase `--timeout` if you need longer sync duration (e.g., `--timeout 86400` for 24 hours)
3. For continuous sync, use a scheduler (e.g., systemd timer, cron, Kubernetes CronJob) to run incremental sync periodically
4. Avoid wrapping surreal-sync in a shell loop - signal handling becomes difficult

### No New Changes Available

**Symptom:**
Incremental sync appears to hang or shows no activity

**Clarification:**
This is **normal behavior** when there are no new committed transactions. Long-running transactions remain invisible until they commit - they do NOT cause errors or blocking.

**What's happening:**
- The sync is polling for new committed transactions
- Long-running transactions are invisible (not causing any issues)
- Sync will continue once new transactions commit or timeout is reached

**Solutions:**
1. **If you expect this behavior**: Just wait for new transactions to commit, or let the timeout expire
2. **If you want to stop early**: Use Ctrl+C to gracefully terminate (signal handling works properly)
3. **For continuous monitoring**: Use a scheduler to run incremental sync periodically with appropriate timeout
4. **To verify activity**: Check WAL position and replication lag:
```sql
-- Check current WAL position
SELECT pg_current_wal_lsn();

-- Check replication slot status
SELECT slot_name, restart_lsn, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name = 'surreal_sync_slot';
```

## References

### External Documentation
- [wal2json GitHub Repository](https://github.com/eulerto/wal2json)
- [PostgreSQL Logical Replication Documentation](https://www.postgresql.org/docs/current/logical-replication.html)
- [PostgreSQL Replication Slots](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS)
- [PostgreSQL WAL Configuration](https://www.postgresql.org/docs/current/runtime-config-wal.html)

### Source Code
- Implementation: [crates/postgresql-wal2json-source/](../crates/postgresql-wal2json-source/)
- Core logic: [src/logical_replication.rs](../crates/postgresql-wal2json-source/src/logical_replication.rs)
- wal2json parser: [src/wal2json.rs](../crates/postgresql-wal2json-source/src/wal2json.rs)
- Change conversion: [src/change.rs](../crates/postgresql-wal2json-source/src/change.rs)
- Value converters: [src/value/](../crates/postgresql-wal2json-source/src/value/)

### Test Examples
- Incremental sync CLI test: [tests/postgresql_logical/postgresql_logical_incremental_sync_only_cli.rs](../tests/postgresql_logical/postgresql_logical_incremental_sync_only_cli.rs)
- Full sync CLI test: [tests/postgresql_logical/postgresql_logical_full_sync_only_cli.rs](../tests/postgresql_logical/postgresql_logical_full_sync_only_cli.rs)
