# surreal-sync-postgresql-wal2json-source

A PostgreSQL logical replication library using the [wal2json](https://github.com/eulerto/wal2json) output plugin and the [replication functions](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-REPLICATION), designed for `surreal-sync`'s specific use case.

## Features

- Uses regular SQL connections (not replication protocol) for compatibility
- Assumes wal2json output plugin for JSON-formatted change data
- **At-least-once delivery guarantees** through explicit slot advancement

## At-Least-Once Delivery Pattern

This library implements at-least-once delivery by separating change retrieval from slot advancement:

```rust
// 1. Peek at available changes (doesn't consume them)
let changes = slot.peek().await?;

if !changes.is_empty() {
    // 2. Process all changes in the batch
    for (lsn, change) in &changes {
        // Process the change in your target system
        process_change_to_target_db(&change)?;
    }

    // 3. Advance to the last LSN after ALL changes are processed successfully
    // This provides batch acknowledgment for better performance
    let last_lsn = &changes.last().unwrap().0;
    slot.advance(last_lsn).await?;
}
```

If processing fails at step 2, none of the changes are consumed (the slot isn't advanced), and all changes will be redelivered on the next call to `peek()`. This ensures no data loss during failures while maximizing throughput with batch processing.

## Usage

See [the demo application](./src/main.rs) and the test instructions in it, along with [the example PostgreSQL + wal2json Dockerfile](./Dockerfile.postgres16.wal2json).

## Implementation Notes

This implementation uses regular SQL connections with `pg_logical_slot_peek_changes()` and `pg_replication_slot_advance()` instead of [the PostgreSQL replication protocol](https://www.postgresql.org/docs/current/protocol-replication.html), as the rust-postgres library doesn't support replication sessions (see [issue #116](https://github.com/rust-postgres/rust-postgres/issues/116)).
