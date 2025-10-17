# surreal-sync-postgresql-replication

A PostgreSQL logical replication library using the [wal2json](https://github.com/eulerto/wal2json) output plugin, designed for `surreal-sync`'s specific use case.

## Features

- Uses regular SQL connections (not replication protocol) for compatibility
- Assumes wal2json output plugin for JSON-formatted change data
- Graceful shutdown handling

## Usage

See [the demo application](./src/main.rs) and the test instructions in it, along with [the example PostgreSQL + wal2json Dockerfile](./Dockerfile.postgres16.wal2json).

## Implementation Notes

This implementation uses regular SQL connections with `pg_logical_slot_get_changes()` instead of the PostgreSQL replication protocol, as the rust-postgres library doesn't support replication sessions (see [issue #116](https://github.com/rust-postgres/rust-postgres/issues/116)).
