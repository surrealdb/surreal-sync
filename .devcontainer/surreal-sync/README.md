# SurrealSync DevContainer

A minimal development environment for surreal-sync:

- Rust development environment with cargo and rust-analyzer
- A Docker daemon (via the docker-in-docker devcontainer feature)

There are intentionally **no long-lived database services** here. The test
suite starts its own throwaway database containers (SurrealDB, PostgreSQL,
MySQL, MongoDB, Neo4j, Kafka) on demand and cleans them up afterwards, so the
only requirement for running tests is a working Docker daemon.

## Usage

1. Open the project root in VS Code/Cursor.
2. Select "Reopen in Container".
3. Run `make test`.

See [docs/devcontainer.md](../../docs/devcontainer.md) for the full development
workflow (including running natively on your host).
