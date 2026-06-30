# Development environment

The test suite is self-contained: each test binary starts its own throwaway
database containers (SurrealDB, PostgreSQL, MySQL, MongoDB, Neo4j, Kafka) on
demand via Docker, using random ports and per-test databases/namespaces for
isolation (see [`src/testing/shared_containers.rs`](../src/testing/shared_containers.rs)).

This means you only need two things to build and test locally:

1. A working **Docker daemon**.
2. The Rust toolchain + a few **system build dependencies**.

You can work either natively on your host or inside the devcontainer.

## Option A: Native (recommended)

1. Install the system build dependencies:

   ```bash
   make system-deps
   ```

   - macOS: installs `openssl@3`, `pkg-config`, `cmake`, `protobuf` via Homebrew.
     The `Makefile` automatically points cargo at Homebrew's OpenSSL.
   - Debian/Ubuntu: prints the `apt-get` command to run
     (`build-essential cmake libssl-dev libsasl2-dev pkg-config protobuf-compiler postgresql-client`).

2. Install the toolchain components and `cargo-nextest`:

   ```bash
   make install-tools
   ```

3. Make sure Docker is running, then:

   ```bash
   make test
   ```

`make test` formats, runs clippy, builds the debug binary, then runs the full
suite with [`cargo-nextest`](https://nexte.st/) (which parallelizes tests across
the whole workspace) plus the doctests. Tuning of test concurrency lives in
[`.config/nextest.toml`](../.config/nextest.toml).

## Option B: Devcontainer

The devcontainer ([`.devcontainer/surreal-sync`](../.devcontainer/surreal-sync))
provides the build environment plus a Docker daemon (via the docker-in-docker
feature), which is all the tests need.

1. Open the project in VS Code/Cursor.
2. Select "Reopen in Container".
3. Run `make test`.

The devcontainer no longer runs long-lived database services — the tests start
their own, exactly as they do natively.

## Troubleshooting

- "Cannot connect to the Docker daemon": ensure Docker Desktop / the daemon is
  running. The tests shell out to `docker run`.
- Leftover containers after an interrupted run: `docker ps -a` then
  `docker rm -f <name>` (test containers are named `shared-*`, `test-*`).
- macOS build errors about `openssl/ssl.h` not found: run `make system-deps`
  (the Makefile sets `OPENSSL_DIR`/`CFLAGS` for Homebrew OpenSSL automatically).
