# Source ports onto the transform framework

Implementer checklist for wiring each `from *` sync path through
`sync-transform` (`SourceDriver` / `run_source_runtime` / `write_rows` /
`write_relations`) and the shared CLI helper in [`src/from/transforms.rs`](../src/from/transforms.rs).

Operator-facing transforms docs stay in [transforms.md](transforms.md).

## Shared CLI

- [x] `load_transforms_from_args` in `src/from/transforms.rs`
- [x] `from mysql-binlog sync` uses the shared loader
- [x] `from kafka`, `from csv`, and `from jsonl` take `--transforms-config`
      and call the shared loader

**Omit flag vs empty / passthrough file** — both yield an identity pipeline,
but `ApplyOpts` differ (buffering cadence):

| CLI input | Pipeline | `ApplyOpts` |
|-----------|----------|-------------|
| `--transforms-config` omitted | Identity (no stage dispatch) | `ApplyOpts::identity()` (`batch_size = 1`) |
| Empty / passthrough-only TOML | Identity stages | `ApplyOpts::default()` (`batch_size = 1000`) |

Fail-fast on bad TOML / missing or unresolvable worker before sync starts
(CLI loader wraps with `load --transforms-config` context; deeper worker
spawn coverage is in `sync-transform` config tests).

## Per-source status

| Source | Framework apply path | `--transforms-config` CLI | Notes |
|--------|----------------------|---------------------------|--------|
| mysql-binlog | SourceDriver + `run_source_runtime_with` | Yes (shared loader + CLI e2e) | Reference port |
| postgresql-pgoutput | SourceDriver + `run_source_runtime_with` | Yes (shared loader + CLI e2e) | Binlog parity |
| postgresql-wal2json | SourceDriver | Yes | FK pre-push enrichment preserved |
| postgresql-trigger | SourceDriver | Yes | FK pre-push enrichment preserved |
| mysql (trigger) | SourceDriver | Yes | |
| mongodb | SourceDriver + `write_rows` full | Yes | |
| neo4j | SourceDriver + `write_rows` / `write_relations` | Yes | Nodes + edges via mixed events |
| kafka | `write_rows` (decode batch then apply) | Yes (shared loader + CLI e2e) | Continuous stream consumer; offset commit stays Kafka consumer-group |
| csv | `write_rows` | Yes (shared loader + CLI e2e) | |
| jsonl | `write_rows` | Yes (shared loader + CLI e2e) | `conversion_rules` before Pipeline |

## Porting rules (short)

1. Streaming CDC sources implement `SourceDriver` and call
   `run_source_runtime` / `run_source_runtime_with` — no production
   hand-rolled `ApplyContext` loops after port; do not use `ChangeFeed`
   for production ports.
2. File batch importers (csv, jsonl) use `write_rows` only. Kafka is a
   continuous stream consumer: decode each fetch batch, then apply once
   via `write_rows` (not a file batch importer).
3. Every WatermarkSource consumer gets transform-aware interleaved /
   ad-hoc entrypoints and threads `Pipeline` / `ApplyOpts` from CLI.
4. Identity (omit `--transforms-config`) must stay green; add at least one
   external-transform e2e when porting a streaming source.
