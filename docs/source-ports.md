# Source ports onto the transform framework

Implementer checklist for wiring each `from *` sync path through
`sync-transform` (`SourceDriver` / `run_source_runtime` / `write_rows` /
`write_relations`) and the shared CLI helper in [`src/from/transforms.rs`](../src/from/transforms.rs).

Operator-facing transforms docs stay in [transforms.md](transforms.md).

## Shared CLI

- [x] `load_transforms_from_args` in `src/from/transforms.rs`
- [x] Every `from *` sync/import command with a sync path takes `--transforms-config`
      and calls the shared loader (mysql-binlog, postgresql-pgoutput, postgresql /
      wal2json, postgresql-trigger, mysql trigger, mongodb, neo4j, kafka, csv, jsonl)

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

| Source | Framework apply path | `--transforms-config` CLI | Interleaved `_with_transforms` | Notes |
|--------|----------------------|---------------------------|--------------------------------|--------|
| mysql-binlog | SourceDriver + `run_source_runtime_with` | Yes (shared loader + CLI e2e) | Yes | Reference port; sink-safe CatchUpProgress |
| postgresql-pgoutput | SourceDriver + `run_source_runtime_with` | Yes (shared loader + CLI e2e) | Yes | Binlog parity; sink-safe CatchUpProgress |
| postgresql-wal2json | SourceDriver | Yes (shared loader + CLI e2e) | Yes | FK pre-push enrichment preserved |
| postgresql-trigger | SourceDriver | Yes (shared loader + CLI e2e) | Yes | FK pre-push enrichment preserved |
| mysql (trigger) | SourceDriver | Yes (shared loader + CLI e2e) | Yes | No DDL / ad-hoc (unchanged gap) |
| mongodb | SourceDriver + `write_rows` full | Yes (shared loader + CLI e2e) | N/A | Resume token as Position |
| neo4j | SourceDriver + `write_rows` / `write_relations` | Yes (shared loader + CLI e2e) | N/A | Nodes + edges via mixed events |
| kafka | `write_rows` (decode batch then apply) | Yes (shared loader + CLI e2e) | N/A | Offset commit stays Kafka consumer-group |
| csv | `write_rows` | Yes (shared loader + CLI e2e) | N/A | |
| jsonl | `write_rows` | Yes (shared loader + CLI e2e) | N/A | `conversion_rules` before Pipeline |

## Implementer checklist (all ports)

### Framework wiring

- [x] Streaming CDC sources implement `SourceDriver` and call
      `run_source_runtime` / `run_source_runtime_with` (no production hand-rolled
      `ApplyContext` loops; do not use `ChangeFeed` for production ports)
- [x] File batch importers (csv, jsonl) use `write_rows` only
- [x] Kafka: decode each fetch batch, then apply once via `write_rows`
- [x] Neo4j: nodes and edges through one `SourceDriver` emitting mixed
      `PositionedEvent`s
- [x] wal2json / postgresql-trigger: FK transforms as source-side pre-push
      enrichment; relation `PositionedEvent`s where those sources already emit relations
- [x] Thin identity wrappers (`run_*` → `run_*_with_transforms` + identity) kept
      for existing public names; production CLI always threads Pipeline / ApplyOpts

### Interleaved / ad-hoc

- [x] Every WatermarkSource consumer exposes `_with_transforms` entrypoints
      (binlog, pgoutput, wal2json, postgresql-trigger, mysql-trigger)
- [x] CLI threads `SnapshotTransforms` / Pipeline / ApplyOpts for interleaved
      full and combined `sync` the same way as binlog
- [x] Ad-hoc snapshot (binlog / pgoutput) uses transform-aware helpers
- [x] mysql-trigger: no ad-hoc / DDL (remain missing by design)

### Cleanup

- [x] Dead direct-apply hot paths removed from ported sources (legacy wal2json
      `sync/` placeholder that called `apply_universal_change` bypassing the
      framework is gone)
- [x] Thin public identity wrappers retained where callers already use them

### Testing expectations

- [x] Identity (omit `--transforms-config`) stays green for each ported source
- [x] At least one external-transform e2e for every ported streaming source
- [x] CLI `--transforms-config` smoke where that source has CLI e2e coverage
      (binlog, pgoutput, wal2json, mysql trigger, postgresql-trigger, mongodb,
      neo4j, kafka, csv, jsonl)

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
