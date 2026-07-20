# Source ports onto the transform framework

Implementer checklist for wiring each `from *` sync path through
`sync-transform` (`SourceDriver` / `run_source_runtime` / `write_rows` /
`write_relations`) and the shared CLI helper in [`src/from/transforms.rs`](../src/from/transforms.rs).

Operator-facing transforms docs stay in [transforms.md](transforms.md).

## Shared CLI

- [x] `load_transforms_from_args` in `src/from/transforms.rs`
- [x] `from mysql-binlog sync` uses the shared loader
- [ ] Other `from *` sync commands take `--transforms-config` and call the
      shared loader when each source is ported (see below)

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
| postgresql-pgoutput | Pending | Pending with port | Target binlog parity |
| postgresql-wal2json | Pending | Pending with port | Preserve FK pre-push enrichment |
| postgresql-trigger | Pending | Pending with port | Preserve FK pre-push enrichment |
| mysql (trigger) | Pending | Pending with port | |
| mongodb | Pending | Pending with port | |
| neo4j | Pending | Pending with port | Nodes + edges via mixed events |
| kafka | Pending (`write_rows`) | Pending with port | |
| csv | Pending (`write_rows`) | Pending with port | |
| jsonl | Pending (`write_rows`) | Pending with port | Keep `conversion_rules` before Pipeline |

## Porting rules (short)

1. Streaming CDC sources implement `SourceDriver` and call
   `run_source_runtime` / `run_source_runtime_with` — no production
   hand-rolled `ApplyContext` loops after port; do not use `ChangeFeed`
   for production ports.
2. Batch importers (csv, jsonl, kafka) use `write_rows` only.
3. Every WatermarkSource consumer gets transform-aware interleaved /
   ad-hoc entrypoints and threads `Pipeline` / `ApplyOpts` from CLI.
4. Identity (omit `--transforms-config`) must stay green; add at least one
   external-transform e2e when porting a streaming source.
