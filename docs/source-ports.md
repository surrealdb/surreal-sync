# Wiring sources into the shared apply path

Implementer checklist for wiring each `from *` sync path through
`sync-transform` (`SourceDriver` / `run_source_runtime` / `write_rows` /
`write_relations`) and the shared CLI helper in [`src/from/transforms.rs`](../src/from/transforms.rs).

Operator-facing sync pipeline docs (including optional transforms) stay in [sync-pipeline.md](sync-pipeline.md).

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

| Source | Apply path | `--transforms-config` CLI | Interleaved `_with_transforms` | Notes |
|--------|------------|---------------------------|--------------------------------|--------|
| mysql-binlog | SourceDriver + `run_source_runtime_with` | Yes (shared loader + CLI e2e) | Yes | Reference port; sink-safe CatchUpProgress |
| postgresql-pgoutput | SourceDriver + `run_source_runtime_with` | Yes (shared loader + CLI e2e) | Yes | Binlog parity; sink-safe CatchUpProgress |
| postgresql-wal2json | SourceDriver | Yes (shared loader + CLI e2e) | Yes | Non-consuming peek + prefix skip; slot advance after sunk |
| postgresql-trigger | SourceDriver | Yes (shared loader + CLI e2e) | Yes | FK pre-push enrichment; `advance_watermark` → in-memory `commit_sunk` (sunk vs read cursor); no mid-run durable store |
| mysql (trigger) | SourceDriver | Yes (shared loader + CLI e2e) | Yes | `advance_watermark` → in-memory `commit_sunk` (sunk vs read); no DDL / ad-hoc; no mid-run durable store |
| mongodb | SourceDriver + RowChunkDriver full | Yes (shared loader + CLI e2e) | N/A | Resume token advanced on `advance_watermark` after sink (not on stream read); no mid-run durable store |
| neo4j | SourceDriver + RowChunkDriver / RelationChunkDriver full | Yes (shared loader + CLI e2e) | N/A | Nodes fully before edges; `advance_watermark` → in-memory `commit_sunk` (timestamp + tie-break ids); fetch may be ahead of sunk |
| kafka | SourceDriver + `run_source_runtime` | Yes (shared loader + CLI e2e) | N/A | `commit_batch` all sunk msgs; `note_sunk_events` counts |
| csv | Long-lived SourceDriver stream | Yes (shared loader + CLI e2e) | N/A | File read polls into window (no per-batch runtime restart); **one runtime per file** (no cross-file R∩T∩W) |
| jsonl | Long-lived SourceDriver stream | Yes (shared loader + CLI e2e) | N/A | `conversion_rules` before Pipeline; **one runtime per file** (same as CSV) |

## Porting checklist

1. Streaming CDC sources implement `SourceDriver` and call
   `run_source_runtime` / `run_source_runtime_with` — no production
   hand-rolled `ApplyContext` loops; do not use `ChangeFeed` for production
   ports.
2. File batch importers (csv, jsonl) use a **long-lived** `SourceDriver` that
   streams reads into `run_source_runtime` so `max_in_flight` windowing applies
   continuously **within each file**. Multi-file imports intentionally restart
   the runtime per file (no cross-file pipelining). Kafka commits consumer-group
   offsets for **all** sunk messages in a batch only after sink success.
3. Neo4j: nodes and edges through one `SourceDriver` emitting mixed
   `PositionedEvent`s. wal2json / postgresql-trigger: FK transforms as
   source-side pre-push enrichment; relation `PositionedEvent`s where those
   sources already emit relations.
4. Every WatermarkSource consumer gets transform-aware interleaved /
   ad-hoc entrypoints and threads `Pipeline` / `ApplyOpts` from CLI
   (binlog, pgoutput, wal2json, postgresql-trigger, mysql-trigger). Ad-hoc
   snapshot (binlog / pgoutput) uses transform-aware helpers; mysql-trigger
   has no ad-hoc / DDL by design.
5. Thin identity wrappers (`run_*` → `run_*_with_transforms` + identity) may
   stay for existing public names; production CLI always threads Pipeline /
   ApplyOpts. Dead direct-apply paths that bypass the shared apply path must
   stay gone.
6. Identity (omit `--transforms-config`) must stay green; add at least one
   external-transform e2e when porting a streaming source. CLI
   `--transforms-config` smoke where that source has CLI e2e coverage.

## R∩T∩W gates (intentional)

Best-case read∩transform∩write overlap needs `max_in_flight > 1`. Even then,
some ports **gate the next unit of source work** until the current unit is
fully sink-safe — within-unit R∩T∩W still applies:

| Port / path | Within-unit overlap @ W>1 | Next-unit gate |
|-------------|---------------------------|----------------|
| Interleaved snapshot | Yes, within a chunk (CDC + surviving rows share one window) | Next chunk only after `events_sunk >= events_emitted` (`commit_reconciled`) |
| wal2json incremental | Yes, within a peek / emitted prefix | Next peek only after slot `advance` requiring sunk ≥ emitted |
| CSV / JSONL multi-file | Yes, within a file | Next file starts a fresh `run_source_runtime` |

These gates are intentional — they keep the next chunk/peek/file from starting
before the current one is fully written, so watermark / slot / reconcile
cursors cannot race ahead of unsunk apply work.
