# How sync works

surreal-sync moves data from a source into SurrealDB through one shared apply path: **read → optional transform → ordered sink → watermark**. Transforms are optional; omit them and docs pass through unchanged with **zero** transform overhead.

**Who should read this:** anyone shipping production or high-throughput syncs (batching, overlapping apply windows, checkpoint/watermark rules). Operators who only need enrichment/ETL can jump to [Optional transform workers](#optional-transform-workers). Source-specific setup stays in the per-source guides — this page is the end-to-end pipeline, not a duplicate of connector docs.

## The pipeline at a glance

```text
Source (CDC / snapshot / file / Kafka)
  →  read / buffer into apply window
  →  optional transform stages (T)
  →  ordered SurrealDB sink (W)
  →  advance_watermark / checkpoint
```

In short: **R** (source reads) can run ahead while **T** (transforms) and **W** (sink writes) are in flight under `max_in_flight`. Sink apply and watermark advance stay **strictly ordered**. A successful transform worker response is **not** durability — the checkpoint advances only after SurrealDB apply succeeds (or `skip` + `advance_watermark`; see [Failure policy](#failure-policy)).

surreal-sync owns batching, applying docs to SurrealDB, and when the source checkpoint may advance. Your transform worker (if any) only sees batches of documents (or changes) and returns transformed batches.

## Source

Each `from *` sync/import path uses the same shared apply path. Operator guides cover connection strings, snapshots, and resume; implementers wire ports through `SourceDriver` / `run_source_runtime` (or chunk helpers).

| Topic | Where to go |
|-------|-------------|
| Per-source setup (MySQL, PostgreSQL, MongoDB, …) | Guides linked in [See also](#see-also) |
| Port checklist / R∩T∩W gates | [Source ports](source-ports.md) |
| Full vs incremental model | [Design overview](design.md) |

**Read-ahead vs sunk:** On streaming CDC, surreal-sync may read ahead while transform/apply still has buffered or in-flight work. Persisted catch-up / last-sunk positions track the **last successfully sunk** batch — they do **not** jump to a read-ahead cursor past unsunk work. See [CatchUpProgress and unsunk work](#catchupprogress-and-unsunk-work-streaming-cdc).

Some ports still **gate the next chunk / peek / file** until the current unit is fully sunk (interleaved snapshot next-chunk, wal2json next-peek after slot advance, CSV/JSONL next-file runtime). Within that unit, `max_in_flight > 1` still overlaps reads, transforms, and ordered writes — see [Source ports — R∩T∩W gates](source-ports.md#rtw-gates-intentional).

## Apply window / `[pipeline]` knobs

The apply window controls how many batches may be transforming or waiting for ordered sink at once. Options live under **`[pipeline]`** in the transforms TOML (or defaults when you pass an empty/passthrough file). They are **not** SurrealDB sink settings — the name is `pipeline` so they are not confused with sink/`apply` APIs.

| Key | Default | Meaning |
|-----|---------|---------|
| `failure_policy` | `"fail"` | `"fail"` or `"skip"` after a batch permanently fails transform or sink — see [Failure policy](#failure-policy) |
| `batch_size` | `1000` | Changes/rows accumulated before starting a transform batch (`>= 1`) |
| `batch_max_wait` | `"500ms"` | Flush a partial batch after this idle wait |
| `timeout` | `"60s"` | Outer timeout covering the full stage chain (including per-stage retries) |
| `max_in_flight` | `1` | Apply window size (`>= 1`) — see [Choosing batch size, timeouts, and `max_in_flight`](#choosing-batch-size-timeouts-and-max_in_flight) |

### Choosing batch size, timeouts, and `max_in_flight`

- **`batch_size` / `batch_max_wait`** — how large a batch becomes before transform starts (`[pipeline]`). Larger batches amortize worker overhead; smaller batches reduce latency.
- **`[pipeline].timeout`** — outer bound for the whole stage chain (including retries). Prefer per-stage `timeout` for individual workers.
- **Per-stage `timeout` / `retry`** — how long one exchange may take on that stage, and how many times to retry with backoff before the batch fails (only when using command workers).
- **`max_in_flight`** — apply window size (default `1`). With `max_in_flight > 1`, surreal-sync may transform several batches at once and keep reading while earlier batches write to SurrealDB. Writes and watermark advances stay in source order. Full sync uses the same rules — there is no separate “identity shortcut.”

Tune `max_in_flight` like batch size for latency hiding under a slow worker. Reliability rules do not change with W. **Omit `--transforms-config`** → `ApplyOpts::identity()` (`batch_size = 1`, `max_in_flight = 1`) so CDC stays on per-event cadence with no overlapping window; overlap requires an explicit TOML (or empty/passthrough file defaults, which use `batch_size = 1000` but still `max_in_flight = 1` unless set).

**Best-case R∩T∩W** (source reads continuing while transforms run and ordered sink writes stay in flight) needs **`max_in_flight > 1`**. With the default `1`, surreal-sync still orders sink apply and sink-gates cursors, but there is no overlapping transform/sink window to hide latency.

## Sink and durability

Durability is the **source checkpoint**, not the transform worker.

For every incremental batch on sources with a real post-sink durability hook, surreal-sync runs this order:

1. Buffer changes + positions in memory (not yet sink-safe).
2. Transform — in-process stages finish, or the external worker returns a successful framed response that **echoes the same `batch_id`** (identity if no transforms).
3. Sink apply — write docs to SurrealDB; wait for success.
4. `note_sunk_events` → `advance_watermark` → checkpoint policy → optional `persist_checkpoint`.

| Source family | What `advance_watermark` after sink means |
|---------------|--------------------------------|
| MySQL/MariaDB binlog, PostgreSQL pgoutput | Store / binlog client commit + sink-safe CatchUpProgress |
| PostgreSQL wal2json | Slot `advance` only after emitted events are sunk (peeks may continue under window capacity via non-consuming peek + prefix skip) |
| Kafka | Consumer-group `commit_batch` of **all** messages in the sunk batch (not only the last position) |
| CSV / JSONL | No source cursor (file import) |
| MySQL/PostgreSQL trigger, MongoDB change stream, Neo4j | After SurrealDB write succeeds, `advance_watermark(position)` marks an **in-memory sink-safe cursor**. Fetch/read-ahead may be ahead of that cursor; `checkpoint()` / resume-token handles report the sunk watermark, not the read head. There is still **no mid-run durable store write** on these ports — process restart resumes from the last **persisted** sync checkpoint (phase markers / `--from`), so long incremental runs may reprocess after a crash (at-least-once). |

| Hop | What “ack” means |
|-----|------------------|
| surreal-sync → worker | NDJSON request with `batch_id` (when using a command worker) |
| worker → surreal-sync | Response with the **same `batch_id`** (transform finished in memory only) |
| surreal-sync → SurrealDB | Sink write success |
| surreal-sync → source | `advance_watermark` / `persist_checkpoint` where the source implements post-sink hooks (see table above) |

There is **no** post-sink ack back to the worker. Workers are treated as **stateless**. If your worker has side effects (HTTP calls, etc.), it must tolerate **at-least-once** delivery of the same work after retries (often under a **new** `batch_id` while source positions replay).

### Restarts and retries

| Failure point | Checkpoint advanced? | On restart |
|---------------|----------------------|------------|
| Before/during transform (timeout, crash, bad NDJSON) | No | Same source positions replayed; worker may see duplicate work |
| Transform OK, sink fails | No | Replay; SurrealDB upserts make typical creates/updates idempotent |
| Sink OK, crash before `advance_watermark` | No | Replay; possible duplicate applies |
| `advance_watermark` succeeded (and persist, when policy requires it) | Yes | Batch done |

Default `failure_policy = "fail"`: stop the sync process; on restart, resume from the last successful checkpoint — **no silent drop**.

### CatchUpProgress and unsunk work (streaming CDC)

During streaming on sources that persist a catch-up / last-sunk checkpoint (notably MySQL/MariaDB binlog and PostgreSQL pgoutput), surreal-sync may read ahead while transform/apply still has buffered, in-flight, or completed-but-not-yet-sunk batches. Persisted catch-up positions follow the **last successfully sunk** batch in that situation — they do **not** jump to a read-ahead cursor past unsunk work.

Once the apply window is fully drained:

- Sunk watermarks are written promptly (same durability idea as persisting last-sunk on sink success).
- On `--checkpoint-interval` (default 10s where supported), the store may also advance to the **current** source position through filtered/unrelated traffic — there is nothing unsunk to protect, so catching up past other schemas/tables does not freeze progress until process exit.

This keeps resume from replaying past docs that never landed in SurrealDB, without stalling checkpoint heartbeats during filtered-only catch-up.

### Failure policy

| Policy | Behavior |
|--------|----------|
| `fail` (default) | Stop sync on transform or sink failure for a batch. Checkpoint stays behind that batch. Restart resumes from the last successful watermark advance. |
| `skip` | Log the failure, **do not write** that batch to SurrealDB, but **still `advance_watermark` past it**. |

**Warning — data loss by configuration:** `failure_policy = "skip"` means a failed transform or sink batch is **never** applied to SurrealDB, yet `advance_watermark` still runs past it. Those source events are gone for this sync unless you re-seed from an earlier checkpoint or re-run a full sync. Prefer the default `fail` unless dropping bad batches is an explicit, accepted trade-off.

## Optional transform workers

Use transforms when you need enrichment or light ETL (e.g. call an OCR/embedding worker, reshape fields) before upserts and deletes land in the target. If you want source data unchanged, omit transforms entirely — that is the default.

### What you can do today

| Capability | Status |
|------------|--------|
| No config → identity (docs pass through unchanged) | Available |
| External worker over child-process stdio (NDJSON) | Available (`type = "command"`) |
| `--transforms-config` on every `from *` sync path listed below | Available |
| `failure_policy` `fail` (default) or `skip` | Available (`[pipeline]`) |
| Per-stage `retry` / backoff | Available on each `[[transforms]]` command stage |
| Overlapping transform window via `max_in_flight` | Available on CDC/`SourceDriver` paths (`[pipeline]`) |
| Full-sync / `write_rows` windowing | Available (same ApplyContext window; no oneshot bypass) |

### What is not available yet

- HTTP / Unix-socket / TCP workers
- Built-in field-mapping DSL, WASM plugins, or declarative rules in TOML
- Exactly-once end-to-end (delivery is at-least-once; see [Sink and durability](#sink-and-durability))
- Dead-letter queues or worker-side durable queues

### Commands that support `--transforms-config`

Every sync/import path below loads the same TOML via the shared CLI helper and runs through the shared apply path. Omit the flag for identity.

| Command | Sync paths |
|---------|------------|
| `from mysql-binlog sync` | Snapshot + stream (and ad-hoc `snapshot` while streaming) |
| `from postgresql-pgoutput sync` | Snapshot + stream (and ad-hoc `snapshot` while streaming) |
| `from postgresql full` / `incremental` / `sync` | wal2json snapshot, stream, and interleaved `sync` |
| `from postgresql-trigger full` / `incremental` / `sync` | Trigger snapshot, stream, and interleaved `sync` |
| `from mysql full` / `incremental` / `sync` | Trigger snapshot, stream, and interleaved `sync` (also MariaDB via the same subcommand) |
| `from mongodb full` / `incremental` | Collection dump + change stream |
| `from neo4j full` / `incremental` | Nodes and relationships |
| `from kafka` | SourceDriver window; offset `commit_batch` of all sunk messages after sink |
| `from csv` | Long-lived SourceDriver streams file reads into the window |
| `from jsonl` | Long-lived SourceDriver streams line reads into the window |

### CLI quick start

Transforms are configured with `--transforms-config <PATH>` on any command in the table above. The examples below use MySQL/MariaDB binlog; the same flag and TOML work on the other sources. Omit the flag for identity.

#### No transform config (identity)

```bash
surreal-sync from mysql-binlog sync \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --tables "users,orders" \
  --checkpoint-dir ".surreal-sync-checkpoints"
```

Without `--transforms-config`, docs go source → SurrealDB unchanged. There is **no** transform stage dispatch (empty pipeline).

#### Command-worker config (no passthrough)

Create `transforms.toml` with only your worker stage(s) — do **not** add a `passthrough` entry:

```toml
[pipeline]
failure_policy = "fail"
batch_size = 1000
batch_max_wait = "500ms"
timeout = "120s"
max_in_flight = 1

[[transforms]]
type = "command"
command = ["./enrich-worker"]
mode = "persistent"
timeout = "60s"
stdio.framer = "ndjson"
retry.max_attempts = 3
retry.initial_backoff = "200ms"
retry.max_backoff = "30s"
```

```bash
surreal-sync from mysql-binlog sync \
  --connection-string "$CONNECTION_STRING" \
  --database "myapp" \
  --surreal-endpoint "$SURREAL_ENDPOINT" \
  --surreal-username "root" \
  --surreal-password "root" \
  --to-namespace "production" \
  --to-database "migrated_data" \
  --tables "users,orders" \
  --checkpoint-dir ".surreal-sync-checkpoints" \
  --transforms-config transforms.toml
```

`command[0]` must exist on `PATH` or as a file path; surreal-sync fails fast at config load if the program is missing.

#### Daisy-chained stages (two commands)

Each `[[transforms]]` entry is its own stage with its own argv, stdio framer, timeout, and retry policy. Stages run in listed order. Do **not** use `[[transforms.stdio]]` — that is not how TOML array-of-tables nesting works for multiple stages.

```toml
[pipeline]
batch_size = 100
max_in_flight = 2
failure_policy = "fail"

[[transforms]]
type = "command"
command = ["./ocr-worker"]
mode = "persistent"
timeout = "60s"
stdio.framer = "ndjson"
retry.max_attempts = 5
retry.initial_backoff = "200ms"
retry.max_backoff = "30s"

[[transforms]]
type = "command"
command = ["./embed-worker", "--model", "text-embedding"]
mode = "persistent"
timeout = "30s"
stdio.framer = "ndjson"
retry.max_attempts = 3
retry.initial_backoff = "100ms"
retry.max_backoff = "10s"
```

v1 supports only `stdio.framer = "ndjson"`, but the framer is still **per stage** so future framers can differ across the chain.

### Configuration reference

#### Omitting transforms / empty list = identity

| Config | Behavior |
|--------|----------|
| No `--transforms-config` | Identity; no stage dispatch |
| Empty / whitespace-only file, or `transforms = []` | Identity |
| Lone `type = "passthrough"` | Collapses to identity (unnecessary for operators) |
| One or more `type = "command"` stages | Those stages run in order |

**Omit transforms entirely for “do nothing.”** **Omit `passthrough` when configuring `command`.** `passthrough` exists mainly for tests/library completeness, not as something you must write.

`[pipeline]` keys are documented under [Apply window / `[pipeline]` knobs](#apply-window--pipeline-knobs).

#### `[[transforms]]` schema (`type = "command"`)

| Key | Default | Meaning |
|-----|---------|---------|
| `type` | (required) | `"command"` (or unused `"passthrough"`) |
| `command` | (required) | Argv to spawn, e.g. `["./enrich-worker"]` or `["worker", "--flag"]` |
| `mode` | `"persistent"` | `"persistent"` or `"transient"` |
| `timeout` | (none) | Per-exchange timeout for **this** stage only |
| `stdio.framer` | `"ndjson"` | Wire framer for this stage (v1: NDJSON only) |
| `retry.max_attempts` | `1` | Total tries including the first (`1` = no retry) |
| `retry.initial_backoff` | `"200ms"` | Backoff after the first failure |
| `retry.max_backoff` | `"30s"` | Cap for exponential backoff |
| `retry.jitter` | `true` | Scale each sleep by ~0.5–1.5× |

Duration strings accept `ms`, `s`, `m`, `h` suffixes (case-insensitive), or a plain integer as seconds (`"500ms"`, `"60s"`, `"2m"`, `"1h"`, `"45"`).

#### Child stdio (`type = "command"`)

A command stage spawns a worker and talks on **that process’s** stdin (requests) and stdout (responses) — not the surreal-sync CLI’s stdin. stderr is inherited for logs only (not framed control).

| `mode` | Lifecycle |
|--------|-----------|
| `persistent` (default) | Spawn once at pipeline start; many batch exchanges on the same pipes |
| `transient` | Spawn → one batch exchange → wait for exit → repeat per batch |

Prefer `persistent` for real enrichment (avoids per-batch process startup). `transient` is useful for debugging/simple scripts; expect lower throughput. Overlap from `max_in_flight > 1` is much more useful with `persistent`.

Per-stage `retry` re-runs the stdio exchange for that stage only (same `batch_id`) with exponential backoff. After attempts are exhausted, the batch fails and `[pipeline].failure_policy` applies (`fail` or `skip`). Checkpoint still advances only after SurrealDB sink success (or skip + `advance_watermark`).

### Writing an external worker

#### Whose pipes?

surreal-sync **spawns** your `command` and talks on **that process’s** stdin (requests) and stdout (responses). Do not read from the surreal-sync CLI process’s stdin.

#### NDJSON protocol

**Request** (surreal-sync → worker):

1. Header line: `{"batch_id":<u64>,"count":<n>}` — optional `"kind"` of `change` (default), `row`, `relation_change`, or `relation`
2. Exactly `count` NDJSON item lines — each a serialized change (incremental), row (snapshot / full sync), relation change, or relation (matching `kind`)

**Response** (worker → surreal-sync):

1. Header line that **must echo the same `batch_id`**, plus either:
   - `"count": <m>` and then `m` item lines, or
   - `"error": "<message>"` (no items; batch fails)
2. Item lines are positional; `count` may change for filter / fan-out **on homogeneous batches only**

Mismatched or missing `batch_id` ⇒ failed exchange (no SurrealDB write, no checkpoint advance). Responses may complete **out of order** relative to sends when `max_in_flight > 1`; surreal-sync correlates by `batch_id` and still applies in source order.

**Mixed change+relation batches:** External stages exchange **both** kinds over NDJSON (no silent relation pass-through). Filter/fan-out that changes item count is **not** supported when a batch interleaves row changes and relation changes — use homogeneous batches (all changes or all relations) for length-changing transforms. The same limit applies to in-process `BatchTransformer::transform_events`.

When one External stage sees a mixed batch, surreal-sync issues **two** sequential wire exchanges with **distinct** `batch_id`s: row changes keep the apply batch id; relation changes use that id with the high bit set (`relation_wire_batch_id`). Workers and scripts must not assume a single `batch_id` covers both kinds in one apply batch.

#### `persistent` vs `transient`

Both modes execute a process. Naming is about **lifetime**: one long-lived worker vs a new process per batch. Prefer `persistent` in production.

#### Minimal echo worker (Python)

```python
#!/usr/bin/env python3
"""Echo NDJSON transform worker for surreal-sync (stdin/stdout)."""
import json
import sys

def main() -> None:
    stdin = sys.stdin
    while True:
        header_line = stdin.readline()
        if not header_line:
            break
        header_line = header_line.strip()
        if not header_line:
            continue
        header = json.loads(header_line)
        batch_id = header["batch_id"]
        count = int(header["count"])
        items = [json.loads(stdin.readline()) for _ in range(count)]
        # Optional: mutate items here (enrich, filter, fan-out).
        sys.stdout.write(json.dumps({"batch_id": batch_id, "count": len(items)}) + "\n")
        for item in items:
            sys.stdout.write(json.dumps(item, separators=(",", ":")) + "\n")
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

Make it executable and point `command` at it. Item JSON is the serde shape of surreal-sync’s universal row/change documents (typed values under `fields` / `data`, etc.). Start by echoing; then mutate fields your enrichment needs.

#### Tips for enrichment tools

- Keep the worker **stateless** with respect to surreal-sync checkpoints.
- Side effects (external APIs) must tolerate retries / duplicate `batch_id`s or replayed source positions.
- Prefer `persistent` mode and tune `batch_size` / `max_in_flight` for throughput.
- Kreuzberg-style or similar tools: wrap the library in a long-running process that speaks this NDJSON loop rather than spawning a CLI per row.

#### At-least-once

There is no exactly-once guarantee across transform + SurrealDB + source checkpoint. Design SurrealDB writes to be idempotent (upsert by primary key). Design workers so re-running a batch is safe.

### Using transforms with any supported source

`--transforms-config` applies to every command in [Commands that support `--transforms-config`](#commands-that-support---transforms-config).

- **CDC / Kafka / CSV / JSONL** — long-lived `SourceDriver` + `run_source_runtime`: reads can overlap transforms and ordered sink writes under `max_in_flight` (CSV/JSONL: within each file; multi-file imports restart the runtime per file).
- **Full sync / keyset table scans** — MySQL and PostgreSQL keyset paths use a long-lived `RowChunkDriver` so the next chunk read can overlap prior-chunk transform/sink when `max_in_flight > 1` (same idea as CSV/JSONL streaming). Relation tables and no-PK tables stream via `RelationChunkDriver` / OFFSET chunks the same way (no monolithic `SELECT *` + serial `write_rows`).
- **Interleaved snapshot reconciliation** — CDC events and surviving chunk rows share one long-lived `run_source_runtime` window across chunks, so polls continue under spare `max_in_flight` while ordered sink runs; `commit_reconciled` runs only after each chunk is sink-safe (the next chunk does not start until then).
- **MongoDB / Neo4j full** — cursor/result streams use `RowChunkDriver` / `RelationChunkDriver` (Neo4j: all nodes before any edges).
- **Ad-hoc helpers** — remaining `write_rows` / `write_relations` call sites use the same ApplyContext window within each call; they are not a continuous source poll loop.

Operations (checkpoints, resume, ad-hoc `snapshot` where the source supports it) follow the durability rules above. For catch-up vs unsunk work on streaming CDC, see [CatchUpProgress and unsunk work](#catchupprogress-and-unsunk-work-streaming-cdc). Per-source guides and [Source ports](source-ports.md) cover implementer details.

### Limitations

- No custom code in the TOML config — logic lives in your external worker (or, for embedders, library APIs; see rustdoc for `sync-transform`).
- Filter / reshape / fan-out via the worker’s returned item list (`count` may differ from the request) on **homogeneous** batches only. Mixed change+relation batches must preserve length of each kind.
- Relation events are first-class on the External NDJSON wire (`kind: relation_change` / `relation`); they are never silently skipped past External stages.
- At-least-once delivery, not exactly-once.
- v1 transport/framer: child stdio + NDJSON only.
- Trigger / MongoDB / Neo4j incremental ports keep an **in-memory** sink-safe cursor after SurrealDB apply (`advance_watermark` / `commit_sunk`); they do **not** persist that cursor mid-run to the checkpoint store. Crash resume uses the last **persisted** phase marker / `--from` (at-least-once). See [Sink and durability](#sink-and-durability).
- Tables without a primary key on non-interleaved MySQL/PostgreSQL full sync stream via `LIMIT`/`OFFSET` chunks (PostgreSQL orders by `ctid`; ids are synthetic row indexes). That path is unsafe under concurrent source writes — prefer a usable PK with keyset reads or interleaved-snapshot.
- CSV / JSONL multi-file imports run one long-lived driver **per file**; there is no cross-file apply window.

### Troubleshooting

| Symptom | What to check |
|---------|----------------|
| Worker not starting / config load fails | `command[0]` on `PATH` or as an existing file; argv non-empty; TOML parse errors |
| Timeouts | Raise `timeout`; check worker hangs; reduce `batch_size` or `max_in_flight` while debugging |
| Bad NDJSON / wrong `batch_id` | Echo the request `batch_id`; flush stdout after each response; no extra stderr framing on stdout |
| Sync stopped on failure | Default `failure_policy = "fail"` — fix the worker or sink, restart; checkpoint did not advance past the failed batch |
| Unexpected missing docs | You set `failure_policy = "skip"` — failed batches are advanced past without writing |
| Checkpoint seems “stuck” behind read position | Expected while transform/apply still has unsunk work; `CatchUpProgress` tracks last sunk |

## See also

- [MySQL/MariaDB Binlog Source](mysql-binlog.md) — snapshot, stream, checkpoints, resume
- [PostgreSQL pgoutput](postgresql-pgoutput-source.md), [wal2json](postgresql-wal2json-source.md), [trigger](postgresql.md)
- [MySQL trigger](mysql.md) / [MariaDB trigger](mariadb.md), [MongoDB](mongodb.md), [Neo4j](neo4j.md), [Kafka](kafka.md), [CSV](csv.md), [JSONL](jsonl.md)
- [Source ports](source-ports.md) — implementer checklist for wiring sources through `sync-transform`
- [Design overview](design.md) — full vs incremental sync model
- [GitHub issue #118](https://github.com/surrealdb/surreal-sync/issues/118) — transform pipeline tracking

### Advanced: embedding surreal-sync

For in-process transforms and custom drivers, see `sync-transform` rustdoc (`ApplyContext`, `SourceDriver`). TOML only covers external command workers.
