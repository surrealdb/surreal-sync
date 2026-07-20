# Transforms

Transform records on the way from the source into SurrealDB. Use this when you need enrichment or light ETL (e.g. call an OCR/embedding worker, reshape fields) before upserts and deletes land in the target. If you want source data unchanged, omit transforms entirely — that is the default and has **zero** transform overhead.

## How it fits

```text
Source (CDC / snapshot)  →  optional transform stages  →  SurrealDB
```

surreal-sync still owns batching, applying docs to SurrealDB, and when the source checkpoint may advance. Your transform worker only sees batches of documents (or changes) and returns transformed batches. A successful worker response is **not** durability — the checkpoint advances only after SurrealDB apply succeeds.

## What you can do today (v1)

| Capability | Status |
|------------|--------|
| No config → identity (docs pass through unchanged) | Available |
| External worker over child-process stdio (NDJSON) | Available |
| MySQL / MariaDB binlog `sync` (snapshot + stream) | Available |
| `failure_policy` `fail` (default) or `skip` | Available |
| Overlapping transform window via `max_in_flight` | Available |

## What is not available yet

- Other sources (PostgreSQL, MongoDB, trigger MySQL, …) — not wired to this path yet
- HTTP / Unix-socket / TCP workers
- Built-in field-mapping DSL, WASM plugins, or declarative rules in TOML
- Exactly-once end-to-end (delivery is at-least-once; see [Durability](#durability-and-acknowledgements))
- Dead-letter queues or worker-side durable queues

## CLI quick start

Transforms are configured with `--transforms-config <PATH>` on `surreal-sync from mysql-binlog sync`. Omit the flag for identity.

### No transform config (identity)

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

### External-only config (no passthrough)

Create `transforms.toml` with only your worker stage — do **not** add a `passthrough` entry:

```toml
[[transforms]]
type = "external"
failure_policy = "fail"
batch_size = 1000
batch_max_wait = "500ms"
timeout = "60s"
max_in_flight = 1
transport = "stdin"
stdin.mode = "persistent"
stdin.command = ["./enrich-worker"]
stdin.framer = "ndjson"
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

`stdin.command[0]` must exist on `PATH` or as a file path; surreal-sync fails fast at config load if the program is missing.

## Configuration reference

### Omitting transforms / empty list = identity

| Config | Behavior |
|--------|----------|
| No `--transforms-config` | Identity; no stage dispatch |
| Empty / whitespace-only file, or `transforms = []` | Identity |
| Lone `type = "passthrough"` | Collapses to identity (unnecessary for operators) |
| One or more `type = "external"` stages | Those stages run in order |

**Omit transforms entirely for “do nothing.”** **Omit `passthrough` when configuring `external`.** `passthrough` exists mainly for tests/library completeness, not as something you must write.

### `[[transforms]]` schema (`type = "external"`)

| Key | Default | Meaning |
|-----|---------|---------|
| `type` | (required) | `"external"` (or unused `"passthrough"`) |
| `failure_policy` | `"fail"` | `"fail"` or `"skip"` — see [Failure policy](#failure-policy) |
| `batch_size` | `1000` | Changes/rows accumulated before starting a transform batch (`>= 1`) |
| `batch_max_wait` | `"500ms"` | Flush a partial batch after this idle wait |
| `timeout` | `"60s"` | Per-batch transform timeout |
| `max_in_flight` | `1` | Transform window size (`>= 1`) — see [Tuning](#choosing-batch-size-timeouts-and-max_in_flight) |
| `transport` | `"stdin"` | v1: child-process stdio only |
| `stdin.mode` | `"persistent"` | `"persistent"` or `"transient"` |
| `stdin.command` | (required) | Argv to spawn, e.g. `["./enrich-worker"]` or `["worker", "--flag"]` |
| `stdin.framer` | `"ndjson"` | v1: NDJSON only |

Duration strings accept `ms`, `s`, `m`, `h` suffixes (case-insensitive), or a plain integer as seconds (`"500ms"`, `"60s"`, `"2m"`, `"1h"`, `"45"`).

When multiple external stages set the same option, the **last** stage wins for apply opts (`batch_size`, `timeout`, etc.). Stages themselves still run in listed order.

### Child stdio (`transport = "stdin"`)

`transport = "stdin"` means **the worker child’s** stdin/stdout pipes — not the surreal-sync CLI’s stdin. surreal-sync always `spawn`s `stdin.command`.

| `stdin.mode` | Lifecycle |
|--------------|-----------|
| `persistent` (default) | Spawn once at pipeline start; many batch exchanges on the same pipes |
| `transient` | Spawn → one batch exchange → wait for exit → repeat per batch |

Prefer `persistent` for real enrichment (avoids per-batch process startup). `transient` is useful for debugging/simple scripts; expect lower throughput. Overlap from `max_in_flight > 1` is much more useful with `persistent`.

## Choosing batch size, timeouts, and `max_in_flight`

- **`batch_size` / `batch_max_wait`** — how large a batch becomes before transform starts. Larger batches amortize worker overhead; smaller batches reduce latency.
- **`timeout`** — how long one transform exchange may take. On timeout the batch fails (see failure policy).
- **`max_in_flight`** — window size for concurrent transforms (default `1`). W=1 and W=16 share the **same** apply runtime: surreal-sync may transform several batches at once, match completions by `batch_id`, then **sink apply and source commit stay strictly ordered**. A failed batch blocks commit of later ones; in-flight successors are discarded (never committed).

Tune `max_in_flight` like batch size for latency hiding under a slow worker. Reliability rules do not change with W.

## Durability and acknowledgements

Durability is the **source checkpoint**, not the transform worker.

For every incremental batch, surreal-sync runs this order:

1. Buffer changes + positions in memory (not committed).
2. Transform — in-process stages finish, or the external worker returns a successful framed response that **echoes the same `batch_id`**.
3. Sink apply — write transformed docs to SurrealDB; wait for success.
4. Source commit — advance the binlog / GTID checkpoint past that batch.

| Hop | What “ack” means |
|-----|------------------|
| surreal-sync → worker | NDJSON request with `batch_id` |
| worker → surreal-sync | Response with the **same `batch_id`** (transform finished in memory only) |
| surreal-sync → SurrealDB | Sink write success |
| surreal-sync → source | Checkpoint / commit — safe to forget those CDC events |

There is **no** post-sink ack back to the worker. Workers are treated as **stateless**. If your worker has side effects (HTTP calls, etc.), it must tolerate **at-least-once** delivery of the same work after retries (often under a **new** `batch_id` while source positions replay).

### Restarts and retries

| Failure point | Checkpoint advanced? | On restart |
|---------------|----------------------|------------|
| Before/during transform (timeout, crash, bad NDJSON) | No | Same source positions replayed; worker may see duplicate work |
| Transform OK, sink fails | No | Replay; SurrealDB upserts make typical creates/updates idempotent |
| Sink OK, crash before commit | No | Replay; possible duplicate applies |
| Commit succeeded | Yes | Batch done |

Default `failure_policy = "fail"`: stop the sync process; on restart, resume from the last successful checkpoint — **no silent drop**.

### CatchUpProgress and unsunk work (MySQL/MariaDB binlog)

During streaming, surreal-sync may read ahead while transform/apply still has buffered, in-flight, or completed-but-not-yet-sunk batches. Persisted `CatchUpProgress` positions follow the **last successfully sunk** batch in that situation — they do **not** jump to a read-ahead cursor past unsunk work. After transform and apply drain, the usual stream position is safe to persist again. This keeps resume from replaying past docs that never landed in SurrealDB.

## Failure policy

| Policy | Behavior |
|--------|----------|
| `fail` (default) | Stop sync on transform or sink failure for a batch. Checkpoint stays behind that batch. Restart resumes from the last successful commit. |
| `skip` | Log the failure, **do not write** that batch to SurrealDB, but **still commit past it**. |

**Warning:** `skip` can **lose data** by explicit operator choice. Use only when dropping a bad batch is acceptable.

## Writing an external worker

### Whose pipes?

surreal-sync **spawns** your `stdin.command` and talks on **that process’s** stdin (requests) and stdout (responses). Do not read from the surreal-sync CLI process’s stdin.

### NDJSON protocol

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

### `persistent` vs `transient`

Both modes execute a process. Naming is about **lifetime**: one long-lived worker vs a new process per batch. Prefer `persistent` in production.

### Minimal echo worker (Python)

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

Make it executable and point `stdin.command` at it. Item JSON is the serde shape of surreal-sync’s universal row/change documents (typed values under `fields` / `data`, etc.). Start by echoing; then mutate fields your enrichment needs.

### Tips for enrichment tools

- Keep the worker **stateless** with respect to surreal-sync checkpoints.
- Side effects (external APIs) must tolerate retries / duplicate `batch_id`s or replayed source positions.
- Prefer `persistent` mode and tune `batch_size` / `max_in_flight` for throughput.
- Kreuzberg-style or similar tools: wrap the library in a long-running process that speaks this NDJSON loop rather than spawning a CLI per row.

### At-least-once

There is no exactly-once guarantee across transform + SurrealDB + source checkpoint. Design SurrealDB writes to be idempotent (upsert by primary key). Design workers so re-running a batch is safe.

## Using transforms with MySQL/MariaDB binlog

`--transforms-config` applies to `surreal-sync from mysql-binlog sync` for both the snapshot phase and the live stream. The same pipeline runs for snapshot row writes and CDC changes.

Operations (checkpoints, resume, GTID vs file+offset, ad-hoc `snapshot`) are unchanged aside from the durability rules above — especially that `CatchUpProgress` does not advance past unsunk transform/apply work. See [MySQL/MariaDB Binlog Source](mysql-binlog.md).

## Limitations

- No custom code in the TOML config — logic lives in your external worker (or, for embedders, library APIs; see rustdoc for `sync-transform`).
- Filter / reshape / fan-out via the worker’s returned item list (`count` may differ from the request) on **homogeneous** batches only. Mixed change+relation batches must preserve length of each kind.
- Relation events are first-class on the External NDJSON wire (`kind: relation_change` / `relation`); they are never silently skipped past External stages.
- v1 source coverage: MySQL/MariaDB binlog only.
- At-least-once delivery, not exactly-once.
- v1 transport/framer: child stdio + NDJSON only.

## Troubleshooting

| Symptom | What to check |
|---------|----------------|
| Worker not starting / config load fails | `stdin.command[0]` on `PATH` or as an existing file; argv non-empty; TOML parse errors |
| Timeouts | Raise `timeout`; check worker hangs; reduce `batch_size` or `max_in_flight` while debugging |
| Bad NDJSON / wrong `batch_id` | Echo the request `batch_id`; flush stdout after each response; no extra stderr framing on stdout |
| Sync stopped on failure | Default `failure_policy = "fail"` — fix the worker or sink, restart; checkpoint did not advance past the failed batch |
| Unexpected missing docs | You set `failure_policy = "skip"` — failed batches are committed past without writing |
| Checkpoint seems “stuck” behind read position | Expected while transform/apply still has unsunk work; `CatchUpProgress` tracks last sunk |

## See also

- [MySQL/MariaDB Binlog Source](mysql-binlog.md) — snapshot, stream, checkpoints, resume
- [Design overview](design.md) — full vs incremental sync model
- [GitHub issue #118](https://github.com/surrealdb/surreal-sync/issues/118) — transform pipeline tracking

### Advanced: embedding surreal-sync

Library / WASM hosts that need zero-copy in-process transforms should use the `sync-transform` crate rustdoc. The **general** APIs are `ApplyContext` (rows, changes, and relation edges) and `SourceDriver` / `run_source_runtime` (control-plane hooks for schema refresh, ad-hoc snapshot via injected `AdhocApply` helpers, cancel/deadline, sink-safe checkpoints including `CheckpointPolicy::IntervalWhenDrained`). `ChangeFeed` / `run_change_feed` are a convenience for simple row CDC. In-process stages use `InPlaceTransform` + `Pipeline::push_inplace` (including schema-aware FK→record-link transforms constructed with a catalog). That path is not configured via TOML.
