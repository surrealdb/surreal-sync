#!/usr/bin/env python3
"""Run the Snowflake TPC-H ingestion benchmark and emit loadtest-compatible metrics.

Drives `surreal-sync from snowflake` against the SNOWFLAKE_SAMPLE_DATA shared
database (TPC-H at a chosen scale factor) and writes a metrics.json in the schema
that loadtest/scripts/compare_metrics.py already consumes, so the existing
summary/regression tooling works unchanged.

Two passes, both optional via BENCH_MODE:

  dry-run  reads and type-converts every row but writes nothing, isolating
           source throughput (Snowflake REST + partition paging + conversion)
  write    the full pipeline, isolating what the SurrealDB sink adds

Verification compares the row count the source reported for each table against
the count actually present in SurrealDB afterwards, so it needs both passes'
information and does not hardcode any expected TPC-H row counts.

Configuration is entirely environment-driven; see docs/snowflake.md.
"""

import base64
import json
import os
import re
import resource
import subprocess
import sys
import time
import urllib.error
import urllib.request

# `Ingested 6001215 row(s) from table 'LINEITEM'` -- full_sync.rs
INGESTED_RE = re.compile(r"Ingested (\d+) row\(s\) from table '([^']+)'")
# `Dry-run scanned table 'LINEITEM': 6001215 row(s)` -- full_sync.rs
SCANNED_RE = re.compile(r"Dry-run scanned table '([^']+)': (\d+) row\(s\)")


def env(name, default=None, required=False):
    value = os.environ.get(name) or default
    if required and not value:
        sys.exit(f"error: {name} must be set")
    return value


class Config:
    def __init__(self):
        self.binary = env("SURREAL_SYNC_BIN", required=True)
        self.account = env("SNOWFLAKE_ACCOUNT", required=True)
        self.user = env("SNOWFLAKE_USER", required=True)
        self.key_path = env("SNOWFLAKE_PRIVATE_KEY_PATH", required=True)
        self.warehouse = env("SNOWFLAKE_WAREHOUSE", required=True)
        self.role = env("SNOWFLAKE_ROLE")
        self.scale = env("BENCH_SCALE", "TPCH_SF100")
        self.tables = [t.strip() for t in env("BENCH_TABLES", "CUSTOMER").split(",") if t.strip()]
        self.batch_size = env("BENCH_BATCH_SIZE", "5000")
        self.mode = env("BENCH_MODE", "both")
        self.namespace = env("BENCH_NAMESPACE", "bench")
        self.database = env("BENCH_DATABASE", "tpch")
        self.endpoint = env("SURREAL_ENDPOINT", "http://localhost:8000").rstrip("/")
        self.surreal_user = env("SURREAL_USERNAME", "root")
        self.surreal_pass = env("SURREAL_PASSWORD", "root")
        self.out_dir = env("BENCH_OUT_DIR", "bench-out")

        if self.mode not in ("both", "write", "dry-run"):
            sys.exit(f"error: BENCH_MODE must be both|write|dry-run, got {self.mode!r}")
        if not self.tables:
            sys.exit("error: BENCH_TABLES resolved to an empty list")


def peak_child_rss_mb():
    """Peak RSS across all children spawned so far, in MB.

    ru_maxrss is a high-water mark that never decreases, so this is the maximum
    over every pass rather than per-pass. That is exactly what we want to track:
    the source is documented to hold roughly one Snowflake result partition in
    memory, so this number should stay flat as the row count grows.
    """
    kb = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    if sys.platform == "darwin":  # macOS reports bytes, Linux kilobytes
        kb //= 1024
    return round(kb / 1024, 1)


def run_sync(cfg, tables, dry_run, log_path, namespace=None, database=None):
    """Run one sync pass. Returns (duration_seconds, per_table_counts)."""
    cmd = [
        cfg.binary, "from", "snowflake",
        "--account", cfg.account,
        "--user", cfg.user,
        "--private-key-path", cfg.key_path,
        "--warehouse", cfg.warehouse,
        # The shared sample-data database. Tables are always listed explicitly so
        # autoconf::list_tables never queries INFORMATION_SCHEMA on an imported
        # share, and --id-columns is deliberately omitted: it applies to every
        # table at once, and TPC-H tables have different primary keys. Omitting it
        # gives each table a sequential per-table index.
        "--database", "SNOWFLAKE_SAMPLE_DATA",
        "--schema", cfg.scale,
        "--tables", ",".join(tables),
        "--batch-size", cfg.batch_size,
        "--to-namespace", namespace or cfg.namespace,
        "--to-database", database or cfg.database,
        "--surreal-endpoint", cfg.endpoint,
        "--surreal-username", cfg.surreal_user,
        "--surreal-password", cfg.surreal_pass,
    ]
    if cfg.role:
        cmd += ["--role", cfg.role]
    if dry_run:
        cmd.append("--dry-run")

    child_env = dict(os.environ)
    # init() uses EnvFilter::from_default_env(), which is error-only when RUST_LOG
    # is unset -- we need info to read the per-table counts back out.
    child_env.setdefault("RUST_LOG", "info")

    label = "dry-run" if dry_run else "write"
    print(f"--> {label} pass: {', '.join(tables)}", flush=True)

    start = time.monotonic()
    with open(log_path, "w") as log:
        proc = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, env=child_env)
    duration = time.monotonic() - start

    output = open(log_path, encoding="utf-8", errors="replace").read()
    if proc.returncode != 0:
        sys.stderr.write(output[-8000:])
        sys.exit(f"error: {label} pass failed with exit code {proc.returncode}")

    pattern = SCANNED_RE if dry_run else INGESTED_RE
    counts = {}
    for match in pattern.finditer(output):
        table, count = (match.group(1), match.group(2)) if dry_run else (match.group(2), match.group(1))
        counts[table] = int(count)

    print(f"    {label} pass: {duration:.1f}s, {sum(counts.values())} rows", flush=True)
    return duration, counts


def surreal_query(cfg, sql):
    request = urllib.request.Request(
        f"{cfg.endpoint}/sql",
        data=sql.encode(),
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "text/plain",
            # v3 spells these surreal-ns/surreal-db; older servers accept NS/DB.
            "surreal-ns": cfg.namespace,
            "surreal-db": cfg.database,
            "NS": cfg.namespace,
            "DB": cfg.database,
            "Authorization": "Basic "
            + base64.b64encode(f"{cfg.surreal_user}:{cfg.surreal_pass}".encode()).decode(),
        },
    )
    with urllib.request.urlopen(request, timeout=300) as response:
        return json.load(response)


def count_surreal_rows(cfg, table):
    """Row count via count()/GROUP ALL -- never SELECT *, which would materialize
    millions of records client-side.

    SurrealDB reports statement errors inside a 200 response with status "ERR",
    so a failed query has to be detected from the body, not the HTTP status. A
    NotFound table is a real answer (zero rows ingested); anything else is a
    problem with the query itself and returns None so it is not mistaken for 0.
    """
    try:
        body = surreal_query(cfg, f"SELECT count() FROM {table} GROUP ALL")
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as err:
        print(f"    warning: count query for {table} failed: {err}", file=sys.stderr)
        return None

    for statement in body if isinstance(body, list) else []:
        if statement.get("status") == "ERR":
            message = str(statement.get("result") or "")
            # v3 tags this as kind "NotFound"; v2 only gives the message text.
            if statement.get("kind") == "NotFound" or "does not exist" in message:
                return 0
            print(
                f"    warning: count query for {table} errored: {message}",
                file=sys.stderr,
            )
            return None
        result = statement.get("result") or []
        # An existing but empty table yields result == [], i.e. zero rows.
        if result and isinstance(result[0], dict) and "count" in result[0]:
            return int(result[0]["count"])
    return 0


def main():
    cfg = Config()
    os.makedirs(cfg.out_dir, exist_ok=True)

    # Resume the warehouse on a trivial table so its cold-start does not get
    # attributed to ingestion throughput. NATION is 25 rows at every scale.
    run_sync(
        cfg, ["NATION"], dry_run=True,
        log_path=os.path.join(cfg.out_dir, "warmup.log"),
        namespace=cfg.namespace, database="warmup",
    )

    dry_run_seconds = None
    source_counts = {}
    if cfg.mode in ("both", "dry-run"):
        dry_run_seconds, source_counts = run_sync(
            cfg, cfg.tables, dry_run=True,
            log_path=os.path.join(cfg.out_dir, "dry-run.log"),
        )

    write_seconds = None
    written_counts = {}
    if cfg.mode in ("both", "write"):
        write_seconds, written_counts = run_sync(
            cfg, cfg.tables, dry_run=False,
            log_path=os.path.join(cfg.out_dir, "write.log"),
        )

    peak_mb = peak_child_rss_mb()

    # Verification: what the source reported vs what actually landed in SurrealDB.
    matched, mismatched, detail = 0, 0, {}
    if written_counts:
        for table in cfg.tables:
            expected = written_counts.get(table)
            actual = count_surreal_rows(cfg, table)
            detail[table] = {"source_rows": expected, "surrealdb_rows": actual}
            if expected is not None and actual == expected:
                matched += expected
            else:
                mismatched += expected or 0
                print(
                    f"::error::{table}: source reported {expected} rows, "
                    f"SurrealDB holds {actual}",
                    flush=True,
                )
        # Cross-check the two passes against each other when we ran both.
        for table, scanned in source_counts.items():
            written = written_counts.get(table)
            if written is not None and scanned != written:
                print(
                    f"::warning::{table}: dry-run scanned {scanned} rows but the "
                    f"write pass ingested {written}",
                    flush=True,
                )

    total_rows = sum(written_counts.values()) or sum(source_counts.values())
    primary_seconds = write_seconds if write_seconds is not None else dry_run_seconds
    total_seconds = sum(s for s in (dry_run_seconds, write_seconds) if s is not None)

    def rate(rows, seconds):
        """Rows/sec, guarding against a duration too small to divide by."""
        if not rows or seconds is None or seconds < 0.001:
            return None
        return round(rows / seconds, 1)

    def secs(value):
        return round(value, 2) if value is not None else None

    metrics = {
        "source": "snowflake",
        "preset": cfg.scale,
        "git_sha": os.environ.get("GITHUB_SHA", ""),
        "git_ref": os.environ.get("GITHUB_REF_NAME", ""),
        "results": {
            "throughput_total_rows_per_sec": rate(total_rows, primary_seconds) or 0,
            "total_duration_seconds": secs(total_seconds) or 0,
            "sync_duration_seconds": secs(primary_seconds) or 0,
        },
        "resources": {"peak_memory_mb": peak_mb},
        "verification": {"matched": matched, "mismatched": mismatched},
        "config": {
            "preset": cfg.scale,
            "sync_containers": 1,
            "cpu_limit": str(os.cpu_count()),
            "memory_limit": "-",
        },
        # Extra context compare_metrics.py ignores but humans want.
        "snowflake": {
            "scale": cfg.scale,
            "tables": cfg.tables,
            "batch_size": int(cfg.batch_size),
            "mode": cfg.mode,
            "total_rows": total_rows,
            "dry_run_seconds": secs(dry_run_seconds),
            "write_seconds": secs(write_seconds),
            "dry_run_rows_per_sec": rate(sum(source_counts.values()), dry_run_seconds),
            "write_rows_per_sec": rate(sum(written_counts.values()), write_seconds),
            "verification_detail": detail,
            "surrealdb_image": os.environ.get("SURREALDB_IMAGE", ""),
        },
    }

    metrics_path = os.path.join(cfg.out_dir, "metrics.json")
    with open(metrics_path, "w") as handle:
        json.dump(metrics, handle, indent=2)
    print(json.dumps(metrics, indent=2))
    print(f"wrote {metrics_path}")

    if mismatched:
        sys.exit("error: row count verification failed")


if __name__ == "__main__":
    main()
