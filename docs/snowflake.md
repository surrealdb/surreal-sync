# Snowflake Source Usage Guide

The Snowflake source in surreal-sync imports tables from a Snowflake database into SurrealDB. This is a **full-snapshot source**: it runs a one-shot read of the selected tables via the Snowflake SQL REST API v2 and writes the rows to SurrealDB. There is no incremental/CDC mode for Snowflake.

Optional transforms: pass `--transforms-config` with a TOML file. Omit the flag to leave rows unchanged. Details: [How sync works](sync-pipeline.md).

## How It Works

The Snowflake source connects to your account over the SQL REST API v2, authenticating with key-pair (JWT) auth. It then reads the tables you name (or every table in the schema) and upserts each row into a SurrealDB table of the same name.

**Record IDs:** By default each table gets a sequential per-table index as its record ID. Pass `--id-columns` to build the SurrealDB record ID from one or more source columns instead. Two or more columns produce an Array record ID. See [How sync works — Record IDs](sync-pipeline.md#record-ids-and-composite-primary-keys).

## Prerequisites

Before using the Snowflake source, ensure you have:

1. **SurrealDB** running locally or accessible via network
2. **surreal-sync** available in your PATH
3. **A Snowflake account** reachable at `<account>.snowflakecomputing.com`
4. **Key-pair auth configured** — an RSA key-pair registered for a Snowflake user, with the private key available as an unencrypted PKCS#8 PEM file

### Setting up key-pair authentication

Generate an unencrypted PKCS#8 private key and its public key:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Register the public key on the Snowflake user (strip the PEM header/footer and newlines):

```sql
ALTER USER sync_user SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';
```

> Encrypted private keys are not currently supported — `--private-key-passphrase` is accepted but the key must be unencrypted PKCS#8.

## Command Structure

```bash
surreal-sync from snowflake \
  # Source (Snowflake) Settings
  --account <ACCOUNT> \
  --user <USER> \
  --private-key-path <PATH> \
  --warehouse <WAREHOUSE> \
  --database <DATABASE> \
  --schema <SCHEMA> \
  # Target (SurrealDB) Settings
  --to-namespace <TO_NAMESPACE> \
  --to-database <TO_DATABASE> \
  # Optional Behavior Settings
  [OPTIONS]
```

## Required Flags

| Flag | Env var | Description |
|------|---------|-------------|
| `--account <ACCOUNT>` | `SNOWFLAKE_ACCOUNT` | Account identifier as used in `<account>.snowflakecomputing.com` (e.g. `myorg-myaccount`) |
| `--user <USER>` | `SNOWFLAKE_USER` | Snowflake user whose key-pair is registered for JWT auth |
| `--private-key-path <PATH>` | `SNOWFLAKE_PRIVATE_KEY_PATH` | Path to the unencrypted PKCS#8 private key PEM file |
| `--warehouse <WAREHOUSE>` | `SNOWFLAKE_WAREHOUSE` | Virtual warehouse used to run the queries |
| `--database <DATABASE>` | `SNOWFLAKE_DATABASE` | Database to read from |
| `--to-namespace <NAMESPACE>` | — | Target SurrealDB namespace |
| `--to-database <DATABASE>` | — | Target SurrealDB database |

## Optional Flags

### Snowflake Settings

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--schema <SCHEMA>` | `SNOWFLAKE_SCHEMA` | `PUBLIC` | Schema within the database |
| `--role <ROLE>` | `SNOWFLAKE_ROLE` | (session default) | Role to assume for the session |
| `--private-key-passphrase <PASS>` | `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | (none) | Passphrase for an encrypted private key (currently unsupported) |
| `--tables <A,B,...>` | — | (all tables in the schema) | Comma-separated list of tables to ingest |
| `--id-columns <A,B,...>` | — | (sequential per-table index) | Columns forming the SurrealDB record ID; two or more → Array ID |
| `--transforms-config <PATH>` | — | (identity) | TOML file describing the transform pipeline (`[[transforms]]`) |

### SurrealDB Connection Settings

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--surreal-endpoint <URL>` | `SURREAL_ENDPOINT` | `http://localhost:8000` | SurrealDB endpoint URL |
| `--surreal-username <USER>` | `SURREAL_USERNAME` | `root` | SurrealDB username |
| `--surreal-password <PASS>` | `SURREAL_PASSWORD` | `root` | SurrealDB password |
| `--batch-size <COUNT>` | — | `1000` | Batch size for writing to SurrealDB |
| `--dry-run` | — | `false` | Don't actually write data (testing mode) |

Run `surreal-sync from snowflake --help` for full flag details.

## Usage Examples

### Example 1: Basic snapshot of an entire schema

```bash
surreal-sync from snowflake \
  --account myorg-myaccount \
  --user sync_user \
  --private-key-path ./rsa_key.p8 \
  --warehouse COMPUTE_WH \
  --database APP \
  --schema PUBLIC \
  --to-namespace production \
  --to-database app
```

### Example 2: Selected tables with a composite record ID

```bash
surreal-sync from snowflake \
  --account myorg-myaccount \
  --user sync_user \
  --private-key-path ./rsa_key.p8 \
  --warehouse COMPUTE_WH \
  --database APP \
  --schema SALES \
  --tables orders,order_items \
  --id-columns order_id,line_no \
  --to-namespace production \
  --to-database sales
```

### Example 3: Using environment variables

```bash
export SNOWFLAKE_ACCOUNT="myorg-myaccount"
export SNOWFLAKE_USER="sync_user"
export SNOWFLAKE_PRIVATE_KEY_PATH="./rsa_key.p8"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DATABASE="APP"
export SURREAL_ENDPOINT="ws://localhost:8000"
export SURREAL_USERNAME="admin"
export SURREAL_PASSWORD="secure-password"

surreal-sync from snowflake \
  --to-namespace production \
  --to-database app
```

## Embedding in Your Own Rust Binary

The Snowflake source is also available as a library entrypoint, so you can run the
same import from your own binary and append in-process transforms written in Rust —
for example to redact PII, rename fields, or promote foreign keys into SurrealDB
record links. Your binary accepts the same flags as `surreal-sync from snowflake`
(without the `from snowflake` prefix):

```rust
use surreal_sync::snowflake;
use surreal_sync::{FlattenId, InPlaceTransform};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    snowflake::run([
        Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>,
        // your own Box<dyn InPlaceTransform> stages …
    ])
    .await
}
```

See [`examples/snowflake_custom_transform.rs`](../examples/snowflake_custom_transform.rs) for a complete, runnable example.

## Continuous Integration

Snowflake is the one source that cannot be tested against a throwaway Docker container — there is no emulator, and the client talks to the real SQL REST API v2. Live coverage therefore needs a real account plus repository credentials.

Two workflows:

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| [`snowflake.yml`](../.github/workflows/snowflake.yml) | PRs and pushes touching Snowflake paths, nightly at 03:00 UTC, manual | Ingests `SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER` (1,500,000 rows) and asserts every source row landed. Runs against both SurrealDB v2 and v3. |
| [`snowflake-benchmark.yml`](../.github/workflows/snowflake-benchmark.yml) | Manual dispatch and `v*.*.*` tags | Ingests `SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER` (15,000,000 rows) and records throughput, peak memory, and row-count verification. |

Both are **read-only**. Neither creates anything in Snowflake, so the CI role needs no write privileges and there is nothing to clean up afterwards.

### Running the live test locally

Only four variables are required; database, schema, and table have defaults.

```bash
export SNOWFLAKE_ACCOUNT=myorg-myaccount
export SNOWFLAKE_USER=SURREAL_SYNC_CI
export SNOWFLAKE_PRIVATE_KEY_PATH=./rsa_key.p8
export SNOWFLAKE_WAREHOUSE=SURREAL_SYNC_CI_WH

# Defaults to TPCH_SF10 (1.5M rows). Override for a fast iteration loop:
export SNOWFLAKE_SCHEMA=TPCH_SF1   # 150,000 rows

cargo nextest run -E 'binary(snowflake)'
```

| Variable | Default | Purpose |
|----------|---------|---------|
| `SNOWFLAKE_DATABASE` | `SNOWFLAKE_SAMPLE_DATA` | Source database |
| `SNOWFLAKE_SCHEMA` | `TPCH_SF10` | Scale factor |
| `SNOWFLAKE_TEST_TABLE` | `CUSTOMER` | Table to ingest |
| `SNOWFLAKE_TEST_ID_COLUMNS` | `C_CUSTKEY` | Record-ID columns; set empty for a sequential index |

The test derives its expected row count from `SELECT COUNT(*)` on the source rather than hardcoding TPC-H totals, so it holds at any scale factor or table.

Without the four required variables the test prints a skip notice and passes, so it stays out of the way of contributors who have no Snowflake account. That is convenient locally but dangerous in CI: a misnamed secret would look like a passing test forever. Setting `SNOWFLAKE_REQUIRED=1` turns the skip into a failure that names the missing variables, and `snowflake.yml` always sets it.

Because a 1.5M-row sync runs far longer than a container-backed test, [`.config/nextest.toml`](../.config/nextest.toml) carries a `binary(snowflake)` override that widens the slow-timeout to a 60-minute ceiling and sets `retries = 0` — retrying would re-read millions of rows and re-bill warehouse credits without smoothing over any real flakiness.

### Repository configuration

Create an **Environment** named `snowflake` (Settings → Environments) and restrict its deployment branches to `main` plus the `v*.*.*` tag pattern, so a pull-request branch can never read the key. Both workflows declare `environment: snowflake`.

**Secrets** (Settings → Secrets and variables → Actions → Secrets, scoped to that environment):

| Secret | Value |
|--------|-------|
| `SNOWFLAKE_ACCOUNT` | Account identifier as used in `<account>.snowflakecomputing.com`, e.g. `myorg-myaccount` |
| `SNOWFLAKE_USER` | Login name of the CI service user, e.g. `SURREAL_SYNC_CI` |
| `SNOWFLAKE_PRIVATE_KEY` | The **entire** unencrypted PKCS#8 PEM, including the `-----BEGIN PRIVATE KEY-----` and `-----END PRIVATE KEY-----` lines |

**Variables** (same page, Variables tab). These are not sensitive, and keeping them readable in logs makes failures much easier to diagnose:

| Variable | Suggested value | Required |
|----------|-----------------|----------|
| `SNOWFLAKE_WAREHOUSE` | `SURREAL_SYNC_CI_WH` | Yes |
| `SNOWFLAKE_ROLE` | `SURREAL_SYNC_CI_ROLE` | Recommended |
| `SNOWFLAKE_DATABASE` | *(leave unset)* | No |
| `SNOWFLAKE_SCHEMA` | *(leave unset)* | No |
| `SNOWFLAKE_TEST_TABLE` | *(leave unset)* | No |

Leave the last three unset unless you want to point the test somewhere other than `SNOWFLAKE_SAMPLE_DATA` / `TPCH_SF10` / `CUSTOMER`. An unset repository variable expands to the empty string in the workflow, and the test treats empty as absent so its defaults still apply.

Secrets are never exposed to pull requests from forks. `snowflake.yml` detects that, emits a notice, and passes, so it is safe to mark as a required check — a contributor's PR gets its live Snowflake coverage when it merges to `main`.

### Snowflake account setup

Generate an unencrypted key-pair (encrypted keys are not supported — see [Limitations](#current-limitations)):

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Put the whole `rsa_key.p8` in the `SNOWFLAKE_PRIVATE_KEY` secret. For the SQL below, use `rsa_key.pub` with its header, footer, and newlines stripped.

Both workflows are read-only, so the CI role needs exactly two things: a warehouse to run queries on, and read access to the sample-data share. No scratch database, no `CREATE TABLE`.

```sql
USE ROLE ACCOUNTADMIN;

-- The shared sample data both workflows read. Skip if it already exists.
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_SAMPLE_DATA FROM SHARE SFC_SAMPLES.SAMPLE_DATA;

CREATE WAREHOUSE IF NOT EXISTS SURREAL_SYNC_CI_WH
  WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

CREATE ROLE IF NOT EXISTS SURREAL_SYNC_CI_ROLE;
GRANT USAGE, OPERATE ON WAREHOUSE SURREAL_SYNC_CI_WH TO ROLE SURREAL_SYNC_CI_ROLE;
-- IMPORTED PRIVILEGES on a share grants read only; objects cannot be created
-- inside SNOWFLAKE_SAMPLE_DATA, which is why the test reads instead of seeding.
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE SURREAL_SYNC_CI_ROLE;

-- TYPE = SERVICE means key-pair only, no password, and exemption from the MFA
-- policy Snowflake enforces on human users. Required for unattended CI.
CREATE USER IF NOT EXISTS SURREAL_SYNC_CI
  TYPE = SERVICE
  DEFAULT_ROLE = SURREAL_SYNC_CI_ROLE
  DEFAULT_WAREHOUSE = SURREAL_SYNC_CI_WH
  RSA_PUBLIC_KEY = 'MIIBIjANBgkq...';
GRANT ROLE SURREAL_SYNC_CI_ROLE TO USER SURREAL_SYNC_CI;

-- Cost guardrail: suspend the warehouse if CI ever runs away.
CREATE RESOURCE MONITOR IF NOT EXISTS SURREAL_SYNC_CI_RM
  WITH CREDIT_QUOTA = 50 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE SURREAL_SYNC_CI_WH SET RESOURCE_MONITOR = SURREAL_SYNC_CI_RM;
```

Because surreal-sync cannot read encrypted private keys, the secret necessarily holds an unencrypted one. The least-privilege `SERVICE` user and the resource monitor are the mitigations. Rotate with `ALTER USER SURREAL_SYNC_CI SET RSA_PUBLIC_KEY_2 = '...'` for a zero-downtime handover.

### Cost

Worth understanding before enabling these, because the integration test is no longer a smoke test:

- The warehouse only bills while a query is actually running, and auto-suspends after 60 seconds. Snowflake's side of the work — scanning `CUSTOMER` and paging out result partitions — is cheap; the wall-clock is dominated by surreal-sync writing into SurrealDB, which costs nothing.
- Even so, `snowflake.yml` runs **1.5M rows twice** (SurrealDB v2 and v3 legs) on every Snowflake-touching PR, every push to `main`, and nightly. That is real, recurring spend.
- `snowflake-benchmark.yml` at the `TPCH_SF100` default reads 15M rows per run, but only on manual dispatch and release tags.

If the recurring cost or the PR wall-clock turns out to be too much, the cheapest lever is setting the `SNOWFLAKE_SCHEMA` repository variable to `TPCH_SF1`, which drops the test to 150,000 rows without touching any code. Dropping the `pull_request` trigger from `snowflake.yml` (leaving pushes and the nightly run) is the next one. The resource monitor above is the backstop either way.

### The benchmark

The benchmark reads TPC-H from the `SNOWFLAKE_SAMPLE_DATA` share, which every account has and which needs no seeding. Scale is just a schema name, and each table scales linearly with the factor:

| Schema | CUSTOMER | ORDERS | LINEITEM |
|--------|---------:|-------:|---------:|
| `TPCH_SF1` | 150,000 | 1,500,000 | 6,001,215 |
| `TPCH_SF10` | 1,500,000 | 15,000,000 | 59,986,052 |
| `TPCH_SF100` | 15,000,000 | 150,000,000 | 600,037,902 |

**The default is `TPCH_SF100` / `CUSTOMER` — 15,000,000 rows.** Dispatch inputs cover scale, table list, batch size, SurrealDB image, runner size, and mode.

`mode: both` (the default) runs a `--dry-run` pass and then a write pass, which separates source throughput (REST calls, partition paging, type conversion) from what the SurrealDB sink adds — that split is what makes a regression actionable. It also doubles the row count processed, so `mode: write` roughly halves the runtime when you only need the end-to-end number. Verification works in either mode.

Since the default now runs on every `v*.*.*` tag, expect a release to spend a meaningful stretch of wall-clock here and to draw more warehouse credits than a smoke test would. `TPCH_SF1` / `CUSTOMER` (150,000 rows) is the cheap configuration for validating the workflow itself.

The driver is [`scripts/snowflake-benchmark.py`](../scripts/snowflake-benchmark.py). It emits `metrics.json` in the same schema the load-test harness uses, so [`loadtest/scripts/compare_metrics.py`](../loadtest/scripts/compare_metrics.py) renders the summary and baseline delta without modification. To run it locally against your own SurrealDB:

```bash
cargo build --release --bin surreal-sync
docker run -d --name sdb -p 8000:8000 surrealdb/surrealdb:v3.1.5 \
  start --user root --pass root rocksdb:/data

# TPCH_SF1 rather than the TPCH_SF100 default: 150,000 rows instead of 15,000,000.
SURREAL_SYNC_BIN=./target/release/surreal-sync \
BENCH_SCALE=TPCH_SF1 BENCH_TABLES=CUSTOMER BENCH_MODE=both \
python3 scripts/snowflake-benchmark.py
```

Verification compares the row count the source reported for each table against the count actually in SurrealDB afterwards, rather than hardcoding expected TPC-H totals. A mismatch fails the run; a throughput regression is reported but does not fail, because a single sample on a shared runner is too noisy to gate on.

Two numbers are worth watching over time:

- **`resources.peak_memory_mb`** should stay roughly flat as the row count grows. The source is documented to hold about one Snowflake result partition in memory at a time; if this scales with row count, something in the streaming path is buffering.
- **`snowflake.dry_run_rows_per_sec` vs `write_rows_per_sec`** localises a slowdown to the source or the sink.

Beyond the timing, `TPCH_SF1` is the only thing in CI that exercises multi-partition result paging, JWT re-minting across a long sync, HTTP 202 statement polling, and `NUMBER(12,2)`/`DATE` conversion at volume.

## Current Limitations

- **Full snapshot only.** There is no incremental or CDC mode for Snowflake; each run reads the selected tables in full.
- **Key-pair (JWT) auth only.** Username/password and OAuth are not supported.
- **Unencrypted private keys only.** `--private-key-passphrase` is accepted but encrypted keys are not currently supported.

If your use case requires additional capabilities, please file a feature request at: https://github.com/surrealdb/surreal-sync/issues
