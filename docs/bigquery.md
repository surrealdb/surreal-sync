# BigQuery Source Usage Guide

The BigQuery source in surreal-sync imports tables from a Google BigQuery dataset into SurrealDB. This is a **full-snapshot source**: it runs a one-shot read of the selected tables via the BigQuery REST API v2 and writes the rows to SurrealDB. There is no incremental/CDC mode for BigQuery.

Optional transforms: pass `--transforms-config` with a TOML file. Omit the flag to leave rows unchanged. Details: [How sync works](sync-pipeline.md).

## How It Works

The BigQuery source submits a `SELECT *` per table through the [REST API v2] `jobs.query` endpoint, authenticating with a service-account key (RS256 JWT exchanged for an OAuth2 access token). Results are read back page by page via `jobs.getQueryResults`, so only one result page is held in memory at a time regardless of table size. Each row is upserted into a SurrealDB table of the same name.

**Record IDs:** By default each table gets a sequential per-table index as its record ID. Pass `--id-columns` to build the SurrealDB record ID from one or more source columns instead. Two or more columns produce an Array record ID. See [How sync works — Record IDs](sync-pipeline.md#record-ids-and-composite-primary-keys).

> **A column named `id`:** SurrealDB reserves `id` for the record ID, so a table with an `id` column will be rejected unless you pass `--id-columns id` (which promotes it to the record ID instead of leaving it as a field) or rename it with a transform.

**Case sensitivity:** BigQuery identifiers are case-sensitive, and table names are carried into SurrealDB verbatim — a BigQuery table `Orders` becomes a SurrealDB table `Orders`, not `orders`.

[REST API v2]: https://cloud.google.com/bigquery/docs/reference/rest

## Prerequisites

Before using the BigQuery source, ensure you have:

1. **SurrealDB** running locally or accessible via network
2. **surreal-sync** available in your PATH
3. **A Google Cloud project** with the BigQuery API enabled
4. **A service account** with a JSON key, granted read access to the dataset

### Setting up a service account

The source only reads, so two predefined roles are enough — `roles/bigquery.dataViewer` to read the tables and `roles/bigquery.jobUser` to run the query jobs:

```bash
gcloud iam service-accounts create surreal-sync \
  --display-name "surreal-sync BigQuery reader"

gcloud projects add-iam-policy-binding MY_PROJECT \
  --member "serviceAccount:surreal-sync@MY_PROJECT.iam.gserviceaccount.com" \
  --role roles/bigquery.dataViewer

gcloud projects add-iam-policy-binding MY_PROJECT \
  --member "serviceAccount:surreal-sync@MY_PROJECT.iam.gserviceaccount.com" \
  --role roles/bigquery.jobUser

gcloud iam service-accounts keys create ./bigquery-key.json \
  --iam-account surreal-sync@MY_PROJECT.iam.gserviceaccount.com
```

Point `--credentials-path` (or `GOOGLE_APPLICATION_CREDENTIALS`) at the resulting JSON file.

> Only service-account keys are supported. Authorized-user credentials (`gcloud auth application-default login`) and external-account/workload-identity files are rejected with an explanatory error.

## Command Structure

```bash
surreal-sync from bigquery \
  # Source (BigQuery) Settings
  --project-id <PROJECT_ID> \
  --dataset <DATASET> \
  --credentials-path <PATH> \
  # Target (SurrealDB) Settings
  --to-namespace <TO_NAMESPACE> \
  --to-database <TO_DATABASE> \
  # Optional Behavior Settings
  [OPTIONS]
```

## Required Flags

| Flag | Env var | Description |
|------|---------|-------------|
| `--project-id <PROJECT_ID>` | `BIGQUERY_PROJECT_ID` | Google Cloud project that owns the dataset |
| `--dataset <DATASET>` | `BIGQUERY_DATASET` | BigQuery dataset to read from |
| `--credentials-path <PATH>` | `GOOGLE_APPLICATION_CREDENTIALS` | Service-account JSON key file. Required unless `--api-endpoint` points at an emulator |
| `--to-namespace <NAMESPACE>` | — | Target SurrealDB namespace |
| `--to-database <DATABASE>` | — | Target SurrealDB database |

## Optional Flags

### BigQuery Settings

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--billing-project-id <ID>` | `BIGQUERY_BILLING_PROJECT_ID` | `--project-id` | Project billed for the query jobs |
| `--location <LOCATION>` | `BIGQUERY_LOCATION` | (inferred) | Dataset location, e.g. `US`, `EU`, `europe-west2` |
| `--api-endpoint <URL>` | `BIGQUERY_API_ENDPOINT` | `https://bigquery.googleapis.com` | API root; override to target a local emulator |
| `--tables <A,B,...>` | — | (all tables in the dataset) | Comma-separated list of tables to ingest |
| `--id-columns <A,B,...>` | — | (sequential per-table index) | Columns forming the SurrealDB record ID; two or more → Array ID |
| `--page-size <COUNT>` | — | `10000` | Rows fetched per BigQuery result page |
| `--transforms-config <PATH>` | — | (identity) | TOML file describing the transform pipeline (`[[transforms]]`) |

### SurrealDB Connection Settings

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--surreal-endpoint <URL>` | `SURREAL_ENDPOINT` | `http://localhost:8000` | SurrealDB endpoint URL |
| `--surreal-username <USER>` | `SURREAL_USERNAME` | `root` | SurrealDB username |
| `--surreal-password <PASS>` | `SURREAL_PASSWORD` | `root` | SurrealDB password |
| `--batch-size <COUNT>` | — | `1000` | Batch size for writing to SurrealDB |
| `--dry-run` | — | `false` | Don't actually write data (testing mode) |

Run `surreal-sync from bigquery --help` for full flag details.

## Data Type Mapping

Every scalar arrives from the REST API as a JSON string; the source converts it using the column's declared type. Both the legacy REST spellings (`INTEGER`, `FLOAT`, `BOOLEAN`, `RECORD`) and the GoogleSQL names (`INT64`, `FLOAT64`, `BOOL`, `STRUCT`) are accepted.

| BigQuery type | SurrealDB mapping | Notes |
|---------------|-------------------|-------|
| `STRING` | `string` | Direct |
| `BYTES` | `bytes` | Base64 on the wire, decoded on ingest |
| `INTEGER` / `INT64` | `int` | 64-bit |
| `FLOAT` / `FLOAT64` | `float` | `NaN`, `Infinity`, `-Infinity` preserved |
| `NUMERIC` | `decimal` | Exact; precision/scale from the schema, else 38/9 |
| `BIGNUMERIC` | `decimal` | Exact; precision/scale from the schema, else 76/38 |
| `BOOLEAN` / `BOOL` | `bool` | Direct |
| `TIMESTAMP` | `datetime` (UTC instant) | Requested as int64 microseconds; float-seconds encodings also accepted |
| `DATE` | `datetime` at midnight UTC | |
| `TIME` | `datetime` on 1970-01-01 | |
| `DATETIME` | `datetime` (civil time, no offset) | |
| `JSON` | `object` / JSON value | Parsed from the serialized document |
| `RECORD` / `STRUCT` | `object` | Nested, keyed by the declared field names |
| any type with `mode: REPEATED` | `array` | Recursively converted; a NULL array becomes `[]`, since BigQuery has no null arrays |
| `GEOGRAPHY` | `string` | Preserved as WKT — surreal-sync does not parse WKT |
| `INTERVAL`, `RANGE`, anything unrecognised | `string` | Preserved verbatim rather than failing the sync |

## Usage Examples

### Example 1: Basic snapshot of an entire dataset

```bash
surreal-sync from bigquery \
  --project-id my-project \
  --dataset analytics \
  --credentials-path ./bigquery-key.json \
  --to-namespace production \
  --to-database app
```

### Example 2: Selected tables with a composite record ID

```bash
surreal-sync from bigquery \
  --project-id my-project \
  --dataset sales \
  --credentials-path ./bigquery-key.json \
  --tables orders,order_items \
  --id-columns order_id,line_no \
  --to-namespace production \
  --to-database sales
```

### Example 3: Using environment variables

```bash
export BIGQUERY_PROJECT_ID="my-project"
export BIGQUERY_DATASET="analytics"
export GOOGLE_APPLICATION_CREDENTIALS="./bigquery-key.json"
export SURREAL_ENDPOINT="ws://localhost:8000"
export SURREAL_USERNAME="admin"
export SURREAL_PASSWORD="secure-password"

surreal-sync from bigquery \
  --to-namespace production \
  --to-database app
```

### Example 4: Against a local emulator

[goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) speaks the same REST API and disables authentication, which makes it useful for trying the source out without touching a real project. Pointing `--api-endpoint` anywhere other than `googleapis.com` allows running without credentials:

```bash
docker run --rm -d -p 9050:9050 ghcr.io/goccy/bigquery-emulator:0.8.1 \
  --project=demo --dataset=app
```

```bash
surreal-sync from bigquery \
  --project-id demo \
  --dataset app \
  --api-endpoint http://127.0.0.1:9050 \
  --to-namespace test \
  --to-database app
```

Against the real API, omitting credentials is an error rather than an anonymous request.

## Embedding in Your Own Rust Binary

The BigQuery source is also available as a library entrypoint, so you can run the
same import from your own binary and append in-process transforms written in Rust —
for example to redact PII, rename fields, or promote foreign keys into SurrealDB
record links. Your binary accepts the same flags as `surreal-sync from bigquery`
(without the `from bigquery` prefix):

```rust
use surreal_sync_bigquery::{run, FlattenId, InPlaceTransform};
use surreal_sync_surreal::Surreal3Sink;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run::<Surreal3Sink>([
        Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>,
        // your own Box<dyn InPlaceTransform> stages …
    ])
    .await
}
```

See [`examples/from-bigquery`](../examples/from-bigquery) for a complete, runnable example.

## Testing

The integration tests in `tests/bigquery/` run against the BigQuery emulator in a
throwaway Docker container, so they need no credentials and run on every pull request.

The emulator is not BigQuery. It does not authenticate at all, it cannot produce
`GEOGRAPHY` values, and it has no `RANGE`/`INTERVAL` support — so the wire encodings
of the real API, including `TIMESTAMP` in scientific notation, are pinned by the unit
tests in `crates/bigquery/src/types.rs` instead. The service-account path is covered
by `tests/bigquery/bigquery_real_account_lib.rs`, which skips unless
`BIGQUERY_PROJECT_ID`, `BIGQUERY_DATASET`, `BIGQUERY_TABLE`, and
`GOOGLE_APPLICATION_CREDENTIALS` are set.

## Current Limitations

- **Full snapshot only.** There is no incremental or CDC mode for BigQuery; each run reads the selected tables in full.
- **Service-account (JWT) auth only.** Authorized-user credentials, workload identity, and the metadata server are not supported.
- **No Storage Read API.** Reads go through the REST API, which is simpler and cheaper to start with but slower than the Storage Read API on very large tables.
- **Views and external tables are skipped.** Table discovery lists base tables only; name a view explicitly with `--tables` if you want it.
- **`GEOGRAPHY` is not parsed.** Values are preserved as WKT strings rather than converted to SurrealDB geometry.

If your use case requires additional capabilities, please file a feature request at: https://github.com/surrealdb/surreal-sync/issues
