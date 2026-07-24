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

## Current Limitations

- **Full snapshot only.** There is no incremental or CDC mode for Snowflake; each run reads the selected tables in full.
- **Key-pair (JWT) auth only.** Username/password and OAuth are not supported.
- **Unencrypted private keys only.** `--private-key-passphrase` is accepted but encrypted keys are not currently supported.

If your use case requires additional capabilities, please file a feature request at: https://github.com/surrealdb/surreal-sync/issues
