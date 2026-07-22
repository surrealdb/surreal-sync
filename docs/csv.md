# CSV Import

The CSV source in surreal-sync imports CSV files in the local filesystem or S3 buckets into a SurrealDB table with automatic type detection and optional [record ID](https://surrealdb.com/docs/surrealql/datamodel/ids) generation.

Optional transforms: pass `--transforms-config` with a TOML file. Omit the flag to leave rows unchanged. Details: [How sync works](sync-pipeline.md).

## Usage

### Import from local filesystem

```bash
surreal-sync csv \
  --files users.csv --files customers.csv \
  --table people \
  --to-namespace test \
  --to-database test
```

### Import from S3

```bash
surreal-sync csv \
  --s3-uris s3://mybucket/users.csv --s3-uris s3://mybucket/customers.csv \
  --table people \
  --to-namespace test \
  --to-database test
```

### Custom Options

```bash
surreal-sync csv \
  --files data.csv \
  --table products \
  --to-namespace store \
  --to-database main \
  --delimiter ";" \           # Use semicolon delimiter
  --id-field product_id \     # Single-column record ID
  --batch-size 500 \          # Process 500 records at a time
  --dry-run                   # Test without writing data
```

For a composite ID, prefer `--id-columns` (takes precedence over `--id-field`):

```bash
surreal-sync csv \
  --files orders.csv \
  --table orders \
  --to-namespace store \
  --to-database main \
  --id-columns order_id,line_id
```

Multi-column IDs become Surreal array keys (`orders:[1, 2]`). To restore colon-joined Text, add `type = "flatten_id"` in `--transforms-config` — see [How sync works — Record IDs](sync-pipeline.md#record-ids-and-composite-primary-keys).

## Command Options

| Option | Description | Default |
|--------|-------------|---------|
| `--files` | CSV file paths (required, multiple allowed) | - |
| `--table` | Target SurrealDB table name | - |
| `--to-namespace` | Target namespace | - |
| `--to-database` | Target database | - |
| `--has-headers` | Whether CSV has headers | `true` |
| `--delimiter` | CSV delimiter character | `,` |
| `--id-field` | Single field to use as record ID | auto-generated |
| `--id-columns` | Columns forming the record ID (comma-separated); two or more → Array ID (overrides `--id-field`) | - |
| `--batch-size` | Records per poll into the long-lived apply window (file reads continue under spare `max_in_flight`) | `1000` |
| `--dry-run` | Test without writing | `false` |

## Data Type Handling

The importer automatically detects and converts data types:

| CSV Value | SurrealDB Type |
|-----------|----------------|
| `123` | Integer |
| `45.67` | Float |
| `true`/`false` | Boolean |
| `text` | String |
| (empty) | NULL |

## Example CSV

```csv
id,name,age,active
1,Alice,30,true
2,Bob,25,false
3,Charlie,35,true
```

This will create SurrealDB records with properly typed fields:
- `id`: Integer
- `name`: String
- `age`: Integer
- `active`: Boolean
