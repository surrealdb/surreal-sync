# CSV Import

The CSV source in surreal-sync imports CSV files into a SurrealDB table with automatic type detection and optional record ID generation.

## Usage

### Basic Import

```bash
surreal-sync csv \
  --files data.csv \
  --table users \
  --to-namespace myapp \
  --to-database production
```

### Multiple Files

```bash
surreal-sync csv \
  --files users.csv --files customers.csv \
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
  --id-field product_id \     # Use product_id column as record ID
  --batch-size 500 \          # Process 500 records at a time
  --dry-run                   # Test without writing data
```

## Command Options

| Option | Description | Default |
|--------|-------------|---------|
| `--files` | CSV file paths (required, multiple allowed) | - |
| `--table` | Target SurrealDB table name | - |
| `--to-namespace` | Target namespace | - |
| `--to-database` | Target database | - |
| `--has-headers` | Whether CSV has headers | `true` |
| `--delimiter` | CSV delimiter character | `,` |
| `--id-field` | Field to use as record ID | auto-generated |
| `--batch-size` | Records per batch | `1000` |
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
