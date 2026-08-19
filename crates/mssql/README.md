# surreal-sync-mssql

Sync SQL Server tables into SurrealDB using CDC and watermark snapshots.

## Depend

```toml
surreal-sync-mssql = { version = "0.6", features = ["from_mssql"] }
surreal-sync-surreal = "0.6" # SurrealDB 3 by default; use features = ["v2"] for v2
```

## Embed

Define optional transforms, then call `run` with a SurrealDB sink. Pass the same flags as `surreal-sync from mssql sync`.

```rust
use anyhow::Result;
use std::collections::HashMap;
use surreal_sync_mssql::{run, FlattenId, InPlaceTransform, Value};
use surreal_sync_surreal::Surreal3Sink;

struct RedactPii;

impl InPlaceTransform for RedactPii {
    fn transform(
        &self,
        _table: &str,
        _id: &mut Value,
        fields: Option<&mut HashMap<String, Value>>,
    ) -> Result<()> {
        if let Some(fields) = fields {
            fields.remove("password_hash");
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run::<Surreal3Sink>([
        Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>,
        Box::new(RedactPii),
    ])
    .await
}
```

Example argv (same flags as the stock CLI):

```text
sync \
  --connection-string 'Server=tcp:localhost,1433;User=sa;Password=...;Database=App;TrustServerCertificate=true;Encrypt=true' \
  --tables dbo.Article,sales.Order \
  --to-namespace prod --to-database app \
  --checkpoints-surreal-table sync_checkpoints
```

Default `--strategy interleaved-snapshot` copies tables while SQL Server CDC runs, then stays in the replication tail. Pass `--strategy sequential-snapshot` for a SNAPSHOT-isolation dump (writers are not locked). Pass `--schemafull` to emit `DEFINE TABLE` / `FIELD` / `INDEX`; the default is schemaless.

SQL Server must have CDC enabled (`EXEC sys.sp_cdc_enable_db;`) and SQL Server Agent running (Linux containers: `MSSQL_AGENT_ENABLED=true`). Windows Integrated Auth (`IntegratedSecurity=true`) is Windows-only.
