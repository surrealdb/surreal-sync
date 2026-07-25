# surreal-sync-mysql

Sync MySQL or MariaDB into SurrealDB, including ongoing changes from the binlog.

## Depend

```toml
surreal-sync-mysql = { version = "0.6", features = ["from_binlog"] }
surreal-sync-surreal = "0.6" # SurrealDB 3 by default; use features = ["v2"] for v2
```

## Embed

Define optional transforms, then call `run` with a SurrealDB sink. Pass the same flags as `surreal-sync from mysql-binlog` (for example `--checkpoint-dir` or `--checkpoints-surreal-table` so sync can resume).

```rust
use anyhow::Result;
use std::collections::HashMap;
use surreal_sync_mysql::from_binlog::{run, FlattenId, InPlaceTransform, Value};
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

Full working example: [`examples/from-mysql-binlog`](https://github.com/surrealdb/surreal-sync/tree/main/examples/from-mysql-binlog).
