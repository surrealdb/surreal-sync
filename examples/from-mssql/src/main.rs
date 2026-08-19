//! Custom in-process SQL Server transforms (embedder example).
//!
//! Same flags as `surreal-sync from mssql sync`, but argv is source-shaped
//! (no `from mssql` prefix). Pick a sink crate, define transforms, call `run`
//! — it connects SurrealDB and chooses checkpoint storage from
//! `--checkpoint-dir` or `--checkpoints-surreal-table`.
//!
//! ```bash
//! cargo run -p surreal-sync-example-from-mssql -- sync \
//!   --connection-string 'Server=tcp:localhost,1433;User=sa;Password=...;Database=App;TrustServerCertificate=true;Encrypt=true' \
//!   --tables dbo.Article,sales.Order \
//!   --to-namespace prod --to-database app \
//!   --checkpoints-surreal-table sync_checkpoints
//! ```

use anyhow::Result;
use std::collections::HashMap;
use surreal_sync_mssql::{run, FlattenId, InPlaceTransform, Value};
use surreal_sync_surreal::Surreal3Sink;

/// Drop columns that must not leave the source VPC.
struct RedactPii;

impl RedactPii {
    const DROP: &'static [&'static str] = &["password_hash", "ssn", "credit_card"];
}

impl InPlaceTransform for RedactPii {
    fn transform(
        &self,
        _table: &str,
        _id: &mut Value,
        fields: Option<&mut HashMap<String, Value>>,
    ) -> Result<()> {
        if let Some(fields) = fields {
            for key in Self::DROP {
                fields.remove(*key);
            }
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
