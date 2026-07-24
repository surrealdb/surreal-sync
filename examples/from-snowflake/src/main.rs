//! Custom in-process Snowflake transforms (embedder example).
//!
//! Same flags as `surreal-sync from snowflake`, but without the `from snowflake`
//! prefix. Uses SurrealDB 3 via `run::<Surreal3Sink>`:
//!
//! ```bash
//! cargo run -p surreal-sync-example-from-snowflake -- \
//!   --account myorg-myaccount \
//!   --user sync_user \
//!   --private-key-path ./rsa_key.p8 \
//!   --warehouse COMPUTE_WH \
//!   --database APP \
//!   --schema PUBLIC \
//!   --to-namespace prod --to-database app
//! ```

use anyhow::Result;
use std::collections::HashMap;
use surreal_sync_snowflake::{run, FlattenId, InPlaceTransform, Value};
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

struct RenameFields;

impl InPlaceTransform for RenameFields {
    fn transform(
        &self,
        _table: &str,
        _id: &mut Value,
        fields: Option<&mut HashMap<String, Value>>,
    ) -> Result<()> {
        if let Some(fields) = fields {
            if let Some(name) = fields.remove("name") {
                fields.insert("full_name".into(), name);
            }
        }
        Ok(())
    }
}

struct FkToRecordLink {
    field: &'static str,
    table: &'static str,
}

impl InPlaceTransform for FkToRecordLink {
    fn transform(
        &self,
        _table: &str,
        _id: &mut Value,
        fields: Option<&mut HashMap<String, Value>>,
    ) -> Result<()> {
        let Some(fields) = fields else {
            return Ok(());
        };
        let Some(id) = fields.remove(self.field) else {
            return Ok(());
        };
        if matches!(id, Value::Null) {
            fields.insert(self.field.into(), Value::Null);
            return Ok(());
        }
        fields.insert(
            self.field.into(),
            Value::Thing {
                table: self.table.into(),
                id: Box::new(id),
            },
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run::<Surreal3Sink>([
        Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>,
        Box::new(RedactPii),
        Box::new(RenameFields),
        Box::new(FkToRecordLink {
            field: "org_id",
            table: "organizations",
        }),
    ])
    .await
}
