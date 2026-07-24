//! Custom in-process MySQL binlog transforms (embedder example).
//!
//! Same flags as `surreal-sync from mysql-binlog sync|snapshot`, but argv is
//! source-shaped (no `from mysql-binlog` prefix):
//!
//! ```bash
//! cargo run --example mysql_binlog_custom_transform -- sync \
//!   --connection-string 'mysql://user:pass@127.0.0.1:3306/app' \
//!   --database app \
//!   --to-namespace prod --to-database app \
//!   --checkpoint-dir ./checkpoints
//! ```

use anyhow::Result;
use std::collections::HashMap;
use surreal_sync::mysql_binlog;
use surreal_sync::{FlattenId, InPlaceTransform, Value};

/// Drop columns that must not leave the source VPC (CDC often mirrors more than the target needs).
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

/// Rename / reshape fields toward the SurrealDB document shape.
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

/// Promote a scalar FK into a SurrealDB record link (`organizations:<id>`).
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
    // Parses argv (`sync|snapshot` + flags), loads optional --transforms-config,
    // then appends these in-place transforms (after any TOML transforms), and
    // runs the same orchestration as stock `surreal-sync from mysql-binlog`.
    //
    // `Box<dyn InPlaceTransform>` so different concrete types can share one list
    // (Rust arrays are homogeneous).
    mysql_binlog::run([
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
