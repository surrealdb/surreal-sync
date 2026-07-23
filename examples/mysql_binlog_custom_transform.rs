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
use surreal_sync::{FlattenId, InPlaceTransform, UniversalChange, UniversalRow, UniversalValue};

/// Drop columns that must not leave the source VPC (CDC often mirrors more than the target needs).
struct RedactPii;

impl RedactPii {
    const DROP: &'static [&'static str] = &["password_hash", "ssn", "credit_card"];

    fn scrub(fields: &mut HashMap<String, UniversalValue>) {
        for key in Self::DROP {
            fields.remove(*key);
        }
    }
}

impl InPlaceTransform for RedactPii {
    fn transform_row(&self, row: &mut UniversalRow) -> Result<()> {
        Self::scrub(&mut row.fields);
        Ok(())
    }

    fn transform_change(&self, change: &mut UniversalChange) -> Result<()> {
        // Create/Update carry `data`; Delete does not — leave id/table alone.
        if let Some(data) = change.data.as_mut() {
            Self::scrub(data);
        }
        Ok(())
    }
}

/// Rename / reshape fields toward the SurrealDB document shape.
struct RenameFields;

impl RenameFields {
    fn rename(fields: &mut HashMap<String, UniversalValue>) {
        if let Some(name) = fields.remove("name") {
            fields.insert("full_name".into(), name);
        }
    }
}

impl InPlaceTransform for RenameFields {
    fn transform_row(&self, row: &mut UniversalRow) -> Result<()> {
        Self::rename(&mut row.fields);
        Ok(())
    }

    fn transform_change(&self, change: &mut UniversalChange) -> Result<()> {
        if let Some(data) = change.data.as_mut() {
            Self::rename(data);
        }
        Ok(())
    }
}

/// Promote a scalar FK into a SurrealDB record link (`organizations:<id>`).
struct FkToRecordLink {
    field: &'static str,
    table: &'static str,
}

impl FkToRecordLink {
    fn linkify(&self, fields: &mut HashMap<String, UniversalValue>) {
        let Some(id) = fields.remove(self.field) else {
            return;
        };
        if matches!(id, UniversalValue::Null) {
            fields.insert(self.field.into(), UniversalValue::Null);
            return;
        }
        fields.insert(
            self.field.into(),
            UniversalValue::Thing {
                table: self.table.into(),
                id: Box::new(id),
            },
        );
    }
}

impl InPlaceTransform for FkToRecordLink {
    fn transform_row(&self, row: &mut UniversalRow) -> Result<()> {
        self.linkify(&mut row.fields);
        Ok(())
    }

    fn transform_change(&self, change: &mut UniversalChange) -> Result<()> {
        if let Some(data) = change.data.as_mut() {
            self.linkify(data);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parses argv (`sync|snapshot` + flags), loads optional --transforms-config,
    // then appends these in-place stages (after any TOML stages), and runs the
    // same orchestration as stock `surreal-sync from mysql-binlog`.
    //
    // `stage(...)` boxes each transform so heterogeneous types can share one list
    // (Rust arrays are homogeneous).
    mysql_binlog::run([
        mysql_binlog::stage(FlattenId::default()),
        mysql_binlog::stage(RedactPii),
        mysql_binlog::stage(RenameFields),
        mysql_binlog::stage(FkToRecordLink {
            field: "org_id",
            table: "organizations",
        }),
    ])
    .await
}
