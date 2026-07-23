//! Custom in-process Snowflake transforms (embedder example).
//!
//! Same flags as `surreal-sync from snowflake`, but without the `from snowflake`
//! prefix:
//!
//! ```bash
//! cargo run --example snowflake_custom_transform -- \
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
use surreal_sync::snowflake;
use surreal_sync::{FlattenId, InPlaceTransform, UniversalChange, UniversalRow, UniversalValue};

/// Drop columns that must not leave the source VPC (imports often mirror more
/// than the target needs).
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
        // Snowflake imports rows only; keep the change path for trait completeness.
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
    // Parses argv (same flags as `surreal-sync from snowflake`), loads optional
    // --transforms-config, then appends these in-place transforms (after any
    // TOML transforms), and runs the same import as the stock binary.
    //
    // `Box<dyn InPlaceTransform>` so different concrete types can share one list
    // (Rust arrays are homogeneous).
    snowflake::run([
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
