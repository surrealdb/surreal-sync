//! Built-in in-place transform: flatten Array record IDs to Text.

use anyhow::Result;
use std::collections::HashMap;
use surreal_sync_core::InPlaceTransform;
use surreal_sync_core::{flatten_composite_id, Relation, RelationChange, Value};

/// Default separator matching relation FK flattening and historical Snowflake IDs.
pub const DEFAULT_FLATTEN_ID_SEPARATOR: &str = ":";

/// Rewrite `Value::Array` record IDs to Text joined with [`Self::separator`].
///
/// Scalar IDs are left unchanged. Relation **edge** IDs are also flattened when
/// they are Arrays (endpoint Thing refs are flattened the same way so links stay
/// consistent if a source emitted Array endpoint IDs).
#[derive(Debug, Clone)]
pub struct FlattenId {
    /// Joiner between composite key parts (default `:`).
    pub separator: String,
}

impl Default for FlattenId {
    fn default() -> Self {
        Self {
            separator: DEFAULT_FLATTEN_ID_SEPARATOR.to_string(),
        }
    }
}

impl FlattenId {
    /// Create with an explicit separator.
    pub fn new(separator: impl Into<String>) -> Self {
        Self {
            separator: separator.into(),
        }
    }
}

impl InPlaceTransform for FlattenId {
    fn transform(
        &self,
        _table: &str,
        id: &mut Value,
        _fields: Option<&mut HashMap<String, Value>>,
    ) -> Result<()> {
        *id = flatten_composite_id(std::mem::replace(id, Value::Null), &self.separator);
        Ok(())
    }

    fn transform_relation(&self, relation: &mut Relation) -> Result<()> {
        relation.id = flatten_composite_id(
            std::mem::replace(&mut relation.id, Value::Null),
            &self.separator,
        );
        relation.input.id = flatten_composite_id(
            std::mem::replace(&mut relation.input.id, Value::Null),
            &self.separator,
        );
        relation.output.id = flatten_composite_id(
            std::mem::replace(&mut relation.output.id, Value::Null),
            &self.separator,
        );
        Ok(())
    }

    fn transform_relation_change(&self, change: &mut RelationChange) -> Result<()> {
        self.transform_relation(&mut change.relation)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use surreal_sync_core::{Row, Type, Value};

    #[test]
    fn flattens_array_row_id() {
        let t = FlattenId::default();
        let mut row = Row::new(
            "ledger",
            0,
            Value::Array {
                elements: vec![Value::Int32(1), Value::Int32(2)],
                element_type: Box::new(Type::Int32),
            },
            Default::default(),
        );
        t.transform_row(&mut row).unwrap();
        assert_eq!(row.id, Value::Text("1:2".into()));
    }

    #[test]
    fn leaves_scalar_unchanged() {
        let t = FlattenId::new("_");
        let mut row = Row::new("t", 0, Value::Int64(9), Default::default());
        t.transform_row(&mut row).unwrap();
        assert_eq!(row.id, Value::Int64(9));
    }

    #[test]
    fn pipeline_from_flatten_id_toml_applies_to_rows() {
        use crate::pipeline::{parse_transforms_toml, Pipeline};

        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "flatten_id"
separator = ":"
"#,
        )
        .unwrap();
        let pipeline = Pipeline::from_config(&cfg).unwrap();
        let rows = pipeline
            .apply_rows(vec![Row::new(
                "ledger",
                0,
                Value::Array {
                    elements: vec![Value::Text("a".into()), Value::Text("b".into())],
                    element_type: Box::new(Type::Text),
                },
                Default::default(),
            )])
            .unwrap();
        assert_eq!(rows[0].id, Value::Text("a:b".into()));
    }
}
