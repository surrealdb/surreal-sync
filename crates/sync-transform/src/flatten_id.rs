//! Built-in in-place transform: flatten Array record IDs to Text.

use crate::inplace::InPlaceTransform;
use anyhow::Result;
use sync_core::{
    flatten_composite_id, UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow,
};

/// Default separator matching relation FK flattening and historical Snowflake IDs.
pub const DEFAULT_FLATTEN_ID_SEPARATOR: &str = ":";

/// Rewrite `UniversalValue::Array` record IDs to Text joined with [`Self::separator`].
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
    fn transform_row(&self, row: &mut UniversalRow) -> Result<()> {
        row.id = flatten_composite_id(
            std::mem::replace(&mut row.id, sync_core::UniversalValue::Null),
            &self.separator,
        );
        Ok(())
    }

    fn transform_change(&self, change: &mut UniversalChange) -> Result<()> {
        change.id = flatten_composite_id(
            std::mem::replace(&mut change.id, sync_core::UniversalValue::Null),
            &self.separator,
        );
        Ok(())
    }

    fn transform_relation(&self, relation: &mut UniversalRelation) -> Result<()> {
        relation.id = flatten_composite_id(
            std::mem::replace(&mut relation.id, sync_core::UniversalValue::Null),
            &self.separator,
        );
        relation.input.id = flatten_composite_id(
            std::mem::replace(&mut relation.input.id, sync_core::UniversalValue::Null),
            &self.separator,
        );
        relation.output.id = flatten_composite_id(
            std::mem::replace(&mut relation.output.id, sync_core::UniversalValue::Null),
            &self.separator,
        );
        Ok(())
    }

    fn transform_relation_change(&self, change: &mut UniversalRelationChange) -> Result<()> {
        self.transform_relation(&mut change.relation)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::{UniversalType, UniversalValue};

    #[test]
    fn flattens_array_row_id() {
        let t = FlattenId::default();
        let mut row = UniversalRow::new(
            "ledger",
            0,
            UniversalValue::Array {
                elements: vec![UniversalValue::Int32(1), UniversalValue::Int32(2)],
                element_type: Box::new(UniversalType::Int32),
            },
            Default::default(),
        );
        t.transform_row(&mut row).unwrap();
        assert_eq!(row.id, UniversalValue::Text("1:2".into()));
    }

    #[test]
    fn leaves_scalar_unchanged() {
        let t = FlattenId::new("_");
        let mut row = UniversalRow::new("t", 0, UniversalValue::Int64(9), Default::default());
        t.transform_row(&mut row).unwrap();
        assert_eq!(row.id, UniversalValue::Int64(9));
    }

    #[test]
    fn pipeline_from_flatten_id_toml_applies_to_rows() {
        use crate::{parse_transforms_toml, Pipeline};

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
            .apply_rows(vec![UniversalRow::new(
                "ledger",
                0,
                UniversalValue::Array {
                    elements: vec![
                        UniversalValue::Text("a".into()),
                        UniversalValue::Text("b".into()),
                    ],
                    element_type: Box::new(UniversalType::Text),
                },
                Default::default(),
            )])
            .unwrap();
        assert_eq!(rows[0].id, UniversalValue::Text("a:b".into()));
    }
}
