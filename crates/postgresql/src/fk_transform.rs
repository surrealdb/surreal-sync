//! Foreign key value transformation utilities.
//!
//! Converts FK column values to SurrealDB record links (`UniversalValue::Thing`)
//! and builds `UniversalRelation` instances from join-table rows.

use std::collections::HashMap;
use sync_core::{
    ForeignKeyDefinition, TableDefinition, UniversalRelation, UniversalThingRef, UniversalValue,
};

/// Transform FK column values in a field map to record links.
///
/// For each FK on the table, the raw value (e.g. `Int64(1)`) is wrapped
/// as `UniversalValue::Thing { table: referenced_table, id: raw_value }`.
/// Null values are left as `UniversalValue::Null`.
pub fn transform_fk_values(
    fields: &mut HashMap<String, UniversalValue>,
    table_def: &TableDefinition,
) {
    for fk in &table_def.foreign_keys {
        // Only handle single-column FKs for record link conversion
        if fk.columns.len() == 1 {
            let col = &fk.columns[0];
            if let Some(value) = fields.remove(col) {
                let transformed = match value {
                    UniversalValue::Null => UniversalValue::Null,
                    other => UniversalValue::Thing {
                        table: fk.referenced_table.clone(),
                        id: Box::new(other),
                    },
                };
                fields.insert(col.clone(), transformed);
            }
        }
    }
}

/// Build a `UniversalRelation` from a join-table row.
///
/// The two FK endpoints become the `input` and `output` of the relation.
/// FK columns are consumed; remaining fields become the relation's `data`.
pub fn build_relation_from_row(
    table_name: &str,
    id: UniversalValue,
    mut fields: HashMap<String, UniversalValue>,
    in_fk: &ForeignKeyDefinition,
    out_fk: &ForeignKeyDefinition,
) -> UniversalRelation {
    let in_id = extract_fk_value(&mut fields, in_fk);
    let out_id = extract_fk_value(&mut fields, out_fk);

    let input = UniversalThingRef::new(in_fk.referenced_table.clone(), in_id);
    let output = UniversalThingRef::new(out_fk.referenced_table.clone(), out_id);

    // For composite PK Arrays, flatten into a string ID for the relation.
    // SurrealDB relation IDs must be a simple type, not an array.
    let rel_id = flatten_composite_id(id);

    UniversalRelation::new(table_name, rel_id, input, output, fields)
}

/// Build a `UniversalRelation` from an incremental change event.
///
/// In incremental sync, FK columns that are also PK columns have already
/// been stripped from `data` and placed in `id` (as a composite Array).
/// This function reconstructs FK values from the composite ID when they
/// are missing from the data map.
pub fn build_relation_from_change(
    table_name: &str,
    id: UniversalValue,
    mut data: HashMap<String, UniversalValue>,
    in_fk: &ForeignKeyDefinition,
    out_fk: &ForeignKeyDefinition,
) -> UniversalRelation {
    // If FK columns are missing from data (because they were PK columns),
    // inject them back from the composite ID array.
    if let UniversalValue::Array { ref elements, .. } = id {
        inject_missing_fk_from_composite_id(&mut data, in_fk, out_fk, elements);
    }

    build_relation_from_row(table_name, id, data, in_fk, out_fk)
}

/// Inject FK values back into the data map from a composite ID array.
///
/// The composite PK columns map positionally to the FK columns in the
/// same order as the PK was defined. We match FK column names against
/// the table's composite PK order.
fn inject_missing_fk_from_composite_id(
    data: &mut HashMap<String, UniversalValue>,
    in_fk: &ForeignKeyDefinition,
    out_fk: &ForeignKeyDefinition,
    id_elements: &[UniversalValue],
) {
    let all_fk_cols: Vec<&str> = in_fk
        .columns
        .iter()
        .chain(out_fk.columns.iter())
        .map(|s| s.as_str())
        .collect();

    for (i, element) in id_elements.iter().enumerate() {
        if i < all_fk_cols.len() {
            let col = all_fk_cols[i];
            if !data.contains_key(col) {
                data.insert(col.to_string(), element.clone());
            }
        }
    }
}

/// Flatten a composite (Array) ID into a string ID.
/// Simple IDs are returned as-is. Array IDs are joined with `:` separators.
fn flatten_composite_id(id: UniversalValue) -> UniversalValue {
    match id {
        UniversalValue::Array { elements, .. } => {
            let parts: Vec<String> = elements
                .into_iter()
                .map(|v| match v {
                    UniversalValue::Int32(n) => n.to_string(),
                    UniversalValue::Int64(n) => n.to_string(),
                    UniversalValue::Text(s) => s,
                    UniversalValue::Uuid(u) => u.to_string(),
                    other => format!("{other:?}"),
                })
                .collect();
            UniversalValue::Text(parts.join(":"))
        }
        other => other,
    }
}

/// Extract and remove the FK value from the fields map.
/// For single-column FKs, returns the value directly.
/// For multi-column FKs, returns an array of values.
fn extract_fk_value(
    fields: &mut HashMap<String, UniversalValue>,
    fk: &ForeignKeyDefinition,
) -> UniversalValue {
    if fk.columns.len() == 1 {
        fields
            .remove(&fk.columns[0])
            .unwrap_or(UniversalValue::Null)
    } else {
        let vals: Vec<UniversalValue> = fk
            .columns
            .iter()
            .map(|c| fields.remove(c).unwrap_or(UniversalValue::Null))
            .collect();
        UniversalValue::Array {
            elements: vals,
            element_type: Box::new(sync_core::UniversalType::Text),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::{ColumnDefinition, UniversalType};

    fn make_table_with_fks(fks: Vec<ForeignKeyDefinition>) -> TableDefinition {
        let mut td = TableDefinition::new(
            "books",
            ColumnDefinition::new("id", UniversalType::Int64),
            vec![
                ColumnDefinition::new("title", UniversalType::Text),
                ColumnDefinition::new("author_id", UniversalType::Int32),
            ],
        );
        td.foreign_keys = fks;
        td
    }

    #[test]
    fn test_transform_fk_values_converts_fk_to_thing() {
        let fks = vec![ForeignKeyDefinition {
            constraint_name: "fk_author".to_string(),
            columns: vec!["author_id".to_string()],
            referenced_table: "authors".to_string(),
            referenced_columns: vec!["id".to_string()],
        }];
        let table = make_table_with_fks(fks);

        let mut fields = HashMap::new();
        fields.insert("title".to_string(), UniversalValue::Text("Rust".to_string()));
        fields.insert("author_id".to_string(), UniversalValue::Int32(42));

        transform_fk_values(&mut fields, &table);

        assert_eq!(
            fields.get("title"),
            Some(&UniversalValue::Text("Rust".to_string()))
        );
        match fields.get("author_id") {
            Some(UniversalValue::Thing { table, id }) => {
                assert_eq!(table, "authors");
                assert_eq!(**id, UniversalValue::Int32(42));
            }
            other => panic!("Expected Thing, got {:?}", other),
        }
    }

    #[test]
    fn test_transform_fk_values_null_stays_null() {
        let fks = vec![ForeignKeyDefinition {
            constraint_name: "fk_author".to_string(),
            columns: vec!["author_id".to_string()],
            referenced_table: "authors".to_string(),
            referenced_columns: vec!["id".to_string()],
        }];
        let table = make_table_with_fks(fks);

        let mut fields = HashMap::new();
        fields.insert("author_id".to_string(), UniversalValue::Null);

        transform_fk_values(&mut fields, &table);

        assert_eq!(fields.get("author_id"), Some(&UniversalValue::Null));
    }

    #[test]
    fn test_transform_fk_values_non_fk_field_unchanged() {
        let table = make_table_with_fks(vec![]);
        let mut fields = HashMap::new();
        fields.insert("title".to_string(), UniversalValue::Text("Go".to_string()));

        transform_fk_values(&mut fields, &table);

        assert_eq!(
            fields.get("title"),
            Some(&UniversalValue::Text("Go".to_string()))
        );
    }

    #[test]
    fn test_transform_fk_values_multiple_fks() {
        let fks = vec![
            ForeignKeyDefinition {
                constraint_name: "fk_author".to_string(),
                columns: vec!["author_id".to_string()],
                referenced_table: "authors".to_string(),
                referenced_columns: vec!["id".to_string()],
            },
            ForeignKeyDefinition {
                constraint_name: "fk_editor".to_string(),
                columns: vec!["editor_id".to_string()],
                referenced_table: "editors".to_string(),
                referenced_columns: vec!["id".to_string()],
            },
        ];
        let mut td = TableDefinition::new(
            "books",
            ColumnDefinition::new("id", UniversalType::Int64),
            vec![
                ColumnDefinition::new("author_id", UniversalType::Int32),
                ColumnDefinition::new("editor_id", UniversalType::Int32),
            ],
        );
        td.foreign_keys = fks;

        let mut fields = HashMap::new();
        fields.insert("author_id".to_string(), UniversalValue::Int32(1));
        fields.insert("editor_id".to_string(), UniversalValue::Int32(2));

        transform_fk_values(&mut fields, &td);

        match fields.get("author_id") {
            Some(UniversalValue::Thing { table, .. }) => assert_eq!(table, "authors"),
            other => panic!("Expected Thing for author_id, got {:?}", other),
        }
        match fields.get("editor_id") {
            Some(UniversalValue::Thing { table, .. }) => assert_eq!(table, "editors"),
            other => panic!("Expected Thing for editor_id, got {:?}", other),
        }
    }

    #[test]
    fn test_build_relation_from_row() {
        let in_fk = ForeignKeyDefinition {
            constraint_name: "fk_book".to_string(),
            columns: vec!["book_id".to_string()],
            referenced_table: "books".to_string(),
            referenced_columns: vec!["id".to_string()],
        };
        let out_fk = ForeignKeyDefinition {
            constraint_name: "fk_tag".to_string(),
            columns: vec!["tag_id".to_string()],
            referenced_table: "tags".to_string(),
            referenced_columns: vec!["id".to_string()],
        };

        let mut fields = HashMap::new();
        fields.insert("book_id".to_string(), UniversalValue::Int32(10));
        fields.insert("tag_id".to_string(), UniversalValue::Int32(20));
        fields.insert(
            "created_at".to_string(),
            UniversalValue::Text("2024-01-01".to_string()),
        );

        let rel = build_relation_from_row(
            "book_tags",
            UniversalValue::Int64(1),
            fields,
            &in_fk,
            &out_fk,
        );

        assert_eq!(rel.relation_type, "book_tags");
        assert_eq!(rel.input.table, "books");
        assert_eq!(rel.input.id, UniversalValue::Int32(10));
        assert_eq!(rel.output.table, "tags");
        assert_eq!(rel.output.id, UniversalValue::Int32(20));
        // FK columns should be removed; only extra data remains
        assert_eq!(rel.data.len(), 1);
        assert!(rel.data.contains_key("created_at"));
        assert!(!rel.data.contains_key("book_id"));
        assert!(!rel.data.contains_key("tag_id"));
    }
}
