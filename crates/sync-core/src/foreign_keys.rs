//! Foreign key definitions and table classification.
//!
//! This module provides types for representing foreign key constraints
//! and logic for classifying tables as entity tables or relation (join) tables.

use crate::schema::TableDefinition;
use serde::{Deserialize, Serialize};

/// A foreign key constraint on a table.
///
/// Represents a single FK constraint linking one or more columns in the
/// current table to columns in a referenced table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignKeyDefinition {
    /// Constraint name in the source database
    pub constraint_name: String,
    /// Column(s) in this table that form the FK
    pub columns: Vec<String>,
    /// The table referenced by this FK
    pub referenced_table: String,
    /// The column(s) in the referenced table
    pub referenced_columns: Vec<String>,
}

/// Classification of a table for SurrealDB sync purposes.
///
/// Entity tables are synced as regular SurrealDB records (with FK columns
/// converted to record links). Relation tables are synced as SurrealDB
/// graph edges via `RELATE`.
#[derive(Debug, Clone)]
pub enum TableKind {
    /// A normal entity table. FK columns become record links.
    Entity,
    /// A join/relation table. The two FKs become the `in` and `out`
    /// endpoints of a SurrealDB graph edge.
    Relation {
        in_fk: ForeignKeyDefinition,
        out_fk: ForeignKeyDefinition,
    },
}

/// Classify a table as entity or relation.
///
/// **Auto-detection heuristic**: a table is a relation table when it has
/// exactly 2 foreign keys that point to 2 distinct tables, and the primary
/// key is composed entirely of those FK columns (classic composite-PK join table).
///
/// **Override**: if `relation_table_overrides` contains the table name the
/// table is forced to `TableKind::Relation` regardless of the heuristic
/// (provided it has at least 2 FKs pointing to distinct tables).
pub fn classify_table(
    table: &TableDefinition,
    relation_table_overrides: &[String],
) -> TableKind {
    let forced = relation_table_overrides.contains(&table.name);

    let fks = &table.foreign_keys;

    // Need at least 2 FKs to form a relation
    if fks.len() < 2 {
        return TableKind::Entity;
    }

    if forced {
        // When forced, use the first two FKs (even if they reference the same table,
        // e.g. self-referencing relations like mentorship or friendship).
        return TableKind::Relation {
            in_fk: fks[0].clone(),
            out_fk: fks[1].clone(),
        };
    }

    // Auto-detection: pick the first two FKs referencing distinct tables.
    let (in_fk, out_fk) = match find_distinct_fk_pair(fks) {
        Some(pair) => pair,
        None => return TableKind::Entity,
    };

    // PK must be composed entirely of those FK columns
    if pk_is_composed_of_fk_columns(table, in_fk, out_fk) {
        TableKind::Relation {
            in_fk: in_fk.clone(),
            out_fk: out_fk.clone(),
        }
    } else {
        TableKind::Entity
    }
}

/// Find the first pair of FKs that reference two distinct tables.
fn find_distinct_fk_pair(
    fks: &[ForeignKeyDefinition],
) -> Option<(&ForeignKeyDefinition, &ForeignKeyDefinition)> {
    for (i, a) in fks.iter().enumerate() {
        for b in &fks[i + 1..] {
            if a.referenced_table != b.referenced_table {
                return Some((a, b));
            }
        }
    }
    None
}

/// Check whether the table's primary key columns are exactly the union
/// of the two FK column sets.
fn pk_is_composed_of_fk_columns(
    table: &TableDefinition,
    in_fk: &ForeignKeyDefinition,
    out_fk: &ForeignKeyDefinition,
) -> bool {
    let mut fk_cols: Vec<&str> = in_fk
        .columns
        .iter()
        .chain(out_fk.columns.iter())
        .map(|s| s.as_str())
        .collect();
    fk_cols.sort();
    fk_cols.dedup();

    let mut pk_cols = table.primary_key_column_names();
    pk_cols.sort();

    pk_cols == fk_cols
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnDefinition;
    use crate::types::UniversalType;

    fn make_table(
        name: &str,
        pk_names: Vec<&str>,
        column_names: Vec<&str>,
        fks: Vec<ForeignKeyDefinition>,
    ) -> TableDefinition {
        let primary_key = if pk_names.len() == 1 {
            ColumnDefinition::new(pk_names[0], UniversalType::Int64)
        } else {
            ColumnDefinition::new(pk_names[0], UniversalType::Int64)
        };

        let mut columns: Vec<ColumnDefinition> = column_names
            .iter()
            .map(|n| ColumnDefinition::new(*n, UniversalType::Int64))
            .collect();

        // For composite PK, add the extra PK columns to columns list
        for pk in &pk_names[1..] {
            if !column_names.contains(pk) {
                columns.push(ColumnDefinition::new(*pk, UniversalType::Int64));
            }
        }

        let mut td = TableDefinition::new(name, primary_key, columns);
        td.foreign_keys = fks;
        if pk_names.len() > 1 {
            td.composite_primary_key = Some(pk_names.iter().map(|s| s.to_string()).collect());
        }
        td
    }

    fn fk(name: &str, cols: Vec<&str>, ref_table: &str, ref_cols: Vec<&str>) -> ForeignKeyDefinition {
        ForeignKeyDefinition {
            constraint_name: name.to_string(),
            columns: cols.iter().map(|s| s.to_string()).collect(),
            referenced_table: ref_table.to_string(),
            referenced_columns: ref_cols.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_entity_no_fks() {
        let table = make_table("authors", vec!["id"], vec!["name"], vec![]);
        assert!(matches!(classify_table(&table, &[]), TableKind::Entity));
    }

    #[test]
    fn test_entity_one_fk() {
        let table = make_table(
            "books",
            vec!["id"],
            vec!["title", "author_id"],
            vec![fk("fk_author", vec!["author_id"], "authors", vec!["id"])],
        );
        assert!(matches!(classify_table(&table, &[]), TableKind::Entity));
    }

    #[test]
    fn test_entity_two_fks_but_pk_not_composed_of_fks() {
        let table = make_table(
            "collaborations",
            vec!["id"],
            vec!["author1_id", "author2_id", "project"],
            vec![
                fk("fk_a1", vec!["author1_id"], "authors", vec!["id"]),
                fk("fk_a2", vec!["author2_id"], "editors", vec!["id"]),
            ],
        );
        assert!(matches!(classify_table(&table, &[]), TableKind::Entity));
    }

    #[test]
    fn test_relation_classic_join_table() {
        let table = make_table(
            "book_tags",
            vec!["book_id", "tag_id"],
            vec!["created_at"],
            vec![
                fk("fk_book", vec!["book_id"], "books", vec!["id"]),
                fk("fk_tag", vec!["tag_id"], "tags", vec!["id"]),
            ],
        );
        match classify_table(&table, &[]) {
            TableKind::Relation { in_fk, out_fk } => {
                assert_eq!(in_fk.referenced_table, "books");
                assert_eq!(out_fk.referenced_table, "tags");
            }
            TableKind::Entity => panic!("Expected Relation"),
        }
    }

    #[test]
    fn test_entity_pk_has_extra_col_beyond_fks() {
        let table = make_table(
            "events",
            vec!["book_id", "tag_id", "seq"],
            vec!["data"],
            vec![
                fk("fk_book", vec!["book_id"], "books", vec!["id"]),
                fk("fk_tag", vec!["tag_id"], "tags", vec!["id"]),
            ],
        );
        assert!(matches!(classify_table(&table, &[]), TableKind::Entity));
    }

    #[test]
    fn test_override_forces_relation() {
        let table = make_table(
            "collaborations",
            vec!["id"],
            vec!["author1_id", "author2_id", "project"],
            vec![
                fk("fk_a1", vec!["author1_id"], "authors", vec!["id"]),
                fk("fk_a2", vec!["author2_id"], "editors", vec!["id"]),
            ],
        );
        let overrides = vec!["collaborations".to_string()];
        match classify_table(&table, &overrides) {
            TableKind::Relation { in_fk, out_fk } => {
                assert_eq!(in_fk.referenced_table, "authors");
                assert_eq!(out_fk.referenced_table, "editors");
            }
            TableKind::Entity => panic!("Expected Relation with override"),
        }
    }

    #[test]
    fn test_override_not_matching_uses_heuristic() {
        let table = make_table(
            "books",
            vec!["id"],
            vec!["title", "author_id"],
            vec![fk("fk_author", vec!["author_id"], "authors", vec!["id"])],
        );
        let overrides = vec!["other_table".to_string()];
        assert!(matches!(
            classify_table(&table, &overrides),
            TableKind::Entity
        ));
    }

    #[test]
    fn test_two_fks_same_table_no_relation() {
        let table = make_table(
            "friendships",
            vec!["user1_id", "user2_id"],
            vec![],
            vec![
                fk("fk_u1", vec!["user1_id"], "users", vec!["id"]),
                fk("fk_u2", vec!["user2_id"], "users", vec!["id"]),
            ],
        );
        // Both FKs point to the same table, so auto-detection returns Entity
        assert!(matches!(classify_table(&table, &[]), TableKind::Entity));
    }

    #[test]
    fn test_override_same_table_fks_forces_relation() {
        let table = make_table(
            "friendships",
            vec!["user1_id", "user2_id"],
            vec![],
            vec![
                fk("fk_u1", vec!["user1_id"], "users", vec!["id"]),
                fk("fk_u2", vec!["user2_id"], "users", vec!["id"]),
            ],
        );
        let overrides = vec!["friendships".to_string()];
        match classify_table(&table, &overrides) {
            TableKind::Relation { in_fk, out_fk } => {
                assert_eq!(in_fk.referenced_table, "users");
                assert_eq!(out_fk.referenced_table, "users");
            }
            TableKind::Entity => panic!("Override should force Relation even with same-table FKs"),
        }
    }
}
