//! SurrealQL SCHEMAFULL helpers (`DEFINE TABLE` / `FIELD` / `INDEX`).
//!
//! Used when an operator passes `--schemafull`. Schemaless remains the default:
//! these statements are not emitted unless that flag is set.

use std::collections::HashSet;

use crate::foreign_keys::{classify_table, TableKind};
use crate::schema::{ColumnDefinition, DatabaseSchema, TableDefinition};
use crate::sink::SurrealSink;
use crate::types::{ToDdl, Type};

/// A btree-style index to emit as `DEFINE INDEX` (only with `--schemafull`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaIndex {
    /// SurrealDB table the index is defined on.
    pub table: String,
    /// Index name (`DEFINE INDEX <name>`).
    pub name: String,
    /// Field names, in order.
    pub fields: Vec<String>,
    /// Whether the index is `UNIQUE`.
    pub unique: bool,
}

/// Extra `DEFINE FIELD` not present on the source table (for example `is_current`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaFieldExtra {
    /// SurrealDB table.
    pub table: String,
    /// Field name.
    pub name: String,
    /// SurrealQL type, e.g. `bool` or `datetime`.
    pub type_name: String,
    /// When true, wrap as `option<type>`.
    pub nullable: bool,
}

/// Period start/end pair used for `ASSERT $parent.<start> <= $value` on the end field.
///
/// The check lives on the end field so the start value is already present when
/// SurrealDB evaluates the assertion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeriodBound {
    /// SurrealDB table.
    pub table: String,
    /// Period-start field name (SQL Server `ValidFrom` or equivalent).
    pub start: String,
    /// Period-end field name (SQL Server `ValidTo` or equivalent).
    pub end: String,
}

/// Optional extras merged into SCHEMAFULL statements (indexes, temporal fields).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemafullExtras {
    /// Indexes to emit. Record `id` is never indexed (it is already the primary key).
    pub indexes: Vec<SchemaIndex>,
    /// Extra fields such as `is_current`.
    pub extra_fields: Vec<SchemaFieldExtra>,
    /// Tables whose incoming FKs must stay scalars (system-versioned targets).
    pub scalar_fk_targets: HashSet<String>,
    /// Tables forced to `TYPE RELATION` (same override list as `classify_table`).
    pub relation_tables: Vec<String>,
    /// Period columns that get `ASSERT $parent.<start> <= $value` on the end field.
    pub period_bounds: Vec<PeriodBound>,
}

/// Maps [`Type`] to a SurrealQL field type for `DEFINE FIELD`.
pub struct SurrealDdl;

impl ToDdl for SurrealDdl {
    fn to_ddl(&self, sync_type: &Type) -> String {
        match sync_type {
            Type::Bool => "bool".to_string(),
            Type::Int8 { .. } | Type::Int16 | Type::Int32 | Type::Int64 => "int".to_string(),
            Type::Float32 | Type::Float64 => "float".to_string(),
            Type::Decimal { .. } => "decimal".to_string(),
            Type::Char { .. } | Type::VarChar { .. } | Type::Text => "string".to_string(),
            Type::Blob | Type::Bytes => "bytes".to_string(),
            Type::Date | Type::LocalDateTime | Type::LocalDateTimeNano | Type::ZonedDateTime => {
                "datetime".to_string()
            }
            Type::Time | Type::TimeTz => "string".to_string(),
            Type::Uuid => "uuid".to_string(),
            Type::Ulid => "ulid".to_string(),
            Type::Json | Type::Jsonb | Type::Object => "object".to_string(),
            Type::Array { element_type } => format!("array<{}>", self.to_ddl(element_type)),
            Type::Set { .. } | Type::Enum { .. } => "string".to_string(),
            Type::Geometry { .. } => "geometry".to_string(),
            Type::Duration => "duration".to_string(),
            Type::Thing => "record".to_string(),
        }
    }
}

/// Quote a SurrealQL ident when it is not a plain ASCII name.
pub fn ident(name: &str) -> String {
    if !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
    {
        name.to_string()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

fn option_wrap(inner: String, nullable: bool) -> String {
    if nullable {
        format!("option<{inner}>")
    } else {
        inner
    }
}

fn field_type_for_column(
    table: &TableDefinition,
    col: &ColumnDefinition,
    extras: &SchemafullExtras,
    ddl: &SurrealDdl,
) -> String {
    for fk in &table.foreign_keys {
        if fk.columns.len() == 1 && fk.columns[0] == col.name {
            if extras.scalar_fk_targets.contains(&fk.referenced_table) {
                return option_wrap(ddl.to_ddl(&col.column_type), col.nullable);
            }
            return option_wrap(
                format!("record<{}>", ident(&fk.referenced_table)),
                col.nullable,
            );
        }
    }
    option_wrap(ddl.to_ddl(&col.column_type), col.nullable)
}

fn period_assert(table: &str, field: &str, extras: &SchemafullExtras) -> Option<String> {
    extras.period_bounds.iter().find_map(|bound| {
        if bound.table == table && bound.end == field {
            Some(format!("$parent.{} <= $value", ident(&bound.start)))
        } else {
            None
        }
    })
}

fn define_field(table: &str, name: &str, type_ddl: &str, assert: Option<&str>) -> String {
    let mut sql = format!(
        "DEFINE FIELD {} ON {} TYPE {};",
        ident(name),
        ident(table),
        type_ddl
    );
    if let Some(assert) = assert {
        sql = format!(
            "DEFINE FIELD {} ON {} TYPE {} ASSERT {};",
            ident(name),
            ident(table),
            type_ddl,
            assert
        );
    }
    sql
}

fn all_columns(table: &TableDefinition) -> Vec<&ColumnDefinition> {
    let mut cols = vec![&table.primary_key];
    cols.extend(table.columns.iter());
    cols
}

/// Build `DEFINE TABLE` / `DEFINE FIELD` / `DEFINE INDEX` statements.
///
/// Does not write anything. Call [`emit_schemafull`] to run them on a sink.
pub fn schemafull_statements(schema: &DatabaseSchema, extras: &SchemafullExtras) -> Vec<String> {
    let ddl = SurrealDdl;
    let mut stmts = Vec::new();

    for table in &schema.tables {
        let kind = match classify_table(table, &extras.relation_tables) {
            TableKind::Relation { in_fk, out_fk }
                if extras.scalar_fk_targets.contains(&in_fk.referenced_table)
                    || extras.scalar_fk_targets.contains(&out_fk.referenced_table) =>
            {
                TableKind::Entity
            }
            other => other,
        };
        match kind {
            TableKind::Entity => {
                stmts.push(format!("DEFINE TABLE {} SCHEMAFULL;", ident(&table.name)));
                for col in all_columns(table) {
                    let type_ddl = field_type_for_column(table, col, extras, &ddl);
                    let assert = period_assert(&table.name, &col.name, extras);
                    stmts.push(define_field(
                        &table.name,
                        &col.name,
                        &type_ddl,
                        assert.as_deref(),
                    ));
                }
            }
            TableKind::Relation { in_fk, out_fk } => {
                stmts.push(format!(
                    "DEFINE TABLE {} TYPE RELATION IN {} OUT {} SCHEMAFULL;",
                    ident(&table.name),
                    ident(&in_fk.referenced_table),
                    ident(&out_fk.referenced_table)
                ));
                let skip: HashSet<&str> = in_fk
                    .columns
                    .iter()
                    .chain(out_fk.columns.iter())
                    .map(|s| s.as_str())
                    .collect();
                for col in all_columns(table) {
                    if skip.contains(col.name.as_str()) {
                        continue;
                    }
                    let type_ddl = option_wrap(ddl.to_ddl(&col.column_type), col.nullable);
                    stmts.push(define_field(&table.name, &col.name, &type_ddl, None));
                }
            }
        }

        for extra in extras.extra_fields.iter().filter(|f| f.table == table.name) {
            if all_columns(table).iter().any(|c| c.name == extra.name) {
                continue;
            }
            let type_ddl = option_wrap(extra.type_name.clone(), extra.nullable);
            let assert = period_assert(&table.name, &extra.name, extras);
            stmts.push(define_field(
                &table.name,
                &extra.name,
                &type_ddl,
                assert.as_deref(),
            ));
        }
    }

    for index in &extras.indexes {
        if index.fields.len() == 1 && index.fields[0] == "id" {
            continue;
        }
        let fields = index
            .fields
            .iter()
            .map(|f| ident(f))
            .collect::<Vec<_>>()
            .join(", ");
        let unique = if index.unique { " UNIQUE" } else { "" };
        stmts.push(format!(
            "DEFINE INDEX {} ON {} FIELDS {}{};",
            ident(&index.name),
            ident(&index.table),
            fields,
            unique
        ));
    }

    stmts
}

/// Log and run SCHEMAFULL statements on `sink`.
pub async fn emit_schemafull<S: SurrealSink>(
    sink: &S,
    schema: &DatabaseSchema,
    extras: &SchemafullExtras,
) -> anyhow::Result<()> {
    for stmt in schemafull_statements(schema, extras) {
        tracing::info!("{stmt}");
        sink.query(&stmt).await?;
    }
    Ok(())
}

/// Print (and optionally run) SCHEMAFULL statements when `--schemafull` is set.
pub async fn maybe_emit_schemafull<S: SurrealSink>(
    sink: &S,
    schema: &DatabaseSchema,
    extras: &SchemafullExtras,
    schemafull: bool,
    dry_run: bool,
) -> anyhow::Result<()> {
    if !schemafull {
        return Ok(());
    }
    if dry_run {
        for stmt in schemafull_statements(schema, extras) {
            tracing::info!("{stmt}");
        }
        return Ok(());
    }
    emit_schemafull(sink, schema, extras).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDefinition, ForeignKeyDefinition, TableDefinition, Type};

    fn users_books() -> DatabaseSchema {
        let users = TableDefinition::new(
            "users",
            ColumnDefinition::new("id", Type::Int64),
            vec![
                ColumnDefinition::new("name", Type::Text),
                ColumnDefinition::nullable("email", Type::Text),
            ],
        );
        let mut books = TableDefinition::new(
            "books",
            ColumnDefinition::new("id", Type::Int64),
            vec![
                ColumnDefinition::new("title", Type::Text),
                ColumnDefinition::new("author_id", Type::Int32),
            ],
        );
        books.foreign_keys = vec![ForeignKeyDefinition {
            constraint_name: "fk_author".to_string(),
            columns: vec!["author_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
        }];
        DatabaseSchema::new(vec![users, books])
    }

    fn join_schema() -> DatabaseSchema {
        let mut tags = TableDefinition::new(
            "book_tags",
            ColumnDefinition::new("book_id", Type::Int32),
            vec![
                ColumnDefinition::new("tag_id", Type::Int32),
                ColumnDefinition::nullable("created_at", Type::Text),
            ],
        );
        tags.composite_primary_key = Some(vec!["book_id".to_string(), "tag_id".to_string()]);
        tags.foreign_keys = vec![
            ForeignKeyDefinition {
                constraint_name: "fk_book".to_string(),
                columns: vec!["book_id".to_string()],
                referenced_table: "books".to_string(),
                referenced_columns: vec!["id".to_string()],
            },
            ForeignKeyDefinition {
                constraint_name: "fk_tag".to_string(),
                columns: vec!["tag_id".to_string()],
                referenced_table: "tags".to_string(),
                referenced_columns: vec!["id".to_string()],
            },
        ];
        DatabaseSchema::new(vec![tags])
    }

    fn article_temporal() -> DatabaseSchema {
        let article = TableDefinition::new(
            "Article",
            ColumnDefinition::new("Id", Type::Int32),
            vec![
                ColumnDefinition::new("Title", Type::Text),
                ColumnDefinition::new("ValidFrom", Type::ZonedDateTime),
                ColumnDefinition::new("ValidTo", Type::ZonedDateTime),
            ],
        );
        DatabaseSchema::new(vec![article])
    }

    #[test]
    fn option_string_and_record_user() {
        let stmts = schemafull_statements(&users_books(), &SchemafullExtras::default());
        assert!(stmts.contains(&"DEFINE TABLE users SCHEMAFULL;".to_string()));
        assert!(stmts.contains(&"DEFINE FIELD name ON users TYPE string;".to_string()));
        assert!(stmts.contains(&"DEFINE FIELD email ON users TYPE option<string>;".to_string()));
        assert!(stmts.contains(&"DEFINE FIELD author_id ON books TYPE record<users>;".to_string()));
    }

    #[test]
    fn relation_table() {
        let stmts = schemafull_statements(&join_schema(), &SchemafullExtras::default());
        assert!(stmts.contains(
            &"DEFINE TABLE book_tags TYPE RELATION IN books OUT tags SCHEMAFULL;".to_string()
        ));
        assert!(stmts
            .contains(&"DEFINE FIELD created_at ON book_tags TYPE option<string>;".to_string()));
        assert!(!stmts.iter().any(|s| s.contains("book_id")));
        assert!(!stmts
            .iter()
            .any(|s| s.contains("tag_id") && s.contains("DEFINE FIELD")));
    }

    #[test]
    fn period_assert() {
        let extras = SchemafullExtras {
            period_bounds: vec![PeriodBound {
                table: "Article".to_string(),
                start: "ValidFrom".to_string(),
                end: "ValidTo".to_string(),
            }],
            extra_fields: vec![SchemaFieldExtra {
                table: "Article".to_string(),
                name: "is_current".to_string(),
                type_name: "bool".to_string(),
                nullable: false,
            }],
            ..SchemafullExtras::default()
        };
        let stmts = schemafull_statements(&article_temporal(), &extras);
        assert!(stmts.contains(
            &"DEFINE FIELD ValidTo ON Article TYPE datetime ASSERT $parent.ValidFrom <= $value;"
                .to_string()
        ));
        assert!(stmts.contains(&"DEFINE FIELD is_current ON Article TYPE bool;".to_string()));
    }

    #[test]
    fn unique_vs_non_unique_index() {
        let extras = SchemafullExtras {
            indexes: vec![
                SchemaIndex {
                    table: "users".to_string(),
                    name: "users_email_unique".to_string(),
                    fields: vec!["email".to_string()],
                    unique: true,
                },
                SchemaIndex {
                    table: "users".to_string(),
                    name: "users_name".to_string(),
                    fields: vec!["name".to_string()],
                    unique: false,
                },
                SchemaIndex {
                    table: "users".to_string(),
                    name: "users_id".to_string(),
                    fields: vec!["id".to_string()],
                    unique: true,
                },
            ],
            ..SchemafullExtras::default()
        };
        let stmts = schemafull_statements(&users_books(), &extras);
        assert!(stmts.contains(
            &"DEFINE INDEX users_email_unique ON users FIELDS email UNIQUE;".to_string()
        ));
        assert!(stmts.contains(&"DEFINE INDEX users_name ON users FIELDS name;".to_string()));
        assert!(!stmts.iter().any(|s| s.contains("users_id")));
    }

    #[test]
    fn temporal_cookbook_indexes() {
        let extras = SchemafullExtras {
            extra_fields: vec![SchemaFieldExtra {
                table: "Article".to_string(),
                name: "is_current".to_string(),
                type_name: "bool".to_string(),
                nullable: false,
            }],
            indexes: vec![
                SchemaIndex {
                    table: "Article".to_string(),
                    name: "Article_is_current".to_string(),
                    fields: vec!["is_current".to_string()],
                    unique: false,
                },
                SchemaIndex {
                    table: "Article".to_string(),
                    name: "Article_Id".to_string(),
                    fields: vec!["Id".to_string()],
                    unique: false,
                },
                SchemaIndex {
                    table: "Article".to_string(),
                    name: "Article_Id_is_current".to_string(),
                    fields: vec!["Id".to_string(), "is_current".to_string()],
                    unique: false,
                },
                SchemaIndex {
                    table: "Article".to_string(),
                    name: "Article_ValidFrom_ValidTo".to_string(),
                    fields: vec!["ValidFrom".to_string(), "ValidTo".to_string()],
                    unique: false,
                },
            ],
            ..SchemafullExtras::default()
        };
        let stmts = schemafull_statements(&article_temporal(), &extras);
        assert!(stmts
            .iter()
            .any(|s| s.contains("FIELDS is_current;")
                && s.contains("DEFINE INDEX Article_is_current")));
        assert!(stmts.contains(&"DEFINE INDEX Article_Id ON Article FIELDS Id;".to_string()));
        assert!(stmts.contains(
            &"DEFINE INDEX Article_Id_is_current ON Article FIELDS Id, is_current;".to_string()
        ));
        assert!(stmts.contains(
            &"DEFINE INDEX Article_ValidFrom_ValidTo ON Article FIELDS ValidFrom, ValidTo;"
                .to_string()
        ));
        assert!(!stmts
            .iter()
            .any(|s| s.contains("UNIQUE") && s.contains("Id")));
    }

    #[test]
    fn relation_demoted_when_endpoint_is_temporal() {
        let schema = join_schema();
        let extras = SchemafullExtras {
            scalar_fk_targets: HashSet::from(["books".to_string()]),
            relation_tables: vec!["book_tags".to_string()],
            ..SchemafullExtras::default()
        };
        let stmts = schemafull_statements(&schema, &extras);
        assert!(stmts.contains(&"DEFINE TABLE book_tags SCHEMAFULL;".to_string()));
        assert!(!stmts.iter().any(|s| s.contains("TYPE RELATION")));
        assert!(stmts.contains(&"DEFINE FIELD book_id ON book_tags TYPE int;".to_string()));
        assert!(stmts.contains(&"DEFINE FIELD tag_id ON book_tags TYPE record<tags>;".to_string()));
    }

    #[test]
    fn scalar_fk_to_temporal_target() {
        let mut orders = TableDefinition::new(
            "orders",
            ColumnDefinition::new("id", Type::Int64),
            vec![ColumnDefinition::new("article_id", Type::Int32)],
        );
        orders.foreign_keys = vec![ForeignKeyDefinition {
            constraint_name: "fk_article".to_string(),
            columns: vec!["article_id".to_string()],
            referenced_table: "Article".to_string(),
            referenced_columns: vec!["Id".to_string()],
        }];
        let schema = DatabaseSchema::new(vec![orders]);
        let extras = SchemafullExtras {
            scalar_fk_targets: HashSet::from(["Article".to_string()]),
            ..SchemafullExtras::default()
        };
        let stmts = schemafull_statements(&schema, &extras);
        assert!(stmts.contains(&"DEFINE FIELD article_id ON orders TYPE int;".to_string()));
        assert!(!stmts.iter().any(|s| s.contains("record<Article>")));
    }
}
