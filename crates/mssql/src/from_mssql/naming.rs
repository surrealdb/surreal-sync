//! Source table names (`dbo.Article`) and SurrealDB target names (`Article`).

use anyhow::{anyhow, Result};
use std::collections::HashMap;

/// Schema-qualified SQL Server table name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedName {
    /// Schema (defaults to `dbo` when a bare table name is given).
    pub schema: String,
    /// Table name without schema.
    pub table: String,
}

impl QualifiedName {
    /// Build a qualified name.
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// `schema.table` without brackets.
    pub fn dotted(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Bracketed `[schema].[table]` for T-SQL.
    pub fn quoted(&self) -> String {
        format!("{}.{}", bracket(&self.schema), bracket(&self.table))
    }
}

impl std::fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.dotted())
    }
}

/// Quote a T-SQL identifier.
pub fn bracket(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

fn strip_brackets(part: &str) -> &str {
    let part = part.trim();
    part.strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(part)
}

/// Parse `table`, `schema.table`, or `[dbo].[Article]`. A bare name is `dbo`.
pub fn parse_table_ref(input: &str) -> Result<QualifiedName> {
    let input = input.trim();
    if input.is_empty() {
        anyhow::bail!("empty table name");
    }
    let parts: Vec<&str> = if input.contains('[') {
        input
            .split('.')
            .map(strip_brackets)
            .filter(|p| !p.is_empty())
            .collect()
    } else {
        input.split('.').map(str::trim).collect()
    };
    match parts.as_slice() {
        [table] => Ok(QualifiedName::new("dbo", *table)),
        [schema, table] => Ok(QualifiedName::new(*schema, *table)),
        _ => Err(anyhow!(
            "invalid table name `{input}`; use `table` or `schema.table`"
        )),
    }
}

/// SurrealDB table name: `dbo.T` → `T`, `sales.Order` → `sales__Order`.
pub fn target_table_name(name: &QualifiedName) -> String {
    if name.schema.eq_ignore_ascii_case("dbo") {
        name.table.clone()
    } else {
        format!("{}__{}", name.schema, name.table)
    }
}

/// Error if two source tables would write the same SurrealDB table.
pub fn detect_collisions(names: &[QualifiedName]) -> Result<()> {
    let mut by_target: HashMap<String, Vec<String>> = HashMap::new();
    for name in names {
        by_target
            .entry(target_table_name(name))
            .or_default()
            .push(name.dotted());
    }
    let mut colliding: Vec<(String, Vec<String>)> = by_target
        .into_iter()
        .filter(|(_, sources)| sources.len() > 1)
        .collect();
    if colliding.is_empty() {
        return Ok(());
    }
    colliding.sort_by(|a, b| a.0.cmp(&b.0));
    let detail = colliding
        .into_iter()
        .map(|(target, sources)| format!("{target} <= {}", sources.join(", ")))
        .collect::<Vec<_>>()
        .join("; ");
    Err(anyhow!(
        "source tables map to the same SurrealDB table name: {detail}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bare_and_qualified() {
        assert_eq!(
            parse_table_ref("Article").unwrap(),
            QualifiedName::new("dbo", "Article")
        );
        assert_eq!(
            parse_table_ref("sales.Order").unwrap(),
            QualifiedName::new("sales", "Order")
        );
        assert_eq!(
            parse_table_ref("[dbo].[Article]").unwrap(),
            QualifiedName::new("dbo", "Article")
        );
    }

    #[test]
    fn target_names() {
        assert_eq!(
            target_table_name(&QualifiedName::new("dbo", "Article")),
            "Article"
        );
        assert_eq!(
            target_table_name(&QualifiedName::new("sales", "Order")),
            "sales__Order"
        );
    }

    #[test]
    fn collisions_list_sources() {
        detect_collisions(&[
            QualifiedName::new("dbo", "T"),
            QualifiedName::new("sales", "Order"),
        ])
        .unwrap();
        let err = detect_collisions(&[
            QualifiedName::new("dbo", "sales__Order"),
            QualifiedName::new("sales", "Order"),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("sales__Order"), "{err}");
    }
}
