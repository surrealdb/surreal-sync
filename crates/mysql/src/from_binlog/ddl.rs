use crate::binlog_protocol::QueryEvent;

pub(crate) fn is_table_affecting_ddl(query: &QueryEvent, database: &str) -> bool {
    if !query.database.is_empty() && !query.database.eq_ignore_ascii_case(database) {
        return false;
    }
    let tokens = sql_tokens(&query.sql);
    matches_table_ddl(&tokens)
}

fn matches_table_ddl(tokens: &[String]) -> bool {
    match tokens.first().map(String::as_str) {
        Some("ALTER") => tokens.get(1).is_some_and(|t| t == "TABLE"),
        Some("CREATE") => {
            tokens.get(1).is_some_and(|t| t == "TABLE")
                || (tokens.get(1).is_some_and(|t| t == "TEMPORARY")
                    && tokens.get(2).is_some_and(|t| t == "TABLE"))
        }
        Some("DROP") => {
            tokens.get(1).is_some_and(|t| t == "TABLE")
                || (tokens.get(1).is_some_and(|t| t == "TEMPORARY")
                    && tokens.get(2).is_some_and(|t| t == "TABLE"))
        }
        Some("RENAME") => tokens.get(1).is_some_and(|t| t == "TABLE"),
        Some("TRUNCATE") => true,
        _ => false,
    }
}

/// A single `old -> new` rename extracted from a `RENAME TABLE` or
/// `ALTER TABLE ... RENAME TO ...` statement. Names are unquoted and stripped of
/// any `db.` qualifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableRename {
    pub old: String,
    pub new: String,
}

/// Extract table renames from a DDL statement, if it is a rename.
///
/// Handles both `RENAME TABLE a TO b, c TO d` and
/// `ALTER TABLE a RENAME TO b` / `ALTER TABLE a RENAME b`. Returns an empty
/// vec for any other statement. The parser is deliberately lightweight (the
/// binlog only carries already-validated DDL), covering the common forms.
pub(crate) fn parse_table_renames(sql: &str) -> Vec<TableRename> {
    let tokens = sql_tokens_all(sql);
    let upper: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();

    match upper.first().map(String::as_str) {
        Some("RENAME") if upper.get(1).is_some_and(|t| t == "TABLE") => {
            parse_rename_pairs(&tokens[2..], &upper[2..])
        }
        Some("ALTER") if upper.get(1).is_some_and(|t| t == "TABLE") => {
            // ALTER TABLE <old> RENAME [TO] <new>
            let Some(rename_at) = upper.iter().position(|t| t == "RENAME") else {
                return Vec::new();
            };
            // `ALTER TABLE t RENAME COLUMN a TO b` / `RENAME INDEX ...` rename
            // sub-objects, not the table itself.
            if upper
                .get(rename_at + 1)
                .is_some_and(|t| matches!(t.as_str(), "COLUMN" | "INDEX" | "KEY"))
            {
                return Vec::new();
            }
            let old = tokens.get(2).map(|t| strip_qualifier(t));
            let mut new_idx = rename_at + 1;
            if upper.get(new_idx).is_some_and(|t| t == "TO") {
                new_idx += 1;
            }
            match (old, tokens.get(new_idx)) {
                (Some(old), Some(new)) if rename_at == 3 => vec![TableRename {
                    old,
                    new: strip_qualifier(new),
                }],
                _ => Vec::new(),
            }
        }
        _ => Vec::new(),
    }
}

/// Parse `a TO b, c TO d` comma-separated rename pairs.
fn parse_rename_pairs(tokens: &[String], upper: &[String]) -> Vec<TableRename> {
    let mut out = Vec::new();
    let mut i = 0;
    // Each pair needs three tokens: <old> TO <new>.
    while i + 2 < tokens.len() {
        if upper[i + 1] != "TO" {
            break;
        }
        out.push(TableRename {
            old: strip_qualifier(&tokens[i]),
            new: strip_qualifier(&tokens[i + 2]),
        });
        i += 3;
        // Skip a separating comma token if present.
        if i < tokens.len() && tokens[i] == "," {
            i += 1;
        }
    }
    out
}

fn strip_qualifier(name: &str) -> String {
    let unquoted = name.trim_matches(|c| c == '`' || c == '"');
    match unquoted.rsplit_once('.') {
        Some((_, table)) => table.trim_matches(|c| c == '`' || c == '"').to_string(),
        None => unquoted.to_string(),
    }
}

/// Tokenize preserving identifiers, `.` qualifiers and `,` separators, so the
/// rename parser can walk `a TO b, c TO d`.
fn sql_tokens_all(sql: &str) -> Vec<String> {
    let sql = strip_leading_comments(sql);
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in sql.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '`' || ch == '"' || ch == '.' {
            current.push(ch);
        } else if ch == ',' {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            tokens.push(",".to_string());
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn sql_tokens(sql: &str) -> Vec<String> {
    let sql = strip_leading_comments(sql);
    sql.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .filter(|token| !token.is_empty())
        .take(4)
        .map(|token| token.to_ascii_uppercase())
        .collect()
}

fn strip_leading_comments(mut sql: &str) -> &str {
    loop {
        let trimmed = sql.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--") {
            if let Some((_, after)) = rest.split_once('\n') {
                sql = after;
                continue;
            }
            return "";
        }
        if let Some(rest) = trimmed.strip_prefix('#') {
            if let Some((_, after)) = rest.split_once('\n') {
                sql = after;
                continue;
            }
            return "";
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            if let Some((_, after)) = rest.split_once("*/") {
                sql = after;
                continue;
            }
            return "";
        }
        return trimmed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(sql: &str) -> QueryEvent {
        QueryEvent {
            thread_id: 1,
            database: "app".to_string(),
            sql: sql.to_string(),
        }
    }

    #[test]
    fn detects_representative_table_ddl() {
        for sql in [
            "CREATE TABLE `widgets` (id int primary key)",
            "create temporary table t (id int)",
            "ALTER TABLE widgets ADD COLUMN name text",
            "DROP TABLE `widgets`",
            "RENAME TABLE old_name TO new_name",
            "TRUNCATE TABLE widgets",
            "/* gh-ost */ ALTER TABLE `widgets` ADD COLUMN c int",
        ] {
            assert!(is_table_affecting_ddl(&query(sql), "app"), "{sql}");
        }
    }

    #[test]
    fn ignores_non_table_or_other_database_queries() {
        assert!(!is_table_affecting_ddl(&query("BEGIN"), "app"));
        assert!(!is_table_affecting_ddl(
            &query("CREATE DATABASE other"),
            "app"
        ));

        let mut q = query("ALTER TABLE widgets ADD COLUMN c int");
        q.database = "other".to_string();
        assert!(!is_table_affecting_ddl(&q, "app"));
    }

    #[test]
    fn parses_single_rename_table() {
        assert_eq!(
            parse_table_renames("RENAME TABLE old_name TO new_name"),
            vec![TableRename {
                old: "old_name".into(),
                new: "new_name".into()
            }]
        );
    }

    #[test]
    fn parses_multi_rename_table() {
        assert_eq!(
            parse_table_renames("RENAME TABLE `a` TO `b`, c TO d"),
            vec![
                TableRename {
                    old: "a".into(),
                    new: "b".into()
                },
                TableRename {
                    old: "c".into(),
                    new: "d".into()
                },
            ]
        );
    }

    #[test]
    fn parses_db_qualified_rename() {
        assert_eq!(
            parse_table_renames("RENAME TABLE app.old TO app.new"),
            vec![TableRename {
                old: "old".into(),
                new: "new".into()
            }]
        );
    }

    #[test]
    fn parses_alter_rename_to() {
        assert_eq!(
            parse_table_renames("ALTER TABLE old RENAME TO new"),
            vec![TableRename {
                old: "old".into(),
                new: "new".into()
            }]
        );
        assert_eq!(
            parse_table_renames("ALTER TABLE old RENAME new"),
            vec![TableRename {
                old: "old".into(),
                new: "new".into()
            }]
        );
    }

    #[test]
    fn non_rename_ddl_yields_no_pairs() {
        assert!(parse_table_renames("ALTER TABLE widgets ADD COLUMN c int").is_empty());
        assert!(parse_table_renames("CREATE TABLE t (id int)").is_empty());
        assert!(parse_table_renames("DROP TABLE t").is_empty());
    }

    #[test]
    fn alter_rename_column_is_not_a_table_rename() {
        assert!(parse_table_renames("ALTER TABLE t RENAME COLUMN a TO b").is_empty());
        assert!(parse_table_renames("ALTER TABLE t RENAME INDEX i TO j").is_empty());
    }
}
