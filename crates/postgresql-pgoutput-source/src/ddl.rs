//! DDL handling for pgoutput Relation and Truncate events.

use pgoutput_protocol::RelationMeta;

/// A table rename detected from consecutive Relation events on the same OID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableRename {
    pub old: String,
    pub new: String,
}

/// Detect renames when a Relation event arrives for a known OID with a new name.
pub(crate) fn detect_relation_renames(
    cache: &std::collections::HashMap<u32, RelationMeta>,
    meta: &RelationMeta,
) -> Vec<TableRename> {
    let Some(prev) = cache.get(&meta.relation_oid) else {
        return Vec::new();
    };
    if prev.table == meta.table {
        return Vec::new();
    }
    vec![TableRename {
        old: prev.table.clone(),
        new: meta.table.clone(),
    }]
}

/// Whether a truncate event affects any of the synced tables.
pub(crate) fn truncate_affects_synced(tables: &[String], synced: &[String]) -> bool {
    tables.iter().any(|t| synced.iter().any(|s| s == t))
}

/// When the synced table filter still lists an old name, detect a rename from
/// the first Relation event that arrives under the new name.
pub(crate) fn detect_relation_rename_from_filter(
    filter: &Option<Vec<String>>,
    meta: &RelationMeta,
) -> Option<TableRename> {
    let tables = filter.as_ref()?;
    if tables.contains(&meta.table) {
        return None;
    }
    if tables.len() == 1 {
        return Some(TableRename {
            old: tables[0].clone(),
            new: meta.table.clone(),
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_walstream::ReplicaIdentity;

    fn meta(oid: u32, table: &str) -> RelationMeta {
        RelationMeta {
            relation_oid: oid,
            schema: "public".into(),
            table: table.into(),
            columns: vec![],
            replica_identity: ReplicaIdentity::Default,
        }
    }

    #[test]
    fn detects_rename_from_relation_oid() {
        let mut cache = std::collections::HashMap::new();
        cache.insert(42, meta(42, "widgets"));
        let renames = detect_relation_renames(&cache, &meta(42, "widgets_v2"));
        assert_eq!(
            renames,
            vec![TableRename {
                old: "widgets".into(),
                new: "widgets_v2".into()
            }]
        );
    }

    #[test]
    fn first_relation_is_not_rename() {
        let cache = std::collections::HashMap::new();
        assert!(detect_relation_renames(&cache, &meta(1, "widgets")).is_empty());
    }
}
