//! Unit tests: type reject list, naming, ADO.NET parse, ids, LSN order.

use std::collections::HashMap;
use surreal_sync_core::{Checkpoint, Type, Value};
use surreal_sync_mssql::from_mssql::{
    detect_collisions, parse_table_ref, record_id, target_table_name, version_id, MssqlCheckpoint,
    MssqlLsn, QualifiedName, TableSyncKind,
};
use surreal_sync_mssql::types::mssql_column_to_universal_type;

#[test]
fn rejects_lossy_sql_server_types() {
    for name in [
        "sql_variant",
        "hierarchyid",
        "geography",
        "geometry",
        "cursor",
        "table",
        "rowversion",
        "timestamp",
        "udt",
        "MyClrUdt",
    ] {
        assert!(
            mssql_column_to_universal_type(name, None, None, None, None).is_err(),
            "{name} should be rejected"
        );
    }
}

#[test]
fn maps_supported_types() {
    assert_eq!(
        mssql_column_to_universal_type("bit", None, None, None, None).unwrap(),
        Type::Bool
    );
    assert_eq!(
        mssql_column_to_universal_type("tinyint", None, None, None, None).unwrap(),
        Type::Int16
    );
    assert_eq!(
        mssql_column_to_universal_type("datetimeoffset", None, None, None, None).unwrap(),
        Type::ZonedDateTime
    );
    assert_eq!(
        mssql_column_to_universal_type("uniqueidentifier", None, None, None, None).unwrap(),
        Type::Uuid
    );
    assert_eq!(
        mssql_column_to_universal_type("sysname", None, None, None, None).unwrap(),
        Type::VarChar { length: 128 }
    );
}

#[test]
fn naming_and_collisions() {
    let a = parse_table_ref("Article").unwrap();
    assert_eq!(a, QualifiedName::new("dbo", "Article"));
    assert_eq!(target_table_name(&a), "Article");
    let b = parse_table_ref("[sales].[Order]").unwrap();
    assert_eq!(target_table_name(&b), "sales__Order");
    detect_collisions(&[a, b]).unwrap();
    let err = detect_collisions(&[
        QualifiedName::new("dbo", "sales__Order"),
        QualifiedName::new("sales", "Order"),
    ])
    .unwrap_err()
    .to_string();
    assert!(err.contains("sales__Order"), "{err}");
}

#[test]
fn ado_net_parse_sql_auth_and_integrated() {
    tiberius::Config::from_ado_string(
        "Server=tcp:localhost,1433;User=sa;Password=Surreal_Sync1;Database=App;\
         TrustServerCertificate=true;Encrypt=true",
    )
    .expect("SQL auth ADO.NET string");
    tiberius::Config::from_ado_string(
        "Server=tcp:localhost,1433;IntegratedSecurity=true;Database=App;\
         TrustServerCertificate=true;Encrypt=true",
    )
    .expect("IntegratedSecurity ADO.NET string");
}

#[test]
fn table_sync_kind_regular_vs_temporal() {
    assert_ne!(TableSyncKind::Regular, TableSyncKind::Temporal);
}

#[test]
fn regular_vs_temporal_ids() {
    let pk = vec![Value::Int32(1)];
    let regular = record_id(pk.clone());
    assert_eq!(regular, Value::Int32(1));

    let mut fields = HashMap::new();
    fields.insert("title".into(), Value::Text("hello".into()));
    let v1 = version_id(
        pk.clone(),
        "2020-01-01T00:00:00Z",
        "9999-12-31T00:00:00Z",
        &fields,
        0,
    );
    let v2 = version_id(
        pk.clone(),
        "2020-01-01T00:00:00Z",
        "9999-12-31T00:00:00Z",
        &fields,
        0,
    );
    assert_eq!(v1, v2);
    let v3 = version_id(
        pk,
        "2020-01-01T00:00:00Z",
        "9999-12-31T00:00:00Z",
        &fields,
        1,
    );
    assert_ne!(v1, v3);
}

#[test]
fn mssql_lsn_order_and_cli() {
    let a = MssqlLsn::from_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1]).unwrap();
    let b = MssqlLsn::from_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 2]).unwrap();
    assert!(a < b);
    let cp = MssqlCheckpoint::new(b.clone());
    let decoded = MssqlCheckpoint::from_cli_string(&cp.to_cli_string()).unwrap();
    assert_eq!(decoded.lsn, b);
    let prefixed = MssqlCheckpoint::from_cli_string(&format!("mssql:{}", b.to_hex())).unwrap();
    assert_eq!(prefixed.lsn, b);
}
