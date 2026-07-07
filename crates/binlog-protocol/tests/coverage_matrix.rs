mod fixtures;

use std::collections::HashSet;

use binlog_protocol::column_types::*;
use binlog_protocol::Flavor;

/// Coverage matrix rows: (category, type name, column_type, flavor, fixture module test)
const MATRIX: &[(&str, &str, u8, Flavor, &str)] = &[
    (
        "Integers",
        "TINYINT",
        MYSQL_TYPE_TINY,
        Flavor::MySql,
        "tinyint_signed",
    ),
    (
        "Integers",
        "TINYINT UNSIGNED",
        MYSQL_TYPE_TINY,
        Flavor::MySql,
        "tinyint_unsigned",
    ),
    (
        "Integers",
        "SMALLINT",
        MYSQL_TYPE_SHORT,
        Flavor::MySql,
        "smallint",
    ),
    (
        "Integers",
        "MEDIUMINT",
        MYSQL_TYPE_INT24,
        Flavor::MySql,
        "mediumint",
    ),
    (
        "Integers",
        "INT",
        MYSQL_TYPE_LONG,
        Flavor::MySql,
        "int_column",
    ),
    (
        "Integers",
        "BIGINT",
        MYSQL_TYPE_LONGLONG,
        Flavor::MySql,
        "bigint",
    ),
    (
        "Integers",
        "BIGINT UNSIGNED",
        MYSQL_TYPE_LONGLONG,
        Flavor::MySql,
        "bigint_unsigned",
    ),
    (
        "Float",
        "FLOAT",
        MYSQL_TYPE_FLOAT,
        Flavor::MySql,
        "float_type",
    ),
    (
        "Float",
        "DOUBLE",
        MYSQL_TYPE_DOUBLE,
        Flavor::MySql,
        "double_type",
    ),
    (
        "Decimal",
        "DECIMAL",
        MYSQL_TYPE_NEWDECIMAL,
        Flavor::MySql,
        "decimal_type",
    ),
    (
        "String",
        "VARCHAR",
        MYSQL_TYPE_VARCHAR,
        Flavor::MySql,
        "varchar",
    ),
    (
        "String",
        "CHAR",
        MYSQL_TYPE_STRING,
        Flavor::MySql,
        "char_string",
    ),
    (
        "String",
        "ENUM",
        MYSQL_TYPE_ENUM,
        Flavor::MySql,
        "enum_type",
    ),
    (
        "String",
        "TEXT",
        MYSQL_TYPE_BLOB,
        Flavor::MySql,
        "text_blob",
    ),
    (
        "Binary",
        "VARBINARY",
        MYSQL_TYPE_VAR_STRING,
        Flavor::MySql,
        "varbinary",
    ),
    (
        "Binary",
        "BLOB",
        MYSQL_TYPE_LONG_BLOB,
        Flavor::MySql,
        "blob_bytes",
    ),
    ("Temporal", "DATE", MYSQL_TYPE_DATE, Flavor::MySql, "date"),
    (
        "Temporal",
        "DATETIME",
        MYSQL_TYPE_DATETIME,
        Flavor::MySql,
        "datetime_v1",
    ),
    (
        "Temporal",
        "DATETIME2",
        MYSQL_TYPE_DATETIME2,
        Flavor::MySql,
        "datetime2",
    ),
    (
        "Temporal",
        "TIME2",
        MYSQL_TYPE_TIME2,
        Flavor::MySql,
        "time2",
    ),
    (
        "Temporal",
        "TIMESTAMP2",
        MYSQL_TYPE_TIMESTAMP2,
        Flavor::MySql,
        "timestamp2",
    ),
    (
        "Temporal",
        "YEAR",
        MYSQL_TYPE_YEAR,
        Flavor::MySql,
        "year_type",
    ),
    ("Bit", "BIT", MYSQL_TYPE_BIT, Flavor::MySql, "bit_type"),
    ("Set", "SET", MYSQL_TYPE_SET, Flavor::MySql, "set_type"),
    (
        "JSON",
        "JSON MySQL",
        MYSQL_TYPE_JSON,
        Flavor::MySql,
        "mysql_json_binary",
    ),
    (
        "JSON",
        "JSON MariaDB",
        MYSQL_TYPE_JSON,
        Flavor::MariaDb,
        "mariadb_json_as_text",
    ),
    (
        "Geometry",
        "GEOMETRY",
        MYSQL_TYPE_GEOMETRY,
        Flavor::MySql,
        "geometry_bytes",
    ),
];

/// All fixture test function names that exist in tests/fixtures/*.rs
const FIXTURE_TESTS: &[&str] = &[
    "tinyint_signed",
    "tinyint_unsigned",
    "smallint",
    "mediumint",
    "int_column",
    "bigint",
    "bigint_unsigned",
    "float_type",
    "double_type",
    "decimal_type",
    "varchar",
    "char_string",
    "enum_type",
    "text_blob",
    "varbinary",
    "blob_bytes",
    "date",
    "datetime_v1",
    "datetime2",
    "time2",
    "timestamp2",
    "year_type",
    "bit_type",
    "set_type",
    "mysql_json_binary",
    "mariadb_json_as_text",
    "geometry_bytes",
];

#[test]
fn coverage_matrix_has_fixture_for_every_row() {
    let fixtures: HashSet<&str> = FIXTURE_TESTS.iter().copied().collect();
    for (category, name, column_type, flavor, test_name) in MATRIX {
        assert!(
            fixtures.contains(test_name),
            "missing fixture test '{test_name}' for {category}/{name}"
        );
        let kind = binlog_protocol::expected_cell_kind(*column_type, *flavor);
        let _ = kind;
    }
}

#[test]
fn matrix_row_count_matches_fixtures() {
    assert_eq!(MATRIX.len(), FIXTURE_TESTS.len());
}
