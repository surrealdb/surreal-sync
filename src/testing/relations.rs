//! Relations test data
//!
//! Defines relationships between tables for graph database testing

use crate::testing::postgresql::PostgreSQLField;
use crate::testing::value::PostgreSQLValue;
use crate::testing::{
    field::Field,
    schema::{
        ColumnDef, IndexDef, MongoDBSchema, MongoIndexDef, MySQLSchema, Neo4jConstraintDef,
        Neo4jIndexDef, Neo4jIndexType, Neo4jSchema, PostgreSQLSchema, PropertyDef, TestSchema,
    },
    table::{TestDoc, TestTable},
    value::SurrealDBValue,
};
use chrono::{SubsecRound, Utc};

/// Create the user-post relation
pub fn create_user_post_relation() -> TestTable {
    let ts = Utc::now().trunc_subsecs(1);

    TestTable::new(
        "authored_by",
        authored_by_schema(),
        vec![
            TestDoc::new(vec![
                Field::simple(
                    "in",
                    SurrealDBValue::Thing {
                        table: "all_types_users".to_string(),
                        id: Box::new(SurrealDBValue::String("user_001".to_string())),
                    },
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "user_id".to_string(),
                    column_value: PostgreSQLValue::Text("user_001".to_string()),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                }),
                Field::simple(
                    "out",
                    SurrealDBValue::Thing {
                        table: "all_types_posts".to_string(),
                        id: Box::new(SurrealDBValue::String("post_001".to_string())),
                    },
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "post_id".to_string(),
                    column_value: PostgreSQLValue::Text("post_001".to_string()),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                }),
                Field::simple("relationship_created", SurrealDBValue::DateTime(ts))
                    .with_postgresql(PostgreSQLField {
                        column_name: "relationship_created".to_string(),
                        column_value: PostgreSQLValue::TimestampTz(ts),
                        data_type: "TIMESTAMPTZ".to_string(),
                        precision: None,
                        scale: None,
                    }),
            ]),
            TestDoc::new(vec![
                Field::simple(
                    "in",
                    SurrealDBValue::Thing {
                        table: "all_types_users".to_string(),
                        id: Box::new(SurrealDBValue::String("user_002".to_string())),
                    },
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "user_id".to_string(),
                    column_value: PostgreSQLValue::Text("user_002".to_string()),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                }),
                Field::simple(
                    "out",
                    SurrealDBValue::Thing {
                        table: "all_types_posts".to_string(),
                        id: Box::new(SurrealDBValue::String("post_002".to_string())),
                    },
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "post_id".to_string(),
                    column_value: PostgreSQLValue::Text("post_002".to_string()),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                }),
                Field::simple("relationship_created", SurrealDBValue::DateTime(ts))
                    .with_postgresql(PostgreSQLField {
                        column_name: "relationship_created".to_string(),
                        column_value: PostgreSQLValue::TimestampTz(ts),
                        data_type: "TIMESTAMPTZ".to_string(),
                        precision: None,
                        scale: None,
                    }),
            ]),
        ],
    )
}

/// Create schema for the authored_by relation
pub fn authored_by_schema() -> TestSchema {
    TestSchema {
        postgresql: PostgreSQLSchema {
            columns: vec![
                ColumnDef::new("post_id", "TEXT").not_null(),
                ColumnDef::new("user_id", "TEXT").not_null(),
                ColumnDef::new("relationship_created", "TIMESTAMPTZ"),
            ],
            primary_key: Some("post_id".to_string()),
            indexes: vec![IndexDef::new(
                "idx_authored_by_user_id",
                vec!["user_id".to_string()],
            )],
            constraints: vec![],
        },
        mysql: MySQLSchema {
            columns: vec![
                ColumnDef::new("post_id", "VARCHAR(255)").not_null(),
                ColumnDef::new("user_id", "VARCHAR(255)").not_null(),
                ColumnDef::new("relationship_created", "DATETIME(6)"),
            ],
            primary_key: Some("post_id".to_string()),
            indexes: vec![IndexDef::new(
                "idx_authored_by_user_id",
                vec!["user_id".to_string()],
            )],
            constraints: vec![],
            engine: "InnoDB".to_string(),
            charset: "utf8mb4".to_string(),
            collation: "utf8mb4_unicode_ci".to_string(),
        },
        mongodb: MongoDBSchema {
            collection_name: "authored_by".to_string(),
            indexes: vec![
                MongoIndexDef::new("post_id", true).unique(),
                MongoIndexDef::new("user_id", false),
            ],
            validation_schema: None,
            capped: false,
            size: None,
            max: None,
        },
        neo4j: Neo4jSchema {
            node_labels: vec!["Post".to_string(), "User".to_string()],
            relationship_types: vec!["AUTHORED_BY".to_string()],
            properties: vec![
                PropertyDef::new("post_id", "String").required(),
                PropertyDef::new("user_id", "String").required(),
                PropertyDef::new("relationship_created", "DateTime"),
            ],
            constraints: vec![
                Neo4jConstraintDef::unique("Post", "post_id"),
                Neo4jConstraintDef::unique("User", "user_id"),
            ],
            indexes: vec![Neo4jIndexDef {
                name: "idx_authored_by_user_id".to_string(),
                label: "AUTHORED_BY".to_string(),
                properties: vec!["user_id".to_string()],
                index_type: Neo4jIndexType::Range,
            }],
        },
    }
}
