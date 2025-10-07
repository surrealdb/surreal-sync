//! Pre-defined test schemas for commonly used tables
//!
//! Provides ready-to-use TestSchema definitions for standard test tables
//! to enable empty table creation for checkpoint generation.

use crate::testing::schema::{
    ColumnDef, IndexDef, MongoDBSchema, MongoIndexDef, MySQLSchema, Neo4jConstraintDef,
    Neo4jIndexDef, Neo4jIndexType, Neo4jSchema, PostgreSQLSchema, PropertyDef, TestSchema,
};

/// Create schema for the all_types_users test table
pub fn create_all_types_users_schema() -> TestSchema {
    TestSchema {
        postgresql: PostgreSQLSchema {
            columns: vec![
                ColumnDef::new("user_id", "TEXT"),
                ColumnDef::new("name", "VARCHAR(255)").not_null(),
                ColumnDef::new("email", "VARCHAR(255)").unique(),
                ColumnDef::new("age", "INTEGER"),
                ColumnDef::new("active", "BOOLEAN").default("true"),
                ColumnDef::new("account_balance", "NUMERIC(19, 5)"),
                ColumnDef::new("metadata", "JSONB"),
                ColumnDef::new("score", "DOUBLE PRECISION"),
                ColumnDef::new("created_at", "TIMESTAMPTZ").default("CURRENT_TIMESTAMP"),
                ColumnDef::new("reference_id", "TEXT"),
            ],
            primary_key: Some("user_id".to_string()),
            indexes: vec![
                IndexDef::new("idx_users_email", vec!["email".to_string()]).unique(),
                IndexDef::new("idx_users_active", vec!["active".to_string()]),
            ],
            constraints: vec![],
        },
        mysql: MySQLSchema {
            columns: vec![
                ColumnDef::new("id", "VARCHAR(255)"),
                ColumnDef::new("name", "VARCHAR(255)").not_null(),
                ColumnDef::new("email", "VARCHAR(255)").unique(),
                ColumnDef::new("age", "INT"),
                ColumnDef::new("active", "TINYINT(1)").default("1"),
                ColumnDef::new("account_balance", "DECIMAL(19, 5)"),
                ColumnDef::new("score", "DOUBLE"),
                ColumnDef::new("metadata", "JSON"),
                ColumnDef::new("created_at", "TIMESTAMP").default("CURRENT_TIMESTAMP"),
                ColumnDef::new("reference_id", "VARCHAR(255)"),
            ],
            primary_key: Some("id".to_string()),
            indexes: vec![
                IndexDef::new("idx_users_email", vec!["email".to_string()]).unique(),
                IndexDef::new("idx_users_active", vec!["active".to_string()]),
            ],
            constraints: vec![],
            engine: "InnoDB".to_string(),
            charset: "utf8mb4".to_string(),
            collation: "utf8mb4_general_ci".to_string(),
        },
        mongodb: MongoDBSchema {
            collection_name: "all_types_users".to_string(),
            indexes: vec![
                MongoIndexDef::new("_id", true).unique(),
                MongoIndexDef::new("email", true).unique(),
                MongoIndexDef::new("active", true),
            ],
            validation_schema: None,
            capped: false,
            size: None,
            max: None,
        },
        neo4j: Neo4jSchema {
            node_labels: vec!["all_types_users".to_string()],
            relationship_types: vec![],
            properties: vec![
                PropertyDef::new("id", "String").required(),
                PropertyDef::new("name", "String").required(),
                PropertyDef::new("email", "String"),
                PropertyDef::new("age", "Integer"),
                PropertyDef::new("active", "Boolean"),
                PropertyDef::new("account_balance", "Float"),
                PropertyDef::new("score", "Float"),
                PropertyDef::new("created_at", "DateTime"),
                PropertyDef::new("tags", "List<String>"),
                PropertyDef::new("metadata", "String"),
            ],
            constraints: vec![
                Neo4jConstraintDef::unique("all_types_users", "id"),
                Neo4jConstraintDef::unique("all_types_users", "email"),
            ],
            indexes: vec![Neo4jIndexDef {
                name: "idx_user_active".to_string(),
                label: "all_types_users".to_string(),
                properties: vec!["active".to_string()],
                index_type: Neo4jIndexType::Range,
            }],
        },
    }
}

/// Create schema for the all_types_posts test table
pub fn create_all_types_posts_schema() -> TestSchema {
    TestSchema {
        postgresql: PostgreSQLSchema {
            columns: vec![
                ColumnDef::new("post_id", "TEXT"),
                ColumnDef::new("title", "TEXT").not_null(),
                ColumnDef::new("content", "TEXT"),
                ColumnDef::new("author_id", "TEXT").not_null(),
                ColumnDef::new("published", "BOOLEAN").default("false"),
                ColumnDef::new("view_count", "BIGINT").default("0"),
                ColumnDef::new("post_categories", "TEXT[]"),
                ColumnDef::new("rating", "NUMERIC(3, 2)"),
                ColumnDef::new("created_at", "TIMESTAMPTZ").default("CURRENT_TIMESTAMP"),
                ColumnDef::new("updated_at", "TIMESTAMPTZ"),
                ColumnDef::new("location", "POINT"),
            ],
            primary_key: Some("post_id".to_string()),
            indexes: vec![
                IndexDef::new("idx_posts_author", vec!["author_id".to_string()]),
                IndexDef::new("idx_posts_published", vec!["published".to_string()]),
            ],
            constraints: vec![],
        },
        mysql: MySQLSchema {
            columns: vec![
                ColumnDef::new("id", "VARCHAR(255)"),
                ColumnDef::new("title", "TEXT").not_null(),
                ColumnDef::new("content", "TEXT"),
                ColumnDef::new("author_id", "VARCHAR(255)").not_null(),
                ColumnDef::new("published", "TINYINT(1)").default("0"),
                ColumnDef::new("view_count", "BIGINT").default("0"),
                ColumnDef::new("rating", "DECIMAL(3, 2)"),
                ColumnDef::new("created_at", "TIMESTAMP").default("CURRENT_TIMESTAMP"),
                ColumnDef::new("updated_at", "DATETIME"),
                ColumnDef::new(
                    "post_categories",
                    "SET('technology', 'tutorial', 'news', 'opinion')",
                ),
            ],
            primary_key: Some("id".to_string()),
            indexes: vec![
                IndexDef::new("idx_posts_author", vec!["author_id".to_string()]),
                IndexDef::new("idx_posts_published", vec!["published".to_string()]),
            ],
            constraints: vec![],
            engine: "InnoDB".to_string(),
            charset: "utf8mb4".to_string(),
            collation: "utf8mb4_general_ci".to_string(),
        },
        mongodb: MongoDBSchema {
            collection_name: "all_types_posts".to_string(),
            indexes: vec![
                MongoIndexDef::new("_id", true).unique(),
                MongoIndexDef::new("author_id", true),
                MongoIndexDef::new("published", true),
            ],
            validation_schema: None,
            capped: false,
            size: None,
            max: None,
        },
        neo4j: Neo4jSchema {
            node_labels: vec!["all_types_posts".to_string()],
            relationship_types: vec!["AUTHORED_BY".to_string()],
            properties: vec![
                PropertyDef::new("id", "String").required(),
                PropertyDef::new("title", "String").required(),
                PropertyDef::new("content", "String"),
                PropertyDef::new("author_id", "String").required(),
                PropertyDef::new("published", "Boolean"),
                PropertyDef::new("view_count", "Integer"),
                PropertyDef::new("rating", "Float"),
                PropertyDef::new("created_at", "DateTime"),
                PropertyDef::new("updated_at", "DateTime"),
                PropertyDef::new("post_categories", "String"),
            ],
            constraints: vec![Neo4jConstraintDef::unique("all_types_posts", "id")],
            indexes: vec![
                Neo4jIndexDef {
                    name: "idx_post_author".to_string(),
                    label: "all_types_posts".to_string(),
                    properties: vec!["author_id".to_string()],
                    index_type: Neo4jIndexType::Range,
                },
                Neo4jIndexDef {
                    name: "idx_post_published".to_string(),
                    label: "all_types_posts".to_string(),
                    properties: vec!["published".to_string()],
                    index_type: Neo4jIndexType::Range,
                },
            ],
        },
    }
}
