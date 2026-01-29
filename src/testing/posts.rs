//! Posts table test data
//!
//! Post data with all database-specific type representations

use crate::testing::{
    field::Field,
    mongodb::MongoDBField,
    mysql::MySQLField,
    neo4j::bolt,
    neo4j::Neo4jField,
    postgresql::PostgreSQLField,
    table::{TestDoc, TestTable},
    value::{MongoDBValue, MySQLValue, PostgreSQLValue, SurrealDBValue},
};
use chrono::Utc;

/// Create the posts table with type coverage
pub fn create_posts_table() -> TestTable {
    use crate::testing::schemas_for_tests::create_all_types_posts_schema;
    TestTable::new(
        "all_types_posts",
        create_all_types_posts_schema(),
        vec![
            TestDoc::new(vec![
                Field::simple(
                    "id",
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
                })
                .with_mysql(MySQLField {
                    column_name: "id".to_string(),
                    column_value: MySQLValue::Varchar("post_001".to_string()),
                    data_type: "VARCHAR(255)".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "_id".to_string(),
                    field_value: MongoDBValue::String("post_001".to_string()),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "id".to_string(),
                    property_value: bolt::string("post_001"),
                }),
                Field::simple(
                    "author_id",
                    SurrealDBValue::Thing {
                        table: "all_types_users".to_string(),
                        id: Box::new(SurrealDBValue::String("user_001".to_string())),
                    },
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "author_id".to_string(),
                    column_value: PostgreSQLValue::Text("user_001".to_string()),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "author_id".to_string(),
                    column_value: MySQLValue::Varchar("user_001".to_string()),
                    data_type: "VARCHAR(255)".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "author_id".to_string(),
                    field_value: MongoDBValue::String("user_001".to_string()),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "author_id".to_string(),
                    property_value: bolt::string("user_001"),
                }),
                Field::simple(
                    "title",
                    SurrealDBValue::String("Database Testing".to_string()),
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "title".to_string(),
                    column_value: PostgreSQLValue::Varchar("Database Testing".to_string()),
                    data_type: "VARCHAR".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "title".to_string(),
                    column_value: MySQLValue::Varchar("Database Testing".to_string()),
                    data_type: "VARCHAR".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "title".to_string(),
                    field_value: MongoDBValue::String("Database Testing".to_string()),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "title".to_string(),
                    property_value: bolt::string("Database Testing"),
                }),
                Field::simple(
                    "content",
                    SurrealDBValue::String(
                        "This post tests complex data types across databases".to_string(),
                    ),
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "content".to_string(),
                    column_value: PostgreSQLValue::Text(
                        "This post tests complex data types across databases".to_string(),
                    ),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "content".to_string(),
                    column_value: MySQLValue::Text(
                        "This post tests complex data types across databases".to_string(),
                    ),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "content".to_string(),
                    field_value: MongoDBValue::String(
                        "This post tests complex data types across databases".to_string(),
                    ),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "content".to_string(),
                    property_value: bolt::string(
                        "This post tests complex data types across databases",
                    ),
                }),
                Field::simple("view_count", SurrealDBValue::Int64(150))
                    .with_postgresql(PostgreSQLField {
                        column_name: "view_count".to_string(),
                        column_value: PostgreSQLValue::BigInt(150),
                        data_type: "BIGINT".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "view_count".to_string(),
                        column_value: MySQLValue::BigInt(150),
                        data_type: "BIGINT".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "view_count".to_string(),
                        field_value: MongoDBValue::Int64(150),
                        bson_type: "long".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "view_count".to_string(),
                        property_value: bolt::int(150),
                    }),
                Field::simple("published", SurrealDBValue::Bool(true))
                    .with_postgresql(PostgreSQLField {
                        column_name: "published".to_string(),
                        column_value: PostgreSQLValue::Bool(true),
                        data_type: "BOOLEAN".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "published".to_string(),
                        column_value: MySQLValue::TinyInt(1),
                        data_type: "TINYINT".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "published".to_string(),
                        field_value: MongoDBValue::Bool(true),
                        bson_type: "bool".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "published".to_string(),
                        property_value: bolt::bool(true),
                    }),
                // MongoDB RegularExpression field (database-specific)
                // MongoDB regex is converted to SurrealDB/Rust regex format: (?flags)pattern
                Field::simple(
                    "content_pattern",
                    SurrealDBValue::String("(?i)^[A-Za-z0-9\\s]+$".to_string()),
                )
                .with_mongodb(MongoDBField {
                    field_name: "content_pattern".to_string(),
                    field_value: MongoDBValue::RegularExpression {
                        pattern: "^[A-Za-z0-9\\s]+$".to_string(),
                        flags: "i".to_string(),
                    },
                    bson_type: "regex".to_string(),
                }),
                // MySQL SET field (database-specific)
                Field::simple(
                    "post_categories",
                    SurrealDBValue::Array(vec![
                        SurrealDBValue::String("technology".to_string()),
                        SurrealDBValue::String("tutorial".to_string()),
                    ]),
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "post_categories".to_string(),
                    column_value: PostgreSQLValue::Array(vec![
                        PostgreSQLValue::Text("technology".to_string()),
                        PostgreSQLValue::Text("tutorial".to_string()),
                    ]),
                    data_type: "TEXT[]".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "post_categories".to_string(),
                    column_value: MySQLValue::Set(vec![
                        "technology".to_string(),
                        "tutorial".to_string(),
                    ]),
                    data_type: "SET".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: Some(vec![
                        "technology".to_string(),
                        "tutorial".to_string(),
                        "news".to_string(),
                        "opinion".to_string(),
                    ]),
                })
                .with_mongodb(MongoDBField {
                    field_name: "post_categories".to_string(),
                    field_value: MongoDBValue::Array(vec![
                        MongoDBValue::String("technology".to_string()),
                        MongoDBValue::String("tutorial".to_string()),
                    ]),
                    bson_type: "array".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "post_categories".to_string(),
                    property_value: bolt::string("[\"technology\",\"tutorial\"]"),
                }),
                Field::simple("created_at", SurrealDBValue::DateTime(Utc::now()))
                    .with_postgresql(PostgreSQLField {
                        column_name: "created_at".to_string(),
                        column_value: PostgreSQLValue::TimestampTz(Utc::now()),
                        data_type: "TIMESTAMPTZ".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "created_at".to_string(),
                        column_value: MySQLValue::Timestamp(Utc::now().naive_utc()),
                        data_type: "TIMESTAMP".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "created_at".to_string(),
                        field_value: MongoDBValue::DateTime(Utc::now()),
                        bson_type: "date".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "created_at".to_string(),
                        property_value: bolt::datetime(Utc::now()),
                    }),
                Field::simple("updated_at", SurrealDBValue::DateTime(Utc::now()))
                    .with_postgresql(PostgreSQLField {
                        column_name: "updated_at".to_string(),
                        column_value: PostgreSQLValue::TimestampTz(Utc::now()),
                        data_type: "TIMESTAMPTZ".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "updated_at".to_string(),
                        column_value: MySQLValue::Timestamp(Utc::now().naive_utc()),
                        data_type: "TIMESTAMP".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "updated_at".to_string(),
                        field_value: MongoDBValue::DateTime(Utc::now()),
                        bson_type: "date".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "updated_at".to_string(),
                        property_value: bolt::datetime(Utc::now()),
                    }),
            ]),
            // Second post
            TestDoc::new(vec![
                Field::simple(
                    "id",
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
                })
                .with_mysql(MySQLField {
                    column_name: "id".to_string(),
                    column_value: MySQLValue::Varchar("post_002".to_string()),
                    data_type: "VARCHAR(255)".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "_id".to_string(),
                    field_value: MongoDBValue::String("post_002".to_string()),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "id".to_string(),
                    property_value: bolt::string("post_002"),
                }),
                Field::simple(
                    "author_id",
                    SurrealDBValue::Thing {
                        table: "all_types_users".to_string(),
                        id: Box::new(SurrealDBValue::String("user_002".to_string())),
                    },
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "author_id".to_string(),
                    column_value: PostgreSQLValue::Text("user_002".to_string()),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "author_id".to_string(),
                    column_value: MySQLValue::Varchar("user_002".to_string()),
                    data_type: "VARCHAR(255)".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "author_id".to_string(),
                    field_value: MongoDBValue::String("user_002".to_string()),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "author_id".to_string(),
                    property_value: bolt::string("user_002"),
                }),
                Field::simple(
                    "title",
                    SurrealDBValue::String("Advanced Sync Patterns".to_string()),
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "title".to_string(),
                    column_value: PostgreSQLValue::Varchar("Advanced Sync Patterns".to_string()),
                    data_type: "VARCHAR".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "title".to_string(),
                    column_value: MySQLValue::Varchar("Advanced Sync Patterns".to_string()),
                    data_type: "VARCHAR".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "title".to_string(),
                    field_value: MongoDBValue::String("Advanced Sync Patterns".to_string()),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "title".to_string(),
                    property_value: bolt::string("Advanced Sync Patterns"),
                }),
                Field::simple(
                    "content",
                    SurrealDBValue::String("Exploring incremental sync strategies".to_string()),
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "content".to_string(),
                    column_value: PostgreSQLValue::Text(
                        "Exploring incremental sync strategies".to_string(),
                    ),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "content".to_string(),
                    column_value: MySQLValue::Text(
                        "Exploring incremental sync strategies".to_string(),
                    ),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "content".to_string(),
                    field_value: MongoDBValue::String(
                        "Exploring incremental sync strategies".to_string(),
                    ),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "content".to_string(),
                    property_value: bolt::string("Exploring incremental sync strategies"),
                }),
                Field::simple("view_count", SurrealDBValue::Int64(89))
                    .with_postgresql(PostgreSQLField {
                        column_name: "view_count".to_string(),
                        column_value: PostgreSQLValue::BigInt(89),
                        data_type: "BIGINT".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "view_count".to_string(),
                        column_value: MySQLValue::BigInt(89),
                        data_type: "BIGINT".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "view_count".to_string(),
                        field_value: MongoDBValue::Int64(89),
                        bson_type: "long".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "view_count".to_string(),
                        property_value: bolt::int(89),
                    }),
                Field::simple("published", SurrealDBValue::Bool(false))
                    .with_postgresql(PostgreSQLField {
                        column_name: "published".to_string(),
                        column_value: PostgreSQLValue::Bool(false),
                        data_type: "BOOLEAN".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "published".to_string(),
                        column_value: MySQLValue::TinyInt(0),
                        data_type: "TINYINT".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "published".to_string(),
                        field_value: MongoDBValue::Bool(false),
                        bson_type: "bool".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "published".to_string(),
                        property_value: bolt::bool(false),
                    }),
                // MongoDB RegularExpression field (database-specific)
                // MongoDB regex is converted to SurrealDB/Rust regex format: (?)pattern for empty flags
                Field::simple(
                    "content_pattern",
                    SurrealDBValue::String("(?)^\\w+$".to_string()),
                )
                .with_mongodb(MongoDBField {
                    field_name: "content_pattern".to_string(),
                    field_value: MongoDBValue::RegularExpression {
                        pattern: "^\\w+$".to_string(),
                        flags: "".to_string(),
                    },
                    bson_type: "regex".to_string(),
                }),
                // MySQL SET field (database-specific)
                Field::simple(
                    "post_categories",
                    SurrealDBValue::Array(vec![SurrealDBValue::String("news".to_string())]),
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "post_categories".to_string(),
                    column_value: PostgreSQLValue::Array(vec![PostgreSQLValue::Text(
                        "news".to_string(),
                    )]),
                    data_type: "TEXT[]".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "post_categories".to_string(),
                    column_value: MySQLValue::Set(vec!["news".to_string()]),
                    data_type: "SET".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: Some(vec![
                        "technology".to_string(),
                        "tutorial".to_string(),
                        "news".to_string(),
                        "opinion".to_string(),
                    ]),
                })
                .with_mongodb(MongoDBField {
                    field_name: "post_categories".to_string(),
                    field_value: MongoDBValue::Array(vec![MongoDBValue::String(
                        "news".to_string(),
                    )]),
                    bson_type: "array".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "post_categories".to_string(),
                    property_value: bolt::string("[\"news\"]"),
                }),
                Field::simple("created_at", SurrealDBValue::DateTime(Utc::now()))
                    .with_postgresql(PostgreSQLField {
                        column_name: "created_at".to_string(),
                        column_value: PostgreSQLValue::TimestampTz(Utc::now()),
                        data_type: "TIMESTAMPTZ".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "created_at".to_string(),
                        column_value: MySQLValue::Timestamp(Utc::now().naive_utc()),
                        data_type: "TIMESTAMP".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "created_at".to_string(),
                        field_value: MongoDBValue::DateTime(Utc::now()),
                        bson_type: "date".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "created_at".to_string(),
                        property_value: bolt::datetime(Utc::now()),
                    }),
                Field::simple("updated_at", SurrealDBValue::DateTime(Utc::now()))
                    .with_postgresql(PostgreSQLField {
                        column_name: "updated_at".to_string(),
                        column_value: PostgreSQLValue::TimestampTz(Utc::now()),
                        data_type: "TIMESTAMPTZ".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "updated_at".to_string(),
                        column_value: MySQLValue::Timestamp(Utc::now().naive_utc()),
                        data_type: "TIMESTAMP".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "updated_at".to_string(),
                        field_value: MongoDBValue::DateTime(Utc::now()),
                        bson_type: "date".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "updated_at".to_string(),
                        property_value: bolt::datetime(Utc::now()),
                    }),
            ]),
        ],
    )
}
