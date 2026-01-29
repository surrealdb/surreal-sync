//! Users table test data
//!
//! User data with all database-specific type representations

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
use std::collections::HashMap;

/// Create the users table with type coverage
pub fn create_users_table() -> TestTable {
    use crate::testing::schemas_for_tests::create_all_types_users_schema;
    TestTable::new(
        "all_types_users",
        create_all_types_users_schema(),
        vec![
            TestDoc::new(vec![
                // Universal ID field with different representations per database
                Field::simple(
                    "id",
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
                })
                .with_mysql(MySQLField {
                    column_name: "id".to_string(),
                    column_value: MySQLValue::Varchar("user_001".to_string()),
                    data_type: "VARCHAR(255)".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "_id".to_string(),
                    field_value: MongoDBValue::String("user_001".to_string()),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "id".to_string(),
                    property_value: bolt::string("user_001"),
                }),
                // High-precision decimal with different precision per database
                Field::simple(
                    "account_balance",
                    SurrealDBValue::Decimal {
                        value: "12345.67890".to_string(),
                        precision: Some(10),
                        scale: Some(5),
                    },
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "account_balance".to_string(),
                    column_value: PostgreSQLValue::Numeric {
                        value: "12345.67890".to_string(),
                        precision: Some(10),
                        scale: Some(5),
                    },
                    data_type: "NUMERIC".to_string(),
                    precision: Some(10),
                    scale: Some(5),
                })
                .with_mysql(MySQLField {
                    column_name: "account_balance".to_string(),
                    column_value: MySQLValue::Decimal {
                        value: "12345.67890".to_string(),
                        precision: Some(15),
                        scale: Some(5),
                    },
                    data_type: "DECIMAL".to_string(),
                    precision: Some(15),
                    scale: Some(5),
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "account_balance".to_string(),
                    field_value: MongoDBValue::Decimal128("12345.67890".to_string()),
                    bson_type: "decimal128".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "account_balance".to_string(),
                    property_value: bolt::float(12345.67890),
                }),
                // Complex Object field with database-specific representations
                Field::simple(
                    "metadata",
                    SurrealDBValue::Object(HashMap::from([
                        (
                            "preferences".to_string(),
                            SurrealDBValue::Object(HashMap::from([
                                (
                                    "theme".to_string(),
                                    SurrealDBValue::String("dark".to_string()),
                                ),
                                (
                                    "language".to_string(),
                                    SurrealDBValue::String("en".to_string()),
                                ),
                            ])),
                        ),
                        (
                            "tags".to_string(),
                            SurrealDBValue::Array(vec![
                                SurrealDBValue::String("premium".to_string()),
                                SurrealDBValue::String("verified".to_string()),
                            ]),
                        ),
                        (
                            "settings".to_string(),
                            SurrealDBValue::Object(HashMap::from([
                                ("notifications".to_string(), SurrealDBValue::Bool(true)),
                                (
                                    "privacy".to_string(),
                                    SurrealDBValue::String("strict".to_string()),
                                ),
                            ])),
                        ),
                    ])),
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "metadata".to_string(),
                    column_value: PostgreSQLValue::Jsonb(serde_json::json!({
                        "preferences": {"theme": "dark", "language": "en"},
                        "tags": ["premium", "verified"],
                        "settings": {"notifications": true, "privacy": "strict"}
                    })),
                    data_type: "JSONB".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "metadata".to_string(),
                    column_value: MySQLValue::Json(serde_json::json!({
                        "preferences": {"theme": "dark", "language": "en"},
                        "tags": ["premium", "verified"],
                        "settings": {"notifications": true, "privacy": "strict"}
                    })),
                    data_type: "JSON".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "metadata".to_string(),
                    field_value: MongoDBValue::Document(HashMap::from([
                        (
                            "preferences".to_string(),
                            MongoDBValue::Document(HashMap::from([
                                (
                                    "theme".to_string(),
                                    MongoDBValue::String("dark".to_string()),
                                ),
                                (
                                    "language".to_string(),
                                    MongoDBValue::String("en".to_string()),
                                ),
                            ])),
                        ),
                        (
                            "tags".to_string(),
                            MongoDBValue::Array(vec![
                                MongoDBValue::String("premium".to_string()),
                                MongoDBValue::String("verified".to_string()),
                            ]),
                        ),
                        (
                            "settings".to_string(),
                            MongoDBValue::Document(HashMap::from([
                                ("notifications".to_string(), MongoDBValue::Bool(true)),
                                (
                                    "privacy".to_string(),
                                    MongoDBValue::String("strict".to_string()),
                                ),
                            ])),
                        ),
                    ])),
                    bson_type: "object".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "metadata".to_string(),
                    // Neo4j doesn't support nested maps as node properties
                    // Store as JSON string representation
                    property_value: bolt::string(
                        r#"{"preferences":{"theme":"dark","language":"en"},"tags":["premium","verified"],"settings":{"notifications":true,"privacy":"strict"}}"#
                    ),
                }),
                // Database-specific field that only exists in MongoDB (JavaScript code with scope)
                // Field name matches MongoDB field name since sync preserves source field names
                // MongoDB JavaScript with scope is stored as object: {"$code": "...", "$scope": {...}}
                Field::simple(
                    "validator",
                    SurrealDBValue::Object(HashMap::from([
                        (
                            "$code".to_string(),
                            SurrealDBValue::String(
                                "function validate(input) { return input > threshold; }".to_string(),
                            ),
                        ),
                        (
                            "$scope".to_string(),
                            SurrealDBValue::Object(HashMap::from([(
                                "threshold".to_string(),
                                SurrealDBValue::Int64(100),
                            )])),
                        ),
                    ])),
                )
                .with_mongodb(MongoDBField {
                    field_name: "validator".to_string(),
                    field_value: MongoDBValue::JavaScript {
                        code: "function validate(input) { return input > threshold; }".to_string(),
                        scope: Some(HashMap::from([(
                            "threshold".to_string(),
                            MongoDBValue::Int32(100),
                        )])),
                    },
                    bson_type: "javascript".to_string(),
                }),
                // MongoDB ObjectId field to test ObjectId handling (stored as string in other DBs)
                Field::simple(
                    "reference_id",
                    SurrealDBValue::String("507f1f77bcf86cd799439011".to_string()),
                )
                .with_mongodb(MongoDBField {
                    field_name: "reference_id".to_string(),
                    field_value: MongoDBValue::ObjectId("507f1f77bcf86cd799439011".to_string()),
                    bson_type: "objectid".to_string(),
                })
                .with_postgresql(PostgreSQLField {
                    column_name: "reference_id".to_string(),
                    column_value: PostgreSQLValue::Text("507f1f77bcf86cd799439011".to_string()),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "reference_id".to_string(),
                    column_value: MySQLValue::Varchar("507f1f77bcf86cd799439011".to_string()),
                    data_type: "VARCHAR(255)".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_neo4j(Neo4jField {
                    property_name: "reference_id".to_string(),
                    property_value: bolt::string("507f1f77bcf86cd799439011"),
                }),
                // Common fields with explicit definitions for all databases
                Field::simple("name", SurrealDBValue::String("Alice Smith".to_string()))
                    .with_postgresql(PostgreSQLField {
                        column_name: "name".to_string(),
                        column_value: PostgreSQLValue::Text("Alice Smith".to_string()),
                        data_type: "TEXT".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "name".to_string(),
                        column_value: MySQLValue::Varchar("Alice Smith".to_string()),
                        data_type: "VARCHAR".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "name".to_string(),
                        field_value: MongoDBValue::String("Alice Smith".to_string()),
                        bson_type: "string".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "name".to_string(),
                        property_value: bolt::string("Alice Smith"),
                    }),
                Field::simple(
                    "email",
                    SurrealDBValue::String("alice@example.com".to_string()),
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "email".to_string(),
                    column_value: PostgreSQLValue::Varchar("alice@example.com".to_string()),
                    data_type: "VARCHAR".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "email".to_string(),
                    column_value: MySQLValue::Varchar("alice@example.com".to_string()),
                    data_type: "VARCHAR".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "email".to_string(),
                    field_value: MongoDBValue::String("alice@example.com".to_string()),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "email".to_string(),
                    property_value: bolt::string("alice@example.com"),
                }),
                Field::simple("age", SurrealDBValue::Int32(30))
                    .with_postgresql(PostgreSQLField {
                        column_name: "age".to_string(),
                        column_value: PostgreSQLValue::Int(30),
                        data_type: "INTEGER".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "age".to_string(),
                        column_value: MySQLValue::Int(30),
                        data_type: "INT".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "age".to_string(),
                        field_value: MongoDBValue::Int32(30),
                        bson_type: "int".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "age".to_string(),
                        property_value: bolt::int(30),
                    }),
                Field::simple("active", SurrealDBValue::Bool(true))
                    .with_postgresql(PostgreSQLField {
                        column_name: "active".to_string(),
                        column_value: PostgreSQLValue::Bool(true),
                        data_type: "BOOLEAN".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "active".to_string(),
                        column_value: MySQLValue::TinyInt(1),
                        data_type: "TINYINT".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "active".to_string(),
                        field_value: MongoDBValue::Bool(true),
                        bson_type: "bool".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "active".to_string(),
                        property_value: bolt::bool(true),
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
                Field::simple("score", SurrealDBValue::Float64(95.7))
                    .with_postgresql(PostgreSQLField {
                        column_name: "score".to_string(),
                        column_value: PostgreSQLValue::Double(95.7),
                        data_type: "DOUBLE PRECISION".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "score".to_string(),
                        column_value: MySQLValue::Double(95.7),
                        data_type: "DOUBLE".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "score".to_string(),
                        field_value: MongoDBValue::Float64(95.7),
                        bson_type: "double".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "score".to_string(),
                        property_value: bolt::float(95.7),
                    }),
            ]),
            // Second user with different values
            TestDoc::new(vec![
                Field::simple(
                    "id",
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
                })
                .with_mysql(MySQLField {
                    column_name: "id".to_string(),
                    column_value: MySQLValue::Varchar("user_002".to_string()),
                    data_type: "VARCHAR(255)".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "_id".to_string(),
                    field_value: MongoDBValue::String("user_002".to_string()),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "id".to_string(),
                    property_value: bolt::string("user_002"),
                }),
                Field::simple(
                    "account_balance",
                    SurrealDBValue::Decimal {
                        value: "98765.43210".to_string(),
                        precision: Some(10),
                        scale: Some(5),
                    },
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "account_balance".to_string(),
                    column_value: PostgreSQLValue::Numeric {
                        value: "98765.43210".to_string(),
                        precision: Some(10),
                        scale: Some(5),
                    },
                    data_type: "NUMERIC".to_string(),
                    precision: Some(10),
                    scale: Some(5),
                })
                .with_mysql(MySQLField {
                    column_name: "account_balance".to_string(),
                    column_value: MySQLValue::Decimal {
                        value: "98765.43210".to_string(),
                        precision: Some(15),
                        scale: Some(5),
                    },
                    data_type: "DECIMAL".to_string(),
                    precision: Some(15),
                    scale: Some(5),
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "account_balance".to_string(),
                    field_value: MongoDBValue::Decimal128("98765.43210".to_string()),
                    bson_type: "decimal128".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "account_balance".to_string(),
                    property_value: bolt::float(98765.43210),
                }),
                Field::simple(
                    "metadata",
                    SurrealDBValue::Object(HashMap::from([
                        (
                            "preferences".to_string(),
                            SurrealDBValue::Object(HashMap::from([
                                (
                                    "theme".to_string(),
                                    SurrealDBValue::String("light".to_string()),
                                ),
                                (
                                    "language".to_string(),
                                    SurrealDBValue::String("es".to_string()),
                                ),
                            ])),
                        ),
                        (
                            "tags".to_string(),
                            SurrealDBValue::Array(vec![
                                SurrealDBValue::String("standard".to_string()),
                                SurrealDBValue::String("user".to_string()),
                            ]),
                        ),
                        (
                            "settings".to_string(),
                            SurrealDBValue::Object(HashMap::from([
                                ("notifications".to_string(), SurrealDBValue::Bool(false)),
                                (
                                    "privacy".to_string(),
                                    SurrealDBValue::String("public".to_string()),
                                ),
                            ])),
                        ),
                    ])),
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "metadata".to_string(),
                    column_value: PostgreSQLValue::Jsonb(serde_json::json!({
                        "preferences": {"theme": "light", "language": "es"},
                        "tags": ["standard", "user"],
                        "settings": {"notifications": false, "privacy": "public"}
                    })),
                    data_type: "JSONB".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "metadata".to_string(),
                    column_value: MySQLValue::Json(serde_json::json!({
                        "preferences": {"theme": "light", "language": "es"},
                        "tags": ["standard", "user"],
                        "settings": {"notifications": false, "privacy": "public"}
                    })),
                    data_type: "JSON".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "metadata".to_string(),
                    field_value: MongoDBValue::Document(HashMap::from([
                        (
                            "preferences".to_string(),
                            MongoDBValue::Document(HashMap::from([
                                (
                                    "theme".to_string(),
                                    MongoDBValue::String("light".to_string()),
                                ),
                                (
                                    "language".to_string(),
                                    MongoDBValue::String("es".to_string()),
                                ),
                            ])),
                        ),
                        (
                            "tags".to_string(),
                            MongoDBValue::Array(vec![
                                MongoDBValue::String("standard".to_string()),
                                MongoDBValue::String("user".to_string()),
                            ]),
                        ),
                        (
                            "settings".to_string(),
                            MongoDBValue::Document(HashMap::from([
                                ("notifications".to_string(), MongoDBValue::Bool(false)),
                                (
                                    "privacy".to_string(),
                                    MongoDBValue::String("public".to_string()),
                                ),
                            ])),
                        ),
                    ])),
                    bson_type: "document".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "metadata".to_string(),
                    // Neo4j doesn't support nested maps as node properties
                    // Store as JSON string representation
                    property_value: bolt::string(
                        r#"{"preferences":{"theme":"light","language":"es"},"tags":["standard","user"],"settings":{"notifications":false,"privacy":"public"}}"#
                    ),
                }),
                // Field name matches MongoDB field name since sync preserves source field names
                Field::simple(
                    "validator",
                    SurrealDBValue::String("function isValid() { return true; }".to_string()),
                )
                .with_mongodb(MongoDBField {
                    field_name: "validator".to_string(),
                    field_value: MongoDBValue::JavaScript {
                        code: "function isValid() { return true; }".to_string(),
                        scope: None,
                    },
                    bson_type: "javascript".to_string(),
                }),
                // MongoDB ObjectId field to test ObjectId handling (stored as string in other DBs)
                Field::simple(
                    "reference_id",
                    SurrealDBValue::String("507f191e810c19729de860ea".to_string()),
                )
                .with_mongodb(MongoDBField {
                    field_name: "reference_id".to_string(),
                    field_value: MongoDBValue::ObjectId("507f191e810c19729de860ea".to_string()),
                    bson_type: "objectid".to_string(),
                })
                .with_postgresql(PostgreSQLField {
                    column_name: "reference_id".to_string(),
                    column_value: PostgreSQLValue::Text("507f191e810c19729de860ea".to_string()),
                    data_type: "TEXT".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "reference_id".to_string(),
                    column_value: MySQLValue::Varchar("507f191e810c19729de860ea".to_string()),
                    data_type: "VARCHAR(255)".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_neo4j(Neo4jField {
                    property_name: "reference_id".to_string(),
                    property_value: bolt::string("507f191e810c19729de860ea"),
                }),
                // Common fields with complete database definitions
                Field::simple("name", SurrealDBValue::String("Bob Johnson".to_string()))
                    .with_postgresql(PostgreSQLField {
                        column_name: "name".to_string(),
                        column_value: PostgreSQLValue::Text("Bob Johnson".to_string()),
                        data_type: "TEXT".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "name".to_string(),
                        column_value: MySQLValue::Varchar("Bob Johnson".to_string()),
                        data_type: "VARCHAR".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "name".to_string(),
                        field_value: MongoDBValue::String("Bob Johnson".to_string()),
                        bson_type: "string".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "name".to_string(),
                        property_value: bolt::string("Bob Johnson"),
                    }),
                Field::simple(
                    "email",
                    SurrealDBValue::String("bob@example.com".to_string()),
                )
                .with_postgresql(PostgreSQLField {
                    column_name: "email".to_string(),
                    column_value: PostgreSQLValue::Varchar("bob@example.com".to_string()),
                    data_type: "VARCHAR".to_string(),
                    precision: None,
                    scale: None,
                })
                .with_mysql(MySQLField {
                    column_name: "email".to_string(),
                    column_value: MySQLValue::Varchar("bob@example.com".to_string()),
                    data_type: "VARCHAR".to_string(),
                    precision: None,
                    scale: None,
                    enum_values: None,
                })
                .with_mongodb(MongoDBField {
                    field_name: "email".to_string(),
                    field_value: MongoDBValue::String("bob@example.com".to_string()),
                    bson_type: "string".to_string(),
                })
                .with_neo4j(Neo4jField {
                    property_name: "email".to_string(),
                    property_value: bolt::string("bob@example.com"),
                }),
                Field::simple("age", SurrealDBValue::Int32(25))
                    .with_postgresql(PostgreSQLField {
                        column_name: "age".to_string(),
                        column_value: PostgreSQLValue::Int(25),
                        data_type: "INTEGER".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "age".to_string(),
                        column_value: MySQLValue::Int(25),
                        data_type: "INT".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "age".to_string(),
                        field_value: MongoDBValue::Int32(25),
                        bson_type: "int".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "age".to_string(),
                        property_value: bolt::int(25),
                    }),
                Field::simple("active", SurrealDBValue::Bool(false))
                    .with_postgresql(PostgreSQLField {
                        column_name: "active".to_string(),
                        column_value: PostgreSQLValue::Bool(false),
                        data_type: "BOOLEAN".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "active".to_string(),
                        column_value: MySQLValue::TinyInt(0),
                        data_type: "TINYINT".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "active".to_string(),
                        field_value: MongoDBValue::Bool(false),
                        bson_type: "bool".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "active".to_string(),
                        property_value: bolt::bool(false),
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
                Field::simple("score", SurrealDBValue::Float64(87.3))
                    .with_postgresql(PostgreSQLField {
                        column_name: "score".to_string(),
                        column_value: PostgreSQLValue::Double(87.3),
                        data_type: "DOUBLE PRECISION".to_string(),
                        precision: None,
                        scale: None,
                    })
                    .with_mysql(MySQLField {
                        column_name: "score".to_string(),
                        column_value: MySQLValue::Double(87.3),
                        data_type: "DOUBLE".to_string(),
                        precision: None,
                        scale: None,
                        enum_values: None,
                    })
                    .with_mongodb(MongoDBField {
                        field_name: "score".to_string(),
                        field_value: MongoDBValue::Float64(87.3),
                        bson_type: "double".to_string(),
                    })
                    .with_neo4j(Neo4jField {
                        property_name: "score".to_string(),
                        property_value: bolt::float(87.3),
                    }),
            ]),
        ],
    )
}
