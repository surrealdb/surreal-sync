//! Test-only data type system for database type testing
//!
//! This module defines type enums that can represent data as it exists in each
//! specific database system, plus SurrealDBValue for expected results after sync.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use std::collections::HashMap;
use std::fmt;

/// SurrealDB value types - what we expect in SurrealDB after sync
#[derive(Debug, Clone, PartialEq)]
pub enum SurrealDBValue {
    // Universal types (what SurrealDB can represent)
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<SurrealDBValue>),
    Object(HashMap<String, SurrealDBValue>),

    // High-precision types
    Decimal {
        value: String,
        precision: Option<u32>,
        scale: Option<u32>,
    },

    // Date/Time types
    DateTime(DateTime<Utc>),
    Date(NaiveDate),
    Time(NaiveTime),

    // Common structured types
    Uuid(String),

    // SurrealDB-specific types only
    Thing {
        table: String,
        id: Box<SurrealDBValue>,
    },
    Geometry(String),
    Duration(String),
    Record(String),
    None,
}

/// MongoDB value types - native MongoDB/BSON representations
#[derive(Debug, Clone, PartialEq)]
pub enum MongoDBValue {
    // Universal types
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Array(Vec<MongoDBValue>),
    Document(HashMap<String, MongoDBValue>),

    // Date/Time
    DateTime(DateTime<Utc>),
    Timestamp {
        timestamp: u32,
        increment: u32,
    },

    // MongoDB-specific types
    ObjectId(String),
    JavaScript {
        code: String,
        scope: Option<HashMap<String, MongoDBValue>>,
    },
    RegularExpression {
        pattern: String,
        flags: String,
    },
    Decimal128(String),
    MinKey,
    MaxKey,
    Symbol(String),
    DBPointer {
        namespace: String,
        id: String,
    },
}

/// PostgreSQL value types - native PostgreSQL representations
#[derive(Debug, Clone, PartialEq)]
pub enum PostgreSQLValue {
    // Universal types
    Null,
    Bool(bool),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Real(f32),
    Double(f64),
    Text(String),
    Varchar(String),
    Char(String),
    Bytea(Vec<u8>),

    // Date/Time types
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    Interval(String),

    // Numeric types
    Numeric {
        value: String,
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Money(String),

    // PostgreSQL-specific types
    Uuid(String),
    Json(serde_json::Value),
    Jsonb(serde_json::Value),
    Inet(String),
    Cidr(String),
    MacAddr(String),
    Point {
        x: f64,
        y: f64,
    },
    Path(Vec<(f64, f64)>),
    Polygon(Vec<(f64, f64)>),
    Array(Vec<PostgreSQLValue>),
    Range(String),
    Hstore(HashMap<String, Option<String>>),
}

/// MySQL value types - native MySQL representations
#[derive(Debug, Clone, PartialEq)]
pub enum MySQLValue {
    // Universal types
    Null,
    TinyInt(i8),
    SmallInt(i16),
    MediumInt(i32),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Decimal {
        value: String,
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Char(String),
    Varchar(String),
    Text(String),
    Binary(Vec<u8>),
    VarBinary(Vec<u8>),
    Blob(Vec<u8>),

    // Date/Time types
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    Timestamp(NaiveDateTime),

    // MySQL-specific types
    Enum(String),
    Set(Vec<String>),
    Year(u16),
    Bit(Vec<bool>),
    Json(serde_json::Value),
    Geometry(String),
}

/// Neo4j value types - native Neo4j/Cypher representations
#[derive(Debug, Clone, PartialEq)]
pub enum Neo4jValue {
    // Universal types
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Neo4jValue>),
    Map(HashMap<String, Neo4jValue>),

    // Date/Time types
    Date(NaiveDate),
    Time(NaiveTime),
    LocalTime(NaiveTime),
    DateTime(DateTime<Utc>),
    LocalDateTime(NaiveDateTime),

    // Neo4j-specific types
    Point {
        x: f64,
        y: f64,
        z: Option<f64>,
        srid: Option<i32>,
    },
    Duration {
        months: i64,
        days: i64,
        seconds: i64,
        nanos: i32,
    },
    Node {
        id: i64,
        labels: Vec<String>,
        properties: HashMap<String, Neo4jValue>,
    },
    Relationship {
        id: i64,
        start_id: i64,
        end_id: i64,
        rel_type: String,
        properties: HashMap<String, Neo4jValue>,
    },
    Path(Vec<String>), // Simplified path representation for testing
}

// Implement Display for SurrealDBValue to enable .to_string()
impl fmt::Display for SurrealDBValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SurrealDBValue::Null => write!(f, "NULL"),
            SurrealDBValue::Bool(b) => write!(f, "{b}"),
            SurrealDBValue::Int32(i) => write!(f, "{i}"),
            SurrealDBValue::Int64(i) => write!(f, "{i}"),
            SurrealDBValue::Float32(fl) => write!(f, "{fl}"),
            SurrealDBValue::Float64(fl) => write!(f, "{fl}"),
            SurrealDBValue::String(s) => write!(f, "{s}"),
            SurrealDBValue::Bytes(b) => write!(f, "BYTES[{}]", b.len()),
            SurrealDBValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| v.to_string()).collect();
                write!(f, "[{}]", items.join(", "))
            }
            SurrealDBValue::Object(obj) => {
                let items: Vec<String> = obj.iter().map(|(k, v)| format!("{k}: {v}")).collect();
                write!(f, "{{{}}}", items.join(", "))
            }
            SurrealDBValue::Decimal { value, .. } => write!(f, "{value}"),
            SurrealDBValue::DateTime(dt) => write!(f, "{}", dt.to_rfc3339()),
            SurrealDBValue::Date(d) => write!(f, "{d}"),
            SurrealDBValue::Time(t) => write!(f, "{t}"),
            SurrealDBValue::Uuid(u) => write!(f, "{u}"),
            SurrealDBValue::Thing { table, id } => write!(f, "{table}:{id}"),
            SurrealDBValue::Geometry(g) => write!(f, "{g}"),
            SurrealDBValue::Duration(d) => write!(f, "{d}"),
            SurrealDBValue::Record(r) => write!(f, "{r}"),
            SurrealDBValue::None => write!(f, "NONE"),
        }
    }
}

// Note: Conversion functions have been removed to enforce explicit field definitions.
// All database-specific fields must be explicitly defined in test datasets.
// This ensures test data integrity and avoids testing conversion logic itself.
