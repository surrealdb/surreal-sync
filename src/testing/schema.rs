//! Schema definitions for test tables
//!
//! Provides TestSchema that defines table structures for all supported databases,
//! enabling creation of empty tables with proper schemas for checkpoint generation.

use serde_json::Value as JsonValue;

/// Central schema definition for a test table across all sources
#[derive(Debug, Clone)]
pub struct TestSchema {
    pub postgresql: PostgreSQLSchema,
    pub mysql: MySQLSchema,
    pub mongodb: MongoDBSchema,
    pub neo4j: Neo4jSchema,
}

/// PostgreSQL table schema definition
#[derive(Debug, Clone)]
pub struct PostgreSQLSchema {
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<String>,
    pub indexes: Vec<IndexDef>,
    pub constraints: Vec<ConstraintDef>,
}

/// MySQL table schema definition
#[derive(Debug, Clone)]
pub struct MySQLSchema {
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<String>,
    pub indexes: Vec<IndexDef>,
    pub constraints: Vec<ConstraintDef>,
    pub engine: String,
    pub charset: String,
    pub collation: String,
}

/// MongoDB collection schema definition
#[derive(Debug, Clone)]
pub struct MongoDBSchema {
    pub collection_name: String,
    pub indexes: Vec<MongoIndexDef>,
    pub validation_schema: Option<JsonValue>,
    pub capped: bool,
    pub size: Option<u64>,
    pub max: Option<u64>,
}

/// Neo4j node/relationship schema definition
#[derive(Debug, Clone)]
pub struct Neo4jSchema {
    pub node_labels: Vec<String>,
    pub relationship_types: Vec<String>,
    pub properties: Vec<PropertyDef>,
    pub constraints: Vec<Neo4jConstraintDef>,
    pub indexes: Vec<Neo4jIndexDef>,
}

/// Column definition for SQL databases
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub unique: bool,
    pub auto_increment: bool,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        ColumnDef {
            name: name.into(),
            data_type: data_type.into(),
            nullable: true,
            default: None,
            unique: false,
            auto_increment: false,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn auto_increment(mut self) -> Self {
        self.auto_increment = true;
        self
    }

    pub fn default(mut self, value: impl Into<String>) -> Self {
        self.default = Some(value.into());
        self
    }

    /// Generate PostgreSQL column definition
    pub fn to_postgresql_ddl(&self) -> String {
        let mut ddl = format!("{} {}", self.name, self.data_type);

        if !self.nullable {
            ddl.push_str(" NOT NULL");
        }

        if self.unique {
            ddl.push_str(" UNIQUE");
        }

        if let Some(ref default) = self.default {
            ddl.push_str(&format!(" DEFAULT {default}"));
        }

        ddl
    }

    /// Generate MySQL column definition
    pub fn to_mysql_ddl(&self) -> String {
        let mut ddl = format!("`{}` {}", self.name, self.data_type);

        if self.auto_increment {
            ddl.push_str(" AUTO_INCREMENT");
        }

        if !self.nullable {
            ddl.push_str(" NOT NULL");
        }

        if self.unique {
            ddl.push_str(" UNIQUE");
        }

        if let Some(ref default) = self.default {
            ddl.push_str(&format!(" DEFAULT {default}"));
        }

        ddl
    }
}

/// Index definition for SQL databases
#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: Option<String>, // BTREE, HASH, etc.
}

impl IndexDef {
    pub fn new(name: impl Into<String>, columns: Vec<String>) -> Self {
        IndexDef {
            name: name.into(),
            columns,
            unique: false,
            index_type: None,
        }
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// Constraint definition for SQL databases
#[derive(Debug, Clone)]
pub struct ConstraintDef {
    pub name: String,
    pub constraint_type: ConstraintType,
}

#[derive(Debug, Clone)]
pub enum ConstraintType {
    Check(String),
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: Option<String>,
        on_update: Option<String>,
    },
}

/// MongoDB index definition
#[derive(Debug, Clone)]
pub struct MongoIndexDef {
    pub keys: Vec<(String, i32)>, // field name and direction (1 or -1)
    pub unique: bool,
    pub sparse: bool,
    pub ttl: Option<i32>,
    pub name: Option<String>,
}

impl MongoIndexDef {
    pub fn new(field: impl Into<String>, ascending: bool) -> Self {
        MongoIndexDef {
            keys: vec![(field.into(), if ascending { 1 } else { -1 })],
            unique: false,
            sparse: false,
            ttl: None,
            name: None,
        }
    }

    pub fn compound(keys: Vec<(String, i32)>) -> Self {
        MongoIndexDef {
            keys,
            unique: false,
            sparse: false,
            ttl: None,
            name: None,
        }
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// Neo4j property definition
#[derive(Debug, Clone)]
pub struct PropertyDef {
    pub name: String,
    pub data_type: String, // String, Integer, Float, Boolean, DateTime, etc.
    pub required: bool,
}

impl PropertyDef {
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        PropertyDef {
            name: name.into(),
            data_type: data_type.into(),
            required: false,
        }
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }
}

/// Neo4j constraint definition
#[derive(Debug, Clone)]
pub struct Neo4jConstraintDef {
    pub name: String,
    pub constraint_type: Neo4jConstraintType,
}

#[derive(Debug, Clone)]
pub enum Neo4jConstraintType {
    NodeKey {
        label: String,
        properties: Vec<String>,
    },
    Unique {
        label: String,
        property: String,
    },
    Exists {
        label: String,
        property: String,
    },
}

impl Neo4jConstraintDef {
    pub fn unique(label: impl Into<String>, property: impl Into<String>) -> Self {
        let label_str = label.into();
        let property_str = property.into();
        Neo4jConstraintDef {
            name: format!("constraint_{}_{}unique", &label_str, &property_str),
            constraint_type: Neo4jConstraintType::Unique {
                label: label_str,
                property: property_str,
            },
        }
    }
}

/// Neo4j index definition
#[derive(Debug, Clone)]
pub struct Neo4jIndexDef {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub index_type: Neo4jIndexType,
}

#[derive(Debug, Clone)]
pub enum Neo4jIndexType {
    Range,
    Text,
    Point,
    Fulltext,
}

impl PostgreSQLSchema {
    /// Generate CREATE TABLE statement for PostgreSQL
    pub fn to_create_table_ddl(&self, table_name: &str) -> String {
        let mut ddl = format!("CREATE TABLE {table_name} (\n");

        // Add columns
        let column_defs: Vec<String> = self
            .columns
            .iter()
            .map(|col| format!("    {}", col.to_postgresql_ddl()))
            .collect();
        ddl.push_str(&column_defs.join(",\n"));

        // Add primary key if defined
        if let Some(ref pk) = self.primary_key {
            ddl.push_str(&format!(",\n    PRIMARY KEY ({pk})"));
        }

        ddl.push_str("\n)");
        ddl
    }

    /// Generate CREATE INDEX statements for PostgreSQL
    pub fn to_create_index_ddl(&self, table_name: &str, index: &IndexDef) -> String {
        if index.unique {
            format!(
                "CREATE UNIQUE INDEX {} ON {} ({})",
                index.name,
                table_name,
                index.columns.join(", ")
            )
        } else {
            format!(
                "CREATE INDEX {} ON {} ({})",
                index.name,
                table_name,
                index.columns.join(", ")
            )
        }
    }
}

impl MySQLSchema {
    /// Generate CREATE TABLE statement for MySQL
    pub fn to_create_table_ddl(&self, table_name: &str) -> String {
        let mut ddl = format!("CREATE TABLE `{table_name}` (\n");

        // Add columns
        let column_defs: Vec<String> = self
            .columns
            .iter()
            .map(|col| format!("    {}", col.to_mysql_ddl()))
            .collect();
        ddl.push_str(&column_defs.join(",\n"));

        // Add primary key if defined
        if let Some(ref pk) = self.primary_key {
            ddl.push_str(&format!(",\n    PRIMARY KEY (`{pk}`)"));
        }

        ddl.push_str(&format!(
            "\n) ENGINE={} DEFAULT CHARSET={} COLLATE={}",
            self.engine, self.charset, self.collation
        ));

        ddl
    }

    /// Generate CREATE INDEX statements for MySQL
    pub fn to_create_index_ddl(&self, table_name: &str, index: &IndexDef) -> String {
        if index.unique {
            format!(
                "CREATE UNIQUE INDEX {} ON {} ({})",
                index.name,
                table_name,
                index
                    .columns
                    .iter()
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            format!(
                "CREATE INDEX {} ON {} ({})",
                index.name,
                table_name,
                index
                    .columns
                    .iter()
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }
}

impl Default for MySQLSchema {
    fn default() -> Self {
        MySQLSchema {
            columns: vec![],
            primary_key: None,
            indexes: vec![],
            constraints: vec![],
            engine: "InnoDB".to_string(),
            charset: "utf8mb4".to_string(),
            collation: "utf8mb4_general_ci".to_string(),
        }
    }
}
