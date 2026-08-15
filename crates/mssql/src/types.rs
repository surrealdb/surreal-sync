//! Map SQL Server column types to the surreal-sync universal [`Type`].
//!
//! Types that cannot be copied losslessly are rejected before export.

use surreal_sync_core::Type;

/// A SQL Server type that surreal-sync will not copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedMssqlType {
    /// SQL Server type name as reported by the catalog (for example `geography`).
    pub type_name: String,
}

impl std::fmt::Display for UnsupportedMssqlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SQL Server type `{}` cannot be synced to SurrealDB",
            self.type_name
        )
    }
}

impl std::error::Error for UnsupportedMssqlType {}

impl UnsupportedMssqlType {
    /// Create an error for `type_name`.
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
        }
    }
}

fn is_rejected(type_name: &str) -> bool {
    matches!(
        type_name,
        "sql_variant"
            | "hierarchyid"
            | "geography"
            | "geometry"
            | "cursor"
            | "table"
            | "rowversion"
            | "timestamp"
            | "udt"
    )
}

/// Map a SQL Server catalog type to the universal type universe.
///
/// `length` is the character length when known. `max_length` is
/// `sys.columns.max_length` (`-1` means `MAX`).
///
/// `timestamp` is SQL Server's binary rowversion, not a datetime, and is
/// rejected.
pub fn mssql_column_to_universal_type(
    type_name: &str,
    precision: Option<u8>,
    scale: Option<u8>,
    length: Option<u16>,
    max_length: Option<i32>,
) -> Result<Type, UnsupportedMssqlType> {
    let normalized = type_name.trim().to_ascii_lowercase();
    if is_rejected(&normalized) {
        return Err(UnsupportedMssqlType::new(type_name));
    }

    let is_max = max_length == Some(-1);

    Ok(match normalized.as_str() {
        "bit" => Type::Bool,
        // tinyint is 0–255; Int16 is the lossless signed home.
        "tinyint" => Type::Int16,
        "smallint" => Type::Int16,
        "int" => Type::Int32,
        "bigint" => Type::Int64,
        "decimal" | "numeric" => Type::Decimal {
            precision: precision.unwrap_or(18),
            scale: scale.unwrap_or(0),
        },
        "money" => Type::Decimal {
            precision: 19,
            scale: 4,
        },
        "smallmoney" => Type::Decimal {
            precision: 10,
            scale: 4,
        },
        "float" => Type::Float64,
        "real" => Type::Float32,
        "date" => Type::Date,
        "time" => Type::Time,
        "datetime" | "datetime2" | "smalldatetime" => Type::LocalDateTime,
        "datetimeoffset" => Type::ZonedDateTime,
        "char" | "nchar" => Type::Char {
            length: length.unwrap_or(1),
        },
        "varchar" | "nvarchar" => {
            if is_max {
                Type::Text
            } else {
                Type::VarChar {
                    length: length.unwrap_or(1),
                }
            }
        }
        "text" | "ntext" | "xml" => Type::Text,
        "sysname" => Type::VarChar { length: 128 },
        "uniqueidentifier" => Type::Uuid,
        "binary" | "varbinary" | "image" => Type::Bytes,
        // CLR user-defined types show up under their type name, not `udt`.
        other => return Err(UnsupportedMssqlType::new(other)),
    })
}

/// Convert one Tiberius cell to a universal [`surreal_sync_core::Value`] using
/// the catalog type as a hint.
#[cfg(feature = "from_mssql")]
pub fn tiberius_to_value(
    row: &tiberius::Row,
    col_idx: usize,
    type_hint: &Type,
) -> anyhow::Result<surreal_sync_core::Value> {
    use anyhow::Context;
    use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use surreal_sync_core::Value;

    fn naive_utc(n: NaiveDateTime) -> DateTime<Utc> {
        DateTime::<Utc>::from_naive_utc_and_offset(n, Utc)
    }

    let ctx = || format!("reading SQL Server column {col_idx}");

    match type_hint {
        Type::Bool => match row.try_get::<bool, _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(v) => Ok(Value::Bool(v)),
        },
        Type::Int16 => {
            if let Ok(Some(v)) = row.try_get::<u8, _>(col_idx) {
                return Ok(Value::Int16(v as i16));
            }
            match row.try_get::<i16, _>(col_idx).with_context(ctx)? {
                None => Ok(Value::Null),
                Some(v) => Ok(Value::Int16(v)),
            }
        }
        Type::Int32 => match row.try_get::<i32, _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(v) => Ok(Value::Int32(v)),
        },
        Type::Int64 => match row.try_get::<i64, _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(v) => Ok(Value::Int64(v)),
        },
        Type::Float32 => match row.try_get::<f32, _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(v) => Ok(Value::Float32(v)),
        },
        Type::Float64 => match row.try_get::<f64, _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(v) => Ok(Value::Float64(v)),
        },
        Type::Decimal { precision, scale } => {
            // DECIMAL/NUMERIC arrive as Tiberius Numeric. MONEY/SMALLMONEY are
            // exposed as f64 by the driver.
            if let Ok(opt) = row.try_get::<tiberius::numeric::Numeric, _>(col_idx) {
                return Ok(match opt {
                    None => Value::Null,
                    Some(n) => Value::Decimal {
                        value: n.to_string(),
                        precision: *precision,
                        scale: *scale,
                    },
                });
            }
            match row.try_get::<f64, _>(col_idx).with_context(ctx)? {
                None => Ok(Value::Null),
                Some(f) => Ok(Value::Decimal {
                    value: format!("{f:.prec$}", prec = *scale as usize),
                    precision: *precision,
                    scale: *scale,
                }),
            }
        }
        Type::Char { length } => match row.try_get::<&str, _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(s) => Ok(Value::Char {
                value: s.to_string(),
                length: *length,
            }),
        },
        Type::VarChar { length } => match row.try_get::<&str, _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(s) => Ok(Value::VarChar {
                value: s.to_string(),
                length: *length,
            }),
        },
        Type::Text => {
            match row.try_get::<&tiberius::xml::XmlData, _>(col_idx) {
                Ok(Some(xml)) => return Ok(Value::Text(xml.to_string())),
                Ok(None) => return Ok(Value::Null),
                Err(_) => {}
            }
            match row.try_get::<&str, _>(col_idx).with_context(ctx)? {
                None => Ok(Value::Null),
                Some(s) => Ok(Value::Text(s.to_string())),
            }
        }
        Type::Bytes | Type::Blob => match row.try_get::<&[u8], _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(bytes) => Ok(Value::Bytes(bytes.to_vec())),
        },
        Type::Date => match row.try_get::<NaiveDate, _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(d) => Ok(Value::Date(naive_utc(
                d.and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow::anyhow!("invalid date"))?,
            ))),
        },
        Type::Time => match row.try_get::<NaiveTime, _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(t) => {
                let d = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .ok_or_else(|| anyhow::anyhow!("invalid epoch date"))?;
                Ok(Value::Time(naive_utc(d.and_time(t))))
            }
        },
        Type::LocalDateTime | Type::LocalDateTimeNano => {
            match row.try_get::<NaiveDateTime, _>(col_idx).with_context(ctx)? {
                None => Ok(Value::Null),
                Some(n) => Ok(Value::LocalDateTime(naive_utc(n))),
            }
        }
        Type::ZonedDateTime => {
            if let Ok(opt) = row.try_get::<DateTime<FixedOffset>, _>(col_idx) {
                return Ok(match opt {
                    None => Value::Null,
                    Some(dt) => Value::ZonedDateTime(dt.to_utc()),
                });
            }
            match row.try_get::<DateTime<Utc>, _>(col_idx).with_context(ctx)? {
                None => Ok(Value::Null),
                Some(dt) => Ok(Value::ZonedDateTime(dt)),
            }
        }
        Type::Uuid => match row.try_get::<uuid::Uuid, _>(col_idx).with_context(ctx)? {
            None => Ok(Value::Null),
            Some(u) => Ok(Value::Uuid(u)),
        },
        other => anyhow::bail!("no SQL Server conversion for type {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_core_types() {
        assert_eq!(
            mssql_column_to_universal_type("bit", None, None, None, None).unwrap(),
            Type::Bool
        );
        assert_eq!(
            mssql_column_to_universal_type("tinyint", None, None, None, None).unwrap(),
            Type::Int16
        );
        assert_eq!(
            mssql_column_to_universal_type("int", None, None, None, None).unwrap(),
            Type::Int32
        );
        assert_eq!(
            mssql_column_to_universal_type("bigint", None, None, None, None).unwrap(),
            Type::Int64
        );
        assert_eq!(
            mssql_column_to_universal_type("money", None, None, None, None).unwrap(),
            Type::Decimal {
                precision: 19,
                scale: 4
            }
        );
        assert_eq!(
            mssql_column_to_universal_type("sysname", None, None, None, None).unwrap(),
            Type::VarChar { length: 128 }
        );
        assert_eq!(
            mssql_column_to_universal_type("nvarchar", None, None, Some(50), Some(100)).unwrap(),
            Type::VarChar { length: 50 }
        );
        assert_eq!(
            mssql_column_to_universal_type("varchar", None, None, None, Some(-1)).unwrap(),
            Type::Text
        );
    }

    #[test]
    fn rejects_lossy_types() {
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
                "{name}"
            );
        }
    }
}
