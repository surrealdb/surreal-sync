# MySQL Data Types Support in surreal-sync

This document provides an overview of MySQL data type support in surreal-sync, detailing which types are supported during migration from MySQL to SurrealDB.

surreal-sync converts MySQL table rows to SurrealDB records by processing MySQL's wire protocol data types. The conversion handles all the MySQL data types while maintaining data integrity where possible.

## Data Type Support Table

|   MySQL Data Type   | Wire Protocol Type |               SQL Representation                |      Support Status       | SurrealDB Mapping |                     Notes                      |
| ------------------- | ------------------ | ----------------------------------------------- | ------------------------- | ----------------- | ---------------------------------------------- |
| **BOOLEAN/BOOL**    | Tiny               | `0`/`1`                                         | ✅ **Fully Supported**     | `bool`            | 0=false, 1=true conversion                     |
| **TINYINT**         | Tiny               | `-128` to `127`                                 | ✅ **Fully Supported**     | `int`             | Converted to 64-bit integer                    |
| **SMALLINT**        | Short              | `-32768` to `32767`                             | ✅ **Fully Supported**     | `int`             | Converted to 64-bit integer                    |
| **MEDIUMINT**       | Int24              | `-8388608` to `8388607`                         | ✅ **Fully Supported**     | `int`             | Converted to 64-bit integer                    |
| **INT/INTEGER**     | Long               | `-2147483648` to `2147483647`                   | ✅ **Fully Supported**     | `int`             | Converted to 64-bit integer                    |
| **BIGINT**          | LongLong           | `-9223372036854775808` to `9223372036854775807` | ✅ **Fully Supported**     | `int`             | Direct conversion                              |
| **FLOAT**           | Float              | `3.14`                                          | ✅ **Fully Supported**     | `float` (f64)     | Converted to double precision                  |
| **DOUBLE**          | Double             | `3.141592653589793`                             | ✅ **Fully Supported**     | `float` (f64)     | Direct conversion                              |
| **DECIMAL/NUMERIC** | NewDecimal         | `123.45`                                        | ✅ **Fully Supported**     | `number`          | Converted to SurrealDB Number with precision   |
| **CHAR(n)**         | String             | `'text'`                                        | ✅ **Fully Supported**     | `string`          | Fixed-length, padding removed                  |
| **VARCHAR(n)**      | VarString          | `'text'`                                        | ✅ **Fully Supported**     | `string`          | Variable-length string                         |
| **TEXT**            | Blob               | `'long text'`                                   | ✅ **Fully Supported**     | `string`          | Text blob as string                            |
| **TINYTEXT**        | TinyBlob           | `'short text'`                                  | ✅ **Fully Supported**     | `string`          | Small text as string                           |
| **MEDIUMTEXT**      | MediumBlob         | `'medium text'`                                 | ✅ **Fully Supported**     | `string`          | Medium text as string                          |
| **LONGTEXT**        | LongBlob           | `'very long text'`                              | ✅ **Fully Supported**     | `string`          | Large text as string                           |
| **BINARY(n)**       | String             | `0x48656c6c6f`                                  | ✅ **Fully Supported**     | `bytes`           | Fixed-length binary data                       |
| **VARBINARY(n)**    | VarString          | `0x48656c6c6f`                                  | ✅ **Fully Supported**     | `bytes`           | Variable-length binary data                    |
| **BLOB**            | Blob               | `0x48656c6c6f`                                  | ✅ **Fully Supported**     | `bytes`           | Binary large object                            |
| **TINYBLOB**        | TinyBlob           | `0x48656c6c6f`                                  | ✅ **Fully Supported**     | `bytes`           | Small binary data                              |
| **MEDIUMBLOB**      | MediumBlob         | `0x48656c6c6f`                                  | ✅ **Fully Supported**     | `bytes`           | Medium binary data                             |
| **LONGBLOB**        | LongBlob           | `0x48656c6c6f`                                  | ✅ **Fully Supported**     | `bytes`           | Large binary data                              |
| **DATE**            | Date               | `'2024-01-15'`                                  | ✅ **Fully Supported**     | `datetime`        | Converted to datetime at midnight UTC          |
| **TIME**            | Time               | `'14:30:00'`                                    | ✅ **Fully Supported**     | `string`          | Time format as string (HH:MM:SS.microseconds)  |
| **DATETIME**        | DateTime           | `'2024-01-15 14:30:00'`                         | ✅ **Fully Supported**     | `datetime`        | Converted to UTC datetime                      |
| **TIMESTAMP**       | Timestamp          | `'2024-01-15 14:30:00'`                         | ✅ **Fully Supported**     | `datetime`        | Timezone-aware, converted to UTC               |
| **YEAR**            | Year               | `2024`                                          | ✅ **Fully Supported**     | `int`             | Year as integer                                |
| **JSON**            | Json               | `'{"key": "value"}'`                            | ✅ **Fully Supported**     | `object`          | Parsed and converted recursively               |
| **GEOMETRY**        | Geometry           | `ST_GeomFromText('POINT(1 2)')`                 | 🔶 **Partially Supported** | `object`          | Converted to geometric object with coordinates |
| **POINT**           | Geometry           | `POINT(1.5, 2.5)`                               | 🔶 **Partially Supported** | `object`          | Converted to `{"x": 1.5, "y": 2.5}` object     |
| **ENUM**            | Enum               | `'option1'`                                     | ✅ **Fully Supported**     | `string`          | Enum value as string, constraints lost         |
| **SET**             | Set                | `'value1,value2'`                               | ✅ **Fully Supported**     | `array`           | Converted to array of strings                  |
| **BIT(n)**          | Bit                | `b'1010'`                                       | ✅ **Fully Supported**     | `string`          | Bit string as binary string representation     |

## Support Status Definitions

- ✅ **Fully Supported**: The data type is converted with complete semantic preservation and no data loss
- 🔶 **Partially Supported**: The data is preserved but may lose some type-specific semantics, precision, or functionality
- ❌ **Not Supported**: The data type cannot be migrated and will cause migration to fail if encountered

## Geometric Data Type Conversion Details

### Spatial Types

MySQL geometric types are converted to objects that preserve coordinate data:

**POINT:**
```json
{
  "x": 1.5,
  "y": 2.5
}
```

**GEOMETRY (complex shapes):**
```json
{
  "type": "geometry",
  "coordinates": [[1.0, 2.0], [3.0, 4.0]],
  "geometry_type": "LINESTRING"
}
```

Note: Complex spatial operations specific to MySQL are not preserved.

## Limitations and Considerations

### Type Conversion Notes

- **AUTO_INCREMENT**: Auto-increment behavior is lost, becomes regular integer
- **ENUM**: Converted to string values, lose enum constraints and validation
- **SET**: Converted to array, loses MySQL SET semantics
- **TIME**: Converted to string since SurrealDB lacks pure time type
- **Geometric Types**: Lose MySQL-specific spatial functions
- **BIT**: Converted to string representation, loses bit manipulation functions

### Special Limitations

- **Custom Collations**: String collation rules are not preserved
- **Constraints**: Foreign key, unique, and check constraints are not migrated
- **Triggers**: Source triggers are not migrated (different from sync triggers)
- **Stored Procedures**: Functions and procedures are not migrated
- **Views**: Only base tables are migrated, not views

### Data Integrity Notes

- **NULL handling**: MySQL NULL values become SurrealDB null
- **Zero dates**: MySQL zero dates ('0000-00-00') may cause conversion issues
- **Invalid dates**: Invalid MySQL dates are converted to null with warning
- **Charset encoding**: Text data is converted assuming UTF-8 encoding

## Testing and Validation

See `/tests/e2e_mysql.rs` for test examples demonstrating the conversion of various MySQL data types.

## References

- [MySQL Data Types Documentation](https://dev.mysql.com/doc/refman/8.0/en/data-types.html)
- [MySQL Protocol Documentation](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html)
