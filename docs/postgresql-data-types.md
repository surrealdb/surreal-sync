# PostgreSQL Data Types Support in surreal-sync

This document provides an overview of PostgreSQL data type support in surreal-sync, detailing which types are supported during migration from PostgreSQL to SurrealDB.

surreal-sync converts PostgreSQL table rows to SurrealDB records by processing PostgreSQL's wire protocol data types. The conversion handles all the PostgreSQL data types while maintaining data integrity where possible.

## Data Type Support Table

| PostgreSQL Data Type | Wire Protocol Type |            SQL Representation            |      Support Status       | SurrealDB Mapping |                            Notes                             |
| -------------------- | ------------------ | ---------------------------------------- | ------------------------- | ----------------- | ------------------------------------------------------------ |
| **BOOLEAN**          | Boolean            | `true`/`false`                           | ✅ **Fully Supported**     | `bool`            | Direct conversion                                            |
| **SMALLINT**         | Int2               | `32767`                                  | ✅ **Fully Supported**     | `int`             | Converted to 64-bit integer                                  |
| **INTEGER**          | Int4               | `2147483647`                             | ✅ **Fully Supported**     | `int`             | Converted to 64-bit integer                                  |
| **BIGINT**           | Int8               | `9223372036854775807`                    | ✅ **Fully Supported**     | `int`             | Direct conversion                                            |
| **SERIAL**           | Int4               | `1, 2, 3...`                             | ✅ **Fully Supported**     | `int`             | Auto-increment converted to regular integer                  |
| **BIGSERIAL**        | Int8               | `1, 2, 3...`                             | ✅ **Fully Supported**     | `int`             | Auto-increment converted to regular integer                  |
| **REAL**             | Float4             | `3.14`                                   | ✅ **Fully Supported**     | `float` (f64)     | Converted to double precision                                |
| **DOUBLE PRECISION** | Float8             | `3.141592653589793`                      | ✅ **Fully Supported**     | `float` (f64)     | Direct conversion                                            |
| **NUMERIC/DECIMAL**  | Numeric            | `123.45`                                 | ✅ **Fully Supported**     | `number`          | Converted to SurrealDB Number with exact precision preserved |
| **MONEY**            | Money              | `$123.45`                                | ✅ **Fully Supported**     | `number`          | Currency symbol removed, converted to number                 |
| **CHAR(n)**          | Bpchar             | `'text'`                                 | ✅ **Fully Supported**     | `string`          | Fixed-length, padding removed                                |
| **VARCHAR(n)**       | Varchar            | `'text'`                                 | ✅ **Fully Supported**     | `string`          | Variable-length string                                       |
| **TEXT**             | Text               | `'long text'`                            | ✅ **Fully Supported**     | `string`          | Unlimited length string                                      |
| **BYTEA**            | Bytea              | `\\x48656c6c6f`                          | ✅ **Fully Supported**     | `bytes`           | Binary data, hex decoded                                     |
| **DATE**             | Date               | `'2024-01-15'`                           | ✅ **Fully Supported**     | `datetime`        | Converted to datetime at midnight UTC                        |
| **TIME**             | Time               | `'14:30:00'`                             | ✅ **Fully Supported**     | `string`          | Time-only as string (SurrealDB has no pure time type)        |
| **TIMESTAMP**        | Timestamp          | `'2024-01-15 14:30:00'`                  | ✅ **Fully Supported**     | `datetime`        | Converted to UTC datetime                                    |
| **TIMESTAMPTZ**      | Timestamptz        | `'2024-01-15 14:30:00+00'`               | ✅ **Fully Supported**     | `datetime`        | Timezone-aware, converted to UTC                             |
| **INTERVAL**         | Interval           | `'1 day 2 hours'`                        | ✅ **Fully Supported**     | `duration`        | Converted to SurrealDB duration                              |
| **UUID**             | Uuid               | `'550e8400-e29b-41d4-a716-446655440000'` | ✅ **Fully Supported**     | `string`          | UUID string representation                                   |
| **JSON**             | Json               | `'{"key": "value"}'`                     | ✅ **Fully Supported**     | `string`          | JSON stored as string representation                         |
| **JSONB**            | Jsonb              | `'{"key": "value"}'`                     | ✅ **Fully Supported**     | `string`          | Binary JSON stored as string representation                  |
| **ARRAY**            | Array              | `'{1,2,3}'`                              | ✅ **Fully Supported**     | `array`           | Recursively processed, element types converted               |
| **POINT**            | Point              | `'(1.5, 2.5)'`                           | 🔶 **Partially Supported** | `object`          | Converted to `{"x": 1.5, "y": 2.5}` object                   |
| **LINE**             | Line               | `'{1,2,3}'`                              | 🔶 **Partially Supported** | `object`          | Converted to coefficient object                              |
| **LSEG**             | Lseg               | `'[(1,2),(3,4)]'`                        | 🔶 **Partially Supported** | `object`          | Line segment as start/end point object                       |
| **BOX**              | Box                | `'(1,2),(3,4)'`                          | 🔶 **Partially Supported** | `object`          | Bounding box as corner points object                         |
| **PATH**             | Path               | `'[(1,2),(3,4)]'`                        | 🔶 **Partially Supported** | `array`           | Array of point objects                                       |
| **POLYGON**          | Polygon            | `'((1,2),(3,4),(5,6))'`                  | 🔶 **Partially Supported** | `array`           | Array of point objects                                       |
| **CIRCLE**           | Circle             | `'<(1,2),3>'`                            | 🔶 **Partially Supported** | `object`          | Center point and radius object                               |
| **INET**             | Inet               | `'192.168.1.1'`                          | ✅ **Fully Supported**     | `string`          | IP address as string                                         |
| **CIDR**             | Cidr               | `'192.168.0.0/24'`                       | ✅ **Fully Supported**     | `string`          | Network address as string                                    |
| **MACADDR**          | Macaddr            | `'08:00:2b:01:02:03'`                    | ✅ **Fully Supported**     | `string`          | MAC address as string                                        |

## Support Status Definitions

- ✅ **Fully Supported**: The data type is converted with complete semantic preservation and no data loss
- 🔶 **Partially Supported**: The data is preserved but may lose some type-specific semantics, precision, or functionality
- ❌ **Not Supported**: The data type cannot be migrated and will cause migration to fail if encountered

## Geometric Data Type Conversion Details

### Spatial Types

PostgreSQL geometric types are converted to objects that preserve coordinate data:

**POINT:**
```json
{
  "x": 1.5,
  "y": 2.5
}
```

**BOX (bounding box):**
```json
{
  "upper_right": {"x": 3.0, "y": 4.0},
  "lower_left": {"x": 1.0, "y": 2.0}
}
```

**POLYGON:**
```json
[
  {"x": 1.0, "y": 2.0},
  {"x": 3.0, "y": 4.0},
  {"x": 5.0, "y": 6.0}
]
```

## Limitations and Considerations

### Type Conversion Notes

- **SERIAL/BIGSERIAL**: Auto-increment behavior is lost, becomes regular integer
- **MONEY**: Currency formatting lost, converted to numeric value
- **TIME**: Converted to string since SurrealDB lacks pure time type
- **Geometric Types**: Lose PostgreSQL-specific geometric functions
- **Network Types**: Converted to strings, lose network-specific operations

### Special Limitations

- **Custom Types**: User-defined types converted based on underlying representation
- **Enum Types**: Converted to string values, lose enum constraints
- **Range Types**: Not directly supported, would need custom handling
- **Composite Types**: Converted as JSON objects, lose type structure

## Testing and Validation

See `/tests/e2e_postgresql.rs` for test examples demonstrating the conversion of various PostgreSQL data types.

## References

- [PostgreSQL Data Types Documentation](https://www.postgresql.org/docs/current/datatype.html)
- [PostgreSQL Wire Protocol](https://www.postgresql.org/docs/current/protocol.html)