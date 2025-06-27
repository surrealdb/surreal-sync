# Neo4j Data Types Support in surreal-sync

This document provides a comprehensive overview of Neo4j data type support in surreal-sync, detailing which types are supported during migration from Neo4j to SurrealDB.

surreal-sync converts Neo4j nodes and relationships to SurrealDB records by processing Neo4j's Bolt protocol data types. The conversion handles all the Neo4j data types while maintaining data integrity where possible.

## Data Type Support Table

| Neo4j Data Type | Bolt Type | Support Status | SurrealDB Mapping | Notes |
|-----------------|-----------|----------------|-------------------|-------|
| **Boolean** | Boolean | ✅ **Fully Supported** | `bool` | Direct conversion |
| **Integer** | Integer | ✅ **Fully Supported** | `int` | Direct conversion |
| **Float** | Float | ✅ **Fully Supported** | `float` (f64) | Direct conversion |
| **String** | String | ✅ **Fully Supported** | `string` | Direct conversion |
| **List** | List | ✅ **Fully Supported** | `array` | Recursively processed, nested types converted |
| **Map** | Map | ✅ **Fully Supported** | `object` | Recursively processed as nested object |
| **Null** | Null | ✅ **Fully Supported** | `null` | Direct conversion |
| **Date** | Date | ✅ **Fully Supported** | `datetime` | Converted to `datetime` |
| **DateTime** | DateTime | ✅ **Fully Supported** | `datetime` | Converted to `datetime` |
| **LocalDateTime** | LocalDateTime | ✅ **Fully Supported** | `datetime` | Converted to `datetime` |
| **Duration** | Duration | ✅ **Fully Supported** | `duration` | Converted to `duration` |
| **Time** | Time | 🔶 **Partially Supported** | `object` | Converted to object with hour, minute, second, nanosecond, offset_seconds fields |
| **LocalTime** | LocalTime | 🔶 **Partially Supported** | `object` | Converted to object with hour, minute, second, nanosecond fields |
| **Point2D** | Point2D | 🔶 **Partially Supported** | `object` | Converted to object with type, srid, x, y fields, loses spatial indexing |
| **Point3D** | Point3D | 🔶 **Partially Supported** | `object` | Converted to object with type, srid, x, y, z fields, loses spatial indexing |
| **Node** | Node | 🔶 **Partially Supported** | `string` | Converted to debug string representation |
| **Relation** | Relation | 🔶 **Partially Supported** | `string` | Converted to debug string representation |
| **UnboundedRelation** | UnboundedRelation | 🔶 **Partially Supported** | `string` | Converted to debug string representation |
| **Bytes** | Bytes | 🔶 **Partially Supported** | `string` | Converted to debug string representation |
| **Path** | Path | 🔶 **Partially Supported** | `string` | Converted to debug string representation |
| **DateTimeZoneId** | DateTimeZoneId | 🔶 **Partially Supported** | `string` | Converted to debug string representation |

## Support Status Definitions

- ✅ **Fully Supported**: The data type is converted with complete semantic preservation and no data loss
- 🔶 **Partially Supported**: The data is preserved but may lose some type-specific semantics, precision, or functionality
- ❌ **Not Supported**: The data type cannot be migrated (this status is not currently used as all data is preserved in some form)

## Limitations and Considerations

### Partially Supported Types

- **Time/LocalTime**: Converted to objects with time components rather than native time types, losing time-specific operations
- **Spatial Types (Point2D/Point3D)**: Converted to objects with coordinate data but lose spatial indexing and geospatial query capabilities
- **Graph Types (Node/Relation/UnboundedRelation/Path)**: Converted to string representations, preserving structure but losing graph traversal capabilities
- **Bytes**: Converted to string representation, losing efficient binary operations
- **DateTimeZoneId**: Converted to string representation, losing timezone-specific operations

## Testing and Validation

See `/tests/e2e_neo4j.rs` for test examples demonstrating the conversion of various Neo4j data types.
