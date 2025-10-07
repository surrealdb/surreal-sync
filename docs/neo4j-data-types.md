# Neo4j Data Types Support in surreal-sync

This document provides an overview of Neo4j data type support in surreal-sync, detailing which types are supported during migration from Neo4j to SurrealDB.

surreal-sync converts Neo4j nodes and relationships to SurrealDB records by processing Neo4j's Bolt protocol data types. The conversion handles Neo4j data types while maintaining data integrity where possible.

## Data Type Support Table

|  Neo4j Data Type   |   Bolt Type    |      Support Status       | SurrealDB Mapping |                                                  Notes                                                   |
| ------------------ | -------------- | ------------------------- | ----------------- | -------------------------------------------------------------------------------------------------------- |
| **Boolean**        | Boolean        | ✅ **Fully Supported**     | `bool`            | Direct conversion                                                                                        |
| **Integer**        | Integer        | ✅ **Fully Supported**     | `int`             | Direct conversion                                                                                        |
| **Float**          | Float          | ✅ **Fully Supported**     | `float` (f64)     | Direct conversion                                                                                        |
| **String**         | String         | ✅ **Fully Supported**     | `string`          | Direct conversion                                                                                        |
| **List**           | List           | ✅ **Fully Supported**     | `array`           | Recursively processed, nested types converted                                                            |
| **Map**            | Map            | ✅ **Fully Supported**     | `object`          | Recursively processed as nested object                                                                   |
| **Null**           | Null           | ✅ **Fully Supported**     | `null`            | Direct conversion                                                                                        |
| **Date**           | Date           | ✅ **Fully Supported**     | `datetime`        | Converted to UTC datetime (assumes local timezone)                                                       |
| **DateTime**       | DateTime       | ✅ **Fully Supported**     | `datetime`        | Converted to UTC datetime                                                                                |
| **LocalDateTime**  | LocalDateTime  | ✅ **Fully Supported**     | `datetime`        | Converted to UTC datetime (assumes UTC)                                                                  |
| **Duration**       | Duration       | ✅ **Fully Supported**     | `duration`        | Direct conversion                                                                                        |
| **Bytes**          | Bytes          | ✅ **Fully Supported**     | `bytes`           | Direct conversion                                                                                        |
| **Time**           | Time           | 🔶 **Partially Supported** | `object`          | Converted to object with `type: "$Neo4jTime"`, hour, minute, second, nanosecond, offset_seconds fields   |
| **LocalTime**      | LocalTime      | 🔶 **Partially Supported** | `object`          | Converted to object with `type: "$Neo4jLocalTime"`, hour, minute, second, nanosecond fields              |
| **Point2D**        | Point2D        | 🔶 **Partially Supported** | `object`          | GeoJSON-like object with `type: "Point"`, `srid` (4326), `coordinates: [longitude, latitude]`            |
| **Point3D**        | Point3D        | 🔶 **Partially Supported** | `object`          | GeoJSON-like object with `type: "Point"`, `srid` (4979), `coordinates: [longitude, latitude, elevation]` |
| **DateTimeZoneId** | DateTimeZoneId | ✅ **Fully Supported**     | `datetime`        | Converted to UTC datetime using embedded timezone ID                                                     |

## Support Status Definitions

- ✅ **Fully Supported**: The data type is converted with complete semantic preservation and no data loss
- 🔶 **Partially Supported**: The data is preserved but may lose some type-specific semantics, precision, or functionality
- ❌ **Not Supported**: The data type cannot be migrated and will cause migration to fail if encountered

## Spatial Data Type Conversion Details

### Point2D and Point3D Conversion

Neo4j spatial types are converted to GeoJSON-like objects that maintain compatibility with SurrealDB's geometry functions:

**Point2D (WGS-84, SRID 4326):**
```json
{
  "type": "Point",
  "srid": 4326,
  "coordinates": [longitude, latitude]
}
```

**Point3D (WGS-84 3D, SRID 4979):**
```json
{
  "type": "Point",
  "srid": 4979,
  "coordinates": [longitude, latitude, elevation]
}
```

These converted objects can be used with SurrealDB's geo functions through `type::point()` conversion:
- `geo::distance(type::point(obj.coordinates), type::point(other.coordinates))`
- `geo::bearing(type::point(obj.coordinates), type::point(other.coordinates))`

## References

- [Neo4j Data Types Documentation](https://neo4j.com/docs/cypher-manual/current/values-and-types/)
