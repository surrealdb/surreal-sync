# MongoDB Data Types Support in surreal-sync

This document provides an overview of MongoDB data type support in surreal-sync, detailing which types are supported during migration from MongoDB to SurrealDB.

surreal-sync converts MongoDB documents to SurrealDB records by processing data in BSON. The conversion handles all the MongoDB data types while maintaining data integrity where possible.

## Data Type Support Table

|     MongoDB Data Type     |        BSON Type        |                                          JSON Extended Format v2                                           |      Support Status       | SurrealDB Mapping |                                       Notes                                       |
| ------------------------- | ----------------------- | ---------------------------------------------------------------------------------------------------------- | ------------------------- | ----------------- | --------------------------------------------------------------------------------- |
| **Double**                | Double                  | `3.14` (Relaxed) or `{"$numberDouble": "3.14"}` (Canonical)                                                | ✅ **Fully Supported**     | `float` (f64)     | Direct conversion                                                                 |
| **String**                | String                  | `"text"`                                                                                                   | ✅ **Fully Supported**     | `string`          | Direct conversion                                                                 |
| **Object**                | Document                | `{"key": "value"}`                                                                                         | ✅ **Fully Supported**     | `object`          | Recursively processed as nested object                                            |
| **Array**                 | Array                   | `[1, 2, 3]`                                                                                                | ✅ **Fully Supported**     | `array`           | Recursively processed, nested types converted                                     |
| **Binary ata**            | Binary                  | `{"$binary": {"base64": "...", "subType": "..."}}`                                                         | ✅ **Fully Supported**     | `bytes`           | Direct conversion to bytes                                                        |
| **Undefined**             | Undefined               | `{"$undefined": true}`                                                                                     | ✅ **Fully Supported**     | `none`            | See [None and null](https://surrealdb.com/docs/surrealql/datamodel/none-and-null) |
| **ObjectId**              | ObjectId                | `{"$oid": "507f1f77bcf86cd799439011"}`                                                                     | ✅ **Fully Supported**     | `string`          | Converted to string, used for SurrealDB record IDs                                |
| **Boolean**               | Boolean                 | `true`/`false`                                                                                             | ✅ **Fully Supported**     | `bool`            | Direct conversion                                                                 |
| **Date**                  | DateTime                | `{"$date": "2024-01-01T00:00:00Z"}` (Relaxed) or `{"$date": {"$numberLong": "1672531200000"}}` (Canonical) | ✅ **Fully Supported**     | `datetime`        | Converted using chrono                                                            |
| **Null**                  | Null                    | `null`                                                                                                     | ✅ **Fully Supported**     | `null`            | See [None and null](https://surrealdb.com/docs/surrealql/datamodel/none-and-null) |
| **Regular Expression**    | RegularExpression       | `{"$regularExpression": {"pattern": "...", "options": "..."}}`                                             | ✅ **Fully Supported**     | `regex`           | Converted to string format "(?<options>)<pattern>"                                |
| **DBPointer**             | DbPointer               | `{"$dbPointer": {"$ref": "...", "$id": {...}}}`                                                            | 🔶 **Partially Supported** | `string`          | Stored as "$dbPointer" string (deprecated type)                                   |
| **JavaScript**            | JavaScriptCode          | `{"$code": "function(){}"}`                                                                                | ✅ **Fully Supported**     | `string`          | Code converted to string                                                          |
| **Symbol**                | Symbol                  | `{"$symbol": "text"}`                                                                                      | ✅ **Fully Supported**     | `string`          | Direct conversion to string                                                       |
| **JavaScript with scope** | JavaScriptCodeWithScope | `{"$code": "...", "$scope": {...}}`                                                                        | ✅ **Fully Supported**     | `object`          | Stored as `{"$code": CODE, "$scope": SCOPE}` object                               |
| **32-bit integer**        | Int32                   | `42` (Relaxed) or `{"$numberInt": "42"}` (Canonical)                                                       | ✅ **Fully Supported**     | `int`             | Converted to 64-bit integer                                                       |
| **Timestamp**             | Timestamp               | `{"$timestamp": {"t": 1672531200, "i": 1}}`                                                                | ✅ **Fully Supported**     | `datetime`        | Converted using timestamp seconds, increment as nanoseconds                       |
| **64-bit integer**        | Int64                   | `{"$numberLong": "123"}`                                                                                   | ✅ **Fully Supported**     | `int`             | Direct conversion                                                                 |
| **Decimal128**            | Decimal128              | `{"$numberDecimal": "123.45"}`                                                                             | ✅ **Fully Supported**     | `number`          | Converted to SurrealDB Number type with full precision                            |
| **DBRef**                 | Document                | `{"$ref": "users", "$id": "123"}`                                                                          | ✅ **Fully Supported**     | `record`          | Converted to SurrealDB record ID                                                  |
| **Min key**               | MinKey                  | `{"$minKey": 1}`                                                                                           | 🔶 **Partially Supported** | `object`          | Stored as `{"$minKey": 1}` object, loses special ordering                         |
| **Max key**               | MaxKey                  | `{"$maxKey": 1}`                                                                                           | 🔶 **Partially Supported** | `object`          | Stored as `{"$maxKey": 1}` object, loses special ordering                         |

## Support Status Definitions

- ✅ **Fully Supported**: The data type is converted with complete semantic preservation and no data loss
- 🔶 **Partially Supported**: The data is preserved but may lose some type-specific semantics, precision, or functionality
- ❌ **Not Supported**: The data type cannot be migrated (this status is not currently used as all data is preserved in some form)

## Limitations and Considerations

### Type Conversion Notes

- **JavaScript Code**: Preserved as string but loses executable nature. This can then be manually added to SurrealQL statements which allow [scripting](https://surrealdb.com/docs/surrealql/functions/script) if the capability [is enabled](https://surrealdb.com/docs/surrealdb/cli/start#enabling-capabilities).
- **JavaScript with Scope**: Preserved as an object containing code and scope but loses executable nature
- **Special Keys (MinKey/MaxKey)**: Converted to special marker objects, lose their ordering behavior
- **DBPointer**: Deprecated MongoDB type, stored as marker string, loses its content
- **Symbol**: Converted to regular string, loses symbol semantics

### Special Limitations

- **DBRef**: When the document contains both `$ref` and `$id` fields, it is converted to a SurrealDB RecordID. Otherwise treated as a regular document.
- **Decimal128**: Conversion may fail if the decimal string cannot be parsed as a SurrealDB Number.

## Testing and Validation

See `/tests/e2e_mongodb.rs` for test examples demonstrating the conversion of various MongoDB data types.

## References

- Refer to https://www.mongodb.com/docs/manual/reference/bson-types for all the documented MongoDB BSON data types
