# MongoDB Data Types Support in surreal-sync

This document provides an overview of MongoDB data type support in surreal-sync, detailing which types are supported during migration from MongoDB to SurrealDB.

surreal-sync converts MongoDB documents to SurrealDB records by processing BSON data through MongoDB's Extended JSON v2 format. The conversion handles all the MongoDB data types while maintaining data integrity where possible.

## Data Type Support Table

| MongoDB Data Type | BSON Type | JSON Extended Format v2 | Support Status | SurrealDB Mapping | Notes |
|-------------------|-----------|---------------------|----------------|-------------------|-------|
| **String** | String | `"text"` | ✅ **Fully Supported** | `string` | Direct conversion |
| **Number (Int32)** | Int32 | `42` (Relaxed) or `{"$numberInt": "42"}` (Canonical) | ✅ **Fully Supported** | `int` | Converted to 64-bit integer |
| **Number (Int64)** | Int64 | `{"$numberLong": "123"}` | ✅ **Fully Supported** | `int` | Explicitly handled with parsing |
| **Number (Double)** | Double | `3.14` (Relaxed) or `{"$numberDouble": "3.14"}` (Canonical) | ✅ **Fully Supported** | `float` (f64) | Supports both Relaxed and Canonical formats |
| **Number (Decimal128)** | Decimal128 | `{"$numberDecimal": "123.45"}` | ✅ **Fully Supported** | `number` (surrealdb::sql::Number) | Converted to SurrealDB Number type |
| **Boolean** | Boolean | `true`/`false` | ✅ **Fully Supported** | `bool` | Direct conversion |
| **Date** | DateTime | `{"$date": "2024-01-01T00:00:00Z"}` (Relaxed) or `{"$date": {"$numberLong": "1672531200000"}}` (Canonical) | ✅ **Fully Supported** | `datetime` | Supports both Relaxed and Canonical formats |
| **ObjectId** | ObjectId | `{"$oid": "507f1f77bcf86cd799439011"}` | ✅ **Fully Supported** | `string` | Converted to string, used for SurrealDB record IDs |
| **Array** | Array | `[1, 2, 3]` | ✅ **Fully Supported** | `array` | Recursively processed, nested types converted |
| **Object/Document** | Document | `{"key": "value"}` | ✅ **Fully Supported** | `object` | Recursively processed as nested object |
| **Null** | Null | `null` | ✅ **Fully Supported** | Null | Direct conversion |
| **Binary Data** | Binary | `{"$binary": {"base64": "...", "subType": "..."}}` | ✅ **Fully Supported** | `bytes` | Base64 decoded to bytes, invalid base64 falls back to string |
| **Regular Expression** | Regex | `{"$regex": "pattern"}` | 🔶 **Partially Supported** | `object` | Preserved as generic object with pattern and flags |
| **JavaScript Code** | Code | `{"$code": "function(){}"}` | 🔶 **Partially Supported** | `object` | Preserved as generic object, loses executable nature |
| **Timestamp** | Timestamp | `{"$timestamp": {"t": 1672531200, "i": 1}}` | ✅ **Fully Supported** | `datetime` | Converted using timestamp seconds |
| **MinKey** | MinKey | `{"$minKey": 1}` | 🔶 **Partially Supported** | `object` | Preserved as generic object, loses special ordering behavior |
| **MaxKey** | MaxKey | `{"$maxKey": 1}` | 🔶 **Partially Supported** | `object` | Preserved as generic object, loses special ordering behavior |
| **DBRef** | DBRef | `{"$ref": "collection", "$id": "..."}` | ✅ **Fully Supported** | `record` (surrealdb::sql::Thing) | Converted to SurrealDB Thing (collection:id) |
| **Symbol** | Symbol | `{"$symbol": "text"}` | 🔶 **Partially Supported** | `object` | Preserved as generic object, loses symbol type semantics |
| **Undefined** | Undefined | `{"$undefined": true}` | 🔶 **Partially Supported** | `object` | Preserved as generic object |

## Support Status Definitions

- ✅ **Fully Supported**: The data type is converted with complete semantic preservation and no data loss
- 🔶 **Partially Supported**: The data is preserved but may lose some type-specific semantics, precision, or functionality
- ❌ **Not Supported**: The data type cannot be migrated (this status is not currently used as all data is preserved in some form)

## Limitations and Considerations

### Not Yet Implemented Types

- **Timestamp**: TODO implementation planned (see src/mongodb.rs:307-308)
- **Regular Expression**: Loses executable nature and becomes a generic object. Use [string::matches](https://surrealdb.com/docs/surrealql/datamodel/regex) function with the pattern extracted from the object for pattern matching.
  [We will also have `regex` data type since SurrealDB v3](https://surrealdb.com/docs/surrealql/datamodel/regex#regex).
- **JavaScript Code**: Preserved as generic object but loses executable nature
- **Special Keys (MinKey/MaxKey)**: Lose their special ordering behavior in queries and comparisons
- **Symbol/DBRef**: Lose their specialized MongoDB semantics but preserve structural data

### Special Limitations

- **Negative Timestamps**: Dates before 1970 or after 9999 are not supported and will cause conversion errors
- **Invalid Binary Data**: Base64 decoding failures fall back to string representation with warning logs

## Testing and Validation

See `/tests/e2e_mongodb.rs` for test examples demonstrating the conversion of various MongoDB data types.
