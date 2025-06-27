# MongoDB Data Types Support in surreal-sync

This document provides an overview of MongoDB data type support in surreal-sync, detailing which types are supported during migration from MongoDB to SurrealDB.

surreal-sync converts MongoDB documents to SurrealDB records by processing BSON data through MongoDB's Extended JSON v2 format. The conversion handles all the MongoDB data types while maintaining data integrity where possible.

## Data Type Support Table

| MongoDB Data Type | BSON Type | JSON Extended Format v2 | Support Status | SurrealDB Mapping | Notes |
|-------------------|-----------|---------------------|----------------|-------------------|-------|
| **String** | String | `"text"` | ✅ **Fully Supported** | `string` | Direct conversion |
| **Number (Int32)** | Int32 | `42` | ✅ **Fully Supported** | `int` | Converted to 64-bit integer |
| **Number (Int64)** | Int64 | `{"$numberLong": "123"}` | ✅ **Fully Supported** | `int` | Explicitly handled with parsing |
| **Number (Double)** | Double | `3.14` | ✅ **Fully Supported** | `float` (f64) | Direct conversion |
| **Number (Decimal128)** | Decimal128 | `{"$numberDecimal": "123.45"}` | 🔶 **Partially Supported** | `float` (f64) | Converted to f64, loses arbitrary precision |
| **Boolean** | Boolean | `true`/`false` | ✅ **Fully Supported** | `bool` | Direct conversion |
| **Date** | DateTime | `{"$date": "2024-01-01T00:00:00Z"}` | ✅ **Fully Supported** | `datetime` | Converted to `datetime` |
| **ObjectId** | ObjectId | `{"$oid": "507f1f77bcf86cd799439011"}` | 🔶 **Partially Supported** | `thing` | Converted to SurrealDB Thing like `type::thing(table, '507f1f77bcf86cd799439011')` |
| **Array** | Array | `[1, 2, 3]` | ✅ **Fully Supported** | `array` | Recursively processed, nested types converted |
| **Object/Document** | Document | `{"key": "value"}` | ✅ **Fully Supported** | `object` | Recursively processed as nested object |
| **Null** | Null | `null` | ✅ **Fully Supported** | Null | Direct conversion |
| **Binary Data** | BinData | `{"$binary": {...}}` | 🔶 **Partially Supported** | `object` | Preserved as generic object with all structural data |
| **Regular Expression** | Regex | `{"$regex": "pattern"}` | 🔶 **Partially Supported** | `object` | Preserved as generic object with pattern and flags |
| **JavaScript Code** | Code | `{"$code": "function(){}"}` | 🔶 **Partially Supported** | `object` | Preserved as generic object, loses executable nature |
| **Timestamp** | Timestamp | `{"$timestamp": {...}}` | 🔶 **Partially Supported** | `object` | Preserved as generic object with timestamp data |
| **MinKey** | MinKey | `{"$minKey": 1}` | 🔶 **Partially Supported** | `object` | Preserved as generic object, loses special ordering behavior |
| **MaxKey** | MaxKey | `{"$maxKey": 1}` | 🔶 **Partially Supported** | `object` | Preserved as generic object, loses special ordering behavior |
| **DBRef** | DBRef | `{"$ref": "collection", "$id": "..."}` | 🔶 **Partially Supported** | `object` | Preserved as generic object with reference data |
| **Symbol** | Symbol | `{"$symbol": "text"}` | 🔶 **Partially Supported** | `object` | Preserved as generic object, loses symbol type semantics |
| **Undefined** | Undefined | `{"$undefined": true}` | 🔶 **Partially Supported** | `object` | Preserved as generic object |

## Support Status Definitions

- ✅ **Fully Supported**: The data type is converted with complete semantic preservation and no data loss
- 🔶 **Partially Supported**: The data is preserved but may lose some type-specific semantics, precision, or functionality
- ❌ **Not Supported**: The data type cannot be migrated (this status is not currently used as all data is preserved in some form)

## Limitations and Considerations

### Partially Supported Types

- **Decimal128**: Converted to f64, which may lose precision for very large numbers or those requiring arbitrary precision arithmetic
- **ObjectId**: Loses ObjectId type semantics and becomes a string, but the unique identifier value is preserved
- **Regular Expression**: Loses executable nature and becomes a generic object. Use [string::matches](https://surrealdb.com/docs/surrealql/datamodel/regex) function with the pattern extracted from the object for pattern matching
- **Binary Data**: Preserved as generic object but loses efficient binary storage and operations
- **JavaScript Code**: Preserved as generic object but loses executable nature
- **Special Keys (MinKey/MaxKey)**: Lose their special ordering behavior in queries and comparisons
- **Timestamp/Symbol/DBRef**: Lose their specialized MongoDB semantics but preserve structural data

## Testing and Validation

See `/tests/e2e_mongodb.rs` for test examples demonstrating the conversion of various MongoDB data types.
