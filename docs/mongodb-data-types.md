# MongoDB Data Types Support in surreal-sync

This document provides a comprehensive overview of MongoDB data type support in surreal-sync, detailing which types are supported during migration from MongoDB to SurrealDB.

## Overview

surreal-sync converts MongoDB documents to SurrealDB records by processing BSON data through MongoDB's Extended JSON v2 format. The conversion system handles the most commonly used MongoDB data types while maintaining data integrity where possible.

## Data Type Support Table

| MongoDB Data Type | BSON Type | JSON Extended Format | Support Status | SurrealDB Mapping | Notes |
|-------------------|-----------|---------------------|----------------|-------------------|-------|
| **String** | String | `"text"` | ✅ **Supported** | String | Direct conversion |
| **Number (Int32)** | Int32 | `42` | ✅ **Supported** | Int (i64) | Converted to 64-bit integer |
| **Number (Int64)** | Int64 | `{"$numberLong": "123"}` | ✅ **Supported** | Int (i64) | Explicitly handled with parsing |
| **Number (Double)** | Double | `3.14` | ✅ **Supported** | Float (f64) | Direct conversion |
| **Number (Decimal128)** | Decimal128 | `{"$numberDecimal": "123.45"}` | ✅ **Supported** | Decimal (arbitrary precision) | Converted to Decimal type in SurrealDB |
| **Boolean** | Boolean | `true`/`false` | ✅ **Supported** | Bool | Direct conversion |
| **Date** | DateTime | `{"$date": "2024-01-01T00:00:00Z"}` | ✅ **Supported** | DateTime | Converted to `DateTime<Utc>` using RFC3339 parsing |
| **ObjectId** | ObjectId | `{"$oid": "507f1f77bcf86cd799439011"}` | ✅ **Supported** | Thing | Creates SurrealDB Thing using `type::thing(table, '507f1f77bcf86cd799439011')` |
| **Array** | Array | `[1, 2, 3]` | ✅ **Supported** | Array | Recursively processed, nested types converted |
| **Object/Document** | Document | `{"key": "value"}` | ✅ **Supported** | Object | Recursively processed as nested object |
| **Null** | Null | `null` | ✅ **Supported** | Null | Direct conversion |
| **Binary Data** | BinData | `{"$binary": {...}}` | ❌ **Not Supported** | Object | Treated as generic object |
| **Regular Expression** | Regex | `{"$regex": "pattern"}` | ❌ **Not Supported** | Object | Treated as generic object |
| **JavaScript Code** | Code | `{"$code": "function(){}"}` | ❌ **Not Supported** | Object | Treated as generic object |
| **Timestamp** | Timestamp | `{"$timestamp": {...}}` | ❌ **Not Supported** | Object | Treated as generic object |
| **MinKey** | MinKey | `{"$minKey": 1}` | ❌ **Not Supported** | Object | Treated as generic object |
| **MaxKey** | MaxKey | `{"$maxKey": 1}` | ❌ **Not Supported** | Object | Treated as generic object |
| **DBRef** | DBRef | `{"$ref": "collection", "$id": "..."}` | ❌ **Not Supported** | Object | Treated as generic object |
| **Symbol** | Symbol | `{"$symbol": "text"}` | ❌ **Not Supported** | Object | Treated as generic object |
| **Undefined** | Undefined | `{"$undefined": true}` | ❌ **Not Supported** | Object | Treated as generic object |

## Limitations and Considerations

- MongoDB regular expressions lose their executable nature and become generic objects. Use [string::matches](https://surrealdb.com/docs/surrealql/datamodel/regex) function with the regexp pattern extracted from the object for pattern matching.
- Other special MongoDB types like MinKey, and MaxKey lose their special meaning and behavior when converted to SurrealDB. They become generic objects containing their structural data.

## Testing and Validation

See `/tests/e2e_mongodb.rs` for test examples demonstrating the conversion of various MongoDB data types.
