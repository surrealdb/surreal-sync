# JSONL Data Types Support in surreal-sync

This document provides an overview of JSONL data type support in surreal-sync, detailing which types are supported during migration from JSON Lines files to SurrealDB.

surreal-sync converts JSONL file records to SurrealDB records by processing JSON data types. The conversion handles all standard JSON data types while maintaining data integrity where possible.

## Data Type Support Table

|      JSON Data Type      |           JSON Representation            |      Support Status       |   SurrealDB Mapping    |                        Notes                        |
| ------------------------ | ---------------------------------------- | ------------------------- | ---------------------- | --------------------------------------------------- |
| **String**               | `"text"`                                 | ✅ **Fully Supported**     | `string`               | Direct conversion                                   |
| **Number**               | `42` or `3.14`                           | ✅ **Fully Supported**     | `int` or `float`       | Auto-detected as integer or float                   |
| **Boolean**              | `true`/`false`                           | ✅ **Fully Supported**     | `bool`                 | Direct conversion                                   |
| **null**                 | `null`                                   | ✅ **Fully Supported**     | `null`                 | Direct conversion                                   |
| **Array**                | `[1, 2, 3]`                              | ✅ **Fully Supported**     | `array`                | Recursively processed, nested types converted       |
| **Object**               | `{"key": "value"}`                       | ✅ **Fully Supported**     | `object`               | Recursively processed as nested object              |
| **ISO 8601 Date String** | `"2024-01-15T14:30:00Z"`                 | 🔶 **Partially Supported** | `string` or `datetime` | Stored as string, use conversion rules for datetime |
| **UUID String**          | `"550e8400-e29b-41d4-a716-446655440000"` | 🔶 **Partially Supported** | `string` or `uuid`     | Stored as string, use conversion rules for uuid     |
| **Base64 String**        | `"SGVsbG8gV29ybGQ="`                     | 🔶 **Partially Supported** | `string` or `bytes`    | Stored as string, use conversion rules for bytes    |

## Support Status Definitions

- ✅ **Fully Supported**: The data type is converted with complete semantic preservation and no data loss
- 🔶 **Partially Supported**: The data is preserved but may lose some type-specific semantics, precision, or functionality
- ❌ **Not Supported**: The data type cannot be migrated and will cause migration to fail if encountered

## Advanced Features

### Type Conversion Rules

JSONL source supports **conversion rules** to transform string data into appropriate SurrealDB types:

**Date/Time Conversion:**
```bash
surreal-sync sync jsonl \
  --source-uri /path/to/data \
  --to-namespace prod --to-database data \
  --rule 'created_at="2024-01-15T10:00:00Z",created_at datetime:created_at'
```

**Record Reference Conversion:**
```bash
surreal-sync sync jsonl \
  --source-uri /path/to/data \
  --to-namespace prod --to-database data \
  --rule 'parent_id="page1",parent_id pages:parent_id'
```

**Custom ID Field:**
```bash
surreal-sync sync jsonl \
  --source-uri /path/to/data \
  --to-namespace prod --to-database data \
  --id-field "custom_id"
```

### Conversion Rule Syntax

Format: `'field_name="expected_value",field_name target_type:target_value'`

**Available conversions:**
- **Record references**: `pages:page_id` → Creates SurrealDB Thing
- **Datetime**: `datetime:timestamp_field` → Converts ISO strings to datetime
- **Numbers**: `number:numeric_field` → Converts strings to numbers
- **Arrays**: `array:comma_separated_field` → Splits strings into arrays

## Example Conversions

### Basic JSON to SurrealDB

**Input JSONL:**
```json
{"id": "user1", "name": "Alice", "age": 30, "active": true}
{"id": "user2", "name": "Bob", "age": 25, "active": false}
```

**SurrealDB Result:**
```sql
-- Table: filename (based on JSONL filename)
-- Records:
user1: { name: "Alice", age: 30, active: true }
user2: { name: "Bob", age: 25, active: false }
```

### Advanced Conversion with Rules

**Input JSONL:**
```json
{"page_id": "page1", "title": "Home", "parent": "section1", "created": "2024-01-15T10:00:00Z"}
{"page_id": "page2", "title": "About", "parent": "section1", "created": "2024-01-15T11:00:00Z"}
```

**Command with conversion rules:**
```bash
surreal-sync sync jsonl \
  --source-uri /path/to/pages.jsonl \
  --to-namespace docs --to-database content \
  --id-field "page_id" \
  --rule 'parent="section1",parent sections:section1' \
  --rule 'created="2024-01-15T10:00:00Z",created datetime:created'
```

**SurrealDB Result:**
```sql
-- Table: pages
-- Records:
page1: {
  title: "Home",
  parent: sections:section1,  -- Record reference
  created: "2024-01-15T10:00:00Z"  -- Datetime
}
```

## Limitations and Considerations

### Type Conversion Notes

- **No Schema Validation**: JSON doesn't enforce types, inconsistent data may exist
- **String Ambiguity**: Cannot auto-detect intended types (dates, UUIDs, references)
- **Numeric Precision**: Very large numbers may lose precision in JSON representation
- **Nested Complexity**: Deeply nested structures preserved but may impact query performance

### Special Limitations

- **No Incremental Sync**: JSONL files are static, no change tracking possible
- **File-based Only**: Cannot track modifications, only full imports supported
- **Memory Usage**: Large JSONL files loaded into memory during processing
- **ID Requirements**: Each record must have identifiable unique field for SurrealDB record ID

### Conversion Rule Limitations

- **Manual Configuration**: Requires explicit rules for type conversions
- **String Matching**: Rules match exact string values, not patterns
- **Limited Types**: Only supports basic type conversions (datetime, references, numbers)
- **No Validation**: Invalid conversion rules may cause migration failures

## Testing and Validation

See `/tests/e2e_jsonl.rs` for test examples demonstrating the conversion of various JSON data types and conversion rules.

## References

- [JSON Data Types Specification](https://www.json.org/)
- [JSON Lines Format](https://jsonlines.org/)
- [SurrealDB Data Types](https://surrealdb.com/docs/surrealql/datamodel)