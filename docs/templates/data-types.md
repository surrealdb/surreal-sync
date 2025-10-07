# [SOURCE_DATABASE] Data Types Support in surreal-sync

This document provides an overview of [SOURCE_DATABASE] data type support in surreal-sync, detailing which types are supported during migration from [SOURCE_DATABASE] to SurrealDB.

surreal-sync converts [SOURCE_DATABASE] [DATA_FORMAT] to SurrealDB records by processing [SOURCE_DATABASE]'s [PROTOCOL/FORMAT] data types. The conversion handles all the [SOURCE_DATABASE] data types while maintaining data integrity where possible.

## Data Type Support Table

| [SOURCE_DATABASE] Data Type | [PROTOCOL] Type | [FORMAT/REPRESENTATION] | Support Status | SurrealDB Mapping | Notes |
|------------------------------|-----------------|-------------------------|----------------|-------------------|-------|
| **Boolean** | Boolean | `true`/`false` | ✅ **Fully Supported** | `bool` | Direct conversion |
| **Integer** | Integer | `42` | ✅ **Fully Supported** | `int` | Direct conversion |
| **Float** | Float | `3.14` | ✅ **Fully Supported** | `float` (f64) | Direct conversion |
| **String** | String | `"text"` | ✅ **Fully Supported** | `string` | Direct conversion |
| **Array/List** | Array/List | `[1, 2, 3]` | ✅ **Fully Supported** | `array` | Recursively processed, nested types converted |
| **Object/Map** | Object/Map | `{"key": "value"}` | ✅ **Fully Supported** | `object` | Recursively processed as nested object |
| **Null** | Null | `null` | ✅ **Fully Supported** | `null` | Direct conversion |
| **Date/DateTime** | DateTime | `YYYY-MM-DDTHH:MM:SSZ` | ✅ **Fully Supported** | `datetime` | Converted using chrono |
| **Binary Data** | Bytes | `[binary data]` | ✅ **Fully Supported** | `bytes` | Direct conversion |
| **[SPECIAL_TYPE_1]** | [PROTOCOL_TYPE] | `[FORMAT_EXAMPLE]` | 🔶 **Partially Supported** | `[SURREAL_TYPE]` | [CONVERSION_NOTES] |
| **[SPECIAL_TYPE_2]** | [PROTOCOL_TYPE] | `[FORMAT_EXAMPLE]` | ❌ **Not Supported** | - | [LIMITATION_EXPLANATION] |

## Support Status Definitions

- ✅ **Fully Supported**: The data type is converted with complete semantic preservation and no data loss
- 🔶 **Partially Supported**: The data is preserved but may lose some type-specific semantics, precision, or functionality
- ❌ **Not Supported**: The data type cannot be migrated and will cause migration to fail if encountered

## [SPECIAL_SECTION_1] Conversion Details

### [SPECIFIC_TYPE_CATEGORY]

[Detailed explanation of how specific complex types are converted]

**[TYPE_NAME] ([DETAILS]):**
```json
{
  "example": "converted format",
  "notes": "additional details"
}
```

[Usage examples with SurrealDB functions if applicable]

## Limitations and Considerations

### Type Conversion Notes

- **[TYPE_1]**: [Specific limitation or consideration]
- **[TYPE_2]**: [Specific limitation or consideration]
- **[TYPE_3]**: [Specific limitation or consideration]

### Special Limitations

- **[LIMITATION_1]**: [Detailed explanation]
- **[LIMITATION_2]**: [Detailed explanation]

## Testing and Validation

See `/tests/e2e_[source_database].rs` for test examples demonstrating the conversion of various [SOURCE_DATABASE] data types.

## References

- [Link to official SOURCE_DATABASE data type documentation]

---

## Template Usage Instructions

**To create a new data types document:**

1. **Replace placeholders:**
   - `[SOURCE_DATABASE]` → Database name (e.g., "MySQL", "PostgreSQL")
   - `[DATA_FORMAT]` → Data format (e.g., "documents", "rows", "nodes")
   - `[PROTOCOL/FORMAT]` → Connection protocol (e.g., "BSON", "Bolt protocol", "MySQL protocol")
   - `[PROTOCOL]` → Wire protocol type system
   - `[FORMAT/REPRESENTATION]` → How data appears in queries/exports

2. **Fill data type table:**
   - Add all relevant data types for the source database
   - Include native protocol types and common representations
   - Use appropriate support status indicators
   - Add conversion notes for special cases

3. **Add special sections as needed:**
   - Spatial/geometric types → Detailed conversion examples
   - Complex types → Object structure examples
   - Database-specific types → Special handling explanations

4. **Update testing reference:**
   - Point to actual test file for the source database
   - Include relevant test function names if helpful

5. **Add official documentation links:**
   - Link to authoritative source database documentation
   - Include version-specific notes if needed