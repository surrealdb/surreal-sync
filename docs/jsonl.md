# JSONL Source Usage Guide

The JSONL source in surreal-sync allows you to import JSON Lines (JSONL) files into SurrealDB. Each JSONL file becomes a table in SurrealDB, and each line in the file becomes a document in that table.

## Overview

JSONL source is particularly useful for:
- Importing data from APIs that export in JSON (You might use `jq` to convert JSON to JSONL)
- Migrating from document-based systems like Notion
- Bulk loading structured JSON data, like configuration files or logs
- Converting references between documents into SurrealDB's record links (Things)

## Basic Usage

```bash
surreal-sync full jsonl \
  --source-uri /path/to/jsonl/directory \
  --to-namespace myns \
  --to-database mydb
```

## Prerequisites

Before using JSONL source, ensure you have:
1. SurrealDB running locally or accessible via network
2. surreal-sync built and available in your PATH

To start SurrealDB locally:
```bash
surreal start --user root --pass root
```

## Example: Importing Notion-like Data

Let's walk through an example using sample Notion-like data with pages, blocks, and databases.

### Sample JSONL Files

Create a directory with the following JSONL files:

**databases.jsonl**:
```json
{"id": "db1", "name": "Documentation", "description": "Main documentation database", "created_at": "2023-12-01T10:00:00Z", "properties": {"status": "active", "version": 1.0}}
{"id": "db2", "name": "API Docs", "description": "API reference documentation", "created_at": "2023-12-15T10:00:00Z", "properties": {"status": "beta", "version": 0.5}}
```

**pages.jsonl**:
```json
{"id": "page1", "title": "Getting Started", "content": "Welcome to our documentation", "parent": {"type": "database_id", "database_id": "db1"}, "created_at": "2024-01-01T10:00:00Z"}
{"id": "page2", "title": "Advanced Topics", "content": "Deep dive into advanced features", "parent": {"type": "page_id", "page_id": "page1"}, "created_at": "2024-01-02T10:00:00Z"}
{"id": "page3", "title": "API Reference", "content": "Complete API documentation", "parent": {"type": "database_id", "database_id": "db2"}, "created_at": "2024-01-03T10:00:00Z"}
```

**blocks.jsonl**:
```json
{"id": "block1", "type": "paragraph", "text": "This is a paragraph block", "parent": {"type": "page_id", "page_id": "page1"}, "order": 1}
{"id": "block2", "type": "heading", "text": "Introduction", "level": 1, "parent": {"type": "page_id", "page_id": "page1"}, "order": 2}
{"id": "block3", "type": "code", "text": "console.log('Hello World');", "language": "javascript", "parent": {"type": "page_id", "page_id": "page2"}, "order": 1}
{"id": "block4", "type": "list", "items": ["Item 1", "Item 2", "Item 3"], "parent": {"type": "block_id", "block_id": "block2"}, "order": 3}
```

### Running the Import

Use the following command to import the data with conversion rules for parent references:

```bash
surreal-sync sync jsonl \
  --source-uri /workspace/tests/test_data/jsonl \
  --to-namespace notion \
  --to-database docs \
  --surreal-endpoint http://surrealdb:8000 \
  --surreal-username root \
  --surreal-password root \
  --rule 'type="database_id",database_id databases:database_id' \
  --rule 'type="page_id",page_id pages:page_id' \
  --rule 'type="block_id",block_id blocks:block_id'
```

**Note**: Replace `http://surrealdb:8000` with `http://localhost:8000` if running SurrealDB locally on your machine.

If successful, the command will complete without output. You can verify the import worked by checking the database as shown in the next section.

### Understanding Conversion Rules

The `--rule` flag defines how to convert JSON objects into SurrealDB record links. The format is:
```
--rule 'type="TYPE_VALUE",ID_FIELD TARGET_TABLE:ID_FIELD'
```

For example:
- `type="page_id",page_id pages:page_id` means:
  - When a JSON object has `"type": "page_id"`
  - Take the value from the `page_id` field
  - Convert it to a record link like `pages:page1` if the `page_id` is `page1`.

### Verifying the Results

After running the import, you can verify the data using SurrealQL. You can use the SurrealDB CLI, web interface, or HTTP API to run these queries.

Using the SurrealDB CLI:
```bash
surreal sql --endpoint http://localhost:8000 --username root --password root --namespace notion --database docs
```

Or using curl:
```bash
curl -X POST http://localhost:8000/sql \
  -H "Accept: application/json" \
  -u "root:root" \
  -d 'USE NS notion DB docs; YOUR_QUERY_HERE;'
```

Here are some example queries to verify your data:

1. **Check all tables**:
```sql
INFO FOR DB;
```

2. **View all databases**:
```sql
SELECT * FROM databases;
```

Expected output:
```json
[
  {
    "id": "databases:db1",
    "name": "Documentation",
    "description": "Main documentation database",
    "created_at": "2023-12-01T10:00:00Z",
    "properties": {
      "status": "active",
      "version": 1.0
    }
  },
  {
    "id": "databases:db2",
    "name": "API Docs",
    "description": "API reference documentation",
    "created_at": "2023-12-15T10:00:00Z",
    "properties": {
      "status": "beta",
      "version": 0.5
    }
  }
]
```

3. **View pages with their parent references**:
```sql
SELECT id, title, parent FROM pages;
```

Expected output:
```json
[
  {
    "id": "pages:page1",
    "title": "Getting Started",
    "parent": "databases:db1"
  },
  {
    "id": "pages:page2",
    "title": "Advanced Topics",
    "parent": "pages:page1"
  },
  {
    "id": "pages:page3",
    "title": "API Reference",
    "parent": "databases:db2"
  }
]
```

4. **Query pages belonging to a specific database**:
```sql
SELECT * FROM pages WHERE parent = databases:db1;
```

5. **Find all blocks in a specific page**:
```sql
SELECT id, type, text FROM blocks WHERE parent = pages:page1;
```

Expected output:
```json
[
  {
    "id": "blocks:block1",
    "type": "paragraph",
    "text": "This is a paragraph block"
  },
  {
    "id": "blocks:block2",
    "type": "heading",
    "text": "Introduction"
  }
]
```

6. **Traverse relationships - find pages and their blocks**:
```sql
-- First, find pages in a database
SELECT * FROM pages WHERE parent = databases:db1;

-- Then, find blocks in those pages
SELECT * FROM blocks WHERE parent = pages:page1;
```

### Working with Relationships in SurrealDB

Since we've converted the parent references to Thing types, we can work with them as relationships in SurrealDB.

1. **Fetch the full parent record instead of just the reference**:
```sql
SELECT *, parent.* FROM pages:page2;
```

Result:
```json
{
  "id": "pages:page2",
  "title": "Advanced Topics",
  "content": "Deep dive into advanced features",
  "created_at": "2024-01-02T10:00:00Z",
  "parent": {
    "id": "pages:page1",
    "title": "Getting Started",
    "content": "Welcome to our documentation",
    "created_at": "2024-01-01T10:00:00Z",
    "parent": "databases:db1"
  }
}
```

2. **Fetch specific fields from the parent**:
```sql
SELECT id, title, parent.name as parent_name FROM pages;
```

3. **Find all pages with their parent database info**:
```sql
SELECT id, title, parent.name as database_name 
FROM pages 
WHERE parent.id IN (SELECT id FROM databases);
```

4. **Find pages and count their child blocks**:
```sql
SELECT p.id, p.title, 
       count((SELECT * FROM blocks WHERE parent = p.id)) as block_count
FROM pages p;
```

5. **Find blocks with their parent page title**:
```sql
SELECT id, type, text, parent.title as page_title 
FROM blocks 
WHERE parent.id IN (SELECT id FROM pages);
```

### Advanced Relationship Queries

For more complex traversals, you can use subqueries:

1. **Find all blocks in pages that belong to a specific database**:
```sql
SELECT * FROM blocks 
WHERE parent IN (
  SELECT id FROM pages WHERE parent = databases:db1
);
```

2. **Count content at each level**:
```sql
-- Count pages per database
SELECT id, name,
       (SELECT count() FROM pages WHERE parent = databases.id) as page_count
FROM databases;

-- Count blocks per page  
SELECT id, title,
       (SELECT count() FROM blocks WHERE parent = pages.id) as block_count  
FROM pages;
```

3. **Find orphaned records (blocks whose parent is another block)**:
```sql
SELECT b1.id, b1.type, b1.text, b2.type as parent_type
FROM blocks b1
WHERE b1.parent IN (SELECT id FROM blocks);
```

**Note**: SurrealDB's graph traversal syntax with `->` and `<-` operators works with explicitly defined graph edges (RELATE statements). The record links we're using here look more like embedded documents, although it works similar to traditional foreign keys combined with SQL joins and subqueries to traverse the relationships under the hood.

### Creating Graph Edges (Optional)

If you want to use SurrealDB's powerful graph traversal syntax, you can create explicit edges after importing:

```sql
-- Create parent edges from pages to databases
RELATE pages:page1->parent->databases:db1;
RELATE pages:page3->parent->databases:db2;

-- Create parent edges from blocks to pages
RELATE blocks:block1->parent->pages:page1;
RELATE blocks:block2->parent->pages:page1;
RELATE blocks:block3->parent->pages:page2;

-- Create parent edges from page to page
RELATE pages:page2->parent->pages:page1;

-- Create parent edge from block to block
RELATE blocks:block4->parent->blocks:block2;
```

After creating edges, you can use graph traversal:

```sql
-- Find all pages connected to a database
SELECT ->parent->pages FROM databases:db1;

-- Find all blocks connected to a page
SELECT ->parent->blocks FROM pages:page1;

-- Traverse multiple levels
SELECT ->parent->pages->parent->blocks FROM databases:db1;
```

However, for most use cases, the Thing references created during import are sufficient and simpler to work with.

## Custom ID Fields

By default, surreal-sync looks for an `id` field in each JSON object. You can specify a different field name:

```bash
surreal-sync sync jsonl \
  --source-uri /path/to/jsonl \
  --to-namespace myns \
  --to-database mydb \
  --id-field "item_id"
```

Example JSONL with custom ID field:
```json
{"item_id": "prod1", "name": "Widget", "price": 19.99}
{"item_id": "prod2", "name": "Gadget", "price": 29.99}
```

## Advanced Options

### Batch Size
Control how many records are processed at once:
```bash
--batch-size 500
```

### Dry Run
Test the import without actually writing data:
```bash
--dry-run
```

### Environment Variables
You can also use environment variables for configuration:
```bash
export SURREAL_ENDPOINT=http://localhost:8000
export SURREAL_USERNAME=root
export SURREAL_PASSWORD=root
export SOURCE_URI=/path/to/jsonl

surreal-sync sync jsonl \
  --to-namespace myns \
  --to-database mydb
```

## Tips and Best Practices

1. **File Naming**: Name your JSONL files exactly as you want your SurrealDB tables to be named.

2. **ID Values**: Ensure all documents have unique ID values within each file.

3. **Data Types**: JSONL source preserves JSON data types:
   - Numbers remain as integers or floats
   - Strings remain as strings
   - Arrays and objects are preserved
   - Booleans remain as booleans
   - Null values are preserved

4. **References**: Use conversion rules to maintain relationships between documents across different tables.

5. **Performance**: For large datasets, adjust the batch size based on your system's memory and SurrealDB's capacity.

## Troubleshooting

1. **Missing ID Field**: If you see "Missing ID field" errors, ensure your JSON objects have the ID field (default: "id") or specify the correct field with `--id-field`.

2. **Invalid Rule Format**: Conversion rules must follow the exact format. Check for proper quoting and spacing.

3. **File Not Found**: Ensure the source URI points to a directory containing `.jsonl` files, not individual files.

4. **Connection Issues**: Verify SurrealDB is running and accessible at the specified endpoint.