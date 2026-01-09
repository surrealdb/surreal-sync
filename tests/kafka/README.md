# Kafka Source E2E Tests

This directory contains end-to-end tests for the Kafka incremental sync functionality.

## Overview

Kafka is a **streaming-only** data source with unique characteristics:

- ✅ **Incremental sync ONLY** - No full sync required (Kafka has no concept of "snapshots")
- ✅ **No checkpoint files** - Kafka manages consumer offsets internally
- ✅ **Real-time streaming** - Consumes messages as they arrive
- ✅ **Consumer groups** - Multiple consumers can process messages in parallel
- ✅ **Random topic names** - Each test run uses unique topic names to avoid conflicts and cleanup

## Tests

### `incremental_sync_lib.rs`

Comprehensive E2E test that validates Kafka incremental sync:

1. **Setup**: Creates Kafka topics for users, posts, and relations
2. **Publishing**: Publishes protobuf-encoded test messages using `surreal-sync-kafka-producer`
3. **Sync**: Runs incremental sync for each topic to consume messages
4. **Validation**: Verifies data is correctly synced to SurrealDB

**Key differences from other database tests:**
- No full sync phase (Kafka is streaming-only)
- No checkpoint emission or reading
- Each topic is synced independently with its own consumer group
- Uses deadline-based consumption (stops after specified time)

## Running the Tests

### Prerequisites

1. **Kafka running** in devcontainer:
   ```bash
   # Kafka should be available at kafka:9092
   docker ps | grep kafka
   ```

2. **SurrealDB running**:
   ```bash
   # SurrealDB should be available at surrealdb:8000
   docker ps | grep surrealdb
   ```

### Run the test

```bash
# Run all Kafka tests
cargo test --test kafka

# Run with debug output
RUST_LOG=debug cargo test --test kafka -- --nocapture

# Run specific test
cargo test --test kafka test_kafka_incremental_sync_lib
```

## Test Architecture

### Message Flow

```
Test Data → Protobuf Encoding → Kafka Topics → Consumer Groups → SurrealDB Tables
```

1. **Test data**: Defined in test helper functions (`publish_test_users`, etc.)
2. **Protobuf encoding**: Uses `surreal-sync-kafka-producer` library
3. **Kafka topics**: Randomly generated per test run (e.g., `test-users-abc123`, `test-posts-abc123`)
4. **Consumer groups**: Each topic has its own consumer group with unique ID
5. **SurrealDB tables**: Topic names become table names (e.g., `test-users-abc123` table)

### Test Isolation

Each test run generates unique topic names using the test ID:
- `test-users-{test_id}`
- `test-posts-{test_id}`
- `test-user-posts-{test_id}`

This approach provides:
- **No cleanup required**: Old topics can be retained or cleaned up separately
- **Parallel test runs**: Multiple tests can run simultaneously without conflicts
- **Debugging**: Test data remains in Kafka for post-mortem analysis
- **Fast execution**: No time wasted deleting/recreating topics

### Protobuf Schemas

The test uses the same protobuf schemas as the kafka-producer library:

- `user.proto` - User messages with metadata, preferences, and settings
- `post.proto` - Post messages with categories and timestamps
- `user_post_relation.proto` - Relation messages linking users and posts

### Sync Configuration

Each topic sync uses:
- **Consumer group**: Unique per test run to avoid offset conflicts
- **Session timeout**: 6 seconds (fast failure detection)
- **Batch size**: 100 messages per batch
- **Deadline**: 5 seconds (enough to consume test messages)
- **Buffer size**: 1000 messages

### Verification

The test verifies:
1. **Record count**: Correct number of records synced to each table
2. **Specific fields**: Sample field values (e.g., user name, age)
3. **Table names**: Kafka topics correctly mapped to SurrealDB tables

## Troubleshooting

### Kafka Connection Issues

```bash
# Check if Kafka is accessible
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# List all test topics
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092 | grep test-

# Delete old test topics (optional cleanup)
docker exec -it kafka kafka-topics.sh --delete --topic test-users-old-id --bootstrap-server localhost:9092
```

### Consumer Group Issues

If tests fail with "Consumer group not found":
- Each test run uses a unique consumer group ID (includes test_id)
- Old consumer groups are cleaned up automatically by Kafka after timeout

### Topic Cleanup (Optional)

Random topic names mean tests don't require cleanup, but you can manually clean up old test topics:

```bash
# List all test topics
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092 | grep "test-"

# Delete topics older than needed (example)
docker exec -it kafka kafka-topics.sh --delete --topic "test-users-old-id" --bootstrap-server localhost:9092
```

### Message Not Found

If verification fails:
- Check Kafka logs for message production
- Increase deadline duration if messages take longer to process
- Verify protobuf schema matches published messages

### SurrealDB Query Issues

If table queries fail:
- Kafka topic names with hyphens need to be quoted: `` `test-users` ``
- Record IDs are taken from message key or "id" field
- Check actual table names in SurrealDB: `INFO FOR DB;`

## Performance Considerations

### Test Speed

- Tests complete in ~10-15 seconds
- Most time spent waiting for Kafka metadata and message delivery
- Consumer deadline prevents tests from running indefinitely

### Parallelism

Multiple consumers can be spawned per topic:
```rust
num_consumers: 3,  // Spawn 3 consumers in the same group
```

This is useful for testing parallel message processing.

## Future Enhancements

Potential test improvements:

1. **Multiple message batches**: Test consuming large message volumes
2. **Schema evolution**: Test handling protobuf schema changes
3. **Error handling**: Test invalid message handling
4. **Offset management**: Test consumer offset commits
5. **Rebalancing**: Test consumer group rebalancing
6. **Dead letter queue**: Test failed message handling
