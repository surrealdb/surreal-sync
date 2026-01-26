# Kafka Source Usage Guide

The Kafka source in surreal-sync allows you to import Kafka messages into SurrealDB. This is an **incremental-only source** that consumes messages from Kafka topics and syncs them to SurrealDB tables.

## How It Works

The Kafka source acts as a Kafka consumer in a consumer group, subscribing to a specific topic in the cluster.

The specified Kafka topic becomes a table in SurrealDB, where each message from the topic becomes a record in the table, with configurable deduplication.

Each Kafka message must be encoded using Protobuf. The Kafka source decodes every message payload using Protobuf and converts it into SurrealDB Upsert queries, transforming the payloads into SurrealDB records.

**Deduplication:** The Kafka source provides two ID strategies for deduplication:
- **Message Key Strategy**: Use Kafka message keys as record IDs (base64 encoded)
- **Field Strategy**: Extract IDs from a field in the message payload (default: "id" field)

Payloads with the same IDs will be merged in SurrealDB (upsert behavior). This deduplication is especially useful when importing data from external services via Kafka, where the external service writes change logs to Kafka.

**Timeout-Based Consumption:** The consumer runs for a specified timeout (default: 1 hour) and then stops gracefully. This makes it suitable for batch processing or periodic sync runs. For continuous consumption, use process managers or container orchestration with automatic restarts.

**Schema-Aware Conversion:** Optionally provide a schema file for precise type mapping from Protobuf to SurrealDB types.

## Prerequisites

Before using the Kafka source, ensure you have:

1. **SurrealDB** running locally or accessible via network
2. **surreal-sync** available in your PATH
3. **Kafka cluster** with accessible brokers
4. **Protobuf schema file** (.proto file) for message decoding

## Command Structure

```bash
surreal-sync from kafka \
  # Source (Kafka) Settings
  --proto-path <PROTO_PATH> \
  --brokers <BROKERS> \
  --group-id <GROUP_ID> \
  --topic <TOPIC> \
  --message-type <MESSAGE_TYPE> \
  # Target (SurrealDB) Settings
  --to-namespace <TO_NAMESPACE> \
  --to-database <TO_DATABASE> \
  # Optional Behavior Settings
  [OPTIONS]
```

## Required Flags

| Flag | Description |
|------|-------------|
| `--proto-path <PATH>` | Path to the protobuf schema file (.proto) |
| `--brokers <BROKER[,BROKER,...]>` | Kafka brokers (comma-separated or multiple `--brokers` flags) |
| `--group-id <GROUP_ID>` | Consumer group ID for this sync process |
| `--topic <TOPIC>` | Kafka topic to consume from |
| `--message-type <MESSAGE_TYPE>` | Protobuf message type name (must be defined in .proto file) |
| `--to-namespace <NAMESPACE>` | Target SurrealDB namespace |
| `--to-database <DATABASE>` | Target SurrealDB database |

## Optional Flags

### Kafka Consumer Settings

| Flag | Default | Description |
|------|---------|-------------|
| `--buffer-size <SIZE>` | 1000 | Maximum decoded protobuf messages held in internal peek buffer |
| `--session-timeout-ms <MS>` | "30000" | Kafka session timeout in milliseconds |
| `--num-consumers <COUNT>` | 1 | Number of consumers in the consumer group to spawn |
| `--kafka-batch-size <COUNT>` | 100 | Number of decoded protobuf messages to fetch and process per batch |

### SurrealDB Connection Settings

| Flag | Default | Description |
|------|---------|-------------|
| `--surreal-endpoint <URL>` | "http://localhost:8000" | SurrealDB endpoint URL (env: SURREAL_ENDPOINT) |
| `--surreal-username <USER>` | "root" | SurrealDB username (env: SURREAL_USERNAME) |
| `--surreal-password <PASS>` | "root" | SurrealDB password (env: SURREAL_PASSWORD) |
| `--batch-size <COUNT>` | 1000 | Batch size for writing to SurrealDB (see note below) |
| `--dry-run` | false | Don't actually write data (testing mode) |

**Note on Batch Sizes:** There are TWO different batch size parameters:
- `--kafka-batch-size` controls how many decoded protobuf messages are fetched and processed per batch (affects Kafka consumer throughput and memory usage)
- `--batch-size` controls how many records are written to SurrealDB per batch (affects SurrealDB write performance)

### Behavior Settings

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout <DURATION>` | "1h" | How long to consume messages (e.g., "1h", "30m", "300s") before stopping |
| `--table-name <NAME>` | (topic name) | Table name in SurrealDB (defaults to topic name if not specified) |
| `--schema-file <PATH>` | (none) | Optional schema file for type-aware conversion |
| `--max-messages <COUNT>` | (none) | Maximum messages to process before exiting (useful for load testing) |

### ID Strategy Settings

These settings control how record IDs are determined for deduplication:

| Flag | Default | Description |
|------|---------|-------------|
| `--use-message-key-as-id` | false | Use Kafka message key as SurrealDB record ID (base64 encoded) |
| `--id-field <FIELD>` | "id" | Field name to extract from message payload as record ID (ignored if `--use-message-key-as-id` is set) |

## Understanding ID Strategy

### Why ID Strategy Matters

SurrealDB uses record IDs for upsert behavior. When a record with the same ID already exists, it gets updated instead of creating a duplicate. This is critical for deduplication when importing change logs or event streams.

### Two Approaches

**Approach 1: Use Kafka Message Key** (`--use-message-key-as-id`)

Uses the Kafka message key as the SurrealDB record ID.
- Message key bytes are base64 encoded to create the record ID
- Requires all messages to have keys set
- **Use when:** Kafka keys already represent unique entity identifiers (e.g., user IDs, order IDs)

Example:
```bash
surreal-sync from kafka \
  --proto-path ./schemas/event.proto \
  --brokers localhost:9092 \
  --group-id event-sync \
  --topic events \
  --message-type Event \
  --use-message-key-as-id \
  --to-namespace production \
  --to-database events
```

**Approach 2: Use Message Field** (`--id-field <name>`)

Extracts the ID from a specific field in the message payload.
- Default field name is "id" but can be customized
- Requires the specified field to exist in all messages
- **Use when:** IDs are embedded in the message payload itself

Example:
```bash
surreal-sync from kafka \
  --proto-path ./schemas/user.proto \
  --brokers localhost:9092 \
  --group-id user-sync \
  --topic users \
  --message-type User \
  --id-field user_uuid \
  --to-namespace production \
  --to-database users
```

## Understanding Timeout and Max Messages

### Timeout (`--timeout <DURATION>`)

Controls how long the consumer runs before stopping.
- **Format:** "1h", "30m", "300s", or plain seconds
- **Default:** "1h" (one hour)
- **Behavior:** After timeout expires, consumer stops gracefully and exits
- **Use for:** Batch processing or periodic sync runs

Example timeout values:
- `--timeout 6h` - Run for 6 hours
- `--timeout 30m` - Run for 30 minutes
- `--timeout 300s` - Run for 300 seconds

### Max Messages (`--max-messages <COUNT>`)

Exits immediately after processing a specific number of messages.
- Takes priority over timeout (exits as soon as count is reached)
- Useful for load testing when the exact message count is known
- Consumer will process exactly this many messages and then exit

Example:
```bash
surreal-sync from kafka \
  --proto-path ./schemas/test.proto \
  --brokers localhost:9092 \
  --group-id loadtest \
  --topic test-data \
  --message-type TestEvent \
  --max-messages 10000 \
  --to-namespace test \
  --to-database loadtest
```

## Schema-Aware Type Conversion (Limited Support)

The optional `--schema-file` parameter is an internal feature that allows you to specify how to convert Protobuf types to specific semantic data types and SurrealDB types.

The only schema-aware conversion currently implemented for the Kafka source is:
- **Protobuf `string` field → SurrealDB `object`** by parsing the string as JSON internally

This is useful when your Protobuf schema encodes JSON data as string fields, and you want them stored as proper JSON/object types in SurrealDB.
**Example:**
```bash
surreal-sync from kafka \
  --proto-path ./schemas/analytics.proto \
  --brokers localhost:9092 \
  --group-id analytics-sync \
  --topic analytics \
  --message-type AnalyticsEvent \
  --schema-file ./sync-schemas/analytics.yaml \
  --to-namespace production \
  --to-database analytics
```

**Need More Conversions?**

If your use case requires additional type conversions (e.g., `int32` 0/1 → `bool`, timestamp strings → datetime, etc.), please file a feature request at: https://github.com/surrealdb/surreal-sync/issues

If you are going to fork and/or contribute to this project for the addition, please refer to [the reverse conversion logic for the Kafka source](../crates/kafka-types/src/reverse.rs) for the current implementation.

## Usage Examples

### Example 1: Basic Kafka to SurrealDB Sync

Simple sync with defaults:

```bash
surreal-sync from kafka \
  --proto-path ./schemas/user.proto \
  --brokers localhost:9092 \
  --group-id surreal-sync-users \
  --topic user-events \
  --message-type UserEvent \
  --to-namespace production \
  --to-database analytics
```

### Example 2: Production Setup with Custom Settings

Production configuration with multiple brokers and custom timeouts:

```bash
surreal-sync from kafka \
  --proto-path ./schemas/order.proto \
  --brokers kafka1:9092,kafka2:9092,kafka3:9092 \
  --group-id order-sync-prod \
  --topic orders \
  --message-type OrderEvent \
  --to-namespace production \
  --to-database orders \
  --surreal-endpoint ws://surreal-cluster:8000 \
  --surreal-username admin \
  --surreal-password "$SURREAL_PASSWORD" \
  --timeout 6h \
  --kafka-batch-size 500 \
  --batch-size 2000 \
  --num-consumers 4
```

### Example 3: Using Environment Variables

Leverage environment variables for credentials:

```bash
export SURREAL_ENDPOINT="ws://localhost:8000"
export SURREAL_USERNAME="admin"
export SURREAL_PASSWORD="secure-password"

surreal-sync from kafka \
  --proto-path ./schemas/event.proto \
  --brokers localhost:9092 \
  --group-id event-sync \
  --topic events \
  --message-type Event \
  --to-namespace production \
  --to-database events
```

### Example 4: Custom Table Name

Override the default table name (which would be the topic name):

```bash
surreal-sync from kafka \
  --proto-path ./schemas/log.proto \
  --brokers localhost:9092 \
  --group-id log-sync \
  --topic application-logs \
  --message-type LogEntry \
  --table-name app_logs \
  --to-namespace production \
  --to-database logging
```

## Production Scenarios

### Run One surreal-sync Process Per Topic

When importing multiple Kafka topics to SurrealDB, run a separate Kafka source process for each topic. This approach adapts well to various advanced scenarios and provides better isolation and control by allowing you to:
- Use different consumer group IDs per topic
- Specify different message types and proto schema files
- Configure different batch sizes and timeouts per topic
- Scale each topic independently

### Scaling Considerations

**Consumer Groups:**
- Use `--num-consumers` to scale within a single consumer group
- Multiple consumers within the group will share the partition load
- Don't exceed the partition count (extra consumers will be idle)

**Partition Distribution:**
- Kafka distributes partitions across consumers in a group
- More consumers = better parallelism (up to partition count)
- Monitor partition assignment and consumer lag

### Running Continuously

For production deployments that need continuous consumption:

**Option 1: Process Manager**
- Use systemd, supervisord, or similar
- Configure automatic restart on failure
- Set longer timeout or omit timeout parameter

**Option 2: Container Orchestration**
- Deploy in Kubernetes, Docker Compose, etc.
- Configure restart policies (e.g., `restart: always`)
- Use health checks and readiness probes
- Set appropriate job run duration with `--timeout`

**Monitoring:**
- Set appropriate `--timeout` to control total job run duration (e.g., `6h` for a 6-hour batch job)
- Use `--max-messages` for validation and testing runs
- Monitor consumer lag via Kafka monitoring tools
- Track SurrealDB write throughput and query performance
- Alert on process failures and restarts

## Troubleshooting

### Consumer Group Offset Issues

**Problem:** "Failed to add partition offset: {details}" or "Failed to commit offset: {details}"

**Solutions:**
- Check if consumer group has permission to commit offsets
- Verify Kafka broker ACLs if security is enabled
- Review consumer logs for specific offset commit failures

**Problem:** Need to reprocess all messages from the beginning (data recovery, schema changes, etc.)

**Solutions:**

1. **Preferred: Use a new consumer group ID** (simplest approach):
   ```bash
   surreal-sync from kafka --group-id my-sync-v2 ...
   ```
   A new `--group-id` with no committed offsets automatically starts from the beginning.

2. **Alternative: Reset offsets for existing group** (if you must keep the same group ID):
   - Check current offsets:
     ```bash
     kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
       --group your-group-id \
       --describe
     ```
   - Reset to earliest (WARNING: will reprocess all messages):
     ```bash
     kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
       --group your-group-id \
       --reset-offsets --to-earliest \
       --topic your-topic \
       --execute
     ```

**Important notes:**
- Reprocessing causes duplicate writes to SurrealDB (though upsert behavior prevents duplicates, it's still wasteful)
- Consumer groups (specified via `--group-id`) track offsets per group - reusing a group ID resumes from last committed offset
- surreal-sync internally configures Kafka consumers with `auto.offset.reset=earliest`, so new groups without committed offsets start from the beginning

### Protobuf Schema Errors

**Problem:** "Message type not found: {message_type}" error

**Solutions:**
- Ensure `--message-type` exactly matches a type defined in your .proto file (case-sensitive)
- Verify the message type name doesn't include package prefix (use "User", not "mypackage.User")
- Check that the .proto file includes all necessary imports and dependencies

**Problem:** "Protobuf decode error: {details}" or "Protobuf parse error: {details}"

**Solutions:**
- Verify `--proto-path` points to the correct .proto file
- Ensure the .proto file syntax and contents are valid
- Verify the protobuf encoding version matches (proto2 vs proto3)

### Kafka Connection Issues

**Problem:** "Failed to create consumer: {details}" or "Failed to subscribe to topic: {details}"

**Solutions:**
- Verify broker addresses are correct and accessible from your network:
  ```bash
  # Test connectivity
  kafka-broker-api-versions.sh --bootstrap-server your-broker:9092
  ```
- Check network connectivity and firewall rules
- Ensure the topic exists in the Kafka cluster:
  ```bash
  kafka-topics.sh --bootstrap-server localhost:9092 --list
  ```
- Verify the consumer group ID is valid (alphanumeric and special chars like `-` and `_`)
- Increase `--session-timeout-ms` if network is slow or unreliable
- Check Kafka broker logs for connection issues

### ID Extraction Failures

**Problem:** "use_message_key_as_id is enabled but message has no key"

**Solutions:**
- Ensure your Kafka producer sets message keys for all messages
- Verify messages in the topic have keys using Kafka CLI tools:
  ```bash
  kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic your-topic --from-beginning \
    --property print.key=true
  ```
- If messages don't have keys, use `--id-field` instead of `--use-message-key-as-id`

**Problem:** "Message has no '{field_name}' field"

**Solutions:**
- Ensure the field specified by `--id-field` exists in ALL messages
- Check message payload structure using dry-run mode
- Verify field name spelling (case-sensitive)
- If field name varies, consider using message keys instead

**Problem:** "Cannot convert {value} to SurrealDB ID"

**Solutions:**
- Ensure ID field contains valid ID types (int32, int64, string, UUID)
- Check that ID values are not null, arrays, or complex objects
- SurrealDB IDs support: integers, strings, and UUIDs only

### Memory Issues with Large Batches

**Problem:** Out of memory errors or high memory usage

**Solutions:**
- Reduce `--kafka-batch-size` (surreal-sync: number of decoded protobuf messages to fetch and process per batch)
- Reduce `--buffer-size` (surreal-sync: max decoded protobuf messages held in internal peek buffer)
- Monitor memory usage during operation:
  ```bash
  # Linux
  top -p $(pgrep surreal-sync)
  # macOS
  top -pid $(pgrep surreal-sync)
  ```
- Consider splitting workload across multiple consumers with `--num-consumers`
- Adjust based on message size and available system memory

### SurrealDB Connection Issues

**Problem:** "Failed to connect to SurrealDB" or authentication errors

**Solutions:**
- Verify `--surreal-endpoint` is correct (use `ws://` or `wss://` for WebSocket)
- Check credentials with `--surreal-username` and `--surreal-password` (or their environemnt variable counterparts)
- Test SurrealDB connectivity:
  ```bash
  surreal sql --endpoint ws://... --username ... --password ...
  ```
- Verify namespace and database exist or can be created with the user

### Performance Optimization

**Slow ingestion rate:**
- Increase `--kafka-batch-size` to process more decoded messages per batch (trade-off: more memory for decoded protobuf data)
- Increase `--batch-size` for better SurrealDB write performance
- Use `--num-consumers` to parallelize consumption (up to partition count)
- Consider hardware resources (CPU, memory, network bandwidth)

**High CPU usage:**
- Reduce `--num-consumers` if oversubscribed
- Reduce batch sizes if processing is CPU-bound
- Check for inefficient Protobuf schemas (excessive nesting, large messages)

## Current Limitations

### Kafka Client Settings Flexibility

surreal-sync uses the Kafka client library (librdkafka) to become a Kafka consumer which is configured with only basic settings in [`crates/kafka-source/src/consumer.rs:88-94`](../crates/kafka-source/src/consumer.rs#L88-L94). All other librdkafka settings use their defaults.

See [librdkafka Configuration Documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) for complete details on all settings and their defaults.

If your use case requires tuning these Kafka client settings (e.g., for extremely large messages, memory-constrained environments, or specific latency requirements), please submit a feature request at: https://github.com/surrealdb/surreal-sync/issues
