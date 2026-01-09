# Kafka Source Usage Guide

The Kafka source in surreal-sync allows you to import Kafka messages into SurrealDB.

## How it works

The Kafka source acts as a Kafka consumer in a consumer group, that subscribes to a specific topic in the cluster.

The specified Kafka topic becomes a table in SurrealDB, where each message coming from the topic becomes a record in the table, with optional deduplication.

Each Kafka message must be encoded using Protobuf. The Kafka source tries to decode every Kafka message payload using Protobuf and turn it into SurrealDB Upsert queries, so that the payloads are converted to SurrealDB records.

There's also optional "deduplication" mechanism that deduplicates payloads by Kafka message keys, or the payloads' "id" fields, so that the payloads with the same IDs will be merged in SurrealDB. This deduplication is specially useful when you are importing data from external service via Kafka, where the external service writes change logs to Kafka.

To protobuf decoding, the Kafka source requires the local filesystem path to the .proto schema file, and the messasge type name that specifies one of the message types defined in the .proto schema file.

## Usage

```bash
surreal-sync from kafka [OPTIONS] --to-namespace <TO_NAMESPACE> --to-database <TO_DATABASE> <PROTO_PATH> [BROKERS]... <GROUP_ID> <TOPIC> <MESSAGE_TYPE> <BUFFER_SIZE> <SESSION_TIMEOUT_MS>

Arguments:
  <PROTO_PATH>          Proto file path
  [BROKERS]...          Kafka brokers
  <GROUP_ID>            Consumer group ID
  <TOPIC>               Topic to consume from
  <MESSAGE_TYPE>        Protobuf message type name
  <BUFFER_SIZE>         Maximum buffer size for peeked messages
  <SESSION_TIMEOUT_MS>  Session timeout in milliseconds

Options:
      --num-consumers <NUM_CONSUMERS>
          Number of consumers in the consumer group to spawn [default: 1]
      --batch-size <BATCH_SIZE>
          Batch size for processing messages [default: 100]
      --table-name <TABLE_NAME>
          Optional table name to use in SurrealDB (defaults to topic name)
      --to-namespace <TO_NAMESPACE>
          Target SurrealDB namespace
      --to-database <TO_DATABASE>
          Target SurrealDB database
      --surreal-endpoint <SURREAL_ENDPOINT>
          SurrealDB endpoint URL [env: SURREAL_ENDPOINT=] [default: http://localhost:8000]
      --surreal-username <SURREAL_USERNAME>
          SurrealDB username [env: SURREAL_USERNAME=] [default: root]
      --surreal-password <SURREAL_PASSWORD>
          SurrealDB password [env: SURREAL_PASSWORD=] [default: root]
      --batch-size <BATCH_SIZE>
          Batch size for data migration [default: 1000]
      --dry-run
          Dry run mode - don't actually write data
```

## Prerequisites

Before using Kafka source, ensure you have:
1. SurrealDB running locally or accessible via network
2. surreal-sync built and available in your PATH
3. The source Kafka nodes and cluster is up and running

To start SurrealDB locally:
```bash
surreal start --user root --pass root
```

## Production Scenarios

### Run one surreal-sync process per topic

- In case you are to import two or more Kafka topics to SurrealDB, run the Kafka source for each topic. This is so to simplify the usage of the Kafka source- by running a `surreal-sync from kafka` process per topic, you can differentiate the group IDs, topics, message types, and the proto schema files per topic, which enables it to adapt to whatever advanced scenarios we can imagine.
