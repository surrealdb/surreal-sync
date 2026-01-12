# surreal-sync-kafka-source

A Kafka consumer library designed for `surreal-sync`'s specific use case of processing Kafka messages
whose values are encoded in protobuf.

Features:

- Runtime Protobuf Support: Parse `.proto` files at runtime and decode messages without code generation
- Field Introspection: List fields and extract values from decoded messages dynamically
- Consumer Groups: Spawn multiple consumers in the same consumer group
- Batch Processing: Process messages in batches with atomic commits

Please see [the example](examples/multi_consumer.rs) for the expected usage.

### Components

1. **ProtoParser** (`proto_parser.rs`): Parses `.proto` files and extracts message schemas
2. **ProtoDecoder** (`proto_decoder.rs`): Decodes protobuf messages using runtime schema
3. **KafkaConsumer** (`consumer.rs`): Low-level consumer with peek buffer and manual offsets
4. **KafkaClient** (`client.rs`): High-level API for spawning consumer tasks
