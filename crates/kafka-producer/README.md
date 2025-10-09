# Kafka Producer for Testing

This is a simple Kafka producer that publishes sample `UserEvent` protobuf messages to the `user-events` topic. It's designed to work with the `surreal-sync-kafka` consumer example.

## UserEvent Schema

```protobuf
syntax = "proto3";

package example;

message UserEvent {
    string user_id = 1;
    string event_type = 2;
    int64 timestamp = 3;
    string data = 4;
}
```

## Prerequisites

Start Kafka with Docker:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```

## Usage

### 1. Run the Producer

From the workspace root:

```bash
cargo run -p surreal-sync-kafka-producer
```

This will:
- Create the `user-events` topic with 3 partitions (if it doesn't exist)
- Publish 10 sample UserEvent messages with various event types:
  - login
  - signup
  - purchase
  - profile_update
  - logout
  - view

### 2. Run the Consumer

In another terminal, run the multi-consumer example:

```bash
cargo run -p surreal-sync-kafka --example multi_consumer
```

The consumer will:
- Connect to the same `user-events` topic
- Spawn 3 consumers in a consumer group
- Decode the protobuf messages
- Print the events to the console

## Sample Output

Producer:
```
Creating topic 'user-events' if it doesn't exist...
Topic 'user-events' already exists
Creating Kafka producer...
Publishing UserEvent messages to topic 'user-events'...
Published message 1: user=user_001, event=login, timestamp=1760008782
Published message 2: user=user_002, event=signup, timestamp=1760008782
Published message 3: user=user_001, event=purchase, timestamp=1760008782
...
Successfully published 10 messages to 'user-events'
```

Consumer:
```
Kafka client created successfully
Spawning 3 consumers in the same consumer group...
Consumers running. Press Ctrl+C to stop.
[Partition 1] User: user_003, Event: login, Timestamp: 1760008739, Data: User logged in from mobile app
[Partition 2] User: user_001, Event: login, Timestamp: 1760008738, Data: User logged in from browser
[Partition 1] User: user_003, Event: logout, Timestamp: 1760008739, Data: User logged out
...
```
