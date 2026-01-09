# Distributed Load Testing for Surreal-Sync

A container-based distributed load testing system for surreal-sync, supporting
both Docker Compose and Kubernetes deployments with configurable resource limits
and memory-backed storage.

## Architecture Overview

```
Separate CLI Binary: surreal-loadtest
                |
                v
+----------------------------------+
|    loadtest-distributed crate    |
|  - Config generation (Rust)      |
|  - Work partitioning (by table)  |
|  - Results aggregation           |
|  - Environment verification      |
+----------------------------------+
         |                |
         v                v
  Docker Compose    Kubernetes
  (deploy.resources) (resources.limits)
  (tmpfs: mount)    (emptyDir: Memory)
         |                |
         +-------+--------+
                 v
+----------------------------------+
|   Source Database Containers     |
|  (auto-deployed via compose/k8s) |
+----------------------------------+
                 |
                 v
+----------------------------------+
|   Worker Containers              |
|  - Startup env verification logs |
|  - Write metrics to shared volume|
+----------------------------------+
                 |
                 v
+----------------------------------+
|   Results Aggregation            |
|  (collect from shared volume)    |
+----------------------------------+
```

## Quick Start

### Docker Compose

```bash
# Generate loadtest configuration
surreal-loadtest generate \
  --platform docker-compose \
  --source mysql \
  --preset medium \
  --schema ./schema.yaml \
  --output-dir ./loadtest-output

# Run the load test
cd loadtest-output
docker-compose -f docker-compose.loadtest.yml up

# View results
surreal-loadtest aggregate --results-dir ./results
```

### Kubernetes

```bash
# Generate K8s manifests
surreal-loadtest generate \
  --platform kubernetes \
  --source mongodb \
  --preset large \
  --schema ./schema.yaml \
  --output-dir ./k8s-loadtest

# Deploy
kubectl apply -f ./k8s-loadtest/kubernetes/

# Wait for completion and aggregate results
kubectl cp loadtest/loadtest-results:/results ./results
surreal-loadtest aggregate --results-dir ./results
```

## CLI Commands

### generate

Generate Docker Compose or Kubernetes configurations for distributed load testing.

```bash
surreal-loadtest generate
  --preset <small|medium|large>     # Resource preset
  --platform <docker-compose|kubernetes|both>
  --source <mysql|postgresql|mongodb|neo4j|kafka|csv|jsonl>
  --workers <N>                     # Number of workers (default: from preset)
  --output-dir <path>               # Output directory
  --tmpfs                           # Enable ramdisk for data directories
  --tmpfs-size <size>               # e.g., "2g", "512m"
  --cpu-limit <cores>               # e.g., "2.0" (per worker)
  --memory-limit <size>             # e.g., "4Gi" (per worker)
  --schema <path>                   # Loadtest schema YAML
  --row-count <N>                   # Rows per table
  --batch-size <N>                  # Batch size for inserts
```

### run

Generate and immediately run (Docker Compose only).

```bash
surreal-loadtest run [same options as generate]
```

### aggregate

Aggregate results from completed workers.

```bash
surreal-loadtest aggregate
  --results-dir <path>
  --output-format <json|table|markdown>
```

### worker

Run a single worker (used inside containers, not typically called directly).

```bash
surreal-loadtest worker
  --worker-id <id>
  --schema <path>
  --source <mysql|postgresql|mongodb|...>
  --tables <table1,table2,...>
  --row-count <N>
  --seed <N>
  --metrics-output <path>
  --connection-string <...>
  --batch-size <N>
```

## Presets

| Preset | Workers | CPU/Worker | Memory/Worker | tmpfs Size | Row Count | Batch Size |
|--------|---------|------------|---------------|------------|-----------|------------|
| small  | 2       | 0.5        | 512Mi         | 256Mi      | 10,000    | 500        |
| medium | 4       | 1.0        | 1Gi           | 1Gi        | 100,000   | 1,000      |
| large  | 8       | 2.0        | 4Gi           | 4Gi        | 1,000,000 | 5,000      |

## Resource Limits

| Feature        | Docker Compose                        | Kubernetes                           |
|----------------|---------------------------------------|--------------------------------------|
| CPU Limit      | `deploy.resources.limits.cpus`        | `resources.limits.cpu`               |
| Memory Limit   | `deploy.resources.limits.memory`      | `resources.limits.memory`            |
| tmpfs Mount    | `tmpfs: ["/path:size=1g"]`            | `emptyDir: {medium: Memory}`         |

## Worker Environment Verification

Each worker logs runtime environment info at startup to verify resource configuration:

```
=== Runtime Environment Verification ===
CPU cores visible: 2
Total memory: 2048 MB
Available memory: 1856 MB
/data: tmpfs (tmpfs: true)
  tmpfs size: 1024 MB
/results: ext4 (tmpfs: false)
cgroup memory limit: 2147483648
cgroup CPU quota: 200000 100000
========================================
```

This verifies:
- CPU cores visible to the container
- Memory available and limits
- Storage type (tmpfs vs disk) for each mount point
- cgroup resource limits (confirms container constraints are active)

## Metrics Collection

Workers write JSON metrics to a shared volume:

```
Worker-1 --> /results/worker-1.json --|
Worker-2 --> /results/worker-2.json --|--> Shared Volume --> Aggregator
Worker-3 --> /results/worker-3.json --|
```

### Metrics JSON Schema

```json
{
  "worker_id": "worker-1",
  "hostname": "loadtest-worker-1-abc123",
  "started_at": "2024-01-15T10:00:00Z",
  "completed_at": "2024-01-15T10:05:32Z",
  "success": true,
  "environment": {
    "cpu_cores": 2,
    "memory_mb": 2048,
    "tmpfs_enabled": true,
    "tmpfs_size_mb": 1024
  },
  "tables_processed": ["users", "orders"],
  "operation": "populate",
  "metrics": {
    "rows_processed": 50000,
    "duration_ms": 332000,
    "batch_count": 500,
    "rows_per_second": 150.6,
    "bytes_written": 12500000
  },
  "errors": [],
  "verification_report": null
}
```

### Aggregation Output

```
┌──────────┬─────────┬─────────────┬────────────┬───────────┐
│ Worker   │ Tables  │ Rows        │ Duration   │ Rows/sec  │
├──────────┼─────────┼─────────────┼────────────┼───────────┤
│ worker-1 │ users   │ 50,000      │ 5m 32s     │ 150.6     │
│ worker-2 │ orders  │ 50,000      │ 4m 15s     │ 196.1     │
│ worker-3 │ products│ 50,000      │ 6m 01s     │ 138.4     │
├──────────┼─────────┼─────────────┼────────────┼───────────┤
│ TOTAL    │ 3       │ 150,000     │ 6m 01s*    │ 485.1†    │
└──────────┴─────────┴─────────────┴────────────┴───────────┘
* Wall clock (parallel)  † Aggregate throughput
```

## Source Database Deployment

Generated configurations include full database deployment with optimized settings.

### Database-Specific Configurations

**MySQL:**
- `max_connections=200` (high concurrency)
- `innodb_buffer_pool_size` scaled to memory limit
- Binary logging disabled (`skip-log-bin`)
- Optional tmpfs for datadir

**PostgreSQL:**
- `max_connections=200`
- `shared_buffers` scaled to memory
- WAL level minimal (no replication during loadtest)
- `synchronous_commit=off` for faster writes
- Optional tmpfs for data

**MongoDB:**
- Replica set mode (required for change streams)
- Write concern: `{w: 1}` (faster)
- Journal disabled for loadtest
- Optional tmpfs for dbpath

**Kafka:**
- Single-node KRaft mode (no Zookeeper)
- Multiple partitions for parallelism
- Retention minimal (loadtest data disposable)

**Neo4j:**
- APOC procedures enabled
- Memory limits configured via `NEO4J_dbms_memory_*`

## Design Decisions

### Table-Based Partitioning

Work is distributed by table assignment (each worker handles different tables), not by row ranges.

**Rationale:**
- Simpler implementation - no overlapping writes to same table
- Natural fit for schemas with multiple tables
- Each worker uses a different seed, ensuring determinism
- Avoids coordination complexity of range-based splits
- Matches typical load test scenarios

**Example with 3 workers and 3 tables:**
```
Worker-1: users (seed=42)
Worker-2: orders (seed=43)
Worker-3: products (seed=44)
```

If more tables than workers: round-robin distribution.
If more workers than tables: extra workers are assigned additional tables.

### Maintainability Strategy

Both Docker Compose and Kubernetes configs are generated from the same Rust data structures:

```rust
pub trait ConfigGenerator {
    fn generate(&self, config: &ClusterConfig) -> Result<String>;
    fn filename(&self) -> &str;
}

impl ConfigGenerator for DockerComposeGenerator { ... }
impl ConfigGenerator for KubernetesGenerator { ... }
```

This ensures:
1. Single source of truth in Rust types
2. Both platforms always have identical worker configurations
3. Changes to config types require updating both generators

## Project Structure

```
crates/loadtest-distributed/
├── Cargo.toml              # [[bin]] name = "surreal-loadtest"
├── README.md               # This file
├── src/
│   ├── main.rs             # CLI entry point
│   ├── lib.rs              # Library exports
│   ├── cli.rs              # CLI argument structs (clap)
│   ├── config.rs           # ClusterConfig, WorkerConfig, ResourceLimits
│   ├── partitioner.rs      # Work distribution by table
│   ├── preset.rs           # small/medium/large presets
│   ├── metrics.rs          # WorkerMetrics JSON schema
│   ├── aggregator.rs       # Results aggregation
│   ├── environment.rs      # Runtime env verification
│   ├── worker.rs           # Worker execution logic
│   └── generator/
│       ├── mod.rs          # ConfigGenerator trait
│       ├── docker_compose.rs
│       ├── kubernetes.rs
│       └── databases/      # Database-specific configs
│           ├── mod.rs
│           ├── mysql.rs
│           ├── postgresql.rs
│           ├── mongodb.rs
│           ├── neo4j.rs
│           ├── kafka.rs
│           └── csv_jsonl.rs
```

## Building

```bash
# Build the surreal-loadtest binary
cargo build --release --bin surreal-loadtest

# Build Docker image
docker build -f loadtest/Dockerfile -t surreal-loadtest:latest .
```

## Dependencies

This crate uses existing loadtest infrastructure:
- `loadtest-generator` - Deterministic data generation
- `loadtest-populate-mysql` - MySQL populator
- `loadtest-populate-postgresql` - PostgreSQL populator
- `loadtest-populate-mongodb` - MongoDB populator
- `loadtest-populate-neo4j` - Neo4j populator
- `loadtest-populate-csv` - CSV file generator
- `loadtest-populate-jsonl` - JSONL file generator
- `loadtest-populate-kafka` - Kafka message producer
- `sync-core` - Schema definitions

## Schema File Format

The loadtest schema uses the same format as surreal-sync's generator schema:

```yaml
tables:
  - name: users
    primary_key: id
    columns:
      - name: id
        data_type: integer
        primary_key: true
      - name: name
        data_type: name
      - name: email
        data_type: email
      - name: created_at
        data_type: timestamp
  - name: orders
    primary_key: id
    columns:
      - name: id
        data_type: uuid
        primary_key: true
      - name: user_id
        data_type: integer
      - name: amount
        data_type: decimal
      - name: status
        data_type:
          enum:
            - pending
            - completed
            - cancelled
```

## License

Same as surreal-sync project.
