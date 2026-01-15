//! Distributed load testing for surreal-sync.
//!
//! This crate provides container-based distributed load testing capabilities
//! for surreal-sync, supporting both Docker Compose and Kubernetes deployments.
//!
//! ## Features
//!
//! - **Docker Compose & Kubernetes**: Generate configurations for both platforms
//! - **Resource Limits**: Configure CPU and memory limits per container
//! - **tmpfs Support**: Use memory-backed storage to eliminate I/O bottlenecks
//! - **Multiple Databases**: Support for MySQL, PostgreSQL, MongoDB, Neo4j, Kafka, CSV, JSONL
//! - **Environment Verification**: Runtime logging of CPU, memory, and storage info
//! - **Metrics Aggregation**: Collect and aggregate results from all containers via HTTP
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                        surreal-sync CLI                              │
//! │   loadtest generate | loadtest aggregate-server | loadtest populate │
//! └─────────────────────────────────────────────────────────────────────┘
//!                                    │
//!                    ┌───────────────┴───────────────┐
//!                    ▼                               ▼
//!           ┌────────────────┐              ┌────────────────┐
//!           │ Docker Compose │              │  Kubernetes    │
//!           │   Generator    │              │   Generator    │
//!           └────────────────┘              └────────────────┘
//!                    │                               │
//!                    └───────────────┬───────────────┘
//!                                    ▼
//!           ┌─────────────────────────────────────────────────┐
//!           │               Source Database                    │
//!           │    (MySQL, PostgreSQL, MongoDB, Neo4j, Kafka)   │
//!           └─────────────────────────────────────────────────┘
//!                                    │
//!                    ┌───────────────┼───────────────┐
//!                    ▼               ▼               ▼
//!           ┌────────────┐  ┌────────────┐  ┌────────────┐
//!           │  Populate 1│  │  Populate 2│  │  Populate N│
//!           │ (table A)  │  │ (table B)  │  │ (table C)  │
//!           └────────────┘  └────────────┘  └────────────┘
//!                    │               │               │
//!                    └───────────────┼───────────────┘
//!                                    ▼ POST /metrics
//!           ┌─────────────────────────────────────────────────┐
//!           │              Aggregator Server                   │
//!           │       (HTTP server, collects metrics)            │
//!           └─────────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ```bash
//! # Generate Docker Compose configuration
//! surreal-sync loadtest generate \
//!   --platform docker-compose \
//!   --source mysql \
//!   --preset medium \
//!   --schema ./schema.yaml \
//!   --output-dir ./loadtest-output
//!
//! # Run the load test
//! cd loadtest-output
//! docker-compose -f docker-compose.loadtest.yml up
//! ```

pub mod aggregator;
pub mod aggregator_server;
pub mod cli;
pub mod config;
pub mod environment;
pub mod generator;
pub mod metrics;
pub mod partitioner;
pub mod preset;

pub use aggregator::*;
pub use aggregator_server::run_aggregator_server;
pub use cli::*;
pub use config::*;
pub use environment::*;
pub use generator::*;
pub use metrics::*;
pub use partitioner::*;
pub use preset::*;
