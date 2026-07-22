//! Kafka source E2E tests
//!
//! Tests for Kafka incremental sync functionality. Kafka is a streaming-only source
//! that does not require full sync or checkpoint management.

mod incremental_sync_lib;
mod kafka_transforms_config_cli;
mod sasl_ssl_mtls_sync;
mod transforms_lib;
