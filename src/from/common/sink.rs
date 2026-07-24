//! Helpers for constructing SurrealDB sinks with config-driven options.

use surreal_sync_core::ZeroTemporalPolicy;

/// Build a SurrealDB v2 sink with the given zero-temporal policy.
pub fn make_surreal2_sink(
    client: surreal_sync_surreal::v2::SurrealClient,
    zero_temporal: ZeroTemporalPolicy,
) -> surreal_sync_surreal::v2::Surreal2Sink {
    surreal_sync_surreal::v2::Surreal2Sink::with_zero_temporal_policy(client, zero_temporal)
}

/// Build a SurrealDB v3 sink with the given zero-temporal policy.
pub fn make_surreal3_sink(
    client: surreal_sync_surreal::v3::SurrealClient,
    zero_temporal: ZeroTemporalPolicy,
) -> surreal_sync_surreal::v3::Surreal3Sink {
    surreal_sync_surreal::v3::Surreal3Sink::with_zero_temporal_policy(client, zero_temporal)
}
