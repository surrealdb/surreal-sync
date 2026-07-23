//! Helpers for constructing SurrealDB sinks with config-driven options.

use sync_core::ZeroTemporalPolicy;

/// Build a SurrealDB v2 sink with the given zero-temporal policy.
pub fn make_surreal2_sink(
    client: surreal2_sink::SurrealClient,
    zero_temporal: ZeroTemporalPolicy,
) -> surreal2_sink::Surreal2Sink {
    surreal2_sink::Surreal2Sink::with_zero_temporal_policy(client, zero_temporal)
}

/// Build a SurrealDB v3 sink with the given zero-temporal policy.
pub fn make_surreal3_sink(
    client: surreal3_sink::SurrealClient,
    zero_temporal: ZeroTemporalPolicy,
) -> surreal3_sink::Surreal3Sink {
    surreal3_sink::Surreal3Sink::with_zero_temporal_policy(client, zero_temporal)
}
