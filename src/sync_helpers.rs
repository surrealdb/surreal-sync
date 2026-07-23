//! Shared helpers for library sync entrypoints (SDK selection, sinks, duration).

use sync_core::ZeroTemporalPolicy;

/// SDK version to use for SurrealDB operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SdkVersion {
    V2,
    V3,
}

/// Get the SDK version to use, either from explicit user preference or auto-detection.
pub(crate) async fn get_sdk_version(
    endpoint: &str,
    explicit: Option<&str>,
) -> anyhow::Result<SdkVersion> {
    match explicit {
        Some("v2") => {
            tracing::info!("Using SurrealDB SDK v2 (explicitly specified)");
            Ok(SdkVersion::V2)
        }
        Some("v3") => {
            tracing::info!("Using SurrealDB SDK v3 (explicitly specified)");
            Ok(SdkVersion::V3)
        }
        Some(other) => {
            anyhow::bail!("Unknown SDK version: '{other}'. Valid values: v2, v3")
        }
        None => {
            tracing::debug!("Auto-detecting SurrealDB server version...");
            let detected = surreal_version::detect_server_version(endpoint).await?;
            let version = match detected {
                surreal_version::SurrealMajorVersion::V2 => SdkVersion::V2,
                surreal_version::SurrealMajorVersion::V3 => SdkVersion::V3,
            };
            tracing::info!(
                "Auto-detected SurrealDB server version: {detected}, using SDK {version:?}"
            );
            Ok(version)
        }
    }
}

/// Build a SurrealDB v2 sink with the given zero-temporal policy.
pub(crate) fn make_surreal2_sink(
    client: surreal2_sink::SurrealClient,
    zero_temporal: ZeroTemporalPolicy,
) -> surreal2_sink::Surreal2Sink {
    surreal2_sink::Surreal2Sink::with_zero_temporal_policy(client, zero_temporal)
}

/// Build a SurrealDB v3 sink with the given zero-temporal policy.
pub(crate) fn make_surreal3_sink(
    client: surreal3_sink::SurrealClient,
    zero_temporal: ZeroTemporalPolicy,
) -> surreal3_sink::Surreal3Sink {
    surreal3_sink::Surreal3Sink::with_zero_temporal_policy(client, zero_temporal)
}

/// Parse a duration string like "1h", "30m", "300s", "300" into seconds.
pub(crate) fn parse_duration_to_secs(s: &str) -> anyhow::Result<i64> {
    use anyhow::Context;

    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("Empty duration string");
    }

    if let Some(num_str) = s.strip_suffix('h') {
        let hours: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid hours value: {num_str}"))?;
        return Ok(hours * 3600);
    }
    if let Some(num_str) = s.strip_suffix('m') {
        let minutes: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid minutes value: {num_str}"))?;
        return Ok(minutes * 60);
    }
    if let Some(num_str) = s.strip_suffix('s') {
        let secs: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid seconds value: {num_str}"))?;
        return Ok(secs);
    }

    s.parse::<i64>()
        .with_context(|| format!("Invalid duration value: {s}"))
}
