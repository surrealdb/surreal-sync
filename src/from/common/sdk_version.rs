//! SurrealDB SDK version detection and selection.

/// SDK version to use for SurrealDB operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdkVersion {
    V2,
    V3,
}

/// Get the SDK version to use, either from explicit user preference or auto-detection.
pub async fn get_sdk_version(endpoint: &str, explicit: Option<&str>) -> anyhow::Result<SdkVersion> {
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
