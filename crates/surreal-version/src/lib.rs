//! Auto-detect SurrealDB server version via HTTP.
//!
//! This crate provides utilities to detect the SurrealDB server version
//! by querying the `/version` HTTP endpoint. This is used to select the
//! appropriate SDK version (v2 or v3) since they use incompatible WebSocket
//! subprotocols.

use reqwest::Client;
use semver::Version;

/// Detected SurrealDB major version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SurrealMajorVersion {
    /// SurrealDB v2.x
    V2,
    /// SurrealDB v3.x
    V3,
}

impl std::fmt::Display for SurrealMajorVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V2 => write!(f, "v2"),
            Self::V3 => write!(f, "v3"),
        }
    }
}

/// Auto-detect SurrealDB server version via HTTP GET to /version endpoint.
///
/// Returns the major version (V2 or V3) for SDK selection.
///
/// # Arguments
///
/// * `endpoint` - The SurrealDB server endpoint (can be ws://, wss://, http://, or https://)
///
/// # Returns
///
/// * `Ok(SurrealMajorVersion)` - The detected major version
/// * `Err` - If the version could not be detected
///
/// # Example responses from /version:
///
/// - "surrealdb-2.4.1" -> V2
/// - "surrealdb-3.0.0-beta.2" -> V3
pub async fn detect_server_version(endpoint: &str) -> anyhow::Result<SurrealMajorVersion> {
    // Convert ws:// to http:// for version check
    let http_endpoint = endpoint
        .replace("ws://", "http://")
        .replace("wss://", "https://");

    let version_url = format!("{}/version", http_endpoint.trim_end_matches('/'));

    tracing::debug!("Detecting SurrealDB version at {version_url}");

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let response = client.get(&version_url).send().await.map_err(|e| {
        anyhow::anyhow!("Failed to fetch SurrealDB version from '{version_url}': {e}")
    })?;

    if !response.status().is_success() {
        let status = response.status();
        return Err(anyhow::anyhow!(
            "SurrealDB version endpoint returned status {status}: {version_url}"
        ));
    }

    let version_string = response.text().await.map_err(|e| {
        anyhow::anyhow!("Failed to read SurrealDB version response from '{version_url}': {e}")
    })?;

    parse_version_string(&version_string)
}

/// Parse a SurrealDB version string like "surrealdb-2.4.1" or "surrealdb-3.0.0-beta.2".
///
/// Returns the major version (V2 or V3).
pub fn parse_version_string(version_string: &str) -> anyhow::Result<SurrealMajorVersion> {
    let version_string = version_string.trim();

    tracing::debug!("Parsing SurrealDB version string: '{version_string}'");

    // Parse version string like "surrealdb-2.4.1" or "surrealdb-3.0.0-beta.2"
    let version_part = version_string.strip_prefix("surrealdb-").ok_or_else(|| {
        anyhow::anyhow!(
            "Invalid version format: '{version_string}'. Expected format: 'surrealdb-X.Y.Z'"
        )
    })?;

    // Parse semver (handles pre-release like -beta.2)
    let version = Version::parse(version_part)
        .map_err(|e| anyhow::anyhow!("Failed to parse SurrealDB version '{version_part}': {e}"))?;

    tracing::info!(
        "Detected SurrealDB server version: {} (major: {})",
        version,
        version.major
    );

    match version.major {
        2 => Ok(SurrealMajorVersion::V2),
        3 => Ok(SurrealMajorVersion::V3),
        _ => Err(anyhow::anyhow!(
            "Unsupported SurrealDB version: {version}. Only v2 and v3 are supported."
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_version_v2() {
        let version = parse_version_string("surrealdb-2.4.1").unwrap();
        assert_eq!(version, SurrealMajorVersion::V2);
    }

    #[test]
    fn test_parse_version_v2_minor() {
        let version = parse_version_string("surrealdb-2.3.7").unwrap();
        assert_eq!(version, SurrealMajorVersion::V2);
    }

    #[test]
    fn test_parse_version_v3_beta() {
        let version = parse_version_string("surrealdb-3.0.0-beta.2").unwrap();
        assert_eq!(version, SurrealMajorVersion::V3);
    }

    #[test]
    fn test_parse_version_v3_stable() {
        let version = parse_version_string("surrealdb-3.0.0").unwrap();
        assert_eq!(version, SurrealMajorVersion::V3);
    }

    #[test]
    fn test_parse_version_v3_with_whitespace() {
        let version = parse_version_string("  surrealdb-3.0.0-beta.2  \n").unwrap();
        assert_eq!(version, SurrealMajorVersion::V3);
    }

    #[test]
    fn test_parse_version_invalid_prefix() {
        let result = parse_version_string("surreal-2.4.1");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid version format"));
    }

    #[test]
    fn test_parse_version_invalid_semver() {
        let result = parse_version_string("surrealdb-invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to parse"));
    }

    #[test]
    fn test_parse_version_unsupported_v1() {
        let result = parse_version_string("surrealdb-1.0.0");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported SurrealDB version"));
    }

    #[test]
    fn test_parse_version_unsupported_v4() {
        let result = parse_version_string("surrealdb-4.0.0");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported SurrealDB version"));
    }

    #[test]
    fn test_version_display() {
        assert_eq!(SurrealMajorVersion::V2.to_string(), "v2");
        assert_eq!(SurrealMajorVersion::V3.to_string(), "v3");
    }
}
