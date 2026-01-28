//! SDK version enumeration.
//!
//! Provides the `SurrealSdkVersion` enum for selecting between
//! SurrealDB SDK v2 and v3.

/// SurrealDB SDK version selection.
///
/// Used to select which SDK implementation to use at runtime.
/// The CLI entry point branches once based on this value, and
/// all subsequent code is monomorphized for that specific SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SurrealSdkVersion {
    /// SurrealDB SDK v2 (for v2 servers)
    ///
    /// Uses the "revision" WebSocket subprotocol.
    V2,
    /// SurrealDB SDK v3 (for v3 servers)
    ///
    /// Uses the "flatbuffers" WebSocket subprotocol.
    V3,
}

impl Default for SurrealSdkVersion {
    fn default() -> Self {
        // Default to v2 since most users are on v2 servers
        // and the WebSocket subprotocols are incompatible
        Self::V2
    }
}

impl std::fmt::Display for SurrealSdkVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V2 => write!(f, "v2"),
            Self::V3 => write!(f, "v3"),
        }
    }
}

impl std::str::FromStr for SurrealSdkVersion {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "v2" | "2" => Ok(Self::V2),
            "v3" | "3" => Ok(Self::V3),
            _ => Err(anyhow::anyhow!(
                "Invalid SDK version: '{s}'. Expected 'v2' or 'v3'"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_v2() {
        assert_eq!(SurrealSdkVersion::default(), SurrealSdkVersion::V2);
    }

    #[test]
    fn test_display() {
        assert_eq!(SurrealSdkVersion::V2.to_string(), "v2");
        assert_eq!(SurrealSdkVersion::V3.to_string(), "v3");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            "v2".parse::<SurrealSdkVersion>().unwrap(),
            SurrealSdkVersion::V2
        );
        assert_eq!(
            "V2".parse::<SurrealSdkVersion>().unwrap(),
            SurrealSdkVersion::V2
        );
        assert_eq!(
            "2".parse::<SurrealSdkVersion>().unwrap(),
            SurrealSdkVersion::V2
        );
        assert_eq!(
            "v3".parse::<SurrealSdkVersion>().unwrap(),
            SurrealSdkVersion::V3
        );
        assert_eq!(
            "V3".parse::<SurrealSdkVersion>().unwrap(),
            SurrealSdkVersion::V3
        );
        assert_eq!(
            "3".parse::<SurrealSdkVersion>().unwrap(),
            SurrealSdkVersion::V3
        );
    }

    #[test]
    fn test_from_str_invalid() {
        assert!("v4".parse::<SurrealSdkVersion>().is_err());
        assert!("invalid".parse::<SurrealSdkVersion>().is_err());
    }
}
