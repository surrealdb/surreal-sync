//! SDK version enumeration.

/// SurrealDB SDK version selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SurrealSdkVersion {
    /// SurrealDB SDK v2 (for v2 servers)
    V2,
    /// SurrealDB SDK v3 (for v3 servers)
    V3,
}

impl Default for SurrealSdkVersion {
    fn default() -> Self {
        // Default to v2 since most users are on v2 servers
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
