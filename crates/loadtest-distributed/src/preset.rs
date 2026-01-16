//! Preset configurations for distributed load testing.

use crate::config::{ResourceLimits, TmpfsConfig};
use serde::{Deserialize, Serialize};

/// Preset size for load testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PresetSize {
    Small,
    Medium,
    Large,
}

impl std::fmt::Display for PresetSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PresetSize::Small => write!(f, "small"),
            PresetSize::Medium => write!(f, "medium"),
            PresetSize::Large => write!(f, "large"),
        }
    }
}

impl std::str::FromStr for PresetSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "small" | "s" => Ok(PresetSize::Small),
            "medium" | "m" => Ok(PresetSize::Medium),
            "large" | "l" => Ok(PresetSize::Large),
            _ => Err(format!("Unknown preset size: {s}")),
        }
    }
}

/// Complete preset configuration.
#[derive(Debug, Clone)]
pub struct Preset {
    pub size: PresetSize,
    pub num_containers: usize,
    pub container_resources: ResourceLimits,
    pub container_tmpfs: TmpfsConfig,
    pub database_resources: ResourceLimits,
    pub surrealdb_resources: ResourceLimits,
    pub row_count: u64,
    pub batch_size: u64,
}

impl Preset {
    /// Get preset by size.
    pub fn by_size(size: PresetSize) -> Self {
        match size {
            PresetSize::Small => Self::small(),
            PresetSize::Medium => Self::medium(),
            PresetSize::Large => Self::large(),
        }
    }

    /// Small preset: 2 containers, 10K rows.
    /// Optimized for 4-core runner: 2×0.5 + 1.0 + 1.0 = 3.0 cores
    pub fn small() -> Self {
        Self {
            size: PresetSize::Small,
            num_containers: 2,
            container_resources: ResourceLimits {
                cpu_limit: "0.5".to_string(),
                memory_limit: "512Mi".to_string(),
                cpu_request: Some("0.25".to_string()),
                memory_request: Some("256Mi".to_string()),
            },
            container_tmpfs: TmpfsConfig {
                size: "256Mi".to_string(),
                path: "/data".to_string(),
            },
            database_resources: ResourceLimits {
                cpu_limit: "1.0".to_string(),
                memory_limit: "1Gi".to_string(),
                cpu_request: Some("0.5".to_string()),
                memory_request: Some("512Mi".to_string()),
            },
            surrealdb_resources: ResourceLimits {
                cpu_limit: "1.0".to_string(),
                memory_limit: "1Gi".to_string(),
                cpu_request: Some("0.5".to_string()),
                memory_request: Some("512Mi".to_string()),
            },
            row_count: 10_000,
            batch_size: 500,
        }
    }

    /// Medium preset: 4 containers, 100K rows.
    /// Optimized for 8-core runner: 4×0.5 + 2.0 + 2.0 = 6.0 cores
    pub fn medium() -> Self {
        Self {
            size: PresetSize::Medium,
            num_containers: 4,
            container_resources: ResourceLimits {
                cpu_limit: "0.5".to_string(),
                memory_limit: "512Mi".to_string(),
                cpu_request: Some("0.25".to_string()),
                memory_request: Some("256Mi".to_string()),
            },
            container_tmpfs: TmpfsConfig {
                size: "512Mi".to_string(),
                path: "/data".to_string(),
            },
            database_resources: ResourceLimits {
                cpu_limit: "2.0".to_string(),
                memory_limit: "2Gi".to_string(),
                cpu_request: Some("1.0".to_string()),
                memory_request: Some("1Gi".to_string()),
            },
            surrealdb_resources: ResourceLimits {
                cpu_limit: "2.0".to_string(),
                memory_limit: "2Gi".to_string(),
                cpu_request: Some("1.0".to_string()),
                memory_request: Some("1Gi".to_string()),
            },
            row_count: 100_000,
            batch_size: 1000,
        }
    }

    /// Large preset: 8 containers, 1M rows.
    /// Optimized for 16-core runner: 8×0.625 + 5.0 + 5.0 = 15.0 cores
    pub fn large() -> Self {
        Self {
            size: PresetSize::Large,
            num_containers: 8,
            container_resources: ResourceLimits {
                cpu_limit: "0.625".to_string(),
                memory_limit: "512Mi".to_string(),
                cpu_request: Some("0.3".to_string()),
                memory_request: Some("256Mi".to_string()),
            },
            container_tmpfs: TmpfsConfig {
                size: "512Mi".to_string(),
                path: "/data".to_string(),
            },
            database_resources: ResourceLimits {
                cpu_limit: "5.0".to_string(),
                memory_limit: "8Gi".to_string(),
                cpu_request: Some("2.5".to_string()),
                memory_request: Some("4Gi".to_string()),
            },
            surrealdb_resources: ResourceLimits {
                cpu_limit: "5.0".to_string(),
                memory_limit: "8Gi".to_string(),
                cpu_request: Some("2.5".to_string()),
                memory_request: Some("4Gi".to_string()),
            },
            row_count: 1_000_000,
            batch_size: 5000,
        }
    }

    /// Create a custom preset with overrides.
    pub fn with_overrides(
        mut self,
        num_containers: Option<usize>,
        cpu_limit: Option<String>,
        memory_limit: Option<String>,
        tmpfs_size: Option<String>,
        row_count: Option<u64>,
        batch_size: Option<u64>,
    ) -> Self {
        if let Some(n) = num_containers {
            self.num_containers = n;
        }
        if let Some(cpu) = cpu_limit {
            self.container_resources.cpu_limit = cpu;
        }
        if let Some(mem) = memory_limit {
            self.container_resources.memory_limit = mem;
        }
        if let Some(tmpfs) = tmpfs_size {
            self.container_tmpfs.size = tmpfs;
        }
        if let Some(rows) = row_count {
            self.row_count = rows;
        }
        if let Some(batch) = batch_size {
            self.batch_size = batch;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preset_sizes() {
        let small = Preset::small();
        assert_eq!(small.num_containers, 2);
        assert_eq!(small.row_count, 10_000);

        let medium = Preset::medium();
        assert_eq!(medium.num_containers, 4);
        assert_eq!(medium.row_count, 100_000);

        let large = Preset::large();
        assert_eq!(large.num_containers, 8);
        assert_eq!(large.row_count, 1_000_000);
    }

    #[test]
    fn test_preset_with_overrides() {
        let preset = Preset::medium().with_overrides(
            Some(6),
            Some("3.0".to_string()),
            None,
            None,
            Some(500_000),
            None,
        );

        assert_eq!(preset.num_containers, 6);
        assert_eq!(preset.container_resources.cpu_limit, "3.0");
        assert_eq!(preset.container_resources.memory_limit, "512Mi"); // unchanged
        assert_eq!(preset.row_count, 500_000);
    }
}
