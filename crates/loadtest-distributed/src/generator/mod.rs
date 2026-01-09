//! Configuration generators for distributed load testing.
//!
//! This module provides generators for Docker Compose and Kubernetes configurations.

pub mod databases;
pub mod docker_compose;
pub mod kubernetes;

use crate::config::ClusterConfig;
use anyhow::Result;

/// Trait for configuration generators.
pub trait ConfigGenerator {
    /// Generate configuration from cluster config.
    fn generate(&self, config: &ClusterConfig) -> Result<String>;

    /// Get the output filename for this generator.
    fn filename(&self) -> &str;
}

pub use docker_compose::DockerComposeGenerator;
pub use kubernetes::KubernetesGenerator;
