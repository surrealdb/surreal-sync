//! Work partitioning for distributed load testing.
//!
//! Distributes tables across containers using table-based partitioning.

use crate::config::{ContainerConfig, ResourceLimits, SourceType, TmpfsConfig};

/// Partition tables across containers.
///
/// Uses table-based partitioning where each container handles different tables.
/// If there are more tables than containers, tables are distributed round-robin.
/// If there are more containers than tables, extra containers will have no tables assigned.
#[allow(clippy::too_many_arguments)]
pub fn partition_tables(
    tables: &[String],
    num_containers: usize,
    source_type: SourceType,
    row_count: u64,
    base_seed: u64,
    resources: &ResourceLimits,
    tmpfs: Option<&TmpfsConfig>,
    connection_string: &str,
    batch_size: u64,
) -> Vec<ContainerConfig> {
    let mut containers = Vec::with_capacity(num_containers);

    // Distribute tables round-robin across containers
    let mut container_tables: Vec<Vec<String>> = vec![Vec::new(); num_containers];
    for (i, table) in tables.iter().enumerate() {
        let container_idx = i % num_containers;
        container_tables[container_idx].push(table.clone());
    }

    for (i, assigned_tables) in container_tables.into_iter().enumerate() {
        // Skip containers with no tables assigned
        if assigned_tables.is_empty() && i >= tables.len() {
            continue;
        }

        let container_id = format!("populate-{}", i + 1);
        let seed = base_seed + i as u64;

        containers.push(ContainerConfig {
            id: container_id,
            source_type,
            tables: assigned_tables,
            row_count,
            seed,
            resources: resources.clone(),
            tmpfs: tmpfs.cloned(),
            connection_string: connection_string.to_string(),
            batch_size,
        });
    }

    containers
}

/// Calculate optimal number of containers based on table count.
///
/// Returns min(requested_containers, num_tables) to avoid idle containers.
pub fn optimal_container_count(requested_containers: usize, num_tables: usize) -> usize {
    if num_tables == 0 {
        return 1; // At least one container
    }
    std::cmp::min(requested_containers, num_tables)
}

/// Describe the partitioning plan for logging.
pub fn describe_partitioning(containers: &[ContainerConfig]) -> String {
    let mut lines = Vec::new();
    lines.push("Work distribution:".to_string());

    for container in containers {
        let tables_str = if container.tables.is_empty() {
            "(no tables - will run verification)".to_string()
        } else {
            container.tables.join(", ")
        };
        lines.push(format!(
            "  {}: {} (seed={})",
            container.id, tables_str, container.seed
        ));
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_resources() -> ResourceLimits {
        ResourceLimits {
            cpu_limit: "1.0".to_string(),
            memory_limit: "1Gi".to_string(),
            cpu_request: None,
            memory_request: None,
        }
    }

    #[test]
    fn test_partition_equal_distribution() {
        let tables = vec![
            "users".to_string(),
            "orders".to_string(),
            "products".to_string(),
        ];
        let resources = default_resources();

        let containers = partition_tables(
            &tables,
            3,
            SourceType::MySQL,
            1000,
            42,
            &resources,
            None,
            "mysql://localhost/test",
            100,
        );

        assert_eq!(containers.len(), 3);
        assert_eq!(containers[0].tables, vec!["users"]);
        assert_eq!(containers[1].tables, vec!["orders"]);
        assert_eq!(containers[2].tables, vec!["products"]);

        // Check seeds
        assert_eq!(containers[0].seed, 42);
        assert_eq!(containers[1].seed, 43);
        assert_eq!(containers[2].seed, 44);
    }

    #[test]
    fn test_partition_more_tables_than_containers() {
        let tables = vec![
            "t1".to_string(),
            "t2".to_string(),
            "t3".to_string(),
            "t4".to_string(),
            "t5".to_string(),
        ];
        let resources = default_resources();

        let containers = partition_tables(
            &tables,
            2,
            SourceType::PostgreSQL,
            1000,
            0,
            &resources,
            None,
            "postgres://localhost/test",
            100,
        );

        assert_eq!(containers.len(), 2);
        // Round-robin: populate-1 gets t1, t3, t5; populate-2 gets t2, t4
        assert_eq!(containers[0].tables, vec!["t1", "t3", "t5"]);
        assert_eq!(containers[1].tables, vec!["t2", "t4"]);
    }

    #[test]
    fn test_partition_more_containers_than_tables() {
        let tables = vec!["users".to_string(), "orders".to_string()];
        let resources = default_resources();

        let containers = partition_tables(
            &tables,
            5,
            SourceType::MongoDB,
            1000,
            100,
            &resources,
            None,
            "mongodb://localhost/test",
            100,
        );

        // Should only create containers for tables that exist
        assert_eq!(containers.len(), 2);
        assert_eq!(containers[0].tables, vec!["users"]);
        assert_eq!(containers[1].tables, vec!["orders"]);
    }

    #[test]
    fn test_optimal_container_count() {
        assert_eq!(optimal_container_count(4, 3), 3);
        assert_eq!(optimal_container_count(2, 5), 2);
        assert_eq!(optimal_container_count(3, 3), 3);
        assert_eq!(optimal_container_count(10, 0), 1);
    }

    #[test]
    fn test_describe_partitioning() {
        let tables = vec!["users".to_string(), "orders".to_string()];
        let resources = default_resources();

        let containers = partition_tables(
            &tables,
            2,
            SourceType::MySQL,
            1000,
            42,
            &resources,
            None,
            "mysql://localhost/test",
            100,
        );

        let description = describe_partitioning(&containers);
        assert!(description.contains("populate-1"));
        assert!(description.contains("users"));
        assert!(description.contains("seed=42"));
    }
}
