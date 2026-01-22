//! PostgreSQL Logical Replication Docker and Kubernetes configurations for load testing.
//!
//! This module provides configurations for PostgreSQL with WAL-based logical replication
//! using the wal2json output plugin. It differs from the trigger-based PostgreSQL sync
//! in that it uses PostgreSQL's native logical replication feature.

use super::{add_environment, add_healthcheck, add_tmpfs, add_volume, create_base_docker_service};
use crate::config::DatabaseConfig;
use serde_yaml::Value;

/// Generate PostgreSQL Logical Replication Docker service configuration.
///
/// This configuration enables WAL-based logical replication by:
/// - Setting wal_level=logical (required for logical replication)
/// - Setting max_wal_senders and max_replication_slots for replication connections
/// - Building a custom postgres:16 image with wal2json extension
pub fn generate_postgresql_logical_docker_service(config: &DatabaseConfig) -> Value {
    let mut service = create_base_docker_service(&config.image, &config.resources, "loadtest");

    // Build custom image with wal2json extension
    // Context is parent directory since docker-compose runs from output/
    let mut build = serde_yaml::Mapping::new();
    build.insert(
        Value::String("context".to_string()),
        Value::String("..".to_string()),
    );
    build.insert(
        Value::String("dockerfile".to_string()),
        Value::String("Dockerfile.postgres16.wal2json".to_string()),
    );
    service.insert(Value::String("build".to_string()), Value::Mapping(build));

    // Environment variables for PostgreSQL
    add_environment(
        &mut service,
        vec![
            ("POSTGRES_USER", "postgres"),
            ("POSTGRES_PASSWORD", "postgres"),
            ("POSTGRES_DB", &config.database_name),
        ],
    );

    // Command with settings optimized for logical replication
    // Key differences from trigger-based:
    // - wal_level=logical (required for logical replication)
    // - max_wal_senders=10 (allow replication connections)
    // - max_replication_slots=10 (allow replication slots)
    let command = vec![
        "postgres",
        "-c",
        "max_connections=200",
        "-c",
        "shared_buffers=512MB",
        "-c",
        "effective_cache_size=1GB",
        "-c",
        "wal_level=logical",
        "-c",
        "max_wal_senders=10",
        "-c",
        "max_replication_slots=10",
        "-c",
        "synchronous_commit=off",
    ];
    service.insert(
        Value::String("command".to_string()),
        Value::Sequence(
            command
                .into_iter()
                .map(|s| Value::String(s.to_string()))
                .collect(),
        ),
    );

    // Storage: tmpfs or volume
    if config.tmpfs_storage {
        let size = config.tmpfs_size.as_deref().unwrap_or("4g");
        add_tmpfs(&mut service, "/var/lib/postgresql/data", size);
    } else {
        add_volume(
            &mut service,
            vec!["postgresql_data:/var/lib/postgresql/data".to_string()],
        );
    }

    // Healthcheck
    add_healthcheck(
        &mut service,
        vec!["CMD-SHELL", "pg_isready -U postgres"],
        "5s",
        "3s",
        10,
    );

    Value::Mapping(service)
}

/// Generate PostgreSQL Logical Replication Kubernetes StatefulSet.
pub fn generate_postgresql_logical_k8s_statefulset(
    config: &DatabaseConfig,
    namespace: &str,
) -> String {
    let memory_limit = &config.resources.memory_limit;
    let cpu_limit = &config.resources.cpu_limit;
    let memory_request = config
        .resources
        .memory_request
        .as_deref()
        .unwrap_or("512Mi");
    let cpu_request = config.resources.cpu_request.as_deref().unwrap_or("0.5");

    let volume_spec = if config.tmpfs_storage {
        let size = config.tmpfs_size.as_deref().unwrap_or("4Gi");
        format!(
            r#"      - name: postgresql-data
        emptyDir:
          medium: Memory
          sizeLimit: {size}"#
        )
    } else {
        r#"  volumeClaimTemplates:
  - metadata:
      name: postgresql-data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi"#
            .to_string()
    };

    format!(
        r#"apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgresql
  namespace: {}
spec:
  serviceName: postgresql
  replicas: 1
  selector:
    matchLabels:
      app: postgresql
  template:
    metadata:
      labels:
        app: postgresql
    spec:
      containers:
      - name: postgresql
        image: {}
        env:
        - name: POSTGRES_USER
          value: "postgres"
        - name: POSTGRES_PASSWORD
          value: "postgres"
        - name: POSTGRES_DB
          value: "{}"
        - name: PGDATA
          value: "/var/lib/postgresql/data/pgdata"
        args:
        - "-c"
        - "max_connections=200"
        - "-c"
        - "shared_buffers=512MB"
        - "-c"
        - "effective_cache_size=1GB"
        - "-c"
        - "wal_level=logical"
        - "-c"
        - "max_wal_senders=10"
        - "-c"
        - "max_replication_slots=10"
        - "-c"
        - "synchronous_commit=off"
        ports:
        - containerPort: 5432
        resources:
          limits:
            cpu: "{}"
            memory: "{}"
          requests:
            cpu: "{}"
            memory: "{}"
        volumeMounts:
        - name: postgresql-data
          mountPath: /var/lib/postgresql/data
        readinessProbe:
          exec:
            command:
            - pg_isready
            - -U
            - postgres
          initialDelaySeconds: 10
          periodSeconds: 5
        livenessProbe:
          exec:
            command:
            - pg_isready
            - -U
            - postgres
          initialDelaySeconds: 30
          periodSeconds: 10
      volumes:
{}
"#,
        namespace,
        config.image,
        config.database_name,
        cpu_limit,
        memory_limit,
        cpu_request,
        memory_request,
        volume_spec
    )
}

/// Generate PostgreSQL Logical Replication Kubernetes Service.
pub fn generate_postgresql_logical_k8s_service(namespace: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: postgresql
  namespace: {namespace}
spec:
  selector:
    app: postgresql
  ports:
  - port: 5432
    targetPort: 5432
  clusterIP: None
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ResourceLimits, SourceType};

    fn test_config() -> DatabaseConfig {
        DatabaseConfig {
            source_type: SourceType::PostgreSQLLogical,
            image: "postgres-wal2json:latest".to_string(),
            resources: ResourceLimits::default(),
            tmpfs_storage: false,
            tmpfs_size: None,
            database_name: "testdb".to_string(),
            environment: vec![],
            command_args: vec![],
        }
    }

    #[test]
    fn test_generate_postgresql_logical_docker_service() {
        let config = test_config();
        let service = generate_postgresql_logical_docker_service(&config);
        let yaml = serde_yaml::to_string(&service).unwrap();

        assert!(yaml.contains("postgres-wal2json:latest"));
        assert!(yaml.contains("Dockerfile.postgres16.wal2json"));
        assert!(yaml.contains("POSTGRES_PASSWORD=postgres"));
        // Verify logical replication settings
        assert!(yaml.contains("wal_level=logical"));
        assert!(yaml.contains("max_wal_senders=10"));
        assert!(yaml.contains("max_replication_slots=10"));
    }

    #[test]
    fn test_generate_postgresql_logical_k8s_statefulset() {
        let config = test_config();
        let statefulset = generate_postgresql_logical_k8s_statefulset(&config, "loadtest");

        assert!(statefulset.contains("kind: StatefulSet"));
        assert!(statefulset.contains("namespace: loadtest"));
        assert!(statefulset.contains("postgres-wal2json:latest"));
        // Verify logical replication settings
        assert!(statefulset.contains("wal_level=logical"));
        assert!(statefulset.contains("max_wal_senders=10"));
        assert!(statefulset.contains("max_replication_slots=10"));
    }
}
