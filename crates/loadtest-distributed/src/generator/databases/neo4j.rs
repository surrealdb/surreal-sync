//! Neo4j Docker and Kubernetes configurations for load testing.

use super::{add_environment, add_healthcheck, add_tmpfs, add_volume, create_base_docker_service};
use crate::config::DatabaseConfig;
use serde_yaml::Value;

/// Generate Neo4j Docker service configuration.
pub fn generate_neo4j_docker_service(config: &DatabaseConfig) -> Value {
    let mut service = create_base_docker_service(&config.image, &config.resources, "loadtest");

    // Calculate memory settings based on resource limits
    let heap_size = calculate_heap_size(&config.resources.memory_limit);
    let pagecache_size = calculate_pagecache_size(&config.resources.memory_limit);

    // Environment variables for Neo4j
    add_environment(
        &mut service,
        vec![
            ("NEO4J_AUTH", "neo4j/password"),
            ("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes"),
            ("NEO4J_dbms_memory_heap_initial__size", &heap_size),
            ("NEO4J_dbms_memory_heap_max__size", &heap_size),
            ("NEO4J_dbms_memory_pagecache_size", &pagecache_size),
            ("NEO4J_PLUGINS", "[\"apoc\"]"),
            // Disable unnecessary features for loadtest
            ("NEO4J_dbms_security_procedures_unrestricted", "apoc.*"),
            ("NEO4J_dbms_logs_query_enabled", "OFF"),
        ],
    );

    // Storage: tmpfs or volume
    if config.tmpfs_storage {
        let size = config.tmpfs_size.as_deref().unwrap_or("4g");
        add_tmpfs(&mut service, "/data", size);
    } else {
        add_volume(&mut service, vec!["neo4j_data:/data".to_string()]);
    }

    // Healthcheck
    add_healthcheck(
        &mut service,
        vec!["CMD", "wget", "-q", "--spider", "http://localhost:7474"],
        "10s",
        "5s",
        10,
    );

    Value::Mapping(service)
}

/// Generate Neo4j Kubernetes StatefulSet.
pub fn generate_neo4j_k8s_statefulset(config: &DatabaseConfig, namespace: &str) -> String {
    let memory_limit = &config.resources.memory_limit;
    let cpu_limit = &config.resources.cpu_limit;
    let memory_request = config
        .resources
        .memory_request
        .as_deref()
        .unwrap_or("512Mi");
    let cpu_request = config.resources.cpu_request.as_deref().unwrap_or("0.5");

    let heap_size = calculate_heap_size(memory_limit);
    let pagecache_size = calculate_pagecache_size(memory_limit);

    let volume_spec = if config.tmpfs_storage {
        let size = config.tmpfs_size.as_deref().unwrap_or("4Gi");
        format!(
            r#"      - name: neo4j-data
        emptyDir:
          medium: Memory
          sizeLimit: {size}"#
        )
    } else {
        r#"  volumeClaimTemplates:
  - metadata:
      name: neo4j-data
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
  name: neo4j
  namespace: {}
spec:
  serviceName: neo4j
  replicas: 1
  selector:
    matchLabels:
      app: neo4j
  template:
    metadata:
      labels:
        app: neo4j
    spec:
      containers:
      - name: neo4j
        image: {}
        env:
        - name: NEO4J_AUTH
          value: "neo4j/password"
        - name: NEO4J_ACCEPT_LICENSE_AGREEMENT
          value: "yes"
        - name: NEO4J_dbms_memory_heap_initial__size
          value: "{}"
        - name: NEO4J_dbms_memory_heap_max__size
          value: "{}"
        - name: NEO4J_dbms_memory_pagecache_size
          value: "{}"
        - name: NEO4J_PLUGINS
          value: '["apoc"]'
        - name: NEO4J_dbms_security_procedures_unrestricted
          value: "apoc.*"
        - name: NEO4J_dbms_logs_query_enabled
          value: "OFF"
        ports:
        - containerPort: 7474
          name: http
        - containerPort: 7687
          name: bolt
        resources:
          limits:
            cpu: "{}"
            memory: "{}"
          requests:
            cpu: "{}"
            memory: "{}"
        volumeMounts:
        - name: neo4j-data
          mountPath: /data
        readinessProbe:
          httpGet:
            path: /
            port: 7474
          initialDelaySeconds: 30
          periodSeconds: 10
        livenessProbe:
          httpGet:
            path: /
            port: 7474
          initialDelaySeconds: 60
          periodSeconds: 30
      volumes:
{}
"#,
        namespace,
        config.image,
        heap_size,
        heap_size,
        pagecache_size,
        cpu_limit,
        memory_limit,
        cpu_request,
        memory_request,
        volume_spec
    )
}

/// Generate Neo4j Kubernetes Service.
pub fn generate_neo4j_k8s_service(namespace: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: neo4j
  namespace: {namespace}
spec:
  selector:
    app: neo4j
  ports:
  - port: 7474
    targetPort: 7474
    name: http
  - port: 7687
    targetPort: 7687
    name: bolt
  clusterIP: None
"#
    )
}

/// Calculate Neo4j heap size based on container memory limit.
fn calculate_heap_size(memory_limit: &str) -> String {
    let mb = parse_memory_to_mb(memory_limit);
    // Use 40% of memory for heap
    let heap_mb = (mb as f64 * 0.4) as u64;
    format!("{}m", heap_mb.max(256))
}

/// Calculate Neo4j pagecache size based on container memory limit.
fn calculate_pagecache_size(memory_limit: &str) -> String {
    let mb = parse_memory_to_mb(memory_limit);
    // Use 30% of memory for pagecache
    let pagecache_mb = (mb as f64 * 0.3) as u64;
    format!("{}m", pagecache_mb.max(128))
}

/// Parse memory limit string to megabytes.
fn parse_memory_to_mb(memory: &str) -> u64 {
    let memory = memory.to_lowercase();
    if memory.ends_with("gi") {
        memory.trim_end_matches("gi").parse::<u64>().unwrap_or(1) * 1024
    } else if memory.ends_with("g") {
        memory.trim_end_matches('g').parse::<u64>().unwrap_or(1) * 1024
    } else if memory.ends_with("mi") {
        memory.trim_end_matches("mi").parse::<u64>().unwrap_or(512)
    } else if memory.ends_with('m') {
        memory.trim_end_matches('m').parse::<u64>().unwrap_or(512)
    } else {
        1024 // Default to 1GB
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ResourceLimits, SourceType};

    fn test_config() -> DatabaseConfig {
        DatabaseConfig {
            source_type: SourceType::Neo4j,
            image: "neo4j:5".to_string(),
            resources: ResourceLimits {
                cpu_limit: "2.0".to_string(),
                memory_limit: "4Gi".to_string(),
                cpu_request: None,
                memory_request: None,
            },
            tmpfs_storage: false,
            tmpfs_size: None,
            database_name: "neo4j".to_string(),
            environment: vec![],
            command_args: vec![],
        }
    }

    #[test]
    fn test_generate_neo4j_docker_service() {
        let config = test_config();
        let service = generate_neo4j_docker_service(&config);
        let yaml = serde_yaml::to_string(&service).unwrap();

        assert!(yaml.contains("neo4j:5"));
        assert!(yaml.contains("NEO4J_AUTH=neo4j/password"));
        assert!(yaml.contains("heap"));
    }

    #[test]
    fn test_parse_memory_to_mb() {
        assert_eq!(parse_memory_to_mb("1Gi"), 1024);
        assert_eq!(parse_memory_to_mb("4Gi"), 4096);
        assert_eq!(parse_memory_to_mb("512Mi"), 512);
        assert_eq!(parse_memory_to_mb("2g"), 2048);
    }

    #[test]
    fn test_calculate_heap_size() {
        assert_eq!(calculate_heap_size("4Gi"), "1638m"); // 40% of 4096
        assert_eq!(calculate_heap_size("1Gi"), "409m"); // 40% of 1024
    }
}
