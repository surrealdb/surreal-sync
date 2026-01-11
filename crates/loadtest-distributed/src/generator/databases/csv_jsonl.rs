//! CSV and JSONL file generator Docker and Kubernetes configurations for load testing.
//!
//! For CSV and JSONL sources, we don't need a separate database service.
//! Instead, workers generate files directly to the shared volume.

use crate::config::DatabaseConfig;
use serde_yaml::{Mapping, Value};

/// Generate a placeholder Docker service for file-based sources.
/// For CSV/JSONL, the workers generate files directly, so this is minimal.
pub fn generate_file_generator_docker_service(_config: &DatabaseConfig) -> Value {
    let mut service = Mapping::new();

    // Use busybox as a minimal placeholder that just sleeps
    service.insert(
        Value::String("image".to_string()),
        Value::String("busybox:latest".to_string()),
    );

    // Just stay alive for health checks
    service.insert(
        Value::String("command".to_string()),
        Value::Sequence(vec![
            Value::String("sh".to_string()),
            Value::String("-c".to_string()),
            Value::String("mkdir -p /data && touch /data/.ready && sleep infinity".to_string()),
        ]),
    );

    // Mount the shared data volume
    let volumes = vec![Value::String("loadtest-data:/data".to_string())];
    service.insert(
        Value::String("volumes".to_string()),
        Value::Sequence(volumes),
    );

    // Networks
    let networks = vec![Value::String("loadtest".to_string())];
    service.insert(
        Value::String("networks".to_string()),
        Value::Sequence(networks),
    );

    // Healthcheck - just check if the ready file exists
    let mut healthcheck = Mapping::new();
    healthcheck.insert(
        Value::String("test".to_string()),
        Value::Sequence(vec![
            Value::String("CMD".to_string()),
            Value::String("test".to_string()),
            Value::String("-f".to_string()),
            Value::String("/data/.ready".to_string()),
        ]),
    );
    healthcheck.insert(
        Value::String("interval".to_string()),
        Value::String("5s".to_string()),
    );
    healthcheck.insert(
        Value::String("timeout".to_string()),
        Value::String("3s".to_string()),
    );
    healthcheck.insert(
        Value::String("retries".to_string()),
        Value::Number(3.into()),
    );
    service.insert(
        Value::String("healthcheck".to_string()),
        Value::Mapping(healthcheck),
    );

    Value::Mapping(service)
}

/// Generate a Kubernetes Job for file-based sources (minimal placeholder).
pub fn generate_file_generator_k8s_job(_config: &DatabaseConfig, namespace: &str) -> String {
    format!(
        r#"apiVersion: batch/v1
kind: Job
metadata:
  name: file-generator-init
  namespace: {namespace}
spec:
  template:
    spec:
      containers:
      - name: init
        image: busybox:latest
        command:
        - sh
        - -c
        - mkdir -p /data && touch /data/.ready
        volumeMounts:
        - name: loadtest-data
          mountPath: /data
      volumes:
      - name: loadtest-data
        persistentVolumeClaim:
          claimName: loadtest-data
      restartPolicy: OnFailure
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ResourceLimits, SourceType};

    fn test_csv_config() -> DatabaseConfig {
        DatabaseConfig {
            source_type: SourceType::Csv,
            image: "busybox:latest".to_string(),
            resources: ResourceLimits::default(),
            tmpfs_storage: false,
            tmpfs_size: None,
            database_name: "loadtest".to_string(),
            environment: vec![],
            command_args: vec![],
        }
    }

    #[test]
    fn test_generate_file_generator_docker_service() {
        let config = test_csv_config();
        let service = generate_file_generator_docker_service(&config);
        let yaml = serde_yaml::to_string(&service).unwrap();

        assert!(yaml.contains("busybox"));
        assert!(yaml.contains("/data/.ready"));
    }

    #[test]
    fn test_generate_file_generator_k8s_job() {
        let config = test_csv_config();
        let job = generate_file_generator_k8s_job(&config, "loadtest");

        assert!(job.contains("kind: Job"));
        assert!(job.contains("namespace: loadtest"));
    }
}
