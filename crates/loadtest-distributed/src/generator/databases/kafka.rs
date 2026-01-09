//! Kafka Docker and Kubernetes configurations for load testing.

use super::{add_environment, add_healthcheck, add_tmpfs, add_volume, create_base_docker_service};
use crate::config::DatabaseConfig;
use serde_yaml::Value;

/// Generate Kafka Docker service configuration.
pub fn generate_kafka_docker_service(config: &DatabaseConfig) -> Value {
    let mut service =
        create_base_docker_service(&config.image, &config.resources, "loadtest-network");

    // Environment variables for Kafka (KRaft mode - no Zookeeper)
    add_environment(
        &mut service,
        vec![
            ("KAFKA_NODE_ID", "1"),
            ("KAFKA_PROCESS_ROLES", "broker,controller"),
            ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093"),
            (
                "KAFKA_LISTENERS",
                "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
            ),
            ("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092"),
            (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            ),
            ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
            ("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT"),
            ("CLUSTER_ID", "loadtest-kafka-cluster-001"),
            // Performance settings
            ("KAFKA_NUM_PARTITIONS", "8"),
            ("KAFKA_DEFAULT_REPLICATION_FACTOR", "1"),
            ("KAFKA_LOG_RETENTION_HOURS", "1"),
            ("KAFKA_LOG_RETENTION_BYTES", "1073741824"),
            ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true"),
        ],
    );

    // Storage: tmpfs or volume
    if config.tmpfs_storage {
        let size = config.tmpfs_size.as_deref().unwrap_or("4g");
        add_tmpfs(&mut service, "/var/lib/kafka/data", size);
    } else {
        add_volume(
            &mut service,
            vec!["kafka_data:/var/lib/kafka/data".to_string()],
        );
    }

    // Healthcheck
    add_healthcheck(
        &mut service,
        vec![
            "CMD-SHELL",
            "kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1",
        ],
        "10s",
        "10s",
        10,
    );

    Value::Mapping(service)
}

/// Generate Kafka Kubernetes StatefulSet.
pub fn generate_kafka_k8s_statefulset(config: &DatabaseConfig, namespace: &str) -> String {
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
            r#"      - name: kafka-data
        emptyDir:
          medium: Memory
          sizeLimit: {size}"#
        )
    } else {
        r#"  volumeClaimTemplates:
  - metadata:
      name: kafka-data
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
  name: kafka
  namespace: {}
spec:
  serviceName: kafka
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
      - name: kafka
        image: {}
        env:
        - name: KAFKA_NODE_ID
          value: "1"
        - name: KAFKA_PROCESS_ROLES
          value: "broker,controller"
        - name: KAFKA_CONTROLLER_QUORUM_VOTERS
          value: "1@kafka:9093"
        - name: KAFKA_LISTENERS
          value: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
        - name: KAFKA_ADVERTISED_LISTENERS
          value: "PLAINTEXT://kafka:9092"
        - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
          value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
        - name: KAFKA_CONTROLLER_LISTENER_NAMES
          value: "CONTROLLER"
        - name: KAFKA_INTER_BROKER_LISTENER_NAME
          value: "PLAINTEXT"
        - name: CLUSTER_ID
          value: "loadtest-kafka-cluster-001"
        - name: KAFKA_NUM_PARTITIONS
          value: "8"
        - name: KAFKA_DEFAULT_REPLICATION_FACTOR
          value: "1"
        - name: KAFKA_LOG_RETENTION_HOURS
          value: "1"
        - name: KAFKA_AUTO_CREATE_TOPICS_ENABLE
          value: "true"
        ports:
        - containerPort: 9092
          name: kafka
        - containerPort: 9093
          name: controller
        resources:
          limits:
            cpu: "{}"
            memory: "{}"
          requests:
            cpu: "{}"
            memory: "{}"
        volumeMounts:
        - name: kafka-data
          mountPath: /var/lib/kafka/data
        readinessProbe:
          exec:
            command:
            - bash
            - -c
            - kafka-broker-api-versions --bootstrap-server localhost:9092
          initialDelaySeconds: 30
          periodSeconds: 10
        livenessProbe:
          exec:
            command:
            - bash
            - -c
            - kafka-broker-api-versions --bootstrap-server localhost:9092
          initialDelaySeconds: 60
          periodSeconds: 30
      volumes:
{}
"#,
        namespace, config.image, cpu_limit, memory_limit, cpu_request, memory_request, volume_spec
    )
}

/// Generate Kafka Kubernetes Service.
pub fn generate_kafka_k8s_service(namespace: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: {namespace}
spec:
  selector:
    app: kafka
  ports:
  - port: 9092
    targetPort: 9092
    name: kafka
  - port: 9093
    targetPort: 9093
    name: controller
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
            source_type: SourceType::Kafka,
            image: "confluentinc/cp-kafka:7.5.0".to_string(),
            resources: ResourceLimits::default(),
            tmpfs_storage: false,
            tmpfs_size: None,
            database_name: "loadtest".to_string(),
            environment: vec![],
            command_args: vec![],
        }
    }

    #[test]
    fn test_generate_kafka_docker_service() {
        let config = test_config();
        let service = generate_kafka_docker_service(&config);
        let yaml = serde_yaml::to_string(&service).unwrap();

        assert!(yaml.contains("confluentinc/cp-kafka"));
        assert!(yaml.contains("KAFKA_PROCESS_ROLES=broker,controller"));
        assert!(yaml.contains("KAFKA_NUM_PARTITIONS=8"));
    }

    #[test]
    fn test_generate_kafka_k8s_statefulset() {
        let config = test_config();
        let statefulset = generate_kafka_k8s_statefulset(&config, "loadtest");

        assert!(statefulset.contains("kind: StatefulSet"));
        assert!(statefulset.contains("namespace: loadtest"));
        assert!(statefulset.contains("KAFKA_PROCESS_ROLES"));
    }
}
