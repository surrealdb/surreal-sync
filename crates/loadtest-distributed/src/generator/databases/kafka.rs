//! Kafka Docker and Kubernetes configurations for load testing.

use super::{add_environment, add_healthcheck, add_tmpfs, add_volume, create_base_docker_service};
use crate::config::DatabaseConfig;
use serde_yaml::Value;

/// Generate Kafka Docker service configuration.
pub fn generate_kafka_docker_service(config: &DatabaseConfig) -> Value {
    let mut service = create_base_docker_service(&config.image, &config.resources, "loadtest");

    // Environment variables for Kafka (KRaft mode - no Zookeeper)
    // Using Apache Kafka native format (no CLUSTER_ID needed - auto-generated)
    add_environment(
        &mut service,
        vec![
            ("KAFKA_NODE_ID", "1"),
            ("KAFKA_PROCESS_ROLES", "broker,controller"),
            ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093"),
            (
                "KAFKA_LISTENERS",
                "PLAINTEXT://kafka:19092,PLAINTEXT_HOST://kafka:9092,CONTROLLER://kafka:9093",
            ),
            (
                "KAFKA_ADVERTISED_LISTENERS",
                "PLAINTEXT://kafka:19092,PLAINTEXT_HOST://kafka:9092",
            ),
            (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
            ),
            ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
            // Performance settings
            ("KAFKA_NUM_PARTITIONS", "8"),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
            ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
            ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
            ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
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

    // Healthcheck - Apache Kafka image uses .sh scripts
    // Use kafka:9092 to match KAFKA_ADVERTISED_LISTENERS
    add_healthcheck(
        &mut service,
        vec![
            "CMD-SHELL",
            "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 || exit 1",
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

    // For tmpfs, we use emptyDir with Memory medium (goes under volumes)
    // For persistent storage, we use volumeClaimTemplates (goes at StatefulSet spec level)
    let (volumes_section, volume_claim_templates) = if config.tmpfs_storage {
        let size = config.tmpfs_size.as_deref().unwrap_or("4Gi");
        (
            format!(
                r#"      volumes:
      - name: kafka-data
        emptyDir:
          medium: Memory
          sizeLimit: {size}"#
            ),
            String::new(),
        )
    } else {
        (
            String::new(), // No volumes section needed for volumeClaimTemplates
            r#"  volumeClaimTemplates:
  - metadata:
      name: kafka-data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi"#
                .to_string(),
        )
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
          value: "1@localhost:9093"
        - name: KAFKA_LISTENERS
          value: "PLAINTEXT://:19092,PLAINTEXT_HOST://:9092,CONTROLLER://:9093"
        - name: KAFKA_ADVERTISED_LISTENERS
          value: "PLAINTEXT://kafka-0.kafka:19092,PLAINTEXT_HOST://kafka-0.kafka:9092"
        - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
          value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
        - name: KAFKA_CONTROLLER_LISTENER_NAMES
          value: "CONTROLLER"
        - name: KAFKA_NUM_PARTITIONS
          value: "8"
        - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
          value: "1"
        - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
          value: "1"
        - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
          value: "1"
        - name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS
          value: "0"
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
            - /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 30
        livenessProbe:
          exec:
            command:
            - bash
            - -c
            - /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
          initialDelaySeconds: 60
          periodSeconds: 30
          timeoutSeconds: 30
{volumes_section}
{volume_claim_templates}
"#,
        namespace,
        config.image,
        cpu_limit,
        memory_limit,
        cpu_request,
        memory_request,
        volumes_section = volumes_section,
        volume_claim_templates = volume_claim_templates
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
            image: "apache/kafka:latest".to_string(),
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

        assert!(yaml.contains("apache/kafka"));
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
