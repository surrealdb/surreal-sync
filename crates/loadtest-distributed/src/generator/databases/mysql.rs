//! MySQL Docker and Kubernetes configurations for load testing.

use super::{add_environment, add_healthcheck, add_tmpfs, add_volume, create_base_docker_service};
use crate::config::DatabaseConfig;
use serde_yaml::Value;

/// Generate MySQL Docker service configuration.
pub fn generate_mysql_docker_service(config: &DatabaseConfig) -> Value {
    let mut service =
        create_base_docker_service(&config.image, &config.resources, "loadtest-network");

    // Environment variables for MySQL
    add_environment(
        &mut service,
        vec![
            ("MYSQL_ROOT_PASSWORD", "root"),
            ("MYSQL_DATABASE", &config.database_name),
        ],
    );

    // Command with optimized settings for loadtest
    let command = vec![
        "--default-authentication-plugin=mysql_native_password",
        "--max_connections=200",
        "--innodb_buffer_pool_size=1G",
        "--innodb_log_file_size=256M",
        "--innodb_flush_log_at_trx_commit=2",
        "--sync_binlog=0",
        "--skip-log-bin",
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
        add_tmpfs(&mut service, "/var/lib/mysql", size);
    } else {
        add_volume(&mut service, vec!["mysql_data:/var/lib/mysql".to_string()]);
    }

    // Healthcheck
    add_healthcheck(
        &mut service,
        vec!["CMD", "mysqladmin", "ping", "-h", "localhost"],
        "5s",
        "3s",
        10,
    );

    Value::Mapping(service)
}

/// Generate MySQL Kubernetes StatefulSet.
pub fn generate_mysql_k8s_statefulset(config: &DatabaseConfig, namespace: &str) -> String {
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
            r#"      - name: mysql-data
        emptyDir:
          medium: Memory
          sizeLimit: {size}"#
        )
    } else {
        r#"  volumeClaimTemplates:
  - metadata:
      name: mysql-data
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
  name: mysql
  namespace: {}
spec:
  serviceName: mysql
  replicas: 1
  selector:
    matchLabels:
      app: mysql
  template:
    metadata:
      labels:
        app: mysql
    spec:
      containers:
      - name: mysql
        image: {}
        env:
        - name: MYSQL_ROOT_PASSWORD
          value: "root"
        - name: MYSQL_DATABASE
          value: "{}"
        args:
        - "--default-authentication-plugin=mysql_native_password"
        - "--max_connections=200"
        - "--innodb_buffer_pool_size=1G"
        - "--innodb_log_file_size=256M"
        - "--innodb_flush_log_at_trx_commit=2"
        - "--sync_binlog=0"
        - "--skip-log-bin"
        ports:
        - containerPort: 3306
        resources:
          limits:
            cpu: "{}"
            memory: "{}"
          requests:
            cpu: "{}"
            memory: "{}"
        volumeMounts:
        - name: mysql-data
          mountPath: /var/lib/mysql
        readinessProbe:
          exec:
            command:
            - mysqladmin
            - ping
            - -h
            - localhost
          initialDelaySeconds: 10
          periodSeconds: 5
        livenessProbe:
          exec:
            command:
            - mysqladmin
            - ping
            - -h
            - localhost
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

/// Generate MySQL Kubernetes Service.
pub fn generate_mysql_k8s_service(namespace: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: mysql
  namespace: {namespace}
spec:
  selector:
    app: mysql
  ports:
  - port: 3306
    targetPort: 3306
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
            source_type: SourceType::MySQL,
            image: "mysql:8.0".to_string(),
            resources: ResourceLimits::default(),
            tmpfs_storage: false,
            tmpfs_size: None,
            database_name: "testdb".to_string(),
            environment: vec![],
            command_args: vec![],
        }
    }

    #[test]
    fn test_generate_mysql_docker_service() {
        let config = test_config();
        let service = generate_mysql_docker_service(&config);
        let yaml = serde_yaml::to_string(&service).unwrap();

        assert!(yaml.contains("mysql:8.0"));
        assert!(yaml.contains("MYSQL_ROOT_PASSWORD=root"));
        assert!(yaml.contains("max_connections=200"));
    }

    #[test]
    fn test_generate_mysql_k8s_statefulset() {
        let config = test_config();
        let statefulset = generate_mysql_k8s_statefulset(&config, "loadtest");

        assert!(statefulset.contains("kind: StatefulSet"));
        assert!(statefulset.contains("namespace: loadtest"));
        assert!(statefulset.contains("mysql:8.0"));
    }
}
