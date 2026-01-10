//! MongoDB Docker and Kubernetes configurations for load testing.

use super::{add_healthcheck, add_tmpfs, add_volume, create_base_docker_service};
use crate::config::DatabaseConfig;
use serde_yaml::{Mapping, Value};

/// Generate MongoDB Docker service configuration.
pub fn generate_mongodb_docker_service(config: &DatabaseConfig) -> Value {
    let mut service =
        create_base_docker_service(&config.image, &config.resources, "loadtest-network");

    // Command for replica set mode (required for change streams)
    // Note: We use --noauth to avoid keyFile complexity in single-node loadtest replica sets
    // Authentication user is created by mongodb-init after replica set initialization
    let command = vec!["mongod", "--replSet", "rs0", "--bind_ip_all", "--noauth"];
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
        add_tmpfs(&mut service, "/data/db", size);
    } else {
        add_volume(&mut service, vec!["mongodb_data:/data/db".to_string()]);
    }

    // Healthcheck
    add_healthcheck(
        &mut service,
        vec!["CMD", "mongosh", "--eval", "db.adminCommand('ping')"],
        "10s",
        "5s",
        10,
    );

    Value::Mapping(service)
}

/// Generate MongoDB init service for replica set initialization.
pub(crate) fn generate_mongodb_init_service_internal() -> Value {
    let mut service = Mapping::new();

    service.insert(
        Value::String("image".to_string()),
        Value::String("mongo:7".to_string()),
    );

    // Depends on MongoDB being healthy
    let mut depends_on = Mapping::new();
    let mut mongodb_dep = Mapping::new();
    mongodb_dep.insert(
        Value::String("condition".to_string()),
        Value::String("service_healthy".to_string()),
    );
    depends_on.insert(
        Value::String("mongodb".to_string()),
        Value::Mapping(mongodb_dep),
    );
    service.insert(
        Value::String("depends_on".to_string()),
        Value::Mapping(depends_on),
    );

    // Init script to configure replica set and create root user
    // Note: MongoDB is running with --noauth, so we connect without auth
    // We create the root user for compatibility with authenticated connection strings
    let command = r#"mongosh --host mongodb --quiet --eval "
        try {
            rs.status();
            print('Replica set already configured');
        } catch (e) {
            print('Initializing replica set...');
            rs.initiate({
                _id: 'rs0',
                members: [{ _id: 0, host: 'mongodb:27017' }]
            });
            print('Replica set initialized');
        }

        // Wait for replica set to become primary
        while (!db.isMaster().ismaster) {
            sleep(1000);
        }

        // Create root user for compatibility with authenticated connection strings
        db = db.getSiblingDB('admin');
        try {
            db.createUser({
                user: 'root',
                pwd: 'root',
                roles: ['root']
            });
            print('Root user created');
        } catch(e) {
            print('User creation skipped (may already exist): ' + e);
        }
    ""#;

    service.insert(
        Value::String("command".to_string()),
        Value::Sequence(vec![
            Value::String("bash".to_string()),
            Value::String("-c".to_string()),
            Value::String(command.to_string()),
        ]),
    );

    // Restart policy
    service.insert(
        Value::String("restart".to_string()),
        Value::String("on-failure".to_string()),
    );

    // Networks
    let networks = vec![Value::String("loadtest-network".to_string())];
    service.insert(
        Value::String("networks".to_string()),
        Value::Sequence(networks),
    );

    Value::Mapping(service)
}

/// Generate MongoDB Kubernetes StatefulSet.
pub fn generate_mongodb_k8s_statefulset(config: &DatabaseConfig, namespace: &str) -> String {
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
            r#"      - name: mongodb-data
        emptyDir:
          medium: Memory
          sizeLimit: {size}"#
        )
    } else {
        r#"  volumeClaimTemplates:
  - metadata:
      name: mongodb-data
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
  name: mongodb
  namespace: {}
spec:
  serviceName: mongodb
  replicas: 1
  selector:
    matchLabels:
      app: mongodb
  template:
    metadata:
      labels:
        app: mongodb
    spec:
      containers:
      - name: mongodb
        image: {}
        env:
        - name: MONGO_INITDB_ROOT_USERNAME
          value: "root"
        - name: MONGO_INITDB_ROOT_PASSWORD
          value: "root"
        command:
        - mongod
        - --replSet
        - rs0
        - --bind_ip_all
        ports:
        - containerPort: 27017
        resources:
          limits:
            cpu: "{}"
            memory: "{}"
          requests:
            cpu: "{}"
            memory: "{}"
        volumeMounts:
        - name: mongodb-data
          mountPath: /data/db
        readinessProbe:
          exec:
            command:
            - mongosh
            - --eval
            - "db.adminCommand('ping')"
          initialDelaySeconds: 10
          periodSeconds: 5
        livenessProbe:
          exec:
            command:
            - mongosh
            - --eval
            - "db.adminCommand('ping')"
          initialDelaySeconds: 30
          periodSeconds: 10
      volumes:
{}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: mongodb-init
  namespace: {}
spec:
  template:
    spec:
      containers:
      - name: init
        image: mongo:7
        command:
        - bash
        - -c
        - |
          sleep 10
          mongosh --host mongodb -u root -p root --authenticationDatabase admin --eval "
            rs.initiate({{
              _id: 'rs0',
              members: [{{ _id: 0, host: 'mongodb:27017' }}]
            }})
          "
          sleep 5
          mongosh --host mongodb -u root -p root --authenticationDatabase admin --eval "
            while (!rs.isMaster().ismaster) {{ sleep(1000); }}
            print('Replica set initialized');
          "
      restartPolicy: OnFailure
"#,
        namespace,
        config.image,
        cpu_limit,
        memory_limit,
        cpu_request,
        memory_request,
        volume_spec,
        namespace
    )
}

/// Generate MongoDB Kubernetes Service.
pub fn generate_mongodb_k8s_service(namespace: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: mongodb
  namespace: {namespace}
spec:
  selector:
    app: mongodb
  ports:
  - port: 27017
    targetPort: 27017
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
            source_type: SourceType::MongoDB,
            image: "mongo:7".to_string(),
            resources: ResourceLimits::default(),
            tmpfs_storage: false,
            tmpfs_size: None,
            database_name: "testdb".to_string(),
            environment: vec![],
            command_args: vec![],
        }
    }

    #[test]
    fn test_generate_mongodb_docker_service() {
        let config = test_config();
        let service = generate_mongodb_docker_service(&config);
        let yaml = serde_yaml::to_string(&service).unwrap();

        assert!(yaml.contains("mongo:7"));
        assert!(yaml.contains("replSet"));
        // MongoDB uses --noauth mode for single-node loadtest replica sets
        // Authentication is set up via mongodb-init container after replica set initialization
        assert!(yaml.contains("--noauth"));
    }

    #[test]
    fn test_generate_mongodb_k8s_statefulset() {
        let config = test_config();
        let statefulset = generate_mongodb_k8s_statefulset(&config, "loadtest");

        assert!(statefulset.contains("kind: StatefulSet"));
        assert!(statefulset.contains("namespace: loadtest"));
        assert!(statefulset.contains("replSet"));
    }
}
