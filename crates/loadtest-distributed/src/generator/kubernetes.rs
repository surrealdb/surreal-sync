//! Kubernetes configuration generator.
//!
//! Uses HTTP-based metrics collection - no shared volumes required.
//! Containers POST metrics to aggregator server via HTTP.

use super::databases;
use super::ConfigGenerator;
use crate::config::{ClusterConfig, SourceType};
use anyhow::Result;
use std::collections::HashMap;

/// Generator for Kubernetes configurations.
pub struct KubernetesGenerator;

impl ConfigGenerator for KubernetesGenerator {
    fn generate(&self, config: &ClusterConfig) -> Result<String> {
        // For Kubernetes, we generate multiple files concatenated with ---
        // In practice, these should be written to separate files
        let mut manifests = Vec::new();

        // Namespace
        manifests.push(generate_namespace(config));

        // ConfigMap for schema
        manifests.push(generate_configmap(config));

        // Database service
        let db_statefulset =
            databases::generate_k8s_statefulset(&config.database, &config.network_name);
        if !db_statefulset.is_empty() {
            manifests.push(db_statefulset);
        }

        let db_service = databases::generate_k8s_service(&config.database, &config.network_name);
        if !db_service.is_empty() {
            manifests.push(db_service);
        }

        // SurrealDB
        manifests.push(generate_surrealdb_deployment(config));
        manifests.push(generate_surrealdb_service(config));

        // Aggregator Deployment and Service (HTTP-based, no PVC needed)
        manifests.push(generate_aggregator_deployment(config));
        manifests.push(generate_aggregator_service(config));

        // Populate Worker Jobs
        manifests.push(generate_populate_job(config));

        // Sync Job (runs after populate completes)
        manifests.push(generate_sync_job(config));

        // Verify Worker Jobs (runs after sync completes)
        manifests.push(generate_verify_job(config));

        // Join all manifests with YAML document separator
        Ok(manifests.join("\n---\n"))
    }

    fn filename(&self) -> &str {
        "loadtest.yaml"
    }
}

impl KubernetesGenerator {
    /// Generate manifests to separate files.
    pub fn generate_to_files(&self, config: &ClusterConfig) -> Result<HashMap<String, String>> {
        let mut files = HashMap::new();

        files.insert("namespace.yaml".to_string(), generate_namespace(config));
        files.insert("configmap.yaml".to_string(), generate_configmap(config));

        // Database manifests
        let db_name = database_name(config.source_type);
        let db_statefulset =
            databases::generate_k8s_statefulset(&config.database, &config.network_name);
        if !db_statefulset.is_empty() {
            files.insert(format!("{db_name}-statefulset.yaml"), db_statefulset);
        }
        let db_service = databases::generate_k8s_service(&config.database, &config.network_name);
        if !db_service.is_empty() {
            files.insert(format!("{db_name}-service.yaml"), db_service);
        }

        // SurrealDB
        files.insert(
            "surrealdb-deployment.yaml".to_string(),
            generate_surrealdb_deployment(config),
        );
        files.insert(
            "surrealdb-service.yaml".to_string(),
            generate_surrealdb_service(config),
        );

        // Aggregator (HTTP-based, no PVC needed)
        files.insert(
            "aggregator-deployment.yaml".to_string(),
            generate_aggregator_deployment(config),
        );
        files.insert(
            "aggregator-service.yaml".to_string(),
            generate_aggregator_service(config),
        );

        // Populate containers
        files.insert(
            "populate-job.yaml".to_string(),
            generate_populate_job(config),
        );

        // Sync job
        files.insert("sync-job.yaml".to_string(), generate_sync_job(config));

        // Verify containers
        files.insert("verify-job.yaml".to_string(), generate_verify_job(config));

        Ok(files)
    }
}

/// Get database name for a source type.
fn database_name(source_type: SourceType) -> &'static str {
    match source_type {
        SourceType::MySQL => "mysql",
        SourceType::PostgreSQL | SourceType::PostgreSQLLogical => "postgresql",
        SourceType::MongoDB => "mongodb",
        SourceType::Neo4j => "neo4j",
        SourceType::Kafka => "kafka",
        SourceType::Csv | SourceType::Jsonl => "file-generator",
    }
}

/// Generate Namespace manifest.
fn generate_namespace(config: &ClusterConfig) -> String {
    format!(
        r#"apiVersion: v1
kind: Namespace
metadata:
  name: {}
  labels:
    app: surreal-loadtest
"#,
        config.network_name
    )
}

/// Generate ConfigMap for schema and configuration.
fn generate_configmap(config: &ClusterConfig) -> String {
    format!(
        r#"apiVersion: v1
kind: ConfigMap
metadata:
  name: loadtest-config
  namespace: {}
data:
  # Schema file should be added here or mounted from external source
  # schema.yaml: |
  #   tables:
  #     - name: users
  #       ...
"#,
        config.network_name
    )
}

/// Generate Aggregator Deployment (HTTP-based metrics collection).
fn generate_aggregator_deployment(config: &ClusterConfig) -> String {
    // Expected containers = populate containers + verify containers
    let expected_containers = config.containers.len() * 2;

    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: aggregator
  namespace: {namespace}
  labels:
    app: loadtest-aggregator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: loadtest-aggregator
  template:
    metadata:
      labels:
        app: loadtest-aggregator
    spec:
      containers:
      - name: aggregator
        image: surreal-sync:latest
        imagePullPolicy: IfNotPresent
        args:
        - loadtest
        - aggregate-server
        - --expected-workers
        - "{expected_containers}"
        - --timeout
        - "30m"
        ports:
        - containerPort: 9090
        readinessProbe:
          httpGet:
            path: /health
            port: 9090
          initialDelaySeconds: 2
          periodSeconds: 5
        resources:
          limits:
            cpu: "0.5"
            memory: "256Mi"
          requests:
            cpu: "0.1"
            memory: "64Mi"
"#,
        namespace = config.network_name,
        expected_containers = expected_containers,
    )
}

/// Generate Aggregator Service.
fn generate_aggregator_service(config: &ClusterConfig) -> String {
    format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: aggregator
  namespace: {}
spec:
  selector:
    app: loadtest-aggregator
  ports:
  - port: 9090
    targetPort: 9090
"#,
        config.network_name
    )
}

/// Generate SurrealDB Deployment.
fn generate_surrealdb_deployment(config: &ClusterConfig) -> String {
    let storage_cmd = if config.surrealdb.memory_storage {
        "memory"
    } else {
        "file:/data/database.db"
    };

    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: surrealdb
  namespace: {}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: surrealdb
  template:
    metadata:
      labels:
        app: surrealdb
    spec:
      containers:
      - name: surrealdb
        image: {}
        imagePullPolicy: IfNotPresent
        args:
        - start
        - --log
        - info
        - --user
        - root
        - --pass
        - root
        - --bind
        - 0.0.0.0:8000
        - {}
        ports:
        - containerPort: 8000
        resources:
          limits:
            cpu: "{}"
            memory: "{}"
          requests:
            cpu: "{}"
            memory: "{}"
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
"#,
        config.network_name,
        config.surrealdb.image,
        storage_cmd,
        config.surrealdb.resources.cpu_limit,
        config.surrealdb.resources.memory_limit,
        config
            .surrealdb
            .resources
            .cpu_request
            .as_deref()
            .unwrap_or("0.5"),
        config
            .surrealdb
            .resources
            .memory_request
            .as_deref()
            .unwrap_or("512Mi"),
    )
}

/// Generate SurrealDB Service.
fn generate_surrealdb_service(config: &ClusterConfig) -> String {
    format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: surrealdb
  namespace: {}
spec:
  selector:
    app: surrealdb
  ports:
  - port: 8000
    targetPort: 8000
"#,
        config.network_name
    )
}

/// Generate Populate Worker Job.
/// Containers POST metrics to aggregator server when done.
fn generate_populate_job(config: &ClusterConfig) -> String {
    let db_name = database_name(config.source_type);
    let container_count = config.containers.len();
    let source_type_lower = format!("{}", config.source_type).to_lowercase();

    // Build init containers to wait for database and aggregator
    let mut init_containers = String::new();
    if config.source_type != SourceType::Csv && config.source_type != SourceType::Jsonl {
        init_containers = format!(
            r#"      initContainers:
      - name: wait-for-db
        image: busybox:latest
        command:
        - sh
        - -c
        - |
          until nc -z {db_name} {db_port}; do
            echo "Waiting for {db_name} to be ready..."
            sleep 2
          done
          echo "{db_name} is ready!"
      - name: wait-for-aggregator
        image: busybox:latest
        command:
        - sh
        - -c
        - |
          until wget -q --spider http://aggregator:9090/health; do
            echo "Waiting for aggregator to be ready..."
            sleep 2
          done
          echo "Aggregator is ready!"
"#,
            db_name = db_name,
            db_port = get_db_port(config.source_type),
        );
    }

    let first_container = config.containers.first();
    let default_resources = crate::config::ResourceLimits::default();
    let worker_resources = first_container
        .map(|w| &w.resources)
        .unwrap_or(&default_resources);

    let tables_all: Vec<String> = config
        .containers
        .iter()
        .flat_map(|c| c.tables.clone())
        .collect();
    let tables_arg = tables_all.join(",");

    let connection_string = first_container
        .map(|w| w.connection_string.as_str())
        .unwrap_or("");

    let dry_run_flag = if config.dry_run {
        " \\\n            --dry-run"
    } else {
        ""
    };

    format!(
        r#"apiVersion: batch/v1
kind: Job
metadata:
  name: loadtest-populate
  namespace: {namespace}
  labels:
    app: loadtest-populate
spec:
  parallelism: {container_count}
  completions: {container_count}
  completionMode: Indexed
  template:
    spec:
{init_containers}      containers:
      - name: populate
        image: surreal-sync:latest
        imagePullPolicy: IfNotPresent
        command:
        - /bin/sh
        - -c
        - |
          WORKER_ID="populate-$JOB_COMPLETION_INDEX"
          surreal-sync loadtest populate {source_type} \
            --worker-id $WORKER_ID \
            --tables {tables_arg} \
            --row-count {row_count} \
            --seed $((42 + JOB_COMPLETION_INDEX)) \
            --batch-size {batch_size} \
            --aggregator-url http://aggregator:9090 \
            --connection-string '{connection_string}'{dry_run_flag}
        env:
        - name: RUST_LOG
          value: "info"
        resources:
          limits:
            cpu: "{cpu_limit}"
            memory: "{memory_limit}"
          requests:
            cpu: "{cpu_request}"
            memory: "{memory_request}"
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: loadtest-config
      restartPolicy: OnFailure
"#,
        namespace = config.network_name,
        container_count = container_count,
        init_containers = init_containers,
        source_type = source_type_lower,
        tables_arg = tables_arg,
        row_count = first_container.map(|w| w.row_count).unwrap_or(10000),
        batch_size = first_container.map(|w| w.batch_size).unwrap_or(1000),
        connection_string = connection_string,
        dry_run_flag = dry_run_flag,
        cpu_limit = worker_resources.cpu_limit,
        memory_limit = worker_resources.memory_limit,
        cpu_request = worker_resources.cpu_request.as_deref().unwrap_or("0.25"),
        memory_request = worker_resources
            .memory_request
            .as_deref()
            .unwrap_or("256Mi"),
    )
}

/// Generate Sync Job.
/// Runs surreal-sync from <source> after populate containers complete.
fn generate_sync_job(config: &ClusterConfig) -> String {
    let dry_run_flag = if config.dry_run { " --dry-run" } else { "" };

    // Build command args based on source type (new CLI structure)
    let command_args = match config.source_type {
        SourceType::MySQL => {
            format!(
                r#"        - from
        - mysql
        - full
        - --connection-string
        - 'mysql://root:root@mysql:3306/{db}'
        - --to-namespace
        - {ns}
        - --to-database
        - {database}
        - --surreal-endpoint
        - 'http://surrealdb:8000'
        - --surreal-username
        - root
        - --surreal-password
        - root
        - --schema-file
        - /config/schema.yaml{dry_run}"#,
                db = config.database.database_name,
                ns = config.surrealdb.namespace,
                database = config.surrealdb.database,
                dry_run = if config.dry_run {
                    "\n        - --dry-run"
                } else {
                    ""
                }
            )
        }
        SourceType::PostgreSQL => {
            format!(
                r#"        - from
        - postgresql-trigger
        - full
        - --connection-string
        - 'postgresql://postgres:postgres@postgresql:5432/{db}'
        - --to-namespace
        - {ns}
        - --to-database
        - {database}
        - --surreal-endpoint
        - 'http://surrealdb:8000'
        - --surreal-username
        - root
        - --surreal-password
        - root
        - --schema-file
        - /config/schema.yaml{dry_run}"#,
                db = config.database.database_name,
                ns = config.surrealdb.namespace,
                database = config.surrealdb.database,
                dry_run = if config.dry_run {
                    "\n        - --dry-run"
                } else {
                    ""
                }
            )
        }
        SourceType::PostgreSQLLogical => {
            // Get all table names from containers for the --tables argument
            let tables: Vec<String> = config
                .containers
                .iter()
                .flat_map(|c| c.tables.clone())
                .collect();
            let tables_arg = tables.join(",");
            format!(
                r#"        - from
        - postgresql
        - full
        - --connection-string
        - 'postgresql://postgres:postgres@postgresql:5432/{db}'
        - --slot
        - surreal_sync_loadtest
        - --tables
        - '{tables}'
        - --to-namespace
        - {ns}
        - --to-database
        - {database}
        - --surreal-endpoint
        - 'http://surrealdb:8000'
        - --surreal-username
        - root
        - --surreal-password
        - root
        - --schema-file
        - /config/schema.yaml{dry_run}"#,
                db = config.database.database_name,
                tables = tables_arg,
                ns = config.surrealdb.namespace,
                database = config.surrealdb.database,
                dry_run = if config.dry_run {
                    "\n        - --dry-run"
                } else {
                    ""
                }
            )
        }
        SourceType::MongoDB => {
            format!(
                r#"        - from
        - mongodb
        - full
        - --connection-string
        - 'mongodb://root:root@mongodb:27017'
        - --database
        - {db}
        - --to-namespace
        - {ns}
        - --to-database
        - {database}
        - --surreal-endpoint
        - 'http://surrealdb:8000'
        - --surreal-username
        - root
        - --surreal-password
        - root
        - --schema-file
        - /config/schema.yaml{dry_run}"#,
                db = config.database.database_name,
                ns = config.surrealdb.namespace,
                database = config.surrealdb.database,
                dry_run = if config.dry_run {
                    "\n        - --dry-run"
                } else {
                    ""
                }
            )
        }
        SourceType::Neo4j => {
            format!(
                r#"        - from
        - neo4j
        - full
        - --connection-string
        - 'bolt://neo4j:7687'
        - --username
        - neo4j
        - --password
        - password
        - --to-namespace
        - {ns}
        - --to-database
        - {database}
        - --surreal-endpoint
        - 'http://surrealdb:8000'
        - --surreal-username
        - root
        - --surreal-password
        - root{dry_run}"#,
                ns = config.surrealdb.namespace,
                database = config.surrealdb.database,
                dry_run = if config.dry_run {
                    "\n        - --dry-run"
                } else {
                    ""
                }
            )
        }
        SourceType::Kafka => {
            format!(
                r#"        - from
        - kafka
        - full
        - --brokers
        - 'kafka:9092'
        - --to-namespace
        - {ns}
        - --to-database
        - {database}
        - --surreal-endpoint
        - 'http://surrealdb:8000'
        - --surreal-username
        - root
        - --surreal-password
        - root{dry_run}"#,
                ns = config.surrealdb.namespace,
                database = config.surrealdb.database,
                dry_run = if config.dry_run {
                    "\n        - --dry-run"
                } else {
                    ""
                }
            )
        }
        SourceType::Csv | SourceType::Jsonl => {
            let source_type = if config.source_type == SourceType::Csv {
                "csv"
            } else {
                "jsonl"
            };
            format!(
                r#"        - from
        - {source}
        - full
        - --input-dir
        - '/data'
        - --to-namespace
        - {ns}
        - --to-database
        - {database}
        - --surreal-endpoint
        - 'http://surrealdb:8000'
        - --surreal-username
        - root
        - --surreal-password
        - root
        - --schema-file
        - /config/schema.yaml{dry_run}"#,
                source = source_type,
                ns = config.surrealdb.namespace,
                database = config.surrealdb.database,
                dry_run = if config.dry_run {
                    "\n        - --dry-run"
                } else {
                    ""
                }
            )
        }
    };

    // Suppress unused variable warning
    let _ = dry_run_flag;

    format!(
        r#"apiVersion: batch/v1
kind: Job
metadata:
  name: loadtest-sync
  namespace: {namespace}
  labels:
    app: loadtest-sync
  annotations:
    # This job should wait for populate to complete
    argocd.argoproj.io/sync-wave: "1"
spec:
  template:
    spec:
      initContainers:
      - name: wait-for-populate
        image: bitnami/kubectl:latest
        command:
        - sh
        - -c
        - |
          echo "Waiting for populate job to complete..."
          kubectl wait --for=condition=complete job/loadtest-populate -n {namespace} --timeout=30m
          echo "Populate job completed!"
      containers:
      - name: sync
        image: surreal-sync:latest
        imagePullPolicy: IfNotPresent
        command:
        - surreal-sync
{command_args}
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
        env:
        - name: RUST_LOG
          value: "info"
        resources:
          limits:
            cpu: "1"
            memory: "1Gi"
          requests:
            cpu: "0.5"
            memory: "512Mi"
      volumes:
      - name: config
        configMap:
          name: loadtest-config
      restartPolicy: OnFailure
"#,
        namespace = config.network_name,
        command_args = command_args,
    )
}

/// Generate Verify Worker Job.
/// Runs after sync completes and verifies data in SurrealDB.
fn generate_verify_job(config: &ClusterConfig) -> String {
    let container_count = config.containers.len();

    let first_container = config.containers.first();
    let default_resources = crate::config::ResourceLimits::default();
    let worker_resources = first_container
        .map(|w| &w.resources)
        .unwrap_or(&default_resources);

    let tables_all: Vec<String> = config
        .containers
        .iter()
        .flat_map(|c| c.tables.clone())
        .collect();
    let tables_arg = tables_all.join(",");

    let dry_run_flag = if config.dry_run {
        " \\\n            --dry-run"
    } else {
        ""
    };

    format!(
        r#"apiVersion: batch/v1
kind: Job
metadata:
  name: loadtest-verify
  namespace: {namespace}
  labels:
    app: loadtest-verify
  annotations:
    # This job should wait for sync to complete
    argocd.argoproj.io/sync-wave: "2"
spec:
  parallelism: {container_count}
  completions: {container_count}
  completionMode: Indexed
  template:
    spec:
      initContainers:
      - name: wait-for-sync
        image: bitnami/kubectl:latest
        command:
        - sh
        - -c
        - |
          echo "Waiting for sync job to complete..."
          kubectl wait --for=condition=complete job/loadtest-sync -n {namespace} --timeout=30m
          echo "Sync job completed!"
      containers:
      - name: verify
        image: surreal-sync:latest
        imagePullPolicy: IfNotPresent
        command:
        - /bin/sh
        - -c
        - |
          surreal-sync loadtest verify \
            --surreal-endpoint 'http://surrealdb:8000' \
            --surreal-username root \
            --surreal-password root \
            --surreal-namespace {ns} \
            --surreal-database {db} \
            --schema /config/schema.yaml \
            --tables {tables_arg} \
            --seed $((42 + JOB_COMPLETION_INDEX)) \
            --row-count {row_count}{dry_run_flag}
        env:
        - name: RUST_LOG
          value: "info"
        resources:
          limits:
            cpu: "{cpu_limit}"
            memory: "{memory_limit}"
          requests:
            cpu: "{cpu_request}"
            memory: "{memory_request}"
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: loadtest-config
      restartPolicy: OnFailure
"#,
        namespace = config.network_name,
        ns = config.surrealdb.namespace,
        db = config.surrealdb.database,
        container_count = container_count,
        tables_arg = tables_arg,
        row_count = first_container.map(|w| w.row_count).unwrap_or(10000),
        dry_run_flag = dry_run_flag,
        cpu_limit = worker_resources.cpu_limit,
        memory_limit = worker_resources.memory_limit,
        cpu_request = worker_resources.cpu_request.as_deref().unwrap_or("0.25"),
        memory_request = worker_resources
            .memory_request
            .as_deref()
            .unwrap_or("256Mi"),
    )
}
/// Get default port for a database type.
fn get_db_port(source_type: SourceType) -> u16 {
    match source_type {
        SourceType::MySQL => 3306,
        SourceType::PostgreSQL | SourceType::PostgreSQLLogical => 5432,
        SourceType::MongoDB => 27017,
        SourceType::Neo4j => 7687,
        SourceType::Kafka => 9092,
        SourceType::Csv | SourceType::Jsonl => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn test_config() -> ClusterConfig {
        test_config_with_dry_run(false)
    }

    fn test_config_with_dry_run(dry_run: bool) -> ClusterConfig {
        ClusterConfig {
            platform: Platform::Kubernetes,
            source_type: SourceType::MySQL,
            containers: vec![ContainerConfig {
                id: "populate-1".to_string(),
                source_type: SourceType::MySQL,
                tables: vec!["users".to_string()],
                row_count: 1000,
                seed: 42,
                resources: ResourceLimits::default(),
                tmpfs: None,
                connection_string: "mysql://root:root@mysql:3306/testdb".to_string(),
                batch_size: 100,
            }],
            results_volume: VolumeConfig::default(),
            database: DatabaseConfig {
                source_type: SourceType::MySQL,
                image: "mysql:8.0".to_string(),
                resources: ResourceLimits::default(),
                tmpfs_storage: false,
                tmpfs_size: None,
                database_name: "testdb".to_string(),
                environment: vec![],
                command_args: vec![],
            },
            surrealdb: SurrealDbConfig::default(),
            schema_path: "schema.yaml".to_string(),
            network_name: "loadtest".to_string(),
            dry_run,
        }
    }

    #[test]
    fn test_generate_kubernetes() {
        let config = test_config();
        let generator = KubernetesGenerator;
        let yaml = generator.generate(&config).unwrap();

        assert!(yaml.contains("kind: Namespace"));
        assert!(yaml.contains("kind: ConfigMap"));
        assert!(yaml.contains("kind: Job"));
        assert!(yaml.contains("surrealdb"));
    }

    #[test]
    fn test_generate_to_files() {
        let config = test_config();
        let generator = KubernetesGenerator;
        let files = generator.generate_to_files(&config).unwrap();

        assert!(files.contains_key("namespace.yaml"));
        assert!(files.contains_key("configmap.yaml"));
        assert!(files.contains_key("populate-job.yaml"));
        assert!(files.contains_key("sync-job.yaml"));
        assert!(files.contains_key("verify-job.yaml"));
        assert!(files.contains_key("aggregator-deployment.yaml"));
        assert!(files.contains_key("aggregator-service.yaml"));
        assert!(files.contains_key("surrealdb-deployment.yaml"));
    }

    #[test]
    fn test_kubernetes_without_dry_run() {
        let config = test_config_with_dry_run(false);
        let generator = KubernetesGenerator;
        let yaml = generator.generate(&config).unwrap();

        // Should NOT contain --dry-run flag
        assert!(!yaml.contains("--dry-run"));
        // Should still contain populate command
        assert!(yaml.contains("loadtest populate mysql"));
    }

    #[test]
    fn test_kubernetes_with_dry_run() {
        let config = test_config_with_dry_run(true);
        let generator = KubernetesGenerator;
        let files = generator.generate_to_files(&config).unwrap();

        // Check populate job contains --dry-run
        let populate_job = files.get("populate-job.yaml").unwrap();
        assert!(
            populate_job.contains("--dry-run"),
            "Populate job should contain --dry-run flag"
        );

        // Check verify job contains --dry-run
        let verify_job = files.get("verify-job.yaml").unwrap();
        assert!(
            verify_job.contains("--dry-run"),
            "Verify job should contain --dry-run flag"
        );

        // Verify the combined YAML also contains --dry-run
        let combined = generator.generate(&config).unwrap();
        assert!(combined.contains("--dry-run"));
    }
}
