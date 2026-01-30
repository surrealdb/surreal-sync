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

        // RBAC for kubectl wait init containers
        manifests.push(generate_rbac(config));

        // ConfigMap for schema
        manifests.push(generate_configmap(config));

        // Proto ConfigMap for Kafka source
        if let Some(proto_cm) = generate_proto_configmap(config) {
            manifests.push(proto_cm);
        }

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
        files.insert("rbac.yaml".to_string(), generate_rbac(config));
        files.insert("configmap.yaml".to_string(), generate_configmap(config));

        // Proto ConfigMap for Kafka source
        if let Some(proto_cm) = generate_proto_configmap(config) {
            files.insert("proto-configmap.yaml".to_string(), proto_cm);
        }

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
        SourceType::MySQL | SourceType::MySQLIncremental => "mysql",
        SourceType::PostgreSQL
        | SourceType::PostgreSQLTriggerIncremental
        | SourceType::PostgreSQLWal2JsonIncremental => "postgresql",
        SourceType::MongoDB | SourceType::MongoDBIncremental => "mongodb",
        SourceType::Neo4j | SourceType::Neo4jIncremental => "neo4j",
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

/// Generate RBAC resources (ServiceAccount, Role, RoleBinding) for kubectl wait in init containers.
fn generate_rbac(config: &ClusterConfig) -> String {
    format!(
        r#"apiVersion: v1
kind: ServiceAccount
metadata:
  name: loadtest-runner
  namespace: {namespace}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: loadtest-job-watcher
  namespace: {namespace}
rules:
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: loadtest-runner-binding
  namespace: {namespace}
subjects:
- kind: ServiceAccount
  name: loadtest-runner
  namespace: {namespace}
roleRef:
  kind: Role
  name: loadtest-job-watcher
  apiGroup: rbac.authorization.k8s.io
"#,
        namespace = config.network_name
    )
}

/// Generate ConfigMap for schema and configuration.
fn generate_configmap(config: &ClusterConfig) -> String {
    // If schema content is provided, embed it in the ConfigMap
    let schema_data = if let Some(ref content) = config.schema_content {
        // Indent each line of the schema content for proper YAML formatting
        let indented_content = content
            .lines()
            .map(|line| format!("    {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        format!("  schema.yaml: |\n{indented_content}")
    } else {
        // Placeholder if schema content not provided
        "  # Schema file should be added here or mounted from external source\n  # schema.yaml: |\n  #   tables:\n  #     - name: users\n  #       ...".to_string()
    };

    format!(
        r#"apiVersion: v1
kind: ConfigMap
metadata:
  name: loadtest-config
  namespace: {namespace}
data:
{schema_data}
"#,
        namespace = config.network_name,
        schema_data = schema_data
    )
}

/// Generate ConfigMap for proto files (Kafka source only).
fn generate_proto_configmap(config: &ClusterConfig) -> Option<String> {
    let proto_contents = config.proto_contents.as_ref()?;
    if proto_contents.is_empty() {
        return None;
    }

    let mut proto_data = String::new();
    for (table_name, proto_content) in proto_contents {
        // Indent each line of the proto content for proper YAML formatting
        let indented_content = proto_content
            .lines()
            .map(|line| format!("    {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        proto_data.push_str(&format!("  {table_name}.proto: |\n{indented_content}\n"));
    }

    Some(format!(
        r#"apiVersion: v1
kind: ConfigMap
metadata:
  name: loadtest-proto
  namespace: {namespace}
data:
{proto_data}"#,
        namespace = config.network_name,
        proto_data = proto_data
    ))
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

/// Generate Populate Worker Jobs.
/// Creates separate jobs for each container (matching Docker Compose pattern).
/// Each job handles a specific set of tables with its own seed.
fn generate_populate_job(config: &ClusterConfig) -> String {
    let db_name = database_name(config.source_type);
    let source_type_lower = match config.source_type {
        SourceType::MySQL | SourceType::MySQLIncremental => "mysql",
        // All PostgreSQL variants use the same populate command (data goes to same database)
        SourceType::PostgreSQL
        | SourceType::PostgreSQLTriggerIncremental
        | SourceType::PostgreSQLWal2JsonIncremental => "postgresql",
        SourceType::MongoDB | SourceType::MongoDBIncremental => "mongodb",
        SourceType::Neo4j | SourceType::Neo4jIncremental => "neo4j",
        SourceType::Kafka => "kafka",
        SourceType::Csv => "csv",
        SourceType::Jsonl => "jsonl",
    };

    // Build init containers to wait for database and aggregator
    let init_containers =
        if config.source_type != SourceType::Csv && config.source_type != SourceType::Jsonl {
            let (wait_image, wait_command) = get_db_health_check(config.source_type, db_name);

            // For MongoDB, add extra init container to wait for mongodb-init job
            let mongodb_init_wait = if config.source_type == SourceType::MongoDB
                || config.source_type == SourceType::MongoDBIncremental
            {
                format!(
                    r#"      - name: wait-for-mongodb-init
        image: bitnami/kubectl:latest
        imagePullPolicy: IfNotPresent
        command:
        - sh
        - -c
        - |
          echo "Waiting for mongodb-init job to complete..."
          kubectl wait --for=condition=complete job/mongodb-init -n {namespace} --timeout=5m
          echo "mongodb-init job completed!"
"#,
                    namespace = config.network_name
                )
            } else {
                String::new()
            };

            format!(
                r#"      initContainers:
      - name: wait-for-db
        image: {wait_image}
        imagePullPolicy: IfNotPresent
        command:
        - sh
        - -c
        - |
{wait_command}
{mongodb_init_wait}      - name: wait-for-aggregator
        image: busybox:latest
        imagePullPolicy: IfNotPresent
        command:
        - sh
        - -c
        - |
          until wget -q --spider http://aggregator:9090/health; do
            echo "Waiting for aggregator to be ready..."
            sleep 2
          done
          echo "Aggregator is ready!"
"#
            )
        } else {
            String::new()
        };

    // For MongoDB, we need serviceAccountName for the kubectl wait in init container
    let service_account_line = if config.source_type == SourceType::MongoDB
        || config.source_type == SourceType::MongoDBIncremental
    {
        "      serviceAccountName: loadtest-runner\n"
    } else {
        ""
    };

    let dry_run_flag = if config.dry_run {
        "\n        - --dry-run"
    } else {
        ""
    };

    // Generate separate jobs for each container (matching Docker Compose pattern)
    let mut jobs = Vec::new();

    for (idx, container) in config.containers.iter().enumerate() {
        let job_index = idx + 1;
        let tables_arg = container.tables.join(",");

        // Build connection string args as separate YAML list items (each flag and value on its own line)
        let connection_args = match config.source_type {
            SourceType::MySQL | SourceType::MySQLIncremental => {
                format!(
                    "- --mysql-connection-string\n        - '{}'",
                    container.connection_string
                )
            }
            SourceType::PostgreSQL
            | SourceType::PostgreSQLTriggerIncremental
            | SourceType::PostgreSQLWal2JsonIncremental => {
                format!(
                    "- --postgresql-connection-string\n        - '{}'",
                    container.connection_string
                )
            }
            SourceType::MongoDB | SourceType::MongoDBIncremental => {
                format!(
                    "- --mongodb-connection-string\n        - '{}'\n        - --mongodb-database\n        - {}",
                    container.connection_string, config.database.database_name
                )
            }
            SourceType::Neo4j | SourceType::Neo4jIncremental => {
                format!(
                    "- --neo4j-connection-string\n        - '{}'\n        - --neo4j-username\n        - neo4j\n        - --neo4j-password\n        - password",
                    container.connection_string
                )
            }
            SourceType::Kafka => {
                format!(
                    "- --kafka-brokers\n        - '{}'",
                    container.connection_string
                )
            }
            SourceType::Csv | SourceType::Jsonl => {
                format!(
                    "- --output-dir\n        - '{}'",
                    container.connection_string
                )
            }
        };

        let job = format!(
            r#"apiVersion: batch/v1
kind: Job
metadata:
  name: loadtest-populate-{job_index}
  namespace: {namespace}
  labels:
    app: loadtest-populate
spec:
  template:
    spec:
{service_account_line}{init_containers}      containers:
      - name: populate
        image: surreal-sync:latest
        imagePullPolicy: IfNotPresent
        command:
        - surreal-sync
        args:
        - loadtest
        - populate
        - {source_type}
        - --schema
        - /config/schema.yaml
        - --tables
        - '{tables_arg}'
        - --row-count
        - '{row_count}'
        - --seed
        - '{seed}'
        - --batch-size
        - '{batch_size}'
        {connection_args}
        - --aggregator-url
        - http://aggregator:9090{dry_run_flag}
        env:
        - name: CONTAINER_ID
          value: "populate-{job_index}"
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
      restartPolicy: OnFailure"#,
            namespace = config.network_name,
            job_index = job_index,
            service_account_line = service_account_line,
            init_containers = init_containers,
            source_type = source_type_lower,
            tables_arg = tables_arg,
            row_count = container.row_count,
            seed = container.seed,
            batch_size = container.batch_size,
            connection_args = connection_args,
            dry_run_flag = dry_run_flag,
            cpu_limit = container.resources.cpu_limit,
            memory_limit = container.resources.memory_limit,
            cpu_request = container.resources.cpu_request.as_deref().unwrap_or("0.25"),
            memory_request = container
                .resources
                .memory_request
                .as_deref()
                .unwrap_or("256Mi"),
        );
        jobs.push(job);
    }

    jobs.join("\n---\n")
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
        SourceType::MySQLIncremental => {
            // For incremental trigger-based sync, this generates the full-sync-setup job
            let tables: Vec<String> = config
                .containers
                .iter()
                .flat_map(|c| c.tables.clone())
                .collect();
            let tables_arg = tables.join(",");
            format!(
                r#"        - from
        - mysql
        - full
        - --connection-string
        - 'mysql://root:root@mysql:3306/{db}'
        - --tables
        - '{tables}'
        - --checkpoints-surreal-table
        - surreal_sync_checkpoints
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
        SourceType::PostgreSQLTriggerIncremental => {
            // For incremental trigger-based sync, this generates the full-sync-setup job
            let tables: Vec<String> = config
                .containers
                .iter()
                .flat_map(|c| c.tables.clone())
                .collect();
            let tables_arg = tables.join(",");
            format!(
                r#"        - from
        - postgresql-trigger
        - full
        - --connection-string
        - 'postgresql://postgres:postgres@postgresql:5432/{db}'
        - --tables
        - '{tables}'
        - --checkpoints-surreal-table
        - surreal_sync_checkpoints
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
        SourceType::PostgreSQLWal2JsonIncremental => {
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
        SourceType::MongoDBIncremental => {
            // For incremental change stream sync, this generates the full-sync-setup job
            let tables: Vec<String> = config
                .containers
                .iter()
                .flat_map(|c| c.tables.clone())
                .collect();
            let tables_arg = tables.join(",");
            format!(
                r#"        - from
        - mongodb
        - full
        - --connection-string
        - 'mongodb://root:root@mongodb:27017'
        - --database
        - {db}
        - --tables
        - '{tables}'
        - --checkpoints-surreal-table
        - surreal_sync_checkpoints
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
        - root
        - --schema-file
        - /config/schema.yaml{dry_run}"#,
                ns = config.surrealdb.namespace,
                database = config.surrealdb.database,
                dry_run = if config.dry_run {
                    "\n        - --dry-run"
                } else {
                    ""
                }
            )
        }
        SourceType::Neo4jIncremental => {
            // For incremental timestamp-based sync, this generates the full-sync-setup job
            let tables: Vec<String> = config
                .containers
                .iter()
                .flat_map(|c| c.tables.clone())
                .collect();
            let tables_arg = tables.join(",");
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
        - --tables
        - '{tables}'
        - --checkpoints-surreal-table
        - surreal_sync_checkpoints
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
        SourceType::Kafka => {
            // Kafka needs multiple sync jobs (one per topic), handled by generate_kafka_sync_jobs
            // Return empty string here - Kafka sync is handled separately
            return generate_kafka_sync_jobs(config);
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

    // Build wait-for-populate command that waits for all populate jobs
    let container_count = config.containers.len();
    let wait_for_populate_cmd = if container_count == 1 {
        format!(
            r#"echo "Waiting for populate job to complete..."
          kubectl wait --for=condition=complete job/loadtest-populate-1 -n {namespace} --timeout=30m
          echo "Populate job completed!""#,
            namespace = config.network_name
        )
    } else {
        let job_indices: Vec<String> = (1..=container_count).map(|i| i.to_string()).collect();
        format!(
            r#"echo "Waiting for all populate jobs to complete..."
          for i in {indices}; do
            echo "Waiting for loadtest-populate-$i..."
            kubectl wait --for=condition=complete job/loadtest-populate-$i -n {namespace} --timeout=30m
          done
          echo "All populate jobs completed!""#,
            indices = job_indices.join(" "),
            namespace = config.network_name
        )
    };

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
      serviceAccountName: loadtest-runner
      initContainers:
      - name: wait-for-populate
        image: bitnami/kubectl:latest
        imagePullPolicy: IfNotPresent
        command:
        - sh
        - -c
        - |
          {wait_for_populate_cmd}
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

/// Generate Verify Worker Jobs.
/// Creates separate jobs for each container (matching Docker Compose pattern).
/// Each job verifies a specific set of tables with its matching seed.
fn generate_verify_job(config: &ClusterConfig) -> String {
    let tables_all: Vec<String> = config
        .containers
        .iter()
        .flat_map(|c| c.tables.clone())
        .collect();

    let dry_run_flag = if config.dry_run {
        "\n        - --dry-run"
    } else {
        ""
    };

    // Build wait-for-sync command
    let wait_for_sync_command = if config.source_type == SourceType::Kafka {
        // For Kafka, wait for all sync jobs (one per table)
        // Convert table names to valid K8s names (replace underscores with dashes)
        let k8s_table_names: Vec<String> = tables_all.iter().map(|t| t.replace('_', "-")).collect();
        format!(
            r#"echo "Waiting for all Kafka sync jobs to complete..."
          for table in {}; do
            echo "Waiting for loadtest-sync-$table..."
            kubectl wait --for=condition=complete job/loadtest-sync-$table -n {} --timeout=30m
          done
          echo "All sync jobs completed!""#,
            k8s_table_names.join(" "),
            config.network_name
        )
    } else {
        format!(
            r#"echo "Waiting for sync job to complete..."
          kubectl wait --for=condition=complete job/loadtest-sync -n {} --timeout=30m
          echo "Sync job completed!""#,
            config.network_name
        )
    };

    // Generate separate jobs for each container (matching Docker Compose pattern)
    let mut jobs = Vec::new();

    for (idx, container) in config.containers.iter().enumerate() {
        let job_index = idx + 1;
        let tables_arg = container.tables.join(",");

        let job = format!(
            r#"apiVersion: batch/v1
kind: Job
metadata:
  name: loadtest-verify-{job_index}
  namespace: {namespace}
  labels:
    app: loadtest-verify
  annotations:
    # This job should wait for sync to complete
    argocd.argoproj.io/sync-wave: "2"
spec:
  template:
    spec:
      serviceAccountName: loadtest-runner
      initContainers:
      - name: wait-for-sync
        image: bitnami/kubectl:latest
        imagePullPolicy: IfNotPresent
        command:
        - sh
        - -c
        - |
          {wait_for_sync_command}
      containers:
      - name: verify
        image: surreal-sync:latest
        imagePullPolicy: IfNotPresent
        command:
        - surreal-sync
        args:
        - loadtest
        - verify
        - --surreal-endpoint
        - 'http://surrealdb:8000'
        - --surreal-username
        - root
        - --surreal-password
        - root
        - --surreal-namespace
        - {ns}
        - --surreal-database
        - {db}
        - --schema
        - /config/schema.yaml
        - --tables
        - '{tables_arg}'
        - --seed
        - '{seed}'
        - --row-count
        - '{row_count}'
        - --aggregator-url
        - http://aggregator:9090{dry_run_flag}
        env:
        - name: CONTAINER_ID
          value: "verify-{job_index}"
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
      restartPolicy: OnFailure"#,
            namespace = config.network_name,
            job_index = job_index,
            wait_for_sync_command = wait_for_sync_command,
            ns = config.surrealdb.namespace,
            db = config.surrealdb.database,
            tables_arg = tables_arg,
            seed = container.seed,
            row_count = container.row_count,
            dry_run_flag = dry_run_flag,
            cpu_limit = container.resources.cpu_limit,
            memory_limit = container.resources.memory_limit,
            cpu_request = container.resources.cpu_request.as_deref().unwrap_or("0.25"),
            memory_request = container
                .resources
                .memory_request
                .as_deref()
                .unwrap_or("256Mi"),
        );
        jobs.push(job);
    }

    jobs.join("\n---\n")
}

/// Convert table name to PascalCase for Kafka message type.
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

/// Generate Kafka sync jobs - one job per topic/table.
/// Each job reads from a Kafka topic and syncs to SurrealDB.
/// Uses --max-messages to enable early exit once all expected messages are consumed.
fn generate_kafka_sync_jobs(config: &ClusterConfig) -> String {
    // Collect tables with their row counts from containers
    let tables_with_counts: Vec<(String, u64)> = config
        .containers
        .iter()
        .flat_map(|c| c.tables.iter().map(|t| (t.clone(), c.row_count)))
        .collect();

    let dry_run_flag = if config.dry_run { " --dry-run" } else { "" };

    // Build wait-for-populate command for all populate jobs
    let container_count = config.containers.len();
    let wait_for_populate_cmd = if container_count == 1 {
        format!(
            r#"echo "Waiting for populate job to complete..."
          kubectl wait --for=condition=complete job/loadtest-populate-1 -n {namespace} --timeout=30m
          echo "Populate job completed!""#,
            namespace = config.network_name
        )
    } else {
        let job_indices: Vec<String> = (1..=container_count).map(|i| i.to_string()).collect();
        format!(
            r#"echo "Waiting for all populate jobs to complete..."
          for i in {indices}; do
            echo "Waiting for loadtest-populate-$i..."
            kubectl wait --for=condition=complete job/loadtest-populate-$i -n {namespace} --timeout=30m
          done
          echo "All populate jobs completed!""#,
            indices = job_indices.join(" "),
            namespace = config.network_name
        )
    };

    let mut jobs = Vec::new();

    for (table_name, row_count) in &tables_with_counts {
        let message_type = to_pascal_case(table_name);
        // Convert table name to valid K8s name (replace underscores with dashes)
        let k8s_name = table_name.replace('_', "-");
        let job = format!(
            r#"apiVersion: batch/v1
kind: Job
metadata:
  name: loadtest-sync-{k8s_name}
  namespace: {namespace}
  labels:
    app: loadtest-sync
    topic: {table_name}
  annotations:
    # This job should wait for populate to complete
    argocd.argoproj.io/sync-wave: "1"
spec:
  template:
    spec:
      serviceAccountName: loadtest-runner
      initContainers:
      - name: wait-for-populate
        image: bitnami/kubectl:latest
        imagePullPolicy: IfNotPresent
        command:
        - sh
        - -c
        - |
          {wait_for_populate_cmd}
      containers:
      - name: sync
        image: surreal-sync:latest
        imagePullPolicy: IfNotPresent
        command:
        - surreal-sync
        args:
        - from
        - kafka
        - --proto-path
        - '/proto/{table_name}.proto'
        - --brokers
        - 'kafka-0.kafka:9092'
        - --group-id
        - 'loadtest-sync-{table_name}'
        - --topic
        - '{table_name}'
        - --message-type
        - '{message_type}'
        - --buffer-size
        - '1000'
        - --session-timeout-ms
        - '30000'
        - --kafka-batch-size
        - '100'
        - --max-messages
        - '{row_count}'
        - --timeout
        - '3m'
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
        - /config/schema.yaml{dry_run_flag}
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
        - name: proto
          mountPath: /proto
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
      - name: proto
        configMap:
          name: loadtest-proto
      restartPolicy: OnFailure"#,
            namespace = config.network_name,
            k8s_name = k8s_name,
            table_name = table_name,
            message_type = message_type,
            row_count = row_count,
            ns = config.surrealdb.namespace,
            database = config.surrealdb.database,
            dry_run_flag = dry_run_flag,
            wait_for_populate_cmd = wait_for_populate_cmd,
        );
        jobs.push(job);
    }

    jobs.join("\n---\n")
}

/// Get database-specific health check configuration for init containers.
/// Returns (image, command) tuple for the health check.
fn get_db_health_check(source_type: SourceType, db_name: &str) -> (&'static str, String) {
    match source_type {
        SourceType::MySQL | SourceType::MySQLIncremental => (
            "mysql:8.0",
            format!(
                r#"          echo "Waiting for MySQL to be ready..."
          until mysqladmin ping -h{db_name} -uroot -proot --silent; do
            echo "MySQL is not ready yet..."
            sleep 2
          done
          echo "MySQL is ready!""#
            ),
        ),
        SourceType::PostgreSQL
        | SourceType::PostgreSQLTriggerIncremental
        | SourceType::PostgreSQLWal2JsonIncremental => (
            "postgres:16",
            format!(
                r#"          echo "Waiting for PostgreSQL to be ready..."
          until pg_isready -h {db_name} -U postgres; do
            echo "PostgreSQL is not ready yet..."
            sleep 2
          done
          echo "PostgreSQL is ready!""#
            ),
        ),
        SourceType::MongoDB | SourceType::MongoDBIncremental => (
            "mongo:7",
            format!(
                r#"          echo "Waiting for MongoDB to be ready..."
          until mongosh --host {db_name} --eval "db.adminCommand('ping')" --quiet; do
            echo "MongoDB is not ready yet..."
            sleep 2
          done
          echo "MongoDB is ready!""#
            ),
        ),
        SourceType::Neo4j | SourceType::Neo4jIncremental => (
            "busybox:latest",
            format!(
                r#"          echo "Waiting for Neo4j to be ready..."
          until wget -q --spider http://{db_name}:7474; do
            echo "Neo4j is not ready yet..."
            sleep 2
          done
          echo "Neo4j is ready!""#
            ),
        ),
        SourceType::Kafka => (
            "apache/kafka:latest",
            // For Kafka with headless service, use pod-specific DNS name (kafka-0.kafka)
            format!(
                r#"          echo "Waiting for Kafka to be ready..."
          until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server {db_name}-0.{db_name}:9092 2>/dev/null; do
            echo "Kafka is not ready yet..."
            sleep 2
          done
          echo "Kafka is ready!""#
            ),
        ),
        SourceType::Csv | SourceType::Jsonl => (
            "busybox:latest",
            "          echo \"No database to wait for\"".to_string(),
        ),
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
            schema_content: Some(
                "tables:\n  - name: users\n    columns:\n      - name: id\n        type: int\n"
                    .to_string(),
            ),
            proto_contents: None, // Only needed for Kafka source
            network_name: "loadtest".to_string(),
            dry_run,
            num_sync_containers: 1, // MySQL uses single sync container
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
        assert!(files.contains_key("rbac.yaml"));
        assert!(files.contains_key("configmap.yaml"));
        assert!(files.contains_key("populate-job.yaml"));
        assert!(files.contains_key("sync-job.yaml"));
        assert!(files.contains_key("verify-job.yaml"));
        assert!(files.contains_key("aggregator-deployment.yaml"));
        assert!(files.contains_key("aggregator-service.yaml"));
        assert!(files.contains_key("surrealdb-deployment.yaml"));
    }

    #[test]
    fn test_configmap_contains_schema() {
        let config = test_config();
        let generator = KubernetesGenerator;
        let files = generator.generate_to_files(&config).unwrap();
        let configmap = files.get("configmap.yaml").unwrap();

        assert!(
            configmap.contains("schema.yaml: |"),
            "ConfigMap should contain schema.yaml data"
        );
        assert!(
            configmap.contains("name: users"),
            "ConfigMap should contain the actual schema content"
        );
    }

    #[test]
    fn test_rbac_contains_required_resources() {
        let config = test_config();
        let generator = KubernetesGenerator;
        let files = generator.generate_to_files(&config).unwrap();
        let rbac = files.get("rbac.yaml").unwrap();

        assert!(
            rbac.contains("kind: ServiceAccount"),
            "RBAC should contain ServiceAccount"
        );
        assert!(rbac.contains("kind: Role"), "RBAC should contain Role");
        assert!(
            rbac.contains("kind: RoleBinding"),
            "RBAC should contain RoleBinding"
        );
        assert!(
            rbac.contains("loadtest-runner"),
            "RBAC should use loadtest-runner service account"
        );
    }

    #[test]
    fn test_sync_and_verify_jobs_use_service_account() {
        let config = test_config();
        let generator = KubernetesGenerator;
        let files = generator.generate_to_files(&config).unwrap();

        let sync_job = files.get("sync-job.yaml").unwrap();
        assert!(
            sync_job.contains("serviceAccountName: loadtest-runner"),
            "Sync job should use loadtest-runner service account"
        );

        let verify_job = files.get("verify-job.yaml").unwrap();
        assert!(
            verify_job.contains("serviceAccountName: loadtest-runner"),
            "Verify job should use loadtest-runner service account"
        );
    }

    #[test]
    fn test_kubernetes_without_dry_run() {
        let config = test_config_with_dry_run(false);
        let generator = KubernetesGenerator;
        let yaml = generator.generate(&config).unwrap();

        // Should NOT contain --dry-run flag
        assert!(!yaml.contains("--dry-run"));
        // Should still contain populate command (in args array format)
        assert!(yaml.contains("- populate"));
        assert!(yaml.contains("- mysql"));
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
