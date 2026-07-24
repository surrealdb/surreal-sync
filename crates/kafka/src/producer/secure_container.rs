//! Docker container management for SASL_SSL + mTLS Kafka test instances.
//!
//! Starts an `apache/kafka:latest` broker with SCRAM-SHA-256 client authentication,
//! required client TLS certificates, and a custom entrypoint that bootstraps SCRAM
//! credentials into KRaft metadata at storage format time.

use crate::producer::certs::{
    KafkaTestSecrets, DEFAULT_SASL_PASSWORD, DEFAULT_SASL_USERNAME, INTER_BROKER_PASSWORD,
    INTER_BROKER_USERNAME,
};
use anyhow::{Context, Result};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::{debug, info};

const DOCKER_IMAGE: &str = "apache/kafka:latest";
const CLUSTER_ID: &str = "4L6g3nShT-eMCtK--X86sw";

/// A secured test Kafka container (SASL_SSL + SCRAM-SHA-256 + required mTLS).
pub struct SecureKafkaContainer {
    pub container_name: String,
    pub host_port: u16,
    /// Broker address in `localhost:<port>` form for rdkafka clients.
    pub broker_address: String,
    secrets: KafkaTestSecrets,
}

impl SecureKafkaContainer {
    /// Creates a new container configuration. Generates TLS/SASL secrets eagerly.
    /// Call [`start`](Self::start) before using the broker.
    pub fn new(container_name: &str) -> Result<Self> {
        Ok(Self {
            container_name: container_name.to_string(),
            host_port: 0,
            broker_address: String::new(),
            secrets: KafkaTestSecrets::generate()?,
        })
    }

    /// Reference to the generated TLS/SASL secrets (PEM paths, SCRAM credentials).
    pub fn secrets(&self) -> &KafkaTestSecrets {
        &self.secrets
    }

    /// Finds a free host port, starts the secured Kafka container, and sets
    /// [`broker_address`](Self::broker_address).
    pub fn start(&mut self) -> Result<()> {
        info!("Starting secured Kafka container: {}", self.container_name);

        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        let port = find_available_port()?;
        let port_mapping = format!("{port}:9092");
        let advertised = format!("SASL_SSL://localhost:{port}");

        write_entrypoint_script(self.secrets.secrets_dir())?;

        let secrets_mount = format!(
            "{}:/etc/kafka/secrets",
            self.secrets.secrets_dir().display()
        );

        let output = Command::new("docker")
            .args([
                "run",
                "--name",
                &self.container_name,
                "-v",
                &secrets_mount,
                "--entrypoint",
                "/etc/kafka/secrets/secure-entrypoint.sh",
                "-e",
                "KAFKA_NODE_ID=1",
                "-e",
                "KAFKA_PROCESS_ROLES=broker,controller",
                "-e",
                "KAFKA_LISTENERS=SASL_SSL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
                "-e",
                &format!("KAFKA_ADVERTISED_LISTENERS={advertised}"),
                "-e",
                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,SASL_SSL:SASL_SSL",
                "-e",
                "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                "-e",
                "KAFKA_SASL_ENABLED_MECHANISMS=SCRAM-SHA-256",
                "-e",
                "KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=SCRAM-SHA-256",
                "-e",
                "KAFKA_SECURITY_INTER_BROKER_PROTOCOL=SASL_SSL",
                "-e",
                "KAFKA_SSL_CLIENT_AUTH=required",
                "-e",
                "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=",
                "-e",
                "KAFKA_SSL_KEYSTORE_FILENAME=kafka.keystore.jks",
                "-e",
                "KAFKA_SSL_KEYSTORE_CREDENTIALS=kafka_keystore_creds",
                "-e",
                "KAFKA_SSL_KEY_CREDENTIALS=kafka_ssl_key_creds",
                "-e",
                "KAFKA_SSL_TRUSTSTORE_FILENAME=kafka.truststore.jks",
                "-e",
                "KAFKA_SSL_TRUSTSTORE_CREDENTIALS=kafka_truststore_creds",
                "-e",
                "KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf",
                "-e",
                &format!("CLUSTER_ID={CLUSTER_ID}"),
                "-e",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                "-e",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
                "-e",
                "KAFKA_NUM_PARTITIONS=3",
                "-e",
                "KAFKA_LOG_DIRS=/tmp/kraft-combined-logs",
                "-e",
                &format!("KAFKA_SCRAM_CLIENT_USER={DEFAULT_SASL_USERNAME}"),
                "-e",
                &format!("KAFKA_SCRAM_CLIENT_PASSWORD={DEFAULT_SASL_PASSWORD}"),
                "-e",
                &format!("KAFKA_SCRAM_BROKER_USER={INTER_BROKER_USERNAME}"),
                "-e",
                &format!("KAFKA_SCRAM_BROKER_PASSWORD={INTER_BROKER_PASSWORD}"),
                "-p",
                &port_mapping,
                "-d",
                DOCKER_IMAGE,
            ])
            .output()
            .context("Failed to start secured Kafka Docker container")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to start secured Kafka container: {stderr}");
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        info!("Started secured Kafka container: {}", container_id);

        self.host_port = port;
        self.broker_address = format!("localhost:{port}");

        info!(
            "Secured Kafka container bound to dynamic port {} (broker: {})",
            self.host_port, self.broker_address
        );

        Ok(())
    }

    /// Polls until the broker accepts SASL_SSL + mTLS + SCRAM connections, up to `timeout_secs`.
    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        info!("Waiting for secured Kafka to be ready...");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout {
            let status = Command::new("docker")
                .args([
                    "exec",
                    &self.container_name,
                    "/opt/kafka/bin/kafka-broker-api-versions.sh",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--command-config",
                    "/etc/kafka/secrets/client.properties",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();

            match status {
                Ok(s) if s.success() => {
                    info!("Secured Kafka is ready!");
                    return Ok(());
                }
                Ok(_) => {
                    debug!("Secured Kafka not ready yet, retrying...");
                }
                Err(e) => {
                    debug!("Secured Kafka health check command failed: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("Secured Kafka did not become ready within {timeout_secs} seconds")
    }

    pub fn stop(&self) -> Result<()> {
        info!("Stopping secured Kafka container: {}", self.container_name);

        let output = Command::new("docker")
            .args(["stop", &self.container_name])
            .output()
            .context("Failed to stop container")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!("Failed to stop container (may not exist): {}", stderr);
        }

        let output = Command::new("docker")
            .args(["rm", &self.container_name])
            .output()
            .context("Failed to remove container")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!("Failed to remove container (may not exist): {}", stderr);
        }

        info!("Secured Kafka container stopped and removed");
        Ok(())
    }

    pub fn get_logs(&self) -> Result<String> {
        let output = Command::new("docker")
            .args(["logs", &self.container_name])
            .output()
            .context("Failed to get container logs")?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        Ok(format!("STDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"))
    }
}

impl Drop for SecureKafkaContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Custom entrypoint: configure broker properties, bootstrap SCRAM credentials into
/// KRaft metadata, then delegate to the image's normal startup script.
///
/// Inter-broker authentication uses SCRAM-SHA-256 over SASL_SSL (same listener as clients).
/// If single-node KRaft inter-broker SCRAM proves flaky, fall back to PLAIN for
/// `KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL` only while keeping SCRAM-SHA-256 for
/// client connections — client-side SASL_SSL + mTLS + SCRAM remains unchanged.
fn write_entrypoint_script(secrets_dir: &Path) -> Result<()> {
    let script = r#"#!/usr/bin/env bash
set -euo pipefail

. /etc/kafka/docker/bash-config
. /etc/kafka/docker/configureDefaults
. /etc/kafka/docker/configure

LOG_DIR="${KAFKA_LOG_DIRS:-/tmp/kraft-combined-logs}"
CLUSTER_ID="${CLUSTER_ID:?CLUSTER_ID is required}"
CLIENT_USER="${KAFKA_SCRAM_CLIENT_USER:?}"
CLIENT_PASSWORD="${KAFKA_SCRAM_CLIENT_PASSWORD:?}"
BROKER_USER="${KAFKA_SCRAM_BROKER_USER:?}"
BROKER_PASSWORD="${KAFKA_SCRAM_BROKER_PASSWORD:?}"

if [ ! -f "${LOG_DIR}/meta.properties" ]; then
  echo "===> Formatting KRaft storage with SCRAM credentials ..."
  /opt/kafka/bin/kafka-storage.sh format \
    -t "${CLUSTER_ID}" \
    -c /opt/kafka/config/server.properties \
    --standalone \
    --add-scram "SCRAM-SHA-256=[name=${CLIENT_USER},password=${CLIENT_PASSWORD}]" \
    --add-scram "SCRAM-SHA-256=[name=${BROKER_USER},password=${BROKER_PASSWORD}]"
fi

exec /etc/kafka/docker/run
"#;

    let path = secrets_dir.join("secure-entrypoint.sh");
    std::fs::write(&path, script).context("Failed to write secure entrypoint script")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .context("Failed to mark secure entrypoint script executable")?;
    }

    Ok(())
}

fn find_available_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("Failed to bind to ephemeral port")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Full broker boot smoke test (Docker required). Run with:
    /// `cargo test -p surreal-sync-kafka (producer) secure_boot -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires Docker and ~30-60s broker startup"]
    async fn secure_boot_smoke() {
        let mut container =
            SecureKafkaContainer::new("surreal-sync-secure-kafka-boot-smoke").unwrap();
        container.start().unwrap();
        if let Err(err) = container.wait_until_ready(90).await {
            let logs = container.get_logs().unwrap_or_default();
            panic!("broker not ready: {err}\n{logs}");
        }
    }
}
