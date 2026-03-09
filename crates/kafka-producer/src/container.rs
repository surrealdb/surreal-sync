//! Docker container management for Kafka test instances.
//!
//! Provides a `KafkaContainer` that runs an `apache/kafka:latest` Docker image
//! with dynamic port binding. Unlike PostgreSQL containers, Kafka requires the
//! advertised listener address to match the actual host port, so we find a free
//! port *before* starting the container.

use anyhow::{Context, Result};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::{debug, info};

const DOCKER_IMAGE: &str = "apache/kafka:latest";

/// A test Kafka container backed by Docker with dynamic port binding.
pub struct KafkaContainer {
    pub container_name: String,
    pub host_port: u16,
    /// Broker address in `localhost:<port>` form, ready to pass to rdkafka clients.
    pub broker_address: String,
}

impl KafkaContainer {
    /// Creates a new container configuration. Call [`start`](Self::start) before using it.
    pub fn new(container_name: &str) -> Self {
        Self {
            container_name: container_name.to_string(),
            host_port: 0,
            broker_address: String::new(),
        }
    }

    /// Finds a free host port, starts the Kafka container with dynamic port binding,
    /// and sets [`broker_address`](Self::broker_address).
    pub fn start(&mut self) -> Result<()> {
        info!("Starting Kafka container: {}", self.container_name);

        // Remove any leftover container with the same name
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        let port = find_available_port()?;
        let port_mapping = format!("{port}:9092");
        let advertised = format!("PLAINTEXT://localhost:{port}");

        let output = Command::new("docker")
            .args([
                "run",
                "--name",
                &self.container_name,
                "-e",
                "KAFKA_NODE_ID=1",
                "-e",
                "KAFKA_PROCESS_ROLES=broker,controller",
                "-e",
                "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
                "-e",
                &format!("KAFKA_ADVERTISED_LISTENERS={advertised}"),
                "-e",
                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "-e",
                "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
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
                "-p",
                &port_mapping,
                "-d",
                DOCKER_IMAGE,
            ])
            .output()
            .context("Failed to start Kafka Docker container")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to start Kafka container: {stderr}");
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        info!("Started Kafka container: {}", container_id);

        self.host_port = port;
        self.broker_address = format!("localhost:{port}");

        info!(
            "Kafka container bound to dynamic port {} (broker: {})",
            self.host_port, self.broker_address
        );

        Ok(())
    }

    /// Polls until Kafka is ready to accept connections, up to `timeout_secs`.
    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        info!("Waiting for Kafka to be ready...");

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
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();

            match status {
                Ok(s) if s.success() => {
                    info!("Kafka is ready!");
                    return Ok(());
                }
                Ok(_) => {
                    debug!("Kafka not ready yet, retrying...");
                }
                Err(e) => {
                    debug!("Health check command failed: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("Kafka did not become ready within {timeout_secs} seconds")
    }

    pub fn stop(&self) -> Result<()> {
        info!("Stopping Kafka container: {}", self.container_name);

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

        info!("Kafka container stopped and removed");
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

impl Drop for KafkaContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Finds a free TCP port on localhost by binding to port 0.
fn find_available_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("Failed to bind to ephemeral port")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}
