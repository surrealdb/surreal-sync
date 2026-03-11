//! Docker container management for Neo4j test instances.
//!
//! Provides a `Neo4jContainer` that runs `neo4j:5` with dynamic port binding
//! (`-p 0:7687`) and discovers the assigned host port via `docker port`.

use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::{debug, info};

const DOCKER_IMAGE: &str = "neo4j:5";
const NEO4J_USERNAME: &str = "neo4j";
const NEO4J_PASSWORD: &str = "password";
const NEO4J_DATABASE: &str = "neo4j";

/// A test Neo4j container backed by Docker with dynamic port binding.
pub struct Neo4jContainer {
    pub container_name: String,
    pub host_port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
}

impl Neo4jContainer {
    /// Creates a new container configuration. Call [`start`](Self::start) before
    /// using the connection.
    pub fn new(container_name: &str) -> Self {
        Self {
            container_name: container_name.to_string(),
            host_port: 0,
            username: NEO4J_USERNAME.to_string(),
            password: NEO4J_PASSWORD.to_string(),
            database: NEO4J_DATABASE.to_string(),
        }
    }

    /// Returns a Bolt URI pointing to the dynamically bound host port.
    pub fn bolt_uri(&self) -> String {
        format!("bolt://localhost:{}", self.host_port)
    }

    /// Starts the container with dynamic port binding and discovers the assigned port.
    pub fn start(&mut self) -> Result<()> {
        info!("Starting Neo4j container: {}", self.container_name);

        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        let output = Command::new("docker")
            .args([
                "run",
                "--name",
                &self.container_name,
                "-e",
                &format!("NEO4J_AUTH={NEO4J_USERNAME}/{NEO4J_PASSWORD}"),
                "-e",
                "NEO4J_ACCEPT_LICENSE_AGREEMENT=yes",
                "-e",
                "NEO4J_dbms_default__database=neo4j",
                "-e",
                "NEO4J_dbms_security_procedures_unrestricted=apoc.*",
                "-e",
                "NEO4J_dbms_security_procedures_allowlist=apoc.*",
                "-p",
                "0:7687",
                "-d",
                DOCKER_IMAGE,
            ])
            .output()
            .context("Failed to start Neo4j Docker container")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to start Neo4j container: {stderr}");
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        info!("Started container: {}", container_id);

        self.host_port = get_dynamic_port(&self.container_name)?;

        info!(
            "Neo4j container bound to dynamic port {} (bolt://localhost:{})",
            self.host_port, self.host_port
        );

        Ok(())
    }

    /// Waits for Neo4j to accept Bolt connections, polling up to `timeout_secs`.
    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        info!("Waiting for Neo4j to be ready...");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout {
            match self.test_connection().await {
                Ok(_) => {
                    info!("Neo4j is ready!");
                    return Ok(());
                }
                Err(e) => {
                    debug!("Connection attempt failed: {}", e);
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        }

        let logs = self.get_logs().unwrap_or_default();
        anyhow::bail!(
            "Neo4j did not become ready within {timeout_secs} seconds.\nContainer logs:\n{logs}"
        )
    }

    async fn test_connection(&self) -> Result<()> {
        let config = neo4rs::ConfigBuilder::default()
            .uri(self.bolt_uri())
            .user(&self.username)
            .password(&self.password)
            .db(&*self.database)
            .build()
            .context("Failed to build Neo4j config")?;

        let graph = neo4rs::Graph::connect(config)
            .context("Failed to connect to Neo4j")?;

        let mut result = graph
            .execute(neo4rs::query("RETURN 1 AS n"))
            .await
            .context("Failed to execute test query")?;

        result.next().await.context("No result from test query")?;

        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        info!("Stopping Neo4j container: {}", self.container_name);

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

        info!("Container stopped and removed");
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

impl Drop for Neo4jContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Queries Docker for the host port dynamically bound to container port 7687.
fn get_dynamic_port(container_name: &str) -> Result<u16> {
    for attempt in 0..10 {
        let output = Command::new("docker")
            .args(["port", container_name, "7687"])
            .output()
            .context("Failed to query dynamic port")?;

        if output.status.success() {
            let port_output = String::from_utf8_lossy(&output.stdout);
            if let Some(port) = port_output
                .lines()
                .next()
                .and_then(|line| line.rsplit(':').next())
                .and_then(|p| p.trim().parse::<u16>().ok())
            {
                return Ok(port);
            }
        }

        if attempt < 9 {
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }

    let output = Command::new("docker")
        .args(["port", container_name, "7687"])
        .output()
        .context("Failed to query dynamic port")?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    anyhow::bail!("docker port failed after retries: {stderr}")
}
