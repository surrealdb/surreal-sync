//! Docker container management for PostgreSQL test instances.
//!
//! Provides a unified `PostgresContainer` that builds a PostgreSQL 16 + wal2json
//! Docker image and runs it with dynamic port binding. Suitable for both
//! trigger-based and wal2json-based (logical replication) test suites.

use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;
use tracing::{debug, info};

const DOCKER_IMAGE_NAME: &str = "postgres-wal2json-test";

/// A test PostgreSQL container backed by Docker with dynamic port binding.
pub struct PostgresContainer {
    pub container_name: String,
    pub host_port: u16,
    pub connection_string: String,
    image_name: String,
}

impl PostgresContainer {
    /// Creates a new container configuration. Call [`build_image`](Self::build_image)
    /// and [`start`](Self::start) before using the connection.
    pub fn new(container_name: &str) -> Self {
        Self {
            container_name: container_name.to_string(),
            image_name: DOCKER_IMAGE_NAME.to_string(),
            host_port: 0,
            connection_string: String::new(),
        }
    }

    /// Builds the Docker image from `Dockerfile.postgres16.wal2json`.
    ///
    /// The Dockerfile is located by walking up from the current directory
    /// until the workspace root (containing the crate directory) is found.
    /// Building is idempotent thanks to Docker layer caching.
    pub fn build_image(&self) -> Result<()> {
        info!("Building Docker image: {}", self.image_name);

        let mut current_dir = std::env::current_dir()?;
        let dockerfile_path;
        let context_path;

        loop {
            let candidate = current_dir
                .join("crates/postgresql-wal2json-source/Dockerfile.postgres16.wal2json");
            if candidate.exists() {
                dockerfile_path = candidate;
                context_path = current_dir.join("crates/postgresql-wal2json-source");
                break;
            }

            if !current_dir.pop() {
                anyhow::bail!("Could not find workspace root with PostgreSQL Dockerfile");
            }
        }

        info!("Using Dockerfile: {:?}", dockerfile_path);
        info!("Using context: {:?}", context_path);

        let output = Command::new("docker")
            .args([
                "build",
                "-t",
                &self.image_name,
                "-f",
                dockerfile_path.to_str().unwrap(),
                context_path.to_str().unwrap(),
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()
            .context("Failed to execute docker build command")?;

        if !output.status.success() {
            anyhow::bail!("Docker build failed");
        }

        info!("Successfully built Docker image");
        Ok(())
    }

    /// Starts the container with dynamic port binding and discovers the assigned port.
    pub fn start(&mut self) -> Result<()> {
        info!("Starting PostgreSQL container: {}", self.container_name);

        // Remove any leftover container with the same name
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
                "POSTGRES_USER=postgres",
                "-e",
                "POSTGRES_PASSWORD=postgres",
                "-e",
                "POSTGRES_DB=testdb",
                "-p",
                "0:5432",
                "-d",
                &self.image_name,
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_wal_senders=10",
                "-c",
                "max_replication_slots=10",
                "-c",
                "track_commit_timestamp=on",
            ])
            .output()
            .context("Failed to start Docker container")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to start container: {stderr}");
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        info!("Started container: {}", container_id);

        self.host_port = get_dynamic_port(&self.container_name)?;
        self.connection_string = format!(
            "host=localhost port={} user=postgres password=postgres dbname=testdb",
            self.host_port
        );

        info!(
            "Container bound to dynamic port {} (connection: {})",
            self.host_port, self.connection_string
        );

        Ok(())
    }

    /// Waits for PostgreSQL to accept connections, polling up to `timeout_secs`.
    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        info!("Waiting for PostgreSQL to be ready...");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout {
            match self.test_connection().await {
                Ok(_) => {
                    info!("PostgreSQL is ready!");
                    return Ok(());
                }
                Err(e) => {
                    debug!("Connection attempt failed: {}", e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        anyhow::bail!("PostgreSQL did not become ready within {timeout_secs} seconds")
    }

    async fn test_connection(&self) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .context("Failed to connect")?;

        tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .execute("SELECT 1", &[])
            .await
            .context("Failed to execute test query")?;

        Ok(())
    }

    /// Returns a `postgresql://` URL form of the connection string.
    pub fn connection_url(&self) -> String {
        format!(
            "postgresql://postgres:postgres@localhost:{}/testdb",
            self.host_port
        )
    }

    pub fn stop(&self) -> Result<()> {
        info!("Stopping container: {}", self.container_name);

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

impl Drop for PostgresContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Queries Docker for the host port dynamically bound to container port 5432.
fn get_dynamic_port(container_name: &str) -> Result<u16> {
    for attempt in 0..10 {
        let output = Command::new("docker")
            .args(["port", container_name, "5432"])
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
        .args(["port", container_name, "5432"])
        .output()
        .context("Failed to query dynamic port")?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    anyhow::bail!("docker port failed after retries: {stderr}")
}
