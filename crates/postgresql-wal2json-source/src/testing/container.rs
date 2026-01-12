//! Docker container management for PostgreSQL with wal2json

use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;
use tracing::{debug, info};

/// Configuration for a test PostgreSQL container
pub struct PostgresContainer {
    /// Container name
    pub container_name: String,
    /// Host port to bind to
    pub host_port: u16,
    /// Image name
    pub image_name: String,
    /// Connection string for the container
    pub connection_string: String,
}

impl PostgresContainer {
    /// Creates a new PostgreSQL container configuration
    pub fn new(container_name: &str, host_port: u16) -> Self {
        let image_name = format!("postgres-wal2json-test-{container_name}");
        let connection_string = format!(
            "host=localhost port={host_port} user=postgres password=postgres dbname=testdb",
        );

        Self {
            container_name: container_name.to_string(),
            host_port,
            image_name,
            connection_string,
        }
    }

    /// Builds the Docker image from the Dockerfile
    pub fn build_image(&self) -> Result<()> {
        info!("Building Docker image: {}", self.image_name);

        // Find the workspace root by looking for Cargo.toml with workspace section
        let mut current_dir = std::env::current_dir()?;
        let dockerfile_path;
        let context_path;

        // Try to find the workspace root
        loop {
            let cargo_toml = current_dir.join("Cargo.toml");
            if cargo_toml.exists() {
                // Check if this is the workspace root by looking for the postgresql-wal2json-source crate
                let postgresql_dockerfile = current_dir
                    .join("crates/postgresql-wal2json-source/Dockerfile.postgres16.wal2json");
                if postgresql_dockerfile.exists() {
                    dockerfile_path = postgresql_dockerfile;
                    context_path = current_dir.join("crates/postgresql-wal2json-source");
                    break;
                }
            }

            // Go up one directory
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

    /// Starts the PostgreSQL container
    pub fn start(&self) -> Result<()> {
        info!("Starting PostgreSQL container: {}", self.container_name);

        // First, try to stop and remove any existing container with the same name
        let _ = Command::new("docker")
            .args(["stop", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        let _ = Command::new("docker")
            .args(["rm", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        // Start the new container
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
                &format!("{}:5432", self.host_port),
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

        Ok(())
    }

    /// Waits for PostgreSQL to be ready to accept connections
    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        info!("Waiting for PostgreSQL to be ready...");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout {
            // Try to connect
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

        anyhow::bail!("PostgreSQL did not become ready within {timeout_secs} seconds",)
    }

    /// Tests if we can connect to PostgreSQL
    async fn test_connection(&self) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .context("Failed to connect")?;

        // Spawn the connection handler
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Try a simple query
        client
            .execute("SELECT 1", &[])
            .await
            .context("Failed to execute test query")?;

        Ok(())
    }

    /// Stops and removes the container
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

    /// Gets logs from the container
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
        // Best effort cleanup
        let _ = self.stop();
    }
}
