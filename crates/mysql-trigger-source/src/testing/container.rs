//! Docker container management for MySQL testing

use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Configuration for a test MySQL container
pub struct MySQLContainer {
    /// Container name
    pub container_name: String,
    /// Host port to bind to
    pub host_port: u16,
    /// Image name (uses official mysql image)
    pub image_name: String,
    /// Connection string for the container
    pub connection_string: String,
}

impl MySQLContainer {
    /// Creates a new MySQL container configuration
    pub fn new(container_name: &str, host_port: u16) -> Self {
        // Use official MySQL 8 image
        let image_name = "mysql:8.0".to_string();
        let connection_string = format!("mysql://root:testpass@127.0.0.1:{host_port}/testdb",);

        Self {
            container_name: container_name.to_string(),
            host_port,
            image_name,
            connection_string,
        }
    }

    /// Starts the MySQL container
    pub fn start(&self) -> Result<()> {
        info!("Starting MySQL container: {}", self.container_name);

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
                "MYSQL_ROOT_PASSWORD=testpass",
                "-e",
                "MYSQL_DATABASE=testdb",
                "-p",
                &format!("{}:3306", self.host_port),
                "-d",
                &self.image_name,
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

    /// Waits for MySQL to be ready to accept connections
    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        info!("Waiting for MySQL to be ready...");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout {
            // Try to connect
            match self.test_connection().await {
                Ok(_) => {
                    info!("MySQL is ready!");
                    return Ok(());
                }
                Err(e) => {
                    debug!("Connection attempt failed: {}", e);
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        }

        anyhow::bail!("MySQL did not become ready within {timeout_secs} seconds")
    }

    /// Tests if we can connect to MySQL
    async fn test_connection(&self) -> Result<()> {
        let pool = mysql_async::Pool::from_url(&self.connection_string)
            .context("Failed to create connection pool")?;

        let mut conn = pool.get_conn().await.context("Failed to get connection")?;

        use mysql_async::prelude::*;
        let _: Option<i32> = conn
            .query_first("SELECT 1")
            .await
            .context("Failed to execute test query")?;

        drop(conn);
        pool.disconnect()
            .await
            .context("Failed to disconnect pool")?;

        Ok(())
    }

    /// Gets a connection pool for the container
    pub fn get_pool(&self) -> Result<mysql_async::Pool> {
        mysql_async::Pool::from_url(&self.connection_string)
            .context("Failed to create connection pool")
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

impl Drop for MySQLContainer {
    fn drop(&mut self) {
        // Best effort cleanup
        let _ = self.stop();
    }
}
