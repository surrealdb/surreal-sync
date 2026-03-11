//! Docker container management for MySQL testing

use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// A test MySQL container backed by Docker with dynamic port binding.
pub struct MySQLContainer {
    pub container_name: String,
    pub host_port: u16,
    pub connection_string: String,
    image_name: String,
}

impl MySQLContainer {
    /// Creates a new container configuration. Call [`start`](Self::start) before
    /// using the connection.
    pub fn new(container_name: &str) -> Self {
        Self {
            container_name: container_name.to_string(),
            image_name: "mysql:8.0".to_string(),
            host_port: 0,
            connection_string: String::new(),
        }
    }

    /// Starts the container with dynamic port binding and discovers the assigned port.
    pub fn start(&mut self) -> Result<()> {
        info!("Starting MySQL container: {}", self.container_name);

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
                "MYSQL_ROOT_PASSWORD=testpass",
                "-e",
                "MYSQL_DATABASE=testdb",
                "-p",
                "0:3306",
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

        self.host_port = get_dynamic_port(&self.container_name)?;
        self.connection_string = format!(
            "mysql://root:testpass@127.0.0.1:{}/testdb",
            self.host_port
        );

        info!(
            "Container bound to dynamic port {} (connection: {})",
            self.host_port, self.connection_string
        );

        Ok(())
    }

    /// Waits for MySQL to be ready to accept connections
    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        info!("Waiting for MySQL to be ready...");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout {
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

impl Drop for MySQLContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Queries Docker for the host port dynamically bound to container port 3306.
fn get_dynamic_port(container_name: &str) -> Result<u16> {
    for attempt in 0..10 {
        let output = Command::new("docker")
            .args(["port", container_name, "3306"])
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
        .args(["port", container_name, "3306"])
        .output()
        .context("Failed to query dynamic port")?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    anyhow::bail!("docker port failed after retries: {stderr}")
}
