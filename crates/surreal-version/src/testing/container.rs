//! Docker container management for SurrealDB test instances.
//!
//! Provides a `SurrealDbContainer` that runs SurrealDB in-memory via Docker
//! with dynamic port binding. The Docker image defaults to
//! `surrealdb/surrealdb:v2.3.7` and can be overridden with the
//! `SURREALDB_IMAGE` environment variable.

use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::{debug, info};

use crate::SurrealMajorVersion;

const DEFAULT_IMAGE: &str = "surrealdb/surrealdb:v2.3.7";

/// A test SurrealDB container backed by Docker with dynamic port binding.
pub struct SurrealDbContainer {
    pub container_name: String,
    pub host_port: u16,
    image: String,
    /// The server's major version, populated by `wait_until_ready()`.
    pub detected_version: Option<SurrealMajorVersion>,
}

impl SurrealDbContainer {
    /// Creates a new container configuration.
    ///
    /// The Docker image is read from `SURREALDB_IMAGE` env var, falling back
    /// to `surrealdb/surrealdb:v2.3.7`.
    pub fn new(container_name: &str) -> Self {
        let image =
            std::env::var("SURREALDB_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.to_string());
        Self {
            container_name: container_name.to_string(),
            host_port: 0,
            image,
            detected_version: None,
        }
    }

    /// Starts the container with dynamic port binding and discovers the assigned port.
    pub fn start(&mut self) -> Result<()> {
        info!(
            "Starting SurrealDB container: {} (image: {})",
            self.container_name, self.image
        );

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
                "-p",
                "0:8000",
                "-d",
                &self.image,
                "start",
                "--log",
                "trace",
                "--user",
                "root",
                "--pass",
                "root",
                "memory",
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
        info!(
            "Container bound to dynamic port {} (ws: {}, http: {})",
            self.host_port,
            self.ws_endpoint(),
            self.http_endpoint()
        );

        Ok(())
    }

    /// Waits for SurrealDB to accept connections by sending raw HTTP
    /// requests to the `/version` endpoint.
    ///
    /// We avoid `reqwest::blocking` here because its internal tokio runtime
    /// conflicts with `#[tokio::test]` when the container is dropped.
    pub fn wait_until_ready(&mut self, timeout_secs: u64) -> Result<()> {
        info!("Waiting for SurrealDB to be ready...");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", self.host_port).parse().unwrap();
        let request = format!(
            "GET /version HTTP/1.0\r\nHost: localhost:{}\r\n\r\n",
            self.host_port
        );

        while start.elapsed() < timeout {
            if let Ok(mut stream) =
                std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1))
            {
                use std::io::{Read, Write};
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .ok();

                if stream.write_all(request.as_bytes()).is_ok() {
                    let mut buf = vec![0u8; 1024];
                    if let Ok(n) = stream.read(&mut buf) {
                        let response = String::from_utf8_lossy(&buf[..n]);
                        if response.contains("200") && response.contains("surrealdb-") {
                            if let Some(version_line) =
                                response.lines().last().filter(|l| l.contains("surrealdb-"))
                            {
                                let trimmed = version_line.trim();
                                info!("SurrealDB is ready! Version: {}", trimmed);
                                self.detected_version =
                                    crate::parse_version_string(trimmed).ok();
                            } else {
                                info!("SurrealDB is ready!");
                            }
                            return Ok(());
                        }
                        debug!("SurrealDB not ready yet (response: {}...)", &response[..response.len().min(80)]);
                    }
                }
            } else {
                debug!("TCP connection to {} not yet available", addr);
            }
            std::thread::sleep(Duration::from_millis(250));
        }

        anyhow::bail!(
            "SurrealDB did not become ready within {timeout_secs} seconds (endpoint: {addr})"
        )
    }

    /// Returns a WebSocket endpoint URL.
    pub fn ws_endpoint(&self) -> String {
        format!("ws://localhost:{}", self.host_port)
    }

    /// Returns an HTTP endpoint URL.
    pub fn http_endpoint(&self) -> String {
        format!("http://localhost:{}", self.host_port)
    }

    /// Returns `true` when the container is running SurrealDB v3.
    pub fn is_v3(&self) -> bool {
        self.detected_version == Some(SurrealMajorVersion::V3)
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

impl Drop for SurrealDbContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Queries Docker for the host port dynamically bound to container port 8000.
/// Retries briefly to handle the race between `docker run` returning and
/// port mappings becoming visible.
fn get_dynamic_port(container_name: &str) -> Result<u16> {
    for attempt in 0..10 {
        let output = Command::new("docker")
            .args(["port", container_name, "8000"])
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
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    let output = Command::new("docker")
        .args(["port", container_name, "8000"])
        .output()
        .context("Failed to query dynamic port")?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    anyhow::bail!("docker port failed after retries: {stderr}")
}
