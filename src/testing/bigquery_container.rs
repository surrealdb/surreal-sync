//! Docker container management for BigQuery emulator test instances.
//!
//! Provides a [`BigQueryEmulatorContainer`] that runs
//! [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) with
//! dynamic port binding. The emulator speaks the BigQuery REST API v2 and
//! disables authentication entirely, so the integration tests exercise the real
//! client code path on every PR without any credentials.
//!
//! The emulator is not BigQuery: see `docs/bigquery.md` for the fidelity gaps it
//! does not cover (auth, GEOGRAPHY, RANGE). Those are covered by the type unit
//! tests in `surreal-sync-bigquery` and by the credential-gated real-account test.

use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Pinned so CI is reproducible. Override with `BIGQUERY_EMULATOR_IMAGE`; the
/// canonical tag lives in `scripts/test-images.env`.
const DEFAULT_IMAGE: &str = "ghcr.io/goccy/bigquery-emulator:0.8.1";

/// The emulator's REST port inside the container.
const EMULATOR_PORT: &str = "9050";

/// A test BigQuery emulator container backed by Docker with dynamic port binding.
pub struct BigQueryEmulatorContainer {
    pub container_name: String,
    pub host_port: u16,
    pub project_id: String,
    pub dataset_id: String,
    image: String,
}

impl BigQueryEmulatorContainer {
    pub fn new(container_name: &str, project_id: &str, dataset_id: &str) -> Self {
        let image =
            std::env::var("BIGQUERY_EMULATOR_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.to_string());
        Self {
            container_name: container_name.to_string(),
            host_port: 0,
            project_id: project_id.to_string(),
            dataset_id: dataset_id.to_string(),
            image,
        }
    }

    /// Starts the emulator with dynamic port binding and discovers the assigned port.
    ///
    /// `--dataset` is required: the emulator creates the dataset at boot, and
    /// without it `datasets.tables.list` has nothing to return.
    pub fn start(&mut self) -> Result<()> {
        info!(
            "Starting BigQuery emulator container: {} (image: {})",
            self.container_name, self.image
        );

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
                &format!("0:{EMULATOR_PORT}"),
                "-d",
                &self.image,
                "--project",
                &self.project_id,
                "--dataset",
                &self.dataset_id,
                "--log-level",
                "warn",
            ])
            .output()
            .context("Failed to start Docker container")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to start container: {stderr}");
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        info!("Started container: {container_id}");

        self.host_port = get_dynamic_port(&self.container_name)?;
        info!(
            "Container bound to dynamic port {} (endpoint: {})",
            self.host_port,
            self.api_endpoint()
        );

        Ok(())
    }

    /// Polls the datasets endpoint over a raw TCP HTTP/1.0 request until the
    /// emulator answers 200.
    ///
    /// Deliberately not `reqwest`: the root crate has no direct HTTP client
    /// dependency, and this mirrors `SurrealDbContainer::wait_until_ready`.
    pub fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        info!("Waiting for the BigQuery emulator to be ready...");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", self.host_port)
            .parse()
            .context("failed to parse the emulator address")?;
        let request = format!(
            "GET /bigquery/v2/projects/{}/datasets HTTP/1.0\r\nHost: localhost:{}\r\n\r\n",
            self.project_id, self.host_port
        );

        while start.elapsed() < timeout {
            if let Ok(mut stream) =
                std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1))
            {
                use std::io::{Read, Write};
                stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
                if stream.write_all(request.as_bytes()).is_ok() {
                    let mut response = String::new();
                    if stream.read_to_string(&mut response).is_ok()
                        && response
                            .lines()
                            .next()
                            .is_some_and(|line| line.contains("200"))
                    {
                        info!("BigQuery emulator is ready");
                        return Ok(());
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(250));
        }

        let logs = self.get_logs().unwrap_or_default();
        anyhow::bail!("BigQuery emulator not ready within {timeout_secs}s. Logs:\n{logs}")
    }

    /// The `--api-endpoint` value tests should pass to the BigQuery source.
    pub fn api_endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.host_port)
    }

    pub fn stop(&self) -> Result<()> {
        info!("Stopping container: {}", self.container_name);

        let output = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .output()
            .context("Failed to remove container")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!("Failed to remove container (may not exist): {stderr}");
        }

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

impl Drop for BigQueryEmulatorContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Queries Docker for the host port dynamically bound to the emulator's REST port.
fn get_dynamic_port(container_name: &str) -> Result<u16> {
    for attempt in 0..10 {
        let output = Command::new("docker")
            .args(["port", container_name, EMULATOR_PORT])
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
        .args(["port", container_name, EMULATOR_PORT])
        .output()
        .context("Failed to query dynamic port")?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    anyhow::bail!("docker port failed after retries: {stderr}")
}
