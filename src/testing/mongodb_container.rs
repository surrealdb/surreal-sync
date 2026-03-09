//! Docker container management for MongoDB test instances.
//!
//! Provides a `MongoContainer` that runs a MongoDB 7 replica set Docker
//! container with dynamic port binding. Required for change-stream-based
//! tests that need a replica set.

use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::{debug, info};

const DOCKER_IMAGE: &str = "mongo:7";

/// A test MongoDB container backed by Docker with dynamic port binding.
pub struct MongoContainer {
    pub container_name: String,
    pub host_port: u16,
}

impl MongoContainer {
    pub fn new(container_name: &str) -> Self {
        Self {
            container_name: container_name.to_string(),
            host_port: 0,
        }
    }

    /// Starts the container with dynamic port binding, initialises the
    /// replica set, and creates the root user.
    pub fn start(&mut self) -> Result<()> {
        info!("Starting MongoDB container: {}", self.container_name);

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
                "0:27017",
                "-d",
                DOCKER_IMAGE,
                "mongod",
                "--replSet",
                "rs0",
                "--bind_ip_all",
                "--noauth",
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
            "Container bound to dynamic port {} (URI: {})",
            self.host_port,
            self.connection_uri()
        );

        self.wait_for_mongod()?;
        self.init_replica_set()?;
        self.create_root_user()?;

        Ok(())
    }

    /// Polls `mongosh` inside the container until `mongod` responds.
    fn wait_for_mongod(&self) -> Result<()> {
        info!("Waiting for mongod to accept connections...");
        let start = Instant::now();
        let timeout = Duration::from_secs(30);

        while start.elapsed() < timeout {
            let status = Command::new("docker")
                .args([
                    "exec",
                    &self.container_name,
                    "mongosh",
                    "--quiet",
                    "--eval",
                    "db.adminCommand('ping')",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .context("Failed to exec into container")?;

            if status.success() {
                info!("mongod is accepting connections");
                return Ok(());
            }

            std::thread::sleep(Duration::from_millis(500));
        }

        anyhow::bail!("mongod did not become ready within 30 seconds")
    }

    /// Initiates a single-member replica set and waits for primary election.
    fn init_replica_set(&self) -> Result<()> {
        info!("Initialising replica set...");

        let script = r#"
            rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'localhost:27017' }] });
            while (!db.isMaster().ismaster) { sleep(500); }
        "#;

        let output = Command::new("docker")
            .args([
                "exec",
                &self.container_name,
                "mongosh",
                "--quiet",
                "--eval",
                script,
            ])
            .output()
            .context("Failed to initialise replica set")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            anyhow::bail!("Replica set init failed.\nstdout: {stdout}\nstderr: {stderr}");
        }

        info!("Replica set initialised");
        Ok(())
    }

    /// Creates the `root` user in the `admin` database.
    fn create_root_user(&self) -> Result<()> {
        info!("Creating root user...");

        let script = r#"
            db = db.getSiblingDB('admin');
            try {
                db.createUser({ user: 'root', pwd: 'root', roles: ['root'] });
            } catch(e) {
                // User may already exist
            }
        "#;

        let output = Command::new("docker")
            .args([
                "exec",
                &self.container_name,
                "mongosh",
                "--quiet",
                "--eval",
                script,
            ])
            .output()
            .context("Failed to create root user")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!("Root user creation output: {}", stderr);
        }

        info!("Root user created");
        Ok(())
    }

    /// Waits for MongoDB to be fully ready by pinging via the Rust driver.
    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        info!("Waiting for MongoDB to be ready via driver...");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout {
            match self.test_connection().await {
                Ok(_) => {
                    info!("MongoDB is ready!");
                    return Ok(());
                }
                Err(e) => {
                    debug!("Connection attempt failed: {}", e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        anyhow::bail!("MongoDB did not become ready within {timeout_secs} seconds")
    }

    async fn test_connection(&self) -> Result<()> {
        let client = mongodb::Client::with_uri_str(&self.connection_uri())
            .await
            .context("Failed to create MongoDB client")?;

        client
            .database("admin")
            .run_command(mongodb::bson::doc! { "ping": 1 })
            .await
            .context("Ping failed")?;

        Ok(())
    }

    /// Returns a connection URI with `directConnection=true` so the driver
    /// does not attempt replica-set discovery via the container-internal host.
    pub fn connection_uri(&self) -> String {
        format!(
            "mongodb://root:root@localhost:{}/?directConnection=true",
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

impl Drop for MongoContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Queries Docker for the host port dynamically bound to container port 27017.
fn get_dynamic_port(container_name: &str) -> Result<u16> {
    let output = Command::new("docker")
        .args(["port", container_name, "27017"])
        .output()
        .context("Failed to query dynamic port")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("docker port failed: {stderr}");
    }

    let port_output = String::from_utf8_lossy(&output.stdout);
    let port = port_output
        .lines()
        .next()
        .and_then(|line| line.rsplit(':').next())
        .and_then(|p| p.trim().parse::<u16>().ok())
        .with_context(|| {
            format!("Failed to parse dynamic port from docker output: {port_output}")
        })?;

    Ok(port)
}
