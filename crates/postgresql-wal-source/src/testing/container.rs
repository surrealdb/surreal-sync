use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;
use tracing::{debug, info};

const POSTGRES_IMAGE: &str = "postgres:16";

pub struct PostgresWalContainer {
    pub container_name: String,
    pub host_port: u16,
    pub connection_string: String,
}

impl PostgresWalContainer {
    pub fn new(container_name: &str) -> Self {
        Self {
            container_name: container_name.to_string(),
            host_port: 0,
            connection_string: String::new(),
        }
    }

    pub fn start(&mut self) -> Result<()> {
        info!("Starting PostgreSQL WAL container: {}", self.container_name);

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
                POSTGRES_IMAGE,
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
            anyhow::bail!(
                "Failed to start container: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        self.host_port = get_dynamic_port(&self.container_name)?;
        self.connection_string = format!(
            "host=localhost port={} user=postgres password=postgres dbname=testdb",
            self.host_port
        );
        Ok(())
    }

    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(timeout_secs) {
            if self.test_connection().await.is_ok() {
                return Ok(());
            }
            debug!("PostgreSQL not ready yet");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        anyhow::bail!("PostgreSQL did not become ready within {timeout_secs}s")
    }

    async fn test_connection(&self) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        let _ = Command::new("docker")
            .args(["stop", &self.container_name])
            .output();
        let _ = Command::new("docker")
            .args(["rm", &self.container_name])
            .output();
        Ok(())
    }
}

impl Drop for PostgresWalContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

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
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    anyhow::bail!("docker port failed for {container_name}")
}
