use anyhow::{Context, Result};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tracing::{debug, info};

const DEFAULT_IMAGE: &str = "mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04";
const SA_PASSWORD: &str = "Surreal_Sync1";

fn image() -> String {
    std::env::var("MSSQL_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.to_string())
}

/// Official `mcr.microsoft.com/mssql/server` container with Agent and CDC.
pub struct MssqlContainer {
    pub container_name: String,
    pub host_port: u16,
    pub connection_string: String,
}

impl MssqlContainer {
    pub fn new(container_name: &str) -> Self {
        Self {
            container_name: container_name.to_string(),
            host_port: 0,
            connection_string: String::new(),
        }
    }

    pub fn start(&mut self) -> Result<()> {
        info!("Starting SQL Server container: {}", self.container_name);

        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        let image = image();
        let output = Command::new("docker")
            .args([
                "run",
                "--name",
                &self.container_name,
                "--platform",
                "linux/amd64",
                "-e",
                // Required by Microsoft's image (SQL Server EULA:
                // https://go.microsoft.com/fwlink/?linkid=2143497). Developer
                // edition is for test only.
                "ACCEPT_EULA=Y",
                "-e",
                &format!("MSSQL_SA_PASSWORD={SA_PASSWORD}"),
                "-e",
                "MSSQL_PID=Developer",
                "-e",
                "MSSQL_AGENT_ENABLED=true",
                "-p",
                "0:1433",
                "-d",
                &image,
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
        self.connection_string = self.connection_string_for("testdb");
        Ok(())
    }

    /// ADO.NET string for `database` on this container (SQL auth, encrypt, trust cert).
    pub fn connection_string_for(&self, database: &str) -> String {
        format!(
            "Server=tcp:localhost,{};User=sa;Password={SA_PASSWORD};Database={database};\
             TrustServerCertificate=true;Encrypt=true",
            self.host_port
        )
    }

    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        let start = Instant::now();
        let master = format!(
            "Server=tcp:localhost,{};User=sa;Password={SA_PASSWORD};Database=master;\
             TrustServerCertificate=true;Encrypt=true",
            self.host_port
        );
        while start.elapsed() < Duration::from_secs(timeout_secs) {
            if crate::from_mssql::client::connect(&master).await.is_ok() {
                return Ok(());
            }
            debug!("SQL Server TDS not ready yet");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        anyhow::bail!("SQL Server did not become ready within {timeout_secs}s")
    }

    /// Wait for Agent, create `testdb`, and enable CDC.
    pub async fn setup_testdb(&self) -> Result<()> {
        let master = format!(
            "Server=tcp:localhost,{};User=sa;Password={SA_PASSWORD};Database=master;\
             TrustServerCertificate=true;Encrypt=true",
            self.host_port
        );
        let client = crate::from_mssql::client::connect(&master).await?;
        wait_agent(&client).await?;
        client
            .simple_query(
                "IF DB_ID(N'testdb') IS NULL CREATE DATABASE testdb; \
                 ALTER DATABASE testdb SET ALLOW_SNAPSHOT_ISOLATION ON;",
            )
            .await?;
        let testdb = crate::from_mssql::client::connect(&self.connection_string).await?;
        if testdb
            .simple_query("EXEC testdb.sys.sp_cdc_enable_db;")
            .await
            .is_err()
        {
            testdb.simple_query("EXEC sys.sp_cdc_enable_db;").await?;
        }
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

impl Drop for MssqlContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

async fn wait_agent(client: &crate::from_mssql::client::MssqlClient) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        let rows = client
            .query(
                "SELECT status_desc FROM sys.dm_server_services \
                 WHERE servicename LIKE N'SQL Server Agent%'",
                &[],
            )
            .await;
        if let Ok(rows) = rows {
            if let Some(row) = rows.first() {
                let status: Option<&str> = row.try_get(0).ok().flatten();
                if status.is_some_and(|s| s.eq_ignore_ascii_case("Running")) {
                    return Ok(());
                }
            }
        }
        // Agent DMV may be empty on Linux; retry CDC enable as a proxy.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Ok(())
}

fn get_dynamic_port(container_name: &str) -> Result<u16> {
    for attempt in 0..10 {
        let output = Command::new("docker")
            .args(["port", container_name, "1433"])
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
