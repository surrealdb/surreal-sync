//! Shared Docker container helpers for postgresql-pgoutput-source integration tests.

use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use surreal_sync_postgresql_pgoutput_source::{PgoutputCheckpoint, SourceOpts};
use tokio::sync::OnceCell;
use tokio_postgres::Client;

static CONTAINER_NAMES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

fn register_cleanup(name: &str) {
    let names = CONTAINER_NAMES.get_or_init(|| {
        extern "C" fn cleanup() {
            if let Some(names) = CONTAINER_NAMES.get() {
                if let Ok(names) = names.lock() {
                    for name in names.iter() {
                        let _ = Command::new("docker")
                            .args(["rm", "-f", name])
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .status();
                    }
                }
            }
        }
        unsafe { libc::atexit(cleanup) };
        Mutex::new(Vec::new())
    });
    if let Ok(mut names) = names.lock() {
        names.push(name.to_string());
    }
}

/// PostgreSQL pgoutput WAL test container (`postgres:16`, logical replication via docker run).
pub struct WalContainer {
    pub container_name: String,
    pub host_port: u16,
    pub connection_string: String,
}

impl WalContainer {
    pub fn new(name: &str) -> Self {
        Self {
            container_name: name.to_string(),
            host_port: 0,
            connection_string: String::new(),
        }
    }

    pub fn start(&mut self) -> Result<()> {
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
                "postgres:16",
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
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        anyhow::bail!("PostgreSQL did not become ready within {timeout_secs}s")
    }

    async fn test_connection(&self) -> Result<()> {
        let (client, conn) =
            tokio_postgres::connect(&self.connection_string, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = conn.await;
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

impl Drop for WalContainer {
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

static PG_WAL: OnceCell<WalContainer> = OnceCell::const_new();

pub async fn shared_postgresql_pgoutput() -> &'static WalContainer {
    PG_WAL
        .get_or_init(|| async {
            let name = format!("suite-pg-wal-{}", std::process::id());
            register_cleanup(&name);
            let mut c = WalContainer::new(&name);
            c.start().expect("PostgreSQL WAL start failed");
            c.wait_until_ready(60)
                .await
                .expect("PostgreSQL WAL not ready");
            c
        })
        .await
}

pub async fn create_test_db(container: &WalContainer, db_name: &str) -> Result<String> {
    let (client, conn) =
        tokio_postgres::connect(&container.connection_string, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    match client
        .execute(&format!("CREATE DATABASE \"{db_name}\""), &[])
        .await
    {
        Ok(_) => {}
        Err(e) if e.to_string().contains("already exists") => {}
        Err(e) => return Err(e.into()),
    }
    Ok(container
        .connection_string
        .replace("dbname=testdb", &format!("dbname={db_name}")))
}

pub fn source_opts(
    conn_str: &str,
    slot: &str,
    publication: &str,
    tables: Vec<String>,
) -> SourceOpts {
    SourceOpts {
        connection_string: conn_str.to_string(),
        schema: "public".to_string(),
        tables,
        slot_name: slot.to_string(),
        publication_name: publication.to_string(),
    }
}

pub async fn pg_connect(conn_str: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });
    Ok(client)
}

pub async fn capture_head(
    conn_str: &str,
    slot: &str,
    publication: &str,
    tables: Vec<String>,
) -> Result<PgoutputCheckpoint> {
    surreal_sync_postgresql_pgoutput_source::capture_head_checkpoint(&source_opts(
        conn_str,
        slot,
        publication,
        tables,
    ))
    .await
}

pub fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
}
