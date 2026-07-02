//! Shared Docker container helpers for mysql-binlog-source integration tests.

use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use binlog_protocol::{BinlogClient, Flavor, ResumePosition};
use tokio::sync::OnceCell;

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

/// Binlog-enabled MySQL/MariaDB test container (stock images, server flags via docker run).
pub struct BinlogContainer {
    pub container_name: String,
    pub host_port: u16,
    pub connection_string: String,
    flavor: Flavor,
    image_name: String,
}

impl BinlogContainer {
    pub fn mysql(name: &str) -> Self {
        Self::with_image(name, "mysql:8.0", Flavor::MySql)
    }

    pub fn mariadb(name: &str) -> Self {
        Self::with_image(name, "mariadb:11.4", Flavor::MariaDb)
    }

    fn with_image(name: &str, image: &str, flavor: Flavor) -> Self {
        Self {
            container_name: name.to_string(),
            image_name: image.to_string(),
            flavor,
            host_port: 0,
            connection_string: String::new(),
        }
    }

    pub fn flavor(&self) -> Flavor {
        self.flavor
    }

    fn docker_server_args(&self) -> &[&'static str] {
        match self.flavor {
            Flavor::MySql => &[
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--gtid-mode=ON",
                "--enforce-gtid-consistency=ON",
                "--server-id=1",
                "--log-slave-updates=ON",
            ],
            Flavor::MariaDb => &[
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--server-id=1",
                "--gtid-strict-mode=ON",
            ],
        }
    }

    pub fn start(&mut self) -> Result<()> {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        let mut args = vec![
            "run".to_string(),
            "--name".to_string(),
            self.container_name.clone(),
            "-e".to_string(),
            "MYSQL_ROOT_PASSWORD=testpass".to_string(),
            "-e".to_string(),
            "MYSQL_DATABASE=testdb".to_string(),
            "-p".to_string(),
            "0:3306".to_string(),
            "-d".to_string(),
            self.image_name.clone(),
        ];
        args.extend(
            self.docker_server_args()
                .iter()
                .map(|arg| (*arg).to_string()),
        );

        let output = Command::new("docker")
            .args(&args)
            .output()
            .context("Failed to start Docker container")?;
        if !output.status.success() {
            anyhow::bail!(
                "Failed to start container: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        self.host_port = get_dynamic_port(&self.container_name)?;
        self.connection_string =
            format!("mysql://root:testpass@127.0.0.1:{}/testdb", self.host_port);
        Ok(())
    }

    pub async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(timeout_secs) {
            if self.test_connection().await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        anyhow::bail!(
            "{} did not become ready within {timeout_secs}s",
            self.flavor.as_str()
        )
    }

    async fn test_connection(&self) -> Result<()> {
        let pool = mysql_async::Pool::from_url(&self.connection_string)?;
        let mut conn = pool.get_conn().await?;
        use mysql_async::prelude::*;
        let _: Option<i32> = conn.query_first("SELECT 1").await?;
        drop(conn);
        pool.disconnect().await?;
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

impl Drop for BinlogContainer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

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
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    anyhow::bail!("docker port failed for {container_name}")
}

static MYSQL_BINLOG: OnceCell<BinlogContainer> = OnceCell::const_new();
static MARIADB_BINLOG: OnceCell<BinlogContainer> = OnceCell::const_new();

pub async fn ensure_binlog_repl_user(conn_str: &str, flavor: Flavor) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::from_url(conn_str)?;
    let mut conn = pool.get_conn().await?;
    use mysql_async::prelude::Queryable;
    let create_user = match flavor {
        Flavor::MySql => {
            "CREATE USER IF NOT EXISTS 'surreal_sync'@'%' \
             IDENTIFIED WITH mysql_native_password BY 'surreal_sync_pass'"
        }
        Flavor::MariaDb => {
            "CREATE USER IF NOT EXISTS 'surreal_sync'@'%' IDENTIFIED BY 'surreal_sync_pass'"
        }
    };
    conn.query_drop(create_user).await?;
    conn.query_drop("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'surreal_sync'@'%'")
        .await?;
    conn.query_drop("FLUSH PRIVILEGES").await?;
    drop(conn);
    drop(pool);
    Ok(())
}

pub fn repl_connection_string(base: &str) -> String {
    base.replacen("root:testpass@", "surreal_sync:surreal_sync_pass@", 1)
}

pub async fn shared_mysql_binlog() -> &'static BinlogContainer {
    MYSQL_BINLOG
        .get_or_init(|| async {
            let name = format!("suite-mysql-binlog-{}", std::process::id());
            register_cleanup(&name);
            let mut c = BinlogContainer::mysql(&name);
            c.start().expect("MySQL binlog start failed");
            c.wait_until_ready(60)
                .await
                .expect("MySQL binlog not ready");
            ensure_binlog_repl_user(&c.connection_string, Flavor::MySql)
                .await
                .expect("MySQL repl user setup failed");
            c
        })
        .await
}

pub async fn shared_mariadb_binlog() -> &'static BinlogContainer {
    MARIADB_BINLOG
        .get_or_init(|| async {
            let name = format!("suite-mariadb-binlog-{}", std::process::id());
            register_cleanup(&name);
            let mut c = BinlogContainer::mariadb(&name);
            c.start().expect("MariaDB binlog start failed");
            c.wait_until_ready(90)
                .await
                .expect("MariaDB binlog not ready");
            ensure_binlog_repl_user(&c.connection_string, Flavor::MariaDb)
                .await
                .expect("MariaDB repl user setup failed");
            c
        })
        .await
}

pub async fn create_test_db(container: &BinlogContainer, db_name: &str) -> Result<String> {
    let pool = mysql_async::Pool::from_url(&container.connection_string)?;
    let mut conn = pool.get_conn().await?;
    use mysql_async::prelude::Queryable;
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{db_name}`"))
        .await?;
    drop(conn);
    pool.disconnect().await?;
    Ok(container
        .connection_string
        .replace("/testdb", &format!("/{db_name}")))
}

pub fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
}

pub async fn start_binlog_at_master_end(
    client: &mut BinlogClient,
    conn_str: &str,
) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::from_url(conn_str)?;
    let mut conn = pool.get_conn().await?;
    use mysql_async::prelude::*;
    use mysql_async::Row;
    let row: Option<Row> = conn.query_first("SHOW MASTER STATUS").await?;
    let row = row.ok_or_else(|| anyhow::anyhow!("SHOW MASTER STATUS returned no rows"))?;
    let file: String = row
        .get(0)
        .ok_or_else(|| anyhow::anyhow!("SHOW MASTER STATUS missing File column"))?;
    let pos: u64 = row
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("SHOW MASTER STATUS missing Position column"))?;
    drop(conn);
    drop(pool);
    client
        .start_stream(ResumePosition::FilePos {
            file,
            pos: pos as u32,
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

pub fn parse_mysql_uri(uri: &str) -> anyhow::Result<(String, u16, String, String, Option<String>)> {
    let rest = uri
        .strip_prefix("mysql://")
        .ok_or_else(|| anyhow::anyhow!("invalid MySQL connection string (expected mysql://)"))?;
    let (auth, hostpart) = rest
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("invalid MySQL connection string (missing @)"))?;
    let (username, password) = match auth.split_once(':') {
        Some((u, p)) => (u.to_string(), p.to_string()),
        None => (auth.to_string(), String::new()),
    };
    let (hostport, dbpart) = match hostpart.split_once('/') {
        Some((hp, db)) => (hp, Some(db.to_string())),
        None => (hostpart, None),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(3306)),
        None => (hostport.to_string(), 3306),
    };
    Ok((host, port, username, password, dbpart))
}
