//! Capture real binlog wire bytes into checked-in fixtures (Docker required).
//!
//! Run: `CAPTURE_BINLOG_FIXTURES=1 cargo test -p surreal-sync-mysql capture_binlog_fixtures -- --nocapture`

mod captured;

use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use captured::support::{
    capture_sql_block, delete_sql, fixture_paths, mariadb_txn_sql, primary_insert_sql, update_sql,
    FixtureMeta, FIXTURE_DIR,
};
use mysql_async::prelude::*;
use surreal_sync_mysql::binlog_protocol::test_images::{mariadb_binlog_image, mysql_binlog_image};
use surreal_sync_mysql::binlog_protocol::{
    BinlogClient, EventBody, Flavor, ReplicaOptions, ResumePosition, SslMode,
};

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

struct BinlogContainer {
    container_name: String,
    host_port: u16,
    connection_string: String,
    flavor: Flavor,
    image_name: String,
}

impl BinlogContainer {
    fn mysql(name: &str) -> Self {
        let image = mysql_binlog_image();
        Self::with_image(name, &image, Flavor::MySql)
    }

    fn mariadb(name: &str) -> Self {
        let image = mariadb_binlog_image();
        Self::with_image(name, &image, Flavor::MariaDb)
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

    fn start(&mut self) -> Result<()> {
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

    async fn wait_until_ready(&self, timeout_secs: u64) -> Result<()> {
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
        let _: Option<i32> = conn.query_first("SELECT 1").await?;
        drop(conn);
        pool.disconnect().await?;
        Ok(())
    }
}

impl Drop for BinlogContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["stop", &self.container_name])
            .output();
        let _ = Command::new("docker")
            .args(["rm", &self.container_name])
            .output();
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

async fn ensure_binlog_repl_user(conn_str: &str, flavor: Flavor) -> Result<()> {
    let pool = mysql_async::Pool::from_url(conn_str)?;
    let mut conn = pool.get_conn().await?;
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
    pool.disconnect().await?;
    Ok(())
}

fn repl_connection_string(base: &str) -> String {
    base.replacen("root:testpass@", "surreal_sync:surreal_sync_pass@", 1)
}

fn parse_mysql_uri(uri: &str) -> Result<(String, u16, String, String)> {
    let rest = uri
        .strip_prefix("mysql://")
        .ok_or_else(|| anyhow::anyhow!("invalid MySQL connection string"))?;
    let (auth, hostpart) = rest
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("invalid MySQL connection string"))?;
    let (username, password) = match auth.split_once(':') {
        Some((u, p)) => (u.to_string(), p.to_string()),
        None => (auth.to_string(), String::new()),
    };
    let hostport = hostpart.split('/').next().unwrap_or(hostpart);
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(3306)),
        None => (hostport.to_string(), 3306),
    };
    Ok((host, port, username, password))
}

async fn create_test_db(container: &BinlogContainer, db_name: &str) -> Result<String> {
    let pool = mysql_async::Pool::from_url(&container.connection_string)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{db_name}`"))
        .await?;
    drop(conn);
    pool.disconnect().await?;
    Ok(container
        .connection_string
        .replace("/testdb", &format!("/{db_name}")))
}

async fn start_binlog_at_master_end(client: &mut BinlogClient, conn_str: &str) -> Result<()> {
    let pool = mysql_async::Pool::from_url(conn_str)?;
    let mut conn = pool.get_conn().await?;
    use mysql_async::Row;
    let row: Option<Row> = conn.query_first("SHOW MASTER STATUS").await?;
    let row = row.context("SHOW MASTER STATUS returned no rows")?;
    let file: String = row.get(0).context("missing File")?;
    let pos: u64 = row.get(1).context("missing Position")?;
    drop(conn);
    pool.disconnect().await?;
    client
        .start_stream(ResumePosition::FilePos {
            file,
            pos: pos as u32,
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

async fn connect_client(conn_str: &str, flavor: Flavor, server_id: u32) -> Result<BinlogClient> {
    let binlog_conn = repl_connection_string(conn_str);
    let (host, port, user, pass) = parse_mysql_uri(&binlog_conn)?;
    BinlogClient::connect(ReplicaOptions {
        host,
        port,
        username: user,
        password: pass,
        server_id,
        ssl: SslMode::Disabled,
        blocking_poll: Duration::from_millis(200),
        flavor: Some(flavor),
        mariadb_flags: surreal_sync_mysql::binlog_protocol::MariaDbDumpFlags {
            send_annotate_rows: true,
        },
        mariadb_gtid_strict_mode:
            surreal_sync_mysql::binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
    })
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))
}

async fn run_dml(conn: &mut mysql_async::Conn, flavor: Flavor) -> Result<()> {
    let json_lit = "'{\"a\":1,\"b\":\"two\"}'";
    let schema = match flavor {
        Flavor::MariaDb => captured::support::mariadb_schema_sql(),
        Flavor::MySql => captured::support::mysql_schema_sql(),
    };
    conn.query_drop(schema).await?;
    conn.query_drop(primary_insert_sql(json_lit)).await?;
    conn.query_drop(update_sql()).await?;
    conn.query_drop(delete_sql()).await?;
    if flavor == Flavor::MariaDb {
        conn.query_drop(mariadb_txn_sql(json_lit)).await?;
    }
    Ok(())
}

async fn capture_one(base: &str, container: &mut BinlogContainer) -> Result<()> {
    let flavor = container.flavor;
    let conn_str = create_test_db(container, "bcd_fixture").await?;
    ensure_binlog_repl_user(&conn_str, flavor).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;

    let server_id = match flavor {
        Flavor::MySql => 9_100_001,
        Flavor::MariaDb => 9_100_002,
    };
    let mut client = connect_client(&conn_str, flavor, server_id).await?;
    client.enable_stream_recording();
    start_binlog_at_master_end(&mut client, &conn_str).await?;

    run_dml(&mut conn, flavor).await?;

    let expected_rows = if flavor == Flavor::MariaDb { 6 } else { 3 };
    let mut row_events = 0usize;
    for _ in 0..120 {
        let events = client.next_events(32).await?;
        for event in &events {
            if let EventBody::Rows(rows) = &event.body {
                row_events += rows.rows.len();
            }
        }
        if row_events >= expected_rows {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::ensure!(
        row_events >= expected_rows,
        "expected at least {expected_rows} row events, got {row_events}"
    );

    let bytes = client.take_recorded_stream();
    anyhow::ensure!(!bytes.is_empty(), "recorded stream is empty");

    drop(conn);
    pool.disconnect().await?;

    std::fs::create_dir_all(FIXTURE_DIR)?;
    let (bin_path, meta_path) = fixture_paths(base);
    std::fs::write(&bin_path, &bytes)?;
    let meta = FixtureMeta {
        server_image: container.image_name.clone(),
        flavor: flavor.as_str().to_string(),
        checksum_enabled: true,
        description: format!("Captured post-COM_BINLOG_DUMP event bytes for {base}"),
        sql: capture_sql_block(flavor),
    };
    std::fs::write(&meta_path, serde_json::to_string_pretty(&meta)?)?;

    eprintln!(
        "wrote {} ({} bytes) and {}",
        bin_path.display(),
        bytes.len(),
        meta_path.display()
    );
    Ok(())
}

#[tokio::test]
async fn capture_binlog_fixtures() -> Result<()> {
    if std::env::var("CAPTURE_BINLOG_FIXTURES").ok().as_deref() != Some("1") {
        eprintln!(
            "skip capture_binlog_fixtures (set CAPTURE_BINLOG_FIXTURES=1 to record fixtures)"
        );
        return Ok(());
    }

    let mysql_name = format!("capture-mysql-{}", std::process::id());
    register_cleanup(&mysql_name);
    let mut mysql = BinlogContainer::mysql(&mysql_name);
    mysql.start()?;
    mysql.wait_until_ready(90).await?;
    capture_one("mysql_8_basic", &mut mysql).await?;

    let mariadb_name = format!("capture-mariadb-{}", std::process::id());
    register_cleanup(&mariadb_name);
    let mut mariadb = BinlogContainer::mariadb(&mariadb_name);
    mariadb.start()?;
    mariadb.wait_until_ready(90).await?;
    capture_one("mariadb_11_4_basic", &mut mariadb).await?;

    Ok(())
}
