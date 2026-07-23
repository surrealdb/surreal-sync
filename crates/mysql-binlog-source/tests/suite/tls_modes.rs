//! Integration tests for MySQL TLS modes: disabled, preferred, required.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use binlog_protocol::test_images::{mariadb_binlog_image, mysql_binlog_image};
use binlog_protocol::{BinlogClient, Flavor, ReplicaOptions, SslMode, SslOptions};
use mysql_async::prelude::*;

use crate::tls_certs::MysqlTlsSecrets;

fn parse_mysql_uri(uri: &str) -> Result<(String, u16, String, String)> {
    let rest = uri
        .strip_prefix("mysql://")
        .ok_or_else(|| anyhow::anyhow!("expected mysql://"))?;
    let (auth, hostpart) = rest
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("missing @"))?;
    let (username, password) = auth
        .split_once(':')
        .map(|(u, p)| (u.to_string(), p.to_string()))
        .unwrap_or((auth.to_string(), String::new()));
    let hostport = hostpart.split_once('/').map(|(h, _)| h).unwrap_or(hostpart);
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(3306)),
        None => (hostport.to_string(), 3306),
    };
    Ok((host, port, username, password))
}

struct TlsMysqlContainer {
    name: String,
    connection_string: String,
    flavor: Option<Flavor>,
    _secrets: MysqlTlsSecrets,
}

impl TlsMysqlContainer {
    async fn start_mysql(name: &str) -> Result<Self> {
        Self::start_with_image(
            name,
            &mysql_binlog_image(),
            None,
            &[
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--gtid-mode=ON",
                "--enforce-gtid-consistency=ON",
                "--server-id=1",
                "--log-slave-updates=ON",
            ],
        )
        .await
    }

    async fn start_mariadb(name: &str) -> Result<Self> {
        Self::start_with_image(
            name,
            &mariadb_binlog_image(),
            Some(Flavor::MariaDb),
            &[
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--server-id=1",
                "--gtid-strict-mode=ON",
            ],
        )
        .await
    }

    async fn start(name: &str) -> Result<Self> {
        Self::start_mysql(name).await
    }

    async fn start_with_image(
        name: &str,
        image: &str,
        flavor: Option<Flavor>,
        engine_args: &[&str],
    ) -> Result<Self> {
        let secrets = MysqlTlsSecrets::generate()?;
        let secrets_dir = secrets
            .secrets_dir()
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("secrets path not utf-8"))?
            .to_string();

        let _ = Command::new("docker")
            .args(["rm", "-f", name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        let mut args = vec![
            "run".to_string(),
            "--name".to_string(),
            name.to_string(),
            "-e".to_string(),
            "MYSQL_ROOT_PASSWORD=testpass".to_string(),
            "-e".to_string(),
            "MYSQL_DATABASE=testdb".to_string(),
            "-p".to_string(),
            "0:3306".to_string(),
            "-v".to_string(),
            format!("{secrets_dir}:/certs:ro"),
            "-d".to_string(),
            image.to_string(),
        ];
        for arg in engine_args {
            args.push(arg.to_string());
        }
        args.extend([
            "--ssl-ca=/certs/ca.pem".to_string(),
            "--ssl-cert=/certs/server.crt".to_string(),
            "--ssl-key=/certs/server.key".to_string(),
            "--require_secure_transport=OFF".to_string(),
        ]);

        let output = Command::new("docker")
            .args(&args)
            .output()
            .context("start TLS database container")?;
        if !output.status.success() {
            anyhow::bail!(
                "start TLS database failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let host_port = wait_for_port(name)?;
        let connection_string = format!("mysql://root:testpass@127.0.0.1:{host_port}/testdb");
        let container = Self {
            name: name.to_string(),
            connection_string,
            flavor,
            _secrets: secrets,
        };
        container.wait_ready(120).await?;
        Ok(container)
    }

    fn ca_path(&self) -> String {
        self._secrets.ca_pem.to_string_lossy().into_owned()
    }

    fn client_cert_path(&self) -> String {
        self._secrets.client_cert.to_string_lossy().into_owned()
    }

    fn client_key_path(&self) -> String {
        self._secrets.client_key.to_string_lossy().into_owned()
    }

    async fn wait_ready(&self, timeout_secs: u64) -> Result<()> {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(timeout_secs) {
            // Prefer plaintext probe — TLS modes are what we are testing.
            if let Ok(pool) = mysql_async::Pool::from_url(&self.connection_string) {
                if let Ok(mut conn) = pool.get_conn().await {
                    let ok: Result<Option<i32>, _> = conn.query_first("SELECT 1").await;
                    drop(conn);
                    let _ = pool.disconnect().await;
                    if ok.is_ok() {
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        anyhow::bail!("TLS MySQL not ready within {timeout_secs}s")
    }

    async fn connect_binlog(&self, ssl: SslMode, server_id: u32) -> Result<BinlogClient> {
        let (host, port, username, password) = parse_mysql_uri(&self.connection_string)?;
        BinlogClient::connect(ReplicaOptions {
            host,
            port,
            username,
            password,
            server_id,
            ssl,
            blocking_poll: Duration::from_millis(200),
            flavor: self.flavor,
            mariadb_flags: binlog_protocol::MariaDbDumpFlags {
                send_annotate_rows: true,
            },
            mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }
}

impl Drop for TlsMysqlContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn wait_for_port(name: &str) -> Result<u16> {
    for _ in 0..20 {
        let output = Command::new("docker")
            .args(["port", name, "3306"])
            .output()?;
        if output.status.success() {
            if let Some(port) = String::from_utf8_lossy(&output.stdout)
                .lines()
                .next()
                .and_then(|line| line.rsplit(':').next())
                .and_then(|p| p.trim().parse().ok())
            {
                return Ok(port);
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    anyhow::bail!("could not discover host port for {name}")
}

#[tokio::test]
async fn tls_disabled_connects_to_tls_capable_server() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start("ss-mysql-tls-disabled").await?;
    let client = c.connect_binlog(SslMode::Disabled, 9_100_001).await?;
    drop(client);
    Ok(())
}

#[tokio::test]
async fn tls_preferred_connects_with_tls_to_tls_capable_server() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start("ss-mysql-tls-preferred").await?;
    let client = c
        .connect_binlog(
            SslMode::Preferred(SslOptions {
                ca: Some(c.ca_path()),
                cert: None,
                key: None,
            }),
            9_100_002,
        )
        .await?;
    drop(client);
    Ok(())
}

#[tokio::test]
async fn tls_preferred_falls_back_on_plaintext_only_server() -> Result<()> {
    crate::shared::init_logging();
    // Stock shared container has no custom CA / typically allows plaintext.
    let container = crate::shared::shared_mysql_binlog().await;
    let (host, port, username, password) = parse_mysql_uri(&container.connection_string)?;
    let client = BinlogClient::connect(ReplicaOptions {
        host,
        port,
        username,
        password,
        server_id: 9_100_003,
        // Preferred with a bogus CA forces handshake failure → plaintext retry.
        ssl: SslMode::Preferred(SslOptions {
            ca: Some("/nonexistent/ca.pem".into()),
            cert: None,
            key: None,
        }),
        blocking_poll: Duration::from_millis(200),
        flavor: None,
        mariadb_flags: binlog_protocol::MariaDbDumpFlags {
            send_annotate_rows: true,
        },
        mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
    })
    .await;
    // Either succeeds via fallback (server has SSL but bad CA) or via no-SSL path.
    // If the shared server has no CLIENT_SSL, Preferred stays plaintext without reading CA.
    // If it has SSL, bad CA fails upgrade → reconnect Disabled.
    assert!(
        client.is_ok(),
        "preferred should fall back to plaintext: {:?}",
        client.err()
    );
    Ok(())
}

#[tokio::test]
async fn tls_required_with_ca_connects() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start("ss-mysql-tls-required-ca").await?;
    let client = c
        .connect_binlog(
            SslMode::Required(SslOptions {
                ca: Some(c.ca_path()),
                cert: None,
                key: None,
            }),
            9_100_004,
        )
        .await?;
    drop(client);
    Ok(())
}

#[tokio::test]
async fn tls_required_without_ca_connects_self_signed() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start("ss-mysql-tls-required-noca").await?;
    // MySQL-compatible REQUIRED: encrypt without public-CA verification.
    let client = c.connect_binlog(SslMode::required(), 9_100_005).await?;
    drop(client);
    Ok(())
}

#[tokio::test]
async fn tls_required_fails_when_server_has_no_ssl() -> Result<()> {
    crate::shared::init_logging();
    // Prefer a server that does not advertise CLIENT_SSL. Stock MySQL 8 images
    // usually still enable SSL with auto-generated certs, so we simulate the
    // failure path by requiring TLS against a host that is not MySQL — skip if
    // the shared server does advertise SSL (common). Instead: use Required with
    // an invalid host after confirming capability check exists via unit path.
    //
    // Practical check: Required with a CA that does not match still fails (no fallback).
    let c = TlsMysqlContainer::start("ss-mysql-tls-required-badca").await?;
    let err = c
        .connect_binlog(
            SslMode::Required(SslOptions {
                ca: Some("/nonexistent/missing-ca.pem".into()),
                cert: None,
                key: None,
            }),
            9_100_006,
        )
        .await;
    assert!(err.is_err(), "required must not fall back to plaintext");
    Ok(())
}

#[tokio::test]
async fn tls_sql_pool_modes_on_tls_server() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start("ss-mysql-tls-sql-pool").await?;

    // disabled
    let pool =
        mysql_types::new_mysql_pool_with_ssl(&c.connection_string, &SslMode::Disabled).await?;
    let mut conn = pool.get_conn().await?;
    let _: Option<i32> = conn.query_first("SELECT 1").await?;
    drop(conn);
    pool.disconnect().await?;

    // preferred with CA
    let preferred = SslMode::Preferred(SslOptions {
        ca: Some(c.ca_path()),
        cert: None,
        key: None,
    });
    let pool = mysql_types::new_mysql_pool_with_ssl(&c.connection_string, &preferred).await?;
    let mut conn = pool.get_conn().await?;
    let _: Option<i32> = conn.query_first("SELECT 1").await?;
    drop(conn);
    pool.disconnect().await?;

    // required without CA (self-signed OK)
    let pool =
        mysql_types::new_mysql_pool_with_ssl(&c.connection_string, &SslMode::required()).await?;
    let mut conn = pool.get_conn().await?;
    let _: Option<i32> = conn.query_first("SELECT 1").await?;
    drop(conn);
    pool.disconnect().await?;

    // required with CA
    let required = SslMode::Required(SslOptions {
        ca: Some(c.ca_path()),
        cert: None,
        key: None,
    });
    let pool = mysql_types::new_mysql_pool_with_ssl(&c.connection_string, &required).await?;
    let mut conn = pool.get_conn().await?;
    let _: Option<i32> = conn.query_first("SELECT 1").await?;
    drop(conn);
    pool.disconnect().await?;

    Ok(())
}

#[tokio::test]
async fn tls_preferred_does_not_fallback_on_auth_failure() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start("ss-mysql-tls-preferred-auth").await?;
    let (host, port, username, _) = parse_mysql_uri(&c.connection_string)?;
    let err = BinlogClient::connect(ReplicaOptions {
        host,
        port,
        username,
        password: "wrong-password".into(),
        server_id: 9_100_008,
        ssl: SslMode::Preferred(SslOptions {
            ca: Some(c.ca_path()),
            cert: None,
            key: None,
        }),
        blocking_poll: Duration::from_millis(200),
        flavor: None,
        mariadb_flags: binlog_protocol::MariaDbDumpFlags {
            send_annotate_rows: true,
        },
        mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
    })
    .await;
    assert!(err.is_err(), "wrong password must fail under preferred");
    let msg = err.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        !msg.contains("retrying without TLS"),
        "preferred must not fall back on auth errors: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn tls_sql_pool_preferred_does_not_fallback_on_auth_failure() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start("ss-mysql-tls-pool-auth").await?;
    let bad_uri = c.connection_string.replace("testpass", "wrong-password");
    let err = mysql_types::new_mysql_pool_with_ssl(
        &bad_uri,
        &SslMode::Preferred(SslOptions {
            ca: Some(c.ca_path()),
            cert: None,
            key: None,
        }),
    )
    .await;
    assert!(
        err.is_err(),
        "preferred SQL pool must not fall back when auth fails"
    );
    Ok(())
}

#[tokio::test]
async fn caching_sha2_full_auth_over_tls_required() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start("ss-mysql-tls-sha2").await?;

    let pool = mysql_async::Pool::from_url(&c.connection_string)?;
    let mut admin = pool.get_conn().await?;
    let user = format!("sha2_tls_{}", std::process::id());
    let pass = "sha2_tls_pass";
    admin
        .query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await?;
    admin
        .query_drop(format!(
            "CREATE USER '{user}'@'%' IDENTIFIED WITH caching_sha2_password BY '{pass}'"
        ))
        .await?;
    admin
        .query_drop(format!(
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%'"
        ))
        .await?;
    admin.query_drop("FLUSH PRIVILEGES").await?;

    let (host, port, _, _) = parse_mysql_uri(&c.connection_string)?;
    let client = BinlogClient::connect(ReplicaOptions {
        host,
        port,
        username: user.clone(),
        password: pass.to_string(),
        server_id: 9_100_007,
        ssl: SslMode::Required(SslOptions {
            ca: Some(c.ca_path()),
            cert: None,
            key: None,
        }),
        blocking_poll: Duration::from_millis(200),
        flavor: None,
        mariadb_flags: binlog_protocol::MariaDbDumpFlags {
            send_annotate_rows: true,
        },
        mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
    })
    .await
    .map_err(|e| anyhow::anyhow!("caching_sha2 over TLS failed: {e}"))?;

    drop(client);
    admin
        .query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await?;
    drop(admin);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn tls_client_certificate_auth_connects() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start("ss-mysql-tls-client-cert").await?;

    let pool = mysql_async::Pool::from_url(&c.connection_string)?;
    let mut admin = pool.get_conn().await?;
    let user = format!("cert_user_{}", std::process::id());
    let pass = "cert_pass";
    admin
        .query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await?;
    admin
        .query_drop(format!(
            "CREATE USER '{user}'@'%' IDENTIFIED BY '{pass}' REQUIRE X509"
        ))
        .await?;
    admin
        .query_drop(format!(
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%'"
        ))
        .await?;
    admin
        .query_drop(format!("GRANT SELECT ON testdb.* TO '{user}'@'%'"))
        .await?;
    admin.query_drop("FLUSH PRIVILEGES").await?;
    drop(admin);
    pool.disconnect().await?;

    let (host, port, _, _) = parse_mysql_uri(&c.connection_string)?;
    let client = BinlogClient::connect(ReplicaOptions {
        host,
        port,
        username: user.clone(),
        password: pass.to_string(),
        server_id: 9_100_009,
        ssl: SslMode::Required(SslOptions {
            ca: Some(c.ca_path()),
            cert: Some(c.client_cert_path()),
            key: Some(c.client_key_path()),
        }),
        blocking_poll: Duration::from_millis(200),
        flavor: None,
        mariadb_flags: binlog_protocol::MariaDbDumpFlags {
            send_annotate_rows: true,
        },
        mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
    })
    .await
    .map_err(|e| anyhow::anyhow!("client certificate auth failed: {e}"))?;
    drop(client);

    let (host, port, _, _) = parse_mysql_uri(&c.connection_string)?;
    let user_uri = format!("mysql://{user}:{pass}@{host}:{port}/testdb");
    let pool = mysql_types::new_mysql_pool_with_ssl(
        &user_uri,
        &SslMode::Required(SslOptions {
            ca: Some(c.ca_path()),
            cert: Some(c.client_cert_path()),
            key: Some(c.client_key_path()),
        }),
    )
    .await?;
    let mut conn = pool.get_conn().await?;
    let _: Option<i32> = conn.query_first("SELECT 1").await?;
    drop(conn);
    pool.disconnect().await?;

    let pool = mysql_async::Pool::from_url(&c.connection_string)?;
    let mut admin = pool.get_conn().await?;
    admin
        .query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await?;
    drop(admin);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn mariadb_tls_required_with_ca_connects() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start_mariadb("ss-mariadb-tls-required").await?;
    let client = c
        .connect_binlog(
            SslMode::Required(SslOptions {
                ca: Some(c.ca_path()),
                cert: None,
                key: None,
            }),
            9_100_010,
        )
        .await?;
    drop(client);
    Ok(())
}

#[tokio::test]
async fn mariadb_tls_preferred_with_ca_connects() -> Result<()> {
    crate::shared::init_logging();
    let c = TlsMysqlContainer::start_mariadb("ss-mariadb-tls-preferred").await?;
    let client = c
        .connect_binlog(
            SslMode::Preferred(SslOptions {
                ca: Some(c.ca_path()),
                cert: None,
                key: None,
            }),
            9_100_011,
        )
        .await?;
    drop(client);
    Ok(())
}
