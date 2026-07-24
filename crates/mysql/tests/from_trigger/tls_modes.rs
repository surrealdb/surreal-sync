//! Trigger-source SQL pool TLS mode smoke tests.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use mysql_async::prelude::*;
use surreal_sync_mysql::binlog_protocol::test_images::mysql_binlog_image;
use surreal_sync_mysql::binlog_protocol::{SslMode, SslOptions};
use surreal_sync_mysql::from_trigger::new_mysql_pool_with_ssl;

/// Minimal cert generation + TLS MySQL container (mirrors binlog suite harness).
struct TlsMysql {
    name: String,
    connection_string: String,
    ca_pem: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

impl TlsMysql {
    async fn start(name: &str) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let work = dir.path();
        let script = r#"#!/bin/sh
set -eu
cd /work
openssl req -new -x509 -keyout ca.key -out ca.pem -days 3650 -nodes -subj "/CN=TriggerTlsCA"
openssl req -new -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=localhost"
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -extfile server.ext
chmod 644 ca.pem server.crt server.key
"#;
        std::fs::write(work.join("generate.sh"), script)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                work.join("generate.sh"),
                std::fs::Permissions::from_mode(0o755),
            )?;
        }
        let work_str = work.to_str().unwrap();
        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--entrypoint",
                "sh",
                "-v",
                &format!("{work_str}:/work"),
                "-w",
                "/work",
                "alpine/openssl:latest",
                "/work/generate.sh",
            ])
            .output()?;
        if !out.status.success() {
            anyhow::bail!("{}", String::from_utf8_lossy(&out.stderr));
        }

        let _ = Command::new("docker")
            .args(["rm", "-f", name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let image = mysql_binlog_image();
        let out = Command::new("docker")
            .args([
                "run",
                "--name",
                name,
                "-e",
                "MYSQL_ROOT_PASSWORD=testpass",
                "-e",
                "MYSQL_DATABASE=testdb",
                "-p",
                "0:3306",
                "-v",
                &format!("{work_str}:/certs:ro"),
                "-d",
                &image,
                "--ssl-ca=/certs/ca.pem",
                "--ssl-cert=/certs/server.crt",
                "--ssl-key=/certs/server.key",
                "--require_secure_transport=OFF",
            ])
            .output()?;
        if !out.status.success() {
            anyhow::bail!("{}", String::from_utf8_lossy(&out.stderr));
        }

        let port = loop_port(name)?;
        let connection_string = format!("mysql://root:testpass@127.0.0.1:{port}/testdb");
        let this = Self {
            name: name.to_string(),
            connection_string,
            ca_pem: work.join("ca.pem"),
            _dir: dir,
        };
        this.wait_ready().await?;
        Ok(this)
    }

    async fn wait_ready(&self) -> Result<()> {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(120) {
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
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("not ready")
    }
}

impl Drop for TlsMysql {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn loop_port(name: &str) -> Result<u16> {
    for _ in 0..20 {
        let output = Command::new("docker")
            .args(["port", name, "3306"])
            .output()?;
        if let Some(port) = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .and_then(|l| l.rsplit(':').next())
            .and_then(|p| p.trim().parse().ok())
        {
            return Ok(port);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    anyhow::bail!("no port")
}

#[tokio::test]
async fn trigger_pool_tls_modes() -> Result<()> {
    let c = TlsMysql::start("ss-mysql-trigger-tls").await?;

    for ssl in [
        SslMode::Disabled,
        SslMode::Preferred(SslOptions {
            ca: Some(c.ca_pem.to_string_lossy().into_owned()),
            cert: None,
            key: None,
        }),
        SslMode::required(),
        SslMode::Required(SslOptions {
            ca: Some(c.ca_pem.to_string_lossy().into_owned()),
            cert: None,
            key: None,
        }),
    ] {
        let pool = new_mysql_pool_with_ssl(&c.connection_string, &ssl)
            .await
            .with_context(|| format!("pool for {ssl:?}"))?;
        let mut conn = pool.get_conn().await?;
        let _: Option<i32> = conn.query_first("SELECT 1").await?;
        drop(conn);
        pool.disconnect().await?;
    }
    Ok(())
}
