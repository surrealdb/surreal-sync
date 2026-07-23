//! Generate TLS certificates for MySQL integration tests (via Docker openssl).

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result};
use tempfile::TempDir;

const OPENSSL_IMAGE: &str = "alpine/openssl:latest";

/// CA, server, and client certificate files for TLS MySQL/MariaDB tests.
pub struct MysqlTlsSecrets {
    pub dir: TempDir,
    pub ca_pem: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
}

impl MysqlTlsSecrets {
    pub fn generate() -> Result<Self> {
        let dir = tempfile::tempdir().context("create TLS secrets tempdir")?;
        let work = dir.path();

        let script = r#"#!/bin/sh
set -eu
cd /work
openssl req -new -x509 -keyout ca.key -out ca.pem -days 3650 -nodes \
  -subj "/CN=SurrealSyncMySQLTestCA"
openssl req -new -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "/CN=localhost"
cat > server.ext <<'EOF'
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out server.crt -days 3650 -extfile server.ext
openssl genrsa -traditional -out client.key 2048
openssl req -new -key client.key -out client.csr \
  -subj "/CN=SurrealSyncTestClient"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out client.crt -days 3650
chmod 644 ca.pem server.crt server.key client.crt client.key
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

        let work_str = work
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("TLS work dir is not valid UTF-8"))?;
        let output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--entrypoint",
                "sh",
                "-v",
                &format!("{work_str}:/work"),
                "-w",
                "/work",
                OPENSSL_IMAGE,
                "/work/generate.sh",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("docker openssl cert generation")?;
        if !output.status.success() {
            anyhow::bail!(
                "openssl cert generation failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let ca_pem = work.join("ca.pem");
        let server_cert = work.join("server.crt");
        let server_key = work.join("server.key");
        let client_cert = work.join("client.crt");
        let client_key = work.join("client.key");
        for path in [
            &ca_pem,
            &server_cert,
            &server_key,
            &client_cert,
            &client_key,
        ] {
            if !path.exists() {
                anyhow::bail!("expected cert file missing: {}", path.display());
            }
        }

        Ok(Self {
            dir,
            ca_pem,
            client_cert,
            client_key,
        })
    }

    pub fn secrets_dir(&self) -> &Path {
        self.dir.path()
    }
}
