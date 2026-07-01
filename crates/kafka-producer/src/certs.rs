//! Generate TLS certificates and broker JKS secrets for secured Kafka tests.
//!
//! Certificate generation runs inside a one-off `apache/kafka:latest` container so
//! the host only needs Docker (openssl + keytool are bundled in the image).

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use tempfile::TempDir;

const DOCKER_IMAGE: &str = "apache/kafka:latest";

/// Fixed password for broker JKS/truststore files in tests.
pub const TEST_JKS_PASSWORD: &str = "changeit";

/// Default SCRAM username for client connections in secured test brokers.
pub const DEFAULT_SASL_USERNAME: &str = "syncuser";

/// Default SCRAM password for client connections in secured test brokers.
pub const DEFAULT_SASL_PASSWORD: &str = "syncpass";

/// Inter-broker SCRAM/PLAIN username baked into broker JAAS.
pub const INTER_BROKER_USERNAME: &str = "admin";

/// Inter-broker SCRAM/PLAIN password baked into broker JAAS.
pub const INTER_BROKER_PASSWORD: &str = "admin-secret";

/// Generated TLS/SASL secrets for a secured Kafka test broker.
pub struct KafkaTestSecrets {
    pub dir: TempDir,
    pub ca_pem: PathBuf,
    pub client_cert_pem: PathBuf,
    pub client_key_pem: PathBuf,
    pub sasl_username: String,
    pub sasl_password: String,
}

impl KafkaTestSecrets {
    /// Generate a fresh secrets directory with broker JKS files and client PEM files.
    pub fn generate() -> Result<Self> {
        Self::generate_with_credentials(DEFAULT_SASL_USERNAME, DEFAULT_SASL_PASSWORD)
    }

    /// Generate secrets using custom SCRAM credentials for client connections.
    pub fn generate_with_credentials(username: &str, password: &str) -> Result<Self> {
        let dir = tempfile::tempdir().context("Failed to create temp secrets directory")?;
        let work_dir = dir.path();

        write_generate_script(work_dir)?;
        write_jaas_config(work_dir)?;

        run_cert_generation_in_docker(work_dir)?;

        write_client_properties(work_dir, username, password)?;

        let ca_pem = work_dir.join("ca.pem");
        let client_cert_pem = work_dir.join("client.pem");
        let client_key_pem = work_dir.join("client.key");

        for path in [&ca_pem, &client_cert_pem, &client_key_pem] {
            if !path.exists() {
                anyhow::bail!("Expected generated file missing: {}", path.display());
            }
        }

        Ok(Self {
            dir,
            ca_pem,
            client_cert_pem,
            client_key_pem,
            sasl_username: username.to_string(),
            sasl_password: password.to_string(),
        })
    }

    /// Directory containing all broker and client secret files (mount at `/etc/kafka/secrets`).
    pub fn secrets_dir(&self) -> &Path {
        self.dir.path()
    }

    /// Path to the broker JAAS configuration file.
    pub fn jaas_config_path(&self) -> PathBuf {
        self.dir.path().join("kafka_server_jaas.conf")
    }

    /// Path to a client properties file for health checks (SASL_SSL + mTLS + SCRAM).
    pub fn client_properties_path(&self) -> PathBuf {
        self.dir.path().join("client.properties")
    }
}

fn write_jaas_config(work_dir: &Path) -> Result<()> {
    // SCRAM login module for inter-broker authentication. Client SCRAM credentials are
    // bootstrapped into KRaft metadata by the secure container entrypoint.
    let jaas = format!(
        r#"KafkaServer {{
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="{INTER_BROKER_USERNAME}"
    password="{INTER_BROKER_PASSWORD}";
}};
"#
    );
    std::fs::write(work_dir.join("kafka_server_jaas.conf"), jaas)
        .context("Failed to write kafka_server_jaas.conf")?;
    Ok(())
}

fn write_generate_script(work_dir: &Path) -> Result<()> {
    let script = r#"#!/bin/bash
set -euo pipefail
cd /work

PASSWORD="changeit"

# --- CA ---
openssl req -new -x509 -keyout ca.key -out ca.pem -days 3650 -nodes \
  -subj "/CN=KafkaTestCA"

# --- Broker server certificate (SAN: localhost + 127.0.0.1) ---
openssl req -new -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "/CN=localhost"
cat > server.ext <<'EOF'
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out server.crt -days 3650 -extfile server.ext

# --- Client certificate for mTLS ---
openssl req -new -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
  -subj "/CN=syncuser"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out client.pem -days 3650

# --- Broker keystore (PKCS12 -> JKS) ---
openssl pkcs12 -export -in server.crt -inkey server.key -out server.p12 \
  -name kafka -password pass:${PASSWORD} -CAfile ca.pem
keytool -importkeystore -deststorepass "${PASSWORD}" -destkeypass "${PASSWORD}" \
  -destkeystore kafka.keystore.jks -srckeystore server.p12 -srcstoretype PKCS12 \
  -srcstorepass "${PASSWORD}" -alias kafka -noprompt

# --- Broker truststore (CA) ---
keytool -import -file ca.pem -alias CARoot -keystore kafka.truststore.jks \
  -storepass "${PASSWORD}" -noprompt

# --- Client keystore (PKCS12 -> JKS) for Java CLI health checks ---
openssl pkcs12 -export -in client.pem -inkey client.key -out client.p12 \
  -name client -password pass:${PASSWORD} -CAfile ca.pem
keytool -importkeystore -deststorepass "${PASSWORD}" -destkeypass "${PASSWORD}" \
  -destkeystore client.keystore.jks -srckeystore client.p12 -srcstoretype PKCS12 \
  -srcstorepass "${PASSWORD}" -alias client -noprompt

# --- Password credential files expected by apache/kafka docker image ---
echo "${PASSWORD}" > kafka_keystore_creds
echo "${PASSWORD}" > kafka_truststore_creds
echo "${PASSWORD}" > kafka_ssl_key_creds
"#;

    let script_path = work_dir.join("generate-certs.sh");
    std::fs::write(&script_path, script).context("Failed to write cert generation script")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755))
            .context("Failed to mark cert generation script executable")?;
    }

    Ok(())
}

fn write_client_properties(work_dir: &Path, username: &str, password: &str) -> Result<()> {
    let contents = format!(
        r#"security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks
ssl.truststore.password={TEST_JKS_PASSWORD}
ssl.keystore.location=/etc/kafka/secrets/client.keystore.jks
ssl.keystore.password={TEST_JKS_PASSWORD}
ssl.key.password={TEST_JKS_PASSWORD}
ssl.endpoint.identification.algorithm=
"#
    );
    std::fs::write(work_dir.join("client.properties"), contents)
        .context("Failed to write client.properties")?;
    Ok(())
}

fn run_cert_generation_in_docker(work_dir: &Path) -> Result<()> {
    let mount = format!("{}:/work", work_dir.display());

    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &mount,
            DOCKER_IMAGE,
            "bash",
            "/work/generate-certs.sh",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("Failed to run cert generation container")?;

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "Cert generation failed (exit {}):\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}",
            output.status
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test: cert generation completes and expected files exist.
    /// Requires Docker; skipped when Docker is unavailable.
    #[test]
    fn generate_certs_smoke() {
        if !docker_available() {
            eprintln!("Skipping generate_certs_smoke: docker not available");
            return;
        }

        let secrets = KafkaTestSecrets::generate().expect("cert generation should succeed");
        assert!(secrets.ca_pem.exists());
        assert!(secrets.client_cert_pem.exists());
        assert!(secrets.client_key_pem.exists());
        assert!(secrets.secrets_dir().join("kafka.keystore.jks").exists());
        assert!(secrets.secrets_dir().join("kafka.truststore.jks").exists());
        assert_eq!(secrets.sasl_username, DEFAULT_SASL_USERNAME);
        assert_eq!(secrets.sasl_password, DEFAULT_SASL_PASSWORD);
    }

    fn docker_available() -> bool {
        Command::new("docker")
            .args(["info"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}
