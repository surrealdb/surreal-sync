//! SQL Server connection over Tiberius (rustls).

use anyhow::{anyhow, Context, Result};
use std::sync::Arc;
use tiberius::{Client, Config, Query};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

/// Tiberius client wrapped for shared async use (the client is not `Sync`).
pub type RawClient = Client<Compat<TcpStream>>;

/// Shared SQL Server client.
#[derive(Clone)]
pub struct MssqlClient {
    inner: Arc<Mutex<RawClient>>,
}

/// Owned query parameter.
#[derive(Debug, Clone)]
pub enum SqlArg {
    I16(i16),
    I32(i32),
    I64(i64),
    F64(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Null,
}

impl SqlArg {
    /// Bind a universal value for keyset pagination.
    pub fn from_value(value: &surreal_sync_core::Value) -> Result<Self> {
        use surreal_sync_core::Value;
        Ok(match value {
            Value::Int16(v) => SqlArg::I16(*v),
            Value::Int32(v) => SqlArg::I32(*v),
            Value::Int64(v) => SqlArg::I64(*v),
            Value::Int8 { value, .. } => SqlArg::I16(*value as i16),
            Value::Bool(v) => SqlArg::Bool(*v),
            Value::Float64(v) => SqlArg::F64(*v),
            Value::Float32(v) => SqlArg::F64(*v as f64),
            Value::Text(s) | Value::VarChar { value: s, .. } | Value::Char { value: s, .. } => {
                SqlArg::String(s.clone())
            }
            Value::Uuid(u) => SqlArg::Uuid(*u),
            Value::Bytes(b) | Value::Blob(b) => SqlArg::Bytes(b.clone()),
            Value::Null => SqlArg::Null,
            other => anyhow::bail!("cannot bind SQL Server parameter from {other:?}"),
        })
    }
}

/// True when the ADO.NET string asks for Windows Integrated Auth.
pub fn integrated_security_requested(ado: &str) -> bool {
    ado.split(';').any(|part| {
        let part = part.trim();
        let Some((key, value)) = part.split_once('=') else {
            return false;
        };
        key.trim().eq_ignore_ascii_case("IntegratedSecurity")
            && matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "true" | "yes" | "1"
            )
    })
}

/// Parse an ADO.NET connection string without connecting.
pub fn parse_ado_config(ado: &str) -> Result<Config> {
    Config::from_ado_string(ado).map_err(|e| anyhow!("invalid SQL Server connection string: {e}"))
}

fn reject_integrated_on_non_windows(ado: &str) -> Result<()> {
    if integrated_security_requested(ado) {
        #[cfg(not(windows))]
        {
            anyhow::bail!(
                "Windows Integrated Auth (IntegratedSecurity=true) is Windows-only. \
                 Use SQL authentication (User ID / Password) on this platform."
            );
        }
        #[cfg(windows)]
        {
            let _ = ado;
        }
    }
    Ok(())
}

fn install_rustls_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Connect with rustls. `IntegratedSecurity=true` is Windows-only.
pub async fn connect(ado: &str) -> Result<MssqlClient> {
    reject_integrated_on_non_windows(ado)?;
    install_rustls_provider();
    let config = parse_ado_config(ado)?;
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .with_context(|| format!("connecting to SQL Server at {}", config.get_addr()))?;
    tcp.set_nodelay(true)?;
    let raw = Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| anyhow!("SQL Server login failed: {e}"))?;
    Ok(MssqlClient {
        inner: Arc::new(Mutex::new(raw)),
    })
}

impl MssqlClient {
    /// Run a parameterized query and return the first result set.
    pub async fn query(&self, sql: &str, args: &[SqlArg]) -> Result<Vec<tiberius::Row>> {
        let mut client = self.inner.lock().await;
        let mut q = Query::new(sql);
        bind_args(&mut q, args);
        let stream = q
            .query(&mut *client)
            .await
            .map_err(|e| anyhow!("SQL Server query failed: {e}"))?;
        stream
            .into_first_result()
            .await
            .map_err(|e| anyhow!("SQL Server query failed: {e}"))
    }

    /// Run a statement (INSERT/EXEC/…) and return rows affected when known.
    pub async fn execute(&self, sql: &str, args: &[SqlArg]) -> Result<u64> {
        let mut client = self.inner.lock().await;
        let mut q = Query::new(sql);
        bind_args(&mut q, args);
        let result = q
            .execute(&mut *client)
            .await
            .map_err(|e| anyhow!("SQL Server execute failed: {e}"))?;
        Ok(result.rows_affected().first().copied().unwrap_or(0))
    }

    /// Run T-SQL with no parameters (session settings, DDL).
    pub async fn simple_query(&self, sql: &str) -> Result<()> {
        let mut client = self.inner.lock().await;
        client
            .simple_query(sql)
            .await
            .map_err(|e| anyhow!("SQL Server query failed: {e}"))?
            .into_results()
            .await
            .map_err(|e| anyhow!("SQL Server query failed: {e}"))?;
        Ok(())
    }
}

fn bind_args(q: &mut Query<'_>, args: &[SqlArg]) {
    for arg in args {
        match arg {
            SqlArg::I16(v) => {
                q.bind(*v);
            }
            SqlArg::I32(v) => {
                q.bind(*v);
            }
            SqlArg::I64(v) => {
                q.bind(*v);
            }
            SqlArg::F64(v) => {
                q.bind(*v);
            }
            SqlArg::Bool(v) => {
                q.bind(*v);
            }
            SqlArg::String(v) => {
                q.bind(v.clone());
            }
            SqlArg::Bytes(v) => {
                q.bind(v.clone());
            }
            SqlArg::Uuid(v) => {
                q.bind(*v);
            }
            SqlArg::Null => {
                q.bind(Option::<i32>::None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sql_auth() {
        let cfg = parse_ado_config(
            "Server=tcp:localhost,1433;User=sa;Password=Surreal_Sync1;Database=App;\
             TrustServerCertificate=true;Encrypt=true",
        )
        .unwrap();
        assert!(cfg.get_addr().contains("localhost"));
        assert!(cfg.get_addr().contains("1433"));
    }

    #[test]
    fn parse_integrated_security() {
        let cfg = parse_ado_config(
            "Server=tcp:localhost,1433;IntegratedSecurity=true;Database=App;\
             TrustServerCertificate=true;Encrypt=true",
        )
        .unwrap();
        assert!(cfg.get_addr().contains("1433"));
        assert!(integrated_security_requested(
            "Server=localhost;IntegratedSecurity=true"
        ));
        assert!(!integrated_security_requested(
            "Server=localhost;User=sa;Password=x"
        ));
    }

    #[cfg(not(windows))]
    #[test]
    fn integrated_security_rejected_on_unix() {
        let err = reject_integrated_on_non_windows("Server=localhost;IntegratedSecurity=true")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Windows-only"), "{err}");
    }
}
