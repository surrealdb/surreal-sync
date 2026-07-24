//! Shared MySQL TLS types and helpers for `mysql_async` connection pools.
//!
//! Maps surreal-sync [`SslMode`] (disabled / preferred / required) onto
//! `mysql_async::SslOpts` with MySQL-client-compatible semantics.

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use mysql_async::{Opts, OptsBuilder, Pool, SslOpts};
use tracing::warn;

/// Paths for client/server TLS material.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SslOptions {
    pub ca: Option<String>,
    pub cert: Option<String>,
    pub key: Option<String>,
}

/// MySQL-client-compatible TLS mode for surreal-sync MySQL origins.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SslMode {
    #[default]
    Disabled,
    Preferred(SslOptions),
    Required(SslOptions),
}

impl SslMode {
    pub fn preferred() -> Self {
        Self::Preferred(SslOptions::default())
    }

    pub fn required() -> Self {
        Self::Required(SslOptions::default())
    }

    pub fn is_preferred(&self) -> bool {
        matches!(self, Self::Preferred(_))
    }

    pub fn is_required(&self) -> bool {
        matches!(self, Self::Required(_))
    }

    pub fn options(&self) -> Option<&SslOptions> {
        match self {
            Self::Disabled => None,
            Self::Preferred(options) | Self::Required(options) => Some(options),
        }
    }
}

/// Build `mysql_async` SSL options from surreal-sync TLS options.
///
/// - With a CA path: verify the server against that CA only.
/// - Without a CA: encrypt without requiring a public-CA match (MySQL REQUIRED).
fn mysql_async_ssl_opts(options: &SslOptions) -> Result<SslOpts> {
    let mut ssl = SslOpts::default();

    match (&options.cert, &options.key) {
        (Some(cert), Some(key)) => {
            let identity = mysql_async::ClientIdentity::new(
                PathBuf::from(cert).into(),
                PathBuf::from(key).into(),
            );
            ssl = ssl.with_client_identity(Some(identity));
        }
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => {
            bail!("--tls-cert and --tls-key must be provided together");
        }
    }

    if let Some(ca) = &options.ca {
        ssl = ssl
            .with_root_certs(vec![PathBuf::from(ca).into()])
            .with_disable_built_in_roots(true);
    } else {
        // MySQL `--ssl-mode=REQUIRED`: encrypt without public-CA verification.
        ssl = ssl
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true);
    }

    Ok(ssl)
}

fn pool_with_ssl(connection_string: &str, options: &SslOptions) -> Result<Pool> {
    // mysql_async rustls backend needs a process-wide CryptoProvider.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let base = Opts::from_url(connection_string)
        .with_context(|| format!("invalid MySQL connection string: {connection_string}"))?;
    let ssl_opts = mysql_async_ssl_opts(options)?;
    let opts = OptsBuilder::from_opts(base).ssl_opts(Some(ssl_opts));
    Ok(Pool::new(opts))
}

fn pool_plaintext(connection_string: &str) -> Result<Pool> {
    Pool::from_url(connection_string)
        .with_context(|| format!("invalid MySQL connection string: {connection_string}"))
}

/// Whether a pool connect failure under Preferred should retry without TLS.
fn is_tls_encryption_failure(err: &mysql_async::Error) -> bool {
    use mysql_async::{DriverError, Error, IoError};
    matches!(
        err,
        Error::Io(IoError::Tls(_)) | Error::Driver(DriverError::NoClientSslFlagFromServer)
    )
}

/// Create a MySQL pool honouring [`SslMode`].
///
/// For [`SslMode::Preferred`], probes an SSL connection and falls back to
/// plaintext if the TLS handshake fails (MySQL Preferred semantics).
pub async fn new_mysql_pool_with_ssl(connection_string: &str, ssl: &SslMode) -> Result<Pool> {
    match ssl {
        SslMode::Disabled => pool_plaintext(connection_string),
        SslMode::Required(options) => pool_with_ssl(connection_string, options),
        SslMode::Preferred(options) => {
            let pool = pool_with_ssl(connection_string, options)?;
            match pool.get_conn().await {
                Ok(_conn) => Ok(pool),
                Err(e) if is_tls_encryption_failure(&e) => {
                    warn!(
                        "TLS handshake failed under --tls-mode preferred ({e}); retrying without TLS"
                    );
                    pool_plaintext(connection_string)
                }
                Err(e) => Err(e).with_context(|| {
                    format!("failed to connect to MySQL with TLS: {connection_string}")
                }),
            }
        }
    }
}

/// Synchronous pool builder for callers that cannot await Preferred fallback.
///
/// [`SslMode::Preferred`] is treated like Required for the initial pool config
/// (TLS attempted on first connect). Prefer [`new_mysql_pool_with_ssl`] when
/// Preferred fallback is required.
pub fn new_mysql_pool_with_ssl_sync(connection_string: &str, ssl: &SslMode) -> Result<Pool> {
    match ssl {
        SslMode::Disabled => pool_plaintext(connection_string),
        SslMode::Preferred(options) | SslMode::Required(options) => {
            pool_with_ssl(connection_string, options)
        }
    }
}
