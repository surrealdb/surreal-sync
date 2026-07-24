//! Process init shared by the CLI and embed entrypoints.

use rustls::crypto::CryptoProvider;

/// Install the TLS crypto provider and initialize tracing.
///
/// Called automatically by `from-*` `run` / `run::<Sink>` entrypoints. Call this
/// yourself if you use lower-level `run_sync(&sink, …)` APIs directly.
pub fn init() {
    if let Err(err) = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider())
    {
        eprintln!("Error setting up crypto provider for TLS: {err:?}");
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}
