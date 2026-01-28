use std::time::Duration;

/// SurrealDB connection options
#[derive(Clone, Debug)]
pub struct SurrealOpts {
    pub surreal_endpoint: String,
    pub surreal_username: String,
    pub surreal_password: String,
}

/// Default number of connection retry attempts
const DEFAULT_RETRY_ATTEMPTS: u32 = 5;
/// Default delay between retry attempts in seconds
const DEFAULT_RETRY_DELAY_SECS: u64 = 2;

pub async fn surreal_connect(
    opts: &SurrealOpts,
    ns: &str,
    db: &str,
) -> anyhow::Result<surrealdb::Surreal<surrealdb::engine::any::Any>> {
    surreal_connect_with_retries(
        opts,
        ns,
        db,
        DEFAULT_RETRY_ATTEMPTS,
        DEFAULT_RETRY_DELAY_SECS,
    )
    .await
}

/// Connect to SurrealDB with configurable retries.
///
/// This function will retry connection failures up to `max_retries` times,
/// waiting `retry_delay_secs` seconds between attempts. This handles transient
/// connection issues when services are starting up.
pub async fn surreal_connect_with_retries(
    opts: &SurrealOpts,
    ns: &str,
    db: &str,
    max_retries: u32,
    retry_delay_secs: u64,
) -> anyhow::Result<surrealdb::Surreal<surrealdb::engine::any::Any>> {
    // Convert http:// to ws:// for WebSocket connection
    let surreal_endpoint = opts
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");

    tracing::debug!(
        "Connecting to SurrealDB at {} (namespace: {}, database: {})",
        surreal_endpoint,
        ns,
        db
    );

    let mut last_error = None;

    for attempt in 1..=max_retries {
        match try_connect(&surreal_endpoint, opts, ns, db).await {
            Ok(surreal) => {
                if attempt > 1 {
                    tracing::info!(
                        "Successfully connected to SurrealDB after {} attempts",
                        attempt
                    );
                }
                return Ok(surreal);
            }
            Err(e) => {
                last_error = Some(e);
                if attempt < max_retries {
                    tracing::warn!(
                        "Failed to connect to SurrealDB at '{}' (attempt {}/{}): {}. Retrying in {}s...",
                        surreal_endpoint,
                        attempt,
                        max_retries,
                        last_error.as_ref().unwrap(),
                        retry_delay_secs
                    );
                    tokio::time::sleep(Duration::from_secs(retry_delay_secs)).await;
                }
            }
        }
    }

    // All retries exhausted
    Err(anyhow::anyhow!(
        "Failed to connect to SurrealDB at '{}' after {} attempts. Last error: {}",
        surreal_endpoint,
        max_retries,
        last_error.unwrap()
    ))
}

/// Attempt a single connection to SurrealDB.
async fn try_connect(
    endpoint: &str,
    opts: &SurrealOpts,
    ns: &str,
    db: &str,
) -> anyhow::Result<surrealdb::Surreal<surrealdb::engine::any::Any>> {
    // Connect to SurrealDB
    let surreal = surrealdb::engine::any::connect(endpoint)
        .await
        .map_err(|e| anyhow::anyhow!("SurrealDB connection to '{endpoint}' failed: {e}"))?;

    // Sign in
    let username = &opts.surreal_username;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username,
            password: &opts.surreal_password,
        })
        .await
        .map_err(|e| {
            anyhow::anyhow!("SurrealDB authentication failed (user: '{username}'): {e}")
        })?;

    // Select namespace and database
    surreal.use_ns(ns).use_db(db).await.map_err(|e| {
        anyhow::anyhow!("SurrealDB failed to select namespace '{ns}' / database '{db}': {e}")
    })?;

    Ok(surreal)
}
