//! Loadtest verify command handlers.

mod surreal2;
mod surreal3;

pub use surreal2::run_verify_v2;
pub use surreal3::run_verify_v3;

use loadtest_verify_surreal2::VerifyArgs;

/// Run verify command to check synced data in SurrealDB.
/// This auto-detects the server version and dispatches to the appropriate implementation.
pub async fn run_verify(args: VerifyArgs) -> anyhow::Result<()> {
    // Convert http:// to ws:// for WebSocket connection (SurrealDB client uses WebSocket protocol)
    let ws_endpoint = args
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");

    // Convert ws:// to http:// for version detection
    let http_endpoint = ws_endpoint
        .replace("ws://", "http://")
        .replace("wss://", "https://");

    // Auto-detect server version
    tracing::info!("Detecting SurrealDB server version at {}", http_endpoint);
    let detected = surreal_version::detect_server_version(&http_endpoint).await;

    match detected {
        Ok(version) => {
            tracing::info!("Detected SurrealDB server version: {:?}", version);
            match version {
                surreal_version::SurrealMajorVersion::V2 => {
                    tracing::info!("Using SurrealDB SDK v2 for verification");
                    run_verify_v2(args).await
                }
                surreal_version::SurrealMajorVersion::V3 => {
                    tracing::info!("Using SurrealDB SDK v3 for verification");
                    run_verify_v3(args).await
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                "Failed to detect SurrealDB version: {}. Defaulting to v2 SDK.",
                e
            );
            run_verify_v2(args).await
        }
    }
}
