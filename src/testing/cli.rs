use std::process::{Command, Output};
use tempfile::TempDir;

/// Execute surreal-sync CLI command and return the output.
///
/// The binary location is resolved in this order:
/// 1. `SURREAL_SYNC_BIN` environment variable
/// 2. `./target/release/surreal-sync` relative to cwd
/// 3. `/workspace/target/release/surreal-sync` (devcontainer fallback)
pub fn execute_surreal_sync(args: &[&str]) -> Result<Output, Box<dyn std::error::Error>> {
    let binary = resolve_binary_path();
    let output = Command::new(binary)
        .args(args)
        .env("RUST_LOG", "surreal_sync=debug")
        .output()?;
    Ok(output)
}

fn resolve_binary_path() -> String {
    if let Ok(bin) = std::env::var("SURREAL_SYNC_BIN") {
        return bin;
    }
    if let Ok(cwd) = std::env::current_dir() {
        let local = cwd.join("target/release/surreal-sync");
        if local.exists() {
            return local.to_string_lossy().to_string();
        }
    }
    "/workspace/target/release/surreal-sync".to_string()
}

/// Create a temporary checkpoint directory for tests
pub fn create_temp_checkpoint_dir() -> Result<TempDir, Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    Ok(temp_dir)
}

/// Verify CLI command succeeded
pub fn assert_cli_success(output: &Output, command_desc: &str) {
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        panic!(
            "{} failed!\nExit code: {:?}\nStdout: {}\nStderr: {}",
            command_desc,
            output.status.code(),
            stdout,
            stderr
        );
    }
}
