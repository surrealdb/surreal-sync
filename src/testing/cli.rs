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
        let release = cwd.join("target/release/surreal-sync");
        let debug = cwd.join("target/debug/surreal-sync");
        match (release.exists(), debug.exists()) {
            (true, true) => {
                // Pick whichever was compiled more recently so that
                // `cargo test` (debug) and `make test` (release-then-test)
                // both use the binary that matches the code under test.
                let r_mtime = std::fs::metadata(&release).and_then(|m| m.modified()).ok();
                let d_mtime = std::fs::metadata(&debug).and_then(|m| m.modified()).ok();
                return match (r_mtime, d_mtime) {
                    (Some(r), Some(d)) if r >= d => release.to_string_lossy().to_string(),
                    (Some(_), Some(_)) => debug.to_string_lossy().to_string(),
                    _ => release.to_string_lossy().to_string(),
                };
            }
            (true, false) => return release.to_string_lossy().to_string(),
            (false, true) => return debug.to_string_lossy().to_string(),
            (false, false) => {}
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
