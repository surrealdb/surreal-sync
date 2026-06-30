use std::process::{Command, Output};
use tempfile::TempDir;

/// Execute surreal-sync CLI command and return the output.
///
/// The binary location is resolved in this order:
/// 1. `SURREAL_SYNC_BIN` environment variable (if the path exists)
/// 2. `NEXTEST_BIN_EXE_surreal_sync` / `CARGO_BIN_EXE_surreal-sync` (nextest archive runs)
/// 3. `./target/ci/surreal-sync`, `./target/release/surreal-sync`, or `./target/debug/surreal-sync`
/// 4. `/workspace/target/release/surreal-sync` (devcontainer fallback)
pub fn execute_surreal_sync(args: &[&str]) -> Result<Output, Box<dyn std::error::Error>> {
    let binary = resolve_binary_path();
    let output = Command::new(binary)
        .args(args)
        .env("RUST_LOG", "surreal_sync=debug")
        .output()?;
    Ok(output)
}

fn resolve_binary_path() -> String {
    for key in [
        "SURREAL_SYNC_BIN",
        "NEXTEST_BIN_EXE_surreal_sync",
        "CARGO_BIN_EXE_surreal-sync",
    ] {
        if let Ok(bin) = std::env::var(key) {
            if std::path::Path::new(&bin).exists() {
                return bin;
            }
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        let candidates = [
            cwd.join("target/ci/surreal-sync"),
            cwd.join("target/release/surreal-sync"),
            cwd.join("target/debug/surreal-sync"),
        ];
        let existing: Vec<_> = candidates.iter().filter(|path| path.exists()).collect();
        if existing.len() == 1 {
            return existing[0].to_string_lossy().to_string();
        }
        if existing.len() > 1 {
            // Pick whichever was compiled most recently so that `cargo test` (debug),
            // `make test` (release-then-test), and CI (`ci` profile) all use the right binary.
            let newest = existing.into_iter().max_by_key(|path| {
                std::fs::metadata(path)
                    .and_then(|m| m.modified())
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
            });
            if let Some(path) = newest {
                return path.to_string_lossy().to_string();
            }
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
