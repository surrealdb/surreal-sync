use std::process::{Command, Output};
use tempfile::TempDir;

/// Execute surreal-sync CLI command and return the output
pub fn execute_surreal_sync(args: &[&str]) -> Result<Output, Box<dyn std::error::Error>> {
    let output = Command::new("/workspace/target/release/surreal-sync")
        .args(args)
        .env("RUST_LOG", "surreal_sync=debug")
        .output()?;
    Ok(output)
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
