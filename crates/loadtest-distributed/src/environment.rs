//! Runtime environment verification for workers.
//!
//! This module provides functionality to log and verify the runtime environment
//! at worker startup, including CPU, memory, tmpfs, and cgroup limits.

use crate::metrics::EnvironmentInfo;
use std::fs;
use tracing::info;

/// Log and capture runtime environment information.
///
/// This function logs critical runtime environment info at startup to verify
/// that resource limits and tmpfs configuration are correctly applied.
pub fn log_runtime_environment() -> EnvironmentInfo {
    info!("=== Runtime Environment Verification ===");

    // CPU info
    let cpu_cores = num_cpus::get();
    info!("CPU cores visible: {}", cpu_cores);

    // Memory info using sysinfo
    let sys = sysinfo::System::new_all();
    let total_memory_mb = sys.total_memory() / 1024 / 1024;
    let available_memory_mb = sys.available_memory() / 1024 / 1024;
    info!("Total memory: {} MB", total_memory_mb);
    info!("Available memory: {} MB", available_memory_mb);

    // Storage verification (detect tmpfs)
    let (tmpfs_enabled, tmpfs_size_mb, data_fs_type) = check_tmpfs_mounts();

    // cgroup limits (if in container)
    let cgroup_memory_limit = read_cgroup_memory_limit();
    let cgroup_cpu_quota = read_cgroup_cpu_quota();

    if let Some(limit) = cgroup_memory_limit {
        info!("cgroup memory limit: {} bytes", limit);
    }
    if let Some(ref quota) = cgroup_cpu_quota {
        info!("cgroup CPU quota: {}", quota);
    }

    info!("========================================");

    EnvironmentInfo {
        cpu_cores,
        memory_mb: total_memory_mb,
        available_memory_mb,
        tmpfs_enabled,
        tmpfs_size_mb,
        cgroup_memory_limit,
        cgroup_cpu_quota,
        data_fs_type,
    }
}

/// Check if common paths are mounted as tmpfs and return info.
fn check_tmpfs_mounts() -> (bool, Option<u64>, Option<String>) {
    let paths_to_check = ["/data", "/results", "/tmp"];
    let mut tmpfs_enabled = false;
    let mut tmpfs_size_mb = None;
    let mut data_fs_type = None;

    for path in paths_to_check {
        if let Ok(stat) = nix::sys::statfs::statfs(path) {
            let fs_type = stat.filesystem_type();
            let is_tmpfs = is_tmpfs_filesystem(fs_type);
            let fs_name = format_fs_type(fs_type);

            if path == "/data" {
                data_fs_type = Some(fs_name.clone());
                tmpfs_enabled = is_tmpfs;
                if is_tmpfs {
                    // Calculate tmpfs size in MB
                    let block_size = stat.block_size() as u64;
                    let total_blocks = stat.blocks();
                    tmpfs_size_mb = Some((total_blocks * block_size) / 1024 / 1024);
                }
            }

            info!(
                "{}: {} (tmpfs: {})",
                path,
                fs_name,
                if is_tmpfs { "true" } else { "false" }
            );

            if is_tmpfs {
                let block_size = stat.block_size() as u64;
                let total_blocks = stat.blocks();
                let size_mb = (total_blocks * block_size) / 1024 / 1024;
                info!("  tmpfs size: {} MB", size_mb);
            }
        }
    }

    (tmpfs_enabled, tmpfs_size_mb, data_fs_type)
}

/// Check if the filesystem type is tmpfs.
fn is_tmpfs_filesystem(fs_type: nix::sys::statfs::FsType) -> bool {
    // TMPFS_MAGIC = 0x01021994
    const TMPFS_MAGIC: i64 = 0x01021994;
    fs_type.0 == TMPFS_MAGIC
}

/// Format filesystem type to human-readable string.
fn format_fs_type(fs_type: nix::sys::statfs::FsType) -> String {
    const TMPFS_MAGIC: i64 = 0x01021994;
    const EXT4_MAGIC: i64 = 0xEF53;
    const XFS_MAGIC: i64 = 0x58465342;
    const BTRFS_MAGIC: i64 = 0x9123683E;
    const OVERLAY_MAGIC: i64 = 0x794c7630;

    match fs_type.0 {
        x if x == TMPFS_MAGIC => "tmpfs".to_string(),
        x if x == EXT4_MAGIC => "ext4".to_string(),
        x if x == XFS_MAGIC => "xfs".to_string(),
        x if x == BTRFS_MAGIC => "btrfs".to_string(),
        x if x == OVERLAY_MAGIC => "overlay".to_string(),
        _ => format!("unknown(0x{:x})", fs_type.0),
    }
}

/// Read cgroup v2 memory limit.
fn read_cgroup_memory_limit() -> Option<u64> {
    // Try cgroup v2 first
    if let Ok(content) = fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = content.trim();
        if trimmed == "max" {
            return None; // No limit
        }
        return trimmed.parse().ok();
    }

    // Fall back to cgroup v1
    if let Ok(content) = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        let trimmed = content.trim();
        // Very large values indicate no limit
        if let Ok(value) = trimmed.parse::<u64>() {
            if value < 9_000_000_000_000_000_000 {
                return Some(value);
            }
        }
    }

    None
}

/// Read cgroup v2 CPU quota.
fn read_cgroup_cpu_quota() -> Option<String> {
    // Try cgroup v2 first
    if let Ok(content) = fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let trimmed = content.trim();
        if trimmed != "max 100000" {
            return Some(trimmed.to_string());
        }
    }

    // Fall back to cgroup v1
    if let Ok(quota) = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") {
        let quota_trimmed = quota.trim();
        if let Ok(period) = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us") {
            let period_trimmed = period.trim();
            if quota_trimmed != "-1" {
                return Some(format!("{quota_trimmed} {period_trimmed}"));
            }
        }
    }

    None
}

/// Verify that expected resources are available.
///
/// Returns a list of warnings if resources don't match expectations.
pub fn verify_expected_resources(
    expected_cpu: Option<f64>,
    expected_memory_mb: Option<u64>,
    expected_tmpfs: bool,
    env: &EnvironmentInfo,
) -> Vec<String> {
    let mut warnings = Vec::new();

    // Check CPU
    if let Some(expected) = expected_cpu {
        let visible = env.cpu_cores as f64;
        // CPU cores might be limited by cgroup quota, so just warn if significantly different
        if visible < expected * 0.5 {
            warnings.push(format!(
                "Expected ~{} CPU cores but only {} visible",
                expected, env.cpu_cores
            ));
        }
    }

    // Check memory
    if let Some(expected) = expected_memory_mb {
        let actual = env
            .cgroup_memory_limit
            .unwrap_or(env.memory_mb * 1024 * 1024);
        let expected_bytes = expected * 1024 * 1024;
        // Allow 10% variance
        if actual < (expected_bytes as f64 * 0.9) as u64 {
            warnings.push(format!(
                "Expected ~{} MB memory but cgroup limit is {} MB",
                expected,
                actual / 1024 / 1024
            ));
        }
    }

    // Check tmpfs
    if expected_tmpfs && !env.tmpfs_enabled {
        warnings.push("Expected tmpfs for /data but filesystem is not tmpfs".to_string());
    }

    warnings
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_fs_type() {
        // Just test that formatting doesn't panic
        let tmpfs_type = nix::sys::statfs::FsType(0x01021994);
        assert_eq!(format_fs_type(tmpfs_type), "tmpfs");

        let ext4_type = nix::sys::statfs::FsType(0xEF53);
        assert_eq!(format_fs_type(ext4_type), "ext4");

        let unknown_type = nix::sys::statfs::FsType(0x12345678);
        assert!(format_fs_type(unknown_type).starts_with("unknown"));
    }
}
