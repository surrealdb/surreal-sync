//! Docker image tags for binlog integration tests.
//!
//! Packaged defaults live in `crates/mysql/test-images.env` (must stay in sync
//! with `scripts/test-images.env` used by Makefile / CI). Override at runtime
//! with `MYSQL_BINLOG_IMAGE` / `MARIADB_BINLOG_IMAGE`.

const ENV_FILE: &str = include_str!("../../test-images.env");

fn from_env_file(key: &str) -> Option<String> {
    ENV_FILE.lines().find_map(|line| {
        let line = line.split('#').next()?.trim();
        if line.is_empty() {
            return None;
        }
        let (k, v) = line.split_once('=')?;
        (k.trim() == key).then(|| v.trim().to_string())
    })
}

fn binlog_image(env_var: &str) -> String {
    std::env::var(env_var)
        .ok()
        .or_else(|| from_env_file(env_var))
        .unwrap_or_else(|| {
            panic!("{env_var} not set and missing from crates/mysql/test-images.env")
        })
}

/// Stock MySQL image for binlog CDC integration tests.
pub fn mysql_binlog_image() -> String {
    binlog_image("MYSQL_BINLOG_IMAGE")
}

/// Stock MariaDB image for binlog CDC integration tests.
pub fn mariadb_binlog_image() -> String {
    binlog_image("MARIADB_BINLOG_IMAGE")
}
