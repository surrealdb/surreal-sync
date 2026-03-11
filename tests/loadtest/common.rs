/// Row count for loadtests. Reads `LOADTEST_ROWS` env var, defaults to 5 for fast local runs.
/// CI sets `LOADTEST_ROWS=50` for more thorough testing.
pub fn row_count() -> u64 {
    std::env::var("LOADTEST_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
}
