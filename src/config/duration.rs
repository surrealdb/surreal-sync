//! Duration parsing utilities.

use anyhow::Context;

/// Parse a duration string like "1h", "30m", "300s", "300" into seconds.
/// Supports:
/// - Plain numbers (interpreted as seconds): "300"
/// - Seconds suffix: "300s"
/// - Minutes suffix: "30m"
/// - Hours suffix: "1h"
pub fn parse_duration_to_secs(s: &str) -> anyhow::Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("Empty duration string");
    }

    // Check for suffix
    if let Some(num_str) = s.strip_suffix('h') {
        let hours: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid hours value: {num_str}"))?;
        return Ok(hours * 3600);
    }
    if let Some(num_str) = s.strip_suffix('m') {
        let minutes: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid minutes value: {num_str}"))?;
        return Ok(minutes * 60);
    }
    if let Some(num_str) = s.strip_suffix('s') {
        let secs: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid seconds value: {num_str}"))?;
        return Ok(secs);
    }

    // No suffix - treat as seconds
    s.parse::<i64>()
        .with_context(|| format!("Invalid duration value: {s}"))
}
