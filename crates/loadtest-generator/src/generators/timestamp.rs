//! Timestamp value generators.

use chrono::{DateTime, Utc};
use rand::Rng;
use sync_core::UniversalValue;

// Hardcoded increment range (can be made configurable later)
const DEFAULT_MIN_INCREMENT_MS: i64 = 1;
const DEFAULT_MAX_INCREMENT_MS: i64 = 1000;

/// Generate monotonically increasing timestamp (deterministic).
///
/// Generates: seed_derived_base + cumulative_random_increments[0..index]
/// - Base timestamp derived from seed (2024-01-01 + seed % 10 years)
/// - Increments: hardcoded 1-1000ms per row (can be made configurable later)
pub fn generate_timestamp_now<R: Rng + Clone>(
    rng: &mut R,
    seed: u64,
    index: u64,
) -> UniversalValue {
    // Derive base timestamp from seed (deterministic)
    // Use a fixed epoch (2024-01-01) and add seed modulo ~10 years in seconds
    // This gives a range of timestamps around 2024-2034 depending on seed
    let epoch = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    // Seed determines offset from epoch (0 to ~10 years)
    let max_offset_secs = 10 * 365 * 24 * 60 * 60; // ~10 years in seconds
    let offset_secs = (seed % max_offset_secs) as i64;
    let base_dt = epoch + chrono::Duration::seconds(offset_secs);

    // Generate cumulative millisecond offset for this index
    // We regenerate the sequence to maintain determinism with the RNG
    let mut temp_rng = rng.clone();
    let mut total_ms = 0i64;

    for _ in 0..index {
        let increment = temp_rng.random_range(DEFAULT_MIN_INCREMENT_MS..=DEFAULT_MAX_INCREMENT_MS);
        total_ms += increment;
    }

    // Consume RNG state for current index to maintain proper sequence
    let _current = rng.random_range(DEFAULT_MIN_INCREMENT_MS..=DEFAULT_MAX_INCREMENT_MS);

    let timestamp = base_dt + chrono::Duration::milliseconds(total_ms);
    UniversalValue::LocalDateTime(timestamp)
}

/// Generate timestamp in a range (deterministic, monotonically increasing).
///
/// Behaves deterministically:
/// - Uses `start` as base timestamp + seed offset for uniqueness
/// - Generates: (start + seed_offset) + cumulative_random_increments[0..index]
/// - Hardcoded increments: 1-1000ms per row
/// - Seed offset ensures each table gets unique timestamps
/// - May deprecate this function in favor of `timestamp_now` in the future
pub fn generate_timestamp_range<R: Rng + Clone>(
    rng: &mut R,
    seed: u64,
    start: &str,
    _end: &str, // Ignored for now - kept for API compatibility
    index: u64,
) -> UniversalValue {
    // Parse base timestamp from start parameter
    let start_dt = parse_timestamp(start).expect("Invalid start timestamp in schema");

    // Add seed-based offset to ensure different tables get unique timestamps
    // Use modulo to keep offset reasonable (within ~10 years)
    let max_offset_secs = 10 * 365 * 24 * 60 * 60; // ~10 years in seconds
    let offset_secs = (seed % max_offset_secs) as i64;
    let base_dt = start_dt + chrono::Duration::seconds(offset_secs);

    // Generate cumulative millisecond offset (same logic as timestamp_now)
    let mut temp_rng = rng.clone();
    let mut total_ms = 0i64;

    for _ in 0..index {
        let increment = temp_rng.random_range(DEFAULT_MIN_INCREMENT_MS..=DEFAULT_MAX_INCREMENT_MS);
        total_ms += increment;
    }

    // Consume RNG state for current index to maintain proper sequence
    let _current = rng.random_range(DEFAULT_MIN_INCREMENT_MS..=DEFAULT_MAX_INCREMENT_MS);

    let timestamp = base_dt + chrono::Duration::milliseconds(total_ms);
    UniversalValue::LocalDateTime(timestamp)
}

/// Parse a timestamp string in various formats.
fn parse_timestamp(s: &str) -> Option<DateTime<Utc>> {
    // Try RFC 3339 / ISO 8601
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }

    // Try common date-only format
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(date.and_hms_opt(0, 0, 0)?.and_utc());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_generate_timestamp_range() {
        let mut rng = StdRng::seed_from_u64(42);

        let value = generate_timestamp_range(
            &mut rng,
            42,
            "2020-01-01T00:00:00Z",
            "2024-12-31T23:59:59Z",
            5,
        );

        if let UniversalValue::LocalDateTime(dt) = value {
            assert!(dt.year() >= 2020);
        } else {
            panic!("Expected DateTime value");
        }
    }

    #[test]
    fn test_generate_timestamp_with_dates_only() {
        let mut rng = StdRng::seed_from_u64(42);

        let value = generate_timestamp_range(&mut rng, 42, "2020-01-01", "2024-12-31", 5);

        if let UniversalValue::LocalDateTime(dt) = value {
            assert!(dt.year() >= 2020);
        } else {
            panic!("Expected DateTime value");
        }
    }

    #[test]
    fn test_deterministic_generation() {
        let mut rng1 = StdRng::seed_from_u64(42);
        let mut rng2 = StdRng::seed_from_u64(42);

        let value1 = generate_timestamp_range(
            &mut rng1,
            42,
            "2020-01-01T00:00:00Z",
            "2024-12-31T23:59:59Z",
            5,
        );
        let value2 = generate_timestamp_range(
            &mut rng2,
            42,
            "2020-01-01T00:00:00Z",
            "2024-12-31T23:59:59Z",
            5,
        );

        assert_eq!(value1, value2);
    }

    #[test]
    fn test_timestamp_now_deterministic() {
        let mut rng1 = StdRng::seed_from_u64(42);
        let mut rng2 = StdRng::seed_from_u64(42);

        let ts1 = generate_timestamp_now(&mut rng1, 42, 5);
        let ts2 = generate_timestamp_now(&mut rng2, 42, 5);

        assert_eq!(ts1, ts2, "Same seed should produce same timestamp");
    }

    #[test]
    fn test_timestamp_now_monotonic() {
        let mut rng = StdRng::seed_from_u64(42);

        let ts0 = generate_timestamp_now(&mut rng, 42, 0);

        let mut rng = StdRng::seed_from_u64(42);
        let ts1 = generate_timestamp_now(&mut rng, 42, 1);

        let mut rng = StdRng::seed_from_u64(42);
        let ts2 = generate_timestamp_now(&mut rng, 42, 2);

        if let (
            UniversalValue::LocalDateTime(t0),
            UniversalValue::LocalDateTime(t1),
            UniversalValue::LocalDateTime(t2),
        ) = (ts0, ts1, ts2)
        {
            assert!(t0 < t1, "Timestamps should increase");
            assert!(t1 < t2, "Timestamps should increase");
        } else {
            panic!("Expected DateTime values");
        }
    }

    #[test]
    fn test_timestamp_now_no_collisions() {
        let mut timestamps = Vec::new();

        for i in 0..100 {
            let mut rng = StdRng::seed_from_u64(42);
            let ts = generate_timestamp_now(&mut rng, 42, i);
            if let UniversalValue::LocalDateTime(dt) = ts {
                timestamps.push(dt);
            }
        }

        // Check all timestamps are unique
        let mut sorted = timestamps.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            timestamps.len(),
            sorted.len(),
            "All timestamps should be unique"
        );
    }
}
