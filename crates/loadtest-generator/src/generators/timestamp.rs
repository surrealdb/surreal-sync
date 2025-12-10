//! Timestamp value generators.

use chrono::{DateTime, Utc};
use rand::Rng;
use sync_core::UniversalValue;

/// Generate the current UTC timestamp.
///
/// This is NOT deterministic - each call returns the current time.
/// Useful for `updated_at` fields in incremental sync scenarios.
pub fn generate_timestamp_now() -> UniversalValue {
    UniversalValue::LocalDateTime(Utc::now())
}

/// Generate a random timestamp in the given range.
///
/// The start and end should be ISO 8601 formatted timestamps.
pub fn generate_timestamp_range<R: Rng>(rng: &mut R, start: &str, end: &str) -> UniversalValue {
    let start_dt = parse_timestamp(start);
    let end_dt = parse_timestamp(end);

    match (start_dt, end_dt) {
        (Some(start), Some(end)) => {
            let start_ts = start.timestamp();
            let end_ts = end.timestamp();

            if start_ts >= end_ts {
                UniversalValue::LocalDateTime(start)
            } else {
                let random_ts = rng.gen_range(start_ts..=end_ts);
                let dt = DateTime::from_timestamp(random_ts, 0).unwrap_or(start);
                UniversalValue::LocalDateTime(dt)
            }
        }
        (Some(dt), None) | (None, Some(dt)) => UniversalValue::LocalDateTime(dt),
        (None, None) => UniversalValue::LocalDateTime(Utc::now()),
    }
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

        let value =
            generate_timestamp_range(&mut rng, "2020-01-01T00:00:00Z", "2024-12-31T23:59:59Z");

        if let UniversalValue::LocalDateTime(dt) = value {
            assert!(dt.year() >= 2020 && dt.year() <= 2024);
        } else {
            panic!("Expected DateTime value");
        }
    }

    #[test]
    fn test_generate_timestamp_with_dates_only() {
        let mut rng = StdRng::seed_from_u64(42);

        let value = generate_timestamp_range(&mut rng, "2020-01-01", "2024-12-31");

        if let UniversalValue::LocalDateTime(dt) = value {
            assert!(dt.year() >= 2020 && dt.year() <= 2024);
        } else {
            panic!("Expected DateTime value");
        }
    }

    #[test]
    fn test_deterministic_generation() {
        let mut rng1 = StdRng::seed_from_u64(42);
        let mut rng2 = StdRng::seed_from_u64(42);

        let value1 =
            generate_timestamp_range(&mut rng1, "2020-01-01T00:00:00Z", "2024-12-31T23:59:59Z");
        let value2 =
            generate_timestamp_range(&mut rng2, "2020-01-01T00:00:00Z", "2024-12-31T23:59:59Z");

        assert_eq!(value1, value2);
    }
}
