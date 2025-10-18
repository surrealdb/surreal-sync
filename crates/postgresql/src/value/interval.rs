use std::time::Duration;

/// PostgreSQL INTERVAL value
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval(pub String);

impl Interval {
    /// Converts to std::time::Duration
    /// Handles PostgreSQL interval formats including:
    /// - "6 years 5 months 4 days 3 hours 2 minutes 1 second"
    /// - "6years 5months 4days 3hours 2minutes 1second"
    /// - "1 day", "2 hours", "30 seconds"
    ///
    /// Note: Years are 360 days (12 months × 30 days), months are 30 days
    ///
    /// # Important: Semantic Loss in Conversion
    ///
    /// This conversion is **lossy** and may not preserve the semantics of PostgreSQL intervals
    /// when used with other systems like SurrealDB:
    ///
    /// - In PostgreSQL: `now() + interval '1 year'` correctly adds one calendar year
    ///   (accounting for leap years, etc.)
    /// - After conversion: A 1-year interval becomes a fixed 360-day duration
    /// - In target systems: Adding this duration may not land on the same date/month
    ///   one year later
    ///
    /// Similarly, there's no way to express "exactly one month later" in systems like
    /// SurrealDB that use fixed durations, since months have varying lengths (28-31 days).
    ///
    /// **Use with caution** when converting PostgreSQL intervals to other duration systems.
    /// This conversion is best suited for:
    /// - Approximate time calculations
    /// - Systems where calendar-aware arithmetic is not required
    /// - Logging and display purposes
    pub fn to_duration(&self) -> Result<Duration, String> {
        let mut total_secs: f64 = 0.0;
        let input = self.0.trim();

        // Split by whitespace first
        let parts: Vec<&str> = input.split_whitespace().collect();

        let mut i = 0;
        while i < parts.len() {
            let part = parts[i];

            // Try to parse as "valueunit" format (e.g., "6years")
            if let Some((value, unit)) = Self::parse_combined_part(part) {
                total_secs += Self::unit_to_seconds(value, unit)?;
                i += 1;
            }
            // Try to parse as "value unit" format (e.g., "6 years")
            else if i + 1 < parts.len() {
                let value: f64 = part
                    .parse()
                    .map_err(|_| format!("Invalid interval value: {}", part))?;
                let unit = parts[i + 1];
                total_secs += Self::unit_to_seconds(value, unit)?;
                i += 2;
            } else {
                return Err(format!(
                    "Invalid interval format at position {}: {}",
                    i, part
                ));
            }
        }

        Ok(Duration::from_secs_f64(total_secs))
    }

    /// Parses a combined part like "6years" into (value, unit)
    fn parse_combined_part(part: &str) -> Option<(f64, &str)> {
        // Find where the unit starts (first non-digit, non-decimal point character)
        let unit_start = part.find(|c: char| !c.is_ascii_digit() && c != '.')?;

        let value_str = &part[..unit_start];
        let unit_str = &part[unit_start..];

        if value_str.is_empty() || unit_str.is_empty() {
            return None;
        }

        let value: f64 = value_str.parse().ok()?;
        Some((value, unit_str))
    }

    /// Converts a value and unit to seconds
    fn unit_to_seconds(value: f64, unit: &str) -> Result<f64, String> {
        let multiplier = match unit {
            "second" | "seconds" | "sec" | "secs" | "s" => 1.0,
            "minute" | "minutes" | "min" | "mins" | "m" => 60.0,
            "hour" | "hours" | "hr" | "hrs" | "h" => 3600.0,
            "day" | "days" | "d" => 86400.0,
            "week" | "weeks" | "w" => 604800.0,
            "month" | "months" | "mon" | "mons" => 2592000.0, // 30 days
            "year" | "years" | "yr" | "yrs" | "y" => 31104000.0, // 360 days (12 months × 30 days)
            _ => return Err(format!("Unsupported interval unit: {}", unit)),
        };

        Ok(value * multiplier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_to_duration() {
        // Simple formats
        assert_eq!(
            Interval("30 seconds".to_string()).to_duration().unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            Interval("5 minutes".to_string()).to_duration().unwrap(),
            Duration::from_secs(300)
        );
        assert_eq!(
            Interval("2 hours".to_string()).to_duration().unwrap(),
            Duration::from_secs(7200)
        );
        assert_eq!(
            Interval("1 day".to_string()).to_duration().unwrap(),
            Duration::from_secs(86400)
        );

        // Combined format with spaces
        let complex_interval =
            Interval("6 years 5 months 4 days 3 hours 2 minutes 1 second".to_string());
        let result = complex_interval.to_duration().unwrap();
        let expected_secs = 6.0 * 31104000.0  // 6 years (360 days each)
            + 5.0 * 2592000.0  // 5 months (30 days each)
            + 4.0 * 86400.0    // 4 days
            + 3.0 * 3600.0     // 3 hours
            + 2.0 * 60.0       // 2 minutes
            + 1.0; // 1 second
        assert_eq!(result, Duration::from_secs_f64(expected_secs));

        // Combined format without spaces
        let compact_interval = Interval("6years 5months 4days 3hours 2minutes 1second".to_string());
        let result = compact_interval.to_duration().unwrap();
        assert_eq!(result, Duration::from_secs_f64(expected_secs));

        // Mixed format
        let mixed = Interval("1 day 12 hours".to_string());
        assert_eq!(
            mixed.to_duration().unwrap(),
            Duration::from_secs(86400 + 43200)
        );

        // Compact mixed format
        let compact_mixed = Interval("1day 12hours".to_string());
        assert_eq!(
            compact_mixed.to_duration().unwrap(),
            Duration::from_secs(86400 + 43200)
        );
    }
}
