use std::time::Duration;

/// PostgreSQL INTERVAL value
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval(pub String);

impl Interval {
    /// Converts to std::time::Duration
    /// Handles PostgreSQL interval formats including:
    /// - HH:MM:SS format: "00:00:30", "02:30:45"
    /// - Verbose format: "30 seconds", "1 day", "2 hours"
    /// - Mixed format: "1 day 02:30:00" (combines verbose with HH:MM:SS)
    /// - ISO 8601 format: "P1DT2H30M" (parsed by PostgreSQL)
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
        let input = self.0.trim();

        // Special case: if the entire string is in HH:MM:SS format, parse it directly
        if Self::is_pure_hms_format(input) {
            return Self::parse_hms_component(input)
                .map(Duration::from_secs_f64)
                .ok_or_else(|| format!("Invalid HH:MM:SS format: {input}"));
        }

        // Otherwise, parse as a potentially mixed format
        let mut total_seconds: f64 = 0.0;

        // Split by whitespace to get tokens
        let tokens: Vec<&str> = input.split_whitespace().collect();

        let mut i = 0;
        while i < tokens.len() {
            let token = tokens[i];

            // Check if this token looks like HH:MM:SS
            if Self::is_pure_hms_format(token) {
                if let Some(seconds) = Self::parse_hms_component(token) {
                    total_seconds += seconds;
                    i += 1;
                    continue;
                }
            }

            // Try to parse as a number
            if let Ok(value) = token.parse::<f64>() {
                // Look for the unit in the next token
                if i + 1 < tokens.len() {
                    let unit = tokens[i + 1];

                    // Check if the unit is actually an HH:MM:SS format
                    if Self::is_pure_hms_format(unit) {
                        // This means we have something like "1 02:30:00" which should be "1 day 02:30:00"
                        // The number is likely days
                        total_seconds += value * 86400.0; // Assume days
                        if let Some(hms_seconds) = Self::parse_hms_component(unit) {
                            total_seconds += hms_seconds;
                        }
                        i += 2;
                    } else {
                        // Normal unit
                        total_seconds += Self::unit_to_seconds(value, unit)?;
                        i += 2;
                    }
                } else {
                    return Err(format!("Value without unit: {token}"));
                }
            } else {
                // Not a number, might be a combined format like "6years"
                if let Some((value, unit)) = Self::parse_combined_part(token) {
                    total_seconds += Self::unit_to_seconds(value, unit)?;
                    i += 1;
                } else {
                    return Err(format!("Invalid interval token: {token}"));
                }
            }
        }

        Ok(Duration::from_secs_f64(total_seconds))
    }

    /// Checks if a string is in HH:MM:SS format
    fn is_pure_hms_format(s: &str) -> bool {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 3 {
            return false;
        }

        // Check if all parts can be parsed as numbers (allowing decimals in seconds)
        parts[0].parse::<f64>().is_ok()
            && parts[1].parse::<f64>().is_ok()
            && parts[2].parse::<f64>().is_ok()
    }

    /// Parses HH:MM:SS or HH:MM:SS.microseconds format into total seconds
    fn parse_hms_component(s: &str) -> Option<f64> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 3 {
            return None;
        }

        let hours = parts[0].parse::<f64>().ok()?;
        let minutes = parts[1].parse::<f64>().ok()?;
        let seconds = parts[2].parse::<f64>().ok()?;

        Some(hours * 3600.0 + minutes * 60.0 + seconds)
    }

    /// Parses a combined part like "6years" into (value, unit)
    fn parse_combined_part(part: &str) -> Option<(f64, &str)> {
        // Find where the unit starts (first non-digit, non-decimal point, non-minus character)
        let unit_start = part.find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-')?;

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
            _ => return Err(format!("Unsupported interval unit: {unit}")),
        };

        Ok(value * multiplier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hms_formats() {
        // Pure HH:MM:SS formats
        assert_eq!(
            Interval("00:00:30".to_string()).to_duration().unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            Interval("00:05:00".to_string()).to_duration().unwrap(),
            Duration::from_secs(300)
        );
        assert_eq!(
            Interval("02:00:00".to_string()).to_duration().unwrap(),
            Duration::from_secs(7200)
        );
        assert_eq!(
            Interval("01:30:00".to_string()).to_duration().unwrap(),
            Duration::from_secs(5400)
        );

        // With fractional seconds
        assert_eq!(
            Interval("00:00:30.5".to_string()).to_duration().unwrap(),
            Duration::from_secs_f64(30.5)
        );
    }

    #[test]
    fn test_verbose_formats() {
        // Simple verbose formats
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
        assert_eq!(
            Interval("3 days".to_string()).to_duration().unwrap(),
            Duration::from_secs(259200)
        );
        assert_eq!(
            Interval("14 days".to_string()).to_duration().unwrap(),
            Duration::from_secs(1209600)
        );
        assert_eq!(
            Interval("1 mon".to_string()).to_duration().unwrap(),
            Duration::from_secs(2592000)
        );
        assert_eq!(
            Interval("1 year".to_string()).to_duration().unwrap(),
            Duration::from_secs(31104000)
        );
    }

    #[test]
    fn test_mixed_formats() {
        // PostgreSQL's mixed format: verbose + HH:MM:SS
        assert_eq!(
            Interval("1 day 02:30:00".to_string())
                .to_duration()
                .unwrap(),
            Duration::from_secs(86400 + 9000) // 1 day + 2.5 hours
        );

        // Just "1 02:30:00" should interpret 1 as days
        assert_eq!(
            Interval("1 02:30:00".to_string()).to_duration().unwrap(),
            Duration::from_secs(86400 + 9000) // 1 day + 2.5 hours
        );

        // Complex mixed format
        assert_eq!(
            Interval("2 days 03:45:30".to_string())
                .to_duration()
                .unwrap(),
            Duration::from_secs(2 * 86400 + 3 * 3600 + 45 * 60 + 30)
        );
    }

    #[test]
    fn test_combined_unit_formats() {
        // Combined format without spaces
        assert_eq!(
            Interval("30seconds".to_string()).to_duration().unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            Interval("5minutes".to_string()).to_duration().unwrap(),
            Duration::from_secs(300)
        );
        assert_eq!(
            Interval("2hours".to_string()).to_duration().unwrap(),
            Duration::from_secs(7200)
        );
        assert_eq!(
            Interval("1day".to_string()).to_duration().unwrap(),
            Duration::from_secs(86400)
        );
    }

    #[test]
    fn test_complex_intervals() {
        // Multiple units with spaces
        let interval = Interval("1 day 2 hours 30 minutes".to_string());
        assert_eq!(
            interval.to_duration().unwrap(),
            Duration::from_secs(86400 + 7200 + 1800)
        );

        // Multiple units without spaces
        let interval = Interval("1day 2hours 30minutes".to_string());
        assert_eq!(
            interval.to_duration().unwrap(),
            Duration::from_secs(86400 + 7200 + 1800)
        );

        // Very complex format
        let interval = Interval("6 years 5 months 4 days 3 hours 2 minutes 1 second".to_string());
        let expected_secs = 6.0 * 31104000.0  // 6 years
            + 5.0 * 2592000.0  // 5 months
            + 4.0 * 86400.0    // 4 days
            + 3.0 * 3600.0     // 3 hours
            + 2.0 * 60.0       // 2 minutes
            + 1.0; // 1 second
        assert_eq!(
            interval.to_duration().unwrap(),
            Duration::from_secs_f64(expected_secs)
        );
    }

    #[test]
    fn test_edge_cases() {
        // Empty string
        assert_eq!(
            Interval("".to_string()).to_duration().unwrap(),
            Duration::from_secs(0)
        );

        // Just whitespace
        assert_eq!(
            Interval("  ".to_string()).to_duration().unwrap(),
            Duration::from_secs(0)
        );

        // Note: Negative intervals are not supported by std::time::Duration
        // They would need to be handled at the application level
    }

    #[test]
    fn test_iso8601_style() {
        // PostgreSQL converts ISO 8601 durations internally
        // When it outputs them, they're already converted to verbose format
        // So "P1DT2H30M" becomes "1 day 02:30:00"
        // This test verifies our parser handles what PostgreSQL outputs
        assert_eq!(
            Interval("1 day 02:30:00".to_string())
                .to_duration()
                .unwrap(),
            Duration::from_secs(86400 + 9000)
        );
    }
}
