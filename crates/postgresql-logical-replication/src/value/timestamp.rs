use chrono::{DateTime, NaiveDateTime, Utc};

/// PostgreSQL TIMESTAMP value (without timezone)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timestamp(pub String);

impl Timestamp {
    /// Converts to chrono DateTime<Utc>
    /// Assumes the timestamp is in UTC if no timezone is specified
    ///
    /// Supports multiple PostgreSQL timestamp formats:
    /// - ISO 8601: "2024-01-15T10:30:00Z"
    /// - PostgreSQL wal2json: "2024-01-15 10:30:00", "1997-12-17 15:37:16.123456"
    pub fn to_chrono_datetime_utc(&self) -> Result<DateTime<Utc>, String> {
        // Try ISO 8601 format with 'Z' first
        if let Ok(dt) = self.0.parse::<DateTime<Utc>>() {
            return Ok(dt);
        }

        // Try PostgreSQL wal2json format: "2024-01-15 10:30:00"
        // This is a NaiveDateTime (no timezone), so we interpret it as UTC
        let formats = vec![
            "%Y-%m-%d %H:%M:%S",    // 2024-01-15 10:30:00
            "%Y-%m-%d %H:%M:%S%.f", // With fractional seconds
            "%Y-%m-%dT%H:%M:%S",    // ISO 8601 without timezone
            "%Y-%m-%dT%H:%M:%S%.f", // ISO 8601 with fractional seconds, no timezone
        ];

        for format in formats {
            if let Ok(naive_dt) = NaiveDateTime::parse_from_str(&self.0, format) {
                // Interpret as UTC since TIMESTAMP has no timezone
                return Ok(DateTime::from_naive_utc_and_offset(naive_dt, Utc));
            }
        }

        Err(format!("Unable to parse timestamp: {}", self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_to_chrono() {
        // ISO 8601 with Z
        let ts = Timestamp("2024-01-15T10:30:00Z".to_string());
        let result = ts.to_chrono_datetime_utc().unwrap();
        assert_eq!(result.to_rfc3339(), "2024-01-15T10:30:00+00:00");

        // PostgreSQL wal2json format
        let ts2 = Timestamp("2024-01-15 10:30:00".to_string());
        let result2 = ts2.to_chrono_datetime_utc().unwrap();
        assert_eq!(result2.to_rfc3339(), "2024-01-15T10:30:00+00:00");

        // With fractional seconds
        let ts3 = Timestamp("1997-12-17 15:37:16.123456".to_string());
        let result3 = ts3.to_chrono_datetime_utc().unwrap();
        assert_eq!(result3.to_rfc3339(), "1997-12-17T15:37:16.123456+00:00");
    }
}
