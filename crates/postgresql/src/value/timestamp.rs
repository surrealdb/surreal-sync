use chrono::{DateTime, Utc};

/// PostgreSQL TIMESTAMP value (without timezone)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timestamp(pub String);

impl Timestamp {
    /// Converts to chrono DateTime<Utc>
    /// Assumes the timestamp is in UTC if no timezone is specified
    pub fn to_chrono_datetime_utc(&self) -> Result<DateTime<Utc>, chrono::ParseError> {
        self.0.parse::<DateTime<Utc>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_to_chrono() {
        let ts = Timestamp("2024-01-15T10:30:00Z".to_string());
        let result = ts.to_chrono_datetime_utc().unwrap();
        assert_eq!(result.to_rfc3339(), "2024-01-15T10:30:00+00:00");
    }
}
