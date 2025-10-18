use chrono::{DateTime, NaiveDate, NaiveTime, Utc};

/// PostgreSQL TIME value (without timezone)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Time(pub String);

impl Time {
    /// Converts to chrono DateTime<Utc> (using epoch date 1970-01-01)
    pub fn to_chrono_datetime_utc(&self) -> Result<DateTime<Utc>, String> {
        let naive_time = NaiveTime::parse_from_str(&self.0, "%H:%M:%S%.f")
            .map_err(|e| format!("Failed to parse time: {e}"))?;
        let epoch_date =
            NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| "Invalid epoch date".to_string())?;
        let naive_datetime = epoch_date.and_time(naive_time);
        Ok(DateTime::from_naive_utc_and_offset(naive_datetime, Utc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_to_chrono() {
        let time = Time("10:30:45.123".to_string());
        let result = time.to_chrono_datetime_utc().unwrap();
        assert_eq!(result.to_rfc3339(), "1970-01-01T10:30:45.123+00:00");
    }
}
