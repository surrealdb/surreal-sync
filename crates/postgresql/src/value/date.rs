use chrono::{DateTime, NaiveDate, Utc};

/// PostgreSQL DATE value
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Date(pub String);

impl Date {
    /// Converts to chrono DateTime<Utc> (at midnight UTC)
    pub fn to_chrono_datetime_utc(&self) -> Result<DateTime<Utc>, String> {
        let naive_date = NaiveDate::parse_from_str(&self.0, "%Y-%m-%d")
            .map_err(|e| format!("Failed to parse date: {e}"))?;
        let naive_datetime = naive_date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| "Invalid date components".to_string())?;
        Ok(DateTime::from_naive_utc_and_offset(naive_datetime, Utc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_to_chrono() {
        let date = Date("2024-01-15".to_string());
        let result = date.to_chrono_datetime_utc().unwrap();
        assert_eq!(result.to_rfc3339(), "2024-01-15T00:00:00+00:00");
    }
}
