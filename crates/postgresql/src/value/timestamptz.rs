use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

/// PostgreSQL TIMESTAMPTZ value (with timezone)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampTz(pub String);

impl TimestampTz {
    /// Converts to chrono DateTime<Utc>
    /// Supports multiple PostgreSQL timestamp formats:
    /// - ISO 8601: "2024-01-15T10:30:00+00:00", "1997-12-17T15:37:16Z"
    /// - Traditional SQL: "12/17/1997 07:37:16.00 PST"
    /// - Traditional PostgreSQL: "Wed Dec 17 07:37:16 1997 PST"
    pub fn to_chrono_datetime_utc(&self) -> Result<DateTime<Utc>, String> {
        // Try ISO 8601 format first (most common from wal2json)
        if let Ok(dt) = self.0.parse::<DateTime<Utc>>() {
            return Ok(dt);
        }

        // Try traditional SQL format: "12/17/1997 07:37:16.00 PST"
        if let Ok(dt) = Self::parse_sql_format(&self.0) {
            return Ok(dt);
        }

        // Try traditional PostgreSQL format: "Wed Dec 17 07:37:16 1997 PST"
        if let Ok(dt) = Self::parse_postgresql_format(&self.0) {
            return Ok(dt);
        }

        Err(format!("Unable to parse timestamp: {}", self.0))
    }

    /// Parses traditional SQL timestamp format: "12/17/1997 07:37:16.00 PST"
    fn parse_sql_format(s: &str) -> Result<DateTime<Utc>, String> {
        // Format: MM/DD/YYYY HH:MM:SS.SS TZ
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 3 {
            return Err("Invalid SQL timestamp format".to_string());
        }

        let date_part = parts[0];
        let time_part = parts[1];
        let tz_part = parts[2];

        let date_components: Vec<&str> = date_part.split('/').collect();
        if date_components.len() != 3 {
            return Err("Invalid date component".to_string());
        }

        let month: u32 = date_components[0].parse().map_err(|_| "Invalid month")?;
        let day: u32 = date_components[1].parse().map_err(|_| "Invalid day")?;
        let year: i32 = date_components[2].parse().map_err(|_| "Invalid year")?;

        let time_components: Vec<&str> = time_part.split(':').collect();
        if time_components.len() != 3 {
            return Err("Invalid time component".to_string());
        }

        let hour: u32 = time_components[0].parse().map_err(|_| "Invalid hour")?;
        let minute: u32 = time_components[1].parse().map_err(|_| "Invalid minute")?;
        let second: f64 = time_components[2].parse().map_err(|_| "Invalid second")?;

        let naive_date = NaiveDate::from_ymd_opt(year, month, day).ok_or("Invalid date")?;
        let naive_time = NaiveTime::from_hms_milli_opt(
            hour,
            minute,
            second as u32,
            (second.fract() * 1000.0) as u32,
        )
        .ok_or("Invalid time")?;
        let naive_datetime = NaiveDateTime::new(naive_date, naive_time);

        // Apply timezone offset
        Self::apply_timezone_offset(naive_datetime, tz_part)
    }

    /// Parses traditional PostgreSQL timestamp format: "Wed Dec 17 07:37:16 1997 PST"
    fn parse_postgresql_format(s: &str) -> Result<DateTime<Utc>, String> {
        // Format: DOW MON DD HH:MM:SS YYYY TZ
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 6 {
            return Err("Invalid PostgreSQL timestamp format".to_string());
        }

        // parts[0] = day of week (ignored)
        let month_str = parts[1];
        let day: u32 = parts[2].parse().map_err(|_| "Invalid day")?;
        let time_str = parts[3];
        let year: i32 = parts[4].parse().map_err(|_| "Invalid year")?;
        let tz_part = parts[5];

        let month = Self::parse_month(month_str)?;

        let time_components: Vec<&str> = time_str.split(':').collect();
        if time_components.len() != 3 {
            return Err("Invalid time component".to_string());
        }

        let hour: u32 = time_components[0].parse().map_err(|_| "Invalid hour")?;
        let minute: u32 = time_components[1].parse().map_err(|_| "Invalid minute")?;
        let second: u32 = time_components[2].parse().map_err(|_| "Invalid second")?;

        let naive_date = NaiveDate::from_ymd_opt(year, month, day).ok_or("Invalid date")?;
        let naive_time = NaiveTime::from_hms_opt(hour, minute, second).ok_or("Invalid time")?;
        let naive_datetime = NaiveDateTime::new(naive_date, naive_time);

        // Apply timezone offset
        Self::apply_timezone_offset(naive_datetime, tz_part)
    }

    /// Parses month name to month number
    fn parse_month(s: &str) -> Result<u32, String> {
        match s {
            "Jan" => Ok(1),
            "Feb" => Ok(2),
            "Mar" => Ok(3),
            "Apr" => Ok(4),
            "May" => Ok(5),
            "Jun" => Ok(6),
            "Jul" => Ok(7),
            "Aug" => Ok(8),
            "Sep" => Ok(9),
            "Oct" => Ok(10),
            "Nov" => Ok(11),
            "Dec" => Ok(12),
            _ => Err(format!("Invalid month: {}", s)),
        }
    }

    /// Applies timezone offset to naive datetime
    fn apply_timezone_offset(naive_dt: NaiveDateTime, tz: &str) -> Result<DateTime<Utc>, String> {
        let offset_hours = match tz {
            "PST" => -8, // Pacific Standard Time
            "PDT" => -7, // Pacific Daylight Time
            "MST" => -7, // Mountain Standard Time
            "MDT" => -6, // Mountain Daylight Time
            "CST" => -6, // Central Standard Time
            "CDT" => -5, // Central Daylight Time
            "EST" => -5, // Eastern Standard Time
            "EDT" => -4, // Eastern Daylight Time
            "UTC" | "GMT" => 0,
            _ => return Err(format!("Unsupported timezone: {}", tz)),
        };

        // Convert to UTC by subtracting the offset
        let utc_dt = naive_dt - chrono::Duration::hours(offset_hours);
        Ok(Utc.from_utc_datetime(&utc_dt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamptz_to_chrono() {
        // ISO 8601 format (what wal2json typically outputs)
        let ts = TimestampTz("2024-01-15T10:30:00+00:00".to_string());
        let result = ts.to_chrono_datetime_utc().unwrap();
        assert_eq!(result.to_rfc3339(), "2024-01-15T10:30:00+00:00");

        // With 'Z' timezone indicator
        let ts2 = TimestampTz("1997-12-17T15:37:16Z".to_string());
        let result2 = ts2.to_chrono_datetime_utc().unwrap();
        assert_eq!(result2.to_rfc3339(), "1997-12-17T15:37:16+00:00");

        // With different timezone offset
        let ts3 = TimestampTz("1997-12-17T07:37:16-08:00".to_string());
        let result3 = ts3.to_chrono_datetime_utc().unwrap();
        // -08:00 (PST) should convert to UTC (15:37:16)
        assert_eq!(result3.to_rfc3339(), "1997-12-17T15:37:16+00:00");
    }

    #[test]
    fn test_timestamptz_traditional_sql_format() {
        // Traditional SQL timestamp: "12/17/1997 07:37:16.00 PST"
        let ts = TimestampTz("12/17/1997 07:37:16.00 PST".to_string());
        let result = ts.to_chrono_datetime_utc();

        if let Ok(dt) = result {
            println!("Parsed SQL format: {} -> {}", ts.0, dt.to_rfc3339());
            // PST is UTC-8, so 07:37:16 PST = 15:37:16 UTC
            assert_eq!(dt.to_rfc3339(), "1997-12-17T15:37:16+00:00");
        } else {
            // Traditional SQL format not supported by default chrono parser
            // Would need custom parsing logic with DateTime::parse_from_str
            panic!("Traditional SQL timestamp format not supported: {}", ts.0);
        }
    }

    #[test]
    fn test_timestamptz_traditional_postgresql_format() {
        // Traditional PostgreSQL timestamp: "Wed Dec 17 07:37:16 1997 PST"
        let ts = TimestampTz("Wed Dec 17 07:37:16 1997 PST".to_string());
        let result = ts.to_chrono_datetime_utc();

        if let Ok(dt) = result {
            println!("Parsed PG format: {} -> {}", ts.0, dt.to_rfc3339());
            // PST is UTC-8, so 07:37:16 PST = 15:37:16 UTC
            assert_eq!(dt.to_rfc3339(), "1997-12-17T15:37:16+00:00");
        } else {
            // Traditional PostgreSQL format not supported by default chrono parser
            // Would need custom parsing logic with DateTime::parse_from_str
            panic!(
                "Traditional PostgreSQL timestamp format not supported: {}",
                ts.0
            );
        }
    }
}
