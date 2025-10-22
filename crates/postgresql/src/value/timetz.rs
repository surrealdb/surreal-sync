use chrono::{DateTime, Utc};

/// PostgreSQL TIMETZ value (with timezone)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeTz(pub String);

impl TimeTz {
    /// Converts to chrono DateTime<Utc> (using epoch date 1970-01-01)
    pub fn to_chrono_datetime_utc(&self) -> Result<DateTime<Utc>, chrono::ParseError> {
        // PostgreSQL may return timezone offsets like "+00" instead of "+00:00"
        // We need to normalize this to the full format
        let normalized_time = Self::normalize_timezone_offset(&self.0);

        // Parse as a full timestamp with epoch date
        let datetime_str = format!("1970-01-01 {normalized_time}");
        datetime_str.parse::<DateTime<Utc>>()
    }

    /// Normalizes timezone offset from "+00" to "+00:00" format
    fn normalize_timezone_offset(time_str: &str) -> String {
        // Check if the timezone offset is in short form (e.g., "+00", "-05")
        // and convert to full form (e.g., "+00:00", "-05:00")
        if let Some(plus_pos) = time_str.rfind('+') {
            let (time_part, tz_part) = time_str.split_at(plus_pos);
            if tz_part.len() == 3 {
                // Format is "+00", convert to "+00:00"
                return format!("{time_part}{tz_part}:00");
            }
        } else if let Some(minus_pos) = time_str.rfind('-') {
            let (time_part, tz_part) = time_str.split_at(minus_pos);
            if tz_part.len() == 3 {
                // Format is "-05", convert to "-05:00"
                return format!("{time_part}{tz_part}:00");
            }
        }

        // If it's already in full form or 'Z', return as is
        time_str.to_string()
    }
}

#[cfg(test)]
mod tests {
    // Note: TimeTz tests would go here if needed
    // Currently no tests as the implementation is straightforward
}
