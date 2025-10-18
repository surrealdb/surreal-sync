use chrono::{DateTime, Utc};

/// PostgreSQL TIMETZ value (with timezone)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeTz(pub String);

impl TimeTz {
    /// Converts to chrono DateTime<Utc> (using epoch date 1970-01-01)
    pub fn to_chrono_datetime_utc(&self) -> Result<DateTime<Utc>, chrono::ParseError> {
        // Parse as a full timestamp with epoch date
        let datetime_str = format!("1970-01-01 {}", self.0);
        datetime_str.parse::<DateTime<Utc>>()
    }
}

#[cfg(test)]
mod tests {
    // Note: TimeTz tests would go here if needed
    // Currently no tests as the implementation is straightforward
}
