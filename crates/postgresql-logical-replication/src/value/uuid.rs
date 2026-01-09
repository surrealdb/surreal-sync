/// PostgreSQL UUID value
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uuid(pub String);

impl Uuid {
    /// Converts to uuid::Uuid
    pub fn to_uuid(&self) -> Result<uuid::Uuid, uuid::Error> {
        uuid::Uuid::parse_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_to_uuid() {
        let uuid_value = Uuid("550e8400-e29b-41d4-a716-446655440000".to_string());
        let result = uuid_value.to_uuid().unwrap();
        assert_eq!(result.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }
}
