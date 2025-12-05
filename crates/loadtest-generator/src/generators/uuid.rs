//! UUID value generator.

use rand::Rng;
use sync_core::GeneratedValue;
use uuid::Uuid;

/// Generate a random UUID v4 using the provided RNG.
pub fn generate_uuid_v4<R: Rng>(rng: &mut R) -> GeneratedValue {
    // Generate 16 random bytes
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);

    // Set version (4) and variant (RFC 4122) bits
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // Version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // Variant RFC 4122

    GeneratedValue::Uuid(Uuid::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_generate_uuid_v4() {
        let mut rng = StdRng::seed_from_u64(42);
        let value = generate_uuid_v4(&mut rng);
        assert!(matches!(value, GeneratedValue::Uuid(_)));

        // Ensure uniqueness
        let value2 = generate_uuid_v4(&mut rng);
        assert_ne!(value, value2);
    }

    #[test]
    fn test_uuid_deterministic() {
        let mut rng1 = StdRng::seed_from_u64(42);
        let mut rng2 = StdRng::seed_from_u64(42);

        let value1 = generate_uuid_v4(&mut rng1);
        let value2 = generate_uuid_v4(&mut rng2);

        assert_eq!(value1, value2);
    }

    #[test]
    fn test_uuid_version() {
        let mut rng = StdRng::seed_from_u64(42);
        let value = generate_uuid_v4(&mut rng);

        if let GeneratedValue::Uuid(uuid) = value {
            assert_eq!(uuid.get_version_num(), 4);
        } else {
            panic!("Expected UUID");
        }
    }
}
