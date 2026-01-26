//! Pattern-based string generator.
//!
//! Supports placeholders:
//! - `{index}` - row index
//! - `{uuid}` - random UUID
//! - `{rand:N}` - random N-digit number

use rand::Rng;
use sync_core::UniversalValue;
use uuid::Uuid;

/// Generate a string based on a pattern with placeholders.
pub fn generate_pattern<R: Rng>(pattern: &str, rng: &mut R, index: u64) -> UniversalValue {
    let mut result = pattern.to_string();

    // Replace {index}
    result = result.replace("{index}", &index.to_string());

    // Replace {uuid}
    while result.contains("{uuid}") {
        result = result.replacen("{uuid}", &Uuid::new_v4().to_string(), 1);
    }

    // Replace {rand:N} patterns
    while let Some(start) = result.find("{rand:") {
        if let Some(end) = result[start..].find('}') {
            let end = start + end;
            let digits_str = &result[start + 6..end];
            if let Ok(digits) = digits_str.parse::<usize>() {
                let random_num = generate_random_digits(rng, digits);
                result = format!("{}{}{}", &result[..start], random_num, &result[end + 1..]);
            } else {
                // Invalid format, skip this one
                break;
            }
        } else {
            break;
        }
    }

    UniversalValue::Text(result)
}

/// Generate a random number with exactly N digits.
fn generate_random_digits<R: Rng>(rng: &mut R, digits: usize) -> String {
    if digits == 0 {
        return String::new();
    }

    let mut result = String::with_capacity(digits);

    // First digit should be 1-9 to avoid leading zeros
    result.push(char::from_digit(rng.random_range(1..10), 10).unwrap());

    // Remaining digits can be 0-9
    for _ in 1..digits {
        result.push(char::from_digit(rng.random_range(0..10), 10).unwrap());
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_generate_pattern_index() {
        let mut rng = StdRng::seed_from_u64(42);
        let value = generate_pattern("user_{index}@example.com", &mut rng, 123);

        assert_eq!(
            value,
            UniversalValue::Text("user_123@example.com".to_string())
        );
    }

    #[test]
    fn test_generate_pattern_uuid() {
        let mut rng = StdRng::seed_from_u64(42);
        let value = generate_pattern("id-{uuid}", &mut rng, 0);

        if let UniversalValue::Text(s) = value {
            assert!(s.starts_with("id-"));
            assert_eq!(s.len(), 3 + 36); // "id-" + UUID
        } else {
            panic!("Expected Text value");
        }
    }

    #[test]
    fn test_generate_pattern_random_digits() {
        let mut rng = StdRng::seed_from_u64(42);
        let value = generate_pattern("code-{rand:6}", &mut rng, 0);

        if let UniversalValue::Text(s) = value {
            assert!(s.starts_with("code-"));
            assert_eq!(s.len(), 5 + 6); // "code-" + 6 digits
                                        // Check that the random part is all digits
            let random_part = &s[5..];
            assert!(random_part.chars().all(|c| c.is_ascii_digit()));
        } else {
            panic!("Expected Text value");
        }
    }

    #[test]
    fn test_generate_pattern_multiple_placeholders() {
        let mut rng = StdRng::seed_from_u64(42);
        let value = generate_pattern("user_{index}_code_{rand:4}", &mut rng, 42);

        if let UniversalValue::Text(s) = value {
            assert!(s.starts_with("user_42_code_"));
            // Total length: "user_42_code_" (13) + 4 digits
            assert_eq!(s.len(), 13 + 4);
        } else {
            panic!("Expected Text value");
        }
    }
}
