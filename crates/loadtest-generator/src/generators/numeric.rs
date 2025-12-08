//! Numeric value generators.

use rand::Rng;
use sync_core::UniversalValue;

/// Generate a random integer in the given range (inclusive).
pub fn generate_int_range<R: Rng>(rng: &mut R, min: i64, max: i64) -> UniversalValue {
    UniversalValue::BigInt(rng.gen_range(min..=max))
}

/// Generate a random float in the given range (inclusive).
pub fn generate_float_range<R: Rng>(rng: &mut R, min: f64, max: f64) -> UniversalValue {
    UniversalValue::Double(rng.gen_range(min..=max))
}

/// Generate a random decimal in the given range.
///
/// The decimal is stored as a string with 2 decimal places.
pub fn generate_decimal_range<R: Rng>(rng: &mut R, min: f64, max: f64) -> UniversalValue {
    let value = rng.gen_range(min..=max);
    // Format with 2 decimal places by default
    UniversalValue::Decimal {
        value: format!("{value:.2}"),
        precision: 10,
        scale: 2,
    }
}

/// Generate a random duration in the given range (in seconds).
pub fn generate_duration_range<R: Rng>(
    rng: &mut R,
    min_secs: u64,
    max_secs: u64,
) -> UniversalValue {
    let secs = rng.gen_range(min_secs..=max_secs);
    UniversalValue::Duration(std::time::Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_generate_int_range() {
        let mut rng = StdRng::seed_from_u64(42);

        for _ in 0..100 {
            let value = generate_int_range(&mut rng, 10, 20);
            if let UniversalValue::BigInt(v) = value {
                assert!((10..=20).contains(&v));
            } else {
                panic!("Expected BigInt value");
            }
        }
    }

    #[test]
    fn test_generate_float_range() {
        let mut rng = StdRng::seed_from_u64(42);

        for _ in 0..100 {
            let value = generate_float_range(&mut rng, 0.0, 100.0);
            if let UniversalValue::Double(v) = value {
                assert!((0.0..=100.0).contains(&v));
            } else {
                panic!("Expected Double value");
            }
        }
    }

    #[test]
    fn test_generate_decimal_range() {
        let mut rng = StdRng::seed_from_u64(42);

        let value = generate_decimal_range(&mut rng, 0.0, 100.0);
        if let UniversalValue::Decimal {
            value,
            precision,
            scale,
        } = value
        {
            assert_eq!(precision, 10);
            assert_eq!(scale, 2);
            // Check the value is a valid decimal string
            let parsed: f64 = value.parse().unwrap();
            assert!((0.0..=100.0).contains(&parsed));
        } else {
            panic!("Expected Decimal value");
        }
    }

    #[test]
    fn test_generate_duration_range() {
        let mut rng = StdRng::seed_from_u64(42);

        for _ in 0..100 {
            let value = generate_duration_range(&mut rng, 60, 3600);
            if let UniversalValue::Duration(d) = value {
                assert!(d.as_secs() >= 60 && d.as_secs() <= 3600);
            } else {
                panic!("Expected Duration value");
            }
        }
    }
}
