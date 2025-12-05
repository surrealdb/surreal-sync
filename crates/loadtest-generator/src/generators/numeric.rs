//! Numeric value generators.

use rand::Rng;
use sync_core::GeneratedValue;

/// Generate a random integer in the given range (inclusive).
pub fn generate_int_range<R: Rng>(rng: &mut R, min: i64, max: i64) -> GeneratedValue {
    GeneratedValue::Int64(rng.gen_range(min..=max))
}

/// Generate a random float in the given range (inclusive).
pub fn generate_float_range<R: Rng>(rng: &mut R, min: f64, max: f64) -> GeneratedValue {
    GeneratedValue::Float64(rng.gen_range(min..=max))
}

/// Generate a random decimal in the given range.
///
/// The decimal is stored as a string with 2 decimal places.
pub fn generate_decimal_range<R: Rng>(rng: &mut R, min: f64, max: f64) -> GeneratedValue {
    let value = rng.gen_range(min..=max);
    // Format with 2 decimal places by default
    GeneratedValue::Decimal {
        value: format!("{value:.2}"),
        precision: 10,
        scale: 2,
    }
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
            if let GeneratedValue::Int64(v) = value {
                assert!((10..=20).contains(&v));
            } else {
                panic!("Expected Int64 value");
            }
        }
    }

    #[test]
    fn test_generate_float_range() {
        let mut rng = StdRng::seed_from_u64(42);

        for _ in 0..100 {
            let value = generate_float_range(&mut rng, 0.0, 100.0);
            if let GeneratedValue::Float64(v) = value {
                assert!((0.0..=100.0).contains(&v));
            } else {
                panic!("Expected Float64 value");
            }
        }
    }

    #[test]
    fn test_generate_decimal_range() {
        let mut rng = StdRng::seed_from_u64(42);

        let value = generate_decimal_range(&mut rng, 0.0, 100.0);
        if let GeneratedValue::Decimal {
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
}
