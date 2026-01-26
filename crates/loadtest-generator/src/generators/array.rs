//! Array value generators.

use rand::seq::{IndexedRandom, SliceRandom};
use rand::Rng;
use sync_core::{UniversalType, UniversalValue};

/// Convert a string value to the appropriate UniversalValue based on the target type.
fn string_to_typed_value(s: &str, target_type: &UniversalType) -> UniversalValue {
    match target_type {
        UniversalType::Int32 => match s.parse::<i32>() {
            Ok(i) => UniversalValue::Int32(i),
            Err(_) => UniversalValue::Text(s.to_string()),
        },
        UniversalType::Int16 => match s.parse::<i16>() {
            Ok(i) => UniversalValue::Int16(i),
            Err(_) => UniversalValue::Text(s.to_string()),
        },
        UniversalType::Int64 => match s.parse::<i64>() {
            Ok(i) => UniversalValue::Int64(i),
            Err(_) => UniversalValue::Text(s.to_string()),
        },
        UniversalType::Int8 { width: 1 } => {
            // TinyInt(1) is often used as boolean
            match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => UniversalValue::Bool(true),
                "false" | "0" | "no" => UniversalValue::Bool(false),
                _ => match s.parse::<i8>() {
                    Ok(i) => UniversalValue::Int8 { value: i, width: 1 },
                    Err(_) => UniversalValue::Text(s.to_string()),
                },
            }
        }
        UniversalType::Int8 { width } => match s.parse::<i8>() {
            Ok(i) => UniversalValue::Int8 {
                value: i,
                width: *width,
            },
            Err(_) => UniversalValue::Text(s.to_string()),
        },
        UniversalType::Float32 => match s.parse::<f32>() {
            Ok(f) => UniversalValue::Float32(f),
            Err(_) => UniversalValue::Text(s.to_string()),
        },
        UniversalType::Float64 => match s.parse::<f64>() {
            Ok(f) => UniversalValue::Float64(f),
            Err(_) => UniversalValue::Text(s.to_string()),
        },
        UniversalType::Decimal { precision, scale } => match s.parse::<f64>() {
            Ok(_) => UniversalValue::Decimal {
                value: s.to_string(),
                precision: *precision,
                scale: *scale,
            },
            Err(_) => UniversalValue::Text(s.to_string()),
        },
        UniversalType::Bool => match s.to_lowercase().as_str() {
            "true" | "1" | "yes" => UniversalValue::Bool(true),
            "false" | "0" | "no" => UniversalValue::Bool(false),
            _ => UniversalValue::Text(s.to_string()),
        },
        // For text types and all others, keep as string
        _ => UniversalValue::Text(s.to_string()),
    }
}

/// Generate an array by sampling from a pool of values.
pub fn generate_sample_array<R: Rng>(
    rng: &mut R,
    pool: &[String],
    min_length: usize,
    max_length: usize,
) -> UniversalValue {
    // Default to string element type
    generate_sample_array_typed(rng, pool, min_length, max_length, &UniversalType::Text)
}

/// Generate an array by sampling from a pool of values with type-aware conversion.
pub fn generate_sample_array_typed<R: Rng>(
    rng: &mut R,
    pool: &[String],
    min_length: usize,
    max_length: usize,
    element_type: &UniversalType,
) -> UniversalValue {
    if pool.is_empty() || max_length == 0 {
        return UniversalValue::Array {
            elements: vec![],
            element_type: Box::new(element_type.clone()),
        };
    }

    let length = rng.random_range(min_length..=max_length);

    // Randomly select `length` items from the pool (with potential duplicates)
    let items: Vec<UniversalValue> = (0..length)
        .map(|_| {
            let item = pool.choose(rng).unwrap();
            string_to_typed_value(item, element_type)
        })
        .collect();

    UniversalValue::Array {
        elements: items,
        element_type: Box::new(element_type.clone()),
    }
}

/// Generate an array by sampling unique items from a pool.
pub fn generate_unique_sample_array<R: Rng>(
    rng: &mut R,
    pool: &[String],
    min_length: usize,
    max_length: usize,
) -> UniversalValue {
    if pool.is_empty() || max_length == 0 {
        return UniversalValue::Array {
            elements: vec![],
            element_type: Box::new(UniversalType::Text),
        };
    }

    // Clamp max_length to pool size for unique sampling
    let effective_max = max_length.min(pool.len());
    let effective_min = min_length.min(effective_max);

    let length = rng.random_range(effective_min..=effective_max);

    // Shuffle and take first `length` items
    let mut shuffled = pool.to_vec();
    shuffled.shuffle(rng);

    let items: Vec<UniversalValue> = shuffled
        .into_iter()
        .take(length)
        .map(UniversalValue::Text)
        .collect();

    UniversalValue::Array {
        elements: items,
        element_type: Box::new(UniversalType::Text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_generate_sample_array() {
        let mut rng = StdRng::seed_from_u64(42);
        let pool = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        for _ in 0..10 {
            let value = generate_sample_array(&mut rng, &pool, 1, 3);
            if let UniversalValue::Array { elements, .. } = value {
                assert!(!elements.is_empty());
                assert!(elements.len() <= 3);
            } else {
                panic!("Expected Array value");
            }
        }
    }

    #[test]
    fn test_generate_sample_array_empty_pool() {
        let mut rng = StdRng::seed_from_u64(42);
        let pool: Vec<String> = vec![];

        let value = generate_sample_array(&mut rng, &pool, 0, 3);
        assert!(matches!(
            value,
            UniversalValue::Array {
                elements,
                ..
            } if elements.is_empty()
        ));
    }

    #[test]
    fn test_generate_unique_sample_array() {
        let mut rng = StdRng::seed_from_u64(42);
        let pool = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        let value = generate_unique_sample_array(&mut rng, &pool, 3, 3);
        if let UniversalValue::Array { elements, .. } = value {
            assert_eq!(elements.len(), 3);
            // Check uniqueness
            let strings: Vec<&String> = elements
                .iter()
                .filter_map(|v| {
                    if let UniversalValue::Text(s) = v {
                        Some(s)
                    } else {
                        None
                    }
                })
                .collect();
            let mut sorted = strings.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(strings.len(), sorted.len());
        } else {
            panic!("Expected Array value");
        }
    }

    #[test]
    fn test_deterministic_generation() {
        let pool = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        let mut rng1 = StdRng::seed_from_u64(42);
        let mut rng2 = StdRng::seed_from_u64(42);

        let value1 = generate_sample_array(&mut rng1, &pool, 1, 3);
        let value2 = generate_sample_array(&mut rng2, &pool, 1, 3);

        assert_eq!(value1, value2);
    }
}
