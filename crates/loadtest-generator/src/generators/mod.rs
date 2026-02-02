//! Individual value generators for different data types.
//!
//! This module provides the generation logic for each type of value
//! based on the generator configuration from the schema.

pub mod array;
pub mod numeric;
pub mod pattern;
pub mod static_value;
pub mod timestamp;
pub mod uuid;

use rand::Rng;
use sync_core::{GeneratorConfig, UniversalType, UniversalValue};

/// Trait for generating values.
pub trait ValueGenerator {
    /// Generate a value using the given RNG and row index.
    fn generate<R: Rng>(&self, rng: &mut R, index: u64) -> UniversalValue;
}

/// Generate a value based on the generator configuration.
/// Uses default string type for arrays.
pub fn generate_value<R: Rng + Clone>(
    config: &GeneratorConfig,
    rng: &mut R,
    seed: u64,
    index: u64,
) -> UniversalValue {
    generate_value_typed(config, rng, seed, index, None)
}

/// Generate a value based on the generator configuration with optional type hint.
/// The target_type is used for type-aware conversion (e.g., arrays with int elements).
pub fn generate_value_typed<R: Rng + Clone>(
    config: &GeneratorConfig,
    rng: &mut R,
    seed: u64,
    index: u64,
    target_type: Option<&UniversalType>,
) -> UniversalValue {
    let raw_value = match config {
        GeneratorConfig::UuidV4 => uuid::generate_uuid_v4(rng),

        GeneratorConfig::Sequential { start } => UniversalValue::Int64(start + index as i64),

        GeneratorConfig::Pattern { pattern } => pattern::generate_pattern(pattern, rng, index),

        GeneratorConfig::IntRange { min, max } => numeric::generate_int_range(rng, *min, *max),

        GeneratorConfig::FloatRange { min, max } => numeric::generate_float_range(rng, *min, *max),

        GeneratorConfig::DecimalRange { min, max } => {
            numeric::generate_decimal_range(rng, *min, *max)
        }

        GeneratorConfig::TimestampRange { start, end } => {
            timestamp::generate_timestamp_range(rng, seed, start, end, index)
        }

        GeneratorConfig::TimestampNow => timestamp::generate_timestamp_now(rng, seed, index),

        GeneratorConfig::WeightedBool { true_weight } => {
            UniversalValue::Bool(rng.random_bool(*true_weight))
        }

        GeneratorConfig::OneOf { values } => {
            if values.is_empty() {
                UniversalValue::Null
            } else {
                let idx = rng.random_range(0..values.len());
                static_value::yaml_to_generated_value(&values[idx])
            }
        }

        GeneratorConfig::SampleArray {
            pool,
            min_length,
            max_length,
        } => {
            // Extract element type from target type if available
            let element_type = match target_type {
                Some(UniversalType::Array { element_type }) => element_type.as_ref(),
                _ => &UniversalType::Text,
            };
            array::generate_sample_array_typed(rng, pool, *min_length, *max_length, element_type)
        }

        GeneratorConfig::Static { value } => static_value::yaml_to_generated_value(value),

        GeneratorConfig::Null => UniversalValue::Null,

        GeneratorConfig::DurationRange { min_secs, max_secs } => {
            numeric::generate_duration_range(rng, *min_secs, *max_secs)
        }
    };

    // Convert to strict 1:1 type-value matching if target_type is specified
    convert_to_strict_type(raw_value, target_type)
}

/// Convert a raw UniversalValue to the strict 1:1 matching value for the target type.
fn convert_to_strict_type(
    value: UniversalValue,
    target_type: Option<&UniversalType>,
) -> UniversalValue {
    match (target_type, value) {
        // Keep null as-is
        (_, UniversalValue::Null) => UniversalValue::Null,

        // String type conversions - strict 1:1 matching
        (Some(UniversalType::Char { length }), UniversalValue::Text(s)) => UniversalValue::Char {
            value: s,
            length: *length,
        },
        (Some(UniversalType::VarChar { length }), UniversalValue::Text(s)) => {
            UniversalValue::VarChar {
                value: s,
                length: *length,
            }
        }

        // Integer type conversions - strict 1:1 matching
        (Some(UniversalType::Int8 { width }), UniversalValue::Int64(i)) => UniversalValue::Int8 {
            value: i as i8,
            width: *width,
        },
        (Some(UniversalType::Int16), UniversalValue::Int64(i)) => UniversalValue::Int16(i as i16),
        (Some(UniversalType::Int32), UniversalValue::Int64(i)) => UniversalValue::Int32(i as i32),

        // Float type conversions - strict 1:1 matching
        (Some(UniversalType::Float32), UniversalValue::Float64(f)) => {
            UniversalValue::Float32(f as f32)
        }

        // Decimal conversion
        (Some(UniversalType::Decimal { precision, scale }), UniversalValue::Float64(f)) => {
            UniversalValue::Decimal {
                value: f.to_string(),
                precision: *precision,
                scale: *scale,
            }
        }

        // Date/time conversions - strict 1:1 matching
        (Some(UniversalType::Date), UniversalValue::LocalDateTime(dt)) => UniversalValue::Date(dt),
        (Some(UniversalType::Time), UniversalValue::LocalDateTime(dt)) => UniversalValue::Time(dt),
        (Some(UniversalType::LocalDateTimeNano), UniversalValue::LocalDateTime(dt)) => {
            UniversalValue::LocalDateTimeNano(dt)
        }
        (Some(UniversalType::ZonedDateTime), UniversalValue::LocalDateTime(dt)) => {
            UniversalValue::ZonedDateTime(dt)
        }

        // Binary type conversions
        (Some(UniversalType::Blob), UniversalValue::Bytes(b)) => UniversalValue::Blob(b),

        // Enum conversion - strict 1:1 matching
        (Some(UniversalType::Enum { values }), UniversalValue::Text(s)) => UniversalValue::Enum {
            value: s,
            allowed_values: values.clone(),
        },

        // Set conversion - strict 1:1 matching
        (Some(UniversalType::Set { values }), UniversalValue::Array { elements, .. }) => {
            let set_elements: Vec<String> = elements
                .into_iter()
                .filter_map(|v| {
                    if let UniversalValue::Text(s) = v {
                        Some(s)
                    } else {
                        None
                    }
                })
                .collect();
            UniversalValue::Set {
                elements: set_elements,
                allowed_values: values.clone(),
            }
        }

        // No conversion needed or type not specified
        (_, value) => value,
    }
}
