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

use rand::RngExt;
use surreal_sync_core::{GeneratorConfig, Type, Value};

/// Trait for generating values.
pub trait ValueGenerator {
    /// Generate a value using the given RNG and row index.
    fn generate<R: RngExt>(&self, rng: &mut R, index: u64) -> Value;
}

/// Generate a value based on the generator configuration.
/// Uses default string type for arrays.
pub fn generate_value<R: RngExt>(
    config: &GeneratorConfig,
    rng: &mut R,
    seed: u64,
    index: u64,
) -> Value {
    generate_value_typed(config, rng, seed, index, None)
}

/// Generate a value based on the generator configuration with optional type hint.
/// The target_type is used for type-aware conversion (e.g., arrays with int elements).
pub fn generate_value_typed<R: RngExt>(
    config: &GeneratorConfig,
    rng: &mut R,
    seed: u64,
    index: u64,
    target_type: Option<&Type>,
) -> Value {
    let raw_value = match config {
        GeneratorConfig::UuidV4 => uuid::generate_uuid_v4(rng),

        GeneratorConfig::Sequential { start } => Value::Int64(start + index as i64),

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

        GeneratorConfig::WeightedBool { true_weight } => Value::Bool(rng.random_bool(*true_weight)),

        GeneratorConfig::OneOf { values } => {
            if values.is_empty() {
                Value::Null
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
                Some(Type::Array { element_type }) => element_type.as_ref(),
                _ => &Type::Text,
            };
            array::generate_sample_array_typed(rng, pool, *min_length, *max_length, element_type)
        }

        GeneratorConfig::Static { value } => static_value::yaml_to_generated_value(value),

        GeneratorConfig::Null => Value::Null,

        GeneratorConfig::DurationRange { min_secs, max_secs } => {
            numeric::generate_duration_range(rng, *min_secs, *max_secs)
        }
    };

    // Convert to strict 1:1 type-value matching if target_type is specified
    convert_to_strict_type(raw_value, target_type)
}

/// Convert a raw Value to the strict 1:1 matching value for the target type.
fn convert_to_strict_type(value: Value, target_type: Option<&Type>) -> Value {
    match (target_type, value) {
        // Keep null as-is
        (_, Value::Null) => Value::Null,

        // String type conversions - strict 1:1 matching
        (Some(Type::Char { length }), Value::Text(s)) => Value::Char {
            value: s,
            length: *length,
        },
        (Some(Type::VarChar { length }), Value::Text(s)) => Value::VarChar {
            value: s,
            length: *length,
        },

        // Integer type conversions - strict 1:1 matching
        (Some(Type::Int8 { width }), Value::Int64(i)) => Value::Int8 {
            value: i as i8,
            width: *width,
        },
        (Some(Type::Int16), Value::Int64(i)) => Value::Int16(i as i16),
        (Some(Type::Int32), Value::Int64(i)) => Value::Int32(i as i32),

        // Float type conversions - strict 1:1 matching
        (Some(Type::Float32), Value::Float64(f)) => Value::Float32(f as f32),

        // Decimal conversion
        (Some(Type::Decimal { precision, scale }), Value::Float64(f)) => Value::Decimal {
            value: f.to_string(),
            precision: *precision,
            scale: *scale,
        },

        // Date/time conversions - strict 1:1 matching
        (Some(Type::Date), Value::LocalDateTime(dt)) => Value::Date(dt),
        (Some(Type::Time), Value::LocalDateTime(dt)) => Value::Time(dt),
        (Some(Type::LocalDateTimeNano), Value::LocalDateTime(dt)) => Value::LocalDateTimeNano(dt),
        (Some(Type::ZonedDateTime), Value::LocalDateTime(dt)) => Value::ZonedDateTime(dt),

        // Binary type conversions
        (Some(Type::Blob), Value::Bytes(b)) => Value::Blob(b),

        // Enum conversion - strict 1:1 matching
        (Some(Type::Enum { values }), Value::Text(s)) => Value::Enum {
            value: s,
            allowed_values: values.clone(),
        },

        // Set conversion - strict 1:1 matching
        (Some(Type::Set { values }), Value::Array { elements, .. }) => {
            let set_elements: Vec<String> = elements
                .into_iter()
                .filter_map(|v| {
                    if let Value::Text(s) = v {
                        Some(s)
                    } else {
                        None
                    }
                })
                .collect();
            Value::Set {
                elements: set_elements,
                allowed_values: values.clone(),
            }
        }

        // No conversion needed or type not specified
        (_, value) => value,
    }
}
