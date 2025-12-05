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
use sync_core::{GeneratedValue, GeneratorConfig, SyncDataType};

/// Trait for generating values.
pub trait ValueGenerator {
    /// Generate a value using the given RNG and row index.
    fn generate<R: Rng>(&self, rng: &mut R, index: u64) -> GeneratedValue;
}

/// Generate a value based on the generator configuration.
/// Uses default string type for arrays.
pub fn generate_value<R: Rng>(config: &GeneratorConfig, rng: &mut R, index: u64) -> GeneratedValue {
    generate_value_typed(config, rng, index, None)
}

/// Generate a value based on the generator configuration with optional type hint.
/// The target_type is used for type-aware conversion (e.g., arrays with int elements).
pub fn generate_value_typed<R: Rng>(
    config: &GeneratorConfig,
    rng: &mut R,
    index: u64,
    target_type: Option<&SyncDataType>,
) -> GeneratedValue {
    match config {
        GeneratorConfig::UuidV4 => uuid::generate_uuid_v4(rng),

        GeneratorConfig::Sequential { start } => GeneratedValue::Int64(start + index as i64),

        GeneratorConfig::Pattern { pattern } => pattern::generate_pattern(pattern, rng, index),

        GeneratorConfig::IntRange { min, max } => numeric::generate_int_range(rng, *min, *max),

        GeneratorConfig::FloatRange { min, max } => numeric::generate_float_range(rng, *min, *max),

        GeneratorConfig::DecimalRange { min, max } => {
            numeric::generate_decimal_range(rng, *min, *max)
        }

        GeneratorConfig::TimestampRange { start, end } => {
            timestamp::generate_timestamp_range(rng, start, end)
        }

        GeneratorConfig::WeightedBool { true_weight } => {
            GeneratedValue::Bool(rng.gen_bool(*true_weight))
        }

        GeneratorConfig::OneOf { values } => {
            if values.is_empty() {
                GeneratedValue::Null
            } else {
                let idx = rng.gen_range(0..values.len());
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
                Some(SyncDataType::Array { element_type }) => element_type.as_ref(),
                _ => &SyncDataType::Text,
            };
            array::generate_sample_array_typed(rng, pool, *min_length, *max_length, element_type)
        }

        GeneratorConfig::Static { value } => static_value::yaml_to_generated_value(value),

        GeneratorConfig::Null => GeneratedValue::Null,
    }
}
