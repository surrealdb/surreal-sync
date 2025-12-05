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
use sync_core::{GeneratedValue, GeneratorConfig};

/// Trait for generating values.
pub trait ValueGenerator {
    /// Generate a value using the given RNG and row index.
    fn generate<R: Rng>(&self, rng: &mut R, index: u64) -> GeneratedValue;
}

/// Generate a value based on the generator configuration.
pub fn generate_value<R: Rng>(config: &GeneratorConfig, rng: &mut R, index: u64) -> GeneratedValue {
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
        } => array::generate_sample_array(rng, pool, *min_length, *max_length),

        GeneratorConfig::Static { value } => static_value::yaml_to_generated_value(value),

        GeneratorConfig::Null => GeneratedValue::Null,
    }
}
