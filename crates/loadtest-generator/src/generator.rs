//! Main data generator for producing test data rows.

use crate::generators::generate_value_typed;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::collections::HashMap;
use sync_core::{Schema, UniversalRow, UniversalValue};

/// Error type for generator operations.
#[derive(Debug, thiserror::Error)]
pub enum GeneratorError {
    /// Table not found in schema
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// Schema error
    #[error("Schema error: {0}")]
    SchemaError(#[from] sync_core::SchemaError),
}

/// Data generator that produces deterministic test data rows.
///
/// The generator uses a seeded random number generator to ensure
/// reproducible results across runs with the same seed and schema.
pub struct DataGenerator {
    /// Schema defining the tables and field generators
    schema: Schema,
    /// Seeded random number generator for reproducibility
    rng: StdRng,
    /// Current row index (for incremental generation)
    index: u64,
}

impl DataGenerator {
    /// Create a new data generator with the given schema and seed.
    pub fn new(schema: Schema, seed: u64) -> Self {
        Self {
            schema,
            rng: StdRng::seed_from_u64(seed),
            index: 0,
        }
    }

    /// Set the starting index for row generation.
    ///
    /// This is useful for incremental generation where you want to
    /// resume from a specific point.
    ///
    /// Note: This also re-seeds the RNG to ensure determinism.
    /// The RNG state for index N is computed by advancing from seed.
    pub fn with_start_index(mut self, index: u64) -> Self {
        self.index = index;
        // Re-seed and advance RNG to the correct state
        self.rng = StdRng::seed_from_u64(self.compute_rng_seed_for_index(index));
        self
    }

    /// Compute the RNG seed for a specific index.
    ///
    /// This allows jumping to any index while maintaining determinism.
    fn compute_rng_seed_for_index(&self, index: u64) -> u64 {
        // Combine the base seed with the index
        let base_seed = self.schema.seed.unwrap_or(0);
        base_seed.wrapping_add(index.wrapping_mul(0x9E3779B97F4A7C15))
    }

    /// Get the current row index.
    pub fn current_index(&self) -> u64 {
        self.index
    }

    /// Generate the next internal row for the given table.
    pub fn next_internal_row(&mut self, table: &str) -> Result<UniversalRow, GeneratorError> {
        // Verify table exists
        let table_schema = self
            .schema
            .get_table(table)
            .ok_or_else(|| GeneratorError::TableNotFound(table.to_string()))?;

        let index = self.index;
        let table_name = table_schema.name.clone();

        // Generate the primary key
        let id = generate_value_typed(
            &table_schema.id.generator,
            &mut self.rng,
            index,
            Some(&table_schema.id.id_type),
        );

        // Generate all fields with type-aware generation
        let fields: HashMap<String, UniversalValue> = table_schema
            .fields
            .iter()
            .map(|field| {
                let value = generate_value_typed(
                    &field.generator,
                    &mut self.rng,
                    index,
                    Some(&field.field_type),
                );
                (field.name.clone(), value)
            })
            .collect();

        self.index += 1;

        Ok(UniversalRow::new(table_name, index, id, fields))
    }

    /// Generate multiple internal rows for the given table.
    ///
    /// Returns an iterator that lazily generates rows.
    pub fn internal_rows(
        &mut self,
        table: &str,
        count: u64,
    ) -> Result<UniversalRowIterator<'_>, GeneratorError> {
        // Verify the table exists
        if self.schema.get_table(table).is_none() {
            return Err(GeneratorError::TableNotFound(table.to_string()));
        }

        Ok(UniversalRowIterator {
            generator: self,
            table: table.to_string(),
            remaining: count,
        })
    }

    /// Get a reference to the schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Iterator that lazily generates internal rows.
pub struct UniversalRowIterator<'a> {
    generator: &'a mut DataGenerator,
    table: String,
    remaining: u64,
}

impl Iterator for UniversalRowIterator<'_> {
    type Item = UniversalRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        self.remaining -= 1;

        // This should not fail since we verified the table exists
        self.generator.next_internal_row(&self.table).ok()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining as usize;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for UniversalRowIterator<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::Schema;

    fn test_schema() -> Schema {
        let yaml = r#"
version: 1
seed: 42

tables:
  - name: users
    id:
      type: uuid
      generator:
        type: uuid_v4

    fields:
      - name: email
        type:
          type: var_char
          length: 255
        generator:
          type: pattern
          pattern: "user_{index}@example.com"

      - name: age
        type: int
        generator:
          type: int_range
          min: 18
          max: 80

      - name: is_active
        type: bool
        generator:
          type: weighted_bool
          true_weight: 0.8
"#;
        Schema::from_yaml(yaml).unwrap()
    }

    #[test]
    fn test_generate_single_row() {
        let schema = test_schema();
        let mut generator = DataGenerator::new(schema, 42);

        let row = generator.next_internal_row("users").unwrap();

        assert_eq!(row.table, "users");
        assert_eq!(row.index, 0);
        assert!(matches!(row.id, UniversalValue::Uuid(_)));
        // Email is VarChar(255), so it should be a VarChar value
        if let Some(UniversalValue::VarChar { value, .. }) = row.get_field("email") {
            assert_eq!(value, "user_0@example.com");
        } else {
            panic!(
                "Expected VarChar for email, got {:?}",
                row.get_field("email")
            );
        }

        // Age should be in range - field type is Int so should be Int value
        if let Some(UniversalValue::Int32(age)) = row.get_field("age") {
            assert!(*age >= 18 && *age <= 80);
        } else {
            panic!("Expected Int for age, got {:?}", row.get_field("age"));
        }
    }

    #[test]
    fn test_deterministic_generation() {
        let schema = test_schema();

        let mut gen1 = DataGenerator::new(schema.clone(), 42);
        let mut gen2 = DataGenerator::new(schema, 42);

        let row1 = gen1.next_internal_row("users").unwrap();
        let row2 = gen2.next_internal_row("users").unwrap();

        assert_eq!(row1.id, row2.id);
        assert_eq!(row1.get_field("email"), row2.get_field("email"));
        assert_eq!(row1.get_field("age"), row2.get_field("age"));
        assert_eq!(row1.get_field("is_active"), row2.get_field("is_active"));
    }

    #[test]
    fn test_generate_multiple_rows() {
        let schema = test_schema();
        let mut generator = DataGenerator::new(schema, 42);

        let rows: Vec<_> = generator.internal_rows("users", 10).unwrap().collect();

        assert_eq!(rows.len(), 10);

        // Verify indices are sequential
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.index, i as u64);
        }

        // Verify emails contain correct indices
        for (i, row) in rows.iter().enumerate() {
            if let Some(UniversalValue::VarChar { value: email, .. }) = row.get_field("email") {
                assert!(email.contains(&format!("user_{i}")));
            }
        }
    }

    #[test]
    fn test_table_not_found() {
        let schema = test_schema();
        let mut generator = DataGenerator::new(schema, 42);

        let result = generator.next_internal_row("nonexistent");
        assert!(matches!(result, Err(GeneratorError::TableNotFound(_))));
    }

    #[test]
    fn test_with_start_index() {
        let schema = test_schema();

        // Generate rows 5-9 directly
        let mut gen1 = DataGenerator::new(schema.clone(), 42).with_start_index(5);
        let row1 = gen1.next_internal_row("users").unwrap();
        assert_eq!(row1.index, 5);

        // Email should use index 5
        if let Some(UniversalValue::VarChar { value: email, .. }) = row1.get_field("email") {
            assert!(email.contains("user_5"));
        } else {
            panic!(
                "Expected VarChar for email, got {:?}",
                row1.get_field("email")
            );
        }
    }

    #[test]
    fn test_current_index() {
        let schema = test_schema();
        let mut generator = DataGenerator::new(schema, 42);

        assert_eq!(generator.current_index(), 0);
        generator.next_internal_row("users").unwrap();
        assert_eq!(generator.current_index(), 1);
        generator.next_internal_row("users").unwrap();
        assert_eq!(generator.current_index(), 2);
    }
}
