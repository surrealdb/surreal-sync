//! Unified full test dataset for database type testing
//!
//! This module provides a single, test dataset that includes
//! ALL documented data types across all supported database sources. The same
//! logical dataset is used across all source-specific tests to ensure consistency.

use crate::testing::{
    posts::create_posts_table, relations::create_user_post_relation, table::TestDataSet,
    users::create_users_table,
};

/// Create the unified test dataset
///
/// This dataset contains ALL documented data types from all database sources
/// with proper source-specific representations. Each field demonstrates:
/// 1. How the same logical data is represented in each database
/// 2. How database-specific types map to SurrealDB expectations
/// 3. How high-precision and complex types are handled
pub fn create_unified_full_dataset() -> TestDataSet {
    TestDataSet::new()
        .add_table(create_users_table())
        .add_table(create_posts_table())
        .add_relation(create_user_post_relation())
}
