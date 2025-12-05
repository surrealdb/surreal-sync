//! Load test pipeline orchestration.

use crate::config::LoadTestConfig;
use crate::error::LoadTestError;
use crate::metrics::{StageMetrics, TableMetrics};
use crate::report::{ConfigSummary, LoadTestReport, TestStatus};
use crate::source::SourceType;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{error, info, warn};

/// Load test pipeline that orchestrates population, sync, and verification.
pub struct LoadTestPipeline {
    config: LoadTestConfig,
}

impl LoadTestPipeline {
    /// Create a new load test pipeline.
    pub fn new(config: LoadTestConfig) -> Self {
        Self { config }
    }

    /// Run the complete load test pipeline.
    ///
    /// This method:
    /// 1. Populates the source database with test data
    /// 2. Returns the report (sync must be run externally with surreal-sync CLI)
    /// 3. Verification can be run separately after sync completes
    pub async fn run_population_only(&self) -> Result<LoadTestReport, LoadTestError> {
        let start_time = Instant::now();

        let config_summary = self.create_config_summary();
        let mut report = LoadTestReport::new(config_summary);
        report.status = TestStatus::Running;

        info!(
            "Starting load test population: {} tables, {} rows each",
            self.config.tables_to_test().len(),
            self.config.row_count
        );

        // Stage 1: Population
        let population_result = self.run_population().await;
        match population_result {
            Ok((metrics, table_metrics)) => {
                report.pipeline_metrics.population = metrics;
                report.table_metrics = table_metrics;
                info!(
                    "Population complete: {} rows in {:?}",
                    report.pipeline_metrics.population.rows_processed,
                    report.pipeline_metrics.population.duration
                );
            }
            Err(e) => {
                error!("Population failed: {}", e);
                report.status = TestStatus::Error;
                report.errors.push(format!("Population error: {e}"));
                report.pipeline_metrics.total_duration = start_time.elapsed();
                return Ok(report);
            }
        }

        report.status = TestStatus::Passed;
        report.pipeline_metrics.total_duration = start_time.elapsed();

        info!(
            "Load test population completed in {:?}",
            report.pipeline_metrics.total_duration
        );

        Ok(report)
    }

    /// Run population stage.
    async fn run_population(&self) -> Result<(StageMetrics, Vec<TableMetrics>), LoadTestError> {
        let start_time = Instant::now();
        let mut metrics = StageMetrics::default();
        let mut table_metrics = Vec::new();

        let tables = self.config.tables_to_test();

        match &self.config.source.source_type {
            SourceType::MySQL { connection_string } => {
                let mut populator = loadtest_populate_mysql::MySQLPopulator::new(
                    connection_string,
                    self.config.schema.clone(),
                    self.config.seed,
                )
                .await?
                .with_batch_size(self.config.batch_size);

                for table_name in &tables {
                    let table_start = Instant::now();
                    let pop_metrics = populator
                        .populate(table_name, self.config.row_count)
                        .await?;

                    let mut tm = TableMetrics::new(table_name);
                    tm.rows_populated = pop_metrics.rows_inserted;
                    tm.population_duration = table_start.elapsed();
                    table_metrics.push(tm);

                    metrics.rows_processed += pop_metrics.rows_inserted;
                }
            }

            SourceType::PostgreSQL { connection_string } => {
                let mut populator = loadtest_populate_postgresql::PostgreSQLPopulator::new(
                    connection_string,
                    self.config.schema.clone(),
                    self.config.seed,
                )
                .await?
                .with_batch_size(self.config.batch_size);

                for table_name in &tables {
                    let table_start = Instant::now();
                    let pop_metrics = populator
                        .populate(table_name, self.config.row_count)
                        .await?;

                    let mut tm = TableMetrics::new(table_name);
                    tm.rows_populated = pop_metrics.rows_inserted;
                    tm.population_duration = table_start.elapsed();
                    table_metrics.push(tm);

                    metrics.rows_processed += pop_metrics.rows_inserted;
                }
            }

            SourceType::MongoDB {
                connection_string,
                database,
            } => {
                let mut populator = loadtest_populate_mongodb::MongoDBPopulator::new(
                    connection_string,
                    database,
                    self.config.schema.clone(),
                    self.config.seed,
                )
                .await?
                .with_batch_size(self.config.batch_size);

                for table_name in &tables {
                    let table_start = Instant::now();
                    let pop_metrics = populator
                        .populate(table_name, self.config.row_count)
                        .await?;

                    let mut tm = TableMetrics::new(table_name);
                    tm.rows_populated = pop_metrics.rows_inserted;
                    tm.population_duration = table_start.elapsed();
                    table_metrics.push(tm);

                    metrics.rows_processed += pop_metrics.rows_inserted;
                }
            }

            SourceType::Csv { output_dir } => {
                std::fs::create_dir_all(output_dir)?;
                let mut populator = loadtest_populate_csv::CSVPopulator::new(
                    self.config.schema.clone(),
                    self.config.seed,
                );

                for table_name in &tables {
                    let table_start = Instant::now();
                    let output_path = PathBuf::from(output_dir).join(format!("{table_name}.csv"));
                    let pop_metrics =
                        populator.populate(table_name, &output_path, self.config.row_count)?;

                    let mut tm = TableMetrics::new(table_name);
                    tm.rows_populated = pop_metrics.rows_written;
                    tm.population_duration = table_start.elapsed();
                    table_metrics.push(tm);

                    metrics.rows_processed += pop_metrics.rows_written;
                    metrics.bytes_processed += pop_metrics.file_size_bytes;
                }
            }

            SourceType::Jsonl { output_dir } => {
                std::fs::create_dir_all(output_dir)?;
                let mut populator = loadtest_populate_jsonl::JsonlPopulator::new(
                    self.config.schema.clone(),
                    self.config.seed,
                );

                for table_name in &tables {
                    let table_start = Instant::now();
                    let output_path = PathBuf::from(output_dir).join(format!("{table_name}.jsonl"));
                    let pop_metrics =
                        populator.populate(table_name, &output_path, self.config.row_count)?;

                    let mut tm = TableMetrics::new(table_name);
                    tm.rows_populated = pop_metrics.rows_written;
                    tm.population_duration = table_start.elapsed();
                    table_metrics.push(tm);

                    metrics.rows_processed += pop_metrics.rows_written;
                    metrics.bytes_processed += pop_metrics.file_size_bytes;
                }
            }
        }

        metrics.duration = start_time.elapsed();
        Ok((metrics, table_metrics))
    }

    /// Run verification stage against SurrealDB.
    pub async fn run_verification(
        &self,
        surreal: surrealdb::Surreal<surrealdb::engine::any::Any>,
    ) -> Result<LoadTestReport, LoadTestError> {
        let start_time = Instant::now();

        let config_summary = self.create_config_summary();
        let mut report = LoadTestReport::new(config_summary);
        report.status = TestStatus::Running;

        info!(
            "Starting verification: {} tables, {} rows each",
            self.config.tables_to_test().len(),
            self.config.row_count
        );

        let tables = self.config.tables_to_test();
        let mut all_passed = true;

        for table_name in &tables {
            let table_start = Instant::now();

            let mut verifier = loadtest_verify::StreamingVerifier::new(
                surreal.clone(),
                self.config.schema.clone(),
                self.config.seed,
                table_name,
            )?;

            let verify_report = verifier.verify_streaming(self.config.row_count).await?;

            let mut tm = TableMetrics::new(table_name);
            tm.rows_verified = verify_report.expected;
            tm.rows_matched = verify_report.matched;
            tm.rows_mismatched = verify_report.mismatched;
            tm.rows_missing = verify_report.missing;
            tm.verification_duration = table_start.elapsed();

            if !verify_report.is_success() {
                all_passed = false;
                warn!(
                    "Table '{}' verification failed: {} missing, {} mismatched",
                    table_name, verify_report.missing, verify_report.mismatched
                );
            }

            report.table_metrics.push(tm);
            report.pipeline_metrics.verification.rows_processed += verify_report.expected;
        }

        report.pipeline_metrics.verification.duration = start_time.elapsed();
        report.pipeline_metrics.total_duration = start_time.elapsed();

        report.status = if all_passed {
            TestStatus::Passed
        } else {
            TestStatus::Failed
        };

        info!(
            "Verification completed in {:?}: {}",
            report.pipeline_metrics.total_duration,
            if all_passed { "PASSED" } else { "FAILED" }
        );

        Ok(report)
    }

    /// Create config summary for reporting.
    fn create_config_summary(&self) -> ConfigSummary {
        ConfigSummary {
            source_type: self.config.source.source_type.name().to_string(),
            seed: self.config.seed,
            row_count: self.config.row_count,
            tables: self.config.tables_to_test(),
            verify: self.config.verify,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::SyncSchema;

    fn test_schema() -> SyncSchema {
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
"#;
        SyncSchema::from_yaml(yaml).unwrap()
    }

    #[test]
    fn test_pipeline_creation() {
        let schema = test_schema();
        let config = LoadTestConfig::new(schema, 42)
            .with_source(SourceType::csv("/tmp/test"))
            .with_row_count(100);

        let pipeline = LoadTestPipeline::new(config);
        let summary = pipeline.create_config_summary();

        assert_eq!(summary.source_type, "csv");
        assert_eq!(summary.seed, 42);
        assert_eq!(summary.row_count, 100);
    }
}
