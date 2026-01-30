//! Populate command runner.

use anyhow::Context;
use sync_core::Schema;

use super::mask_connection_password;
use crate::loadtest::populate_verify::post_metrics_to_aggregator;
use crate::PopulateSource;

/// Run populate command to fill source database with deterministic test data
pub async fn run_populate(source: PopulateSource) -> anyhow::Result<()> {
    match source {
        PopulateSource::MySQL { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder to track container start/stop state
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate MySQL with {} rows per table (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!(
                    "[DRY-RUN] Connection: {}",
                    mask_connection_password(&args.mysql_connection_string)
                );
                tracing::info!("[DRY-RUN] Tables: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                // Even in dry-run mode, create and POST metrics to test aggregator integration
                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }

                return Ok(());
            }

            tracing::info!(
                "Populating MySQL with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            // Accumulate metrics from all tables
            let mut total_rows = 0u64;
            let mut total_batch_count = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                //
                // The DataGenerator uses an internal index counter that increments with each row.
                // If we reused the same populator across tables, the second table would start at
                // index N (after generating N rows for the first table), causing sequential IDs
                // to be offset (e.g., table2 would have IDs starting at 1001 instead of 1).
                //
                // This offset causes verification failures because:
                // 1. The verifier looks up records by generated ID (e.g., `SELECT * FROM orders:1`)
                // 2. If populate used offset IDs (orders:1001-2000), the record `orders:1` doesn't exist
                // 3. The verifier reports these as "missing" even though the data exists at different IDs
                //
                // Both populate and verify must use the same per-table generator reset strategy.
                let mut populator = loadtest_populate_mysql::MySQLPopulator::new(
                    &args.mysql_connection_string,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                .context("Failed to connect to MySQL")?
                .with_batch_size(args.common.batch_size);

                // Skip table creation in data-only mode (tables must already exist)
                if !args.common.data_only {
                    if let Err(e) = populator.create_table(table_name).await {
                        let error_msg = format!("Failed to create table '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                        continue;
                    }
                }

                // Skip data insertion in schema-only mode
                if args.common.schema_only {
                    tracing::info!(
                        "Skipping data insertion for '{}' (schema-only mode)",
                        table_name
                    );
                    continue;
                }

                match populator.populate(table_name, args.common.row_count).await {
                    Ok(metrics) => {
                        total_rows += metrics.rows_inserted;
                        total_batch_count += metrics.batch_count;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Populated {}: {} rows in {:?}",
                            table_name,
                            metrics.rows_inserted,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to populate table '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            // Finalize container metrics
            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: total_batch_count,
                    rows_per_second,
                    bytes_written: None, // MySQL doesn't track bytes written
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            // POST to aggregator if URL provided
            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            // Fail if there were any errors
            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::PostgreSQL { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate PostgreSQL with {} rows per table (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!(
                    "[DRY-RUN] Connection: {}",
                    mask_connection_password(&args.postgresql_connection_string)
                );
                tracing::info!("[DRY-RUN] Tables: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }

                return Ok(());
            }

            tracing::info!(
                "Populating PostgreSQL with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut total_rows = 0u64;
            let mut total_batch_count = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator = loadtest_populate_postgresql::PostgreSQLPopulator::new(
                    &args.postgresql_connection_string,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                .context("Failed to connect to PostgreSQL")?
                .with_batch_size(args.common.batch_size);

                // Skip table creation in data-only mode (tables must already exist)
                if !args.common.data_only {
                    if let Err(e) = populator.create_table(table_name).await {
                        let error_msg = format!("Failed to create table '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                        continue;
                    }
                }

                // Skip data insertion in schema-only mode
                if args.common.schema_only {
                    tracing::info!(
                        "Skipping data insertion for '{}' (schema-only mode)",
                        table_name
                    );
                    continue;
                }

                match populator.populate(table_name, args.common.row_count).await {
                    Ok(metrics) => {
                        total_rows += metrics.rows_inserted;
                        total_batch_count += metrics.batch_count;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Populated {}: {} rows in {:?}",
                            table_name,
                            metrics.rows_inserted,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to populate table '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: total_batch_count,
                    rows_per_second,
                    bytes_written: None,
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::MongoDB { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate MongoDB with {} documents per collection (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!(
                    "[DRY-RUN] Connection: {}",
                    mask_connection_password(&args.mongodb_connection_string)
                );
                tracing::info!("[DRY-RUN] Database: {}", args.mongodb_database);
                tracing::info!("[DRY-RUN] Collections: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }

                return Ok(());
            }

            tracing::info!(
                "Populating MongoDB with {} documents per collection (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut total_rows = 0u64;
            let mut total_batch_count = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator = loadtest_populate_mongodb::MongoDBPopulator::new(
                    &args.mongodb_connection_string,
                    &args.mongodb_database,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                .context("Failed to connect to MongoDB")?
                .with_batch_size(args.common.batch_size);

                // Create collection explicitly if not in data-only mode
                if !args.common.data_only {
                    // Drop existing collection first to ensure clean state
                    populator.drop_collection(table_name).await.ok();
                    if let Err(e) = populator.create_collection(table_name).await {
                        let error_msg = format!("Failed to create collection '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                        continue;
                    }
                }

                // Skip document insertion in schema-only mode
                if args.common.schema_only {
                    tracing::info!(
                        "Skipping document insertion for '{}' (schema-only mode)",
                        table_name
                    );
                    continue;
                }

                match populator.populate(table_name, args.common.row_count).await {
                    Ok(metrics) => {
                        total_rows += metrics.rows_inserted;
                        total_batch_count += metrics.batch_count;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Populated {}: {} documents in {:?}",
                            table_name,
                            metrics.rows_inserted,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg =
                            format!("Failed to populate collection '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: total_batch_count,
                    rows_per_second,
                    bytes_written: None,
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::Csv { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would generate CSV files with {} rows per table (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!("[DRY-RUN] Output directory: {:?}", args.output_dir);
                tracing::info!("[DRY-RUN] Tables: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                // POST dry-run metrics to aggregator
                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }
                return Ok(());
            }

            tracing::info!(
                "Generating CSV files with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            if let Err(e) = std::fs::create_dir_all(&args.output_dir) {
                let error_msg = format!(
                    "Failed to create output directory {:?}: {}",
                    args.output_dir, e
                );
                tracing::error!("{}", error_msg);
                let container_metrics =
                    metrics_builder.finish_populate(None, vec![error_msg.clone()], false);
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    }
                }
                anyhow::bail!(error_msg);
            }

            let mut total_rows = 0u64;
            let mut total_bytes = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator =
                    loadtest_populate_csv::CSVPopulator::new(schema.clone(), args.common.seed);

                let output_path = args.output_dir.join(format!("{table_name}.csv"));

                match populator.populate(table_name, &output_path, args.common.row_count) {
                    Ok(metrics) => {
                        total_rows += metrics.rows_written;
                        total_bytes += metrics.file_size_bytes;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Generated {:?}: {} rows in {:?}",
                            output_path,
                            metrics.rows_written,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to generate CSV for '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            // Construct aggregated metrics
            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: 0, // CSV writes all rows at once
                    rows_per_second,
                    bytes_written: Some(total_bytes),
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            // POST metrics to aggregator
            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::Jsonl { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would generate JSONL files with {} rows per table (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!("[DRY-RUN] Output directory: {:?}", args.output_dir);
                tracing::info!("[DRY-RUN] Tables: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                // POST dry-run metrics to aggregator
                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }
                return Ok(());
            }

            tracing::info!(
                "Generating JSONL files with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            if let Err(e) = std::fs::create_dir_all(&args.output_dir) {
                let error_msg = format!(
                    "Failed to create output directory {:?}: {}",
                    args.output_dir, e
                );
                tracing::error!("{}", error_msg);
                let container_metrics =
                    metrics_builder.finish_populate(None, vec![error_msg.clone()], false);
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    }
                }
                anyhow::bail!(error_msg);
            }

            let mut total_rows = 0u64;
            let mut total_bytes = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator =
                    loadtest_populate_jsonl::JsonlPopulator::new(schema.clone(), args.common.seed);

                let output_path = args.output_dir.join(format!("{table_name}.jsonl"));

                match populator.populate(table_name, &output_path, args.common.row_count) {
                    Ok(metrics) => {
                        total_rows += metrics.rows_written;
                        total_bytes += metrics.file_size_bytes;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Generated {:?}: {} rows in {:?}",
                            output_path,
                            metrics.rows_written,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to generate JSONL for '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            // Construct aggregated metrics
            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: 0, // JSONL writes all rows at once
                    rows_per_second,
                    bytes_written: Some(total_bytes),
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            // POST metrics to aggregator
            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::Kafka { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate Kafka topics with {} messages per topic (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!("[DRY-RUN] Brokers: {}", args.kafka_brokers);
                tracing::info!("[DRY-RUN] Topics: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                // POST dry-run metrics to aggregator
                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }
                return Ok(());
            }

            tracing::info!(
                "Populating Kafka topics with {} rows per table (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut total_rows = 0u64;
            let mut total_batch_count = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator = match loadtest_populate_kafka::KafkaPopulator::new(
                    &args.kafka_brokers,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                {
                    Ok(p) => p.with_batch_size(args.common.batch_size),
                    Err(e) => {
                        let error_msg =
                            format!("Failed to create Kafka populator for '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                        continue;
                    }
                };

                // Prepare table (generates .proto file)
                if let Err(e) = populator.prepare_table(table_name) {
                    let error_msg = format!("Failed to prepare proto for '{table_name}': {e}");
                    tracing::error!("{}", error_msg);
                    errors.push(error_msg);
                    continue;
                }

                // Create topic
                if let Err(e) = populator.create_topic(table_name).await {
                    let error_msg = format!("Failed to create topic '{table_name}': {e}");
                    tracing::error!("{}", error_msg);
                    errors.push(error_msg);
                    continue;
                }

                // Populate
                match populator.populate(table_name, args.common.row_count).await {
                    Ok(metrics) => {
                        total_rows += metrics.messages_published;
                        total_batch_count += metrics.batch_count;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Populated '{}': {} messages in {:?} ({:.2} msg/sec)",
                            table_name,
                            metrics.messages_published,
                            metrics.total_duration,
                            metrics.messages_per_second()
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to populate topic '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            // Construct aggregated metrics
            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: total_batch_count,
                    rows_per_second,
                    bytes_written: None, // Kafka doesn't track bytes written
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            // POST metrics to aggregator
            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
        PopulateSource::Neo4j { args } => {
            let schema = Schema::from_file(&args.common.schema)
                .with_context(|| format!("Failed to load schema from {:?}", args.common.schema))?;

            let tables = if args.common.tables.is_empty() {
                schema.table_names()
            } else {
                args.common.tables.iter().map(|s| s.as_str()).collect()
            };

            // Create metrics builder
            let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
            let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
                loadtest_distributed::metrics::Operation::Populate,
                tables_vec.clone(),
            )?;

            if args.common.dry_run {
                tracing::info!(
                    "[DRY-RUN] Would populate Neo4j with {} nodes per label (seed={})",
                    args.common.row_count,
                    args.common.seed
                );
                tracing::info!(
                    "[DRY-RUN] Connection: {}",
                    mask_connection_password(&args.neo4j_connection_string)
                );
                tracing::info!("[DRY-RUN] Database: {}", args.neo4j_database);
                tracing::info!("[DRY-RUN] Labels: {:?}", tables);
                tracing::info!("[DRY-RUN] Schema validated successfully");

                // POST dry-run metrics to aggregator
                let container_metrics = metrics_builder.finish_dry_run();
                if let Some(url) = &args.common.aggregator_url {
                    if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                        tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                    } else {
                        tracing::info!("Successfully posted dry-run metrics to aggregator");
                    }
                }
                return Ok(());
            }

            tracing::info!(
                "Populating Neo4j with {} nodes per label (seed={})",
                args.common.row_count,
                args.common.seed
            );

            let mut total_rows = 0u64;
            let mut total_batch_count = 0u64;
            let mut total_duration = std::time::Duration::ZERO;
            let mut errors = Vec::new();

            for table_name in &tables {
                // Create a fresh populator (and thus a fresh DataGenerator) for each table.
                // See MySQL populator comment above for detailed explanation.
                let mut populator = match loadtest_populate_neo4j::Neo4jPopulator::new(
                    &args.neo4j_connection_string,
                    &args.neo4j_username,
                    &args.neo4j_password,
                    &args.neo4j_database,
                    schema.clone(),
                    args.common.seed,
                )
                .await
                {
                    Ok(p) => p.with_batch_size(args.common.batch_size),
                    Err(e) => {
                        let error_msg =
                            format!("Failed to connect to Neo4j for '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                        continue;
                    }
                };

                // Skip node deletion in data-only mode (append to existing nodes)
                if !args.common.data_only {
                    // Delete existing nodes (ignore errors)
                    populator.delete_nodes(table_name).await.ok();
                }

                // Skip node creation in schema-only mode
                // Note: Neo4j doesn't have a separate schema concept like SQL databases,
                // but we skip node creation to match the behavior of other populators
                if args.common.schema_only {
                    tracing::info!(
                        "Skipping node creation for '{}' (schema-only mode)",
                        table_name
                    );
                    continue;
                }

                // Populate
                match populator.populate(table_name, args.common.row_count).await {
                    Ok(metrics) => {
                        total_rows += metrics.rows_inserted;
                        total_batch_count += metrics.batch_count;
                        total_duration += metrics.total_duration;

                        tracing::info!(
                            "Populated {}: {} nodes in {:?}",
                            table_name,
                            metrics.rows_inserted,
                            metrics.total_duration
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to populate label '{table_name}': {e}");
                        tracing::error!("{}", error_msg);
                        errors.push(error_msg);
                    }
                }
            }

            // Construct aggregated metrics
            let success = errors.is_empty();
            let populate_metrics = if total_rows > 0 {
                let duration_ms = total_duration.as_millis() as u64;
                let rows_per_second = if duration_ms > 0 {
                    (total_rows as f64) / (duration_ms as f64 / 1000.0)
                } else {
                    0.0
                };

                Some(loadtest_distributed::metrics::PopulateMetrics {
                    rows_processed: total_rows,
                    duration_ms,
                    batch_count: total_batch_count,
                    rows_per_second,
                    bytes_written: None, // Neo4j doesn't track bytes written
                })
            } else {
                None
            };

            let container_metrics =
                metrics_builder.finish_populate(populate_metrics, errors.clone(), success);

            // POST metrics to aggregator
            if let Some(url) = &args.common.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                } else {
                    tracing::info!("Successfully posted metrics to aggregator");
                }
            }

            if !success {
                anyhow::bail!("Populate failed with {} error(s)", errors.len());
            }
        }
    }

    tracing::info!("Populate completed successfully");
    Ok(())
}
