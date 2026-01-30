//! SurrealDB v3 SDK verification handler.

use anyhow::Context;
use loadtest_verify_surreal2::VerifyArgs;
use sync_core::Schema;

use crate::loadtest::populate_verify::post_metrics_to_aggregator;

/// Run verify command using SurrealDB SDK v3.
pub async fn run_verify_v3(args: VerifyArgs) -> anyhow::Result<()> {
    let schema = Schema::from_file(&args.schema)
        .with_context(|| format!("Failed to load schema from {:?}", args.schema))?;

    let tables = if args.tables.is_empty() {
        schema.table_names()
    } else {
        args.tables.iter().map(|s| s.as_str()).collect()
    };

    // Create metrics builder
    let tables_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
    let metrics_builder = loadtest_distributed::metrics::ContainerMetricsBuilder::start(
        loadtest_distributed::metrics::Operation::Verify,
        tables_vec.clone(),
    )?;

    if args.dry_run {
        tracing::info!(
            "[DRY-RUN] Would verify {} rows per table in SurrealDB v3 (seed={})",
            args.row_count,
            args.seed
        );
        tracing::info!("[DRY-RUN] Endpoint: {}", args.surreal_endpoint);
        tracing::info!(
            "[DRY-RUN] Namespace/Database: {}/{}",
            args.surreal_namespace,
            args.surreal_database
        );
        tracing::info!("[DRY-RUN] Tables: {:?}", tables);
        tracing::info!("[DRY-RUN] Schema validated successfully");

        // POST dry-run metrics to aggregator
        let container_metrics = metrics_builder.finish_dry_run();
        if let Some(url) = &args.aggregator_url {
            if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                tracing::warn!("Failed to POST metrics to aggregator: {}", e);
            } else {
                tracing::info!("Successfully posted dry-run metrics to aggregator");
            }
        }
        return Ok(());
    }

    tracing::info!(
        "Verifying {} rows per table in SurrealDB v3 (seed={})",
        args.row_count,
        args.seed
    );

    // Convert http:// to ws:// for WebSocket connection (SurrealDB client uses WebSocket protocol)
    let endpoint = args
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");

    let surreal = match surrealdb3::engine::any::connect(&endpoint).await {
        Ok(s) => s,
        Err(e) => {
            let error_msg = format!("Failed to connect to SurrealDB v3: {e}");
            tracing::error!("{}", error_msg);
            let container_metrics =
                metrics_builder.finish_verify(None, vec![error_msg.clone()], false);
            if let Some(url) = &args.aggregator_url {
                if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                    tracing::warn!("Failed to POST metrics to aggregator: {}", e);
                }
            }
            anyhow::bail!(error_msg);
        }
    };

    // V3 SDK requires String for auth credentials
    if let Err(e) = surreal
        .signin(surrealdb3::opt::auth::Root {
            username: args.surreal_username.clone(),
            password: args.surreal_password.clone(),
        })
        .await
    {
        let error_msg = format!("Failed to authenticate with SurrealDB v3: {e}");
        tracing::error!("{}", error_msg);
        let container_metrics = metrics_builder.finish_verify(None, vec![error_msg.clone()], false);
        if let Some(url) = &args.aggregator_url {
            if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                tracing::warn!("Failed to POST metrics to aggregator: {}", e);
            }
        }
        anyhow::bail!(error_msg);
    }

    if let Err(e) = surreal
        .use_ns(&args.surreal_namespace)
        .use_db(&args.surreal_database)
        .await
    {
        let error_msg = format!("Failed to select namespace/database: {e}");
        tracing::error!("{}", error_msg);
        let container_metrics = metrics_builder.finish_verify(None, vec![error_msg.clone()], false);
        if let Some(url) = &args.aggregator_url {
            if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
                tracing::warn!("Failed to POST metrics to aggregator: {}", e);
            }
        }
        anyhow::bail!(error_msg);
    }

    let mut table_reports = Vec::new();
    let mut errors = Vec::new();

    for table_name in &tables {
        let mut verifier = match loadtest_verify_surreal3::StreamingVerifier3::new(
            surreal.clone(),
            schema.clone(),
            args.seed,
            table_name,
        ) {
            Ok(v) => v,
            Err(e) => {
                let error_msg =
                    format!("Failed to create v3 verifier for table '{table_name}': {e}");
                tracing::error!("{}", error_msg);
                errors.push(error_msg);
                continue;
            }
        };

        match verifier.verify_streaming(args.row_count).await {
            Ok(report) => {
                if report.is_success() {
                    tracing::info!(
                        "Table '{}': {} rows verified successfully",
                        table_name,
                        report.matched
                    );
                } else {
                    tracing::error!(
                        "Table '{}': {} matched, {} missing, {} mismatched",
                        table_name,
                        report.matched,
                        report.missing,
                        report.mismatched
                    );
                }
                table_reports.push(report);
            }
            Err(e) => {
                let error_msg = format!("Failed to verify table '{table_name}': {e}");
                tracing::error!("{}", error_msg);
                errors.push(error_msg);
            }
        }
    }

    // Build combined verification report
    let combined_report = if !table_reports.is_empty() {
        let table_results: Vec<loadtest_distributed::metrics::VerificationResult> = tables
            .iter()
            .zip(table_reports.iter())
            .map(
                |(table_name, report)| loadtest_distributed::metrics::VerificationResult {
                    table_name: table_name.to_string(),
                    expected: report.expected,
                    found: report.found,
                    missing: report.missing,
                    mismatched: report.mismatched,
                    matched: report.matched,
                },
            )
            .collect();

        Some(loadtest_distributed::metrics::VerificationReport {
            tables: table_results,
            total_expected: table_reports.iter().map(|r| r.expected).sum(),
            total_found: table_reports.iter().map(|r| r.found).sum(),
            total_missing: table_reports.iter().map(|r| r.missing).sum(),
            total_mismatched: table_reports.iter().map(|r| r.mismatched).sum(),
            total_matched: table_reports.iter().map(|r| r.matched).sum(),
        })
    } else {
        None
    };

    let success = errors.is_empty() && table_reports.iter().all(|r| r.is_success());
    let container_metrics = metrics_builder.finish_verify(combined_report, errors.clone(), success);

    // POST metrics to aggregator
    if let Some(url) = &args.aggregator_url {
        if let Err(e) = post_metrics_to_aggregator(url, &container_metrics) {
            tracing::warn!("Failed to POST metrics to aggregator: {}", e);
        } else {
            tracing::info!("Successfully posted metrics to aggregator");
        }
    }

    if success {
        tracing::info!("Verification completed successfully - all tables match expected data");
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Verification failed - some tables have missing or mismatched data"
        ))
    }
}
