//! Results aggregation for distributed load testing.

use crate::metrics::{AggregatedReport, ContainerMetrics, VerificationSummary};
use anyhow::{Context, Result};
use chrono::Utc;
use comfy_table::{presets::UTF8_FULL, Cell, Color, Table};
use std::fs;
use std::path::Path;

/// Aggregate container results from a Vec of ContainerMetrics.
/// This is the core aggregation logic used by both file-based and HTTP-based collection.
pub fn aggregate_results_from_vec(containers: Vec<ContainerMetrics>) -> AggregatedReport {
    // Calculate aggregates
    let total_containers = containers.len();
    let completed_containers = containers.iter().filter(|w| w.success).count();
    let failed_containers = total_containers - completed_containers;

    let total_rows_populated: u64 = containers
        .iter()
        .filter(|c| c.metrics.is_some())
        .filter_map(|c| c.metrics.as_ref().map(|m| m.rows_processed))
        .sum();

    let total_rows_verified: u64 = containers
        .iter()
        .filter_map(|c| c.verification_report.as_ref().map(|v| v.total_found))
        .sum();

    // Wall clock time is the maximum container duration
    let wall_clock_duration_secs = containers
        .iter()
        .map(|c| c.duration_secs())
        .fold(0.0f64, |a, b| a.max(b));

    // Aggregate throughput is sum of all container throughputs
    let aggregate_rows_per_second: f64 =
        containers.iter().filter_map(|c| c.rows_per_second()).sum();

    // Aggregate verification results
    let verification_summary = aggregate_verification(&containers);

    AggregatedReport {
        total_containers,
        completed_containers,
        failed_containers,
        total_rows_populated,
        total_rows_verified,
        wall_clock_duration_secs,
        aggregate_rows_per_second,
        verification_summary,
        containers,
        aggregated_at: Utc::now(),
    }
}

/// Aggregate container results from a directory (file-based).
pub fn aggregate_results(results_dir: &Path) -> Result<AggregatedReport> {
    let mut containers = Vec::new();

    // Read all JSON files in the directory
    for entry in fs::read_dir(results_dir)
        .with_context(|| format!("Failed to read results directory: {results_dir:?}"))?
    {
        let entry = entry?;
        let path = entry.path();

        if path.extension().map(|e| e == "json").unwrap_or(false) {
            let content = fs::read_to_string(&path)
                .with_context(|| format!("Failed to read result file: {path:?}"))?;

            let metrics: ContainerMetrics = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse result file: {path:?}"))?;

            containers.push(metrics);
        }
    }

    if containers.is_empty() {
        anyhow::bail!("No container result files found in {results_dir:?}");
    }

    // Use the common aggregation function
    Ok(aggregate_results_from_vec(containers))
}

/// Aggregate verification results across containers.
fn aggregate_verification(containers: &[ContainerMetrics]) -> Option<VerificationSummary> {
    let verification_containers: Vec<_> = containers
        .iter()
        .filter_map(|c| c.verification_report.as_ref())
        .collect();

    if verification_containers.is_empty() {
        return None;
    }

    let total_expected: u64 = verification_containers
        .iter()
        .map(|v| v.total_expected)
        .sum();
    let total_found: u64 = verification_containers.iter().map(|v| v.total_found).sum();
    let total_missing: u64 = verification_containers
        .iter()
        .map(|v| v.total_missing)
        .sum();
    let total_mismatched: u64 = verification_containers
        .iter()
        .map(|v| v.total_mismatched)
        .sum();
    let total_matched: u64 = verification_containers
        .iter()
        .map(|v| v.total_matched)
        .sum();

    let pass_rate = if total_expected > 0 {
        total_matched as f64 / total_expected as f64
    } else {
        1.0
    };

    Some(VerificationSummary {
        total_expected,
        total_found,
        total_missing,
        total_mismatched,
        total_matched,
        pass_rate,
    })
}

/// Format aggregated report as a table.
pub fn format_table(report: &AggregatedReport) -> String {
    let mut output = String::new();

    // Container details table
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(vec![
        "Container",
        "Tables",
        "Rows",
        "Duration",
        "Rows/sec",
        "Status",
    ]);

    for container in &report.containers {
        let tables_str = if container.tables_processed.is_empty() {
            "-".to_string()
        } else {
            container.tables_processed.join(", ")
        };

        let rows = container.total_rows();
        let duration = format_duration(container.duration_secs());
        let rows_per_sec = container
            .rows_per_second()
            .map(|r| format!("{r:.1}"))
            .unwrap_or_else(|| "-".to_string());

        let status_cell = if container.success {
            Cell::new("OK").fg(Color::Green)
        } else {
            Cell::new("FAILED").fg(Color::Red)
        };

        table.add_row(vec![
            Cell::new(&container.container_id),
            Cell::new(tables_str),
            Cell::new(format_number(rows)),
            Cell::new(duration),
            Cell::new(rows_per_sec),
            status_cell,
        ]);
    }

    // Add totals row
    table.add_row(vec![
        Cell::new("TOTAL").fg(Color::Cyan),
        Cell::new(format!(
            "{} tables",
            count_unique_tables(&report.containers)
        )),
        Cell::new(format_number(
            report.total_rows_populated + report.total_rows_verified,
        )),
        Cell::new(format!(
            "{}*",
            format_duration(report.wall_clock_duration_secs)
        )),
        Cell::new(format!("{:.1}†", report.aggregate_rows_per_second)),
        Cell::new(format!(
            "{}/{}",
            report.completed_containers, report.total_containers
        )),
    ]);

    output.push_str(&table.to_string());
    output.push_str("\n* Wall clock (parallel)  † Aggregate throughput\n");

    // Verification summary if present
    if let Some(ref v) = report.verification_summary {
        output.push_str("\nVerification Summary:\n");
        output.push_str(&format!(
            "  Expected: {}  Found: {}  Missing: {}  Mismatched: {}  Matched: {}\n",
            format_number(v.total_expected),
            format_number(v.total_found),
            format_number(v.total_missing),
            format_number(v.total_mismatched),
            format_number(v.total_matched)
        ));
        output.push_str(&format!("  Pass rate: {:.2}%\n", v.pass_rate * 100.0));
    }

    // Failed containers
    if report.failed_containers > 0 {
        output.push_str("\nFailed Containers:\n");
        for container in report.containers.iter().filter(|c| !c.success) {
            output.push_str(&format!(
                "  {}: {:?}\n",
                container.container_id, container.errors
            ));
        }
    }

    output
}

/// Format aggregated report as markdown.
pub fn format_markdown(report: &AggregatedReport) -> String {
    let mut output = String::new();

    output.push_str("# Load Test Results\n\n");
    output.push_str(&format!(
        "**Aggregated at:** {}\n\n",
        report.aggregated_at.format("%Y-%m-%d %H:%M:%S UTC")
    ));

    output.push_str("## Summary\n\n");
    output.push_str(&format!(
        "- **Total Containers:** {}\n",
        report.total_containers
    ));
    output.push_str(&format!(
        "- **Completed:** {}\n",
        report.completed_containers
    ));
    output.push_str(&format!("- **Failed:** {}\n", report.failed_containers));
    output.push_str(&format!(
        "- **Total Rows Populated:** {}\n",
        format_number(report.total_rows_populated)
    ));
    output.push_str(&format!(
        "- **Wall Clock Duration:** {}\n",
        format_duration(report.wall_clock_duration_secs)
    ));
    output.push_str(&format!(
        "- **Aggregate Throughput:** {:.1} rows/sec\n\n",
        report.aggregate_rows_per_second
    ));

    output.push_str("## Container Details\n\n");
    output.push_str("| Container | Tables | Rows | Duration | Rows/sec | Status |\n");
    output.push_str("|-----------|--------|------|----------|----------|--------|\n");

    for container in &report.containers {
        let tables_str = if container.tables_processed.is_empty() {
            "-".to_string()
        } else {
            container.tables_processed.join(", ")
        };

        let rows = container.total_rows();
        let duration = format_duration(container.duration_secs());
        let rows_per_sec = container
            .rows_per_second()
            .map(|r| format!("{r:.1}"))
            .unwrap_or_else(|| "-".to_string());

        let status = if container.success { "OK" } else { "FAILED" };

        output.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            container.container_id,
            tables_str,
            format_number(rows),
            duration,
            rows_per_sec,
            status
        ));
    }

    if let Some(ref v) = report.verification_summary {
        output.push_str("\n## Verification Summary\n\n");
        output.push_str(&format!(
            "- **Expected:** {}\n",
            format_number(v.total_expected)
        ));
        output.push_str(&format!("- **Found:** {}\n", format_number(v.total_found)));
        output.push_str(&format!(
            "- **Missing:** {}\n",
            format_number(v.total_missing)
        ));
        output.push_str(&format!(
            "- **Mismatched:** {}\n",
            format_number(v.total_mismatched)
        ));
        output.push_str(&format!(
            "- **Matched:** {}\n",
            format_number(v.total_matched)
        ));
        output.push_str(&format!("- **Pass Rate:** {:.2}%\n", v.pass_rate * 100.0));
    }

    output
}

/// Format duration in human-readable format.
fn format_duration(secs: f64) -> String {
    if secs < 60.0 {
        format!("{secs:.1}s")
    } else if secs < 3600.0 {
        let mins = (secs / 60.0).floor();
        let remaining_secs = secs - (mins * 60.0);
        format!("{}m {:02.0}s", mins as u64, remaining_secs)
    } else {
        let hours = (secs / 3600.0).floor();
        let remaining = secs - (hours * 3600.0);
        let mins = (remaining / 60.0).floor();
        format!("{}h {:02.0}m", hours as u64, mins as u64)
    }
}

/// Format number with thousands separators.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }

    result
}

/// Count unique tables across all containers.
fn count_unique_tables(containers: &[ContainerMetrics]) -> usize {
    let mut tables: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for container in containers {
        for table in &container.tables_processed {
            tables.insert(table);
        }
    }
    tables.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(5.5), "5.5s");
        assert_eq!(format_duration(65.0), "1m 05s");
        assert_eq!(format_duration(3661.0), "1h 01m");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(100), "100");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1000000), "1,000,000");
    }
}
