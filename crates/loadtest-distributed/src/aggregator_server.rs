//! HTTP-based aggregator server for collecting metrics from distributed containers.
//!
//! This module provides an HTTP server that collects ContainerMetrics from
//! distributed containers, eliminating the need for shared volumes (ReadWriteMany).

use crate::cli::{AggregateServerArgs, OutputFormat};
use crate::metrics::ContainerMetrics;
use crate::{aggregate_results_from_vec, format_markdown, format_table};
use anyhow::{Context, Result};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// Collected metrics from containers.
struct CollectedMetrics {
    containers: Vec<ContainerMetrics>,
    expected_count: usize,
}

impl CollectedMetrics {
    fn new(expected_count: usize) -> Self {
        Self {
            containers: Vec::with_capacity(expected_count),
            expected_count,
        }
    }

    fn add(&mut self, metrics: ContainerMetrics) {
        self.containers.push(metrics);
    }

    fn count(&self) -> usize {
        self.containers.len()
    }

    fn is_complete(&self) -> bool {
        self.containers.len() >= self.expected_count
    }
}

/// Run the aggregator HTTP server.
pub async fn run_aggregator_server(args: AggregateServerArgs) -> Result<()> {
    let timeout = parse_duration(&args.timeout)?;
    let metrics = Arc::new(Mutex::new(CollectedMetrics::new(args.expected_containers)));

    info!(
        "Aggregator server starting on {} (expecting {} containers, timeout: {:?})",
        args.listen, args.expected_containers, timeout
    );

    let listener = TcpListener::bind(&args.listen)
        .with_context(|| format!("Failed to bind to {}", args.listen))?;

    // Set non-blocking so we can check timeout
    listener
        .set_nonblocking(true)
        .context("Failed to set non-blocking mode")?;

    let start = Instant::now();

    loop {
        // Check if we've collected all expected metrics
        {
            let collected = metrics.lock().unwrap();
            if collected.is_complete() {
                info!(
                    "All {} containers reported, generating report...",
                    args.expected_containers
                );
                let report = aggregate_results_from_vec(collected.containers.clone());
                output_report(&report, args.output_format);
                return Ok(());
            }
        }

        // Check timeout
        if start.elapsed() > timeout {
            let collected = metrics.lock().unwrap();
            warn!(
                "Timeout reached after {:?}. Received {}/{} container reports.",
                timeout,
                collected.count(),
                args.expected_containers
            );
            if !collected.containers.is_empty() {
                let report = aggregate_results_from_vec(collected.containers.clone());
                output_report(&report, args.output_format);
            }
            return Err(anyhow::anyhow!(
                "Timeout: only {}/{} containers reported",
                collected.count(),
                args.expected_containers
            ));
        }

        // Try to accept a connection
        match listener.accept() {
            Ok((stream, addr)) => {
                info!("Connection from {}", addr);
                if let Err(e) = handle_connection(stream, &metrics) {
                    error!("Error handling connection from {}: {}", addr, e);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No connection ready, sleep briefly
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                error!("Error accepting connection: {}", e);
            }
        }
    }
}

/// Handle an incoming HTTP connection.
fn handle_connection(mut stream: TcpStream, metrics: &Arc<Mutex<CollectedMetrics>>) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;

    let buf_reader = BufReader::new(&stream);
    let mut lines = buf_reader.lines();

    // Read request line
    let request_line = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("Empty request"))??;

    // Parse headers to get Content-Length
    let mut content_length: usize = 0;
    for line in lines.by_ref() {
        let line = line?;
        if line.is_empty() {
            break;
        }
        if let Some(len_str) = line.strip_prefix("Content-Length: ") {
            content_length = len_str.parse().unwrap_or(0);
        }
    }

    // Route the request
    let (status, body) = if request_line.starts_with("POST /metrics") {
        // Read body
        let mut body_bytes = vec![0u8; content_length];
        std::io::Read::read_exact(&mut stream.try_clone()?, &mut body_bytes)?;

        match serde_json::from_slice::<ContainerMetrics>(&body_bytes) {
            Ok(container_metrics) => {
                let container_id = container_metrics.container_id.clone();
                let mut collected = metrics.lock().unwrap();
                collected.add(container_metrics);
                info!(
                    "Received metrics from container '{}' ({}/{})",
                    container_id,
                    collected.count(),
                    collected.expected_count
                );
                ("200 OK", r#"{"status":"ok"}"#.to_string())
            }
            Err(e) => {
                error!("Failed to parse ContainerMetrics: {e}");
                (
                    "400 Bad Request",
                    format!(r#"{{"error":"invalid json: {e}"}}"#),
                )
            }
        }
    } else if request_line.starts_with("GET /health") {
        ("200 OK", r#"{"status":"healthy"}"#.to_string())
    } else if request_line.starts_with("GET /status") {
        let collected = metrics.lock().unwrap();
        (
            "200 OK",
            format!(
                r#"{{"received":{},"expected":{}}}"#,
                collected.count(),
                collected.expected_count
            ),
        )
    } else {
        ("404 Not Found", r#"{"error":"not found"}"#.to_string())
    };

    // Send response
    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        body.len(),
        body
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;

    Ok(())
}

/// Output the aggregated report.
fn output_report(report: &crate::metrics::AggregatedReport, format: OutputFormat) {
    match format {
        OutputFormat::Json => {
            if let Ok(json) = serde_json::to_string_pretty(report) {
                println!("{json}");
            }
        }
        OutputFormat::Table => {
            println!("{}", format_table(report));
        }
        OutputFormat::Markdown => {
            println!("{}", format_markdown(report));
        }
    }
}

/// Parse duration string like "30m", "1h", "90s".
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Duration::from_secs(1800)); // Default 30 minutes
    }

    let (num_str, suffix) = if let Some(stripped) = s.strip_suffix('h') {
        (stripped, "h")
    } else if let Some(stripped) = s.strip_suffix('m') {
        (stripped, "m")
    } else if let Some(stripped) = s.strip_suffix('s') {
        (stripped, "s")
    } else {
        // Assume seconds if no suffix
        (s, "s")
    };

    let num: u64 = num_str
        .parse()
        .with_context(|| format!("Invalid duration: {s}"))?;

    let secs = match suffix {
        "h" => num * 3600,
        "m" => num * 60,
        "s" => num,
        _ => num,
    };

    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1800));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
    }

    #[test]
    fn test_collected_metrics() {
        let mut collected = CollectedMetrics::new(2);
        assert!(!collected.is_complete());
        assert_eq!(collected.count(), 0);

        // Create sample metrics
        let metrics1 = create_test_metrics("populate-1");
        let metrics2 = create_test_metrics("populate-2");

        collected.add(metrics1);
        assert!(!collected.is_complete());
        assert_eq!(collected.count(), 1);

        collected.add(metrics2);
        assert!(collected.is_complete());
        assert_eq!(collected.count(), 2);
    }

    fn create_test_metrics(id: &str) -> ContainerMetrics {
        use crate::metrics::EnvironmentInfo;
        use crate::metrics::Operation;
        use chrono::Utc;

        ContainerMetrics {
            container_id: id.to_string(),
            hostname: "test-host".to_string(),
            started_at: Utc::now(),
            completed_at: Utc::now(),
            environment: EnvironmentInfo {
                cpu_cores: 4,
                memory_mb: 8192,
                available_memory_mb: 4096,
                tmpfs_enabled: false,
                tmpfs_size_mb: None,
                cgroup_memory_limit: None,
                cgroup_cpu_quota: None,
                data_fs_type: None,
            },
            tables_processed: vec!["users".to_string()],
            operation: Operation::Populate,
            metrics: None,
            verification_report: None,
            errors: vec![],
            success: true,
        }
    }
}
