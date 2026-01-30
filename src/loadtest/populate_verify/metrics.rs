//! Metrics posting utilities shared between populate and verify handlers.

use anyhow::Context;

/// Post ContainerMetrics to the aggregator server via HTTP POST with retry logic.
///
/// This function uses std::net::TcpStream to avoid external HTTP client dependencies.
/// It sends a simple HTTP/1.1 POST request with JSON body and validates the response.
///
/// Retries up to 3 times with 2-second delays to handle transient network issues.
pub fn post_metrics_to_aggregator(
    url: &str,
    metrics: &loadtest_distributed::metrics::ContainerMetrics,
) -> anyhow::Result<()> {
    use std::thread;
    use std::time::Duration;

    const MAX_RETRIES: u32 = 3;
    const RETRY_DELAY: Duration = Duration::from_secs(2);

    let mut last_error = None;

    for attempt in 1..=MAX_RETRIES {
        match try_post_metrics(url, metrics) {
            Ok(()) => {
                if attempt > 1 {
                    tracing::info!(
                        "Successfully posted metrics to aggregator after {attempt} attempts"
                    );
                }
                return Ok(());
            }
            Err(e) => {
                last_error = Some(e);
                if attempt < MAX_RETRIES {
                    tracing::warn!(
                        "Failed to POST metrics to aggregator (attempt {}/{}): {}. Retrying in {:?}...",
                        attempt,
                        MAX_RETRIES,
                        last_error.as_ref().unwrap(),
                        RETRY_DELAY
                    );
                    thread::sleep(RETRY_DELAY);
                }
            }
        }
    }

    Err(last_error.unwrap()).context(format!(
        "Failed to POST metrics to aggregator after {MAX_RETRIES} attempts"
    ))
}

/// Single attempt to POST metrics to aggregator.
fn try_post_metrics(
    url: &str,
    metrics: &loadtest_distributed::metrics::ContainerMetrics,
) -> anyhow::Result<()> {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    // Parse URL to extract host:port and path
    let url = url.strip_prefix("http://").unwrap_or(url);
    let (host, path) = if let Some(pos) = url.find('/') {
        (&url[..pos], &url[pos..])
    } else {
        (url, "/metrics")
    };

    // Connect to aggregator
    let mut stream = TcpStream::connect(host)
        .with_context(|| format!("Failed to connect to aggregator at {host}"))?;

    // Serialize metrics to JSON
    let json =
        serde_json::to_string(metrics).context("Failed to serialize ContainerMetrics to JSON")?;

    // Build HTTP POST request
    let request = format!(
        "POST {} HTTP/1.1\r\n\
         Host: {}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        path,
        host,
        json.len(),
        json
    );

    // Send request
    stream
        .write_all(request.as_bytes())
        .context("Failed to write HTTP request")?;
    stream.flush().context("Failed to flush HTTP request")?;

    // Read response
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .context("Failed to read HTTP response")?;

    // Validate response status
    if !response.starts_with("HTTP/1.1 200") {
        let status_line = response.lines().next().unwrap_or("(no status line)");
        anyhow::bail!("Aggregator returned non-200 response: {status_line}");
    }

    Ok(())
}
