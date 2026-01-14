#!/usr/bin/env python3
"""
Compare loadtest metrics against baseline.

Usage: ./scripts/compare_metrics.py <current_metrics.json> [baseline_metrics.json]

Compares current run against baseline and outputs:
- Markdown summary table
- Exit code 0 if no regressions, 1 if regression detected

Environment variables:
  THRESHOLD      - Regression threshold percentage (default: 10)
  OUTPUT_FORMAT  - Output format: markdown, json (default: markdown)

Exit codes:
  0 = No regressions detected (or no baseline to compare)
  1 = Regression detected (performance degraded beyond threshold)
  2 = Invalid input (file not found, invalid JSON)
"""

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple


@dataclass
class MetricComparison:
    """Result of comparing a single metric."""
    name: str
    current: Optional[float]
    baseline: Optional[float]
    change_percent: Optional[float]
    is_regression: bool
    higher_is_better: bool


def calc_change_percent(current: Optional[float], baseline: Optional[float]) -> Optional[float]:
    """Calculate percentage change between current and baseline.

    Args:
        current: Current value
        baseline: Baseline value

    Returns:
        Percentage change, or None if cannot be calculated
    """
    if current is None or baseline is None:
        return None
    if baseline == 0:
        return None
    return ((current - baseline) / baseline) * 100


def is_regression(
    current: Optional[float],
    baseline: Optional[float],
    threshold: float,
    higher_is_better: bool
) -> bool:
    """Determine if a metric shows regression.

    Args:
        current: Current value
        baseline: Baseline value
        threshold: Regression threshold as percentage (e.g., 10 for 10%)
        higher_is_better: True if higher values are better (e.g., throughput)

    Returns:
        True if regression detected, False otherwise
    """
    if current is None or baseline is None:
        return False
    if baseline == 0:
        return False

    change_percent = calc_change_percent(current, baseline)
    if change_percent is None:
        return False

    if higher_is_better:
        # For metrics where higher is better (throughput), negative change is bad
        return change_percent < -threshold
    else:
        # For metrics where lower is better (duration), positive change is bad
        return change_percent > threshold


def get_status_emoji(is_regressed: bool, has_baseline: bool) -> str:
    """Get status emoji for markdown output."""
    if not has_baseline:
        return "-"
    return ":warning:" if is_regressed else ":white_check_mark:"


def format_change(change_percent: Optional[float]) -> str:
    """Format percentage change for display."""
    if change_percent is None:
        return "N/A"
    sign = "+" if change_percent >= 0 else ""
    return f"{sign}{change_percent:.1f}%"


def format_value(value: Optional[float], decimals: int = 1) -> str:
    """Format a numeric value for display."""
    if value is None:
        return "N/A"
    if decimals == 0:
        return str(int(value))
    return f"{value:.{decimals}f}"


def load_metrics(filepath: str) -> Optional[dict]:
    """Load metrics from JSON file.

    Args:
        filepath: Path to JSON file

    Returns:
        Parsed JSON dict, or None if file doesn't exist or is invalid
    """
    try:
        path = Path(filepath)
        if not path.exists():
            return None
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def get_nested(data: dict, *keys, default=None):
    """Safely get nested dictionary value."""
    for key in keys:
        if not isinstance(data, dict):
            return default
        data = data.get(key, default)
        if data is None:
            return default
    return data


def compare_metrics(
    current: dict,
    baseline: Optional[dict],
    threshold: float
) -> Tuple[list[MetricComparison], bool]:
    """Compare current metrics against baseline.

    Args:
        current: Current metrics dict
        baseline: Baseline metrics dict (may be None)
        threshold: Regression threshold percentage

    Returns:
        Tuple of (list of comparisons, has_any_regression)
    """
    baseline = baseline or {}
    comparisons = []
    has_regression = False

    # Define metrics to compare: (name, json_path, higher_is_better, decimals)
    metrics = [
        ("Throughput (rows/sec)", ("results", "throughput_total_rows_per_sec"), True, 1),
        ("Duration (sec)", ("results", "total_duration_seconds"), False, 0),
        ("Sync Duration (sec)", ("results", "sync_duration_seconds"), False, 0),
        ("Peak Memory (MB)", ("resources", "peak_memory_mb"), False, 0),
    ]

    for name, path, higher_is_better, _ in metrics:
        curr_val = get_nested(current, *path)
        base_val = get_nested(baseline, *path)

        # Convert to float if possible
        try:
            curr_val = float(curr_val) if curr_val is not None else None
        except (ValueError, TypeError):
            curr_val = None
        try:
            base_val = float(base_val) if base_val is not None else None
        except (ValueError, TypeError):
            base_val = None

        change = calc_change_percent(curr_val, base_val)
        regressed = is_regression(curr_val, base_val, threshold, higher_is_better)

        if regressed:
            has_regression = True

        comparisons.append(MetricComparison(
            name=name,
            current=curr_val,
            baseline=base_val,
            change_percent=change,
            is_regression=regressed,
            higher_is_better=higher_is_better
        ))

    return comparisons, has_regression


def generate_markdown(
    current: dict,
    baseline: Optional[dict],
    comparisons: list[MetricComparison],
    has_baseline: bool
) -> str:
    """Generate markdown summary table.

    Args:
        current: Current metrics dict
        baseline: Baseline metrics dict
        comparisons: List of metric comparisons
        has_baseline: Whether baseline was provided

    Returns:
        Markdown formatted string
    """
    source = current.get("source", "unknown")
    lines = [
        f"## Load Test Results: {source}",
        "",
    ]

    if not has_baseline:
        lines.append("_No baseline available for comparison_")
        lines.append("")

    # Table header
    lines.extend([
        "| Metric | Current | Baseline | Change | Status |",
        "|--------|---------|----------|--------|--------|",
    ])

    # Metric rows
    for comp in comparisons:
        curr_str = format_value(comp.current)
        base_str = format_value(comp.baseline) if has_baseline else "-"
        change_str = format_change(comp.change_percent) if has_baseline else "-"

        # Memory doesn't trigger regressions in our logic, show "-" for status
        if "Memory" in comp.name:
            status = "-"
        else:
            status = get_status_emoji(comp.is_regression, has_baseline)

        lines.append(f"| {comp.name} | {curr_str} | {base_str} | {change_str} | {status} |")

    # Verification stats (not compared)
    matched = get_nested(current, "verification", "matched", default=0)
    mismatched = get_nested(current, "verification", "mismatched", default=0)
    mismatch_status = ":white_check_mark:" if mismatched == 0 else ":x:"

    lines.append(f"| Rows Verified | {matched} | - | - | - |")
    lines.append(f"| Mismatches | {mismatched} | - | - | {mismatch_status} |")

    lines.append("")

    # Status
    status = get_nested(current, "results", "status", default="unknown")
    lines.append(f"**Status**: {status}")

    # Git info
    git_sha = current.get("git_sha", "")
    git_ref = current.get("git_ref", "")
    if git_sha:
        lines.append(f"**Commit**: {git_sha} ({git_ref})")

    lines.append("")

    # Add timeline table if available
    timeline_table = generate_timeline_table(current)
    if timeline_table:
        lines.append(timeline_table)

    return "\n".join(lines)


def generate_timeline_table(current: dict) -> str:
    """Generate timeline table from metrics.

    Args:
        current: Current metrics dict containing timeline data

    Returns:
        Markdown formatted timeline table, or empty string if no data
    """
    timeline = current.get("timeline", {})
    containers = timeline.get("containers", [])

    if not containers:
        return ""

    lines = [
        "### Container Timeline",
        "",
        "| Container | Type | Start | End | Duration | Status |",
        "|-----------|------|-------|-----|----------|--------|",
    ]

    for c in containers:
        # Format end time - show "running" if None
        end_sec = c.get('end_sec')
        end_str = f"{end_sec}s" if end_sec is not None else "running"

        # Format duration - show "-" if None
        duration_sec = c.get('duration_sec')
        duration_str = f"{duration_sec}s" if duration_sec is not None else "-"

        # Status - show running emoji if container hasn't finished
        if end_sec is None:
            status = ":hourglass:"  # Running
        elif c.get("exit_code", -1) == 0:
            status = ":white_check_mark:"
        else:
            status = ":x:"

        lines.append(
            f"| {c.get('name', 'unknown')} | {c.get('type', 'unknown')} | "
            f"{c.get('start_sec', 0)}s | {end_str} | "
            f"{duration_str} | {status} |"
        )

    lines.append("")
    return "\n".join(lines)


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)

    current_file = sys.argv[1]
    baseline_file = sys.argv[2] if len(sys.argv) > 2 else None

    # Configuration
    threshold = float(os.environ.get("THRESHOLD", "10"))

    # Load current metrics
    current = load_metrics(current_file)
    if current is None:
        print(f"Error: Cannot load current metrics from {current_file}", file=sys.stderr)
        sys.exit(2)

    # Load baseline (optional)
    baseline = None
    has_baseline = False
    if baseline_file:
        baseline = load_metrics(baseline_file)
        has_baseline = baseline is not None

    # Compare metrics
    comparisons, has_regression = compare_metrics(current, baseline, threshold)

    # Generate output
    markdown = generate_markdown(current, baseline, comparisons, has_baseline)
    print(markdown)

    # Note: Don't write to GITHUB_STEP_SUMMARY here - the workflow handles that
    # by redirecting stdout to the summary file. Writing here would cause duplicates.

    # Report regression
    if has_regression:
        print(f"\nWARNING: Performance regression detected (threshold: {threshold}%)", file=sys.stderr)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
