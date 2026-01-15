#!/usr/bin/env python3
"""
Generate loadtest summary by comparing metrics against baseline.

Usage: ./scripts/generate_summary.py --metrics-dir <dir> --preset <preset> [--baseline-dir <dir>]

Arguments:
  --metrics-dir    Directory containing metrics-* subdirectories
  --baseline-dir   Directory containing baseline metrics (optional)
  --preset         Current test preset (small/medium/large)
  --output         Output file (default: stdout)

Expected directory structures:

  metrics-dir (from current run):
    metrics/
      metrics-kafka/
        metrics.json
      metrics-mysql/
        metrics.json
      metrics-postgresql/
        metrics.json

  baseline-dir (downloaded from previous run artifact):
    baseline/
      metrics-kafka/
        metrics.json
      metrics-mysql/
        metrics.json
      metrics-postgresql/
        metrics.json

  Both directories use the same "metrics-<source>" subdirectory structure for consistency.

Examples:
  # Compare current metrics against baseline with preset validation
  ./scripts/generate_summary.py --metrics-dir metrics/ --baseline-dir baseline/ --preset small

  # Generate summary without baseline (first run or no matching preset)
  ./scripts/generate_summary.py --metrics-dir metrics/ --preset medium

  # Write output to file instead of stdout
  ./scripts/generate_summary.py --metrics-dir metrics/ --preset small --output summary.md

Preset matching:
  The script reads the "preset" field from each baseline metrics.json file.
  If the baseline preset doesn't match --preset, comparison is skipped for that source
  and a message like this is shown:
    "_Baseline preset mismatch: current=small, baseline=medium - skipping comparison_"

Exit codes:
  0 = Success (summary generated, may or may not have regressions)
  1 = Regression detected in at least one source (only when presets match)
  2 = Invalid input (missing metrics dir, etc.)
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional


def get_preset_from_metrics(metrics_file: Path) -> str:
    """Extract preset from metrics.json file.

    Args:
        metrics_file: Path to metrics.json

    Returns:
        Preset string, or "unknown" if not found or file is invalid
    """
    try:
        with open(metrics_file) as f:
            data = json.load(f)
        return data.get("preset", "unknown")
    except (json.JSONDecodeError, IOError):
        return "unknown"


def generate_summary(
    metrics_dir: Path,
    baseline_dir: Optional[Path],
    current_preset: str,
    output_file: Optional[Path] = None
) -> tuple[str, bool]:
    """Generate markdown summary for all sources.

    Args:
        metrics_dir: Directory containing metrics-* subdirectories
        baseline_dir: Directory containing baseline metrics (may be None)
        current_preset: Current test preset
        output_file: Optional file to write output to

    Returns:
        Tuple of (markdown summary string, has_any_regression)
    """
    lines = []
    scripts_dir = Path(__file__).parent
    compare_script = scripts_dir / "compare_metrics.py"
    has_any_regression = False

    # Find all metrics-* directories
    metrics_dirs = sorted(metrics_dir.glob("metrics-*"))

    if not metrics_dirs:
        lines.append("_No metrics found_")
        lines.append("")

    for source_dir in metrics_dirs:
        source = source_dir.name.replace("metrics-", "")
        metrics_file = source_dir / "metrics.json"

        if not metrics_file.exists():
            continue

        lines.append(f"### Source: {source}")
        lines.append("")

        # Check for baseline (uses same metrics-<source> directory structure)
        baseline_file: Optional[Path] = None
        baseline_preset: Optional[str] = None
        if baseline_dir:
            candidate = baseline_dir / source_dir.name / "metrics.json"
            if candidate.exists():
                baseline_file = candidate
                baseline_preset = get_preset_from_metrics(candidate)

        # Check preset compatibility
        use_baseline = False
        if baseline_file and baseline_preset:
            if baseline_preset != current_preset:
                lines.append(
                    f"_Baseline preset mismatch: current={current_preset}, "
                    f"baseline={baseline_preset} - skipping comparison_"
                )
                lines.append("")
            else:
                use_baseline = True

        # Run compare_metrics.py
        cmd = ["python3", str(compare_script), str(metrics_file)]
        if use_baseline and baseline_file:
            cmd.append(str(baseline_file))

        result = subprocess.run(cmd, capture_output=True, text=True)

        # Check for regression (exit code 1)
        if result.returncode == 1:
            has_any_regression = True

        if result.stdout:
            lines.append(result.stdout.rstrip())
        if result.stderr:
            # Only include stderr that's not just the warning message
            stderr = result.stderr.rstrip()
            if stderr and "WARNING: Performance regression detected" not in stderr:
                lines.append(stderr)

        lines.append("")

    summary = "\n".join(lines)

    if output_file:
        with open(output_file, "w") as f:
            f.write(summary)

    return summary, has_any_regression


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--metrics-dir",
        type=Path,
        required=True,
        help="Directory containing metrics-* subdirectories"
    )
    parser.add_argument(
        "--baseline-dir",
        type=Path,
        default=None,
        help="Directory containing baseline metrics (optional)"
    )
    parser.add_argument(
        "--preset",
        required=True,
        help="Current test preset (small/medium/large)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file (default: stdout)"
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.metrics_dir.exists():
        print(f"Error: metrics directory not found: {args.metrics_dir}", file=sys.stderr)
        sys.exit(2)

    if args.baseline_dir and not args.baseline_dir.exists():
        # Baseline dir not existing is not an error, just means no baseline
        args.baseline_dir = None

    summary, has_regression = generate_summary(
        args.metrics_dir,
        args.baseline_dir,
        args.preset,
        args.output
    )

    if not args.output:
        print(summary)

    # Exit with 1 if regression detected
    if has_regression:
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
