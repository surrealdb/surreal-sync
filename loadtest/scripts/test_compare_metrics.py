#!/usr/bin/env python3
"""Unit tests for compare_metrics.py functions."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

# Add script directory to path
sys.path.insert(0, str(Path(__file__).parent))

from compare_metrics import (
    calc_change_percent,
    is_regression,
    format_change,
    format_value,
    load_metrics,
    get_nested,
    compare_metrics,
    generate_markdown,
    generate_timeline_table,
    MetricComparison,
)


class TestCalcChangePercent(unittest.TestCase):
    """Tests for calc_change_percent function."""

    def test_positive_change(self):
        """110 vs 100 = +10%"""
        result = calc_change_percent(110, 100)
        self.assertAlmostEqual(result, 10.0, places=1)

    def test_negative_change(self):
        """80 vs 100 = -20%"""
        result = calc_change_percent(80, 100)
        self.assertAlmostEqual(result, -20.0, places=1)

    def test_no_change(self):
        """100 vs 100 = 0%"""
        result = calc_change_percent(100, 100)
        self.assertAlmostEqual(result, 0.0, places=1)

    def test_none_current(self):
        """None current returns None"""
        result = calc_change_percent(None, 100)
        self.assertIsNone(result)

    def test_none_baseline(self):
        """None baseline returns None"""
        result = calc_change_percent(100, None)
        self.assertIsNone(result)

    def test_zero_baseline(self):
        """Zero baseline returns None (division by zero)"""
        result = calc_change_percent(100, 0)
        self.assertIsNone(result)

    def test_double_value(self):
        """102.5 vs 100 = +2.5%"""
        result = calc_change_percent(102.5, 100)
        self.assertAlmostEqual(result, 2.5, places=1)


class TestIsRegression(unittest.TestCase):
    """Tests for is_regression function."""

    def test_throughput_regression(self):
        """Throughput dropped 20% with 10% threshold = regression"""
        result = is_regression(80, 100, threshold=10, higher_is_better=True)
        self.assertTrue(result)

    def test_throughput_no_regression(self):
        """Throughput dropped 5% with 10% threshold = no regression"""
        result = is_regression(95, 100, threshold=10, higher_is_better=True)
        self.assertFalse(result)

    def test_throughput_improvement(self):
        """Throughput improved 20% = no regression"""
        result = is_regression(120, 100, threshold=10, higher_is_better=True)
        self.assertFalse(result)

    def test_duration_regression(self):
        """Duration increased 20% with 10% threshold = regression"""
        result = is_regression(120, 100, threshold=10, higher_is_better=False)
        self.assertTrue(result)

    def test_duration_no_regression(self):
        """Duration increased 5% with 10% threshold = no regression"""
        result = is_regression(105, 100, threshold=10, higher_is_better=False)
        self.assertFalse(result)

    def test_duration_improvement(self):
        """Duration decreased 20% = no regression"""
        result = is_regression(80, 100, threshold=10, higher_is_better=False)
        self.assertFalse(result)

    def test_none_values(self):
        """None values = no regression"""
        self.assertFalse(is_regression(None, 100, 10, True))
        self.assertFalse(is_regression(100, None, 10, True))

    def test_zero_baseline(self):
        """Zero baseline = no regression"""
        result = is_regression(100, 0, threshold=10, higher_is_better=True)
        self.assertFalse(result)

    def test_custom_threshold(self):
        """25% threshold should not flag 20% change"""
        result = is_regression(80, 100, threshold=25, higher_is_better=True)
        self.assertFalse(result)


class TestFormatChange(unittest.TestCase):
    """Tests for format_change function."""

    def test_positive_change(self):
        result = format_change(10.5)
        self.assertEqual(result, "+10.5%")

    def test_negative_change(self):
        result = format_change(-20.3)
        self.assertEqual(result, "-20.3%")

    def test_zero_change(self):
        result = format_change(0)
        self.assertEqual(result, "+0.0%")

    def test_none_change(self):
        result = format_change(None)
        self.assertEqual(result, "N/A")


class TestFormatValue(unittest.TestCase):
    """Tests for format_value function."""

    def test_integer_display(self):
        result = format_value(100.0, decimals=0)
        self.assertEqual(result, "100")

    def test_float_display(self):
        result = format_value(100.567, decimals=1)
        self.assertEqual(result, "100.6")

    def test_none_value(self):
        result = format_value(None)
        self.assertEqual(result, "N/A")


class TestGetNested(unittest.TestCase):
    """Tests for get_nested function."""

    def test_single_level(self):
        data = {"key": "value"}
        result = get_nested(data, "key")
        self.assertEqual(result, "value")

    def test_nested(self):
        data = {"level1": {"level2": {"level3": "value"}}}
        result = get_nested(data, "level1", "level2", "level3")
        self.assertEqual(result, "value")

    def test_missing_key(self):
        data = {"key": "value"}
        result = get_nested(data, "missing", default="default")
        self.assertEqual(result, "default")

    def test_none_in_path(self):
        data = {"level1": None}
        result = get_nested(data, "level1", "level2", default="default")
        self.assertEqual(result, "default")


class TestLoadMetrics(unittest.TestCase):
    """Tests for load_metrics function."""

    def test_load_valid_json(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"key": "value"}, f)
            f.flush()
            result = load_metrics(f.name)
            self.assertEqual(result, {"key": "value"})
            Path(f.name).unlink()

    def test_load_nonexistent_file(self):
        result = load_metrics("/nonexistent/path/file.json")
        self.assertIsNone(result)

    def test_load_invalid_json(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("not valid json")
            f.flush()
            result = load_metrics(f.name)
            self.assertIsNone(result)
            Path(f.name).unlink()


class TestCompareMetrics(unittest.TestCase):
    """Tests for compare_metrics function."""

    def setUp(self):
        self.baseline = {
            "results": {
                "throughput_rows_per_sec": 100,
                "total_duration_seconds": 45,
                "sync_duration_seconds": 30,
            },
            "resources": {
                "peak_memory_mb": 500
            }
        }

    def test_no_regression(self):
        current = {
            "results": {
                "throughput_rows_per_sec": 102,
                "total_duration_seconds": 44,
                "sync_duration_seconds": 29,
            },
            "resources": {
                "peak_memory_mb": 490
            }
        }
        comparisons, has_regression = compare_metrics(current, self.baseline, threshold=10)
        self.assertFalse(has_regression)

    def test_throughput_regression(self):
        current = {
            "results": {
                "throughput_rows_per_sec": 80,  # -20%
                "total_duration_seconds": 45,
                "sync_duration_seconds": 30,
            },
            "resources": {
                "peak_memory_mb": 500
            }
        }
        comparisons, has_regression = compare_metrics(current, self.baseline, threshold=10)
        self.assertTrue(has_regression)

    def test_duration_regression(self):
        current = {
            "results": {
                "throughput_rows_per_sec": 100,
                "total_duration_seconds": 55,  # +22%
                "sync_duration_seconds": 30,
            },
            "resources": {
                "peak_memory_mb": 500
            }
        }
        comparisons, has_regression = compare_metrics(current, self.baseline, threshold=10)
        self.assertTrue(has_regression)

    def test_no_baseline(self):
        current = {
            "results": {
                "throughput_rows_per_sec": 80,
                "total_duration_seconds": 55,
                "sync_duration_seconds": 40,
            },
            "resources": {
                "peak_memory_mb": 600
            }
        }
        comparisons, has_regression = compare_metrics(current, None, threshold=10)
        self.assertFalse(has_regression)  # No baseline = no regression


class TestGenerateMarkdown(unittest.TestCase):
    """Tests for generate_markdown function."""

    def test_contains_table_markers(self):
        current = {
            "source": "kafka",
            "results": {
                "status": "success",
                "throughput_rows_per_sec": 100,
            },
            "verification": {
                "matched": 1000,
                "mismatched": 0,
            }
        }
        comparisons = [
            MetricComparison(
                name="Throughput",
                current=100,
                baseline=None,
                change_percent=None,
                is_regression=False,
                higher_is_better=True
            )
        ]
        result = generate_markdown(current, None, comparisons, has_baseline=False)

        self.assertIn("| Metric |", result)
        self.assertIn("|--------|", result)
        self.assertIn("kafka", result)

    def test_shows_regression_warning(self):
        current = {
            "source": "kafka",
            "results": {
                "status": "success",
                "throughput_rows_per_sec": 80,
            },
            "verification": {
                "matched": 1000,
                "mismatched": 0,
            }
        }
        baseline = {
            "source": "kafka",
            "results": {
                "status": "success",
                "throughput_rows_per_sec": 100,
            }
        }
        comparisons = [
            MetricComparison(
                name="Throughput",
                current=80,
                baseline=100,
                change_percent=-20.0,
                is_regression=True,
                higher_is_better=True
            )
        ]
        result = generate_markdown(current, baseline, comparisons, has_baseline=True)

        self.assertIn(":warning:", result)


class TestGenerateTimelineTable(unittest.TestCase):
    """Tests for generate_timeline_table function."""

    def test_generates_table_with_containers(self):
        """Returns markdown table when timeline data exists."""
        metrics = {
            "timeline": {
                "containers": [
                    {"name": "output-kafka-1", "type": "kafka",
                     "start_sec": 0.0, "end_sec": 60.0, "duration_sec": 60.0, "exit_code": 0},
                    {"name": "output-populate-1", "type": "populate",
                     "start_sec": 5.0, "end_sec": 30.0, "duration_sec": 25.0, "exit_code": 0},
                    {"name": "output-sync-users", "type": "sync",
                     "start_sec": 31.0, "end_sec": 32.0, "duration_sec": 1.0, "exit_code": 0}
                ]
            }
        }
        result = generate_timeline_table(metrics)

        self.assertIn("### Container Timeline", result)
        self.assertIn("| Container |", result)
        self.assertIn("output-kafka-1", result)
        self.assertIn("output-populate-1", result)
        self.assertIn("output-sync-users", result)
        self.assertIn(":white_check_mark:", result)

    def test_shows_failure_status_for_nonzero_exit(self):
        """Shows :x: for containers with non-zero exit code."""
        metrics = {
            "timeline": {
                "containers": [
                    {"name": "output-sync-1", "type": "sync",
                     "start_sec": 0.0, "end_sec": 10.0, "duration_sec": 10.0, "exit_code": 1}
                ]
            }
        }
        result = generate_timeline_table(metrics)

        self.assertIn(":x:", result)

    def test_returns_empty_string_when_no_timeline(self):
        """Returns empty string when timeline key is missing."""
        result = generate_timeline_table({})
        self.assertEqual(result, "")

    def test_returns_empty_string_when_no_containers(self):
        """Returns empty string when containers list is empty."""
        result = generate_timeline_table({"timeline": {"containers": []}})
        self.assertEqual(result, "")

    def test_handles_missing_fields_gracefully(self):
        """Handles containers with missing fields using defaults."""
        metrics = {
            "timeline": {
                "containers": [
                    {"name": "output-sync-1"}  # Missing most fields
                ]
            }
        }
        result = generate_timeline_table(metrics)

        self.assertIn("output-sync-1", result)
        self.assertIn("unknown", result)  # Default type
        self.assertIn(":hourglass:", result)  # Missing end_sec treated as running
        self.assertIn("running", result)  # Shows "running" text for end time


if __name__ == "__main__":
    unittest.main(verbosity=2)
