#!/usr/bin/env python3
"""Unit tests for run_ci.py functions."""

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from run_ci import (
    Config,
    Timing,
    VerificationStats,
    ResourceStats,
    ContainerStatus,
    get_expected_sync_containers,
    parse_verification_line,
    aggregate_verification_stats,
    parse_container_status,
    calculate_durations,
    calculate_throughput,
    exit_code_to_status,
    build_metrics,
    CommandRunner,
    CIRunner,
)


class TestGetExpectedSyncContainers(unittest.TestCase):
    """Tests for get_expected_sync_containers function."""

    def test_kafka_returns_4(self):
        """Kafka uses 4 per-table sync containers."""
        result = get_expected_sync_containers("kafka")
        self.assertEqual(result, 4)

    def test_mysql_returns_1(self):
        """MySQL uses 1 sync container."""
        result = get_expected_sync_containers("mysql")
        self.assertEqual(result, 1)

    def test_postgresql_returns_1(self):
        """PostgreSQL uses 1 sync container."""
        result = get_expected_sync_containers("postgresql")
        self.assertEqual(result, 1)

    def test_mongodb_returns_1(self):
        """MongoDB uses 1 sync container."""
        result = get_expected_sync_containers("mongodb")
        self.assertEqual(result, 1)

    def test_neo4j_returns_1(self):
        """Neo4j uses 1 sync container."""
        result = get_expected_sync_containers("neo4j")
        self.assertEqual(result, 1)

    def test_unknown_returns_1(self):
        """Unknown sources default to 1 sync container."""
        result = get_expected_sync_containers("unknown")
        self.assertEqual(result, 1)


class TestParseVerificationLine(unittest.TestCase):
    """Tests for parse_verification_line function."""

    def test_success_line(self):
        """Parse a successful verification line."""
        line = "Verification complete: 100 rows verified in 40.515207ms - 100 matched, 0 missing, 0 mismatched"
        result = parse_verification_line(line)
        self.assertIsNotNone(result)
        self.assertEqual(result.matched, 100)
        self.assertEqual(result.missing, 0)
        self.assertEqual(result.mismatched, 0)

    def test_failure_line_with_missing(self):
        """Parse a line with missing rows."""
        line = "Verification complete: 100 rows verified in 6.087453ms - 0 matched, 100 missing, 0 mismatched"
        result = parse_verification_line(line)
        self.assertIsNotNone(result)
        self.assertEqual(result.matched, 0)
        self.assertEqual(result.missing, 100)
        self.assertEqual(result.mismatched, 0)

    def test_failure_line_with_mismatched(self):
        """Parse a line with mismatched rows."""
        line = "Verification complete: 100 rows verified in 5ms - 80 matched, 0 missing, 20 mismatched"
        result = parse_verification_line(line)
        self.assertIsNotNone(result)
        self.assertEqual(result.matched, 80)
        self.assertEqual(result.missing, 0)
        self.assertEqual(result.mismatched, 20)

    def test_non_matching_line(self):
        """Non-matching lines return None."""
        line = "Starting streaming verification of 100 rows for table 'users'"
        result = parse_verification_line(line)
        self.assertIsNone(result)

    def test_info_line(self):
        """Info lines without stats return None."""
        line = "[INFO] Verifying 100 rows per table in SurrealDB (seed=42)"
        result = parse_verification_line(line)
        self.assertIsNone(result)

    def test_partial_match(self):
        """Lines with only partial stats return None."""
        line = "100 matched rows found"
        result = parse_verification_line(line)
        self.assertIsNone(result)


class TestAggregateVerificationStats(unittest.TestCase):
    """Tests for aggregate_verification_stats function."""

    def test_aggregate_multiple_tables(self):
        """Aggregate stats from multiple tables."""
        lines = [
            "Table 'users': 100 matched, 0 missing, 0 mismatched",
            "Table 'products': 100 matched, 0 missing, 0 mismatched",
            "Table 'orders': 100 matched, 0 missing, 0 mismatched",
            "Table 'order_items': 100 matched, 0 missing, 0 mismatched",
        ]
        result = aggregate_verification_stats(lines)
        self.assertEqual(result.matched, 400)
        self.assertEqual(result.missing, 0)
        self.assertEqual(result.mismatched, 0)

    def test_aggregate_with_failures(self):
        """Aggregate stats including failures."""
        lines = [
            "Table 'users': 100 matched, 0 missing, 0 mismatched",
            "Table 'products': 0 matched, 100 missing, 0 mismatched",
        ]
        result = aggregate_verification_stats(lines)
        self.assertEqual(result.matched, 100)
        self.assertEqual(result.missing, 100)
        self.assertEqual(result.mismatched, 0)

    def test_aggregate_empty_lines(self):
        """Aggregate empty lines returns zeros."""
        result = aggregate_verification_stats([])
        self.assertEqual(result.matched, 0)
        self.assertEqual(result.missing, 0)
        self.assertEqual(result.mismatched, 0)

    def test_aggregate_with_noise(self):
        """Aggregate ignores non-matching lines."""
        lines = [
            "[INFO] Starting verification...",
            "Table 'users': 100 matched, 0 missing, 0 mismatched",
            "[INFO] Done",
        ]
        result = aggregate_verification_stats(lines)
        self.assertEqual(result.matched, 100)
        self.assertEqual(result.missing, 0)
        self.assertEqual(result.mismatched, 0)


class TestParseContainerStatus(unittest.TestCase):
    """Tests for parse_container_status function."""

    def test_all_completed(self):
        """Parse output with all containers completed."""
        ps_output = """
NAME                         STATUS
output-populate-1-1          Exited (0) 5 minutes ago
output-sync-users-1          Exited (0) 4 minutes ago
output-sync-products-1       Exited (0) 4 minutes ago
output-sync-orders-1         Exited (0) 4 minutes ago
output-sync-order_items-1    Exited (0) 4 minutes ago
output-verify-1-1            Exited (0) 3 minutes ago
"""
        result = parse_container_status(ps_output, workers=1, expected_sync=4)
        self.assertEqual(result.populate_done, 1)
        self.assertEqual(result.sync_done, 4)
        self.assertEqual(result.verify_done, 1)
        self.assertEqual(len(result.failed_containers), 0)

    def test_with_failures(self):
        """Parse output with failed containers."""
        ps_output = """
NAME                         STATUS
output-populate-1-1          Exited (0) 5 minutes ago
output-sync-users-1          Exited (1) 4 minutes ago
output-sync-products-1       Exited (0) 4 minutes ago
output-verify-1-1            Exited (0) 3 minutes ago
"""
        result = parse_container_status(ps_output, workers=1, expected_sync=2)
        self.assertEqual(result.populate_done, 1)
        self.assertEqual(result.sync_done, 1)
        self.assertEqual(result.verify_done, 1)
        self.assertIn("output-sync-users-1", result.failed_containers)

    def test_partial_completion(self):
        """Parse output with partial completion."""
        ps_output = """
NAME                         STATUS
output-populate-1-1          Exited (0) 5 minutes ago
output-sync-1-1              Up 4 minutes
output-verify-1-1            Created
"""
        result = parse_container_status(ps_output, workers=1, expected_sync=1)
        self.assertEqual(result.populate_done, 1)
        self.assertEqual(result.sync_done, 0)
        self.assertEqual(result.verify_done, 0)

    def test_multiple_workers(self):
        """Parse output with multiple workers."""
        ps_output = """
NAME                         STATUS
output-populate-1-1          Exited (0) 5 minutes ago
output-populate-2-1          Exited (0) 5 minutes ago
output-sync-1-1              Exited (0) 4 minutes ago
output-verify-1-1            Exited (0) 3 minutes ago
output-verify-2-1            Exited (0) 3 minutes ago
"""
        result = parse_container_status(ps_output, workers=2, expected_sync=1)
        self.assertEqual(result.populate_done, 2)
        self.assertEqual(result.sync_done, 1)
        self.assertEqual(result.verify_done, 2)


class TestCalculateDurations(unittest.TestCase):
    """Tests for calculate_durations function."""

    def test_complete_timing(self):
        """Calculate durations with all timing data."""
        timing = Timing(
            start=1000,
            populate_start=1000,
            populate_end=1010,
            sync_start=1010,
            sync_end=1040,
            verify_start=1040,
            verify_end=1045,
            end=1045
        )
        result = calculate_durations(timing)
        self.assertEqual(result["total"], 45)
        self.assertEqual(result["populate"], 10)
        self.assertEqual(result["sync"], 30)
        self.assertEqual(result["verify"], 5)

    def test_missing_populate_end(self):
        """Calculate durations when populate_end is missing."""
        timing = Timing(
            start=1000,
            populate_start=1000,
            end=1045
        )
        result = calculate_durations(timing)
        self.assertEqual(result["total"], 45)
        # When populate_end is missing, it uses end time
        self.assertEqual(result["populate"], 45)

    def test_zero_sync_duration_with_populate(self):
        """When sync duration is 0 but populate took time, estimate sync from remaining time."""
        timing = Timing(
            start=1000,
            populate_start=1000,
            populate_end=1010,
            sync_start=1010,
            sync_end=1010,  # Same as sync_start - suspicious
            verify_start=1010,
            verify_end=1045,
            end=1045
        )
        result = calculate_durations(timing)
        # total=45, populate=10, verify=35
        # estimated_sync = 45 - 10 - 35 = 0, so sync stays 0
        self.assertEqual(result["sync"], 0)

    def test_zero_sync_duration_estimated(self):
        """When sync duration is 0, estimate from total - populate - verify."""
        timing = Timing(
            start=1000,
            populate_start=1000,
            populate_end=1010,
            sync_start=1010,
            sync_end=1010,  # Same as sync_start - suspicious
            verify_start=1010,
            verify_end=1015,
            end=1045
        )
        result = calculate_durations(timing)
        # total=45, populate=10, verify=5
        # estimated_sync = 45 - 10 - 5 = 30
        self.assertEqual(result["sync"], 30)

    def test_raises_on_zero_start(self):
        """Raise ValueError if timing.start is 0."""
        timing = Timing(start=0, end=1045)
        with self.assertRaises(ValueError) as ctx:
            calculate_durations(timing)
        self.assertIn("timing.start is 0", str(ctx.exception))

    def test_raises_on_zero_end(self):
        """Raise ValueError if timing.end is 0."""
        timing = Timing(start=1000, end=0)
        with self.assertRaises(ValueError) as ctx:
            calculate_durations(timing)
        self.assertIn("timing.end is 0", str(ctx.exception))

    def test_raises_on_end_before_start(self):
        """Raise ValueError if timing.end is before timing.start."""
        timing = Timing(start=1045, end=1000)
        with self.assertRaises(ValueError) as ctx:
            calculate_durations(timing)
        self.assertIn("before timing.start", str(ctx.exception))


class TestCalculateThroughput(unittest.TestCase):
    """Tests for calculate_throughput function."""

    def test_normal_throughput(self):
        """Calculate normal throughput."""
        durations = {"total": 100, "sync": 40, "verify": 10, "populate": 50}
        result = calculate_throughput(row_count=100, durations=durations)
        # 400 rows / 40 seconds = 10 rows/sec
        self.assertEqual(result, 10.0)

    def test_zero_sync_uses_total(self):
        """When sync is 0, use total duration."""
        durations = {"total": 100, "sync": 0, "verify": 10, "populate": 50}
        result = calculate_throughput(row_count=100, durations=durations)
        # 400 rows / 100 seconds = 4 rows/sec
        self.assertEqual(result, 4.0)

    def test_zero_total_returns_zero(self):
        """When both are 0, return 0."""
        durations = {"total": 0, "sync": 0, "verify": 0, "populate": 0}
        result = calculate_throughput(row_count=100, durations=durations)
        self.assertEqual(result, 0.0)

    def test_high_throughput(self):
        """Calculate high throughput."""
        durations = {"total": 10, "sync": 2, "verify": 1, "populate": 7}
        result = calculate_throughput(row_count=1000, durations=durations)
        # 4000 rows / 2 seconds = 2000 rows/sec
        self.assertEqual(result, 2000.0)


class TestExitCodeToStatus(unittest.TestCase):
    """Tests for exit_code_to_status function."""

    def test_success(self):
        self.assertEqual(exit_code_to_status(0), "success")

    def test_timeout(self):
        self.assertEqual(exit_code_to_status(1), "timeout")

    def test_failure(self):
        self.assertEqual(exit_code_to_status(2), "failure")

    def test_verification_failed(self):
        self.assertEqual(exit_code_to_status(3), "verification_failed")

    def test_unknown(self):
        self.assertEqual(exit_code_to_status(99), "unknown")


class TestBuildMetrics(unittest.TestCase):
    """Tests for build_metrics function."""

    def test_success_metrics(self):
        """Build metrics for successful run."""
        config = Config(
            source="kafka",
            preset="small",
            row_count=100,
            workers=1
        )
        timing = Timing(
            start=1000,
            populate_start=1000,
            populate_end=1010,
            sync_start=1010,
            sync_end=1040,
            verify_start=1040,
            verify_end=1045,
            end=1045
        )
        verification = VerificationStats(matched=400, mismatched=0, missing=0)
        resources = ResourceStats(peak_memory_mb=512, avg_cpu_percent=45.2)

        result = build_metrics(config, timing, 0, verification, resources)

        self.assertEqual(result["source"], "kafka")
        self.assertEqual(result["preset"], "small")
        self.assertEqual(result["row_count"], 100)
        self.assertEqual(result["workers"], 1)
        self.assertEqual(result["results"]["status"], "success")
        self.assertEqual(result["results"]["exit_code"], 0)
        self.assertEqual(result["results"]["total_duration_seconds"], 45)
        self.assertEqual(result["verification"]["matched"], 400)
        self.assertEqual(result["verification"]["mismatched"], 0)
        self.assertEqual(result["resources"]["peak_memory_mb"], 512)

    def test_failure_metrics(self):
        """Build metrics for failed run."""
        config = Config(source="mysql", row_count=50)
        timing = Timing(start=1000, end=1100)
        verification = VerificationStats(matched=100, mismatched=50, missing=50)
        resources = ResourceStats()

        result = build_metrics(config, timing, 3, verification, resources)

        self.assertEqual(result["results"]["status"], "verification_failed")
        self.assertEqual(result["verification"]["mismatched"], 50)
        self.assertEqual(result["verification"]["missing"], 50)


class TestCIRunnerCheckVerificationResults(unittest.TestCase):
    """Tests for CIRunner.check_verification_results method."""

    def setUp(self):
        self.config = Config(source="kafka")
        self.runner = CIRunner(self.config)

    def test_all_pass(self):
        """All tables pass verification."""
        logs = [
            "100 matched, 0 missing, 0 mismatched",
            "100 matched, 0 missing, 0 mismatched",
            "100 matched, 0 missing, 0 mismatched",
            "100 matched, 0 missing, 0 mismatched",
        ]
        result = self.runner.check_verification_results(logs)
        self.assertTrue(result)

    def test_with_missing(self):
        """Some tables have missing rows."""
        logs = [
            "100 matched, 0 missing, 0 mismatched",
            "0 matched, 100 missing, 0 mismatched",
        ]
        result = self.runner.check_verification_results(logs)
        self.assertFalse(result)

    def test_with_mismatched(self):
        """Some tables have mismatched rows."""
        logs = [
            "80 matched, 0 missing, 20 mismatched",
        ]
        result = self.runner.check_verification_results(logs)
        self.assertFalse(result)

    def test_empty_logs(self):
        """Empty logs pass (no failures detected)."""
        result = self.runner.check_verification_results([])
        self.assertTrue(result)


class MockCommandRunner(CommandRunner):
    """Mock command runner for testing."""

    def __init__(self):
        self.commands = []
        self.results = {}

    def set_result(self, cmd_pattern: str, result: subprocess.CompletedProcess):
        """Set result for a command pattern."""
        self.results[cmd_pattern] = result

    def run(self, cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
        """Record and return mock result."""
        cmd_str = " ".join(cmd)
        self.commands.append(cmd_str)

        # Look for matching pattern
        for pattern, result in self.results.items():
            if pattern in cmd_str:
                return result

        # Default success result
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    def run_check(self, cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
        """Record and return mock result, raising on failure."""
        result = self.run(cmd, **kwargs)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, cmd)
        return result


class TestCIRunnerWithMock(unittest.TestCase):
    """Tests for CIRunner with mocked commands."""

    def setUp(self):
        self.config = Config(
            source="kafka",
            output_dir=Path("/tmp/test-output"),
            skip_build=True,
            skip_cleanup=True
        )
        self.mock_runner = MockCommandRunner()

    def test_get_container_status_calls_docker_compose(self):
        """get_container_status calls docker compose ps."""
        self.mock_runner.set_result(
            "docker compose",
            subprocess.CompletedProcess([], 0, stdout="output-sync-users-1  Exited (0)", stderr="")
        )

        runner = CIRunner(self.config, self.mock_runner)
        runner.compose_file = Path("/tmp/test/docker-compose.yml")
        status = runner.get_container_status()

        self.assertTrue(any("docker compose" in cmd for cmd in self.mock_runner.commands))
        self.assertEqual(status.sync_done, 1)


class TestCollectContainerLogs(unittest.TestCase):
    """Tests for CIRunner.collect_container_logs method."""

    def setUp(self):
        # Use unique temp directory for each test
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_logs_"))
        self.config = Config(
            source="kafka",
            output_dir=self.test_dir,
            skip_build=True,
            skip_cleanup=True
        )
        self.mock_runner = MockCommandRunner()

    def tearDown(self):
        # Clean up temp directory after each test
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_collect_logs_creates_directory_and_files(self):
        """collect_container_logs creates logs dir, containers.json, and log files."""
        # Mock docker compose ps
        self.mock_runner.set_result(
            "docker compose",
            subprocess.CompletedProcess([], 0, stdout="output-sync-1\noutput-verify-1\n", stderr="")
        )
        # Mock docker inspect
        self.mock_runner.set_result(
            "docker inspect",
            subprocess.CompletedProcess([], 0, stdout="exited|0|2026-01-13T00:00:00Z|2026-01-13T00:01:00Z", stderr="")
        )
        # Mock docker logs
        self.mock_runner.set_result(
            "docker logs",
            subprocess.CompletedProcess([], 0, stdout="test log output", stderr="test error output")
        )

        runner = CIRunner(self.config, self.mock_runner)
        runner.compose_file = self.test_dir / "docker-compose.yml"
        runner.collect_container_logs()

        logs_dir = self.test_dir / "logs"
        self.assertTrue(logs_dir.exists())
        self.assertTrue((logs_dir / "containers.json").exists())

        # Check containers.json content
        with open(logs_dir / "containers.json") as f:
            summary = json.load(f)
        self.assertEqual(summary["total_containers"], 2)
        self.assertEqual(len(summary["containers"]), 2)

    def test_collect_logs_separates_stdout_stderr(self):
        """collect_container_logs creates separate .stdout.log and .stderr.log files."""
        self.mock_runner.set_result(
            "docker compose",
            subprocess.CompletedProcess([], 0, stdout="output-populate-1\n", stderr="")
        )
        self.mock_runner.set_result(
            "docker inspect",
            subprocess.CompletedProcess([], 0, stdout="exited|0|2026-01-13T00:00:00Z|2026-01-13T00:01:00Z", stderr="")
        )
        self.mock_runner.set_result(
            "docker logs",
            subprocess.CompletedProcess([], 0, stdout="stdout content", stderr="stderr content")
        )

        runner = CIRunner(self.config, self.mock_runner)
        runner.compose_file = self.test_dir / "docker-compose.yml"
        runner.collect_container_logs()

        logs_dir = self.test_dir / "logs"
        stdout_file = logs_dir / "output-populate-1.stdout.log"
        stderr_file = logs_dir / "output-populate-1.stderr.log"

        self.assertTrue(stdout_file.exists())
        self.assertTrue(stderr_file.exists())
        self.assertEqual(stdout_file.read_text(), "stdout content")
        self.assertEqual(stderr_file.read_text(), "stderr content")

    def test_collect_logs_no_file_for_empty_output(self):
        """collect_container_logs doesn't create log file if output is empty."""
        self.mock_runner.set_result(
            "docker compose",
            subprocess.CompletedProcess([], 0, stdout="output-sync-1\n", stderr="")
        )
        self.mock_runner.set_result(
            "docker inspect",
            subprocess.CompletedProcess([], 0, stdout="exited|0|2026-01-13T00:00:00Z|2026-01-13T00:01:00Z", stderr="")
        )
        # Empty stdout and stderr
        self.mock_runner.set_result(
            "docker logs",
            subprocess.CompletedProcess([], 0, stdout="", stderr="")
        )

        runner = CIRunner(self.config, self.mock_runner)
        runner.compose_file = self.test_dir / "docker-compose.yml"
        runner.collect_container_logs()

        logs_dir = self.test_dir / "logs"
        # Neither file should exist
        self.assertFalse((logs_dir / "output-sync-1.stdout.log").exists())
        self.assertFalse((logs_dir / "output-sync-1.stderr.log").exists())

    def test_collect_logs_identifies_failed_containers(self):
        """collect_container_logs correctly identifies failed containers."""
        self.mock_runner.set_result(
            "docker compose",
            subprocess.CompletedProcess([], 0, stdout="output-sync-1\noutput-verify-1\n", stderr="")
        )
        # First container failed (exit code 1), second succeeded
        self.mock_runner.results["docker inspect output-sync-1"] = subprocess.CompletedProcess(
            [], 0, stdout="exited|1|2026-01-13T00:00:00Z|2026-01-13T00:01:00Z", stderr=""
        )
        self.mock_runner.results["docker inspect output-verify-1"] = subprocess.CompletedProcess(
            [], 0, stdout="exited|0|2026-01-13T00:00:00Z|2026-01-13T00:01:00Z", stderr=""
        )
        self.mock_runner.set_result(
            "docker logs",
            subprocess.CompletedProcess([], 0, stdout="log", stderr="")
        )

        runner = CIRunner(self.config, self.mock_runner)
        runner.compose_file = self.test_dir / "docker-compose.yml"
        runner.collect_container_logs()

        with open(self.test_dir / "logs" / "containers.json") as f:
            summary = json.load(f)

        self.assertEqual(summary["failed_containers"], 1)
        # Failed container should be first (sorted)
        self.assertEqual(summary["containers"][0]["name"], "output-sync-1")
        self.assertTrue(summary["containers"][0]["failed"])
        self.assertEqual(summary["containers"][0]["exit_code"], 1)

    def test_collect_logs_identifies_container_types(self):
        """collect_container_logs correctly identifies container types."""
        self.mock_runner.set_result(
            "docker compose",
            subprocess.CompletedProcess(
                [], 0,
                stdout="output-populate-1\noutput-sync-users\noutput-verify-1\noutput-kafka-1\noutput-surrealdb-1\n",
                stderr=""
            )
        )
        self.mock_runner.set_result(
            "docker inspect",
            subprocess.CompletedProcess([], 0, stdout="exited|0|2026-01-13T00:00:00Z|2026-01-13T00:01:00Z", stderr="")
        )
        self.mock_runner.set_result(
            "docker logs",
            subprocess.CompletedProcess([], 0, stdout="", stderr="")
        )

        runner = CIRunner(self.config, self.mock_runner)
        runner.compose_file = self.test_dir / "docker-compose.yml"
        runner.collect_container_logs()

        with open(self.test_dir / "logs" / "containers.json") as f:
            summary = json.load(f)

        types = {c["name"]: c["type"] for c in summary["containers"]}
        self.assertEqual(types["output-populate-1"], "populate")
        self.assertEqual(types["output-sync-users"], "sync")
        self.assertEqual(types["output-verify-1"], "verify")
        self.assertEqual(types["output-kafka-1"], "kafka")
        self.assertEqual(types["output-surrealdb-1"], "surrealdb")

    def test_collect_logs_handles_no_containers(self):
        """collect_container_logs handles case with no containers gracefully."""
        self.mock_runner.set_result(
            "docker compose",
            subprocess.CompletedProcess([], 0, stdout="", stderr="")
        )

        runner = CIRunner(self.config, self.mock_runner)
        runner.compose_file = self.test_dir / "docker-compose.yml"
        runner.collect_container_logs()

        # Should not create containers.json if no containers
        logs_dir = self.test_dir / "logs"
        self.assertTrue(logs_dir.exists())
        self.assertFalse((logs_dir / "containers.json").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
