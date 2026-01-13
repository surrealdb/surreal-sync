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
    VerificationStats,
    ResourceStats,
    ContainerStatus,
    get_expected_sync_containers,
    parse_verification_line,
    aggregate_verification_stats,
    parse_container_status,
    calculate_throughput,
    exit_code_to_status,
    build_metrics,
    parse_docker_memory,
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
        durations = {
            "total": 45.0,
            "populate": 10.0,
            "sync": 30.0,
            "verify": 5.0
        }
        verification = VerificationStats(matched=400, mismatched=0, missing=0)
        resources = ResourceStats(peak_memory_mb=512, avg_cpu_percent=45.2)

        result = build_metrics(config, durations, 0, verification, resources)

        self.assertEqual(result["source"], "kafka")
        self.assertEqual(result["preset"], "small")
        self.assertEqual(result["row_count"], 100)
        self.assertEqual(result["workers"], 1)
        self.assertEqual(result["results"]["status"], "success")
        self.assertEqual(result["results"]["exit_code"], 0)
        self.assertEqual(result["results"]["total_duration_seconds"], 45.0)
        self.assertEqual(result["verification"]["matched"], 400)
        self.assertEqual(result["verification"]["mismatched"], 0)
        self.assertEqual(result["resources"]["peak_memory_mb"], 512)

    def test_failure_metrics(self):
        """Build metrics for failed run."""
        config = Config(source="mysql", row_count=50)
        durations = {
            "total": 100.0,
            "populate": 0.0,
            "sync": 0.0,
            "verify": 0.0
        }
        verification = VerificationStats(matched=100, mismatched=50, missing=50)
        resources = ResourceStats()

        result = build_metrics(config, durations, 3, verification, resources)

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


class TestBuildTimeline(unittest.TestCase):
    """Tests for CIRunner.build_timeline method."""

    def setUp(self):
        # Use unique temp directory for each test
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_timeline_"))
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

    def test_build_timeline_with_valid_containers(self):
        """build_timeline returns sorted timeline with relative times and stripped prefix."""
        logs_dir = self.test_dir / "logs"
        logs_dir.mkdir(parents=True)

        containers_data = {
            "containers": [
                {
                    "name": "output-populate-1",
                    "type": "populate",
                    "started_at": "2026-01-13T00:00:05Z",
                    "finished_at": "2026-01-13T00:00:30Z",
                    "exit_code": 0
                },
                {
                    "name": "output-kafka-1",
                    "type": "kafka",
                    "started_at": "2026-01-13T00:00:00Z",  # Earliest
                    "finished_at": "2026-01-13T00:01:00Z",
                    "exit_code": 0
                },
                {
                    "name": "output-sync-users",
                    "type": "sync",
                    "started_at": "2026-01-13T00:00:31Z",
                    "finished_at": "2026-01-13T00:00:35Z",
                    "exit_code": 0
                }
            ]
        }

        with open(logs_dir / "containers.json", 'w') as f:
            json.dump(containers_data, f)

        runner = CIRunner(self.config, self.mock_runner)
        result = runner.build_timeline()

        self.assertIn("containers", result)
        containers = result["containers"]
        self.assertEqual(len(containers), 3)

        # Should be sorted by start time, with "output-" prefix stripped
        # (output_dir is self.test_dir which has "test_timeline_" prefix, not "output")
        # So names should retain full name since prefix doesn't match
        self.assertEqual(containers[0]["name"], "output-kafka-1")
        self.assertEqual(containers[0]["start_sec"], 0.0)

        # Populate started 5s after kafka
        self.assertEqual(containers[1]["name"], "output-populate-1")
        self.assertEqual(containers[1]["start_sec"], 5.0)
        self.assertEqual(containers[1]["duration_sec"], 25.0)

        # Sync started 31s after kafka
        self.assertEqual(containers[2]["name"], "output-sync-users")
        self.assertEqual(containers[2]["start_sec"], 31.0)
        self.assertEqual(containers[2]["duration_sec"], 4.0)

    def test_build_timeline_strips_project_prefix(self):
        """build_timeline strips Docker Compose project prefix from container names."""
        # Create a test dir named "output" to match the prefix
        output_dir = Path(tempfile.mkdtemp(prefix="")) / "output"
        output_dir.mkdir(parents=True)
        logs_dir = output_dir / "logs"
        logs_dir.mkdir()

        config = Config(
            source="kafka",
            output_dir=output_dir,
            skip_build=True,
            skip_cleanup=True
        )

        containers_data = {
            "containers": [
                {
                    "name": "output-kafka-1",
                    "type": "kafka",
                    "started_at": "2026-01-13T00:00:00Z",
                    "finished_at": "2026-01-13T00:01:00Z",
                    "exit_code": 0
                },
                {
                    "name": "output-sync-users",
                    "type": "sync",
                    "started_at": "2026-01-13T00:00:30Z",
                    "finished_at": "2026-01-13T00:00:35Z",
                    "exit_code": 0
                }
            ]
        }

        with open(logs_dir / "containers.json", 'w') as f:
            json.dump(containers_data, f)

        runner = CIRunner(config, self.mock_runner)
        result = runner.build_timeline()

        # Prefix "output-" should be stripped since output_dir.name is "output"
        names = [c["name"] for c in result["containers"]]
        self.assertIn("kafka-1", names)
        self.assertIn("sync-users", names)
        self.assertNotIn("output-kafka-1", names)
        self.assertNotIn("output-sync-users", names)

        # Cleanup
        import shutil
        shutil.rmtree(output_dir.parent)

    def test_build_timeline_no_containers_file(self):
        """build_timeline returns empty dict if containers.json missing."""
        runner = CIRunner(self.config, self.mock_runner)
        result = runner.build_timeline()
        self.assertEqual(result, {})

    def test_build_timeline_empty_containers(self):
        """build_timeline returns empty dict if no containers."""
        logs_dir = self.test_dir / "logs"
        logs_dir.mkdir(parents=True)

        with open(logs_dir / "containers.json", 'w') as f:
            json.dump({"containers": []}, f)

        runner = CIRunner(self.config, self.mock_runner)
        result = runner.build_timeline()
        self.assertEqual(result, {})

    def test_build_timeline_handles_missing_timestamps(self):
        """build_timeline handles containers with missing timestamps."""
        logs_dir = self.test_dir / "logs"
        logs_dir.mkdir(parents=True)

        containers_data = {
            "containers": [
                {
                    "name": "output-kafka-1",
                    "type": "kafka",
                    "started_at": "2026-01-13T00:00:00Z",
                    "finished_at": "2026-01-13T00:01:00Z",
                    "exit_code": 0
                },
                {
                    "name": "output-missing-1",
                    "type": "unknown",
                    "started_at": "",  # Missing
                    "finished_at": "",  # Missing
                    "exit_code": 0
                }
            ]
        }

        with open(logs_dir / "containers.json", 'w') as f:
            json.dump(containers_data, f)

        runner = CIRunner(self.config, self.mock_runner)
        result = runner.build_timeline()

        self.assertIn("containers", result)
        containers = result["containers"]
        self.assertEqual(len(containers), 2)

        # Container with missing timestamps should have None for end_sec and duration_sec
        missing_container = next(c for c in containers if c["name"] == "output-missing-1")
        self.assertEqual(missing_container["start_sec"], 0.0)
        self.assertIsNone(missing_container["end_sec"])
        self.assertIsNone(missing_container["duration_sec"])

    def test_build_timeline_preserves_exit_code(self):
        """build_timeline preserves exit_code from container info."""
        logs_dir = self.test_dir / "logs"
        logs_dir.mkdir(parents=True)

        containers_data = {
            "containers": [
                {
                    "name": "output-failed-1",
                    "type": "sync",
                    "started_at": "2026-01-13T00:00:00Z",
                    "finished_at": "2026-01-13T00:00:10Z",
                    "exit_code": 1
                }
            ]
        }

        with open(logs_dir / "containers.json", 'w') as f:
            json.dump(containers_data, f)

        runner = CIRunner(self.config, self.mock_runner)
        result = runner.build_timeline()

        self.assertEqual(result["containers"][0]["exit_code"], 1)


class TestParseDockerMemory(unittest.TestCase):
    """Tests for parse_docker_memory function."""

    def test_parse_mib(self):
        """Parse MiB memory string."""
        self.assertEqual(parse_docker_memory("256MiB"), 256)
        self.assertEqual(parse_docker_memory("1024MiB"), 1024)

    def test_parse_gib(self):
        """Parse GiB memory string and convert to MB."""
        self.assertEqual(parse_docker_memory("1GiB"), 1024)
        self.assertEqual(parse_docker_memory("1.5GiB"), 1536)
        self.assertEqual(parse_docker_memory("2GiB"), 2048)

    def test_parse_kib(self):
        """Parse KiB memory string and convert to MB."""
        self.assertEqual(parse_docker_memory("1024KiB"), 1)
        self.assertEqual(parse_docker_memory("512KiB"), 0)  # < 1 MB rounds to 0

    def test_parse_with_spaces(self):
        """Parse memory string with extra spaces."""
        self.assertEqual(parse_docker_memory("  256MiB  "), 256)
        self.assertEqual(parse_docker_memory("1 GiB"), 1024)

    def test_parse_lowercase(self):
        """Parse lowercase memory units."""
        self.assertEqual(parse_docker_memory("256mib"), 256)
        self.assertEqual(parse_docker_memory("1gib"), 1024)

    def test_parse_empty(self):
        """Return 0 for empty string."""
        self.assertEqual(parse_docker_memory(""), 0)
        self.assertEqual(parse_docker_memory("   "), 0)

    def test_parse_invalid(self):
        """Return 0 for invalid format."""
        self.assertEqual(parse_docker_memory("invalid"), 0)
        self.assertEqual(parse_docker_memory("256"), 0)
        self.assertEqual(parse_docker_memory("MB"), 0)


class TestCollectResourceStats(unittest.TestCase):
    """Tests for CIRunner.collect_resource_stats method."""

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_resources_"))
        self.config = Config(
            source="kafka",
            output_dir=self.test_dir,
            skip_build=True,
            skip_cleanup=True
        )

    def tearDown(self):
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_collect_resource_stats_parses_memory(self):
        """collect_resource_stats correctly parses docker stats output."""
        mock_runner = MockCommandRunner()
        mock_runner.set_result(
            "docker stats",
            subprocess.CompletedProcess(
                [], 0,
                stdout="256MiB / 8GiB\n512MiB / 8GiB\n",
                stderr=""
            )
        )

        runner = CIRunner(self.config, mock_runner)
        result = runner.collect_resource_stats()

        # Peak should be 512 MB (highest among containers)
        self.assertEqual(result.peak_memory_mb, 512)

    def test_collect_resource_stats_handles_gib(self):
        """collect_resource_stats correctly converts GiB to MB."""
        mock_runner = MockCommandRunner()
        mock_runner.set_result(
            "docker stats",
            subprocess.CompletedProcess(
                [], 0,
                stdout="1.5GiB / 8GiB\n",
                stderr=""
            )
        )

        runner = CIRunner(self.config, mock_runner)
        result = runner.collect_resource_stats()

        # 1.5 GiB = 1536 MB
        self.assertEqual(result.peak_memory_mb, 1536)

    def test_collect_resource_stats_handles_empty_output(self):
        """collect_resource_stats returns 0 when no containers running."""
        mock_runner = MockCommandRunner()
        mock_runner.set_result(
            "docker stats",
            subprocess.CompletedProcess([], 0, stdout="", stderr="")
        )

        runner = CIRunner(self.config, mock_runner)
        result = runner.collect_resource_stats()

        self.assertEqual(result.peak_memory_mb, 0)

    def test_collect_resource_stats_handles_command_failure(self):
        """collect_resource_stats returns 0 on docker stats failure."""
        mock_runner = MockCommandRunner()
        mock_runner.set_result(
            "docker stats",
            subprocess.CompletedProcess(
                [], 1,
                stdout="",
                stderr="Error: no containers running"
            )
        )

        runner = CIRunner(self.config, mock_runner)
        result = runner.collect_resource_stats()

        self.assertEqual(result.peak_memory_mb, 0)

    def test_collect_resource_stats_tracks_peak_across_calls(self):
        """Peak memory is tracked across multiple collection calls."""
        mock_runner = MockCommandRunner()
        runner = CIRunner(self.config, mock_runner)

        # First call: 256 MB
        mock_runner.set_result(
            "docker stats",
            subprocess.CompletedProcess([], 0, stdout="256MiB / 8GiB\n", stderr="")
        )
        runner.collect_resource_stats()

        # Second call: 512 MB (new peak)
        mock_runner.results.clear()
        mock_runner.set_result(
            "docker stats",
            subprocess.CompletedProcess([], 0, stdout="512MiB / 8GiB\n", stderr="")
        )
        runner.collect_resource_stats()

        # Third call: 128 MB (lower, shouldn't change peak)
        mock_runner.results.clear()
        mock_runner.set_result(
            "docker stats",
            subprocess.CompletedProcess([], 0, stdout="128MiB / 8GiB\n", stderr="")
        )
        result = runner.collect_resource_stats()

        # Peak should still be 512 MB
        self.assertEqual(result.peak_memory_mb, 512)


class TestBuildTimelineErrors(unittest.TestCase):
    """Tests for CIRunner.build_timeline error handling."""

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_timeline_errors_"))
        self.config = Config(
            source="kafka",
            output_dir=self.test_dir,
            skip_build=True,
            skip_cleanup=True
        )
        self.mock_runner = MockCommandRunner()

    def tearDown(self):
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_build_timeline_raises_on_invalid_started_at(self):
        """build_timeline raises RuntimeError on invalid started_at timestamp."""
        logs_dir = self.test_dir / "logs"
        logs_dir.mkdir(parents=True)

        containers_data = {
            "containers": [
                {
                    "name": "output-kafka-1",
                    "type": "kafka",
                    "started_at": "invalid-timestamp",
                    "finished_at": "2026-01-13T00:01:00Z",
                    "exit_code": 0
                }
            ]
        }

        with open(logs_dir / "containers.json", 'w') as f:
            json.dump(containers_data, f)

        runner = CIRunner(self.config, self.mock_runner)

        with self.assertRaises(RuntimeError) as ctx:
            runner.build_timeline()

        self.assertIn("output-kafka-1", str(ctx.exception))
        self.assertIn("started_at", str(ctx.exception))

    def test_build_timeline_raises_on_invalid_finished_at(self):
        """build_timeline raises RuntimeError on invalid finished_at timestamp."""
        logs_dir = self.test_dir / "logs"
        logs_dir.mkdir(parents=True)

        containers_data = {
            "containers": [
                {
                    "name": "output-kafka-1",
                    "type": "kafka",
                    "started_at": "2026-01-13T00:00:00Z",
                    "finished_at": "not-a-valid-date",
                    "exit_code": 0
                }
            ]
        }

        with open(logs_dir / "containers.json", 'w') as f:
            json.dump(containers_data, f)

        runner = CIRunner(self.config, self.mock_runner)

        with self.assertRaises(RuntimeError) as ctx:
            runner.build_timeline()

        self.assertIn("output-kafka-1", str(ctx.exception))
        self.assertIn("finished_at", str(ctx.exception))


class TestGetDurationsFromTimeline(unittest.TestCase):
    """Tests for CIRunner.get_durations_from_timeline method."""

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_timeline_dur_"))
        self.config = Config(
            source="kafka",
            output_dir=self.test_dir,
            skip_build=True,
            skip_cleanup=True
        )
        self.mock_runner = MockCommandRunner()

    def tearDown(self):
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_calculates_sync_duration_from_containers(self):
        """Sync duration is calculated from first sync start to last sync end."""
        runner = CIRunner(self.config, self.mock_runner)
        timeline = {
            "containers": [
                {"name": "populate-1", "type": "populate", "start_sec": 0.0, "end_sec": 5.0, "exit_code": 0},
                {"name": "sync-users", "type": "sync", "start_sec": 5.1, "end_sec": 6.7, "exit_code": 0},
                {"name": "sync-orders", "type": "sync", "start_sec": 5.2, "end_sec": 7.5, "exit_code": 0},
            ]
        }
        result = runner.get_durations_from_timeline(timeline)

        # Sync duration = 7.5 - 5.1 = 2.4 seconds
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result["sync"], 2.4, places=1)

    def test_raises_on_running_sync_containers(self):
        """Raises RuntimeError when sync containers haven't finished."""
        runner = CIRunner(self.config, self.mock_runner)
        timeline = {
            "containers": [
                {"name": "populate-1", "type": "populate", "start_sec": 0.0, "end_sec": 5.0, "exit_code": 0},
                {"name": "sync-users", "type": "sync", "start_sec": 5.1, "end_sec": None, "exit_code": -1},
            ]
        }

        with self.assertRaises(RuntimeError) as ctx:
            runner.get_durations_from_timeline(timeline)

        self.assertIn("have not finished", str(ctx.exception))

    def test_raises_on_empty_timeline(self):
        """Raises RuntimeError when timeline has no containers."""
        runner = CIRunner(self.config, self.mock_runner)

        with self.assertRaises(RuntimeError) as ctx:
            runner.get_durations_from_timeline({})

        self.assertIn("No container timeline data", str(ctx.exception))

    def test_raises_on_no_sync_containers(self):
        """Raises RuntimeError when there are no sync containers."""
        runner = CIRunner(self.config, self.mock_runner)
        timeline = {
            "containers": [
                {"name": "populate-1", "type": "populate", "start_sec": 0.0, "end_sec": 5.0, "exit_code": 0},
            ]
        }

        with self.assertRaises(RuntimeError) as ctx:
            runner.get_durations_from_timeline(timeline)

        self.assertIn("No sync containers", str(ctx.exception))

    def test_calculates_all_duration_phases(self):
        """All duration phases are calculated correctly."""
        runner = CIRunner(self.config, self.mock_runner)
        timeline = {
            "containers": [
                {"name": "kafka-1", "type": "kafka", "start_sec": 0.0, "end_sec": 60.0, "exit_code": 0},
                {"name": "populate-1", "type": "populate", "start_sec": 1.0, "end_sec": 10.0, "exit_code": 0},
                {"name": "sync-users", "type": "sync", "start_sec": 10.5, "end_sec": 20.0, "exit_code": 0},
                {"name": "verify-1", "type": "verify", "start_sec": 20.5, "end_sec": 25.0, "exit_code": 0},
            ]
        }
        result = runner.get_durations_from_timeline(timeline)

        self.assertEqual(result["total"], 60.0)  # Last container end
        self.assertEqual(result["populate"], 10.0)  # populate end_sec
        self.assertAlmostEqual(result["sync"], 9.5, places=1)  # 20.0 - 10.5
        self.assertAlmostEqual(result["verify"], 4.5, places=1)  # 25.0 - 20.5


class TestBuildTimelineRunningContainers(unittest.TestCase):
    """Tests for running container handling in build_timeline."""

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="test_running_"))
        self.logs_dir = self.test_dir / "logs"
        self.logs_dir.mkdir(parents=True)
        self.config = Config(
            source="kafka",
            output_dir=self.test_dir,
            skip_build=True,
            skip_cleanup=True
        )
        self.mock_runner = MockCommandRunner()

    def tearDown(self):
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_running_container_has_none_end_time(self):
        """Running containers have None for end_sec and duration_sec."""
        # Create containers.json with running container (Docker's zero timestamp)
        containers_data = {
            "containers": [
                {
                    "name": "output-sync-1",
                    "type": "sync",
                    "started_at": "2024-01-15T10:00:00Z",
                    "finished_at": "0001-01-01T00:00:00Z",  # Docker's "still running" value
                    "exit_code": -1
                }
            ]
        }
        with open(self.logs_dir / "containers.json", "w") as f:
            json.dump(containers_data, f)

        runner = CIRunner(self.config, self.mock_runner)
        timeline = runner.build_timeline()

        self.assertEqual(len(timeline["containers"]), 1)
        container = timeline["containers"][0]
        self.assertIsNone(container["end_sec"])
        self.assertIsNone(container["duration_sec"])

    def test_completed_container_has_numeric_end_time(self):
        """Completed containers have numeric end_sec and duration_sec."""
        containers_data = {
            "containers": [
                {
                    "name": "output-sync-1",
                    "type": "sync",
                    "started_at": "2024-01-15T10:00:00Z",
                    "finished_at": "2024-01-15T10:00:30Z",
                    "exit_code": 0
                }
            ]
        }
        with open(self.logs_dir / "containers.json", "w") as f:
            json.dump(containers_data, f)

        runner = CIRunner(self.config, self.mock_runner)
        timeline = runner.build_timeline()

        container = timeline["containers"][0]
        self.assertEqual(container["end_sec"], 30.0)
        self.assertEqual(container["duration_sec"], 30.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
