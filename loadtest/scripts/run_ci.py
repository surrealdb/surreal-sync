#!/usr/bin/env python3
"""
CI runner script for loadtest with JSON metrics output.

Usage: ./scripts/run_ci.py [OPTIONS]

Options:
  --source SOURCE       Data source (default: kafka)
  --preset PRESET       Size preset: small, medium, large (default: small)
  --row-count COUNT     Number of rows per table (default: from preset)
  --workers COUNT       Number of worker containers (default: 1)
  --timeout SECONDS     Timeout in seconds (default: 300)
  --output-dir DIR      Output directory (default: ./output)
  --skip-build          Skip docker build step
  --skip-cleanup        Skip initial cleanup of previous resources
  --preserve-on-failure Keep resources on failure for debugging
  --help                Show this help message

Exit codes:
  0 = Success (all tests passed)
  1 = Timeout
  2 = Container failure
  3 = Verification mismatch

On failure with --preserve-on-failure, resources are kept for debugging.
Run ./scripts/cleanup.sh to remove them manually.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional


def parse_docker_memory(mem_str: str) -> int:
    """Parse docker memory string like '256MiB' to MB.

    Args:
        mem_str: Memory string from docker stats (e.g., "256MiB", "1.5GiB", "512KiB")

    Returns:
        Memory in MB (integer), 0 if cannot parse
    """
    mem_str = mem_str.strip()
    if not mem_str:
        return 0

    # Extract numeric value and unit
    match = re.match(r'([\d.]+)\s*([KMGT]i?B)', mem_str, re.IGNORECASE)
    if not match:
        return 0

    value = float(match.group(1))
    unit = match.group(2).upper()

    # Convert to MB
    if unit in ('KIB', 'KB'):
        return int(value / 1024)
    elif unit in ('MIB', 'MB'):
        return int(value)
    elif unit in ('GIB', 'GB'):
        return int(value * 1024)
    elif unit in ('TIB', 'TB'):
        return int(value * 1024 * 1024)

    return 0


def check_prerequisites() -> list[str]:
    """Check that required binaries are available.

    Returns:
        List of error messages for missing prerequisites. Empty if all ok.
    """
    errors = []

    # Check for docker
    if not shutil.which("docker"):
        errors.append("'docker' not found in PATH. Please install Docker.")

    # Check for docker compose (v2) - it's a subcommand, not a separate binary
    if shutil.which("docker"):
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            errors.append(
                "'docker compose' (v2) not available. "
                "Please install Docker Compose v2 plugin."
            )

    # Check for git (optional, for git info in metrics)
    # Don't error, just warn
    if not shutil.which("git"):
        print("Warning: 'git' not found in PATH. Git info will be empty in metrics.",
              file=sys.stderr)

    return errors


@dataclass
class Config:
    """Input configuration for the CI runner (from CLI arguments)."""
    source: str = "kafka"
    preset: str = "small"
    row_count: Optional[int] = None  # None means use preset default
    workers: int = 1
    timeout: int = 300
    output_dir: Path = field(default_factory=lambda: Path("./output"))
    skip_build: bool = False
    skip_cleanup: bool = False
    preserve_on_failure: bool = True
    poll_interval: int = 5
    image_name: str = "surreal-sync:latest"
    project_root: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent)
    loadtest_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent)


@dataclass
class GeneratorConfig:
    """Resolved configuration from the generator (what was actually used)."""
    row_count: int
    batch_size: int
    num_containers: int
    tables: list  # List of table names from ClusterConfig


@dataclass
class VerificationStats:
    """Verification statistics."""
    matched: int = 0
    mismatched: int = 0
    missing: int = 0


@dataclass
class ResourceStats:
    """Resource usage statistics."""
    peak_memory_mb: float = 0
    avg_cpu_percent: float = 0


@dataclass
class ContainerStatus:
    """Status of container completion."""
    populate_done: int = 0
    sync_done: int = 0
    verify_done: int = 0
    failed_containers: list = field(default_factory=list)


def get_expected_sync_containers(source: str) -> int:
    """Get the expected number of sync containers based on source type.

    Kafka uses per-table sync containers (4 tables = 4 containers).
    Other sources use a single sync container.

    Args:
        source: The data source type

    Returns:
        Number of expected sync containers
    """
    if source == "kafka":
        return 4  # sync-users, sync-products, sync-orders, sync-order_items
    return 1


def parse_verification_line(line: str) -> Optional[VerificationStats]:
    """Parse a verification log line to extract stats.

    Expected format: "N rows verified in Xms - N matched, N missing, N mismatched"

    Args:
        line: A log line from verification output

    Returns:
        VerificationStats if the line contains stats, None otherwise
    """
    # Match pattern: "N matched, N missing, N mismatched"
    match = re.search(r'(\d+)\s+matched,\s+(\d+)\s+missing,\s+(\d+)\s+mismatched', line)
    if match:
        return VerificationStats(
            matched=int(match.group(1)),
            missing=int(match.group(2)),
            mismatched=int(match.group(3))
        )
    return None


def aggregate_verification_stats(log_lines: list[str]) -> VerificationStats:
    """Aggregate verification stats from multiple log lines.

    Args:
        log_lines: List of log lines from verification containers

    Returns:
        Aggregated VerificationStats
    """
    total = VerificationStats()
    for line in log_lines:
        stats = parse_verification_line(line)
        if stats:
            total.matched += stats.matched
            total.missing += stats.missing
            total.mismatched += stats.mismatched
    return total


def parse_container_status(ps_output: str, workers: int, expected_sync: int) -> ContainerStatus:
    """Parse docker-compose ps output to determine container status.

    Args:
        ps_output: Output from docker-compose ps -a
        workers: Expected number of worker containers
        expected_sync: Expected number of sync containers

    Returns:
        ContainerStatus with counts and failed container names
    """
    status = ContainerStatus()

    for line in ps_output.splitlines():
        # Check for populate containers
        if re.search(r'output-populate-\d+-1', line):
            if 'Exited (0)' in line:
                status.populate_done += 1
            elif re.search(r'Exited \([1-9]', line):
                name = line.split()[0] if line.split() else "unknown"
                status.failed_containers.append(name)

        # Check for sync containers (both patterns: sync-1 and sync-tablename)
        if re.search(r'output-sync-', line):
            if 'Exited (0)' in line:
                status.sync_done += 1
            elif re.search(r'Exited \([1-9]', line):
                name = line.split()[0] if line.split() else "unknown"
                status.failed_containers.append(name)

        # Check for verify containers
        if re.search(r'output-verify-\d+-1', line):
            if 'Exited (0)' in line:
                status.verify_done += 1
            elif re.search(r'Exited \([1-9]', line):
                name = line.split()[0] if line.split() else "unknown"
                status.failed_containers.append(name)

    return status


def calculate_throughput(row_count: int, num_tables: int, durations: dict) -> float:
    """Calculate throughput in rows per second.

    Args:
        row_count: Number of rows per table
        num_tables: Number of tables
        durations: Dictionary with duration values

    Returns:
        Throughput in rows per second (total rows / sync duration)
    """
    total_rows = row_count * num_tables
    # Use sync_duration if available, otherwise total_duration
    effective_duration = durations["sync"] if durations["sync"] > 0 else durations["total"]
    if effective_duration > 0:
        return round(total_rows / effective_duration, 1)
    return 0.0


def get_git_info() -> tuple[str, str]:
    """Get git SHA and ref from the repository.

    Returns:
        Tuple of (git_sha, git_ref)
    """
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True
        ).stdout.strip()
        ref = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, check=True
        ).stdout.strip()
        return sha, ref
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "", ""


def exit_code_to_status(exit_code: int) -> str:
    """Convert exit code to status string.

    Exit codes:
        0: success - All containers completed, verification passed
        1: timeout - Test did not complete within timeout
        2: failure - Infrastructure failure (populate/sync container crashed)
        3: verification_failed - Verify container detected missing/mismatched data

    Args:
        exit_code: The exit code from the test

    Returns:
        Status string
    """
    status_map = {
        0: "success",
        1: "timeout",
        2: "failure",
        3: "verification_failed"
    }
    return status_map.get(exit_code, "unknown")


def build_metrics(
    config: Config,
    generator_config: GeneratorConfig,
    durations: dict,
    exit_code: int,
    verification: VerificationStats,
    resources: ResourceStats
) -> dict:
    """Build the metrics dictionary.

    Args:
        config: Input configuration from CLI arguments
        generator_config: Resolved configuration from the generator
        durations: Pre-computed durations from timeline (total, populate, sync, verify)
        exit_code: Exit code from the test
        verification: Verification statistics
        resources: Resource usage statistics

    Returns:
        Metrics dictionary ready for JSON serialization
    """
    git_sha, git_ref = get_git_info()

    num_tables = len(generator_config.tables)
    total_rows = generator_config.row_count * num_tables

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "git_sha": git_sha,
        "git_ref": git_ref,
        "source": config.source,
        "preset": config.preset,
        "row_count_per_table": generator_config.row_count,
        "num_tables": num_tables,
        "tables": generator_config.tables,
        "workers": config.workers,
        "platform": "docker-compose",
        "runner": os.environ.get("GITHUB_RUNNER", "local"),
        "results": {
            "status": exit_code_to_status(exit_code),
            "exit_code": exit_code,
            "total_duration_seconds": durations["total"],
            "populate_duration_seconds": durations["populate"],
            "sync_duration_seconds": durations["sync"],
            "verify_duration_seconds": durations["verify"],
            "total_rows_synced": total_rows,
            "throughput_total_rows_per_sec": calculate_throughput(
                generator_config.row_count, num_tables, durations
            )
        },
        "resources": {
            "peak_memory_mb": resources.peak_memory_mb,
            "avg_cpu_percent": resources.avg_cpu_percent
        },
        "verification": {
            "matched": verification.matched,
            "mismatched": verification.mismatched,
            "missing": verification.missing
        }
    }


class CommandRunner:
    """Interface for running shell commands (can be mocked for testing)."""

    def run(self, cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
        """Run a command and return the result."""
        return subprocess.run(cmd, **kwargs)

    def run_check(self, cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
        """Run a command and raise on failure."""
        return subprocess.run(cmd, check=True, **kwargs)


class CIRunner:
    """Main CI runner class."""

    def __init__(self, config: Config, runner: Optional[CommandRunner] = None):
        self.config = config
        self.runner = runner or CommandRunner()
        self.test_failed = False
        self.expected_sync_containers = get_expected_sync_containers(config.source)
        self.compose_file = config.output_dir / "docker-compose.loadtest.yml"
        self._start_time: float = 0  # Track test start time for duration logging
        self._peak_memory_mb = 0  # Track peak memory across all samples
        self.generator_config: Optional[GeneratorConfig] = None  # Set after generate_config()

    def log(self, message: str):
        """Log a message with timestamp."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")

    def log_error(self, message: str):
        """Log an error message with timestamp."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] ERROR: {message}", file=sys.stderr)

    def docker_compose(self, *args, capture_output: bool = False) -> subprocess.CompletedProcess:
        """Run a docker compose command (v2 syntax)."""
        cmd = ["docker", "compose", "-f", str(self.compose_file)] + list(args)
        return self.runner.run(
            cmd,
            capture_output=capture_output,
            text=True
        )

    def initial_cleanup(self):
        """Clean up any previous failed resources."""
        if self.config.skip_cleanup:
            self.log("Skipping initial cleanup (--skip-cleanup)")
            return

        self.log("Cleaning up any previous failed resources...")

        # Clean docker compose resources
        if self.compose_file.exists():
            try:
                self.docker_compose("down", "-v", "--remove-orphans")
            except Exception as e:
                # Non-fatal - old compose file may reference non-existent resources
                self.log(f"Initial cleanup warning: {e}")

        # Clean orphaned containers
        result = self.runner.run(
            ["docker", "ps", "-aq", "--filter", "label=com.surreal-loadtest"],
            capture_output=True, text=True
        )
        if result.stdout.strip():
            containers = result.stdout.strip().split('\n')
            self.runner.run(["docker", "rm", "-f"] + containers)

        # Clean networks and volumes
        self.runner.run(["docker", "network", "rm", "loadtest_default"], capture_output=True)
        self.runner.run(
            ["docker", "network", "prune", "-f", "--filter", "label=com.surreal-loadtest"],
            capture_output=True
        )
        self.runner.run(
            ["docker", "volume", "prune", "-f", "--filter", "label=com.surreal-loadtest"],
            capture_output=True
        )

        self.log("Initial cleanup complete")

    def build_docker_image(self):
        """Build the Docker image."""
        if self.config.skip_build:
            self.log("Skipping docker build (--skip-build)")
            return

        self.log("Building Docker image...")
        self.runner.run_check([
            "docker", "build", "-t", self.config.image_name,
            "-f", str(self.config.project_root / "Dockerfile"),
            str(self.config.project_root)
        ])

    def generate_config(self):
        """Generate docker-compose configuration using Docker image.

        Uses the surreal-sync Docker image to run config generation,
        avoiding the need for cargo/rust on the host.

        Raises:
            FileNotFoundError: If required input files don't exist
            RuntimeError: If configuration generation fails
        """
        # Check input files exist
        schema_file = self.config.loadtest_dir / "config" / "schema.yaml"
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        self.log("Generating docker-compose configuration using Docker...")

        # Ensure output directory exists for volume mount
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

        # Use Docker to run config generation
        # Mount loadtest dir (for schema.yaml) and output dir (for generated files)
        cmd = [
            "docker", "run", "--rm",
            "-v", f"{self.config.loadtest_dir}/config:/config:ro",
            "-v", f"{self.config.output_dir}:/output",
            self.config.image_name,
            "loadtest", "generate",
            "--platform", "docker-compose",
            "--source", self.config.source,
            "--preset", self.config.preset,
            "--schema", "/config/schema.yaml",
            "--output-dir", "/output",
            "--workers", str(self.config.workers),
        ]
        # Only add --row-count if explicitly specified (otherwise use preset default)
        if self.config.row_count is not None:
            cmd.extend(["--row-count", str(self.config.row_count)])

        result = self.runner.run(cmd, capture_output=True)
        if result.returncode != 0:
            # Log stderr on failure before raising
            if result.stderr:
                self.log(f"Generator stderr: {result.stderr}")
            raise RuntimeError("Failed to generate configuration")

        # Parse JSON output from generator to create GeneratorConfig
        try:
            # The generator outputs ClusterConfig JSON on the first line of stdout
            stdout = result.stdout.decode("utf-8") if result.stdout else ""
            for line in stdout.split('\n'):
                line = line.strip()
                if line.startswith('{'):
                    config_json = json.loads(line)
                    # Extract values from containers (all containers have same row_count/batch_size)
                    if config_json.get("containers"):
                        container = config_json["containers"][0]
                        # Collect all unique table names from all containers
                        all_tables = []
                        for c in config_json["containers"]:
                            all_tables.extend(c.get("tables", []))
                        # Remove duplicates while preserving order
                        tables = list(dict.fromkeys(all_tables))
                        self.generator_config = GeneratorConfig(
                            row_count=container["row_count"],
                            batch_size=container.get("batch_size", 1000),
                            num_containers=len(config_json["containers"]),
                            tables=tables
                        )
                        self.log(f"Generator config: row_count={self.generator_config.row_count}, "
                                f"batch_size={self.generator_config.batch_size}, "
                                f"num_containers={self.generator_config.num_containers}, "
                                f"tables={self.generator_config.tables}")
                    break
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            self.log(f"Warning: Could not parse generator output: {e}")

        # Verify output files were created
        compose_file = self.config.output_dir / "docker-compose.loadtest.yml"
        if not compose_file.exists():
            raise RuntimeError(f"docker-compose file was not created: {compose_file}")

        # For kafka, verify proto files were generated
        if self.config.source == "kafka":
            proto_dir = self.config.output_dir / "config" / "proto"
            if not proto_dir.exists():
                raise RuntimeError(f"Proto directory was not created for kafka: {proto_dir}")
            proto_files = list(proto_dir.glob("*.proto"))
            if not proto_files:
                raise RuntimeError(f"No proto files found in {proto_dir}")
            self.log(f"Generated {len(proto_files)} proto files for kafka sync")

    def start_containers(self):
        """Start the docker-compose containers."""
        self.log("Starting containers...")
        self.docker_compose("up", "-d", "--build")

    def get_container_status(self) -> ContainerStatus:
        """Get the current status of containers."""
        result = self.docker_compose("ps", "-a", capture_output=True)
        return parse_container_status(
            result.stdout,
            self.config.workers,
            self.expected_sync_containers
        )

    def get_verification_logs(self) -> list[str]:
        """Get verification container logs.

        Returns:
            List of log lines from all verification containers
        """
        # Get logs from all verify containers dynamically
        # First, list containers matching the verify pattern
        ps_result = self.docker_compose("ps", "-a", "--format", "{{.Names}}", capture_output=True)
        all_containers = ps_result.stdout.strip().splitlines()
        verify_containers = [c for c in all_containers if 'verify' in c.lower()]

        if not verify_containers:
            raise RuntimeError(
                "No verification containers found. Expected containers with 'verify' in name. "
                f"Available containers: {all_containers}"
            )

        all_logs = []
        for container_name in verify_containers:
            # Use docker logs directly to get container logs
            result = self.runner.run(
                ["docker", "logs", container_name],
                capture_output=True, text=True
            )
            logs = (result.stdout + result.stderr).splitlines()
            all_logs.extend(logs)

        return all_logs

    def check_verification_results(self, verify_logs: list[str]) -> bool:
        """Check if verification passed.

        Args:
            verify_logs: List of verification log lines

        Returns:
            True if verification passed, False if mismatches detected
        """
        for line in verify_logs:
            # Check for non-zero mismatched count in the pattern
            match = re.search(r'(\d+)\s+matched,\s+(\d+)\s+missing,\s+(\d+)\s+mismatched', line)
            if match:
                missing = int(match.group(2))
                mismatched = int(match.group(3))
                if missing > 0 or mismatched > 0:
                    return False
        return True

    def wait_for_completion(self) -> tuple[int, VerificationStats]:
        """Wait for containers to complete.

        Returns:
            Tuple of (exit_code, verification_stats)
            Exit code: 0=success, 1=timeout, 2=failure, 3=verification_failed
        """
        start_time = time.time()

        while True:
            elapsed = int(time.time() - start_time)
            status = self.get_container_status()

            # Check for failures
            if status.failed_containers:
                # Distinguish between verify container failures (expected when data mismatches)
                # and other container failures (unexpected infrastructure issues)
                verify_failures = [c for c in status.failed_containers if 'verify' in c]
                other_failures = [c for c in status.failed_containers if 'verify' not in c]

                if other_failures:
                    # Populate or sync containers failed - this is a real infrastructure failure
                    print()  # Newline after progress
                    self.log_error("FAILURE: Container(s) exited with non-zero status")
                    self._report_container_failure(other_failures, status)
                    self.test_failed = True
                    # Return empty verification stats - verify containers may not have run
                    return 2, VerificationStats()

                if verify_failures:
                    # Capture verification stats - verify containers ran but found mismatches
                    verification = self.get_verification_stats()
                    # Verify container failed - this means data verification failed
                    # This is expected behavior when there are missing/mismatched rows
                    print()  # Newline after progress
                    self.log_error("VERIFICATION FAILED: Data mismatches detected")
                    for name in verify_failures[:5]:
                        result = self.runner.run(
                            ["docker", "logs", name, "--tail", "50"],
                            capture_output=True, text=True
                        )
                        self.log(f"Verification logs from: {name}")
                        print(result.stdout + result.stderr)
                    self.test_failed = True
                    return 3, verification

            # Progress display
            print(
                f"\r[{elapsed:3d}s] Populate: {status.populate_done}/{self.config.workers}, "
                f"Sync: {status.sync_done}/{self.expected_sync_containers}, "
                f"Verify: {status.verify_done}/{self.config.workers}",
                end="", flush=True
            )

            # Check for completion
            if (status.sync_done >= self.expected_sync_containers and
                status.verify_done >= self.config.workers):
                print()  # Newline after progress

                # Capture verification stats and check results
                verification = self.get_verification_stats()
                verify_logs = self.get_verification_logs()
                if not self.check_verification_results(verify_logs):
                    self.log_error("VERIFICATION FAILED: Data mismatches detected")
                    self.test_failed = True
                    return 3, verification

                self.log("SUCCESS: All tables verified successfully")
                return 0, verification

            # Check timeout
            if elapsed >= self.config.timeout:
                print()  # Newline after progress
                self.log_error(f"TIMEOUT: Test did not complete within {self.config.timeout} seconds")
                self._report_timeout_failure(status)
                self.test_failed = True
                # Return empty verification stats - verify containers may not have completed
                return 1, VerificationStats()

            # Collect resource stats periodically (every poll)
            self.collect_resource_stats()

            time.sleep(self.config.poll_interval)

    def _report_container_failure(self, failed_containers: list[str], status: ContainerStatus):
        """Report detailed information about container failures.

        Args:
            failed_containers: List of failed container names
            status: Current container status
        """
        print()
        print("=" * 70)
        print("CONTAINER FAILURE REPORT")
        print("=" * 70)

        # Test configuration
        print(f"\nTest configuration:")
        print(f"  Source: {self.config.source}")
        print(f"  Preset: {self.config.preset}")
        if self.generator_config:
            print(f"  Row count per table: {self.generator_config.row_count}")
            print(f"  Tables: {self.generator_config.tables}")

        # Progress summary
        print(f"\nProgress at failure:")
        print(f"  Populate: {status.populate_done}/{self.config.workers}")
        print(f"  Sync: {status.sync_done}/{self.expected_sync_containers}")
        print(f"  Verify: {status.verify_done}/{self.config.workers}")

        # Check infrastructure containers (database, surrealdb)
        print("\nInfrastructure container status:")
        ps_result = self.docker_compose("ps", "-a", "--format", "{{.Names}}|{{.Status}}",
                                         capture_output=True)
        for line in ps_result.stdout.strip().splitlines():
            if "|" in line:
                name, container_status = line.split("|", 1)
                # Check for infrastructure containers
                if any(x in name.lower() for x in [self.config.source, "surrealdb", "kafka", "mysql",
                                                    "postgresql", "mongodb", "neo4j"]):
                    print(f"  {name}: {container_status}")

        # Failed containers detail
        print(f"\nFailed containers ({len(failed_containers)}):")
        for name in failed_containers:
            # Get exit code
            inspect_result = self.runner.run(
                ["docker", "inspect", name, "--format",
                 "{{.State.ExitCode}}|{{.State.Status}}|{{.State.OOMKilled}}"],
                capture_output=True, text=True
            )
            exit_code = "unknown"
            oom_killed = False
            if inspect_result.returncode == 0:
                parts = inspect_result.stdout.strip().split("|")
                if len(parts) >= 3:
                    exit_code = parts[0]
                    oom_killed = parts[2].lower() == "true"

            oom_note = " (OOM KILLED)" if oom_killed else ""
            print(f"\n  Container: {name}")
            print(f"  Exit code: {exit_code}{oom_note}")

            # Get last 100 lines of logs
            result = self.runner.run(
                ["docker", "logs", name, "--tail", "100"],
                capture_output=True, text=True
            )

            print(f"  Last 100 lines of logs:")
            print("  " + "-" * 60)
            combined_output = (result.stdout or "") + (result.stderr or "")
            for line in combined_output.strip().split("\n")[-100:]:
                print(f"    {line}")
            print("  " + "-" * 60)

        # Debugging guidance
        print("\n" + "=" * 70)
        print("DEBUGGING GUIDANCE")
        print("=" * 70)
        print("""
To investigate further:
1. Check the full container logs in the 'container-logs-<source>' artifact
2. Look for OOM kills: containers may need more memory for large datasets
3. Check database connectivity: source/SurrealDB may have failed to start
4. Review the docker-compose.loadtest.yml for configuration issues

Common failure causes:
- OOM (Out of Memory): Increase memory limits or reduce row_count
- Connection timeout: Database containers may need longer startup time
- Schema mismatch: Check populate container logs for DDL errors
- Disk space: Large datasets may exhaust tmpfs storage
""")
        print("=" * 70)

    def _report_timeout_failure(self, status: ContainerStatus):
        """Report detailed information about timeout failures.

        Args:
            status: Current container status at timeout
        """
        print()
        print("=" * 70)
        print("TIMEOUT FAILURE REPORT")
        print("=" * 70)

        # Progress summary
        print(f"\nProgress at timeout (after {self.config.timeout}s):")
        print(f"  Populate: {status.populate_done}/{self.config.workers}")
        print(f"  Sync: {status.sync_done}/{self.expected_sync_containers}")
        print(f"  Verify: {status.verify_done}/{self.config.workers}")

        # Determine which phase timed out
        if status.populate_done < self.config.workers:
            stuck_phase = "populate"
            print("\n  ⚠ STUCK IN: Populate phase")
        elif status.sync_done < self.expected_sync_containers:
            stuck_phase = "sync"
            print("\n  ⚠ STUCK IN: Sync phase")
        else:
            stuck_phase = "verify"
            print("\n  ⚠ STUCK IN: Verify phase")

        # Get running containers
        ps_result = self.docker_compose("ps", "--filter", "status=running",
                                         "--format", "{{.Names}}", capture_output=True)
        running = [c.strip() for c in ps_result.stdout.strip().splitlines() if c.strip()]

        if running:
            print(f"\nStill running containers ({len(running)}):")
            for name in running[:10]:
                # Get resource usage
                stats_result = self.runner.run(
                    ["docker", "stats", name, "--no-stream",
                     "--format", "{{.CPUPerc}}|{{.MemUsage}}"],
                    capture_output=True, text=True
                )
                cpu = mem = "unknown"
                if stats_result.returncode == 0 and stats_result.stdout.strip():
                    parts = stats_result.stdout.strip().split("|")
                    if len(parts) >= 2:
                        cpu = parts[0]
                        mem = parts[1]

                print(f"\n  Container: {name}")
                print(f"  CPU: {cpu}, Memory: {mem}")

                # Get last 50 lines of logs
                result = self.runner.run(
                    ["docker", "logs", name, "--tail", "50"],
                    capture_output=True, text=True
                )
                print(f"  Last 50 lines of logs:")
                print("  " + "-" * 60)
                combined_output = (result.stdout or "") + (result.stderr or "")
                for line in combined_output.strip().split("\n")[-50:]:
                    print(f"    {line}")
                print("  " + "-" * 60)

        # Debugging guidance
        print("\n" + "=" * 70)
        print("DEBUGGING GUIDANCE")
        print("=" * 70)
        print(f"""
The test timed out during the {stuck_phase} phase.

To investigate further:
1. Check the full container logs in the 'container-logs-<source>' artifact
2. Review the resource usage above - high CPU may indicate processing
3. Low CPU with no progress may indicate a deadlock or waiting condition

Common timeout causes:
- Large dataset with insufficient timeout: Increase --timeout value
- Database performance: Source or SurrealDB may be slow
- Network issues: Container communication problems
- Resource contention: CPU/memory limits may be too restrictive

Suggested actions:
- For {stuck_phase} timeout: Check {stuck_phase} container logs
- Try reducing row_count or increasing timeout
- Check if database containers are healthy
""")
        print("=" * 70)

    def get_verification_stats(self) -> VerificationStats:
        """Get verification statistics from logs.

        Returns:
            VerificationStats object with aggregated stats from all verify containers

        Raises:
            RuntimeError: If logs are empty or no stats could be parsed
        """
        verify_logs = self.get_verification_logs()
        if not verify_logs:
            raise RuntimeError(
                "No verification logs found - containers may not have produced output"
            )

        stats = aggregate_verification_stats(verify_logs)

        # Validate that we actually got some stats if logs exist
        total_checked = stats.matched + stats.missing + stats.mismatched
        if total_checked == 0:
            # Log the first few lines to help debug
            self.log_error("Failed to parse verification stats from logs")
            self.log("First 20 log lines:")
            for line in verify_logs[:20]:
                print(f"  {line}")
            raise RuntimeError(
                f"Failed to parse verification stats from {len(verify_logs)} log lines. "
                "Check log format matches expected pattern."
            )

        return stats

    def collect_container_logs(self):
        """Collect logs and status info from all containers.

        Creates a 'logs' subdirectory with:
        - containers.json: Summary of all container statuses
        - <container_name>.log: Full logs for each container
        """
        logs_dir = self.config.output_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        self.log(f"Collecting container logs to {logs_dir}")

        # Get all container names
        ps_result = self.docker_compose("ps", "-a", "--format", "{{.Names}}", capture_output=True)
        all_containers = [c.strip() for c in ps_result.stdout.strip().splitlines() if c.strip()]

        if not all_containers:
            self.log_error("No containers found to collect logs from")
            return

        container_info = []

        for container_name in all_containers:
            # Get container inspect info (exit code, status, etc.)
            inspect_result = self.runner.run(
                ["docker", "inspect", container_name,
                 "--format", "{{.State.Status}}|{{.State.ExitCode}}|{{.State.StartedAt}}|{{.State.FinishedAt}}"],
                capture_output=True, text=True
            )

            status = "unknown"
            exit_code = -1
            started_at = ""
            finished_at = ""

            if inspect_result.returncode == 0 and inspect_result.stdout.strip():
                parts = inspect_result.stdout.strip().split("|")
                if len(parts) >= 4:
                    status = parts[0]
                    try:
                        exit_code = int(parts[1])
                    except ValueError:
                        exit_code = -1
                    started_at = parts[2]
                    finished_at = parts[3]

            # Determine container type
            container_type = "unknown"
            if "populate" in container_name.lower():
                container_type = "populate"
            elif "sync" in container_name.lower():
                container_type = "sync"
            elif "verify" in container_name.lower():
                container_type = "verify"
            elif "kafka" in container_name.lower():
                container_type = "kafka"
            elif "surrealdb" in container_name.lower():
                container_type = "surrealdb"
            elif "aggregator" in container_name.lower():
                container_type = "aggregator"

            info = {
                "name": container_name,
                "type": container_type,
                "status": status,
                "exit_code": exit_code,
                "started_at": started_at,
                "finished_at": finished_at,
                "failed": exit_code != 0 and status == "exited"
            }
            container_info.append(info)

            # Get full logs for the container
            log_result = self.runner.run(
                ["docker", "logs", container_name],
                capture_output=True, text=True
            )

            # Write separate log files for stdout and stderr
            if log_result.stdout:
                stdout_file = logs_dir / f"{container_name}.stdout.log"
                with open(stdout_file, 'w') as f:
                    f.write(log_result.stdout)

            if log_result.stderr:
                stderr_file = logs_dir / f"{container_name}.stderr.log"
                with open(stderr_file, 'w') as f:
                    f.write(log_result.stderr)

        # Sort container info: failed first, then by type
        type_order = {"populate": 0, "sync": 1, "verify": 2, "kafka": 3, "surrealdb": 4, "aggregator": 5, "unknown": 6}
        container_info.sort(key=lambda x: (not x["failed"], type_order.get(x["type"], 6), x["name"]))

        # Write summary JSON
        summary = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_containers": len(container_info),
            "failed_containers": sum(1 for c in container_info if c["failed"]),
            "containers": container_info
        }

        summary_file = logs_dir / "containers.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        self.log(f"Collected logs from {len(container_info)} containers")

        # Log summary of failed containers
        failed = [c for c in container_info if c["failed"]]
        if failed:
            self.log_error(f"Failed containers ({len(failed)}):")
            for c in failed:
                print(f"  - {c['name']}: exit_code={c['exit_code']}, type={c['type']}")

    def collect_resource_stats(self) -> ResourceStats:
        """Collect resource usage from running containers via docker stats.

        Updates the internal peak memory tracker and returns current stats.

        Returns:
            ResourceStats with current peak memory (tracked across all calls)
        """
        try:
            result = self.runner.run(
                ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}"],
                capture_output=True, text=True
            )

            if result.returncode != 0:
                return ResourceStats(peak_memory_mb=self._peak_memory_mb)

            # Parse memory values from each container
            # Format: "256MiB / 8GiB" - we want the first part (used memory)
            max_current_memory = 0
            for line in result.stdout.strip().splitlines():
                if "/" in line:
                    used_mem = line.split("/")[0].strip()
                    mem_mb = parse_docker_memory(used_mem)
                    max_current_memory = max(max_current_memory, mem_mb)

            # Update peak if current is higher
            if max_current_memory > self._peak_memory_mb:
                self._peak_memory_mb = max_current_memory

        except Exception:
            # Don't fail the test run if stats collection fails
            pass

        return ResourceStats(peak_memory_mb=self._peak_memory_mb)

    def build_timeline(self) -> dict:
        """Build container timeline from containers.json.

        Parses container timestamps and computes relative times from the
        earliest container start.

        Returns:
            Dictionary with 'containers' list, or empty dict if no data
        """
        logs_dir = self.config.output_dir / "logs"
        containers_file = logs_dir / "containers.json"

        if not containers_file.exists():
            return {}

        with open(containers_file) as f:
            data = json.load(f)

        containers = data.get("containers", [])
        if not containers:
            return {}

        # Find earliest start time as baseline
        start_times = []
        for c in containers:
            if c.get("started_at"):
                try:
                    start_times.append(
                        datetime.fromisoformat(c["started_at"].replace("Z", "+00:00"))
                    )
                except ValueError as e:
                    raise RuntimeError(
                        f"Failed to parse started_at timestamp for container "
                        f"'{c.get('name', 'unknown')}': {c.get('started_at')} - {e}"
                    )

        if not start_times:
            return {}

        baseline = min(start_times)

        timeline = []
        for c in containers:
            started = c.get("started_at", "")
            finished = c.get("finished_at", "")

            start_rel = 0.0
            end_rel = None  # None for running containers
            duration = None  # None for running containers

            container_name = c.get("name", "unknown")

            if started:
                try:
                    start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                    start_rel = (start_dt - baseline).total_seconds()
                except ValueError as e:
                    raise RuntimeError(
                        f"Failed to parse started_at timestamp for container "
                        f"'{container_name}': {started} - {e}"
                    )

            if finished and not finished.startswith("0001-01-01"):
                # Skip zero timestamps (Docker returns 0001-01-01 for running containers)
                try:
                    end_dt = datetime.fromisoformat(finished.replace("Z", "+00:00"))
                    end_rel = (end_dt - baseline).total_seconds()
                    duration = end_rel - start_rel
                except ValueError as e:
                    raise RuntimeError(
                        f"Failed to parse finished_at timestamp for container "
                        f"'{container_name}': {finished} - {e}"
                    )

            # Strip Docker Compose project prefix from container name for cleaner display
            # e.g., "output-kafka-1" -> "kafka-1" when output_dir is "output"
            name = c.get("name", "unknown")
            project_prefix = self.config.output_dir.name + "-"
            if name.startswith(project_prefix):
                name = name[len(project_prefix):]

            timeline.append({
                "name": name,
                "type": c.get("type", "unknown"),
                "start_sec": round(start_rel, 1),
                "end_sec": round(end_rel, 1) if end_rel is not None else None,
                "duration_sec": round(duration, 1) if duration is not None else None,
                "exit_code": c.get("exit_code", -1)
            })

        # Sort by start time, then by name
        timeline.sort(key=lambda x: (x["start_sec"], x["name"]))

        return {"containers": timeline}

    def get_durations_from_timeline(self, timeline: dict) -> dict:
        """Calculate duration values from timeline container timestamps.

        Uses actual container start/end times for accurate duration calculation.

        Args:
            timeline: Dictionary from build_timeline() with 'containers' list

        Returns:
            Dictionary with duration values (total, populate, sync, verify)

        Raises:
            RuntimeError: If timeline data is missing or incomplete
        """
        containers = timeline.get("containers", [])
        if not containers:
            raise RuntimeError("No container timeline data available for duration calculation")

        # Separate containers by type
        populate_containers = [c for c in containers if c["type"] == "populate"]
        sync_containers = [c for c in containers if c["type"] == "sync"]
        verify_containers = [c for c in containers if c["type"] == "verify"]

        # Need sync containers to calculate sync duration
        if not sync_containers:
            raise RuntimeError("No sync containers found in timeline")

        # Get populate end time
        populate_end = 0.0
        for c in populate_containers:
            if c["end_sec"] is not None:
                populate_end = max(populate_end, c["end_sec"])

        # Get sync start/end times
        sync_start = float('inf')
        sync_end = 0.0
        for c in sync_containers:
            sync_start = min(sync_start, c["start_sec"])
            if c["end_sec"] is not None:
                sync_end = max(sync_end, c["end_sec"])

        # If sync_end is still 0, sync containers haven't finished
        if sync_end == 0:
            raise RuntimeError("Sync containers have not finished - cannot compute duration")

        # Sync duration = time from first sync start to last sync end
        sync_duration = sync_end - sync_start

        # Verify duration (if available)
        verify_duration = 0.0
        if verify_containers:
            verify_start = min(c["start_sec"] for c in verify_containers)
            verify_ends = [c["end_sec"] for c in verify_containers if c["end_sec"] is not None]
            if verify_ends:
                verify_end = max(verify_ends)
                verify_duration = verify_end - verify_start

        # Total duration from first container start to last container end
        all_end_times = [c["end_sec"] for c in containers if c["end_sec"] is not None]
        total_end = max(all_end_times) if all_end_times else 0.0
        total_duration = total_end  # baseline is 0

        return {
            "total": total_duration,
            "populate": populate_end,
            "sync": sync_duration,
            "verify": verify_duration
        }

    def write_metrics(self, exit_code: int, verification: VerificationStats):
        """Write metrics to JSON file.

        Args:
            exit_code: Exit code from the test
            verification: Verification statistics captured before cleanup
        """
        # Get final resource stats (peak memory tracked during polling)
        resources = self.collect_resource_stats()

        # Build timeline first (used for both display and duration calculation)
        timeline = self.build_timeline()

        # Get durations from timeline (raises RuntimeError if incomplete)
        durations = self.get_durations_from_timeline(timeline)

        # Build metrics with timeline-based durations
        # Ensure generator_config is available (should always be set after generate_config())
        if self.generator_config is None:
            raise RuntimeError("Generator config not available - generate_config() must be called first")

        metrics = build_metrics(
            self.config,
            self.generator_config,
            durations,
            exit_code,
            verification,
            resources
        )

        # Add timeline data
        metrics["timeline"] = timeline

        metrics_file = self.config.output_dir / "metrics.json"
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2)

        self.log(f"Metrics written to {metrics_file}")

    def cleanup(self, force: bool = False):
        """Clean up docker compose resources.

        This method catches and logs errors to avoid masking the original
        test failure. Cleanup failures are logged but don't change the exit code.
        """
        if self.test_failed and self.config.preserve_on_failure and not force:
            print()
            print("=" * 72)
            print("  RESOURCES PRESERVED FOR DEBUGGING")
            print("=" * 72)
            print()
            print("To inspect resources:")
            print(f"  docker compose -f {self.compose_file} ps -a")
            print(f"  docker compose -f {self.compose_file} logs <service>")
            print()
            print("To clean up manually:")
            print("  ./scripts/cleanup.sh")
            print()
            print("=" * 72)
            return

        self.log("Cleaning up docker compose resources...")
        if self.compose_file.exists():
            try:
                self.docker_compose("down", "-v", "--remove-orphans")
            except Exception as e:
                # Log but don't fail - cleanup errors shouldn't mask test errors
                self.log_error(f"Cleanup failed (non-fatal): {e}")

    def run(self) -> int:
        """Run the CI test.

        Returns:
            Exit code
        """
        # Check prerequisites first - fail fast with clear error
        prereq_errors = check_prerequisites()
        if prereq_errors:
            self.log_error("Missing prerequisites:")
            for error in prereq_errors:
                print(f"  - {error}", file=sys.stderr)
            self.log_error("Please install missing dependencies before running.")
            return 2  # Exit with failure code

        self.log("=== Starting CI Load Test ===")
        self.log(f"Source: {self.config.source}")
        self.log(f"Preset: {self.config.preset}")
        row_count_desc = str(self.config.row_count) if self.config.row_count is not None else "(from preset)"
        self.log(f"Row count: {row_count_desc}")
        self.log(f"Workers: {self.config.workers}")
        self.log(f"Timeout: {self.config.timeout}s")
        self.log(f"Expected sync containers: {self.expected_sync_containers}")
        self.log(f"Preserve on failure: {self.config.preserve_on_failure}")

        try:
            # Initial cleanup
            self.initial_cleanup()

            # Ensure output directory exists
            self.config.output_dir.mkdir(parents=True, exist_ok=True)

            # Build and generate
            self.build_docker_image()
            self.generate_config()

            # Start timing (for duration logging only - actual durations from container timeline)
            self._start_time = time.time()

            # Start containers
            self.start_containers()

            # Wait for completion
            self.log("Monitoring for completion...")
            exit_code, verification = self.wait_for_completion()

            end_time = time.time()

            # Collect container logs before cleanup (for debugging)
            self.collect_container_logs()

            # Write metrics (with verification stats captured before cleanup)
            self.write_metrics(exit_code, verification)

            # Show summary
            self.log("=== Load Test Complete ===")
            self.log(f"Exit code: {exit_code}")
            self.log(f"Duration: {int(end_time - self._start_time)}s")

            metrics_file = self.config.output_dir / "metrics.json"
            if metrics_file.exists():
                with open(metrics_file) as f:
                    metrics = json.load(f)
                self.log("Metrics summary:")
                print(f"  Status: {metrics['results']['status']}")
                print(f"  Total rows synced: {metrics['results']['total_rows_synced']}")
                print(f"  Throughput: {metrics['results']['throughput_total_rows_per_sec']} rows/sec")
                print(f"  Verification: {metrics['verification']['matched']} matched, "
                      f"{metrics['verification']['mismatched']} mismatched")

            return exit_code

        finally:
            self.cleanup()


def parse_args() -> Config:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="CI runner script for loadtest with JSON metrics output",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument("--source", default="kafka",
                        help="Data source (default: kafka)")
    parser.add_argument("--preset", default="small",
                        choices=["small", "medium", "large"],
                        help="Size preset (default: small)")
    parser.add_argument("--row-count", type=int, default=None,
                        help="Number of rows per table (default: from preset)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of worker containers (default: 1)")
    parser.add_argument("--timeout", type=int, default=300,
                        help="Timeout in seconds (default: 300)")
    parser.add_argument("--output-dir", type=Path, default=Path("./output"),
                        help="Output directory (default: ./output)")
    parser.add_argument("--skip-build", action="store_true",
                        help="Skip docker build step")
    parser.add_argument("--skip-cleanup", action="store_true",
                        help="Skip initial cleanup of previous resources")
    parser.add_argument("--preserve-on-failure", action="store_true", default=True,
                        help="Keep resources on failure for debugging (default)")
    parser.add_argument("--no-preserve-on-failure", action="store_true",
                        help="Clean up resources even on failure")

    args = parser.parse_args()

    # Determine absolute paths
    script_dir = Path(__file__).parent
    loadtest_dir = script_dir.parent
    project_root = loadtest_dir.parent

    # Resolve output dir relative to loadtest dir
    output_dir = args.output_dir
    if not output_dir.is_absolute():
        output_dir = loadtest_dir / output_dir

    return Config(
        source=args.source,
        preset=args.preset,
        row_count=args.row_count,
        workers=args.workers,
        timeout=args.timeout,
        output_dir=output_dir,
        skip_build=args.skip_build,
        skip_cleanup=args.skip_cleanup,
        preserve_on_failure=not args.no_preserve_on_failure,
        loadtest_dir=loadtest_dir,
        project_root=project_root
    )


def main():
    """Main entry point."""
    config = parse_args()
    runner = CIRunner(config)
    exit_code = runner.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
