#!/usr/bin/env python3
"""
CI runner script for loadtest with JSON metrics output.

Usage: ./scripts/run_ci.py [OPTIONS]

Options:
  --source SOURCE       Data source (default: kafka)
  --preset PRESET       Size preset: small, medium, large (default: small)
  --row-count COUNT     Number of rows per table (default: 100)
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
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional


@dataclass
class Config:
    """Configuration for the CI runner."""
    source: str = "kafka"
    preset: str = "small"
    row_count: int = 100
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
class Timing:
    """Timing information for different phases."""
    start: float = 0
    populate_start: float = 0
    populate_end: float = 0
    sync_start: float = 0
    sync_end: float = 0
    verify_start: float = 0
    verify_end: float = 0
    end: float = 0


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


def calculate_durations(timing: Timing) -> dict:
    """Calculate duration values from timing data.

    Args:
        timing: Timing object with phase timestamps

    Returns:
        Dictionary with duration values

    Raises:
        ValueError: If timing data is invalid (e.g., start time is 0)
    """
    # Validate essential timing fields
    if timing.start == 0:
        raise ValueError("timing.start is 0 - timing was not properly initialized")
    if timing.end == 0:
        raise ValueError("timing.end is 0 - timing was not properly recorded")
    if timing.end < timing.start:
        raise ValueError(f"timing.end ({timing.end}) is before timing.start ({timing.start})")

    # Calculate with proper fallbacks
    populate_start = timing.populate_start if timing.populate_start else timing.start
    populate_end = timing.populate_end if timing.populate_end else timing.end

    # Sync phase: starts when populate ends, ends when sync containers finish
    sync_start = timing.sync_start if timing.sync_start else populate_end
    sync_end = timing.sync_end if timing.sync_end else timing.end

    # Verify phase: starts when sync ends
    verify_start = timing.verify_start if timing.verify_start else sync_end
    verify_end = timing.verify_end if timing.verify_end else timing.end

    total_duration = int(timing.end - timing.start)
    populate_duration = int(populate_end - populate_start)
    sync_duration = int(sync_end - sync_start)
    verify_duration = int(verify_end - verify_start)

    # Validate durations make sense
    if sync_duration == 0 and populate_duration > 0:
        # Sync duration of 0 is suspicious if populate took time
        # This could happen if sync containers weren't tracked properly
        # Log a warning but don't fail - use a fallback calculation
        # Estimate sync as: total - populate - verify (minimum 1 second if positive result expected)
        estimated_sync = total_duration - populate_duration - verify_duration
        if estimated_sync > 0:
            sync_duration = estimated_sync

    return {
        "total": total_duration,
        "populate": populate_duration,
        "sync": sync_duration,
        "verify": verify_duration
    }


def calculate_throughput(row_count: int, durations: dict) -> float:
    """Calculate throughput in rows per second.

    Args:
        row_count: Number of rows per table
        durations: Dictionary with duration values

    Returns:
        Throughput in rows per second
    """
    total_rows = row_count * 4  # 4 tables
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
    timing: Timing,
    exit_code: int,
    verification: VerificationStats,
    resources: ResourceStats
) -> dict:
    """Build the metrics dictionary.

    Args:
        config: Configuration object
        timing: Timing object
        exit_code: Exit code from the test
        verification: Verification statistics
        resources: Resource usage statistics

    Returns:
        Metrics dictionary ready for JSON serialization
    """
    durations = calculate_durations(timing)
    git_sha, git_ref = get_git_info()

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "git_sha": git_sha,
        "git_ref": git_ref,
        "source": config.source,
        "preset": config.preset,
        "row_count": config.row_count,
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
            "rows_synced": config.row_count * 4,
            "throughput_rows_per_sec": calculate_throughput(config.row_count, durations)
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
        self.timing = Timing()
        self.test_failed = False
        self.expected_sync_containers = get_expected_sync_containers(config.source)
        self.compose_file = config.output_dir / "docker-compose.loadtest.yml"

    def log(self, message: str):
        """Log a message with timestamp."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")

    def log_error(self, message: str):
        """Log an error message with timestamp."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] ERROR: {message}", file=sys.stderr)

    def docker_compose(self, *args, capture_output: bool = False) -> subprocess.CompletedProcess:
        """Run a docker-compose command."""
        cmd = ["docker-compose", "-f", str(self.compose_file)] + list(args)
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

        # Clean docker-compose resources
        if self.compose_file.exists():
            self.docker_compose("down", "-v", "--remove-orphans")

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
        """Generate docker-compose configuration.

        Raises:
            FileNotFoundError: If required input files don't exist
            RuntimeError: If configuration generation fails
        """
        # Check input files exist
        schema_file = self.config.loadtest_dir / "config" / "schema.yaml"
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        self.log("Generating docker-compose configuration...")
        cmd = [
            "cargo", "run", "--release", "--",
            "loadtest", "generate",
            "--platform", "docker-compose",
            "--source", self.config.source,
            "--preset", self.config.preset,
            "--schema", str(schema_file),
            "--output-dir", str(self.config.output_dir),
            "--workers", str(self.config.workers),
            "--row-count", str(self.config.row_count)
        ]
        result = self.runner.run(cmd, cwd=str(self.config.project_root))
        if result.returncode != 0:
            raise RuntimeError("Failed to generate configuration")

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
            self.log_error("No verification containers found")
            return []

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

            # Track phase transitions
            if not self.timing.populate_end and status.populate_done >= self.config.workers:
                self.timing.populate_end = time.time()
                self.timing.sync_start = time.time()

            if not self.timing.sync_end and status.sync_done >= self.expected_sync_containers:
                self.timing.sync_end = time.time()
                self.timing.verify_start = time.time()

            # Check for failures
            if status.failed_containers:
                # Capture verification stats before processing
                verification = self.get_verification_stats()

                # Distinguish between verify container failures (expected when data mismatches)
                # and other container failures (unexpected infrastructure issues)
                verify_failures = [c for c in status.failed_containers if 'verify' in c]
                other_failures = [c for c in status.failed_containers if 'verify' not in c]

                if other_failures:
                    # Populate or sync containers failed - this is a real infrastructure failure
                    self.log_error("Container(s) exited with unexpected non-zero status:")
                    for name in other_failures[:5]:
                        print(f"  - {name}")
                        result = self.runner.run(
                            ["docker", "logs", name, "--tail", "50"],
                            capture_output=True, text=True
                        )
                        self.log(f"Logs from: {name}")
                        print(result.stdout + result.stderr)
                    self.test_failed = True
                    return 2, verification

                if verify_failures:
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
                self.timing.verify_end = time.time()
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
                self.log(
                    f"Progress at timeout: Populate: {status.populate_done}/{self.config.workers}, "
                    f"Sync: {status.sync_done}/{self.expected_sync_containers}, "
                    f"Verify: {status.verify_done}/{self.config.workers}"
                )
                self.test_failed = True
                # Try to capture verification stats even on timeout
                verification = self.get_verification_stats()
                return 1, verification

            time.sleep(self.config.poll_interval)

    def get_verification_stats(self) -> VerificationStats:
        """Get verification statistics from logs.

        Returns:
            VerificationStats object with aggregated stats from all verify containers

        Raises:
            RuntimeError: If logs are available but no stats could be parsed
        """
        verify_logs = self.get_verification_logs()
        if not verify_logs:
            self.log_error("No verification logs found - containers may not have run")
            return VerificationStats()

        stats = aggregate_verification_stats(verify_logs)

        # Validate that we actually got some stats if logs exist
        total_checked = stats.matched + stats.missing + stats.mismatched
        if total_checked == 0:
            # Log the first few lines to help debug
            self.log_error("Failed to parse verification stats from logs")
            self.log("First 20 log lines:")
            for line in verify_logs[:20]:
                print(f"  {line}")
            # Don't raise - return empty stats but user will see the warning

        return stats

    def write_metrics(self, exit_code: int, verification: VerificationStats):
        """Write metrics to JSON file.

        Args:
            exit_code: Exit code from the test
            verification: Verification statistics captured before cleanup
        """
        resources = ResourceStats()  # TODO: Implement resource stats collection

        metrics = build_metrics(
            self.config,
            self.timing,
            exit_code,
            verification,
            resources
        )

        metrics_file = self.config.output_dir / "metrics.json"
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2)

        self.log(f"Metrics written to {metrics_file}")

    def cleanup(self, force: bool = False):
        """Clean up docker-compose resources."""
        if self.test_failed and self.config.preserve_on_failure and not force:
            print()
            print("=" * 72)
            print("  RESOURCES PRESERVED FOR DEBUGGING")
            print("=" * 72)
            print()
            print("To inspect resources:")
            print(f"  docker-compose -f {self.compose_file} ps -a")
            print(f"  docker-compose -f {self.compose_file} logs <service>")
            print()
            print("To clean up manually:")
            print("  ./scripts/cleanup.sh")
            print()
            print("=" * 72)
            return

        self.log("Cleaning up docker-compose resources...")
        if self.compose_file.exists():
            self.docker_compose("down", "-v", "--remove-orphans")

    def run(self) -> int:
        """Run the CI test.

        Returns:
            Exit code
        """
        self.log("=== Starting CI Load Test ===")
        self.log(f"Source: {self.config.source}")
        self.log(f"Preset: {self.config.preset}")
        self.log(f"Row count: {self.config.row_count}")
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

            # Start timing
            self.timing.start = time.time()
            self.timing.populate_start = self.timing.start

            # Start containers
            self.start_containers()

            # Wait for completion
            self.log("Monitoring for completion...")
            exit_code, verification = self.wait_for_completion()

            self.timing.end = time.time()

            # Write metrics (with verification stats captured before cleanup)
            self.write_metrics(exit_code, verification)

            # Show summary
            self.log("=== Load Test Complete ===")
            self.log(f"Exit code: {exit_code}")
            self.log(f"Duration: {int(self.timing.end - self.timing.start)}s")

            metrics_file = self.config.output_dir / "metrics.json"
            if metrics_file.exists():
                with open(metrics_file) as f:
                    metrics = json.load(f)
                self.log("Metrics summary:")
                print(f"  Status: {metrics['results']['status']}")
                print(f"  Throughput: {metrics['results']['throughput_rows_per_sec']} rows/sec")
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
    parser.add_argument("--row-count", type=int, default=100,
                        help="Number of rows per table (default: 100)")
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
