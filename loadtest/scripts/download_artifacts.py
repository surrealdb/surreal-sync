#!/usr/bin/env python3
"""
Download GitHub Actions artifacts for a specific workflow run.

Usage: ./scripts/download_artifacts.py --run-id RUN_ID [OPTIONS]

Options:
  --run-id RUN_ID       GitHub Actions workflow run ID (required)
  --output-dir DIR      Output directory (default: ./artifacts/<run-id>)
  --pattern PATTERN     Only download artifacts matching pattern (default: all)
  --help                Show this help message

Example:
  ./scripts/download_artifacts.py --run-id 20992382723
  ./scripts/download_artifacts.py --run-id 20992382723 --pattern "metrics-*"
  ./scripts/download_artifacts.py --run-id 20992382723 --output-dir ./my-artifacts

Exit codes:
  0 = Success
  1 = General error
  2 = Prerequisites not met
"""

import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path
from typing import Optional


def check_prerequisites() -> list[str]:
    """Check that required binaries are available.

    Returns:
        List of error messages for missing prerequisites. Empty if all ok.
    """
    errors = []

    # Check for gh CLI
    if not shutil.which("gh"):
        errors.append(
            "'gh' not found in PATH. Please install GitHub CLI: "
            "https://cli.github.com/"
        )
    else:
        # Check if authenticated
        result = subprocess.run(
            ["gh", "auth", "status"],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            errors.append(
                "GitHub CLI is not authenticated. Please run: gh auth login"
            )

    return errors


def get_repo_from_run(run_id: str) -> tuple[str, str]:
    """Get repository owner and name from a run ID.

    Args:
        run_id: GitHub Actions workflow run ID

    Returns:
        Tuple of (owner, repo_name)

    Raises:
        RuntimeError: If repository cannot be determined
    """
    # Get the run URL which contains the repo info
    result = subprocess.run(
        ["gh", "run", "view", run_id, "--json", "url"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to get run information for {run_id}. "
            f"Error: {result.stderr.strip()}\n"
            f"Make sure the run ID is correct and you have access to the repository."
        )

    try:
        data = json.loads(result.stdout)
        url = data.get("url", "")
        # URL format: https://github.com/owner/repo/actions/runs/12345
        parts = url.split("/")
        if len(parts) >= 5 and parts[2] == "github.com":
            owner = parts[3]
            repo_name = parts[4]
            return owner, repo_name
        else:
            raise RuntimeError(f"Unexpected URL format: {url}")
    except (json.JSONDecodeError, IndexError, ValueError) as e:
        raise RuntimeError(f"Failed to parse run URL: {e}")


def list_artifacts(run_id: str) -> list[dict]:
    """List all artifacts for a workflow run.

    Args:
        run_id: GitHub Actions workflow run ID

    Returns:
        List of artifact dictionaries with 'name' and other metadata

    Raises:
        RuntimeError: If gh command fails
    """
    # Get repository from run ID
    owner, repo_name = get_repo_from_run(run_id)

    # List artifacts using the API
    api_path = f"repos/{owner}/{repo_name}/actions/runs/{run_id}/artifacts"
    result = subprocess.run(
        ["gh", "api", api_path, "--jq", ".artifacts"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to list artifacts for run {run_id}. "
            f"Error: {result.stderr.strip()}"
        )

    try:
        artifacts = json.loads(result.stdout)
        return artifacts
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse artifact list: {e}")


def extract_nested_archives(directory: Path) -> int:
    """Recursively extract any archive files found in directory.

    Handles .zip, .tar.gz, .tgz, and .gz files.

    Args:
        directory: Directory to search for archives

    Returns:
        Number of archives extracted
    """
    extracted_count = 0

    # Keep extracting until no more archives are found
    while True:
        archives_found = []

        # Find all archive files
        for root, dirs, files in os.walk(directory):
            root_path = Path(root)
            for filename in files:
                file_path = root_path / filename

                # Check file extensions
                if filename.endswith('.zip'):
                    archives_found.append(('zip', file_path))
                elif filename.endswith('.tar.gz') or filename.endswith('.tgz'):
                    archives_found.append(('tar.gz', file_path))
                elif filename.endswith('.gz') and not filename.endswith('.tar.gz'):
                    archives_found.append(('gz', file_path))

        if not archives_found:
            break

        # Extract all found archives
        for archive_type, archive_path in archives_found:
            extract_dir = archive_path.parent

            try:
                if archive_type == 'zip':
                    with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                    archive_path.unlink()  # Remove zip after extraction
                    extracted_count += 1

                elif archive_type == 'tar.gz':
                    with tarfile.open(archive_path, 'r:gz') as tar_ref:
                        tar_ref.extractall(extract_dir)
                    archive_path.unlink()  # Remove tar.gz after extraction
                    extracted_count += 1

                elif archive_type == 'gz':
                    # For standalone .gz files, decompress to same name without .gz
                    output_path = extract_dir / archive_path.stem
                    with gzip.open(archive_path, 'rb') as gz_file:
                        with open(output_path, 'wb') as out_file:
                            shutil.copyfileobj(gz_file, out_file)
                    archive_path.unlink()  # Remove .gz after extraction
                    extracted_count += 1

            except Exception as e:
                print(f"Warning: Failed to extract {archive_path}: {e}", file=sys.stderr)

    return extracted_count


def download_artifacts(run_id: str, output_dir: Path, pattern: Optional[str] = None) -> list[str]:
    """Download and extract artifacts from a workflow run.

    Args:
        run_id: GitHub Actions workflow run ID
        output_dir: Directory to save artifacts
        pattern: Optional pattern to filter artifact names

    Returns:
        List of downloaded artifact names

    Raises:
        RuntimeError: If download fails
    """
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading artifacts to: {output_dir}")

    # Build gh command
    cmd = ["gh", "run", "download", run_id, "--dir", str(output_dir)]
    if pattern:
        cmd.extend(["--pattern", pattern])

    # Download artifacts
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to download artifacts for run {run_id}. "
            f"Error: {result.stderr.strip()}"
        )

    # gh downloads artifacts into subdirectories named after the artifact
    # Each artifact is downloaded as a directory containing the artifact contents
    downloaded = []
    for item in output_dir.iterdir():
        if item.is_dir():
            downloaded.append(item.name)

    if not downloaded:
        print("Warning: No artifacts were downloaded. They may have expired or not match the pattern.", file=sys.stderr)

    return downloaded


def print_summary(artifacts_dir: Path):
    """Print a summary of downloaded artifacts and their contents.

    Args:
        artifacts_dir: Directory containing downloaded artifacts
    """
    print("\n" + "=" * 70)
    print("DOWNLOADED ARTIFACTS")
    print("=" * 70)

    if not artifacts_dir.exists():
        print("No artifacts directory found.")
        return

    # Get all artifact directories
    artifact_dirs = sorted([d for d in artifacts_dir.iterdir() if d.is_dir()])

    if not artifact_dirs:
        print("No artifacts downloaded.")
        return

    for artifact_dir in artifact_dirs:
        print(f"\n{artifact_dir.name}/")

        # List all files in this artifact (recursively)
        files = []
        for root, dirs, filenames in os.walk(artifact_dir):
            root_path = Path(root)
            for filename in filenames:
                file_path = root_path / filename
                rel_path = file_path.relative_to(artifact_dir)
                files.append((rel_path, file_path))

        # Sort by path
        files.sort(key=lambda x: str(x[0]))

        if not files:
            print("  (empty)")
        else:
            for rel_path, full_path in files:
                # Get file size
                size = full_path.stat().st_size
                size_str = format_size(size)
                print(f"  {rel_path} ({size_str})")

    print("\n" + "=" * 70)
    print(f"Artifacts directory: {artifacts_dir.absolute()}")
    print("=" * 70)


def format_size(size_bytes: int) -> str:
    """Format file size in human-readable format.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted size string (e.g., "1.5 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Download GitHub Actions artifacts for a specific workflow run",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        "--run-id",
        required=True,
        help="GitHub Actions workflow run ID (e.g., 20992382723)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory (default: ./artifacts/<run-id>)"
    )
    parser.add_argument(
        "--pattern",
        help="Only download artifacts matching pattern (e.g., 'metrics-*')"
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Validate run ID is numeric
    if not args.run_id.isdigit():
        print(f"Error: Invalid run ID '{args.run_id}'. Must be a numeric ID.", file=sys.stderr)
        print("Example: 20992382723", file=sys.stderr)
        sys.exit(1)

    # Set default output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        # Default to ./artifacts/<run-id>
        script_dir = Path(__file__).parent
        loadtest_dir = script_dir.parent
        output_dir = loadtest_dir / "artifacts" / args.run_id

    # Check prerequisites
    prereq_errors = check_prerequisites()
    if prereq_errors:
        print("ERROR: Missing prerequisites:", file=sys.stderr)
        for error in prereq_errors:
            print(f"  - {error}", file=sys.stderr)
        sys.exit(2)

    try:
        # List available artifacts
        print(f"Fetching artifact list for run {args.run_id}...")
        artifacts = list_artifacts(args.run_id)

        if not artifacts:
            print(f"No artifacts found for run {args.run_id}")
            sys.exit(0)

        print(f"Found {len(artifacts)} artifact(s)")
        for artifact in artifacts:
            name = artifact.get('name', 'unknown')
            print(f"  - {name}")

        # Download artifacts
        print()
        downloaded = download_artifacts(args.run_id, output_dir, args.pattern)

        if not downloaded:
            sys.exit(0)

        print(f"Downloaded {len(downloaded)} artifact(s)")

        # Extract any nested archives
        print("Extracting nested archives...")
        extracted_count = extract_nested_archives(output_dir)
        if extracted_count > 0:
            print(f"Extracted {extracted_count} nested archive(s)")

        # Print summary
        print_summary(output_dir)

        print(f"\nSuccess! Artifacts saved to: {output_dir.absolute()}")

    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
