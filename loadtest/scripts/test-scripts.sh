#!/bin/bash
set -euo pipefail

# Unit tests for CI scripts - NO external dependencies (no docker, no cargo)
#
# This script runs:
# - Python unit tests for compare_metrics.py
# - Integration tests using sample JSON data
#
# Usage: ./scripts/test-scripts.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TEST_DATA_DIR="$SCRIPT_DIR/testdata"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

log_section() {
    echo ""
    echo "========================================"
    echo "$*"
    echo "========================================"
}

log_test() {
    echo -e "${YELLOW}TEST:${NC} $*"
}

log_pass() {
    echo -e "${GREEN}PASS${NC}"
    ((TESTS_PASSED++))
}

log_fail() {
    echo -e "${RED}FAIL:${NC} $*"
    ((TESTS_FAILED++))
}

# Create sample metrics files for integration testing
setup_test_data() {
    mkdir -p "$TEST_DATA_DIR"

    # Sample baseline metrics
    cat > "$TEST_DATA_DIR/baseline.json" << 'EOF'
{
  "timestamp": "2025-01-01T00:00:00+00:00",
  "git_sha": "abc123",
  "git_ref": "main",
  "source": "kafka",
  "preset": "small",
  "row_count": 1000,
  "workers": 1,
  "platform": "docker-compose",
  "runner": "local",
  "results": {
    "status": "success",
    "exit_code": 0,
    "total_duration_seconds": 45,
    "populate_duration_seconds": 10,
    "sync_duration_seconds": 30,
    "verify_duration_seconds": 5,
    "total_rows_synced": 4000,
    "throughput_total_rows_per_sec": 100.0
  },
  "resources": {
    "peak_memory_mb": 500,
    "avg_cpu_percent": 50.0
  },
  "verification": {
    "matched": 4000,
    "mismatched": 0,
    "missing": 0
  }
}
EOF

    # Current metrics - no regression (slightly better)
    cat > "$TEST_DATA_DIR/current_ok.json" << 'EOF'
{
  "timestamp": "2025-01-02T00:00:00+00:00",
  "git_sha": "def456",
  "git_ref": "main",
  "source": "kafka",
  "preset": "small",
  "row_count": 1000,
  "workers": 1,
  "platform": "docker-compose",
  "runner": "local",
  "results": {
    "status": "success",
    "exit_code": 0,
    "total_duration_seconds": 44,
    "populate_duration_seconds": 10,
    "sync_duration_seconds": 29,
    "verify_duration_seconds": 5,
    "total_rows_synced": 4000,
    "throughput_total_rows_per_sec": 102.0
  },
  "resources": {
    "peak_memory_mb": 490,
    "avg_cpu_percent": 48.0
  },
  "verification": {
    "matched": 4000,
    "mismatched": 0,
    "missing": 0
  }
}
EOF

    # Current metrics - with regression (20% slower throughput)
    cat > "$TEST_DATA_DIR/current_regression.json" << 'EOF'
{
  "timestamp": "2025-01-02T00:00:00+00:00",
  "git_sha": "ghi789",
  "git_ref": "feature-branch",
  "source": "kafka",
  "preset": "small",
  "row_count": 1000,
  "workers": 1,
  "platform": "docker-compose",
  "runner": "local",
  "results": {
    "status": "success",
    "exit_code": 0,
    "total_duration_seconds": 55,
    "populate_duration_seconds": 10,
    "sync_duration_seconds": 40,
    "verify_duration_seconds": 5,
    "total_rows_synced": 4000,
    "throughput_total_rows_per_sec": 80.0
  },
  "resources": {
    "peak_memory_mb": 600,
    "avg_cpu_percent": 60.0
  },
  "verification": {
    "matched": 4000,
    "mismatched": 0,
    "missing": 0
  }
}
EOF
}

cleanup() {
    rm -rf "$TEST_DATA_DIR"
}

# Run Python unit tests for compare_metrics.py
run_compare_metrics_tests() {
    log_section "Python Unit Tests: compare_metrics.py"
    ((TESTS_RUN++))

    if python3 "$SCRIPT_DIR/test_compare_metrics.py" 2>&1; then
        log_pass
    else
        log_fail "compare_metrics.py unit tests failed"
        return 1
    fi
}

# Run Python unit tests for run_ci.py
run_run_ci_tests() {
    log_section "Python Unit Tests: run_ci.py"
    ((TESTS_RUN++))

    if python3 "$SCRIPT_DIR/test_run_ci.py" 2>&1; then
        log_pass
    else
        log_fail "run_ci.py unit tests failed"
        return 1
    fi
}

# Integration test: compare-metrics.py with no regression
test_integration_no_regression() {
    ((TESTS_RUN++))
    log_test "compare_metrics.py integration: no regression..."

    local output
    output=$(python3 "$SCRIPT_DIR/compare_metrics.py" \
        "$TEST_DATA_DIR/current_ok.json" \
        "$TEST_DATA_DIR/baseline.json" 2>&1)
    local exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        log_fail "Expected exit code 0, got $exit_code"
        echo "$output"
        return 1
    fi
    log_pass
}

# Integration test: compare-metrics.py with regression
test_integration_with_regression() {
    ((TESTS_RUN++))
    log_test "compare_metrics.py integration: with regression..."

    local output
    set +e
    output=$(python3 "$SCRIPT_DIR/compare_metrics.py" \
        "$TEST_DATA_DIR/current_regression.json" \
        "$TEST_DATA_DIR/baseline.json" 2>&1)
    local exit_code=$?
    set -e

    if [[ $exit_code -ne 1 ]]; then
        log_fail "Expected exit code 1 (regression), got $exit_code"
        echo "$output"
        return 1
    fi
    log_pass
}

# Integration test: compare-metrics.py with missing baseline
test_integration_missing_baseline() {
    ((TESTS_RUN++))
    log_test "compare_metrics.py integration: missing baseline..."

    local output
    output=$(python3 "$SCRIPT_DIR/compare_metrics.py" \
        "$TEST_DATA_DIR/current_ok.json" \
        "$TEST_DATA_DIR/nonexistent.json" 2>&1)
    local exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        log_fail "Expected exit code 0 (no baseline = no regression), got $exit_code"
        echo "$output"
        return 1
    fi
    log_pass
}

# Integration test: Output is valid markdown
test_integration_markdown_output() {
    ((TESTS_RUN++))
    log_test "compare_metrics.py integration: valid markdown output..."

    local output
    output=$(python3 "$SCRIPT_DIR/compare_metrics.py" \
        "$TEST_DATA_DIR/current_ok.json" \
        "$TEST_DATA_DIR/baseline.json" 2>&1)

    # Check for markdown table markers
    if ! echo "$output" | grep -q "^|.*|$"; then
        log_fail "Output doesn't contain markdown table rows"
        echo "$output"
        return 1
    fi

    # Check for header row
    if ! echo "$output" | grep -q "| Metric |"; then
        log_fail "Output missing table header"
        return 1
    fi

    log_pass
}

# Print summary
print_summary() {
    log_section "Test Summary"
    echo "Tests run:    $TESTS_RUN"
    echo -e "Tests passed: ${GREEN}$TESTS_PASSED${NC}"
    if [[ $TESTS_FAILED -gt 0 ]]; then
        echo -e "Tests failed: ${RED}$TESTS_FAILED${NC}"
    else
        echo "Tests failed: $TESTS_FAILED"
    fi
    echo "========================================"
}

main() {
    log_section "Loadtest Script Tests"
    echo "No external dependencies (docker, cargo)"

    trap cleanup EXIT

    # Run Python unit tests first (most important)
    run_compare_metrics_tests || true
    run_run_ci_tests || true

    # Set up integration test data
    setup_test_data

    # Run integration tests
    log_section "Integration Tests"
    test_integration_no_regression || true
    test_integration_with_regression || true
    test_integration_missing_baseline || true
    test_integration_markdown_output || true

    # Print summary
    print_summary

    # Return appropriate exit code
    if [[ $TESTS_FAILED -gt 0 ]]; then
        exit 1
    fi
    exit 0
}

main "$@"
