#!/bin/bash
set -euo pipefail

# CI runner script for loadtest with JSON metrics output
# Usage: ./scripts/run-ci.sh [OPTIONS]
#
# Options:
#   --source SOURCE       Data source (default: kafka)
#   --preset PRESET       Size preset: small, medium, large (default: small)
#   --row-count COUNT     Number of rows per table (default: 100)
#   --workers COUNT       Number of worker containers (default: 1)
#   --timeout SECONDS     Timeout in seconds (default: 300)
#   --output-dir DIR      Output directory (default: ./output)
#   --skip-build          Skip docker build step
#   --skip-cleanup        Skip initial cleanup of previous resources
#   --preserve-on-failure Keep resources on failure for debugging
#   --help                Show this help message
#
# Exit codes:
#   0 = Success (all tests passed)
#   1 = Timeout
#   2 = Container failure
#   3 = Verification mismatch
#
# On failure with --preserve-on-failure, resources are kept for debugging.
# Run ./scripts/cleanup.sh to remove them manually.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOADTEST_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$LOADTEST_DIR")"

# Default configuration
SOURCE="${SOURCE:-kafka}"
PRESET="${PRESET:-small}"
ROW_COUNT="${ROW_COUNT:-100}"
WORKERS="${WORKERS:-1}"
TIMEOUT="${TIMEOUT:-300}"
OUTPUT_DIR="${OUTPUT_DIR:-$LOADTEST_DIR/output}"
SKIP_BUILD="${SKIP_BUILD:-false}"
SKIP_CLEANUP="${SKIP_CLEANUP:-false}"
PRESERVE_ON_FAILURE="${PRESERVE_ON_FAILURE:-true}"
POLL_INTERVAL="${POLL_INTERVAL:-5}"
IMAGE_NAME="${IMAGE_NAME:-surreal-sync:latest}"

# State tracking
TEST_FAILED=false
CLEANUP_DONE=false
EXPECTED_SYNC_CONTAINERS=1  # Will be updated based on source type

# Timing variables
START_TIME=""
POPULATE_START=""
POPULATE_END=""
SYNC_START=""
SYNC_END=""
VERIFY_START=""
VERIFY_END=""
END_TIME=""

# Resource stats
STATS_PID=""
STATS_FILE=""

usage() {
    head -28 "$0" | tail -26 | sed 's/^# //' | sed 's/^#//'
    exit 0
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --source)
                SOURCE="$2"
                shift 2
                ;;
            --preset)
                PRESET="$2"
                shift 2
                ;;
            --row-count)
                ROW_COUNT="$2"
                shift 2
                ;;
            --workers)
                WORKERS="$2"
                shift 2
                ;;
            --timeout)
                TIMEOUT="$2"
                shift 2
                ;;
            --output-dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --skip-build)
                SKIP_BUILD="true"
                shift
                ;;
            --skip-cleanup)
                SKIP_CLEANUP="true"
                shift
                ;;
            --preserve-on-failure)
                PRESERVE_ON_FAILURE="true"
                shift
                ;;
            --no-preserve-on-failure)
                PRESERVE_ON_FAILURE="false"
                shift
                ;;
            --help|-h)
                usage
                ;;
            *)
                echo "Unknown option: $1"
                usage
                ;;
        esac
    done
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $*" >&2
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
}

# Get the expected number of sync containers based on source type
# Kafka uses per-table sync containers (4 tables = 4 containers)
# Other sources use a single sync container
get_expected_sync_containers() {
    case "$SOURCE" in
        kafka)
            # Kafka uses per-table sync: sync-users, sync-products, sync-orders, sync-order_items
            echo 4
            ;;
        *)
            # Other sources use a single sync container
            echo 1
            ;;
    esac
}

# Clean up docker-compose resources
cleanup_docker_compose() {
    local force="${1:-false}"

    if [[ "$CLEANUP_DONE" == "true" ]] && [[ "$force" != "true" ]]; then
        return 0
    fi

    stop_stats_collector

    if [[ -f "$OUTPUT_DIR/docker-compose.loadtest.yml" ]]; then
        log "Cleaning up docker-compose resources..."
        docker-compose -f "$OUTPUT_DIR/docker-compose.loadtest.yml" down -v --remove-orphans 2>/dev/null || true
        CLEANUP_DONE=true
    fi

    # Also clean up any orphaned loadtest containers
    local orphans
    orphans=$(docker ps -aq --filter "label=com.surreal-loadtest" 2>/dev/null) || true
    if [[ -n "$orphans" ]]; then
        log "Removing orphaned loadtest containers..."
        echo "$orphans" | xargs -r docker rm -f 2>/dev/null || true
    fi

    # Clean up volumes
    docker volume prune -f --filter "label=com.surreal-loadtest" 2>/dev/null || true
}

# Initial cleanup of any previous failed resources
initial_cleanup() {
    if [[ "$SKIP_CLEANUP" == "true" ]]; then
        log "Skipping initial cleanup (--skip-cleanup)"
        return 0
    fi

    log "Cleaning up any previous failed resources..."

    # Clean docker-compose resources
    if [[ -f "$OUTPUT_DIR/docker-compose.loadtest.yml" ]]; then
        docker-compose -f "$OUTPUT_DIR/docker-compose.loadtest.yml" down -v --remove-orphans 2>/dev/null || true
    fi

    # Clean orphaned containers with loadtest label
    local orphans
    orphans=$(docker ps -aq --filter "label=com.surreal-loadtest" 2>/dev/null) || true
    if [[ -n "$orphans" ]]; then
        log "Removing orphaned loadtest containers..."
        echo "$orphans" | xargs -r docker rm -f 2>/dev/null || true
    fi

    # Clean up networks
    docker network rm loadtest_default 2>/dev/null || true
    docker network prune -f --filter "label=com.surreal-loadtest" 2>/dev/null || true

    # Clean up volumes
    docker volume prune -f --filter "label=com.surreal-loadtest" 2>/dev/null || true

    log "Initial cleanup complete"
}

start_stats_collector() {
    STATS_FILE="$OUTPUT_DIR/resource-stats.jsonl"
    rm -f "$STATS_FILE"

    # Collect docker stats every 2 seconds in background
    (
        while true; do
            if docker-compose -f "$OUTPUT_DIR/docker-compose.loadtest.yml" ps -q 2>/dev/null | head -1 | grep -q .; then
                docker stats --no-stream --format '{"timestamp":"{{json .}}","time":"'"$(date -Iseconds)"'"}' 2>/dev/null >> "$STATS_FILE" || true
            fi
            sleep 2
        done
    ) &
    STATS_PID=$!
}

stop_stats_collector() {
    if [[ -n "${STATS_PID:-}" ]]; then
        kill "$STATS_PID" 2>/dev/null || true
        wait "$STATS_PID" 2>/dev/null || true
        STATS_PID=""
    fi
}

calculate_resource_stats() {
    local peak_memory=0
    local total_cpu=0
    local cpu_count=0

    if [[ -f "$STATS_FILE" ]]; then
        # Parse resource stats - this is a simplified version
        # Real implementation would parse docker stats JSON properly
        while IFS= read -r line; do
            # Extract memory and CPU from docker stats output
            # Format varies, so we'll estimate based on common patterns
            local mem=$(echo "$line" | grep -oP '\d+(\.\d+)?MiB' | head -1 | sed 's/MiB//' || echo "0")
            local cpu=$(echo "$line" | grep -oP '\d+(\.\d+)?%' | head -1 | sed 's/%//' || echo "0")

            if [[ -n "$mem" ]] && (( $(echo "$mem > $peak_memory" | bc -l 2>/dev/null || echo 0) )); then
                peak_memory="$mem"
            fi
            if [[ -n "$cpu" ]]; then
                total_cpu=$(echo "$total_cpu + $cpu" | bc -l 2>/dev/null || echo "$total_cpu")
                ((cpu_count++)) || true
            fi
        done < "$STATS_FILE"
    fi

    local avg_cpu=0
    if [[ $cpu_count -gt 0 ]]; then
        avg_cpu=$(echo "scale=1; $total_cpu / $cpu_count" | bc -l 2>/dev/null || echo "0")
    fi

    echo "{\"peak_memory_mb\": ${peak_memory:-0}, \"avg_cpu_percent\": ${avg_cpu:-0}}"
}

wait_for_completion() {
    local start_time
    start_time=$(date +%s)
    local exit_code=0

    cd "$OUTPUT_DIR"

    while true; do
        local elapsed=$(( $(date +%s) - start_time ))

        local ps_output
        ps_output=$(docker-compose -f docker-compose.loadtest.yml ps -a 2>/dev/null) || true

        # Count completed containers
        # Populate containers: output-populate-N-1
        local populate_done
        populate_done=$(echo "$ps_output" | grep -E "output-populate-[0-9]+-1" | grep -c "Exited (0)" 2>/dev/null) || populate_done=0

        # Sync containers: For kafka, uses per-table sync (sync-users, sync-products, etc.)
        # For other sources, uses output-sync-1
        local sync_done
        sync_done=$(echo "$ps_output" | grep -E "output-sync-" | grep -c "Exited (0)" 2>/dev/null) || sync_done=0

        # Verify containers: output-verify-N-1
        local verify_done
        verify_done=$(echo "$ps_output" | grep -E "output-verify-[0-9]+-1" | grep -c "Exited (0)" 2>/dev/null) || verify_done=0

        # Track phase transitions for timing
        if [[ -z "$POPULATE_END" ]] && [[ "$populate_done" -ge "$WORKERS" ]]; then
            POPULATE_END=$(date +%s)
            SYNC_START=$(date +%s)
        fi
        if [[ -z "$SYNC_END" ]] && [[ "$sync_done" -ge "$EXPECTED_SYNC_CONTAINERS" ]]; then
            SYNC_END=$(date +%s)
            VERIFY_START=$(date +%s)
        fi

        # Check for failures - match any sync container pattern
        local failed
        failed=$(echo "$ps_output" | grep -E "output-(populate|sync|verify)" | grep -E "Exited \([1-9]" | awk '{print $1}' | head -5) || true
        if [[ -n "$failed" ]]; then
            log_error "Container(s) exited with non-zero status"
            echo "$failed" | sed 's/^/  - /'
            for container in $failed; do
                log "Logs from: $container"
                docker logs "$container" 2>&1 | tail -50 || true
            done
            TEST_FAILED=true
            exit_code=2
            break
        fi

        printf "\r[%3ds] Populate: %s/%s, Sync: %s/%s, Verify: %s/%s" "$elapsed" "$populate_done" "$WORKERS" "$sync_done" "$EXPECTED_SYNC_CONTAINERS" "$verify_done" "$WORKERS"

        # Check for completion
        if [[ "$sync_done" -ge "$EXPECTED_SYNC_CONTAINERS" ]] && [[ "$verify_done" -ge "$WORKERS" ]]; then
            VERIFY_END=$(date +%s)
            echo ""

            # Check verification results
            local verify_logs
            verify_logs=$(docker-compose -f docker-compose.loadtest.yml logs verify-1 verify-2 2>&1) || true
            if echo "$verify_logs" | grep -qE "[1-9][0-9]* mismatched"; then
                log_error "VERIFICATION FAILED: Data mismatches detected"
                echo "$verify_logs" | grep -E "Table|matched|mismatched|missing|successfully|Error" | sed 's/^/  /'
                TEST_FAILED=true
                exit_code=3
            else
                log "SUCCESS: All tables verified successfully"
                echo "$verify_logs" | grep -E "Table|rows verified|successfully" | sed 's/^/  /' | tail -20
                exit_code=0
            fi
            break
        fi

        # Check timeout
        if [[ $elapsed -ge $TIMEOUT ]]; then
            echo ""
            log_error "TIMEOUT: Test did not complete within $TIMEOUT seconds"
            log "Progress at timeout: Populate: $populate_done/$WORKERS, Sync: $sync_done/$EXPECTED_SYNC_CONTAINERS, Verify: $verify_done/$WORKERS"
            TEST_FAILED=true
            exit_code=1
            break
        fi

        sleep "$POLL_INTERVAL"
    done

    cd - > /dev/null
    return $exit_code
}

extract_verification_stats() {
    local verify_logs
    verify_logs=$(docker-compose -f "$OUTPUT_DIR/docker-compose.loadtest.yml" logs verify-1 verify-2 2>&1) || true

    local matched=0
    local mismatched=0
    local missing=0

    # Parse verification logs - format: "N matched, N missing, N mismatched"
    # Example: "100 rows verified in 40.515207ms - 100 matched, 0 missing, 0 mismatched"
    while IFS= read -r line; do
        # Extract numbers using grep -E (extended regex) with word boundaries
        local m
        m=$(echo "$line" | grep -oE '[0-9]+ matched' | grep -oE '[0-9]+' | head -1) || m=""
        if [[ -n "$m" ]]; then matched=$((matched + m)); fi
        m=$(echo "$line" | grep -oE '[0-9]+ mismatched' | grep -oE '[0-9]+' | head -1) || m=""
        if [[ -n "$m" ]]; then mismatched=$((mismatched + m)); fi
        m=$(echo "$line" | grep -oE '[0-9]+ missing' | grep -oE '[0-9]+' | head -1) || m=""
        if [[ -n "$m" ]]; then missing=$((missing + m)); fi
    done <<< "$verify_logs"

    echo "{\"matched\": $matched, \"mismatched\": $mismatched, \"missing\": $missing}"
}

write_metrics() {
    local exit_code="$1"

    local status="success"
    case $exit_code in
        0) status="success" ;;
        1) status="timeout" ;;
        2) status="failure" ;;
        3) status="verification_failed" ;;
        *) status="unknown" ;;
    esac

    # Calculate durations with safe defaults (bash doesn't support chained defaults)
    local populate_end_val="${POPULATE_END:-$END_TIME}"
    local sync_start_val="${SYNC_START:-$populate_end_val}"
    local sync_end_val="${SYNC_END:-$END_TIME}"
    local verify_start_val="${VERIFY_START:-$sync_end_val}"

    local total_duration=$((END_TIME - START_TIME))
    local populate_duration=$((populate_end_val - ${POPULATE_START:-$START_TIME}))
    local sync_duration=$((sync_end_val - sync_start_val))
    local verify_duration=$((${VERIFY_END:-$END_TIME} - verify_start_val))

    # Calculate throughput (rows synced per second)
    local total_rows=$((ROW_COUNT * 4))  # 4 tables
    local throughput=0
    # Use total_duration if sync_duration is 0 (happens when sync completes very quickly)
    local effective_sync_duration=$sync_duration
    if [[ $effective_sync_duration -le 0 ]]; then
        effective_sync_duration=$total_duration
    fi
    if [[ $effective_sync_duration -gt 0 ]]; then
        throughput=$(echo "scale=1; $total_rows / $effective_sync_duration" | bc -l 2>/dev/null || echo "0")
    fi

    local resource_stats
    resource_stats=$(calculate_resource_stats)

    local verification_stats
    verification_stats=$(extract_verification_stats)

    local git_sha=""
    local git_ref=""
    if command -v git &> /dev/null && git rev-parse --git-dir &> /dev/null; then
        git_sha=$(git rev-parse --short HEAD 2>/dev/null || echo "")
        git_ref=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
    fi

    cat > "$OUTPUT_DIR/metrics.json" << EOF
{
  "timestamp": "$(date -Iseconds)",
  "git_sha": "$git_sha",
  "git_ref": "$git_ref",
  "source": "$SOURCE",
  "preset": "$PRESET",
  "row_count": $ROW_COUNT,
  "workers": $WORKERS,
  "platform": "docker-compose",
  "runner": "local",
  "results": {
    "status": "$status",
    "exit_code": $exit_code,
    "total_duration_seconds": $total_duration,
    "populate_duration_seconds": $populate_duration,
    "sync_duration_seconds": $sync_duration,
    "verify_duration_seconds": $verify_duration,
    "rows_synced": $total_rows,
    "throughput_rows_per_sec": $throughput
  },
  "resources": $resource_stats,
  "verification": $verification_stats
}
EOF

    log "Metrics written to $OUTPUT_DIR/metrics.json"
}

# Cleanup handler for exit trap
cleanup_on_exit() {
    local exit_code=$?

    stop_stats_collector

    if [[ "$TEST_FAILED" == "true" ]] && [[ "$PRESERVE_ON_FAILURE" == "true" ]]; then
        echo ""
        log_warn "Test failed - preserving resources for debugging"
        echo ""
        echo "========================================================================"
        echo "  RESOURCES PRESERVED FOR DEBUGGING"
        echo "========================================================================"
        echo ""
        echo "To inspect resources:"
        echo "  docker-compose -f $OUTPUT_DIR/docker-compose.loadtest.yml ps -a"
        echo "  docker-compose -f $OUTPUT_DIR/docker-compose.loadtest.yml logs <service>"
        echo ""
        echo "To clean up manually:"
        echo "  ./scripts/cleanup.sh"
        echo "  # or"
        echo "  docker-compose -f $OUTPUT_DIR/docker-compose.loadtest.yml down -v"
        echo ""
        echo "========================================================================"
    else
        cleanup_docker_compose
    fi

    return $exit_code
}

main() {
    parse_args "$@"

    # Set expected sync containers based on source type
    EXPECTED_SYNC_CONTAINERS=$(get_expected_sync_containers)

    log "=== Starting CI Load Test ==="
    log "Source: $SOURCE"
    log "Preset: $PRESET"
    log "Row count: $ROW_COUNT"
    log "Workers: $WORKERS"
    log "Timeout: ${TIMEOUT}s"
    log "Expected sync containers: $EXPECTED_SYNC_CONTAINERS"
    log "Preserve on failure: $PRESERVE_ON_FAILURE"

    # Set up exit trap
    trap cleanup_on_exit EXIT

    # Initial cleanup of any previous failed resources (mandatory)
    initial_cleanup

    mkdir -p "$OUTPUT_DIR"

    # Build docker image
    if [[ "$SKIP_BUILD" != "true" ]]; then
        log "Building Docker image..."
        docker build -t "$IMAGE_NAME" -f "$PROJECT_ROOT/Dockerfile" "$PROJECT_ROOT"
    fi

    # Generate configuration
    log "Generating docker-compose configuration..."
    (
        cd "$PROJECT_ROOT"
        cargo run --release -- loadtest generate \
            --platform docker-compose \
            --source "$SOURCE" \
            --preset "$PRESET" \
            --schema "$LOADTEST_DIR/config/schema.yaml" \
            --output-dir "$OUTPUT_DIR" \
            --workers "$WORKERS" \
            --row-count "$ROW_COUNT"
    )

    # Start timing
    START_TIME=$(date +%s)
    POPULATE_START=$START_TIME

    # Start resource stats collection
    start_stats_collector

    # Start containers
    log "Starting containers..."
    docker-compose -f "$OUTPUT_DIR/docker-compose.loadtest.yml" up -d --build

    # Wait for completion
    log "Monitoring for completion..."
    local exit_code=0
    wait_for_completion || exit_code=$?

    END_TIME=$(date +%s)

    # Stop stats collection
    stop_stats_collector

    # Write metrics
    write_metrics "$exit_code"

    # Show summary
    log "=== Load Test Complete ==="
    log "Exit code: $exit_code"
    log "Duration: $((END_TIME - START_TIME))s"

    if [[ -f "$OUTPUT_DIR/metrics.json" ]]; then
        log "Metrics summary:"
        cat "$OUTPUT_DIR/metrics.json" | jq -r '"  Status: \(.results.status)\n  Throughput: \(.results.throughput_rows_per_sec) rows/sec\n  Verification: \(.verification.matched) matched, \(.verification.mismatched) mismatched"' 2>/dev/null || cat "$OUTPUT_DIR/metrics.json"
    fi

    exit $exit_code
}

main "$@"
