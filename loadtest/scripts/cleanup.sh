#!/bin/bash
set -euo pipefail

# Cleanup script for loadtest resources
# Usage: ./scripts/cleanup.sh [OPTIONS]
#
# This script removes all loadtest resources from a previous run,
# including docker-compose containers, volumes, networks, and orphans.
#
# Options:
#   --output-dir DIR    Output directory (default: ./output)
#   --force             Force cleanup without confirmation
#   --help              Show this help message

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOADTEST_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="${OUTPUT_DIR:-$LOADTEST_DIR/output}"
FORCE="${FORCE:-false}"

usage() {
    head -14 "$0" | tail -12 | sed 's/^# //' | sed 's/^#//'
    exit 0
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --output-dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --force|-f)
                FORCE="true"
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

confirm() {
    if [[ "$FORCE" == "true" ]]; then
        return 0
    fi

    echo "This will remove all loadtest resources:"
    echo "  - Docker containers from docker-compose"
    echo "  - Associated volumes and networks"
    echo "  - Orphaned loadtest containers"
    echo ""
    read -p "Continue? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
}

cleanup_docker_compose() {
    if [[ -f "$OUTPUT_DIR/docker-compose.loadtest.yml" ]]; then
        log "Stopping docker-compose services..."
        docker-compose -f "$OUTPUT_DIR/docker-compose.loadtest.yml" down -v --remove-orphans 2>/dev/null || true
    else
        log "No docker-compose.loadtest.yml found in $OUTPUT_DIR"
    fi
}

cleanup_orphans() {
    log "Looking for orphaned loadtest containers..."

    # Find containers with loadtest label
    local orphans
    orphans=$(docker ps -aq --filter "label=com.surreal-loadtest" 2>/dev/null) || true

    if [[ -n "$orphans" ]]; then
        log "Removing $(echo "$orphans" | wc -w) orphaned containers..."
        echo "$orphans" | xargs -r docker rm -f 2>/dev/null || true
    else
        log "No orphaned containers found"
    fi
}

cleanup_networks() {
    log "Cleaning up networks..."

    # Remove loadtest default network if it exists
    docker network rm loadtest_default 2>/dev/null || true
    docker network rm output_default 2>/dev/null || true

    # Prune networks with loadtest label
    docker network prune -f --filter "label=com.surreal-loadtest" 2>/dev/null || true
}

cleanup_volumes() {
    log "Cleaning up volumes..."

    # Prune volumes with loadtest label
    docker volume prune -f --filter "label=com.surreal-loadtest" 2>/dev/null || true

    # Also try to remove common loadtest volume names
    local volume_patterns=(
        "output_mysql-data"
        "output_postgresql-data"
        "output_mongodb-data"
        "output_neo4j-data"
        "output_kafka-data"
        "output_surrealdb-data"
        "loadtest_mysql-data"
        "loadtest_postgresql-data"
        "loadtest_mongodb-data"
        "loadtest_neo4j-data"
        "loadtest_kafka-data"
        "loadtest_surrealdb-data"
    )

    for vol in "${volume_patterns[@]}"; do
        docker volume rm "$vol" 2>/dev/null || true
    done
}

show_remaining() {
    echo ""
    log "Checking for remaining resources..."

    local containers
    containers=$(docker ps -a --filter "label=com.surreal-loadtest" --format "{{.Names}}" 2>/dev/null) || true
    if [[ -n "$containers" ]]; then
        log "Remaining containers:"
        echo "$containers" | sed 's/^/  - /'
    fi

    local networks
    networks=$(docker network ls --filter "label=com.surreal-loadtest" --format "{{.Name}}" 2>/dev/null) || true
    if [[ -n "$networks" ]]; then
        log "Remaining networks:"
        echo "$networks" | sed 's/^/  - /'
    fi

    if [[ -z "$containers" ]] && [[ -z "$networks" ]]; then
        log "All loadtest resources cleaned up successfully"
    fi
}

main() {
    parse_args "$@"

    log "=== Loadtest Cleanup ==="

    confirm

    cleanup_docker_compose
    cleanup_orphans
    cleanup_networks
    cleanup_volumes
    show_remaining

    log "=== Cleanup Complete ==="
}

main "$@"
