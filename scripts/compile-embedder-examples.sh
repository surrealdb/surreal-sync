#!/usr/bin/env bash
set -euo pipefail

# Compile embedder example packages (examples/from-*).
# Mirrors the "Compile embedder examples" CI gate.
#
# Usage: ./scripts/compile-embedder-examples.sh [--profile PROFILE]
#
# Defaults to cargo's default profile (dev). CI passes --profile ci.

PROFILE_ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --profile)
      if [[ $# -lt 2 ]]; then
        echo "error: --profile requires an argument" >&2
        exit 1
      fi
      PROFILE_ARGS=(--profile "$2")
      shift 2
      ;;
    -h|--help)
      sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Usage: $0 [--profile PROFILE]" >&2
      exit 1
      ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PACKAGES=(
  surreal-sync-example-from-mysql-binlog
  surreal-sync-example-from-snowflake
)

for pkg in "${PACKAGES[@]}"; do
  echo "Compiling $pkg..."
  cargo build "${PROFILE_ARGS[@]}" -p "$pkg"
done

echo "Embedder examples compiled successfully"
