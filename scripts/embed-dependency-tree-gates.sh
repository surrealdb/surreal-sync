#!/usr/bin/env bash
set -euo pipefail

# Embed dependency tree gates for surreal-sync crates and examples/from-*.
# Mirrors the "Embed dependency tree gates" CI step.
#
# Requires: cargo (with cargo-tree), ripgrep (rg)
#
# Usage: ./scripts/embed-dependency-tree-gates.sh

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v rg >/dev/null 2>&1; then
  echo "error: ripgrep (rg) is required but not found on PATH" >&2
  echo "  macOS:  brew install ripgrep" >&2
  echo "  Debian/Ubuntu: sudo apt-get install -y ripgrep" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required but not found on PATH" >&2
  exit 1
fi

# Kafka / snowflake / runtime / mysql must not pull surrealdb
# in the normal+build graph (embed examples live in separate packages).
for pkg in surreal-sync-kafka surreal-sync-snowflake surreal-sync-runtime surreal-sync-mysql; do
  out=$(cargo tree -p "$pkg" -e normal,build -i surrealdb 2>&1 || true)
  if echo "$out" | rg -q '^surrealdb v'; then
    echo "FAIL: $pkg unexpectedly depends on surrealdb:"
    echo "$out"
    exit 1
  fi
  echo "OK: $pkg has no surrealdb in normal/build graph"
done

# snowflake / mysql must not pull kafka or neo4j clients
for pkg in surreal-sync-snowflake surreal-sync-mysql; do
  for deny in rdkafka neo4rs; do
    out=$(cargo tree -p "$pkg" -e normal,build -i "$deny" 2>&1 || true)
    if echo "$out" | rg -q "^${deny} "; then
      echo "FAIL: $pkg unexpectedly depends on $deny:"
      echo "$out"
      exit 1
    fi
  done
  echo "OK: $pkg has no rdkafka/neo4rs in normal/build graph"
done

# Surreal crate must not pull clap by default (runtime cli feature is opt-in)
for feats in "v2" "v3"; do
  out=$(cargo tree -p surreal-sync-surreal --features "$feats" -e normal,build -i clap 2>&1 || true)
  if echo "$out" | rg -q '^clap v'; then
    echo "FAIL: surreal-sync-surreal --features $feats unexpectedly depends on clap:"
    echo "$out"
    exit 1
  fi
  echo "OK: surreal-sync-surreal --features $feats has no clap in normal/build graph"
done

# surreal-sync-core must not list tokio in [dependencies] (dev-only ok)
if rg -n '^\s*tokio\s*=' crates/sync-core/Cargo.toml; then
  echo "FAIL: surreal-sync-core must not depend on tokio"
  exit 1
fi
echo "OK: surreal-sync-core has no tokio dep"

# Single-major feature graphs: exactly one surrealdb major
# (default includes v3 — disable defaults so `v2` is not dual-SDK)
for feats in "v2" "v3"; do
  tree=$(cargo tree -p surreal-sync-surreal --no-default-features --features "$feats" -e normal,build)
  majors=$(echo "$tree" | rg -o 'surrealdb v[0-9]+' | sort -u | wc -l | tr -d ' ')
  if [[ "$majors" != "1" ]]; then
    echo "FAIL: surreal-sync-surreal --no-default-features --features $feats should depend on exactly one surrealdb major, got $majors"
    echo "$tree"
    exit 1
  fi
  echo "OK: surreal-sync-surreal --no-default-features --features $feats has one surrealdb major"
done

# Documented embed graphs (examples/from-* packages mirror a real embedder).
for pkg in surreal-sync-example-from-snowflake surreal-sync-example-from-mysql-binlog; do
  for deny in rdkafka neo4rs; do
    out=$(cargo tree -p "$pkg" -e normal,build -i "$deny" 2>&1 || true)
    if echo "$out" | rg -q "^${deny} "; then
      echo "FAIL: $pkg unexpectedly depends on $deny:"
      echo "$out"
      exit 1
    fi
  done
  tree=$(cargo tree -p "$pkg" -e normal,build -i surrealdb 2>&1 || true)
  if ! echo "$tree" | rg -q '^surrealdb v'; then
    echo "FAIL: $pkg should link surrealdb (Surreal3Sink)"
    echo "$tree"
    exit 1
  fi
  majors=$(echo "$tree" | rg -o 'surrealdb v[0-9]+' | sort -u | wc -l | tr -d ' ')
  if [[ "$majors" != "1" ]]; then
    echo "FAIL: $pkg should depend on exactly one surrealdb major, got $majors"
    echo "$tree"
    exit 1
  fi
  if echo "$tree" | rg -q 'surrealdb v2'; then
    echo "FAIL: $pkg should use SurrealDB v3 only"
    echo "$tree"
    exit 1
  fi
  echo "OK: $pkg has no kafka/neo4j and exactly one surrealdb v3 major"
done

# mysql-binlog CDC checkpoints live in surreal-sync-runtime (checkpoint_fs),
# not a separate checkpoint-fs package
if ! cargo tree -p surreal-sync-example-from-mysql-binlog -e normal,build \
    | rg -q 'surreal-sync-surreal'; then
  echo "FAIL: mysql-binlog example should depend on surreal-sync-surreal"
  exit 1
fi
if ! cargo tree -p surreal-sync-example-from-mysql-binlog -e normal,build \
    | rg -q 'surreal-sync-runtime'; then
  echo "FAIL: mysql-binlog example should pull surreal-sync-runtime (checkpoint_fs)"
  exit 1
fi
if cargo tree -p surreal-sync-example-from-mysql-binlog -e normal,build \
    | rg -q 'surreal-sync-checkpoint-fs'; then
  echo "FAIL: obsolete package surreal-sync-checkpoint-fs must not appear in the graph"
  exit 1
fi
echo "OK: mysql-binlog example uses surreal-sync-runtime checkpoints"

# Embed example sources monomorphize Surreal3Sink only
if ! rg -n 'run::<Surreal3Sink>' examples/from-snowflake examples/from-mysql-binlog; then
  echo "FAIL: examples should monomorphize Surreal3Sink"
  exit 1
fi
if rg -n 'run::<Surreal2Sink>|Surreal2Sink' examples/from-snowflake examples/from-mysql-binlog; then
  echo "FAIL: embed examples must not reference Surreal2Sink"
  exit 1
fi
echo "OK: examples use run::<Surreal3Sink> only"

echo "Embed dependency tree gates passed"
