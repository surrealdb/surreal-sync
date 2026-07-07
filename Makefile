.PHONY: test check fmt clippy build build-debug clean clean-logs help install-tools install-hooks system-deps prebuild-test-images prepull-binlog-images print-test-images services-info

include scripts/test-images.mk

# Path to the debug CLI binary used by the *_cli integration tests.
# We deliberately use the debug binary (not release) so the test path doesn't
# trigger a second, full release compile of the dependency tree.
SURREAL_SYNC_BIN := $(CURDIR)/target/debug/surreal-sync

# macOS: help cargo's C build scripts (rdkafka, openssl-sys, ...) find Homebrew
# OpenSSL, since the system has no pkg-config-discoverable OpenSSL by default.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
OPENSSL_PREFIX := $(shell brew --prefix openssl@3 2>/dev/null)
ifneq ($(OPENSSL_PREFIX),)
export OPENSSL_DIR := $(OPENSSL_PREFIX)
export OPENSSL_ROOT_DIR := $(OPENSSL_PREFIX)
export CFLAGS := -I$(OPENSSL_PREFIX)/include $(CFLAGS)
export CPPFLAGS := -I$(OPENSSL_PREFIX)/include $(CPPFLAGS)
export LDFLAGS := -L$(OPENSSL_PREFIX)/lib $(LDFLAGS)
endif
endif

# Default target
help:
	@echo "Surreal-Sync Development Targets:"
	@echo ""
	@echo "Quality Assurance:"
	@echo "  test          - Build (debug) + run all checks and tests via cargo-nextest"
	@echo "  clippy        - Run the clippy linter"
	@echo "  fmt           - Format code"
	@echo ""
	@echo "Development:"
	@echo "  build         - Build the release binary"
	@echo "  build-debug   - Build the debug binary (used by CLI tests)"
	@echo "  clean         - Clean build artifacts"
	@echo "  clean-logs    - Clean test log files"
	@echo "  install-tools - Install Rust toolchain components + cargo-nextest"
	@echo "  install-hooks - Install git pre-push hook for local CI checks"
	@echo "  system-deps   - Install/print the required system build dependencies"
	@echo ""
	@echo "Tests require a running Docker daemon: the suite starts its own"
	@echo "throwaway database containers (SurrealDB/Postgres/MySQL/Mongo/Neo4j/Kafka)."

# Install required Rust toolchain components and cargo-nextest
install-tools:
	@echo "🔧 Installing Rust toolchain components..."
	rustup component add rustfmt clippy
	@echo "🔧 Installing cargo-nextest..."
	@command -v cargo-nextest >/dev/null 2>&1 || cargo install cargo-nextest --locked
	@echo "✅ Toolchain components and cargo-nextest installed"

# Install the system build dependencies (OpenSSL, pkg-config, cmake, protobuf, ...)
system-deps:
ifeq ($(UNAME_S),Darwin)
	brew install openssl@3 pkg-config cmake protobuf
else
	@echo "On Debian/Ubuntu, run:"
	@echo "  sudo apt-get update && sudo apt-get install -y \\"
	@echo "    build-essential cmake libssl-dev libsasl2-dev pkg-config protobuf-compiler postgresql-client"
endif

# Install git hooks for pre-push checks (fmt + clippy)
install-hooks:
	@echo "🔗 Installing git hooks..."
	git config core.hooksPath .githooks
	@echo "✅ Git hooks installed (using .githooks/)"

# Format code according to Rust standards
fmt:
	@echo "🎨 Formatting code..."
	cargo fmt --all
	@echo "✅ Code formatting complete"

# Check for compilation warnings (not part of `test`; clippy already compiles all targets)
check:
	@echo "🔍 Checking for compilation warnings..."
	cargo check --workspace --all-targets --all-features
	@echo "✅ No compilation warnings found"

# Run clippy linter for code quality (including all workspace members)
clippy:
	@echo "📎 Running clippy linter..."
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	@echo "✅ Clippy checks passed with no warnings"

# Build the release binary
build:
	@echo "🔨 Building release binary..."
	cargo build --release
	@echo "✅ Build complete"

# Build the debug binary (used by the *_cli integration tests)
build-debug:
	@echo "🔨 Building debug binary..."
	cargo build
	@echo "✅ Debug build complete"

# Pre-pull binlog images and pre-build the custom PostgreSQL (wal2json) test image
# so parallel test processes find them in Docker's layer cache instead of racing.
prebuild-test-images: prepull-binlog-images
	@echo "🐳 Pre-building PostgreSQL wal2json test image..."
	docker build -t postgres-wal2json-test \
		-f crates/postgresql-wal2json-source/Dockerfile.postgres16.wal2json \
		crates/postgresql-wal2json-source
	@echo "✅ Test images ready"

# Run fmt, clippy, build the debug binary, then the full test suite via nextest.
# Integration tests spin up their own Docker containers, so a Docker daemon must
# be running. nextest runs tests in parallel across the whole workspace; doctests
# are run separately because nextest does not execute them.
test: fmt clippy build-debug prebuild-test-images
	@echo "Running unit + integration tests (cargo-nextest)..."
	SURREAL_SYNC_BIN=$(SURREAL_SYNC_BIN) cargo nextest run --workspace
	@echo "Running documentation tests..."
	cargo test --workspace --doc
	@echo "✅ All tests passed"

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	cargo clean
	@echo "✅ Clean complete"

# Clean test logs
clean-logs:
	rm -rf logs/test/
