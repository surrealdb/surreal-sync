.PHONY: test check fmt clippy build clean clean-logs help install-tools install-hooks

# Log directory for test output
LOGS_DIR := logs/test

# Default target
help:
	@echo "Surreal-Sync Development Targets:"
	@echo ""
	@echo "Quality Assurance:"
	@echo "  test          - Run all checks and tests"
	@echo ""
	@echo "Development:"
	@echo "  build         - Build the release binary"
	@echo "  clean         - Clean build artifacts"
	@echo "  clean-logs    - Clean test log files"
	@echo "  install-tools - Install required Rust toolchain components"
	@echo "  install-hooks - Install git pre-push hook for local CI checks"

# Install required Rust toolchain components
install-tools:
	@echo "🔧 Installing Rust toolchain components..."
	rustup component add rustfmt clippy
	@echo "✅ Toolchain components installed"

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

# Check for compilation warnings (fail if any warnings found, including all workspace members)
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

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	cargo clean
	@echo "✅ Clean complete"

# Depends on build because cli tests need the binary
test: fmt check clippy build
	@mkdir -p $(LOGS_DIR)
	@echo "Test logs will be written to: $(LOGS_DIR)/"

	@echo "Running unit tests..."
	@if ! cargo test --workspace --no-fail-fast --lib > $(LOGS_DIR)/unit.log 2>&1; then \
		echo "Unit tests failed. See: $(LOGS_DIR)/unit.log"; \
		exit 1; \
	fi
	@echo "Unit tests passed"

	@echo "Running integration tests..."
	@if ! SURREAL_SYNC_BIN=$(CURDIR)/target/release/surreal-sync RUST_TEST_THREADS=1 cargo test --workspace --no-fail-fast --tests > $(LOGS_DIR)/integration.log 2>&1; then \
		echo "Integration tests failed. See: $(LOGS_DIR)/integration.log"; \
		exit 1; \
	fi
	@echo "Integration tests passed"

	@echo "Running documentation tests..."
	@if ! cargo test --workspace --no-fail-fast --doc > $(LOGS_DIR)/doc.log 2>&1; then \
		echo "Documentation tests failed. See: $(LOGS_DIR)/doc.log"; \
		exit 1; \
	fi
	@echo "Documentation tests passed"

	@echo "All tests passed. Logs: $(LOGS_DIR)/"

# Clean test logs
clean-logs:
	rm -rf logs/test/
