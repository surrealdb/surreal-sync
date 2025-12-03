.PHONY: test check fmt clippy build clean help install-tools

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
	@echo "  install-tools - Install required Rust toolchain components"

# Install required Rust toolchain components
install-tools:
	@echo "🔧 Installing Rust toolchain components..."
	rustup component add rustfmt clippy
	@echo "✅ Toolchain components installed"

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
	@echo "📊 Unit tests..."
	@if ! cargo test --workspace --no-fail-fast --lib; then \
		echo "❌ Unit tests failed"; \
		exit 1; \
	fi

	@echo "🔗 Integration tests..."
	@echo "   Running all integration tests with database isolation..."
	@if ! RUST_TEST_THREADS=1 cargo test --workspace --no-fail-fast --tests; then \
		echo "❌ Integration tests failed"; \
		exit 1; \
	fi

	@echo "📖 Documentation tests..."
	@if ! cargo test --workspace --no-fail-fast --doc; then \
		echo "❌ Documentation tests failed"; \
		exit 1; \
	fi

	@echo "✅ All the tests passed"
