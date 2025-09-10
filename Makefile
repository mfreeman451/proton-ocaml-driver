.PHONY: all build test clean install doc example lint lint-fmt lint-opam lint-doc lint-fix

# Default target
all: build

# Build the library
build:
	opam exec -- dune build @all

# Run all tests (always shows output)  
test:
	@opam exec -- dune exec test_lwt/test_suite_lwt.exe

# Run tests silently (only shows failures)
test-silent:
	@opam exec -- dune test

# Run tests with verbose output
test-verbose:
	@opam exec -- dune test --verbose

# Reader micro-benchmark only (no server needed)
bench-readers:
	@echo "Running reader micro-benchmarks (ONLY_READER_MICRO=1)"
	@ONLY_READER_MICRO=1 opam exec -- dune exec benchmark/benchmark_main.exe

# Clean build artifacts  
clean:
	opam exec -- dune clean

# Install the library locally
install:
	opam exec -- dune install

# Build documentation
doc:
	opam exec -- dune build @doc

# Run the example query
example:
	@opam exec -- dune exec examples/query

# Run the compression example
compression-example:
	@opam exec -- dune exec examples/compression_example

# Format code
format:
	opam exec -- dune build @fmt --auto-promote

# Check code formatting
check-format:
	opam exec -- dune build @fmt

# Development watch mode - rebuilds on file changes
watch:
	opam exec -- dune build @all --watch

# Run tests in watch mode
test-watch:
	opam exec -- dune test --watch

# Run live tests against real Proton database
livetest:
	@echo "Running live tests against Proton database..."
	@opam exec -- dune build @livetest

# Run end-to-end tests (alias for livetest)
e2e: livetest

# Run live test executable directly
test-live:
	@opam exec -- dune exec test_live/test_live.exe

# Show available targets
help:
	@echo "Available targets:"
	@echo "  build              - Build the library"
	@echo "  test               - Run all tests"
	@echo "  test-silent        - Run tests silently"  
	@echo "  test-verbose       - Run tests with verbose output"
	@echo "  bench-readers      - Run reader micro-benchmarks"
	@echo "  clean              - Clean build artifacts"
	@echo "  install            - Install the library locally"
	@echo "  doc                - Build documentation"
	@echo "  example            - Run the example query"
	@echo "  compression-example- Run compression example"
	@echo "  format             - Format code (auto-promote)"
	@echo "  check-format       - Check code formatting"
	@echo "  watch              - Development watch mode"
	@echo "  test-watch         - Run tests in watch mode"
	@echo "  livetest/e2e       - Run live tests against Proton"
	@echo "  test-live          - Run live test executable"
	@echo ""
	@echo "Linting targets:"
	@echo "  lint               - Run all linting checks"
	@echo "  lint-fmt           - Check code formatting"
	@echo "  lint-opam          - Check opam files"
	@echo "  lint-doc           - Check documentation"
	@echo "  lint-fix           - Auto-fix formatting issues"

# Run all linting checks (same as GitHub workflow)
lint: lint-fmt lint-opam lint-doc
	@echo "✅ All lint checks passed!"

# Check code formatting (same as ocaml/setup-ocaml/lint-fmt@v3)
lint-fmt:
	@echo "🔍 Checking code formatting..."
	@opam list ocamlformat > /dev/null 2>&1 || (echo "Installing ocamlformat..." && opam install ocamlformat -y)
	@opam exec -- dune build @fmt

# Check opam files (same as ocaml/setup-ocaml/lint-opam@v3)  
lint-opam:
	@echo "🔍 Checking opam files..."
	@opam exec -- opam lint ./proton.opam

# Check documentation (same as ocaml/setup-ocaml/lint-doc@v3)
lint-doc:
	@echo "🔍 Checking documentation..."
	@opam exec -- dune build @doc

# Fix all linting issues automatically
lint-fix:
	@echo "🔧 Auto-fixing formatting issues..."
	@opam list ocamlformat > /dev/null 2>&1 || (echo "Installing ocamlformat..." && opam install ocamlformat -y)
	@opam exec -- dune build @fmt --auto-promote
	@echo "✅ Formatting issues fixed!"
