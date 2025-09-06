.PHONY: all build test clean install doc example

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
