.PHONY: all build test clean install doc example

# Default target
all: build

# Build the library
build:
	dune build @all

# Run all tests (always shows output)  
test:
	@dune build test/test_suite.exe
	@./_build/default/test/test_suite.exe

# Run tests silently (only shows failures)
test-silent:
	@dune test

# Run tests with verbose output
test-verbose:
	@dune test --verbose

# Clean build artifacts  
clean:
	dune clean

# Install the library locally
install:
	dune install

# Build documentation
doc:
	dune build @doc

# Run the example query
example:
	@dune exec examples/query

# Run the compression example
compression-example:
	@dune exec examples/compression_example

# Format code
format:
	dune build @fmt --auto-promote

# Check code formatting
check-format:
	dune build @fmt

# Development watch mode - rebuilds on file changes
watch:
	dune build @all --watch

# Run tests in watch mode
test-watch:
	dune test --watch