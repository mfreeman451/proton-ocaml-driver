#!/bin/bash
set -e

echo "🔧 Proton OCaml Driver Test Runner"
echo "=================================="

# Wait for Proton to be ready
echo "⏳ Waiting for Proton database to be ready..."
timeout=60
count=0
while [ $count -lt $timeout ]; do
    if curl -sf http://proton:8123/?query=SELECT%201 > /dev/null 2>&1; then
        echo "✅ Proton is ready!"
        break
    fi
    echo "   ... waiting ($count/$timeout)"
    sleep 2
    count=$((count + 2))
done

if [ $count -ge $timeout ]; then
    echo "❌ Proton failed to start within $timeout seconds"
    exit 1
fi

# Set up OCaml environment
echo "🐪 Setting up OCaml environment..."
eval $(opam env)

# Test basic connectivity
echo "🔌 Testing basic connectivity..."
curl -sf "http://proton:8123/?query=SELECT%20version()" || {
    echo "❌ Failed to connect to Proton"
    exit 1
}

# Build the project
echo "🔨 Building Proton OCaml driver..."
dune clean
dune build

# Run simple tests first
echo "🧪 Running simple tests..."
dune exec test_simple/simple_test.exe

# Run comprehensive test suite
echo "🧪 Running comprehensive test suite..."
dune test

# Run examples if they exist
echo "🎯 Running examples..."
if [ -f "_build/default/examples/compression_example.exe" ]; then
    echo "   Running compression example..."
    ./_build/default/examples/compression_example.exe
fi

if [ -f "_build/default/examples/tls_example.exe" ]; then
    echo "   Running TLS example (may fail without proper TLS setup)..."
    ./_build/default/examples/tls_example.exe || echo "   TLS example failed (expected without TLS certs)"
fi

echo ""
echo "✅ All tests completed successfully!"
echo "🎉 Your Proton OCaml driver is ready for development!"