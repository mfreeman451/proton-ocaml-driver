# Testing the Proton OCaml Driver

## Quick Start

Run all tests:
```bash
make test
# or
dune test
```

## Test Commands

### Basic Testing
```bash
# Run all tests
dune test

# Run tests with verbose output
dune test --verbose

# Clean and rebuild before testing
dune clean && dune test
```

### Using Make
```bash
# Run tests (always shows output)
make test

# Run tests silently (only shows failures) 
make test-silent

# Verbose tests with build info
make test-verbose  

# Watch mode (re-runs on file changes)
make test-watch

# Clean build artifacts
make clean
```

### Test Output

All tests use the Alcotest framework, which provides colored output:
- ✅ `[OK]` - Test passed
- ❌ `[FAIL]` - Test failed with details

## Test Structure

```
test/
└── test_suite.ml    # Main test file with all test cases
```

### Test Categories

1. **CityHash** - Tests the CityHash128 implementation
2. **Compression** - LZ4 compression/decompression tests
3. **Binary** - Binary encoding/decoding tests  
4. **Connection** - Connection and client creation tests

## Adding New Tests

1. Add your test function to `test/test_suite.ml`:
```ocaml
let test_my_feature () =
  Alcotest.(check string) "description" "expected" "actual"
```

2. Add it to a test suite:
```ocaml
let my_tests = [
  Alcotest.test_case "My test" `Quick test_my_feature;
]
```

3. Register the suite in the main runner:
```ocaml
let () =
  Alcotest.run "Proton OCaml Driver" [
    "My Tests", my_tests;
    (* ... other suites ... *)
  ]
```

## Running Specific Tests

To run a specific test suite, you can filter by name:
```bash
dune exec test/test_suite.exe -- test "Compression"
```

## Continuous Integration

The test suite is designed to be CI-friendly:
- Exit code 0 on success, non-zero on failure
- Machine-readable output available
- No external dependencies beyond OCaml libraries