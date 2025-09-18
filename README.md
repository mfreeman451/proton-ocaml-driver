# 🐪 Timeplus Proton OCaml Driver

[![Lint](https://github.com/mfreeman451/proton-ocaml-driver/actions/workflows/lint.yml/badge.svg)](https://github.com/mfreeman451/proton-ocaml-driver/actions/workflows/lint.yml)
[![Build and Test](https://github.com/mfreeman451/proton-ocaml-driver/actions/workflows/build-test.yml/badge.svg)](https://github.com/mfreeman451/proton-ocaml-driver/actions/workflows/build-test.yml)
[![Opam Dependency Submission](https://github.com/mfreeman451/proton-ocaml-driver/actions/workflows/opam-dependency-submission.yml/badge.svg)](https://github.com/mfreeman451/proton-ocaml-driver/actions/workflows/opam-dependency-submission.yml)

A high-performance, feature-rich OCaml driver for [Timeplus Proton](https://timeplus.com/) - the streaming database built on ClickHouse.

## Features

- **Streaming Queries** - Process large datasets with constant memory usage
- **Async Inserts** - High-throughput data ingestion with automatic batching
- **Compression** - LZ4 and ZSTD support for reduced network overhead  
- **TLS Security** - Secure connections with certificate validation
- **Connection Pooling** - Efficient resource management for high-concurrency applications
- **Rich Data Types** - Full support for ClickHouse types including Arrays, Maps, Enums, DateTime64
- **Idiomatic OCaml** - Functional API leveraging OCaml's strengths

## Table of Contents

- [Quick Start](#quick-start)
- [Parameterized Queries](#parameterized-queries)
- [Streaming Queries](#streaming-queries)
- [Async Inserts](#async-inserts)
- [Data Types](#data-types)
- [Configuration Options](#configuration-options)
- [Real-World Examples](#real-world-examples)
- [Development Setup](#development-setup)
- [Testing](#testing)
- [Docker Environment](#docker-environment)
- [TLS Configuration](#tls-configuration)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## Quick Start

### Installation

```bash
opam install proton
```

### Basic Usage

```ocaml
open Proton

(* Create a client *)
let client = Client.create ~host:"localhost" ~port:8463 ~database:"default" () in

(* Execute a simple query *)
let%lwt result = Client.execute client "SELECT name, age FROM users LIMIT 10" in
match result with
| Client.NoRows -> Lwt_io.println "No results found"
| Client.Rows (rows, columns) ->
    List.iter (fun row ->
      let values = List.map Column.value_to_string row in
      Printf.printf "%s\n" (String.concat ", " values)
    ) rows;
    Lwt.return_unit
```

## Parameterized Queries

Avoid string interpolation by binding values through named placeholders. Placeholders use
the syntax `{{name}}` and are supplied as `(string * Proton.Column.value)` pairs.

### One-off execution

```ocaml
open Proton

let client = Client.create () in

let%lwt result =
  Client.execute_with_params client
    "SELECT * FROM events WHERE tenant = {{tenant}} AND ts >= {{start}}"
    ~params:[
      ("tenant", Column.String "acme");
      ("start", Column.DateTime64 (1_700_000_000_000L, 3, Some "UTC"));
    ]
in
match result with
| Client.NoRows -> print_endline "no matches"
| Client.Rows (rows, _) -> Printf.printf "fetched %d rows\n" (List.length rows)
```

### Reusable prepared statements

```ocaml
open Proton

let client = Client.create () in
let stmt = Client.prepare client
  "SELECT count(*) FROM metrics WHERE name = {{metric}} AND ts >= {{window_start}}"
in

let run metric window_start =
  Client.execute_prepared client stmt
    ~params:[
      ("metric", Column.String metric);
      ("window_start", Column.DateTime64 (window_start, 3, Some "UTC"));
    ]
in

let%lwt yesterday = run "cpu.usage" 1_699_913_600_000L in
let%lwt today = run "cpu.usage" 1_699_999_200_000L in
(* ... *)
```

All streaming helpers have `_with_params` and `_prepared` variants so you can bind values while
folding or iterating over large result sets.

## Streaming Queries

Process arbitrarily large result sets with constant memory usage using OCaml's functional patterns:

### Fold Pattern - Aggregate Data

```ocaml
(* Calculate total sales amount *)
let%lwt total_sales = Client.query_fold client
  "SELECT amount FROM sales WHERE date >= '2024-01-01'"
  ~init:0.0
  ~f:(fun acc row -> match row with
    | [Column.Float64 amount] -> Lwt.return (acc +. amount)  
    | _ -> Lwt.return acc)
in
Printf.printf "Total sales: $%.2f\n" total_sales
```

### Iter Pattern - Process Each Row

```ocaml
(* Send personalized emails *)
let%lwt () = Client.query_iter client
  "SELECT name, email FROM users WHERE active = 1"
  ~f:(fun row -> match row with
    | [Column.String name; Column.String email] ->
        send_email ~to:email ~subject:("Hi " ^ name) ~body:"..."
    | _ -> Lwt.return_unit)
```

### Sequence Pattern - Lazy Processing Pipeline

```ocaml
(* Process data in pipeline with lazy evaluation *)
let%lwt user_seq = Client.query_to_seq client "SELECT * FROM users" in
let processed = user_seq
  |> Seq.filter is_premium_user
  |> Seq.map extract_preferences  
  |> Seq.take 1000
  |> Seq.fold_left update_recommendations init_state
```

### Advanced - With Column Metadata

```ocaml
(* Get both data and column information *)
let%lwt result = Client.query_fold_with_columns client
  "SELECT id, name, created_at FROM products"
  ~init:[]
  ~f:(fun acc row columns ->
    let product = parse_product_row row columns in
    Lwt.return (product :: acc))
in
Printf.printf "Found %d products with columns: %s\n"
  (List.length result.rows)
  (String.concat "," (List.map fst result.columns))
```

## Async Inserts

The Proton OCaml driver provides powerful async insert functionality for high-throughput data ingestion scenarios where you need to insert many rows efficiently without blocking your application.

### Key Features

- **Batching**: Automatically batches multiple rows before sending to reduce network overhead
- **Background processing**: Non-blocking inserts that don't block your main application logic
- **Configurable triggers**: Flush based on row count, byte size, or time intervals
- **Error handling**: Automatic retry logic with exponential backoff
- **Buffer management**: Automatic memory management with configurable limits
- **Binary protocol**: Uses ClickHouse native binary Data packet protocol for optimal performance

### Simple Insert

```ocaml
open Proton

let client = Client.create ~host:"localhost" ~database:"mydb" () in

(* Insert a single row *)
let row = [
  Column.String "user123";
  Column.Int32 42l;
  Column.DateTime (Int64.of_float (Unix.gettimeofday ()), None)
] in

Client.insert_row client "events" 
  ~columns:[("user_id", "String"); ("value", "Int32"); ("timestamp", "DateTime")]
  row
```

### Batch Insert

```ocaml
let rows = [
  [Column.String "user1"; Column.Int32 10l];
  [Column.String "user2"; Column.Int32 20l];
  [Column.String "user3"; Column.Int32 30l];
] in

Client.insert_rows client "users"
  ~columns:[("name", "String"); ("score", "Int32")]
  rows
```

### Advanced Usage with Custom Configuration

```ocaml
(* Create custom configuration *)
let config = { 
  (Async_insert.default_config "events") with
  max_batch_size = 10000;     (* Batch up to 10k rows *)
  max_batch_bytes = 5_000_000; (* Or 5MB of data *)
  flush_interval = 10.0;       (* Flush every 10 seconds *)
  max_retries = 5;             (* Retry failed batches 5 times *)
  retry_delay = 2.0;           (* Start with 2 second retry delay *)
} in

(* Create and start async inserter *)
let inserter = Async_insert.create config client.conn in
Async_insert.start inserter;

(* Add rows continuously *)
let rec add_events () =
  let event = generate_event_data () in
  let%lwt () = Async_insert.add_row inserter event in
  (* Your application logic continues immediately *)
  let%lwt () = Lwt_unix.sleep 0.1 in
  add_events ()
in

Lwt.async add_events;

(* Later, when shutting down *)
let%lwt () = Async_insert.stop inserter in
Client.disconnect client
```

### Configuration Options

#### `Async_insert.config`

- **`table_name`**: Target table name
- **`max_batch_size`**: Maximum rows per batch (default: 1000)
- **`max_batch_bytes`**: Maximum bytes per batch (default: 1MB)
- **`flush_interval`**: Seconds between automatic flushes (default: 5.0)
- **`max_retries`**: Maximum retry attempts on failure (default: 3)
- **`retry_delay`**: Initial retry delay in seconds (default: 1.0)

### API Reference

#### Client Methods

- `Client.insert_row`: Insert a single row (convenience method)
- `Client.insert_rows`: Insert multiple rows (convenience method)
- `Client.create_async_inserter`: Create a reusable async inserter

#### Async_insert Methods

- `Async_insert.create`: Create an async inserter
- `Async_insert.start`: Start background processing
- `Async_insert.stop`: Stop and flush remaining data
- `Async_insert.add_row`: Add a single row to buffer
- `Async_insert.add_rows`: Add multiple rows to buffer
- `Async_insert.flush`: Force immediate flush
- `Async_insert.get_stats`: Get current buffer statistics

### Performance Considerations

1. **Batch size**: Larger batches reduce network overhead but use more memory
2. **Flush interval**: Longer intervals improve throughput but increase latency
3. **Retry strategy**: More retries improve reliability but can delay error detection
4. **Binary protocol**: Uses native ClickHouse binary format for maximum efficiency

### Error Handling

The async inserter handles errors gracefully:
- Network failures trigger automatic retries with exponential backoff
- After max retries, errors are logged but don't crash your application
- Use `Async_insert.stop` to ensure all data is flushed before shutdown

### Thread Safety

The async inserter is thread-safe and uses Lwt mutexes internally. Multiple threads can safely call `add_row` concurrently.

### Real-World Example

```ocaml
(* High-throughput event ingestion system *)
open Lwt.Syntax

let setup_event_pipeline () =
  let client = Client.create ~host:"proton-cluster" ~database:"analytics" () in
  
  let config = { 
    (Async_insert.default_config "events") with
    max_batch_size = 5000;
    max_batch_bytes = 2_000_000;
    flush_interval = 5.0;
    max_retries = 3;
  } in
  
  let inserter = Async_insert.create config client.conn in
  Async_insert.start inserter;
  
  (* Process incoming events *)
  let process_event event_json =
    let event_row = [
      Column.String event_json.user_id;
      Column.String event_json.event_type;
      Column.DateTime (Int64.of_float event_json.timestamp, Some "UTC");
      Column.String (Yojson.to_string event_json.properties);
    ] in
    Async_insert.add_row inserter event_row
  in
  
  (* Setup graceful shutdown *)
  Lwt_unix.on_signal Sys.sigterm (fun _ ->
    Printf.printf "Shutting down event pipeline...\n";
    Lwt.async (fun () ->
      let* () = Async_insert.stop inserter in
      let* () = Client.disconnect client in
      Lwt_io.println "Pipeline shutdown complete"
    )
  );
  
  (inserter, process_event)
```

## Data Types

Full support for ClickHouse data types with OCaml-native representations:

```ocaml
open Column

let sample_row = [
  String "Hello World";
  Int32 42l;
  Int64 1234567890L;
  Float64 3.14159;
  DateTime (Int64.of_float (Unix.gettimeofday ()), Some "UTC");
  DateTime64 (Int64.of_float (Unix.gettimeofday () *. 1000.), 3, Some "UTC");
  Enum8 ("status", 1);
  Array [| Int32 1l; Int32 2l; Int32 3l |];
  Map [(String "key1", String "value1"); (String "key2", String "value2")];
]
```

## Configuration Options

### Connection Configuration

```ocaml
let client = Client.create
  ~host:"proton.example.com"
  ~port:8463
  ~database:"analytics" 
  ~user:"readonly"
  ~password:"secret123"
  ~compression:Protocol.LZ4      (* or ZSTD, or None *)
  ~tls_config:{
    ca_file = Some "/path/to/ca.pem";
    cert_file = Some "/path/to/client.pem"; 
    key_file = Some "/path/to/client.key";
    verify_hostname = true;
  }
  ~settings:[
    ("max_block_size", "65536");
    ("connect_timeout", "10");
  ]
  ()
```

### Connection Pooling

```ocaml
(* For high-concurrency applications *)
let pool = Pool_lwt.create 
  ~max_connections:20
  ~create:(fun () -> 
    Client.create ~host:"localhost" ~database:"default" ())
  ~validate:(fun client ->
    Lwt.catch
      (fun () -> let%lwt _ = Client.execute client "SELECT 1" in Lwt.return true)
      (fun _ -> Lwt.return false))
  ()
in

Pool_lwt.use pool (fun client ->
  Client.query_fold client "SELECT * FROM large_table" ~init:0 ~f:(...)
)
```

## Real-World Examples

### Analytics Pipeline

```ocaml
(* Process clickstream data *)
let process_clickstream () =
  let%lwt () = Client.query_iter client
    "SELECT user_id, page, timestamp FROM clicks WHERE date = today()"
    ~f:(fun row -> match row with
      | [String user_id; String page; DateTime (ts, _)] ->
          update_user_session user_id page ts
      | _ -> Lwt.return_unit)
  in
  Lwt_io.println "Clickstream processing complete"

(* Real-time aggregation *)
let calculate_metrics () =
  let%lwt metrics = Client.query_fold_with_columns client
    "SELECT country, COUNT(*) as visits, AVG(duration) as avg_duration 
     FROM sessions 
     WHERE timestamp >= now() - INTERVAL 1 HOUR
     GROUP BY country"
    ~init:[]
    ~f:(fun acc row _columns -> match row with
      | [String country; Int64 visits; Float64 duration] ->
          let metric = { country; visits = Int64.to_int visits; avg_duration = duration } in
          Lwt.return (metric :: acc)
      | _ -> Lwt.return acc)
  in
  publish_metrics_to_dashboard metrics.rows
```

### ETL Processing

```ocaml
(* Extract, Transform, Load pipeline *)
let etl_pipeline source_query transform_fn target_table =
  let inserter = Client.create_async_inserter client target_table in
  Async_insert.start inserter;
  
  let%lwt () = Client.query_iter client source_query
    ~f:(fun row ->
      let transformed = transform_fn row in
      Async_insert.add_row inserter transformed)
  in
  
  Async_insert.stop inserter
```

## Development Setup

### Prerequisites

```bash
# macOS
brew install opam pkg-config openssl@3 lz4

# Ubuntu/Debian  
sudo apt install opam pkg-config libssl-dev liblz4-dev build-essential

# Setup OCaml
opam init --disable-sandboxing
opam switch create proton 4.14.0
opam install dune lwt tls-lwt lz4 zstd alcotest
```

### Build & Test

```bash
git clone <repository-url>
cd proton-ocaml-driver
dune build
dune runtest
```

## Testing

### Quick Start

Run all tests:
```bash
make test
# or
dune test
```

### Test Commands

#### Basic Testing
```bash
# Run all tests
dune test

# Run tests with verbose output
dune test --verbose

# Clean and rebuild before testing
dune clean && dune test
```

#### Using Make
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

### Test Structure

```
test/
└── test_suite.ml    # Main test file with all test cases
```

#### Test Categories

1. **CityHash** - Tests the CityHash128 implementation
2. **Compression** - LZ4 compression/decompression tests
3. **Binary** - Binary encoding/decoding tests  
4. **Connection** - Connection and client creation tests

### Adding New Tests

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

### Running Specific Tests

To run a specific test suite, you can filter by name:
```bash
dune exec test/test_suite.exe -- test "Compression"
```

### Continuous Integration

The test suite is designed to be CI-friendly:
- Exit code 0 on success, non-zero on failure
- Machine-readable output available
- No external dependencies beyond OCaml libraries

## Docker Environment

This directory contains a complete Docker-based test environment for the Proton OCaml driver, based on the ServiceRadar project's proven Proton setup.

### Quick Start

1. **Start the environment:**
   ```bash
   make -f Makefile.docker up
   ```

2. **Run all tests:**
   ```bash
   make -f Makefile.docker test
   ```

3. **Get a development shell:**
   ```bash
   make -f Makefile.docker shell
   ```

### Architecture

The test environment consists of two main services:

- **proton**: Timeplus Proton database configured for testing
- **ocaml-env**: OCaml development environment with all dependencies

### Services Details

#### Proton Database (`proton`)
- **Image**: `ghcr.io/timeplus-io/proton:latest`
- **Ports**:
  - `8123`: HTTP interface
  - `8463`: Native TCP protocol (primary)
  - `8443`: HTTPS (TLS)
  - `9440`: Native TCP with TLS
- **Configuration**: Custom config optimized for testing
- **Data**: Persisted in Docker volume `proton-data`

#### OCaml Environment (`ocaml-env`)
- **Base**: `ocaml/opam:ubuntu-24.04-ocaml-5.1`
- **Features**:
  - OCaml 5.1.1
  - All project dependencies pre-installed
  - Development tools (utop, merlin, ocaml-lsp-server)
  - Source code mounted for live development
- **Cache**: OPAM and Dune caches persisted in volumes

### Available Commands

Use `make -f Makefile.docker <command>`:

| Command | Description |
|---------|-------------|
| `help` | Show all available commands |
| `up` | Start the test environment |
| `down` | Stop the environment |
| `test` | Run all tests |
| `test-simple` | Run simple tests only |
| `test-lwt` | Run Lwt tests only |
| `build-project` | Build the OCaml project |
| `shell` | Get shell in OCaml container |
| `proton-shell` | Connect to Proton CLI |
| `logs` | Show all service logs |
| `clean` | Clean up everything |
| `examples` | Run example programs |

### Development Workflow

1. **Start development environment:**
   ```bash
   make -f Makefile.docker dev-setup
   ```

2. **Make changes to your OCaml code** (files are mounted, so changes are immediate)

3. **Test your changes:**
   ```bash
   make -f Makefile.docker build-project
   make -f Makefile.docker test
   ```

4. **Debug interactively:**
   ```bash
   make -f Makefile.docker shell
   # Inside container:
   dune utop src/
   ```

### Configuration

#### Environment Variables

The OCaml container has these environment variables set:

- `PROTON_HOST=proton`
- `PROTON_PORT=8463`
- `PROTON_HTTP_PORT=8123`
- `PROTON_DATABASE=default`
- `PROTON_USER=default`
- `PROTON_PASSWORD=` (empty for testing)

#### Proton Configuration

The Proton database is configured with:

- **Memory**: Limited to 2GB for development
- **Logging**: Error level only
- **Authentication**: Default user with no password
- **Compression**: LZ4 enabled for testing compression features
- **Streaming**: Optimized for streaming query testing

### Testing Features

The environment supports testing all driver features:

- ✅ **Basic Connectivity**: TCP and HTTP protocols
- ✅ **Data Types**: All ClickHouse/Proton types
- ✅ **Compression**: LZ4 and ZSTD
- ✅ **Streaming Queries**: Real-time data streaming
- ✅ **Async Inserts**: Batch and streaming inserts  
- ✅ **TLS/SSL**: Secure connections (ports 8443, 9440)
- ✅ **Connection Pooling**: Multi-connection scenarios
- ✅ **Error Handling**: Server exception testing

### Troubleshooting

#### Proton won't start
```bash
make -f Makefile.docker logs-proton
```

#### Build failures
```bash
make -f Makefile.docker shell
dune clean
dune build --verbose
```

#### Test failures
```bash
# Run tests individually
make -f Makefile.docker shell
dune exec test_simple/simple_test.exe
dune test --verbose
```

#### Network connectivity
```bash
# Test from OCaml container
make -f Makefile.docker shell
curl http://proton:8123/?query=SELECT%201
```

#### Clean restart
```bash
make -f Makefile.docker clean
make -f Makefile.docker up
```

### Files Structure

```
docker/
├── Dockerfile.ocaml     # OCaml development environment
├── proton-config.xml    # Proton server configuration  
├── users.xml           # Proton user configuration
└── test-runner.sh      # Comprehensive test script
```

### Integration with CI/CD

This Docker setup can be easily integrated into CI/CD pipelines:

```yaml
# GitHub Actions example
- name: Start test environment
  run: make -f Makefile.docker up

- name: Run tests
  run: make -f Makefile.docker test

- name: Cleanup
  run: make -f Makefile.docker down
```

### Performance Notes

- **First startup**: ~30-60 seconds (downloading images, building OCaml)
- **Subsequent startups**: ~10-15 seconds  
- **Test execution**: ~30-60 seconds for full suite
- **Memory usage**: ~2-3GB total (Proton: 2GB, OCaml: 1GB)

### Production vs Testing

This setup is optimized for **testing and development**. For production:

- Enable authentication and TLS
- Increase memory limits
- Configure persistent storage
- Set up monitoring and logging
- Use production-grade container orchestration

## TLS Configuration

This document describes how to set up and use Timeplus Proton with TLS/mTLS support for the OCaml driver.

### Quick Start

#### 1. Generate Certificates

```bash
./docker/generate-certs.sh
```

This creates the following certificates in `docker/certs/`:
- `ca.pem` / `ca-key.pem` - Certificate Authority
- `server.pem` / `server-key.pem` - Server certificate for Proton
- `client.pem` / `client-key.pem` - Client certificate for OCaml driver
- `dhparam.pem` - DH parameters for TLS

#### 2. Start Services

```bash
# Start both TLS and non-TLS Proton instances
docker-compose -f docker-compose-tls.yml up -d
```

This starts:
- **proton** - Standard Proton without TLS (ports 8123, 8463)
- **proton-tls** - Proton with mTLS enabled (ports 8443, 9440)
- **ocaml-env** - OCaml development environment

#### 3. Test Connections

```bash
./docker/test-tls.sh
```

### Configuration Details

#### Non-TLS Proton (Default)
- HTTP Port: 8123
- Native TCP Port: 8463
- User: default (no password)
- Config: `docker/proton-config.xml`, `docker/users.xml`

#### TLS-Enabled Proton
- HTTPS Port: 8443
- Native TCP Secure Port: 9440
- HTTP Port (non-TLS): 8124
- Native TCP Port (non-TLS): 8464
- Config: `docker/proton-config-tls.xml`, `docker/users-tls.xml`

#### Authentication

Two users are configured for TLS mode:

1. **proton_user** (full access)
   - Password: `proton_ocaml_test`
   
2. **readonly_user** (read-only)
   - Password: `readonly_test`

### Using with OCaml Driver

#### Environment Variables

The docker-compose file sets up these environment variables:

```bash
# Non-TLS configuration
PROTON_HOST=proton
PROTON_PORT=8463
PROTON_USER=default
PROTON_PASSWORD=

# TLS configuration
PROTON_TLS_HOST=proton-tls
PROTON_TLS_PORT=9440
PROTON_TLS_USER=proton_user
PROTON_TLS_PASSWORD=proton_ocaml_test
PROTON_TLS_CA_CERT=/app/certs/ca.pem
PROTON_TLS_CLIENT_CERT=/app/certs/client.pem
PROTON_TLS_CLIENT_KEY=/app/certs/client-key.pem
```

#### OCaml Code Example

```ocaml
(* Non-TLS connection *)
let client = Proton.Client.create 
  ~host:"proton"
  ~port:8463
  ()

(* TLS connection with mTLS *)
let tls_config = Proton.Tls.{
  ca_cert = "/app/certs/ca.pem";
  client_cert = Some "/app/certs/client.pem";
  client_key = Some "/app/certs/client-key.pem";
  verify_mode = Strict;
}

let tls_client = Proton.Client.create
  ~host:"proton-tls"
  ~port:9440
  ~user:"proton_user"
  ~password:"proton_ocaml_test"
  ~tls:tls_config
  ()
```

### Testing

#### Manual Connection Test

```bash
# Test non-TLS
curl "http://localhost:8123/?query=SELECT%201"

# Test TLS with authentication
curl -k "https://localhost:8443/?query=SELECT%201" \
  -H "X-ClickHouse-User: proton_user" \
  -H "X-ClickHouse-Key: proton_ocaml_test" \
  --cert docker/certs/client.pem \
  --key docker/certs/client-key.pem \
  --cacert docker/certs/ca.pem
```

#### Run Driver Tests

```bash
# Inside the OCaml container
docker-compose -f docker-compose-tls.yml exec ocaml-env bash

# Run tests
dune test

# Run specific TLS tests (when implemented)
dune test test_tls
```

### Security Notes

1. **Certificates**: The generated certificates are for testing only. In production:
   - Use certificates from a trusted CA
   - Store private keys securely
   - Rotate certificates regularly

2. **Passwords**: The example passwords are hardcoded for testing. In production:
   - Use strong, randomly generated passwords
   - Store passwords in secure vaults
   - Use environment variables or secrets management

3. **Network**: The test setup allows connections from anywhere (`::/0`). In production:
   - Restrict network access to specific IPs/ranges
   - Use firewalls and network policies
   - Enable audit logging

### Troubleshooting

#### Certificate Issues
```bash
# Verify certificate
openssl x509 -in docker/certs/server.pem -text -noout

# Test TLS connection
openssl s_client -connect localhost:9440 \
  -cert docker/certs/client.pem \
  -key docker/certs/client-key.pem \
  -CAfile docker/certs/ca.pem
```

#### Connection Refused
- Check if services are running: `docker-compose -f docker-compose-tls.yml ps`
- Check logs: `docker-compose -f docker-compose-tls.yml logs proton-tls`
- Verify ports are exposed: `docker port proton-ocaml-test-tls`

#### Authentication Failed
- Verify user exists: Connect with default user first
- Check password hash: `echo -n "password" | sha256sum`
- Review Proton logs for auth errors

### Files Reference

- `docker/generate-certs.sh` - Certificate generation script
- `docker/proton-config-tls.xml` - Proton TLS configuration
- `docker/users-tls.xml` - User configuration with passwords
- `docker/Dockerfile.proton-tls` - Docker image for TLS Proton
- `docker-compose-tls.yml` - Docker Compose with TLS services
- `docker/test-tls.sh` - TLS connection test script
- `docker/certs/` - Generated certificates directory

## Contributing

Contributions are welcome! Please see our [contribution guidelines](CONTRIBUTING.md).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/mfreeman451/proton-ocaml-driver/issues)
- **Timeplus Community**: [Timeplus Slack](https://timeplus.com/slack)
