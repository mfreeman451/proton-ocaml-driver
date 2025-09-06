# Docker Test Environment for Proton OCaml Driver

This directory contains a complete Docker-based test environment for the Proton OCaml driver, based on the ServiceRadar project's proven Proton setup.

## Quick Start

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

## Architecture

The test environment consists of two main services:

- **proton**: Timeplus Proton database configured for testing
- **ocaml-env**: OCaml development environment with all dependencies

## Services Details

### Proton Database (`proton`)
- **Image**: `ghcr.io/timeplus-io/proton:latest`
- **Ports**:
  - `8123`: HTTP interface
  - `8463`: Native TCP protocol (primary)
  - `8443`: HTTPS (TLS)
  - `9440`: Native TCP with TLS
- **Configuration**: Custom config optimized for testing
- **Data**: Persisted in Docker volume `proton-data`

### OCaml Environment (`ocaml-env`)
- **Base**: `ocaml/opam:ubuntu-24.04-ocaml-5.1`
- **Features**:
  - OCaml 5.1.1
  - All project dependencies pre-installed
  - Development tools (utop, merlin, ocaml-lsp-server)
  - Source code mounted for live development
- **Cache**: OPAM and Dune caches persisted in volumes

## Available Commands

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

## Development Workflow

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

## Configuration

### Environment Variables

The OCaml container has these environment variables set:

- `PROTON_HOST=proton`
- `PROTON_PORT=8463`
- `PROTON_HTTP_PORT=8123`
- `PROTON_DATABASE=default`
- `PROTON_USER=default`
- `PROTON_PASSWORD=` (empty for testing)

### Proton Configuration

The Proton database is configured with:

- **Memory**: Limited to 2GB for development
- **Logging**: Error level only
- **Authentication**: Default user with no password
- **Compression**: LZ4 enabled for testing compression features
- **Streaming**: Optimized for streaming query testing

## Testing Features

The environment supports testing all driver features:

- ✅ **Basic Connectivity**: TCP and HTTP protocols
- ✅ **Data Types**: All ClickHouse/Proton types
- ✅ **Compression**: LZ4 and ZSTD
- ✅ **Streaming Queries**: Real-time data streaming
- ✅ **Async Inserts**: Batch and streaming inserts  
- ✅ **TLS/SSL**: Secure connections (ports 8443, 9440)
- ✅ **Connection Pooling**: Multi-connection scenarios
- ✅ **Error Handling**: Server exception testing

## Troubleshooting

### Proton won't start
```bash
make -f Makefile.docker logs-proton
```

### Build failures
```bash
make -f Makefile.docker shell
dune clean
dune build --verbose
```

### Test failures
```bash
# Run tests individually
make -f Makefile.docker shell
dune exec test_simple/simple_test.exe
dune test --verbose
```

### Network connectivity
```bash
# Test from OCaml container
make -f Makefile.docker shell
curl http://proton:8123/?query=SELECT%201
```

### Clean restart
```bash
make -f Makefile.docker clean
make -f Makefile.docker up
```

## Files Structure

```
docker/
├── Dockerfile.ocaml     # OCaml development environment
├── proton-config.xml    # Proton server configuration  
├── users.xml           # Proton user configuration
└── test-runner.sh      # Comprehensive test script
```

## Integration with CI/CD

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

## Performance Notes

- **First startup**: ~30-60 seconds (downloading images, building OCaml)
- **Subsequent startups**: ~10-15 seconds  
- **Test execution**: ~30-60 seconds for full suite
- **Memory usage**: ~2-3GB total (Proton: 2GB, OCaml: 1GB)

## Production vs Testing

This setup is optimized for **testing and development**. For production:

- Enable authentication and TLS
- Increase memory limits
- Configure persistent storage
- Set up monitoring and logging
- Use production-grade container orchestration