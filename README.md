# 🐪 Timeplus Proton OCaml Driver

OCaml driver for [Timeplus Proton](https://timeplus.com/proton), a streaming database based on ClickHouse.

## Build Instructions

### Prerequisites

You'll need the following system dependencies installed:

#### macOS (via Homebrew)
```bash
brew install opam pkg-config openssl@3 lz4
```

#### Ubuntu/Debian
```bash
sudo apt update
sudo apt install opam pkg-config libssl-dev liblz4-dev build-essential
```

#### CentOS/RHEL/Fedora
```bash
# Fedora
sudo dnf install opam pkg-config openssl-devel lz4-devel gcc-c++

# CentOS/RHEL (with EPEL)
sudo yum install opam pkg-config openssl-devel lz4-devel gcc-c++
```

### Setup OCaml Environment

1. **Initialize opam** (first time only):
   ```bash
   opam init --disable-sandboxing
   ```
   Answer `y` when prompted to modify your shell configuration.

2. **Create a new opam switch** (recommended):
   ```bash
   opam switch create proton-driver 4.14.0
   eval $(opam env)
   ```

3. **Install required OCaml packages**:
   ```bash
   opam install dune lwt tls-lwt x509 ca-certs domain-name mirage-crypto lz4 alcotest
   ```

### Platform-Specific Configuration

#### macOS
Set these environment variables (add to your `~/.bashrc` or `~/.zshrc`):
```bash
export PKG_CONFIG="$(brew --prefix)/bin/pkg-config"
export PKG_CONFIG_PATH="$(brew --prefix)/lib/pkgconfig:$(brew --prefix)/share/pkgconfig:$(brew --prefix)/opt/openssl@3/lib/pkgconfig"
```

Then reload your shell:
```bash
source ~/.bashrc  # or ~/.zshrc
```

### Build the Project

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd proton-ocaml-driver
   ```

2. **Build the project**:
   ```bash
   dune build
   ```

3. **Run tests**:
   ```bash
   make test
   ```

4. **Run examples**:
   ```bash
   # Compression example
   dune exec examples/compression_example.exe
   
   # TLS example
   dune exec examples/tls_example.exe
   
   # Basic query example
   dune exec examples/query.exe
   ```

### Development Workflow

- **Build**: `dune build`
- **Test**: `make test`
- **Clean**: `dune clean`
- **Install locally**: `dune install`

### Troubleshooting

#### Common Issues

**"No config found for file" LSP errors**:
```bash
dune build  # Regenerate build files
```

**Missing system libraries**:
- Ensure `pkg-config`, `openssl`, and `lz4` development libraries are installed
- On macOS, verify Homebrew paths are in your PKG_CONFIG_PATH

**opam switch not active**:
```bash
eval $(opam env)
```

**Build fails with SSL/TLS errors**:
- On macOS: Make sure OpenSSL@3 path is in PKG_CONFIG_PATH (see platform config above)
- On Linux: Install `libssl-dev` or `openssl-devel` package

### Project Structure

```
src/               # Main library source
├── connection.ml  # Connection management
├── client.ml      # Client interface
├── columns*.ml    # Column type parsers
├── compress.ml    # LZ4 compression
├── pool*.ml       # Connection pooling
└── ...

examples/          # Usage examples
test*/             # Test suites
```

### Testing

Run with:
```bash
make test
```
