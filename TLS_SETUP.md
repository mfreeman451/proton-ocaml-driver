# Proton TLS Configuration

This document describes how to set up and use Timeplus Proton with TLS/mTLS support for the OCaml driver.

## Quick Start

### 1. Generate Certificates

```bash
./docker/generate-certs.sh
```

This creates the following certificates in `docker/certs/`:
- `ca.pem` / `ca-key.pem` - Certificate Authority
- `server.pem` / `server-key.pem` - Server certificate for Proton
- `client.pem` / `client-key.pem` - Client certificate for OCaml driver
- `dhparam.pem` - DH parameters for TLS

### 2. Start Services

```bash
# Start both TLS and non-TLS Proton instances
docker-compose -f docker-compose-tls.yml up -d
```

This starts:
- **proton** - Standard Proton without TLS (ports 8123, 8463)
- **proton-tls** - Proton with mTLS enabled (ports 8443, 9440)
- **ocaml-env** - OCaml development environment

### 3. Test Connections

```bash
./docker/test-tls.sh
```

## Configuration Details

### Non-TLS Proton (Default)
- HTTP Port: 8123
- Native TCP Port: 8463
- User: default (no password)
- Config: `docker/proton-config.xml`, `docker/users.xml`

### TLS-Enabled Proton
- HTTPS Port: 8443
- Native TCP Secure Port: 9440
- HTTP Port (non-TLS): 8124
- Native TCP Port (non-TLS): 8464
- Config: `docker/proton-config-tls.xml`, `docker/users-tls.xml`

### Authentication

Two users are configured for TLS mode:

1. **proton_user** (full access)
   - Password: `proton_ocaml_test`
   
2. **readonly_user** (read-only)
   - Password: `readonly_test`

## Using with OCaml Driver

### Environment Variables

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

### OCaml Code Example

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

## Testing

### Manual Connection Test

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

### Run Driver Tests

```bash
# Inside the OCaml container
docker-compose -f docker-compose-tls.yml exec ocaml-env bash

# Run tests
dune test

# Run specific TLS tests (when implemented)
dune test test_tls
```

## Security Notes

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

## Troubleshooting

### Certificate Issues
```bash
# Verify certificate
openssl x509 -in docker/certs/server.pem -text -noout

# Test TLS connection
openssl s_client -connect localhost:9440 \
  -cert docker/certs/client.pem \
  -key docker/certs/client-key.pem \
  -CAfile docker/certs/ca.pem
```

### Connection Refused
- Check if services are running: `docker-compose -f docker-compose-tls.yml ps`
- Check logs: `docker-compose -f docker-compose-tls.yml logs proton-tls`
- Verify ports are exposed: `docker port proton-ocaml-test-tls`

### Authentication Failed
- Verify user exists: Connect with default user first
- Check password hash: `echo -n "password" | sha256sum`
- Review Proton logs for auth errors

## Files Reference

- `docker/generate-certs.sh` - Certificate generation script
- `docker/proton-config-tls.xml` - Proton TLS configuration
- `docker/users-tls.xml` - User configuration with passwords
- `docker/Dockerfile.proton-tls` - Docker image for TLS Proton
- `docker-compose-tls.yml` - Docker Compose with TLS services
- `docker/test-tls.sh` - TLS connection test script
- `docker/certs/` - Generated certificates directory