#!/bin/bash
# Script to enable TLS in Proton config

CONFIG_FILE="docker/base-config.yaml"
TLS_CONFIG_FILE="docker/proton-tls-full-config.yaml"

# Copy base config
cp "$CONFIG_FILE" "$TLS_CONFIG_FILE"

# Enable TLS ports by uncommenting them
sed -i 's/# https_port: 8443/https_port: 8443/' "$TLS_CONFIG_FILE"
sed -i 's/# tcp_port_secure: 9440/tcp_port_secure: 9440/' "$TLS_CONFIG_FILE"

# Update certificate paths in openSSL section
sed -i 's|certificateFile: /etc/proton-server/server.crt|certificateFile: /etc/proton-server/certs/server.pem|' "$TLS_CONFIG_FILE"
sed -i 's|privateKeyFile: /etc/proton-server/server.key|privateKeyFile: /etc/proton-server/certs/server-key.pem|' "$TLS_CONFIG_FILE"
sed -i 's|dhParamsFile: /etc/proton-server/dhparam.pem|dhParamsFile: /etc/proton-server/certs/dhparam.pem|' "$TLS_CONFIG_FILE"

# Update verification mode for mTLS
sed -i 's/verificationMode: none/verificationMode: strict/' "$TLS_CONFIG_FILE"

# Add CA config for client certificate verification
sed -i '/verificationMode: strict/a\        caConfig: /etc/proton-server/certs/ca.pem' "$TLS_CONFIG_FILE"

echo "TLS configuration enabled in $TLS_CONFIG_FILE"