#!/bin/bash
# Generate TLS certificates for Proton mTLS configuration
set -e

# Configuration
CERT_DIR="${CERT_DIR:-./docker/certs}"
DAYS_VALID=3650
COUNTRY="US"
STATE="CA"
LOCALITY="San Francisco"
ORGANIZATION="ProtonOCamlDriver"
ORG_UNIT="Development"

# Create certificate directory
mkdir -p "$CERT_DIR"
chmod 755 "$CERT_DIR"

# Generate Root CA
if [ ! -f "$CERT_DIR/ca.pem" ]; then
    echo "Generating Root CA..."
    openssl genrsa -out "$CERT_DIR/ca-key.pem" 4096
    openssl req -new -x509 -sha256 -key "$CERT_DIR/ca-key.pem" -out "$CERT_DIR/ca.pem" \
        -days $DAYS_VALID -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORG_UNIT/CN=Proton Test CA"
    
    chmod 644 "$CERT_DIR/ca.pem"
    chmod 600 "$CERT_DIR/ca-key.pem"
    
    echo "Root CA generated."
else
    echo "Root CA already exists."
fi

# Function to generate certificate for a component
generate_cert() {
    local name=$1
    local cn=$2
    local san=$3
    
    if [ -f "$CERT_DIR/$name.pem" ]; then
        echo "Certificate for $name already exists, skipping."
        return
    fi
    
    echo "Generating certificate for $name..."
    
    # Generate private key
    openssl genrsa -out "$CERT_DIR/$name-key.pem" 2048
    
    # Create config file with SAN
    cat > "$CERT_DIR/$name.conf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = $COUNTRY
ST = $STATE
L = $LOCALITY
O = $ORGANIZATION
OU = $ORG_UNIT
CN = $cn

[v3_req]
keyUsage = keyEncipherment, dataEncipherment, digitalSignature
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = $san
EOF
    
    # Generate CSR
    openssl req -new -sha256 -key "$CERT_DIR/$name-key.pem" \
        -out "$CERT_DIR/$name.csr" -config "$CERT_DIR/$name.conf"
    
    # Sign certificate
    openssl x509 -req -in "$CERT_DIR/$name.csr" -CA "$CERT_DIR/ca.pem" \
        -CAkey "$CERT_DIR/ca-key.pem" -CAcreateserial -out "$CERT_DIR/$name.pem" \
        -days $DAYS_VALID -sha256 -extensions v3_req -extfile "$CERT_DIR/$name.conf"
    
    # Clean up
    rm "$CERT_DIR/$name.csr" "$CERT_DIR/$name.conf"
    
    chmod 644 "$CERT_DIR/$name.pem"
    chmod 600 "$CERT_DIR/$name-key.pem"
    
    echo "Certificate for $name generated."
}

# Generate server certificate for Proton
generate_cert "server" "proton.local" "DNS:proton,DNS:proton-tls,DNS:proton-ocaml-test-tls,DNS:localhost,IP:127.0.0.1"

# Generate client certificate for OCaml driver
generate_cert "client" "ocaml-client" "DNS:ocaml-env,DNS:localhost,IP:127.0.0.1"

# Generate DH parameters for Proton (required for TLS)
if [ ! -f "$CERT_DIR/dhparam.pem" ]; then
    echo "Generating DH parameters (this may take a while)..."
    openssl dhparam -out "$CERT_DIR/dhparam.pem" 2048
    chmod 644 "$CERT_DIR/dhparam.pem"
    echo "DH parameters generated."
else
    echo "DH parameters already exist."
fi

echo ""
echo "All certificates generated successfully in $CERT_DIR"
echo ""
echo "Files generated:"
ls -la "$CERT_DIR"/*.pem 2>/dev/null | awk '{print $9}' | sort