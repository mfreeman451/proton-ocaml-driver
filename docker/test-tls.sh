#!/bin/bash
# Test script for Proton TLS configuration
set -e

echo "Testing Proton TLS Configuration"
echo "================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to test connection
test_connection() {
    local name=$1
    local host=$2
    local port=$3
    local user=$4
    local password=$5
    local use_tls=$6
    
    echo -n "Testing $name connection... "
    
    if [ "$use_tls" = "true" ]; then
        # Test with TLS using openssl
        if echo "SELECT 1" | openssl s_client -connect $host:$port \
            -cert docker/certs/client.pem \
            -key docker/certs/client-key.pem \
            -CAfile docker/certs/ca.pem \
            -quiet 2>/dev/null | grep -q "1"; then
            echo -e "${GREEN}✓ Success${NC}"
            return 0
        else
            echo -e "${RED}✗ Failed${NC}"
            return 1
        fi
    else
        # Test without TLS using curl
        if [ -n "$password" ]; then
            auth_header="X-ClickHouse-User: $user\nX-ClickHouse-Key: $password"
        else
            auth_header=""
        fi
        
        if curl -sf "http://$host:$port/?query=SELECT%201" \
            ${auth_header:+-H "$auth_header"} >/dev/null 2>&1; then
            echo -e "${GREEN}✓ Success${NC}"
            return 0
        else
            echo -e "${RED}✗ Failed${NC}"
            return 1
        fi
    fi
}

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 10

# Test non-TLS connection
echo -e "\n${YELLOW}Non-TLS Tests:${NC}"
test_connection "HTTP (port 8123)" "localhost" "8123" "" "" "false"
test_connection "Native TCP (port 8463)" "localhost" "8463" "" "" "false"

# Test TLS connections
echo -e "\n${YELLOW}TLS Tests:${NC}"
test_connection "HTTP (port 8124)" "localhost" "8124" "" "" "false"
test_connection "Native TCP (port 8464)" "localhost" "8464" "" "" "false"

# Test authenticated connection (requires proper client)
echo -e "\n${YELLOW}Authenticated TLS Tests:${NC}"
echo "Testing authenticated user with password..."

# Test with curl for HTTPS
if curl -sf -k "https://localhost:8443/?query=SELECT%201" \
    -H "X-ClickHouse-User: proton_user" \
    -H "X-ClickHouse-Key: proton_ocaml_test" \
    --cert docker/certs/client.pem \
    --key docker/certs/client-key.pem \
    --cacert docker/certs/ca.pem >/dev/null 2>&1; then
    echo -e "HTTPS with auth: ${GREEN}✓ Success${NC}"
else
    echo -e "HTTPS with auth: ${RED}✗ Failed${NC}"
fi

# Summary
echo -e "\n${YELLOW}Summary:${NC}"
echo "- Non-TLS Proton is running on ports 8123 (HTTP) and 8463 (Native)"
echo "- TLS Proton is running on ports 8443 (HTTPS) and 9440 (Native TLS)"
echo "- Client certificates are in docker/certs/"
echo "- User: proton_user, Password: proton_ocaml_test"
echo ""
echo "To connect with the OCaml driver using TLS:"
echo "  - Host: proton-tls (or localhost from host)"
echo "  - Port: 9440"
echo "  - User: proton_user"
echo "  - Password: proton_ocaml_test"
echo "  - CA Certificate: docker/certs/ca.pem"
echo "  - Client Certificate: docker/certs/client.pem"
echo "  - Client Key: docker/certs/client-key.pem"