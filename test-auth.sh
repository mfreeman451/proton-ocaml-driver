#!/bin/bash

echo "Testing Proton Authentication"
echo "=============================="

# Test 1: Default user (no auth)
echo "1. Default user (no auth):"
curl -s "http://localhost:8125/?query=SELECT%20current_user()" && echo

# Test 2: Using X-ClickHouse headers
echo "2. proton_user with X-ClickHouse headers:"
curl -s "http://localhost:8125/?query=SELECT%20current_user()" \
  -H "X-ClickHouse-User: proton_user" \
  -H "X-ClickHouse-Key: proton_ocaml_test" && echo

# Test 3: Using query parameters
echo "3. proton_user with query parameters:"
curl -s "http://localhost:8125/?query=SELECT%20current_user()&user=proton_user&password=proton_ocaml_test" && echo

# Test 4: Using basic auth
echo "4. proton_user with basic auth:"
curl -s --user "proton_user:proton_ocaml_test" "http://localhost:8125/?query=SELECT%20current_user()" && echo

# Test 5: Native client
echo "5. proton_user with native client:"
/opt/homebrew/bin/proton client --host localhost --port 8465 --user proton_user --password proton_ocaml_test --query "SELECT current_user()"

echo "Done!"