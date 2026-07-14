#!/bin/bash
set -euo pipefail

# Output directory
OUT_DIR="tests/certs"
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

echo "Generating CA (with required extensions)..."
openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
  -keyout ca.key -out ca.pem \
  -subj "/CN=Test CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

echo "Generating Server Certificate..."
# Create server key and CSR
openssl req -newkey rsa:2048 -nodes -keyout server-key.pem -out server.csr \
  -subj "/CN=localhost"

# Create server certificate signed by CA, with localhost and 127.0.0.1 SAN
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out server-cert.pem -days 365 \
  -extfile <(cat <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF
)

echo "Generating Client Certificate..."
# Create client key and CSR
openssl req -newkey rsa:2048 -nodes -keyout client-key.pem -out client.csr \
  -subj "/CN=Test Client"

# Create client certificate signed by CA
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out client-cert.pem -days 365 \
  -extfile <(cat <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
EOF
)

# Cleanup
rm -f *.csr *.srl

echo "Certificates generated in: $(pwd)"
