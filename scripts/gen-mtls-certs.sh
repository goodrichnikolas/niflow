#!/usr/bin/env bash
# Generate throwaway TLS material for the local mTLS test NiFi (make nifi-mtls-up):
# a test CA, a server cert (SAN: localhost), a client cert (CN=niflow-client),
# and the PKCS12 keystore/truststore the NiFi image consumes. Idempotent.
set -euo pipefail

DIR="${1:-certs/mtls}"
PASS="niflowpass123"

if [ -f "$DIR/client.pem" ] && [ -f "$DIR/keystore.p12" ]; then
    echo "mTLS certs already present in $DIR"
    exit 0
fi

mkdir -p "$DIR"
cd "$DIR"

# Test CA. Python 3.13 verifies in strict X.509 mode: without an explicit
# keyUsage=keyCertSign extension it rejects the CA ("CA cert does not
# include key usage extension") even though openssl verify accepts it.
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout ca.key -out ca.pem -subj "/CN=niflow-test-ca" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

# Server certificate for localhost.
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
    -subj "/CN=localhost" 2>/dev/null
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days 3650 -out server.pem \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature,keyEncipherment") 2>/dev/null

# Client certificate — the identity niflow presents.
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
    -subj "/CN=niflow-client" 2>/dev/null
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days 3650 -out client.pem \
    -extfile <(printf "extendedKeyUsage=clientAuth\nkeyUsage=critical,digitalSignature") 2>/dev/null

# NiFi-side stores. The truststore MUST be built with -jdktrust (openssl
# 3.2+): a plain -nokeys export yields certBags Java does not recognise as
# trusted-cert entries — keytool sees "0 entries" and the server silently
# rejects every client certificate after the handshake.
openssl pkcs12 -export -in server.pem -inkey server.key -name nifi \
    -out keystore.p12 -password "pass:$PASS"
openssl pkcs12 -export -nokeys -in ca.pem -name niflow-test-ca \
    -jdktrust anyExtendedKeyUsage \
    -out truststore.p12 -password "pass:$PASS"

rm -f server.csr client.csr
# The NiFi container (uid 1000) must be able to read the stores; this is
# throwaway local test material, not real secrets.
chmod 644 keystore.p12 truststore.p12 ca.pem server.pem client.pem
chmod 600 ca.key server.key client.key

echo "Generated mTLS test certs in $DIR (client identity: CN=niflow-client)"
