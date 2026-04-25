#!/usr/bin/env bash
# deploy.sh — Zero-downtime deploy via systemd.
set -euo pipefail

BINARY_PATH="${1:?Usage: deploy.sh <path-to-binary>}"
SERVICE="rocksqueue"
HTTP_READY="http://127.0.0.1:9090/ready"
TIMEOUT=30

echo "Deploying $BINARY_PATH"

# Copy binary
cp "$BINARY_PATH" /usr/local/bin/rocksqueue.new
chmod +x /usr/local/bin/rocksqueue.new
mv /usr/local/bin/rocksqueue.new /usr/local/bin/rocksqueue

# Reload systemd and restart
systemctl daemon-reload
systemctl restart "$SERVICE"

# Wait for /ready
echo "Waiting for service to become ready..."
for i in $(seq 1 $TIMEOUT); do
    if curl -sf "$HTTP_READY" >/dev/null 2>&1; then
        echo "Service ready after ${i}s"
        exit 0
    fi
    sleep 1
done

echo "ERROR: Service did not become ready within ${TIMEOUT}s" >&2
systemctl status "$SERVICE" --no-pager || true
exit 1
