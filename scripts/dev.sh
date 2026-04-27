#!/usr/bin/env bash
# dev.sh — Local development helpers.
set -euo pipefail

GRPC_ADDR="${GRPC_ADDR:-localhost:50051}"
HTTP_ADDR="http://localhost:9090"
DATA_DIR="./data"

cmd="${1:-help}"

case "$cmd" in
  seed)
    echo "Provisioning tenants: acme (premium), globex (standard), startup (standard)"
    grpcurl -plaintext -d '{"tenant_id":"acme","tier":"premium"}' \
      "$GRPC_ADDR" controlplane.ControlPlane/ProvisionTenant
    grpcurl -plaintext -d '{"tenant_id":"globex","tier":"standard"}' \
      "$GRPC_ADDR" controlplane.ControlPlane/ProvisionTenant
    grpcurl -plaintext -d '{"tenant_id":"startup","tier":"standard"}' \
      "$GRPC_ADDR" controlplane.ControlPlane/ProvisionTenant
    echo "Done."
    ;;

  reset)
    echo "Wiping $DATA_DIR..."
    rm -rf "$DATA_DIR"
    echo "Done."
    ;;

  checkpoint)
    TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
    LOCAL_PATH="$DATA_DIR/checkpoints/$TIMESTAMP"
    echo "Creating checkpoint at $LOCAL_PATH"
    curl -sf -X POST "$HTTP_ADDR/admin/checkpoint?path=$LOCAL_PATH"
    echo "Checkpoint: $LOCAL_PATH"
    ;;

  restore)
    LATEST=$(ls -d "$DATA_DIR/checkpoints"/*/  2>/dev/null | sort | tail -1)
    if [[ -z "$LATEST" ]]; then
        echo "No local checkpoints found"
        exit 1
    fi
    echo "Restoring from $LATEST"
    rm -rf "$DATA_DIR/rocksqueue" "$DATA_DIR/rocksqueue-wal"
    cp -r "$LATEST" "$DATA_DIR/rocksqueue"
    echo "Restored."
    ;;

  status)
    echo "=== Health ==="
    curl -sf "$HTTP_ADDR/health"
    echo ""
    echo "=== Ready ==="
    curl -sf "$HTTP_ADDR/ready"
    echo ""
    echo "=== Tenants ==="
    grpcurl -plaintext -d '{}' "$GRPC_ADDR" controlplane.ControlPlane/ListTenants
    ;;

  stats)
    grpcurl -plaintext -d '{}' "$GRPC_ADDR" controlplane.ControlPlane/ListAllStats
    ;;

  traffic)
    TENANT="${TENANT:-acme}"
    QUEUE="${QUEUE:-default}"
    BATCH_SIZE="${BATCH_SIZE:-10}"
    DEQUEUE_LIMIT="${DEQUEUE_LIMIT:-10}"
    SLEEP_SECS="${SLEEP_SECS:-0.2}"
    ITERATION=0

    echo "Generating queue traffic for tenant=$TENANT queue=$QUEUE batch_size=$BATCH_SIZE dequeue_limit=$DEQUEUE_LIMIT"
    echo "Press Ctrl-C to stop."

    while true; do
      ITERATION=$((ITERATION + 1))
      payloads=""
      for i in $(seq 1 "$BATCH_SIZE"); do
        payload="dummy-${TENANT}-${ITERATION}-${i}"
        if [[ -n "$payloads" ]]; then
          payloads="${payloads},"
        fi
        payloads="${payloads}\"${payload}\""
      done

      grpcurl -plaintext \
        -d "{\"tenant_id\":\"${TENANT}\",\"queue\":\"${QUEUE}\",\"payloads\":[${payloads}]}" \
        "$GRPC_ADDR" controlplane.ControlPlane/EnqueueBatch >/dev/null

      response=$(grpcurl -plaintext \
        -d "{\"tenant_id\":\"${TENANT}\",\"queue\":\"${QUEUE}\",\"limit\":${DEQUEUE_LIMIT}}" \
        "$GRPC_ADDR" controlplane.ControlPlane/DequeueTasks)

      acked=$(printf '%s\n' "$response" | sed -n 's/.*"ackKey": "\([^"]*\)".*/\1/p' | wc -l | tr -d ' ')
      printf '%s\n' "$response" | sed -n 's/.*"ackKey": "\([^"]*\)".*/\1/p' | while read -r ack_key; do
        grpcurl -plaintext \
          -d "{\"tenant_id\":\"${TENANT}\",\"ack_key\":\"${ack_key}\"}" \
          "$GRPC_ADDR" controlplane.ControlPlane/AckTask >/dev/null
      done

      if (( ITERATION % 10 == 0 )); then
        echo "iteration=$ITERATION enqueued=$BATCH_SIZE acked=$acked"
      fi

      sleep "$SLEEP_SECS"
    done
    ;;

  *)
    echo "Usage: $0 {seed|reset|checkpoint|restore|status|stats|traffic}"
    exit 1
    ;;
esac
