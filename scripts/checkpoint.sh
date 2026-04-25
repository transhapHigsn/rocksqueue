#!/usr/bin/env bash
# checkpoint.sh — Create RocksDB checkpoint and sync to S3.
# Runs every 6h via systemd timer.
set -euo pipefail

S3_BUCKET="${S3_BUCKET:?S3_BUCKET required}"
CHECKPOINT_PATH="${CHECKPOINT_PATH:-/data/checkpoints}"
METRICS_ADDR="${METRICS_ADDR:-0.0.0.0:9090}"
HOST="http://127.0.0.1:${METRICS_ADDR##*:}"
AWS_REGION="${AWS_REGION:-us-east-1}"

TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOCAL_PATH="$CHECKPOINT_PATH/$TIMESTAMP"

echo "Creating checkpoint at $LOCAL_PATH"

# POST to admin endpoint
curl -sf -X POST "$HOST/admin/checkpoint?path=$LOCAL_PATH" \
    || { echo "ERROR: checkpoint HTTP call failed" >&2; exit 1; }

echo "Checkpoint created locally — syncing to S3"

aws s3 sync "$LOCAL_PATH/" \
    "s3://$S3_BUCKET/checkpoints/$TIMESTAMP/" \
    --region "$AWS_REGION" \
    --storage-class STANDARD_IA

echo "S3 sync complete: s3://$S3_BUCKET/checkpoints/$TIMESTAMP/"

# Keep only last 2 local checkpoints
ls -d "$CHECKPOINT_PATH"/*/  2>/dev/null \
    | sort \
    | head -n -2 \
    | xargs -r rm -rf

echo "Checkpoint done: $TIMESTAMP"
