#!/usr/bin/env bash
# checkpoint.sh — Create RocksDB checkpoint and sync to object storage.
# Runs every 6h via systemd timer.
#
# Object storage sync (lines below marked [OBJECT STORAGE]) currently uses
# the AWS CLI (`aws s3 sync`) targeting an S3-compatible endpoint.
# To switch providers, replace those lines with the equivalent CLI for your
# object storage service (e.g. `gsutil rsync` for GCS, `az storage blob sync`
# for Azure Blob Storage, or `mc mirror` for any S3-compatible endpoint via MinIO Client).
# The Rust binary itself has no object storage dependency — all sync happens here.
set -euo pipefail

BUCKET="${OBJECT_STORE_BUCKET:-${S3_BUCKET:?OBJECT_STORE_BUCKET required}}"
CHECKPOINT_PATH="${CHECKPOINT_PATH:-/data/checkpoints}"
METRICS_ADDR="${METRICS_ADDR:-0.0.0.0:9090}"
HOST="http://127.0.0.1:${METRICS_ADDR##*:}"
REGION="${CLOUD_REGION:-${AWS_REGION:-us-east-1}}"

# Map provider-neutral env vars to AWS CLI equivalents
export AWS_ACCESS_KEY_ID="${OBJECT_STORE_ACCESS_KEY:-${AWS_ACCESS_KEY_ID:-}}"
export AWS_SECRET_ACCESS_KEY="${OBJECT_STORE_SECRET_KEY:-${AWS_SECRET_ACCESS_KEY:-}}"
export AWS_REGION="$REGION"

ENDPOINT_FLAG=""
if [ -n "${S3_ENDPOINT:-}" ]; then
  ENDPOINT_FLAG="--endpoint-url $S3_ENDPOINT"
fi

TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOCAL_PATH="$CHECKPOINT_PATH/$TIMESTAMP"

echo "Creating checkpoint at $LOCAL_PATH"

# POST to admin endpoint — triggers RocksDB hard-link snapshot
curl -sf -X POST "$HOST/admin/checkpoint?path=$LOCAL_PATH" \
    || { echo "ERROR: checkpoint HTTP call failed" >&2; exit 1; }

echo "Checkpoint created locally — syncing to object storage"

# [OBJECT STORAGE] Upload checkpoint to S3-compatible object storage.
# Replace this block to target a different cloud provider or CLI tool.
aws s3 sync "$LOCAL_PATH/" \
    "s3://$BUCKET/checkpoints/$TIMESTAMP/" \
    --region "$REGION" \
    --storage-class STANDARD_IA \
    ${ENDPOINT_FLAG:+$ENDPOINT_FLAG}
# [/OBJECT STORAGE]

echo "Sync complete: $BUCKET/checkpoints/$TIMESTAMP/"

# Keep only last 2 local checkpoints
ls -d "$CHECKPOINT_PATH"/*/  2>/dev/null \
    | sort \
    | head -n -2 \
    | xargs -r rm -rf

echo "Checkpoint done: $TIMESTAMP"
