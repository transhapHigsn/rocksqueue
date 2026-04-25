#!/usr/bin/env bash
# restore_from_s3.sh — Restore latest RocksDB checkpoint from object storage.
# Called by user_data.sh after setup_storage.sh.
#
# Object storage access (lines marked [OBJECT STORAGE]) currently uses the
# AWS CLI targeting an S3-compatible endpoint. To switch providers, replace
# those lines with the equivalent CLI for your object storage service.
# The Rust binary itself has no object storage dependency — all restore happens here.
set -euo pipefail

BUCKET="${OBJECT_STORE_BUCKET:-${S3_BUCKET:?OBJECT_STORE_BUCKET required}}"
ROCKSDB_PATH="${ROCKSDB_PATH:-/data/rocksqueue}"
REGION="${CLOUD_REGION:-${AWS_REGION:-us-east-1}}"
STAGING="${ROCKSDB_PATH}.restore-staging"

# Skip if data already present (normal reboot — NVMe survived)
if [[ -d "$ROCKSDB_PATH" ]] && [[ -f "$ROCKSDB_PATH/CURRENT" ]]; then
    echo "RocksDB data exists at $ROCKSDB_PATH — skipping restore"
    exit 0
fi

echo "RocksDB data missing — attempting restore from object storage ($BUCKET/checkpoints/)"

# [OBJECT STORAGE] List checkpoints (lexicographic sort → latest last).
# Replace with the equivalent list command for your object storage provider.
LATEST=$(aws s3 ls "s3://$BUCKET/checkpoints/" --region "$REGION" \
    | awk '{print $2}' | sort | tail -1 | tr -d '/')
# [/OBJECT STORAGE]

if [[ -z "$LATEST" ]]; then
    echo "No checkpoints found in $BUCKET/checkpoints/ — fresh start"
    exit 2
fi

echo "Restoring checkpoint: $LATEST"

# Download to staging
rm -rf "$STAGING"
mkdir -p "$STAGING"

# [OBJECT STORAGE] Download checkpoint from object storage.
# Replace with the equivalent sync/download command for your provider.
aws s3 sync "s3://$BUCKET/checkpoints/$LATEST/" "$STAGING/" --region "$REGION"
# [/OBJECT STORAGE]

# Verify integrity
for required in CURRENT MANIFEST OPTIONS; do
    if ! ls "$STAGING"/$required* &>/dev/null; then
        echo "ERROR: Missing $required in checkpoint — aborting restore" >&2
        rm -rf "$STAGING"
        exit 1
    fi
done

# Atomic move staging → live
rm -rf "$ROCKSDB_PATH"
mv "$STAGING" "$ROCKSDB_PATH"

# Check age and warn if stale (>6h)
CHECKPOINT_TS=$(echo "$LATEST" | grep -oP '\d{8}T\d{6}' || echo "")
if [[ -n "$CHECKPOINT_TS" ]]; then
    CHECKPOINT_EPOCH=$(date -d "${CHECKPOINT_TS:0:8} ${CHECKPOINT_TS:9:2}:${CHECKPOINT_TS:11:2}:${CHECKPOINT_TS:13:2}" +%s 2>/dev/null || echo 0)
    NOW_EPOCH=$(date +%s)
    AGE_HOURS=$(( (NOW_EPOCH - CHECKPOINT_EPOCH) / 3600 ))
    if [[ $AGE_HOURS -gt 6 ]]; then
        echo "WARNING: Checkpoint is ${AGE_HOURS}h old — up to ${AGE_HOURS}h of tasks may be missing"
    fi
fi

# Write restore manifest
cat > /etc/rocksqueue/restore-manifest.json <<EOF
{
  "checkpoint": "$LATEST",
  "restored_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "source": "$BUCKET/checkpoints/$LATEST"
}
EOF

echo "Restore complete: $ROCKSDB_PATH (from $LATEST)"
