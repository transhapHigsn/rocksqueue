#!/usr/bin/env bash
# validate_storage.sh — Pre-flight checks before starting rocksqueue.
set -euo pipefail

FAIL=0

check() {
    local desc="$1"
    local result="$2"
    if [[ "$result" == "ok" ]]; then
        echo "  ✓ $desc"
    else
        echo "  ✗ $desc — $result"
        FAIL=1
    fi
}

echo "=== RocksQueue Storage Validation ==="

# /data mounted on local NVMe?
if mountpoint -q /data; then
    device=$(findmnt -n -o SOURCE /data)
    check "/data mounted" "ok"
    # Check it's NVMe
    if [[ "$device" == /dev/nvme* ]]; then
        check "/data on NVMe device ($device)" "ok"
    else
        check "/data on NVMe device" "not NVMe: $device"
    fi
else
    check "/data mounted" "not a mountpoint"
fi

# /wal mounted on local NVMe?
if mountpoint -q /wal; then
    device=$(findmnt -n -o SOURCE /wal)
    check "/wal mounted" "ok"
    if [[ "$device" == /dev/nvme* ]]; then
        check "/wal on NVMe device ($device)" "ok"
    else
        check "/wal on NVMe device" "not NVMe: $device"
    fi
else
    check "/wal mounted" "not a mountpoint"
fi

# Mount options: noatime + nobarrier
data_opts=$(findmnt -n -o OPTIONS /data 2>/dev/null || echo "")
if echo "$data_opts" | grep -q "noatime"; then
    check "/data noatime" "ok"
else
    check "/data noatime" "missing from options: $data_opts"
fi

# ROCKSDB_PATH env
rdb_path="${ROCKSDB_PATH:-}"
if [[ -z "$rdb_path" ]]; then
    check "ROCKSDB_PATH set" "not set"
elif [[ "$rdb_path" == /data* ]]; then
    check "ROCKSDB_PATH=/data/..." "ok ($rdb_path)"
else
    check "ROCKSDB_PATH=/data/..." "unexpected: $rdb_path"
fi

# ROCKSDB_WAL_PATH env
wal_path="${ROCKSDB_WAL_PATH:-}"
if [[ -z "$wal_path" ]]; then
    check "ROCKSDB_WAL_PATH set" "not set"
elif [[ "$wal_path" == /wal* ]]; then
    check "ROCKSDB_WAL_PATH=/wal/..." "ok ($wal_path)"
else
    check "ROCKSDB_WAL_PATH=/wal/..." "unexpected: $wal_path"
fi

echo ""
if [[ $FAIL -ne 0 ]]; then
    echo "VALIDATION FAILED — aborting bootstrap"
    exit 1
else
    echo "All checks passed"
fi
