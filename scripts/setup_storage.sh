#!/usr/bin/env bash
# setup_storage.sh — Partition, format, and mount local NVMe for production.
# Run once on first boot (user_data.sh calls this).
set -euo pipefail

DISK=""

# Auto-detect local NVMe (skip EBS root which has model "Amazon Elastic Block Store")
for dev in /dev/nvme1n1 /dev/nvme2n1 /dev/nvme0n1; do
    if [[ -b "$dev" ]]; then
        model=$(nvme id-ctrl "$dev" -o json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('mn',''))" 2>/dev/null || true)
        if [[ "$model" != *"Amazon Elastic Block Store"* ]]; then
            DISK="$dev"
            break
        fi
    fi
done

if [[ -z "$DISK" ]]; then
    echo "ERROR: No local NVMe disk found" >&2
    exit 1
fi

echo "Using disk: $DISK"

# Partition: p1=70% (SST), p2=30% (WAL)
parted -s "$DISK" mklabel gpt
parted -s "$DISK" mkpart primary ext4 1MiB 70%
parted -s "$DISK" mkpart primary ext4 70% 100%

PART1="${DISK}p1"
PART2="${DISK}p2"

# Format
mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "$PART1"
mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "$PART2"

# Get UUIDs for stable fstab entries
UUID1=$(blkid -s UUID -o value "$PART1")
UUID2=$(blkid -s UUID -o value "$PART2")

# Mount points
mkdir -p /data /wal
mount -o noatime,nodiratime,nobarrier,data=writeback UUID="$UUID1" /data
mount -o noatime,nodiratime,nobarrier,data=writeback UUID="$UUID2" /wal

# Write fstab
cat >> /etc/fstab <<EOF
UUID=$UUID1  /data  ext4  noatime,nodiratime,nobarrier,data=writeback  0 2
UUID=$UUID2  /wal   ext4  noatime,nodiratime,nobarrier,data=writeback  0 2
EOF

# I/O scheduler: none (NVMe pass-through) — persist via udev rule
cat > /etc/udev/rules.d/99-nvme-scheduler.rules <<EOF
ACTION=="add|change", KERNEL=="nvme*", ATTR{queue/scheduler}="none"
EOF
udevadm control --reload-rules
udevadm trigger

# Disable transparent hugepages
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
cat > /etc/rc.local <<'EOF'
#!/bin/sh
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
exit 0
EOF
chmod +x /etc/rc.local

# Create data directories
mkdir -p /data/rocksqueue /wal/rocksqueue-wal /data/checkpoints

# Write environment file for systemd
mkdir -p /etc/rocksqueue
cat > /etc/rocksqueue/env <<EOF
ROCKSDB_PATH=/data/rocksqueue
ROCKSDB_WAL_PATH=/wal/rocksqueue-wal
CHECKPOINT_PATH=/data/checkpoints
GRPC_ADDR=0.0.0.0:50051
METRICS_ADDR=0.0.0.0:9090
RUST_LOG=rocksqueue=info
EOF

echo "Storage setup complete: /data ($UUID1) + /wal ($UUID2)"
