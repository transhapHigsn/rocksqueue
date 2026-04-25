# RocksQueue

A production-grade, RocksDB-backed multi-tenant task queue written in Rust. Targets 1,000+ tasks/second with hard noisy-neighbour isolation, adaptive scheduling, and self-healing single-node deployment on AWS.

## Features

- **Multi-tenant isolation** — per-tenant Column Families with scoped compaction, bloom filters, and iterators
- **Weighted Fair Queue (WFQ)** — proportional slot allocation by weight with per-tenant token buckets
- **Backlog quotas** — Reject / Block / EvictOldest policies enforced at enqueue time
- **Compaction-based GC** — TTL filtering for pending, inflight, and DLQ tasks runs free during normal RocksDB compaction
- **Adaptive throttling** — EMA stats + CUSUM drift detection auto-throttle noisy tenants without operator intervention
- **Visibility timeout reaper** — inflight tasks past deadline are automatically re-queued
- **Self-healing bootstrap** — restores from S3 checkpoint on fresh EC2 instance, zero manual steps
- **gRPC control plane** — 30 RPCs covering tenant lifecycle, policy, stats, throttle, baseline, and operations
- **Persistent state** — stats, throttle decisions, and baselines survive restarts via `__system__` CF

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  gRPC Control Plane (:50051)   HTTP Metrics/Health (:9090)  │
└───────────────────────┬─────────────────────────────────────┘
                        │
         ┌──────────────▼──────────────┐
         │       WFQScheduler          │  weight + token bucket per tenant
         │       StatsCollector        │  EMA: arrival_rate, burst_score
         │       AutoThrottle          │  CUSUM drift → adaptive rate limit
         └──────────────┬──────────────┘
                        │
         ┌──────────────▼──────────────┐
         │       TenantRegistry        │  RocksDB, per-tenant CFs
         │  {tenant}__pending          │  ← enqueue target
         │  {tenant}__inflight         │  ← dequeue moves tasks here
         │  {tenant}__dlq              │  ← nack after max attempts
         │  __system__                 │  ← stats / throttle / baselines
         └─────────────────────────────┘
              SST: /data/rocksqueue
              WAL: /wal/rocksqueue-wal   (separate NVMe partition)
```

### Key design decisions

| Decision | Rationale |
|---|---|
| Per-tenant Column Families | Compaction, bloom filters, iterators scoped per tenant — no cross-tenant interference |
| WAL on separate NVMe partition | Sequential WAL writes don't contend with random SST I/O |
| WriteBatch for all multi-key ops | 100 tasks = 1 WAL record instead of 100 |
| `set_sync(false)` on hot path | ~10x throughput — WAL written but not fsynced per-write |
| Compaction filters per CF | GC is free — runs during normal compaction, no dedicated GC thread |
| Big-endian sequence keys | Lexicographic order = insertion order; prefix scans O(1) to position |
| WFQ + token bucket | `weight=50` gets 5x more slots than `weight=10`; burst absorbed cleanly |
| CUSUM drift detector | Sustained traffic growth promotes baseline; transient spikes trigger throttle |

### Noisy-neighbour protection stack

```
Layer 1 — Storage:    per-tenant CFs + compaction filter TTL
Layer 2 — Writes:     backlog quota (Reject / Block / EvictOldest)
Layer 3 — Scheduling: WFQ slot allocation + token bucket rate cap
Layer 4 — Adaptive:   EMA stats → AutoThrottle → CUSUM baseline promotion
           (all state persisted — isolation survives restarts)
```

New and dormant tenants always receive a minimum guaranteed slot allocation regardless of history or throttle state.

## Prerequisites

```bash
# Rust 1.91+
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustup update stable

# protoc (gRPC codegen)
# Ubuntu / Debian
sudo apt-get install -y protobuf-compiler libclang-dev clang

# macOS
brew install protobuf
```

## Quick start

```bash
git clone <repo>
cd rocksqueue

# Start locally (reads .env, creates ./data/)
cargo run

# In another terminal — seed tenants
./scripts/dev.sh seed

# Run tests
cargo test
```

## Configuration

All config is loaded from environment variables. Copy `.env` and adjust:

```bash
ROCKSDB_PATH=./data/rocksqueue        # SST files
ROCKSDB_WAL_PATH=./data/rocksqueue-wal # WAL files
CHECKPOINT_PATH=./data/checkpoints
GRPC_ADDR=0.0.0.0:50051
METRICS_ADDR=0.0.0.0:9090
S3_BUCKET=rocksqueue-local
S3_ENDPOINT=http://localhost:9000      # MinIO for local dev; omit for AWS
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
RUST_LOG=rocksqueue=debug
```

**Environment detection:** if `ROCKSDB_PATH` starts with `/data`, production tuning is applied (4 GB block cache). Otherwise local defaults apply (256 MB).

## Full stack with Docker Compose

```bash
docker compose up

# MinIO (S3-compatible)  → http://localhost:9001  (minioadmin / minioadmin)
# Prometheus             → http://localhost:9091
# Grafana                → http://localhost:3000   (admin / admin)
# RocksQueue gRPC        → localhost:50051
# RocksQueue metrics     → http://localhost:9090
```

## gRPC API

### Tenant lifecycle

```bash
# Provision (tier: "standard" | "premium")
grpcurl -plaintext -d '{"tenant_id":"acme","tier":"premium"}' \
  localhost:50051 controlplane.ControlPlane/ProvisionTenant

# Drop
grpcurl -plaintext -d '{"tenant_id":"acme"}' \
  localhost:50051 controlplane.ControlPlane/DropTenant
```

### Backlog quota

```bash
grpcurl -plaintext -d '{
  "tenant_id":"acme",
  "backlog_quota":50000,
  "backlog_policy":"reject",
  "pending_retention_secs":86400,
  "dlq_retention_secs":604800,
  "inflight_stale_secs":300
}' localhost:50051 controlplane.ControlPlane/SetNamespacePolicy
```

Backlog policies:
- `reject` — return error immediately when quota exceeded
- `block` — wait up to `block_timeout_ms` for space, then reject
- `evict_oldest` — delete oldest pending tasks to make room

### Live policy update (no restart)

```bash
grpcurl -plaintext -d '{
  "tenant_id":"noisy",
  "policy":{"weight":1,"max_inflight":10,"burst_tokens":5,"rate_per_sec":5}
}' localhost:50051 controlplane.ControlPlane/UpdatePolicy
```

### Observability

```bash
# Queue depth
grpcurl -plaintext -d '{"tenant_id":"acme"}' \
  localhost:50051 controlplane.ControlPlane/GetTenantStatus

# Live stats stream
grpcurl -plaintext -d '{"tenant_id":"acme","interval_secs":1}' \
  localhost:50051 controlplane.ControlPlane/WatchStats

# Auto-throttle state
grpcurl -plaintext -d '{"tenant_id":"acme"}' \
  localhost:50051 controlplane.ControlPlane/GetThrottleStatus

# CUSUM baseline drift
grpcurl -plaintext -d '{"tenant_id":"acme"}' \
  localhost:50051 controlplane.ControlPlane/GetBaselineStatus
```

### Operations

```bash
# Force release a throttled tenant
grpcurl -plaintext -d '{"tenant_id":"acme"}' \
  localhost:50051 controlplane.ControlPlane/ForceReleaseThrottle

# Drain → safe shutdown of a tenant
grpcurl -plaintext -d '{"tenant_id":"old","timeout_secs":30}' \
  localhost:50051 controlplane.ControlPlane/DrainTenant

# Replay DLQ after a bug fix
grpcurl -plaintext -d '{"tenant_id":"acme"}' \
  localhost:50051 controlplane.ControlPlane/ReplayDLQ

# Force compaction (reclaim disk immediately)
grpcurl -plaintext -d '{"tenant_id":"acme"}' \
  localhost:50051 controlplane.ControlPlane/CompactTenant
```

## HTTP endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness — always `ok` |
| `GET /ready` | Readiness — pings RocksDB; used by deploy script and ASG lifecycle hooks |
| `GET /metrics` | Prometheus scrape |
| `POST /admin/checkpoint?path=<path>` | Create a RocksDB checkpoint at the given path |

## Dev helpers

```bash
./scripts/dev.sh seed        # provision acme (premium), globex, startup
./scripts/dev.sh reset       # wipe ./data/
./scripts/dev.sh checkpoint  # create local checkpoint
./scripts/dev.sh restore     # restore from latest local checkpoint
./scripts/dev.sh status      # /health + /ready + ListTenants
./scripts/dev.sh stats       # ListAllStats
```

## Production deployment (AWS)

### Recommended instance

`i4i.2xlarge` — local NVMe, 8 vCPU, 64 GB RAM.

### Storage layout

```
/dev/nvme1n1p1  70%  →  /data  →  SST files  (/data/rocksqueue)
/dev/nvme1n1p2  30%  →  /wal   →  WAL files  (/wal/rocksqueue-wal)
```

Mount options: `noatime,nodiratime,nobarrier,data=writeback`  
I/O scheduler: `none` (NVMe pass-through)

### Self-healing bootstrap

Each new EC2 instance provisions itself automatically via `user_data`:

1. `scripts/setup_storage.sh` — partition NVMe, format ext4, mount, write `/etc/rocksqueue/env`
2. `scripts/restore_from_s3.sh` — if `/data/rocksqueue` is empty, download latest S3 checkpoint
3. `scripts/validate_storage.sh` — pre-flight checks; abort if anything fails
4. Install binary + systemd units
5. `systemctl start rocksqueue`
6. Poll `/ready` → complete ASG lifecycle hook

### Checkpoints

Systemd timer runs `scripts/checkpoint.sh` every 6 hours:
1. `POST /admin/checkpoint` — RocksDB creates a hard-link snapshot
2. `aws s3 sync` — upload to `s3://<bucket>/checkpoints/<timestamp>/`
3. Keep the last 2 local checkpoints; delete older ones

**Instance store warning:** stopping an EC2 instance wipes NVMe data. Rebooting preserves it. Maximum recovery gap is 6 hours (one checkpoint interval).

Production environment variables are read from `/etc/rocksqueue/env` (written by `setup_storage.sh`) via systemd `EnvironmentFile`.

```bash
ROCKSDB_PATH=/data/rocksqueue
ROCKSDB_WAL_PATH=/wal/rocksqueue-wal
CHECKPOINT_PATH=/data/checkpoints
GRPC_ADDR=0.0.0.0:50051
METRICS_ADDR=0.0.0.0:9090
S3_BUCKET=<your-bucket>
AWS_REGION=us-east-1
RUST_LOG=rocksqueue=info
```

## Prometheus alerts

```yaml
- alert: RocksQueueDown
  expr: up{job="rocksqueue"} == 0
  for: 1m

- alert: CompactionLag
  expr: rocksdb_l0_files > 20
  for: 5m

- alert: TenantThrottled
  expr: rocksqueue_tasks_throttled_total > 0
  for: 0m

- alert: HighBacklog
  expr: rocksqueue_queue_depth_pending > 50000
  for: 5m

- alert: DLQSpike
  expr: rate(rocksqueue_tasks_dlq_total[5m]) > 10
  for: 2m

- alert: CheckpointStale
  expr: time() - rocksqueue_last_checkpoint_timestamp > 25200
  for: 0m

- alert: WriteStall
  expr: rocksdb_write_stalls == 1
  for: 0m
```

## Module map

| Module | Responsibility |
|---|---|
| `tenant` | Core RocksDB engine — enqueue, dequeue, ack, nack, depth, compaction |
| `compaction` | Compaction filter factories for pending / inflight / DLQ TTL |
| `scheduler` | Weighted Fair Queue with per-tenant token bucket |
| `stats` | EMA-based stats collection and adaptive slot allocation |
| `stats_store` | RocksDB persistence for stats, throttle state, and baselines |
| `stats_daemon` | Background 1 s loop: refresh → backlog → baseline → throttle → flush |
| `throttle` | Auto-throttle: signal evaluation, hysteresis release, persistence |
| `baseline` | CUSUM drift detector for gradual traffic regime changes |
| `reaper` | Visibility timeout enforcement — returns stale inflight tasks to pending |
| `fair_worker` | Worker pool that consumes allocations from `StatsCollector` |
| `policy` | Backlog and retention policy types |
| `config` | `Config::from_env()` — loads `.env` + env vars |
| `ownership` | Single-node ownership map (seed for future multi-node routing) |
| `grpc` | All 30 gRPC RPC handlers |

## Tenant tiers

| | Standard | Premium |
|---|---|---|
| `weight` | 10 | 50 |
| `max_inflight` | 500 | 5,000 |
| `rate_per_sec` | 100 | 1,000 |
| `backlog_quota` | 100,000 | 1,000,000 |
| `backlog_policy` | Reject | Block (5 s timeout) |

## Future work

| Item | Description |
|---|---|
| Namespace layer | Sub-tenant policy granularity (Pulsar Lesson 2) |
| Key_Shared routing | Sticky worker assignment for ordered processing (Lesson 5) |
| Cursor-based ack | Eliminate physical CF moves on ack (Lesson 6) |
| Delayed delivery | `deliver_at_ms` field + delayed CF (Lesson 9) |
| Multi-node routing | Promote `OwnershipMap` from seed to active tenant routing |
| Prometheus metrics | Wire actual counters to `/metrics` endpoint |
| Criterion benchmarks | `benches/throughput.rs` — 1,000+ tasks/sec validation |
