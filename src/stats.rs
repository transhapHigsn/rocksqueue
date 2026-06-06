use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing::debug;

use crate::stats_store::StatsStore;

#[derive(Debug, Clone)]
pub struct TenantStats {
    pub tenant_id: String,
    pub arrival_rate: f64,
    pub processing_rate: f64,
    pub avg_task_latency: f64,
    pub burst_score: f64,
    pub backlog_ratio: f64,
    pub last_enqueue_at: Instant,
    pub last_dequeue_at: Instant,
    pub last_alloc_fraction: f64,
    pub is_new: bool,
    pub total_enqueued: u64,
    pub total_acked: u64,
    pub total_nacked: u64,
    pub stats_version: u64,
}

struct TenantCounters {
    enqueue_count: AtomicU64,
    ack_count: AtomicU64,
    nack_count: AtomicU64,
    latency_sum_ms: AtomicU64,
    total_enqueued: AtomicU64,
    total_acked: AtomicU64,
    total_nacked: AtomicU64,
}

impl TenantCounters {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            enqueue_count: AtomicU64::new(0),
            ack_count: AtomicU64::new(0),
            nack_count: AtomicU64::new(0),
            latency_sum_ms: AtomicU64::new(0),
            total_enqueued: AtomicU64::new(0),
            total_acked: AtomicU64::new(0),
            total_nacked: AtomicU64::new(0),
        })
    }
}

struct TenantStatsState {
    stats: TenantStats,
    counters: Arc<TenantCounters>,
}

pub struct StatsCollector {
    tenants: DashMap<String, TenantStatsState>,
    alpha: f64,
    dormancy_threshold_secs: f64,
    pub min_guarantee_slots: usize,
    burst_penalty: f64,
}

impl StatsCollector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tenants: DashMap::new(),
            alpha: 0.2,
            dormancy_threshold_secs: 30.0,
            min_guarantee_slots: 5,
            burst_penalty: 2.0,
        })
    }

    pub fn register(&self, tenant_id: &str) {
        let now = Instant::now();
        let counters = TenantCounters::new();
        let stats = TenantStats {
            tenant_id: tenant_id.to_string(),
            arrival_rate: 0.0,
            processing_rate: 0.0,
            avg_task_latency: 0.0,
            burst_score: 0.0,
            backlog_ratio: 0.0,
            last_enqueue_at: now,
            last_dequeue_at: now,
            last_alloc_fraction: 0.0,
            is_new: true,
            total_enqueued: 0,
            total_acked: 0,
            total_nacked: 0,
            stats_version: 0,
        };
        self.tenants
            .insert(tenant_id.to_string(), TenantStatsState { stats, counters });
    }

    pub fn deregister(&self, tenant_id: &str) {
        self.tenants.remove(tenant_id);
    }

    pub fn record_enqueue(&self, tenant_id: &str, count: u64) {
        if let Some(state) = self.tenants.get(tenant_id) {
            state
                .counters
                .enqueue_count
                .fetch_add(count, Ordering::Relaxed);
            state
                .counters
                .total_enqueued
                .fetch_add(count, Ordering::Relaxed);
        }
    }

    pub fn record_ack(&self, tenant_id: &str, latency: Duration) {
        self.record_ack_count(tenant_id, 1, latency);
    }

    pub fn record_ack_count(&self, tenant_id: &str, count: u64, latency: Duration) {
        if let Some(state) = self.tenants.get(tenant_id) {
            state.counters.ack_count.fetch_add(count, Ordering::Relaxed);
            state
                .counters
                .latency_sum_ms
                .fetch_add((latency.as_millis() as u64) * count, Ordering::Relaxed);
            state
                .counters
                .total_acked
                .fetch_add(count, Ordering::Relaxed);
        }
    }

    pub fn record_nack(&self, tenant_id: &str) {
        if let Some(state) = self.tenants.get(tenant_id) {
            state.counters.nack_count.fetch_add(1, Ordering::Relaxed);
            state.counters.total_nacked.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_backlog(&self, tenant_id: &str, pending: usize, inflight: usize) {
        if let Some(mut state) = self.tenants.get_mut(tenant_id) {
            let total = pending + inflight;
            state.stats.backlog_ratio = if total > 0 {
                pending as f64 / total as f64
            } else {
                0.0
            };
        }
    }

    /// EMA update from cycle counters. Called every stats cycle (1s).
    pub fn refresh(&self) {
        for mut entry in self.tenants.iter_mut() {
            let state = entry.value_mut();
            let enqueued = state.counters.enqueue_count.swap(0, Ordering::Relaxed);
            let acked = state.counters.ack_count.swap(0, Ordering::Relaxed);
            let nacked = state.counters.nack_count.swap(0, Ordering::Relaxed);
            let latency_sum = state.counters.latency_sum_ms.swap(0, Ordering::Relaxed);

            let alpha = self.alpha;

            // arrival rate: tasks/sec (cycle = 1s)
            let new_arrival = enqueued as f64;
            state.stats.arrival_rate =
                alpha * new_arrival + (1.0 - alpha) * state.stats.arrival_rate;

            // processing rate
            let new_proc = (acked + nacked) as f64;
            state.stats.processing_rate =
                alpha * new_proc + (1.0 - alpha) * state.stats.processing_rate;

            // avg task latency
            if acked > 0 {
                let new_lat = latency_sum as f64 / acked as f64 / 1000.0; // seconds
                state.stats.avg_task_latency =
                    alpha * new_lat + (1.0 - alpha) * state.stats.avg_task_latency;
            }

            // burst_score: EMA of deviation² from arrival_rate (variance proxy)
            let deviation = new_arrival - state.stats.arrival_rate;
            let new_burst = deviation * deviation;
            state.stats.burst_score = alpha * new_burst + (1.0 - alpha) * state.stats.burst_score;

            state.stats.total_enqueued = state.counters.total_enqueued.load(Ordering::Relaxed);
            state.stats.total_acked = state.counters.total_acked.load(Ordering::Relaxed);
            state.stats.total_nacked = state.counters.total_nacked.load(Ordering::Relaxed);
            state.stats.stats_version += 1;

            // After first activity, mark as not new
            if state.stats.is_new && enqueued > 0 {
                state.stats.is_new = false;
            }

            debug!(
                "Stats refreshed for {}: arrival={:.2} proc={:.2} burst={:.4}",
                state.stats.tenant_id,
                state.stats.arrival_rate,
                state.stats.processing_rate,
                state.stats.burst_score
            );
        }
    }

    pub fn warm_from_store(&self, store: &StatsStore) {
        for persisted in store.load_all_stats() {
            if let Some(mut state) = self.tenants.get_mut(&persisted.tenant_id) {
                state.stats.arrival_rate = persisted.arrival_rate;
                state.stats.processing_rate = persisted.processing_rate;
                state.stats.avg_task_latency = persisted.avg_task_latency;
                state.stats.burst_score = persisted.burst_score;
                state.stats.backlog_ratio = persisted.backlog_ratio;
                state.stats.total_enqueued = persisted.total_enqueued;
                state.stats.total_acked = persisted.total_acked;
                state.stats.total_nacked = persisted.total_nacked;
                state.stats.stats_version = persisted.stats_version;
                state.stats.is_new = false;
            }
        }
    }

    /// Adaptive slot allocation.
    pub fn allocate_slots(&self, total_slots: usize) -> Vec<(String, usize)> {
        if total_slots == 0 {
            return vec![];
        }

        let all: Vec<TenantStats> = self.tenants.iter().map(|e| e.stats.clone()).collect();

        let guaranteed: Vec<&TenantStats> = all
            .iter()
            .filter(|s| {
                let dormant =
                    s.last_enqueue_at.elapsed().as_secs_f64() > self.dormancy_threshold_secs;
                s.is_new || dormant
            })
            .collect();

        let normal: Vec<&TenantStats> = all
            .iter()
            .filter(|s| {
                let dormant =
                    s.last_enqueue_at.elapsed().as_secs_f64() > self.dormancy_threshold_secs;
                !(s.is_new || dormant)
            })
            .collect();

        let guaranteed_budget = (self.min_guarantee_slots * guaranteed.len()).min(total_slots);
        let normal_budget = total_slots.saturating_sub(guaranteed_budget);

        // Compute scores for normal tenants
        let mut scored: Vec<(&TenantStats, f64)> = normal
            .iter()
            .map(|s| {
                let backlog_pressure = 1.0 + s.backlog_ratio;
                let burst_damp = 1.0 + self.burst_penalty * s.burst_score.sqrt();
                let score = s.arrival_rate * backlog_pressure / burst_damp;
                (*s, score)
            })
            .collect();

        let total_score: f64 = scored.iter().map(|(_, s)| s).sum();

        let mut allocations: Vec<(String, usize)> = Vec::new();
        let mut leftover = normal_budget;

        if total_score > 0.0 {
            for (stats, score) in &scored {
                let fraction = score / total_score;
                let slots = (normal_budget as f64 * fraction).floor() as usize;
                leftover = leftover.saturating_sub(slots);
                allocations.push((stats.tenant_id.clone(), slots));
            }
        }

        // Leftover to highest backlog normal tenant
        if leftover > 0 && !scored.is_empty() {
            scored.sort_by(|a, b| {
                b.0.backlog_ratio
                    .partial_cmp(&a.0.backlog_ratio)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            if let Some(entry) = allocations
                .iter_mut()
                .find(|(id, _)| *id == scored[0].0.tenant_id)
            {
                entry.1 += leftover;
            }
        }

        // Guaranteed tenants get min_guarantee_slots each
        for stats in guaranteed {
            allocations.push((stats.tenant_id.clone(), self.min_guarantee_slots));
        }

        allocations.retain(|(_, slots)| *slots > 0);
        allocations
    }

    pub fn snapshot(&self, tenant_id: &str) -> Option<TenantStats> {
        self.tenants.get(tenant_id).map(|e| e.stats.clone())
    }

    pub fn all_snapshots(&self) -> Vec<TenantStats> {
        self.tenants.iter().map(|e| e.stats.clone()).collect()
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self {
            tenants: DashMap::new(),
            alpha: 0.2,
            dormancy_threshold_secs: 30.0,
            min_guarantee_slots: 5,
            burst_penalty: 2.0,
        }
    }
}
