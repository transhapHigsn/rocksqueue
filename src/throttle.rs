use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tracing::{info, warn};

use crate::baseline::BaselineRegistry;
use crate::scheduler::{TenantPolicy, WFQScheduler};
use crate::stats::TenantStats;
use crate::stats_store::{PersistedThrottle, StatsStore};

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone)]
pub struct ThrottleConfig {
    pub burst_threshold: f64,
    pub rate_spike_factor: f64,
    pub backlog_threshold: f64,
    pub reduction_factor: f64,
    pub min_rate_floor: u32,
    pub cooldown_secs: u64,
    pub baseline_window_secs: u64,
}

impl Default for ThrottleConfig {
    fn default() -> Self {
        Self {
            burst_threshold: 5.0,
            rate_spike_factor: 3.0,
            backlog_threshold: 0.85,
            reduction_factor: 0.25,
            min_rate_floor: 5,
            cooldown_secs: 30,
            baseline_window_secs: 300,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ThrottleState {
    pub tenant_id: String,
    pub throttled: bool,
    pub reason: String,
    pub original_rate: u32,
    pub applied_rate: u32,
    pub triggered_at_ms: u64,
    pub cooldown_until_ms: u64,
}

pub struct AutoThrottle {
    config: ThrottleConfig,
    states: DashMap<String, ThrottleState>,
    scheduler: Arc<WFQScheduler>,
    baselines: Arc<BaselineRegistry>,
    store: Arc<StatsStore>,
}

impl AutoThrottle {
    pub fn new(
        config: ThrottleConfig,
        scheduler: Arc<WFQScheduler>,
        baselines: Arc<BaselineRegistry>,
        store: Arc<StatsStore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            states: DashMap::new(),
            scheduler,
            baselines,
            store,
        })
    }

    pub fn restore_from_store(&self) {
        for persisted in self.store.load_all_throttles() {
            if persisted.throttled {
                let now = now_millis();
                if persisted.cooldown_until_ms > now {
                    // Still in cooldown — re-apply throttle
                    let throttled_policy = self.scheduler.policy(&persisted.tenant_id);
                    if let Some(mut policy) = throttled_policy {
                        policy.rate_per_sec = persisted.applied_rate;
                        policy.burst_tokens = persisted.applied_rate.max(1);
                        self.scheduler.update_policy(policy);
                    }
                    self.states.insert(
                        persisted.tenant_id.clone(),
                        ThrottleState {
                            tenant_id: persisted.tenant_id,
                            throttled: true,
                            reason: persisted.reason,
                            original_rate: persisted.original_rate,
                            applied_rate: persisted.applied_rate,
                            triggered_at_ms: persisted.triggered_at_ms,
                            cooldown_until_ms: persisted.cooldown_until_ms,
                        },
                    );
                } else {
                    // Cooldown expired — clear
                    self.store.clear_throttle(&persisted.tenant_id);
                }
            }
        }
    }

    pub fn evaluate(&self, all_stats: &[TenantStats]) {
        let now = now_millis();

        for stats in all_stats {
            let tenant_id = &stats.tenant_id;
            let is_spike = self.baselines.is_spike(tenant_id, stats.arrival_rate);

            let should_throttle = stats.burst_score > self.config.burst_threshold
                || stats.backlog_ratio > self.config.backlog_threshold
                || is_spike;

            if let Some(existing) = self.states.get(tenant_id) {
                if existing.throttled {
                    // Check release conditions
                    let cooldown_done = now >= existing.cooldown_until_ms;
                    let burst_ok = stats.burst_score < self.config.burst_threshold * 0.5;
                    let backlog_ok = stats.backlog_ratio < self.config.backlog_threshold * 0.7;
                    let spike_threshold = self
                        .baselines
                        .spike_threshold(tenant_id)
                        .unwrap_or(f64::MAX);
                    let rate_ok = stats.arrival_rate < spike_threshold * 0.7;

                    if cooldown_done && burst_ok && backlog_ok && rate_ok {
                        self.release_throttle(tenant_id, existing.original_rate);
                    }
                    continue;
                }
            }

            if should_throttle {
                let reason = if stats.burst_score > self.config.burst_threshold {
                    "burst_score_exceeded"
                } else if stats.backlog_ratio > self.config.backlog_threshold {
                    "backlog_threshold_exceeded"
                } else {
                    "rate_spike_detected"
                };
                self.apply_throttle(tenant_id, stats, reason, now);
            }
        }
    }

    fn apply_throttle(&self, tenant_id: &str, stats: &TenantStats, reason: &str, now: u64) {
        if let Some(policy) = self.scheduler.policy(tenant_id) {
            let original_rate = policy.rate_per_sec;
            let applied_rate = ((original_rate as f64 * self.config.reduction_factor) as u32)
                .max(self.config.min_rate_floor);

            let mut throttled_policy = policy.clone();
            throttled_policy.rate_per_sec = applied_rate;
            throttled_policy.burst_tokens = applied_rate.max(1);
            throttled_policy.max_inflight =
                (policy.max_inflight as f64 * self.config.reduction_factor) as usize;
            self.scheduler.update_policy(throttled_policy);

            let state = ThrottleState {
                tenant_id: tenant_id.to_string(),
                throttled: true,
                reason: reason.to_string(),
                original_rate,
                applied_rate,
                triggered_at_ms: now,
                cooldown_until_ms: now + self.config.cooldown_secs * 1000,
            };

            self.store.persist_throttle(&PersistedThrottle {
                tenant_id: tenant_id.to_string(),
                throttled: true,
                reason: reason.to_string(),
                original_rate,
                applied_rate,
                triggered_at_ms: now,
                cooldown_until_ms: state.cooldown_until_ms,
            });

            warn!(
                "Throttled tenant {tenant_id}: {original_rate} → {applied_rate} req/s (reason: {reason})"
            );
            self.states.insert(tenant_id.to_string(), state);
        }
    }

    fn release_throttle(&self, tenant_id: &str, original_rate: u32) {
        if let Some(mut policy) = self.scheduler.policy(tenant_id) {
            policy.rate_per_sec = original_rate;
            policy.burst_tokens = original_rate;
            self.scheduler.update_policy(policy);
        }
        self.store.clear_throttle(tenant_id);
        self.states.remove(tenant_id);
        info!("Released throttle for tenant: {tenant_id}");
    }

    pub fn force_release(&self, tenant_id: &str) {
        if let Some(state) = self.states.get(tenant_id) {
            let original_rate = state.original_rate;
            drop(state);
            self.release_throttle(tenant_id, original_rate);
        } else {
            self.store.clear_throttle(tenant_id);
        }
    }

    pub fn snapshot(&self, tenant_id: &str) -> Option<ThrottleState> {
        self.states.get(tenant_id).map(|e| e.clone())
    }

    pub fn all_snapshots(&self) -> Vec<ThrottleState> {
        self.states.iter().map(|e| e.value().clone()).collect()
    }
}
