use std::time::Instant;

use dashmap::DashMap;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct TenantPolicy {
    pub tenant_id: String,
    pub weight: u32,
    pub max_inflight: usize,
    pub burst_tokens: u32,
    pub rate_per_sec: u32,
}

impl TenantPolicy {
    pub fn standard(id: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            tenant_id: id,
            weight: 10,
            max_inflight: 500,
            burst_tokens: 100,
            rate_per_sec: 100,
        }
    }

    pub fn premium(id: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            tenant_id: id,
            weight: 50,
            max_inflight: 5000,
            burst_tokens: 1000,
            rate_per_sec: 1000,
        }
    }
}

struct TenantState {
    policy: TenantPolicy,
    tokens: f64,
    inflight: usize,
    paused: bool,
    virtual_finish_time: f64,
    last_refill: Instant,
    created_at: Instant,
}

impl TenantState {
    fn new(policy: TenantPolicy) -> Self {
        let tokens = policy.burst_tokens as f64;
        Self {
            policy,
            tokens,
            inflight: 0,
            paused: false,
            virtual_finish_time: 0.0,
            last_refill: Instant::now(),
            created_at: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        let refill = elapsed * self.policy.rate_per_sec as f64;
        self.tokens = (self.tokens + refill).min(self.policy.burst_tokens as f64);
        self.last_refill = now;
    }
}

pub struct WFQScheduler {
    tenants: DashMap<String, TenantState>,
    /// Global virtual time for WFQ ordering
    virtual_time: std::sync::atomic::AtomicU64,
}

impl WFQScheduler {
    pub fn new() -> Self {
        Self {
            tenants: DashMap::new(),
            virtual_time: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn register(&self, policy: TenantPolicy) {
        self.tenants
            .insert(policy.tenant_id.clone(), TenantState::new(policy));
    }

    pub fn deregister(&self, tenant_id: &str) {
        self.tenants.remove(tenant_id);
    }

    pub fn update_policy(&self, policy: TenantPolicy) {
        if let Some(mut state) = self.tenants.get_mut(&policy.tenant_id) {
            state.policy = policy;
        } else {
            self.tenants
                .insert(policy.tenant_id.clone(), TenantState::new(policy));
        }
    }

    /// Proportional slot allocation by weight, sorted by virtual_finish_time (WFQ).
    pub fn schedule(&self, total_slots: usize) -> Vec<(String, usize)> {
        if total_slots == 0 {
            return vec![];
        }

        let vt = self.virtual_time.load(std::sync::atomic::Ordering::Relaxed) as f64;

        // Collect eligible tenants
        let mut candidates: Vec<(String, f64, u32, f64)> = Vec::new();
        let total_weight: u32 = self
            .tenants
            .iter()
            .filter(|e| !e.paused && e.inflight < e.policy.max_inflight)
            .map(|e| e.policy.weight)
            .sum();

        if total_weight == 0 {
            return vec![];
        }

        for mut entry in self.tenants.iter_mut() {
            let state = entry.value_mut();
            if state.paused || state.inflight >= state.policy.max_inflight {
                continue;
            }
            state.refill();
            candidates.push((
                state.policy.tenant_id.clone(),
                state.virtual_finish_time,
                state.policy.weight,
                state.tokens,
            ));
        }

        // Sort by virtual_finish_time (WFQ fairness)
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut allocations: Vec<(String, usize)> = Vec::new();
        let mut remaining = total_slots;

        for (tenant_id, _vft, weight, tokens) in &candidates {
            if remaining == 0 {
                break;
            }
            let fraction = *weight as f64 / total_weight as f64;
            let raw_slots = (total_slots as f64 * fraction).floor() as usize;
            let token_cap = tokens.floor() as usize;
            let slots = raw_slots.min(token_cap).min(remaining);
            if slots == 0 {
                continue;
            }

            // Consume tokens and update virtual finish time
            if let Some(mut state) = self.tenants.get_mut(tenant_id) {
                state.tokens -= slots as f64;
                let packet_size = slots as f64;
                let new_vft =
                    vt.max(state.virtual_finish_time) + packet_size / (*weight).max(1) as f64;
                state.virtual_finish_time = new_vft;
                state.inflight += slots;
            }

            allocations.push((tenant_id.clone(), slots));
            remaining -= slots;

            debug!("Scheduled {slots} slots for tenant {tenant_id}");
        }

        // Give leftover slots to highest-backlog tenant (first in sorted order)
        if remaining > 0 {
            if let Some(entry) = allocations.first_mut() {
                entry.1 += remaining;
                if let Some(mut state) = self.tenants.get_mut(&entry.0) {
                    state.inflight += remaining;
                }
            }
        }

        // Advance global virtual time
        let new_vt = (vt + 1.0) as u64;
        self.virtual_time
            .store(new_vt, std::sync::atomic::Ordering::Relaxed);

        allocations
    }

    pub fn set_paused(&self, tenant_id: &str, paused: bool) -> Option<()> {
        self.tenants.get_mut(tenant_id).map(|mut e| {
            e.paused = paused;
        })
    }

    pub fn release(&self, tenant_id: &str, n: usize) {
        if let Some(mut state) = self.tenants.get_mut(tenant_id) {
            state.inflight = state.inflight.saturating_sub(n);
        }
    }

    pub fn policy(&self, tenant_id: &str) -> Option<TenantPolicy> {
        self.tenants.get(tenant_id).map(|e| e.policy.clone())
    }

    pub fn baseline_rate(&self, tenant_id: &str) -> Option<f64> {
        self.tenants
            .get(tenant_id)
            .map(|e| e.policy.rate_per_sec as f64)
    }

    pub fn tenant_snapshot(&self, id: &str) -> Option<(TenantPolicy, f64, bool, Instant)> {
        self.tenants
            .get(id)
            .map(|e| (e.policy.clone(), e.tokens, e.paused, e.created_at))
    }

    pub fn all_tenant_ids(&self) -> Vec<String> {
        self.tenants.iter().map(|e| e.key().clone()).collect()
    }

    pub fn all_snapshots(&self) -> Vec<(String, TenantPolicy, f64, usize)> {
        self.tenants
            .iter()
            .map(|e| (e.key().clone(), e.policy.clone(), e.tokens, e.inflight))
            .collect()
    }
}

impl Default for WFQScheduler {
    fn default() -> Self {
        Self::new()
    }
}
