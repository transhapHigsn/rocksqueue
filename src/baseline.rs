use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::stats_store::StatsStore;

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CusumState {
    pub cusum: f64,
    pub h_threshold: f64,
    pub k_slack: f64,
    pub consecutive_calm: u32,
}

impl CusumState {
    fn new(baseline: f64) -> Self {
        Self {
            cusum: 0.0,
            h_threshold: 50.0,
            k_slack: baseline * 0.3,
            consecutive_calm: 0,
        }
    }

    /// Feed a new value (short_ema vs long_ema deviation).
    /// Returns true if drift is confirmed.
    fn update(&mut self, x: f64, mu: f64) -> bool {
        let s = x - mu - self.k_slack;
        self.cusum = (self.cusum + s).max(0.0);

        if self.cusum > self.h_threshold {
            return true;
        }

        self.consecutive_calm += 1;
        // Slow decay during calm periods
        self.cusum *= 0.9;
        false
    }

    fn reset(&mut self, new_baseline: f64) {
        self.cusum = 0.0;
        self.k_slack = new_baseline * 0.3;
        self.consecutive_calm = 0;
    }
}

#[derive(Debug, Clone)]
pub struct BaselinePromotion {
    pub tenant_id: String,
    pub old_threshold: f64,
    pub new_threshold: f64,
    pub new_baseline: f64,
    pub promoted_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct TenantBaseline {
    pub tenant_id: String,
    pub short_ema: f64,
    pub long_ema: f64,
    pub rolling_peak: f64,
    pub spike_threshold: f64,
    pub cusum: CusumState,
    pub promotions: Vec<BaselinePromotion>,
    pub last_updated_ms: u64,
    /// 5-minute rolling peak window: (timestamp_ms, rate)
    peak_window: Vec<(u64, f64)>,
}

impl TenantBaseline {
    fn new(tenant_id: String, initial_rate: f64) -> Self {
        let initial_threshold = initial_rate * 3.0;
        Self {
            short_ema: initial_rate,
            long_ema: initial_rate,
            rolling_peak: initial_rate,
            spike_threshold: initial_threshold.max(10.0),
            cusum: CusumState::new(initial_rate),
            promotions: Vec::new(),
            last_updated_ms: now_millis(),
            tenant_id,
            peak_window: Vec::new(),
        }
    }

    pub fn update(
        &mut self,
        arrival_rate: f64,
        short_alpha: f64,
        long_alpha: f64,
        spike_factor: f64,
    ) -> Option<BaselinePromotion> {
        // 1. Update short EMA
        self.short_ema = short_alpha * arrival_rate + (1.0 - short_alpha) * self.short_ema;

        // 2. Update long EMA
        self.long_ema = long_alpha * arrival_rate + (1.0 - long_alpha) * self.long_ema;

        // 3. Update rolling peak (5-min = 300,000ms window)
        let now = now_millis();
        self.peak_window.push((now, arrival_rate));
        let cutoff = now.saturating_sub(5 * 60 * 1000);
        self.peak_window.retain(|(ts, _)| *ts >= cutoff);
        self.rolling_peak = self
            .peak_window
            .iter()
            .map(|(_, r)| *r)
            .fold(f64::NEG_INFINITY, f64::max)
            .max(0.0);

        // 4. Feed CUSUM with short_ema deviation from long_ema
        let drift = self.cusum.update(self.short_ema, self.long_ema);

        // 5. Promote if drift confirmed and threshold would grow > 5%
        if drift {
            let new_baseline = self.rolling_peak * 0.8;
            let new_threshold = new_baseline * spike_factor;
            if new_threshold > self.spike_threshold * 1.05 {
                let old_threshold = self.spike_threshold;
                self.spike_threshold = new_threshold;
                self.long_ema = new_baseline;
                self.cusum.reset(new_baseline);

                let promotion = BaselinePromotion {
                    tenant_id: self.tenant_id.clone(),
                    old_threshold,
                    new_threshold,
                    new_baseline,
                    promoted_at_ms: now,
                };

                self.promotions.push(promotion.clone());
                if self.promotions.len() > 20 {
                    self.promotions.remove(0);
                }

                info!(
                    "Baseline promoted for {}: {old_threshold:.2} → {new_threshold:.2}",
                    self.tenant_id
                );
                return Some(promotion);
            } else {
                // Reset CUSUM anyway after false alarm
                self.cusum.reset(self.long_ema);
            }
        }

        self.last_updated_ms = now;
        None
    }
}

pub struct BaselineRegistry {
    baselines: DashMap<String, TenantBaseline>,
    store: Arc<StatsStore>,
}

impl BaselineRegistry {
    pub fn new(store: Arc<StatsStore>) -> Arc<Self> {
        Arc::new(Self {
            baselines: DashMap::new(),
            store,
        })
    }

    pub fn register(&self, tenant_id: &str, initial_rate: f64) {
        // Warm from store if available
        let baseline = if let Some(record) = self.store.load_baseline(tenant_id) {
            let mut b = TenantBaseline::new(tenant_id.to_string(), record.long_ema);
            b.short_ema = record.short_ema;
            b.long_ema = record.long_ema;
            b.spike_threshold = record.spike_threshold;
            b
        } else {
            TenantBaseline::new(tenant_id.to_string(), initial_rate)
        };
        self.baselines.insert(tenant_id.to_string(), baseline);
    }

    pub fn deregister(&self, tenant_id: &str) {
        self.baselines.remove(tenant_id);
    }

    pub fn update(
        &self,
        tenant_id: &str,
        arrival_rate: f64,
    ) -> Option<BaselinePromotion> {
        self.baselines.get_mut(tenant_id).and_then(|mut b| {
            b.update(arrival_rate, 0.2, 0.01, 3.0)
        })
    }

    pub fn is_spike(&self, tenant_id: &str, rate: f64) -> bool {
        self.baselines
            .get(tenant_id)
            .map(|b| rate > b.spike_threshold)
            .unwrap_or(false)
    }

    pub fn spike_threshold(&self, tenant_id: &str) -> Option<f64> {
        self.baselines.get(tenant_id).map(|b| b.spike_threshold)
    }

    pub fn snapshot(&self, tenant_id: &str) -> Option<TenantBaseline> {
        self.baselines.get(tenant_id).map(|b| b.clone())
    }

    pub fn all_snapshots(&self) -> Vec<TenantBaseline> {
        self.baselines.iter().map(|e| e.value().clone()).collect()
    }

    pub fn flush(&self) {
        let snapshots: Vec<TenantBaseline> = self.all_snapshots();
        self.store.flush_baselines(&snapshots);
    }
}
