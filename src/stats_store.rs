use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rocksdb::{WriteOptions, DB};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::baseline::TenantBaseline;
use crate::stats::TenantStats;

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

const CF_SYSTEM: &str = "__system__";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedStats {
    pub tenant_id: String,
    pub arrival_rate: f64,
    pub processing_rate: f64,
    pub avg_task_latency: f64,
    pub burst_score: f64,
    pub backlog_ratio: f64,
    pub total_enqueued: u64,
    pub total_acked: u64,
    pub total_nacked: u64,
    pub stats_version: u64,
    pub flushed_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedThrottle {
    pub tenant_id: String,
    pub throttled: bool,
    pub reason: String,
    pub original_rate: u32,
    pub applied_rate: u32,
    pub triggered_at_ms: u64,
    pub cooldown_until_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineRecord {
    pub tenant_id: String,
    pub short_ema: f64,
    pub long_ema: f64,
    pub spike_threshold: f64,
    pub saved_at_ms: u64,
}

pub struct StatsStore {
    db: Arc<DB>,
}

impl StatsStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    fn sync_write_opts() -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        opts.disable_wal(false);
        opts
    }

    fn cf(&self) -> Arc<rocksdb::BoundColumnFamily> {
        self.db.cf_handle(CF_SYSTEM).expect("__system__ CF missing")
    }

    pub fn flush_stats(&self, stats: &[TenantStats]) {
        let cf = self.cf();
        let now = now_millis();
        for s in stats {
            let record = PersistedStats {
                tenant_id: s.tenant_id.clone(),
                arrival_rate: s.arrival_rate,
                processing_rate: s.processing_rate,
                avg_task_latency: s.avg_task_latency,
                burst_score: s.burst_score,
                backlog_ratio: s.backlog_ratio,
                total_enqueued: s.total_enqueued,
                total_acked: s.total_acked,
                total_nacked: s.total_nacked,
                stats_version: s.stats_version,
                flushed_at_ms: now,
            };
            let key = format!("stats:{}", s.tenant_id);
            match bincode::serialize(&record) {
                Ok(value) => {
                    if let Err(e) =
                        self.db
                            .put_cf_opt(&cf, key.as_bytes(), &value, &Self::sync_write_opts())
                    {
                        warn!("Failed to flush stats for {}: {e}", s.tenant_id);
                    }
                }
                Err(e) => warn!("Failed to serialize stats for {}: {e}", s.tenant_id),
            }
        }
    }

    /// Load all persisted stats and apply time-decay.
    pub fn load_all_stats(&self) -> Vec<PersistedStats> {
        let cf = self.cf();
        let prefix = b"stats:";
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(prefix, rocksdb::Direction::Forward),
        );
        let now = now_millis();
        let mut results = Vec::new();

        for item in iter {
            match item {
                Ok((key, value)) => {
                    if !key.starts_with(prefix) {
                        break;
                    }
                    match bincode::deserialize::<PersistedStats>(&value) {
                        Ok(mut record) => {
                            // Time-decay: values decay based on offline time
                            let offline_secs =
                                now.saturating_sub(record.flushed_at_ms) as f64 / 1000.0;
                            let offline_mins = offline_secs / 60.0;
                            let decay = 2f64.powf(-offline_mins);
                            record.arrival_rate *= decay;
                            record.burst_score *= decay;
                            record.backlog_ratio *= decay;
                            record.processing_rate *= (decay + 1.0) / 2.0;
                            record.avg_task_latency *= (decay + 1.0) / 2.0;
                            results.push(record);
                        }
                        Err(e) => warn!("Failed to deserialize stats record: {e}"),
                    }
                }
                Err(e) => warn!("Error iterating stats: {e}"),
            }
        }
        results
    }

    pub fn persist_throttle(&self, state: &PersistedThrottle) {
        let cf = self.cf();
        let key = format!("throttle:{}", state.tenant_id);
        match bincode::serialize(state) {
            Ok(value) => {
                if let Err(e) =
                    self.db
                        .put_cf_opt(&cf, key.as_bytes(), &value, &Self::sync_write_opts())
                {
                    warn!("Failed to persist throttle for {}: {e}", state.tenant_id);
                }
            }
            Err(e) => warn!("Failed to serialize throttle: {e}"),
        }
    }

    pub fn clear_throttle(&self, tenant_id: &str) {
        let cf = self.cf();
        let key = format!("throttle:{tenant_id}");
        if let Err(e) = self
            .db
            .delete_cf_opt(&cf, key.as_bytes(), &Self::sync_write_opts())
        {
            warn!("Failed to clear throttle for {tenant_id}: {e}");
        }
    }

    pub fn load_all_throttles(&self) -> Vec<PersistedThrottle> {
        let cf = self.cf();
        let prefix = b"throttle:";
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(prefix, rocksdb::Direction::Forward),
        );
        let mut results = Vec::new();

        for item in iter {
            match item {
                Ok((key, value)) => {
                    if !key.starts_with(prefix) {
                        break;
                    }
                    match bincode::deserialize::<PersistedThrottle>(&value) {
                        Ok(record) => results.push(record),
                        Err(e) => warn!("Failed to deserialize throttle record: {e}"),
                    }
                }
                Err(e) => warn!("Error iterating throttles: {e}"),
            }
        }
        results
    }

    pub fn flush_baselines(&self, baselines: &[TenantBaseline]) {
        let cf = self.cf();
        let now = now_millis();
        for b in baselines {
            let record = BaselineRecord {
                tenant_id: b.tenant_id.clone(),
                short_ema: b.short_ema,
                long_ema: b.long_ema,
                spike_threshold: b.spike_threshold,
                saved_at_ms: now,
            };
            let key = format!("baseline:{}", b.tenant_id);
            match bincode::serialize(&record) {
                Ok(value) => {
                    if let Err(e) =
                        self.db
                            .put_cf_opt(&cf, key.as_bytes(), &value, &Self::sync_write_opts())
                    {
                        warn!("Failed to flush baseline for {}: {e}", b.tenant_id);
                    }
                }
                Err(e) => warn!("Failed to serialize baseline: {e}"),
            }
        }
    }

    pub fn load_baseline(&self, tenant_id: &str) -> Option<BaselineRecord> {
        let cf = self.cf();
        let key = format!("baseline:{tenant_id}");
        self.db
            .get_cf(&cf, key.as_bytes())
            .ok()
            .flatten()
            .and_then(|v| {
                bincode::deserialize::<BaselineRecord>(&v)
                    .map_err(|e| warn!("Failed to deserialize baseline for {tenant_id}: {e}"))
                    .ok()
            })
    }

    pub fn clear_baseline(&self, tenant_id: &str) {
        let cf = self.cf();
        let key = format!("baseline:{tenant_id}");
        if let Err(e) = self
            .db
            .delete_cf_opt(&cf, key.as_bytes(), &Self::sync_write_opts())
        {
            warn!("Failed to clear baseline for {tenant_id}: {e}");
        }
    }
}
