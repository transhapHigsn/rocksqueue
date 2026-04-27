use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rocksdb::compaction_filter::Decision;

use crate::task::Task;

pub struct CompactionCounters {
    pub pending_expired: AtomicU64,
    pub dlq_expired: AtomicU64,
    pub inflight_stale: AtomicU64,
    pub kept: AtomicU64,
}

impl CompactionCounters {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            pending_expired: AtomicU64::new(0),
            dlq_expired: AtomicU64::new(0),
            inflight_stale: AtomicU64::new(0),
            kept: AtomicU64::new(0),
        })
    }
}

impl Default for CompactionCounters {
    fn default() -> Self {
        Self {
            pending_expired: AtomicU64::new(0),
            dlq_expired: AtomicU64::new(0),
            inflight_stale: AtomicU64::new(0),
            kept: AtomicU64::new(0),
        }
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Returns a compaction filter function for the pending CF.
/// Removes tasks older than `retention_secs`. 0 = keep forever.
pub fn make_pending_filter(
    retention_secs: u64,
    counters: Arc<CompactionCounters>,
) -> impl Fn(u32, &[u8], &[u8]) -> Decision {
    move |_level, _key, value| {
        if retention_secs == 0 {
            counters.kept.fetch_add(1, Ordering::Relaxed);
            return Decision::Keep;
        }
        match Task::deserialize(value) {
            Ok(task) => {
                let age_ms = now_millis().saturating_sub(task.enqueued_at);
                if age_ms > retention_secs * 1000 {
                    counters.pending_expired.fetch_add(1, Ordering::Relaxed);
                    Decision::Remove
                } else {
                    counters.kept.fetch_add(1, Ordering::Relaxed);
                    Decision::Keep
                }
            }
            Err(_) => {
                // Corrupt value — remove
                counters.pending_expired.fetch_add(1, Ordering::Relaxed);
                Decision::Remove
            }
        }
    }
}

/// Returns a compaction filter function for the DLQ CF.
pub fn make_dlq_filter(
    dlq_retention_secs: u64,
    counters: Arc<CompactionCounters>,
) -> impl Fn(u32, &[u8], &[u8]) -> Decision {
    move |_level, _key, value| {
        match Task::deserialize(value) {
            Ok(task) => {
                let age_ms = now_millis().saturating_sub(task.enqueued_at);
                if age_ms > dlq_retention_secs * 1000 {
                    counters.dlq_expired.fetch_add(1, Ordering::Relaxed);
                    Decision::Remove
                } else {
                    counters.kept.fetch_add(1, Ordering::Relaxed);
                    Decision::Keep
                }
            }
            Err(_) => {
                counters.dlq_expired.fetch_add(1, Ordering::Relaxed);
                Decision::Remove
            }
        }
    }
}

/// Returns a compaction filter function for the inflight CF.
/// Only removes corrupt (undeserializable) records — stale-but-valid inflight tasks
/// are recovered by the reaper to preserve at-least-once delivery semantics.
pub fn make_inflight_filter(
    _stale_after_secs: u64,
    counters: Arc<CompactionCounters>,
) -> impl Fn(u32, &[u8], &[u8]) -> Decision {
    move |_level, _key, value| match Task::deserialize(value) {
        Ok(_) => {
            counters.kept.fetch_add(1, Ordering::Relaxed);
            Decision::Keep
        }
        Err(_) => {
            // Corrupt value — unrecoverable, safe to purge
            counters.inflight_stale.fetch_add(1, Ordering::Relaxed);
            Decision::Remove
        }
    }
}
