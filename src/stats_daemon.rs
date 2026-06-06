use std::sync::Arc;
use std::time::Duration;

use tokio::time;
use tracing::{info, warn};

use crate::baseline::BaselineRegistry;
use crate::stats::StatsCollector;
use crate::stats_store::StatsStore;
use crate::tenant::TenantRegistry;
use crate::throttle::AutoThrottle;

pub fn spawn_stats_daemon(
    collector: Arc<StatsCollector>,
    registry: Arc<TenantRegistry>,
    store: Arc<StatsStore>,
    throttle: Arc<AutoThrottle>,
    baselines: Arc<BaselineRegistry>,
    queues: Vec<String>,
    cycle_ms: u64,
) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(cycle_ms));
        let mut cycle_count: u64 = 0;

        loop {
            interval.tick().await;
            cycle_count += 1;

            // 1. Refresh EMA stats from cycle counters
            collector.refresh();

            // 2. Record backlog pressure from RocksDB depth
            for tenant_id in registry.list_tenants() {
                for queue in &queues {
                    match registry.depth(&tenant_id, queue) {
                        Ok((pending, inflight, _)) => {
                            collector.record_backlog(&tenant_id, pending, inflight);
                        }
                        Err(e) => {
                            warn!("Failed to get depth for {tenant_id}/{queue}: {e}");
                        }
                    }
                }
            }

            // 3. Update baselines (CUSUM drift detection)
            let snapshots = collector.all_snapshots();
            for s in &snapshots {
                if let Some(promotion) = baselines.update(&s.tenant_id, s.arrival_rate) {
                    info!(
                        "Baseline promoted for {}: {:.2} → {:.2}",
                        promotion.tenant_id, promotion.old_threshold, promotion.new_threshold
                    );
                    // Flush baseline immediately on promotion
                    if let Some(baseline) = baselines.snapshot(&s.tenant_id) {
                        store.flush_baselines(&[baseline]);
                    }
                }
            }

            // 4. Evaluate throttle signals
            throttle.evaluate(&snapshots);

            // 5. Every 10 cycles: persist stats + baselines
            if cycle_count % 10 == 0 {
                store.flush_stats(&snapshots);
                baselines.flush();
            }
        }
    });
}
