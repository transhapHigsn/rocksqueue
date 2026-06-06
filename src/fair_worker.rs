use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tokio::time;
use tracing::{debug, warn};

use crate::error::Result;
use crate::stats::StatsCollector;
use crate::task::Task;
use crate::tenant::TenantRegistry;

pub type Handler = Arc<dyn Fn(&str, Task) -> Result<()> + Send + Sync + 'static>;

pub struct FairWorkerPool {
    pub registry: Arc<TenantRegistry>,
    pub collector: Arc<StatsCollector>,
    pub queues: Vec<String>,
    pub concurrency: usize,
    pub tick_ms: u64,
    pub total_slots: usize,
    pub handler: Handler,
}

impl FairWorkerPool {
    pub fn run(self: Arc<Self>) {
        for _ in 0..self.concurrency {
            let pool = Arc::clone(&self);
            tokio::spawn(async move {
                pool.worker_loop().await;
            });
        }
    }

    async fn worker_loop(&self) {
        let worker_slots = (self.total_slots / self.concurrency).max(1);
        let mut interval = time::interval(Duration::from_millis(self.tick_ms));

        loop {
            interval.tick().await;

            let schedule = self.collector.allocate_slots(worker_slots);
            if schedule.is_empty() {
                continue;
            }

            for (tenant_id, slots) in schedule {
                if slots == 0 {
                    continue;
                }

                for queue in &self.queues {
                    let tasks = match self.registry.dequeue(&tenant_id, queue, slots) {
                        Ok(t) => t,
                        Err(e) => {
                            warn!("Dequeue error for {tenant_id}/{queue}: {e}");
                            continue;
                        }
                    };

                    for (key, task) in tasks {
                        self.process_task(&tenant_id, &key, task);
                    }
                }
            }
        }
    }

    /// Run the handler for one task, then ack on success (recording the ack only
    /// when the inflight key was actually present — `ack` is idempotent and
    /// returns `false` for an already-removed key) or nack on handler error.
    fn process_task(&self, tenant_id: &str, key: &[u8], task: Task) {
        let start = Instant::now();
        match (self.handler)(tenant_id, task) {
            Ok(()) => match self.registry.ack(tenant_id, key) {
                Ok(true) => {
                    self.collector.record_ack(tenant_id, start.elapsed());
                    debug!("Task acked for tenant {tenant_id}");
                }
                Ok(false) => warn!("Ack key not found for tenant {tenant_id}"),
                Err(e) => warn!("Ack error for {tenant_id}: {e}"),
            },
            Err(e) => {
                warn!("Handler error for {tenant_id}: {e}");
                self.collector.record_nack(tenant_id);
                if let Err(e) = self.registry.nack(tenant_id, key) {
                    warn!("Nack error for {tenant_id}: {e}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tempfile::TempDir;

    use crate::policy::NamespacePolicy;
    use crate::stats::StatsCollector;
    use crate::task::encode_key;
    use crate::tenant::{DbConfig, TenantRegistry};

    fn setup(tmp: &TempDir) -> (Arc<TenantRegistry>, Arc<StatsCollector>, NamespacePolicy) {
        let cfg = DbConfig {
            sst_path: tmp.path().join("sst").to_string_lossy().to_string(),
            wal_path: tmp.path().join("wal").to_string_lossy().to_string(),
            block_cache_bytes: 16 * 1024 * 1024,
            write_buffer_bytes: 4 * 1024 * 1024,
            max_write_buffers: 2,
        };
        let registry = Arc::new(TenantRegistry::open(&cfg).unwrap());
        let policy = NamespacePolicy::standard("acme");
        registry.provision_tenant("acme", policy.clone()).unwrap();
        let collector = StatsCollector::new();
        collector.register("acme");
        (registry, collector, policy)
    }

    fn make_pool(
        registry: Arc<TenantRegistry>,
        collector: Arc<StatsCollector>,
        calls: Arc<AtomicUsize>,
    ) -> FairWorkerPool {
        FairWorkerPool {
            registry,
            collector,
            queues: vec!["default".to_string()],
            concurrency: 1,
            tick_ms: 10,
            total_slots: 1,
            handler: Arc::new(move |_tenant, _task| {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }),
        }
    }

    #[test]
    fn test_process_task_records_ack_on_success() {
        let tmp = TempDir::new().unwrap();
        let (registry, collector, policy) = setup(&tmp);
        registry
            .enqueue("acme", "default", b"task".to_vec(), &policy)
            .unwrap();
        let (key, task) = registry.dequeue("acme", "default", 1).unwrap().remove(0);

        let calls = Arc::new(AtomicUsize::new(0));
        let pool = make_pool(
            Arc::clone(&registry),
            Arc::clone(&collector),
            Arc::clone(&calls),
        );
        pool.process_task("acme", &key, task);

        collector.refresh();
        assert_eq!(calls.load(Ordering::Relaxed), 1, "handler must run");
        assert_eq!(collector.snapshot("acme").unwrap().total_acked, 1);
        assert_eq!(
            registry.depth("acme", "default").unwrap().1,
            0,
            "inflight cleared"
        );
    }

    #[test]
    fn test_process_task_skips_ack_when_key_missing() {
        let tmp = TempDir::new().unwrap();
        let (registry, collector, policy) = setup(&tmp);
        registry
            .enqueue("acme", "default", b"task".to_vec(), &policy)
            .unwrap();
        let (_key, task) = registry.dequeue("acme", "default", 1).unwrap().remove(0);

        let calls = Arc::new(AtomicUsize::new(0));
        let pool = make_pool(
            Arc::clone(&registry),
            Arc::clone(&collector),
            Arc::clone(&calls),
        );
        // A key that was never written to inflight: ack returns false, so the
        // handler runs but no ack is recorded (covers the Ok(false) branch).
        let missing = encode_key("default", u64::MAX);
        pool.process_task("acme", &missing, task);

        collector.refresh();
        assert_eq!(calls.load(Ordering::Relaxed), 1, "handler must still run");
        assert_eq!(
            collector.snapshot("acme").unwrap().total_acked,
            0,
            "no ack recorded for a missing key"
        );
    }
}
