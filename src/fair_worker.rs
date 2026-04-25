use std::sync::Arc;
use std::time::Instant;
use std::time::Duration;

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
                        let start = Instant::now();
                        match (self.handler)(&tenant_id, task) {
                            Ok(()) => {
                                self.collector
                                    .record_ack(&tenant_id, start.elapsed());
                                if let Err(e) = self.registry.ack(&tenant_id, &key) {
                                    warn!("Ack error for {tenant_id}: {e}");
                                }
                                debug!("Task acked for tenant {tenant_id}");
                            }
                            Err(e) => {
                                warn!("Handler error for {tenant_id}: {e}");
                                self.collector.record_nack(&tenant_id);
                                if let Err(e) = self.registry.nack(&tenant_id, &key) {
                                    warn!("Nack error for {tenant_id}: {e}");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
