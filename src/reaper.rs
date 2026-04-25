use std::sync::Arc;
use std::time::Duration;

use rocksdb::WriteBatch;
use tokio::time;
use tracing::{debug, warn};

use crate::task::{now_millis, queue_prefix, Task};
use crate::tenant::TenantRegistry;

pub fn spawn_reaper(
    registry: Arc<TenantRegistry>,
    queue_names: Vec<String>,
    interval_secs: u64,
) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;

            for tenant_id in registry.list_tenants() {
                for queue in &queue_names {
                    if let Err(e) = reap_inflight(&registry, &tenant_id, queue) {
                        warn!("Reaper error for {tenant_id}/{queue}: {e}");
                    }
                }
            }
        }
    });
}

fn reap_inflight(
    registry: &TenantRegistry,
    tenant_id: &str,
    queue: &str,
) -> crate::error::Result<()> {
    let inflight_cf = registry
        .db
        .cf_handle(&format!("{tenant_id}__inflight"))
        .ok_or_else(|| {
            crate::error::QueueError::ColumnFamilyMissing(format!("{tenant_id}__inflight"))
        })?;
    let pending_cf = registry
        .db
        .cf_handle(&format!("{tenant_id}__pending"))
        .ok_or_else(|| {
            crate::error::QueueError::ColumnFamilyMissing(format!("{tenant_id}__pending"))
        })?;

    let prefix = queue_prefix(queue);
    let iter = registry.db.iterator_cf(
        &inflight_cf,
        rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
    );
    let now = now_millis();

    let mut batch = WriteBatch::default();
    let mut reclaimed = 0usize;

    for item in iter {
        let (key, value) = item?;
        if !key.starts_with(&prefix) {
            break;
        }
        let task = Task::deserialize(&value)?;
        if task.deadline > 0 && task.deadline < now {
            let mut reclaimed_task = task;
            reclaimed_task.deadline = 0;
            let new_value = reclaimed_task.serialize()?;

            batch.delete_cf(&inflight_cf, &key);
            batch.put_cf(&pending_cf, &key, &new_value);
            reclaimed += 1;
        }
    }

    if reclaimed > 0 {
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(false);
        registry.db.write_opt(batch, &write_opts)?;
        debug!("Reaper reclaimed {reclaimed} tasks from {tenant_id}/{queue}");
    }

    Ok(())
}
