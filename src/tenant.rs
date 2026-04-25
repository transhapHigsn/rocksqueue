use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, CompactionDecision, DBCompressionType,
    Options, WriteBatch, WriteOptions, DB,
};
use tracing::{debug, info, warn};

use crate::compaction::{
    make_dlq_filter, make_inflight_filter, make_pending_filter, CompactionCounters,
};
use crate::error::{QueueError, Result};
use crate::policy::{BacklogPolicy, NamespacePolicy};
use crate::task::{encode_key, now_millis, queue_prefix, Task};

pub struct DbConfig {
    pub sst_path: String,
    pub wal_path: String,
    pub block_cache_bytes: usize,
    pub write_buffer_bytes: usize,
    pub max_write_buffers: i32,
}

impl DbConfig {
    pub fn local() -> Self {
        Self {
            sst_path: "./data/rocksqueue".to_string(),
            wal_path: "./data/rocksqueue-wal".to_string(),
            block_cache_bytes: 256 * 1024 * 1024,
            write_buffer_bytes: 64 * 1024 * 1024,
            max_write_buffers: 4,
        }
    }

    pub fn production() -> Self {
        Self {
            sst_path: "/data/rocksqueue".to_string(),
            wal_path: "/wal/rocksqueue-wal".to_string(),
            block_cache_bytes: 4 * 1024 * 1024 * 1024,
            write_buffer_bytes: 64 * 1024 * 1024,
            max_write_buffers: 4,
        }
    }
}

/// CF naming: {tenant}__pending, {tenant}__inflight, {tenant}__dlq, __system__
fn cf_pending(tenant: &str) -> String {
    format!("{tenant}__pending")
}
fn cf_inflight(tenant: &str) -> String {
    format!("{tenant}__inflight")
}
fn cf_dlq(tenant: &str) -> String {
    format!("{tenant}__dlq")
}
const CF_SYSTEM: &str = "__system__";

pub struct TenantRegistry {
    pub db: Arc<DB>,
    seq: Arc<AtomicU64>,
    block_cache: Arc<Cache>,
    write_buffer_bytes: usize,
    max_write_buffers: i32,
    counters: Arc<CompactionCounters>,
    /// tenant_id → NamespacePolicy
    policies: DashMap<String, NamespacePolicy>,
}

impl TenantRegistry {
    pub fn open(cfg: &DbConfig, existing_tenants: Vec<(String, NamespacePolicy)>) -> Result<Self> {
        let block_cache = Cache::new_lru_cache(cfg.block_cache_bytes);
        let counters = CompactionCounters::new();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_wal_dir(&cfg.wal_path);
        db_opts.set_wal_bytes_per_sync(512 * 1024);
        db_opts.set_bytes_per_sync(1024 * 1024);
        db_opts.set_allow_concurrent_memtable_write(true);
        db_opts.set_enable_write_thread_adaptive_yield(true);
        db_opts.increase_parallelism(num_cpus::get() as i32);
        db_opts.set_max_background_jobs(num_cpus::get().min(8) as i32);
        db_opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords);

        // Build CF descriptors
        let mut cf_descs = vec![ColumnFamilyDescriptor::new(
            CF_SYSTEM,
            Self::system_cf_opts(),
        )];

        for (tenant, policy) in &existing_tenants {
            let retention = &policy.retention;
            cf_descs.push(ColumnFamilyDescriptor::new(
                cf_pending(tenant),
                Self::cf_options_with_filter(
                    &block_cache,
                    cfg.write_buffer_bytes,
                    cfg.max_write_buffers,
                    Some(make_pending_filter(
                        retention.pending_retention_secs,
                        Arc::clone(&counters),
                    )),
                    None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
                    None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
                ),
            ));
            cf_descs.push(ColumnFamilyDescriptor::new(
                cf_inflight(tenant),
                Self::cf_options_with_filter(
                    &block_cache,
                    cfg.write_buffer_bytes,
                    cfg.max_write_buffers,
                    None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
                    Some(make_inflight_filter(
                        retention.inflight_stale_secs,
                        Arc::clone(&counters),
                    )),
                    None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
                ),
            ));
            cf_descs.push(ColumnFamilyDescriptor::new(
                cf_dlq(tenant),
                Self::cf_options_with_filter(
                    &block_cache,
                    cfg.write_buffer_bytes,
                    cfg.max_write_buffers,
                    None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
                    None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
                    Some(make_dlq_filter(
                        retention.dlq_retention_secs,
                        Arc::clone(&counters),
                    )),
                ),
            ));
        }

        let db = DB::open_cf_descriptors(&db_opts, &cfg.sst_path, cf_descs)?;
        let db = Arc::new(db);

        let policies = DashMap::new();
        for (tenant, policy) in existing_tenants {
            policies.insert(tenant, policy);
        }

        Ok(Self {
            db,
            seq: Arc::new(AtomicU64::new(now_millis())),
            block_cache: Arc::new(block_cache),
            write_buffer_bytes: cfg.write_buffer_bytes,
            max_write_buffers: cfg.max_write_buffers,
            counters,
            policies,
        })
    }

    fn system_cf_opts() -> Options {
        let mut opts = Options::default();
        opts.set_write_buffer_size(16 * 1024 * 1024);
        opts
    }

    fn cf_options_with_filter<FP, FI, FD>(
        block_cache: &Cache,
        write_buffer_bytes: usize,
        max_write_buffers: i32,
        pending_filter: Option<FP>,
        inflight_filter: Option<FI>,
        dlq_filter: Option<FD>,
    ) -> Options
    where
        FP: Fn(u32, &[u8], &[u8]) -> CompactionDecision + Send + Sync + 'static,
        FI: Fn(u32, &[u8], &[u8]) -> CompactionDecision + Send + Sync + 'static,
        FD: Fn(u32, &[u8], &[u8]) -> CompactionDecision + Send + Sync + 'static,
    {
        let mut opts = Options::default();
        opts.set_write_buffer_size(write_buffer_bytes);
        opts.set_max_write_buffer_number(max_write_buffers);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_level_zero_slowdown_writes_trigger(12);
        opts.set_level_zero_stop_writes_trigger(20);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_max_bytes_for_level_base(512 * 1024 * 1024);

        // Compression: None for L0/L1, LZ4 for L2-L4, Zstd for L5-L6
        opts.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
        ]);

        let mut bbo = BlockBasedOptions::default();
        bbo.set_block_cache(block_cache);
        bbo.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&bbo);

        // No prefix extractor — we use IteratorMode::From with manual prefix checks.

        // Register whichever compaction filter was provided
        if let Some(f) = pending_filter {
            opts.set_compaction_filter("pending_filter", f);
        } else if let Some(f) = inflight_filter {
            opts.set_compaction_filter("inflight_filter", f);
        } else if let Some(f) = dlq_filter {
            opts.set_compaction_filter("dlq_filter", f);
        }

        opts
    }

    pub fn provision_tenant(&self, tenant: &str, policy: NamespacePolicy) -> Result<()> {
        let cache_ref = Cache::new_lru_cache(self.write_buffer_bytes);
        let retention = policy.retention.clone();

        let pending_opts = Self::cf_options_with_filter(
            &cache_ref,
            self.write_buffer_bytes,
            self.max_write_buffers,
            Some(make_pending_filter(
                retention.pending_retention_secs,
                Arc::clone(&self.counters),
            )),
            None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
            None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
        );
        let inflight_opts = Self::cf_options_with_filter(
            &cache_ref,
            self.write_buffer_bytes,
            self.max_write_buffers,
            None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
            Some(make_inflight_filter(
                retention.inflight_stale_secs,
                Arc::clone(&self.counters),
            )),
            None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
        );
        let dlq_opts = Self::cf_options_with_filter(
            &cache_ref,
            self.write_buffer_bytes,
            self.max_write_buffers,
            None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
            None::<fn(u32, &[u8], &[u8]) -> CompactionDecision>,
            Some(make_dlq_filter(
                retention.dlq_retention_secs,
                Arc::clone(&self.counters),
            )),
        );

        self.db.create_cf(&cf_pending(tenant), &pending_opts)?;
        self.db.create_cf(&cf_inflight(tenant), &inflight_opts)?;
        self.db.create_cf(&cf_dlq(tenant), &dlq_opts)?;

        self.policies.insert(tenant.to_string(), policy);
        info!("Provisioned tenant: {tenant}");
        Ok(())
    }

    pub fn drop_tenant(&self, tenant: &str) -> Result<()> {
        self.db.drop_cf(&cf_pending(tenant))?;
        self.db.drop_cf(&cf_inflight(tenant))?;
        self.db.drop_cf(&cf_dlq(tenant))?;
        self.policies.remove(tenant);
        info!("Dropped tenant: {tenant}");
        Ok(())
    }

    fn pending_cf(&self, tenant: &str) -> Result<Arc<rocksdb::BoundColumnFamily>> {
        self.db
            .cf_handle(&cf_pending(tenant))
            .ok_or_else(|| QueueError::ColumnFamilyMissing(cf_pending(tenant)))
    }

    fn inflight_cf(&self, tenant: &str) -> Result<Arc<rocksdb::BoundColumnFamily>> {
        self.db
            .cf_handle(&cf_inflight(tenant))
            .ok_or_else(|| QueueError::ColumnFamilyMissing(cf_inflight(tenant)))
    }

    fn dlq_cf(&self, tenant: &str) -> Result<Arc<rocksdb::BoundColumnFamily>> {
        self.db
            .cf_handle(&cf_dlq(tenant))
            .ok_or_else(|| QueueError::ColumnFamilyMissing(cf_dlq(tenant)))
    }

    fn hot_write_opts() -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.set_sync(false);
        opts.disable_wal(false);
        opts
    }

    fn admin_write_opts() -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        opts.disable_wal(false);
        opts
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    pub fn enqueue(
        &self,
        tenant: &str,
        queue: &str,
        payload: Vec<u8>,
        policy: &NamespacePolicy,
    ) -> Result<String> {
        let ids = self.enqueue_batch_sync(tenant, queue, vec![payload], policy)?;
        Ok(ids.into_iter().next().unwrap())
    }

    pub async fn enqueue_batch(
        &self,
        tenant: &str,
        queue: &str,
        payloads: Vec<Vec<u8>>,
        policy: &NamespacePolicy,
    ) -> Result<Vec<String>> {
        self.enqueue_batch_sync(tenant, queue, payloads, policy)
    }

    fn enqueue_batch_sync(
        &self,
        tenant: &str,
        queue: &str,
        payloads: Vec<Vec<u8>>,
        policy: &NamespacePolicy,
    ) -> Result<Vec<String>> {
        let incoming = payloads.len();
        self.enforce_quota(tenant, queue, policy, incoming)?;

        let pending_cf = self.pending_cf(tenant)?;
        let mut batch = WriteBatch::default();
        let mut ids = Vec::with_capacity(incoming);
        let now = now_millis();

        for payload in payloads {
            let id = uuid::Uuid::new_v4().to_string();
            let seq = self.next_seq();
            let task = Task {
                id: id.clone(),
                queue: queue.to_string(),
                payload,
                enqueued_at: now,
                attempts: 0,
                deadline: 0,
            };
            let key = encode_key(queue, seq);
            let value = task.serialize()?;
            batch.put_cf(&pending_cf, &key, &value);
            ids.push(id);
        }

        self.db.write_opt(batch, &Self::hot_write_opts())?;
        Ok(ids)
    }

    fn enforce_quota(
        &self,
        tenant: &str,
        queue: &str,
        policy: &NamespacePolicy,
        incoming: usize,
    ) -> Result<()> {
        let quota = match policy.backlog_quota {
            None => return Ok(()),
            Some(q) => q,
        };

        let (pending, inflight, _) = self.depth(tenant, queue)?;
        let current = pending + inflight;

        match &policy.backlog_policy {
            BacklogPolicy::Reject => {
                if current + incoming > quota {
                    return Err(QueueError::BacklogQuotaExceeded {
                        tenant: tenant.to_string(),
                        queue: queue.to_string(),
                        current,
                        quota,
                        incoming,
                    });
                }
            }
            BacklogPolicy::Block { timeout_ms } => {
                let deadline = now_millis() + timeout_ms;
                loop {
                    let (p, i, _) = self.depth(tenant, queue)?;
                    let cur = p + i;
                    if cur + incoming <= quota {
                        break;
                    }
                    if now_millis() >= deadline {
                        return Err(QueueError::BacklogQuotaExceeded {
                            tenant: tenant.to_string(),
                            queue: queue.to_string(),
                            current: cur,
                            quota,
                            incoming,
                        });
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
            BacklogPolicy::EvictOldest => {
                if current + incoming > quota {
                    let to_evict = (current + incoming).saturating_sub(quota);
                    self.evict_oldest_pending(tenant, queue, to_evict)?;
                }
            }
        }
        Ok(())
    }

    fn evict_oldest_pending(&self, tenant: &str, queue: &str, count: usize) -> Result<()> {
        let pending_cf = self.pending_cf(tenant)?;
        let prefix = queue_prefix(queue);
        let iter = self.db.iterator_cf(
            &pending_cf,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );
        let mut batch = WriteBatch::default();
        let mut evicted = 0;

        for item in iter {
            if evicted >= count {
                break;
            }
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete_cf(&pending_cf, &key);
            evicted += 1;
        }

        if evicted > 0 {
            self.db.write_opt(batch, &Self::hot_write_opts())?;
            debug!("Evicted {evicted} oldest tasks from {tenant}/{queue}");
        }
        Ok(())
    }

    pub fn dequeue(&self, tenant: &str, queue: &str, limit: usize) -> Result<Vec<(Vec<u8>, Task)>> {
        let pending_cf = self.pending_cf(tenant)?;
        let inflight_cf = self.inflight_cf(tenant)?;
        let prefix = queue_prefix(queue);
        let iter = self.db.iterator_cf(
            &pending_cf,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        let mut results = Vec::with_capacity(limit);
        let mut batch = WriteBatch::default();
        let deadline = now_millis() + 60_000; // 60s default visibility

        for item in iter.take(limit) {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let mut task = Task::deserialize(&value)?;
            task.attempts += 1;
            task.deadline = deadline;
            let updated_value = task.serialize()?;

            batch.delete_cf(&pending_cf, &key);
            batch.put_cf(&inflight_cf, &key, &updated_value);
            results.push((key.to_vec(), task));
        }

        if !results.is_empty() {
            self.db.write_opt(batch, &Self::hot_write_opts())?;
        }
        Ok(results)
    }

    pub fn ack(&self, tenant: &str, key: &[u8]) -> Result<()> {
        let inflight_cf = self.inflight_cf(tenant)?;
        self.db
            .delete_cf_opt(&inflight_cf, key, &Self::hot_write_opts())?;
        Ok(())
    }

    pub fn nack(&self, tenant: &str, key: &[u8]) -> Result<()> {
        let pending_cf = self.pending_cf(tenant)?;
        let inflight_cf = self.inflight_cf(tenant)?;
        let dlq_cf = self.dlq_cf(tenant)?;

        let max_attempts: u32 = 5;

        if let Some(value) = self.db.get_cf(&inflight_cf, key)? {
            let task = Task::deserialize(&value)?;
            let mut batch = WriteBatch::default();
            batch.delete_cf(&inflight_cf, key);

            if task.attempts >= max_attempts {
                batch.put_cf(&dlq_cf, key, &value);
                warn!(
                    "Task {} moved to DLQ after {} attempts",
                    task.id, task.attempts
                );
            } else {
                let mut retry_task = task;
                retry_task.deadline = 0;
                let seq = self.next_seq();
                let new_key = encode_key(&retry_task.queue, seq);
                let new_value = retry_task.serialize()?;
                batch.put_cf(&pending_cf, &new_key, &new_value);
            }
            self.db.write_opt(batch, &Self::hot_write_opts())?;
        }
        Ok(())
    }

    pub fn cumulative_ack(&self, tenant: &str, queue: &str, up_to_seq: u64) -> Result<usize> {
        let inflight_cf = self.inflight_cf(tenant)?;
        let prefix = queue_prefix(queue);
        let iter = self.db.iterator_cf(
            &inflight_cf,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        let mut batch = WriteBatch::default();
        let mut count = 0;

        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            // Extract seq from last 8 bytes
            if key.len() >= 8 {
                let seq_bytes: [u8; 8] = key[key.len() - 8..].try_into().unwrap_or([0u8; 8]);
                let seq = u64::from_be_bytes(seq_bytes);
                if seq <= up_to_seq {
                    batch.delete_cf(&inflight_cf, &key);
                    count += 1;
                }
            }
        }

        if count > 0 {
            self.db.write_opt(batch, &Self::admin_write_opts())?;
        }
        Ok(count)
    }

    /// Returns (pending, inflight, dlq) counts for the queue.
    pub fn depth(&self, tenant: &str, queue: &str) -> Result<(usize, usize, usize)> {
        let pending = self.count_prefix(self.pending_cf(tenant)?, queue)?;
        let inflight = self.count_prefix(self.inflight_cf(tenant)?, queue)?;
        let dlq = self.count_prefix(self.dlq_cf(tenant)?, queue)?;
        Ok((pending, inflight, dlq))
    }

    fn count_prefix(
        &self,
        cf: Arc<rocksdb::BoundColumnFamily>,
        queue: &str,
    ) -> Result<usize> {
        let prefix = queue_prefix(queue);
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );
        let mut count = 0;
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    pub fn compact_tenant(&self, tenant: &str) -> Result<()> {
        let pending_cf = self.pending_cf(tenant)?;
        let inflight_cf = self.inflight_cf(tenant)?;
        let dlq_cf = self.dlq_cf(tenant)?;

        self.db
            .compact_range_cf(&pending_cf, None::<&[u8]>, None::<&[u8]>);
        self.db
            .compact_range_cf(&inflight_cf, None::<&[u8]>, None::<&[u8]>);
        self.db
            .compact_range_cf(&dlq_cf, None::<&[u8]>, None::<&[u8]>);
        info!("Manual compaction complete for tenant: {tenant}");
        Ok(())
    }

    pub fn purge_dlq(&self, tenant: &str) -> Result<usize> {
        let dlq_cf = self.dlq_cf(tenant)?;
        let iter = self.db.iterator_cf(&dlq_cf, rocksdb::IteratorMode::Start);
        let mut batch = WriteBatch::default();
        let mut count = 0;

        for item in iter {
            let (key, _) = item?;
            batch.delete_cf(&dlq_cf, &key);
            count += 1;
        }

        if count > 0 {
            self.db.write_opt(batch, &Self::admin_write_opts())?;
        }
        info!("Purged {count} tasks from DLQ for tenant: {tenant}");
        Ok(count)
    }

    pub fn replay_dlq(&self, tenant: &str) -> Result<usize> {
        let pending_cf = self.pending_cf(tenant)?;
        let dlq_cf = self.dlq_cf(tenant)?;
        let iter = self.db.iterator_cf(&dlq_cf, rocksdb::IteratorMode::Start);

        let mut batch = WriteBatch::default();
        let mut count = 0;

        for item in iter {
            let (key, value) = item?;
            let mut task = Task::deserialize(&value)?;
            task.attempts = 0;
            task.deadline = 0;
            let seq = self.next_seq();
            let new_key = encode_key(&task.queue, seq);
            let new_value = task.serialize()?;
            batch.put_cf(&pending_cf, &new_key, &new_value);
            batch.delete_cf(&dlq_cf, &key);
            count += 1;
        }

        if count > 0 {
            self.db.write_opt(batch, &Self::admin_write_opts())?;
        }
        info!("Replayed {count} tasks from DLQ for tenant: {tenant}");
        Ok(count)
    }

    pub fn ping(&self) -> Result<()> {
        let _ = self.db.property_value("rocksdb.stats")?;
        Ok(())
    }

    pub fn list_tenants(&self) -> Vec<String> {
        self.policies.iter().map(|e| e.key().clone()).collect()
    }

    pub fn get_policy(&self, tenant: &str) -> Option<NamespacePolicy> {
        self.policies.get(tenant).map(|p| p.clone())
    }

    pub fn compaction_counters(&self) -> &CompactionCounters {
        &self.counters
    }
}
