use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, CompactionDecision, DBCompressionType,
    Options, PrefixRange, ReadOptions, SliceTransform, WriteBatch, WriteOptions, DB,
};
use tracing::{debug, info, warn};

use crate::compaction::{
    make_dlq_filter, make_inflight_filter, make_pending_filter, CompactionCounters,
};
use crate::error::{QueueError, Result};
use crate::policy::{BacklogPolicy, NamespacePolicy};
use crate::task::{decode_key_seq, encode_key, now_millis, queue_prefix, Task};

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
const DEFAULT_VISIBILITY_TIMEOUT_MS: u64 = 60_000;

pub struct TenantRegistry {
    pub db: Arc<DB>,
    seq: Arc<AtomicU64>,
    block_cache: Arc<Cache>,
    write_buffer_bytes: usize,
    max_write_buffers: i32,
    counters: Arc<CompactionCounters>,
    /// tenant_id → NamespacePolicy
    policies: DashMap<String, NamespacePolicy>,
    /// "{tenant}/{queue}" → per-queue mutex serializing quota check + enqueue write
    queue_locks: DashMap<String, Arc<Mutex<()>>>,
}

const TENANT_META_PREFIX: &[u8] = b"tenant:";

impl TenantRegistry {
    fn queue_prefix_transform(key: &[u8]) -> &[u8] {
        key.iter()
            .position(|b| *b == 0x00)
            .map(|idx| &key[..=idx])
            .unwrap_or(key)
    }

    fn queue_prefix_in_domain(key: &[u8]) -> bool {
        key.contains(&0x00)
    }

    fn queue_read_opts(prefix: &[u8]) -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_iterate_range(PrefixRange(prefix.to_vec()));
        opts.set_prefix_same_as_start(true);
        opts
    }

    /// Read all tenant policies persisted in __system__ CF without acquiring a long-lived handle.
    /// Uses a temporary DB open that drops (and releases the lock) before the real open.
    fn load_persisted_tenants(cfg: &DbConfig) -> Result<Vec<(String, NamespacePolicy)>> {
        let all_cfs = match DB::list_cf(&Options::default(), &cfg.sst_path) {
            Ok(cfs) => cfs,
            Err(_) => return Ok(vec![]), // fresh DB path does not exist yet
        };
        if !all_cfs.iter().any(|cf| cf == CF_SYSTEM) {
            return Ok(vec![]);
        }

        let mut pre_opts = Options::default();
        pre_opts.create_if_missing(false);
        pre_opts.create_missing_column_families(false);
        pre_opts.set_wal_dir(&cfg.wal_path);

        let cf_descs: Vec<ColumnFamilyDescriptor> = all_cfs
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name.as_str(), Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&pre_opts, &cfg.sst_path, cf_descs)?;
        let system_cf = db
            .cf_handle(CF_SYSTEM)
            .ok_or_else(|| QueueError::ColumnFamilyMissing(CF_SYSTEM.to_string()))?;

        let iter = db.iterator_cf(
            &system_cf,
            rocksdb::IteratorMode::From(TENANT_META_PREFIX, rocksdb::Direction::Forward),
        );

        let mut tenants = vec![];
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(TENANT_META_PREFIX) {
                break;
            }
            let tenant_id = String::from_utf8_lossy(&key[TENANT_META_PREFIX.len()..]).into_owned();
            let policy: NamespacePolicy = bincode::deserialize(&value)?;
            tenants.push((tenant_id, policy));
        }
        Ok(tenants) // db drops here, releasing the file lock
    }

    pub fn open(cfg: &DbConfig) -> Result<Self> {
        let existing_tenants = Self::load_persisted_tenants(cfg)?;
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
            queue_locks: DashMap::new(),
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
        opts.set_prefix_extractor(SliceTransform::create(
            "queue_prefix",
            Self::queue_prefix_transform,
            Some(Self::queue_prefix_in_domain),
        ));
        opts.set_memtable_prefix_bloom_ratio(0.1);
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
        let retention = policy.retention.clone();

        let pending_opts = Self::cf_options_with_filter(
            &self.block_cache,
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
            &self.block_cache,
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
            &self.block_cache,
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

        // Persist policy to __system__ so it survives restarts
        let system_cf = self
            .db
            .cf_handle(CF_SYSTEM)
            .ok_or_else(|| QueueError::ColumnFamilyMissing(CF_SYSTEM.to_string()))?;
        let meta_key = [TENANT_META_PREFIX, tenant.as_bytes()].concat();
        let meta_value = bincode::serialize(&policy)?;
        self.db.put_cf_opt(
            &system_cf,
            &meta_key,
            &meta_value,
            &Self::admin_write_opts(),
        )?;

        self.policies.insert(tenant.to_string(), policy);
        info!("Provisioned tenant: {tenant}");
        Ok(())
    }

    pub fn drop_tenant(&self, tenant: &str) -> Result<()> {
        self.db.drop_cf(&cf_pending(tenant))?;
        self.db.drop_cf(&cf_inflight(tenant))?;
        self.db.drop_cf(&cf_dlq(tenant))?;

        // Remove persisted metadata from __system__
        let system_cf = self
            .db
            .cf_handle(CF_SYSTEM)
            .ok_or_else(|| QueueError::ColumnFamilyMissing(CF_SYSTEM.to_string()))?;
        let meta_key = [TENANT_META_PREFIX, tenant.as_bytes()].concat();
        self.db
            .delete_cf_opt(&system_cf, &meta_key, &Self::admin_write_opts())?;

        self.policies.remove(tenant);
        // Remove any queue locks for this tenant
        self.queue_locks
            .retain(|k, _| !k.starts_with(&format!("{tenant}/")));
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

    fn queue_lock(&self, tenant: &str, queue: &str) -> Arc<Mutex<()>> {
        self.queue_locks
            .entry(format!("{tenant}/{queue}"))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn enqueue_batch_sync(
        &self,
        tenant: &str,
        queue: &str,
        payloads: Vec<Vec<u8>>,
        policy: &NamespacePolicy,
    ) -> Result<Vec<String>> {
        let incoming = payloads.len();

        // Hold the per-queue lock across the entire quota-check + write to eliminate the
        // TOCTOU window that lets concurrent producers both observe capacity and both write.
        let lock = self.queue_lock(tenant, queue);
        let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());

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
        let iter = self.db.iterator_cf_opt(
            &pending_cf,
            Self::queue_read_opts(&prefix),
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );
        let mut batch = WriteBatch::default();
        let mut evicted = 0;

        for item in iter {
            if evicted >= count {
                break;
            }
            let (key, _) = item?;
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
        self.dequeue_batch(tenant, queue, limit, DEFAULT_VISIBILITY_TIMEOUT_MS)
    }

    pub fn dequeue_batch(
        &self,
        tenant: &str,
        queue: &str,
        limit: usize,
        visibility_timeout_ms: u64,
    ) -> Result<Vec<(Vec<u8>, Task)>> {
        if limit == 0 {
            return Ok(vec![]);
        }

        let pending_cf = self.pending_cf(tenant)?;
        let inflight_cf = self.inflight_cf(tenant)?;
        let dlq_cf = self.dlq_cf(tenant)?;
        let prefix = queue_prefix(queue);
        let iter = self.db.iterator_cf_opt(
            &pending_cf,
            Self::queue_read_opts(&prefix),
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        let mut results = Vec::with_capacity(limit);
        let mut batch = WriteBatch::default();
        let mut poisoned = 0usize;
        let deadline = now_millis().saturating_add(visibility_timeout_ms);

        for item in iter.take(limit) {
            let (key, value) = item?;
            let mut task = match Task::deserialize(&value) {
                Ok(task) => task,
                Err(e) => {
                    warn!("Corrupt pending record in {tenant}/{queue}, routing to DLQ: {e}");
                    batch.delete_cf(&pending_cf, &key);
                    batch.put_cf(&dlq_cf, &key, &Task::poison(queue, &value).serialize()?);
                    poisoned += 1;
                    continue;
                }
            };
            task.attempts += 1;
            task.deadline = deadline;
            let updated_value = task.serialize()?;

            batch.delete_cf(&pending_cf, &key);
            batch.put_cf(&inflight_cf, &key, &updated_value);
            results.push((key.to_vec(), task));
        }

        if !results.is_empty() || poisoned > 0 {
            self.db.write_opt(batch, &Self::hot_write_opts())?;
        }
        Ok(results)
    }

    pub fn dequeue_with_max_inflight(
        &self,
        tenant: &str,
        queue: &str,
        limit: usize,
        visibility_timeout_ms: u64,
        max_inflight: usize,
    ) -> Result<Vec<(Vec<u8>, Task)>> {
        if max_inflight == 0 {
            return Ok(vec![]);
        }

        let inflight = self.count_prefix(self.inflight_cf(tenant)?, queue)?;
        if inflight >= max_inflight {
            return Ok(vec![]);
        }

        self.dequeue_batch(
            tenant,
            queue,
            limit.min(max_inflight - inflight),
            visibility_timeout_ms,
        )
    }

    pub fn dequeue_lease_batch_experimental(
        &self,
        tenant: &str,
        queue: &str,
        limit: usize,
        visibility_timeout_ms: u64,
    ) -> Result<Vec<(Vec<u8>, Task)>> {
        if limit == 0 {
            return Ok(vec![]);
        }

        let pending_cf = self.pending_cf(tenant)?;
        let inflight_cf = self.inflight_cf(tenant)?;
        let prefix = queue_prefix(queue);
        let iter = self.db.iterator_cf_opt(
            &pending_cf,
            Self::queue_read_opts(&prefix),
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        let mut results = Vec::with_capacity(limit);
        let mut batch = WriteBatch::default();
        let deadline = now_millis().saturating_add(visibility_timeout_ms);

        for item in iter {
            let (key, value) = item?;
            if self.db.get_cf(&inflight_cf, &key)?.is_some() {
                continue;
            }

            let mut task = Task::deserialize(&value)?;
            task.attempts += 1;
            task.deadline = deadline;

            let mut lease_value = Vec::with_capacity(12);
            lease_value.extend_from_slice(&task.attempts.to_be_bytes());
            lease_value.extend_from_slice(&deadline.to_be_bytes());
            batch.put_cf(&inflight_cf, &key, &lease_value);
            results.push((key.to_vec(), task));

            if results.len() >= limit {
                break;
            }
        }

        if !results.is_empty() {
            self.db.write_opt(batch, &Self::hot_write_opts())?;
        }
        Ok(results)
    }

    pub fn ack(&self, tenant: &str, key: &[u8]) -> Result<()> {
        self.ack_batch(tenant, std::iter::once(key))?;
        Ok(())
    }

    pub fn ack_batch<'a, I>(&self, tenant: &str, keys: I) -> Result<usize>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let inflight_cf = self.inflight_cf(tenant)?;
        let mut batch = WriteBatch::default();
        let mut count = 0usize;

        for key in keys {
            batch.delete_cf(&inflight_cf, key);
            count += 1;
        }

        if count > 0 {
            self.db.write_opt(batch, &Self::hot_write_opts())?;
        }
        Ok(count)
    }

    pub fn ack_lease_batch_experimental<'a, I>(&self, tenant: &str, keys: I) -> Result<usize>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let pending_cf = self.pending_cf(tenant)?;
        let inflight_cf = self.inflight_cf(tenant)?;
        let mut batch = WriteBatch::default();
        let mut count = 0usize;

        for key in keys {
            batch.delete_cf(&pending_cf, key);
            batch.delete_cf(&inflight_cf, key);
            count += 1;
        }

        if count > 0 {
            self.db.write_opt(batch, &Self::hot_write_opts())?;
        }
        Ok(count)
    }

    pub fn nack(&self, tenant: &str, key: &[u8]) -> Result<()> {
        let pending_cf = self.pending_cf(tenant)?;
        let inflight_cf = self.inflight_cf(tenant)?;
        let dlq_cf = self.dlq_cf(tenant)?;

        let max_attempts: u32 = 5;

        if let Some(value) = self.db.get_cf(&inflight_cf, key)? {
            let task = match Task::deserialize(&value) {
                Ok(task) => task,
                Err(e) => {
                    warn!("Corrupt inflight record in nack for {tenant}, routing to DLQ: {e}");
                    let queue = String::from_utf8_lossy(
                        &key[..key.iter().position(|b| *b == 0x00).unwrap_or(key.len())],
                    );
                    let mut batch = WriteBatch::default();
                    batch.delete_cf(&inflight_cf, key);
                    batch.put_cf(&dlq_cf, key, &Task::poison(&queue, &value).serialize()?);
                    self.db.write_opt(batch, &Self::hot_write_opts())?;
                    return Ok(());
                }
            };
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
        let iter = self.db.iterator_cf_opt(
            &inflight_cf,
            Self::queue_read_opts(&prefix),
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        let mut batch = WriteBatch::default();
        let mut count = 0;

        for item in iter {
            let (key, _) = item?;
            if let Some(seq) = decode_key_seq(&key) {
                if seq > up_to_seq {
                    break;
                }
                batch.delete_cf(&inflight_cf, &key);
                count += 1;
            }
        }

        if count > 0 {
            self.db.write_opt(batch, &Self::hot_write_opts())?;
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

    fn count_prefix(&self, cf: Arc<rocksdb::BoundColumnFamily>, queue: &str) -> Result<usize> {
        let prefix = queue_prefix(queue);
        let iter = self.db.iterator_cf_opt(
            &cf,
            Self::queue_read_opts(&prefix),
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );
        let mut count = 0;
        for item in iter {
            item?;
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

    /// Update quota/retention policy for an existing tenant (persisted immediately).
    pub fn update_namespace_policy(&self, tenant: &str, policy: NamespacePolicy) -> Result<()> {
        if !self.policies.contains_key(tenant) {
            return Err(QueueError::QueueNotFound(format!(
                "tenant not found: {tenant}"
            )));
        }
        let system_cf = self
            .db
            .cf_handle(CF_SYSTEM)
            .ok_or_else(|| QueueError::ColumnFamilyMissing(CF_SYSTEM.to_string()))?;
        let meta_key = [TENANT_META_PREFIX, tenant.as_bytes()].concat();
        let meta_value = bincode::serialize(&policy)?;
        self.db.put_cf_opt(
            &system_cf,
            &meta_key,
            &meta_value,
            &Self::admin_write_opts(),
        )?;
        self.policies.insert(tenant.to_string(), policy);
        Ok(())
    }

    /// Create a RocksDB hard-link checkpoint at `path`.
    /// `allowed_base` is the configured checkpoint directory; `path` must be under it.
    pub fn create_checkpoint(&self, path: &Path, allowed_base: &Path) -> Result<()> {
        // Reject paths with parent-directory traversal components
        if path
            .components()
            .any(|c| c == std::path::Component::ParentDir)
        {
            return Err(QueueError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "checkpoint path must not contain '..'",
            )));
        }
        if !path.starts_with(allowed_base) {
            return Err(QueueError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "checkpoint path must be under configured checkpoint directory",
            )));
        }
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db)?;
        checkpoint.create_checkpoint(path)?;
        info!("Checkpoint created at {}", path.display());
        Ok(())
    }

    pub fn compaction_counters(&self) -> &CompactionCounters {
        &self.counters
    }
}
