use rocksqueue::policy::{BacklogPolicy, NamespacePolicy};
use rocksqueue::tenant::{DbConfig, TenantRegistry};
use tempfile::TempDir;

fn make_registry(tmp: &TempDir) -> TenantRegistry {
    let cfg = DbConfig {
        sst_path: tmp.path().join("sst").to_string_lossy().to_string(),
        wal_path: tmp.path().join("wal").to_string_lossy().to_string(),
        block_cache_bytes: 16 * 1024 * 1024,
        write_buffer_bytes: 4 * 1024 * 1024,
        max_write_buffers: 2,
    };
    TenantRegistry::open(&cfg).expect("failed to open registry")
}

#[test]
fn test_tenant_isolation() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);

    let policy_a = NamespacePolicy::standard("acme");
    let policy_b = NamespacePolicy::standard("globex");
    registry.provision_tenant("acme", policy_a.clone()).unwrap();
    registry
        .provision_tenant("globex", policy_b.clone())
        .unwrap();

    registry
        .enqueue("acme", "default", b"acme_task".to_vec(), &policy_a)
        .unwrap();
    registry
        .enqueue("globex", "default", b"globex_task".to_vec(), &policy_b)
        .unwrap();

    let acme_tasks = registry.dequeue("acme", "default", 10).unwrap();
    let globex_tasks = registry.dequeue("globex", "default", 10).unwrap();

    assert_eq!(acme_tasks.len(), 1);
    assert_eq!(acme_tasks[0].1.payload, b"acme_task");

    assert_eq!(globex_tasks.len(), 1);
    assert_eq!(globex_tasks[0].1.payload, b"globex_task");
}

#[test]
fn test_drop_tenant() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);

    let policy = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy.clone()).unwrap();

    registry
        .enqueue("acme", "default", b"hello".to_vec(), &policy)
        .unwrap();

    registry.drop_tenant("acme").unwrap();

    // Re-provision should work cleanly
    let policy2 = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy2.clone()).unwrap();

    let (pending, _, _) = registry.depth("acme", "default").unwrap();
    assert_eq!(pending, 0); // Clean slate
}

#[test]
fn test_dlq_after_max_attempts() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);
    let policy = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy.clone()).unwrap();

    registry
        .enqueue("acme", "default", b"bad_task".to_vec(), &policy)
        .unwrap();

    // Nack 5 times to exhaust attempts
    for _ in 0..5 {
        let tasks = registry.dequeue("acme", "default", 1).unwrap();
        if tasks.is_empty() {
            break;
        }
        let (key, _) = &tasks[0];
        registry.nack("acme", key).unwrap();
    }

    let (_, _, dlq) = registry.depth("acme", "default").unwrap();
    assert_eq!(dlq, 1);
}

#[test]
fn test_replay_dlq() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);
    let policy = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy.clone()).unwrap();

    registry
        .enqueue("acme", "default", b"bad_task".to_vec(), &policy)
        .unwrap();

    // Drive to DLQ
    for _ in 0..5 {
        let tasks = registry.dequeue("acme", "default", 1).unwrap();
        if tasks.is_empty() {
            break;
        }
        registry.nack("acme", &tasks[0].0).unwrap();
    }

    let (_, _, dlq_before) = registry.depth("acme", "default").unwrap();
    assert_eq!(dlq_before, 1);

    let replayed = registry.replay_dlq("acme").unwrap();
    assert_eq!(replayed, 1);

    let (pending, _, dlq_after) = registry.depth("acme", "default").unwrap();
    assert_eq!(dlq_after, 0);
    assert_eq!(pending, 1);
}

#[test]
fn test_restart_recovery() {
    let tmp = TempDir::new().unwrap();
    let cfg = DbConfig {
        sst_path: tmp.path().join("sst").to_string_lossy().to_string(),
        wal_path: tmp.path().join("wal").to_string_lossy().to_string(),
        block_cache_bytes: 16 * 1024 * 1024,
        write_buffer_bytes: 4 * 1024 * 1024,
        max_write_buffers: 2,
    };

    // First run: provision tenant and enqueue tasks
    {
        let registry = TenantRegistry::open(&cfg).expect("first open");
        let policy = NamespacePolicy::standard("acme");
        registry.provision_tenant("acme", policy.clone()).unwrap();
        registry
            .enqueue("acme", "default", b"task1".to_vec(), &policy)
            .unwrap();
        registry
            .enqueue("acme", "default", b"task2".to_vec(), &policy)
            .unwrap();
        registry
            .enqueue("acme", "default", b"task3".to_vec(), &policy)
            .unwrap();
        // registry drops here — DB closes
    }

    // Second run: reopen and verify tenant and queue depth survived
    {
        let registry = TenantRegistry::open(&cfg).expect("second open after restart");

        let tenants = registry.list_tenants();
        assert!(
            tenants.contains(&"acme".to_string()),
            "tenant must survive restart"
        );

        let (pending, _, _) = registry.depth("acme", "default").unwrap();
        assert_eq!(pending, 3, "all 3 pending tasks must survive restart");

        let policy = registry
            .get_policy("acme")
            .expect("policy must survive restart");
        assert_eq!(policy.tenant_id, "acme");
    }
}

#[test]
fn test_namespace_policy_update_persisted() {
    let tmp = TempDir::new().unwrap();
    let cfg = DbConfig {
        sst_path: tmp.path().join("sst").to_string_lossy().to_string(),
        wal_path: tmp.path().join("wal").to_string_lossy().to_string(),
        block_cache_bytes: 16 * 1024 * 1024,
        write_buffer_bytes: 4 * 1024 * 1024,
        max_write_buffers: 2,
    };

    {
        let registry = TenantRegistry::open(&cfg).expect("first open");
        let policy = NamespacePolicy::standard("acme");
        registry.provision_tenant("acme", policy).unwrap();

        // Update quota to a specific value
        let mut updated = NamespacePolicy::standard("acme");
        updated.backlog_quota = Some(42);
        updated.backlog_policy = BacklogPolicy::EvictOldest;
        registry.update_namespace_policy("acme", updated).unwrap();

        // Readable immediately
        let p = registry.get_policy("acme").unwrap();
        assert_eq!(p.backlog_quota, Some(42));
    }

    // Verify it survives a restart
    {
        let registry = TenantRegistry::open(&cfg).expect("second open");
        let p = registry
            .get_policy("acme")
            .expect("policy must survive restart");
        assert_eq!(
            p.backlog_quota,
            Some(42),
            "updated quota must persist across restarts"
        );
        assert!(matches!(p.backlog_policy, BacklogPolicy::EvictOldest));
    }

    // Updating a non-existent tenant returns an error
    {
        let registry = TenantRegistry::open(&cfg).expect("third open");
        let result = registry.update_namespace_policy("ghost", NamespacePolicy::standard("ghost"));
        assert!(result.is_err(), "updating unknown tenant must fail");
    }
}

/// S2 regression: tenants provisioned at runtime must share the registry's
/// block cache (block_cache_bytes), not get an isolated per-tenant cache sized
/// at write_buffer_bytes. We assert the RocksDB block-cache-capacity reported by
/// a provisioned tenant's CFs equals the shared cache size. Before the fix this
/// reported write_buffer_bytes (4 MiB) instead of block_cache_bytes (16 MiB).
#[test]
fn test_provisioned_tenant_shares_block_cache() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp); // 16 MiB cache, 4 MiB write buffer
    const SHARED_CACHE_BYTES: u64 = 16 * 1024 * 1024;
    const WRITE_BUFFER_BYTES: u64 = 4 * 1024 * 1024;

    registry
        .provision_tenant("acme", NamespacePolicy::standard("acme"))
        .unwrap();

    for cf_name in ["acme__pending", "acme__inflight", "acme__dlq"] {
        let cf = registry.db.cf_handle(cf_name).expect("cf must exist");
        let cap = registry
            .db
            .property_int_value_cf(&cf, "rocksdb.block-cache-capacity")
            .expect("property read ok")
            .expect("capacity present");
        assert_eq!(
            cap, SHARED_CACHE_BYTES,
            "{cf_name} must use the shared block cache, got {cap} bytes"
        );
        assert_ne!(
            cap, WRITE_BUFFER_BYTES,
            "{cf_name} must not use an isolated per-tenant cache"
        );
    }
}
