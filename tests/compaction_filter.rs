use rocksqueue::policy::{NamespacePolicy, RetentionPolicy};
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
    TenantRegistry::open(&cfg, vec![]).expect("failed to open registry")
}

#[test]
fn test_dlq_purge() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);

    let policy = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy.clone()).unwrap();

    // Drive tasks to DLQ
    registry
        .enqueue("acme", "default", b"task1".to_vec(), &policy)
        .unwrap();
    registry
        .enqueue("acme", "default", b"task2".to_vec(), &policy)
        .unwrap();

    for _ in 0..5 {
        let tasks = registry.dequeue("acme", "default", 2).unwrap();
        for (key, _) in &tasks {
            registry.nack("acme", key).unwrap();
        }
    }

    let (_, _, dlq) = registry.depth("acme", "default").unwrap();
    assert!(dlq > 0);

    let purged = registry.purge_dlq("acme").unwrap();
    assert!(purged > 0);

    let (_, _, dlq_after) = registry.depth("acme", "default").unwrap();
    assert_eq!(dlq_after, 0);
}

#[test]
fn test_compaction_counters_exposed() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);

    let policy = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy.clone()).unwrap();

    // Trigger compact (no-op on empty CF, but verifies the API works)
    registry.compact_tenant("acme").unwrap();

    let counters = registry.compaction_counters();
    // Counters exist and can be read
    let _ = counters.kept.load(std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn test_short_retention_policy() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);

    let mut policy = NamespacePolicy::standard("acme");
    policy.retention = RetentionPolicy {
        pending_retention_secs: 1, // very short
        dlq_retention_secs: 1,
        inflight_stale_secs: 1,
    };
    registry.provision_tenant("acme", policy.clone()).unwrap();

    // Enqueue and dequeue — compaction filter is background but at least the API works
    registry
        .enqueue("acme", "default", b"temp_task".to_vec(), &policy)
        .unwrap();

    let (pending, _, _) = registry.depth("acme", "default").unwrap();
    assert_eq!(pending, 1);
}
