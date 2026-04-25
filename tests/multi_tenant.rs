use rocksqueue::policy::NamespacePolicy;
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
fn test_tenant_isolation() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);

    let policy_a = NamespacePolicy::standard("acme");
    let policy_b = NamespacePolicy::standard("globex");
    registry.provision_tenant("acme", policy_a.clone()).unwrap();
    registry.provision_tenant("globex", policy_b.clone()).unwrap();

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
