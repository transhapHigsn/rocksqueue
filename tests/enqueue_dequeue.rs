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
fn test_enqueue_and_dequeue() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);
    let policy = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy.clone()).unwrap();

    let id = registry
        .enqueue("acme", "default", b"hello".to_vec(), &policy)
        .unwrap();
    assert!(!id.is_empty());

    let tasks = registry.dequeue("acme", "default", 10).unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].1.payload, b"hello");
}

#[test]
fn test_ack_removes_from_inflight() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);
    let policy = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy.clone()).unwrap();

    registry
        .enqueue("acme", "default", b"task1".to_vec(), &policy)
        .unwrap();

    let tasks = registry.dequeue("acme", "default", 1).unwrap();
    let (key, _) = &tasks[0];

    let (_, inflight_before, _) = registry.depth("acme", "default").unwrap();
    assert_eq!(inflight_before, 1);

    registry.ack("acme", key).unwrap();

    let (_, inflight_after, _) = registry.depth("acme", "default").unwrap();
    assert_eq!(inflight_after, 0);
}

#[test]
fn test_nack_requeues_task() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);
    let policy = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy.clone()).unwrap();

    registry
        .enqueue("acme", "default", b"retry_me".to_vec(), &policy)
        .unwrap();

    let tasks = registry.dequeue("acme", "default", 1).unwrap();
    let (key, _) = &tasks[0];
    registry.nack("acme", key).unwrap();

    // After nack, task should be back in pending
    let (pending, inflight, _) = registry.depth("acme", "default").unwrap();
    assert_eq!(inflight, 0);
    assert_eq!(pending, 1);
}

#[test]
fn test_depth_counts() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);
    let policy = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy.clone()).unwrap();

    for i in 0..5 {
        registry
            .enqueue("acme", "default", format!("task{i}").into_bytes(), &policy)
            .unwrap();
    }

    let (pending, inflight, dlq) = registry.depth("acme", "default").unwrap();
    assert_eq!(pending, 5);
    assert_eq!(inflight, 0);
    assert_eq!(dlq, 0);

    let tasks = registry.dequeue("acme", "default", 3).unwrap();
    let (pending2, inflight2, _) = registry.depth("acme", "default").unwrap();
    assert_eq!(pending2, 2);
    assert_eq!(inflight2, 3);
}
