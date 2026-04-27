use std::sync::Arc;

use rocksqueue::error::QueueError;
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
fn test_reject_policy_exceeds_quota() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);

    let mut policy = NamespacePolicy::standard("acme");
    policy.backlog_quota = Some(3);
    policy.backlog_policy = BacklogPolicy::Reject;
    registry.provision_tenant("acme", policy.clone()).unwrap();

    // Enqueue 3 — should succeed
    for i in 0..3 {
        registry
            .enqueue("acme", "default", format!("task{i}").into_bytes(), &policy)
            .unwrap();
    }

    // Enqueue 1 more — should fail
    let result = registry.enqueue("acme", "default", b"overflow".to_vec(), &policy);
    assert!(matches!(result, Err(QueueError::BacklogQuotaExceeded { .. })));
}

#[test]
fn test_evict_oldest_policy() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);

    let mut policy = NamespacePolicy::standard("acme");
    policy.backlog_quota = Some(3);
    policy.backlog_policy = BacklogPolicy::EvictOldest;
    registry.provision_tenant("acme", policy.clone()).unwrap();

    for i in 0..3 {
        registry
            .enqueue("acme", "default", format!("task{i}").into_bytes(), &policy)
            .unwrap();
    }

    // Enqueue 1 more — evict oldest to make room
    registry
        .enqueue("acme", "default", b"new_task".to_vec(), &policy)
        .unwrap();

    let (pending, _, _) = registry.depth("acme", "default").unwrap();
    // Still within quota
    assert!(pending <= 3);
}

#[test]
fn test_no_quota_allows_unlimited() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);

    let mut policy = NamespacePolicy::standard("acme");
    policy.backlog_quota = None;
    registry.provision_tenant("acme", policy.clone()).unwrap();

    for i in 0..100 {
        registry
            .enqueue("acme", "default", format!("task{i}").into_bytes(), &policy)
            .unwrap();
    }

    let (pending, _, _) = registry.depth("acme", "default").unwrap();
    assert_eq!(pending, 100);
}

#[test]
fn test_concurrent_quota_not_exceeded() {
    let tmp = TempDir::new().unwrap();

    let mut policy = NamespacePolicy::standard("acme");
    policy.backlog_quota = Some(50);
    policy.backlog_policy = BacklogPolicy::Reject;

    let registry = Arc::new(make_registry(&tmp));
    registry.provision_tenant("acme", policy.clone()).unwrap();

    // 20 threads each try to enqueue 5 tasks against a quota of 50
    let threads: Vec<_> = (0..20)
        .map(|i| {
            let reg = Arc::clone(&registry);
            let pol = policy.clone();
            std::thread::spawn(move || {
                for j in 0..5 {
                    let _ = reg.enqueue(
                        "acme",
                        "default",
                        format!("t{i}_{j}").into_bytes(),
                        &pol,
                    );
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    let (pending, inflight, _) = registry.depth("acme", "default").unwrap();
    assert!(
        pending + inflight <= 50,
        "quota must never be exceeded: got {pending} pending + {inflight} inflight"
    );
}
