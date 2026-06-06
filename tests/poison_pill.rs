//! S3 regression tests: a single corrupt record must not wedge an entire
//! dequeue / nack / reaper sweep. Corrupt records are routed to the DLQ
//! (wrapped in a valid Task envelope with attempts = u32::MAX) and processing
//! continues.

use rocksqueue::policy::NamespacePolicy;
use rocksqueue::task::{encode_key, now_millis, Task};
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

fn provision(registry: &TenantRegistry, tenant: &str) -> NamespacePolicy {
    let policy = NamespacePolicy::standard(tenant);
    registry.provision_tenant(tenant, policy.clone()).unwrap();
    policy
}

/// A corrupt inflight record must be moved to the DLQ while a valid expired
/// record in the same sweep is still reclaimed to pending.
#[test]
fn test_reaper_skips_corrupt_record() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);
    provision(&registry, "acme");

    let inflight_cf = registry.db.cf_handle("acme__inflight").unwrap();

    // Valid, expired inflight record at seq 1.
    let valid_key = encode_key("default", 1);
    let valid = Task {
        id: "valid".to_string(),
        queue: "default".to_string(),
        payload: b"payload".to_vec(),
        enqueued_at: now_millis(),
        attempts: 1,
        deadline: now_millis() - 1_000, // already past
    };
    registry
        .db
        .put_cf(&inflight_cf, &valid_key, &valid.serialize().unwrap())
        .unwrap();

    // Corrupt record at seq 2 (undeserializable bytes).
    let corrupt_key = encode_key("default", 2);
    registry
        .db
        .put_cf(&inflight_cf, &corrupt_key, b"\xff\xff not a task")
        .unwrap();

    rocksqueue::reaper::reap_inflight(&registry, "acme", "default").unwrap();

    // Both removed from inflight.
    assert!(registry.db.get_cf(&inflight_cf, &valid_key).unwrap().is_none());
    assert!(registry
        .db
        .get_cf(&inflight_cf, &corrupt_key)
        .unwrap()
        .is_none());

    // Valid record reclaimed to pending (sweep not aborted by the poison record).
    let pending_cf = registry.db.cf_handle("acme__pending").unwrap();
    let reclaimed = registry.db.get_cf(&pending_cf, &valid_key).unwrap();
    assert!(reclaimed.is_some(), "valid record must be reclaimed to pending");

    // Corrupt record landed in DLQ as a poison envelope.
    let dlq_cf = registry.db.cf_handle("acme__dlq").unwrap();
    let dlq_val = registry
        .db
        .get_cf(&dlq_cf, &corrupt_key)
        .unwrap()
        .expect("corrupt record must be in DLQ");
    let envelope = Task::deserialize(&dlq_val).expect("DLQ record must be a valid Task");
    assert_eq!(envelope.attempts, u32::MAX);
    assert_eq!(envelope.payload, b"\xff\xff not a task");
}

/// A corrupt pending record must be skipped and DLQ'd while valid records
/// behind it are still dequeued.
#[test]
fn test_dequeue_skips_corrupt_record() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);
    let policy = provision(&registry, "acme");

    let pending_cf = registry.db.cf_handle("acme__pending").unwrap();

    // Corrupt record at the front of the queue (seq 1 sorts before enqueue seqs,
    // which are seeded from now_millis()).
    let corrupt_key = encode_key("default", 1);
    registry
        .db
        .put_cf(&pending_cf, &corrupt_key, b"garbage")
        .unwrap();

    // A valid task enqueued through the normal path.
    registry
        .enqueue("acme", "default", b"good".to_vec(), &policy)
        .unwrap();

    let results = registry.dequeue_batch("acme", "default", 10, 60_000).unwrap();
    assert_eq!(results.len(), 1, "valid task must still be returned");
    assert_eq!(results[0].1.payload, b"good");

    // Corrupt record removed from pending and routed to DLQ.
    assert!(registry
        .db
        .get_cf(&pending_cf, &corrupt_key)
        .unwrap()
        .is_none());
    let dlq_cf = registry.db.cf_handle("acme__dlq").unwrap();
    let dlq_val = registry
        .db
        .get_cf(&dlq_cf, &corrupt_key)
        .unwrap()
        .expect("corrupt pending record must be in DLQ");
    let envelope = Task::deserialize(&dlq_val).expect("DLQ record must be a valid Task");
    assert_eq!(envelope.attempts, u32::MAX);
}

/// nack on a corrupt inflight record must succeed, remove the record, and route
/// it to the DLQ rather than returning an error.
#[test]
fn test_nack_handles_corrupt_record() {
    let tmp = TempDir::new().unwrap();
    let registry = make_registry(&tmp);
    provision(&registry, "acme");

    let inflight_cf = registry.db.cf_handle("acme__inflight").unwrap();
    let key = encode_key("default", 7);
    registry
        .db
        .put_cf(&inflight_cf, &key, b"\x00 corrupt")
        .unwrap();

    registry.nack("acme", &key).expect("nack must not error on corrupt record");

    assert!(registry.db.get_cf(&inflight_cf, &key).unwrap().is_none());
    let dlq_cf = registry.db.cf_handle("acme__dlq").unwrap();
    let dlq_val = registry
        .db
        .get_cf(&dlq_cf, &key)
        .unwrap()
        .expect("corrupt inflight record must be in DLQ");
    let envelope = Task::deserialize(&dlq_val).expect("DLQ record must be a valid Task");
    assert_eq!(envelope.attempts, u32::MAX);
}
