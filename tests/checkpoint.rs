use rocksqueue::policy::NamespacePolicy;
use rocksqueue::tenant::{DbConfig, TenantRegistry};
use tempfile::TempDir;

fn make_registry_with_cfg(tmp: &TempDir) -> (TenantRegistry, DbConfig) {
    let cfg = DbConfig {
        sst_path: tmp.path().join("sst").to_string_lossy().to_string(),
        wal_path: tmp.path().join("wal").to_string_lossy().to_string(),
        block_cache_bytes: 16 * 1024 * 1024,
        write_buffer_bytes: 4 * 1024 * 1024,
        max_write_buffers: 2,
    };
    let registry = TenantRegistry::open(&cfg).expect("failed to open registry");
    (registry, cfg)
}

#[test]
fn test_checkpoint_creates_files() {
    let tmp = TempDir::new().unwrap();
    let (registry, _cfg) = make_registry_with_cfg(&tmp);

    let policy = NamespacePolicy::standard("acme");
    registry.provision_tenant("acme", policy.clone()).unwrap();
    registry
        .enqueue("acme", "default", b"task".to_vec(), &policy)
        .unwrap();

    let checkpoint_base = tmp.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_base).unwrap();
    let checkpoint_dir = checkpoint_base.join("snap1");

    registry
        .create_checkpoint(&checkpoint_dir, &checkpoint_base)
        .expect("checkpoint should succeed");

    // RocksDB checkpoint produces SST/MANIFEST files
    assert!(
        checkpoint_dir.exists(),
        "checkpoint directory must be created"
    );
    let entries: Vec<_> = std::fs::read_dir(&checkpoint_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert!(!entries.is_empty(), "checkpoint must contain files");
}

#[test]
fn test_checkpoint_path_traversal_rejected() {
    let tmp = TempDir::new().unwrap();
    let (registry, _cfg) = make_registry_with_cfg(&tmp);

    let checkpoint_base = tmp.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_base).unwrap();

    // Path with .. should be rejected
    let traversal_path = checkpoint_base.join("../escape");
    let result = registry.create_checkpoint(&traversal_path, &checkpoint_base);
    assert!(result.is_err(), "path with .. must be rejected");
}

#[test]
fn test_checkpoint_outside_base_rejected() {
    let tmp = TempDir::new().unwrap();
    let (registry, _cfg) = make_registry_with_cfg(&tmp);

    let checkpoint_base = tmp.path().join("checkpoints");
    std::fs::create_dir_all(&checkpoint_base).unwrap();

    // Absolute path outside allowed base must be rejected
    let outside_path = tmp.path().join("elsewhere").join("snap");
    let result = registry.create_checkpoint(&outside_path, &checkpoint_base);
    assert!(
        result.is_err(),
        "path outside allowed base must be rejected"
    );
}
