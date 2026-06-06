//! P2/S5 regression: gRPC hot-path handlers must offload blocking RocksDB work
//! to the spawn_blocking pool so a producer stuck in a `BacklogPolicy::Block`
//! wait cannot monopolize the tonic executor and stall unrelated RPCs.
//!
//! Runs on a single worker thread: pre-fix the blocking enqueue owns the only
//! worker for the full block timeout (1s), so a concurrent cheap RPC is delayed
//! by ~timeout_ms; post-fix the enqueue runs on a blocking thread and the cheap
//! RPC returns immediately (asserted under 500ms).

use std::sync::Arc;
use std::time::{Duration, Instant};

use rocksqueue::baseline::BaselineRegistry;
use rocksqueue::grpc::control_plane::ControlPlaneService;
use rocksqueue::grpc::proto::control_plane_server::ControlPlane;
use rocksqueue::grpc::proto::{
    DequeueRequest, Empty, EnqueueBatchRequest, EnqueueRequest, ProvisionRequest, TaskAckRequest,
};
use rocksqueue::policy::{BacklogPolicy, NamespacePolicy};
use rocksqueue::scheduler::WFQScheduler;
use rocksqueue::stats::StatsCollector;
use rocksqueue::stats_store::StatsStore;
use rocksqueue::tenant::{DbConfig, TenantRegistry};
use rocksqueue::throttle::{AutoThrottle, ThrottleConfig};
use tempfile::TempDir;
use tonic::Request;

fn build_service(tmp: &TempDir) -> (Arc<ControlPlaneService>, Arc<TenantRegistry>) {
    let cfg = DbConfig {
        sst_path: tmp.path().join("sst").to_string_lossy().to_string(),
        wal_path: tmp.path().join("wal").to_string_lossy().to_string(),
        block_cache_bytes: 16 * 1024 * 1024,
        write_buffer_bytes: 4 * 1024 * 1024,
        max_write_buffers: 2,
    };
    let registry = Arc::new(TenantRegistry::open(&cfg).expect("open registry"));
    let store = Arc::new(StatsStore::new(Arc::clone(&registry.db)));
    let collector = StatsCollector::new();
    let baselines = BaselineRegistry::new(Arc::clone(&store));
    let scheduler = Arc::new(WFQScheduler::new());
    let throttle = AutoThrottle::new(
        ThrottleConfig::default(),
        Arc::clone(&scheduler),
        Arc::clone(&baselines),
        Arc::clone(&store),
    );

    let svc = ControlPlaneService::new(
        Arc::clone(&registry),
        scheduler,
        collector,
        throttle,
        baselines,
        vec!["default".to_string()],
    );
    (Arc::new(svc), registry)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_blocking_enqueue_does_not_starve_executor() {
    let tmp = TempDir::new().unwrap();
    let (svc, registry) = build_service(&tmp);

    // Quota of 1 with Block policy; fill it so the next enqueue blocks for ~1s.
    let mut policy = NamespacePolicy::standard("acme");
    policy.backlog_quota = Some(1);
    policy.backlog_policy = BacklogPolicy::Block { timeout_ms: 1000 };
    registry.provision_tenant("acme", policy.clone()).unwrap();
    registry
        .enqueue("acme", "default", b"fill".to_vec(), &policy)
        .unwrap();

    // Producer that will block in the quota wait (then error after the timeout).
    let producer = {
        let svc = Arc::clone(&svc);
        tokio::spawn(async move {
            let _ = svc
                .enqueue_batch(Request::new(EnqueueBatchRequest {
                    tenant_id: "acme".to_string(),
                    queue: "default".to_string(),
                    payloads: vec!["overflow".to_string()],
                }))
                .await;
        })
    };

    // Let the producer reach its blocking wait on the (single) worker.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A cheap, in-memory RPC must still return promptly — well under the 1s block.
    let started = Instant::now();
    let resp = svc.list_tenants(Request::new(Empty {})).await.unwrap();
    let elapsed = started.elapsed();

    assert!(
        resp.into_inner().tenant_ids.contains(&"acme".to_string()),
        "list_tenants should report the provisioned tenant"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "list_tenants was starved by the blocking enqueue: took {elapsed:?}"
    );

    producer.abort();
}

/// The gRPC `ack_task` handler remains idempotent at the API boundary: retrying
/// an already-applied acknowledgement still reports success.
#[tokio::test]
async fn test_ack_task_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let (svc, _registry) = build_service(&tmp);

    svc.provision_tenant(Request::new(ProvisionRequest {
        tenant_id: "acme".to_string(),
        tier: "standard".to_string(),
    }))
    .await
    .unwrap();

    svc.enqueue_task(Request::new(EnqueueRequest {
        tenant_id: "acme".to_string(),
        queue: "default".to_string(),
        payload: "hello".to_string(),
    }))
    .await
    .unwrap();

    let dequeued = svc
        .dequeue_tasks(Request::new(DequeueRequest {
            tenant_id: "acme".to_string(),
            queue: "default".to_string(),
            limit: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    let ack_key = dequeued.tasks[0].ack_key.clone();

    // Valid key → success.
    let ok = svc
        .ack_task(Request::new(TaskAckRequest {
            tenant_id: "acme".to_string(),
            ack_key: ack_key.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(ok.success, "live ack key must report success");
    assert_eq!(ok.message, "acked");

    // Same key again (already removed) still succeeds for retry safety.
    let again = svc
        .ack_task(Request::new(TaskAckRequest {
            tenant_id: "acme".to_string(),
            ack_key,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(again.success, "re-acking a removed key must be idempotent");
    assert_eq!(again.message, "acked");
}
