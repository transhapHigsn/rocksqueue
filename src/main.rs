use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing::get, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

use rocksqueue::baseline::BaselineRegistry;
use rocksqueue::config::Config;
use rocksqueue::grpc::control_plane::ControlPlaneService;
use rocksqueue::grpc::proto::control_plane_server::ControlPlaneServer;
use rocksqueue::reaper::spawn_reaper;
use rocksqueue::scheduler::{TenantPolicy, WFQScheduler};
use rocksqueue::stats::StatsCollector;
use rocksqueue::stats_daemon::spawn_stats_daemon;
use rocksqueue::stats_store::StatsStore;
use rocksqueue::tenant::{DbConfig, TenantRegistry};
use rocksqueue::throttle::{AutoThrottle, ThrottleConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Load config
    let cfg = Config::from_env();

    // 2. Init tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    info!("Starting RocksQueue — env={:?}", cfg.env);

    // 3. Ensure local dirs exist
    cfg.ensure_dirs()?;

    // 4. Open RocksDB
    let db_cfg = DbConfig {
        sst_path: cfg.rocksdb_path.to_string_lossy().to_string(),
        wal_path: cfg.rocksdb_wal_path.to_string_lossy().to_string(),
        block_cache_bytes: cfg.block_cache_bytes,
        write_buffer_bytes: 64 * 1024 * 1024,
        max_write_buffers: 4,
    };
    let registry = Arc::new(TenantRegistry::open(&db_cfg)?);
    info!("RocksDB opened at {}", db_cfg.sst_path);

    // 5-9. Wire components
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

    // 10-11. Warm state from last run
    throttle.restore_from_store();

    let restored_tenants = registry.list_tenants();
    for tenant_id in &restored_tenants {
        if let Some(policy) = registry.get_policy(tenant_id) {
            scheduler.register(TenantPolicy {
                tenant_id: tenant_id.clone(),
                weight: policy.weight,
                max_inflight: policy.max_inflight,
                burst_tokens: policy.burst_tokens,
                rate_per_sec: policy.rate_per_sec,
            });
            collector.register(tenant_id);
            baselines.register(tenant_id, 0.0);
        }
    }
    collector.warm_from_store(&store);
    info!(
        "Restored runtime state for {} tenant(s)",
        restored_tenants.len()
    );

    // Default queues watched by daemon and reaper
    let queues = vec!["default".to_string()];

    // 12. Spawn stats daemon
    spawn_stats_daemon(
        Arc::clone(&collector),
        Arc::clone(&registry),
        Arc::clone(&store),
        Arc::clone(&throttle),
        Arc::clone(&baselines),
        queues.clone(),
        1000,
    );

    // 13. Spawn visibility-timeout reaper
    spawn_reaper(Arc::clone(&registry), queues.clone(), 30);

    // 14. HTTP metrics + health server
    let metrics_addr = cfg.metrics_addr;
    let http_state = HttpState {
        registry: Arc::clone(&registry),
        checkpoint_path: cfg.checkpoint_path.clone(),
    };

    tokio::spawn(async move {
        let app = Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/ready", get(ready_handler))
            .route("/metrics", get(|| async { "# rocksqueue metrics\n" }))
            .route("/admin/checkpoint", post(checkpoint_handler))
            .with_state(http_state);

        info!("HTTP server listening on {metrics_addr}");
        let listener = tokio::net::TcpListener::bind(metrics_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // 15-16. Build gRPC service
    let svc = ControlPlaneService::new(
        Arc::clone(&registry),
        Arc::clone(&scheduler),
        Arc::clone(&collector),
        Arc::clone(&throttle),
        Arc::clone(&baselines),
        queues,
    );
    let grpc_svc = ControlPlaneServer::new(svc);
    let reflection_svc = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(rocksqueue::grpc::proto::FILE_DESCRIPTOR_SET)
        .build()?;

    // 17. Signal systemd (no-op locally)
    let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]);

    // 18. Serve gRPC with graceful shutdown
    info!("gRPC server listening on {}", cfg.grpc_addr);
    Server::builder()
        .add_service(grpc_svc)
        .add_service(reflection_svc)
        .serve_with_shutdown(cfg.grpc_addr, shutdown_signal())
        .await?;

    info!("Shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C handler");
    info!("Received SIGTERM/SIGINT — shutting down");
}

#[derive(Clone)]
struct HttpState {
    registry: Arc<rocksqueue::tenant::TenantRegistry>,
    checkpoint_path: PathBuf,
}

async fn ready_handler(State(state): State<HttpState>) -> impl IntoResponse {
    match state.registry.ping() {
        Ok(()) => "ok",
        Err(_) => "error",
    }
}

#[derive(Deserialize)]
struct CheckpointParams {
    path: String,
}

#[derive(Serialize)]
struct CheckpointResponse {
    ok: bool,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn checkpoint_handler(
    State(state): State<HttpState>,
    Query(params): Query<CheckpointParams>,
) -> impl IntoResponse {
    let requested = std::path::Path::new(&params.path);
    match state
        .registry
        .create_checkpoint(requested, &state.checkpoint_path)
    {
        Ok(()) => (
            StatusCode::OK,
            Json(CheckpointResponse {
                ok: true,
                path: params.path,
                error: None,
            }),
        ),
        Err(e) => {
            let status = if e.to_string().contains("must") {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (
                status,
                Json(CheckpointResponse {
                    ok: false,
                    path: params.path,
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}
