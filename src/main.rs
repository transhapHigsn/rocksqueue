use std::sync::Arc;

use axum::{routing::get, Router};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

use rocksqueue::baseline::BaselineRegistry;
use rocksqueue::config::Config;
use rocksqueue::grpc::control_plane::ControlPlaneService;
use rocksqueue::grpc::proto::control_plane_server::ControlPlaneServer;
use rocksqueue::reaper::spawn_reaper;
use rocksqueue::scheduler::WFQScheduler;
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
    let registry = Arc::new(TenantRegistry::open(&db_cfg, vec![])?);
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
    collector.warm_from_store(&store);
    throttle.restore_from_store();

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
    let registry_http = Arc::clone(&registry);
    let metrics_addr = cfg.metrics_addr;
    let checkpoint_path = cfg.checkpoint_path.clone();

    tokio::spawn(async move {
        let app = Router::new()
            .route("/health", get(|| async { "ok" }))
            .route(
                "/ready",
                get({
                    let reg = Arc::clone(&registry_http);
                    move || {
                        let reg = Arc::clone(&reg);
                        async move {
                            match reg.ping() {
                                Ok(()) => "ok",
                                Err(_) => "error",
                            }
                        }
                    }
                }),
            )
            .route("/metrics", get(|| async { "# rocksqueue metrics\n" }));

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

    // 17. Signal systemd (no-op locally)
    let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]);

    // 18. Serve gRPC with graceful shutdown
    info!("gRPC server listening on {}", cfg.grpc_addr);
    Server::builder()
        .add_service(grpc_svc)
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
