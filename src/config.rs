use std::net::SocketAddr;
use std::path::PathBuf;

use tracing::info;

#[derive(Debug, Clone)]
pub enum Environment {
    Local,
    Production,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rocksdb_path: PathBuf,
    pub rocksdb_wal_path: PathBuf,
    pub checkpoint_path: PathBuf,
    pub grpc_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub s3_bucket: Option<String>,
    pub s3_endpoint: Option<String>,
    pub aws_region: String,
    pub block_cache_bytes: usize,
    pub env: Environment,
}

impl Config {
    pub fn from_env() -> Self {
        // Load .env if present (no-op if missing)
        let _ = dotenvy::dotenv();

        let rocksdb_path = PathBuf::from(
            std::env::var("ROCKSDB_PATH").unwrap_or_else(|_| "./data/rocksqueue".to_string()),
        );
        let rocksdb_wal_path = PathBuf::from(
            std::env::var("ROCKSDB_WAL_PATH")
                .unwrap_or_else(|_| "./data/rocksqueue-wal".to_string()),
        );
        let checkpoint_path = PathBuf::from(
            std::env::var("CHECKPOINT_PATH").unwrap_or_else(|_| "./data/checkpoints".to_string()),
        );

        let grpc_addr = std::env::var("GRPC_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
            .parse()
            .expect("Invalid GRPC_ADDR");

        let metrics_addr = std::env::var("METRICS_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:9090".to_string())
            .parse()
            .expect("Invalid METRICS_ADDR");

        let s3_bucket = std::env::var("S3_BUCKET").ok();
        let s3_endpoint = std::env::var("S3_ENDPOINT").ok();
        let aws_region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let env = if rocksdb_path.starts_with("/data") {
            Environment::Production
        } else {
            Environment::Local
        };

        let block_cache_bytes = match env {
            Environment::Production => 4 * 1024 * 1024 * 1024,
            Environment::Local => 256 * 1024 * 1024,
        };

        Self {
            rocksdb_path,
            rocksdb_wal_path,
            checkpoint_path,
            grpc_addr,
            metrics_addr,
            s3_bucket,
            s3_endpoint,
            aws_region,
            block_cache_bytes,
            env,
        }
    }

    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        if self.is_local() {
            std::fs::create_dir_all(&self.rocksdb_path)?;
            std::fs::create_dir_all(&self.rocksdb_wal_path)?;
            std::fs::create_dir_all(&self.checkpoint_path)?;
            info!("Created local data directories");
        }
        Ok(())
    }

    pub fn is_local(&self) -> bool {
        matches!(self.env, Environment::Local)
    }
}
