use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("RocksDB error: {0}")]
    RocksDB(#[from] rocksdb::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] Box<bincode::ErrorKind>),

    #[error("Queue not found: {0}")]
    QueueNotFound(String),

    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("Column family missing: {0}")]
    ColumnFamilyMissing(String),

    #[error("Backlog quota exceeded for tenant={tenant} queue={queue}: current={current} quota={quota} incoming={incoming}")]
    BacklogQuotaExceeded {
        tenant: String,
        queue: String,
        current: usize,
        quota: usize,
        incoming: usize,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, QueueError>;
