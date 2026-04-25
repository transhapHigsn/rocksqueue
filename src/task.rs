use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub queue: String,
    pub payload: Vec<u8>,
    pub enqueued_at: u64,
    pub attempts: u32,
    pub deadline: u64,
}

impl Task {
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| crate::error::QueueError::Serialization(e))
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| crate::error::QueueError::Serialization(e))
    }

    pub fn age_ms(&self) -> u64 {
        now_millis().saturating_sub(self.enqueued_at)
    }
}

/// Encode a RocksDB key for a task: `{queue_bytes}\x00{seq_be_8bytes}`
pub fn encode_key(queue: &str, seq: u64) -> Vec<u8> {
    let mut key = queue.as_bytes().to_vec();
    key.push(0x00);
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

/// Return the prefix for all keys belonging to a queue: `{queue_bytes}\x00`
pub fn queue_prefix(queue: &str) -> Vec<u8> {
    let mut prefix = queue.as_bytes().to_vec();
    prefix.push(0x00);
    prefix
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
