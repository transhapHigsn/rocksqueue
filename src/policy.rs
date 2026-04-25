use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BacklogPolicy {
    Reject,
    Block { timeout_ms: u64 },
    EvictOldest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// 0 = keep forever
    pub pending_retention_secs: u64,
    /// default: 7 days
    pub dlq_retention_secs: u64,
    /// default: 300s — safety net for inflight tasks
    pub inflight_stale_secs: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            pending_retention_secs: 0,
            dlq_retention_secs: 7 * 24 * 3600,
            inflight_stale_secs: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespacePolicy {
    pub tenant_id: String,
    pub namespace: String,
    pub weight: u32,
    pub max_inflight: usize,
    pub burst_tokens: u32,
    pub rate_per_sec: u32,
    pub backlog_quota: Option<usize>,
    pub backlog_policy: BacklogPolicy,
    pub retention: RetentionPolicy,
}

impl NamespacePolicy {
    pub fn standard(tenant_id: impl Into<String>) -> Self {
        let tenant_id = tenant_id.into();
        Self {
            namespace: tenant_id.clone(),
            tenant_id,
            weight: 10,
            max_inflight: 500,
            burst_tokens: 100,
            rate_per_sec: 100,
            backlog_quota: Some(100_000),
            backlog_policy: BacklogPolicy::Reject,
            retention: RetentionPolicy::default(),
        }
    }

    pub fn premium(tenant_id: impl Into<String>) -> Self {
        let tenant_id = tenant_id.into();
        Self {
            namespace: tenant_id.clone(),
            tenant_id,
            weight: 50,
            max_inflight: 5000,
            burst_tokens: 1000,
            rate_per_sec: 1000,
            backlog_quota: Some(1_000_000),
            backlog_policy: BacklogPolicy::Block { timeout_ms: 5000 },
            retention: RetentionPolicy::default(),
        }
    }
}
