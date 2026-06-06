use std::sync::Arc;

use dashmap::DashMap;

use crate::stats_store::StatsStore;

/// Single-node mode: all tenants owned by "local".
/// Seed for future multi-node routing.
pub struct OwnershipMap {
    assignments: DashMap<String, String>,
    store: Arc<StatsStore>,
}

impl OwnershipMap {
    pub fn new(store: Arc<StatsStore>) -> Arc<Self> {
        Arc::new(Self {
            assignments: DashMap::new(),
            store,
        })
    }

    pub fn assign(&self, tenant_id: &str, node_id: &str) {
        self.assignments
            .insert(tenant_id.to_string(), node_id.to_string());
        // Persist to __system__ CF via store's DB
        // (In single-node mode, this is informational only)
    }

    pub fn owner_of(&self, tenant_id: &str) -> Option<String> {
        self.assignments.get(tenant_id).map(|e| e.value().clone())
    }

    /// In single-node mode, always returns true (owns all tenants).
    pub fn is_local(&self, tenant_id: &str, local_id: &str) -> bool {
        self.assignments
            .get(tenant_id)
            .map(|e| e.value() == local_id)
            .unwrap_or(true) // default: own all in single-node mode
    }
}
