use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::baseline::BaselineRegistry;
use crate::error::QueueError;
use crate::grpc::proto::control_plane_server::ControlPlane;
use crate::grpc::proto::*;
use crate::policy::{BacklogPolicy, NamespacePolicy, RetentionPolicy};
use crate::scheduler::{TenantPolicy, WFQScheduler};
use crate::stats::StatsCollector;
use crate::tenant::TenantRegistry;
use crate::throttle::AutoThrottle;

const DEFAULT_DEQUEUE_LIMIT: usize = 500;

pub struct ControlPlaneService {
    registry: Arc<TenantRegistry>,
    scheduler: Arc<WFQScheduler>,
    collector: Arc<StatsCollector>,
    throttle: Arc<AutoThrottle>,
    baselines: Arc<BaselineRegistry>,
    queues: Vec<String>,
}

impl ControlPlaneService {
    pub fn new(
        registry: Arc<TenantRegistry>,
        scheduler: Arc<WFQScheduler>,
        collector: Arc<StatsCollector>,
        throttle: Arc<AutoThrottle>,
        baselines: Arc<BaselineRegistry>,
        queues: Vec<String>,
    ) -> Self {
        Self {
            registry,
            scheduler,
            collector,
            throttle,
            baselines,
            queues,
        }
    }
}

fn not_found(msg: &str) -> Status {
    Status::not_found(msg)
}

fn internal(msg: &str) -> Status {
    Status::internal(msg)
}

fn queue_error(err: QueueError) -> Status {
    match err {
        QueueError::ColumnFamilyMissing(_)
        | QueueError::QueueNotFound(_)
        | QueueError::TaskNotFound(_) => Status::not_found(err.to_string()),
        QueueError::BacklogQuotaExceeded { .. } => Status::resource_exhausted(err.to_string()),
        _ => Status::internal(err.to_string()),
    }
}

fn queue_or_default(queue: String) -> String {
    if queue.is_empty() {
        "default".to_string()
    } else {
        queue
    }
}

fn decode_ack_key(ack_key: &str) -> Result<Vec<u8>, Status> {
    hex::decode(ack_key).map_err(|e| Status::invalid_argument(format!("invalid ack_key: {e}")))
}

/// Run a blocking RocksDB operation on the dedicated blocking pool so it never
/// occupies a tonic executor thread. Join failures map to `internal`; the
/// operation's own `QueueError` maps via `queue_error`.
async fn offload<F, T>(f: F) -> Result<T, Status>
where
    F: FnOnce() -> crate::error::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| internal(&format!("blocking task failed: {e}")))?
        .map_err(queue_error)
}

#[tonic::async_trait]
impl ControlPlane for ControlPlaneService {
    // ── Tenant lifecycle ──────────────────────────────────────────────────────

    async fn provision_tenant(
        &self,
        request: Request<ProvisionRequest>,
    ) -> Result<Response<TenantResponse>, Status> {
        let req = request.into_inner();
        let policy = match req.tier.as_str() {
            "premium" => NamespacePolicy::premium(&req.tenant_id),
            _ => NamespacePolicy::standard(&req.tenant_id),
        };

        let sched_policy = TenantPolicy {
            tenant_id: req.tenant_id.clone(),
            weight: policy.weight,
            max_inflight: policy.max_inflight,
            burst_tokens: policy.burst_tokens,
            rate_per_sec: policy.rate_per_sec,
        };

        {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            offload(move || registry.provision_tenant(&tenant, policy)).await?;
        }

        self.scheduler.register(sched_policy);
        self.collector.register(&req.tenant_id);
        self.baselines.register(&req.tenant_id, 0.0);

        info!("Provisioned tenant: {}", req.tenant_id);
        Ok(Response::new(TenantResponse {
            tenant_id: req.tenant_id,
            success: true,
            message: "provisioned".to_string(),
        }))
    }

    async fn drop_tenant(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<TenantResponse>, Status> {
        let req = request.into_inner();
        {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            offload(move || registry.drop_tenant(&tenant)).await?;
        }
        self.scheduler.deregister(&req.tenant_id);
        self.collector.deregister(&req.tenant_id);
        self.baselines.deregister(&req.tenant_id);

        Ok(Response::new(TenantResponse {
            tenant_id: req.tenant_id,
            success: true,
            message: "dropped".to_string(),
        }))
    }

    // ── Scheduling policy ─────────────────────────────────────────────────────

    async fn update_policy(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<TenantResponse>, Status> {
        let req = request.into_inner();
        let policy = req
            .policy
            .ok_or_else(|| Status::invalid_argument("policy required"))?;
        self.scheduler.update_policy(TenantPolicy {
            tenant_id: req.tenant_id.clone(),
            weight: policy.weight,
            max_inflight: policy.max_inflight as usize,
            burst_tokens: policy.burst_tokens,
            rate_per_sec: policy.rate_per_sec,
        });
        Ok(Response::new(TenantResponse {
            tenant_id: req.tenant_id,
            success: true,
            message: "policy updated".to_string(),
        }))
    }

    async fn get_policy(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<PolicyResponse>, Status> {
        let req = request.into_inner();
        let policy = self
            .scheduler
            .policy(&req.tenant_id)
            .ok_or_else(|| not_found("tenant not found"))?;
        Ok(Response::new(PolicyResponse {
            tenant_id: req.tenant_id,
            policy: Some(Policy {
                weight: policy.weight,
                max_inflight: policy.max_inflight as u32,
                burst_tokens: policy.burst_tokens,
                rate_per_sec: policy.rate_per_sec,
            }),
        }))
    }

    // ── Namespace policy ──────────────────────────────────────────────────────

    async fn set_namespace_policy(
        &self,
        request: Request<NamespacePolicyRequest>,
    ) -> Result<Response<TenantResponse>, Status> {
        let req = request.into_inner();
        let backlog_policy = match req.backlog_policy.as_str() {
            "block" => BacklogPolicy::Block {
                timeout_ms: req.block_timeout_ms,
            },
            "evict_oldest" => BacklogPolicy::EvictOldest,
            _ => BacklogPolicy::Reject,
        };

        let ns_policy = NamespacePolicy {
            tenant_id: req.tenant_id.clone(),
            namespace: req.tenant_id.clone(),
            weight: 10,
            max_inflight: 500,
            burst_tokens: 100,
            rate_per_sec: 100,
            backlog_quota: if req.backlog_quota == 0 {
                None
            } else {
                Some(req.backlog_quota as usize)
            },
            backlog_policy,
            retention: RetentionPolicy {
                pending_retention_secs: req.pending_retention_secs,
                dlq_retention_secs: if req.dlq_retention_secs == 0 {
                    7 * 24 * 3600
                } else {
                    req.dlq_retention_secs
                },
                inflight_stale_secs: if req.inflight_stale_secs == 0 {
                    300
                } else {
                    req.inflight_stale_secs
                },
            },
        };

        {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            offload(move || registry.update_namespace_policy(&tenant, ns_policy)).await?;
        }

        Ok(Response::new(TenantResponse {
            tenant_id: req.tenant_id,
            success: true,
            message: "namespace policy set".to_string(),
        }))
    }

    async fn get_namespace_policy(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<NamespacePolicyResponse>, Status> {
        let req = request.into_inner();
        let policy = self
            .registry
            .get_policy(&req.tenant_id)
            .ok_or_else(|| not_found("tenant not found"))?;

        let backlog_policy_str = match &policy.backlog_policy {
            BacklogPolicy::Reject => "reject",
            BacklogPolicy::Block { .. } => "block",
            BacklogPolicy::EvictOldest => "evict_oldest",
        };

        Ok(Response::new(NamespacePolicyResponse {
            tenant_id: req.tenant_id,
            backlog_quota: policy.backlog_quota.unwrap_or(0) as u64,
            backlog_policy: backlog_policy_str.to_string(),
            pending_retention_secs: policy.retention.pending_retention_secs,
            dlq_retention_secs: policy.retention.dlq_retention_secs,
            inflight_stale_secs: policy.retention.inflight_stale_secs,
        }))
    }

    // ── Observability ─────────────────────────────────────────────────────────

    async fn get_tenant_status(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<TenantStatus>, Status> {
        let req = request.into_inner();
        let queue = self.queues.first().cloned().unwrap_or_default();
        let (pending, inflight, dlq) = {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            offload(move || registry.depth(&tenant, &queue)).await?
        };

        let paused = self
            .scheduler
            .tenant_snapshot(&req.tenant_id)
            .map(|(_, _, paused, _)| paused)
            .unwrap_or(false);

        Ok(Response::new(TenantStatus {
            tenant_id: req.tenant_id,
            paused,
            pending: pending as u64,
            inflight: inflight as u64,
            dlq: dlq as u64,
        }))
    }

    async fn list_tenants(&self, _request: Request<Empty>) -> Result<Response<TenantList>, Status> {
        Ok(Response::new(TenantList {
            tenant_ids: self.registry.list_tenants(),
        }))
    }

    type WatchQueueDepthStream = ReceiverStream<Result<QueueDepthEvent, Status>>;

    async fn watch_queue_depth(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchQueueDepthStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(64);
        let registry = Arc::clone(&self.registry);
        let queues = self.queues.clone();
        let interval_secs = req.interval_secs.max(1);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                for queue in &queues {
                    let depth = {
                        let registry = Arc::clone(&registry);
                        let tenant = req.tenant_id.clone();
                        let queue = queue.clone();
                        tokio::task::spawn_blocking(move || registry.depth(&tenant, &queue)).await
                    };
                    let depth = match depth {
                        Ok(inner) => inner,
                        Err(e) => {
                            let _ = tx
                                .send(Err(internal(&format!("blocking task failed: {e}"))))
                                .await;
                            return;
                        }
                    };
                    match depth {
                        Ok((p, i, d)) => {
                            let event = Ok(QueueDepthEvent {
                                tenant_id: req.tenant_id.clone(),
                                queue: queue.clone(),
                                pending: p as u64,
                                inflight: i as u64,
                                dlq: d as u64,
                                timestamp: crate::task::now_millis(),
                            });
                            if tx.send(event).await.is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(internal(&e.to_string()))).await;
                            return;
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ── Queue workload ───────────────────────────────────────────────────────

    async fn enqueue_task(
        &self,
        request: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        let req = request.into_inner();
        let queue = queue_or_default(req.queue);
        let policy = self
            .registry
            .get_policy(&req.tenant_id)
            .ok_or_else(|| not_found("tenant not found"))?;

        let task_id = {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            let queue = queue.clone();
            let payload = req.payload.into_bytes();
            offload(move || registry.enqueue(&tenant, &queue, payload, &policy)).await?
        };

        self.collector.record_enqueue(&req.tenant_id, 1);

        Ok(Response::new(EnqueueResponse {
            tenant_id: req.tenant_id,
            queue,
            task_ids: vec![task_id],
        }))
    }

    async fn enqueue_batch(
        &self,
        request: Request<EnqueueBatchRequest>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        let req = request.into_inner();
        if req.payloads.is_empty() {
            return Err(Status::invalid_argument("payloads must not be empty"));
        }

        let queue = queue_or_default(req.queue);
        let policy = self
            .registry
            .get_policy(&req.tenant_id)
            .ok_or_else(|| not_found("tenant not found"))?;
        let count = req.payloads.len() as u64;
        let payloads: Vec<Vec<u8>> = req.payloads.into_iter().map(String::into_bytes).collect();

        let task_ids = {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            let queue = queue.clone();
            offload(move || registry.enqueue_batch_sync(&tenant, &queue, payloads, &policy)).await?
        };

        self.collector.record_enqueue(&req.tenant_id, count);

        Ok(Response::new(EnqueueResponse {
            tenant_id: req.tenant_id,
            queue,
            task_ids,
        }))
    }

    async fn dequeue_tasks(
        &self,
        request: Request<DequeueRequest>,
    ) -> Result<Response<DequeueResponse>, Status> {
        let req = request.into_inner();
        let queue = queue_or_default(req.queue);
        let limit = if req.limit == 0 {
            DEFAULT_DEQUEUE_LIMIT
        } else {
            req.limit as usize
        };
        let policy = self
            .registry
            .get_policy(&req.tenant_id)
            .ok_or_else(|| not_found("tenant not found"))?;

        let dequeued = {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            let queue = queue.clone();
            let max_inflight = policy.max_inflight;
            offload(move || {
                registry.dequeue_with_max_inflight(&tenant, &queue, limit, 60_000, max_inflight)
            })
            .await?
        };

        let tasks = dequeued
            .into_iter()
            .map(|(key, task)| DequeuedTask {
                ack_key: hex::encode(key),
                task_id: task.id,
                queue: task.queue,
                payload: String::from_utf8_lossy(&task.payload).into_owned(),
                enqueued_at: task.enqueued_at,
                attempts: task.attempts,
                deadline: task.deadline,
            })
            .collect();

        Ok(Response::new(DequeueResponse {
            tenant_id: req.tenant_id,
            queue,
            tasks,
        }))
    }

    async fn ack_task(
        &self,
        request: Request<TaskAckRequest>,
    ) -> Result<Response<TaskOpResponse>, Status> {
        let req = request.into_inner();
        let key = decode_ack_key(&req.ack_key)?;

        let acked = {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            offload(move || registry.ack(&tenant, &key)).await?
        };
        if acked {
            self.collector
                .record_ack(&req.tenant_id, Duration::from_millis(0));
        }

        Ok(Response::new(TaskOpResponse {
            tenant_id: req.tenant_id,
            success: true,
            message: "acked".to_string(),
        }))
    }

    async fn ack_batch(
        &self,
        request: Request<TaskAckBatchRequest>,
    ) -> Result<Response<TaskOpResponse>, Status> {
        let req = request.into_inner();
        if req.ack_keys.is_empty() {
            return Err(Status::invalid_argument("ack_keys must not be empty"));
        }

        let keys: Vec<Vec<u8>> = req
            .ack_keys
            .iter()
            .map(|ack_key| decode_ack_key(ack_key))
            .collect::<Result<_, _>>()?;

        let count = {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            offload(move || registry.ack_batch(&tenant, keys.iter().map(Vec::as_slice))).await?
        };
        self.collector
            .record_ack_count(&req.tenant_id, count as u64, Duration::from_millis(0));

        Ok(Response::new(TaskOpResponse {
            tenant_id: req.tenant_id,
            success: true,
            message: format!("acked {count}"),
        }))
    }

    async fn nack_task(
        &self,
        request: Request<TaskAckRequest>,
    ) -> Result<Response<TaskOpResponse>, Status> {
        let req = request.into_inner();
        let key = decode_ack_key(&req.ack_key)?;

        {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            offload(move || registry.nack(&tenant, &key)).await?;
        }
        self.collector.record_nack(&req.tenant_id);

        Ok(Response::new(TaskOpResponse {
            tenant_id: req.tenant_id,
            success: true,
            message: "nacked".to_string(),
        }))
    }

    // ── Stats ─────────────────────────────────────────────────────────────────

    async fn get_tenant_stats(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<TenantStatsResponse>, Status> {
        let req = request.into_inner();
        let stats = self
            .collector
            .snapshot(&req.tenant_id)
            .ok_or_else(|| not_found("tenant stats not found"))?;

        Ok(Response::new(TenantStatsResponse {
            tenant_id: stats.tenant_id,
            arrival_rate: stats.arrival_rate,
            processing_rate: stats.processing_rate,
            avg_task_latency: stats.avg_task_latency,
            burst_score: stats.burst_score,
            backlog_ratio: stats.backlog_ratio,
            total_enqueued: stats.total_enqueued,
            total_acked: stats.total_acked,
            total_nacked: stats.total_nacked,
            stats_version: stats.stats_version,
            is_new: stats.is_new,
        }))
    }

    async fn list_all_stats(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<TenantStatsList>, Status> {
        let all = self
            .collector
            .all_snapshots()
            .into_iter()
            .map(|s| TenantStatsResponse {
                tenant_id: s.tenant_id,
                arrival_rate: s.arrival_rate,
                processing_rate: s.processing_rate,
                avg_task_latency: s.avg_task_latency,
                burst_score: s.burst_score,
                backlog_ratio: s.backlog_ratio,
                total_enqueued: s.total_enqueued,
                total_acked: s.total_acked,
                total_nacked: s.total_nacked,
                stats_version: s.stats_version,
                is_new: s.is_new,
            })
            .collect();

        Ok(Response::new(TenantStatsList { stats: all }))
    }

    type WatchStatsStream = ReceiverStream<Result<TenantStatsEvent, Status>>;

    async fn watch_stats(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStatsStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(64);
        let collector = Arc::clone(&self.collector);
        let interval_secs = req.interval_secs.max(1);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                if let Some(s) = collector.snapshot(&req.tenant_id) {
                    let event = Ok(TenantStatsEvent {
                        stats: Some(TenantStatsResponse {
                            tenant_id: s.tenant_id,
                            arrival_rate: s.arrival_rate,
                            processing_rate: s.processing_rate,
                            avg_task_latency: s.avg_task_latency,
                            burst_score: s.burst_score,
                            backlog_ratio: s.backlog_ratio,
                            total_enqueued: s.total_enqueued,
                            total_acked: s.total_acked,
                            total_nacked: s.total_nacked,
                            stats_version: s.stats_version,
                            is_new: s.is_new,
                        }),
                        timestamp: crate::task::now_millis(),
                    });
                    if tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ── Auto-throttle ─────────────────────────────────────────────────────────

    async fn get_throttle_status(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<ThrottleStatus>, Status> {
        let req = request.into_inner();
        let state = self
            .throttle
            .snapshot(&req.tenant_id)
            .ok_or_else(|| not_found("no throttle state"))?;

        Ok(Response::new(ThrottleStatus {
            tenant_id: state.tenant_id,
            throttled: state.throttled,
            reason: state.reason,
            original_rate: state.original_rate,
            applied_rate: state.applied_rate,
            triggered_at_ms: state.triggered_at_ms,
            cooldown_until_ms: state.cooldown_until_ms,
        }))
    }

    async fn list_throttled(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ThrottleList>, Status> {
        let all = self
            .throttle
            .all_snapshots()
            .into_iter()
            .map(|s| ThrottleStatus {
                tenant_id: s.tenant_id,
                throttled: s.throttled,
                reason: s.reason,
                original_rate: s.original_rate,
                applied_rate: s.applied_rate,
                triggered_at_ms: s.triggered_at_ms,
                cooldown_until_ms: s.cooldown_until_ms,
            })
            .collect();

        Ok(Response::new(ThrottleList { throttled: all }))
    }

    async fn force_release_throttle(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<TenantResponse>, Status> {
        let req = request.into_inner();
        self.throttle.force_release(&req.tenant_id);
        Ok(Response::new(TenantResponse {
            tenant_id: req.tenant_id,
            success: true,
            message: "throttle released".to_string(),
        }))
    }

    // ── Baseline ──────────────────────────────────────────────────────────────

    async fn get_baseline_status(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<BaselineStatus>, Status> {
        let req = request.into_inner();
        let b = self
            .baselines
            .snapshot(&req.tenant_id)
            .ok_or_else(|| not_found("no baseline"))?;

        Ok(Response::new(BaselineStatus {
            tenant_id: b.tenant_id,
            short_ema: b.short_ema,
            long_ema: b.long_ema,
            rolling_peak: b.rolling_peak,
            spike_threshold: b.spike_threshold,
            last_updated_ms: b.last_updated_ms,
        }))
    }

    async fn list_all_baselines(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<BaselineList>, Status> {
        let all = self
            .baselines
            .all_snapshots()
            .into_iter()
            .map(|b| BaselineStatus {
                tenant_id: b.tenant_id,
                short_ema: b.short_ema,
                long_ema: b.long_ema,
                rolling_peak: b.rolling_peak,
                spike_threshold: b.spike_threshold,
                last_updated_ms: b.last_updated_ms,
            })
            .collect();

        Ok(Response::new(BaselineList { baselines: all }))
    }

    // ── Operations ────────────────────────────────────────────────────────────

    async fn pause_tenant(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<TenantResponse>, Status> {
        let req = request.into_inner();
        self.scheduler
            .set_paused(&req.tenant_id, true)
            .ok_or_else(|| not_found("tenant not found"))?;
        Ok(Response::new(TenantResponse {
            tenant_id: req.tenant_id,
            success: true,
            message: "paused".to_string(),
        }))
    }

    async fn resume_tenant(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<TenantResponse>, Status> {
        let req = request.into_inner();
        self.scheduler
            .set_paused(&req.tenant_id, false)
            .ok_or_else(|| not_found("tenant not found"))?;
        Ok(Response::new(TenantResponse {
            tenant_id: req.tenant_id,
            success: true,
            message: "resumed".to_string(),
        }))
    }

    async fn drain_tenant(
        &self,
        request: Request<DrainRequest>,
    ) -> Result<Response<DrainResponse>, Status> {
        let req = request.into_inner();
        // Pause new dequeues
        self.scheduler.set_paused(&req.tenant_id, true);

        let deadline = std::time::Instant::now() + Duration::from_secs(req.timeout_secs.max(1));
        let queue = self.queues.first().cloned().unwrap_or_default();

        loop {
            let (pending, inflight, _) = {
                let registry = Arc::clone(&self.registry);
                let tenant = req.tenant_id.clone();
                let queue = queue.clone();
                offload(move || registry.depth(&tenant, &queue)).await?
            };
            if pending == 0 && inflight == 0 {
                return Ok(Response::new(DrainResponse {
                    tenant_id: req.tenant_id,
                    drained: true,
                    remaining: 0,
                }));
            }
            if std::time::Instant::now() >= deadline {
                return Ok(Response::new(DrainResponse {
                    tenant_id: req.tenant_id,
                    drained: false,
                    remaining: (pending + inflight) as u64,
                }));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn purge_dlq(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<PurgeResponse>, Status> {
        let req = request.into_inner();
        let purged = {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            offload(move || registry.purge_dlq(&tenant)).await?
        };

        Ok(Response::new(PurgeResponse {
            tenant_id: req.tenant_id,
            purged: purged as u64,
        }))
    }

    async fn replay_dlq(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<ReplayResponse>, Status> {
        let req = request.into_inner();
        let replayed = {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            offload(move || registry.replay_dlq(&tenant)).await?
        };

        Ok(Response::new(ReplayResponse {
            tenant_id: req.tenant_id,
            replayed: replayed as u64,
        }))
    }

    // ── Compaction ────────────────────────────────────────────────────────────

    async fn compact_tenant(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let req = request.into_inner();
        {
            let registry = Arc::clone(&self.registry);
            let tenant = req.tenant_id.clone();
            offload(move || registry.compact_tenant(&tenant)).await?;
        }

        Ok(Response::new(CompactionResponse {
            tenant_id: req.tenant_id,
            success: true,
        }))
    }

    async fn get_compaction_stats(
        &self,
        request: Request<TenantId>,
    ) -> Result<Response<CompactionStats>, Status> {
        let req = request.into_inner();
        let c = self.registry.compaction_counters();

        Ok(Response::new(CompactionStats {
            tenant_id: req.tenant_id,
            pending_expired: c.pending_expired.load(Ordering::Relaxed),
            dlq_expired: c.dlq_expired.load(Ordering::Relaxed),
            inflight_stale: c.inflight_stale.load(Ordering::Relaxed),
            kept: c.kept.load(Ordering::Relaxed),
        }))
    }
}
