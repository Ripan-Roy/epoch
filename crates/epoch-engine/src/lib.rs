//! Composes the four profile engines without collapsing their semantics.

use std::{collections::HashMap, sync::Arc};

use epoch_bus::{BusConfig, EventBus, PublishResult, Subscription, SubscriptionTarget};
use epoch_cache::{Cache, CacheConfig};
use epoch_core::{
    Clock, DeploymentMode, DurabilityProfile, EpochError, EpochResult, EventEnvelope, ResourceKind,
    SystemClock, validate_resource_name,
};
use epoch_queue::{EnqueueReceipt, Queue, QueueConfig};
use epoch_stream::{AppendReceipt, Stream, StreamConfig};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

pub type CacheHandle = Arc<Mutex<Cache>>;
pub type StreamHandle = Arc<Mutex<Stream>>;
pub type QueueHandle = Arc<Mutex<Queue>>;
pub type BusHandle = Arc<Mutex<EventBus>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceSummary {
    pub name: String,
    pub kind: ResourceKind,
    pub durability: DurabilityProfile,
    pub epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteExecutionStatus {
    Delivered,
    PendingExternalDelivery,
    PullAvailable,
    TargetMissing,
    TargetRejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteExecution {
    pub subscription: String,
    pub target: String,
    pub status: RouteExecutionStatus,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BusPublishOutcome {
    pub publish: PublishResult,
    pub routes: Vec<RouteExecution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineHealth {
    pub status: String,
    pub deployment_mode: DeploymentMode,
    pub profiles: Vec<ResourceKind>,
    pub resource_count: usize,
    pub guarantee_ceiling: DurabilityProfile,
    pub hosted_control_plane_required: bool,
}

#[derive(Debug)]
pub struct EpochEngine {
    deployment_mode: DeploymentMode,
    clock: Arc<dyn Clock>,
    caches: RwLock<HashMap<String, CacheHandle>>,
    streams: RwLock<HashMap<String, StreamHandle>>,
    queues: RwLock<HashMap<String, QueueHandle>>,
    buses: RwLock<HashMap<String, BusHandle>>,
}

impl Default for EpochEngine {
    fn default() -> Self {
        Self::new(DeploymentMode::Standalone, Arc::new(SystemClock))
    }
}

impl EpochEngine {
    pub fn new(deployment_mode: DeploymentMode, clock: Arc<dyn Clock>) -> Self {
        Self {
            deployment_mode,
            clock,
            caches: RwLock::new(HashMap::new()),
            streams: RwLock::new(HashMap::new()),
            queues: RwLock::new(HashMap::new()),
            buses: RwLock::new(HashMap::new()),
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    pub fn create_cache(&self, name: &str, config: CacheConfig) -> EpochResult<CacheHandle> {
        validate_resource_name(name)?;
        let cache = Arc::new(Mutex::new(Cache::new(config)?));
        insert_unique(&self.caches, name, cache.clone())?;
        Ok(cache)
    }

    pub fn create_stream(&self, name: &str, config: StreamConfig) -> EpochResult<StreamHandle> {
        validate_resource_name(name)?;
        let stream = Arc::new(Mutex::new(Stream::new(config)?));
        insert_unique(&self.streams, name, stream.clone())?;
        Ok(stream)
    }

    pub fn create_queue(&self, name: &str, config: QueueConfig) -> EpochResult<QueueHandle> {
        validate_resource_name(name)?;
        let queue = Arc::new(Mutex::new(Queue::new(config)?));
        insert_unique(&self.queues, name, queue.clone())?;
        Ok(queue)
    }

    pub fn create_bus(&self, name: &str, config: BusConfig) -> EpochResult<BusHandle> {
        validate_resource_name(name)?;
        let bus = Arc::new(Mutex::new(EventBus::new(config)));
        insert_unique(&self.buses, name, bus.clone())?;
        Ok(bus)
    }

    pub fn cache(&self, name: &str) -> EpochResult<CacheHandle> {
        get_resource(&self.caches, "cache", name)
    }

    pub fn stream(&self, name: &str) -> EpochResult<StreamHandle> {
        get_resource(&self.streams, "stream", name)
    }

    pub fn queue(&self, name: &str) -> EpochResult<QueueHandle> {
        get_resource(&self.queues, "queue", name)
    }

    pub fn bus(&self, name: &str) -> EpochResult<BusHandle> {
        get_resource(&self.buses, "event bus", name)
    }

    pub fn upsert_subscription(&self, bus: &str, subscription: Subscription) -> EpochResult<u64> {
        self.bus(bus)?.lock().upsert_subscription(subscription)
    }

    pub fn append_stream(
        &self,
        stream: &str,
        envelope: EventEnvelope,
        partition: Option<u32>,
    ) -> EpochResult<AppendReceipt> {
        self.stream(stream)?
            .lock()
            .append(envelope, partition, self.now_ms())
    }

    pub fn enqueue(&self, queue: &str, envelope: EventEnvelope) -> EpochResult<EnqueueReceipt> {
        self.queue(queue)?.lock().enqueue(envelope, self.now_ms())
    }

    pub fn publish_bus(
        &self,
        bus: &str,
        envelope: EventEnvelope,
    ) -> EpochResult<BusPublishOutcome> {
        let now_ms = self.now_ms();
        let publish = self.bus(bus)?.lock().publish(envelope, now_ms)?;
        let routes = publish
            .deliveries
            .iter()
            .map(|delivery| match &delivery.target {
                SubscriptionTarget::Queue { resource } => match self.queue(resource) {
                    Ok(queue) => match queue.lock().enqueue(delivery.envelope.clone(), now_ms) {
                        Ok(_) => RouteExecution {
                            subscription: delivery.subscription.clone(),
                            target: format!("queue:{resource}"),
                            status: RouteExecutionStatus::Delivered,
                            detail: None,
                        },
                        Err(error) => RouteExecution {
                            subscription: delivery.subscription.clone(),
                            target: format!("queue:{resource}"),
                            status: RouteExecutionStatus::TargetRejected,
                            detail: Some(error.to_string()),
                        },
                    },
                    Err(_) => missing_route(&delivery.subscription, "queue", resource),
                },
                SubscriptionTarget::Stream { resource } => match self.stream(resource) {
                    Ok(stream) => {
                        match stream
                            .lock()
                            .append(delivery.envelope.clone(), None, now_ms)
                        {
                            Ok(_) => RouteExecution {
                                subscription: delivery.subscription.clone(),
                                target: format!("stream:{resource}"),
                                status: RouteExecutionStatus::Delivered,
                                detail: None,
                            },
                            Err(error) => RouteExecution {
                                subscription: delivery.subscription.clone(),
                                target: format!("stream:{resource}"),
                                status: RouteExecutionStatus::TargetRejected,
                                detail: Some(error.to_string()),
                            },
                        }
                    }
                    Err(_) => missing_route(&delivery.subscription, "stream", resource),
                },
                SubscriptionTarget::Pull => RouteExecution {
                    subscription: delivery.subscription.clone(),
                    target: "pull".into(),
                    status: RouteExecutionStatus::PullAvailable,
                    detail: Some("durable pull ledger is a later milestone".into()),
                },
                SubscriptionTarget::Webhook { url } | SubscriptionTarget::Http { url } => {
                    RouteExecution {
                        subscription: delivery.subscription.clone(),
                        target: url.clone(),
                        status: RouteExecutionStatus::PendingExternalDelivery,
                        detail: Some(
                            "connector delivery runtime is not enabled in the slice".into(),
                        ),
                    }
                }
            })
            .collect();
        Ok(BusPublishOutcome { publish, routes })
    }

    pub fn resources(&self) -> Vec<ResourceSummary> {
        let mut resources = Vec::new();
        resources.extend(
            self.caches
                .read()
                .iter()
                .map(|(name, cache)| ResourceSummary {
                    name: name.clone(),
                    kind: ResourceKind::Cache,
                    durability: cache.lock().config().durability,
                    epoch: 1,
                }),
        );
        resources.extend(
            self.streams
                .read()
                .iter()
                .map(|(name, stream)| ResourceSummary {
                    name: name.clone(),
                    kind: ResourceKind::Stream,
                    durability: stream.lock().config().durability,
                    epoch: 1,
                }),
        );
        resources.extend(
            self.queues
                .read()
                .iter()
                .map(|(name, queue)| ResourceSummary {
                    name: name.clone(),
                    kind: ResourceKind::Queue,
                    durability: queue.lock().config().durability,
                    epoch: 1,
                }),
        );
        resources.extend(self.buses.read().iter().map(|(name, bus)| ResourceSummary {
            name: name.clone(),
            kind: ResourceKind::EventBus,
            durability: bus.lock().config().durability,
            epoch: 1,
        }));
        resources.sort_by(|left, right| {
            format!("{:?}:{}", left.kind, left.name)
                .cmp(&format!("{:?}:{}", right.kind, right.name))
        });
        resources
    }

    pub fn health(&self) -> EngineHealth {
        let resources = self.resources();
        let mut profiles: Vec<ResourceKind> = resources.iter().map(|item| item.kind).collect();
        profiles.sort_by_key(|kind| format!("{kind:?}"));
        profiles.dedup();
        EngineHealth {
            status: "ok".into(),
            deployment_mode: self.deployment_mode,
            profiles,
            resource_count: resources.len(),
            guarantee_ceiling: match self.deployment_mode {
                DeploymentMode::Embedded | DeploymentMode::Standalone => {
                    DurabilityProfile::LocalDurable
                }
                DeploymentMode::Cluster | DeploymentMode::Managed => {
                    DurabilityProfile::QuorumDurable
                }
            },
            hosted_control_plane_required: self.deployment_mode == DeploymentMode::Managed,
        }
    }

    pub fn maintain(&self, limit_per_cache: usize) {
        let now_ms = self.now_ms();
        for cache in self.caches.read().values() {
            cache.lock().purge_expired(now_ms, limit_per_cache);
        }
        for queue in self.queues.read().values() {
            queue.lock().maintain(now_ms);
        }
    }
}

fn insert_unique<T>(
    resources: &RwLock<HashMap<String, Arc<Mutex<T>>>>,
    name: &str,
    resource: Arc<Mutex<T>>,
) -> EpochResult<()> {
    let mut resources = resources.write();
    if resources.contains_key(name) {
        return Err(EpochError::AlreadyExists(name.to_owned()));
    }
    resources.insert(name.to_owned(), resource);
    Ok(())
}

fn get_resource<T>(
    resources: &RwLock<HashMap<String, Arc<Mutex<T>>>>,
    kind: &str,
    name: &str,
) -> EpochResult<Arc<Mutex<T>>> {
    resources
        .read()
        .get(name)
        .cloned()
        .ok_or_else(|| EpochError::NotFound(format!("{kind}:{name}")))
}

fn missing_route(subscription: &str, kind: &str, resource: &str) -> RouteExecution {
    RouteExecution {
        subscription: subscription.to_owned(),
        target: format!("{kind}:{resource}"),
        status: RouteExecutionStatus::TargetMissing,
        detail: Some("target resource does not exist".into()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use epoch_bus::{EventFilter, EventTransform};
    use epoch_core::ManualClock;
    use serde_json::json;

    use super::*;

    #[test]
    fn bus_routes_to_queue_without_semantic_aliasing() {
        let clock = Arc::new(ManualClock::new(100));
        let engine = EpochEngine::new(DeploymentMode::Standalone, clock);
        engine
            .create_queue("fulfillment", QueueConfig::default())
            .unwrap();
        engine.create_bus("orders", BusConfig::default()).unwrap();
        engine
            .upsert_subscription(
                "orders",
                Subscription {
                    name: "fulfillment-worker".into(),
                    filter: EventFilter {
                        event_type_patterns: vec!["order.created".into()],
                        ..EventFilter::default()
                    },
                    target: SubscriptionTarget::Queue {
                        resource: "fulfillment".into(),
                    },
                    transform: EventTransform {
                        add_headers: BTreeMap::from([("routed-by".into(), "epoch".into())]),
                        ..EventTransform::default()
                    },
                },
            )
            .unwrap();
        let outcome = engine
            .publish_bus(
                "orders",
                EventEnvelope::new("checkout", "order.created", json!({"id": 1}), 100),
            )
            .unwrap();
        assert_eq!(outcome.routes[0].status, RouteExecutionStatus::Delivered);
        assert_eq!(
            engine.queue("fulfillment").unwrap().lock().counts().ready,
            1
        );
    }

    #[test]
    fn health_never_claims_cluster_guarantees_in_standalone() {
        let engine = EpochEngine::default();
        let health = engine.health();
        assert_eq!(health.deployment_mode, DeploymentMode::Standalone);
        assert_eq!(health.guarantee_ceiling, DurabilityProfile::LocalDurable);
        assert!(!health.hosted_control_plane_required);
    }
}
