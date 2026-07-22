//! Composes the four profile engines without collapsing their semantics.

use std::{collections::HashMap, sync::Arc};

use epoch_bus::{BusConfig, EventBus, PublishResult, Subscription, SubscriptionTarget};
use epoch_cache::{Cache, CacheConfig};
use epoch_core::{
    AckMetadata, Clock, DeploymentMode, DurabilityProfile, EpochError, EpochResult, EventEnvelope,
    ResourceKind, SystemClock, validate_resource_name,
};
use epoch_queue::{Delivery, EnqueueReceipt, Queue, QueueConfig};
use epoch_storage::{CommitLog, LogRecord};
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

const JOURNAL_FORMAT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalEntry {
    format_version: u16,
    mutation: JournalMutation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum JournalMutation {
    CreateStream {
        name: String,
        config: StreamConfig,
    },
    AppendStream {
        name: String,
        envelope: Box<EventEnvelope>,
        partition: u32,
        applied_at_ms: u64,
    },
    SetStreamOffset {
        name: String,
        group: String,
        partition: u32,
        next_offset: u64,
        reset: bool,
    },
    CreateQueue {
        name: String,
        config: QueueConfig,
    },
    EnqueueQueue {
        name: String,
        envelope: Box<EventEnvelope>,
        applied_at_ms: u64,
    },
    AcquireQueue {
        name: String,
        consumer: String,
        max_messages: u32,
        visibility_timeout_ms: Option<u64>,
        applied_at_ms: u64,
    },
    SettleQueue {
        name: String,
        settlement: JournalQueueSettlement,
        applied_at_ms: u64,
    },
    RedriveQueue {
        name: String,
        message_id: String,
        applied_at_ms: u64,
    },
    MaintainQueue {
        name: String,
        applied_at_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum JournalQueueSettlement {
    Ack {
        token: String,
    },
    Release {
        token: String,
        delay_ms: u64,
        reason: Option<String>,
    },
    Reject {
        token: String,
        reason: String,
    },
    Extend {
        token: String,
        extension_ms: u64,
    },
}

#[derive(Debug)]
pub struct EpochEngine {
    deployment_mode: DeploymentMode,
    clock: Arc<dyn Clock>,
    journal: Option<Mutex<Box<dyn CommitLog>>>,
    guarantee_ceiling: DurabilityProfile,
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
        Self::empty(deployment_mode, clock, None, DurabilityProfile::Volatile)
    }

    pub fn with_commit_log(
        deployment_mode: DeploymentMode,
        clock: Arc<dyn Clock>,
        log: Box<dyn CommitLog>,
    ) -> EpochResult<Self> {
        let guarantee_ceiling = log.durability();
        let records = log.records_from(0, usize::MAX);
        let engine = Self::empty(
            deployment_mode,
            clock,
            Some(Mutex::new(log)),
            guarantee_ceiling,
        );
        engine.recover(records)?;
        Ok(engine)
    }

    fn empty(
        deployment_mode: DeploymentMode,
        clock: Arc<dyn Clock>,
        journal: Option<Mutex<Box<dyn CommitLog>>>,
        guarantee_ceiling: DurabilityProfile,
    ) -> Self {
        Self {
            deployment_mode,
            clock,
            journal,
            guarantee_ceiling,
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
        self.validate_durability(ResourceKind::Cache, config.durability)?;
        let cache = Arc::new(Mutex::new(Cache::new(config)?));
        insert_unique(&self.caches, name, cache.clone())?;
        Ok(cache)
    }

    pub fn create_stream(&self, name: &str, config: StreamConfig) -> EpochResult<StreamHandle> {
        validate_resource_name(name)?;
        self.validate_durability(ResourceKind::Stream, config.durability)?;
        let stream = Arc::new(Mutex::new(Stream::new(config.clone())?));
        let mut streams = self.streams.write();
        if streams.contains_key(name) {
            return Err(EpochError::AlreadyExists(name.to_owned()));
        }
        self.persist_if_required(
            config.durability,
            JournalMutation::CreateStream {
                name: name.to_owned(),
                config,
            },
            self.now_ms(),
        )?;
        streams.insert(name.to_owned(), stream.clone());
        Ok(stream)
    }

    pub fn create_queue(&self, name: &str, config: QueueConfig) -> EpochResult<QueueHandle> {
        validate_resource_name(name)?;
        self.validate_durability(ResourceKind::Queue, config.durability)?;
        let queue = Arc::new(Mutex::new(Queue::new(config.clone())?));
        let mut queues = self.queues.write();
        if queues.contains_key(name) {
            return Err(EpochError::AlreadyExists(name.to_owned()));
        }
        self.persist_if_required(
            config.durability,
            JournalMutation::CreateQueue {
                name: name.to_owned(),
                config,
            },
            self.now_ms(),
        )?;
        queues.insert(name.to_owned(), queue.clone());
        Ok(queue)
    }

    pub fn create_bus(&self, name: &str, config: BusConfig) -> EpochResult<BusHandle> {
        validate_resource_name(name)?;
        self.validate_durability(ResourceKind::EventBus, config.durability)?;
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
        self.append_stream_at(stream, envelope, partition, self.now_ms())
    }

    pub fn commit_stream_offset(
        &self,
        stream: &str,
        group: &str,
        partition: u32,
        next_offset: u64,
    ) -> EpochResult<()> {
        self.set_stream_offset(stream, group, partition, next_offset, false)
    }

    pub fn reset_stream_offset(
        &self,
        stream: &str,
        group: &str,
        partition: u32,
        next_offset: u64,
    ) -> EpochResult<()> {
        self.set_stream_offset(stream, group, partition, next_offset, true)
    }

    pub fn enqueue(&self, queue: &str, envelope: EventEnvelope) -> EpochResult<EnqueueReceipt> {
        self.enqueue_at(queue, envelope, self.now_ms())
    }

    pub fn acquire_queue(
        &self,
        queue: &str,
        consumer: &str,
        max_messages: usize,
        visibility_timeout_ms: Option<u64>,
    ) -> EpochResult<Vec<Delivery>> {
        let max_messages = u32::try_from(max_messages).map_err(|_| {
            EpochError::InvalidArgument("max_messages exceeds the supported range".into())
        })?;
        let applied_at_ms = self.now_ms();
        self.mutate_queue(
            queue,
            JournalMutation::AcquireQueue {
                name: queue.to_owned(),
                consumer: consumer.to_owned(),
                max_messages,
                visibility_timeout_ms,
                applied_at_ms,
            },
            applied_at_ms,
            |proposed| {
                proposed.acquire(
                    consumer,
                    max_messages as usize,
                    visibility_timeout_ms,
                    applied_at_ms,
                )
            },
        )
    }

    pub fn acknowledge_queue(&self, queue: &str, token: &str) -> EpochResult<AckMetadata> {
        let applied_at_ms = self.now_ms();
        self.maintain_queue_at(queue, applied_at_ms)?;
        self.mutate_queue(
            queue,
            JournalMutation::SettleQueue {
                name: queue.to_owned(),
                settlement: JournalQueueSettlement::Ack {
                    token: token.to_owned(),
                },
                applied_at_ms,
            },
            applied_at_ms,
            |proposed| proposed.acknowledge(token, applied_at_ms),
        )
    }

    pub fn release_queue(
        &self,
        queue: &str,
        token: &str,
        delay_ms: u64,
        reason: Option<String>,
    ) -> EpochResult<()> {
        let applied_at_ms = self.now_ms();
        self.maintain_queue_at(queue, applied_at_ms)?;
        self.mutate_queue(
            queue,
            JournalMutation::SettleQueue {
                name: queue.to_owned(),
                settlement: JournalQueueSettlement::Release {
                    token: token.to_owned(),
                    delay_ms,
                    reason: reason.clone(),
                },
                applied_at_ms,
            },
            applied_at_ms,
            |proposed| proposed.release(token, delay_ms, reason, applied_at_ms),
        )
    }

    pub fn reject_queue(&self, queue: &str, token: &str, reason: String) -> EpochResult<()> {
        let applied_at_ms = self.now_ms();
        self.maintain_queue_at(queue, applied_at_ms)?;
        self.mutate_queue(
            queue,
            JournalMutation::SettleQueue {
                name: queue.to_owned(),
                settlement: JournalQueueSettlement::Reject {
                    token: token.to_owned(),
                    reason: reason.clone(),
                },
                applied_at_ms,
            },
            applied_at_ms,
            |proposed| proposed.reject(token, reason, applied_at_ms),
        )
    }

    pub fn extend_queue_lease(
        &self,
        queue: &str,
        token: &str,
        extension_ms: u64,
    ) -> EpochResult<u64> {
        let applied_at_ms = self.now_ms();
        self.maintain_queue_at(queue, applied_at_ms)?;
        self.mutate_queue(
            queue,
            JournalMutation::SettleQueue {
                name: queue.to_owned(),
                settlement: JournalQueueSettlement::Extend {
                    token: token.to_owned(),
                    extension_ms,
                },
                applied_at_ms,
            },
            applied_at_ms,
            |proposed| proposed.extend_lease(token, extension_ms, applied_at_ms),
        )
    }

    pub fn redrive_queue(&self, queue: &str, message_id: &str) -> EpochResult<()> {
        let applied_at_ms = self.now_ms();
        self.mutate_queue(
            queue,
            JournalMutation::RedriveQueue {
                name: queue.to_owned(),
                message_id: message_id.to_owned(),
                applied_at_ms,
            },
            applied_at_ms,
            |proposed| proposed.redrive(message_id, applied_at_ms),
        )
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
                    Ok(_) => match self.enqueue_at(resource, delivery.envelope.clone(), now_ms) {
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
                SubscriptionTarget::Stream { resource } => {
                    match self.append_stream_at(resource, delivery.envelope.clone(), None, now_ms) {
                        Ok(_) => RouteExecution {
                            subscription: delivery.subscription.clone(),
                            target: format!("stream:{resource}"),
                            status: RouteExecutionStatus::Delivered,
                            detail: None,
                        },
                        Err(EpochError::NotFound(_)) => {
                            missing_route(&delivery.subscription, "stream", resource)
                        }
                        Err(error) => RouteExecution {
                            subscription: delivery.subscription.clone(),
                            target: format!("stream:{resource}"),
                            status: RouteExecutionStatus::TargetRejected,
                            detail: Some(error.to_string()),
                        },
                    }
                }
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

    fn append_stream_at(
        &self,
        stream_name: &str,
        envelope: EventEnvelope,
        requested_partition: Option<u32>,
        applied_at_ms: u64,
    ) -> EpochResult<AppendReceipt> {
        let stream = self.stream(stream_name)?;
        let mut current = stream.lock();
        let durability = current.config().durability;
        let mut proposed = current.clone();
        let receipt = proposed.append(envelope.clone(), requested_partition, applied_at_ms)?;
        if !receipt.acknowledgement.duplicate {
            self.persist_if_required(
                durability,
                JournalMutation::AppendStream {
                    name: stream_name.to_owned(),
                    envelope: Box::new(envelope),
                    partition: receipt.partition,
                    applied_at_ms,
                },
                applied_at_ms,
            )?;
            *current = proposed;
        }
        Ok(receipt)
    }

    fn enqueue_at(
        &self,
        queue_name: &str,
        envelope: EventEnvelope,
        applied_at_ms: u64,
    ) -> EpochResult<EnqueueReceipt> {
        let queue = self.queue(queue_name)?;
        let mut current = queue.lock();
        let durability = current.config().durability;
        if durability == DurabilityProfile::Volatile {
            return current.enqueue(envelope, applied_at_ms);
        }
        let mut proposed = current.clone();
        let receipt = proposed.enqueue(envelope.clone(), applied_at_ms)?;
        if proposed != *current {
            self.persist_if_required(
                durability,
                JournalMutation::EnqueueQueue {
                    name: queue_name.to_owned(),
                    envelope: Box::new(envelope),
                    applied_at_ms,
                },
                applied_at_ms,
            )?;
            *current = proposed;
        }
        Ok(receipt)
    }

    fn mutate_queue<T>(
        &self,
        queue_name: &str,
        mutation: JournalMutation,
        applied_at_ms: u64,
        operation: impl FnOnce(&mut Queue) -> EpochResult<T>,
    ) -> EpochResult<T> {
        let queue = self.queue(queue_name)?;
        let mut current = queue.lock();
        let durability = current.config().durability;
        if durability == DurabilityProfile::Volatile {
            return operation(&mut current);
        }
        let mut proposed = current.clone();
        let result = operation(&mut proposed)?;
        if proposed != *current {
            self.persist_if_required(durability, mutation, applied_at_ms)?;
            *current = proposed;
        }
        Ok(result)
    }

    fn maintain_queue_at(&self, queue_name: &str, applied_at_ms: u64) -> EpochResult<()> {
        self.mutate_queue(
            queue_name,
            JournalMutation::MaintainQueue {
                name: queue_name.to_owned(),
                applied_at_ms,
            },
            applied_at_ms,
            |proposed| {
                proposed.maintain(applied_at_ms);
                Ok(())
            },
        )
    }

    fn set_stream_offset(
        &self,
        stream_name: &str,
        group: &str,
        partition: u32,
        next_offset: u64,
        reset: bool,
    ) -> EpochResult<()> {
        let stream = self.stream(stream_name)?;
        let mut current = stream.lock();
        let durability = current.config().durability;
        let mut proposed = current.clone();
        if reset {
            proposed.reset_offset(group, partition, next_offset)?;
        } else {
            proposed.commit_offset(group, partition, next_offset)?;
        }
        self.persist_if_required(
            durability,
            JournalMutation::SetStreamOffset {
                name: stream_name.to_owned(),
                group: group.to_owned(),
                partition,
                next_offset,
                reset,
            },
            self.now_ms(),
        )?;
        *current = proposed;
        Ok(())
    }

    fn persist_if_required(
        &self,
        durability: DurabilityProfile,
        mutation: JournalMutation,
        timestamp_ms: u64,
    ) -> EpochResult<()> {
        if durability == DurabilityProfile::Volatile {
            return Ok(());
        }
        let journal = self.journal.as_ref().ok_or_else(|| {
            EpochError::Unavailable("no commit log is configured for durable mutations".into())
        })?;
        let payload = serde_json::to_vec(&JournalEntry {
            format_version: JOURNAL_FORMAT_VERSION,
            mutation,
        })
        .map_err(|error| EpochError::Internal(format!("journal encoding failed: {error}")))?;
        journal.lock().append(timestamp_ms, &payload, true)?;
        Ok(())
    }

    fn recover(&self, records: Vec<LogRecord>) -> EpochResult<()> {
        for record in records {
            let entry: JournalEntry = serde_json::from_slice(&record.payload).map_err(|error| {
                EpochError::Storage(format!(
                    "journal sequence {} could not be decoded: {error}",
                    record.sequence
                ))
            })?;
            if entry.format_version != JOURNAL_FORMAT_VERSION {
                return Err(EpochError::Storage(format!(
                    "journal sequence {} uses unsupported engine format {}",
                    record.sequence, entry.format_version
                )));
            }
            self.replay_mutation(entry.mutation).map_err(|error| {
                EpochError::Storage(format!(
                    "journal sequence {} could not be applied: {error}",
                    record.sequence
                ))
            })?;
        }
        Ok(())
    }

    fn replay_mutation(&self, mutation: JournalMutation) -> EpochResult<()> {
        match mutation {
            JournalMutation::CreateStream { name, config } => {
                validate_resource_name(&name)?;
                self.validate_durability(ResourceKind::Stream, config.durability)?;
                ensure_local_durable(config.durability, ResourceKind::Stream)?;
                let stream = Arc::new(Mutex::new(Stream::new(config)?));
                insert_unique(&self.streams, &name, stream)
            }
            JournalMutation::AppendStream {
                name,
                envelope,
                partition,
                applied_at_ms,
            } => {
                let stream = self.stream(&name)?;
                let mut stream = stream.lock();
                ensure_local_durable(stream.config().durability, ResourceKind::Stream)?;
                stream.append(*envelope, Some(partition), applied_at_ms)?;
                Ok(())
            }
            JournalMutation::SetStreamOffset {
                name,
                group,
                partition,
                next_offset,
                reset,
            } => {
                let stream = self.stream(&name)?;
                let mut stream = stream.lock();
                ensure_local_durable(stream.config().durability, ResourceKind::Stream)?;
                if reset {
                    stream.reset_offset(group, partition, next_offset)
                } else {
                    stream.commit_offset(group, partition, next_offset)
                }
            }
            JournalMutation::CreateQueue { name, config } => {
                validate_resource_name(&name)?;
                self.validate_durability(ResourceKind::Queue, config.durability)?;
                ensure_local_durable(config.durability, ResourceKind::Queue)?;
                let queue = Arc::new(Mutex::new(Queue::new(config)?));
                insert_unique(&self.queues, &name, queue)
            }
            JournalMutation::EnqueueQueue {
                name,
                envelope,
                applied_at_ms,
            } => self.with_recovered_queue(&name, |queue| {
                queue.enqueue(*envelope, applied_at_ms).map(|_| ())
            }),
            JournalMutation::AcquireQueue {
                name,
                consumer,
                max_messages,
                visibility_timeout_ms,
                applied_at_ms,
            } => self.with_recovered_queue(&name, |queue| {
                queue.acquire(
                    &consumer,
                    max_messages as usize,
                    visibility_timeout_ms,
                    applied_at_ms,
                )?;
                Ok(())
            }),
            JournalMutation::SettleQueue {
                name,
                settlement,
                applied_at_ms,
            } => self.with_recovered_queue(&name, |queue| {
                replay_queue_settlement(queue, settlement, applied_at_ms)
            }),
            JournalMutation::RedriveQueue {
                name,
                message_id,
                applied_at_ms,
            } => {
                self.with_recovered_queue(&name, |queue| queue.redrive(&message_id, applied_at_ms))
            }
            JournalMutation::MaintainQueue {
                name,
                applied_at_ms,
            } => self.with_recovered_queue(&name, |queue| {
                queue.maintain(applied_at_ms);
                Ok(())
            }),
        }
    }

    fn with_recovered_queue<T>(
        &self,
        queue_name: &str,
        operation: impl FnOnce(&mut Queue) -> EpochResult<T>,
    ) -> EpochResult<T> {
        let queue = self.queue(queue_name)?;
        let mut queue = queue.lock();
        ensure_local_durable(queue.config().durability, ResourceKind::Queue)?;
        operation(&mut queue)
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
            guarantee_ceiling: self.guarantee_ceiling,
            hosted_control_plane_required: self.deployment_mode == DeploymentMode::Managed,
        }
    }

    pub fn maintain(&self, limit_per_cache: usize) -> EpochResult<()> {
        let now_ms = self.now_ms();
        for cache in self.caches.read().values() {
            cache.lock().purge_expired(now_ms, limit_per_cache);
        }
        let queue_names: Vec<String> = self.queues.read().keys().cloned().collect();
        for queue_name in queue_names {
            self.maintain_queue_at(&queue_name, now_ms)?;
        }
        Ok(())
    }

    fn validate_durability(
        &self,
        kind: ResourceKind,
        durability: DurabilityProfile,
    ) -> EpochResult<()> {
        match (kind, durability, self.guarantee_ceiling) {
            (_, DurabilityProfile::Volatile, _)
            | (
                ResourceKind::Stream | ResourceKind::Queue,
                DurabilityProfile::LocalDurable,
                DurabilityProfile::LocalDurable,
            ) => Ok(()),
            _ => Err(EpochError::InvalidArgument(format!(
                "durability {durability:?} is unavailable for {kind:?} in the current {:?} data-plane slice",
                self.deployment_mode
            ))),
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

fn ensure_local_durable(
    durability: DurabilityProfile,
    resource_kind: ResourceKind,
) -> EpochResult<()> {
    if durability == DurabilityProfile::LocalDurable {
        Ok(())
    } else {
        Err(EpochError::InvalidArgument(format!(
            "journal mutation targets a non-durable {resource_kind:?}"
        )))
    }
}

fn replay_queue_settlement(
    queue: &mut Queue,
    settlement: JournalQueueSettlement,
    applied_at_ms: u64,
) -> EpochResult<()> {
    match settlement {
        JournalQueueSettlement::Ack { token } => {
            queue.acknowledge(&token, applied_at_ms).map(|_| ())
        }
        JournalQueueSettlement::Release {
            token,
            delay_ms,
            reason,
        } => queue.release(&token, delay_ms, reason, applied_at_ms),
        JournalQueueSettlement::Reject { token, reason } => {
            queue.reject(&token, reason, applied_at_ms)
        }
        JournalQueueSettlement::Extend {
            token,
            extension_ms,
        } => queue
            .extend_lease(&token, extension_ms, applied_at_ms)
            .map(|_| ()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use epoch_bus::{EventFilter, EventTransform};
    use epoch_core::ManualClock;
    use epoch_storage::MemoryLog;
    use serde_json::json;

    use super::*;

    #[derive(Debug)]
    struct FailAfterLog {
        inner: MemoryLog,
        successful_appends: usize,
    }

    impl CommitLog for FailAfterLog {
        fn durability(&self) -> DurabilityProfile {
            DurabilityProfile::LocalDurable
        }

        fn append(
            &mut self,
            timestamp_ms: u64,
            payload: &[u8],
            durable: bool,
        ) -> EpochResult<LogRecord> {
            let appended = self
                .inner
                .last_sequence()
                .map_or(0, |sequence| sequence.saturating_add(1));
            if usize::try_from(appended).unwrap_or(usize::MAX) >= self.successful_appends {
                return Err(EpochError::Storage("injected fsync failure".into()));
            }
            self.inner.append(timestamp_ms, payload, durable)
        }

        fn records_from(&self, sequence: u64, limit: usize) -> Vec<LogRecord> {
            self.inner.records_from(sequence, limit)
        }

        fn last_sequence(&self) -> Option<u64> {
            self.inner.last_sequence()
        }
    }

    #[derive(Debug, Clone, Default)]
    struct SharedMemoryLog {
        inner: Arc<Mutex<MemoryLog>>,
    }

    impl CommitLog for SharedMemoryLog {
        fn durability(&self) -> DurabilityProfile {
            DurabilityProfile::LocalDurable
        }

        fn append(
            &mut self,
            timestamp_ms: u64,
            payload: &[u8],
            durable: bool,
        ) -> EpochResult<LogRecord> {
            self.inner.lock().append(timestamp_ms, payload, durable)
        }

        fn records_from(&self, sequence: u64, limit: usize) -> Vec<LogRecord> {
            self.inner.lock().records_from(sequence, limit)
        }

        fn last_sequence(&self) -> Option<u64> {
            self.inner.lock().last_sequence()
        }
    }

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
        assert_eq!(health.guarantee_ceiling, DurabilityProfile::Volatile);
        assert!(!health.hosted_control_plane_required);
    }

    #[test]
    fn failed_durable_append_does_not_mutate_stream_memory() {
        let engine = EpochEngine::with_commit_log(
            DeploymentMode::Standalone,
            Arc::new(ManualClock::new(100)),
            Box::new(FailAfterLog {
                inner: MemoryLog::default(),
                successful_appends: 1,
            }),
        )
        .unwrap();
        engine
            .create_stream(
                "audit",
                StreamConfig {
                    durability: DurabilityProfile::LocalDurable,
                    ..StreamConfig::default()
                },
            )
            .unwrap();

        let result = engine.append_stream(
            "audit",
            EventEnvelope::new("tests", "audit.created", json!({"id": 1}), 100),
            None,
        );

        assert!(matches!(result, Err(EpochError::Storage(_))));
        assert!(
            engine
                .stream("audit")
                .unwrap()
                .lock()
                .fetch(0, 0, 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn failed_durable_enqueue_does_not_mutate_queue_memory() {
        let engine = EpochEngine::with_commit_log(
            DeploymentMode::Standalone,
            Arc::new(ManualClock::new(100)),
            Box::new(FailAfterLog {
                inner: MemoryLog::default(),
                successful_appends: 1,
            }),
        )
        .unwrap();
        engine
            .create_queue(
                "jobs",
                QueueConfig {
                    durability: DurabilityProfile::LocalDurable,
                    ..QueueConfig::default()
                },
            )
            .unwrap();

        let result = engine.enqueue(
            "jobs",
            EventEnvelope::new("tests", "job.requested", json!({"id": 1}), 100),
        );

        assert!(matches!(result, Err(EpochError::Storage(_))));
        assert_eq!(engine.queue("jobs").unwrap().lock().counts().ready, 0);
    }

    #[test]
    fn failed_durable_settlement_keeps_the_live_lease() {
        let engine = EpochEngine::with_commit_log(
            DeploymentMode::Standalone,
            Arc::new(ManualClock::new(100)),
            Box::new(FailAfterLog {
                inner: MemoryLog::default(),
                successful_appends: 3,
            }),
        )
        .unwrap();
        engine
            .create_queue(
                "jobs",
                QueueConfig {
                    durability: DurabilityProfile::LocalDurable,
                    ..QueueConfig::default()
                },
            )
            .unwrap();
        engine
            .enqueue(
                "jobs",
                EventEnvelope::new("tests", "job.requested", json!({"id": 1}), 100),
            )
            .unwrap();
        let delivery = engine
            .acquire_queue("jobs", "worker", 1, None)
            .unwrap()
            .remove(0);

        let result = engine.acknowledge_queue("jobs", &delivery.lease_token);

        assert!(matches!(result, Err(EpochError::Storage(_))));
        let counts = engine.queue("jobs").unwrap().lock().counts();
        assert_eq!(counts.in_flight, 1);
        assert_eq!(counts.acknowledged, 0);
    }

    #[test]
    fn durable_maintenance_replays_lease_expiry_and_fences_the_old_token() {
        let log = SharedMemoryLog::default();
        let clock = Arc::new(ManualClock::new(100));
        let engine = EpochEngine::with_commit_log(
            DeploymentMode::Standalone,
            clock.clone(),
            Box::new(log.clone()),
        )
        .unwrap();
        engine
            .create_queue(
                "jobs",
                QueueConfig {
                    durability: DurabilityProfile::LocalDurable,
                    visibility_timeout_ms: 10,
                    retry: epoch_queue::RetryPolicy {
                        initial_delay_ms: 0,
                        max_delay_ms: 0,
                        jitter_percent: 0,
                        ..epoch_queue::RetryPolicy::default()
                    },
                    ..QueueConfig::default()
                },
            )
            .unwrap();
        engine
            .enqueue(
                "jobs",
                EventEnvelope::new("tests", "job.requested", json!({"id": 1}), 100),
            )
            .unwrap();
        let stale_token = engine
            .acquire_queue("jobs", "worker-a", 1, None)
            .unwrap()
            .remove(0)
            .lease_token;
        clock.set(110);
        engine.maintain(1_000).unwrap();
        drop(engine);

        let recovered = EpochEngine::with_commit_log(
            DeploymentMode::Standalone,
            Arc::new(ManualClock::new(110)),
            Box::new(log),
        )
        .unwrap();

        assert_eq!(recovered.queue("jobs").unwrap().lock().counts().ready, 1);
        assert!(matches!(
            recovered.acknowledge_queue("jobs", &stale_token),
            Err(EpochError::Fenced)
        ));
    }

    #[test]
    fn journal_create_stream_encoding_matches_golden_vector() {
        let encoded = serde_json::to_string(&JournalEntry {
            format_version: JOURNAL_FORMAT_VERSION,
            mutation: JournalMutation::CreateStream {
                name: "audit".into(),
                config: StreamConfig {
                    durability: DurabilityProfile::LocalDurable,
                    ..StreamConfig::default()
                },
            },
        })
        .unwrap();

        assert_eq!(
            format!("{encoded}\n"),
            include_str!("../../../spec/formats/engine-journal-v1-create-stream.json")
        );
    }

    #[test]
    fn journal_create_queue_encoding_matches_golden_vector() {
        let encoded = serde_json::to_string(&JournalEntry {
            format_version: JOURNAL_FORMAT_VERSION,
            mutation: JournalMutation::CreateQueue {
                name: "jobs".into(),
                config: QueueConfig {
                    durability: DurabilityProfile::LocalDurable,
                    ..QueueConfig::default()
                },
            },
        })
        .unwrap();

        assert_eq!(
            format!("{encoded}\n"),
            include_str!("../../../spec/formats/engine-journal-v1-create-queue.json")
        );
    }

    #[test]
    fn recovery_rejects_an_unknown_engine_journal_version() {
        let payload = serde_json::to_vec(&JournalEntry {
            format_version: JOURNAL_FORMAT_VERSION + 1,
            mutation: JournalMutation::CreateStream {
                name: "audit".into(),
                config: StreamConfig {
                    durability: DurabilityProfile::LocalDurable,
                    ..StreamConfig::default()
                },
            },
        })
        .unwrap();
        let mut inner = MemoryLog::default();
        inner.append(100, &payload, true).unwrap();

        let result = EpochEngine::with_commit_log(
            DeploymentMode::Standalone,
            Arc::new(ManualClock::new(100)),
            Box::new(FailAfterLog {
                inner,
                successful_appends: usize::MAX,
            }),
        );

        assert!(matches!(
            result,
            Err(EpochError::Storage(message))
                if message.contains("unsupported engine format 2")
        ));
    }
}
