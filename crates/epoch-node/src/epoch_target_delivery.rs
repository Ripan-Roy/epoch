//! Leader-owned Event Bus delivery into regional Queue and Stream tablets.

use std::{
    fmt::{self, Formatter},
    sync::{Arc, RwLock},
    time::Duration,
};

use epoch_bus::{
    EpochTargetDeliveryCandidate, EpochTargetDestination, EpochTargetKind, SubscriptionTarget,
};
use epoch_catalog::ResourceName;
use epoch_consensus::ConsensusRole;
use epoch_core::{Clock, DurabilityProfile, EventEnvelope, ResourceKind, WorkloadProfile};
use epoch_queue::QueueConfig;
use epoch_stream::stream_partition_for;
use epoch_tablet::{
    BusTabletCommand, BusTabletDelivery, BusTabletOperation, BusTabletOperationResult,
    BusTabletOutcome, QueueBindDeadLetterForwardCommand, QueueCompleteDeadLetterForwardCommand,
    QueueTabletCommand, QueueTabletDeadLetterForward, QueueTabletDeadLetterForwardStatus,
    QueueTabletOperation, QueueTabletOperationResult, QueueTabletOutcome, StreamTabletCommand,
    StreamTabletMutationReceipt,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    bus_tablet::BusTabletService,
    delivery_proposal::{ProposalRoute, propose_and_wait},
    queue_tablet::QueueTabletService,
    tablet_materializer::{MaterializedTabletRoute, TabletDirectory},
};

pub const DEFAULT_EPOCH_TARGET_DELIVERY_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_EPOCH_TARGET_DELIVERY_INTERVAL: Duration = Duration::from_mins(1);
const EPOCH_TARGET_DISPATCHER: &str = "epoch-target-v1";
const EPOCH_TARGET_DISPATCHER_EPOCH: u64 = 1;

#[derive(Debug, Clone)]
pub struct EpochTargetDeliveryConfig {
    pub interval: Duration,
}

impl Default for EpochTargetDeliveryConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_EPOCH_TARGET_DELIVERY_INTERVAL,
        }
    }
}

impl EpochTargetDeliveryConfig {
    pub fn validate(&self) -> Result<(), EpochTargetDeliveryError> {
        if self.interval.is_zero() || self.interval > MAX_EPOCH_TARGET_DELIVERY_INTERVAL {
            return Err(EpochTargetDeliveryError::Configuration(format!(
                "Epoch target delivery interval must be between 1 ms and {} ms",
                MAX_EPOCH_TARGET_DELIVERY_INTERVAL.as_millis()
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct EpochTargetDeliveryWorker {
    config: EpochTargetDeliveryConfig,
    commit_wait: Duration,
}

impl EpochTargetDeliveryWorker {
    pub fn new(
        config: EpochTargetDeliveryConfig,
        commit_wait: Duration,
    ) -> Result<Self, EpochTargetDeliveryError> {
        config.validate()?;
        if commit_wait.is_zero() {
            return Err(EpochTargetDeliveryError::Configuration(
                "Epoch target proposal commit wait must be non-zero".into(),
            ));
        }
        Ok(Self {
            config,
            commit_wait,
        })
    }

    pub const fn config(&self) -> &EpochTargetDeliveryConfig {
        &self.config
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EpochTargetDeliveryPass {
    pub tablets_examined: u64,
    pub leaders_examined: u64,
    pub subscriptions_examined: u64,
    pub leases_acquired: u64,
    pub queue_enqueued: u64,
    pub stream_appended: u64,
    pub retry_scheduled: u64,
    pub dead_lettered: u64,
    pub errors: u64,
}

#[derive(Default)]
pub struct EpochTargetDeliveryStatus {
    interval_ms: u64,
    passes: std::sync::atomic::AtomicU64,
    tablets_examined: std::sync::atomic::AtomicU64,
    leaders_examined: std::sync::atomic::AtomicU64,
    subscriptions_examined: std::sync::atomic::AtomicU64,
    leases_acquired: std::sync::atomic::AtomicU64,
    queue_enqueued: std::sync::atomic::AtomicU64,
    stream_appended: std::sync::atomic::AtomicU64,
    retry_scheduled: std::sync::atomic::AtomicU64,
    dead_lettered: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    last_pass_at_ms: std::sync::atomic::AtomicU64,
    last_error: RwLock<Option<String>>,
}

impl fmt::Debug for EpochTargetDeliveryStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EpochTargetDeliveryStatus")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl EpochTargetDeliveryStatus {
    pub fn new(interval: Duration) -> Arc<Self> {
        Arc::new(Self {
            interval_ms: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
            ..Self::default()
        })
    }

    pub fn record(&self, now_ms: u64, pass: EpochTargetDeliveryPass, last_error: Option<String>) {
        use std::sync::atomic::Ordering;
        self.passes.fetch_add(1, Ordering::Relaxed);
        self.tablets_examined
            .fetch_add(pass.tablets_examined, Ordering::Relaxed);
        self.leaders_examined
            .fetch_add(pass.leaders_examined, Ordering::Relaxed);
        self.subscriptions_examined
            .fetch_add(pass.subscriptions_examined, Ordering::Relaxed);
        self.leases_acquired
            .fetch_add(pass.leases_acquired, Ordering::Relaxed);
        self.queue_enqueued
            .fetch_add(pass.queue_enqueued, Ordering::Relaxed);
        self.stream_appended
            .fetch_add(pass.stream_appended, Ordering::Relaxed);
        self.retry_scheduled
            .fetch_add(pass.retry_scheduled, Ordering::Relaxed);
        self.dead_lettered
            .fetch_add(pass.dead_lettered, Ordering::Relaxed);
        self.errors.fetch_add(pass.errors, Ordering::Relaxed);
        self.last_pass_at_ms.store(now_ms, Ordering::Relaxed);
        if let Some(last_error) = last_error
            && let Ok(mut error) = self.last_error.write()
        {
            *error = Some(last_error);
        }
    }

    pub fn snapshot(&self) -> EpochTargetDeliveryStatusSnapshot {
        use std::sync::atomic::Ordering;
        EpochTargetDeliveryStatusSnapshot {
            enabled: true,
            interval_ms: self.interval_ms,
            passes: self.passes.load(Ordering::Relaxed),
            tablets_examined: self.tablets_examined.load(Ordering::Relaxed),
            leaders_examined: self.leaders_examined.load(Ordering::Relaxed),
            subscriptions_examined: self.subscriptions_examined.load(Ordering::Relaxed),
            leases_acquired: self.leases_acquired.load(Ordering::Relaxed),
            queue_enqueued: self.queue_enqueued.load(Ordering::Relaxed),
            stream_appended: self.stream_appended.load(Ordering::Relaxed),
            retry_scheduled: self.retry_scheduled.load(Ordering::Relaxed),
            dead_lettered: self.dead_lettered.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            last_pass_at_ms: self.last_pass_at_ms.load(Ordering::Relaxed),
            last_error: self.last_error.read().ok().and_then(|error| error.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EpochTargetDeliveryStatusSnapshot {
    pub enabled: bool,
    pub interval_ms: u64,
    pub passes: u64,
    pub tablets_examined: u64,
    pub leaders_examined: u64,
    pub subscriptions_examined: u64,
    pub leases_acquired: u64,
    pub queue_enqueued: u64,
    pub stream_appended: u64,
    pub retry_scheduled: u64,
    pub dead_lettered: u64,
    pub errors: u64,
    pub last_pass_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Error)]
pub enum EpochTargetDeliveryError {
    #[error("invalid Epoch target delivery configuration: {0}")]
    Configuration(String),
    #[error("Epoch target delivery state is unavailable: {0}")]
    State(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TargetDisposition {
    Acknowledge(EpochTargetKind),
    Retry(&'static str),
    Reject(&'static str),
}

pub async fn run_epoch_target_delivery_pass(
    directory: &TabletDirectory,
    worker: &EpochTargetDeliveryWorker,
    clock: &dyn Clock,
) -> (EpochTargetDeliveryPass, Option<String>) {
    let routes = match directory.routes() {
        Ok(routes) => routes,
        Err(error) => {
            return (
                EpochTargetDeliveryPass {
                    errors: 1,
                    ..EpochTargetDeliveryPass::default()
                },
                Some(error.to_string()),
            );
        }
    };
    let mut pass = EpochTargetDeliveryPass::default();
    let mut last_error = None;
    for route in routes {
        pass.tablets_examined = pass.tablets_examined.saturating_add(1);
        if let Some(service) = route.bus_service()
            && let Err(error) =
                dispatch_route(directory, &route, &service, worker, clock, &mut pass).await
        {
            pass.errors = pass.errors.saturating_add(1);
            last_error = Some(error.to_string());
        }
        if let Some(service) = route.queue_service()
            && let Err(error) =
                dispatch_queue_dead_letters(directory, &route, &service, worker, clock, &mut pass)
                    .await
        {
            pass.errors = pass.errors.saturating_add(1);
            last_error = Some(error.to_string());
        }
    }
    (pass, last_error)
}

async fn dispatch_queue_dead_letters(
    directory: &TabletDirectory,
    source_route: &MaterializedTabletRoute,
    source_service: &QueueTabletService,
    worker: &EpochTargetDeliveryWorker,
    clock: &dyn Clock,
    pass: &mut EpochTargetDeliveryPass,
) -> Result<(), EpochTargetDeliveryError> {
    let consensus = source_route.consensus();
    let status = consensus
        .status()
        .await
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Ok(());
    }
    let forwards = source_service
        .pending_dead_letter_forwards(128)
        .map_err(EpochTargetDeliveryError::State)?;
    if forwards.is_empty() {
        return Ok(());
    }
    pass.leaders_examined = pass.leaders_examined.saturating_add(1);
    for forward in forwards {
        pass.subscriptions_examined = pass.subscriptions_examined.saturating_add(1);
        dispatch_queue_dead_letter(
            directory,
            source_route,
            source_service,
            worker,
            clock,
            forward,
        )
        .await?;
        pass.queue_enqueued = pass.queue_enqueued.saturating_add(1);
    }
    Ok(())
}

async fn dispatch_queue_dead_letter(
    directory: &TabletDirectory,
    source_route: &MaterializedTabletRoute,
    source_service: &QueueTabletService,
    worker: &EpochTargetDeliveryWorker,
    clock: &dyn Clock,
    forward: QueueTabletDeadLetterForward,
) -> Result<(), EpochTargetDeliveryError> {
    let Some((target_route, destination)) = bind_or_resolve_queue_dead_letter(
        directory,
        source_route,
        source_service,
        worker,
        clock,
        &forward,
    )
    .await?
    else {
        return Ok(());
    };

    let idempotency_key =
        queue_dead_letter_target_key(source_route, forward.dead_letter_history_id, &destination);
    let target_message_id = enqueue_queue_target(
        &target_route,
        idempotency_key,
        forward.envelope.into(),
        clock.wall_time_ms(),
        worker.commit_wait,
    )
    .await
    .map_err(|disposition| {
        EpochTargetDeliveryError::State(format!(
            "Queue dead-letter target is unresolved: {disposition:?}"
        ))
    })?;

    complete_queue_dead_letter(
        source_route,
        source_service,
        worker,
        clock,
        forward.dead_letter_history_id,
        destination,
        target_message_id,
    )
    .await
}

async fn bind_or_resolve_queue_dead_letter(
    directory: &TabletDirectory,
    source_route: &MaterializedTabletRoute,
    source_service: &QueueTabletService,
    worker: &EpochTargetDeliveryWorker,
    clock: &dyn Clock,
    forward: &QueueTabletDeadLetterForward,
) -> Result<Option<(MaterializedTabletRoute, EpochTargetDestination)>, EpochTargetDeliveryError> {
    match forward.status {
        QueueTabletDeadLetterForwardStatus::Pending => {
            let (route, destination) =
                resolve_queue_dead_letter_destination(directory, source_route, &forward.target)?;
            let status = source_route
                .consensus()
                .status()
                .await
                .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
            if status.role != ConsensusRole::Leader || status.fail_stopped {
                return Ok(None);
            }
            let command = QueueTabletCommand::new(
                source_service.scope(),
                queue_forward_command_key("bind", forward.dead_letter_history_id, &destination),
                clock.wall_time_ms().max(
                    source_service
                        .last_applied_time_ms()
                        .map_err(EpochTargetDeliveryError::State)?,
                ),
                QueueTabletOperation::BindDeadLetterForward(QueueBindDeadLetterForwardCommand {
                    partition: 0,
                    dead_letter_history_id: forward.dead_letter_history_id,
                    destination: destination.clone(),
                }),
            )
            .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
            let receipt = propose_queue_command(
                &source_route.consensus(),
                source_service,
                command,
                status.term.get(),
                worker.commit_wait,
                "Queue dead-letter binding",
            )
            .await?;
            if !matches!(
                receipt.outcome,
                QueueTabletOutcome::Applied {
                    result: QueueTabletOperationResult::DeadLetterForwardBound { .. }
                }
            ) {
                return Err(EpochTargetDeliveryError::State(
                    "Queue dead-letter binding was rejected".into(),
                ));
            }
            Ok(Some((route, destination)))
        }
        QueueTabletDeadLetterForwardStatus::Bound => {
            let destination = forward.destination.clone().ok_or_else(|| {
                EpochTargetDeliveryError::State(
                    "bound Queue dead-letter forward has no destination".into(),
                )
            })?;
            let route = resolve_bound_queue_dead_letter_destination(
                directory,
                source_route,
                &forward.target,
                &destination,
            )?;
            Ok(Some((route, destination)))
        }
        QueueTabletDeadLetterForwardStatus::Completed => Ok(None),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "completion requires the source lease, stable delivery identity, and target receipt"
)]
async fn complete_queue_dead_letter(
    source_route: &MaterializedTabletRoute,
    source_service: &QueueTabletService,
    worker: &EpochTargetDeliveryWorker,
    clock: &dyn Clock,
    dead_letter_history_id: u64,
    destination: EpochTargetDestination,
    target_message_id: String,
) -> Result<(), EpochTargetDeliveryError> {
    let status = source_route
        .consensus()
        .status()
        .await
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Ok(());
    }
    let command = QueueTabletCommand::new(
        source_service.scope(),
        queue_forward_command_key("complete", dead_letter_history_id, &destination),
        clock.wall_time_ms().max(
            source_service
                .last_applied_time_ms()
                .map_err(EpochTargetDeliveryError::State)?,
        ),
        QueueTabletOperation::CompleteDeadLetterForward(QueueCompleteDeadLetterForwardCommand {
            partition: 0,
            dead_letter_history_id,
            destination,
            target_message_id,
        }),
    )
    .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    let receipt = propose_queue_command(
        &source_route.consensus(),
        source_service,
        command,
        status.term.get(),
        worker.commit_wait,
        "Queue dead-letter completion",
    )
    .await?;
    if !matches!(
        receipt.outcome,
        QueueTabletOutcome::Applied {
            result: QueueTabletOperationResult::DeadLetterForwardCompleted { .. }
        }
    ) {
        return Err(EpochTargetDeliveryError::State(
            "Queue dead-letter completion was rejected".into(),
        ));
    }
    Ok(())
}

async fn propose_queue_command(
    consensus: &crate::consensus::ConsensusProbeHandle,
    service: &QueueTabletService,
    command: QueueTabletCommand,
    expected_term: u64,
    commit_wait: Duration,
    label: &str,
) -> Result<epoch_tablet::QueueTabletReceipt, EpochTargetDeliveryError> {
    let proposal_id = command
        .proposal_id(service.scope())
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    let payload = command
        .encode(service.scope())
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    let committed = propose_and_wait(
        consensus,
        proposal_id,
        expected_term,
        payload,
        commit_wait,
        label,
        ProposalRoute::LeaderOnly,
    )
    .await
    .map_err(EpochTargetDeliveryError::State)?;
    service
        .committed_receipt(&committed)
        .map_err(EpochTargetDeliveryError::State)
}

async fn dispatch_route(
    directory: &TabletDirectory,
    source_route: &MaterializedTabletRoute,
    service: &BusTabletService,
    worker: &EpochTargetDeliveryWorker,
    clock: &dyn Clock,
    pass: &mut EpochTargetDeliveryPass,
) -> Result<(), EpochTargetDeliveryError> {
    let source_consensus = source_route.consensus();
    let status = source_consensus
        .status()
        .await
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Ok(());
    }
    pass.leaders_examined = pass.leaders_examined.saturating_add(1);
    let now_ms = clock.wall_time_ms();
    let candidates = service
        .epoch_target_delivery_candidates(now_ms)
        .map_err(EpochTargetDeliveryError::State)?;
    for candidate in candidates {
        pass.subscriptions_examined = pass.subscriptions_examined.saturating_add(1);
        dispatch_candidate(
            directory,
            source_route,
            service,
            worker,
            clock,
            pass,
            candidate,
            now_ms,
        )
        .await?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the delivery candidate carries independent source, target, clock, and pass evidence"
)]
async fn dispatch_candidate(
    directory: &TabletDirectory,
    source_route: &MaterializedTabletRoute,
    source_service: &BusTabletService,
    worker: &EpochTargetDeliveryWorker,
    clock: &dyn Clock,
    pass: &mut EpochTargetDeliveryPass,
    candidate: EpochTargetDeliveryCandidate,
    observed_at_ms: u64,
) -> Result<(), EpochTargetDeliveryError> {
    let binding = match candidate.destination.clone() {
        Some(binding) => binding,
        None => match resolve_new_destination(directory, source_route, &candidate) {
            Ok((_, binding)) => binding,
            Err(error) => return Err(EpochTargetDeliveryError::State(error)),
        },
    };
    let source_consensus = source_route.consensus();
    let source_status = source_consensus
        .status()
        .await
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    if source_status.role != ConsensusRole::Leader || source_status.fail_stopped {
        return Ok(());
    }
    let acquire_time = observed_at_ms.max(
        source_service
            .last_applied_time_ms()
            .map_err(EpochTargetDeliveryError::State)?,
    );
    let acquire_key = attempt_idempotency_key(
        "epoch-target-acquire",
        &candidate.delivery_id,
        candidate.next_attempt,
    );
    let command = BusTabletCommand::new(
        source_service.scope(),
        acquire_key,
        acquire_time,
        BusTabletOperation::AcquireDeliveries {
            subscription: candidate.subscription,
            dispatcher: EPOCH_TARGET_DISPATCHER.into(),
            dispatcher_epoch: EPOCH_TARGET_DISPATCHER_EPOCH,
            max_deliveries: 1,
            expected_delivery_id: Some(candidate.delivery_id),
            destination: Some(binding.clone()),
        },
    )
    .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    let receipt = propose_bus_command(
        &source_consensus,
        source_service,
        command,
        source_status.term.get(),
        worker.commit_wait,
        "Epoch target acquire",
    )
    .await?;
    let BusTabletOutcome::Applied {
        result: BusTabletOperationResult::DeliveriesAcquired { deliveries },
    } = receipt.outcome
    else {
        return Ok(());
    };
    let Some(delivery) = deliveries.into_iter().next() else {
        return Ok(());
    };
    pass.leases_acquired = pass.leases_acquired.saturating_add(1);

    let disposition = execute_target(
        directory,
        source_route,
        &delivery,
        &binding,
        clock.wall_time_ms(),
        worker.commit_wait,
    )
    .await;
    settle_source(
        &source_consensus,
        source_service,
        clock,
        pass,
        &delivery,
        disposition,
        worker.commit_wait,
    )
    .await
}

async fn execute_target(
    directory: &TabletDirectory,
    source_route: &MaterializedTabletRoute,
    delivery: &BusTabletDelivery,
    binding: &EpochTargetDestination,
    now_ms: u64,
    commit_wait: Duration,
) -> TargetDisposition {
    if delivery.destination.as_ref() != Some(binding) {
        return TargetDisposition::Reject("destination_binding_mismatch");
    }
    let Ok(route) = resolve_bound_destination(directory, source_route, &delivery.target, binding)
    else {
        return TargetDisposition::Retry("destination_binding_unavailable");
    };
    let idempotency_key = destination_idempotency_key(source_route, delivery, binding);
    let envelope: EventEnvelope = delivery.envelope.clone().into();
    match binding.kind {
        EpochTargetKind::Queue => {
            execute_queue_target(&route, idempotency_key, envelope, now_ms, commit_wait).await
        }
        EpochTargetKind::Stream => {
            execute_stream_target(&route, idempotency_key, envelope, now_ms, commit_wait).await
        }
    }
}

async fn execute_queue_target(
    route: &MaterializedTabletRoute,
    idempotency_key: String,
    envelope: EventEnvelope,
    now_ms: u64,
    commit_wait: Duration,
) -> TargetDisposition {
    match enqueue_queue_target(route, idempotency_key, envelope, now_ms, commit_wait).await {
        Ok(_) => TargetDisposition::Acknowledge(EpochTargetKind::Queue),
        Err(disposition) => disposition,
    }
}

async fn enqueue_queue_target(
    route: &MaterializedTabletRoute,
    idempotency_key: String,
    envelope: EventEnvelope,
    now_ms: u64,
    commit_wait: Duration,
) -> Result<String, TargetDisposition> {
    let Some(service) = route.queue_service() else {
        return Err(TargetDisposition::Reject("destination_profile_mismatch"));
    };
    let consensus = route.consensus();
    let status = match consensus.status().await {
        Ok(status) if !status.fail_stopped && status.leader_id.is_some() => status,
        Ok(_) | Err(_) => return Err(TargetDisposition::Retry("destination_unavailable")),
    };
    let applied_at_ms = match service.last_applied_time_ms() {
        Ok(last) => now_ms.max(last),
        Err(_) => return Err(TargetDisposition::Retry("destination_unavailable")),
    };
    let Ok(command) =
        QueueTabletCommand::enqueue(service.scope(), idempotency_key, envelope, applied_at_ms)
    else {
        return Err(TargetDisposition::Reject("invalid_destination_command"));
    };
    let Ok(proposal_id) = command.proposal_id(service.scope()) else {
        return Err(TargetDisposition::Reject("invalid_destination_command"));
    };
    let Ok(payload) = command.encode(service.scope()) else {
        return Err(TargetDisposition::Reject("invalid_destination_command"));
    };
    let Ok(committed) = propose_and_wait(
        &consensus,
        proposal_id,
        status.term.get(),
        payload,
        commit_wait,
        "Epoch Queue target",
        ProposalRoute::ForwardToKnownLeader,
    )
    .await
    else {
        return Err(TargetDisposition::Retry("destination_commit_unknown"));
    };
    let Ok(receipt) = service.committed_receipt(&committed) else {
        return Err(TargetDisposition::Retry("destination_commit_unknown"));
    };
    match receipt.outcome {
        QueueTabletOutcome::Applied {
            result: QueueTabletOperationResult::Enqueued { message_id, .. },
        } => Ok(message_id),
        QueueTabletOutcome::Rejected { code, .. } => {
            Err(TargetDisposition::Reject(queue_rejection_reason(code)))
        }
        QueueTabletOutcome::Applied { .. } => {
            Err(TargetDisposition::Reject("unexpected_destination_receipt"))
        }
    }
}

async fn execute_stream_target(
    route: &MaterializedTabletRoute,
    idempotency_key: String,
    envelope: EventEnvelope,
    now_ms: u64,
    commit_wait: Duration,
) -> TargetDisposition {
    let Some(service) = route.stream_service() else {
        return TargetDisposition::Reject("destination_profile_mismatch");
    };
    let consensus = route.consensus();
    let status = match consensus.status().await {
        Ok(status) if !status.fail_stopped && status.leader_id.is_some() => status,
        Ok(_) | Err(_) => return TargetDisposition::Retry("destination_unavailable"),
    };
    let Ok(command) =
        StreamTabletCommand::append(service.scope(), idempotency_key, envelope, now_ms)
    else {
        return TargetDisposition::Reject("invalid_destination_command");
    };
    let Ok(proposal_id) = command.proposal_id(service.scope()) else {
        return TargetDisposition::Reject("invalid_destination_command");
    };
    let Ok(payload) = command.encode(service.scope()) else {
        return TargetDisposition::Reject("invalid_destination_command");
    };
    let Ok(committed) = propose_and_wait(
        &consensus,
        proposal_id,
        status.term.get(),
        payload,
        commit_wait,
        "Epoch Stream target",
        ProposalRoute::ForwardToKnownLeader,
    )
    .await
    else {
        return TargetDisposition::Retry("destination_commit_unknown");
    };
    let Ok(receipt) = service.committed_receipt(&committed) else {
        return TargetDisposition::Retry("destination_commit_unknown");
    };
    match receipt {
        StreamTabletMutationReceipt::Append(_) => {
            TargetDisposition::Acknowledge(EpochTargetKind::Stream)
        }
        StreamTabletMutationReceipt::Group(_)
        | StreamTabletMutationReceipt::Retention(_)
        | StreamTabletMutationReceipt::Session(_)
        | StreamTabletMutationReceipt::State(_) => {
            TargetDisposition::Reject("unexpected_destination_receipt")
        }
    }
}

async fn settle_source(
    consensus: &crate::consensus::ConsensusProbeHandle,
    service: &BusTabletService,
    clock: &dyn Clock,
    pass: &mut EpochTargetDeliveryPass,
    delivery: &BusTabletDelivery,
    disposition: TargetDisposition,
    commit_wait: Duration,
) -> Result<(), EpochTargetDeliveryError> {
    let status = consensus.status().await.map_err(|error| {
        EpochTargetDeliveryError::State(format!(
            "destination outcome must be resolved after status failure: {error}"
        ))
    })?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Ok(());
    }
    let (operation, label) = settlement_operation(delivery, &disposition);
    let key = attempt_idempotency_key(label, &delivery.delivery_id, delivery.attempt);
    let applied_at_ms = clock.wall_time_ms().max(
        service
            .last_applied_time_ms()
            .map_err(EpochTargetDeliveryError::State)?,
    );
    let command = BusTabletCommand::new(service.scope(), key, applied_at_ms, operation)
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    let receipt = propose_bus_command(
        consensus,
        service,
        command,
        status.term.get(),
        commit_wait,
        "Epoch target settlement",
    )
    .await?;
    match receipt.outcome {
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::DeliveryAcknowledged { .. },
        } => match disposition {
            TargetDisposition::Acknowledge(EpochTargetKind::Queue) => {
                pass.queue_enqueued = pass.queue_enqueued.saturating_add(1);
            }
            TargetDisposition::Acknowledge(EpochTargetKind::Stream) => {
                pass.stream_appended = pass.stream_appended.saturating_add(1);
            }
            TargetDisposition::Retry(_) | TargetDisposition::Reject(_) => {}
        },
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::DeliveryFailed { .. },
        } => pass.retry_scheduled = pass.retry_scheduled.saturating_add(1),
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::DeliveryRejected { .. },
        } => pass.dead_lettered = pass.dead_lettered.saturating_add(1),
        BusTabletOutcome::Applied { .. } | BusTabletOutcome::Rejected { .. } => {}
    }
    Ok(())
}

async fn propose_bus_command(
    consensus: &crate::consensus::ConsensusProbeHandle,
    service: &BusTabletService,
    command: BusTabletCommand,
    expected_term: u64,
    commit_wait: Duration,
    label: &str,
) -> Result<epoch_tablet::BusTabletReceipt, EpochTargetDeliveryError> {
    let proposal_id = command
        .proposal_id(service.scope())
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    let payload = command
        .encode(service.scope())
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    let committed = propose_and_wait(
        consensus,
        proposal_id,
        expected_term,
        payload,
        commit_wait,
        label,
        ProposalRoute::LeaderOnly,
    )
    .await
    .map_err(EpochTargetDeliveryError::State)?;
    service
        .committed_receipt(&committed)
        .map_err(EpochTargetDeliveryError::State)
}

fn settlement_operation(
    delivery: &BusTabletDelivery,
    disposition: &TargetDisposition,
) -> (BusTabletOperation, &'static str) {
    match disposition {
        TargetDisposition::Acknowledge(_) => (
            BusTabletOperation::AcknowledgeDelivery {
                delivery_id: delivery.delivery_id.clone(),
                dispatcher: EPOCH_TARGET_DISPATCHER.into(),
                dispatcher_epoch: EPOCH_TARGET_DISPATCHER_EPOCH,
                lease_token: delivery.lease_token.clone(),
            },
            "epoch-target-ack",
        ),
        TargetDisposition::Retry(reason) => (
            BusTabletOperation::FailDelivery {
                delivery_id: delivery.delivery_id.clone(),
                dispatcher: EPOCH_TARGET_DISPATCHER.into(),
                dispatcher_epoch: EPOCH_TARGET_DISPATCHER_EPOCH,
                lease_token: delivery.lease_token.clone(),
                reason: (*reason).into(),
            },
            "epoch-target-retry",
        ),
        TargetDisposition::Reject(reason) => (
            BusTabletOperation::RejectDelivery {
                delivery_id: delivery.delivery_id.clone(),
                dispatcher: EPOCH_TARGET_DISPATCHER.into(),
                dispatcher_epoch: EPOCH_TARGET_DISPATCHER_EPOCH,
                lease_token: delivery.lease_token.clone(),
                reason: (*reason).into(),
            },
            "epoch-target-reject",
        ),
    }
}

fn resolve_new_destination(
    directory: &TabletDirectory,
    source_route: &MaterializedTabletRoute,
    candidate: &EpochTargetDeliveryCandidate,
) -> Result<(MaterializedTabletRoute, EpochTargetDestination), String> {
    let (resource, kind) = target_resource(source_route, &candidate.target)?;
    let shard_index = match kind {
        EpochTargetKind::Queue => 0,
        EpochTargetKind::Stream => {
            let shard_zero = directory
                .resource_route(&resource, 0)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("target resource {} is not materialized", resource.name))?;
            validate_target_route(&shard_zero, &resource, kind, None)?;
            stream_partition_for(&candidate.partition_key, shard_zero.metadata().shard_count)
                .map_err(|error| error.to_string())?
        }
    };
    let route = directory
        .resource_route(&resource, shard_index)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "target resource {} shard {shard_index} is not materialized",
                resource.name
            )
        })?;
    validate_target_route(&route, &resource, kind, None)?;
    let descriptor = &route.metadata().descriptor;
    let destination = EpochTargetDestination::new(
        kind,
        resource.name.clone(),
        descriptor.resource_generation,
        descriptor.shard_index,
        descriptor.tablet_id,
        descriptor.tablet_epoch,
    )
    .map_err(|error| error.to_string())?;
    Ok((route, destination))
}

fn resolve_bound_destination(
    directory: &TabletDirectory,
    source_route: &MaterializedTabletRoute,
    target: &SubscriptionTarget,
    binding: &EpochTargetDestination,
) -> Result<MaterializedTabletRoute, String> {
    binding
        .validate_for_target(target)
        .map_err(|error| error.to_string())?;
    let (resource, kind) = target_resource(source_route, target)?;
    if kind != binding.kind {
        return Err("destination kind changed".into());
    }
    let route = directory
        .route(binding.tablet_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "bound destination tablet is not materialized".to_owned())?;
    validate_target_route(&route, &resource, kind, Some(binding))?;
    Ok(route)
}

fn resolve_queue_dead_letter_destination(
    directory: &TabletDirectory,
    source_route: &MaterializedTabletRoute,
    target: &str,
) -> Result<(MaterializedTabletRoute, EpochTargetDestination), EpochTargetDeliveryError> {
    let source = &source_route.metadata().resource;
    if source.kind != ResourceKind::Queue {
        return Err(EpochTargetDeliveryError::State(
            "dead-letter source route is not a Queue".into(),
        ));
    }
    if target == source.name {
        return Err(EpochTargetDeliveryError::State(
            "dead-letter target must differ from the source Queue".into(),
        ));
    }
    let resource = ResourceName::new(
        source.organization.clone(),
        source.project.clone(),
        source.environment.clone(),
        source.namespace.clone(),
        ResourceKind::Queue,
        target,
    )
    .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    let route = directory
        .resource_route(&resource, 0)
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?
        .ok_or_else(|| {
            EpochTargetDeliveryError::State(format!(
                "dead-letter target Queue {target} is not materialized"
            ))
        })?;
    validate_queue_dead_letter_target_route(&route, &resource, None)
        .map_err(EpochTargetDeliveryError::State)?;
    let descriptor = &route.metadata().descriptor;
    let destination = EpochTargetDestination::new(
        EpochTargetKind::Queue,
        target,
        descriptor.resource_generation,
        descriptor.shard_index,
        descriptor.tablet_id,
        descriptor.tablet_epoch,
    )
    .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    Ok((route, destination))
}

fn resolve_bound_queue_dead_letter_destination(
    directory: &TabletDirectory,
    source_route: &MaterializedTabletRoute,
    target: &str,
    destination: &EpochTargetDestination,
) -> Result<MaterializedTabletRoute, EpochTargetDeliveryError> {
    if destination.kind != EpochTargetKind::Queue
        || destination.resource != target
        || destination.shard_index != 0
    {
        return Err(EpochTargetDeliveryError::State(
            "dead-letter Queue binding does not match its target".into(),
        ));
    }
    let source = &source_route.metadata().resource;
    if target == source.name {
        return Err(EpochTargetDeliveryError::State(
            "dead-letter target must differ from the source Queue".into(),
        ));
    }
    let resource = ResourceName::new(
        source.organization.clone(),
        source.project.clone(),
        source.environment.clone(),
        source.namespace.clone(),
        ResourceKind::Queue,
        target,
    )
    .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?;
    let route = directory
        .route(destination.tablet_id)
        .map_err(|error| EpochTargetDeliveryError::State(error.to_string()))?
        .ok_or_else(|| {
            EpochTargetDeliveryError::State(
                "bound dead-letter destination tablet is not materialized".into(),
            )
        })?;
    validate_queue_dead_letter_target_route(&route, &resource, Some(destination))
        .map_err(EpochTargetDeliveryError::State)?;
    Ok(route)
}

fn validate_queue_dead_letter_target_route(
    route: &MaterializedTabletRoute,
    resource: &ResourceName,
    binding: Option<&EpochTargetDestination>,
) -> Result<(), String> {
    validate_target_route(route, resource, EpochTargetKind::Queue, binding)?;
    validate_queue_dead_letter_target_configuration(route.metadata().configuration.as_ref())
}

fn validate_queue_dead_letter_target_configuration(
    configuration: Option<&serde_json::Value>,
) -> Result<(), String> {
    let durability = match configuration {
        Some(configuration) => {
            serde_json::from_value::<QueueConfig>(configuration.clone())
                .map_err(|error| {
                    format!("dead-letter target Queue configuration is invalid: {error}")
                })?
                .durability
        }
        None => DurabilityProfile::QuorumDurable,
    };
    if durability != DurabilityProfile::QuorumDurable {
        return Err("dead-letter target Queue must use quorum_durable durability".into());
    }
    Ok(())
}

fn target_resource(
    source_route: &MaterializedTabletRoute,
    target: &SubscriptionTarget,
) -> Result<(ResourceName, EpochTargetKind), String> {
    let source = &source_route.metadata().resource;
    if source.kind != ResourceKind::EventBus {
        return Err("source route is not an Event Bus".into());
    }
    let (kind, resource_kind, name) = match target {
        SubscriptionTarget::Queue { resource } => {
            (EpochTargetKind::Queue, ResourceKind::Queue, resource)
        }
        SubscriptionTarget::Stream { resource } => {
            (EpochTargetKind::Stream, ResourceKind::Stream, resource)
        }
        SubscriptionTarget::Pull
        | SubscriptionTarget::Webhook { .. }
        | SubscriptionTarget::Http { .. } => {
            return Err("candidate is not an Epoch Queue or Stream target".into());
        }
    };
    let resource = ResourceName::new(
        source.organization.clone(),
        source.project.clone(),
        source.environment.clone(),
        source.namespace.clone(),
        resource_kind,
        name.clone(),
    )
    .map_err(|error| error.to_string())?;
    Ok((resource, kind))
}

fn validate_target_route(
    route: &MaterializedTabletRoute,
    resource: &ResourceName,
    kind: EpochTargetKind,
    binding: Option<&EpochTargetDestination>,
) -> Result<(), String> {
    let metadata = route.metadata();
    let expected_profile = match kind {
        EpochTargetKind::Queue => WorkloadProfile::WorkQueue,
        EpochTargetKind::Stream => WorkloadProfile::StreamLog,
    };
    if metadata.resource != *resource
        || metadata.descriptor.workload_profile != expected_profile
        || metadata.descriptor.shard_index >= metadata.shard_count
    {
        return Err("destination route metadata does not match the target".into());
    }
    if let Some(binding) = binding
        && (metadata.descriptor.resource_generation != binding.resource_generation
            || metadata.descriptor.shard_index != binding.shard_index
            || metadata.descriptor.tablet_id != binding.tablet_id
            || metadata.descriptor.tablet_epoch != binding.tablet_epoch)
    {
        return Err("destination route no longer matches its durable binding".into());
    }
    Ok(())
}

fn destination_idempotency_key(
    source_route: &MaterializedTabletRoute,
    delivery: &BusTabletDelivery,
    destination: &EpochTargetDestination,
) -> String {
    let metadata = source_route.metadata();
    destination_idempotency_key_for(
        &metadata.resource,
        metadata.descriptor.resource_generation,
        metadata.descriptor.tablet_id,
        metadata.descriptor.tablet_epoch,
        &delivery.delivery_id,
        destination,
    )
}

fn queue_forward_command_key(
    action: &str,
    history_id: u64,
    destination: &EpochTargetDestination,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/queue/dead-letter-source-command/v1\0");
    hash_length_prefixed(&mut hasher, action.as_bytes());
    hasher.update(history_id.to_be_bytes());
    hash_length_prefixed(&mut hasher, destination.resource.as_bytes());
    hasher.update(destination.resource_generation.to_be_bytes());
    hasher.update(destination.tablet_id.to_be_bytes());
    hasher.update(destination.tablet_epoch.to_be_bytes());
    format!("epoch-queue-dlq-{action}.{}", lower_hex(&hasher.finalize()))
}

fn queue_dead_letter_target_key(
    source_route: &MaterializedTabletRoute,
    history_id: u64,
    destination: &EpochTargetDestination,
) -> String {
    let metadata = source_route.metadata();
    queue_dead_letter_target_key_for(
        &metadata.resource,
        metadata.descriptor.resource_generation,
        metadata.descriptor.tablet_id,
        metadata.descriptor.tablet_epoch,
        history_id,
        destination,
    )
}

fn queue_dead_letter_target_key_for(
    source_resource: &ResourceName,
    source_generation: u64,
    source_tablet_id: u64,
    source_tablet_epoch: u64,
    history_id: u64,
    destination: &EpochTargetDestination,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/queue/dead-letter-target/v1\0");
    hash_length_prefixed(&mut hasher, source_resource.canonical_name().as_bytes());
    hasher.update(source_generation.to_be_bytes());
    hasher.update(source_tablet_id.to_be_bytes());
    hasher.update(source_tablet_epoch.to_be_bytes());
    hasher.update(history_id.to_be_bytes());
    hash_length_prefixed(&mut hasher, destination.resource.as_bytes());
    hasher.update(destination.resource_generation.to_be_bytes());
    hasher.update(destination.tablet_id.to_be_bytes());
    hasher.update(destination.tablet_epoch.to_be_bytes());
    format!("epoch-queue-dlq-target.{}", lower_hex(&hasher.finalize()))
}

fn destination_idempotency_key_for(
    source_resource: &ResourceName,
    source_generation: u64,
    source_tablet_id: u64,
    source_tablet_epoch: u64,
    delivery_id: &str,
    destination: &EpochTargetDestination,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/event-bus/epoch-target-idempotency/v1\0");
    hash_length_prefixed(&mut hasher, source_resource.canonical_name().as_bytes());
    hasher.update(source_generation.to_be_bytes());
    hasher.update(source_tablet_id.to_be_bytes());
    hasher.update(source_tablet_epoch.to_be_bytes());
    hash_length_prefixed(&mut hasher, delivery_id.as_bytes());
    hasher.update([match destination.kind {
        EpochTargetKind::Queue => 1,
        EpochTargetKind::Stream => 2,
    }]);
    hash_length_prefixed(&mut hasher, destination.resource.as_bytes());
    hasher.update(destination.resource_generation.to_be_bytes());
    hasher.update(destination.shard_index.to_be_bytes());
    hasher.update(destination.tablet_id.to_be_bytes());
    hasher.update(destination.tablet_epoch.to_be_bytes());
    format!("epoch-target-v1.{}", lower_hex(&hasher.finalize()))
}

fn attempt_idempotency_key(label: &str, delivery_id: &str, attempt: u32) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/event-bus/target-worker-command/v1\0");
    hash_length_prefixed(&mut hasher, label.as_bytes());
    hash_length_prefixed(&mut hasher, delivery_id.as_bytes());
    hasher.update(attempt.to_be_bytes());
    format!("{label}.{}", lower_hex(&hasher.finalize()))
}

fn hash_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

const fn queue_rejection_reason(code: epoch_tablet::QueueTabletRejectionCode) -> &'static str {
    use epoch_tablet::QueueTabletRejectionCode;
    match code {
        QueueTabletRejectionCode::AlreadyExists => "queue_rejected_already_exists",
        QueueTabletRejectionCode::NotFound => "queue_rejected_not_found",
        QueueTabletRejectionCode::InvalidArgument => "queue_rejected_invalid_argument",
        QueueTabletRejectionCode::Conflict => "queue_rejected_conflict",
        QueueTabletRejectionCode::Fenced => "queue_rejected_fenced",
        QueueTabletRejectionCode::Capacity => "queue_rejected_capacity",
        QueueTabletRejectionCode::Unavailable => "queue_rejected_unavailable",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_dead_letter_target_requires_quorum_durability() {
        assert!(validate_queue_dead_letter_target_configuration(None).is_ok());

        let quorum = QueueConfig {
            durability: DurabilityProfile::QuorumDurable,
            ..QueueConfig::default()
        };
        let quorum = serde_json::to_value(quorum).unwrap();
        assert!(validate_queue_dead_letter_target_configuration(Some(&quorum)).is_ok());

        let memory_only = QueueConfig {
            durability: DurabilityProfile::ReplicatedMemory,
            ..QueueConfig::default()
        };
        let memory_only = serde_json::to_value(memory_only).unwrap();
        assert!(validate_queue_dead_letter_target_configuration(Some(&memory_only)).is_err());
    }

    #[test]
    fn attempt_keys_are_bounded_and_attempt_specific() {
        let first = attempt_idempotency_key("epoch-target-acquire", &"x".repeat(512), 1);
        let second = attempt_idempotency_key("epoch-target-acquire", &"x".repeat(512), 2);
        assert!(first.len() <= epoch_tablet::MAX_IDEMPOTENCY_KEY_BYTES);
        assert_ne!(first, second);
        assert_eq!(
            first,
            attempt_idempotency_key("epoch-target-acquire", &"x".repeat(512), 1)
        );
    }

    #[test]
    fn destination_key_is_stable_for_retries_and_separates_resource_incarnations() {
        let source = ResourceName::new(
            "acme",
            "shop",
            "dev",
            "core",
            ResourceKind::EventBus,
            "events",
        )
        .unwrap();
        let destination =
            EpochTargetDestination::new(EpochTargetKind::Stream, "orders", 3, 2, 42, 7).unwrap();
        let first = destination_idempotency_key_for(
            &source,
            5,
            29,
            4,
            "epoch.bus.delivery.v1.1.audit",
            &destination,
        );
        assert_eq!(first.len(), "epoch-target-v1.".len() + 64);
        assert_eq!(
            first,
            destination_idempotency_key_for(
                &source,
                5,
                29,
                4,
                "epoch.bus.delivery.v1.1.audit",
                &destination,
            )
        );
        assert_ne!(
            first,
            destination_idempotency_key_for(
                &source,
                6,
                29,
                4,
                "epoch.bus.delivery.v1.1.audit",
                &destination,
            )
        );
        let recreated =
            EpochTargetDestination::new(EpochTargetKind::Stream, "orders", 4, 2, 52, 1).unwrap();
        assert_ne!(
            first,
            destination_idempotency_key_for(
                &source,
                5,
                29,
                4,
                "epoch.bus.delivery.v1.1.audit",
                &recreated,
            )
        );
    }

    #[test]
    fn queue_dead_letter_keys_are_retry_stable_and_incarnation_fenced() {
        let source =
            ResourceName::new("acme", "shop", "dev", "core", ResourceKind::Queue, "jobs").unwrap();
        let destination =
            EpochTargetDestination::new(EpochTargetKind::Queue, "failed-jobs", 3, 0, 42, 7)
                .unwrap();
        let first = queue_dead_letter_target_key_for(&source, 5, 29, 4, 11, &destination);
        assert_eq!(first.len(), "epoch-queue-dlq-target.".len() + 64);
        assert_eq!(
            first,
            queue_dead_letter_target_key_for(&source, 5, 29, 4, 11, &destination)
        );
        assert_ne!(
            first,
            queue_dead_letter_target_key_for(&source, 5, 29, 4, 12, &destination)
        );
        assert_ne!(
            first,
            queue_dead_letter_target_key_for(&source, 6, 29, 4, 11, &destination)
        );
        let recreated =
            EpochTargetDestination::new(EpochTargetKind::Queue, "failed-jobs", 4, 0, 52, 1)
                .unwrap();
        assert_ne!(
            first,
            queue_dead_letter_target_key_for(&source, 5, 29, 4, 11, &recreated)
        );

        let bind = queue_forward_command_key("bind", 11, &destination);
        assert_eq!(bind.len(), "epoch-queue-dlq-bind.".len() + 64);
        assert_eq!(bind, queue_forward_command_key("bind", 11, &destination));
        assert_ne!(
            bind,
            queue_forward_command_key("complete", 11, &destination)
        );
    }

    #[test]
    fn status_accumulates_queue_stream_retry_and_error_evidence() {
        let status = EpochTargetDeliveryStatus::new(Duration::from_millis(25));
        status.record(
            100,
            EpochTargetDeliveryPass {
                tablets_examined: 4,
                leaders_examined: 1,
                subscriptions_examined: 2,
                leases_acquired: 2,
                queue_enqueued: 1,
                stream_appended: 1,
                retry_scheduled: 1,
                dead_lettered: 1,
                errors: 1,
            },
            Some("test failure".into()),
        );
        let snapshot = status.snapshot();
        assert!(snapshot.enabled);
        assert_eq!(snapshot.interval_ms, 25);
        assert_eq!(snapshot.passes, 1);
        assert_eq!(snapshot.queue_enqueued, 1);
        assert_eq!(snapshot.stream_appended, 1);
        assert_eq!(snapshot.last_error.as_deref(), Some("test failure"));
    }
}
