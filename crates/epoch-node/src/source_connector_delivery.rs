//! Leader-owned ingestion for bounded HTTP and `CloudEvents` source connectors.

use std::{
    collections::BTreeSet,
    fmt::{self, Formatter},
    sync::{Arc, RwLock},
    time::Duration,
};

use epoch_bus::{
    ConnectorBatchCommit, ConnectorDirection, ConnectorKind, ConnectorRecordResult,
    ConnectorResource, ConnectorStatus, EventIntegrationState, IntegrationOperation,
};
use epoch_consensus::ConsensusRole;
use epoch_core::{Clock, EventEnvelope};
use epoch_tablet::{
    BusTabletCommand, BusTabletOperation, BusTabletOperationResult, BusTabletOutcome,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

use crate::{
    bus_tablet::BusTabletService,
    consensus::ConsensusProbeHandle,
    delivery_proposal::{ProposalRoute, propose_and_wait},
    managed_target_delivery::{
        AttemptDisposition, ConnectorSourceFetch, ManagedTargetDeliveryConfig,
        ManagedTargetDeliveryWorker, enforce_allowlist,
    },
    tablet_materializer::{MaterializedTabletRoute, TabletDirectory},
    webhook_delivery::safe_http_target,
};

pub const DEFAULT_SOURCE_CONNECTOR_INTERVAL: Duration = Duration::from_millis(500);
pub const MAX_SOURCE_CONNECTOR_INTERVAL: Duration = Duration::from_mins(1);
const DEFAULT_SOURCE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_SOURCE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_SOURCE_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_SOURCE_BATCH_EVENTS: usize = 1_000;

#[derive(Debug, Error)]
pub enum SourceConnectorDeliveryError {
    #[error("invalid source-connector configuration: {0}")]
    Configuration(String),
    #[error("source-connector state is unavailable: {0}")]
    State(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceConnectorDeliveryPass {
    pub tablets_examined: u64,
    pub leaders_examined: u64,
    pub connectors_examined: u64,
    pub batches_fetched: u64,
    pub events_applied: u64,
    pub events_routed_to_error: u64,
    pub checkpoints_committed: u64,
    pub errors: u64,
}

#[derive(Default)]
pub struct SourceConnectorDeliveryStatus {
    interval_ms: u64,
    passes: std::sync::atomic::AtomicU64,
    connectors_examined: std::sync::atomic::AtomicU64,
    batches_fetched: std::sync::atomic::AtomicU64,
    events_applied: std::sync::atomic::AtomicU64,
    events_routed_to_error: std::sync::atomic::AtomicU64,
    checkpoints_committed: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    last_pass_at_ms: std::sync::atomic::AtomicU64,
    last_error: RwLock<Option<String>>,
}

impl fmt::Debug for SourceConnectorDeliveryStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceConnectorDeliveryStatus")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl SourceConnectorDeliveryStatus {
    pub fn new(interval: Duration) -> Arc<Self> {
        Arc::new(Self {
            interval_ms: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
            ..Self::default()
        })
    }

    pub fn record(
        &self,
        now_ms: u64,
        pass: SourceConnectorDeliveryPass,
        last_error: Option<String>,
    ) {
        use std::sync::atomic::Ordering;
        self.passes.fetch_add(1, Ordering::Relaxed);
        self.connectors_examined
            .fetch_add(pass.connectors_examined, Ordering::Relaxed);
        self.batches_fetched
            .fetch_add(pass.batches_fetched, Ordering::Relaxed);
        self.events_applied
            .fetch_add(pass.events_applied, Ordering::Relaxed);
        self.events_routed_to_error
            .fetch_add(pass.events_routed_to_error, Ordering::Relaxed);
        self.checkpoints_committed
            .fetch_add(pass.checkpoints_committed, Ordering::Relaxed);
        self.errors.fetch_add(pass.errors, Ordering::Relaxed);
        self.last_pass_at_ms.store(now_ms, Ordering::Relaxed);
        if let Some(last_error) = last_error
            && let Ok(mut error) = self.last_error.write()
        {
            *error = Some(last_error);
        }
    }

    pub fn snapshot(&self) -> SourceConnectorDeliveryStatusSnapshot {
        use std::sync::atomic::Ordering;
        SourceConnectorDeliveryStatusSnapshot {
            enabled: true,
            interval_ms: self.interval_ms,
            passes: self.passes.load(Ordering::Relaxed),
            connectors_examined: self.connectors_examined.load(Ordering::Relaxed),
            batches_fetched: self.batches_fetched.load(Ordering::Relaxed),
            events_applied: self.events_applied.load(Ordering::Relaxed),
            events_routed_to_error: self.events_routed_to_error.load(Ordering::Relaxed),
            checkpoints_committed: self.checkpoints_committed.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            last_pass_at_ms: self.last_pass_at_ms.load(Ordering::Relaxed),
            last_error: self.last_error.read().ok().and_then(|error| error.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceConnectorDeliveryStatusSnapshot {
    pub enabled: bool,
    pub interval_ms: u64,
    pub passes: u64,
    pub connectors_examined: u64,
    pub batches_fetched: u64,
    pub events_applied: u64,
    pub events_routed_to_error: u64,
    pub checkpoints_committed: u64,
    pub errors: u64,
    pub last_pass_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug)]
pub struct SourceConnectorDeliveryWorker {
    interval: Duration,
    http: ManagedTargetDeliveryWorker,
    commit_wait: Duration,
}

impl SourceConnectorDeliveryWorker {
    pub fn new(
        interval: Duration,
        http_config: ManagedTargetDeliveryConfig,
        commit_wait: Duration,
    ) -> Result<Self, SourceConnectorDeliveryError> {
        if interval.is_zero() || interval > MAX_SOURCE_CONNECTOR_INTERVAL {
            return Err(SourceConnectorDeliveryError::Configuration(format!(
                "poll interval must be between 1 ms and {} ms",
                MAX_SOURCE_CONNECTOR_INTERVAL.as_millis()
            )));
        }
        let http = ManagedTargetDeliveryWorker::new(http_config, commit_wait)
            .map_err(|error| SourceConnectorDeliveryError::Configuration(error.to_string()))?;
        Ok(Self {
            interval,
            http,
            commit_wait,
        })
    }

    pub const fn interval(&self) -> Duration {
        self.interval
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpSourceBatch {
    batch_id: String,
    source_from: String,
    source_to: String,
    events: Vec<EventEnvelope>,
}

#[derive(Debug)]
struct ResolvedSource {
    identity: String,
    url: Url,
    secret_reference: Option<String>,
    source_position: String,
    timeout: Duration,
}

struct SourceBatchContext<'a> {
    consensus: &'a ConsensusProbeHandle,
    service: &'a BusTabletService,
    commit_wait: Duration,
    clock: &'a dyn Clock,
    connector: &'a str,
    batch_id: &'a str,
}

pub async fn run_source_connector_delivery_pass(
    directory: &TabletDirectory,
    worker: &SourceConnectorDeliveryWorker,
    clock: &dyn Clock,
) -> (SourceConnectorDeliveryPass, Option<String>) {
    let routes = match directory.routes() {
        Ok(routes) => routes,
        Err(error) => {
            return (
                SourceConnectorDeliveryPass {
                    errors: 1,
                    ..SourceConnectorDeliveryPass::default()
                },
                Some(error.to_string()),
            );
        }
    };
    let mut pass = SourceConnectorDeliveryPass::default();
    let mut last_error = None;
    for route in routes {
        pass.tablets_examined = pass.tablets_examined.saturating_add(1);
        let Some(service) = route.bus_service() else {
            continue;
        };
        let errors_before = pass.errors;
        if let Err(error) = dispatch_route(&route, &service, worker, clock, &mut pass).await {
            if pass.errors == errors_before {
                pass.errors = pass.errors.saturating_add(1);
            }
            last_error = Some(error.to_string());
        }
    }
    (pass, last_error)
}

async fn dispatch_route(
    route: &MaterializedTabletRoute,
    service: &Arc<BusTabletService>,
    worker: &SourceConnectorDeliveryWorker,
    clock: &dyn Clock,
    pass: &mut SourceConnectorDeliveryPass,
) -> Result<(), SourceConnectorDeliveryError> {
    let consensus = route.consensus();
    let status = consensus
        .status()
        .await
        .map_err(|error| SourceConnectorDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Ok(());
    }
    pass.leaders_examined = pass.leaders_examined.saturating_add(1);
    let integration = service
        .integration_state()
        .map_err(SourceConnectorDeliveryError::State)?;
    let connector_names = integration
        .connectors()
        .resources()
        .filter(|(_, resource)| source_direction(resource.spec.direction))
        .map(|(name, _)| name.to_owned())
        .collect::<Vec<_>>();
    let mut last_connector_error = None;
    for name in connector_names {
        pass.connectors_examined = pass.connectors_examined.saturating_add(1);
        if let Err(error) = dispatch_connector(
            &consensus,
            service,
            worker,
            clock,
            pass,
            &integration,
            &name,
        )
        .await
        {
            pass.errors = pass.errors.saturating_add(1);
            last_connector_error = Some(error);
        }
    }
    last_connector_error.map_or(Ok(()), Err)
}

#[allow(
    clippy::too_many_arguments,
    reason = "source ingestion binds consensus ownership, connector state, I/O, and evidence"
)]
async fn dispatch_connector(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    worker: &SourceConnectorDeliveryWorker,
    clock: &dyn Clock,
    pass: &mut SourceConnectorDeliveryPass,
    integration: &EventIntegrationState,
    name: &str,
) -> Result<(), SourceConnectorDeliveryError> {
    let resource = integration
        .connectors()
        .connector(name)
        .ok_or_else(|| SourceConnectorDeliveryError::State("connector disappeared".into()))?;
    let Some(source) = resolve_source(name, resource, worker.http.config())? else {
        return Ok(());
    };
    let response = worker
        .http
        .fetch_connector_source(ConnectorSourceFetch {
            target: &source.url,
            secret_reference: source.secret_reference.as_deref(),
            connector_identity: &source.identity,
            source_position: &source.source_position,
            timeout: source.timeout,
            maximum_response_bytes: MAX_SOURCE_RESPONSE_BYTES,
            now_ms: clock.wall_time_ms(),
        })
        .await
        .map_err(source_attempt_error)?;
    let Some(response) = response else {
        return Ok(());
    };
    let batch: HttpSourceBatch = serde_json::from_slice(&response).map_err(|error| {
        SourceConnectorDeliveryError::State(format!(
            "connector {name} response is invalid: {error}"
        ))
    })?;
    validate_batch(&batch, &source.source_position)?;
    pass.batches_fetched = pass.batches_fetched.saturating_add(1);

    let batch_context = SourceBatchContext {
        consensus,
        service,
        commit_wait: worker.commit_wait,
        clock,
        connector: name,
        batch_id: &batch.batch_id,
    };
    let mut results = Vec::with_capacity(batch.events.len());
    for (index, event) in batch.events.iter().enumerate() {
        let result = publish_source_event(&batch_context, index, event).await?;
        match result {
            ConnectorRecordResult::Applied { .. } => {
                pass.events_applied = pass.events_applied.saturating_add(1);
            }
            ConnectorRecordResult::RoutedToError { .. } => {
                pass.events_routed_to_error = pass.events_routed_to_error.saturating_add(1);
            }
            ConnectorRecordResult::RetryableFailure { .. } => {}
        }
        results.push(result);
    }
    commit_source_batch(
        consensus,
        service,
        worker.commit_wait,
        clock,
        name,
        batch,
        results,
    )
    .await?;
    pass.checkpoints_committed = pass.checkpoints_committed.saturating_add(1);
    Ok(())
}

fn source_direction(direction: ConnectorDirection) -> bool {
    matches!(
        direction,
        ConnectorDirection::Source | ConnectorDirection::Bidirectional
    )
}

fn resolve_source(
    name: &str,
    resource: &ConnectorResource,
    http_config: &ManagedTargetDeliveryConfig,
) -> Result<Option<ResolvedSource>, SourceConnectorDeliveryError> {
    if resource.status != ConnectorStatus::Active || !source_direction(resource.spec.direction) {
        return Ok(None);
    }
    if !matches!(
        resource.spec.kind,
        ConnectorKind::Http | ConnectorKind::CloudEventBus
    ) {
        return Err(SourceConnectorDeliveryError::Configuration(format!(
            "connector {name} source kind is not supported by the built-in runtime"
        )));
    }
    let raw_url = resource.spec.config.get("source_url").ok_or_else(|| {
        SourceConnectorDeliveryError::Configuration(format!("connector {name} requires source_url"))
    })?;
    let url = safe_http_target(raw_url, http_config.allow_http_loopback).map_err(|_| {
        SourceConnectorDeliveryError::Configuration(format!(
            "connector {name} source_url is unsafe"
        ))
    })?;
    enforce_allowlist(&url, &resource.spec.outbound_allowlist, "connector")
        .map_err(source_attempt_error)?;
    let secret_reference = match resource.spec.secret_refs.len() {
        0 => None,
        1 => resource.spec.secret_refs.iter().next().cloned(),
        _ => {
            return Err(SourceConnectorDeliveryError::Configuration(format!(
                "connector {name} source authentication is ambiguous"
            )));
        }
    };
    let timeout =
        resource
            .spec
            .config
            .get("poll_timeout_ms")
            .map_or(Ok(DEFAULT_SOURCE_TIMEOUT), |raw| {
                raw.parse::<u64>()
                    .ok()
                    .map(Duration::from_millis)
                    .filter(|timeout| !timeout.is_zero() && *timeout <= MAX_SOURCE_TIMEOUT)
                    .ok_or_else(|| {
                        SourceConnectorDeliveryError::Configuration(format!(
                            "connector {name} poll_timeout_ms must be between 1 and {}",
                            MAX_SOURCE_TIMEOUT.as_millis()
                        ))
                    })
            })?;
    let source_position = resource.checkpoint.as_ref().map_or_else(
        || {
            resource
                .spec
                .config
                .get("start_position")
                .cloned()
                .unwrap_or_else(|| "0".into())
        },
        |checkpoint| checkpoint.source_position.clone(),
    );
    Ok(Some(ResolvedSource {
        identity: resource.spec.identity.clone(),
        url,
        secret_reference,
        source_position,
        timeout,
    }))
}

fn validate_batch(
    batch: &HttpSourceBatch,
    expected_source_from: &str,
) -> Result<(), SourceConnectorDeliveryError> {
    validate_source_text("batch_id", &batch.batch_id)?;
    validate_source_text("source_from", &batch.source_from)?;
    validate_source_text("source_to", &batch.source_to)?;
    if batch.source_from != expected_source_from {
        return Err(SourceConnectorDeliveryError::State(format!(
            "source batch starts at {}, expected {expected_source_from}",
            batch.source_from
        )));
    }
    if batch.source_to == batch.source_from {
        return Err(SourceConnectorDeliveryError::State(
            "source batch must advance its position".into(),
        ));
    }
    if batch.events.is_empty() || batch.events.len() > MAX_SOURCE_BATCH_EVENTS {
        return Err(SourceConnectorDeliveryError::State(format!(
            "source batch must contain 1-{MAX_SOURCE_BATCH_EVENTS} events"
        )));
    }
    let mut event_ids = BTreeSet::new();
    for event in &batch.events {
        event.validate().map_err(|error| {
            SourceConnectorDeliveryError::State(format!("source event is invalid: {error}"))
        })?;
        if !event_ids.insert(event.id.as_str()) {
            return Err(SourceConnectorDeliveryError::State(format!(
                "source batch repeats event id {}",
                event.id
            )));
        }
    }
    Ok(())
}

fn validate_source_text(field: &str, value: &str) -> Result<(), SourceConnectorDeliveryError> {
    if value.is_empty()
        || value.len() > 4 * 1024
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    {
        return Err(SourceConnectorDeliveryError::State(format!(
            "source {field} must contain 1-4096 printable bytes"
        )));
    }
    Ok(())
}

async fn publish_source_event(
    context: &SourceBatchContext<'_>,
    index: usize,
    event: &EventEnvelope,
) -> Result<ConnectorRecordResult, SourceConnectorDeliveryError> {
    let receipt = propose_command(
        context.consensus,
        context.service,
        context.commit_wait,
        context.clock,
        stable_key("source-publish", context.connector, context.batch_id, index),
        BusTabletOperation::Publish {
            envelope: event.clone(),
        },
        "source connector publish",
    )
    .await?;
    match receipt.outcome {
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::Published { .. },
        } => Ok(ConnectorRecordResult::Applied {
            record_id: event.id.clone(),
        }),
        BusTabletOutcome::Rejected { code, detail } => Ok(ConnectorRecordResult::RoutedToError {
            record_id: event.id.clone(),
            reason: source_record_reason(code, &detail),
        }),
        BusTabletOutcome::Applied { .. } => Err(SourceConnectorDeliveryError::State(
            "source connector publish returned an unexpected receipt".into(),
        )),
    }
}

fn source_record_reason(code: epoch_tablet::BusTabletRejectionCode, detail: &str) -> String {
    let prefix = format!("{code:?}").to_ascii_lowercase();
    let detail = detail
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        .take(96)
        .collect::<String>();
    if detail.is_empty() {
        prefix
    } else {
        format!("{prefix}:{detail}")
    }
}

async fn commit_source_batch(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    commit_wait: Duration,
    clock: &dyn Clock,
    connector: &str,
    batch: HttpSourceBatch,
    results: Vec<ConnectorRecordResult>,
) -> Result<(), SourceConnectorDeliveryError> {
    let batch_identity = stable_key("source-batch", connector, &batch.batch_id, 0);
    let receipt = propose_command(
        consensus,
        service,
        commit_wait,
        clock,
        stable_key("source-checkpoint", connector, &batch.batch_id, 0),
        BusTabletOperation::ApplyIntegration {
            operation: Box::new(IntegrationOperation::CommitConnectorBatch {
                name: connector.to_owned(),
                commit: ConnectorBatchCommit {
                    batch_id: batch.batch_id,
                    source_from: batch.source_from,
                    source_to: batch.source_to,
                    target_idempotency_key: batch_identity,
                    records: results,
                    committed_at_ms: 0,
                },
            }),
        },
        "source connector checkpoint",
    )
    .await?;
    if matches!(
        receipt.outcome,
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::IntegrationApplied { .. }
        }
    ) {
        Ok(())
    } else {
        Err(SourceConnectorDeliveryError::State(
            "source connector checkpoint was rejected".into(),
        ))
    }
}

async fn propose_command(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    commit_wait: Duration,
    clock: &dyn Clock,
    idempotency_key: String,
    operation: BusTabletOperation,
    label: &'static str,
) -> Result<epoch_tablet::BusTabletReceipt, SourceConnectorDeliveryError> {
    let status = consensus
        .status()
        .await
        .map_err(|error| SourceConnectorDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Err(SourceConnectorDeliveryError::State(format!(
            "{label} lost source leadership"
        )));
    }
    let applied_at_ms = clock.wall_time_ms().max(
        service
            .last_applied_time_ms()
            .map_err(SourceConnectorDeliveryError::State)?,
    );
    let command = BusTabletCommand::new(service.scope(), idempotency_key, applied_at_ms, operation)
        .map_err(|error| SourceConnectorDeliveryError::State(error.to_string()))?;
    let proposal_id = command
        .proposal_id(service.scope())
        .map_err(|error| SourceConnectorDeliveryError::State(error.to_string()))?;
    let payload = command
        .encode(service.scope())
        .map_err(|error| SourceConnectorDeliveryError::State(error.to_string()))?;
    let committed = propose_and_wait(
        consensus,
        proposal_id,
        status.term.get(),
        payload,
        commit_wait,
        label,
        ProposalRoute::LeaderOnly,
    )
    .await
    .map_err(SourceConnectorDeliveryError::State)?;
    service
        .committed_receipt(&committed)
        .map_err(SourceConnectorDeliveryError::State)
}

fn stable_key(prefix: &str, connector: &str, batch_id: &str, index: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/source-connector/v1\0");
    hasher.update(connector.as_bytes());
    hasher.update([0]);
    hasher.update(batch_id.as_bytes());
    hasher.update([0]);
    hasher.update(index.to_be_bytes());
    format!("{prefix}-{}", lower_hex(&hasher.finalize()))
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

fn source_attempt_error(disposition: AttemptDisposition) -> SourceConnectorDeliveryError {
    match disposition {
        AttemptDisposition::Acknowledge => SourceConnectorDeliveryError::State(
            "source request returned an invalid acknowledgement".into(),
        ),
        AttemptDisposition::Retry(reason) | AttemptDisposition::Reject(reason) => {
            SourceConnectorDeliveryError::State(reason)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Mutex,
    };

    use axum::{Json, Router, extract::State, http::HeaderMap, routing::get};
    use epoch_bus::{ConnectorCheckpoint, ConnectorRegistry, ConnectorSpec};
    use serde_json::json;

    use super::*;

    fn event(id: &str) -> EventEnvelope {
        let mut event = EventEnvelope::new("urn:test", "order.created", json!({"id": id}), 1);
        event.id = id.into();
        event
    }

    fn source_resource() -> ConnectorResource {
        let mut registry = ConnectorRegistry::default();
        registry
            .upsert(
                ConnectorSpec {
                    name: "orders-source".into(),
                    kind: ConnectorKind::Http,
                    direction: ConnectorDirection::Source,
                    secret_refs: BTreeSet::new(),
                    outbound_allowlist: BTreeSet::from(["127.0.0.1".into()]),
                    identity: "orders-reader".into(),
                    config: BTreeMap::from([
                        ("source_url".into(), "http://127.0.0.1:9090/events".into()),
                        ("start_position".into(), "cursor-10".into()),
                    ]),
                },
                1,
            )
            .unwrap();
        registry.connector("orders-source").unwrap().clone()
    }

    #[test]
    fn source_resolution_uses_checkpoint_and_enforces_direction() {
        let config = ManagedTargetDeliveryConfig {
            allow_http_loopback: true,
            ..ManagedTargetDeliveryConfig::default()
        };
        let mut resource = source_resource();
        let source = resolve_source("orders-source", &resource, &config)
            .unwrap()
            .unwrap();
        assert_eq!(source.source_position, "cursor-10");
        resource.checkpoint = Some(ConnectorCheckpoint {
            source_position: "cursor-42".into(),
            target_idempotency_key: "target-42".into(),
            batch_id: "batch-42".into(),
            committed_at_ms: 42,
        });
        let source = resolve_source("orders-source", &resource, &config)
            .unwrap()
            .unwrap();
        assert_eq!(source.source_position, "cursor-42");
        resource.spec.direction = ConnectorDirection::Target;
        assert!(
            resolve_source("orders-source", &resource, &config)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn source_batch_rejects_cursor_gaps_and_duplicate_event_ids() {
        let batch = HttpSourceBatch {
            batch_id: "batch-11".into(),
            source_from: "cursor-10".into(),
            source_to: "cursor-11".into(),
            events: vec![event("event-1")],
        };
        validate_batch(&batch, "cursor-10").unwrap();
        assert!(validate_batch(&batch, "cursor-9").is_err());
        let repeated = HttpSourceBatch {
            events: vec![event("event-1"), event("event-1")],
            ..batch
        };
        assert!(validate_batch(&repeated, "cursor-10").is_err());
    }

    #[test]
    fn source_proposal_identity_is_stable_and_record_specific() {
        let first = stable_key("source-publish", "orders", "batch-7", 0);
        assert_eq!(first, stable_key("source-publish", "orders", "batch-7", 0));
        assert_ne!(first, stable_key("source-publish", "orders", "batch-7", 1));
        assert_ne!(first, stable_key("source-publish", "orders", "batch-8", 0));
    }

    async fn source_endpoint(
        State(headers_seen): State<Arc<Mutex<Option<HeaderMap>>>>,
        headers: HeaderMap,
    ) -> Json<serde_json::Value> {
        *headers_seen.lock().unwrap() = Some(headers);
        Json(json!({
            "batch_id": "batch-11",
            "source_from": "cursor-10",
            "source_to": "cursor-11",
            "events": [{
                "id": "event-11",
                "source": "urn:orders",
                "type": "order.created",
                "time_ms": 11,
                "payload": {"order_id": 11}
            }]
        }))
    }

    #[tokio::test]
    async fn source_fetch_sends_identity_and_exact_checkpoint() {
        let headers_seen = Arc::new(Mutex::new(None));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::clone(&headers_seen);
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/events", get(source_endpoint))
                    .with_state(state),
            )
            .await
        });
        let config = ManagedTargetDeliveryConfig {
            allow_http_loopback: true,
            ..ManagedTargetDeliveryConfig::default()
        };
        let worker = SourceConnectorDeliveryWorker::new(
            Duration::from_millis(10),
            config,
            Duration::from_secs(1),
        )
        .unwrap();
        let url = Url::parse(&format!("http://{address}/events")).unwrap();
        let response = worker
            .http
            .fetch_connector_source(ConnectorSourceFetch {
                target: &url,
                secret_reference: None,
                connector_identity: "orders-reader",
                source_position: "cursor-10",
                timeout: Duration::from_secs(1),
                maximum_response_bytes: MAX_SOURCE_RESPONSE_BYTES,
                now_ms: 1,
            })
            .await
            .unwrap()
            .unwrap();
        let batch: HttpSourceBatch = serde_json::from_slice(&response).unwrap();
        validate_batch(&batch, "cursor-10").unwrap();
        let headers = headers_seen.lock().unwrap().take().unwrap();
        assert_eq!(headers["epoch-connector-identity"], "orders-reader");
        assert_eq!(headers["epoch-connector-position"], "cursor-10");
        server.abort();
        let _ = server.await;
    }
}
