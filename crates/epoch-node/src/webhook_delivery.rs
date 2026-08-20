//! Leader-owned HTTP/webhook dispatch outside the replicated state machine.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Formatter},
    fs::File,
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    sync::{Arc, RwLock},
    time::Duration,
};

use epoch_bus::SubscriptionTarget;
use epoch_consensus::{ConsensusError, ConsensusRole, ProposalLookup};
use epoch_core::Clock;
use epoch_tablet::{
    BusTabletCommand, BusTabletOperation, BusTabletOperationResult, BusTabletOutcome,
};
use hmac::{Hmac, KeyInit, Mac};
use reqwest::{
    Client, StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::{Host, Url};

use crate::{
    bus_tablet::BusTabletService,
    consensus::{ConsensusProbeError, ConsensusProbeHandle},
    tablet_materializer::{MaterializedTabletRoute, TabletDirectory},
};

pub const DEFAULT_WEBHOOK_DELIVERY_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_WEBHOOK_DELIVERY_INTERVAL: Duration = Duration::from_mins(1);
const WEBHOOK_DISPATCHER: &str = "epoch-webhook-v1";
const WEBHOOK_DISPATCHER_EPOCH: u64 = 1;
const MAX_SIGNING_KEYS: usize = 32;
const MAX_SIGNING_KEY_BYTES: usize = 4 * 1024;
const MIN_SIGNING_KEY_BYTES: usize = 32;
const MAX_KEY_ID_BYTES: usize = 128;
const MAX_SIGNING_KEY_FILE_BYTES: u64 = 256 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SigningKeyFile {
    format_version: u16,
    keys: Vec<SigningKeyEntry>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SigningKeyEntry {
    id: String,
    secret: String,
}

#[derive(Clone, PartialEq, Eq)]
struct SigningKey {
    id: String,
    secret: Vec<u8>,
}

impl fmt::Debug for SigningKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SigningKey")
            .field("id", &self.id)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Default)]
pub struct WebhookSigningKeys {
    keys: BTreeMap<String, SigningKey>,
}

impl fmt::Debug for WebhookSigningKeys {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebhookSigningKeys")
            .field("key_ids", &self.keys.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl WebhookSigningKeys {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, WebhookDeliveryError> {
        let mut file = File::open(path.as_ref()).map_err(|error| {
            WebhookDeliveryError::Configuration(format!(
                "webhook signing-key file could not be read: {error}"
            ))
        })?;
        let mut bytes = Vec::new();
        file.by_ref()
            .take(MAX_SIGNING_KEY_FILE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| {
                WebhookDeliveryError::Configuration(format!(
                    "webhook signing-key file could not be read: {error}"
                ))
            })?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_SIGNING_KEY_FILE_BYTES {
            return Err(WebhookDeliveryError::Configuration(format!(
                "webhook signing-key file exceeds {MAX_SIGNING_KEY_FILE_BYTES} bytes"
            )));
        }
        let file: SigningKeyFile = serde_json::from_slice(&bytes).map_err(|error| {
            WebhookDeliveryError::Configuration(format!(
                "webhook signing-key file is invalid: {error}"
            ))
        })?;
        if file.format_version != 1 || file.keys.is_empty() || file.keys.len() > MAX_SIGNING_KEYS {
            return Err(WebhookDeliveryError::Configuration(format!(
                "webhook signing-key file must be format version 1 with 1-{MAX_SIGNING_KEYS} keys"
            )));
        }
        let mut keys = BTreeMap::new();
        for entry in file.keys {
            validate_key_id(&entry.id)?;
            if entry.secret.len() < MIN_SIGNING_KEY_BYTES
                || entry.secret.len() > MAX_SIGNING_KEY_BYTES
            {
                return Err(WebhookDeliveryError::Configuration(format!(
                    "webhook signing key {} must be between {MIN_SIGNING_KEY_BYTES} and {MAX_SIGNING_KEY_BYTES} bytes",
                    entry.id
                )));
            }
            let key = SigningKey {
                id: entry.id.clone(),
                secret: entry.secret.into_bytes(),
            };
            if keys.insert(entry.id, key).is_some() {
                return Err(WebhookDeliveryError::Configuration(
                    "webhook signing-key IDs must be unique".into(),
                ));
            }
        }
        Ok(Self { keys })
    }

    fn key(&self, key_id: &str) -> Option<&SigningKey> {
        self.keys.get(key_id)
    }
}

fn validate_key_id(value: &str) -> Result<(), WebhookDeliveryError> {
    if value.is_empty()
        || value.len() > MAX_KEY_ID_BYTES
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(WebhookDeliveryError::Configuration(format!(
            "webhook signing-key ID must be a 1-{MAX_KEY_ID_BYTES} byte identifier"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct WebhookDeliveryConfig {
    pub interval: Duration,
    pub allow_http_loopback: bool,
    pub signing_keys: Arc<WebhookSigningKeys>,
}

impl WebhookDeliveryConfig {
    pub fn validate(&self) -> Result<(), WebhookDeliveryError> {
        if self.interval.is_zero() || self.interval > MAX_WEBHOOK_DELIVERY_INTERVAL {
            return Err(WebhookDeliveryError::Configuration(format!(
                "webhook delivery interval must be between 1 ms and {} ms",
                MAX_WEBHOOK_DELIVERY_INTERVAL.as_millis()
            )));
        }
        if self.signing_keys.keys.is_empty() {
            return Err(WebhookDeliveryError::Configuration(
                "at least one webhook signing key is required".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WebhookDeliveryPass {
    pub tablets_examined: u64,
    pub leaders_examined: u64,
    pub subscriptions_examined: u64,
    pub leases_acquired: u64,
    pub delivered: u64,
    pub retry_scheduled: u64,
    pub dead_lettered: u64,
    pub errors: u64,
}

#[derive(Default)]
pub struct WebhookDeliveryStatus {
    enabled: bool,
    interval_ms: u64,
    passes: std::sync::atomic::AtomicU64,
    tablets_examined: std::sync::atomic::AtomicU64,
    leaders_examined: std::sync::atomic::AtomicU64,
    subscriptions_examined: std::sync::atomic::AtomicU64,
    leases_acquired: std::sync::atomic::AtomicU64,
    delivered: std::sync::atomic::AtomicU64,
    retry_scheduled: std::sync::atomic::AtomicU64,
    dead_lettered: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    last_pass_at_ms: std::sync::atomic::AtomicU64,
    last_error: RwLock<Option<String>>,
}

impl fmt::Debug for WebhookDeliveryStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebhookDeliveryStatus")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl WebhookDeliveryStatus {
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn enabled(interval: Duration) -> Arc<Self> {
        Arc::new(Self {
            enabled: true,
            interval_ms: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
            ..Self::default()
        })
    }

    pub fn record(&self, now_ms: u64, pass: WebhookDeliveryPass, last_error: Option<String>) {
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
        self.delivered.fetch_add(pass.delivered, Ordering::Relaxed);
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

    pub fn snapshot(&self) -> WebhookDeliveryStatusSnapshot {
        use std::sync::atomic::Ordering;
        WebhookDeliveryStatusSnapshot {
            enabled: self.enabled,
            interval_ms: self.interval_ms,
            passes: self.passes.load(Ordering::Relaxed),
            tablets_examined: self.tablets_examined.load(Ordering::Relaxed),
            leaders_examined: self.leaders_examined.load(Ordering::Relaxed),
            subscriptions_examined: self.subscriptions_examined.load(Ordering::Relaxed),
            leases_acquired: self.leases_acquired.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            retry_scheduled: self.retry_scheduled.load(Ordering::Relaxed),
            dead_lettered: self.dead_lettered.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            last_pass_at_ms: self.last_pass_at_ms.load(Ordering::Relaxed),
            last_error: self.last_error.read().ok().and_then(|error| error.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WebhookDeliveryStatusSnapshot {
    pub enabled: bool,
    pub interval_ms: u64,
    pub passes: u64,
    pub tablets_examined: u64,
    pub leaders_examined: u64,
    pub subscriptions_examined: u64,
    pub leases_acquired: u64,
    pub delivered: u64,
    pub retry_scheduled: u64,
    pub dead_lettered: u64,
    pub errors: u64,
    pub last_pass_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Error)]
pub enum WebhookDeliveryError {
    #[error("invalid webhook delivery configuration: {0}")]
    Configuration(String),
    #[error("webhook target is unsafe: {0}")]
    UnsafeTarget(String),
    #[error("webhook delivery metadata is invalid: {0}")]
    InvalidDelivery(String),
    #[error("webhook delivery state is unavailable: {0}")]
    State(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WebhookAttempt {
    target: Url,
    body: Vec<u8>,
    headers: HeaderMap,
    timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AttemptDisposition {
    Acknowledge,
    Retry(String),
    Reject(String),
}

#[derive(Debug, Clone)]
pub struct WebhookDeliveryWorker {
    config: WebhookDeliveryConfig,
    commit_wait: Duration,
    client: Client,
}

impl WebhookDeliveryWorker {
    pub fn new(
        config: WebhookDeliveryConfig,
        commit_wait: Duration,
    ) -> Result<Self, WebhookDeliveryError> {
        config.validate()?;
        if commit_wait.is_zero() {
            return Err(WebhookDeliveryError::Configuration(
                "webhook proposal commit wait must be non-zero".into(),
            ));
        }
        let client = http_client_builder()
            .build()
            .map_err(|error| WebhookDeliveryError::Configuration(error.to_string()))?;
        Ok(Self {
            config,
            commit_wait,
            client,
        })
    }

    pub const fn config(&self) -> &WebhookDeliveryConfig {
        &self.config
    }

    async fn send(&self, attempt: &WebhookAttempt) -> AttemptDisposition {
        match tokio::time::timeout(attempt.timeout, self.send_within_lease(attempt)).await {
            Ok(disposition) => disposition,
            Err(_) => AttemptDisposition::Retry("request_timeout".into()),
        }
    }

    async fn send_within_lease(&self, attempt: &WebhookAttempt) -> AttemptDisposition {
        let client = match self.client_for_target(&attempt.target).await {
            Ok(client) => client,
            Err(disposition) => return disposition,
        };
        let response = client
            .post(attempt.target.clone())
            .headers(attempt.headers.clone())
            .body(attempt.body.clone())
            .timeout(attempt.timeout)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => return AttemptDisposition::Retry(request_reason(&error)),
        };
        classify_status(response.status())
    }

    async fn client_for_target(&self, target: &Url) -> Result<Client, AttemptDisposition> {
        let Some(Host::Domain(domain)) = target.host() else {
            return Ok(self.client.clone());
        };
        let port = target
            .port_or_known_default()
            .ok_or_else(|| AttemptDisposition::Reject("unsafe_target_port".into()))?;
        let resolved = tokio::net::lookup_host((domain, port))
            .await
            .map_err(|_| AttemptDisposition::Retry("dns_resolution_failed".into()))?
            .collect::<BTreeSet<_>>();
        let addresses = resolved.into_iter().collect::<Vec<SocketAddr>>();
        validate_resolved_addresses(target, &addresses, self.config.allow_http_loopback)?;
        http_client_builder()
            .resolve_to_addrs(domain, &addresses)
            .build()
            .map_err(|_| AttemptDisposition::Retry("request_client_failed".into()))
    }
}

fn validate_resolved_addresses(
    target: &Url,
    addresses: &[SocketAddr],
    allow_http_loopback: bool,
) -> Result<(), AttemptDisposition> {
    if addresses.is_empty() {
        return Err(AttemptDisposition::Retry("dns_resolution_failed".into()));
    }
    let allow_loopback = allow_http_loopback
        && target.scheme() == "http"
        && target
            .host_str()
            .is_some_and(|domain| domain.eq_ignore_ascii_case("localhost"));
    if addresses.iter().any(|address| {
        if allow_loopback {
            !address.ip().is_loopback()
        } else {
            !is_public_ip(address.ip())
        }
    }) {
        return Err(AttemptDisposition::Reject("unsafe_target_address".into()));
    }
    Ok(())
}

fn http_client_builder() -> reqwest::ClientBuilder {
    Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .connect_timeout(Duration::from_secs(5))
}

pub async fn run_webhook_delivery_pass(
    directory: &TabletDirectory,
    worker: &WebhookDeliveryWorker,
    clock: &dyn Clock,
) -> (WebhookDeliveryPass, Option<String>) {
    let routes = match directory.routes() {
        Ok(routes) => routes,
        Err(error) => {
            return (
                WebhookDeliveryPass {
                    errors: 1,
                    ..WebhookDeliveryPass::default()
                },
                Some(error.to_string()),
            );
        }
    };
    let mut pass = WebhookDeliveryPass::default();
    let mut last_error = None;
    for route in routes {
        pass.tablets_examined = pass.tablets_examined.saturating_add(1);
        let Some(service) = route.bus_service() else {
            continue;
        };
        if let Err(error) = dispatch_route(&route, &service, worker, clock, &mut pass).await {
            pass.errors = pass.errors.saturating_add(1);
            last_error = Some(error.to_string());
        }
    }
    (pass, last_error)
}

async fn dispatch_route(
    route: &MaterializedTabletRoute,
    service: &BusTabletService,
    worker: &WebhookDeliveryWorker,
    clock: &dyn Clock,
    pass: &mut WebhookDeliveryPass,
) -> Result<(), WebhookDeliveryError> {
    let consensus = route.consensus();
    let status = consensus
        .status()
        .await
        .map_err(|error| WebhookDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Ok(());
    }
    pass.leaders_examined = pass.leaders_examined.saturating_add(1);
    let now_ms = clock.wall_time_ms();
    let candidates = service
        .signed_webhook_delivery_candidates(now_ms)
        .map_err(WebhookDeliveryError::State)?;
    for candidate in candidates {
        pass.subscriptions_examined = pass.subscriptions_examined.saturating_add(1);
        dispatch_candidate(&consensus, service, worker, clock, pass, candidate, now_ms).await?;
    }
    Ok(())
}

async fn dispatch_candidate(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    worker: &WebhookDeliveryWorker,
    clock: &dyn Clock,
    pass: &mut WebhookDeliveryPass,
    candidate: epoch_bus::SignedWebhookDeliveryCandidate,
    observed_at_ms: u64,
) -> Result<(), WebhookDeliveryError> {
    let status = consensus
        .status()
        .await
        .map_err(|error| WebhookDeliveryError::State(error.to_string()))?;
    if status.role != ConsensusRole::Leader || status.fail_stopped {
        return Ok(());
    }
    if worker
        .config
        .signing_keys
        .key(&candidate.signing_key_id)
        .is_none()
    {
        return Err(WebhookDeliveryError::Configuration(format!(
            "webhook signing key {} is not configured",
            candidate.signing_key_id
        )));
    }
    let acquire_key = bounded_idempotency_key(
        "webhook-acquire",
        &candidate.delivery_id,
        u64::from(candidate.next_attempt),
    );
    let command = BusTabletCommand::new(
        service.scope(),
        acquire_key,
        observed_at_ms,
        BusTabletOperation::AcquireDeliveries {
            subscription: candidate.subscription,
            dispatcher: WEBHOOK_DISPATCHER.into(),
            dispatcher_epoch: WEBHOOK_DISPATCHER_EPOCH,
            max_deliveries: 1,
            expected_delivery_id: Some(candidate.delivery_id),
        },
    )
    .map_err(|error| WebhookDeliveryError::State(error.to_string()))?;
    let receipt = propose_command(
        consensus,
        service,
        command,
        status.term.get(),
        worker.commit_wait,
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
    let disposition = match prepare_attempt(
        WebhookAttemptInput::from(&delivery),
        clock.wall_time_ms(),
        &worker.config,
    ) {
        Ok(attempt) => worker.send(&attempt).await,
        Err(error) => prepare_error_disposition(&error),
    };
    settle_attempt(
        consensus,
        service,
        clock,
        pass,
        &delivery,
        disposition,
        worker.commit_wait,
    )
    .await
}

async fn settle_attempt(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    clock: &dyn Clock,
    pass: &mut WebhookDeliveryPass,
    delivery: &epoch_tablet::BusTabletDelivery,
    disposition: AttemptDisposition,
    commit_wait: Duration,
) -> Result<(), WebhookDeliveryError> {
    let settle_status = consensus.status().await.map_err(|error| {
        WebhookDeliveryError::State(format!(
            "target outcome must be resolved after status failure: {error}"
        ))
    })?;
    if settle_status.role != ConsensusRole::Leader || settle_status.fail_stopped {
        return Ok(());
    }
    let (operation, label) = settlement_operation(delivery, disposition);
    let key = bounded_idempotency_key(label, &delivery.delivery_id, u64::from(delivery.attempt));
    let command = BusTabletCommand::new(service.scope(), key, clock.wall_time_ms(), operation)
        .map_err(|error| WebhookDeliveryError::State(error.to_string()))?;
    let receipt = propose_command(
        consensus,
        service,
        command,
        settle_status.term.get(),
        commit_wait,
    )
    .await?;
    match receipt.outcome {
        BusTabletOutcome::Applied {
            result: BusTabletOperationResult::DeliveryAcknowledged { .. },
        } => pass.delivered = pass.delivered.saturating_add(1),
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

async fn propose_command(
    consensus: &ConsensusProbeHandle,
    service: &BusTabletService,
    command: BusTabletCommand,
    expected_term: u64,
    commit_wait: Duration,
) -> Result<epoch_tablet::BusTabletReceipt, WebhookDeliveryError> {
    let proposal_id = command
        .proposal_id(service.scope())
        .map_err(|error| WebhookDeliveryError::State(error.to_string()))?;
    let payload = command
        .encode(service.scope())
        .map_err(|error| WebhookDeliveryError::State(error.to_string()))?;
    // Subscribe before lookup/proposal so a fast commit cannot race past the
    // worker and leave a replicated lease without an HTTP attempt.
    let mut commits = consensus.subscribe_commits();
    let lookup = match consensus.lookup(proposal_id).await {
        Ok(ProposalLookup::Committed(committed)) => ProposalLookup::Committed(committed),
        Ok(ProposalLookup::Pending { payload: pending }) => {
            if pending != payload {
                return Err(WebhookDeliveryError::State(
                    "webhook proposal identity is already bound to another command".into(),
                ));
            }
            ProposalLookup::Pending { payload: pending }
        }
        Ok(ProposalLookup::Unknown) => {
            match consensus.propose(proposal_id, expected_term, payload).await {
                Ok(lookup) => lookup,
                Err(ConsensusProbeError::Consensus(
                    ConsensusError::NotLeader { .. }
                    | ConsensusError::StaleTerm { .. }
                    | ConsensusError::DuplicateProposal(_),
                )) => consensus
                    .lookup(proposal_id)
                    .await
                    .map_err(|error| WebhookDeliveryError::State(error.to_string()))?,
                Err(error) => return Err(WebhookDeliveryError::State(error.to_string())),
            }
        }
        Err(error) => return Err(WebhookDeliveryError::State(error.to_string())),
    };
    if let ProposalLookup::Committed(committed) = lookup {
        return service
            .committed_receipt(&committed)
            .map_err(WebhookDeliveryError::State);
    }

    let deadline = tokio::time::Instant::now() + commit_wait;
    loop {
        match tokio::time::timeout_at(deadline, commits.recv()).await {
            Ok(Ok(committed)) if committed.receipt.proposal_id.get() == proposal_id => {
                return service
                    .committed_receipt(&committed)
                    .map_err(WebhookDeliveryError::State);
            }
            Ok(Ok(_)) => {}
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                if let ProposalLookup::Committed(committed) = consensus
                    .lookup(proposal_id)
                    .await
                    .map_err(|error| WebhookDeliveryError::State(error.to_string()))?
                {
                    return service
                        .committed_receipt(&committed)
                        .map_err(WebhookDeliveryError::State);
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                return Err(WebhookDeliveryError::State(
                    "consensus commit notification channel closed".into(),
                ));
            }
            Err(_) => {
                if let ProposalLookup::Committed(committed) = consensus
                    .lookup(proposal_id)
                    .await
                    .map_err(|error| WebhookDeliveryError::State(error.to_string()))?
                {
                    return service
                        .committed_receipt(&committed)
                        .map_err(WebhookDeliveryError::State);
                }
                return Err(WebhookDeliveryError::State(format!(
                    "webhook proposal {proposal_id} did not commit within {} ms",
                    commit_wait.as_millis()
                )));
            }
        }
    }
}

fn settlement_operation(
    delivery: &epoch_tablet::BusTabletDelivery,
    disposition: AttemptDisposition,
) -> (BusTabletOperation, &'static str) {
    match disposition {
        AttemptDisposition::Acknowledge => (
            BusTabletOperation::AcknowledgeDelivery {
                delivery_id: delivery.delivery_id.clone(),
                dispatcher: WEBHOOK_DISPATCHER.into(),
                dispatcher_epoch: WEBHOOK_DISPATCHER_EPOCH,
                lease_token: delivery.lease_token.clone(),
            },
            "webhook-ack",
        ),
        AttemptDisposition::Retry(reason) => (
            BusTabletOperation::FailDelivery {
                delivery_id: delivery.delivery_id.clone(),
                dispatcher: WEBHOOK_DISPATCHER.into(),
                dispatcher_epoch: WEBHOOK_DISPATCHER_EPOCH,
                lease_token: delivery.lease_token.clone(),
                reason,
            },
            "webhook-retry",
        ),
        AttemptDisposition::Reject(reason) => (
            BusTabletOperation::RejectDelivery {
                delivery_id: delivery.delivery_id.clone(),
                dispatcher: WEBHOOK_DISPATCHER.into(),
                dispatcher_epoch: WEBHOOK_DISPATCHER_EPOCH,
                lease_token: delivery.lease_token.clone(),
                reason,
            },
            "webhook-reject",
        ),
    }
}

fn prepare_error_disposition(error: &WebhookDeliveryError) -> AttemptDisposition {
    match error {
        WebhookDeliveryError::UnsafeTarget(_) => AttemptDisposition::Reject("unsafe_target".into()),
        WebhookDeliveryError::InvalidDelivery(_) => {
            AttemptDisposition::Reject("invalid_delivery_metadata".into())
        }
        WebhookDeliveryError::Configuration(_) => {
            AttemptDisposition::Retry("signing_key_unavailable".into())
        }
        WebhookDeliveryError::State(_) => {
            AttemptDisposition::Retry("delivery_state_unavailable".into())
        }
    }
}

#[derive(Clone, Copy)]
struct WebhookAttemptInput<'a> {
    target: &'a SubscriptionTarget,
    envelope: &'a epoch_tablet::BusTabletEnvelope,
    delivery_id: &'a str,
    subscription: &'a str,
    attempt: u32,
    lease_deadline_ms: u64,
}

impl<'a> From<&'a epoch_tablet::BusTabletDelivery> for WebhookAttemptInput<'a> {
    fn from(delivery: &'a epoch_tablet::BusTabletDelivery) -> Self {
        Self {
            target: &delivery.target,
            envelope: &delivery.envelope,
            delivery_id: &delivery.delivery_id,
            subscription: &delivery.subscription,
            attempt: delivery.attempt,
            lease_deadline_ms: delivery.lease_deadline_ms,
        }
    }
}

fn prepare_attempt(
    input: WebhookAttemptInput<'_>,
    now_ms: u64,
    config: &WebhookDeliveryConfig,
) -> Result<WebhookAttempt, WebhookDeliveryError> {
    let timeout_ms = input
        .lease_deadline_ms
        .checked_sub(now_ms)
        .filter(|remaining| *remaining > 0)
        .ok_or_else(|| {
            WebhookDeliveryError::State("delivery lease expired before dispatch".into())
        })?;
    let (target, key_id) = match input.target {
        SubscriptionTarget::Webhook {
            url,
            signing_key_id,
        }
        | SubscriptionTarget::Http {
            url,
            signing_key_id,
        } => (
            validate_target(url, config.allow_http_loopback)?,
            signing_key_id.as_deref().ok_or_else(|| {
                WebhookDeliveryError::Configuration(
                    "webhook target must declare signing_key_id".into(),
                )
            })?,
        ),
        SubscriptionTarget::Pull
        | SubscriptionTarget::Queue { .. }
        | SubscriptionTarget::Stream { .. } => {
            return Err(WebhookDeliveryError::State(
                "delivery worker received a non-HTTP target".into(),
            ));
        }
    };
    let key = config.signing_keys.key(key_id).ok_or_else(|| {
        WebhookDeliveryError::Configuration(format!(
            "webhook signing key {key_id} is not configured"
        ))
    })?;
    let body = serde_json::to_vec(&input.envelope.payload)
        .map_err(|error| WebhookDeliveryError::State(error.to_string()))?;
    let timestamp = now_ms / 1_000;
    let signature = sign_delivery(
        &key.secret,
        timestamp,
        input.delivery_id,
        input.attempt,
        &body,
    );
    let mut header_values = BTreeMap::from([
        ("ce-specversion".into(), "1.0".into()),
        ("ce-id".into(), input.envelope.id.clone()),
        ("ce-source".into(), input.envelope.source.clone()),
        ("ce-type".into(), input.envelope.event_type.clone()),
        ("epoch-delivery-id".into(), input.delivery_id.into()),
        ("epoch-delivery-attempt".into(), input.attempt.to_string()),
        ("epoch-subscription".into(), input.subscription.into()),
        ("epoch-signature-key-id".into(), key.id.clone()),
        ("epoch-signature-timestamp".into(), timestamp.to_string()),
        ("epoch-signature".into(), format!("v1={signature}")),
    ]);
    if let Some(subject) = &input.envelope.subject {
        header_values.insert("ce-subject".into(), subject.clone());
    }
    if let Some(traceparent) = &input.envelope.traceparent {
        header_values.insert("traceparent".into(), traceparent.clone());
    }
    header_values.insert("content-type".into(), input.envelope.content_type.clone());
    let headers = validated_headers(header_values)?;
    Ok(WebhookAttempt {
        target,
        body,
        headers,
        timeout: Duration::from_millis(timeout_ms),
    })
}

fn validated_headers(values: BTreeMap<String, String>) -> Result<HeaderMap, WebhookDeliveryError> {
    let mut headers = HeaderMap::with_capacity(values.len());
    for (name, value) in values {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
            WebhookDeliveryError::InvalidDelivery("HTTP header name is invalid".into())
        })?;
        let value = HeaderValue::from_str(&value).map_err(|_| {
            WebhookDeliveryError::InvalidDelivery(format!(
                "HTTP header {name} contains a forbidden value"
            ))
        })?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn validate_target(raw: &str, allow_http_loopback: bool) -> Result<Url, WebhookDeliveryError> {
    let url =
        Url::parse(raw).map_err(|error| WebhookDeliveryError::UnsafeTarget(error.to_string()))?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(WebhookDeliveryError::UnsafeTarget(
            "userinfo and fragments are forbidden".into(),
        ));
    }
    let host = url
        .host()
        .ok_or_else(|| WebhookDeliveryError::UnsafeTarget("host is required".into()))?;
    match (url.scheme(), host) {
        ("https", Host::Domain(_)) => {}
        ("https", Host::Ipv4(address)) if is_public_ip(IpAddr::V4(address)) => {}
        ("https", Host::Ipv6(address)) if is_public_ip(IpAddr::V6(address)) => {}
        ("http", Host::Ipv4(address)) if allow_http_loopback && address.is_loopback() => {}
        ("http", Host::Ipv6(address)) if allow_http_loopback && address.is_loopback() => {}
        ("http", Host::Domain(domain))
            if allow_http_loopback && domain.eq_ignore_ascii_case("localhost") => {}
        ("http", _) => {
            return Err(WebhookDeliveryError::UnsafeTarget(
                "HTTP is allowed only for explicit loopback development targets".into(),
            ));
        }
        ("https", _) => {
            return Err(WebhookDeliveryError::UnsafeTarget(
                "private, loopback, link-local, multicast, or unspecified literal IP is forbidden"
                    .into(),
            ));
        }
        _ => {
            return Err(WebhookDeliveryError::UnsafeTarget(
                "target scheme must be HTTPS".into(),
            ));
        }
    }
    Ok(url)
}

fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => {
            let segments = address.segments();
            if let Some(embedded) = address.to_ipv4() {
                return is_public_ipv4(embedded);
            }
            // IANA currently assigns ordinary global unicast from 2000::/3.
            // Deny reserved/special sub-ranges and transition mechanisms rather
            // than relying on whether the host OS happens to route them.
            (segments[0] & 0xe000) == 0x2000
                && !(segments[0] == 0x2001 && segments[1] < 0x0200)
                && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
                && segments[0] != 0x2002
                && !(segments[0] == 0x3fff && (segments[1] & 0xf000) == 0)
        }
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [first, second, third, _] = address.octets();
    !(address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_multicast()
        || address.is_unspecified()
        || address.is_broadcast()
        || first == 0
        || (first == 100 && (64..=127).contains(&second))
        || (first == 192 && second == 0 && third == 0)
        || (first == 192 && second == 0 && third == 2)
        || (first == 192 && second == 88 && third == 99)
        || (first == 198 && matches!(second, 18 | 19))
        || (first == 198 && second == 51 && third == 100)
        || (first == 203 && second == 0 && third == 113)
        || first >= 240)
}

fn classify_status(status: StatusCode) -> AttemptDisposition {
    if status.is_success() {
        AttemptDisposition::Acknowledge
    } else if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        AttemptDisposition::Retry(format!("http_status_{}", status.as_u16()))
    } else {
        AttemptDisposition::Reject(format!("http_status_{}", status.as_u16()))
    }
}

fn request_reason(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        "request_timeout".into()
    } else if error.is_connect() {
        "connection_failed".into()
    } else {
        "request_failed".into()
    }
}

fn bounded_idempotency_key(prefix: &str, identity: &str, ordinal: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/webhook/idempotency/v1\0");
    hasher.update(identity.as_bytes());
    hasher.update(ordinal.to_be_bytes());
    format!("{prefix}-{}", lower_hex(&hasher.finalize()))
}

fn sign_delivery(
    secret: &[u8],
    timestamp: u64,
    delivery_id: &str,
    attempt: u32,
    body: &[u8],
) -> String {
    let body_digest = Sha256::digest(body);
    let canonical = format!(
        "v1\n{timestamp}\n{delivery_id}\n{attempt}\n{}",
        lower_hex(&body_digest)
    );
    lower_hex(&hmac_sha256(secret, canonical.as_bytes()))
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key)
        .expect("HMAC-SHA-256 accepts keys of any length");
    mac.update(message);
    mac.finalize().into_bytes().into()
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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, io::Write as _, sync::Mutex};

    use axum::{Router, body::Bytes, extract::State, routing::post};
    use epoch_bus::SubscriptionTarget;
    use serde_json::json;
    use tokio::sync::oneshot;

    use super::*;

    fn config() -> WebhookDeliveryConfig {
        let key = SigningKey {
            id: "primary".into(),
            secret: b"0123456789abcdef0123456789abcdef".to_vec(),
        };
        WebhookDeliveryConfig {
            interval: Duration::from_millis(100),
            allow_http_loopback: true,
            signing_keys: Arc::new(WebhookSigningKeys {
                keys: BTreeMap::from([(key.id.clone(), key)]),
            }),
        }
    }

    fn envelope() -> epoch_tablet::BusTabletEnvelope {
        epoch_tablet::BusTabletEnvelope {
            id: "evt-1".into(),
            source: "/checkout".into(),
            event_type: "order.created".into(),
            subject: Some("orders/1".into()),
            time_ms: 1_000,
            key: None,
            headers: BTreeMap::new(),
            content_type: "application/json".into(),
            schema_ref: None,
            traceparent: Some("00-trace-parent-01".into()),
            payload: json!({"order_id":"one"}),
            deliver_at_ms: None,
            ttl_ms: None,
            priority: 0,
            dedupe_id: None,
            transaction_id: None,
            extensions: BTreeMap::new(),
        }
    }

    #[test]
    fn hmac_matches_the_published_rfc_4231_vector() {
        assert_eq!(
            lower_hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn signing_key_file_is_bounded_strict_and_debug_redacted() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(
            br#"{"format_version":1,"keys":[{"id":"primary","secret":"0123456789abcdef0123456789abcdef"}]}"#,
        )
        .unwrap();
        let keys = WebhookSigningKeys::load(file.path()).unwrap();
        let debug = format!("{keys:?}");
        assert!(debug.contains("primary"));
        assert!(!debug.contains("0123456789abcdef"));

        let mut duplicate = tempfile::NamedTempFile::new().unwrap();
        duplicate
            .write_all(
                br#"{"format_version":1,"keys":[{"id":"primary","secret":"0123456789abcdef0123456789abcdef"},{"id":"primary","secret":"fedcba9876543210fedcba9876543210"}]}"#,
            )
            .unwrap();
        assert!(WebhookSigningKeys::load(duplicate.path()).is_err());

        let mut oversized = tempfile::NamedTempFile::new().unwrap();
        let oversized_length = usize::try_from(MAX_SIGNING_KEY_FILE_BYTES)
            .unwrap()
            .checked_add(1)
            .unwrap();
        oversized.write_all(&vec![b'x'; oversized_length]).unwrap();
        assert!(matches!(
            WebhookSigningKeys::load(oversized.path()),
            Err(WebhookDeliveryError::Configuration(message))
                if message.contains("exceeds")
        ));
    }

    #[test]
    fn prepared_attempt_is_binary_cloudevents_and_signed_over_exact_body() {
        let target = SubscriptionTarget::Webhook {
            url: "http://127.0.0.1:8080/hooks".into(),
            signing_key_id: Some("primary".into()),
        };
        let envelope = envelope();
        let attempt = prepare_attempt(
            WebhookAttemptInput {
                target: &target,
                envelope: &envelope,
                delivery_id: "epoch.bus.delivery.v1.1.orders",
                subscription: "orders",
                attempt: 2,
                lease_deadline_ms: 1_700_000_030_123,
            },
            1_700_000_000_123,
            &config(),
        )
        .unwrap();
        assert_eq!(attempt.body, br#"{"order_id":"one"}"#);
        assert_eq!(attempt.headers["ce-specversion"], "1.0");
        assert_eq!(attempt.headers["epoch-delivery-attempt"], "2");
        assert_eq!(attempt.headers["epoch-signature-key-id"], "primary");
        assert_eq!(
            attempt.headers["epoch-signature"],
            format!(
                "v1={}",
                sign_delivery(
                    b"0123456789abcdef0123456789abcdef",
                    1_700_000_000,
                    "epoch.bus.delivery.v1.1.orders",
                    2,
                    &attempt.body
                )
            )
        );
        assert_eq!(attempt.timeout, Duration::from_secs(30));
        assert!(matches!(
            prepare_attempt(
                WebhookAttemptInput {
                    target: &target,
                    envelope: &envelope,
                    delivery_id: "epoch.bus.delivery.v1.1.orders",
                    subscription: "orders",
                    attempt: 2,
                    lease_deadline_ms: 1_700_000_030_123,
                },
                1_700_000_030_123,
                &config(),
            ),
            Err(WebhookDeliveryError::State(message))
                if message == "delivery lease expired before dispatch"
        ));
    }

    #[test]
    fn target_policy_and_status_classification_fail_closed() {
        assert!(validate_target("http://127.0.0.1:8080/hook", true).is_ok());
        assert!(validate_target("http://127.0.0.1:8080/hook", false).is_err());
        assert!(validate_target("https://169.254.169.254/latest", false).is_err());
        assert!(validate_target("https://[::1]/hook", false).is_err());
        assert!(!is_public_ip("::ffff:127.0.0.1".parse().unwrap()));
        assert!(!is_public_ip("192.0.2.1".parse().unwrap()));
        assert!(!is_public_ip("2001:db8::1".parse().unwrap()));
        assert!(is_public_ip("1.1.1.1".parse().unwrap()));
        assert!(is_public_ip("2606:4700:4700::1111".parse().unwrap()));
        assert!(validate_target("https://user@example.com/hook", false).is_err());
        assert_eq!(
            classify_status(StatusCode::NO_CONTENT),
            AttemptDisposition::Acknowledge
        );
        assert!(matches!(
            classify_status(StatusCode::TOO_MANY_REQUESTS),
            AttemptDisposition::Retry(_)
        ));
        assert!(matches!(
            classify_status(StatusCode::BAD_REQUEST),
            AttemptDisposition::Reject(_)
        ));

        let public = Url::parse("https://events.example.com/hook").unwrap();
        let public_address = "1.1.1.1:443".parse().unwrap();
        let private_address = "10.0.0.1:443".parse().unwrap();
        assert!(validate_resolved_addresses(&public, &[public_address], false).is_ok());
        assert!(matches!(
            validate_resolved_addresses(&public, &[public_address, private_address], false),
            Err(AttemptDisposition::Reject(reason)) if reason == "unsafe_target_address"
        ));
        let localhost = Url::parse("http://localhost:8080/hook").unwrap();
        let loopback = "127.0.0.1:8080".parse().unwrap();
        assert!(validate_resolved_addresses(&localhost, &[loopback], true).is_ok());
        assert!(validate_resolved_addresses(&localhost, &[loopback], false).is_err());

        for special in [
            "[::ffff:10.0.0.1]:443",
            "[::10.0.0.1]:443",
            "[64:ff9b::a00:1]:443",
            "[2001::1]:443",
            "[2002:a00:1::]:443",
            "[3fff::1]:443",
            "[5f00::1]:443",
        ] {
            assert!(
                !is_public_ip(special.parse::<SocketAddr>().unwrap().ip()),
                "special-purpose IPv6 address {special} must be denied"
            );
        }
        assert!(is_public_ip(
            "[2606:4700:4700::1111]:443"
                .parse::<SocketAddr>()
                .unwrap()
                .ip()
        ));
    }

    #[test]
    fn idempotency_keys_are_bounded_stable_and_operation_separated() {
        let first = bounded_idempotency_key("webhook-acquire", "orders", 7);
        assert_eq!(
            first,
            bounded_idempotency_key("webhook-acquire", "orders", 7)
        );
        assert_ne!(first, bounded_idempotency_key("webhook-ack", "orders", 7));
        assert!(first.len() <= epoch_tablet::MAX_IDEMPOTENCY_KEY_BYTES);
    }

    #[test]
    fn user_derived_http_headers_reject_control_characters() {
        let mut invalid = envelope();
        invalid.source = "checkout\r\ninjected: value".into();
        let target = SubscriptionTarget::Webhook {
            url: "http://127.0.0.1:8080/hooks".into(),
            signing_key_id: Some("primary".into()),
        };
        assert!(matches!(
            prepare_attempt(
                WebhookAttemptInput {
                    target: &target,
                    envelope: &invalid,
                    delivery_id: "delivery-1",
                    subscription: "orders",
                    attempt: 1,
                    lease_deadline_ms: 2_000,
                },
                1_000,
                &config(),
            ),
            Err(WebhookDeliveryError::InvalidDelivery(_))
        ));
    }

    #[tokio::test]
    async fn sends_exact_signed_binary_cloudevent_to_a_real_loopback_receiver() {
        type Captured = Arc<Mutex<Option<(HeaderMap, Bytes)>>>;

        async fn capture(
            State(captured): State<Captured>,
            headers: HeaderMap,
            body: Bytes,
        ) -> StatusCode {
            *captured.lock().unwrap() = Some((headers, body));
            StatusCode::NO_CONTENT
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let captured = Arc::new(Mutex::new(None));
        let app = Router::new()
            .route("/hooks", post(capture))
            .with_state(Arc::clone(&captured));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let now_ms = 1_700_000_000_123;
        let target = SubscriptionTarget::Webhook {
            url: format!("http://{address}/hooks"),
            signing_key_id: Some("primary".into()),
        };
        let envelope = envelope();
        let attempt = prepare_attempt(
            WebhookAttemptInput {
                target: &target,
                envelope: &envelope,
                delivery_id: "epoch.bus.delivery.v1.1.orders",
                subscription: "orders",
                attempt: 1,
                lease_deadline_ms: now_ms + 5_000,
            },
            now_ms,
            &config(),
        )
        .unwrap();
        let worker = WebhookDeliveryWorker::new(config(), Duration::from_secs(1)).unwrap();
        assert_eq!(worker.send(&attempt).await, AttemptDisposition::Acknowledge);

        let (headers, body) = captured.lock().unwrap().take().unwrap();
        assert_eq!(body.as_ref(), br#"{"order_id":"one"}"#);
        assert_eq!(headers["ce-specversion"], "1.0");
        assert_eq!(headers["ce-id"], "evt-1");
        assert_eq!(headers["epoch-signature-key-id"], "primary");
        assert_eq!(headers["epoch-delivery-attempt"], "1");
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(
            headers["epoch-signature"],
            attempt.headers["epoch-signature"]
        );

        let _ = shutdown_tx.send(());
        server.await.unwrap().unwrap();
    }
}
