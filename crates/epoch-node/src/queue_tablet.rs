//! Experimental typed Queue tablet over the fixed-voter consensus runtime.

use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{get, post},
};
use epoch_consensus::{
    CommittedProposal, ConsensusError, ConsensusRole, ConsensusStatus, ProposalLookup,
};
use epoch_core::Clock;
use epoch_queue::QueueConfig;
use epoch_tablet::{
    CommittedCommand, MAX_QUEUE_TABLET_COMMAND_BYTES, QueueAcknowledgeCommand, QueueAcquireCommand,
    QueueEnqueueCommand, QueueExtendLeaseCommand, QueueMaintainCommand, QueueNackCommand,
    QueueRedriveCommand, QueueRejectCommand, QueueReleaseCommand, QueueTablet, QueueTabletCommand,
    QueueTabletCounts, QueueTabletDeadLetterHistory, QueueTabletDisposition, QueueTabletOperation,
    QueueTabletReceipt, QueueTabletRedriveHistory, QueueTabletScope, TabletError,
    queue_proposal_id_for,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};

use crate::consensus::{CommittedProposalApplier, ConsensusProbeError, ConsensusProbeHandle};
use crate::tablet_http::{
    StrictEventEnvelope, TabletApiError, TabletApiResult,
    deserialize_optional_u64_from_number_or_decimal, deserialize_u64_from_number_or_decimal,
    hex_digest, serialize_optional_u64_as_decimal, serialize_u64_as_decimal,
};

pub const EXPERIMENTAL_QUEUE_TABLET_STATUS_PATH: &str = "/experimental/v1/tablets/queue/status";
pub const EXPERIMENTAL_QUEUE_TABLET_MUTATIONS_PATH: &str =
    "/experimental/v1/tablets/queue/mutations";
pub const EXPERIMENTAL_QUEUE_TABLET_MUTATION_PATH: &str =
    "/experimental/v1/tablets/queue/mutations/{proposal_id}";
pub const EXPERIMENTAL_QUEUE_TABLET_COUNTS_PATH: &str = "/experimental/v1/tablets/queue/counts";
pub const EXPERIMENTAL_QUEUE_TABLET_DEAD_LETTERS_PATH: &str =
    "/experimental/v1/tablets/queue/dead-letters";
pub const EXPERIMENTAL_QUEUE_TABLET_REDRIVES_PATH: &str = "/experimental/v1/tablets/queue/redrives";

const MAX_HISTORY_RECORDS: usize = 1_000;
const TABLET_REQUEST_BODY_BYTES: usize = MAX_QUEUE_TABLET_COMMAND_BYTES + 16 * 1024;
pub const DEFAULT_COMMIT_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct QueueTabletService {
    scope: QueueTabletScope,
    config: QueueConfig,
    tablet: RwLock<QueueTablet>,
    failure: RwLock<Option<String>>,
}

impl QueueTabletService {
    pub fn new(scope: QueueTabletScope, config: QueueConfig) -> Result<Arc<Self>, TabletError> {
        let tablet = QueueTablet::new(scope.clone(), config.clone())?;
        Ok(Arc::new(Self {
            scope,
            config,
            tablet: RwLock::new(tablet),
            failure: RwLock::new(None),
        }))
    }

    pub fn with_default_config(scope: QueueTabletScope) -> Result<Arc<Self>, TabletError> {
        Self::new(scope, QueueConfig::default())
    }

    pub fn scope(&self) -> &QueueTabletScope {
        &self.scope
    }

    pub fn last_profile_mutation_index(&self) -> Result<u64, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Queue tablet read lock was poisoned".to_owned())
            .map(|tablet| tablet.last_applied_command_index())
    }

    pub fn last_applied_time_ms(&self) -> Result<u64, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Queue tablet read lock was poisoned".to_owned())
            .map(|tablet| tablet.last_applied_time_ms())
    }

    fn ensure_healthy(&self) -> Result<(), String> {
        let failure = self
            .failure
            .read()
            .map_err(|_| "Queue tablet failure lock was poisoned".to_owned())?;
        if let Some(failure) = failure.as_ref() {
            Err(failure.clone())
        } else {
            Ok(())
        }
    }

    fn fail(&self, error: impl Into<String>) -> String {
        let error = error.into();
        if let Ok(mut failure) = self.failure.write() {
            failure.get_or_insert_with(|| error.clone());
        }
        error
    }

    fn apply_one(&self, committed: &CommittedProposal) -> Result<QueueTabletReceipt, String> {
        self.ensure_healthy()?;
        let command = committed_command(committed);
        let result = self
            .tablet
            .write()
            .map_err(|_| "Queue tablet write lock was poisoned".to_owned())?
            .apply(command)
            .map_err(|error| error.to_string());
        result.map_err(|error| self.fail(error))
    }

    fn committed_receipt(
        &self,
        committed: &CommittedProposal,
    ) -> Result<QueueTabletReceipt, String> {
        self.ensure_healthy()?;
        let result = self
            .tablet
            .read()
            .map_err(|_| self.fail("Queue tablet read lock was poisoned"))?
            .receipt_for_committed(committed_command(committed));
        match result {
            Ok(Some(receipt)) => Ok(receipt),
            Ok(None) => Err(self.fail(format!(
                "consensus commit {} was not applied by the Queue profile actor",
                committed.receipt.proposal_id
            ))),
            Err(error) => Err(self.fail(error.to_string())),
        }
    }

    fn snapshot(&self) -> Result<QueueTabletSnapshot, String> {
        self.ensure_healthy()?;
        let tablet = self
            .tablet
            .read()
            .map_err(|_| "Queue tablet read lock was poisoned".to_owned())?;
        let counts = tablet
            .counts()
            .try_into()
            .map_err(|error: epoch_core::EpochError| {
                format!("Queue tablet counts cannot be represented: {error}")
            })?;
        Ok(QueueTabletSnapshot {
            last_profile_mutation_index: tablet.last_applied_command_index(),
            last_applied_time_ms: tablet.last_applied_time_ms(),
            applied_command_count: u64::try_from(tablet.applied_command_count())
                .map_err(|_| "Queue tablet command count exceeds u64".to_owned())?,
            counts,
            state_digest: hex_digest(tablet.state_digest()),
        })
    }

    fn dead_letter_history(
        &self,
        limit: usize,
    ) -> Result<Vec<QueueTabletDeadLetterHistory>, String> {
        self.ensure_healthy()?;
        Ok(self
            .tablet
            .read()
            .map_err(|_| "Queue tablet read lock was poisoned".to_owned())?
            .dead_letter_history(limit))
    }

    fn redrive_history(&self, limit: usize) -> Result<Vec<QueueTabletRedriveHistory>, String> {
        self.ensure_healthy()?;
        Ok(self
            .tablet
            .read()
            .map_err(|_| "Queue tablet read lock was poisoned".to_owned())?
            .redrive_history(limit))
    }
}

fn committed_command(committed: &CommittedProposal) -> CommittedCommand<'_> {
    CommittedCommand {
        group_id: committed.receipt.group_id.get(),
        group_epoch: committed.receipt.group_epoch.get(),
        proposal_id: committed.receipt.proposal_id.get(),
        term: committed.receipt.term.get(),
        log_index: committed.receipt.log_index.get(),
        payload: &committed.payload,
    }
}

impl CommittedProposalApplier for QueueTabletService {
    fn replay(&self, committed: &[CommittedProposal]) -> Result<(), String> {
        let mut history = committed.to_vec();
        history.sort_by_key(|proposal| proposal.receipt.log_index.get());
        let mut rebuilt = QueueTablet::new(self.scope.clone(), self.config.clone())
            .map_err(|error| error.to_string())?;
        for proposal in &history {
            rebuilt
                .apply(committed_command(proposal))
                .map_err(|error| self.fail(error.to_string()))?;
        }
        *self
            .tablet
            .write()
            .map_err(|_| self.fail("Queue tablet write lock was poisoned"))? = rebuilt;
        Ok(())
    }

    fn apply(&self, committed: &CommittedProposal) -> Result<(), String> {
        self.apply_one(committed).map(|_| ())
    }
}

#[derive(Clone)]
struct QueueTabletApiState {
    service: Arc<QueueTabletService>,
    consensus: ConsensusProbeHandle,
    clock: Arc<dyn Clock>,
    commit_wait: Duration,
    write_serial: Arc<Mutex<()>>,
}

pub fn router(
    service: Arc<QueueTabletService>,
    consensus: ConsensusProbeHandle,
    clock: Arc<dyn Clock>,
    commit_wait: Duration,
) -> Router {
    let state = QueueTabletApiState {
        service,
        consensus,
        clock,
        commit_wait,
        write_serial: Arc::new(Mutex::new(())),
    };
    Router::new()
        .route(EXPERIMENTAL_QUEUE_TABLET_STATUS_PATH, get(tablet_status))
        .route(
            EXPERIMENTAL_QUEUE_TABLET_MUTATIONS_PATH,
            post(submit_mutation),
        )
        .route(
            EXPERIMENTAL_QUEUE_TABLET_MUTATION_PATH,
            get(lookup_mutation),
        )
        .route(EXPERIMENTAL_QUEUE_TABLET_COUNTS_PATH, get(queue_counts))
        .route(
            EXPERIMENTAL_QUEUE_TABLET_DEAD_LETTERS_PATH,
            get(dead_letter_history),
        )
        .route(
            EXPERIMENTAL_QUEUE_TABLET_REDRIVES_PATH,
            get(redrive_history),
        )
        .layer(DefaultBodyLimit::max(TABLET_REQUEST_BODY_BYTES))
        .with_state(state)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueMutationRequest {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    operation: QueueOperationRequest,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum QueueOperationRequest {
    Enqueue {
        #[serde(default)]
        partition: u32,
        envelope: Box<StrictEventEnvelope>,
    },
    Acquire {
        #[serde(default)]
        partition: u32,
        consumer: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        consumer_epoch: u64,
        max_messages: u16,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        visibility_timeout_ms: Option<u64>,
    },
    Acknowledge {
        #[serde(default)]
        partition: u32,
        consumer: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        consumer_epoch: u64,
        lease_token: String,
    },
    ExtendLease {
        #[serde(default)]
        partition: u32,
        consumer: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        consumer_epoch: u64,
        lease_token: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        extension_ms: u64,
    },
    Release {
        #[serde(default)]
        partition: u32,
        consumer: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        consumer_epoch: u64,
        lease_token: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        delay_ms: u64,
        #[serde(default)]
        reason: Option<String>,
    },
    Nack {
        #[serde(default)]
        partition: u32,
        consumer: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        consumer_epoch: u64,
        lease_token: String,
        reason: String,
    },
    Reject {
        #[serde(default)]
        partition: u32,
        consumer: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        consumer_epoch: u64,
        lease_token: String,
        reason: String,
    },
    Redrive {
        #[serde(default)]
        partition: u32,
        message_id: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        dead_letter_history_id: u64,
    },
    Maintain {
        #[serde(default)]
        partition: u32,
    },
}

impl QueueOperationRequest {
    fn to_tablet_operation(&self) -> QueueTabletOperation {
        match self {
            Self::Enqueue {
                partition,
                envelope,
            } => QueueTabletOperation::Enqueue(Box::new(QueueEnqueueCommand {
                partition: *partition,
                envelope: envelope.as_ref().clone().into(),
            })),
            Self::Acquire {
                partition,
                consumer,
                consumer_epoch,
                max_messages,
                visibility_timeout_ms,
            } => QueueTabletOperation::Acquire(QueueAcquireCommand {
                partition: *partition,
                consumer: consumer.clone(),
                consumer_epoch: *consumer_epoch,
                max_messages: *max_messages,
                visibility_timeout_ms: *visibility_timeout_ms,
            }),
            Self::Acknowledge {
                partition,
                consumer,
                consumer_epoch,
                lease_token,
            } => QueueTabletOperation::Acknowledge(QueueAcknowledgeCommand {
                partition: *partition,
                consumer: consumer.clone(),
                consumer_epoch: *consumer_epoch,
                lease_token: lease_token.clone(),
            }),
            Self::ExtendLease {
                partition,
                consumer,
                consumer_epoch,
                lease_token,
                extension_ms,
            } => QueueTabletOperation::ExtendLease(QueueExtendLeaseCommand {
                partition: *partition,
                consumer: consumer.clone(),
                consumer_epoch: *consumer_epoch,
                lease_token: lease_token.clone(),
                extension_ms: *extension_ms,
            }),
            Self::Release {
                partition,
                consumer,
                consumer_epoch,
                lease_token,
                delay_ms,
                reason,
            } => QueueTabletOperation::Release(QueueReleaseCommand {
                partition: *partition,
                consumer: consumer.clone(),
                consumer_epoch: *consumer_epoch,
                lease_token: lease_token.clone(),
                delay_ms: *delay_ms,
                reason: reason.clone(),
            }),
            Self::Nack {
                partition,
                consumer,
                consumer_epoch,
                lease_token,
                reason,
            } => QueueTabletOperation::Nack(QueueNackCommand {
                partition: *partition,
                consumer: consumer.clone(),
                consumer_epoch: *consumer_epoch,
                lease_token: lease_token.clone(),
                reason: reason.clone(),
            }),
            Self::Reject {
                partition,
                consumer,
                consumer_epoch,
                lease_token,
                reason,
            } => QueueTabletOperation::Reject(QueueRejectCommand {
                partition: *partition,
                consumer: consumer.clone(),
                consumer_epoch: *consumer_epoch,
                lease_token: lease_token.clone(),
                reason: reason.clone(),
            }),
            Self::Redrive {
                partition,
                message_id,
                dead_letter_history_id,
            } => QueueTabletOperation::Redrive(QueueRedriveCommand {
                partition: *partition,
                message_id: message_id.clone(),
                dead_letter_history_id: *dead_letter_history_id,
            }),
            Self::Maintain { partition } => QueueTabletOperation::Maintain(QueueMaintainCommand {
                partition: *partition,
            }),
        }
    }
}

async fn submit_mutation(
    State(state): State<QueueTabletApiState>,
    request: Result<Json<QueueMutationRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<QueueTabletMutationResponse>)> {
    let Json(request) = request.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    state
        .service
        .ensure_healthy()
        .map_err(TabletApiError::Profile)?;
    let operation = request.operation.to_tablet_operation();
    // Validate all semantic input before consulting local consensus state. The
    // timestamp is server-owned and does not participate in request identity.
    QueueTabletCommand::new(
        state.service.scope(),
        request.idempotency_key.clone(),
        0,
        operation.clone(),
    )?;
    let proposal_id = queue_proposal_id_for(state.service.scope(), &request.idempotency_key)?;
    let _write_guard = state.write_serial.lock().await;
    let commits = state.consensus.subscribe_commits();

    let initial = state.consensus.lookup(proposal_id).await?;
    let (lookup, replayed) = match initial {
        ProposalLookup::Unknown => {
            let applied_at_ms = state
                .clock
                .wall_time_ms()
                .max(state.service.last_applied_time_ms()?);
            let command = QueueTabletCommand::new(
                state.service.scope(),
                request.idempotency_key.clone(),
                applied_at_ms,
                operation,
            )?;
            let payload = command.encode(state.service.scope())?;
            let (lookup, replayed) = match state
                .consensus
                .propose(proposal_id, request.expected_term, payload)
                .await
            {
                Ok(lookup) => (lookup, false),
                Err(ConsensusProbeError::Consensus(ConsensusError::DuplicateProposal(_))) => {
                    (state.consensus.lookup(proposal_id).await?, true)
                }
                Err(error) => return Err(error.into()),
            };
            (lookup, replayed)
        }
        existing => {
            validate_existing_request(&existing, state.service.scope(), &request)?;
            (existing, true)
        }
    };

    if let Some(response) = committed_response(&state.service, &lookup, &request, replayed)? {
        return Ok((committed_http_status(replayed), Json(response)));
    }

    wait_for_committed_response(&state, commits, proposal_id, &request, replayed).await
}

async fn wait_for_committed_response(
    state: &QueueTabletApiState,
    mut commits: broadcast::Receiver<CommittedProposal>,
    proposal_id: u64,
    request: &QueueMutationRequest,
    replayed: bool,
) -> TabletApiResult<(StatusCode, Json<QueueTabletMutationResponse>)> {
    let deadline = tokio::time::Instant::now() + state.commit_wait;
    loop {
        let notification = tokio::time::timeout_at(deadline, commits.recv()).await;
        match notification {
            Ok(Ok(committed)) => {
                if committed.receipt.proposal_id.get() == proposal_id {
                    let lookup = ProposalLookup::Committed(committed);
                    if let Some(response) =
                        committed_response(&state.service, &lookup, request, replayed)?
                    {
                        return Ok((committed_http_status(replayed), Json(response)));
                    }
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                let lookup = state.consensus.lookup(proposal_id).await?;
                if let Some(response) =
                    committed_response(&state.service, &lookup, request, replayed)?
                {
                    return Ok((committed_http_status(replayed), Json(response)));
                }
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                return Err(TabletApiError::Consensus(
                    ConsensusProbeError::ActorUnavailable,
                ));
            }
            Err(_) => {
                let lookup = state.consensus.lookup(proposal_id).await?;
                if let Some(response) =
                    committed_response(&state.service, &lookup, request, replayed)?
                {
                    return Ok((committed_http_status(replayed), Json(response)));
                }
                return Ok((
                    StatusCode::ACCEPTED,
                    Json(unresolved_response(proposal_id, &lookup)),
                ));
            }
        }
    }
}

fn unresolved_response(proposal_id: u64, lookup: &ProposalLookup) -> QueueTabletMutationResponse {
    match lookup {
        ProposalLookup::Unknown => QueueTabletMutationResponse::unknown(proposal_id),
        ProposalLookup::Pending { .. } => QueueTabletMutationResponse::pending(proposal_id),
        ProposalLookup::Committed(_) => unreachable!("committed lookups return a response"),
    }
}

const fn committed_http_status(replayed: bool) -> StatusCode {
    if replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    }
}

fn validate_existing_request(
    lookup: &ProposalLookup,
    scope: &QueueTabletScope,
    request: &QueueMutationRequest,
) -> TabletApiResult<()> {
    let payload = match lookup {
        ProposalLookup::Unknown => return Ok(()),
        ProposalLookup::Pending { payload } => payload,
        ProposalLookup::Committed(committed) => &committed.payload,
    };
    let command = QueueTabletCommand::decode(payload, scope).map_err(|error| {
        TabletApiError::Profile(format!(
            "tracked consensus command is not a valid Queue tablet command: {error}"
        ))
    })?;
    if command.idempotency_key != request.idempotency_key
        || command.operation != request.operation.to_tablet_operation()
    {
        return Err(TabletApiError::IdempotencyConflict);
    }
    Ok(())
}

fn committed_response(
    service: &QueueTabletService,
    lookup: &ProposalLookup,
    request: &QueueMutationRequest,
    replayed: bool,
) -> TabletApiResult<Option<QueueTabletMutationResponse>> {
    validate_existing_request(lookup, service.scope(), request)?;
    match lookup {
        ProposalLookup::Committed(committed) => {
            let receipt = receipt_for_response(service.committed_receipt(committed)?, replayed);
            Ok(Some(QueueTabletMutationResponse::committed(receipt)))
        }
        ProposalLookup::Unknown | ProposalLookup::Pending { .. } => Ok(None),
    }
}

fn receipt_for_response(mut receipt: QueueTabletReceipt, replayed: bool) -> QueueTabletReceipt {
    if replayed {
        receipt.disposition = QueueTabletDisposition::Replayed;
    }
    receipt
}

async fn lookup_mutation(
    State(state): State<QueueTabletApiState>,
    Path(proposal_id): Path<u64>,
) -> TabletApiResult<Json<QueueTabletMutationResponse>> {
    let lookup = state.consensus.lookup(proposal_id).await?;
    let response = match lookup {
        ProposalLookup::Unknown => QueueTabletMutationResponse::unknown(proposal_id),
        ProposalLookup::Pending { .. } => QueueTabletMutationResponse::pending(proposal_id),
        ProposalLookup::Committed(committed) => {
            QueueTabletMutationResponse::committed(state.service.committed_receipt(&committed)?)
        }
    };
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryQuery {
    #[serde(default = "default_history_limit")]
    limit: usize,
}

const fn default_history_limit() -> usize {
    100
}

fn validate_history_limit(limit: usize) -> TabletApiResult<()> {
    if limit == 0 || limit > MAX_HISTORY_RECORDS {
        Err(TabletApiError::InvalidRequest(format!(
            "limit must be between 1 and {MAX_HISTORY_RECORDS}"
        )))
    } else {
        Ok(())
    }
}

async fn queue_counts(
    State(state): State<QueueTabletApiState>,
) -> TabletApiResult<Json<QueueTabletCountsResponse>> {
    let snapshot = state.service.snapshot()?;
    Ok(Json(QueueTabletCountsResponse {
        observation_scope: "local",
        read_consistency: "local_profile_applied_stale_capable",
        counts: snapshot.counts,
    }))
}

async fn dead_letter_history(
    State(state): State<QueueTabletApiState>,
    Query(query): Query<HistoryQuery>,
) -> TabletApiResult<Json<QueueTabletDeadLettersResponse>> {
    validate_history_limit(query.limit)?;
    Ok(Json(QueueTabletDeadLettersResponse {
        observation_scope: "local",
        read_consistency: "local_profile_applied_stale_capable",
        records: state.service.dead_letter_history(query.limit)?,
    }))
}

async fn redrive_history(
    State(state): State<QueueTabletApiState>,
    Query(query): Query<HistoryQuery>,
) -> TabletApiResult<Json<QueueTabletRedrivesResponse>> {
    validate_history_limit(query.limit)?;
    Ok(Json(QueueTabletRedrivesResponse {
        observation_scope: "local",
        read_consistency: "local_profile_applied_stale_capable",
        records: state.service.redrive_history(query.limit)?,
    }))
}

async fn tablet_status(
    State(state): State<QueueTabletApiState>,
) -> TabletApiResult<Json<QueueTabletStatus>> {
    // Sampling the profile first guarantees it cannot appear ahead of the
    // later actor-owned consensus snapshot.
    let profile = state.service.snapshot()?;
    let consensus = state.consensus.status().await?;
    Ok(Json(QueueTabletStatus::new(
        state.service.scope(),
        &consensus,
        profile,
    )?))
}

#[derive(Debug)]
struct QueueTabletSnapshot {
    last_profile_mutation_index: u64,
    last_applied_time_ms: u64,
    applied_command_count: u64,
    counts: QueueTabletCounts,
    state_digest: String,
}

#[derive(Debug, Serialize)]
struct QueueTabletStatus {
    capability: &'static str,
    stability: &'static str,
    production_readiness: &'static str,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    tablet_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    tablet_epoch: u64,
    resource: String,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    node_id: u64,
    role: &'static str,
    #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
    leader_id: Option<u64>,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    term: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    consensus_commit_index: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    consensus_applied_index: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    last_profile_mutation_index: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    last_applied_time_ms: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    applied_command_count: u64,
    counts: QueueTabletCounts,
    state_digest: String,
    write_guarantee: &'static str,
    read_consistency: &'static str,
    linearizable_read_barrier: bool,
}

impl QueueTabletStatus {
    fn new(
        scope: &QueueTabletScope,
        consensus: &ConsensusStatus,
        profile: QueueTabletSnapshot,
    ) -> Result<Self, String> {
        if profile.last_profile_mutation_index > consensus.applied_index.get() {
            return Err(format!(
                "Queue profile mutation index {} is ahead of consensus applied index {}",
                profile.last_profile_mutation_index,
                consensus.applied_index.get()
            ));
        }
        Ok(Self {
            capability: "single_partition_queue_tablet",
            stability: "experimental",
            production_readiness: "not_production_ready",
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            node_id: consensus.node_id.get(),
            role: match consensus.role {
                ConsensusRole::Follower => "follower",
                ConsensusRole::PreCandidate => "pre_candidate",
                ConsensusRole::Candidate => "candidate",
                ConsensusRole::Leader => "leader",
            },
            leader_id: consensus.leader_id.map(epoch_consensus::NodeId::get),
            term: consensus.term.get(),
            consensus_commit_index: consensus.commit_index.get(),
            consensus_applied_index: consensus.applied_index.get(),
            last_profile_mutation_index: profile.last_profile_mutation_index,
            last_applied_time_ms: profile.last_applied_time_ms,
            applied_command_count: profile.applied_command_count,
            counts: profile.counts,
            state_digest: profile.state_digest,
            write_guarantee: "fixed_three_voter_majority_persisted_then_local_profile_applied",
            read_consistency: "local_profile_applied_stale_capable",
            linearizable_read_barrier: false,
        })
    }
}

#[derive(Debug, Serialize)]
struct QueueTabletCountsResponse {
    observation_scope: &'static str,
    read_consistency: &'static str,
    counts: QueueTabletCounts,
}

#[derive(Debug, Serialize)]
struct QueueTabletDeadLettersResponse {
    observation_scope: &'static str,
    read_consistency: &'static str,
    records: Vec<QueueTabletDeadLetterHistory>,
}

#[derive(Debug, Serialize)]
struct QueueTabletRedrivesResponse {
    observation_scope: &'static str,
    read_consistency: &'static str,
    records: Vec<QueueTabletRedriveHistory>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum MutationState {
    Unknown,
    Pending,
    Committed,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum OutcomeCertainty {
    Unknown,
    Committed,
}

#[derive(Debug, Serialize)]
struct QueueTabletMutationResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    proposal_id: u64,
    state: MutationState,
    outcome_certainty: OutcomeCertainty,
    observation_scope: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<QueueTabletReceipt>,
}

impl QueueTabletMutationResponse {
    fn unknown(proposal_id: u64) -> Self {
        Self {
            proposal_id,
            state: MutationState::Unknown,
            outcome_certainty: OutcomeCertainty::Unknown,
            observation_scope: "local",
            receipt: None,
        }
    }

    fn pending(proposal_id: u64) -> Self {
        Self {
            proposal_id,
            state: MutationState::Pending,
            outcome_certainty: OutcomeCertainty::Unknown,
            observation_scope: "local",
            receipt: None,
        }
    }

    fn committed(receipt: QueueTabletReceipt) -> Self {
        Self {
            proposal_id: receipt.proposal_id,
            state: MutationState::Committed,
            outcome_certainty: OutcomeCertainty::Committed,
            observation_scope: "local",
            receipt: Some(receipt),
        }
    }
}

#[cfg(test)]
mod tests;
