//! Experimental Event Bus ingress/outbox tablet over fixed-voter consensus.

use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{get, post},
};
use epoch_bus::{
    ArchivedEvent, BusConfig, DeliveryAttempt, DeliveryAttemptOutcome, DeliveryCounts,
    DeliveryPolicy, DeliveryRecord, DeliveryRetryPolicy, DeliveryState, DeliveryStateKind,
    EventFilter, MAX_DELIVERY_QUERY_RESULTS, MAX_REPLAY_EVENTS, Subscription, SubscriptionTarget,
};
use epoch_consensus::{
    ApplicationSnapshot, CommittedProposal, ConsensusError, ConsensusRole, ConsensusStatus,
    LogIndex, ProposalLookup,
};
use epoch_core::{Clock, DurabilityProfile, EpochError};
use epoch_tablet::{
    BusTablet, BusTabletCommand, BusTabletDisposition, BusTabletOperation, BusTabletReceipt,
    BusTabletScope, CommittedCommand, MAX_BUS_TABLET_COMMAND_BYTES, TabletError,
    bus_proposal_id_for,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};

use crate::consensus::{CommittedProposalApplier, ConsensusProbeError, ConsensusProbeHandle};
use crate::tablet_http::{
    StrictEventEnvelope, TabletApiError, TabletApiResult, TabletReadMetadata,
    deserialize_u64_from_number_or_decimal, hex_digest, serialize_optional_u64_as_decimal,
    serialize_u64_as_decimal, tablet_read_metadata,
};

pub const EXPERIMENTAL_BUS_TABLET_STATUS_PATH: &str = "/experimental/v1/tablets/bus/status";
pub const EXPERIMENTAL_BUS_TABLET_MUTATIONS_PATH: &str = "/experimental/v1/tablets/bus/mutations";
pub const EXPERIMENTAL_BUS_TABLET_MUTATION_PATH: &str =
    "/experimental/v1/tablets/bus/mutations/{proposal_id}";
pub const EXPERIMENTAL_BUS_TABLET_ARCHIVE_REPLAY_PATH: &str =
    "/experimental/v1/tablets/bus/archive/replay";
pub const EXPERIMENTAL_BUS_TABLET_DELIVERY_QUERY_PATH: &str =
    "/experimental/v1/tablets/bus/deliveries/query";

const TABLET_REQUEST_BODY_BYTES: usize = MAX_BUS_TABLET_COMMAND_BYTES + 16 * 1024;
const DEFAULT_REPLAY_LIMIT: usize = 100;
const BUS_APPLICATION_SNAPSHOT_FORMAT_ID: [u8; 16] = *b"EVENTBUS_STATEV1";
const BUS_APPLICATION_SNAPSHOT_VERSION: u16 = 1;
pub const DEFAULT_COMMIT_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct BusTabletService {
    scope: BusTabletScope,
    config: BusConfig,
    tablet: RwLock<BusTablet>,
    failure: RwLock<Option<String>>,
}

impl BusTabletService {
    pub fn new(scope: BusTabletScope, config: BusConfig) -> Result<Arc<Self>, TabletError> {
        let tablet = BusTablet::new(scope.clone(), config.clone())?;
        Ok(Arc::new(Self {
            scope,
            config,
            tablet: RwLock::new(tablet),
            failure: RwLock::new(None),
        }))
    }

    pub fn with_default_config(scope: BusTabletScope) -> Result<Arc<Self>, TabletError> {
        Self::new(scope, BusConfig::default())
    }

    pub fn scope(&self) -> &BusTabletScope {
        &self.scope
    }

    pub fn last_profile_mutation_index(&self) -> Result<u64, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Event Bus tablet read lock was poisoned".to_owned())
            .map(|tablet| tablet.last_applied_command_index())
    }

    pub fn last_applied_time_ms(&self) -> Result<u64, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Event Bus tablet read lock was poisoned".to_owned())
            .map(|tablet| tablet.last_applied_time_ms())
    }

    fn ensure_healthy(&self) -> Result<(), String> {
        let failure = self
            .failure
            .read()
            .map_err(|_| "Event Bus tablet failure lock was poisoned".to_owned())?;
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

    fn apply_one(&self, committed: &CommittedProposal) -> Result<BusTabletReceipt, String> {
        self.ensure_healthy()?;
        let result = self
            .tablet
            .write()
            .map_err(|_| "Event Bus tablet write lock was poisoned".to_owned())?
            .apply(committed_command(committed))
            .map_err(|error| error.to_string());
        result.map_err(|error| self.fail(error))
    }

    fn committed_receipt(&self, committed: &CommittedProposal) -> Result<BusTabletReceipt, String> {
        self.ensure_healthy()?;
        let result = self
            .tablet
            .read()
            .map_err(|_| self.fail("Event Bus tablet read lock was poisoned"))?
            .receipt_for_committed(committed_command(committed));
        match result {
            Ok(Some(receipt)) => Ok(receipt),
            Ok(None) => Err(self.fail(format!(
                "consensus commit {} was not applied by the Event Bus profile actor",
                committed.receipt.proposal_id
            ))),
            Err(error) => Err(self.fail(error.to_string())),
        }
    }

    fn snapshot(&self) -> Result<BusTabletSnapshot, String> {
        self.ensure_healthy()?;
        let tablet = self
            .tablet
            .read()
            .map_err(|_| "Event Bus tablet read lock was poisoned".to_owned())?;
        let delivery_counts = tablet.delivery_counts();
        Ok(BusTabletSnapshot {
            last_profile_mutation_index: tablet.last_applied_command_index(),
            last_applied_time_ms: tablet.last_applied_time_ms(),
            applied_command_count: usize_to_u64(
                tablet.applied_command_count(),
                "Event Bus tablet command count",
            )?,
            route_plan_version: tablet.route_plan_version(),
            subscription_count: usize_to_u64(
                tablet.subscription_count(),
                "Event Bus subscription count",
            )?,
            commit_position: tablet.commit_position(),
            archived_event_count: usize_to_u64(
                tablet.archived_event_count(),
                "Event Bus archive count",
            )?,
            delivery_counts,
            business_state_digest: hex_digest(tablet.business_state_digest()),
            state_digest: hex_digest(tablet.state_digest()),
        })
    }

    fn replay_archive(
        &self,
        request: &BusArchiveReplayRequest,
    ) -> Result<Vec<ArchivedEvent>, BusArchiveReplayError> {
        self.ensure_healthy()
            .map_err(BusArchiveReplayError::Profile)?;
        let result = self
            .tablet
            .read()
            .map_err(|_| {
                BusArchiveReplayError::Profile("Event Bus tablet read lock was poisoned".to_owned())
            })?
            .replay(
                request.from_ms,
                request.to_ms,
                request.filter.as_ref(),
                request.limit,
            );
        match result {
            Ok(records) => Ok(records),
            Err(TabletError::Profile(EpochError::InvalidArgument(message))) => {
                Err(BusArchiveReplayError::InvalidRequest(message))
            }
            Err(error) => Err(BusArchiveReplayError::Profile(self.fail(error.to_string()))),
        }
    }

    fn query_deliveries(
        &self,
        request: &BusDeliveryQueryRequest,
    ) -> Result<Vec<DeliveryRecord>, BusDeliveryQueryError> {
        self.ensure_healthy()
            .map_err(BusDeliveryQueryError::Profile)?;
        let result = self
            .tablet
            .read()
            .map_err(|_| {
                BusDeliveryQueryError::Profile("Event Bus tablet read lock was poisoned".to_owned())
            })?
            .deliveries(
                request.subscription.as_deref(),
                request.state,
                request.limit,
            );
        match result {
            Ok(records) => Ok(records),
            Err(TabletError::Profile(EpochError::InvalidArgument(message))) => {
                Err(BusDeliveryQueryError::InvalidRequest(message))
            }
            Err(error) => Err(BusDeliveryQueryError::Profile(self.fail(error.to_string()))),
        }
    }
}

enum BusArchiveReplayError {
    InvalidRequest(String),
    Profile(String),
}

enum BusDeliveryQueryError {
    InvalidRequest(String),
    Profile(String),
}

fn usize_to_u64(value: usize, field: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{field} exceeds u64"))
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

impl CommittedProposalApplier for BusTabletService {
    fn replay(&self, committed: &[CommittedProposal]) -> Result<(), String> {
        let mut history = committed.to_vec();
        history.sort_by_key(|proposal| proposal.receipt.log_index.get());
        let mut rebuilt = BusTablet::new(self.scope.clone(), self.config.clone())
            .map_err(|error| error.to_string())?;
        for proposal in &history {
            rebuilt
                .apply(committed_command(proposal))
                .map_err(|error| self.fail(error.to_string()))?;
        }
        *self
            .tablet
            .write()
            .map_err(|_| self.fail("Event Bus tablet write lock was poisoned"))? = rebuilt;
        Ok(())
    }

    fn apply(&self, committed: &CommittedProposal) -> Result<(), String> {
        self.apply_one(committed).map(|_| ())
    }

    fn capture_snapshot(
        &self,
        checkpoint_index: LogIndex,
        retained: &[CommittedProposal],
    ) -> Result<ApplicationSnapshot, String> {
        self.ensure_healthy()?;
        let tablet = self
            .tablet
            .read()
            .map_err(|_| "Event Bus tablet read lock was poisoned".to_owned())?;
        if tablet.last_applied_command_index() > checkpoint_index.get() {
            return Err(format!(
                "Event Bus applied index {} exceeds consensus checkpoint index {}",
                tablet.last_applied_command_index(),
                checkpoint_index
            ));
        }
        let mut retained_ids = BTreeSet::new();
        for committed in retained {
            let proposal_id = committed.receipt.proposal_id.get();
            if !retained_ids.insert(proposal_id) {
                return Err(format!(
                    "Event Bus retry proposal {proposal_id} appears more than once"
                ));
            }
            tablet
                .receipt_for_committed(committed_command(committed))
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!("Event Bus retry proposal {proposal_id} has no typed applied result")
                })?;
        }
        let payload = tablet
            .encode_snapshot(&retained_ids)
            .map_err(|error| error.to_string())?;
        ApplicationSnapshot::new(
            checkpoint_index,
            BUS_APPLICATION_SNAPSHOT_FORMAT_ID,
            BUS_APPLICATION_SNAPSHOT_VERSION,
            tablet.state_digest(),
            payload,
        )
        .map_err(|error| error.to_string())
    }

    fn install_snapshot(&self, snapshot: &ApplicationSnapshot) -> Result<(), String> {
        self.ensure_healthy()?;
        let result: Result<BusTablet, String> = (|| {
            if snapshot.format_id() != BUS_APPLICATION_SNAPSHOT_FORMAT_ID
                || snapshot.format_version() != BUS_APPLICATION_SNAPSHOT_VERSION
            {
                return Err("application snapshot is not a supported Event Bus image".into());
            }
            let restored = BusTablet::decode_snapshot(&self.scope, snapshot.payload())
                .map_err(|error| error.to_string())?;
            let mut expected_config = self.config.clone();
            expected_config.durability = DurabilityProfile::Volatile;
            expected_config.delivery_outbox = true;
            if restored.last_applied_command_index() > snapshot.checkpoint_index().get()
                || restored.state_digest() != snapshot.state_digest()
                || restored.bus_config() != &expected_config
            {
                return Err(
                    "Event Bus application snapshot index, state digest, or configuration is invalid"
                        .into(),
                );
            }
            Ok(restored)
        })();
        match result {
            Ok(restored) => {
                *self
                    .tablet
                    .write()
                    .map_err(|_| self.fail("Event Bus tablet write lock was poisoned"))? = restored;
                Ok(())
            }
            Err(error) => Err(self.fail(error)),
        }
    }

    fn supports_native_snapshots(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct BusTabletApiState {
    service: Arc<BusTabletService>,
    consensus: ConsensusProbeHandle,
    clock: Arc<dyn Clock>,
    commit_wait: Duration,
    write_serial: Arc<Mutex<()>>,
}

pub fn router(
    service: Arc<BusTabletService>,
    consensus: ConsensusProbeHandle,
    clock: Arc<dyn Clock>,
    commit_wait: Duration,
) -> Router {
    let state = BusTabletApiState {
        service,
        consensus,
        clock,
        commit_wait,
        write_serial: Arc::new(Mutex::new(())),
    };
    Router::new()
        .route(EXPERIMENTAL_BUS_TABLET_STATUS_PATH, get(tablet_status))
        .route(
            EXPERIMENTAL_BUS_TABLET_MUTATIONS_PATH,
            post(submit_mutation),
        )
        .route(EXPERIMENTAL_BUS_TABLET_MUTATION_PATH, get(lookup_mutation))
        .route(
            EXPERIMENTAL_BUS_TABLET_ARCHIVE_REPLAY_PATH,
            post(replay_archive),
        )
        .route(
            EXPERIMENTAL_BUS_TABLET_DELIVERY_QUERY_PATH,
            post(query_deliveries),
        )
        .layer(DefaultBodyLimit::max(TABLET_REQUEST_BODY_BYTES))
        .with_state(state)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BusMutationRequest {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    operation: BusOperationRequest,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BusOperationRequest {
    UpsertSubscription {
        subscription: Subscription,
    },
    RemoveSubscription {
        name: String,
    },
    Publish {
        envelope: Box<StrictEventEnvelope>,
    },
    AcquireDeliveries {
        subscription: String,
        dispatcher: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        dispatcher_epoch: u64,
        max_deliveries: u16,
    },
    AcknowledgeDelivery {
        delivery_id: String,
        dispatcher: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        dispatcher_epoch: u64,
        lease_token: String,
    },
    FailDelivery {
        delivery_id: String,
        dispatcher: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        dispatcher_epoch: u64,
        lease_token: String,
        reason: String,
    },
    MaintainDeliveries {
        max_deliveries: u16,
    },
}

impl BusOperationRequest {
    fn to_tablet_operation(&self) -> BusTabletOperation {
        match self {
            Self::UpsertSubscription { subscription } => BusTabletOperation::UpsertSubscription {
                subscription: subscription.clone(),
            },
            Self::RemoveSubscription { name } => {
                BusTabletOperation::RemoveSubscription { name: name.clone() }
            }
            Self::Publish { envelope } => BusTabletOperation::Publish {
                envelope: envelope.as_ref().clone().into(),
            },
            Self::AcquireDeliveries {
                subscription,
                dispatcher,
                dispatcher_epoch,
                max_deliveries,
            } => BusTabletOperation::AcquireDeliveries {
                subscription: subscription.clone(),
                dispatcher: dispatcher.clone(),
                dispatcher_epoch: *dispatcher_epoch,
                max_deliveries: *max_deliveries,
            },
            Self::AcknowledgeDelivery {
                delivery_id,
                dispatcher,
                dispatcher_epoch,
                lease_token,
            } => BusTabletOperation::AcknowledgeDelivery {
                delivery_id: delivery_id.clone(),
                dispatcher: dispatcher.clone(),
                dispatcher_epoch: *dispatcher_epoch,
                lease_token: lease_token.clone(),
            },
            Self::FailDelivery {
                delivery_id,
                dispatcher,
                dispatcher_epoch,
                lease_token,
                reason,
            } => BusTabletOperation::FailDelivery {
                delivery_id: delivery_id.clone(),
                dispatcher: dispatcher.clone(),
                dispatcher_epoch: *dispatcher_epoch,
                lease_token: lease_token.clone(),
                reason: reason.clone(),
            },
            Self::MaintainDeliveries { max_deliveries } => BusTabletOperation::MaintainDeliveries {
                max_deliveries: *max_deliveries,
            },
        }
    }
}

async fn submit_mutation(
    State(state): State<BusTabletApiState>,
    request: Result<Json<BusMutationRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<BusTabletMutationResponse>)> {
    let Json(request) = request.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    state
        .service
        .ensure_healthy()
        .map_err(TabletApiError::Profile)?;
    let operation = request.operation.to_tablet_operation();
    BusTabletCommand::new(
        state.service.scope(),
        request.idempotency_key.clone(),
        0,
        operation.clone(),
    )?;
    let proposal_id = bus_proposal_id_for(state.service.scope(), &request.idempotency_key)?;
    let _write_guard = state.write_serial.lock().await;
    let commits = state.consensus.subscribe_commits();

    let initial = state.consensus.lookup(proposal_id).await?;
    let (lookup, replayed) = match initial {
        ProposalLookup::Unknown => {
            let applied_at_ms = state
                .clock
                .wall_time_ms()
                .max(state.service.last_applied_time_ms()?);
            let command = BusTabletCommand::new(
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
    state: &BusTabletApiState,
    mut commits: broadcast::Receiver<CommittedProposal>,
    proposal_id: u64,
    request: &BusMutationRequest,
    replayed: bool,
) -> TabletApiResult<(StatusCode, Json<BusTabletMutationResponse>)> {
    let deadline = tokio::time::Instant::now() + state.commit_wait;
    loop {
        match tokio::time::timeout_at(deadline, commits.recv()).await {
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

fn unresolved_response(proposal_id: u64, lookup: &ProposalLookup) -> BusTabletMutationResponse {
    match lookup {
        ProposalLookup::Unknown => BusTabletMutationResponse::unknown(proposal_id),
        ProposalLookup::Pending { .. } => BusTabletMutationResponse::pending(proposal_id),
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
    scope: &BusTabletScope,
    request: &BusMutationRequest,
) -> TabletApiResult<()> {
    let payload = match lookup {
        ProposalLookup::Unknown => return Ok(()),
        ProposalLookup::Pending { payload } => payload,
        ProposalLookup::Committed(committed) => &committed.payload,
    };
    let command = BusTabletCommand::decode(payload, scope).map_err(|error| {
        TabletApiError::Profile(format!(
            "tracked consensus command is not a valid Event Bus tablet command: {error}"
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
    service: &BusTabletService,
    lookup: &ProposalLookup,
    request: &BusMutationRequest,
    replayed: bool,
) -> TabletApiResult<Option<BusTabletMutationResponse>> {
    validate_existing_request(lookup, service.scope(), request)?;
    match lookup {
        ProposalLookup::Committed(committed) => {
            let receipt = receipt_for_response(service.committed_receipt(committed)?, replayed);
            Ok(Some(BusTabletMutationResponse::committed(receipt)))
        }
        ProposalLookup::Unknown | ProposalLookup::Pending { .. } => Ok(None),
    }
}

fn receipt_for_response(mut receipt: BusTabletReceipt, replayed: bool) -> BusTabletReceipt {
    if replayed {
        receipt.disposition = BusTabletDisposition::Replayed;
    }
    receipt
}

async fn lookup_mutation(
    State(state): State<BusTabletApiState>,
    Path(proposal_id): Path<u64>,
) -> TabletApiResult<Json<BusTabletMutationResponse>> {
    let lookup = state.consensus.lookup(proposal_id).await?;
    let response = match lookup {
        ProposalLookup::Unknown => BusTabletMutationResponse::unknown(proposal_id),
        ProposalLookup::Pending { .. } => BusTabletMutationResponse::pending(proposal_id),
        ProposalLookup::Committed(committed) => {
            BusTabletMutationResponse::committed(state.service.committed_receipt(&committed)?)
        }
    };
    Ok(Json(response))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BusArchiveReplayRequest {
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    from_ms: u64,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    to_ms: u64,
    #[serde(default)]
    filter: Option<EventFilter>,
    #[serde(default = "default_replay_limit")]
    limit: usize,
}

const fn default_replay_limit() -> usize {
    DEFAULT_REPLAY_LIMIT
}

async fn replay_archive(
    State(state): State<BusTabletApiState>,
    read: Option<Extension<TabletReadMetadata>>,
    request: Result<Json<BusArchiveReplayRequest>, JsonRejection>,
) -> TabletApiResult<Json<BusArchiveReplayResponse>> {
    let Json(request) = request.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    if request.limit == 0 || request.limit > MAX_REPLAY_EVENTS {
        return Err(TabletApiError::InvalidRequest(format!(
            "limit must be between 1 and {MAX_REPLAY_EVENTS}"
        )));
    }
    let records = state
        .service
        .replay_archive(&request)
        .map_err(|error| match error {
            BusArchiveReplayError::InvalidRequest(message) => {
                TabletApiError::InvalidRequest(message)
            }
            BusArchiveReplayError::Profile(message) => TabletApiError::Profile(message),
        })?
        .into_iter()
        .map(BusArchivedEventResponse::from)
        .collect();
    Ok(Json(BusArchiveReplayResponse {
        read: tablet_read_metadata(read),
        records,
    }))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BusDeliveryQueryRequest {
    #[serde(default)]
    subscription: Option<String>,
    #[serde(default)]
    state: Option<DeliveryStateKind>,
    #[serde(default = "default_replay_limit")]
    limit: usize,
}

async fn query_deliveries(
    State(state): State<BusTabletApiState>,
    read: Option<Extension<TabletReadMetadata>>,
    request: Result<Json<BusDeliveryQueryRequest>, JsonRejection>,
) -> TabletApiResult<Json<BusDeliveryQueryResponse>> {
    let Json(request) = request.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    if request.limit == 0 || request.limit > MAX_DELIVERY_QUERY_RESULTS {
        return Err(TabletApiError::InvalidRequest(format!(
            "limit must be between 1 and {MAX_DELIVERY_QUERY_RESULTS}"
        )));
    }
    let records = state
        .service
        .query_deliveries(&request)
        .map_err(|error| match error {
            BusDeliveryQueryError::InvalidRequest(message) => {
                TabletApiError::InvalidRequest(message)
            }
            BusDeliveryQueryError::Profile(message) => TabletApiError::Profile(message),
        })?
        .into_iter()
        .map(BusDeliveryRecordResponse::from)
        .collect();
    Ok(Json(BusDeliveryQueryResponse {
        read: tablet_read_metadata(read),
        records,
    }))
}

async fn tablet_status(
    State(state): State<BusTabletApiState>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<BusTabletStatus>> {
    // Sampling the profile first guarantees it cannot appear ahead of the
    // later actor-owned consensus snapshot.
    let profile = state.service.snapshot()?;
    let consensus = state.consensus.status().await?;
    Ok(Json(BusTabletStatus::new_with_read(
        state.service.scope(),
        &consensus,
        profile,
        tablet_read_metadata(read),
    )?))
}

#[derive(Debug)]
struct BusTabletSnapshot {
    last_profile_mutation_index: u64,
    last_applied_time_ms: u64,
    applied_command_count: u64,
    route_plan_version: u64,
    subscription_count: u64,
    commit_position: u64,
    archived_event_count: u64,
    delivery_counts: DeliveryCounts,
    business_state_digest: String,
    state_digest: String,
}

#[derive(Debug, Serialize)]
struct BusTabletStatus {
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
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    route_plan_version: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    subscription_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    commit_position: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    archived_event_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pending_delivery_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    in_flight_delivery_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    acknowledged_delivery_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    dead_lettered_delivery_count: u64,
    business_state_digest: String,
    state_digest: String,
    write_guarantee: &'static str,
    #[serde(flatten)]
    read: TabletReadMetadata,
    target_dispatch: &'static str,
    durable_target_outbox: bool,
}

impl BusTabletStatus {
    const TARGET_DISPATCH: &'static str = "external_executor_not_implemented";
    const DURABLE_TARGET_OUTBOX: bool = true;

    fn new_with_read(
        scope: &BusTabletScope,
        consensus: &ConsensusStatus,
        profile: BusTabletSnapshot,
        read: TabletReadMetadata,
    ) -> Result<Self, String> {
        if profile.last_profile_mutation_index > consensus.applied_index.get() {
            return Err(format!(
                "Event Bus profile mutation index {} is ahead of consensus applied index {}",
                profile.last_profile_mutation_index,
                consensus.applied_index.get()
            ));
        }
        Ok(Self {
            capability: "single_partition_event_bus_ingress_outbox_tablet",
            stability: "experimental",
            production_readiness: "not_production_ready",
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            node_id: consensus.node_id.get(),
            role: role_name(consensus.role),
            leader_id: consensus.leader_id.map(epoch_consensus::NodeId::get),
            term: consensus.term.get(),
            consensus_commit_index: consensus.commit_index.get(),
            consensus_applied_index: consensus.applied_index.get(),
            last_profile_mutation_index: profile.last_profile_mutation_index,
            last_applied_time_ms: profile.last_applied_time_ms,
            applied_command_count: profile.applied_command_count,
            route_plan_version: profile.route_plan_version,
            subscription_count: profile.subscription_count,
            commit_position: profile.commit_position,
            archived_event_count: profile.archived_event_count,
            pending_delivery_count: usize_to_u64(
                profile.delivery_counts.pending,
                "pending Event Bus delivery count",
            )?,
            in_flight_delivery_count: usize_to_u64(
                profile.delivery_counts.in_flight,
                "in-flight Event Bus delivery count",
            )?,
            acknowledged_delivery_count: usize_to_u64(
                profile.delivery_counts.acknowledged,
                "acknowledged Event Bus delivery count",
            )?,
            dead_lettered_delivery_count: usize_to_u64(
                profile.delivery_counts.dead_lettered,
                "dead-lettered Event Bus delivery count",
            )?,
            business_state_digest: profile.business_state_digest,
            state_digest: profile.state_digest,
            write_guarantee: "fixed_three_voter_majority_persisted_then_local_profile_applied",
            read,
            target_dispatch: Self::TARGET_DISPATCH,
            durable_target_outbox: Self::DURABLE_TARGET_OUTBOX,
        })
    }
}

const fn role_name(role: ConsensusRole) -> &'static str {
    match role {
        ConsensusRole::Follower => "follower",
        ConsensusRole::PreCandidate => "pre_candidate",
        ConsensusRole::Candidate => "candidate",
        ConsensusRole::Leader => "leader",
    }
}

#[derive(Debug, Serialize)]
struct BusArchiveReplayResponse {
    #[serde(flatten)]
    read: TabletReadMetadata,
    records: Vec<BusArchivedEventResponse>,
}

#[derive(Debug, Serialize)]
struct BusArchivedEventResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    position: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    received_at_ms: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    route_plan_version: u64,
    envelope: StrictEventEnvelope,
}

impl From<ArchivedEvent> for BusArchivedEventResponse {
    fn from(record: ArchivedEvent) -> Self {
        Self {
            position: record.position,
            received_at_ms: record.received_at_ms,
            route_plan_version: record.route_plan_version,
            envelope: record.envelope.into(),
        }
    }
}

#[derive(Debug, Serialize)]
struct BusDeliveryQueryResponse {
    #[serde(flatten)]
    read: TabletReadMetadata,
    records: Vec<BusDeliveryRecordResponse>,
}

#[derive(Debug, Serialize)]
struct BusDeliveryRecordResponse {
    delivery_id: String,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    publish_position: u64,
    subscription: String,
    target: SubscriptionTarget,
    envelope: StrictEventEnvelope,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    route_plan_version: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    created_at_ms: u64,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    expires_at_ms: Option<u64>,
    policy: BusDeliveryPolicyResponse,
    state: BusDeliveryStateResponse,
    attempts: Vec<BusDeliveryAttemptResponse>,
}

#[derive(Debug, Serialize)]
struct BusDeliveryPolicyResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    timeout_ms: u64,
    max_in_flight: u16,
    retry: BusDeliveryRetryPolicyResponse,
}

#[derive(Debug, Serialize)]
struct BusDeliveryRetryPolicyResponse {
    strategy: &'static str,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    initial_delay_ms: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    max_delay_ms: u64,
    jitter_percent: u8,
    max_attempts: u32,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    max_age_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BusDeliveryStateResponse {
    Pending {
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        eligible_at_ms: u64,
    },
    InFlight {
        dispatcher: String,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        dispatcher_epoch: u64,
        attempt: u32,
        lease_token: String,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        lease_deadline_ms: u64,
    },
    Acknowledged {
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        acknowledged_at_ms: u64,
    },
    DeadLettered {
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        dead_lettered_at_ms: u64,
        reason: String,
    },
}

#[derive(Debug, Serialize)]
struct BusDeliveryAttemptResponse {
    attempt: u32,
    dispatcher: String,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    dispatcher_epoch: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    leader_term: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    started_at_ms: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    lease_deadline_ms: u64,
    outcome: BusDeliveryAttemptOutcomeResponse,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BusDeliveryAttemptOutcomeResponse {
    InFlight,
    Acknowledged {
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        completed_at_ms: u64,
    },
    Failed {
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        failed_at_ms: u64,
        reason: String,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "serialize_optional_u64_as_decimal"
        )]
        retry_at_ms: Option<u64>,
    },
}

impl From<DeliveryRecord> for BusDeliveryRecordResponse {
    fn from(record: DeliveryRecord) -> Self {
        Self {
            delivery_id: record.delivery_id,
            publish_position: record.publish_position,
            subscription: record.subscription,
            target: record.target,
            envelope: record.envelope.into(),
            route_plan_version: record.route_plan_version,
            created_at_ms: record.created_at_ms,
            expires_at_ms: record.expires_at_ms,
            policy: record.policy.into(),
            state: record.state.into(),
            attempts: record.attempts.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<DeliveryPolicy> for BusDeliveryPolicyResponse {
    fn from(policy: DeliveryPolicy) -> Self {
        Self {
            timeout_ms: policy.timeout_ms,
            max_in_flight: policy.max_in_flight,
            retry: policy.retry.into(),
        }
    }
}

impl From<DeliveryRetryPolicy> for BusDeliveryRetryPolicyResponse {
    fn from(policy: DeliveryRetryPolicy) -> Self {
        let strategy = match policy.strategy {
            epoch_bus::DeliveryBackoffStrategy::Exponential => "exponential",
            epoch_bus::DeliveryBackoffStrategy::Fixed => "fixed",
        };
        Self {
            strategy,
            initial_delay_ms: policy.initial_delay_ms,
            max_delay_ms: policy.max_delay_ms,
            jitter_percent: policy.jitter_percent,
            max_attempts: policy.max_attempts,
            max_age_ms: policy.max_age_ms,
        }
    }
}

impl From<DeliveryState> for BusDeliveryStateResponse {
    fn from(state: DeliveryState) -> Self {
        match state {
            DeliveryState::Pending { eligible_at_ms } => Self::Pending { eligible_at_ms },
            DeliveryState::InFlight {
                dispatcher,
                dispatcher_epoch,
                attempt,
                lease_token,
                lease_deadline_ms,
                ..
            } => Self::InFlight {
                dispatcher,
                dispatcher_epoch,
                attempt,
                lease_token,
                lease_deadline_ms,
            },
            DeliveryState::Acknowledged { acknowledged_at_ms } => {
                Self::Acknowledged { acknowledged_at_ms }
            }
            DeliveryState::DeadLettered {
                dead_lettered_at_ms,
                reason,
            } => Self::DeadLettered {
                dead_lettered_at_ms,
                reason,
            },
        }
    }
}

impl From<DeliveryAttempt> for BusDeliveryAttemptResponse {
    fn from(attempt: DeliveryAttempt) -> Self {
        Self {
            attempt: attempt.attempt,
            dispatcher: attempt.dispatcher,
            dispatcher_epoch: attempt.dispatcher_epoch,
            leader_term: attempt.leader_term,
            started_at_ms: attempt.started_at_ms,
            lease_deadline_ms: attempt.lease_deadline_ms,
            outcome: attempt.outcome.into(),
        }
    }
}

impl From<DeliveryAttemptOutcome> for BusDeliveryAttemptOutcomeResponse {
    fn from(outcome: DeliveryAttemptOutcome) -> Self {
        match outcome {
            DeliveryAttemptOutcome::InFlight => Self::InFlight,
            DeliveryAttemptOutcome::Acknowledged { completed_at_ms } => {
                Self::Acknowledged { completed_at_ms }
            }
            DeliveryAttemptOutcome::Failed {
                failed_at_ms,
                reason,
                retry_at_ms,
            } => Self::Failed {
                failed_at_ms,
                reason,
                retry_at_ms,
            },
        }
    }
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
struct BusTabletMutationResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    proposal_id: u64,
    state: MutationState,
    outcome_certainty: OutcomeCertainty,
    observation_scope: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<BusTabletReceipt>,
}

impl BusTabletMutationResponse {
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

    fn committed(receipt: BusTabletReceipt) -> Self {
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
mod tests {
    use std::{
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };

    use epoch_consensus::{CommitReceipt, GroupEpoch, GroupId, LogIndex, ProposalId, Term};
    use epoch_core::{EventEnvelope, ManualClock};
    use epoch_tablet::{BusTabletOperationResult, BusTabletOutcome};
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::{net::TcpListener, task::JoinHandle};
    use url::Url;

    use super::*;
    use crate::consensus::{ConsensusProbeConfig, ConsensusProbeRuntime};

    fn scope() -> BusTabletScope {
        BusTabletScope::new(7, 3, "events").unwrap()
    }

    fn event(event_id: &str) -> EventEnvelope {
        let mut envelope = EventEnvelope::new("tests", "order.created", json!({"id": event_id}), 1);
        envelope.id = event_id.to_owned();
        envelope
    }

    fn committed(
        key: &str,
        operation: BusTabletOperation,
        applied_at_ms: u64,
        term: u64,
        index: u64,
    ) -> CommittedProposal {
        let scope = scope();
        let command =
            BusTabletCommand::new(&scope, key, applied_at_ms, operation).expect("valid command");
        CommittedProposal {
            receipt: CommitReceipt {
                group_id: GroupId::new(7).unwrap(),
                group_epoch: GroupEpoch::new(3).unwrap(),
                proposal_id: ProposalId::new(command.proposal_id(&scope).unwrap()).unwrap(),
                term: Term::new(term),
                log_index: LogIndex::new(index),
            },
            payload: command.encode(&scope).unwrap(),
        }
    }

    fn publish(key: &str, event_id: &str, applied_at_ms: u64, index: u64) -> CommittedProposal {
        committed(
            key,
            BusTabletOperation::Publish {
                envelope: event(event_id),
            },
            applied_at_ms,
            2,
            index,
        )
    }

    #[test]
    fn mutation_request_is_strict_and_accepts_browser_safe_integers() {
        let request: BusMutationRequest = serde_json::from_value(json!({
            "idempotency_key": "publish-1",
            "expected_term": "7",
            "operation": {
                "kind": "publish",
                "envelope": {
                    "id": "event-1",
                    "source": "tests",
                    "type": "order.created",
                    "time_ms": "9007199254740993",
                    "payload": {"order_id": 1}
                }
            }
        }))
        .unwrap();
        assert_eq!(request.expected_term, 7);
        let BusTabletOperation::Publish { envelope } = request.operation.to_tablet_operation()
        else {
            panic!("expected publish");
        };
        assert_eq!(envelope.time_ms, 9_007_199_254_740_993);

        let acquire: BusMutationRequest = serde_json::from_value(json!({
            "idempotency_key": "acquire-1",
            "expected_term": "7",
            "operation": {
                "kind": "acquire_deliveries",
                "subscription": "sink",
                "dispatcher": "sender",
                "dispatcher_epoch": "9007199254740993",
                "max_deliveries": 10
            }
        }))
        .unwrap();
        assert!(matches!(
            acquire.operation.to_tablet_operation(),
            BusTabletOperation::AcquireDeliveries {
                dispatcher_epoch: 9_007_199_254_740_993,
                max_deliveries: 10,
                ..
            }
        ));

        assert!(
            serde_json::from_value::<BusMutationRequest>(json!({
                "idempotency_key": "publish-1",
                "expected_term": "7",
                "unexpected": true,
                "operation": {
                    "kind": "remove_subscription",
                    "name": "sink"
                }
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<BusMutationRequest>(json!({
                "idempotency_key": "publish-1",
                "expected_term": "7",
                "operation": {
                    "kind": "publish",
                    "envelope": {
                        "id": "event-1",
                        "source": "tests",
                        "type": "order.created",
                        "time_ms": "1",
                        "unknown": true
                    }
                }
            }))
            .is_err()
        );
    }

    #[test]
    fn nested_subscription_contract_is_strict() {
        let unknown_filter = json!({
            "idempotency_key": "subscription-1",
            "expected_term": "1",
            "operation": {
                "kind": "upsert_subscription",
                "subscription": {
                    "name": "sink",
                    "filter": {"event_types": ["order.created"]},
                    "target": {"kind": "pull"},
                    "transform": {}
                }
            }
        });
        assert!(serde_json::from_value::<BusMutationRequest>(unknown_filter).is_err());

        let unknown_target = json!({
            "idempotency_key": "subscription-1",
            "expected_term": "1",
            "operation": {
                "kind": "upsert_subscription",
                "subscription": {
                    "name": "sink",
                    "filter": {},
                    "target": {"kind": "pull", "url": "https://example.com"},
                    "transform": {}
                }
            }
        });
        assert!(serde_json::from_value::<BusMutationRequest>(unknown_target).is_err());
    }

    #[test]
    fn recovery_sorts_commits_and_normalizes_server_time() {
        let first = publish("publish-1", "event-1", 1_000, 4);
        let after_failover = publish("publish-2", "event-2", 500, 5);
        let live = BusTabletService::with_default_config(scope()).unwrap();
        live.apply(&first).unwrap();
        live.apply(&after_failover).unwrap();

        let recovered = BusTabletService::with_default_config(scope()).unwrap();
        recovered.replay(&[after_failover.clone(), first]).unwrap();

        let live_snapshot = live.snapshot().unwrap();
        let recovered_snapshot = recovered.snapshot().unwrap();
        assert_eq!(recovered_snapshot.last_profile_mutation_index, 5);
        assert_eq!(recovered_snapshot.last_applied_time_ms, 1_000);
        assert_eq!(recovered_snapshot.applied_command_count, 2);
        assert_eq!(recovered_snapshot.archived_event_count, 2);
        assert_eq!(recovered_snapshot.state_digest, live_snapshot.state_digest);
        assert_eq!(
            recovered
                .committed_receipt(&after_failover)
                .unwrap()
                .applied_at_ms,
            1_000
        );
    }

    #[test]
    fn exact_live_commit_is_applied_only_once() {
        let service = BusTabletService::with_default_config(scope()).unwrap();
        let command = publish("publish-1", "event-1", 10, 4);
        service.apply(&command).unwrap();
        service.apply(&command).unwrap();

        let snapshot = service.snapshot().unwrap();
        assert_eq!(snapshot.applied_command_count, 1);
        assert_eq!(snapshot.commit_position, 1);
        assert_eq!(snapshot.archived_event_count, 1);
    }

    #[test]
    fn native_snapshot_restores_bus_state_and_only_the_retained_retry_suffix() {
        let service = BusTabletService::with_default_config(scope()).unwrap();
        let first = publish("publish-1", "event-1", 10, 4);
        let second = publish("publish-2", "event-2", 11, 5);
        service.apply(&first).unwrap();
        service.apply(&second).unwrap();
        let expected = service.snapshot().unwrap();

        let image = service
            .capture_snapshot(LogIndex::new(5), std::slice::from_ref(&second))
            .unwrap();
        let restored = BusTabletService::with_default_config(scope()).unwrap();
        restored.install_snapshot(&image).unwrap();

        let actual = restored.snapshot().unwrap();
        assert_eq!(actual.state_digest, expected.state_digest);
        assert_eq!(actual.commit_position, 2);
        assert_eq!(actual.archived_event_count, 2);
        assert_eq!(actual.last_profile_mutation_index, 5);
        assert_eq!(actual.applied_command_count, 1);
        restored
            .apply(&publish("publish-3", "event-3", 12, 6))
            .unwrap();
        assert_eq!(restored.snapshot().unwrap().commit_position, 3);
    }

    #[test]
    fn native_snapshot_install_rejects_foreign_scope_without_partial_state() {
        let source = BusTabletService::with_default_config(scope()).unwrap();
        let proposal = publish("publish-1", "event-1", 10, 4);
        source.apply(&proposal).unwrap();
        let image = source
            .capture_snapshot(LogIndex::new(4), std::slice::from_ref(&proposal))
            .unwrap();
        let target =
            BusTabletService::with_default_config(BusTabletScope::new(8, 3, "events").unwrap())
                .unwrap();

        assert!(target.install_snapshot(&image).is_err());
        assert!(target.snapshot().is_err());
    }

    #[test]
    fn malformed_commit_fail_stops_reads_and_future_apply() {
        let service = BusTabletService::with_default_config(scope()).unwrap();
        let mut malformed = publish("publish-1", "event-1", 10, 4);
        malformed.payload = b"not an Event Bus command".to_vec();

        assert!(service.apply(&malformed).is_err());
        assert!(service.snapshot().is_err());
        assert!(
            service
                .apply(&publish("publish-2", "event-2", 11, 5))
                .is_err()
        );
    }

    #[test]
    fn committed_lookup_cannot_apply_a_commit_the_actor_missed() {
        let service = BusTabletService::with_default_config(scope()).unwrap();
        assert!(
            service
                .committed_receipt(&publish("publish-1", "event-1", 10, 4))
                .is_err()
        );
        assert!(service.snapshot().is_err());
    }

    #[test]
    fn request_identity_ignores_only_term_and_server_time() {
        let request: BusMutationRequest = serde_json::from_value(json!({
            "idempotency_key": "publish-1",
            "expected_term": "9",
            "operation": {
                "kind": "publish",
                "envelope": {
                    "id": "event-1",
                    "source": "tests",
                    "type": "order.created",
                    "time_ms": "1",
                    "payload": {"id": 1}
                }
            }
        }))
        .unwrap();
        let command = BusTabletCommand::new(
            &scope(),
            "publish-1",
            500,
            request.operation.to_tablet_operation(),
        )
        .unwrap();
        let pending = ProposalLookup::Pending {
            payload: command.encode(&scope()).unwrap(),
        };
        validate_existing_request(&pending, &scope(), &request).unwrap();

        let conflicting: BusMutationRequest = serde_json::from_value(json!({
            "idempotency_key": "publish-1",
            "expected_term": "9",
            "operation": {
                "kind": "publish",
                "envelope": {
                    "id": "event-1",
                    "source": "tests",
                    "type": "order.created",
                    "time_ms": "1",
                    "payload": {"id": 2}
                }
            }
        }))
        .unwrap();
        assert!(matches!(
            validate_existing_request(&pending, &scope(), &conflicting),
            Err(TabletApiError::IdempotencyConflict)
        ));
    }

    #[test]
    fn archive_response_uses_browser_safe_integer_strings() {
        let record = BusArchivedEventResponse::from(ArchivedEvent {
            position: u64::MAX,
            received_at_ms: u64::MAX - 1,
            route_plan_version: u64::MAX - 2,
            envelope: event("event-1"),
        });
        let document = serde_json::to_value(record).unwrap();
        assert_eq!(document["position"], u64::MAX.to_string());
        assert_eq!(document["received_at_ms"], (u64::MAX - 1).to_string());
        assert_eq!(document["route_plan_version"], (u64::MAX - 2).to_string());
        assert_eq!(document["envelope"]["time_ms"], "1");
    }

    #[test]
    fn delivery_response_uses_browser_safe_integer_strings_through_attempt_history() {
        let response = BusDeliveryRecordResponse::from(DeliveryRecord {
            delivery_id: "epoch.bus.delivery.v1.1.audit".into(),
            publish_position: u64::MAX,
            subscription: "audit".into(),
            target: SubscriptionTarget::Pull,
            envelope: event("event-1"),
            route_plan_version: u64::MAX - 1,
            created_at_ms: u64::MAX - 2,
            expires_at_ms: Some(u64::MAX - 1),
            policy: DeliveryPolicy {
                timeout_ms: u64::MAX,
                max_in_flight: 1,
                retry: DeliveryRetryPolicy {
                    strategy: epoch_bus::DeliveryBackoffStrategy::Fixed,
                    initial_delay_ms: u64::MAX - 3,
                    max_delay_ms: u64::MAX - 2,
                    jitter_percent: 0,
                    max_attempts: 2,
                    max_age_ms: Some(u64::MAX - 1),
                },
            },
            state: DeliveryState::Pending {
                eligible_at_ms: u64::MAX,
            },
            attempts: vec![DeliveryAttempt {
                attempt: 1,
                dispatcher: "sender".into(),
                dispatcher_epoch: u64::MAX,
                leader_term: u64::MAX - 1,
                started_at_ms: u64::MAX - 2,
                lease_deadline_ms: u64::MAX - 1,
                outcome: DeliveryAttemptOutcome::Failed {
                    failed_at_ms: u64::MAX,
                    reason: "retry".into(),
                    retry_at_ms: Some(u64::MAX),
                },
            }],
        });
        let document = serde_json::to_value(response).unwrap();
        assert_eq!(document["publish_position"], u64::MAX.to_string());
        assert_eq!(document["policy"]["timeout_ms"], u64::MAX.to_string());
        assert_eq!(document["state"]["eligible_at_ms"], u64::MAX.to_string());
        assert_eq!(
            document["attempts"][0]["dispatcher_epoch"],
            u64::MAX.to_string()
        );
        assert_eq!(
            document["attempts"][0]["outcome"]["retry_at_ms"],
            u64::MAX.to_string()
        );
    }

    #[test]
    fn status_contract_claims_the_ledger_but_not_an_external_executor() {
        assert_eq!(
            BusTabletStatus::TARGET_DISPATCH,
            "external_executor_not_implemented"
        );
        const {
            assert!(BusTabletStatus::DURABLE_TARGET_OUTBOX);
        }
    }

    #[test]
    fn applied_publish_receipt_describes_route_plan_not_dispatch() {
        let service = BusTabletService::with_default_config(scope()).unwrap();
        let command = publish("publish-1", "event-1", 10, 4);
        service.apply(&command).unwrap();
        let receipt = service.committed_receipt(&command).unwrap();
        assert!(matches!(
            receipt.outcome,
            BusTabletOutcome::Applied {
                result: BusTabletOperationResult::Published { .. }
            }
        ));
        assert_eq!(receipt.disposition, BusTabletDisposition::New);
        let replayed = receipt_for_response(receipt, true);
        assert_eq!(replayed.disposition, BusTabletDisposition::Replayed);
    }

    struct RunningBusNode {
        runtime: ConsensusProbeRuntime,
        server: JoinHandle<()>,
        base_url: Url,
    }

    struct RunningBusCluster {
        nodes: Vec<RunningBusNode>,
    }

    impl RunningBusCluster {
        async fn start(paths: &[PathBuf]) -> Self {
            let mut listeners = Vec::new();
            for _ in 0..3 {
                listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
            }
            let urls = listeners
                .iter()
                .map(|listener| {
                    Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap()
                })
                .collect::<Vec<_>>();

            let mut nodes = Vec::new();
            for (index, (listener, stable_path)) in
                listeners.into_iter().zip(paths.iter()).enumerate()
            {
                if let Some(parent) = stable_path.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                let node_id = u64::try_from(index).unwrap() + 1;
                let config = ConsensusProbeConfig::new(
                    node_id,
                    7,
                    3,
                    urls.iter()
                        .enumerate()
                        .map(|(peer, url)| (u64::try_from(peer).unwrap() + 1, url.clone())),
                    Duration::from_millis(20),
                )
                .unwrap();
                let service = BusTabletService::with_default_config(scope()).unwrap();
                let applier: Arc<dyn CommittedProposalApplier> = service.clone();
                let runtime =
                    ConsensusProbeRuntime::start_with_profile_applier(config, stable_path, applier)
                        .await
                        .unwrap();
                let app = runtime.internal_router().merge(router(
                    service,
                    runtime.handle(),
                    Arc::new(ManualClock::new(1_000)),
                    Duration::from_secs(2),
                ));
                let server = tokio::spawn(async move {
                    axum::serve(listener, app).await.unwrap();
                });
                nodes.push(RunningBusNode {
                    runtime,
                    server,
                    base_url: urls[index].clone(),
                });
            }
            Self { nodes }
        }

        async fn leader(&self) -> (usize, u64) {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    for (index, node) in self.nodes.iter().enumerate() {
                        if let Ok(status) = node.runtime.handle().status().await
                            && status.role == ConsensusRole::Leader
                        {
                            return (index, status.term.get());
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("fixed-voter Event Bus cluster should elect a leader")
        }

        async fn shutdown(self) {
            for node in &self.nodes {
                node.server.abort();
            }
            for node in self.nodes {
                let _ = node.server.await;
                node.runtime.shutdown().await.unwrap();
            }
        }
    }

    fn tablet_paths(root: &Path) -> Vec<PathBuf> {
        (1..=3)
            .map(|node_id| root.join(format!("node-{node_id}.wal")))
            .collect()
    }

    fn subscription_body(expected_term: u64) -> Value {
        json!({
            "idempotency_key": "subscription-1",
            "expected_term": expected_term.to_string(),
            "operation": {
                "kind": "upsert_subscription",
                "subscription": {
                    "name": "orders",
                    "filter": {
                        "event_type_patterns": ["order.*"]
                    },
                    "target": {
                        "kind": "pull"
                    },
                    "transform": {
                        "add_headers": {
                            "x-epoch-route": "orders"
                        }
                    }
                }
            }
        })
    }

    fn publish_body(expected_term: u64, payload_id: u64) -> Value {
        json!({
            "idempotency_key": "publish-1",
            "expected_term": expected_term.to_string(),
            "operation": {
                "kind": "publish",
                "envelope": {
                    "id": "event-1",
                    "source": "tests",
                    "type": "order.created",
                    "time_ms": "9007199254740993",
                    "deliver_at_ms": "9007199254740994",
                    "ttl_ms": "9007199254740995",
                    "payload": {
                        "id": payload_id
                    }
                }
            }
        })
    }

    async fn post_json(
        client: &reqwest::Client,
        node: &RunningBusNode,
        path: &str,
        body: &Value,
    ) -> (StatusCode, Value) {
        let response = client
            .post(node.base_url.join(path).unwrap())
            .json(body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let document = response.json().await.unwrap();
        (status, document)
    }

    async fn get_status(client: &reqwest::Client, node: &RunningBusNode) -> Value {
        client
            .get(
                node.base_url
                    .join(EXPERIMENTAL_BUS_TABLET_STATUS_PATH)
                    .unwrap(),
            )
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn wait_for_cluster_state(
        cluster: &RunningBusCluster,
        client: &reqwest::Client,
        mutation_index: u64,
    ) -> Vec<Value> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut statuses = Vec::new();
                let mut complete = true;
                for node in &cluster.nodes {
                    let status = get_status(client, node).await;
                    let profile_index = status["last_profile_mutation_index"]
                        .as_str()
                        .and_then(|value| value.parse::<u64>().ok());
                    let consensus_index = status["consensus_applied_index"]
                        .as_str()
                        .and_then(|value| value.parse::<u64>().ok());
                    complete &= profile_index == Some(mutation_index)
                        && consensus_index.is_some_and(|index| index >= mutation_index);
                    statuses.push(status);
                }
                if complete {
                    return statuses;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("all Event Bus voters should apply the committed route plan")
    }

    async fn assert_replay_on_every_node(cluster: &RunningBusCluster, client: &reqwest::Client) {
        for node in &cluster.nodes {
            let (status, replay) = post_json(
                client,
                node,
                EXPERIMENTAL_BUS_TABLET_ARCHIVE_REPLAY_PATH,
                &json!({
                    "from_ms": "0",
                    "to_ms": u64::MAX.to_string(),
                    "filter": {
                        "event_type_patterns": ["order.*"]
                    },
                    "limit": 10
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(replay["observation_scope"], "local");
            assert_eq!(
                replay["read_consistency"],
                "local_profile_applied_stale_capable"
            );
            assert_eq!(replay["records"].as_array().unwrap().len(), 1);
            assert_eq!(replay["records"][0]["position"], "1");
            assert_eq!(replay["records"][0]["received_at_ms"], "1000");
            assert_eq!(replay["records"][0]["route_plan_version"], "2");
            assert_eq!(replay["records"][0]["envelope"]["id"], "event-1");
            assert_eq!(
                replay["records"][0]["envelope"]["time_ms"],
                "9007199254740993"
            );
        }
    }

    async fn assert_acknowledged_delivery_on_every_node(
        cluster: &RunningBusCluster,
        client: &reqwest::Client,
    ) {
        for node in &cluster.nodes {
            let (status, deliveries) = post_json(
                client,
                node,
                EXPERIMENTAL_BUS_TABLET_DELIVERY_QUERY_PATH,
                &json!({
                    "subscription": "orders",
                    "state": "acknowledged",
                    "limit": 10
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(deliveries["observation_scope"], "local");
            assert_eq!(deliveries["records"].as_array().unwrap().len(), 1);
            assert_eq!(
                deliveries["records"][0]["delivery_id"],
                "epoch.bus.delivery.v1.1.orders"
            );
            assert_eq!(deliveries["records"][0]["publish_position"], "1");
            assert_eq!(deliveries["records"][0]["state"]["kind"], "acknowledged");
            assert_eq!(deliveries["records"][0]["attempts"][0]["attempt"], 1);
            assert_eq!(
                deliveries["records"][0]["attempts"][0]["outcome"]["kind"],
                "acknowledged"
            );
        }
    }

    async fn assert_invalid_replay_is_a_client_error(
        client: &reqwest::Client,
        leader: &RunningBusNode,
    ) {
        let (status, invalid_replay) = post_json(
            client,
            leader,
            EXPERIMENTAL_BUS_TABLET_ARCHIVE_REPLAY_PATH,
            &json!({
                "from_ms": "2",
                "to_ms": "1",
                "limit": 10
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(invalid_replay["error"]["code"], "invalid_request");
        assert_eq!(
            invalid_replay["error"]["outcome_certainty"],
            "definite_not_committed"
        );
    }

    async fn commit_route_publish_retry_and_conflict(
        client: &reqwest::Client,
        leader: &RunningBusNode,
        term: u64,
    ) -> u64 {
        let (status, subscription) = post_json(
            client,
            leader,
            EXPERIMENTAL_BUS_TABLET_MUTATIONS_PATH,
            &subscription_body(term),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(subscription["state"], "committed");
        assert_eq!(
            subscription["receipt"]["outcome"]["result"]["kind"],
            "subscription_upserted"
        );
        assert_eq!(
            subscription["receipt"]["outcome"]["result"]["route_plan_version"],
            "2"
        );

        let (status, published) = post_json(
            client,
            leader,
            EXPERIMENTAL_BUS_TABLET_MUTATIONS_PATH,
            &publish_body(term, 1),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            published["receipt"]["outcome"]["result"]["kind"],
            "published"
        );
        assert_eq!(
            published["receipt"]["outcome"]["result"]["delivery_count"],
            1
        );
        assert_eq!(published["receipt"]["outcome"]["result"]["position"], "1");
        let publish_commit_index = published["receipt"]["commit_index"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();

        let (status, replayed) = post_json(
            client,
            leader,
            EXPERIMENTAL_BUS_TABLET_MUTATIONS_PATH,
            &publish_body(term.saturating_add(100), 1),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed["receipt"]["disposition"], "replayed");
        assert_eq!(
            replayed["receipt"]["outcome"]["result"]["delivery_plan_digest"],
            published["receipt"]["outcome"]["result"]["delivery_plan_digest"]
        );

        let (status, conflict) = post_json(
            client,
            leader,
            EXPERIMENTAL_BUS_TABLET_MUTATIONS_PATH,
            &publish_body(term, 2),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(conflict["error"]["code"], "idempotency_conflict");
        assert_eq!(conflict["error"]["outcome_certainty"], "unknown");
        let delivery_commit_index = commit_delivery_ack(client, leader, term).await;
        assert!(delivery_commit_index > publish_commit_index);
        delivery_commit_index
    }

    async fn commit_delivery_ack(
        client: &reqwest::Client,
        leader: &RunningBusNode,
        term: u64,
    ) -> u64 {
        let (status, acquired) = post_json(
            client,
            leader,
            EXPERIMENTAL_BUS_TABLET_MUTATIONS_PATH,
            &json!({
                "idempotency_key": "acquire-1",
                "expected_term": term.to_string(),
                "operation": {
                    "kind": "acquire_deliveries",
                    "subscription": "orders",
                    "dispatcher": "sender",
                    "dispatcher_epoch": "1",
                    "max_deliveries": 1
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let delivery = &acquired["receipt"]["outcome"]["result"]["deliveries"][0];
        assert_eq!(
            acquired["receipt"]["outcome"]["result"]["kind"],
            "deliveries_acquired"
        );
        assert_eq!(delivery["delivery_id"], "epoch.bus.delivery.v1.1.orders");
        assert_eq!(delivery["publish_position"], "1");
        assert_eq!(delivery["attempt"], 1);
        assert_eq!(delivery["lease_deadline_ms"], "31000");

        let (status, acknowledged) = post_json(
            client,
            leader,
            EXPERIMENTAL_BUS_TABLET_MUTATIONS_PATH,
            &json!({
                "idempotency_key": "ack-1",
                "expected_term": term.to_string(),
                "operation": {
                    "kind": "acknowledge_delivery",
                    "delivery_id": delivery["delivery_id"],
                    "dispatcher": "sender",
                    "dispatcher_epoch": "1",
                    "lease_token": delivery["lease_token"]
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            acknowledged["receipt"]["outcome"]["result"]["kind"],
            "delivery_acknowledged"
        );
        assert_eq!(
            acknowledged["receipt"]["outcome"]["result"]["delivery_id"],
            "epoch.bus.delivery.v1.1.orders"
        );
        acknowledged["receipt"]["commit_index"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    }

    async fn assert_converged_status(
        cluster: &RunningBusCluster,
        client: &reqwest::Client,
        publish_commit_index: u64,
    ) -> Value {
        let statuses = wait_for_cluster_state(cluster, client, publish_commit_index).await;
        let state_digest = statuses[0]["state_digest"].clone();
        for status in statuses {
            assert_eq!(status["route_plan_version"], "2");
            assert_eq!(status["subscription_count"], "1");
            assert_eq!(status["commit_position"], "1");
            assert_eq!(status["archived_event_count"], "1");
            assert_eq!(
                status["target_dispatch"],
                "external_executor_not_implemented"
            );
            assert_eq!(status["durable_target_outbox"], true);
            assert_eq!(status["pending_delivery_count"], "0");
            assert_eq!(status["in_flight_delivery_count"], "0");
            assert_eq!(status["acknowledged_delivery_count"], "1");
            assert_eq!(status["dead_lettered_delivery_count"], "0");
            assert_eq!(status["state_digest"], state_digest);
        }
        state_digest
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn event_bus_tablet_commits_converges_and_recovers_on_three_real_runtimes() {
        let temporary = TempDir::new().unwrap();
        let paths = tablet_paths(temporary.path());
        let cluster = RunningBusCluster::start(&paths).await;
        let client = reqwest::Client::new();
        let (leader_index, term) = cluster.leader().await;
        let leader = &cluster.nodes[leader_index];

        assert_invalid_replay_is_a_client_error(&client, leader).await;
        let publish_commit_index =
            commit_route_publish_retry_and_conflict(&client, leader, term).await;
        let state_digest = assert_converged_status(&cluster, &client, publish_commit_index).await;
        assert_replay_on_every_node(&cluster, &client).await;
        assert_acknowledged_delivery_on_every_node(&cluster, &client).await;
        cluster.nodes[leader_index]
            .runtime
            .handle()
            .checkpoint()
            .await
            .expect("Event Bus profile checkpoint should persist before restart");
        cluster.shutdown().await;

        let reopened = RunningBusCluster::start(&paths).await;
        let recovered_statuses =
            wait_for_cluster_state(&reopened, &client, publish_commit_index).await;
        for status in recovered_statuses {
            assert_eq!(status["state_digest"], state_digest);
            assert_eq!(status["archived_event_count"], "1");
            assert_eq!(status["acknowledged_delivery_count"], "1");
        }
        assert_replay_on_every_node(&reopened, &client).await;
        assert_acknowledged_delivery_on_every_node(&reopened, &client).await;
        reopened.shutdown().await;
    }
}
