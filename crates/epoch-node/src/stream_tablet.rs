//! Experimental typed Stream tablet over the fixed-voter consensus runtime.

use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State, rejection::JsonRejection},
    http::StatusCode,
    routing::get,
};
use epoch_consensus::{
    CommittedProposal, ConsensusError, ConsensusRole, ConsensusStatus, ProposalLookup,
};
use epoch_core::{Clock, EventEnvelope};
use epoch_stream::StreamRecord;
use epoch_tablet::{
    CommittedCommand, MAX_STREAM_TABLET_COMMAND_BYTES, StreamTablet, StreamTabletAppendDisposition,
    StreamTabletAppendReceipt, StreamTabletCommand, StreamTabletOperation, StreamTabletScope,
    TabletError, proposal_id_for,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};

use crate::consensus::{CommittedProposalApplier, ConsensusProbeError, ConsensusProbeHandle};
use crate::tablet_http::{
    StrictEventEnvelope, TabletApiError, TabletApiResult, deserialize_strict_event_envelope,
    deserialize_u64_from_number_or_decimal, hex_digest, serialize_optional_u64_as_decimal,
    serialize_u64_as_decimal,
};

pub const EXPERIMENTAL_STREAM_TABLET_STATUS_PATH: &str = "/experimental/v1/tablets/stream/status";
pub const EXPERIMENTAL_STREAM_TABLET_RECORDS_PATH: &str = "/experimental/v1/tablets/stream/records";
pub const EXPERIMENTAL_STREAM_TABLET_MUTATION_PATH: &str =
    "/experimental/v1/tablets/stream/mutations/{proposal_id}";
pub const DEFAULT_COMMIT_WAIT: Duration = Duration::from_secs(5);
const MAX_FETCH_RECORDS: usize = 1_000;
const TABLET_REQUEST_BODY_BYTES: usize = MAX_STREAM_TABLET_COMMAND_BYTES + 16 * 1024;

type StreamTabletApiError = TabletApiError;

#[derive(Debug)]
pub struct StreamTabletService {
    scope: StreamTabletScope,
    tablet: RwLock<StreamTablet>,
    failure: RwLock<Option<String>>,
}

impl StreamTabletService {
    pub fn new(scope: StreamTabletScope) -> Result<Arc<Self>, TabletError> {
        let tablet = StreamTablet::new(scope.clone())?;
        Ok(Arc::new(Self {
            scope,
            tablet: RwLock::new(tablet),
            failure: RwLock::new(None),
        }))
    }

    pub fn scope(&self) -> &StreamTabletScope {
        &self.scope
    }

    pub fn last_profile_mutation_index(&self) -> Result<u64, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Stream tablet read lock was poisoned".to_owned())
            .map(|tablet| tablet.last_applied_command_index())
    }

    fn ensure_healthy(&self) -> Result<(), String> {
        let failure = self
            .failure
            .read()
            .map_err(|_| "Stream tablet failure lock was poisoned".to_owned())?;
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

    fn apply_one(
        &self,
        committed: &CommittedProposal,
    ) -> Result<StreamTabletAppendReceipt, String> {
        self.ensure_healthy()?;
        let command = CommittedCommand {
            group_id: committed.receipt.group_id.get(),
            group_epoch: committed.receipt.group_epoch.get(),
            proposal_id: committed.receipt.proposal_id.get(),
            term: committed.receipt.term.get(),
            log_index: committed.receipt.log_index.get(),
            payload: &committed.payload,
        };
        let result = self
            .tablet
            .write()
            .map_err(|_| "Stream tablet write lock was poisoned".to_owned())?
            .apply(command)
            .map_err(|error| error.to_string());
        result.map_err(|error| self.fail(error))
    }

    fn committed_receipt(
        &self,
        committed: &CommittedProposal,
    ) -> Result<StreamTabletAppendReceipt, String> {
        self.ensure_healthy()?;
        let command = CommittedCommand {
            group_id: committed.receipt.group_id.get(),
            group_epoch: committed.receipt.group_epoch.get(),
            proposal_id: committed.receipt.proposal_id.get(),
            term: committed.receipt.term.get(),
            log_index: committed.receipt.log_index.get(),
            payload: &committed.payload,
        };
        let result = self
            .tablet
            .read()
            .map_err(|_| self.fail("Stream tablet read lock was poisoned"))?
            .receipt_for_committed(command);
        match result {
            Ok(Some(receipt)) => Ok(receipt),
            Ok(None) => Err(self.fail(format!(
                "consensus commit {} was not applied by the profile actor",
                committed.receipt.proposal_id
            ))),
            Err(error) => Err(self.fail(error.to_string())),
        }
    }

    fn fetch(&self, offset: u64, limit: usize) -> Result<Vec<StreamRecord>, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Stream tablet read lock was poisoned".to_owned())?
            .fetch(offset, limit)
            .map_err(|error| error.to_string())
    }

    fn snapshot(&self) -> Result<StreamTabletSnapshot, String> {
        self.ensure_healthy()?;
        let tablet = self
            .tablet
            .read()
            .map_err(|_| "Stream tablet read lock was poisoned".to_owned())?;
        Ok(StreamTabletSnapshot {
            last_profile_mutation_index: tablet.last_applied_command_index(),
            applied_command_count: tablet.applied_command_count(),
            state_digest: hex_digest(tablet.state_digest()),
        })
    }
}

impl CommittedProposalApplier for StreamTabletService {
    fn replay(&self, committed: &[CommittedProposal]) -> Result<(), String> {
        let mut history = committed.to_vec();
        history.sort_by_key(|proposal| proposal.receipt.log_index.get());
        let mut rebuilt =
            StreamTablet::new(self.scope.clone()).map_err(|error| error.to_string())?;
        for proposal in &history {
            rebuilt
                .apply(CommittedCommand {
                    group_id: proposal.receipt.group_id.get(),
                    group_epoch: proposal.receipt.group_epoch.get(),
                    proposal_id: proposal.receipt.proposal_id.get(),
                    term: proposal.receipt.term.get(),
                    log_index: proposal.receipt.log_index.get(),
                    payload: &proposal.payload,
                })
                .map_err(|error| self.fail(error.to_string()))?;
        }
        *self
            .tablet
            .write()
            .map_err(|_| self.fail("Stream tablet write lock was poisoned"))? = rebuilt;
        Ok(())
    }

    fn apply(&self, committed: &CommittedProposal) -> Result<(), String> {
        self.apply_one(committed).map(|_| ())
    }
}

#[derive(Clone)]
struct StreamTabletApiState {
    service: Arc<StreamTabletService>,
    consensus: ConsensusProbeHandle,
    clock: Arc<dyn Clock>,
    commit_wait: Duration,
    write_serial: Arc<Mutex<()>>,
}

pub fn router(
    service: Arc<StreamTabletService>,
    consensus: ConsensusProbeHandle,
    clock: Arc<dyn Clock>,
    commit_wait: Duration,
) -> Router {
    let state = StreamTabletApiState {
        service,
        consensus,
        clock,
        commit_wait,
        write_serial: Arc::new(Mutex::new(())),
    };
    Router::new()
        .route(EXPERIMENTAL_STREAM_TABLET_STATUS_PATH, get(tablet_status))
        .route(
            EXPERIMENTAL_STREAM_TABLET_RECORDS_PATH,
            get(fetch_records).post(append_record),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_MUTATION_PATH,
            get(lookup_mutation),
        )
        .layer(DefaultBodyLimit::max(TABLET_REQUEST_BODY_BYTES))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendRequest {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    #[serde(default)]
    partition: u32,
    #[serde(deserialize_with = "deserialize_strict_event_envelope")]
    envelope: EventEnvelope,
}

async fn append_record(
    State(state): State<StreamTabletApiState>,
    request: Result<Json<AppendRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    state
        .service
        .ensure_healthy()
        .map_err(StreamTabletApiError::Profile)?;
    if request.partition != 0 {
        return Err(StreamTabletApiError::InvalidRequest(
            "the first Stream tablet slice supports only partition 0".into(),
        ));
    }
    request
        .envelope
        .validate()
        .map_err(|error| StreamTabletApiError::InvalidRequest(error.to_string()))?;
    let proposal_id = proposal_id_for(state.service.scope(), &request.idempotency_key)?;
    let _write_guard = state.write_serial.lock().await;
    let commits = state.consensus.subscribe_commits();

    let initial = state.consensus.lookup(proposal_id).await?;
    let (lookup, replayed) = match initial {
        ProposalLookup::Unknown => {
            let command = StreamTabletCommand::append(
                state.service.scope(),
                request.idempotency_key.clone(),
                request.envelope.clone(),
                state.clock.wall_time_ms(),
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
    state: &StreamTabletApiState,
    mut commits: broadcast::Receiver<CommittedProposal>,
    proposal_id: u64,
    request: &AppendRequest,
    replayed: bool,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
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
                return Err(StreamTabletApiError::Consensus(
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
                let unresolved = unresolved_response(proposal_id, &lookup);
                return Ok((StatusCode::ACCEPTED, Json(unresolved)));
            }
        }
    }
}

fn unresolved_response(proposal_id: u64, lookup: &ProposalLookup) -> StreamTabletMutationResponse {
    match lookup {
        ProposalLookup::Unknown => StreamTabletMutationResponse::unknown(proposal_id),
        ProposalLookup::Pending { .. } => StreamTabletMutationResponse::pending(proposal_id),
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
    scope: &StreamTabletScope,
    request: &AppendRequest,
) -> TabletApiResult<()> {
    let payload = match lookup {
        ProposalLookup::Unknown => return Ok(()),
        ProposalLookup::Pending { payload } => payload,
        ProposalLookup::Committed(committed) => &committed.payload,
    };
    let command = StreamTabletCommand::decode(payload, scope).map_err(|error| {
        StreamTabletApiError::Profile(format!(
            "tracked consensus command is not a valid Stream tablet command: {error}"
        ))
    })?;
    let StreamTabletOperation::Append(append) = command.operation;
    if command.idempotency_key != request.idempotency_key
        || append.partition != request.partition
        || append.envelope != request.envelope
    {
        return Err(StreamTabletApiError::IdempotencyConflict);
    }
    Ok(())
}

fn committed_response(
    service: &StreamTabletService,
    lookup: &ProposalLookup,
    request: &AppendRequest,
    replayed: bool,
) -> TabletApiResult<Option<StreamTabletMutationResponse>> {
    validate_existing_request(lookup, service.scope(), request)?;
    match lookup {
        ProposalLookup::Committed(committed) => {
            let receipt = service.committed_receipt(committed)?;
            Ok(Some(StreamTabletMutationResponse::committed(
                receipt_for_response(receipt, replayed),
            )))
        }
        ProposalLookup::Unknown | ProposalLookup::Pending { .. } => Ok(None),
    }
}

fn receipt_for_response(
    mut receipt: StreamTabletAppendReceipt,
    replayed: bool,
) -> StreamTabletAppendReceipt {
    if replayed {
        receipt.disposition = StreamTabletAppendDisposition::Replayed;
    }
    receipt
}

async fn lookup_mutation(
    State(state): State<StreamTabletApiState>,
    Path(proposal_id): Path<u64>,
) -> TabletApiResult<Json<StreamTabletMutationResponse>> {
    let lookup = state.consensus.lookup(proposal_id).await?;
    let response = match lookup {
        ProposalLookup::Unknown => StreamTabletMutationResponse::unknown(proposal_id),
        ProposalLookup::Pending { .. } => StreamTabletMutationResponse::pending(proposal_id),
        ProposalLookup::Committed(committed) => {
            let receipt = state.service.committed_receipt(&committed)?;
            StreamTabletMutationResponse::committed(receipt)
        }
    };
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FetchQuery {
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_fetch_limit")]
    limit: usize,
}

const fn default_fetch_limit() -> usize {
    100
}

async fn fetch_records(
    State(state): State<StreamTabletApiState>,
    Query(query): Query<FetchQuery>,
) -> TabletApiResult<Json<StreamTabletFetchResponse>> {
    if query.limit == 0 || query.limit > MAX_FETCH_RECORDS {
        return Err(StreamTabletApiError::InvalidRequest(format!(
            "limit must be between 1 and {MAX_FETCH_RECORDS}"
        )));
    }
    Ok(Json(StreamTabletFetchResponse {
        observation_scope: "local",
        read_consistency: "local_profile_applied_stale_capable",
        records: state
            .service
            .fetch(query.offset, query.limit)?
            .into_iter()
            .map(StreamTabletRecordResponse::from)
            .collect(),
    }))
}

async fn tablet_status(
    State(state): State<StreamTabletApiState>,
) -> TabletApiResult<Json<StreamTabletStatus>> {
    // Read the profile first, then enqueue the actor status request. The
    // profile snapshot may be stale, but it can never be ahead of the later
    // consensus-applied snapshot.
    let profile = state.service.snapshot()?;
    let consensus = state.consensus.status().await?;
    Ok(Json(StreamTabletStatus::new(
        state.service.scope(),
        &consensus,
        profile,
    )?))
}

#[derive(Debug)]
struct StreamTabletSnapshot {
    last_profile_mutation_index: u64,
    applied_command_count: usize,
    state_digest: String,
}

#[derive(Debug, Serialize)]
struct StreamTabletStatus {
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
    applied_command_count: usize,
    state_digest: String,
    write_guarantee: &'static str,
    read_consistency: &'static str,
    linearizable_read_barrier: bool,
}

impl StreamTabletStatus {
    fn new(
        scope: &StreamTabletScope,
        consensus: &ConsensusStatus,
        profile: StreamTabletSnapshot,
    ) -> Result<Self, String> {
        if profile.last_profile_mutation_index > consensus.applied_index.get() {
            return Err(format!(
                "profile mutation index {} is ahead of consensus applied index {}",
                profile.last_profile_mutation_index,
                consensus.applied_index.get()
            ));
        }
        Ok(Self {
            capability: "single_partition_stream_tablet",
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
            applied_command_count: profile.applied_command_count,
            state_digest: profile.state_digest,
            write_guarantee: "fixed_three_voter_majority_persisted_then_local_profile_applied",
            read_consistency: "local_profile_applied_stale_capable",
            linearizable_read_barrier: false,
        })
    }
}

#[derive(Debug, Serialize)]
struct StreamTabletFetchResponse {
    observation_scope: &'static str,
    read_consistency: &'static str,
    records: Vec<StreamTabletRecordResponse>,
}

#[derive(Debug, Serialize)]
struct StreamTabletRecordResponse {
    partition: u32,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    offset: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    appended_at_ms: u64,
    envelope: StrictEventEnvelope,
}

impl From<StreamRecord> for StreamTabletRecordResponse {
    fn from(record: StreamRecord) -> Self {
        Self {
            partition: record.partition,
            offset: record.offset,
            appended_at_ms: record.appended_at_ms,
            envelope: record.envelope.into(),
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
struct StreamTabletMutationResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    proposal_id: u64,
    state: MutationState,
    outcome_certainty: OutcomeCertainty,
    observation_scope: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<StreamTabletAppendReceipt>,
}

impl StreamTabletMutationResponse {
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

    fn committed(receipt: StreamTabletAppendReceipt) -> Self {
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
    use std::path::{Path, PathBuf};

    use axum::response::IntoResponse;
    use epoch_consensus::{
        CommitReceipt, ConsensusRole, GroupEpoch, GroupId, LogIndex, NodeId, ProposalId, Term,
    };
    use epoch_core::ManualClock;
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::{net::TcpListener, task::JoinHandle};
    use url::Url;

    use super::*;
    use crate::consensus::{ConsensusProbeConfig, ConsensusProbeRuntime};

    fn scope() -> StreamTabletScope {
        StreamTabletScope::new(7, 3, "orders").unwrap()
    }

    fn committed(key: &str, event_id: &str, index: u64) -> CommittedProposal {
        let scope = scope();
        let mut envelope = EventEnvelope::new("tests", "order.created", json!({"id": event_id}), 1);
        envelope.id = event_id.into();
        let command = StreamTabletCommand::append(&scope, key, envelope, 10 + index).unwrap();
        CommittedProposal {
            receipt: CommitReceipt {
                group_id: GroupId::new(7).unwrap(),
                group_epoch: GroupEpoch::new(3).unwrap(),
                proposal_id: ProposalId::new(command.proposal_id(&scope).unwrap()).unwrap(),
                term: Term::new(2),
                log_index: LogIndex::new(index),
            },
            payload: command.encode(&scope).unwrap(),
        }
    }

    #[test]
    fn recovery_rebuilds_before_exposing_last_profile_mutation_index() {
        let service = StreamTabletService::new(scope()).unwrap();
        service
            .replay(&[committed("one", "one", 4), committed("two", "two", 5)])
            .unwrap();
        assert_eq!(service.last_profile_mutation_index().unwrap(), 5);
        assert_eq!(service.fetch(0, 10).unwrap().len(), 2);
        assert_eq!(service.snapshot().unwrap().applied_command_count, 2);
    }

    #[test]
    fn malformed_committed_command_fail_stops_reads_and_future_apply() {
        let service = StreamTabletService::new(scope()).unwrap();
        let mut malformed = committed("one", "one", 4);
        malformed.payload = b"not a tablet command".to_vec();
        assert!(service.apply(&malformed).is_err());
        assert!(service.fetch(0, 10).is_err());
        assert!(service.apply(&committed("two", "two", 5)).is_err());
    }

    #[test]
    fn exact_live_commit_notification_applies_once() {
        let service = StreamTabletService::new(scope()).unwrap();
        let command = committed("one", "one", 4);
        service.apply(&command).unwrap();
        service.apply(&command).unwrap();
        assert_eq!(service.fetch(0, 10).unwrap().len(), 1);
        assert_eq!(service.snapshot().unwrap().applied_command_count, 1);
    }

    #[test]
    fn an_http_lookup_cannot_apply_a_commit_the_actor_missed() {
        let service = StreamTabletService::new(scope()).unwrap();

        assert!(
            service
                .committed_receipt(&committed("one", "one", 4))
                .is_err()
        );
        assert!(service.fetch(0, 10).is_err());
    }

    #[test]
    fn mutation_ids_are_decimal_strings_in_json() {
        let proposal_id = u64::MAX - 1;
        let document =
            serde_json::to_value(StreamTabletMutationResponse::pending(proposal_id)).unwrap();

        assert_eq!(document["proposal_id"], proposal_id.to_string());
    }

    #[test]
    fn tablet_status_serializes_all_u64_metadata_as_decimal_strings() {
        let consensus = ConsensusStatus {
            node_id: NodeId::new(u64::MAX).unwrap(),
            group_id: GroupId::new(7).unwrap(),
            group_epoch: GroupEpoch::new(3).unwrap(),
            role: ConsensusRole::Leader,
            leader_id: Some(NodeId::new(u64::MAX - 1).unwrap()),
            term: Term::new(u64::MAX),
            commit_index: LogIndex::new(u64::MAX),
            applied_index: LogIndex::new(u64::MAX - 1),
            voter_count: 3,
            fail_stopped: false,
        };
        let status = StreamTabletStatus::new(
            &scope(),
            &consensus,
            StreamTabletSnapshot {
                last_profile_mutation_index: u64::MAX - 2,
                applied_command_count: 1,
                state_digest: "00".repeat(32),
            },
        )
        .unwrap();
        let document = serde_json::to_value(status).unwrap();

        for field in [
            "tablet_id",
            "tablet_epoch",
            "node_id",
            "leader_id",
            "term",
            "consensus_commit_index",
            "consensus_applied_index",
            "last_profile_mutation_index",
        ] {
            assert!(document[field].is_string(), "{field} must be exact");
        }
        assert_eq!(document["node_id"], u64::MAX.to_string());
        assert_eq!(document["term"], u64::MAX.to_string());
    }

    #[test]
    fn tablet_status_rejects_a_profile_snapshot_ahead_of_consensus() {
        let consensus = ConsensusStatus {
            node_id: NodeId::new(1).unwrap(),
            group_id: GroupId::new(7).unwrap(),
            group_epoch: GroupEpoch::new(3).unwrap(),
            role: ConsensusRole::Follower,
            leader_id: Some(NodeId::new(2).unwrap()),
            term: Term::new(2),
            commit_index: LogIndex::new(9),
            applied_index: LogIndex::new(8),
            voter_count: 3,
            fail_stopped: false,
        };

        let error = StreamTabletStatus::new(
            &scope(),
            &consensus,
            StreamTabletSnapshot {
                last_profile_mutation_index: 9,
                applied_command_count: 1,
                state_digest: "00".repeat(32),
            },
        )
        .unwrap_err();

        assert!(error.contains("ahead of consensus applied index"));
    }

    #[test]
    fn timeout_response_preserves_unknown_versus_pending_local_state() {
        let unknown =
            serde_json::to_value(unresolved_response(9, &ProposalLookup::Unknown)).unwrap();
        let pending = serde_json::to_value(unresolved_response(
            9,
            &ProposalLookup::Pending {
                payload: b"pending".to_vec(),
            },
        ))
        .unwrap();

        assert_eq!(unknown["state"], "unknown");
        assert_eq!(pending["state"], "pending");
        assert_eq!(unknown["outcome_certainty"], "unknown");
        assert_eq!(pending["outcome_certainty"], "unknown");
    }

    #[test]
    fn append_request_rejects_unknown_nested_envelope_fields() {
        let document = json!({
            "idempotency_key": "request-1",
            "expected_term": 2,
            "partition": 0,
            "envelope": {
                "id": "order-1",
                "source": "tests",
                "type": "order.created",
                "time_ms": 1,
                "payload": {"id": 1},
                "paylod": {"typo": true}
            }
        });

        assert!(serde_json::from_value::<AppendRequest>(document).is_err());
    }

    #[test]
    fn append_request_accepts_browser_safe_decimal_metadata() {
        let mut document = append_body(1);
        document["expected_term"] = json!(u64::MAX.to_string());
        document["envelope"]["time_ms"] = json!(u64::MAX.to_string());
        document["envelope"]["deliver_at_ms"] = json!((u64::MAX - 1).to_string());
        document["envelope"]["ttl_ms"] = json!((u64::MAX - 2).to_string());

        let request: AppendRequest = serde_json::from_value(document).unwrap();

        assert_eq!(request.expected_term, u64::MAX);
        assert_eq!(request.envelope.time_ms, u64::MAX);
        assert_eq!(request.envelope.deliver_at_ms, Some(u64::MAX - 1));
        assert_eq!(request.envelope.ttl_ms, Some(u64::MAX - 2));
    }

    #[tokio::test]
    async fn follower_error_does_not_claim_a_global_non_commit() {
        let (status, document) = error_document(StreamTabletApiError::Consensus(
            ConsensusProbeError::Consensus(ConsensusError::NotLeader {
                leader_hint: Some(NodeId::new(2).unwrap()),
            }),
        ))
        .await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(document["error"]["code"], "not_leader");
        assert_eq!(document["error"]["outcome_certainty"], "unknown");
        assert!(document["error"]["leader_hint"].is_string());
    }

    #[tokio::test]
    async fn semantic_conflict_remains_globally_unknown() {
        let (status, document) = error_document(StreamTabletApiError::IdempotencyConflict).await;

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(document["error"]["code"], "idempotency_conflict");
        assert_eq!(document["error"]["outcome_certainty"], "unknown");
    }

    #[tokio::test]
    async fn stale_term_error_does_not_claim_a_global_non_commit() {
        let (status, document) = error_document(StreamTabletApiError::Consensus(
            ConsensusProbeError::Consensus(ConsensusError::StaleTerm {
                current: Term::new(3),
                observed: Term::new(2),
            }),
        ))
        .await;

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(document["error"]["code"], "stale_term");
        assert_eq!(document["error"]["outcome_certainty"], "unknown");
    }

    async fn error_document(error: StreamTabletApiError) -> (StatusCode, Value) {
        let response = error.into_response();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[test]
    fn request_semantics_ignore_only_the_original_server_time() {
        let scope = scope();
        let mut envelope = EventEnvelope::new("tests", "order.created", json!({"id": 1}), 1);
        envelope.id = "one".into();
        let command = StreamTabletCommand::append(&scope, "key", envelope.clone(), 10).unwrap();
        let pending = ProposalLookup::Pending {
            payload: command.encode(&scope).unwrap(),
        };
        let request = AppendRequest {
            idempotency_key: "key".into(),
            expected_term: 2,
            partition: 0,
            envelope: envelope.clone(),
        };
        validate_existing_request(&pending, &scope, &request).unwrap();

        let mut conflicting = request;
        conflicting.envelope.payload = json!({"id": 2});
        assert!(matches!(
            validate_existing_request(&pending, &scope, &conflicting),
            Err(StreamTabletApiError::IdempotencyConflict)
        ));
        let _clock = ManualClock::new(1);
    }

    #[tokio::test]
    async fn invalid_tracked_command_never_claims_the_request_was_not_committed() {
        let mut envelope = EventEnvelope::new("tests", "order.created", json!({"id": 1}), 1);
        envelope.id = "one".into();
        let request = AppendRequest {
            idempotency_key: "key".into(),
            expected_term: 2,
            partition: 0,
            envelope,
        };
        let error = validate_existing_request(
            &ProposalLookup::Pending {
                payload: b"not-a-tablet-command".to_vec(),
            },
            &scope(),
            &request,
        )
        .unwrap_err();
        let (status, document) = error_document(error).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(document["error"]["code"], "profile_unavailable");
        assert_eq!(document["error"]["outcome_certainty"], "unknown");
    }

    #[test]
    fn a_rebound_commit_cannot_satisfy_the_original_waiter() {
        let service = StreamTabletService::new(scope()).unwrap();
        let rebound = committed("key", "different", 4);
        service.apply(&rebound).unwrap();
        let lookup = ProposalLookup::Committed(rebound);

        let mut original_envelope =
            EventEnvelope::new("tests", "order.created", json!({"id": "original"}), 1);
        original_envelope.id = "original".into();
        let original = AppendRequest {
            idempotency_key: "key".into(),
            expected_term: 2,
            partition: 0,
            envelope: original_envelope,
        };

        assert!(matches!(
            committed_response(&service, &lookup, &original, false),
            Err(StreamTabletApiError::IdempotencyConflict)
        ));
    }

    struct RunningTabletNode {
        runtime: ConsensusProbeRuntime,
        server: JoinHandle<()>,
        base_url: Url,
    }

    struct RunningTabletCluster {
        nodes: Vec<RunningTabletNode>,
    }

    impl RunningTabletCluster {
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
                let service = StreamTabletService::new(scope()).unwrap();
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
                nodes.push(RunningTabletNode {
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
            .expect("fixed-voter tablet cluster should elect a leader")
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

    fn append_body(payload_id: u64) -> Value {
        json!({
            "idempotency_key": "request-1",
            "expected_term": 0,
            "partition": 0,
            "envelope": {
                "id": "order-1",
                "source": "tests",
                "type": "order.created",
                "time_ms": "1000",
                "deliver_at_ms": "1001",
                "ttl_ms": "1002",
                "payload": {"id": payload_id}
            }
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn typed_stream_tablet_commits_retries_and_rebuilds_on_three_real_runtimes() {
        let temporary = TempDir::new().unwrap();
        let paths = tablet_paths(temporary.path());
        let cluster = RunningTabletCluster::start(&paths).await;
        let client = reqwest::Client::new();

        assert_json_rejection_uses_the_typed_error_contract(&cluster, &client).await;
        assert_follower_rejects_write(&cluster, &client).await;
        append_retry_conflict_and_second_record(&cluster, &client).await;
        assert_all_records(&cluster, &client).await;
        cluster.shutdown().await;

        let reopened = RunningTabletCluster::start(&paths).await;
        assert_rebuilt_records(&reopened, &client).await;
        reopened.shutdown().await;
    }

    async fn assert_follower_rejects_write(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
    ) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let (leader, term) = cluster.leader().await;
                let follower = (leader + 1) % cluster.nodes.len();
                let mut body = append_body(1);
                body["expected_term"] = json!(term);
                let follower_response = client
                    .post(append_url_for(&cluster.nodes[follower]))
                    .json(&body)
                    .send()
                    .await
                    .unwrap();
                let status = follower_response.status();
                let document: Value = follower_response.json().await.unwrap();

                if status == StatusCode::SERVICE_UNAVAILABLE
                    && document["error"]["code"] == "not_leader"
                {
                    assert_eq!(document["error"]["outcome_certainty"], "unknown");
                    return;
                }

                // A follower that becomes leader must do so in a newer term, so
                // term fencing can reject this attempt without mutating state.
                if is_stale_term_response(status, &document) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }

                panic!("unexpected response while targeting a follower: {status} {document}");
            }
        })
        .await
        .expect("a stable follower should reject the write");
    }

    async fn assert_json_rejection_uses_the_typed_error_contract(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
    ) {
        let response = client
            .post(append_url_for(&cluster.nodes[0]))
            .json(&json!({
                "idempotency_key": "invalid-request",
                "expected_term": "1",
                "partition": 0,
                "envelope": {
                    "id": "invalid",
                    "source": "tests",
                    "type": "order.created",
                    "time_ms": "1",
                    "payload": {},
                    "paylod": "unknown field"
                }
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let document: Value = response.json().await.unwrap();
        assert_eq!(document["error"]["code"], "invalid_request");
        assert_eq!(
            document["error"]["outcome_certainty"],
            "definite_not_committed"
        );
    }

    async fn append_retry_conflict_and_second_record(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
    ) {
        let body = append_body(1);
        let (status, committed) = post_to_current_leader(cluster, client, &body).await;
        assert!(matches!(status, StatusCode::CREATED | StatusCode::OK));
        assert_eq!(committed["state"], "committed");
        assert_eq!(committed["receipt"]["offset"], "0");
        assert_eq!(committed["receipt"]["durable_voter_acks"], 2);
        assert_eq!(
            committed["receipt"]["write_evidence"],
            "fixed_voter_majority_persisted"
        );
        assert!(committed["proposal_id"].is_string());
        assert_eq!(
            committed["proposal_id"],
            committed["receipt"]["proposal_id"]
        );
        assert_eq!(
            committed["receipt"]["disposition"],
            if status == StatusCode::CREATED {
                "new"
            } else {
                "replayed"
            }
        );

        let (retry_status, replayed) = post_to_current_leader(cluster, client, &body).await;
        assert_eq!(retry_status, StatusCode::OK);
        assert_eq!(replayed["receipt"]["offset"], "0");
        assert_eq!(replayed["receipt"]["disposition"], "replayed");

        let mut conflicting = body.clone();
        conflicting["envelope"]["payload"] = json!({"id": 2});
        let (conflict_status, conflict) =
            post_to_current_leader(cluster, client, &conflicting).await;
        assert_eq!(conflict_status, StatusCode::CONFLICT);
        assert_eq!(conflict["error"]["code"], "idempotency_conflict");

        let mut second = append_body(2);
        second["idempotency_key"] = json!("request-2");
        second["envelope"]["id"] = json!("order-2");
        let (second_status, second_committed) =
            post_to_current_leader(cluster, client, &second).await;
        assert!(matches!(
            second_status,
            StatusCode::CREATED | StatusCode::OK
        ));
        assert_eq!(second_committed["state"], "committed");
        assert_eq!(second_committed["receipt"]["offset"], "1");
        assert_eq!(
            second_committed["receipt"]["disposition"],
            if second_status == StatusCode::CREATED {
                "new"
            } else {
                "replayed"
            }
        );
    }

    async fn post_to_current_leader(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
        request: &Value,
    ) -> (StatusCode, Value) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let (leader, term) = cluster.leader().await;
                let mut attempt = request.clone();
                attempt["expected_term"] = json!(term);
                let response = client
                    .post(append_url_for(&cluster.nodes[leader]))
                    .json(&attempt)
                    .send()
                    .await
                    .unwrap();
                let status = response.status();
                let document: Value = response.json().await.unwrap();

                if is_retryable_leadership_response(status, &document) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                return (status, document);
            }
        })
        .await
        .expect("an exact idempotent request should resolve under stable leadership")
    }

    fn is_retryable_leadership_response(status: StatusCode, document: &Value) -> bool {
        (status == StatusCode::SERVICE_UNAVAILABLE
            && document["error"]["code"] == "not_leader"
            && document["error"]["outcome_certainty"] == "unknown")
            || is_stale_term_response(status, document)
            || (status == StatusCode::ACCEPTED
                && document["outcome_certainty"] == "unknown"
                && matches!(document["state"].as_str(), Some("unknown" | "pending")))
    }

    fn is_stale_term_response(status: StatusCode, document: &Value) -> bool {
        status == StatusCode::CONFLICT
            && document["error"]["code"] == "stale_term"
            && document["error"]["outcome_certainty"] == "unknown"
    }

    async fn assert_all_records(cluster: &RunningTabletCluster, client: &reqwest::Client) {
        for node in &cluster.nodes {
            let fetch_url = append_url_for(node);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let response = client.get(fetch_url.clone()).send().await.unwrap();
                    let document: Value = response.json().await.unwrap();
                    if document["records"]
                        .as_array()
                        .is_some_and(|records| records.len() == 2)
                    {
                        assert_eq!(document["records"][0]["offset"], "0");
                        assert_eq!(document["records"][1]["offset"], "1");
                        assert_eq!(document["records"][0]["envelope"]["time_ms"], "1000");
                        assert_eq!(document["records"][0]["envelope"]["deliver_at_ms"], "1001");
                        assert_eq!(document["records"][0]["envelope"]["ttl_ms"], "1002");
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("every voter should profile-apply the committed record");
        }
    }

    async fn assert_rebuilt_records(cluster: &RunningTabletCluster, client: &reqwest::Client) {
        for node in &cluster.nodes {
            let status_url = node
                .base_url
                .join(EXPERIMENTAL_STREAM_TABLET_STATUS_PATH.trim_start_matches('/'))
                .unwrap();
            let status: Value = client
                .get(status_url)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(status["applied_command_count"], 2);
            assert!(
                status["last_profile_mutation_index"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
                    > 0
            );

            let fetch_url = append_url_for(node);
            let records: Value = client
                .get(fetch_url)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(records["records"].as_array().unwrap().len(), 2);
            assert_eq!(records["records"][0]["envelope"]["id"], "order-1");
            assert_eq!(records["records"][1]["envelope"]["id"], "order-2");
        }
    }

    fn append_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join(EXPERIMENTAL_STREAM_TABLET_RECORDS_PATH.trim_start_matches('/'))
            .unwrap()
    }
}
