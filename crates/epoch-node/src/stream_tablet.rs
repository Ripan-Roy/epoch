//! Experimental typed Stream tablet over the fixed-voter consensus runtime.

use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State, rejection::JsonRejection},
    http::StatusCode,
    routing::get,
};
use epoch_consensus::{
    ApplicationSnapshot, CommittedProposal, ConsensusError, ConsensusRole, ConsensusStatus,
    LogIndex, ProposalLookup,
};
use epoch_core::{Clock, EventEnvelope};
use epoch_stream::{StreamRecord, StreamRetentionPolicy};
use epoch_tablet::{
    CommittedCommand, MAX_STREAM_BATCH_COMPRESSED_BYTES, MAX_STREAM_BATCH_RECORDS,
    MAX_STREAM_BATCH_UNCOMPRESSED_BYTES, MAX_STREAM_CONSUMER_GROUP_BYTES,
    MAX_STREAM_CONSUMER_GROUPS, MAX_STREAM_CONSUMER_MEMBER_BYTES,
    MAX_STREAM_CONSUMER_MEMBERS_PER_GROUP, MAX_STREAM_RETENTION_AGE_MS,
    MAX_STREAM_RETENTION_BYTES_PER_PARTITION, MAX_STREAM_RETENTION_RECORDS_PER_PARTITION,
    MAX_STREAM_SESSION_TIMEOUT_MS, MAX_STREAM_TABLET_COMMAND_BYTES, MIN_STREAM_SESSION_TIMEOUT_MS,
    StreamBatchPayload, StreamCompression, StreamGroupOffsetMode, StreamGroupSessionAction,
    StreamTablet, StreamTabletCommand, StreamTabletGroupObservation, StreamTabletMutationReceipt,
    StreamTabletOperation, StreamTabletRetentionMode, StreamTabletRetentionObservation,
    StreamTabletScope, StreamTabletSessionObservation, TabletError, decode_stream_batch_payload,
    proposal_id_for, validate_retention_policy, validate_stream_consumer_group,
    validate_stream_consumer_member,
};
#[cfg(test)]
use epoch_tablet::{StreamBatchRecord, encode_stream_batch_payload};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, broadcast};

use crate::consensus::{CommittedProposalApplier, ConsensusProbeError, ConsensusProbeHandle};
use crate::regional_maintenance::{RegionalMaintenanceOperation, RegionalMaintenanceProposal};
use crate::tablet_http::{
    StrictEventEnvelope, TabletApiError, TabletApiResult, TabletReadMetadata,
    deserialize_optional_u64_from_number_or_decimal, deserialize_strict_event_envelope,
    deserialize_u64_from_number_or_decimal, hex_digest, serialize_optional_u64_as_decimal,
    serialize_u64_as_decimal, tablet_read_metadata,
};

pub const EXPERIMENTAL_STREAM_TABLET_STATUS_PATH: &str = "/experimental/v1/tablets/stream/status";
pub const EXPERIMENTAL_STREAM_TABLET_RECORDS_PATH: &str = "/experimental/v1/tablets/stream/records";
pub const EXPERIMENTAL_STREAM_TABLET_BATCHES_PATH: &str =
    "/experimental/v1/tablets/stream/records/batches";
pub const EXPERIMENTAL_STREAM_TABLET_MUTATION_PATH: &str =
    "/experimental/v1/tablets/stream/mutations/{proposal_id}";
pub const EXPERIMENTAL_STREAM_TABLET_GROUP_OFFSETS_PATH: &str =
    "/experimental/v1/tablets/stream/groups/{group}/offsets";
pub const EXPERIMENTAL_STREAM_TABLET_GROUP_LAG_PATH: &str =
    "/experimental/v1/tablets/stream/groups/{group}/lag";
pub const EXPERIMENTAL_STREAM_TABLET_GROUP_RECORDS_PATH: &str =
    "/experimental/v1/tablets/stream/groups/{group}/records";
pub const EXPERIMENTAL_STREAM_TABLET_GROUP_CLAIM_PATH: &str =
    "/experimental/v1/tablets/stream/groups/{group}/claim";
pub const EXPERIMENTAL_STREAM_TABLET_GROUP_CLAIMED_RECORDS_PATH: &str =
    "/experimental/v1/tablets/stream/groups/{group}/claimed-records";
pub const EXPERIMENTAL_STREAM_TABLET_RETENTION_PATH: &str =
    "/experimental/v1/tablets/stream/retention";
pub const EXPERIMENTAL_STREAM_TABLET_RETENTION_MAINTENANCE_PATH: &str =
    "/experimental/v1/tablets/stream/retention/maintenance";
pub const EXPERIMENTAL_STREAM_TABLET_GROUP_SESSIONS_PATH: &str =
    "/experimental/v1/tablets/stream/groups/{group}/sessions";
pub const EXPERIMENTAL_STREAM_TABLET_GROUP_SESSION_HEARTBEAT_PATH: &str =
    "/experimental/v1/tablets/stream/groups/{group}/sessions/{member}/heartbeat";
pub const EXPERIMENTAL_STREAM_TABLET_GROUP_SESSION_MEMBER_PATH: &str =
    "/experimental/v1/tablets/stream/groups/{group}/sessions/{member}";
pub const EXPERIMENTAL_STREAM_TABLET_GROUP_SESSION_MAINTENANCE_PATH: &str =
    "/experimental/v1/tablets/stream/groups/{group}/sessions/maintenance";
pub const DEFAULT_COMMIT_WAIT: Duration = Duration::from_secs(5);
const MAX_FETCH_RECORDS: usize = 1_000;
const TABLET_REQUEST_BODY_BYTES: usize = MAX_STREAM_TABLET_COMMAND_BYTES + 16 * 1024;
const STREAM_APPLICATION_SNAPSHOT_FORMAT_ID: [u8; 16] = *b"STREAM__STATE_V1";
const STREAM_APPLICATION_SNAPSHOT_VERSION: u16 = 1;

type StreamTabletApiError = TabletApiError;

#[derive(Debug)]
pub struct StreamTabletService {
    scope: StreamTabletScope,
    shard_index: u32,
    shard_count: u32,
    tablet: RwLock<StreamTablet>,
    failure: RwLock<Option<String>>,
}

impl StreamTabletService {
    pub fn new(scope: StreamTabletScope) -> Result<Arc<Self>, TabletError> {
        Self::new_for_shard(scope, 0, 1)
    }

    pub fn new_for_shard(
        scope: StreamTabletScope,
        shard_index: u32,
        shard_count: u32,
    ) -> Result<Arc<Self>, TabletError> {
        if shard_count == 0 || shard_index >= shard_count {
            return Err(TabletError::InvalidCommand(format!(
                "Stream logical shard {shard_index} is outside resource shard_count {shard_count}"
            )));
        }
        let tablet = StreamTablet::new(scope.clone())?;
        Ok(Arc::new(Self {
            scope,
            shard_index,
            shard_count,
            tablet: RwLock::new(tablet),
            failure: RwLock::new(None),
        }))
    }

    pub fn scope(&self) -> &StreamTabletScope {
        &self.scope
    }

    pub const fn shard_index(&self) -> u32 {
        self.shard_index
    }

    pub const fn shard_count(&self) -> u32 {
        self.shard_count
    }

    pub fn last_profile_mutation_index(&self) -> Result<u64, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Stream tablet read lock was poisoned".to_owned())
            .map(|tablet| tablet.last_applied_command_index())
    }

    pub fn maintenance_proposals(
        &self,
        now_ms: u64,
    ) -> Result<Vec<RegionalMaintenanceProposal>, String> {
        self.ensure_healthy()?;
        let tablet = self
            .tablet
            .read()
            .map_err(|_| "Stream tablet read lock was poisoned".to_owned())?;
        let mut proposals = Vec::new();
        if let Some(due_at_ms) = tablet
            .next_retention_maintenance_deadline_ms()
            .filter(|deadline_ms| *deadline_ms <= now_ms)
        {
            let key = maintenance_key(
                RegionalMaintenanceOperation::StreamRetention,
                due_at_ms,
                None,
                None,
            );
            let command = StreamTabletCommand::maintain_retention(&self.scope, key, due_at_ms)
                .map_err(|error| error.to_string())?;
            proposals.push(RegionalMaintenanceProposal {
                operation: RegionalMaintenanceOperation::StreamRetention,
                due_at_ms,
                proposal_id: command
                    .proposal_id(&self.scope)
                    .map_err(|error| error.to_string())?,
                payload: command
                    .encode(&self.scope)
                    .map_err(|error| error.to_string())?,
            });
        }
        if self.shard_index == 0 {
            for (due_at_ms, group, shard_count) in tablet.due_session_maintenance(now_ms) {
                let key = maintenance_key(
                    RegionalMaintenanceOperation::StreamConsumerSession,
                    due_at_ms,
                    None,
                    Some(&group),
                );
                let command = StreamTabletCommand::maintain_group_sessions(
                    &self.scope,
                    key,
                    group,
                    shard_count,
                    due_at_ms,
                )
                .map_err(|error| error.to_string())?;
                proposals.push(RegionalMaintenanceProposal {
                    operation: RegionalMaintenanceOperation::StreamConsumerSession,
                    due_at_ms,
                    proposal_id: command
                        .proposal_id(&self.scope)
                        .map_err(|error| error.to_string())?,
                    payload: command
                        .encode(&self.scope)
                        .map_err(|error| error.to_string())?,
                });
            }
        }
        proposals.sort_by(|left, right| {
            (left.due_at_ms, left.operation, left.proposal_id).cmp(&(
                right.due_at_ms,
                right.operation,
                right.proposal_id,
            ))
        });
        Ok(proposals)
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
    ) -> Result<StreamTabletMutationReceipt, String> {
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
            .apply_mutation(command)
            .map_err(|error| error.to_string());
        result.map_err(|error| self.fail(error))
    }

    pub(crate) fn committed_receipt(
        &self,
        committed: &CommittedProposal,
    ) -> Result<StreamTabletMutationReceipt, String> {
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
            .mutation_receipt_for_committed(command);
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

    fn fetch_for_group(&self, group: &str, limit: usize) -> Result<Vec<StreamRecord>, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Stream tablet read lock was poisoned".to_owned())?
            .fetch_for_group(group, limit)
            .map_err(|error| error.to_string())
    }

    fn fetch_for_claimed_group(
        &self,
        group: &str,
        member_id: &str,
        group_generation: u64,
        limit: usize,
    ) -> Result<(StreamTabletGroupObservation, Vec<StreamRecord>), ClaimedGroupFetchError> {
        self.ensure_healthy()
            .map_err(ClaimedGroupFetchError::Unavailable)?;
        let tablet = self.tablet.read().map_err(|_| {
            ClaimedGroupFetchError::Unavailable("Stream tablet read lock was poisoned".to_owned())
        })?;
        let records = tablet
            .fetch_for_claimed_group(group, member_id, group_generation, limit)
            .map_err(|error| ClaimedGroupFetchError::Fenced(error.to_string()))?;
        let observation = tablet
            .group_observation(group)
            .map_err(|error| ClaimedGroupFetchError::Unavailable(error.to_string()))?;
        Ok((observation, records))
    }

    fn group_observation(&self, group: &str) -> Result<StreamTabletGroupObservation, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Stream tablet read lock was poisoned".to_owned())?
            .group_observation(group)
            .map_err(|error| error.to_string())
    }

    fn session_observation(&self, group: &str) -> Result<StreamTabletSessionObservation, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Stream tablet read lock was poisoned".to_owned())?
            .session_observation(group)
            .map_err(|error| error.to_string())
    }

    fn retention_observation(&self) -> Result<StreamTabletRetentionObservation, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Stream tablet read lock was poisoned".to_owned())?
            .retention_observation()
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

#[derive(Debug)]
enum ClaimedGroupFetchError {
    Fenced(String),
    Unavailable(String),
}

fn maintenance_key(
    operation: RegionalMaintenanceOperation,
    due_at_ms: u64,
    applied_index: Option<u64>,
    group: Option<&str>,
) -> String {
    let suffix = group.map_or_else(String::new, |group| {
        let digest = Sha256::digest(group.as_bytes());
        format!("-{}", hex_digest_prefix(&digest))
    });
    let sweep = applied_index.map_or_else(String::new, |index| format!("-{index}"));
    format!(
        "epoch-auto-{}-{due_at_ms}{sweep}{suffix}",
        operation.as_str()
    )
}

fn hex_digest_prefix(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

impl CommittedProposalApplier for StreamTabletService {
    fn replay(&self, committed: &[CommittedProposal]) -> Result<(), String> {
        let mut history = committed.to_vec();
        history.sort_by_key(|proposal| proposal.receipt.log_index.get());
        let mut rebuilt =
            StreamTablet::new(self.scope.clone()).map_err(|error| error.to_string())?;
        for proposal in &history {
            rebuilt
                .apply_mutation(CommittedCommand {
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

    fn capture_snapshot(
        &self,
        checkpoint_index: LogIndex,
        retained: &[CommittedProposal],
    ) -> Result<ApplicationSnapshot, String> {
        self.ensure_healthy()?;
        let tablet = self
            .tablet
            .read()
            .map_err(|_| "Stream tablet read lock was poisoned".to_owned())?;
        if tablet.last_applied_command_index() > checkpoint_index.get() {
            return Err(format!(
                "Stream applied index {} exceeds consensus checkpoint index {}",
                tablet.last_applied_command_index(),
                checkpoint_index
            ));
        }

        let mut retained_ids = BTreeSet::new();
        for committed in retained {
            let proposal_id = committed.receipt.proposal_id.get();
            if !retained_ids.insert(proposal_id) {
                return Err(format!(
                    "Stream retry proposal {proposal_id} appears more than once"
                ));
            }
            tablet
                .mutation_receipt_for_committed(CommittedCommand {
                    group_id: committed.receipt.group_id.get(),
                    group_epoch: committed.receipt.group_epoch.get(),
                    proposal_id,
                    term: committed.receipt.term.get(),
                    log_index: committed.receipt.log_index.get(),
                    payload: &committed.payload,
                })
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!("Stream retry proposal {proposal_id} has no typed applied result")
                })?;
        }
        let payload = tablet
            .encode_snapshot(&retained_ids)
            .map_err(|error| error.to_string())?;
        ApplicationSnapshot::new(
            checkpoint_index,
            STREAM_APPLICATION_SNAPSHOT_FORMAT_ID,
            STREAM_APPLICATION_SNAPSHOT_VERSION,
            tablet.state_digest(),
            payload,
        )
        .map_err(|error| error.to_string())
    }

    fn install_snapshot(&self, snapshot: &ApplicationSnapshot) -> Result<(), String> {
        self.ensure_healthy()?;
        let result: Result<StreamTablet, String> = (|| {
            if snapshot.format_id() != STREAM_APPLICATION_SNAPSHOT_FORMAT_ID
                || snapshot.format_version() != STREAM_APPLICATION_SNAPSHOT_VERSION
            {
                return Err("application snapshot is not a supported Stream image".into());
            }
            let restored = StreamTablet::decode_snapshot(&self.scope, snapshot.payload())
                .map_err(|error| error.to_string())?;
            if restored.last_applied_command_index() > snapshot.checkpoint_index().get()
                || restored.state_digest() != snapshot.state_digest()
            {
                return Err("Stream application snapshot index or state digest is invalid".into());
            }
            Ok(restored)
        })();
        match result {
            Ok(restored) => {
                *self
                    .tablet
                    .write()
                    .map_err(|_| self.fail("Stream tablet write lock was poisoned"))? = restored;
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
            EXPERIMENTAL_STREAM_TABLET_BATCHES_PATH,
            axum::routing::post(append_batch),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_MUTATION_PATH,
            get(lookup_mutation),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_GROUP_OFFSETS_PATH,
            axum::routing::put(update_group_offset),
        )
        .route(EXPERIMENTAL_STREAM_TABLET_GROUP_LAG_PATH, get(group_lag))
        .route(
            EXPERIMENTAL_STREAM_TABLET_GROUP_RECORDS_PATH,
            get(fetch_group_records),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_GROUP_CLAIM_PATH,
            axum::routing::put(claim_group),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_GROUP_CLAIMED_RECORDS_PATH,
            get(fetch_claimed_group_records),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_RETENTION_PATH,
            get(get_retention).put(configure_retention),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_RETENTION_MAINTENANCE_PATH,
            axum::routing::post(maintain_retention),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_GROUP_SESSIONS_PATH,
            get(get_group_session).post(join_group_session),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_GROUP_SESSION_HEARTBEAT_PATH,
            axum::routing::put(heartbeat_group_session),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_GROUP_SESSION_MEMBER_PATH,
            axum::routing::delete(leave_group_session),
        )
        .route(
            EXPERIMENTAL_STREAM_TABLET_GROUP_SESSION_MAINTENANCE_PATH,
            axum::routing::post(maintain_group_sessions),
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendBatchRequest {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    #[serde(default)]
    partition: u32,
    compression: StreamCompression,
    record_count: u16,
    uncompressed_bytes: u32,
    compressed_bytes: u32,
    payload_base64: String,
}

impl AppendBatchRequest {
    fn payload(&self) -> StreamBatchPayload {
        StreamBatchPayload {
            compression: self.compression,
            record_count: self.record_count,
            uncompressed_bytes: self.uncompressed_bytes,
            compressed_bytes: self.compressed_bytes,
            payload_base64: self.payload_base64.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupOffsetRequestBody {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    member_id: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    group_generation: u64,
    #[serde(default)]
    partition: u32,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    next_offset: u64,
    mode: StreamGroupOffsetMode,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupClaimRequestBody {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    member_id: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    group_generation: u64,
    #[serde(default)]
    partition: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionConfigureRequest {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    #[serde(default)]
    max_records_per_partition: Option<usize>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
    )]
    max_bytes_per_partition: Option<u64>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
    )]
    max_age_ms: Option<u64>,
}

impl RetentionConfigureRequest {
    const fn policy(&self) -> StreamRetentionPolicy {
        StreamRetentionPolicy {
            max_records_per_partition: self.max_records_per_partition,
            max_bytes_per_partition: self.max_bytes_per_partition,
            max_age_ms: self.max_age_ms,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionMaintenanceRequest {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupSessionJoinRequest {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    member_id: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    session_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupSessionGenerationRequest {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    group_generation: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupSessionMaintenanceRequest {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
}

#[derive(Debug)]
struct GroupSessionRequest {
    group: String,
    shard_count: u32,
    idempotency_key: String,
    expected_term: u64,
    action: StreamGroupSessionAction,
}

#[derive(Debug)]
struct GroupOffsetRequest {
    group: String,
    body: GroupOffsetRequestBody,
}

#[derive(Debug)]
struct GroupClaimRequest {
    group: String,
    body: GroupClaimRequestBody,
}

impl GroupOffsetRequest {
    const fn new(group: String, body: GroupOffsetRequestBody) -> Self {
        Self { group, body }
    }
}

impl GroupClaimRequest {
    const fn new(group: String, body: GroupClaimRequestBody) -> Self {
        Self { group, body }
    }
}

trait StreamMutationSemantics: Sync {
    fn idempotency_key(&self) -> &str;
    fn expected_term(&self) -> u64;
    fn validate(&self, scope: &StreamTabletScope) -> TabletApiResult<()>;
    fn command(
        &self,
        scope: &StreamTabletScope,
        applied_at_ms: u64,
    ) -> Result<StreamTabletCommand, TabletError>;
    fn matches_command(&self, command: &StreamTabletCommand) -> bool;
}

impl StreamMutationSemantics for AppendRequest {
    fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    fn expected_term(&self) -> u64 {
        self.expected_term
    }

    fn validate(&self, _scope: &StreamTabletScope) -> TabletApiResult<()> {
        validate_partition(self.partition)?;
        self.envelope
            .validate()
            .map_err(|error| StreamTabletApiError::InvalidRequest(error.to_string()))
    }

    fn command(
        &self,
        scope: &StreamTabletScope,
        applied_at_ms: u64,
    ) -> Result<StreamTabletCommand, TabletError> {
        StreamTabletCommand::append(
            scope,
            self.idempotency_key.clone(),
            self.envelope.clone(),
            applied_at_ms,
        )
    }

    fn matches_command(&self, command: &StreamTabletCommand) -> bool {
        matches!(
            &command.operation,
            StreamTabletOperation::Append(append)
                if command.idempotency_key == self.idempotency_key
                    && append.partition == self.partition
                    && append.envelope == self.envelope
        )
    }
}

impl StreamMutationSemantics for AppendBatchRequest {
    fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    fn expected_term(&self) -> u64 {
        self.expected_term
    }

    fn validate(&self, _scope: &StreamTabletScope) -> TabletApiResult<()> {
        validate_partition(self.partition)?;
        decode_stream_batch_payload(&self.payload())?;
        Ok(())
    }

    fn command(
        &self,
        scope: &StreamTabletScope,
        applied_at_ms: u64,
    ) -> Result<StreamTabletCommand, TabletError> {
        StreamTabletCommand::append_compressed_batch(
            scope,
            self.idempotency_key.clone(),
            self.payload(),
            applied_at_ms,
        )
    }

    fn matches_command(&self, command: &StreamTabletCommand) -> bool {
        matches!(
            &command.operation,
            StreamTabletOperation::AppendBatch(batch)
                if command.idempotency_key == self.idempotency_key
                    && batch.partition == self.partition
                    && batch.payload == self.payload()
        )
    }
}

impl StreamMutationSemantics for RetentionConfigureRequest {
    fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    fn expected_term(&self) -> u64 {
        self.expected_term
    }

    fn validate(&self, _scope: &StreamTabletScope) -> TabletApiResult<()> {
        validate_retention_policy(self.policy())?;
        Ok(())
    }

    fn command(
        &self,
        scope: &StreamTabletScope,
        applied_at_ms: u64,
    ) -> Result<StreamTabletCommand, TabletError> {
        StreamTabletCommand::configure_retention(
            scope,
            self.idempotency_key.clone(),
            self.policy(),
            applied_at_ms,
        )
    }

    fn matches_command(&self, command: &StreamTabletCommand) -> bool {
        matches!(
            &command.operation,
            StreamTabletOperation::Retention(retention)
                if command.idempotency_key == self.idempotency_key
                    && retention.mode == StreamTabletRetentionMode::Configure
                    && retention.policy == Some(self.policy())
        )
    }
}

impl StreamMutationSemantics for RetentionMaintenanceRequest {
    fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    fn expected_term(&self) -> u64 {
        self.expected_term
    }

    fn validate(&self, _scope: &StreamTabletScope) -> TabletApiResult<()> {
        Ok(())
    }

    fn command(
        &self,
        scope: &StreamTabletScope,
        applied_at_ms: u64,
    ) -> Result<StreamTabletCommand, TabletError> {
        StreamTabletCommand::maintain_retention(scope, self.idempotency_key.clone(), applied_at_ms)
    }

    fn matches_command(&self, command: &StreamTabletCommand) -> bool {
        matches!(
            &command.operation,
            StreamTabletOperation::Retention(retention)
                if command.idempotency_key == self.idempotency_key
                    && retention.mode == StreamTabletRetentionMode::Maintain
                    && retention.policy.is_none()
        )
    }
}

impl StreamMutationSemantics for GroupOffsetRequest {
    fn idempotency_key(&self) -> &str {
        &self.body.idempotency_key
    }

    fn expected_term(&self) -> u64 {
        self.body.expected_term
    }

    fn validate(&self, scope: &StreamTabletScope) -> TabletApiResult<()> {
        self.command(scope, 0).map(|_| ()).map_err(Into::into)
    }

    fn command(
        &self,
        scope: &StreamTabletScope,
        applied_at_ms: u64,
    ) -> Result<StreamTabletCommand, TabletError> {
        StreamTabletCommand::group_offset(
            scope,
            self.body.idempotency_key.clone(),
            self.group.clone(),
            self.body.member_id.clone(),
            self.body.group_generation,
            self.body.partition,
            self.body.next_offset,
            self.body.mode,
            applied_at_ms,
        )
    }

    fn matches_command(&self, command: &StreamTabletCommand) -> bool {
        matches!(
            &command.operation,
            StreamTabletOperation::GroupOffset(group)
                if command.idempotency_key == self.body.idempotency_key
                    && group.group == self.group
                    && group.member_id == self.body.member_id
                    && group.group_generation == self.body.group_generation
                    && group.partition == self.body.partition
                    && group.next_offset == self.body.next_offset
                    && group.mode == self.body.mode
        )
    }
}

impl StreamMutationSemantics for GroupClaimRequest {
    fn idempotency_key(&self) -> &str {
        &self.body.idempotency_key
    }

    fn expected_term(&self) -> u64 {
        self.body.expected_term
    }

    fn validate(&self, scope: &StreamTabletScope) -> TabletApiResult<()> {
        self.command(scope, 0).map(|_| ()).map_err(Into::into)
    }

    fn command(
        &self,
        scope: &StreamTabletScope,
        applied_at_ms: u64,
    ) -> Result<StreamTabletCommand, TabletError> {
        StreamTabletCommand::claim_group(
            scope,
            self.body.idempotency_key.clone(),
            self.group.clone(),
            self.body.member_id.clone(),
            self.body.group_generation,
            self.body.partition,
            applied_at_ms,
        )
    }

    fn matches_command(&self, command: &StreamTabletCommand) -> bool {
        matches!(
            &command.operation,
            StreamTabletOperation::GroupOffset(group)
                if command.idempotency_key == self.body.idempotency_key
                    && group.group == self.group
                    && group.member_id == self.body.member_id
                    && group.group_generation == self.body.group_generation
                    && group.partition == self.body.partition
                    && group.mode == StreamGroupOffsetMode::Claim
        )
    }
}

impl StreamMutationSemantics for GroupSessionRequest {
    fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    fn expected_term(&self) -> u64 {
        self.expected_term
    }

    fn validate(&self, scope: &StreamTabletScope) -> TabletApiResult<()> {
        self.command(scope, 0).map(|_| ()).map_err(Into::into)
    }

    fn command(
        &self,
        scope: &StreamTabletScope,
        applied_at_ms: u64,
    ) -> Result<StreamTabletCommand, TabletError> {
        match &self.action {
            StreamGroupSessionAction::Join {
                member_id,
                session_timeout_ms,
            } => StreamTabletCommand::join_group_session(
                scope,
                self.idempotency_key.clone(),
                self.group.clone(),
                member_id.clone(),
                self.shard_count,
                *session_timeout_ms,
                applied_at_ms,
            ),
            StreamGroupSessionAction::Heartbeat {
                member_id,
                group_generation,
            } => StreamTabletCommand::heartbeat_group_session(
                scope,
                self.idempotency_key.clone(),
                self.group.clone(),
                member_id.clone(),
                self.shard_count,
                *group_generation,
                applied_at_ms,
            ),
            StreamGroupSessionAction::Leave {
                member_id,
                group_generation,
            } => StreamTabletCommand::leave_group_session(
                scope,
                self.idempotency_key.clone(),
                self.group.clone(),
                member_id.clone(),
                self.shard_count,
                *group_generation,
                applied_at_ms,
            ),
            StreamGroupSessionAction::Maintain => StreamTabletCommand::maintain_group_sessions(
                scope,
                self.idempotency_key.clone(),
                self.group.clone(),
                self.shard_count,
                applied_at_ms,
            ),
        }
    }

    fn matches_command(&self, command: &StreamTabletCommand) -> bool {
        matches!(
            &command.operation,
            StreamTabletOperation::GroupSession(session)
                if command.idempotency_key == self.idempotency_key
                    && session.group == self.group
                    && session.shard_count == self.shard_count
                    && session.action == self.action
        )
    }
}

fn validate_partition(partition: u32) -> TabletApiResult<()> {
    if partition != 0 {
        return Err(StreamTabletApiError::InvalidRequest(
            "the first Stream tablet slice supports only partition 0".into(),
        ));
    }
    Ok(())
}

async fn append_record(
    State(state): State<StreamTabletApiState>,
    request: Result<Json<AppendRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    submit_mutation(state, request).await
}

async fn append_batch(
    State(state): State<StreamTabletApiState>,
    request: Result<Json<AppendBatchRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    submit_mutation(state, request).await
}

async fn update_group_offset(
    State(state): State<StreamTabletApiState>,
    Path(group): Path<String>,
    request: Result<Json<GroupOffsetRequestBody>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    submit_mutation(state, GroupOffsetRequest::new(group, request)).await
}

async fn claim_group(
    State(state): State<StreamTabletApiState>,
    Path(group): Path<String>,
    request: Result<Json<GroupClaimRequestBody>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    submit_mutation(state, GroupClaimRequest::new(group, request)).await
}

async fn configure_retention(
    State(state): State<StreamTabletApiState>,
    request: Result<Json<RetentionConfigureRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    submit_mutation(state, request).await
}

async fn maintain_retention(
    State(state): State<StreamTabletApiState>,
    request: Result<Json<RetentionMaintenanceRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    submit_mutation(state, request).await
}

async fn join_group_session(
    State(state): State<StreamTabletApiState>,
    Path(group): Path<String>,
    request: Result<Json<GroupSessionJoinRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    ensure_session_coordinator(&state.service)?;
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    let session = GroupSessionRequest {
        group,
        shard_count: state.service.shard_count(),
        idempotency_key: request.idempotency_key,
        expected_term: request.expected_term,
        action: StreamGroupSessionAction::Join {
            member_id: request.member_id,
            session_timeout_ms: request.session_timeout_ms,
        },
    };
    submit_mutation(state, session).await
}

async fn heartbeat_group_session(
    State(state): State<StreamTabletApiState>,
    Path((group, member)): Path<(String, String)>,
    request: Result<Json<GroupSessionGenerationRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    ensure_session_coordinator(&state.service)?;
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    let session = GroupSessionRequest {
        group,
        shard_count: state.service.shard_count(),
        idempotency_key: request.idempotency_key,
        expected_term: request.expected_term,
        action: StreamGroupSessionAction::Heartbeat {
            member_id: member,
            group_generation: request.group_generation,
        },
    };
    submit_mutation(state, session).await
}

async fn leave_group_session(
    State(state): State<StreamTabletApiState>,
    Path((group, member)): Path<(String, String)>,
    request: Result<Json<GroupSessionGenerationRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    ensure_session_coordinator(&state.service)?;
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    let session = GroupSessionRequest {
        group,
        shard_count: state.service.shard_count(),
        idempotency_key: request.idempotency_key,
        expected_term: request.expected_term,
        action: StreamGroupSessionAction::Leave {
            member_id: member,
            group_generation: request.group_generation,
        },
    };
    submit_mutation(state, session).await
}

async fn maintain_group_sessions(
    State(state): State<StreamTabletApiState>,
    Path(group): Path<String>,
    request: Result<Json<GroupSessionMaintenanceRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    ensure_session_coordinator(&state.service)?;
    let Json(request) = request.map_err(|rejection| StreamTabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    let session = GroupSessionRequest {
        group,
        shard_count: state.service.shard_count(),
        idempotency_key: request.idempotency_key,
        expected_term: request.expected_term,
        action: StreamGroupSessionAction::Maintain,
    };
    submit_mutation(state, session).await
}

fn ensure_session_coordinator(service: &StreamTabletService) -> TabletApiResult<()> {
    if service.shard_index() != 0 {
        return Err(StreamTabletApiError::InvalidRequest(
            "consumer sessions are coordinated by logical Stream shard 0".into(),
        ));
    }
    Ok(())
}

async fn submit_mutation<R: StreamMutationSemantics>(
    state: StreamTabletApiState,
    request: R,
) -> TabletApiResult<(StatusCode, Json<StreamTabletMutationResponse>)> {
    state
        .service
        .ensure_healthy()
        .map_err(StreamTabletApiError::Profile)?;
    request.validate(state.service.scope())?;
    let proposal_id = proposal_id_for(state.service.scope(), request.idempotency_key())?;
    let _write_guard = state.write_serial.lock().await;
    let commits = state.consensus.subscribe_commits();

    let initial = state.consensus.lookup(proposal_id).await?;
    let (lookup, replayed) = match initial {
        ProposalLookup::Unknown => {
            let command = request.command(state.service.scope(), state.clock.wall_time_ms())?;
            let payload = command.encode(state.service.scope())?;
            let (lookup, replayed) = match state
                .consensus
                .propose(proposal_id, request.expected_term(), payload)
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
    request: &impl StreamMutationSemantics,
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
                let unresolved =
                    unresolved_response(proposal_id, &lookup, state.service.shard_index());
                return Ok((StatusCode::ACCEPTED, Json(unresolved)));
            }
        }
    }
}

fn unresolved_response(
    proposal_id: u64,
    lookup: &ProposalLookup,
    shard_index: u32,
) -> StreamTabletMutationResponse {
    match lookup {
        ProposalLookup::Unknown => StreamTabletMutationResponse::unknown(proposal_id, shard_index),
        ProposalLookup::Pending { .. } => {
            StreamTabletMutationResponse::pending(proposal_id, shard_index)
        }
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
    request: &impl StreamMutationSemantics,
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
    if !request.matches_command(&command) {
        return Err(StreamTabletApiError::IdempotencyConflict);
    }
    Ok(())
}

fn committed_response(
    service: &StreamTabletService,
    lookup: &ProposalLookup,
    request: &impl StreamMutationSemantics,
    replayed: bool,
) -> TabletApiResult<Option<StreamTabletMutationResponse>> {
    validate_existing_request(lookup, service.scope(), request)?;
    match lookup {
        ProposalLookup::Committed(committed) => {
            let receipt = service.committed_receipt(committed)?;
            Ok(Some(StreamTabletMutationResponse::committed(
                receipt_for_response(receipt, replayed, service.shard_index()),
                service.shard_index(),
            )))
        }
        ProposalLookup::Unknown | ProposalLookup::Pending { .. } => Ok(None),
    }
}

fn receipt_for_response(
    mut receipt: StreamTabletMutationReceipt,
    replayed: bool,
    shard_index: u32,
) -> StreamTabletMutationReceipt {
    if replayed {
        receipt.mark_replayed();
    }
    match &mut receipt {
        StreamTabletMutationReceipt::Append(receipt) => {
            receipt.partition = shard_index;
            if let Some(batch) = &mut receipt.batch {
                for record in &mut batch.records {
                    record.partition = shard_index;
                }
            }
        }
        StreamTabletMutationReceipt::Group(receipt) => receipt.partition = shard_index,
        StreamTabletMutationReceipt::Retention(_) | StreamTabletMutationReceipt::Session(_) => {}
    }
    receipt
}

async fn lookup_mutation(
    State(state): State<StreamTabletApiState>,
    Path(proposal_id): Path<u64>,
) -> TabletApiResult<Json<StreamTabletMutationResponse>> {
    let lookup = state.consensus.lookup(proposal_id).await?;
    let shard_index = state.service.shard_index();
    let response = match lookup {
        ProposalLookup::Unknown => StreamTabletMutationResponse::unknown(proposal_id, shard_index),
        ProposalLookup::Pending { .. } => {
            StreamTabletMutationResponse::pending(proposal_id, shard_index)
        }
        ProposalLookup::Committed(committed) => {
            let receipt = state.service.committed_receipt(&committed)?;
            StreamTabletMutationResponse::committed(
                receipt_for_response(receipt, false, shard_index),
                shard_index,
            )
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupReadQuery {
    #[serde(default)]
    partition: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupFetchQuery {
    #[serde(default)]
    partition: u32,
    #[serde(default = "default_fetch_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimedGroupFetchQuery {
    #[serde(default)]
    partition: u32,
    member_id: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    group_generation: u64,
    #[serde(default = "default_fetch_limit")]
    limit: usize,
}

const fn default_fetch_limit() -> usize {
    100
}

async fn fetch_records(
    State(state): State<StreamTabletApiState>,
    Query(query): Query<FetchQuery>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<StreamTabletFetchResponse>> {
    if query.limit == 0 || query.limit > MAX_FETCH_RECORDS {
        return Err(StreamTabletApiError::InvalidRequest(format!(
            "limit must be between 1 and {MAX_FETCH_RECORDS}"
        )));
    }
    let shard_index = state.service.shard_index();
    Ok(Json(StreamTabletFetchResponse {
        read: tablet_read_metadata(read),
        shard_index,
        records: state
            .service
            .fetch(query.offset, query.limit)?
            .into_iter()
            .map(|record| StreamTabletRecordResponse::with_partition(record, shard_index))
            .collect(),
    }))
}

async fn group_lag(
    State(state): State<StreamTabletApiState>,
    Path(group): Path<String>,
    Query(query): Query<GroupReadQuery>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<StreamTabletGroupObservationResponse>> {
    validate_partition(query.partition)?;
    validate_stream_consumer_group(&group)?;
    let shard_index = state.service.shard_index();
    let mut checkpoint = state.service.group_observation(&group)?;
    checkpoint.partition = shard_index;
    Ok(Json(StreamTabletGroupObservationResponse {
        read: tablet_read_metadata(read),
        shard_index,
        checkpoint,
    }))
}

async fn get_group_session(
    State(state): State<StreamTabletApiState>,
    Path(group): Path<String>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<StreamTabletSessionObservationResponse>> {
    ensure_session_coordinator(&state.service)?;
    validate_stream_consumer_group(&group)?;
    Ok(Json(StreamTabletSessionObservationResponse {
        read: tablet_read_metadata(read),
        shard_index: state.service.shard_index(),
        session: state.service.session_observation(&group)?,
    }))
}

async fn fetch_group_records(
    State(state): State<StreamTabletApiState>,
    Path(group): Path<String>,
    Query(query): Query<GroupFetchQuery>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<StreamTabletGroupFetchResponse>> {
    validate_partition(query.partition)?;
    validate_fetch_limit(query.limit)?;
    validate_stream_consumer_group(&group)?;
    let shard_index = state.service.shard_index();
    let mut checkpoint = state.service.group_observation(&group)?;
    checkpoint.partition = shard_index;
    let records = state
        .service
        .fetch_for_group(&group, query.limit)?
        .into_iter()
        .map(|record| StreamTabletRecordResponse::with_partition(record, shard_index))
        .collect();
    Ok(Json(StreamTabletGroupFetchResponse {
        read: tablet_read_metadata(read),
        shard_index,
        checkpoint,
        records,
    }))
}

async fn fetch_claimed_group_records(
    State(state): State<StreamTabletApiState>,
    Path(group): Path<String>,
    Query(query): Query<ClaimedGroupFetchQuery>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<StreamTabletGroupFetchResponse>> {
    validate_partition(query.partition)?;
    validate_fetch_limit(query.limit)?;
    validate_stream_consumer_group(&group)?;
    validate_stream_consumer_member(&query.member_id)?;
    if query.group_generation == 0 {
        return Err(StreamTabletApiError::InvalidRequest(
            "consumer group_generation must be non-zero".into(),
        ));
    }
    let shard_index = state.service.shard_index();
    let (mut checkpoint, records) = state
        .service
        .fetch_for_claimed_group(
            &group,
            &query.member_id,
            query.group_generation,
            query.limit,
        )
        .map_err(|error| match error {
            ClaimedGroupFetchError::Fenced(message) => StreamTabletApiError::Fenced(message),
            ClaimedGroupFetchError::Unavailable(message) => StreamTabletApiError::Profile(message),
        })?;
    checkpoint.partition = shard_index;
    let records = records
        .into_iter()
        .map(|record| StreamTabletRecordResponse::with_partition(record, shard_index))
        .collect();
    Ok(Json(StreamTabletGroupFetchResponse {
        read: tablet_read_metadata(read),
        shard_index,
        checkpoint,
        records,
    }))
}

async fn get_retention(
    State(state): State<StreamTabletApiState>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<StreamTabletRetentionResponse>> {
    let shard_index = state.service.shard_index();
    let mut retention = state.service.retention_observation()?;
    retention.partition = shard_index;
    Ok(Json(StreamTabletRetentionResponse {
        read: tablet_read_metadata(read),
        shard_index,
        retention,
    }))
}

fn validate_fetch_limit(limit: usize) -> TabletApiResult<()> {
    if limit == 0 || limit > MAX_FETCH_RECORDS {
        return Err(StreamTabletApiError::InvalidRequest(format!(
            "limit must be between 1 and {MAX_FETCH_RECORDS}"
        )));
    }
    Ok(())
}

async fn tablet_status(
    State(state): State<StreamTabletApiState>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<StreamTabletStatus>> {
    // Read the profile first, then enqueue the actor status request. The
    // profile snapshot may be stale, but it can never be ahead of the later
    // consensus-applied snapshot.
    let profile = state.service.snapshot()?;
    let consensus = state.consensus.status().await?;
    Ok(Json(StreamTabletStatus::new_with_read(
        state.service.scope(),
        state.service.shard_index(),
        state.service.shard_count(),
        &consensus,
        profile,
        tablet_read_metadata(read),
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
    batch_append_atomicity: &'static str,
    supported_batch_compressions: [StreamCompression; 5],
    max_batch_records: u16,
    max_batch_compressed_bytes: usize,
    max_batch_uncompressed_bytes: usize,
    consumer_group_checkpoints: &'static str,
    consumer_group_ownership_fencing: &'static str,
    consumer_group_sessions: &'static str,
    consumer_group_assignment: &'static str,
    consumer_group_claims: &'static str,
    max_consumer_groups: usize,
    max_consumer_group_bytes: usize,
    max_consumer_member_bytes: usize,
    max_consumer_members_per_group: usize,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    min_consumer_session_timeout_ms: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    max_consumer_session_timeout_ms: u64,
    retention_contract: &'static str,
    max_retention_records_per_partition: usize,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    max_retention_bytes_per_partition: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    max_retention_age_ms: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    tablet_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    tablet_epoch: u64,
    shard_index: u32,
    shard_count: u32,
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
    #[serde(flatten)]
    read: TabletReadMetadata,
}

impl StreamTabletStatus {
    #[cfg(test)]
    fn new(
        scope: &StreamTabletScope,
        consensus: &ConsensusStatus,
        profile: StreamTabletSnapshot,
    ) -> Result<Self, String> {
        Self::new_with_read(
            scope,
            0,
            1,
            consensus,
            profile,
            TabletReadMetadata::local_stale(),
        )
    }

    fn new_with_read(
        scope: &StreamTabletScope,
        shard_index: u32,
        shard_count: u32,
        consensus: &ConsensusStatus,
        profile: StreamTabletSnapshot,
        read: TabletReadMetadata,
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
            batch_append_atomicity: "whole_batch_before_visibility",
            supported_batch_compressions: [
                StreamCompression::None,
                StreamCompression::Gzip,
                StreamCompression::Lz4,
                StreamCompression::Snappy,
                StreamCompression::Zstd,
            ],
            max_batch_records: MAX_STREAM_BATCH_RECORDS,
            max_batch_compressed_bytes: MAX_STREAM_BATCH_COMPRESSED_BYTES,
            max_batch_uncompressed_bytes: MAX_STREAM_BATCH_UNCOMPRESSED_BYTES,
            consumer_group_checkpoints: "replicated_commit_reset_lag_and_replay",
            consumer_group_ownership_fencing: "caller_supplied_monotonic_generation",
            consumer_group_sessions: "replicated_join_heartbeat_leave_expiry_and_rebalance",
            consumer_group_assignment: "shard_zero_coordinator_lexical_round_robin",
            consumer_group_claims: "replicated_session_generation_fence_preserving_offset",
            max_consumer_groups: MAX_STREAM_CONSUMER_GROUPS,
            max_consumer_group_bytes: MAX_STREAM_CONSUMER_GROUP_BYTES,
            max_consumer_member_bytes: MAX_STREAM_CONSUMER_MEMBER_BYTES,
            max_consumer_members_per_group: MAX_STREAM_CONSUMER_MEMBERS_PER_GROUP,
            min_consumer_session_timeout_ms: MIN_STREAM_SESSION_TIMEOUT_MS,
            max_consumer_session_timeout_ms: MAX_STREAM_SESSION_TIMEOUT_MS,
            retention_contract: "replicated_v4_time_size_combined_with_explicit_idle_maintenance",
            max_retention_records_per_partition: MAX_STREAM_RETENTION_RECORDS_PER_PARTITION,
            max_retention_bytes_per_partition: MAX_STREAM_RETENTION_BYTES_PER_PARTITION,
            max_retention_age_ms: MAX_STREAM_RETENTION_AGE_MS,
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            shard_index,
            shard_count,
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
            read,
        })
    }
}

#[derive(Debug, Serialize)]
struct StreamTabletFetchResponse {
    #[serde(flatten)]
    read: TabletReadMetadata,
    shard_index: u32,
    records: Vec<StreamTabletRecordResponse>,
}

#[derive(Debug, Serialize)]
struct StreamTabletGroupObservationResponse {
    #[serde(flatten)]
    read: TabletReadMetadata,
    shard_index: u32,
    checkpoint: StreamTabletGroupObservation,
}

#[derive(Debug, Serialize)]
struct StreamTabletGroupFetchResponse {
    #[serde(flatten)]
    read: TabletReadMetadata,
    shard_index: u32,
    checkpoint: StreamTabletGroupObservation,
    records: Vec<StreamTabletRecordResponse>,
}

#[derive(Debug, Serialize)]
struct StreamTabletSessionObservationResponse {
    #[serde(flatten)]
    read: TabletReadMetadata,
    shard_index: u32,
    session: StreamTabletSessionObservation,
}

#[derive(Debug, Serialize)]
struct StreamTabletRetentionResponse {
    #[serde(flatten)]
    read: TabletReadMetadata,
    shard_index: u32,
    retention: StreamTabletRetentionObservation,
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

impl StreamTabletRecordResponse {
    fn with_partition(record: StreamRecord, partition: u32) -> Self {
        Self {
            partition,
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
    shard_index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<StreamTabletMutationReceipt>,
}

impl StreamTabletMutationResponse {
    fn unknown(proposal_id: u64, shard_index: u32) -> Self {
        Self {
            proposal_id,
            state: MutationState::Unknown,
            outcome_certainty: OutcomeCertainty::Unknown,
            observation_scope: "local",
            shard_index,
            receipt: None,
        }
    }

    fn pending(proposal_id: u64, shard_index: u32) -> Self {
        Self {
            proposal_id,
            state: MutationState::Pending,
            outcome_certainty: OutcomeCertainty::Unknown,
            observation_scope: "local",
            shard_index,
            receipt: None,
        }
    }

    fn committed(receipt: StreamTabletMutationReceipt, shard_index: u32) -> Self {
        Self {
            proposal_id: receipt.proposal_id(),
            state: MutationState::Committed,
            outcome_certainty: OutcomeCertainty::Committed,
            observation_scope: "local",
            shard_index,
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
        committed_for_scope(&scope, key, event_id, index)
    }

    fn committed_for_scope(
        scope: &StreamTabletScope,
        key: &str,
        event_id: &str,
        index: u64,
    ) -> CommittedProposal {
        let mut envelope = EventEnvelope::new("tests", "order.created", json!({"id": event_id}), 1);
        envelope.id = event_id.into();
        let command = StreamTabletCommand::append(scope, key, envelope, 10 + index).unwrap();
        committed_command_for_scope(scope, &command, index)
    }

    fn committed_command_for_scope(
        scope: &StreamTabletScope,
        command: &StreamTabletCommand,
        index: u64,
    ) -> CommittedProposal {
        CommittedProposal {
            receipt: CommitReceipt {
                group_id: GroupId::new(scope.consensus_group_id).unwrap(),
                group_epoch: GroupEpoch::new(scope.tablet_epoch).unwrap(),
                proposal_id: ProposalId::new(command.proposal_id(scope).unwrap()).unwrap(),
                term: Term::new(2),
                log_index: LogIndex::new(index),
            },
            payload: command.encode(scope).unwrap(),
        }
    }

    #[test]
    fn automatic_maintenance_proposals_are_due_only_and_stable_until_apply() {
        let scope = scope();
        let service = StreamTabletService::new_for_shard(scope.clone(), 0, 3).unwrap();
        let retention = StreamTabletCommand::configure_retention(
            &scope,
            "retention",
            StreamRetentionPolicy {
                max_age_ms: Some(10),
                ..StreamRetentionPolicy::default()
            },
            100,
        )
        .unwrap();
        service
            .apply(&committed_command_for_scope(&scope, &retention, 1))
            .unwrap();
        let append = committed_for_scope(&scope, "append", "order-1", 2);
        service.apply(&append).unwrap();
        let join = StreamTabletCommand::join_group_session(
            &scope, "join", "billing", "worker-a", 3, 1_000, 100,
        )
        .unwrap();
        service
            .apply(&committed_command_for_scope(&scope, &join, 3))
            .unwrap();

        assert!(service.maintenance_proposals(109).unwrap().is_empty());
        let retention_due = service.maintenance_proposals(110).unwrap();
        assert_eq!(retention_due.len(), 1);
        assert_eq!(
            retention_due[0].operation,
            RegionalMaintenanceOperation::StreamRetention
        );
        let due = service.maintenance_proposals(1_100).unwrap();
        assert_eq!(due.len(), 2);
        assert_eq!(
            due.iter()
                .map(|proposal| proposal.operation)
                .collect::<Vec<_>>(),
            [
                RegionalMaintenanceOperation::StreamRetention,
                RegionalMaintenanceOperation::StreamConsumerSession
            ]
        );
        assert_eq!(due, service.maintenance_proposals(2_000).unwrap());
    }

    #[test]
    fn sharded_service_externalizes_the_logical_partition_and_restores_its_scope() {
        let scope = StreamTabletScope::new_with_consensus_group(21, 31, 3, "orders").unwrap();
        let service = StreamTabletService::new_for_shard(scope.clone(), 14, 16).unwrap();
        let append = committed_for_scope(&scope, "one", "order-1", 4);
        service.apply(&append).unwrap();

        let receipt = receipt_for_response(
            service.committed_receipt(&append).unwrap(),
            false,
            service.shard_index(),
        );
        let StreamTabletMutationReceipt::Append(receipt) = receipt else {
            panic!("append must return an append receipt");
        };
        assert_eq!(receipt.partition, 14);
        let record = StreamTabletRecordResponse::with_partition(
            service.fetch(0, 1).unwrap().remove(0),
            service.shard_index(),
        );
        assert_eq!(record.partition, 14);

        let records = [2, 3].map(|client_sequence| StreamBatchRecord {
            client_sequence,
            envelope: EventEnvelope::new(
                "tests",
                "order.created",
                json!({"id": client_sequence}),
                20 + u64::from(client_sequence),
            ),
        });
        let batch = committed_command_for_scope(
            &scope,
            &StreamTabletCommand::append_batch(
                &scope,
                "batch",
                StreamCompression::None,
                &records,
                15,
            )
            .unwrap(),
            5,
        );
        service.apply(&batch).unwrap();
        let batch_receipt = receipt_for_response(
            service.committed_receipt(&batch).unwrap(),
            false,
            service.shard_index(),
        );
        let StreamTabletMutationReceipt::Append(batch_receipt) = batch_receipt else {
            panic!("batch must return an append receipt");
        };
        assert_eq!(batch_receipt.partition, 14);
        assert!(
            batch_receipt
                .batch
                .unwrap()
                .records
                .iter()
                .all(|record| record.partition == 14)
        );

        let checkpoint = committed_command_for_scope(
            &scope,
            &StreamTabletCommand::group_offset(
                &scope,
                "checkpoint",
                "billing",
                "worker-a",
                1,
                0,
                3,
                StreamGroupOffsetMode::Commit,
                16,
            )
            .unwrap(),
            6,
        );
        service.apply(&checkpoint).unwrap();
        let checkpoint_receipt = receipt_for_response(
            service.committed_receipt(&checkpoint).unwrap(),
            false,
            service.shard_index(),
        );
        let StreamTabletMutationReceipt::Group(checkpoint_receipt) = checkpoint_receipt else {
            panic!("checkpoint must return a group receipt");
        };
        assert_eq!(checkpoint_receipt.partition, 14);

        let image = service
            .capture_snapshot(LogIndex::new(6), &[append, batch, checkpoint])
            .unwrap();
        let restored = StreamTabletService::new_for_shard(scope, 14, 16).unwrap();
        restored.install_snapshot(&image).unwrap();
        assert_eq!(restored.shard_index(), 14);
        assert_eq!(restored.fetch(0, 10).unwrap().len(), 3);
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
    fn native_snapshot_restores_stream_state_and_only_the_retained_retry_suffix() {
        let service = StreamTabletService::new(scope()).unwrap();
        let first = committed("one", "one", 4);
        let second = committed("two", "two", 5);
        service.apply(&first).unwrap();
        service.apply(&second).unwrap();
        let expected = service.snapshot().unwrap();

        let image = service
            .capture_snapshot(LogIndex::new(5), std::slice::from_ref(&second))
            .unwrap();
        let restored = StreamTabletService::new(scope()).unwrap();
        restored.install_snapshot(&image).unwrap();

        let actual = restored.snapshot().unwrap();
        assert_eq!(restored.fetch(0, 10).unwrap().len(), 2);
        assert_eq!(actual.state_digest, expected.state_digest);
        assert_eq!(actual.last_profile_mutation_index, 5);
        assert_eq!(actual.applied_command_count, 1);
        restored.apply(&committed("three", "three", 6)).unwrap();
        assert_eq!(restored.fetch(0, 10).unwrap().len(), 3);
    }

    #[test]
    fn native_snapshot_install_rejects_foreign_scope_without_partial_state() {
        let source = StreamTabletService::new(scope()).unwrap();
        let proposal = committed("one", "one", 4);
        source.apply(&proposal).unwrap();
        let image = source
            .capture_snapshot(LogIndex::new(4), std::slice::from_ref(&proposal))
            .unwrap();
        let target =
            StreamTabletService::new(StreamTabletScope::new(8, 3, "orders").unwrap()).unwrap();

        assert!(target.install_snapshot(&image).is_err());
        assert!(target.snapshot().is_err());
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
            serde_json::to_value(StreamTabletMutationResponse::pending(proposal_id, 0)).unwrap();

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
            checkpoint_index: LogIndex::ZERO,
            retained_log_first_index: LogIndex::new(1),
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
        assert_eq!(
            document["supported_batch_compressions"],
            json!(["none", "gzip", "lz4", "snappy", "zstd"])
        );
        assert_eq!(document["max_batch_records"], MAX_STREAM_BATCH_RECORDS);
        assert_eq!(
            document["max_batch_uncompressed_bytes"],
            MAX_STREAM_BATCH_UNCOMPRESSED_BYTES
        );
        assert_eq!(
            document["consumer_group_checkpoints"],
            "replicated_commit_reset_lag_and_replay"
        );
        assert_eq!(
            document["consumer_group_ownership_fencing"],
            "caller_supplied_monotonic_generation"
        );
        assert_eq!(document["max_consumer_groups"], MAX_STREAM_CONSUMER_GROUPS);
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
            checkpoint_index: LogIndex::ZERO,
            retained_log_first_index: LogIndex::new(1),
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
            serde_json::to_value(unresolved_response(9, &ProposalLookup::Unknown, 0)).unwrap();
        let pending = serde_json::to_value(unresolved_response(
            9,
            &ProposalLookup::Pending {
                payload: b"pending".to_vec(),
            },
            0,
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

    #[test]
    fn batch_request_is_strict_and_accepts_an_exact_compressed_payload() {
        let document = batch_body("batch-1", StreamCompression::Gzip, 10);
        let request: AppendBatchRequest = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(request.payload().compression, StreamCompression::Gzip);
        assert_eq!(request.payload().record_count, 2);

        let mut unknown = document;
        unknown["paylod_base64"] = unknown["payload_base64"].clone();
        assert!(serde_json::from_value::<AppendBatchRequest>(unknown).is_err());
    }

    #[test]
    fn group_offset_request_is_strict_and_browser_safe() {
        let document = json!({
            "idempotency_key": "billing-commit-1",
            "expected_term": u64::MAX.to_string(),
            "member_id": "worker-a",
            "group_generation": u64::MAX.to_string(),
            "partition": 0,
            "next_offset": u64::MAX.to_string(),
            "mode": "commit"
        });
        let request: GroupOffsetRequestBody = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(request.expected_term, u64::MAX);
        assert_eq!(request.group_generation, u64::MAX);
        assert_eq!(request.next_offset, u64::MAX);

        let mut unknown = document;
        unknown["generation"] = json!(1);
        assert!(serde_json::from_value::<GroupOffsetRequestBody>(unknown).is_err());
    }

    #[test]
    fn retention_requests_are_strict_browser_safe_bounded_and_semantic() {
        let document = json!({
            "idempotency_key": "retention-1",
            "expected_term": u64::MAX.to_string(),
            "max_records_per_partition": 100,
            "max_bytes_per_partition": "1048576",
            "max_age_ms": "86400000"
        });
        let request: RetentionConfigureRequest = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(request.expected_term, u64::MAX);
        assert_eq!(request.policy().max_bytes_per_partition, Some(1_048_576));
        assert_eq!(request.policy().max_age_ms, Some(86_400_000));
        request.validate(&scope()).unwrap();
        let command = request.command(&scope(), 123).unwrap();
        assert!(request.matches_command(&command));

        let mut unknown = document;
        unknown["retention_bytes"] = json!(1);
        assert!(serde_json::from_value::<RetentionConfigureRequest>(unknown).is_err());

        let invalid: RetentionConfigureRequest = serde_json::from_value(json!({
            "idempotency_key": "retention-invalid",
            "expected_term": "1",
            "max_bytes_per_partition": "0"
        }))
        .unwrap();
        assert!(invalid.validate(&scope()).is_err());

        let maintenance: RetentionMaintenanceRequest = serde_json::from_value(json!({
            "idempotency_key": "retention-sweep-1",
            "expected_term": "1"
        }))
        .unwrap();
        let command = maintenance.command(&scope(), 124).unwrap();
        assert!(maintenance.matches_command(&command));
    }

    #[test]
    fn group_offset_retry_semantics_compare_every_client_controlled_field() {
        let body: GroupOffsetRequestBody = serde_json::from_value(json!({
            "idempotency_key": "billing-commit-1",
            "expected_term": "2",
            "member_id": "worker-a",
            "group_generation": "1",
            "partition": 0,
            "next_offset": "1",
            "mode": "commit"
        }))
        .unwrap();
        let request = GroupOffsetRequest::new("billing".into(), body);
        let command = request.command(&scope(), 10).unwrap();
        assert!(request.matches_command(&command));

        let mut conflicting = request;
        conflicting.body.next_offset = 0;
        assert!(!conflicting.matches_command(&command));
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
        clock: Arc<ManualClock>,
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
                let service = StreamTabletService::new_for_shard(scope(), 0, 3).unwrap();
                let applier: Arc<dyn CommittedProposalApplier> = service.clone();
                let runtime =
                    ConsensusProbeRuntime::start_with_profile_applier(config, stable_path, applier)
                        .await
                        .unwrap();
                let clock = Arc::new(ManualClock::new(1_000));
                let app = runtime.internal_router().merge(router(
                    service,
                    runtime.handle(),
                    clock.clone(),
                    Duration::from_secs(2),
                ));
                let server = tokio::spawn(async move {
                    axum::serve(listener, app).await.unwrap();
                });
                nodes.push(RunningTabletNode {
                    runtime,
                    server,
                    base_url: urls[index].clone(),
                    clock,
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
        append_compressed_batches(&cluster, &client).await;
        commit_reset_and_fence_consumer_group(&cluster, &client).await;
        claim_and_fenced_fetch_consumer_group(&cluster, &client).await;
        assert_all_records(&cluster, &client, 12).await;
        assert_group_checkpoint_on_every_voter(&cluster, &client, "worker-c", "3", "2").await;
        let (leader_index, _) = cluster.leader().await;
        cluster.nodes[leader_index]
            .runtime
            .handle()
            .checkpoint()
            .await
            .expect("Stream profile checkpoint should persist before restart");
        cluster.shutdown().await;

        let reopened = RunningTabletCluster::start(&paths).await;
        assert_rebuilt_records(&reopened, &client, 12, 13).await;
        assert_group_checkpoint_on_every_voter(&reopened, &client, "worker-c", "3", "2").await;
        reopened.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn retention_commits_converges_and_restores_on_three_real_runtimes() {
        let temporary = TempDir::new().unwrap();
        let paths = tablet_paths(temporary.path());
        let cluster = RunningTabletCluster::start(&paths).await;
        let client = reqwest::Client::new();

        let configure = json!({
            "idempotency_key": "retention-age-10",
            "expected_term": "0",
            "max_records_per_partition": 10,
            "max_bytes_per_partition": "1048576",
            "max_age_ms": "10"
        });
        let (status, configured) =
            put_retention_to_current_leader(&cluster, &client, &configure).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(configured["receipt"]["mode"], "configure");
        assert_eq!(configured["receipt"]["policy"]["max_age_ms"], 10);

        let (status, appended) = post_to_current_leader(&cluster, &client, &append_body(1)).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(appended["receipt"]["offset"], "0");
        for node in &cluster.nodes {
            node.clock.set_wall_time_ms(1_010);
        }
        let maintain = json!({
            "idempotency_key": "retention-sweep-1010",
            "expected_term": "0"
        });
        let (status, maintained) =
            post_retention_maintenance_to_current_leader(&cluster, &client, &maintain).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(maintained["receipt"]["mode"], "maintain");
        assert_eq!(maintained["receipt"]["removed_records"], 1);
        assert_eq!(maintained["receipt"]["base_offset"], "1");

        assert_retention_on_every_voter(&cluster, &client, "1", "1", 0).await;
        let (leader, _) = cluster.leader().await;
        cluster.nodes[leader]
            .runtime
            .handle()
            .checkpoint()
            .await
            .expect("retained Stream boundary should checkpoint");
        cluster.shutdown().await;

        let reopened = RunningTabletCluster::start(&paths).await;
        assert_retention_on_every_voter(&reopened, &client, "1", "1", 0).await;
        reopened.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn consumer_sessions_rebalance_expire_converge_and_restore() {
        let temporary = TempDir::new().unwrap();
        let paths = tablet_paths(temporary.path());
        let cluster = RunningTabletCluster::start(&paths).await;
        let client = reqwest::Client::new();

        let (status, first) = submit_session_to_current_leader(
            &cluster,
            &client,
            reqwest::Method::POST,
            SessionTarget::Group,
            &json!({
                "idempotency_key": "join-a",
                "expected_term": "0",
                "member_id": "worker-a",
                "session_timeout_ms": "1000"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(first["receipt"]["group_generation"], "1");
        assert_eq!(first["receipt"]["assigned_shards"], json!([0, 1, 2]));

        for node in &cluster.nodes {
            node.clock.set_wall_time_ms(1_500);
        }
        let (_, second) = submit_session_to_current_leader(
            &cluster,
            &client,
            reqwest::Method::POST,
            SessionTarget::Group,
            &json!({
                "idempotency_key": "join-b",
                "expected_term": "0",
                "member_id": "worker-b",
                "session_timeout_ms": "5000"
            }),
        )
        .await;
        assert_eq!(second["receipt"]["group_generation"], "2");
        assert_eq!(
            second["receipt"]["members"][0]["assigned_shards"],
            json!([0, 2])
        );
        assert_eq!(
            second["receipt"]["members"][1]["assigned_shards"],
            json!([1])
        );

        let (_, heartbeat) = submit_session_to_current_leader(
            &cluster,
            &client,
            reqwest::Method::PUT,
            SessionTarget::Heartbeat("worker-a"),
            &json!({
                "idempotency_key": "heartbeat-a",
                "expected_term": "0",
                "group_generation": "2"
            }),
        )
        .await;
        assert_eq!(heartbeat["receipt"]["group_generation"], "2");

        for node in &cluster.nodes {
            node.clock.set_wall_time_ms(2_500);
        }
        let (_, maintained) = submit_session_to_current_leader(
            &cluster,
            &client,
            reqwest::Method::POST,
            SessionTarget::Maintenance,
            &json!({"idempotency_key": "maintain-2500", "expected_term": "0"}),
        )
        .await;
        assert_eq!(maintained["receipt"]["group_generation"], "3");
        assert_eq!(
            maintained["receipt"]["expired_members"],
            json!(["worker-a"])
        );
        assert_eq!(
            maintained["receipt"]["members"][0]["assigned_shards"],
            json!([0, 1, 2])
        );

        assert_session_on_every_voter(&cluster, &client, "3", "2500", 1).await;
        let (leader, _) = cluster.leader().await;
        cluster.nodes[leader]
            .runtime
            .handle()
            .checkpoint()
            .await
            .expect("consumer session should checkpoint");
        cluster.shutdown().await;

        let reopened = RunningTabletCluster::start(&paths).await;
        assert_session_on_every_voter(&reopened, &client, "3", "2500", 1).await;
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

    async fn append_compressed_batches(cluster: &RunningTabletCluster, client: &reqwest::Client) {
        for (index, compression) in [
            StreamCompression::None,
            StreamCompression::Gzip,
            StreamCompression::Lz4,
            StreamCompression::Snappy,
            StreamCompression::Zstd,
        ]
        .into_iter()
        .enumerate()
        {
            let sequence = u32::try_from(index).unwrap() * 10;
            let body = batch_body(&format!("batch-{compression:?}"), compression, sequence);
            let (status, committed) = post_batch_to_current_leader(cluster, client, &body).await;
            assert_eq!(status, StatusCode::CREATED);
            assert_eq!(committed["state"], "committed");
            assert_eq!(
                committed["receipt"]["batch"]["compression"],
                compression_name(compression)
            );
            assert_eq!(committed["receipt"]["batch"]["record_count"], 2);
            assert_eq!(
                committed["receipt"]["batch"]["records"][0]["client_sequence"],
                sequence
            );
            assert_eq!(
                committed["receipt"]["batch"]["records"][1]["client_sequence"],
                sequence + 1
            );
            assert!(committed["receipt"]["batch"]["records"][0]["offset"].is_string());

            let (retry_status, replayed) =
                post_batch_to_current_leader(cluster, client, &body).await;
            assert_eq!(retry_status, StatusCode::OK);
            assert_eq!(replayed["receipt"]["disposition"], "replayed");
            assert_eq!(replayed["receipt"]["batch"], committed["receipt"]["batch"]);
        }

        let changed = batch_body("batch-Gzip", StreamCompression::Gzip, 200);
        let (status, document) = post_batch_to_current_leader(cluster, client, &changed).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(document["error"]["code"], "idempotency_conflict");
    }

    async fn commit_reset_and_fence_consumer_group(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
    ) {
        let commit = group_body("group-commit", "worker-a", 1, 4, "commit");
        let (status, receipt) = put_group_to_current_leader(cluster, client, &commit).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(receipt["receipt"]["outcome"], "applied");
        assert_eq!(receipt["receipt"]["committed_offset"], "4");
        assert_eq!(receipt["receipt"]["lag"], "8");

        let (retry_status, retry) = put_group_to_current_leader(cluster, client, &commit).await;
        assert_eq!(retry_status, StatusCode::OK);
        assert_eq!(retry["receipt"]["disposition"], "replayed");

        let wrong_owner = group_body("group-wrong-owner", "worker-b", 1, 6, "commit");
        let (status, rejected) = put_group_to_current_leader(cluster, client, &wrong_owner).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(rejected["receipt"]["outcome"], "rejected");
        assert_eq!(rejected["receipt"]["rejection"], "owner_mismatch");
        assert_eq!(rejected["receipt"]["committed_offset"], "4");

        let reset = group_body("group-reset", "worker-b", 2, 2, "reset");
        let (status, reset_receipt) = put_group_to_current_leader(cluster, client, &reset).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(reset_receipt["receipt"]["outcome"], "applied");
        assert_eq!(reset_receipt["receipt"]["previous_offset"], "4");
        assert_eq!(reset_receipt["receipt"]["committed_offset"], "2");

        let stale = group_body("group-stale", "worker-a", 1, 7, "commit");
        let (status, stale_receipt) = put_group_to_current_leader(cluster, client, &stale).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(stale_receipt["receipt"]["outcome"], "rejected");
        assert_eq!(stale_receipt["receipt"]["rejection"], "stale_generation");
    }

    async fn claim_and_fenced_fetch_consumer_group(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
    ) {
        let claim = json!({
            "idempotency_key": "group-claim-worker-c-3",
            "expected_term": "0",
            "member_id": "worker-c",
            "group_generation": "3",
            "partition": 0
        });
        let (status, receipt) = put_claim_to_current_leader(cluster, client, &claim).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(receipt["receipt"]["outcome"], "applied");
        assert_eq!(receipt["receipt"]["mode"], "claim");
        assert_eq!(receipt["receipt"]["previous_offset"], "2");
        assert_eq!(receipt["receipt"]["committed_offset"], "2");
        assert_eq!(receipt["receipt"]["session_fenced"], true);

        let (retry_status, retry) = put_claim_to_current_leader(cluster, client, &claim).await;
        assert_eq!(retry_status, StatusCode::OK);
        assert_eq!(retry["receipt"]["disposition"], "replayed");

        let (leader, _) = cluster.leader().await;
        let fetched: Value = client
            .get(group_claimed_records_url_for(&cluster.nodes[leader]))
            .query(&[
                ("member_id", "worker-c"),
                ("group_generation", "3"),
                ("limit", "2"),
            ])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(fetched["records"].as_array().unwrap().len(), 2);
        assert_eq!(fetched["records"][0]["offset"], "2");
        assert_eq!(fetched["checkpoint"]["session_fenced"], true);

        for (member, generation) in [("worker-b", "3"), ("worker-c", "2")] {
            let response = client
                .get(group_claimed_records_url_for(&cluster.nodes[leader]))
                .query(&[
                    ("member_id", member),
                    ("group_generation", generation),
                    ("limit", "2"),
                ])
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let error: Value = response.json().await.unwrap();
            assert_eq!(error["error"]["code"], "fenced");
            assert_eq!(
                error["error"]["outcome_certainty"],
                "definite_not_committed"
            );
        }

        let stale_commit = group_body("group-stale-after-claim", "worker-b", 2, 3, "commit");
        let (status, stale) = put_group_to_current_leader(cluster, client, &stale_commit).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(stale["receipt"]["outcome"], "rejected");
        assert_eq!(stale["receipt"]["rejection"], "stale_generation");
    }

    async fn assert_group_checkpoint_on_every_voter(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
        member_id: &str,
        generation: &str,
        committed_offset: &str,
    ) {
        for node in &cluster.nodes {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let lag: Value = client
                        .get(group_lag_url_for(node))
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    if lag["checkpoint"]["committed_offset"] == committed_offset
                        && lag["checkpoint"]["group_generation"] == generation
                    {
                        assert_eq!(lag["checkpoint"]["member_id"], member_id);
                        assert_eq!(lag["checkpoint"]["session_fenced"], true);
                        assert_eq!(lag["checkpoint"]["end_offset"], "12");
                        assert_eq!(lag["checkpoint"]["lag"], "10");
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("every voter should apply the consumer-group checkpoint");

            let replay: Value = client
                .get(group_records_url_for(node))
                .query(&[("limit", 2)])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(replay["checkpoint"]["committed_offset"], committed_offset);
            assert_eq!(replay["records"].as_array().unwrap().len(), 2);
            assert_eq!(replay["records"][0]["offset"], "2");
            assert_eq!(replay["records"][1]["offset"], "3");
        }
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

    async fn post_batch_to_current_leader(
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
                    .post(batch_url_for(&cluster.nodes[leader]))
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
        .expect("an exact idempotent batch should resolve under stable leadership")
    }

    async fn put_group_to_current_leader(
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
                    .put(group_offsets_url_for(&cluster.nodes[leader]))
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
        .expect("an exact group mutation should resolve under stable leadership")
    }

    async fn put_claim_to_current_leader(
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
                    .put(group_claim_url_for(&cluster.nodes[leader]))
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
        .expect("an exact group claim should resolve under stable leadership")
    }

    async fn put_retention_to_current_leader(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
        request: &Value,
    ) -> (StatusCode, Value) {
        submit_retention_to_current_leader(cluster, client, request, true).await
    }

    async fn post_retention_maintenance_to_current_leader(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
        request: &Value,
    ) -> (StatusCode, Value) {
        submit_retention_to_current_leader(cluster, client, request, false).await
    }

    async fn submit_retention_to_current_leader(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
        request: &Value,
        configure: bool,
    ) -> (StatusCode, Value) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let (leader, term) = cluster.leader().await;
                let mut attempt = request.clone();
                attempt["expected_term"] = json!(term);
                let builder = if configure {
                    client.put(retention_url_for(&cluster.nodes[leader]))
                } else {
                    client.post(retention_maintenance_url_for(&cluster.nodes[leader]))
                };
                let response = builder.json(&attempt).send().await.unwrap();
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
        .expect("an exact retention mutation should resolve under stable leadership")
    }

    #[derive(Clone, Copy)]
    enum SessionTarget<'a> {
        Group,
        Heartbeat(&'a str),
        Maintenance,
    }

    async fn submit_session_to_current_leader(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
        method: reqwest::Method,
        target: SessionTarget<'_>,
        request: &Value,
    ) -> (StatusCode, Value) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let (leader, term) = cluster.leader().await;
                let mut attempt = request.clone();
                attempt["expected_term"] = json!(term);
                let url = match target {
                    SessionTarget::Group => session_url_for(&cluster.nodes[leader]),
                    SessionTarget::Heartbeat(member) => {
                        session_heartbeat_url_for(&cluster.nodes[leader], member)
                    }
                    SessionTarget::Maintenance => {
                        session_maintenance_url_for(&cluster.nodes[leader])
                    }
                };
                let response = client
                    .request(method.clone(), url)
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
        .expect("an exact consumer-session mutation should resolve under stable leadership")
    }

    async fn assert_session_on_every_voter(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
        generation: &str,
        watermark_ms: &str,
        members: usize,
    ) {
        for node in &cluster.nodes {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let document: Value = client
                        .get(session_url_for(node))
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    if document["session"]["group_generation"] == generation
                        && document["session"]["watermark_ms"] == watermark_ms
                    {
                        assert_eq!(
                            document["session"]["members"].as_array().unwrap().len(),
                            members
                        );
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("every voter should expose the coordinated session state");
        }
    }

    async fn assert_retention_on_every_voter(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
        expected_base: &str,
        expected_end: &str,
        expected_records: usize,
    ) {
        for node in &cluster.nodes {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let response = client.get(retention_url_for(node)).send().await.unwrap();
                    let document: Value = response.json().await.unwrap();
                    if document["retention"]["base_offset"] == expected_base
                        && document["retention"]["end_offset"] == expected_end
                    {
                        assert_eq!(document["retention"]["retained_records"], expected_records);
                        assert_eq!(document["retention"]["retention_watermark_ms"], "1010");
                        assert_eq!(document["retention"]["policy"]["max_age_ms"], 10);
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("every voter should expose the committed retention boundary");
        }
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

    async fn assert_all_records(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
        expected: usize,
    ) {
        for node in &cluster.nodes {
            let fetch_url = append_url_for(node);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let response = client.get(fetch_url.clone()).send().await.unwrap();
                    let document: Value = response.json().await.unwrap();
                    if document["records"]
                        .as_array()
                        .is_some_and(|records| records.len() == expected)
                    {
                        assert_eq!(document["records"][0]["offset"], "0");
                        assert_eq!(
                            document["records"][expected - 1]["offset"],
                            (expected - 1).to_string()
                        );
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

    async fn assert_rebuilt_records(
        cluster: &RunningTabletCluster,
        client: &reqwest::Client,
        expected_records: usize,
        expected_commands: usize,
    ) {
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
            assert_eq!(status["applied_command_count"], expected_commands);
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
            assert_eq!(
                records["records"].as_array().unwrap().len(),
                expected_records
            );
            assert_eq!(records["records"][0]["envelope"]["id"], "order-1");
            assert_eq!(records["records"][1]["envelope"]["id"], "order-2");
        }
    }

    fn append_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join(EXPERIMENTAL_STREAM_TABLET_RECORDS_PATH.trim_start_matches('/'))
            .unwrap()
    }

    fn batch_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join(EXPERIMENTAL_STREAM_TABLET_BATCHES_PATH.trim_start_matches('/'))
            .unwrap()
    }

    fn group_offsets_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join("experimental/v1/tablets/stream/groups/billing/offsets")
            .unwrap()
    }

    fn group_lag_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join("experimental/v1/tablets/stream/groups/billing/lag")
            .unwrap()
    }

    fn group_records_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join("experimental/v1/tablets/stream/groups/billing/records")
            .unwrap()
    }

    fn group_claim_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join("experimental/v1/tablets/stream/groups/billing/claim")
            .unwrap()
    }

    fn group_claimed_records_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join("experimental/v1/tablets/stream/groups/billing/claimed-records")
            .unwrap()
    }

    fn retention_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join(EXPERIMENTAL_STREAM_TABLET_RETENTION_PATH.trim_start_matches('/'))
            .unwrap()
    }

    fn retention_maintenance_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join(EXPERIMENTAL_STREAM_TABLET_RETENTION_MAINTENANCE_PATH.trim_start_matches('/'))
            .unwrap()
    }

    fn session_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join("experimental/v1/tablets/stream/groups/billing/sessions")
            .unwrap()
    }

    fn session_heartbeat_url_for(node: &RunningTabletNode, member: &str) -> Url {
        node.base_url
            .join(&format!(
                "experimental/v1/tablets/stream/groups/billing/sessions/{member}/heartbeat"
            ))
            .unwrap()
    }

    fn session_maintenance_url_for(node: &RunningTabletNode) -> Url {
        node.base_url
            .join("experimental/v1/tablets/stream/groups/billing/sessions/maintenance")
            .unwrap()
    }

    fn group_body(
        idempotency_key: &str,
        member_id: &str,
        group_generation: u64,
        next_offset: u64,
        mode: &str,
    ) -> Value {
        json!({
            "idempotency_key": idempotency_key,
            "expected_term": "0",
            "member_id": member_id,
            "group_generation": group_generation.to_string(),
            "partition": 0,
            "next_offset": next_offset.to_string(),
            "mode": mode,
        })
    }

    fn batch_body(idempotency_key: &str, compression: StreamCompression, sequence: u32) -> Value {
        let records = [sequence, sequence + 1]
            .into_iter()
            .map(|client_sequence| {
                let mut envelope = EventEnvelope::new(
                    "batch-tests",
                    "order.created",
                    json!({"sequence": client_sequence, "padding": "epoch-epoch-epoch"}),
                    1_000,
                );
                envelope.id = format!("batch-record-{client_sequence}");
                StreamBatchRecord {
                    client_sequence,
                    envelope,
                }
            })
            .collect::<Vec<_>>();
        let payload = encode_stream_batch_payload(&records, compression).unwrap();
        json!({
            "idempotency_key": idempotency_key,
            "expected_term": "0",
            "partition": 0,
            "compression": payload.compression,
            "record_count": payload.record_count,
            "uncompressed_bytes": payload.uncompressed_bytes,
            "compressed_bytes": payload.compressed_bytes,
            "payload_base64": payload.payload_base64,
        })
    }

    const fn compression_name(compression: StreamCompression) -> &'static str {
        match compression {
            StreamCompression::None => "none",
            StreamCompression::Gzip => "gzip",
            StreamCompression::Lz4 => "lz4",
            StreamCompression::Snappy => "snappy",
            StreamCompression::Zstd => "zstd",
        }
    }
}
