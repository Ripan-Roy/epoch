//! Typed, deterministic state machines applied only after consensus commit.
//!
//! The current bounded slices are configured, single-partition Stream, Queue,
//! and Event Bus tablets plus a single-shard Cache tablet. They own strict
//! command validation, deterministic application, idempotency, and replay while
//! the node owns transport and Raft. Stream, Queue, Cache, and Event Bus attach
//! to the experimental node runtime as mutually exclusive profiles for one
//! fixed consensus group. Event Bus replication covers ingress, route-plan
//! evidence, archive, and independent delivery-ledger state; built-in target
//! executors remain open.

mod bus;
mod cache;
mod common;
mod queue;
mod stream_batch;
mod stream_group;
mod stream_retention;
mod stream_session;

use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use common::{
    AppliedCommand, deserialize_u64_from_number_or_decimal, hash_length_prefixed,
    proposal_id_from_domain, serialize_u64_as_decimal, validate_committed_command_scope,
    validate_idempotency_key,
};
use epoch_core::{DurabilityProfile, EventEnvelope};
use epoch_stream::{AppendReceipt, Stream, StreamConfig, StreamRecord, StreamRetentionPolicy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use bus::*;
pub use cache::*;
pub use common::{
    AppliedCommandMetadata, CommittedCommand, MAX_IDEMPOTENCY_KEY_BYTES, StreamTabletScope,
    TabletError, TabletResult, TabletScope, TabletWriteEvidence as StreamTabletWriteEvidence,
    TabletWriteEvidence,
};
pub use queue::*;
pub use stream_batch::*;
pub use stream_group::*;
pub use stream_retention::*;
pub use stream_session::*;

pub const STREAM_TABLET_COMMAND_FORMAT_VERSION: u16 = 1;
pub const STREAM_TABLET_BATCH_COMMAND_FORMAT_VERSION: u16 = 2;
// Kept equal to the current consensus proposal ceiling. The state-machine
// boundary repeats the check so a command can never validate here and then be
// rejected only after it reaches Raft.
pub const MAX_STREAM_TABLET_COMMAND_BYTES: usize = 512 * 1024;
pub const STREAM_TABLET_SNAPSHOT_FORMAT_VERSION: u16 = 2;
const LEGACY_STREAM_TABLET_SNAPSHOT_FORMAT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamTabletCommand {
    pub format_version: u16,
    pub tablet_id: u64,
    pub tablet_epoch: u64,
    pub resource: String,
    pub idempotency_key: String,
    pub applied_at_ms: u64,
    pub operation: StreamTabletOperation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "boxing the historical v1 Append variant would break the public source contract"
)]
pub enum StreamTabletOperation {
    Append(StreamAppendCommand),
    AppendBatch(StreamAppendBatchCommand),
    GroupOffset(StreamGroupOffsetCommand),
    Retention(StreamRetentionCommand),
    GroupSession(StreamGroupSessionCommand),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamAppendCommand {
    pub partition: u32,
    pub envelope: EventEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamAppendBatchCommand {
    pub partition: u32,
    pub payload: StreamBatchPayload,
}

impl StreamTabletCommand {
    pub fn append(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        envelope: EventEnvelope,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        let command = Self {
            format_version: STREAM_TABLET_COMMAND_FORMAT_VERSION,
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            idempotency_key: idempotency_key.into(),
            applied_at_ms,
            operation: StreamTabletOperation::Append(StreamAppendCommand {
                partition: 0,
                envelope,
            }),
        };
        command.validate(scope)?;
        Ok(command)
    }

    pub fn append_batch(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        compression: StreamCompression,
        records: &[StreamBatchRecord],
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        let payload = encode_stream_batch_payload(records, compression)?;
        Self::append_compressed_batch(scope, idempotency_key, payload, applied_at_ms)
    }

    pub fn append_compressed_batch(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        payload: StreamBatchPayload,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        let command = Self {
            format_version: STREAM_TABLET_BATCH_COMMAND_FORMAT_VERSION,
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            idempotency_key: idempotency_key.into(),
            applied_at_ms,
            operation: StreamTabletOperation::AppendBatch(StreamAppendBatchCommand {
                partition: 0,
                payload,
            }),
        };
        command.validate(scope)?;
        Ok(command)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn group_offset(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        group: impl Into<String>,
        member_id: impl Into<String>,
        group_generation: u64,
        partition: u32,
        next_offset: u64,
        mode: StreamGroupOffsetMode,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        let command = Self {
            format_version: STREAM_TABLET_GROUP_COMMAND_FORMAT_VERSION,
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            idempotency_key: idempotency_key.into(),
            applied_at_ms,
            operation: StreamTabletOperation::GroupOffset(StreamGroupOffsetCommand {
                group: group.into(),
                member_id: member_id.into(),
                group_generation,
                partition,
                next_offset,
                mode,
            }),
        };
        command.validate(scope)?;
        Ok(command)
    }

    pub fn configure_retention(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        policy: StreamRetentionPolicy,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        let command = Self {
            format_version: STREAM_TABLET_RETENTION_COMMAND_FORMAT_VERSION,
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            idempotency_key: idempotency_key.into(),
            applied_at_ms,
            operation: StreamTabletOperation::Retention(StreamRetentionCommand {
                mode: StreamTabletRetentionMode::Configure,
                policy: Some(policy),
            }),
        };
        command.validate(scope)?;
        Ok(command)
    }

    pub fn maintain_retention(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        let command = Self {
            format_version: STREAM_TABLET_RETENTION_COMMAND_FORMAT_VERSION,
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            idempotency_key: idempotency_key.into(),
            applied_at_ms,
            operation: StreamTabletOperation::Retention(StreamRetentionCommand {
                mode: StreamTabletRetentionMode::Maintain,
                policy: None,
            }),
        };
        command.validate(scope)?;
        Ok(command)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn join_group_session(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        group: impl Into<String>,
        member_id: impl Into<String>,
        shard_count: u32,
        session_timeout_ms: u64,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        Self::group_session(
            scope,
            idempotency_key,
            group,
            shard_count,
            StreamGroupSessionAction::Join {
                member_id: member_id.into(),
                session_timeout_ms,
            },
            applied_at_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn heartbeat_group_session(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        group: impl Into<String>,
        member_id: impl Into<String>,
        shard_count: u32,
        group_generation: u64,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        Self::group_session(
            scope,
            idempotency_key,
            group,
            shard_count,
            StreamGroupSessionAction::Heartbeat {
                member_id: member_id.into(),
                group_generation,
            },
            applied_at_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn leave_group_session(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        group: impl Into<String>,
        member_id: impl Into<String>,
        shard_count: u32,
        group_generation: u64,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        Self::group_session(
            scope,
            idempotency_key,
            group,
            shard_count,
            StreamGroupSessionAction::Leave {
                member_id: member_id.into(),
                group_generation,
            },
            applied_at_ms,
        )
    }

    pub fn maintain_group_sessions(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        group: impl Into<String>,
        shard_count: u32,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        Self::group_session(
            scope,
            idempotency_key,
            group,
            shard_count,
            StreamGroupSessionAction::Maintain,
            applied_at_ms,
        )
    }

    fn group_session(
        scope: &StreamTabletScope,
        idempotency_key: impl Into<String>,
        group: impl Into<String>,
        shard_count: u32,
        action: StreamGroupSessionAction,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        let command = Self {
            format_version: STREAM_TABLET_SESSION_COMMAND_FORMAT_VERSION,
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            idempotency_key: idempotency_key.into(),
            applied_at_ms,
            operation: StreamTabletOperation::GroupSession(StreamGroupSessionCommand {
                group: group.into(),
                shard_count,
                action,
            }),
        };
        command.validate(scope)?;
        Ok(command)
    }

    pub fn encode(&self, scope: &StreamTabletScope) -> TabletResult<Vec<u8>> {
        self.validate(scope)?;
        let encoded =
            serde_json::to_vec(self).map_err(|error| TabletError::Encoding(error.to_string()))?;
        if encoded.len() > MAX_STREAM_TABLET_COMMAND_BYTES {
            return Err(TabletError::InvalidCommand(format!(
                "encoded command is {} bytes; maximum is {MAX_STREAM_TABLET_COMMAND_BYTES}",
                encoded.len()
            )));
        }
        Ok(encoded)
    }

    pub fn decode(payload: &[u8], scope: &StreamTabletScope) -> TabletResult<Self> {
        if payload.len() > MAX_STREAM_TABLET_COMMAND_BYTES {
            return Err(TabletError::InvalidCommand(format!(
                "encoded command is {} bytes; maximum is {MAX_STREAM_TABLET_COMMAND_BYTES}",
                payload.len()
            )));
        }
        let command: Self = serde_json::from_slice(payload)
            .map_err(|error| TabletError::Decoding(error.to_string()))?;
        command.validate(scope)?;
        let canonical = serde_json::to_vec(&command)
            .map_err(|error| TabletError::Encoding(error.to_string()))?;
        if canonical != payload {
            return Err(TabletError::Decoding(format!(
                "command bytes are not in canonical v{} encoding",
                command.format_version
            )));
        }
        Ok(command)
    }

    pub fn proposal_id(&self, scope: &StreamTabletScope) -> TabletResult<u64> {
        self.validate(scope)?;
        proposal_id_for(scope, &self.idempotency_key)
    }

    pub fn decode_batch_records(&self) -> TabletResult<Vec<StreamBatchRecord>> {
        match &self.operation {
            StreamTabletOperation::AppendBatch(batch) => {
                decode_stream_batch_payload(&batch.payload)
            }
            StreamTabletOperation::Append(_) => Err(TabletError::InvalidCommand(
                "single-record append has no batch payload".into(),
            )),
            StreamTabletOperation::GroupOffset(_) => Err(TabletError::InvalidCommand(
                "consumer-group offset command has no batch payload".into(),
            )),
            StreamTabletOperation::Retention(_) => Err(TabletError::InvalidCommand(
                "retention command has no batch payload".into(),
            )),
            StreamTabletOperation::GroupSession(_) => Err(TabletError::InvalidCommand(
                "consumer-session command has no batch payload".into(),
            )),
        }
    }

    fn validate(&self, scope: &StreamTabletScope) -> TabletResult<()> {
        scope.validate()?;
        if self.tablet_id != scope.tablet_id {
            return Err(TabletError::GroupMismatch {
                expected: scope.tablet_id,
                observed: self.tablet_id,
            });
        }
        if self.tablet_epoch != scope.tablet_epoch {
            return Err(TabletError::FencedEpoch {
                expected: scope.tablet_epoch,
                observed: self.tablet_epoch,
            });
        }
        if self.resource != scope.resource {
            return Err(TabletError::InvalidCommand(format!(
                "command targets resource {}; expected {}",
                self.resource, scope.resource
            )));
        }
        validate_idempotency_key(&self.idempotency_key)?;
        match &self.operation {
            StreamTabletOperation::Append(append) => {
                if self.format_version != STREAM_TABLET_COMMAND_FORMAT_VERSION {
                    return Err(TabletError::InvalidCommand(format!(
                        "single-record append requires format_version {STREAM_TABLET_COMMAND_FORMAT_VERSION}; observed {}",
                        self.format_version
                    )));
                }
                if append.partition != 0 {
                    return Err(TabletError::InvalidCommand(
                        "the first Stream tablet slice supports only partition 0".into(),
                    ));
                }
                append.envelope.validate()?;
            }
            StreamTabletOperation::AppendBatch(batch) => {
                if self.format_version != STREAM_TABLET_BATCH_COMMAND_FORMAT_VERSION {
                    return Err(TabletError::InvalidCommand(format!(
                        "batch append requires format_version {STREAM_TABLET_BATCH_COMMAND_FORMAT_VERSION}; observed {}",
                        self.format_version
                    )));
                }
                if batch.partition != 0 {
                    return Err(TabletError::InvalidCommand(
                        "the first Stream tablet slice supports only partition 0".into(),
                    ));
                }
                decode_stream_batch_payload(&batch.payload)?;
            }
            StreamTabletOperation::GroupOffset(group) => {
                if self.format_version != STREAM_TABLET_GROUP_COMMAND_FORMAT_VERSION {
                    return Err(TabletError::InvalidCommand(format!(
                        "consumer-group offset mutation requires format_version {STREAM_TABLET_GROUP_COMMAND_FORMAT_VERSION}; observed {}",
                        self.format_version
                    )));
                }
                validate_group_offset_command(group)?;
            }
            StreamTabletOperation::Retention(retention) => {
                if self.format_version != STREAM_TABLET_RETENTION_COMMAND_FORMAT_VERSION {
                    return Err(TabletError::InvalidCommand(format!(
                        "retention mutation requires format_version {STREAM_TABLET_RETENTION_COMMAND_FORMAT_VERSION}; observed {}",
                        self.format_version
                    )));
                }
                validate_retention_command(retention)?;
            }
            StreamTabletOperation::GroupSession(session) => {
                if self.format_version != STREAM_TABLET_SESSION_COMMAND_FORMAT_VERSION {
                    return Err(TabletError::InvalidCommand(format!(
                        "consumer-session mutation requires format_version {STREAM_TABLET_SESSION_COMMAND_FORMAT_VERSION}; observed {}",
                        self.format_version
                    )));
                }
                validate_session_command(session)?;
            }
        }
        Ok(())
    }
}

pub fn proposal_id_for(scope: &StreamTabletScope, idempotency_key: &str) -> TabletResult<u64> {
    proposal_id_from_domain(
        b"epoch/stream-tablet/proposal-id/v1\0",
        scope,
        idempotency_key,
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTabletAppendReceipt {
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub proposal_id: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub tablet_id: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub tablet_epoch: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub term: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub commit_index: u64,
    pub partition: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub offset: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub applied_at_ms: u64,
    pub write_evidence: StreamTabletWriteEvidence,
    pub durable_voter_acks: u16,
    pub disposition: StreamTabletAppendDisposition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<StreamTabletBatchReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTabletBatchReceipt {
    pub compression: StreamCompression,
    pub record_count: u16,
    pub compressed_bytes: u32,
    pub uncompressed_bytes: u32,
    pub records: Vec<StreamTabletBatchRecordReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamTabletBatchRecordReceipt {
    pub client_sequence: u32,
    pub partition: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub offset: u64,
    pub disposition: StreamTabletAppendDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletAppendDisposition {
    New,
    Replayed,
    ProfileDeduplicated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StreamTabletMutationReceipt {
    Append(StreamTabletAppendReceipt),
    Group(StreamTabletGroupReceipt),
    Retention(StreamTabletRetentionReceipt),
    Session(StreamTabletSessionReceipt),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedStreamTabletSnapshot {
    format_version: u16,
    scope: StreamTabletScope,
    stream_base64: String,
    consumer_groups: BTreeMap<String, StreamConsumerGroupOwner>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    session_groups: BTreeMap<String, StreamConsumerSessionGroup>,
    applied: Vec<StreamTabletAppliedSnapshot>,
    last_applied_command_index: u64,
    state_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamTabletAppliedSnapshot {
    proposal_id: u64,
    applied: AppliedCommand<StreamTabletMutationReceipt>,
}

impl StreamTabletMutationReceipt {
    pub const fn proposal_id(&self) -> u64 {
        match self {
            Self::Append(receipt) => receipt.proposal_id,
            Self::Group(receipt) => receipt.proposal_id,
            Self::Retention(receipt) => receipt.proposal_id,
            Self::Session(receipt) => receipt.proposal_id,
        }
    }

    pub fn mark_replayed(&mut self) {
        match self {
            Self::Append(receipt) => {
                receipt.disposition = StreamTabletAppendDisposition::Replayed;
            }
            Self::Group(receipt) => {
                receipt.disposition = StreamTabletGroupDisposition::Replayed;
            }
            Self::Retention(receipt) => {
                receipt.disposition = StreamTabletRetentionDisposition::Replayed;
            }
            Self::Session(receipt) => {
                receipt.disposition = StreamTabletSessionDisposition::Replayed;
            }
        }
    }
}

#[derive(Debug)]
pub struct StreamTablet {
    scope: StreamTabletScope,
    stream: Stream,
    consumer_groups: BTreeMap<String, StreamConsumerGroupOwner>,
    session_groups: BTreeMap<String, StreamConsumerSessionGroup>,
    applied: BTreeMap<u64, AppliedCommand<StreamTabletMutationReceipt>>,
    last_applied_command_index: u64,
    state_digest: [u8; 32],
}

impl StreamTablet {
    pub fn new(scope: StreamTabletScope) -> TabletResult<Self> {
        scope.validate()?;
        let stream = Stream::new(StreamConfig {
            partitions: 1,
            // The embedded Stream supplies ordering and deduplication only.
            // Consensus persistence is reported separately as bounded evidence
            // rather than being mislabeled with a product durability profile.
            durability: DurabilityProfile::Volatile,
            max_records_per_partition: None,
            max_bytes_per_partition: None,
            max_age_ms: None,
        })?;
        let mut hasher = Sha256::new();
        hasher.update(b"epoch/stream-tablet/state/v1\0");
        hasher.update(scope.tablet_id.to_be_bytes());
        hasher.update(scope.tablet_epoch.to_be_bytes());
        hash_length_prefixed(&mut hasher, scope.resource.as_bytes());
        let state_digest = hasher.finalize().into();
        Ok(Self {
            scope,
            stream,
            consumer_groups: BTreeMap::new(),
            session_groups: BTreeMap::new(),
            applied: BTreeMap::new(),
            last_applied_command_index: 0,
            state_digest,
        })
    }

    pub fn scope(&self) -> &StreamTabletScope {
        &self.scope
    }

    pub fn apply(
        &mut self,
        committed: CommittedCommand<'_>,
    ) -> TabletResult<StreamTabletAppendReceipt> {
        let command = StreamTabletCommand::decode(committed.payload, &self.scope)?;
        if !matches!(
            command.operation,
            StreamTabletOperation::Append(_) | StreamTabletOperation::AppendBatch(_)
        ) {
            return Err(TabletError::InvalidCommand(
                "non-append commands require apply_mutation".into(),
            ));
        }
        match self.apply_mutation(committed)? {
            StreamTabletMutationReceipt::Append(receipt) => Ok(receipt),
            StreamTabletMutationReceipt::Group(_)
            | StreamTabletMutationReceipt::Retention(_)
            | StreamTabletMutationReceipt::Session(_) => unreachable!("operation checked above"),
        }
    }

    pub fn apply_mutation(
        &mut self,
        committed: CommittedCommand<'_>,
    ) -> TabletResult<StreamTabletMutationReceipt> {
        self.validate_commit_scope(committed)?;
        let metadata = AppliedCommandMetadata::from_committed(committed);
        if let Some(mut receipt) = self.mutation_receipt_for_committed(committed)? {
            receipt.mark_replayed();
            return Ok(receipt);
        }
        if committed.log_index <= self.last_applied_command_index {
            return Err(TabletError::CommitOrder {
                previous: self.last_applied_command_index,
                observed: committed.log_index,
            });
        }

        let command = StreamTabletCommand::decode(committed.payload, &self.scope)?;
        let expected_proposal_id = command.proposal_id(&self.scope)?;
        if committed.proposal_id != expected_proposal_id {
            return Err(TabletError::InvalidCommand(format!(
                "proposal_id {} does not match idempotency_key hash {expected_proposal_id}",
                committed.proposal_id
            )));
        }

        let receipt = match command.operation {
            StreamTabletOperation::Append(append) => StreamTabletMutationReceipt::Append(
                self.apply_append(committed, command.applied_at_ms, append, None)?,
            ),
            StreamTabletOperation::AppendBatch(batch) => {
                let receipt = self.apply_batch(committed, command.applied_at_ms, &batch)?;
                StreamTabletMutationReceipt::Append(receipt)
            }
            StreamTabletOperation::GroupOffset(group) => {
                let receipt = self.apply_group_offset(committed, command.applied_at_ms, group)?;
                StreamTabletMutationReceipt::Group(receipt)
            }
            StreamTabletOperation::Retention(retention) => {
                let receipt = self.apply_retention(committed, command.applied_at_ms, &retention)?;
                StreamTabletMutationReceipt::Retention(receipt)
            }
            StreamTabletOperation::GroupSession(session) => {
                let receipt = self.apply_group_session(committed, command.applied_at_ms, &session);
                StreamTabletMutationReceipt::Session(receipt)
            }
        };
        match &receipt {
            StreamTabletMutationReceipt::Append(receipt) => {
                self.advance_digest(committed, metadata.payload_digest, receipt);
            }
            StreamTabletMutationReceipt::Group(receipt) => {
                self.advance_group_digest(committed, metadata.payload_digest, receipt);
            }
            StreamTabletMutationReceipt::Retention(receipt) => {
                self.advance_retention_digest(committed, metadata.payload_digest, receipt);
            }
            StreamTabletMutationReceipt::Session(receipt) => {
                self.advance_session_digest(committed, metadata.payload_digest, receipt);
            }
        }
        self.last_applied_command_index = committed.log_index;
        self.applied.insert(
            committed.proposal_id,
            AppliedCommand {
                metadata,
                receipt: receipt.clone(),
            },
        );
        Ok(receipt)
    }

    pub fn lookup(&self, proposal_id: u64) -> Option<StreamTabletAppendReceipt> {
        self.applied
            .get(&proposal_id)
            .and_then(|applied| match &applied.receipt {
                StreamTabletMutationReceipt::Append(receipt) => Some(receipt.clone()),
                StreamTabletMutationReceipt::Group(_)
                | StreamTabletMutationReceipt::Retention(_)
                | StreamTabletMutationReceipt::Session(_) => None,
            })
    }

    /// Returns the actor-applied receipt only when the consensus commit exactly
    /// matches the already-applied command metadata.
    pub fn receipt_for_committed(
        &self,
        committed: CommittedCommand<'_>,
    ) -> TabletResult<Option<StreamTabletAppendReceipt>> {
        Ok(match self.mutation_receipt_for_committed(committed)? {
            Some(StreamTabletMutationReceipt::Append(receipt)) => Some(receipt),
            Some(
                StreamTabletMutationReceipt::Group(_)
                | StreamTabletMutationReceipt::Retention(_)
                | StreamTabletMutationReceipt::Session(_),
            )
            | None => None,
        })
    }

    pub fn mutation_receipt_for_committed(
        &self,
        committed: CommittedCommand<'_>,
    ) -> TabletResult<Option<StreamTabletMutationReceipt>> {
        self.validate_commit_scope(committed)?;
        let Some(previous) = self.applied.get(&committed.proposal_id) else {
            return Ok(None);
        };
        previous.metadata.validate_exact(committed)?;
        Ok(Some(previous.receipt.clone()))
    }

    pub fn fetch(&self, offset: u64, limit: usize) -> TabletResult<Vec<StreamRecord>> {
        Ok(self.stream.fetch(0, offset, limit)?)
    }

    pub fn fetch_for_group(&self, group: &str, limit: usize) -> TabletResult<Vec<StreamRecord>> {
        let observation = self.group_observation(group)?;
        self.fetch(observation.committed_offset, limit)
    }

    pub fn group_observation(&self, group: &str) -> TabletResult<StreamTabletGroupObservation> {
        validate_stream_consumer_group(group)?;
        let (base_offset, end_offset) = self.stream.offsets(0)?;
        let lag = self.stream.lag(group, 0)?;
        let owner = self.consumer_groups.get(group);
        Ok(StreamTabletGroupObservation {
            exists: owner.is_some(),
            group: group.to_owned(),
            member_id: owner.map(|owner| owner.member_id.clone()),
            group_generation: owner.map(|owner| owner.generation),
            partition: 0,
            base_offset,
            committed_offset: lag.committed_offset,
            end_offset,
            lag: lag.lag,
            checkpoint_out_of_range: lag.checkpoint_out_of_range,
        })
    }

    pub fn session_observation(&self, group: &str) -> TabletResult<StreamTabletSessionObservation> {
        validate_stream_consumer_group(group)?;
        Ok(self.session_groups.get(group).map_or_else(
            || absent_session_observation(group),
            |session| session.observation(group),
        ))
    }

    pub fn retention_observation(&self) -> TabletResult<StreamTabletRetentionObservation> {
        let (base_offset, end_offset) = self.stream.offsets(0)?;
        let retained_records = self.stream.fetch(0, base_offset, usize::MAX)?.len();
        Ok(StreamTabletRetentionObservation {
            policy: self.stream.retention_policy(),
            retention_watermark_ms: self.stream.retention_watermark_ms(),
            partition: 0,
            base_offset,
            end_offset,
            retained_records,
            retained_bytes: self.stream.retained_bytes(0)?,
        })
    }

    /// Latest consensus index containing a unique command applied to this
    /// profile. Raft no-ops are intentionally outside this state machine.
    pub fn last_applied_command_index(&self) -> u64 {
        self.last_applied_command_index
    }

    pub fn applied_command_count(&self) -> usize {
        self.applied.len()
    }

    pub fn state_digest(&self) -> [u8; 32] {
        self.state_digest
    }

    pub fn encode_snapshot(&self, retained: &BTreeSet<u64>) -> TabletResult<Vec<u8>> {
        let mut applied = self
            .applied
            .iter()
            .filter(|(proposal_id, _)| retained.contains(proposal_id))
            .map(|(proposal_id, applied)| StreamTabletAppliedSnapshot {
                proposal_id: *proposal_id,
                applied: applied.clone(),
            })
            .collect::<Vec<_>>();
        if applied.len() != retained.len() {
            return Err(TabletError::InvalidCommand(
                "Stream snapshot retry set contains an unknown proposal".into(),
            ));
        }
        applied.sort_by_key(|entry| entry.applied.metadata.log_index);
        let stream = self.stream.encode_snapshot()?;
        serde_json::to_vec(&VersionedStreamTabletSnapshot {
            format_version: STREAM_TABLET_SNAPSHOT_FORMAT_VERSION,
            scope: self.scope.clone(),
            stream_base64: STANDARD_NO_PAD.encode(stream),
            consumer_groups: self.consumer_groups.clone(),
            session_groups: self.session_groups.clone(),
            applied,
            last_applied_command_index: self.last_applied_command_index,
            state_digest: self.state_digest,
        })
        .map_err(|error| TabletError::Encoding(error.to_string()))
    }

    pub fn decode_snapshot(
        expected_scope: &StreamTabletScope,
        encoded: &[u8],
    ) -> TabletResult<Self> {
        let snapshot: VersionedStreamTabletSnapshot = serde_json::from_slice(encoded)
            .map_err(|error| TabletError::Decoding(error.to_string()))?;
        if snapshot.format_version != STREAM_TABLET_SNAPSHOT_FORMAT_VERSION
            && snapshot.format_version != LEGACY_STREAM_TABLET_SNAPSHOT_FORMAT_VERSION
        {
            return Err(TabletError::InvalidCommand(format!(
                "unsupported Stream tablet snapshot version {}",
                snapshot.format_version
            )));
        }
        if snapshot.format_version == LEGACY_STREAM_TABLET_SNAPSHOT_FORMAT_VERSION
            && !snapshot.session_groups.is_empty()
        {
            return Err(TabletError::InvalidCommand(
                "legacy Stream tablet snapshot contains consumer-session state".into(),
            ));
        }
        if &snapshot.scope != expected_scope {
            return Err(TabletError::InvalidCommand(
                "Stream tablet snapshot scope is fenced".into(),
            ));
        }
        snapshot.scope.validate()?;
        if serde_json::to_vec(&snapshot)
            .map_err(|error| TabletError::Encoding(error.to_string()))?
            != encoded
        {
            return Err(TabletError::InvalidCommand(
                "Stream tablet snapshot is not canonical".into(),
            ));
        }
        let stream_bytes = STANDARD_NO_PAD
            .decode(&snapshot.stream_base64)
            .map_err(|error| TabletError::Decoding(error.to_string()))?;
        let stream = Stream::decode_snapshot(&stream_bytes)?;
        if stream.config().partitions != 1
            || stream.config().durability != DurabilityProfile::Volatile
        {
            return Err(TabletError::InvalidCommand(
                "Stream tablet snapshot engine configuration is invalid".into(),
            ));
        }
        if snapshot.consumer_groups.len() > MAX_STREAM_CONSUMER_GROUPS {
            return Err(TabletError::InvalidCommand(
                "Stream tablet snapshot exceeds the consumer-group bound".into(),
            ));
        }
        for (group, owner) in &snapshot.consumer_groups {
            validate_stream_consumer_group(group)?;
            stream_group::validate_bounded_identifier(
                "consumer member_id",
                &owner.member_id,
                MAX_STREAM_CONSUMER_MEMBER_BYTES,
            )?;
            if owner.generation == 0 {
                return Err(TabletError::InvalidCommand(
                    "Stream tablet snapshot has a zero group generation".into(),
                ));
            }
        }
        validate_session_group_count(snapshot.session_groups.len())?;
        for (group, session) in &snapshot.session_groups {
            validate_stream_consumer_group(group)?;
            session.validate()?;
        }

        let mut applied = BTreeMap::new();
        let mut previous_index = 0_u64;
        for entry in snapshot.applied {
            let metadata = entry.applied.metadata;
            if entry.proposal_id == 0
                || metadata.proposal_id != entry.proposal_id
                || metadata.term == 0
                || metadata.log_index <= previous_index
                || metadata.log_index > snapshot.last_applied_command_index
                || entry.applied.receipt.proposal_id() != entry.proposal_id
                || !stream_receipt_matches_metadata(
                    &entry.applied.receipt,
                    &metadata,
                    expected_scope,
                )
                || applied.insert(entry.proposal_id, entry.applied).is_some()
            {
                return Err(TabletError::InvalidCommand(
                    "Stream tablet snapshot retry registry is invalid".into(),
                ));
            }
            previous_index = metadata.log_index;
        }
        Ok(Self {
            scope: snapshot.scope,
            stream,
            consumer_groups: snapshot.consumer_groups,
            session_groups: snapshot.session_groups,
            applied,
            last_applied_command_index: snapshot.last_applied_command_index,
            state_digest: snapshot.state_digest,
        })
    }

    fn apply_append(
        &mut self,
        committed: CommittedCommand<'_>,
        applied_at_ms: u64,
        append: StreamAppendCommand,
        batch: Option<StreamTabletBatchReceipt>,
    ) -> TabletResult<StreamTabletAppendReceipt> {
        let appended =
            self.stream
                .append(append.envelope, Some(append.partition), applied_at_ms)?;
        Ok(self.append_receipt(committed, applied_at_ms, &appended, batch))
    }

    fn apply_batch(
        &mut self,
        committed: CommittedCommand<'_>,
        applied_at_ms: u64,
        batch: &StreamAppendBatchCommand,
    ) -> TabletResult<StreamTabletAppendReceipt> {
        let records = decode_stream_batch_payload(&batch.payload)?;
        // A cloned profile makes the whole batch visible atomically.
        let mut next_stream = self.stream.clone();
        let mut results = Vec::with_capacity(records.len());
        let mut first = None;
        for record in records {
            let appended =
                next_stream.append(record.envelope, Some(batch.partition), applied_at_ms)?;
            first.get_or_insert_with(|| appended.clone());
            results.push(StreamTabletBatchRecordReceipt {
                client_sequence: record.client_sequence,
                partition: appended.partition,
                offset: appended.offset,
                disposition: append_disposition(appended.acknowledgement.duplicate),
            });
        }
        let first = first.ok_or_else(|| {
            TabletError::InvalidCommand("Stream batch contains no records".into())
        })?;
        self.stream = next_stream;
        let batch_receipt = StreamTabletBatchReceipt {
            compression: batch.payload.compression,
            record_count: batch.payload.record_count,
            compressed_bytes: batch.payload.compressed_bytes,
            uncompressed_bytes: batch.payload.uncompressed_bytes,
            records: results,
        };
        Ok(self.append_receipt(committed, applied_at_ms, &first, Some(batch_receipt)))
    }

    fn append_receipt(
        &self,
        committed: CommittedCommand<'_>,
        applied_at_ms: u64,
        appended: &AppendReceipt,
        batch: Option<StreamTabletBatchReceipt>,
    ) -> StreamTabletAppendReceipt {
        let profile_deduplicated =
            is_profile_deduplicated(appended.acknowledgement.duplicate, batch.as_ref());
        StreamTabletAppendReceipt {
            proposal_id: committed.proposal_id,
            tablet_id: self.scope.tablet_id,
            tablet_epoch: self.scope.tablet_epoch,
            term: committed.term,
            commit_index: committed.log_index,
            partition: appended.partition,
            offset: appended.offset,
            applied_at_ms,
            write_evidence: StreamTabletWriteEvidence::FixedVoterMajorityPersisted,
            durable_voter_acks: 2,
            disposition: append_disposition(profile_deduplicated),
            batch,
        }
    }

    fn apply_group_offset(
        &mut self,
        committed: CommittedCommand<'_>,
        applied_at_ms: u64,
        command: StreamGroupOffsetCommand,
    ) -> TabletResult<StreamTabletGroupReceipt> {
        let (base_offset, end_offset) = self.stream.offsets(command.partition)?;
        let previous_offset = self
            .stream
            .lag(&command.group, command.partition)?
            .committed_offset;
        let rejection = group_rejection(
            &command,
            self.consumer_groups.get(&command.group),
            self.consumer_groups.len(),
            base_offset,
            previous_offset,
            end_offset,
        );

        if rejection.is_none() {
            let mut next_stream = self.stream.clone();
            match command.mode {
                StreamGroupOffsetMode::Commit => next_stream.commit_offset(
                    command.group.clone(),
                    command.partition,
                    command.next_offset,
                )?,
                StreamGroupOffsetMode::Reset => next_stream.reset_offset(
                    command.group.clone(),
                    command.partition,
                    command.next_offset,
                )?,
            }
            self.consumer_groups.insert(
                command.group.clone(),
                StreamConsumerGroupOwner {
                    member_id: command.member_id.clone(),
                    generation: command.group_generation,
                },
            );
            self.stream = next_stream;
        }

        let lag = self.stream.lag(&command.group, command.partition)?;
        Ok(StreamTabletGroupReceipt {
            proposal_id: committed.proposal_id,
            tablet_id: self.scope.tablet_id,
            tablet_epoch: self.scope.tablet_epoch,
            term: committed.term,
            commit_index: committed.log_index,
            group: command.group,
            member_id: command.member_id,
            group_generation: command.group_generation,
            partition: command.partition,
            mode: command.mode,
            requested_next_offset: command.next_offset,
            previous_offset,
            committed_offset: lag.committed_offset,
            end_offset: lag.end_offset,
            lag: lag.lag,
            applied_at_ms,
            outcome: if rejection.is_some() {
                StreamTabletGroupOutcome::Rejected
            } else {
                StreamTabletGroupOutcome::Applied
            },
            rejection,
            write_evidence: StreamTabletWriteEvidence::FixedVoterMajorityPersisted,
            durable_voter_acks: 2,
            disposition: StreamTabletGroupDisposition::New,
        })
    }

    fn apply_group_session(
        &mut self,
        committed: CommittedCommand<'_>,
        applied_at_ms: u64,
        command: &StreamGroupSessionCommand,
    ) -> StreamTabletSessionReceipt {
        let member_id = command.action.member_id().map(str::to_owned);
        let operation = command.action.operation();
        let existing = self.session_groups.get(&command.group).cloned();
        let group_exists = existing.is_some();
        let can_create = matches!(command.action, StreamGroupSessionAction::Join { .. });
        let capacity_rejection = existing.is_none()
            && can_create
            && self.session_groups.len() >= MAX_STREAM_CONSUMER_GROUPS;

        let (group_generation, watermark_ms, members, assigned_shards, transition) =
            if capacity_rejection {
                (
                    0,
                    0,
                    Vec::new(),
                    Vec::new(),
                    StreamSessionTransition {
                        outcome: StreamTabletSessionOutcome::Rejected,
                        rejection: Some(StreamTabletSessionRejection::GroupCapacityReached),
                        expired_members: Vec::new(),
                    },
                )
            } else if let Some(mut session) = existing.or_else(|| {
                can_create.then(|| StreamConsumerSessionGroup::new(command.shard_count))
            }) {
                let transition = session.apply(command, applied_at_ms);
                let assigned_shards = member_id
                    .as_deref()
                    .map_or_else(Vec::new, |member| session.assigned_shards(member));
                let members = session.members_with_assignments();
                let group_generation = session.generation;
                let watermark_ms = session.watermark_ms;
                if group_exists || session.generation > 0 {
                    self.session_groups.insert(command.group.clone(), session);
                }
                (
                    group_generation,
                    watermark_ms,
                    members,
                    assigned_shards,
                    transition,
                )
            } else {
                (
                    0,
                    0,
                    Vec::new(),
                    Vec::new(),
                    StreamSessionTransition {
                        outcome: StreamTabletSessionOutcome::Rejected,
                        rejection: Some(StreamTabletSessionRejection::UnknownGroup),
                        expired_members: Vec::new(),
                    },
                )
            };

        StreamTabletSessionReceipt {
            proposal_id: committed.proposal_id,
            tablet_id: self.scope.tablet_id,
            tablet_epoch: self.scope.tablet_epoch,
            term: committed.term,
            commit_index: committed.log_index,
            group: command.group.clone(),
            member_id,
            operation,
            shard_count: command.shard_count,
            group_generation,
            watermark_ms,
            members,
            assigned_shards,
            expired_members: transition.expired_members,
            outcome: transition.outcome,
            rejection: transition.rejection,
            write_evidence: StreamTabletWriteEvidence::FixedVoterMajorityPersisted,
            durable_voter_acks: 2,
            disposition: StreamTabletSessionDisposition::New,
        }
    }

    fn apply_retention(
        &mut self,
        committed: CommittedCommand<'_>,
        applied_at_ms: u64,
        command: &StreamRetentionCommand,
    ) -> TabletResult<StreamTabletRetentionReceipt> {
        let previous = self.retention_observation()?;
        let mut next_stream = self.stream.clone();
        let report = match (command.mode, command.policy) {
            (StreamTabletRetentionMode::Configure, Some(policy)) => {
                next_stream.configure_retention(policy, applied_at_ms)?
            }
            (StreamTabletRetentionMode::Maintain, None) => {
                next_stream.maintain_retention(applied_at_ms)?
            }
            _ => {
                return Err(TabletError::InvalidCommand(
                    "retention command mode and policy are inconsistent".into(),
                ));
            }
        };
        let partition = report.partitions.first().ok_or_else(|| {
            TabletError::InvalidCommand("Stream retention produced no partition evidence".into())
        })?;
        let receipt = StreamTabletRetentionReceipt {
            proposal_id: committed.proposal_id,
            tablet_id: self.scope.tablet_id,
            tablet_epoch: self.scope.tablet_epoch,
            term: committed.term,
            commit_index: committed.log_index,
            mode: command.mode,
            policy: next_stream.retention_policy(),
            cutoff_ms: report.cutoff_ms,
            previous_base_offset: previous.base_offset,
            base_offset: partition.base_offset,
            end_offset: partition.end_offset,
            removed_records: report.removed_records,
            removed_bytes: report.removed_bytes,
            retained_records: partition.retained_records,
            retained_bytes: partition.retained_bytes,
            applied_at_ms,
            write_evidence: StreamTabletWriteEvidence::FixedVoterMajorityPersisted,
            durable_voter_acks: 2,
            disposition: StreamTabletRetentionDisposition::New,
        };
        self.stream = next_stream;
        Ok(receipt)
    }

    fn validate_commit_scope(&self, committed: CommittedCommand<'_>) -> TabletResult<()> {
        validate_committed_command_scope(&self.scope, committed)
    }

    fn advance_digest(
        &mut self,
        committed: CommittedCommand<'_>,
        payload_digest: [u8; 32],
        receipt: &StreamTabletAppendReceipt,
    ) {
        let mut hasher = Sha256::new();
        hasher.update(b"epoch/stream-tablet/state-transition/v1\0");
        hasher.update(self.state_digest);
        hasher.update(committed.proposal_id.to_be_bytes());
        hasher.update(committed.term.to_be_bytes());
        hasher.update(committed.log_index.to_be_bytes());
        hasher.update(payload_digest);
        hasher.update(receipt.partition.to_be_bytes());
        hasher.update(receipt.offset.to_be_bytes());
        if let Some(batch) = &receipt.batch {
            hasher.update(b"epoch/stream-tablet/batch-receipt/v2\0");
            hasher.update(batch.record_count.to_be_bytes());
            hasher.update(batch.compressed_bytes.to_be_bytes());
            hasher.update(batch.uncompressed_bytes.to_be_bytes());
            for result in &batch.records {
                hasher.update(result.client_sequence.to_be_bytes());
                hasher.update(result.partition.to_be_bytes());
                hasher.update(result.offset.to_be_bytes());
                hasher.update([match result.disposition {
                    StreamTabletAppendDisposition::New => 0,
                    StreamTabletAppendDisposition::Replayed => 1,
                    StreamTabletAppendDisposition::ProfileDeduplicated => 2,
                }]);
            }
        }
        self.state_digest = hasher.finalize().into();
    }

    fn advance_group_digest(
        &mut self,
        committed: CommittedCommand<'_>,
        payload_digest: [u8; 32],
        receipt: &StreamTabletGroupReceipt,
    ) {
        let mut hasher = Sha256::new();
        hasher.update(b"epoch/stream-tablet/state-transition/group-offset/v3\0");
        hasher.update(self.state_digest);
        hasher.update(committed.proposal_id.to_be_bytes());
        hasher.update(committed.term.to_be_bytes());
        hasher.update(committed.log_index.to_be_bytes());
        hasher.update(payload_digest);
        hash_length_prefixed(&mut hasher, receipt.group.as_bytes());
        hash_length_prefixed(&mut hasher, receipt.member_id.as_bytes());
        hasher.update(receipt.group_generation.to_be_bytes());
        hasher.update(receipt.partition.to_be_bytes());
        hasher.update(receipt.committed_offset.to_be_bytes());
        hasher.update(receipt.end_offset.to_be_bytes());
        hasher.update([group_outcome_code(receipt)]);
        self.state_digest = hasher.finalize().into();
    }

    fn advance_retention_digest(
        &mut self,
        committed: CommittedCommand<'_>,
        payload_digest: [u8; 32],
        receipt: &StreamTabletRetentionReceipt,
    ) {
        let mut hasher = Sha256::new();
        hasher.update(b"epoch/stream-tablet/state-transition/retention/v4\0");
        hasher.update(self.state_digest);
        hasher.update(committed.proposal_id.to_be_bytes());
        hasher.update(committed.term.to_be_bytes());
        hasher.update(committed.log_index.to_be_bytes());
        hasher.update(payload_digest);
        hasher.update(receipt.previous_base_offset.to_be_bytes());
        hasher.update(receipt.base_offset.to_be_bytes());
        hasher.update(receipt.end_offset.to_be_bytes());
        hasher.update(receipt.removed_bytes.to_be_bytes());
        hasher.update(receipt.retained_bytes.to_be_bytes());
        self.state_digest = hasher.finalize().into();
    }

    fn advance_session_digest(
        &mut self,
        committed: CommittedCommand<'_>,
        payload_digest: [u8; 32],
        receipt: &StreamTabletSessionReceipt,
    ) {
        let mut hasher = Sha256::new();
        hasher.update(b"epoch/stream-tablet/state-transition/group-session/v5\0");
        hasher.update(self.state_digest);
        hasher.update(committed.proposal_id.to_be_bytes());
        hasher.update(committed.term.to_be_bytes());
        hasher.update(committed.log_index.to_be_bytes());
        hasher.update(payload_digest);
        hash_length_prefixed(&mut hasher, receipt.group.as_bytes());
        hash_length_prefixed(
            &mut hasher,
            receipt.member_id.as_deref().unwrap_or_default().as_bytes(),
        );
        hasher.update([session_operation_code(receipt.operation)]);
        hasher.update(receipt.shard_count.to_be_bytes());
        hasher.update(receipt.group_generation.to_be_bytes());
        hasher.update(receipt.watermark_ms.to_be_bytes());
        for member in &receipt.members {
            hash_length_prefixed(&mut hasher, member.member_id.as_bytes());
            hasher.update(member.session_timeout_ms.to_be_bytes());
            hasher.update(member.deadline_ms.to_be_bytes());
            for shard in &member.assigned_shards {
                hasher.update(shard.to_be_bytes());
            }
        }
        for member in &receipt.expired_members {
            hash_length_prefixed(&mut hasher, member.as_bytes());
        }
        hasher.update([session_outcome_code(receipt)]);
        self.state_digest = hasher.finalize().into();
    }
}

fn stream_receipt_matches_metadata(
    receipt: &StreamTabletMutationReceipt,
    metadata: &AppliedCommandMetadata,
    scope: &StreamTabletScope,
) -> bool {
    match receipt {
        StreamTabletMutationReceipt::Append(receipt) => {
            receipt.tablet_id == scope.tablet_id
                && receipt.tablet_epoch == scope.tablet_epoch
                && receipt.term == metadata.term
                && receipt.commit_index == metadata.log_index
        }
        StreamTabletMutationReceipt::Group(receipt) => {
            receipt.tablet_id == scope.tablet_id
                && receipt.tablet_epoch == scope.tablet_epoch
                && receipt.term == metadata.term
                && receipt.commit_index == metadata.log_index
        }
        StreamTabletMutationReceipt::Retention(receipt) => {
            receipt.tablet_id == scope.tablet_id
                && receipt.tablet_epoch == scope.tablet_epoch
                && receipt.term == metadata.term
                && receipt.commit_index == metadata.log_index
        }
        StreamTabletMutationReceipt::Session(receipt) => {
            receipt.tablet_id == scope.tablet_id
                && receipt.tablet_epoch == scope.tablet_epoch
                && receipt.term == metadata.term
                && receipt.commit_index == metadata.log_index
        }
    }
}

fn append_disposition(profile_deduplicated: bool) -> StreamTabletAppendDisposition {
    if profile_deduplicated {
        StreamTabletAppendDisposition::ProfileDeduplicated
    } else {
        StreamTabletAppendDisposition::New
    }
}

fn group_rejection(
    command: &StreamGroupOffsetCommand,
    owner: Option<&StreamConsumerGroupOwner>,
    group_count: usize,
    base_offset: u64,
    previous_offset: u64,
    end_offset: u64,
) -> Option<StreamTabletGroupRejection> {
    if let Some(rejection) = generation_rejection(command, owner) {
        return Some(rejection);
    }
    if owner.is_none() && group_count >= MAX_STREAM_CONSUMER_GROUPS {
        return Some(StreamTabletGroupRejection::GroupCapacityReached);
    }
    if command.next_offset < base_offset {
        return Some(StreamTabletGroupRejection::OffsetBeforeRetained);
    }
    if command.next_offset > end_offset {
        return Some(StreamTabletGroupRejection::OffsetBeyondEnd);
    }
    if command.mode == StreamGroupOffsetMode::Commit && command.next_offset < previous_offset {
        return Some(StreamTabletGroupRejection::CommitRewind);
    }
    None
}

fn generation_rejection(
    command: &StreamGroupOffsetCommand,
    owner: Option<&StreamConsumerGroupOwner>,
) -> Option<StreamTabletGroupRejection> {
    let Some(owner) = owner else {
        return (command.group_generation != 1)
            .then_some(StreamTabletGroupRejection::GenerationGap);
    };
    if command.group_generation < owner.generation {
        return Some(StreamTabletGroupRejection::StaleGeneration);
    }
    if command.group_generation == owner.generation && command.member_id != owner.member_id {
        return Some(StreamTabletGroupRejection::OwnerMismatch);
    }
    if command.group_generation > owner.generation.saturating_add(1) {
        return Some(StreamTabletGroupRejection::GenerationGap);
    }
    None
}

fn group_outcome_code(receipt: &StreamTabletGroupReceipt) -> u8 {
    match receipt.rejection {
        None => 0,
        Some(StreamTabletGroupRejection::OwnerMismatch) => 1,
        Some(StreamTabletGroupRejection::StaleGeneration) => 2,
        Some(StreamTabletGroupRejection::GenerationGap) => 3,
        Some(StreamTabletGroupRejection::CommitRewind) => 4,
        Some(StreamTabletGroupRejection::OffsetBeforeRetained) => 5,
        Some(StreamTabletGroupRejection::OffsetBeyondEnd) => 6,
        Some(StreamTabletGroupRejection::GroupCapacityReached) => 7,
    }
}

const fn session_operation_code(operation: StreamGroupSessionOperation) -> u8 {
    match operation {
        StreamGroupSessionOperation::Join => 0,
        StreamGroupSessionOperation::Heartbeat => 1,
        StreamGroupSessionOperation::Leave => 2,
        StreamGroupSessionOperation::Maintain => 3,
    }
}

fn session_outcome_code(receipt: &StreamTabletSessionReceipt) -> u8 {
    match receipt.rejection {
        None => 0,
        Some(StreamTabletSessionRejection::UnknownGroup) => 1,
        Some(StreamTabletSessionRejection::UnknownMember) => 2,
        Some(StreamTabletSessionRejection::StaleGeneration) => 3,
        Some(StreamTabletSessionRejection::ShardCountMismatch) => 4,
        Some(StreamTabletSessionRejection::GroupCapacityReached) => 5,
        Some(StreamTabletSessionRejection::MemberCapacityReached) => 6,
        Some(StreamTabletSessionRejection::DeadlineOverflow) => 7,
    }
}

fn is_profile_deduplicated(
    first_record_is_duplicate: bool,
    batch: Option<&StreamTabletBatchReceipt>,
) -> bool {
    batch.map_or(first_record_is_duplicate, |batch| {
        batch
            .records
            .iter()
            .all(|result| result.disposition == StreamTabletAppendDisposition::ProfileDeduplicated)
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn scope() -> StreamTabletScope {
        StreamTabletScope::new(7, 3, "orders").unwrap()
    }

    fn event(id: &str) -> EventEnvelope {
        let mut envelope = EventEnvelope::new("tests", "order.created", json!({"id": id}), 10);
        envelope.id = id.into();
        envelope
    }

    fn encoded(key: &str, id: &str, applied_at_ms: u64) -> (u64, Vec<u8>) {
        let scope = scope();
        let command = StreamTabletCommand::append(&scope, key, event(id), applied_at_ms).unwrap();
        let proposal_id = command.proposal_id(&scope).unwrap();
        (proposal_id, command.encode(&scope).unwrap())
    }

    fn committed(
        proposal_id: u64,
        term: u64,
        log_index: u64,
        payload: &[u8],
    ) -> CommittedCommand<'_> {
        CommittedCommand {
            group_id: 7,
            group_epoch: 3,
            proposal_id,
            term,
            log_index,
            payload,
        }
    }

    #[test]
    fn command_codec_is_versioned_bounded_and_strict() {
        let (_, valid) = encoded("request-1", "one", 11);
        let decoded = StreamTabletCommand::decode(&valid, &scope()).unwrap();
        assert_eq!(decoded.format_version, STREAM_TABLET_COMMAND_FORMAT_VERSION);

        let mut document: Value = serde_json::from_slice(&valid).unwrap();
        document["format_version"] = json!(99);
        assert!(matches!(
            StreamTabletCommand::decode(&serde_json::to_vec(&document).unwrap(), &scope()),
            Err(TabletError::InvalidCommand(_))
        ));

        document["format_version"] = json!(1);
        document["unknown"] = json!(true);
        assert!(matches!(
            StreamTabletCommand::decode(&serde_json::to_vec(&document).unwrap(), &scope()),
            Err(TabletError::Decoding(_))
        ));

        assert!(matches!(
            StreamTabletCommand::decode(&vec![b'x'; MAX_STREAM_TABLET_COMMAND_BYTES + 1], &scope()),
            Err(TabletError::InvalidCommand(_))
        ));
    }

    #[test]
    fn wrong_scope_and_nonzero_partition_fail_before_application() {
        let (_, valid) = encoded("request-1", "one", 11);
        let wrong_group = StreamTabletScope::new(8, 3, "orders").unwrap();
        assert!(matches!(
            StreamTabletCommand::decode(&valid, &wrong_group),
            Err(TabletError::GroupMismatch { .. })
        ));

        let mut document: Value = serde_json::from_slice(&valid).unwrap();
        document["operation"]["partition"] = json!(1);
        assert!(matches!(
            StreamTabletCommand::decode(&serde_json::to_vec(&document).unwrap(), &scope()),
            Err(TabletError::InvalidCommand(_))
        ));
    }

    #[test]
    fn committed_history_replays_identically_on_every_voter() {
        let histories = [
            encoded("request-1", "one", 11),
            encoded("request-2", "two", 12),
            encoded("request-3", "three", 13),
        ];
        let mut tablets = [
            StreamTablet::new(scope()).unwrap(),
            StreamTablet::new(scope()).unwrap(),
            StreamTablet::new(scope()).unwrap(),
        ];
        for tablet in &mut tablets {
            for (position, (proposal_id, payload)) in histories.iter().enumerate() {
                tablet
                    .apply(committed(
                        *proposal_id,
                        2,
                        u64::try_from(position).unwrap() + 4,
                        payload,
                    ))
                    .unwrap();
            }
        }
        let expected_records = tablets[0].fetch(0, 10).unwrap();
        let expected_digest = tablets[0].state_digest();
        for tablet in &tablets[1..] {
            assert_eq!(tablet.fetch(0, 10).unwrap(), expected_records);
            assert_eq!(tablet.state_digest(), expected_digest);
        }
        assert_eq!(
            expected_records
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
    }

    #[test]
    fn exact_reapplication_returns_original_offset_without_mutating_state() {
        let (proposal_id, payload) = encoded("request-1", "one", 11);
        let commit = committed(proposal_id, 2, 4, &payload);
        let mut tablet = StreamTablet::new(scope()).unwrap();
        let original = tablet.apply(commit).unwrap();
        let digest = tablet.state_digest();
        let duplicate = tablet.apply(commit).unwrap();
        assert_eq!(duplicate.offset, original.offset);
        assert_eq!(
            duplicate.disposition,
            StreamTabletAppendDisposition::Replayed
        );
        assert_eq!(tablet.applied_command_count(), 1);
        assert_eq!(tablet.fetch(0, 10).unwrap().len(), 1);
        assert_eq!(tablet.state_digest(), digest);
        assert_eq!(
            digest,
            [
                0xc1, 0x30, 0xe8, 0x46, 0x59, 0x49, 0xd7, 0x2c, 0x4d, 0x37, 0x4d, 0x05, 0xa3, 0xb7,
                0xb2, 0x00, 0xa5, 0x85, 0x3d, 0x7c, 0xdf, 0x34, 0x55, 0xe4, 0xd6, 0xc3, 0x5a, 0x29,
                0x4f, 0x18, 0x39, 0x5f,
            ]
        );
    }

    #[test]
    fn native_snapshot_restores_business_state_and_only_the_retained_retry_suffix() {
        let (first_id, first_payload) = encoded("request-1", "one", 11);
        let (second_id, second_payload) = encoded("request-2", "two", 12);
        let mut live = StreamTablet::new(scope()).unwrap();
        live.apply(committed(first_id, 2, 4, &first_payload))
            .unwrap();
        live.apply(committed(second_id, 2, 5, &second_payload))
            .unwrap();
        let expected_records = live.fetch(0, 10).unwrap();
        let expected_digest = live.state_digest();

        let snapshot = live.encode_snapshot(&BTreeSet::from([second_id])).unwrap();
        let mut restored = StreamTablet::decode_snapshot(&scope(), &snapshot).unwrap();

        assert_eq!(restored.fetch(0, 10).unwrap(), expected_records);
        assert_eq!(restored.state_digest(), expected_digest);
        assert_eq!(restored.last_applied_command_index(), 5);
        assert_eq!(restored.applied_command_count(), 1);
        assert!(
            restored
                .mutation_receipt_for_committed(committed(first_id, 2, 4, &first_payload))
                .unwrap()
                .is_none()
        );
        assert!(
            restored
                .mutation_receipt_for_committed(committed(second_id, 2, 5, &second_payload))
                .unwrap()
                .is_some()
        );

        let (third_id, third_payload) = encoded("request-3", "three", 13);
        restored
            .apply(committed(third_id, 2, 6, &third_payload))
            .unwrap();
        assert_eq!(restored.fetch(0, 10).unwrap().len(), 3);
    }

    #[test]
    fn native_snapshot_rejects_noncanonical_or_foreign_images() {
        let (proposal_id, payload) = encoded("request-1", "one", 11);
        let mut live = StreamTablet::new(scope()).unwrap();
        live.apply(committed(proposal_id, 2, 4, &payload)).unwrap();
        let encoded = live
            .encode_snapshot(&BTreeSet::from([proposal_id]))
            .unwrap();
        let pretty = serde_json::to_vec_pretty(
            &serde_json::from_slice::<VersionedStreamTabletSnapshot>(&encoded).unwrap(),
        )
        .unwrap();

        assert!(StreamTablet::decode_snapshot(&scope(), &pretty).is_err());
        assert!(
            StreamTablet::decode_snapshot(
                &StreamTabletScope::new(8, 3, "orders").unwrap(),
                &encoded,
            )
            .is_err()
        );
    }

    #[test]
    fn receipt_json_uses_browser_safe_ids_and_bounded_fixed_voter_evidence() {
        let (proposal_id, payload) = encoded("request-1", "one", 11);
        let mut tablet = StreamTablet::new(scope()).unwrap();
        let receipt = tablet
            .apply(committed(proposal_id, 2, 4, &payload))
            .unwrap();
        let document = serde_json::to_value(receipt).unwrap();

        assert_eq!(document["proposal_id"], proposal_id.to_string());
        assert_eq!(document["tablet_id"], "7");
        assert_eq!(document["tablet_epoch"], "3");
        assert_eq!(document["term"], "2");
        assert_eq!(document["commit_index"], "4");
        assert_eq!(document["offset"], "0");
        assert_eq!(document["applied_at_ms"], "11");
        assert_eq!(document["write_evidence"], "fixed_voter_majority_persisted");
        assert_eq!(document["durable_voter_acks"], 2);
        assert!(document.get("configured_durability").is_none());
        assert!(document.get("achieved_durability").is_none());
    }

    #[test]
    fn a_conflicting_payload_or_out_of_order_commit_fails_closed() {
        let (proposal_id, payload) = encoded("request-1", "one", 11);
        let (_, conflicting_payload) = encoded("request-1", "different", 11);
        let mut tablet = StreamTablet::new(scope()).unwrap();
        tablet
            .apply(committed(proposal_id, 2, 4, &payload))
            .unwrap();
        assert!(matches!(
            tablet.apply(committed(proposal_id, 2, 4, &conflicting_payload)),
            Err(TabletError::ConflictingCommand { .. })
        ));
        assert!(matches!(
            tablet.apply(committed(proposal_id, 3, 4, &payload)),
            Err(TabletError::ConflictingCommand { .. })
        ));
        assert!(matches!(
            tablet.apply(committed(proposal_id, 2, 5, &payload)),
            Err(TabletError::ConflictingCommand { .. })
        ));

        let (next_id, next_payload) = encoded("request-2", "two", 12);
        assert!(matches!(
            tablet.apply(committed(next_id, 2, 3, &next_payload)),
            Err(TabletError::CommitOrder { .. })
        ));
        assert_eq!(tablet.fetch(0, 10).unwrap().len(), 1);
    }

    #[test]
    fn proposal_id_is_stable_and_scope_separated() {
        let scope = scope();
        let first = proposal_id_for(&scope, "request-1").unwrap();
        assert_eq!(first, 298_544_817_787_184_225);
        assert_eq!(first, proposal_id_for(&scope, "request-1").unwrap());
        assert_ne!(first, proposal_id_for(&scope, "request-2").unwrap());
        assert_ne!(
            first,
            proposal_id_for(
                &StreamTabletScope::new(7, 4, "orders").unwrap(),
                "request-1"
            )
            .unwrap()
        );
    }

    #[test]
    fn command_encoding_has_a_golden_canonical_vector() {
        let scope = scope();
        let command = StreamTabletCommand::append(&scope, "request-1", event("one"), 11).unwrap();
        let encoded = command.encode(&scope).unwrap();
        assert_eq!(
            String::from_utf8(encoded).unwrap(),
            r#"{"format_version":1,"tablet_id":7,"tablet_epoch":3,"resource":"orders","idempotency_key":"request-1","applied_at_ms":11,"operation":{"kind":"append","partition":0,"envelope":{"id":"one","source":"tests","type":"order.created","time_ms":10,"headers":{},"content_type":"application/json","payload":{"id":"one"},"priority":0,"extensions":{}}}}"#
        );

        let pretty = serde_json::to_vec_pretty(&command).unwrap();
        assert!(matches!(
            StreamTabletCommand::decode(&pretty, &scope),
            Err(TabletError::Decoding(_))
        ));
    }

    #[test]
    fn consumer_group_capacity_rejects_only_a_new_group() {
        let command = StreamGroupOffsetCommand {
            group: "billing".into(),
            member_id: "worker-a".into(),
            group_generation: 1,
            partition: 0,
            next_offset: 0,
            mode: StreamGroupOffsetMode::Commit,
        };
        assert_eq!(
            group_rejection(&command, None, MAX_STREAM_CONSUMER_GROUPS, 0, 0, 0,),
            Some(StreamTabletGroupRejection::GroupCapacityReached)
        );
        assert_eq!(
            group_rejection(
                &command,
                Some(&StreamConsumerGroupOwner {
                    member_id: "worker-a".into(),
                    generation: 1,
                }),
                MAX_STREAM_CONSUMER_GROUPS,
                0,
                0,
                0,
            ),
            None
        );
    }
}
