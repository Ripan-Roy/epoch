//! Typed, deterministic state machines applied only after consensus commit.
//!
//! The first slice is deliberately narrow: one configured, single-partition
//! Stream tablet. It owns command validation, deterministic application,
//! idempotency, and restart replay while the node owns transport and Raft.

mod common;

use std::collections::BTreeMap;

use common::{
    AppliedCommand, hash_length_prefixed, proposal_id_from_domain, serialize_u64_as_decimal,
    validate_committed_command_scope, validate_idempotency_key,
};
use epoch_core::{DurabilityProfile, EventEnvelope};
use epoch_stream::{Stream, StreamConfig, StreamRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use common::{
    AppliedCommandMetadata, CommittedCommand, MAX_IDEMPOTENCY_KEY_BYTES, StreamTabletScope,
    TabletError, TabletResult, TabletScope, TabletWriteEvidence as StreamTabletWriteEvidence,
    TabletWriteEvidence,
};

pub const STREAM_TABLET_COMMAND_FORMAT_VERSION: u16 = 1;
// Kept equal to the current consensus proposal ceiling. The state-machine
// boundary repeats the check so a command can never validate here and then be
// rejected only after it reaches Raft.
pub const MAX_STREAM_TABLET_COMMAND_BYTES: usize = 512 * 1024;

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
pub enum StreamTabletOperation {
    Append(StreamAppendCommand),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamAppendCommand {
    pub partition: u32,
    pub envelope: EventEnvelope,
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
            return Err(TabletError::Decoding(
                "command bytes are not in canonical v1 encoding".into(),
            ));
        }
        Ok(command)
    }

    pub fn proposal_id(&self, scope: &StreamTabletScope) -> TabletResult<u64> {
        self.validate(scope)?;
        proposal_id_for(scope, &self.idempotency_key)
    }

    fn validate(&self, scope: &StreamTabletScope) -> TabletResult<()> {
        scope.validate()?;
        if self.format_version != STREAM_TABLET_COMMAND_FORMAT_VERSION {
            return Err(TabletError::InvalidCommand(format!(
                "unsupported format_version {}",
                self.format_version
            )));
        }
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
                if append.partition != 0 {
                    return Err(TabletError::InvalidCommand(
                        "the first Stream tablet slice supports only partition 0".into(),
                    ));
                }
                append.envelope.validate()?;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamTabletAppendReceipt {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub proposal_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub tablet_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub tablet_epoch: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub term: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub commit_index: u64,
    pub partition: u32,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub offset: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub applied_at_ms: u64,
    pub write_evidence: StreamTabletWriteEvidence,
    pub durable_voter_acks: u16,
    pub disposition: StreamTabletAppendDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletAppendDisposition {
    New,
    Replayed,
    ProfileDeduplicated,
}

#[derive(Debug)]
pub struct StreamTablet {
    scope: StreamTabletScope,
    stream: Stream,
    applied: BTreeMap<u64, AppliedCommand<StreamTabletAppendReceipt>>,
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
        self.validate_commit_scope(committed)?;
        let metadata = AppliedCommandMetadata::from_committed(committed);
        if let Some(mut receipt) = self.receipt_for_committed(committed)? {
            receipt.disposition = StreamTabletAppendDisposition::Replayed;
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

        let StreamTabletOperation::Append(append) = command.operation;
        let appended = self.stream.append(
            append.envelope,
            Some(append.partition),
            command.applied_at_ms,
        )?;
        let receipt = StreamTabletAppendReceipt {
            proposal_id: committed.proposal_id,
            tablet_id: self.scope.tablet_id,
            tablet_epoch: self.scope.tablet_epoch,
            term: committed.term,
            commit_index: committed.log_index,
            partition: appended.partition,
            offset: appended.offset,
            applied_at_ms: command.applied_at_ms,
            write_evidence: StreamTabletWriteEvidence::FixedVoterMajorityPersisted,
            durable_voter_acks: 2,
            disposition: if appended.acknowledgement.duplicate {
                StreamTabletAppendDisposition::ProfileDeduplicated
            } else {
                StreamTabletAppendDisposition::New
            },
        };
        self.advance_digest(committed, metadata.payload_digest, &receipt);
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
            .map(|applied| applied.receipt.clone())
    }

    /// Returns the actor-applied receipt only when the consensus commit exactly
    /// matches the already-applied command metadata.
    pub fn receipt_for_committed(
        &self,
        committed: CommittedCommand<'_>,
    ) -> TabletResult<Option<StreamTabletAppendReceipt>> {
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
        self.state_digest = hasher.finalize().into();
    }
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
}
