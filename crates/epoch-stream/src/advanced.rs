//! Bounded state services layered over the ordered Stream log.
//!
//! These services deliberately keep every correctness decision deterministic
//! and snapshot-compatible. External object stores, push transports, and
//! cross-region workers mirror or drive this state; they do not become an
//! unrecorded source of truth.

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{EpochError, EpochResult, EventEnvelope};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{AppendReceipt, Stream, StreamRecord, retained_record_bytes};

pub const STREAM_STATE_SERVICES_SNAPSHOT_VERSION: u16 = 2;
const LEGACY_STREAM_STATE_SERVICES_SNAPSHOT_VERSION: u16 = 1;
pub const MAX_STREAM_STATE_SERVICES_SNAPSHOT_BYTES: usize = 1024 * 1024;
pub const MAX_STREAM_PRODUCERS: usize = 4_096;
pub const MAX_STREAM_PRODUCER_HISTORY: usize = 256;
pub const MAX_STREAM_TRANSACTIONS: usize = 1_024;
pub const MAX_STREAM_TRANSACTION_RECORDS: usize = 128;
pub const MAX_STREAM_TIER_OBJECTS_PER_PARTITION: usize = 64;
pub const MAX_STREAM_TIER_OBJECT_BYTES: usize = 512 * 1024;
pub const MAX_STREAM_CAPTURE_ARTIFACTS: usize = 32;
pub const MAX_STREAM_CAPTURE_BYTES: usize = 512 * 1024;
pub const MAX_STREAM_CAPTURE_SCHEDULES: usize = 32;
pub const MIN_STREAM_CAPTURE_INTERVAL_MS: u64 = 1_000;
pub const MAX_STREAM_CAPTURE_INTERVAL_MS: u64 = 31 * 24 * 60 * 60 * 1_000;
pub const MAX_STREAM_REPLICATION_SOURCES: usize = 128;
pub const MAX_STREAM_REPLICATION_BATCH_RECORDS: usize = 128;
pub const MAX_STREAM_CLUSTER_ID_BYTES: usize = 128;
pub const MAX_STREAM_IDENTIFIER_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTransactionStatus {
    Pending,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamReadIsolation {
    ReadUncommitted,
    ReadCommitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamProducerDisposition {
    New,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamPosition {
    pub partition: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamProducerAppendOutcome {
    pub producer_id: String,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub producer_epoch: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub sequence: u64,
    pub disposition: StreamProducerDisposition,
    pub positions: Vec<StreamPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProducerSequenceReceipt {
    digest: [u8; 32],
    positions: Vec<StreamPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProducerState {
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    epoch: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    next_sequence: u64,
    history: BTreeMap<u64, ProducerSequenceReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamOffsetCommit {
    pub group: String,
    pub partition: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub next_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamTransactionObservation {
    pub transaction_id: String,
    pub producer_id: String,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub producer_epoch: u64,
    pub status: StreamTransactionStatus,
    pub positions: Vec<StreamPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_commit: Option<StreamOffsetCommit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransactionState {
    producer_id: String,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    producer_epoch: u64,
    status: StreamTransactionStatus,
    positions: Vec<StreamPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    offset_commit: Option<StreamOffsetCommit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamCompactionReport {
    pub partition: u32,
    pub removed_records: usize,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub removed_bytes: u64,
    pub retained_records: usize,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub retained_bytes: u64,
    pub removed_tombstones: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamTierObject {
    pub object_id: String,
    pub partition: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub first_offset: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub end_offset: u64,
    pub record_count: usize,
    pub encoded_bytes: Vec<u8>,
    pub checksum_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamCaptureFormat {
    JsonLines,
    JsonArray,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamCaptureArtifact {
    pub capture_id: String,
    pub partition: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub first_offset: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub end_offset: u64,
    pub record_count: usize,
    pub format: StreamCaptureFormat,
    pub encoded_bytes: Vec<u8>,
    pub checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamCaptureScheduleObservation {
    pub schedule_id: String,
    pub partition: u32,
    pub format: StreamCaptureFormat,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub interval_ms: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub next_offset: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub next_capture_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamCaptureMaintenanceOutcome {
    pub schedule: StreamCaptureScheduleObservation,
    pub artifact: StreamCaptureArtifact,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub skipped_retained_offsets: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamCaptureSchedule {
    partition: u32,
    format: StreamCaptureFormat,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    interval_ms: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    next_offset: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    next_capture_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamReplicationRecord {
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub source_offset: u64,
    pub envelope: EventEnvelope,
    #[serde(default)]
    pub traversed_clusters: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamReplicationBatch {
    pub source_cluster: String,
    pub source_stream: String,
    pub source_partition: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub first_source_offset: u64,
    pub records: Vec<StreamReplicationRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamReplicationMapping {
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub source_offset: u64,
    pub local_partition: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub local_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamReplicationOutcome {
    pub source_cluster: String,
    pub source_stream: String,
    pub source_partition: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub next_source_offset: u64,
    pub duplicate: bool,
    pub mappings: Vec<StreamReplicationMapping>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicationCheckpoint {
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    next_source_offset: u64,
    last_batch_digest: [u8; 32],
    last_outcome: StreamReplicationOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamPartitionAdvice {
    pub current_partitions: u32,
    pub recommended_partitions: u32,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub observed_records: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub observed_bytes: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub target_records_per_partition: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub target_bytes_per_partition: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuperstreamRecord {
    pub member: String,
    pub record: StreamRecord,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedStateServicesSnapshot {
    format_version: u16,
    state: StreamStateServices,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamStateServices {
    local_cluster_id: String,
    producers: BTreeMap<String, ProducerState>,
    transactions: BTreeMap<String, TransactionState>,
    tier_objects: BTreeMap<u32, Vec<StreamTierObject>>,
    captures: BTreeMap<String, StreamCaptureArtifact>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    capture_schedules: BTreeMap<String, StreamCaptureSchedule>,
    replication: BTreeMap<String, ReplicationCheckpoint>,
}

impl StreamStateServices {
    pub fn new(local_cluster_id: impl Into<String>) -> EpochResult<Self> {
        let local_cluster_id = local_cluster_id.into();
        validate_identifier(
            "local cluster ID",
            &local_cluster_id,
            MAX_STREAM_CLUSTER_ID_BYTES,
        )?;
        Ok(Self {
            local_cluster_id,
            producers: BTreeMap::new(),
            transactions: BTreeMap::new(),
            tier_objects: BTreeMap::new(),
            captures: BTreeMap::new(),
            capture_schedules: BTreeMap::new(),
            replication: BTreeMap::new(),
        })
    }

    pub fn local_cluster_id(&self) -> &str {
        &self.local_cluster_id
    }

    pub fn producer_count(&self) -> usize {
        self.producers.len()
    }

    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    pub fn transaction(&self, transaction_id: &str) -> Option<StreamTransactionObservation> {
        self.transactions
            .get(transaction_id)
            .map(|transaction| StreamTransactionObservation {
                transaction_id: transaction_id.to_owned(),
                producer_id: transaction.producer_id.clone(),
                producer_epoch: transaction.producer_epoch,
                status: transaction.status,
                positions: transaction.positions.clone(),
                offset_commit: transaction.offset_commit.clone(),
            })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the producer identity, sequence, target, payload, and logical clock are separate protocol fields"
    )]
    pub fn append_idempotent(
        &mut self,
        stream: &mut Stream,
        producer_id: &str,
        producer_epoch: u64,
        sequence: u64,
        partition: u32,
        envelope: EventEnvelope,
        now_ms: u64,
    ) -> EpochResult<StreamProducerAppendOutcome> {
        if envelope.transaction_id.is_some() {
            return Err(EpochError::InvalidArgument(
                "transactional envelopes must use append_transaction".into(),
            ));
        }
        let digest = digest_json(&(partition, &envelope, Option::<&str>::None))?;
        let mut next_stream = stream.clone();
        let mut next = self.clone();
        let outcome = next.apply_sequence(
            &mut next_stream,
            producer_id,
            producer_epoch,
            sequence,
            digest,
            None,
            vec![envelope],
            partition,
            now_ms,
        )?;
        *stream = next_stream;
        *self = next;
        Ok(outcome)
    }

    pub fn begin_transaction(
        &mut self,
        transaction_id: &str,
        producer_id: &str,
        producer_epoch: u64,
    ) -> EpochResult<StreamTransactionObservation> {
        validate_identifier(
            "transaction ID",
            transaction_id,
            MAX_STREAM_IDENTIFIER_BYTES,
        )?;
        validate_identifier("producer ID", producer_id, MAX_STREAM_IDENTIFIER_BYTES)?;
        validate_epoch(producer_epoch)?;
        if let Some(existing) = self.transactions.get(transaction_id) {
            if existing.producer_id == producer_id && existing.producer_epoch == producer_epoch {
                return self.transaction(transaction_id).ok_or_else(|| {
                    EpochError::Internal("transaction disappeared during replay".into())
                });
            }
            return Err(EpochError::Conflict(
                "transaction ID is already bound to another producer epoch".into(),
            ));
        }
        if self.transactions.len() >= MAX_STREAM_TRANSACTIONS {
            return Err(EpochError::Capacity(format!(
                "Stream supports at most {MAX_STREAM_TRANSACTIONS} retained transactions"
            )));
        }
        self.ensure_producer_epoch(producer_id, producer_epoch)?;
        self.transactions.insert(
            transaction_id.to_owned(),
            TransactionState {
                producer_id: producer_id.to_owned(),
                producer_epoch,
                status: StreamTransactionStatus::Pending,
                positions: Vec::new(),
                offset_commit: None,
            },
        );
        self.transaction(transaction_id)
            .ok_or_else(|| EpochError::Internal("transaction was not retained".into()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_transaction(
        &mut self,
        stream: &mut Stream,
        transaction_id: &str,
        producer_id: &str,
        producer_epoch: u64,
        sequence: u64,
        partition: u32,
        mut envelopes: Vec<EventEnvelope>,
        now_ms: u64,
    ) -> EpochResult<StreamProducerAppendOutcome> {
        if envelopes.is_empty() || envelopes.len() > MAX_STREAM_TRANSACTION_RECORDS {
            return Err(EpochError::InvalidArgument(format!(
                "transaction append must contain between 1 and {MAX_STREAM_TRANSACTION_RECORDS} records"
            )));
        }
        let transaction = self
            .transactions
            .get(transaction_id)
            .ok_or_else(|| EpochError::NotFound(format!("Stream transaction {transaction_id}")))?;
        if transaction.status != StreamTransactionStatus::Pending
            || transaction.producer_id != producer_id
            || transaction.producer_epoch != producer_epoch
        {
            return Err(EpochError::Fenced);
        }
        if transaction.positions.len().saturating_add(envelopes.len())
            > MAX_STREAM_TRANSACTION_RECORDS
        {
            return Err(EpochError::Capacity(format!(
                "transaction exceeds {MAX_STREAM_TRANSACTION_RECORDS} records"
            )));
        }
        for envelope in &mut envelopes {
            if envelope
                .transaction_id
                .as_deref()
                .is_some_and(|observed| observed != transaction_id)
            {
                return Err(EpochError::Conflict(
                    "envelope transaction ID does not match the command".into(),
                ));
            }
            envelope.transaction_id = Some(transaction_id.to_owned());
        }
        let digest = digest_json(&(partition, &envelopes, transaction_id))?;
        let mut next_stream = stream.clone();
        let mut next = self.clone();
        let outcome = next.apply_sequence(
            &mut next_stream,
            producer_id,
            producer_epoch,
            sequence,
            digest,
            Some(transaction_id.to_owned()),
            envelopes,
            partition,
            now_ms,
        )?;
        if outcome.disposition == StreamProducerDisposition::New {
            next.transactions
                .get_mut(transaction_id)
                .ok_or_else(|| EpochError::Internal("transaction disappeared".into()))?
                .positions
                .extend(outcome.positions.iter().cloned());
        }
        *stream = next_stream;
        *self = next;
        Ok(outcome)
    }

    pub fn commit_transaction(
        &mut self,
        stream: &mut Stream,
        transaction_id: &str,
        offset_commit: Option<StreamOffsetCommit>,
    ) -> EpochResult<StreamTransactionObservation> {
        let mut next_stream = stream.clone();
        let mut next = self.clone();
        let transaction = next
            .transactions
            .get_mut(transaction_id)
            .ok_or_else(|| EpochError::NotFound(format!("Stream transaction {transaction_id}")))?;
        match transaction.status {
            StreamTransactionStatus::Committed => {
                if transaction.offset_commit != offset_commit {
                    return Err(EpochError::Conflict(
                        "committed transaction is bound to another offset checkpoint".into(),
                    ));
                }
            }
            StreamTransactionStatus::Aborted => {
                return Err(EpochError::Conflict(
                    "aborted transaction cannot be committed".into(),
                ));
            }
            StreamTransactionStatus::Pending => {
                if let Some(checkpoint) = &offset_commit {
                    validate_identifier(
                        "consumer group",
                        &checkpoint.group,
                        MAX_STREAM_IDENTIFIER_BYTES,
                    )?;
                    next_stream.commit_offset(
                        checkpoint.group.clone(),
                        checkpoint.partition,
                        checkpoint.next_offset,
                    )?;
                }
                transaction.offset_commit = offset_commit;
                transaction.status = StreamTransactionStatus::Committed;
            }
        }
        *stream = next_stream;
        *self = next;
        self.transaction(transaction_id)
            .ok_or_else(|| EpochError::Internal("transaction disappeared".into()))
    }

    pub fn abort_transaction(
        &mut self,
        transaction_id: &str,
    ) -> EpochResult<StreamTransactionObservation> {
        let transaction = self
            .transactions
            .get_mut(transaction_id)
            .ok_or_else(|| EpochError::NotFound(format!("Stream transaction {transaction_id}")))?;
        match transaction.status {
            StreamTransactionStatus::Pending => {
                transaction.status = StreamTransactionStatus::Aborted;
            }
            StreamTransactionStatus::Aborted => {}
            StreamTransactionStatus::Committed => {
                return Err(EpochError::Conflict(
                    "committed transaction cannot be aborted".into(),
                ));
            }
        }
        self.transaction(transaction_id)
            .ok_or_else(|| EpochError::Internal("transaction disappeared".into()))
    }

    pub fn fetch(
        &self,
        stream: &Stream,
        partition: u32,
        offset: u64,
        limit: usize,
        isolation: StreamReadIsolation,
    ) -> EpochResult<Vec<StreamRecord>> {
        if limit == 0 {
            return Err(EpochError::InvalidArgument(
                "Stream fetch limit must be non-zero".into(),
            ));
        }
        let hot = stream.partition(partition)?;
        let earliest_available_offset = self.earliest_available_offset(partition, hot.base_offset);
        if offset < earliest_available_offset {
            return Err(EpochError::Conflict(format!(
                "offset {offset} was removed by retention; earliest is {earliest_available_offset}"
            )));
        }
        let mut records = Vec::new();
        if let Some(objects) = self.tier_objects.get(&partition) {
            for object in objects {
                records.extend(decode_tier_object(object)?);
            }
        }
        records.extend(hot.records.iter().cloned());
        records.sort_by_key(|record| record.offset);
        records.dedup_by_key(|record| record.offset);
        Ok(records
            .into_iter()
            .filter(|record| record.offset >= offset)
            .filter(|record| self.visible(record, isolation))
            .take(limit)
            .collect())
    }

    pub fn compact(
        &mut self,
        stream: &mut Stream,
        partition: u32,
        now_ms: u64,
        tombstone_retention_ms: u64,
    ) -> EpochResult<StreamCompactionReport> {
        if tombstone_retention_ms == 0 {
            return Err(EpochError::InvalidArgument(
                "tombstone retention must be non-zero".into(),
            ));
        }
        if self
            .tier_objects
            .get(&partition)
            .is_some_and(|objects| !objects.is_empty())
        {
            return Err(EpochError::Conflict(
                "immutable tier objects must be compacted before they are tiered".into(),
            ));
        }
        let records = stream.partition(partition)?.records.clone();
        let previous_bytes = records.iter().try_fold(0_u64, |total, record| {
            total
                .checked_add(retained_record_bytes(record)?)
                .ok_or_else(|| EpochError::Capacity("Stream retained byte count overflow".into()))
        })?;
        let mut latest = BTreeMap::<String, u64>::new();
        for record in &records {
            if self.visible(record, StreamReadIsolation::ReadCommitted)
                && let Some(key) = record.envelope.key.as_deref().filter(|key| !key.is_empty())
            {
                latest.insert(key.to_owned(), record.offset);
            }
        }
        let mut retained = BTreeSet::new();
        let mut removed_tombstones = 0_usize;
        for record in &records {
            let keep = match record.envelope.transaction_id.as_deref() {
                Some(transaction_id)
                    if self
                        .transactions
                        .get(transaction_id)
                        .is_some_and(|transaction| {
                            transaction.status == StreamTransactionStatus::Pending
                        }) =>
                {
                    true
                }
                Some(transaction_id)
                    if self
                        .transactions
                        .get(transaction_id)
                        .is_some_and(|transaction| {
                            transaction.status == StreamTransactionStatus::Aborted
                        }) =>
                {
                    false
                }
                _ => match record.envelope.key.as_deref().filter(|key| !key.is_empty()) {
                    Some(key) if latest.get(key) != Some(&record.offset) => false,
                    Some(_) if is_tombstone(record) => {
                        let expired = record
                            .appended_at_ms
                            .checked_add(tombstone_retention_ms)
                            .is_some_and(|deadline| now_ms >= deadline);
                        if expired {
                            removed_tombstones = removed_tombstones.saturating_add(1);
                        }
                        !expired
                    }
                    None | Some(_) => true,
                },
            };
            if keep {
                retained.insert(record.offset);
            }
        }
        remove_hot_records(stream, partition, &retained, false)?;
        let retained_partition = stream.partition(partition)?;
        let retained_bytes =
            retained_partition
                .records
                .iter()
                .try_fold(0_u64, |total, record| {
                    total
                        .checked_add(retained_record_bytes(record)?)
                        .ok_or_else(|| {
                            EpochError::Capacity("Stream retained byte count overflow".into())
                        })
                })?;
        Ok(StreamCompactionReport {
            partition,
            removed_records: records
                .len()
                .saturating_sub(retained_partition.records.len()),
            removed_bytes: previous_bytes.saturating_sub(retained_bytes),
            retained_records: retained_partition.records.len(),
            retained_bytes,
            removed_tombstones,
        })
    }

    pub fn tier_prefix(
        &mut self,
        stream: &mut Stream,
        partition: u32,
        before_offset: u64,
        max_records: usize,
    ) -> EpochResult<Option<StreamTierObject>> {
        if max_records == 0 || max_records > 1_024 {
            return Err(EpochError::InvalidArgument(
                "tier max_records must be between 1 and 1024".into(),
            ));
        }
        let hot_records = stream.partition(partition)?.records.clone();
        let mut selected = Vec::new();
        let mut removable = BTreeSet::new();
        for record in &hot_records {
            if record.offset >= before_offset || removable.len() >= max_records {
                break;
            }
            if record
                .envelope
                .transaction_id
                .as_deref()
                .is_some_and(|transaction_id| {
                    self.transactions
                        .get(transaction_id)
                        .is_some_and(|transaction| {
                            transaction.status == StreamTransactionStatus::Pending
                        })
                })
            {
                break;
            }
            removable.insert(record.offset);
            selected.push(record.clone());
        }
        if removable.is_empty() {
            return Ok(None);
        }
        let encoded_bytes = serde_json::to_vec(&selected)
            .map_err(|error| EpochError::Internal(error.to_string()))?;
        if encoded_bytes.len() > MAX_STREAM_TIER_OBJECT_BYTES {
            return Err(EpochError::Capacity(format!(
                "tier object exceeds {MAX_STREAM_TIER_OBJECT_BYTES} bytes"
            )));
        }
        let checksum_sha256 = sha256_hex(&encoded_bytes);
        let first_offset = removable.iter().next().copied().ok_or_else(|| {
            EpochError::Internal("tier range disappeared while it was encoded".into())
        })?;
        let end_offset = removable
            .iter()
            .next_back()
            .copied()
            .and_then(|offset| offset.checked_add(1))
            .ok_or_else(|| EpochError::Capacity("tier range end overflow".into()))?;
        let object = StreamTierObject {
            object_id: format!("stream/{partition}/{first_offset}-{end_offset}-{checksum_sha256}"),
            partition,
            first_offset,
            end_offset,
            record_count: selected.len(),
            encoded_bytes,
            checksum_sha256,
        };
        let objects = self.tier_objects.entry(partition).or_default();
        if objects.len() >= MAX_STREAM_TIER_OBJECTS_PER_PARTITION {
            return Err(EpochError::Capacity(format!(
                "Stream supports at most {MAX_STREAM_TIER_OBJECTS_PER_PARTITION} tier objects per partition"
            )));
        }
        if objects
            .last()
            .is_some_and(|previous| previous.end_offset > object.first_offset)
        {
            return Err(EpochError::Conflict(
                "tier object overlaps an existing immutable range".into(),
            ));
        }
        objects.push(object.clone());
        let retained = hot_records
            .iter()
            .filter_map(|record| (!removable.contains(&record.offset)).then_some(record.offset))
            .collect();
        remove_hot_records(stream, partition, &retained, true)?;
        Ok(Some(object))
    }

    pub fn tier_objects(&self, partition: u32) -> &[StreamTierObject] {
        self.tier_objects.get(&partition).map_or(&[], Vec::as_slice)
    }

    pub fn capture(
        &mut self,
        stream: &Stream,
        capture_id: &str,
        partition: u32,
        first_offset: u64,
        end_offset: u64,
        format: StreamCaptureFormat,
    ) -> EpochResult<StreamCaptureArtifact> {
        validate_identifier("capture ID", capture_id, MAX_STREAM_IDENTIFIER_BYTES)?;
        if first_offset > end_offset {
            return Err(EpochError::InvalidArgument(
                "capture offset range is inverted".into(),
            ));
        }
        let records = self
            .fetch(
                stream,
                partition,
                first_offset,
                usize::MAX,
                StreamReadIsolation::ReadCommitted,
            )?
            .into_iter()
            .take_while(|record| record.offset < end_offset)
            .collect::<Vec<_>>();
        let encoded_bytes = encode_capture(&records, format)?;
        if encoded_bytes.len() > MAX_STREAM_CAPTURE_BYTES {
            return Err(EpochError::Capacity(format!(
                "capture artifact exceeds {MAX_STREAM_CAPTURE_BYTES} bytes"
            )));
        }
        let artifact = StreamCaptureArtifact {
            capture_id: capture_id.to_owned(),
            partition,
            first_offset,
            end_offset,
            record_count: records.len(),
            format,
            checksum_sha256: sha256_hex(&encoded_bytes),
            encoded_bytes,
        };
        if let Some(existing) = self.captures.get(capture_id) {
            if existing == &artifact {
                return Ok(existing.clone());
            }
            return Err(EpochError::Conflict(
                "capture ID is already bound to another artifact".into(),
            ));
        }
        if self.captures.len() >= MAX_STREAM_CAPTURE_ARTIFACTS {
            return Err(EpochError::Capacity(format!(
                "Stream supports at most {MAX_STREAM_CAPTURE_ARTIFACTS} capture artifacts"
            )));
        }
        self.captures
            .insert(capture_id.to_owned(), artifact.clone());
        Ok(artifact)
    }

    pub fn capture_artifact(&self, capture_id: &str) -> Option<&StreamCaptureArtifact> {
        self.captures.get(capture_id)
    }

    pub fn configure_capture_schedule(
        &mut self,
        stream: &Stream,
        schedule_id: &str,
        partition: u32,
        interval_ms: u64,
        format: StreamCaptureFormat,
        now_ms: u64,
    ) -> EpochResult<StreamCaptureScheduleObservation> {
        validate_identifier(
            "capture schedule ID",
            schedule_id,
            MAX_STREAM_CLUSTER_ID_BYTES,
        )?;
        if !(MIN_STREAM_CAPTURE_INTERVAL_MS..=MAX_STREAM_CAPTURE_INTERVAL_MS).contains(&interval_ms)
        {
            return Err(EpochError::InvalidArgument(format!(
                "capture interval must be between {MIN_STREAM_CAPTURE_INTERVAL_MS} and {MAX_STREAM_CAPTURE_INTERVAL_MS} milliseconds"
            )));
        }
        let hot = stream.partition(partition)?;
        if let Some(existing) = self.capture_schedules.get(schedule_id)
            && existing.partition == partition
            && existing.interval_ms == interval_ms
            && existing.format == format
        {
            return Ok(capture_schedule_observation(schedule_id, existing));
        }
        if !self.capture_schedules.contains_key(schedule_id)
            && self.capture_schedules.len() >= MAX_STREAM_CAPTURE_SCHEDULES
        {
            return Err(EpochError::Capacity(format!(
                "Stream supports at most {MAX_STREAM_CAPTURE_SCHEDULES} capture schedules"
            )));
        }
        let next_capture_at_ms = now_ms
            .checked_add(interval_ms)
            .ok_or_else(|| EpochError::Capacity("capture deadline overflow".into()))?;
        let earliest_available_offset = self.earliest_available_offset(partition, hot.base_offset);
        let next_offset = self
            .capture_schedules
            .get(schedule_id)
            .map_or(earliest_available_offset, |existing| {
                existing.next_offset.max(earliest_available_offset)
            });
        let schedule = StreamCaptureSchedule {
            partition,
            format,
            interval_ms,
            next_offset,
            next_capture_at_ms,
        };
        self.capture_schedules
            .insert(schedule_id.to_owned(), schedule.clone());
        Ok(capture_schedule_observation(schedule_id, &schedule))
    }

    pub fn capture_schedule(&self, schedule_id: &str) -> Option<StreamCaptureScheduleObservation> {
        self.capture_schedules
            .get(schedule_id)
            .map(|schedule| capture_schedule_observation(schedule_id, schedule))
    }

    pub fn due_capture_schedules(&self, now_ms: u64) -> Vec<(u64, String)> {
        let mut due = self
            .capture_schedules
            .iter()
            .filter(|(_, schedule)| schedule.next_capture_at_ms <= now_ms)
            .map(|(schedule_id, schedule)| (schedule.next_capture_at_ms, schedule_id.clone()))
            .collect::<Vec<_>>();
        due.sort();
        due
    }

    pub fn maintain_capture_schedule(
        &mut self,
        stream: &Stream,
        schedule_id: &str,
        now_ms: u64,
    ) -> EpochResult<StreamCaptureMaintenanceOutcome> {
        let schedule = self
            .capture_schedules
            .get(schedule_id)
            .cloned()
            .ok_or_else(|| EpochError::NotFound(format!("capture schedule {schedule_id}")))?;
        if now_ms < schedule.next_capture_at_ms {
            return Err(EpochError::Conflict(format!(
                "capture schedule {schedule_id} is not due until {}",
                schedule.next_capture_at_ms
            )));
        }
        let hot = stream.partition(schedule.partition)?;
        let earliest_available_offset =
            self.earliest_available_offset(schedule.partition, hot.base_offset);
        let first_offset = schedule.next_offset.max(earliest_available_offset);
        let end_offset = hot
            .records
            .iter()
            .filter(|record| record.offset >= first_offset)
            .find(|record| {
                record
                    .envelope
                    .transaction_id
                    .as_deref()
                    .is_some_and(|transaction_id| {
                        self.transactions
                            .get(transaction_id)
                            .is_some_and(|transaction| {
                                transaction.status == StreamTransactionStatus::Pending
                            })
                    })
            })
            .map_or(hot.next_offset, |record| record.offset);
        let skipped_retained_offsets = first_offset.saturating_sub(schedule.next_offset);
        let capture_id = format!("auto-{schedule_id}-{:020}", schedule.next_capture_at_ms);
        let mut next = self.clone();
        if next.captures.len() >= MAX_STREAM_CAPTURE_ARTIFACTS
            && !next.captures.contains_key(&capture_id)
        {
            let prefix = format!("auto-{schedule_id}-");
            let expired = next
                .captures
                .keys()
                .find(|capture_id| capture_id.starts_with(&prefix))
                .cloned()
                .ok_or_else(|| {
                    EpochError::Capacity(
                        "capture artifact capacity is occupied by manual captures".into(),
                    )
                })?;
            next.captures.remove(&expired);
        }
        let artifact = next.capture(
            stream,
            &capture_id,
            schedule.partition,
            first_offset,
            end_offset,
            schedule.format,
        )?;
        let elapsed = now_ms.saturating_sub(schedule.next_capture_at_ms);
        let periods = elapsed
            .checked_div(schedule.interval_ms)
            .and_then(|periods| periods.checked_add(1))
            .ok_or_else(|| EpochError::Capacity("capture interval overflow".into()))?;
        let advance = periods
            .checked_mul(schedule.interval_ms)
            .ok_or_else(|| EpochError::Capacity("capture deadline overflow".into()))?;
        let next_capture_at_ms = schedule
            .next_capture_at_ms
            .checked_add(advance)
            .ok_or_else(|| EpochError::Capacity("capture deadline overflow".into()))?;
        let updated = StreamCaptureSchedule {
            next_offset: end_offset,
            next_capture_at_ms,
            ..schedule
        };
        next.capture_schedules
            .insert(schedule_id.to_owned(), updated.clone());
        *self = next;
        Ok(StreamCaptureMaintenanceOutcome {
            schedule: capture_schedule_observation(schedule_id, &updated),
            artifact,
            skipped_retained_offsets,
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "replication validates and stages one atomic batch before publishing its checkpoint"
    )]
    pub fn replicate(
        &mut self,
        stream: &mut Stream,
        local_partition: u32,
        batch: StreamReplicationBatch,
        now_ms: u64,
    ) -> EpochResult<StreamReplicationOutcome> {
        validate_identifier(
            "source cluster ID",
            &batch.source_cluster,
            MAX_STREAM_CLUSTER_ID_BYTES,
        )?;
        validate_identifier(
            "source stream",
            &batch.source_stream,
            MAX_STREAM_IDENTIFIER_BYTES,
        )?;
        if batch.source_cluster == self.local_cluster_id {
            return Err(EpochError::Conflict(
                "replication source cannot be the local cluster".into(),
            ));
        }
        if batch.records.is_empty() || batch.records.len() > MAX_STREAM_REPLICATION_BATCH_RECORDS {
            return Err(EpochError::InvalidArgument(format!(
                "replication batch must contain between 1 and {MAX_STREAM_REPLICATION_BATCH_RECORDS} records"
            )));
        }
        let source_key = replication_source_key(&batch);
        let digest = digest_json(&batch)?;
        if let Some(checkpoint) = self.replication.get(&source_key) {
            if checkpoint.last_batch_digest == digest {
                let mut duplicate = checkpoint.last_outcome.clone();
                duplicate.duplicate = true;
                return Ok(duplicate);
            }
            if checkpoint.next_source_offset != batch.first_source_offset {
                return Err(EpochError::Conflict(format!(
                    "replication checkpoint is {}; observed batch start {}",
                    checkpoint.next_source_offset, batch.first_source_offset
                )));
            }
        } else if batch.first_source_offset != 0 {
            return Err(EpochError::Conflict(
                "a new replication source must begin at offset 0".into(),
            ));
        }
        if !self.replication.contains_key(&source_key)
            && self.replication.len() >= MAX_STREAM_REPLICATION_SOURCES
        {
            return Err(EpochError::Capacity(format!(
                "Stream supports at most {MAX_STREAM_REPLICATION_SOURCES} replication sources"
            )));
        }

        let mut next_stream = stream.clone();
        let mut mappings = Vec::with_capacity(batch.records.len());
        for (index, record) in batch.records.iter().enumerate() {
            let expected_offset = batch
                .first_source_offset
                .checked_add(u64::try_from(index).map_err(|error| {
                    EpochError::Capacity(format!("replication index overflow: {error}"))
                })?)
                .ok_or_else(|| EpochError::Capacity("replication offset overflow".into()))?;
            if record.source_offset != expected_offset {
                return Err(EpochError::InvalidArgument(
                    "replication source offsets must be contiguous".into(),
                ));
            }
            if record
                .traversed_clusters
                .iter()
                .any(|cluster| cluster == &self.local_cluster_id)
            {
                return Err(EpochError::Conflict(
                    "replication loop includes the local cluster".into(),
                ));
            }
            let mut envelope = record.envelope.clone();
            envelope.extensions.insert(
                "epoch.replication.source_cluster".into(),
                Value::String(batch.source_cluster.clone()),
            );
            envelope.extensions.insert(
                "epoch.replication.source_stream".into(),
                Value::String(batch.source_stream.clone()),
            );
            envelope.extensions.insert(
                "epoch.replication.source_partition".into(),
                Value::from(batch.source_partition),
            );
            envelope.extensions.insert(
                "epoch.replication.source_offset".into(),
                Value::String(record.source_offset.to_string()),
            );
            if envelope.dedupe_id.is_none() {
                envelope.dedupe_id = Some(format!(
                    "repl/{}/{}/{}/{}",
                    batch.source_cluster,
                    batch.source_stream,
                    batch.source_partition,
                    record.source_offset
                ));
            }
            let appended = next_stream.append(envelope, Some(local_partition), now_ms)?;
            mappings.push(StreamReplicationMapping {
                source_offset: record.source_offset,
                local_partition: appended.partition,
                local_offset: appended.offset,
            });
        }
        let next_source_offset = batch
            .first_source_offset
            .checked_add(u64::try_from(batch.records.len()).map_err(|error| {
                EpochError::Capacity(format!("replication batch size overflow: {error}"))
            })?)
            .ok_or_else(|| EpochError::Capacity("replication checkpoint overflow".into()))?;
        let outcome = StreamReplicationOutcome {
            source_cluster: batch.source_cluster,
            source_stream: batch.source_stream,
            source_partition: batch.source_partition,
            next_source_offset,
            duplicate: false,
            mappings,
        };
        self.replication.insert(
            source_key,
            ReplicationCheckpoint {
                next_source_offset,
                last_batch_digest: digest,
                last_outcome: outcome.clone(),
            },
        );
        *stream = next_stream;
        Ok(outcome)
    }

    pub fn encode_snapshot(&self) -> EpochResult<Vec<u8>> {
        self.validate()?;
        let encoded = serde_json::to_vec(&VersionedStateServicesSnapshot {
            format_version: STREAM_STATE_SERVICES_SNAPSHOT_VERSION,
            state: self.clone(),
        })
        .map_err(|error| EpochError::Internal(error.to_string()))?;
        if encoded.len() > MAX_STREAM_STATE_SERVICES_SNAPSHOT_BYTES {
            return Err(EpochError::Capacity(format!(
                "Stream state-services snapshot is {} bytes; maximum is {MAX_STREAM_STATE_SERVICES_SNAPSHOT_BYTES}",
                encoded.len()
            )));
        }
        Ok(encoded)
    }

    pub fn decode_snapshot(encoded: &[u8]) -> EpochResult<Self> {
        if encoded.len() > MAX_STREAM_STATE_SERVICES_SNAPSHOT_BYTES {
            return Err(EpochError::Capacity(format!(
                "Stream state-services snapshot is {} bytes; maximum is {MAX_STREAM_STATE_SERVICES_SNAPSHOT_BYTES}",
                encoded.len()
            )));
        }
        let snapshot: VersionedStateServicesSnapshot =
            serde_json::from_slice(encoded).map_err(|error| {
                EpochError::Storage(format!("invalid Stream state snapshot: {error}"))
            })?;
        if ![
            LEGACY_STREAM_STATE_SERVICES_SNAPSHOT_VERSION,
            STREAM_STATE_SERVICES_SNAPSHOT_VERSION,
        ]
        .contains(&snapshot.format_version)
        {
            return Err(EpochError::InvalidArgument(format!(
                "unsupported Stream state-services snapshot version {}",
                snapshot.format_version
            )));
        }
        snapshot.state.validate()?;
        let canonical = serde_json::to_vec(&snapshot)
            .map_err(|error| EpochError::Internal(error.to_string()))?;
        if canonical.as_slice() != encoded {
            return Err(EpochError::Storage(
                "Stream state-services snapshot is not canonical".into(),
            ));
        }
        Ok(snapshot.state)
    }

    /// Validates state-service references against the colocated ordered log.
    ///
    /// This is intentionally separate from snapshot decoding because the two
    /// components are encoded independently inside the tablet snapshot.
    pub fn validate_against(&self, stream: &Stream) -> EpochResult<()> {
        self.validate()?;
        for (partition, objects) in &self.tier_objects {
            let hot = stream.partition(*partition)?;
            if objects
                .last()
                .is_some_and(|object| object.end_offset > hot.base_offset)
            {
                return Err(EpochError::Storage(
                    "Stream tier range overlaps the hot log".into(),
                ));
            }
        }
        for transaction in self.transactions.values() {
            for position in &transaction.positions {
                if position.offset >= stream.partition(position.partition)?.next_offset {
                    return Err(EpochError::Storage(
                        "Stream transaction position exceeds the ordered log".into(),
                    ));
                }
            }
            if let Some(commit) = &transaction.offset_commit
                && commit.next_offset > stream.partition(commit.partition)?.next_offset
            {
                return Err(EpochError::Storage(
                    "Stream transaction checkpoint exceeds the ordered log".into(),
                ));
            }
        }
        for producer in self.producers.values() {
            for receipt in producer.history.values() {
                for position in &receipt.positions {
                    if position.offset >= stream.partition(position.partition)?.next_offset {
                        return Err(EpochError::Storage(
                            "Stream producer receipt exceeds the ordered log".into(),
                        ));
                    }
                }
            }
        }
        for artifact in self.captures.values() {
            if artifact.first_offset > artifact.end_offset
                || artifact.end_offset > stream.partition(artifact.partition)?.next_offset
            {
                return Err(EpochError::Storage(
                    "Stream capture range exceeds the ordered log".into(),
                ));
            }
        }
        for schedule in self.capture_schedules.values() {
            if schedule.next_offset > stream.partition(schedule.partition)?.next_offset {
                return Err(EpochError::Storage(
                    "Stream capture schedule checkpoint exceeds the ordered log".into(),
                ));
            }
        }
        for checkpoint in self.replication.values() {
            for mapping in &checkpoint.last_outcome.mappings {
                if mapping.local_offset >= stream.partition(mapping.local_partition)?.next_offset {
                    return Err(EpochError::Storage(
                        "Stream replication mapping exceeds the ordered log".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the internal append boundary mirrors the independently fenced protocol fields"
    )]
    fn apply_sequence(
        &mut self,
        stream: &mut Stream,
        producer_id: &str,
        producer_epoch: u64,
        sequence: u64,
        digest: [u8; 32],
        transaction_id: Option<String>,
        envelopes: Vec<EventEnvelope>,
        partition: u32,
        now_ms: u64,
    ) -> EpochResult<StreamProducerAppendOutcome> {
        validate_identifier("producer ID", producer_id, MAX_STREAM_IDENTIFIER_BYTES)?;
        validate_epoch(producer_epoch)?;
        self.ensure_producer_epoch(producer_id, producer_epoch)?;
        let producer = self
            .producers
            .get_mut(producer_id)
            .ok_or_else(|| EpochError::Internal("producer was not retained".into()))?;
        if sequence < producer.next_sequence {
            let existing = producer.history.get(&sequence).ok_or_else(|| {
                EpochError::Conflict(format!(
                    "producer sequence {sequence} is older than retained retry history"
                ))
            })?;
            if existing.digest != digest || existing.transaction_id != transaction_id {
                return Err(EpochError::Conflict(
                    "producer sequence is already bound to another append".into(),
                ));
            }
            return Ok(StreamProducerAppendOutcome {
                producer_id: producer_id.to_owned(),
                producer_epoch,
                sequence,
                disposition: StreamProducerDisposition::Duplicate,
                positions: existing.positions.clone(),
                transaction_id,
            });
        }
        if sequence != producer.next_sequence {
            return Err(EpochError::Conflict(format!(
                "producer expected sequence {}; observed {sequence}",
                producer.next_sequence
            )));
        }
        let mut positions = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            let AppendReceipt {
                partition, offset, ..
            } = stream.append(envelope, Some(partition), now_ms)?;
            positions.push(StreamPosition { partition, offset });
        }
        producer.next_sequence = producer
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("producer sequence exhausted".into()))?;
        producer.history.insert(
            sequence,
            ProducerSequenceReceipt {
                digest,
                positions: positions.clone(),
                transaction_id: transaction_id.clone(),
            },
        );
        while producer.history.len() > MAX_STREAM_PRODUCER_HISTORY {
            let Some(first) = producer.history.keys().next().copied() else {
                break;
            };
            producer.history.remove(&first);
        }
        Ok(StreamProducerAppendOutcome {
            producer_id: producer_id.to_owned(),
            producer_epoch,
            sequence,
            disposition: StreamProducerDisposition::New,
            positions,
            transaction_id,
        })
    }

    fn ensure_producer_epoch(&mut self, producer_id: &str, producer_epoch: u64) -> EpochResult<()> {
        validate_identifier("producer ID", producer_id, MAX_STREAM_IDENTIFIER_BYTES)?;
        validate_epoch(producer_epoch)?;
        match self.producers.get(producer_id) {
            Some(existing) if producer_epoch < existing.epoch => return Err(EpochError::Fenced),
            Some(existing) if producer_epoch == existing.epoch => return Ok(()),
            Some(_) => {
                for transaction in self.transactions.values_mut().filter(|transaction| {
                    transaction.producer_id == producer_id
                        && transaction.status == StreamTransactionStatus::Pending
                }) {
                    transaction.status = StreamTransactionStatus::Aborted;
                }
                self.producers.insert(
                    producer_id.to_owned(),
                    ProducerState {
                        epoch: producer_epoch,
                        next_sequence: 0,
                        history: BTreeMap::new(),
                    },
                );
            }
            None => {
                if self.producers.len() >= MAX_STREAM_PRODUCERS {
                    return Err(EpochError::Capacity(format!(
                        "Stream supports at most {MAX_STREAM_PRODUCERS} producers"
                    )));
                }
                self.producers.insert(
                    producer_id.to_owned(),
                    ProducerState {
                        epoch: producer_epoch,
                        next_sequence: 0,
                        history: BTreeMap::new(),
                    },
                );
            }
        }
        Ok(())
    }

    fn visible(&self, record: &StreamRecord, isolation: StreamReadIsolation) -> bool {
        if isolation == StreamReadIsolation::ReadUncommitted {
            return true;
        }
        record
            .envelope
            .transaction_id
            .as_deref()
            .is_none_or(|transaction_id| {
                self.transactions
                    .get(transaction_id)
                    .is_some_and(|transaction| {
                        transaction.status == StreamTransactionStatus::Committed
                    })
            })
    }

    fn earliest_available_offset(&self, partition: u32, hot_base_offset: u64) -> u64 {
        self.tier_objects
            .get(&partition)
            .and_then(|objects| objects.first())
            .map_or(hot_base_offset, |object| object.first_offset)
    }

    fn validate(&self) -> EpochResult<()> {
        validate_identifier(
            "local cluster ID",
            &self.local_cluster_id,
            MAX_STREAM_CLUSTER_ID_BYTES,
        )?;
        if self.producers.len() > MAX_STREAM_PRODUCERS
            || self.transactions.len() > MAX_STREAM_TRANSACTIONS
            || self.captures.len() > MAX_STREAM_CAPTURE_ARTIFACTS
            || self.capture_schedules.len() > MAX_STREAM_CAPTURE_SCHEDULES
            || self.replication.len() > MAX_STREAM_REPLICATION_SOURCES
        {
            return Err(EpochError::Storage(
                "Stream state-services snapshot exceeds a cardinality bound".into(),
            ));
        }
        self.validate_producers_and_transactions()?;
        self.validate_tier_and_capture_artifacts()?;
        self.validate_replication_checkpoints()
    }

    fn validate_producers_and_transactions(&self) -> EpochResult<()> {
        for (producer_id, producer) in &self.producers {
            validate_identifier("producer ID", producer_id, MAX_STREAM_IDENTIFIER_BYTES)?;
            validate_epoch(producer.epoch)?;
            if producer.history.len() > MAX_STREAM_PRODUCER_HISTORY
                || producer
                    .history
                    .keys()
                    .any(|sequence| *sequence >= producer.next_sequence)
                || producer
                    .history
                    .keys()
                    .next_back()
                    .is_some_and(|sequence| sequence.checked_add(1) != Some(producer.next_sequence))
                || producer
                    .history
                    .keys()
                    .copied()
                    .collect::<Vec<_>>()
                    .windows(2)
                    .any(|pair| pair[0].checked_add(1) != Some(pair[1]))
                || producer.history.values().any(|receipt| {
                    receipt.positions.is_empty()
                        || receipt.positions.len() > MAX_STREAM_TRANSACTION_RECORDS
                })
            {
                return Err(EpochError::Storage(
                    "Stream producer history is invalid".into(),
                ));
            }
        }
        for (transaction_id, transaction) in &self.transactions {
            validate_identifier(
                "transaction ID",
                transaction_id,
                MAX_STREAM_IDENTIFIER_BYTES,
            )?;
            validate_identifier(
                "transaction producer ID",
                &transaction.producer_id,
                MAX_STREAM_IDENTIFIER_BYTES,
            )?;
            if transaction.producer_epoch == 0
                || transaction.positions.len() > MAX_STREAM_TRANSACTION_RECORDS
                || (transaction.status != StreamTransactionStatus::Committed
                    && transaction.offset_commit.is_some())
            {
                return Err(EpochError::Storage(
                    "Stream transaction snapshot is invalid".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_tier_and_capture_artifacts(&self) -> EpochResult<()> {
        for (partition, objects) in &self.tier_objects {
            if objects.len() > MAX_STREAM_TIER_OBJECTS_PER_PARTITION {
                return Err(EpochError::Storage(
                    "Stream tier object count exceeds its bound".into(),
                ));
            }
            let mut previous_end = 0_u64;
            for object in objects {
                if object.partition != *partition
                    || object.first_offset < previous_end
                    || decode_tier_object(object)?.len() != object.record_count
                {
                    return Err(EpochError::Storage(
                        "Stream tier object metadata is invalid".into(),
                    ));
                }
                previous_end = object.end_offset;
            }
        }
        for (capture_id, artifact) in &self.captures {
            validate_identifier("capture ID", capture_id, MAX_STREAM_IDENTIFIER_BYTES)?;
            if artifact.capture_id != *capture_id
                || artifact.encoded_bytes.len() > MAX_STREAM_CAPTURE_BYTES
                || sha256_hex(&artifact.encoded_bytes) != artifact.checksum_sha256
                || decode_capture(artifact)?.len() != artifact.record_count
            {
                return Err(EpochError::Storage(
                    "Stream capture artifact is invalid".into(),
                ));
            }
        }
        for (schedule_id, schedule) in &self.capture_schedules {
            validate_identifier(
                "capture schedule ID",
                schedule_id,
                MAX_STREAM_CLUSTER_ID_BYTES,
            )?;
            if !(MIN_STREAM_CAPTURE_INTERVAL_MS..=MAX_STREAM_CAPTURE_INTERVAL_MS)
                .contains(&schedule.interval_ms)
            {
                return Err(EpochError::Storage(
                    "Stream capture schedule interval is invalid".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_replication_checkpoints(&self) -> EpochResult<()> {
        for (source_key, checkpoint) in &self.replication {
            let outcome = &checkpoint.last_outcome;
            if *source_key
                != format!(
                    "{}/{}/{}",
                    outcome.source_cluster, outcome.source_stream, outcome.source_partition
                )
                || outcome.duplicate
                || checkpoint.next_source_offset != outcome.next_source_offset
                || outcome.mappings.is_empty()
                || outcome.mappings.len() > MAX_STREAM_REPLICATION_BATCH_RECORDS
                || outcome
                    .mappings
                    .windows(2)
                    .any(|pair| pair[0].source_offset.checked_add(1) != Some(pair[1].source_offset))
                || outcome
                    .mappings
                    .last()
                    .and_then(|mapping| mapping.source_offset.checked_add(1))
                    != Some(checkpoint.next_source_offset)
            {
                return Err(EpochError::Storage(
                    "Stream replication checkpoint is invalid".into(),
                ));
            }
        }
        Ok(())
    }
}

pub fn advise_stream_partitions(
    current_partitions: u32,
    observed_records: u64,
    observed_bytes: u64,
    target_records_per_partition: u64,
    target_bytes_per_partition: u64,
) -> EpochResult<StreamPartitionAdvice> {
    if current_partitions == 0
        || target_records_per_partition == 0
        || target_bytes_per_partition == 0
    {
        return Err(EpochError::InvalidArgument(
            "partition advice inputs must be non-zero".into(),
        ));
    }
    let by_records = observed_records
        .div_ceil(target_records_per_partition)
        .max(1);
    let by_bytes = observed_bytes.div_ceil(target_bytes_per_partition).max(1);
    let requested = by_records.max(by_bytes).max(u64::from(current_partitions));
    let recommended_partitions = u32::try_from(requested)
        .map_err(|_| EpochError::Capacity("partition recommendation exceeds u32".into()))?;
    Ok(StreamPartitionAdvice {
        current_partitions,
        recommended_partitions,
        observed_records,
        observed_bytes,
        target_records_per_partition,
        target_bytes_per_partition,
        reason: if recommended_partitions == current_partitions {
            "within_target".into()
        } else if by_bytes >= by_records {
            "retained_bytes".into()
        } else {
            "retained_records".into()
        },
    })
}

pub fn merge_superstream(
    members: impl IntoIterator<Item = SuperstreamRecord>,
    limit: usize,
) -> EpochResult<Vec<SuperstreamRecord>> {
    if limit == 0 {
        return Err(EpochError::InvalidArgument(
            "superstream merge limit must be non-zero".into(),
        ));
    }
    let mut records = members.into_iter().collect::<Vec<_>>();
    for record in &records {
        validate_identifier(
            "superstream member",
            &record.member,
            MAX_STREAM_IDENTIFIER_BYTES,
        )?;
    }
    records.sort_by(|left, right| {
        (
            left.record.appended_at_ms,
            &left.member,
            left.record.partition,
            left.record.offset,
        )
            .cmp(&(
                right.record.appended_at_ms,
                &right.member,
                right.record.partition,
                right.record.offset,
            ))
    });
    records.truncate(limit);
    Ok(records)
}

fn remove_hot_records(
    stream: &mut Stream,
    partition: u32,
    retained_offsets: &BTreeSet<u64>,
    advance_base_offset: bool,
) -> EpochResult<()> {
    let partition_state = stream
        .partitions
        .get_mut(partition as usize)
        .ok_or_else(|| EpochError::NotFound(format!("stream partition {partition}")))?;
    let removed_dedupe = partition_state
        .records
        .iter()
        .filter(|record| !retained_offsets.contains(&record.offset))
        .filter_map(|record| record.envelope.dedupe_id.clone())
        .collect::<BTreeSet<_>>();
    partition_state
        .records
        .retain(|record| retained_offsets.contains(&record.offset));
    if advance_base_offset {
        partition_state.base_offset = partition_state
            .records
            .front()
            .map_or(partition_state.next_offset, |record| record.offset);
    }
    for dedupe_id in removed_dedupe {
        if stream.dedupe.get(&dedupe_id).is_some_and(|receipt| {
            receipt.partition == partition && !retained_offsets.contains(&receipt.offset)
        }) {
            stream.dedupe.remove(&dedupe_id);
        }
    }
    Ok(())
}

fn decode_tier_object(object: &StreamTierObject) -> EpochResult<Vec<StreamRecord>> {
    if object.encoded_bytes.len() > MAX_STREAM_TIER_OBJECT_BYTES
        || sha256_hex(&object.encoded_bytes) != object.checksum_sha256
    {
        return Err(EpochError::Storage(format!(
            "Stream tier object {} failed integrity validation",
            object.object_id
        )));
    }
    let records: Vec<StreamRecord> = serde_json::from_slice(&object.encoded_bytes)
        .map_err(|error| EpochError::Storage(format!("invalid Stream tier object: {error}")))?;
    let expected_object_id = format!(
        "stream/{}/{}-{}-{}",
        object.partition, object.first_offset, object.end_offset, object.checksum_sha256
    );
    if serde_json::to_vec(&records).map_err(|error| EpochError::Internal(error.to_string()))?
        != object.encoded_bytes
        || records.len() != object.record_count
        || object.first_offset >= object.end_offset
        || object.object_id != expected_object_id
        || records.iter().any(|record| {
            record.partition != object.partition
                || record.offset < object.first_offset
                || record.offset >= object.end_offset
        })
        || records
            .windows(2)
            .any(|pair| pair[0].offset >= pair[1].offset)
    {
        return Err(EpochError::Storage(
            "Stream tier object is not canonical or ordered".into(),
        ));
    }
    Ok(records)
}

fn encode_capture(records: &[StreamRecord], format: StreamCaptureFormat) -> EpochResult<Vec<u8>> {
    match format {
        StreamCaptureFormat::JsonArray => {
            serde_json::to_vec(records).map_err(|error| EpochError::Internal(error.to_string()))
        }
        StreamCaptureFormat::JsonLines => {
            let mut encoded = Vec::new();
            for record in records {
                encoded.extend(
                    serde_json::to_vec(record)
                        .map_err(|error| EpochError::Internal(error.to_string()))?,
                );
                encoded.push(b'\n');
            }
            Ok(encoded)
        }
    }
}

fn decode_capture(artifact: &StreamCaptureArtifact) -> EpochResult<Vec<StreamRecord>> {
    let records = match artifact.format {
        StreamCaptureFormat::JsonArray => {
            serde_json::from_slice::<Vec<StreamRecord>>(&artifact.encoded_bytes)
                .map_err(|error| EpochError::Storage(format!("invalid Stream capture: {error}")))?
        }
        StreamCaptureFormat::JsonLines => {
            if artifact.encoded_bytes.is_empty() {
                Vec::new()
            } else {
                if artifact.encoded_bytes.last() != Some(&b'\n') {
                    return Err(EpochError::Storage(
                        "Stream JSON Lines capture is not newline terminated".into(),
                    ));
                }
                artifact.encoded_bytes[..artifact.encoded_bytes.len() - 1]
                    .split(|byte| *byte == b'\n')
                    .map(|line| {
                        serde_json::from_slice::<StreamRecord>(line).map_err(|error| {
                            EpochError::Storage(format!("invalid Stream capture record: {error}"))
                        })
                    })
                    .collect::<EpochResult<Vec<_>>>()?
            }
        }
    };
    if encode_capture(&records, artifact.format)? != artifact.encoded_bytes
        || records.len() != artifact.record_count
        || records.iter().any(|record| {
            record.partition != artifact.partition
                || record.offset < artifact.first_offset
                || record.offset >= artifact.end_offset
        })
        || records
            .windows(2)
            .any(|pair| pair[0].offset >= pair[1].offset)
    {
        return Err(EpochError::Storage(
            "Stream capture is not canonical or ordered".into(),
        ));
    }
    Ok(records)
}

fn replication_source_key(batch: &StreamReplicationBatch) -> String {
    format!(
        "{}/{}/{}",
        batch.source_cluster, batch.source_stream, batch.source_partition
    )
}

fn capture_schedule_observation(
    schedule_id: &str,
    schedule: &StreamCaptureSchedule,
) -> StreamCaptureScheduleObservation {
    StreamCaptureScheduleObservation {
        schedule_id: schedule_id.to_owned(),
        partition: schedule.partition,
        format: schedule.format,
        interval_ms: schedule.interval_ms,
        next_offset: schedule.next_offset,
        next_capture_at_ms: schedule.next_capture_at_ms,
    }
}

fn is_tombstone(record: &StreamRecord) -> bool {
    record
        .envelope
        .key
        .as_deref()
        .is_some_and(|key| !key.is_empty())
        && record.envelope.payload.is_null()
}

fn validate_identifier(label: &str, value: &str, max_bytes: usize) -> EpochResult<()> {
    if value.trim().is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(EpochError::InvalidArgument(format!(
            "{label} must contain 1 to {max_bytes} non-control UTF-8 bytes"
        )));
    }
    Ok(())
}

fn validate_epoch(epoch: u64) -> EpochResult<()> {
    if epoch == 0 {
        return Err(EpochError::InvalidArgument(
            "producer epoch must be non-zero".into(),
        ));
    }
    Ok(())
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde serialize_with requires a shared reference"
)]
fn serialize_u64_as_decimal<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn deserialize_u64_from_number_or_decimal<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Representation {
        Number(u64),
        Decimal(String),
    }

    match Representation::deserialize(deserializer)? {
        Representation::Number(value) => Ok(value),
        Representation::Decimal(value) => value.parse().map_err(serde::de::Error::custom),
    }
}

fn digest_json(value: &impl Serialize) -> EpochResult<[u8; 32]> {
    let encoded = serde_json::to_vec(value)
        .map_err(|error| EpochError::Internal(format!("digest encoding failed: {error}")))?;
    Ok(Sha256::digest(encoded).into())
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        })
}

#[cfg(test)]
mod tests {
    use epoch_core::EventEnvelope;
    use serde_json::json;

    use super::*;
    use crate::StreamConfig;

    fn event(id: &str, key: Option<&str>, payload: Value) -> EventEnvelope {
        let mut event = EventEnvelope::new("advanced-tests", "stream.event", payload, 1);
        event.id = id.into();
        event.key = key.map(str::to_owned);
        event
    }

    #[test]
    fn producer_sequences_deduplicate_exactly_and_fence_old_epochs() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        let mut services = StreamStateServices::new("west").unwrap();
        let envelope = event("one", Some("k"), json!({"v": 1}));
        let first = services
            .append_idempotent(&mut stream, "producer-a", 1, 0, 0, envelope.clone(), 10)
            .unwrap();
        let replay = services
            .append_idempotent(&mut stream, "producer-a", 1, 0, 0, envelope, 11)
            .unwrap();
        assert_eq!(first.positions, replay.positions);
        assert_eq!(replay.disposition, StreamProducerDisposition::Duplicate);
        assert!(
            services
                .append_idempotent(
                    &mut stream,
                    "producer-a",
                    1,
                    2,
                    0,
                    event("gap", None, json!({})),
                    12,
                )
                .is_err()
        );
        services
            .append_idempotent(
                &mut stream,
                "producer-a",
                2,
                0,
                0,
                event("two", None, json!({})),
                13,
            )
            .unwrap();
        assert!(matches!(
            services.append_idempotent(
                &mut stream,
                "producer-a",
                1,
                1,
                0,
                event("stale", None, json!({})),
                14,
            ),
            Err(EpochError::Fenced)
        ));

        let encoded = services.encode_snapshot().unwrap();
        assert_eq!(
            StreamStateServices::decode_snapshot(&encoded).unwrap(),
            services
        );
        let legacy = String::from_utf8(encoded)
            .unwrap()
            .replacen("\"format_version\":2", "\"format_version\":1", 1)
            .into_bytes();
        let restored = StreamStateServices::decode_snapshot(&legacy).unwrap();
        assert_eq!(restored, services);
        assert!(
            restored
                .encode_snapshot()
                .unwrap()
                .starts_with(b"{\"format_version\":2")
        );
    }

    #[test]
    fn transactions_hide_pending_and_aborted_records_and_commit_offsets_atomically() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        stream
            .append(event("input", None, json!({})), Some(0), 1)
            .unwrap();
        let mut services = StreamStateServices::new("west").unwrap();
        services.begin_transaction("tx-1", "producer", 1).unwrap();
        services
            .append_transaction(
                &mut stream,
                "tx-1",
                "producer",
                1,
                0,
                0,
                vec![event("output", Some("a"), json!({"ok": true}))],
                2,
            )
            .unwrap();
        assert_eq!(
            services
                .fetch(&stream, 0, 0, 10, StreamReadIsolation::ReadCommitted)
                .unwrap()
                .len(),
            1
        );
        services
            .commit_transaction(
                &mut stream,
                "tx-1",
                Some(StreamOffsetCommit {
                    group: "workers".into(),
                    partition: 0,
                    next_offset: 1,
                }),
            )
            .unwrap();
        assert_eq!(stream.lag("workers", 0).unwrap().committed_offset, 1);
        assert_eq!(
            services
                .fetch(&stream, 0, 0, 10, StreamReadIsolation::ReadCommitted)
                .unwrap()
                .len(),
            2
        );

        services.begin_transaction("tx-2", "producer", 1).unwrap();
        services
            .append_transaction(
                &mut stream,
                "tx-2",
                "producer",
                1,
                1,
                0,
                vec![event("aborted", None, json!({}))],
                3,
            )
            .unwrap();
        services.abort_transaction("tx-2").unwrap();
        assert_eq!(
            services
                .fetch(&stream, 0, 0, 10, StreamReadIsolation::ReadCommitted)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            services
                .fetch(&stream, 0, 0, 10, StreamReadIsolation::ReadUncommitted)
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn keyed_compaction_keeps_latest_values_and_expires_tombstones() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        for (id, key, payload, time) in [
            ("a1", Some("a"), json!(1), 1),
            ("b1", Some("b"), json!(1), 2),
            ("a2", Some("a"), json!(2), 3),
            ("free", None, json!(3), 4),
            ("b2", Some("b"), Value::Null, 5),
        ] {
            stream
                .append(event(id, key, payload), Some(0), time)
                .unwrap();
        }
        let mut services = StreamStateServices::new("west").unwrap();
        let report = services.compact(&mut stream, 0, 20, 10).unwrap();
        assert_eq!(report.removed_records, 3);
        assert_eq!(report.removed_tombstones, 1);
        let ids = stream
            .fetch(0, 0, 10)
            .unwrap()
            .into_iter()
            .map(|record| record.envelope.id)
            .collect::<Vec<_>>();
        assert_eq!(ids, ["a2", "free"]);
    }

    #[test]
    fn immutable_tier_objects_are_integrity_checked_and_transparently_fetched() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        for id in ["one", "two", "three"] {
            stream
                .append(event(id, None, json!({"id": id})), Some(0), 1)
                .unwrap();
        }
        let mut services = StreamStateServices::new("west").unwrap();
        let object = services
            .tier_prefix(&mut stream, 0, 2, 10)
            .unwrap()
            .unwrap();
        assert_eq!(object.record_count, 2);
        assert_eq!(stream.fetch(0, 2, 10).unwrap().len(), 1);
        let all = services
            .fetch(&stream, 0, 0, 10, StreamReadIsolation::ReadCommitted)
            .unwrap();
        assert_eq!(all.len(), 3);
        let mut corrupt = services.clone();
        corrupt.tier_objects.get_mut(&0).unwrap()[0].encoded_bytes[0] ^= 1;
        assert!(
            corrupt
                .fetch(&stream, 0, 0, 10, StreamReadIsolation::ReadCommitted)
                .is_err()
        );
        assert!(matches!(
            services.compact(&mut stream, 0, 20, 10),
            Err(EpochError::Conflict(_))
        ));
    }

    #[test]
    fn tiering_preserves_aborted_records_for_uncommitted_historical_reads() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        let mut services = StreamStateServices::new("west").unwrap();
        services.begin_transaction("tx", "producer", 1).unwrap();
        services
            .append_transaction(
                &mut stream,
                "tx",
                "producer",
                1,
                0,
                0,
                vec![
                    event("aborted-1", None, json!({})),
                    event("aborted-2", None, json!({})),
                ],
                1,
            )
            .unwrap();
        services.abort_transaction("tx").unwrap();

        let object = services.tier_prefix(&mut stream, 0, 2, 2).unwrap().unwrap();

        assert_eq!((object.first_offset, object.end_offset), (0, 2));
        assert_eq!(object.record_count, 2);
        assert!(
            services
                .fetch(&stream, 0, 0, 10, StreamReadIsolation::ReadCommitted)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            services
                .fetch(&stream, 0, 0, 10, StreamReadIsolation::ReadUncommitted)
                .unwrap()
                .len(),
            2
        );
        services.validate_against(&stream).unwrap();
    }

    #[test]
    fn capture_is_canonical_bounded_and_idempotent() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        stream
            .append(event("one", None, json!({"v": 1})), Some(0), 1)
            .unwrap();
        let mut services = StreamStateServices::new("west").unwrap();
        let artifact = services
            .capture(
                &stream,
                "capture-1",
                0,
                0,
                1,
                StreamCaptureFormat::JsonLines,
            )
            .unwrap();
        assert_eq!(artifact.record_count, 1);
        assert!(artifact.encoded_bytes.ends_with(b"\n"));
        assert_eq!(
            services
                .capture(
                    &stream,
                    "capture-1",
                    0,
                    0,
                    1,
                    StreamCaptureFormat::JsonLines
                )
                .unwrap(),
            artifact
        );
        assert!(
            services
                .capture(
                    &stream,
                    "capture-1",
                    0,
                    0,
                    1,
                    StreamCaptureFormat::JsonArray
                )
                .is_err()
        );
    }

    #[test]
    fn automatic_capture_schedule_checkpoints_open_artifacts_without_clock_drift() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        for id in ["one", "two"] {
            stream
                .append(event(id, None, json!({"id": id})), Some(0), 100)
                .unwrap();
        }
        let mut services = StreamStateServices::new("west").unwrap();
        let configured = services
            .configure_capture_schedule(
                &stream,
                "analytics",
                0,
                1_000,
                StreamCaptureFormat::JsonLines,
                100,
            )
            .unwrap();
        assert_eq!(configured.next_capture_at_ms, 1_100);
        assert!(services.due_capture_schedules(1_099).is_empty());
        assert_eq!(
            services.due_capture_schedules(1_100),
            [(1_100, "analytics".into())]
        );

        let first = services
            .maintain_capture_schedule(&stream, "analytics", 1_150)
            .unwrap();
        assert_eq!(first.artifact.record_count, 2);
        assert_eq!(first.schedule.next_offset, 2);
        assert_eq!(first.schedule.next_capture_at_ms, 2_100);
        assert_eq!(first.skipped_retained_offsets, 0);
        stream
            .append(event("three", None, json!({})), Some(0), 2_000)
            .unwrap();
        let delayed = services
            .maintain_capture_schedule(&stream, "analytics", 4_100)
            .unwrap();
        assert_eq!(
            (delayed.artifact.first_offset, delayed.artifact.end_offset),
            (2, 3)
        );
        assert_eq!(delayed.schedule.next_capture_at_ms, 5_100);

        services.validate_against(&stream).unwrap();
        let encoded = services.encode_snapshot().unwrap();
        assert_eq!(
            StreamStateServices::decode_snapshot(&encoded).unwrap(),
            services
        );
    }

    #[test]
    fn automatic_capture_never_advances_past_a_pending_transaction() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        stream
            .append(event("committed", None, json!({})), Some(0), 1)
            .unwrap();
        let mut services = StreamStateServices::new("west").unwrap();
        services
            .configure_capture_schedule(
                &stream,
                "analytics",
                0,
                1_000,
                StreamCaptureFormat::JsonArray,
                0,
            )
            .unwrap();
        services.begin_transaction("tx", "producer", 1).unwrap();
        services
            .append_transaction(
                &mut stream,
                "tx",
                "producer",
                1,
                0,
                0,
                vec![event("pending", None, json!({}))],
                2,
            )
            .unwrap();

        let blocked = services
            .maintain_capture_schedule(&stream, "analytics", 1_000)
            .unwrap();
        assert_eq!(blocked.artifact.record_count, 1);
        assert_eq!(blocked.schedule.next_offset, 1);
        services
            .commit_transaction(&mut stream, "tx", None)
            .unwrap();
        let resumed = services
            .maintain_capture_schedule(&stream, "analytics", 2_000)
            .unwrap();
        assert_eq!(resumed.artifact.record_count, 1);
        assert_eq!(resumed.schedule.next_offset, 2);
    }

    #[test]
    fn replication_maps_checkpoints_and_rejects_loops() {
        let mut stream = Stream::new(StreamConfig::default()).unwrap();
        let mut services = StreamStateServices::new("west").unwrap();
        let batch = StreamReplicationBatch {
            source_cluster: "east".into(),
            source_stream: "orders".into(),
            source_partition: 2,
            first_source_offset: 0,
            records: vec![StreamReplicationRecord {
                source_offset: 0,
                envelope: event("one", None, json!({})),
                traversed_clusters: vec!["east".into()],
            }],
        };
        let first = services
            .replicate(&mut stream, 0, batch.clone(), 1)
            .unwrap();
        assert_eq!(first.next_source_offset, 1);
        assert!(
            services
                .replicate(&mut stream, 0, batch.clone(), 2)
                .unwrap()
                .duplicate
        );
        let mut looped = batch;
        looped.first_source_offset = 1;
        looped.records[0].source_offset = 1;
        looped.records[0].traversed_clusters.push("west".into());
        assert!(services.replicate(&mut stream, 0, looped, 2).is_err());
    }

    #[test]
    fn partition_advice_and_superstream_merge_are_deterministic() {
        let advice = advise_stream_partitions(2, 1_000, 10_000, 200, 4_000).unwrap();
        assert_eq!(advice.recommended_partitions, 5);
        assert_eq!(advice.reason, "retained_records");

        let records = merge_superstream(
            [
                SuperstreamRecord {
                    member: "b".into(),
                    record: StreamRecord {
                        partition: 0,
                        offset: 0,
                        appended_at_ms: 1,
                        envelope: event("b", None, json!({})),
                    },
                },
                SuperstreamRecord {
                    member: "a".into(),
                    record: StreamRecord {
                        partition: 1,
                        offset: 0,
                        appended_at_ms: 1,
                        envelope: event("a", None, json!({})),
                    },
                },
            ],
            10,
        )
        .unwrap();
        assert_eq!(records[0].member, "a");
    }
}
