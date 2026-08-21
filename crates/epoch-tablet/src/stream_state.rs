//! Replicated advanced Stream state-service contracts.

use epoch_core::EventEnvelope;
use epoch_stream::{
    MAX_STREAM_CAPTURE_INTERVAL_MS, MAX_STREAM_CLUSTER_ID_BYTES, MAX_STREAM_IDENTIFIER_BYTES,
    MAX_STREAM_REPLICATION_BATCH_RECORDS, MAX_STREAM_TRANSACTION_RECORDS,
    MIN_STREAM_CAPTURE_INTERVAL_MS, StreamCaptureArtifact, StreamCaptureFormat,
    StreamCaptureMaintenanceOutcome, StreamCaptureScheduleObservation, StreamCompactionReport,
    StreamOffsetCommit, StreamProducerAppendOutcome, StreamReplicationBatch,
    StreamReplicationOutcome, StreamTierObject, StreamTransactionObservation,
};
use serde::{Deserialize, Serialize};

use crate::{
    StreamTabletWriteEvidence, TabletError, TabletResult,
    common::{deserialize_u64_from_number_or_decimal, serialize_u64_as_decimal},
};

pub const STREAM_TABLET_STATE_COMMAND_FORMAT_VERSION: u16 = 7;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum StreamStateCommand {
    AppendIdempotent {
        producer_id: String,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        producer_epoch: u64,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        sequence: u64,
        partition: u32,
        envelope: Box<EventEnvelope>,
    },
    BeginTransaction {
        transaction_id: String,
        producer_id: String,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        producer_epoch: u64,
    },
    AppendTransaction {
        transaction_id: String,
        producer_id: String,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        producer_epoch: u64,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        sequence: u64,
        partition: u32,
        envelopes: Vec<EventEnvelope>,
    },
    CommitTransaction {
        transaction_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offset_commit: Option<StreamOffsetCommit>,
    },
    AbortTransaction {
        transaction_id: String,
    },
    Compact {
        partition: u32,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        tombstone_retention_ms: u64,
    },
    TierPrefix {
        partition: u32,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        before_offset: u64,
        max_records: usize,
    },
    Capture {
        capture_id: String,
        partition: u32,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        first_offset: u64,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        end_offset: u64,
        format: StreamCaptureFormat,
    },
    ConfigureCaptureSchedule {
        schedule_id: String,
        partition: u32,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        interval_ms: u64,
        format: StreamCaptureFormat,
    },
    MaintainCaptureSchedule {
        schedule_id: String,
    },
    Replicate {
        local_partition: u32,
        batch: StreamReplicationBatch,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum StreamTabletStateResult {
    ProducerAppend(StreamProducerAppendOutcome),
    Transaction(StreamTransactionObservation),
    Compaction(StreamCompactionReport),
    Tier(Option<StreamTierObject>),
    Capture(StreamCaptureArtifact),
    CaptureSchedule(StreamCaptureScheduleObservation),
    CaptureMaintenance(StreamCaptureMaintenanceOutcome),
    Replication(StreamReplicationOutcome),
    Rejected(StreamTabletStateRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletStateRejectionCode {
    AlreadyExists,
    NotFound,
    InvalidArgument,
    Conflict,
    Fenced,
    Capacity,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamTabletStateRejection {
    pub code: StreamTabletStateRejectionCode,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTabletStateDisposition {
    New,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamTabletStateReceipt {
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
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub applied_at_ms: u64,
    pub result: StreamTabletStateResult,
    pub write_evidence: StreamTabletWriteEvidence,
    pub durable_voter_acks: u16,
    pub disposition: StreamTabletStateDisposition,
}

#[allow(
    clippy::too_many_lines,
    reason = "the validator exhaustively pins every variant of the versioned wire protocol"
)]
pub(crate) fn validate_stream_state_command(command: &StreamStateCommand) -> TabletResult<()> {
    match command {
        StreamStateCommand::AppendIdempotent {
            producer_id,
            producer_epoch,
            partition,
            envelope,
            ..
        } => {
            validate_identifier("producer ID", producer_id)?;
            validate_epoch(*producer_epoch)?;
            validate_partition(*partition)?;
            if envelope.transaction_id.is_some() {
                return Err(TabletError::InvalidCommand(
                    "idempotent append cannot include a transaction_id".into(),
                ));
            }
            envelope.validate()?;
        }
        StreamStateCommand::BeginTransaction {
            transaction_id,
            producer_id,
            producer_epoch,
        } => {
            validate_identifier("transaction ID", transaction_id)?;
            validate_identifier("producer ID", producer_id)?;
            validate_epoch(*producer_epoch)?;
        }
        StreamStateCommand::AppendTransaction {
            transaction_id,
            producer_id,
            producer_epoch,
            partition,
            envelopes,
            ..
        } => {
            validate_identifier("transaction ID", transaction_id)?;
            validate_identifier("producer ID", producer_id)?;
            validate_epoch(*producer_epoch)?;
            validate_partition(*partition)?;
            if envelopes.is_empty() || envelopes.len() > MAX_STREAM_TRANSACTION_RECORDS {
                return Err(TabletError::InvalidCommand(format!(
                    "transaction append must contain between 1 and {MAX_STREAM_TRANSACTION_RECORDS} records"
                )));
            }
            for envelope in envelopes {
                envelope.validate()?;
            }
        }
        StreamStateCommand::CommitTransaction {
            transaction_id,
            offset_commit,
        } => {
            validate_identifier("transaction ID", transaction_id)?;
            if let Some(offset) = offset_commit {
                validate_identifier("consumer group", &offset.group)?;
                validate_partition(offset.partition)?;
            }
        }
        StreamStateCommand::AbortTransaction { transaction_id } => {
            validate_identifier("transaction ID", transaction_id)?;
        }
        StreamStateCommand::Compact {
            partition,
            tombstone_retention_ms,
        } => {
            validate_partition(*partition)?;
            if *tombstone_retention_ms == 0 {
                return Err(TabletError::InvalidCommand(
                    "tombstone_retention_ms must be non-zero".into(),
                ));
            }
        }
        StreamStateCommand::TierPrefix {
            partition,
            max_records,
            ..
        } => {
            validate_partition(*partition)?;
            if !(1..=1_024).contains(max_records) {
                return Err(TabletError::InvalidCommand(
                    "tier max_records must be between 1 and 1024".into(),
                ));
            }
        }
        StreamStateCommand::Capture {
            capture_id,
            partition,
            first_offset,
            end_offset,
            ..
        } => {
            validate_identifier("capture ID", capture_id)?;
            validate_partition(*partition)?;
            if first_offset > end_offset {
                return Err(TabletError::InvalidCommand(
                    "capture offset range is inverted".into(),
                ));
            }
        }
        StreamStateCommand::ConfigureCaptureSchedule {
            schedule_id,
            partition,
            interval_ms,
            ..
        } => {
            validate_bounded_identifier(
                "capture schedule ID",
                schedule_id,
                MAX_STREAM_CLUSTER_ID_BYTES,
            )?;
            validate_partition(*partition)?;
            if !(MIN_STREAM_CAPTURE_INTERVAL_MS..=MAX_STREAM_CAPTURE_INTERVAL_MS)
                .contains(interval_ms)
            {
                return Err(TabletError::InvalidCommand(format!(
                    "capture interval must be between {MIN_STREAM_CAPTURE_INTERVAL_MS} and {MAX_STREAM_CAPTURE_INTERVAL_MS} milliseconds"
                )));
            }
        }
        StreamStateCommand::MaintainCaptureSchedule { schedule_id } => {
            validate_bounded_identifier(
                "capture schedule ID",
                schedule_id,
                MAX_STREAM_CLUSTER_ID_BYTES,
            )?;
        }
        StreamStateCommand::Replicate {
            local_partition,
            batch,
        } => {
            validate_partition(*local_partition)?;
            if batch.records.is_empty()
                || batch.records.len() > MAX_STREAM_REPLICATION_BATCH_RECORDS
            {
                return Err(TabletError::InvalidCommand(format!(
                    "replication batch must contain between 1 and {MAX_STREAM_REPLICATION_BATCH_RECORDS} records"
                )));
            }
            for record in &batch.records {
                record.envelope.validate()?;
            }
        }
    }
    Ok(())
}

fn validate_identifier(label: &str, value: &str) -> TabletResult<()> {
    validate_bounded_identifier(label, value, MAX_STREAM_IDENTIFIER_BYTES)
}

fn validate_bounded_identifier(label: &str, value: &str, max_bytes: usize) -> TabletResult<()> {
    if value.trim().is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(TabletError::InvalidCommand(format!(
            "{label} must contain 1 to {max_bytes} non-control UTF-8 bytes"
        )));
    }
    Ok(())
}

fn validate_epoch(epoch: u64) -> TabletResult<()> {
    if epoch == 0 {
        return Err(TabletError::InvalidCommand(
            "producer_epoch must be non-zero".into(),
        ));
    }
    Ok(())
}

fn validate_partition(partition: u32) -> TabletResult<()> {
    if partition != 0 {
        return Err(TabletError::InvalidCommand(
            "a Stream tablet command must target its local partition 0".into(),
        ));
    }
    Ok(())
}
