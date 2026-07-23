//! Public Queue tablet receipts, outcomes, deliveries, and history records.

use std::collections::BTreeMap;

use epoch_core::{EpochError, EventEnvelope};
use epoch_queue::QueueCounts;
use serde::Serialize;

use crate::TabletWriteEvidence;
use crate::common::serialize_u64_as_decimal;

pub type QueueTabletWriteEvidence = TabletWriteEvidence;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueTabletDisposition {
    New,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum QueueTabletOutcome {
    Applied {
        result: QueueTabletOperationResult,
    },
    Rejected {
        code: QueueTabletRejectionCode,
        detail: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueTabletRejectionCode {
    AlreadyExists,
    NotFound,
    InvalidArgument,
    Conflict,
    Fenced,
    Capacity,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueueTabletEnvelope {
    pub id: String,
    pub source: String,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub content_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    pub payload: serde_json::Value,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    pub deliver_at_ms: Option<u64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    pub ttl_ms: Option<u64>,
    pub priority: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedupe_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    pub extensions: BTreeMap<String, serde_json::Value>,
}

impl From<EventEnvelope> for QueueTabletEnvelope {
    fn from(envelope: EventEnvelope) -> Self {
        Self {
            id: envelope.id,
            source: envelope.source,
            event_type: envelope.event_type,
            subject: envelope.subject,
            time_ms: envelope.time_ms,
            key: envelope.key,
            headers: envelope.headers,
            content_type: envelope.content_type,
            schema_ref: envelope.schema_ref,
            traceparent: envelope.traceparent,
            payload: envelope.payload,
            deliver_at_ms: envelope.deliver_at_ms,
            ttl_ms: envelope.ttl_ms,
            priority: envelope.priority,
            dedupe_id: envelope.dedupe_id,
            transaction_id: envelope.transaction_id,
            extensions: envelope.extensions,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueueTabletDelivery {
    pub message_id: String,
    pub envelope: QueueTabletEnvelope,
    pub attempt: u32,
    pub lease_token: String,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub lease_deadline_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct QueueTabletCounts {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub ready: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub scheduled: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub in_flight: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub acknowledged: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub expired: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub dead_lettered: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueueTabletOperationResult {
    Enqueued {
        message_id: String,
        duplicate: bool,
    },
    Acquired {
        deliveries: Vec<QueueTabletDelivery>,
        new_dead_letter_history_ids: Vec<String>,
    },
    Acknowledged {
        message_id: String,
    },
    LeaseExtended {
        message_id: String,
        lease_token: String,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        lease_deadline_ms: u64,
    },
    Released {
        message_id: String,
        #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
        dead_letter_history_id: Option<u64>,
    },
    Nacked {
        message_id: String,
        #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
        dead_letter_history_id: Option<u64>,
    },
    DeadLettered {
        message_id: String,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        dead_letter_history_id: u64,
    },
    Redriven {
        message_id: String,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        dead_letter_history_id: u64,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        redrive_history_id: u64,
    },
    Maintained {
        counts: QueueTabletCounts,
        new_dead_letter_history_ids: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueueTabletReceipt {
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
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub applied_at_ms: u64,
    pub write_evidence: QueueTabletWriteEvidence,
    pub durable_voter_acks: u16,
    pub disposition: QueueTabletDisposition,
    pub outcome: QueueTabletOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueueTabletDeadLetterHistory {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub history_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub recorded_term: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub recorded_commit_index: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub source_proposal_id: u64,
    pub dead_letter: QueueTabletDeadLetter,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueueTabletDeadLetter {
    pub message_id: String,
    pub envelope: QueueTabletEnvelope,
    pub reason: String,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub original_enqueued_at_ms: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub dead_lettered_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueueTabletRedriveHistory {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub history_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub dead_letter_history_id: u64,
    pub message_id: String,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub source_proposal_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub recorded_term: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub recorded_commit_index: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub redriven_at_ms: u64,
}

impl TryFrom<QueueCounts> for QueueTabletCounts {
    type Error = EpochError;

    fn try_from(counts: QueueCounts) -> Result<Self, Self::Error> {
        fn convert(value: usize) -> Result<u64, EpochError> {
            u64::try_from(value).map_err(|_| EpochError::Internal("queue count exceeds u64".into()))
        }

        Ok(Self {
            ready: convert(counts.ready)?,
            scheduled: convert(counts.scheduled)?,
            in_flight: convert(counts.in_flight)?,
            acknowledged: convert(counts.acknowledged)?,
            expired: convert(counts.expired)?,
            dead_lettered: convert(counts.dead_lettered)?,
        })
    }
}

impl From<epoch_queue::DeadLetter> for QueueTabletDeadLetter {
    fn from(dead_letter: epoch_queue::DeadLetter) -> Self {
        Self {
            message_id: dead_letter.message_id,
            envelope: dead_letter.envelope.into(),
            reason: dead_letter.reason,
            original_enqueued_at_ms: dead_letter.original_enqueued_at_ms,
            dead_lettered_at_ms: dead_letter.dead_lettered_at_ms,
            attempts: dead_letter.attempts,
            last_error: dead_letter.last_error,
        }
    }
}

pub(super) fn history_ids_as_decimal(ids: &[u64]) -> Vec<String> {
    ids.iter().map(u64::to_string).collect()
}

#[allow(
    clippy::ref_option,
    clippy::trivially_copy_pass_by_ref,
    reason = "serde serialize_with requires the field's shared-reference signature"
)]
fn serialize_optional_u64_as_decimal<S>(
    value: &Option<u64>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}
