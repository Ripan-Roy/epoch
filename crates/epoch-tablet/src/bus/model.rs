//! Public Event Bus tablet receipts and deterministic outcomes.

use std::collections::BTreeMap;

use epoch_bus::{DeliveryCounts, DeliveryLease, DeliveryStateKind, SubscriptionTarget};
use epoch_core::{EpochError, EventEnvelope};
use serde::{Deserialize, Serialize};

use crate::TabletWriteEvidence;
use crate::common::{
    deserialize_optional_u64_from_number_or_decimal, deserialize_u64_from_number_or_decimal,
    serialize_u64_as_decimal,
};

pub type BusTabletWriteEvidence = TabletWriteEvidence;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BusTabletDisposition {
    New,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BusTabletOutcome {
    Applied {
        result: BusTabletOperationResult,
    },
    Rejected {
        code: BusTabletRejectionCode,
        detail: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BusTabletRejectionCode {
    AlreadyExists,
    NotFound,
    InvalidArgument,
    Conflict,
    Fenced,
    Capacity,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BusTabletOperationResult {
    SubscriptionUpserted {
        name: String,
        replaced: bool,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        route_plan_version: u64,
    },
    SubscriptionRemoved {
        name: String,
        removed: bool,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        route_plan_version: u64,
    },
    Published {
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        position: u64,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        route_plan_version: u64,
        delivery_count: usize,
        delivery_plan_digest: String,
    },
    DeliveriesAcquired {
        deliveries: Vec<BusTabletDelivery>,
    },
    DeliveryAcknowledged {
        delivery_id: String,
    },
    DeliveryFailed {
        delivery_id: String,
        state: DeliveryStateKind,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            serialize_with = "serialize_optional_u64_as_decimal",
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        next_eligible_at_ms: Option<u64>,
    },
    DeliveryRejected {
        delivery_id: String,
        state: DeliveryStateKind,
    },
    DeliveriesMaintained {
        processed: u16,
        retried: u16,
        dead_lettered: u16,
        counts: BusTabletDeliveryCounts,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusTabletDelivery {
    pub delivery_id: String,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub publish_position: u64,
    pub subscription: String,
    pub target: SubscriptionTarget,
    pub envelope: BusTabletEnvelope,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub route_plan_version: u64,
    pub attempt: u32,
    pub lease_token: String,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub lease_deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusTabletEnvelope {
    pub id: String,
    pub source: String,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
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
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal",
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
    )]
    pub deliver_at_ms: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal",
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
    )]
    pub ttl_ms: Option<u64>,
    pub priority: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedupe_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    pub extensions: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusTabletDeliveryCounts {
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub pending: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub in_flight: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub acknowledged: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub dead_lettered: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusTabletReceipt {
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
    pub write_evidence: BusTabletWriteEvidence,
    pub durable_voter_acks: u16,
    pub disposition: BusTabletDisposition,
    pub outcome: BusTabletOutcome,
}

impl From<EventEnvelope> for BusTabletEnvelope {
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

impl From<DeliveryLease> for BusTabletDelivery {
    fn from(delivery: DeliveryLease) -> Self {
        Self {
            delivery_id: delivery.delivery_id,
            publish_position: delivery.publish_position,
            subscription: delivery.subscription,
            target: delivery.target,
            envelope: delivery.envelope.into(),
            route_plan_version: delivery.route_plan_version,
            attempt: delivery.attempt,
            lease_token: delivery.lease_token,
            lease_deadline_ms: delivery.lease_deadline_ms,
        }
    }
}

impl TryFrom<DeliveryCounts> for BusTabletDeliveryCounts {
    type Error = EpochError;

    fn try_from(counts: DeliveryCounts) -> Result<Self, Self::Error> {
        fn convert(value: usize) -> Result<u64, EpochError> {
            u64::try_from(value)
                .map_err(|_| EpochError::Internal("Event Bus delivery count exceeds u64".into()))
        }

        Ok(Self {
            pending: convert(counts.pending)?,
            in_flight: convert(counts.in_flight)?,
            acknowledged: convert(counts.acknowledged)?,
            dead_lettered: convert(counts.dead_lettered)?,
        })
    }
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
