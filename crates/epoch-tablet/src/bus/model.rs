//! Public Event Bus tablet receipts and deterministic outcomes.

use serde::Serialize;

use crate::TabletWriteEvidence;
use crate::common::serialize_u64_as_decimal;

pub type BusTabletWriteEvidence = TabletWriteEvidence;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BusTabletDisposition {
    New,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BusTabletOperationResult {
    SubscriptionUpserted {
        name: String,
        replaced: bool,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        route_plan_version: u64,
    },
    SubscriptionRemoved {
        name: String,
        removed: bool,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        route_plan_version: u64,
    },
    Published {
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        position: u64,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        route_plan_version: u64,
        delivery_count: usize,
        delivery_plan_digest: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BusTabletReceipt {
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
    pub write_evidence: BusTabletWriteEvidence,
    pub durable_voter_acks: u16,
    pub disposition: BusTabletDisposition,
    pub outcome: BusTabletOutcome,
}
