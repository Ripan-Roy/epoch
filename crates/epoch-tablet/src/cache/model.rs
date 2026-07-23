//! Public Cache tablet observations, receipts, outcomes, and lock fences.

use std::collections::{BTreeMap, BTreeSet};

use epoch_cache::{CacheItem, CacheValue};
use serde::Serialize;

use crate::TabletWriteEvidence;
use crate::common::serialize_u64_as_decimal;

pub type CacheTabletWriteEvidence = TabletWriteEvidence;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheTabletDisposition {
    New,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CacheTabletOutcome {
    Applied {
        result: CacheTabletOperationResult,
    },
    Rejected {
        code: CacheTabletRejectionCode,
        detail: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheTabletRejectionCode {
    AlreadyExists,
    NotFound,
    InvalidArgument,
    Conflict,
    Fenced,
    Capacity,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CacheTabletItem {
    #[serde(serialize_with = "serialize_cache_value")]
    pub value: CacheValue,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub version: u64,
    #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
    pub expires_at_ms: Option<u64>,
}

impl From<CacheItem> for CacheTabletItem {
    fn from(item: CacheItem) -> Self {
        Self {
            value: item.value,
            version: item.version,
            expires_at_ms: item.expires_at_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CacheTabletObservation {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub shard_revision: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub observed_at_ms: u64,
    pub item: Option<CacheTabletItem>,
}

/// Downstream-comparable fence scoped to one resource, shard, and lock key.
///
/// Consumers compare `(tablet_epoch, acquisition_index)` lexicographically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct CacheLockFencingToken {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub tablet_epoch: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub acquisition_index: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CacheTransactionMutationResult {
    Set {
        key: String,
        item: CacheTabletItem,
    },
    Deleted {
        key: String,
        deleted: bool,
        #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
        previous_version: Option<u64>,
    },
    ComparedAndSet {
        key: String,
        item: CacheTabletItem,
    },
    Incremented {
        key: String,
        #[serde(serialize_with = "serialize_i64_as_decimal")]
        value: i64,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        version: u64,
        #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
        expires_at_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CacheTabletOperationResult {
    Set {
        key: String,
        item: CacheTabletItem,
    },
    Deleted {
        key: String,
        deleted: bool,
        #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
        previous_version: Option<u64>,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        revision: u64,
    },
    ComparedAndSet {
        key: String,
        item: CacheTabletItem,
    },
    Incremented {
        key: String,
        #[serde(serialize_with = "serialize_i64_as_decimal")]
        value: i64,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        version: u64,
        #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
        expires_at_ms: Option<u64>,
    },
    TransactionCommitted {
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        revision: u64,
        results: Vec<CacheTransactionMutationResult>,
    },
    LockAcquired {
        lock_key: String,
        owner: String,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        owner_epoch: u64,
        fencing_token: CacheLockFencingToken,
        lease_token: String,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        lease_generation: u64,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        lease_deadline_ms: u64,
    },
    LockRenewed {
        lock_key: String,
        fencing_token: CacheLockFencingToken,
        lease_token: String,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        lease_generation: u64,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        lease_deadline_ms: u64,
    },
    LockReleased {
        lock_key: String,
        fencing_token: CacheLockFencingToken,
    },
    Maintained {
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        cache_revision: u64,
        expired_keys: Vec<String>,
        expired_locks: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CacheTabletReceipt {
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
    pub write_evidence: CacheTabletWriteEvidence,
    pub durable_voter_acks: u16,
    pub disposition: CacheTabletDisposition,
    pub outcome: CacheTabletOutcome,
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

#[derive(Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum BrowserSafeCacheValue<'a> {
    String(&'a str),
    Blob(&'a [u8]),
    Counter(String),
    Hash(&'a BTreeMap<String, String>),
    List(&'a [String]),
    Set(&'a BTreeSet<String>),
    SortedSet(&'a BTreeMap<String, f64>),
}

fn serialize_cache_value<S>(value: &CacheValue, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let value = match value {
        CacheValue::String(value) => BrowserSafeCacheValue::String(value),
        CacheValue::Blob(value) => BrowserSafeCacheValue::Blob(value),
        CacheValue::Counter(value) => BrowserSafeCacheValue::Counter(value.to_string()),
        CacheValue::Hash(value) => BrowserSafeCacheValue::Hash(value),
        CacheValue::List(value) => BrowserSafeCacheValue::List(value),
        CacheValue::Set(value) => BrowserSafeCacheValue::Set(value),
        CacheValue::SortedSet(value) => BrowserSafeCacheValue::SortedSet(value),
    };
    value.serialize(serializer)
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde serialize_with requires a shared reference"
)]
fn serialize_i64_as_decimal<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}
