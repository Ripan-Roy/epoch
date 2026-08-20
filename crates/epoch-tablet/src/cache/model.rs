//! Public Cache tablet observations, receipts, outcomes, and lock fences.

use std::collections::{BTreeMap, BTreeSet};

use epoch_cache::{
    CacheBitmap, CacheBloomFilter, CacheCardinality, CacheCuckooFilter, CacheGeoIndex, CacheItem,
    CacheJsonDocument, CacheJsonIndex, CacheStorageClass, CacheValue, CacheVectorIndex,
};
use serde::{Deserialize, Deserializer, Serialize};

use crate::TabletWriteEvidence;
use crate::common::{
    deserialize_optional_u64_from_number_or_decimal, deserialize_u64_from_number_or_decimal,
    serialize_u64_as_decimal,
};

pub type CacheTabletWriteEvidence = TabletWriteEvidence;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheTabletDisposition {
    New,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheTabletItem {
    #[serde(
        serialize_with = "serialize_cache_value",
        deserialize_with = "deserialize_cache_value"
    )]
    pub value: CacheValue,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub version: u64,
    #[serde(
        serialize_with = "serialize_optional_u64_as_decimal",
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
    )]
    pub expires_at_ms: Option<u64>,
    pub storage_class: CacheStorageClass,
}

impl From<CacheItem> for CacheTabletItem {
    fn from(item: CacheItem) -> Self {
        Self {
            value: item.value,
            version: item.version,
            expires_at_ms: item.expires_at_ms,
            storage_class: item.storage_class,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheTabletObservation {
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub shard_revision: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub observed_at_ms: u64,
    pub item: Option<CacheTabletItem>,
}

/// Downstream-comparable fence scoped to one resource, shard, and lock key.
///
/// Consumers compare `(tablet_epoch, acquisition_index)` lexicographically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CacheLockFencingToken {
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub tablet_epoch: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub acquisition_index: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CacheTransactionMutationResult {
    Set {
        key: String,
        item: CacheTabletItem,
    },
    Deleted {
        key: String,
        deleted: bool,
        #[serde(
            serialize_with = "serialize_optional_u64_as_decimal",
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        previous_version: Option<u64>,
    },
    ComparedAndSet {
        key: String,
        item: CacheTabletItem,
    },
    Incremented {
        key: String,
        #[serde(
            serialize_with = "serialize_i64_as_decimal",
            deserialize_with = "deserialize_i64_from_number_or_decimal"
        )]
        value: i64,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        version: u64,
        #[serde(
            serialize_with = "serialize_optional_u64_as_decimal",
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        expires_at_ms: Option<u64>,
    },
    Transformed {
        key: String,
        item: CacheTabletItem,
        changed: bool,
        result: serde_json::Value,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CacheTabletOperationResult {
    Set {
        key: String,
        item: CacheTabletItem,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        evicted_keys: Vec<String>,
    },
    Deleted {
        key: String,
        deleted: bool,
        #[serde(
            serialize_with = "serialize_optional_u64_as_decimal",
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        previous_version: Option<u64>,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        revision: u64,
    },
    ComparedAndSet {
        key: String,
        item: CacheTabletItem,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        evicted_keys: Vec<String>,
    },
    Incremented {
        key: String,
        #[serde(
            serialize_with = "serialize_i64_as_decimal",
            deserialize_with = "deserialize_i64_from_number_or_decimal"
        )]
        value: i64,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        version: u64,
        #[serde(
            serialize_with = "serialize_optional_u64_as_decimal",
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        expires_at_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        evicted_keys: Vec<String>,
    },
    Accessed {
        key: String,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        revision: u64,
        item: Option<CacheTabletItem>,
    },
    Transformed {
        key: String,
        item: CacheTabletItem,
        changed: bool,
        result: serde_json::Value,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        evicted_keys: Vec<String>,
    },
    TransactionCommitted {
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        revision: u64,
        results: Vec<CacheTransactionMutationResult>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        evicted_keys: Vec<String>,
    },
    LockAcquired {
        lock_key: String,
        owner: String,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        owner_epoch: u64,
        fencing_token: CacheLockFencingToken,
        lease_token: String,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        lease_generation: u64,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        lease_deadline_ms: u64,
    },
    LockRenewed {
        lock_key: String,
        fencing_token: CacheLockFencingToken,
        lease_token: String,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        lease_generation: u64,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        lease_deadline_ms: u64,
    },
    LockReleased {
        lock_key: String,
        fencing_token: CacheLockFencingToken,
    },
    Maintained {
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        cache_revision: u64,
        expired_keys: Vec<String>,
        expired_locks: Vec<String>,
    },
    Restored {
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        revision: u64,
        #[serde(
            serialize_with = "serialize_u64_as_decimal",
            deserialize_with = "deserialize_u64_from_number_or_decimal"
        )]
        restored_from_revision: u64,
        restored_keys: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheTabletReceipt {
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
    Bitmap(&'a CacheBitmap),
    Cardinality(&'a CacheCardinality),
    Bloom(&'a CacheBloomFilter),
    Cuckoo(&'a CacheCuckooFilter),
    Geo(&'a CacheGeoIndex),
    Json(&'a CacheJsonDocument),
    JsonIndex(&'a CacheJsonIndex),
    Vector(&'a CacheVectorIndex),
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum OwnedBrowserSafeCacheValue {
    String(String),
    Blob(Vec<u8>),
    Counter(String),
    Hash(BTreeMap<String, String>),
    List(Vec<String>),
    Set(BTreeSet<String>),
    SortedSet(BTreeMap<String, f64>),
    Bitmap(CacheBitmap),
    Cardinality(CacheCardinality),
    Bloom(CacheBloomFilter),
    Cuckoo(CacheCuckooFilter),
    Geo(CacheGeoIndex),
    Json(CacheJsonDocument),
    JsonIndex(CacheJsonIndex),
    Vector(CacheVectorIndex),
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
        CacheValue::Bitmap(value) => BrowserSafeCacheValue::Bitmap(value),
        CacheValue::Cardinality(value) => BrowserSafeCacheValue::Cardinality(value),
        CacheValue::Bloom(value) => BrowserSafeCacheValue::Bloom(value),
        CacheValue::Cuckoo(value) => BrowserSafeCacheValue::Cuckoo(value),
        CacheValue::Geo(value) => BrowserSafeCacheValue::Geo(value),
        CacheValue::Json(value) => BrowserSafeCacheValue::Json(value),
        CacheValue::JsonIndex(value) => BrowserSafeCacheValue::JsonIndex(value),
        CacheValue::Vector(value) => BrowserSafeCacheValue::Vector(value),
    };
    value.serialize(serializer)
}

fn deserialize_cache_value<'de, D>(deserializer: D) -> Result<CacheValue, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(
        match OwnedBrowserSafeCacheValue::deserialize(deserializer)? {
            OwnedBrowserSafeCacheValue::String(value) => CacheValue::String(value),
            OwnedBrowserSafeCacheValue::Blob(value) => CacheValue::Blob(value),
            OwnedBrowserSafeCacheValue::Counter(value) => {
                CacheValue::Counter(value.parse().map_err(serde::de::Error::custom)?)
            }
            OwnedBrowserSafeCacheValue::Hash(value) => CacheValue::Hash(value),
            OwnedBrowserSafeCacheValue::List(value) => CacheValue::List(value),
            OwnedBrowserSafeCacheValue::Set(value) => CacheValue::Set(value),
            OwnedBrowserSafeCacheValue::SortedSet(value) => CacheValue::SortedSet(value),
            OwnedBrowserSafeCacheValue::Bitmap(value) => CacheValue::Bitmap(value),
            OwnedBrowserSafeCacheValue::Cardinality(value) => CacheValue::Cardinality(value),
            OwnedBrowserSafeCacheValue::Bloom(value) => CacheValue::Bloom(value),
            OwnedBrowserSafeCacheValue::Cuckoo(value) => CacheValue::Cuckoo(value),
            OwnedBrowserSafeCacheValue::Geo(value) => CacheValue::Geo(value),
            OwnedBrowserSafeCacheValue::Json(value) => CacheValue::Json(value),
            OwnedBrowserSafeCacheValue::JsonIndex(value) => CacheValue::JsonIndex(value),
            OwnedBrowserSafeCacheValue::Vector(value) => CacheValue::Vector(value),
        },
    )
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

fn deserialize_i64_from_number_or_decimal<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Representation {
        Number(i64),
        Decimal(String),
    }

    match Representation::deserialize(deserializer)? {
        Representation::Number(value) => Ok(value),
        Representation::Decimal(value) => value.parse().map_err(serde::de::Error::custom),
    }
}
