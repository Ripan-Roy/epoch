//! Deterministic, shard-local Cache mutations for replicated tablet runtimes.
//!
//! This state machine is deliberately additive to the legacy memory-first
//! [`crate::Cache`]. It supplies a pure read path, non-ABA item versions, and a
//! bounded atomic mutation boundary without changing standalone behavior.

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{EpochError, EpochResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{CacheItem, CacheValue, SetOptions};

pub const MAX_CACHE_ATOMIC_OPERATIONS: usize = 128;
pub const MAX_CACHE_MAINTENANCE_KEYS: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheTransaction {
    #[serde(default)]
    pub expected_revision: Option<u64>,
    pub operations: Vec<CacheMutation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CacheMutation {
    Set {
        key: String,
        value: CacheValue,
        #[serde(default)]
        options: SetOptions,
    },
    Delete {
        key: String,
        #[serde(default)]
        expected_version: Option<u64>,
    },
    Increment {
        key: String,
        delta: i64,
        #[serde(default)]
        expected_version: Option<u64>,
    },
    CompareAndSet {
        key: String,
        expected_version: u64,
        value: CacheValue,
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
}

impl CacheMutation {
    fn key(&self) -> &str {
        match self {
            Self::Set { key, .. }
            | Self::Delete { key, .. }
            | Self::Increment { key, .. }
            | Self::CompareAndSet { key, .. } => key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheTransactionResult {
    pub revision: u64,
    pub results: Vec<CacheMutationResult>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CacheMutationResult {
    Set {
        item: CacheItem,
    },
    Delete {
        deleted: bool,
        previous_version: Option<u64>,
    },
    Increment {
        value: i64,
        version: u64,
        expires_at_ms: Option<u64>,
    },
    CompareAndSet {
        item: CacheItem,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheObservation {
    pub revision: u64,
    pub item: Option<CacheItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheExpiryResult {
    pub revision: u64,
    pub expired_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct ReplicatedEntry {
    value: CacheValue,
    version: u64,
    expires_at_ms: Option<u64>,
}

impl ReplicatedEntry {
    fn is_live_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms
            .is_none_or(|deadline_ms| deadline_ms > now_ms)
    }

    fn item(&self) -> CacheItem {
        CacheItem {
            value: self.value.clone(),
            version: self.version,
            expires_at_ms: self.expires_at_ms,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheShard {
    max_entries: usize,
    default_ttl_ms: Option<u64>,
    revision: u64,
    entries: BTreeMap<String, ReplicatedEntry>,
}

impl CacheShard {
    /// Creates a deterministic Cache shard.
    ///
    /// The replicated boundary rejects a zero default TTL because it cannot
    /// create a logically live value. The legacy [`crate::Cache`] remains
    /// unchanged.
    pub fn new(max_entries: usize, default_ttl_ms: Option<u64>) -> EpochResult<Self> {
        if max_entries == 0 {
            return Err(EpochError::InvalidArgument(
                "cache shard max_entries must be greater than zero".into(),
            ));
        }
        if default_ttl_ms == Some(0) {
            return Err(EpochError::InvalidArgument(
                "cache shard default TTL must be greater than zero".into(),
            ));
        }
        Ok(Self {
            max_entries,
            default_ttl_ms,
            revision: 0,
            entries: BTreeMap::new(),
        })
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Observes a key without reclaiming expired storage or updating metadata.
    pub fn observe(&self, key: &str, now_ms: u64) -> CacheObservation {
        let item = self
            .entries
            .get(key)
            .filter(|entry| entry.is_live_at(now_ms))
            .map(ReplicatedEntry::item);
        CacheObservation {
            revision: self.revision,
            item,
        }
    }

    pub fn transact(
        &mut self,
        transaction: CacheTransaction,
        now_ms: u64,
    ) -> EpochResult<CacheTransactionResult> {
        Self::validate_transaction(&transaction)?;
        if let Some(expected_revision) = transaction.expected_revision
            && expected_revision != self.revision
        {
            return Err(EpochError::Conflict(format!(
                "cache shard revision mismatch: expected {expected_revision}, current {}",
                self.revision
            )));
        }

        // Expired records are logically absent. Reclamation is staged with the
        // mutation, but is not committed for an otherwise no-op transaction.
        let mut candidate: BTreeMap<_, _> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.is_live_at(now_ms))
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect();
        let changed = validate_operations(
            &candidate,
            &transaction.operations,
            self.default_ttl_ms,
            now_ms,
        )?;
        let resulting_len = resulting_len(&candidate, &transaction.operations);
        if resulting_len > self.max_entries {
            return Err(EpochError::Capacity(format!(
                "cache shard would contain {resulting_len} live entries; maximum is {}",
                self.max_entries
            )));
        }

        if !changed {
            return Ok(CacheTransactionResult {
                revision: self.revision,
                results: transaction
                    .operations
                    .into_iter()
                    .map(|operation| match operation {
                        CacheMutation::Delete { .. } => CacheMutationResult::Delete {
                            deleted: false,
                            previous_version: None,
                        },
                        CacheMutation::Set { .. }
                        | CacheMutation::Increment { .. }
                        | CacheMutation::CompareAndSet { .. } => {
                            unreachable!("validated non-delete mutations always change state")
                        }
                    })
                    .collect(),
            });
        }

        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("cache shard revision is exhausted".into()))?;
        let results = apply_operations(
            &mut candidate,
            transaction.operations,
            self.default_ttl_ms,
            now_ms,
            next_revision,
        )?;
        self.entries = candidate;
        self.revision = next_revision;
        Ok(CacheTransactionResult {
            revision: next_revision,
            results,
        })
    }

    pub fn maintain_expiry(&mut self, now_ms: u64, limit: usize) -> EpochResult<CacheExpiryResult> {
        if limit > MAX_CACHE_MAINTENANCE_KEYS {
            return Err(EpochError::InvalidArgument(format!(
                "cache expiry limit {limit} exceeds maximum {MAX_CACHE_MAINTENANCE_KEYS}"
            )));
        }
        let mut candidates: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                entry
                    .expires_at_ms
                    .filter(|deadline_ms| *deadline_ms <= now_ms)
                    .map(|deadline_ms| (deadline_ms, key.clone()))
            })
            .collect();
        candidates.sort_unstable();
        let expired_keys: Vec<_> = candidates
            .into_iter()
            .take(limit)
            .map(|(_, key)| key)
            .collect();
        if expired_keys.is_empty() {
            return Ok(CacheExpiryResult {
                revision: self.revision,
                expired_keys,
            });
        }

        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("cache shard revision is exhausted".into()))?;
        for key in &expired_keys {
            self.entries.remove(key);
        }
        self.revision = next_revision;
        Ok(CacheExpiryResult {
            revision: next_revision,
            expired_keys,
        })
    }

    /// Returns a deterministic replay-drift checksum of the complete shard.
    pub fn recovery_state_checksum(&self) -> u32 {
        let mut checksum = CanonicalChecksum::new();
        self.encode_recovery_state(&mut checksum);
        checksum.finish()
    }

    /// Returns a cryptographic replay-drift digest of the complete shard.
    ///
    /// The digest consumes the same domain-separated canonical byte stream as
    /// [`Self::recovery_state_checksum`], so the compact checksum remains
    /// useful for observation while transition proofs commit to full state.
    pub fn recovery_state_digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        self.encode_recovery_state(&mut digest);
        digest.finalize().into()
    }

    fn encode_recovery_state(&self, sink: &mut dyn CanonicalSink) {
        let mut encoder = CanonicalEncoder::new(sink);
        encoder.bytes(b"epoch/cache-shard/recovery/v1\0");
        encoder.u64(u64::try_from(self.max_entries).unwrap_or(u64::MAX));
        encoder.option_u64(self.default_ttl_ms);
        encoder.u64(self.revision);
        encoder.u64(u64::try_from(self.entries.len()).unwrap_or(u64::MAX));
        for (key, entry) in &self.entries {
            encoder.length_prefixed(key.as_bytes());
            encoder.cache_value(&entry.value);
            encoder.u64(entry.version);
            encoder.option_u64(entry.expires_at_ms);
        }
    }

    fn validate_transaction(transaction: &CacheTransaction) -> EpochResult<()> {
        let operation_count = transaction.operations.len();
        if operation_count == 0 {
            return Err(EpochError::InvalidArgument(
                "cache transaction requires at least one operation".into(),
            ));
        }
        if operation_count > MAX_CACHE_ATOMIC_OPERATIONS {
            return Err(EpochError::InvalidArgument(format!(
                "cache transaction has {operation_count} operations; maximum is {MAX_CACHE_ATOMIC_OPERATIONS}"
            )));
        }
        let mut keys = BTreeSet::new();
        for operation in &transaction.operations {
            let key = operation.key();
            if key.is_empty() {
                return Err(EpochError::InvalidArgument(
                    "cache transaction keys must be nonempty".into(),
                ));
            }
            if !keys.insert(key) {
                return Err(EpochError::InvalidArgument(format!(
                    "cache transaction contains duplicate key: {key}"
                )));
            }
            if let CacheMutation::Set { options, .. } = operation
                && options.only_if_absent
                && options.only_if_present
            {
                return Err(EpochError::InvalidArgument(
                    "cache set cannot require both absence and presence".into(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_operations(
    entries: &BTreeMap<String, ReplicatedEntry>,
    operations: &[CacheMutation],
    default_ttl_ms: Option<u64>,
    now_ms: u64,
) -> EpochResult<bool> {
    let mut changed = false;
    for operation in operations {
        match operation {
            CacheMutation::Set {
                key,
                value,
                options,
            } => {
                let current = entries.get(key);
                if options.only_if_absent && current.is_some() {
                    return Err(EpochError::Conflict(format!(
                        "cache key already exists: {key}"
                    )));
                }
                if options.only_if_present && current.is_none() {
                    return Err(EpochError::NotFound(key.clone()));
                }
                validate_expected_version(key, options.expected_version, current)?;
                validate_cache_value(value)?;
                expiry_deadline(options.ttl_ms.or(default_ttl_ms), now_ms)?;
                changed = true;
            }
            CacheMutation::Delete {
                key,
                expected_version,
            } => {
                let current = entries.get(key);
                validate_expected_version(key, *expected_version, current)?;
                changed |= current.is_some();
            }
            CacheMutation::Increment {
                key,
                delta,
                expected_version,
            } => {
                let current = entries.get(key);
                validate_expected_version(key, *expected_version, current)?;
                if let Some(entry) = current {
                    let CacheValue::Counter(value) = entry.value else {
                        return Err(EpochError::Conflict(format!(
                            "cache value at {key} is not a counter"
                        )));
                    };
                    value
                        .checked_add(*delta)
                        .ok_or_else(|| EpochError::Capacity("counter overflow".into()))?;
                } else {
                    expiry_deadline(default_ttl_ms, now_ms)?;
                }
                changed = true;
            }
            CacheMutation::CompareAndSet {
                key,
                expected_version,
                value,
                ttl_ms,
            } => {
                validate_expected_version(key, Some(*expected_version), entries.get(key))?;
                validate_cache_value(value)?;
                expiry_deadline(ttl_ms.or(default_ttl_ms), now_ms)?;
                changed = true;
            }
        }
    }
    Ok(changed)
}

fn validate_expected_version(
    key: &str,
    expected_version: Option<u64>,
    current: Option<&ReplicatedEntry>,
) -> EpochResult<()> {
    if let Some(expected_version) = expected_version {
        let current_version = current.map_or(0, |entry| entry.version);
        if expected_version != current_version {
            return Err(EpochError::Conflict(format!(
                "cache version mismatch for {key}: expected {expected_version}, current {current_version}"
            )));
        }
    }
    Ok(())
}

fn resulting_len(
    entries: &BTreeMap<String, ReplicatedEntry>,
    operations: &[CacheMutation],
) -> usize {
    let mut len = entries.len();
    for operation in operations {
        match operation {
            CacheMutation::Set { key, .. }
            | CacheMutation::Increment { key, .. }
            | CacheMutation::CompareAndSet { key, .. } => {
                if !entries.contains_key(key) {
                    len = len.saturating_add(1);
                }
            }
            CacheMutation::Delete { key, .. } => {
                if entries.contains_key(key) {
                    len = len.saturating_sub(1);
                }
            }
        }
    }
    len
}

fn apply_operations(
    entries: &mut BTreeMap<String, ReplicatedEntry>,
    operations: Vec<CacheMutation>,
    default_ttl_ms: Option<u64>,
    now_ms: u64,
    revision: u64,
) -> EpochResult<Vec<CacheMutationResult>> {
    operations
        .into_iter()
        .map(|operation| -> EpochResult<CacheMutationResult> {
            Ok(match operation {
                CacheMutation::Set {
                    key,
                    value,
                    options,
                } => {
                    let expires_at_ms = expiry_deadline(options.ttl_ms.or(default_ttl_ms), now_ms)?;
                    let item = CacheItem {
                        value: value.clone(),
                        version: revision,
                        expires_at_ms,
                    };
                    entries.insert(
                        key,
                        ReplicatedEntry {
                            value,
                            version: revision,
                            expires_at_ms,
                        },
                    );
                    CacheMutationResult::Set { item }
                }
                CacheMutation::Delete { key, .. } => {
                    let previous_version = entries.remove(&key).map(|entry| entry.version);
                    CacheMutationResult::Delete {
                        deleted: previous_version.is_some(),
                        previous_version,
                    }
                }
                CacheMutation::Increment { key, delta, .. } => {
                    let (value, expires_at_ms) = if let Some(entry) = entries.get_mut(&key) {
                        let CacheValue::Counter(value) = &mut entry.value else {
                            unreachable!("counter type was validated before application")
                        };
                        *value = value
                            .checked_add(delta)
                            .expect("counter overflow was validated before application");
                        entry.version = revision;
                        (*value, entry.expires_at_ms)
                    } else {
                        let expires_at_ms = expiry_deadline(default_ttl_ms, now_ms)?;
                        entries.insert(
                            key,
                            ReplicatedEntry {
                                value: CacheValue::Counter(delta),
                                version: revision,
                                expires_at_ms,
                            },
                        );
                        (delta, expires_at_ms)
                    };
                    CacheMutationResult::Increment {
                        value,
                        version: revision,
                        expires_at_ms,
                    }
                }
                CacheMutation::CompareAndSet {
                    key, value, ttl_ms, ..
                } => {
                    let expires_at_ms = expiry_deadline(ttl_ms.or(default_ttl_ms), now_ms)?;
                    let item = CacheItem {
                        value: value.clone(),
                        version: revision,
                        expires_at_ms,
                    };
                    entries.insert(
                        key,
                        ReplicatedEntry {
                            value,
                            version: revision,
                            expires_at_ms,
                        },
                    );
                    CacheMutationResult::CompareAndSet { item }
                }
            })
        })
        .collect()
}

fn expiry_deadline(ttl_ms: Option<u64>, now_ms: u64) -> EpochResult<Option<u64>> {
    ttl_ms
        .map(|ttl_ms| {
            if ttl_ms == 0 {
                return Err(EpochError::InvalidArgument(
                    "cache TTL must be greater than zero".into(),
                ));
            }
            now_ms
                .checked_add(ttl_ms)
                .ok_or_else(|| EpochError::Capacity("cache expiry deadline overflow".into()))
        })
        .transpose()
}

fn validate_cache_value(value: &CacheValue) -> EpochResult<()> {
    if let CacheValue::SortedSet(members) = value
        && members.values().any(|score| !score.is_finite())
    {
        return Err(EpochError::InvalidArgument(
            "sorted-set scores must be finite".into(),
        ));
    }
    Ok(())
}

struct CanonicalChecksum {
    state: u32,
}

trait CanonicalSink {
    fn write(&mut self, bytes: &[u8]);
}

impl CanonicalChecksum {
    const fn new() -> Self {
        Self { state: u32::MAX }
    }

    const fn finish(self) -> u32 {
        !self.state
    }
}

impl CanonicalSink for CanonicalChecksum {
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            let mut value = self.state ^ u32::from(*byte);
            for _ in 0..8 {
                let mask = (value & 1).wrapping_neg();
                value = (value >> 1) ^ (0xedb8_8320 & mask);
            }
            self.state = value;
        }
    }
}

impl CanonicalSink for Sha256 {
    fn write(&mut self, bytes: &[u8]) {
        Digest::update(self, bytes);
    }
}

struct CanonicalEncoder<'a> {
    sink: &'a mut dyn CanonicalSink,
}

impl<'a> CanonicalEncoder<'a> {
    const fn new(sink: &'a mut dyn CanonicalSink) -> Self {
        Self { sink }
    }

    fn u8(&mut self, value: u8) {
        self.bytes(&[value]);
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes(&value.to_be_bytes());
    }

    fn option_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u64(value);
            }
            None => self.u8(0),
        }
    }

    fn length_prefixed(&mut self, value: &[u8]) {
        self.u64(u64::try_from(value.len()).unwrap_or(u64::MAX));
        self.bytes(value);
    }

    fn cache_value(&mut self, value: &CacheValue) {
        match value {
            CacheValue::String(value) => {
                self.u8(0);
                self.length_prefixed(value.as_bytes());
            }
            CacheValue::Blob(value) => {
                self.u8(1);
                self.length_prefixed(value);
            }
            CacheValue::Counter(value) => {
                self.u8(2);
                self.i64(*value);
            }
            CacheValue::Hash(value) => {
                self.u8(3);
                self.u64(u64::try_from(value.len()).unwrap_or(u64::MAX));
                for (key, value) in value {
                    self.length_prefixed(key.as_bytes());
                    self.length_prefixed(value.as_bytes());
                }
            }
            CacheValue::List(value) => {
                self.u8(4);
                self.u64(u64::try_from(value.len()).unwrap_or(u64::MAX));
                for item in value {
                    self.length_prefixed(item.as_bytes());
                }
            }
            CacheValue::Set(value) => {
                self.u8(5);
                self.u64(u64::try_from(value.len()).unwrap_or(u64::MAX));
                for item in value {
                    self.length_prefixed(item.as_bytes());
                }
            }
            CacheValue::SortedSet(value) => {
                self.u8(6);
                self.u64(u64::try_from(value.len()).unwrap_or(u64::MAX));
                for (member, score) in value {
                    self.length_prefixed(member.as_bytes());
                    self.u64(score.to_bits());
                }
            }
        }
    }

    fn bytes(&mut self, value: &[u8]) {
        self.sink.write(value);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use epoch_core::EpochError;

    use super::*;
    use crate::{CacheValue, SetOptions};

    fn set(key: &str, value: CacheValue) -> CacheMutation {
        CacheMutation::Set {
            key: key.to_owned(),
            value,
            options: SetOptions::default(),
        }
    }

    fn transaction(operations: Vec<CacheMutation>) -> CacheTransaction {
        CacheTransaction {
            expected_revision: None,
            operations,
        }
    }

    #[test]
    fn assigns_one_checked_revision_to_every_item_in_an_atomic_batch() {
        let mut shard = CacheShard::new(10, None).unwrap();

        let result = shard
            .transact(
                transaction(vec![
                    set("a", CacheValue::String("one".into())),
                    set("b", CacheValue::Counter(2)),
                ]),
                10,
            )
            .unwrap();

        assert_eq!(result.revision, 1);
        assert_eq!(shard.revision(), 1);
        assert_eq!(shard.observe("a", 10).item.unwrap().version, 1);
        assert_eq!(shard.observe("b", 10).item.unwrap().version, 1);
    }

    #[test]
    fn versions_do_not_repeat_after_delete_and_recreate() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![set("key", CacheValue::String("v1".into()))]),
                1,
            )
            .unwrap();
        shard
            .transact(
                transaction(vec![CacheMutation::Delete {
                    key: "key".into(),
                    expected_version: Some(1),
                }]),
                2,
            )
            .unwrap();
        let recreated = shard
            .transact(
                transaction(vec![set("key", CacheValue::String("v2".into()))]),
                3,
            )
            .unwrap();

        assert_eq!(recreated.revision, 3);
        assert_eq!(shard.observe("key", 3).item.unwrap().version, 3);
        let error = shard
            .transact(
                transaction(vec![CacheMutation::CompareAndSet {
                    key: "key".into(),
                    expected_version: 1,
                    value: CacheValue::String("stale".into()),
                    ttl_ms: None,
                }]),
                4,
            )
            .unwrap_err();
        assert!(matches!(error, EpochError::Conflict(_)));
    }

    #[test]
    fn compare_and_set_creates_and_updates_with_typed_results() {
        let mut shard = CacheShard::new(10, Some(50)).unwrap();

        let created = shard
            .transact(
                transaction(vec![CacheMutation::CompareAndSet {
                    key: "key".into(),
                    expected_version: 0,
                    value: CacheValue::String("created".into()),
                    ttl_ms: Some(20),
                }]),
                10,
            )
            .unwrap();
        assert_eq!(
            created.results,
            [CacheMutationResult::CompareAndSet {
                item: CacheItem {
                    value: CacheValue::String("created".into()),
                    version: 1,
                    expires_at_ms: Some(30),
                },
            }]
        );

        let updated = shard
            .transact(
                transaction(vec![CacheMutation::CompareAndSet {
                    key: "key".into(),
                    expected_version: 1,
                    value: CacheValue::String("updated".into()),
                    ttl_ms: None,
                }]),
                15,
            )
            .unwrap();
        assert_eq!(
            updated.results,
            [CacheMutationResult::CompareAndSet {
                item: CacheItem {
                    value: CacheValue::String("updated".into()),
                    version: 2,
                    expires_at_ms: Some(65),
                },
            }]
        );
        assert_eq!(
            shard.observe("key", 15),
            CacheObservation {
                revision: 2,
                item: Some(CacheItem {
                    value: CacheValue::String("updated".into()),
                    version: 2,
                    expires_at_ms: Some(65),
                }),
            }
        );
    }

    #[test]
    fn expected_shard_revision_fences_an_optimistic_transaction() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(transaction(vec![set("a", CacheValue::Counter(1))]), 1)
            .unwrap();

        let error = shard
            .transact(
                CacheTransaction {
                    expected_revision: Some(0),
                    operations: vec![set("b", CacheValue::Counter(2))],
                },
                2,
            )
            .unwrap_err();

        assert!(matches!(error, EpochError::Conflict(_)));
        assert_eq!(shard.revision(), 1);
        assert!(shard.observe("b", 2).item.is_none());
    }

    #[test]
    fn rolls_back_every_operation_when_one_operation_fails() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![
                    set("counter", CacheValue::Counter(i64::MAX)),
                    set("stable", CacheValue::String("before".into())),
                ]),
                1,
            )
            .unwrap();
        let checksum = shard.recovery_state_checksum();

        let error = shard
            .transact(
                transaction(vec![
                    set("new", CacheValue::String("must-not-appear".into())),
                    CacheMutation::Increment {
                        key: "counter".into(),
                        delta: 1,
                        expected_version: None,
                    },
                ]),
                2,
            )
            .unwrap_err();

        assert!(matches!(error, EpochError::Capacity(_)));
        assert_eq!(shard.revision(), 1);
        assert_eq!(shard.recovery_state_checksum(), checksum);
        assert!(shard.observe("new", 2).item.is_none());
    }

    #[test]
    fn increment_creates_with_default_ttl_and_preserves_it_on_update() {
        let mut shard = CacheShard::new(10, Some(100)).unwrap();

        let created = shard
            .transact(
                transaction(vec![CacheMutation::Increment {
                    key: "counter".into(),
                    delta: 2,
                    expected_version: Some(0),
                }]),
                10,
            )
            .unwrap();
        assert_eq!(
            created.results,
            [CacheMutationResult::Increment {
                value: 2,
                version: 1,
                expires_at_ms: Some(110),
            }]
        );

        let incremented = shard
            .transact(
                transaction(vec![CacheMutation::Increment {
                    key: "counter".into(),
                    delta: 3,
                    expected_version: Some(1),
                }]),
                20,
            )
            .unwrap();
        assert_eq!(
            incremented.results,
            [CacheMutationResult::Increment {
                value: 5,
                version: 2,
                expires_at_ms: Some(110),
            }]
        );
        assert_eq!(
            shard.observe("counter", 20).item,
            Some(CacheItem {
                value: CacheValue::Counter(5),
                version: 2,
                expires_at_ms: Some(110),
            })
        );
    }

    #[test]
    fn no_op_delete_does_not_advance_revision_or_reclaim_expired_entries() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![CacheMutation::Set {
                    key: "expired".into(),
                    value: CacheValue::String("value".into()),
                    options: SetOptions {
                        ttl_ms: Some(1),
                        ..SetOptions::default()
                    },
                }]),
                1,
            )
            .unwrap();

        let result = shard
            .transact(
                transaction(vec![CacheMutation::Delete {
                    key: "missing".into(),
                    expected_version: None,
                }]),
                2,
            )
            .unwrap();

        assert_eq!(result.revision, 1);
        assert_eq!(shard.revision(), 1);
        assert_eq!(shard.len(), 1);
        assert!(matches!(
            &result.results[0],
            CacheMutationResult::Delete {
                deleted: false,
                previous_version: None
            }
        ));
    }

    #[test]
    fn delete_reports_removed_version_and_subsequent_no_op() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![set("key", CacheValue::String("value".into()))]),
                1,
            )
            .unwrap();

        let deleted = shard
            .transact(
                transaction(vec![CacheMutation::Delete {
                    key: "key".into(),
                    expected_version: Some(1),
                }]),
                2,
            )
            .unwrap();
        assert_eq!(deleted.revision, 2);
        assert_eq!(
            deleted.results,
            [CacheMutationResult::Delete {
                deleted: true,
                previous_version: Some(1),
            }]
        );
        assert!(shard.observe("key", 2).item.is_none());

        let no_op = shard
            .transact(
                transaction(vec![CacheMutation::Delete {
                    key: "key".into(),
                    expected_version: Some(0),
                }]),
                3,
            )
            .unwrap();
        assert_eq!(no_op.revision, 2);
        assert_eq!(
            no_op.results,
            [CacheMutationResult::Delete {
                deleted: false,
                previous_version: None,
            }]
        );
    }

    #[test]
    fn expected_versions_treat_logically_expired_items_as_absent() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![CacheMutation::Set {
                    key: "key".into(),
                    value: CacheValue::String("old".into()),
                    options: SetOptions {
                        ttl_ms: Some(5),
                        ..SetOptions::default()
                    },
                }]),
                100,
            )
            .unwrap();
        let checksum = shard.recovery_state_checksum();

        for mutation in [
            CacheMutation::CompareAndSet {
                key: "key".into(),
                expected_version: 1,
                value: CacheValue::String("stale".into()),
                ttl_ms: None,
            },
            CacheMutation::Delete {
                key: "key".into(),
                expected_version: Some(1),
            },
            CacheMutation::Increment {
                key: "key".into(),
                delta: 1,
                expected_version: Some(1),
            },
        ] {
            assert!(matches!(
                shard.transact(transaction(vec![mutation]), 105),
                Err(EpochError::Conflict(_))
            ));
            assert_eq!(shard.revision(), 1);
            assert_eq!(shard.recovery_state_checksum(), checksum);
        }

        let replacement = shard
            .transact(
                transaction(vec![CacheMutation::Set {
                    key: "key".into(),
                    value: CacheValue::String("new".into()),
                    options: SetOptions {
                        expected_version: Some(0),
                        ..SetOptions::default()
                    },
                }]),
                105,
            )
            .unwrap();
        assert_eq!(replacement.revision, 2);
        assert_eq!(
            shard.observe("key", 105).item,
            Some(CacheItem {
                value: CacheValue::String("new".into()),
                version: 2,
                expires_at_ms: None,
            })
        );
    }

    #[test]
    fn set_only_if_conditions_enforce_presence_atomically() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![CacheMutation::Set {
                    key: "key".into(),
                    value: CacheValue::String("created".into()),
                    options: SetOptions {
                        only_if_absent: true,
                        ..SetOptions::default()
                    },
                }]),
                1,
            )
            .unwrap();
        let checksum = shard.recovery_state_checksum();

        assert!(matches!(
            shard.transact(
                transaction(vec![CacheMutation::Set {
                    key: "key".into(),
                    value: CacheValue::String("must-not-apply".into()),
                    options: SetOptions {
                        only_if_absent: true,
                        ..SetOptions::default()
                    },
                }]),
                2,
            ),
            Err(EpochError::Conflict(_))
        ));
        assert_eq!(shard.recovery_state_checksum(), checksum);

        let updated = shard
            .transact(
                transaction(vec![CacheMutation::Set {
                    key: "key".into(),
                    value: CacheValue::String("updated".into()),
                    options: SetOptions {
                        only_if_present: true,
                        ..SetOptions::default()
                    },
                }]),
                2,
            )
            .unwrap();
        assert_eq!(updated.revision, 2);

        let checksum = shard.recovery_state_checksum();
        assert!(matches!(
            shard.transact(
                transaction(vec![CacheMutation::Set {
                    key: "missing".into(),
                    value: CacheValue::String("must-not-apply".into()),
                    options: SetOptions {
                        only_if_present: true,
                        ..SetOptions::default()
                    },
                }]),
                3,
            ),
            Err(EpochError::NotFound(_))
        ));
        assert_eq!(shard.recovery_state_checksum(), checksum);
        assert!(matches!(
            shard.transact(
                transaction(vec![CacheMutation::Set {
                    key: "invalid".into(),
                    value: CacheValue::String("must-not-apply".into()),
                    options: SetOptions {
                        only_if_absent: true,
                        only_if_present: true,
                        ..SetOptions::default()
                    },
                }]),
                3,
            ),
            Err(EpochError::InvalidArgument(_))
        ));
        assert_eq!(shard.recovery_state_checksum(), checksum);
    }

    #[test]
    fn observe_is_pure_and_treats_deadline_as_exclusive() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![CacheMutation::Set {
                    key: "short".into(),
                    value: CacheValue::String("value".into()),
                    options: SetOptions {
                        ttl_ms: Some(10),
                        ..SetOptions::default()
                    },
                }]),
                100,
            )
            .unwrap();
        let checksum = shard.recovery_state_checksum();

        assert!(shard.observe("short", 109).item.is_some());
        assert!(shard.observe("short", 110).item.is_none());
        assert_eq!(shard.revision(), 1);
        assert_eq!(shard.len(), 1);
        assert_eq!(shard.recovery_state_checksum(), checksum);
    }

    #[test]
    fn expiry_maintenance_is_bounded_and_ordered_by_deadline_then_key() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![
                    CacheMutation::Set {
                        key: "b".into(),
                        value: CacheValue::Counter(1),
                        options: SetOptions {
                            ttl_ms: Some(5),
                            ..SetOptions::default()
                        },
                    },
                    CacheMutation::Set {
                        key: "a".into(),
                        value: CacheValue::Counter(2),
                        options: SetOptions {
                            ttl_ms: Some(5),
                            ..SetOptions::default()
                        },
                    },
                    CacheMutation::Set {
                        key: "first".into(),
                        value: CacheValue::Counter(3),
                        options: SetOptions {
                            ttl_ms: Some(4),
                            ..SetOptions::default()
                        },
                    },
                ]),
                10,
            )
            .unwrap();

        let expired = shard.maintain_expiry(15, 2).unwrap();

        assert_eq!(expired.revision, 2);
        assert_eq!(expired.expired_keys, ["first", "a"]);
        assert_eq!(shard.len(), 1);
        let final_expiry = shard.maintain_expiry(15, 2).unwrap();
        assert_eq!(final_expiry.revision, 3);
        assert_eq!(final_expiry.expired_keys, ["b"]);
    }

    #[test]
    fn capacity_counts_only_live_entries_on_a_successful_write() {
        let mut shard = CacheShard::new(1, None).unwrap();
        shard
            .transact(
                transaction(vec![CacheMutation::Set {
                    key: "expired".into(),
                    value: CacheValue::Counter(1),
                    options: SetOptions {
                        ttl_ms: Some(1),
                        ..SetOptions::default()
                    },
                }]),
                1,
            )
            .unwrap();

        shard
            .transact(
                transaction(vec![set("replacement", CacheValue::Counter(2))]),
                2,
            )
            .unwrap();

        assert_eq!(shard.len(), 1);
        assert!(shard.observe("expired", 2).item.is_none());
        assert_eq!(
            shard.observe("replacement", 2).item.unwrap().value,
            CacheValue::Counter(2)
        );
    }

    #[test]
    fn canonical_checksum_is_independent_of_operation_order() {
        let values = [
            ("string", CacheValue::String("value".into())),
            ("blob", CacheValue::Blob(vec![0, 1, 255])),
            ("counter", CacheValue::Counter(-2)),
            (
                "hash",
                CacheValue::Hash(BTreeMap::from([("field".into(), "value".into())])),
            ),
            ("list", CacheValue::List(vec!["a".into(), "b".into()])),
            (
                "set",
                CacheValue::Set(BTreeSet::from(["a".into(), "b".into()])),
            ),
            (
                "sorted_set",
                CacheValue::SortedSet(BTreeMap::from([("a".into(), 1.5)])),
            ),
        ];
        let mut left = CacheShard::new(20, Some(50)).unwrap();
        let mut right = CacheShard::new(20, Some(50)).unwrap();
        left.transact(
            transaction(
                values
                    .iter()
                    .map(|(key, value)| set(key, value.clone()))
                    .collect(),
            ),
            10,
        )
        .unwrap();
        right
            .transact(
                transaction(
                    values
                        .iter()
                        .rev()
                        .map(|(key, value)| set(key, value.clone()))
                        .collect(),
                ),
                10,
            )
            .unwrap();

        assert_eq!(
            left.recovery_state_checksum(),
            right.recovery_state_checksum()
        );
    }

    #[test]
    fn recovery_checksum_has_a_pinned_vector_and_covers_every_state_dimension() {
        let mut shard = CacheShard::new(5, Some(100)).unwrap();
        shard
            .transact(
                transaction(vec![
                    CacheMutation::Set {
                        key: "alpha".into(),
                        value: CacheValue::String("value".into()),
                        options: SetOptions {
                            ttl_ms: Some(50),
                            ..SetOptions::default()
                        },
                    },
                    set("counter", CacheValue::Counter(-7)),
                    set(
                        "set",
                        CacheValue::Set(BTreeSet::from(["a".into(), "b".into()])),
                    ),
                ]),
                1_000,
            )
            .unwrap();
        shard
            .transact(
                transaction(vec![CacheMutation::Increment {
                    key: "counter".into(),
                    delta: 9,
                    expected_version: Some(1),
                }]),
                1_010,
            )
            .unwrap();

        let golden = shard.recovery_state_checksum();
        assert_eq!(golden, 0x83d9_56e5);
        let digest = shard.recovery_state_digest();
        assert_eq!(
            digest,
            [
                0x8b, 0x33, 0x6f, 0xa3, 0x72, 0x03, 0xad, 0xe3, 0x3d, 0x59, 0x9e, 0x46, 0xda, 0x92,
                0x68, 0xdd, 0x3e, 0x0f, 0x6c, 0x25, 0x84, 0x5d, 0xee, 0x67, 0x6b, 0xac, 0x1a, 0x9c,
                0x24, 0xdb, 0xfd, 0x56,
            ]
        );

        let mut changed_config = shard.clone();
        changed_config.max_entries += 1;
        assert_ne!(changed_config.recovery_state_checksum(), golden);
        assert_ne!(changed_config.recovery_state_digest(), digest);

        let mut changed_default_ttl = shard.clone();
        changed_default_ttl.default_ttl_ms = Some(101);
        assert_ne!(changed_default_ttl.recovery_state_checksum(), golden);
        assert_ne!(changed_default_ttl.recovery_state_digest(), digest);

        let mut changed_revision = shard.clone();
        changed_revision.revision += 1;
        assert_ne!(changed_revision.recovery_state_checksum(), golden);
        assert_ne!(changed_revision.recovery_state_digest(), digest);

        let mut changed_key = shard.clone();
        let alpha = changed_key.entries.remove("alpha").unwrap();
        changed_key.entries.insert("beta".into(), alpha);
        assert_ne!(changed_key.recovery_state_checksum(), golden);
        assert_ne!(changed_key.recovery_state_digest(), digest);

        let mut changed_value = shard.clone();
        changed_value.entries.get_mut("counter").unwrap().value = CacheValue::Counter(3);
        assert_ne!(changed_value.recovery_state_checksum(), golden);
        assert_ne!(changed_value.recovery_state_digest(), digest);

        let mut changed_version = shard.clone();
        changed_version.entries.get_mut("alpha").unwrap().version += 1;
        assert_ne!(changed_version.recovery_state_checksum(), golden);
        assert_ne!(changed_version.recovery_state_digest(), digest);

        let mut changed_expiry = shard.clone();
        changed_expiry
            .entries
            .get_mut("alpha")
            .unwrap()
            .expires_at_ms = Some(1_051);
        assert_ne!(changed_expiry.recovery_state_checksum(), golden);
        assert_ne!(changed_expiry.recovery_state_digest(), digest);
    }

    #[test]
    fn ttl_deadline_overflow_is_rejected_atomically_for_every_write_form() {
        let operations = [
            CacheMutation::Set {
                key: "set".into(),
                value: CacheValue::Counter(1),
                options: SetOptions {
                    ttl_ms: Some(1),
                    ..SetOptions::default()
                },
            },
            CacheMutation::CompareAndSet {
                key: "cas".into(),
                expected_version: 0,
                value: CacheValue::Counter(1),
                ttl_ms: Some(1),
            },
        ];
        for operation in operations {
            let mut shard = CacheShard::new(10, None).unwrap();
            let checksum = shard.recovery_state_checksum();
            assert!(matches!(
                shard.transact(transaction(vec![operation]), u64::MAX),
                Err(EpochError::Capacity(_))
            ));
            assert_eq!(shard.revision(), 0);
            assert!(shard.is_empty());
            assert_eq!(shard.recovery_state_checksum(), checksum);
        }

        for operation in [
            set("default-set", CacheValue::Counter(1)),
            CacheMutation::Increment {
                key: "default-increment".into(),
                delta: 1,
                expected_version: Some(0),
            },
        ] {
            let mut shard = CacheShard::new(10, Some(1)).unwrap();
            let checksum = shard.recovery_state_checksum();
            assert!(matches!(
                shard.transact(transaction(vec![operation]), u64::MAX),
                Err(EpochError::Capacity(_))
            ));
            assert_eq!(shard.revision(), 0);
            assert!(shard.is_empty());
            assert_eq!(shard.recovery_state_checksum(), checksum);
        }
    }

    #[test]
    fn revision_exhaustion_is_fail_closed_but_no_op_delete_still_succeeds() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard.revision = u64::MAX;
        let checksum = shard.recovery_state_checksum();

        let error = shard
            .transact(transaction(vec![set("key", CacheValue::Counter(1))]), 1)
            .unwrap_err();
        assert!(matches!(error, EpochError::Capacity(_)));
        assert_eq!(shard.revision(), u64::MAX);
        assert!(shard.is_empty());
        assert_eq!(shard.recovery_state_checksum(), checksum);

        let no_op = shard
            .transact(
                transaction(vec![CacheMutation::Delete {
                    key: "missing".into(),
                    expected_version: Some(0),
                }]),
                1,
            )
            .unwrap();
        assert_eq!(no_op.revision, u64::MAX);
        assert_eq!(shard.recovery_state_checksum(), checksum);
    }

    #[test]
    fn revision_exhaustion_does_not_partially_apply_expiry_maintenance() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![CacheMutation::Set {
                    key: "expired".into(),
                    value: CacheValue::Counter(1),
                    options: SetOptions {
                        ttl_ms: Some(1),
                        ..SetOptions::default()
                    },
                }]),
                1,
            )
            .unwrap();
        shard.revision = u64::MAX;
        let checksum = shard.recovery_state_checksum();

        assert!(matches!(
            shard.maintain_expiry(2, 1),
            Err(EpochError::Capacity(_))
        ));
        assert_eq!(shard.len(), 1);
        assert_eq!(shard.revision(), u64::MAX);
        assert_eq!(shard.recovery_state_checksum(), checksum);
    }

    #[test]
    fn serde_rejects_unknown_mutation_and_nested_option_fields() {
        let unknown_transaction_field = serde_json::json!({
            "expected_revision": null,
            "operations": [],
            "unexpected": true
        });
        assert!(
            serde_json::from_value::<CacheTransaction>(unknown_transaction_field).is_err(),
            "unknown transaction fields must be rejected"
        );

        let misspelled_options = serde_json::json!({
            "operations": [{
                "kind": "set",
                "key": "key",
                "value": { "kind": "counter", "value": 1 },
                "optoins": { "ttl_ms": 10 }
            }]
        });
        assert!(
            serde_json::from_value::<CacheTransaction>(misspelled_options).is_err(),
            "a misspelled options field must not become an unconditional write"
        );

        let misspelled_expected_version = serde_json::json!({
            "operations": [{
                "kind": "set",
                "key": "key",
                "value": { "kind": "counter", "value": 1 },
                "options": {
                    "ttl_ms": null,
                    "expected_version": null,
                    "only_if_absent": false,
                    "only_if_present": false,
                    "expected_verison": 7
                }
            }]
        });
        assert!(
            serde_json::from_value::<CacheTransaction>(misspelled_expected_version).is_err(),
            "a misspelled nested condition must not be ignored"
        );

        let unknown_value_field = serde_json::json!({
            "kind": "counter",
            "value": 1,
            "unexpected": true
        });
        assert!(
            serde_json::from_value::<CacheValue>(unknown_value_field).is_err(),
            "unknown CacheValue fields must be rejected"
        );
    }

    #[test]
    fn public_transaction_and_result_types_round_trip_through_json() {
        let request = CacheTransaction {
            expected_revision: Some(0),
            operations: vec![
                CacheMutation::Set {
                    key: "set".into(),
                    value: CacheValue::String("value".into()),
                    options: SetOptions {
                        ttl_ms: Some(5),
                        expected_version: Some(0),
                        only_if_absent: true,
                        only_if_present: false,
                    },
                },
                CacheMutation::Delete {
                    key: "delete".into(),
                    expected_version: Some(0),
                },
                CacheMutation::Increment {
                    key: "increment".into(),
                    delta: 2,
                    expected_version: Some(0),
                },
                CacheMutation::CompareAndSet {
                    key: "cas".into(),
                    expected_version: 0,
                    value: CacheValue::Blob(vec![0, 1, 255]),
                    ttl_ms: None,
                },
            ],
        };
        let encoded = serde_json::to_value(&request).unwrap();
        assert_eq!(
            serde_json::from_value::<CacheTransaction>(encoded).unwrap(),
            request
        );

        let mut shard = CacheShard::new(10, None).unwrap();
        let result = shard.transact(request, 10).unwrap();
        let encoded = serde_json::to_value(&result).unwrap();
        assert_eq!(
            serde_json::from_value::<CacheTransactionResult>(encoded).unwrap(),
            result
        );

        let observation = shard.observe("increment", 10);
        let encoded = serde_json::to_value(&observation).unwrap();
        assert_eq!(
            serde_json::from_value::<CacheObservation>(encoded).unwrap(),
            observation
        );

        let expiry = shard.maintain_expiry(15, 1).unwrap();
        let encoded = serde_json::to_value(&expiry).unwrap();
        assert_eq!(
            serde_json::from_value::<CacheExpiryResult>(encoded).unwrap(),
            expiry
        );
    }

    #[test]
    fn zero_ttl_is_rejected_without_changing_live_capacity() {
        assert!(matches!(
            CacheShard::new(1, Some(0)),
            Err(EpochError::InvalidArgument(_))
        ));

        let mut shard = CacheShard::new(1, None).unwrap();
        shard
            .transact(transaction(vec![set("live", CacheValue::Counter(1))]), 1)
            .unwrap();
        let checksum = shard.recovery_state_checksum();
        let error = shard
            .transact(
                transaction(vec![CacheMutation::Set {
                    key: "instant".into(),
                    value: CacheValue::Counter(2),
                    options: SetOptions {
                        ttl_ms: Some(0),
                        ..SetOptions::default()
                    },
                }]),
                2,
            )
            .unwrap_err();

        assert!(matches!(error, EpochError::InvalidArgument(_)));
        assert_eq!(shard.len(), 1);
        assert_eq!(shard.revision(), 1);
        assert_eq!(shard.recovery_state_checksum(), checksum);
    }

    #[test]
    fn non_finite_sorted_set_scores_are_rejected_atomically() {
        for operation in [
            set(
                "set",
                CacheValue::SortedSet(BTreeMap::from([("member".into(), f64::NAN)])),
            ),
            CacheMutation::CompareAndSet {
                key: "cas".into(),
                expected_version: 0,
                value: CacheValue::SortedSet(BTreeMap::from([("member".into(), f64::INFINITY)])),
                ttl_ms: None,
            },
        ] {
            let mut shard = CacheShard::new(1, None).unwrap();
            let checksum = shard.recovery_state_checksum();
            assert!(matches!(
                shard.transact(transaction(vec![operation]), 1),
                Err(EpochError::InvalidArgument(_))
            ));
            assert_eq!(shard.revision(), 0);
            assert!(shard.is_empty());
            assert_eq!(shard.recovery_state_checksum(), checksum);
        }
    }

    #[test]
    fn rejects_invalid_and_oversized_transactions_without_mutating_state() {
        let mut shard = CacheShard::new(10, None).unwrap();
        assert!(matches!(
            shard.transact(transaction(Vec::new()), 1),
            Err(EpochError::InvalidArgument(_))
        ));
        assert!(matches!(
            shard.transact(
                transaction(vec![
                    set("same", CacheValue::Counter(1)),
                    set("same", CacheValue::Counter(2)),
                ]),
                1,
            ),
            Err(EpochError::InvalidArgument(_))
        ));
        let too_many = (0..=MAX_CACHE_ATOMIC_OPERATIONS)
            .map(|index| {
                set(
                    &format!("key-{index}"),
                    CacheValue::Counter(i64::try_from(index).expect("test index fits i64")),
                )
            })
            .collect();
        assert!(matches!(
            shard.transact(transaction(too_many), 1),
            Err(EpochError::InvalidArgument(_))
        ));
        assert!(matches!(
            shard.maintain_expiry(1, MAX_CACHE_MAINTENANCE_KEYS + 1),
            Err(EpochError::InvalidArgument(_))
        ));
        assert_eq!(shard.revision(), 0);
        assert!(shard.is_empty());
    }
}
