//! Deterministic, shard-local Cache mutations for replicated tablet runtimes.
//!
//! This state machine is deliberately additive to the legacy memory-first
//! [`crate::Cache`]. It supplies a pure read path, non-ABA item versions, and a
//! bounded atomic mutation boundary without changing standalone behavior.

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{EpochError, EpochResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{CacheItem, CacheStorageClass, CacheValue, EvictionPolicy, SetOptions};

pub const MAX_CACHE_ATOMIC_OPERATIONS: usize = 128;
pub const MAX_CACHE_MAINTENANCE_KEYS: usize = 1_000;
pub const MAX_CACHE_CHANGE_RECORDS: usize = 1_024;
/// Backup artifacts are intentionally bounded below the consensus proposal
/// ceiling after base64 expansion so a restore remains a single atomic command.
pub const MAX_CACHE_BACKUP_BYTES: usize = 320 * 1024;
const CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V1: u16 = 1;
const CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V2: u16 = 2;
const CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V3: u16 = 3;
pub const CACHE_SHARD_SNAPSHOT_FORMAT_VERSION: u16 = 4;
pub const MAX_CACHE_SHARD_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;
const CACHE_BACKUP_FORMAT_VERSION: u16 = 1;

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
    Access {
        key: String,
    },
    Transform {
        key: String,
        transform: CacheTransform,
        #[serde(default)]
        expected_version: Option<u64>,
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CacheTransform {
    Replace {
        value: CacheValue,
        storage_class: CacheStorageClass,
    },
    HashPut {
        field: String,
        value: String,
    },
    HashRemove {
        field: String,
    },
    ListPush {
        value: String,
        front: bool,
    },
    ListPop {
        front: bool,
    },
    SetAdd {
        member: String,
    },
    SetRemove {
        member: String,
    },
    SortedSetAdd {
        member: String,
        score: f64,
    },
    SortedSetRemove {
        member: String,
    },
    BitmapSet {
        bit: u32,
        value: bool,
    },
    CardinalityAdd {
        value: Vec<u8>,
        precision: u8,
    },
    BloomAdd {
        value: Vec<u8>,
        bit_count: u32,
        hashes: u8,
    },
    CuckooAdd {
        value: Vec<u8>,
        bucket_count: u32,
        bucket_size: u8,
    },
    CuckooDelete {
        value: Vec<u8>,
    },
    GeoUpsert {
        member: String,
        point: crate::CacheGeoPoint,
    },
    GeoRemove {
        member: String,
    },
    JsonSet {
        pointer: String,
        value: serde_json::Value,
    },
    JsonRemove {
        pointer: String,
    },
    JsonIndexUpsert {
        id: String,
        document: crate::CacheJsonDocument,
        indexed_pointers: BTreeSet<String>,
    },
    JsonIndexRemove {
        id: String,
    },
    VectorUpsert {
        id: String,
        document: crate::CacheVectorDocument,
    },
    VectorRemove {
        id: String,
    },
}

impl CacheMutation {
    fn key(&self) -> &str {
        match self {
            Self::Set { key, .. }
            | Self::Delete { key, .. }
            | Self::Increment { key, .. }
            | Self::CompareAndSet { key, .. }
            | Self::Access { key }
            | Self::Transform { key, .. } => key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheTransactionResult {
    pub revision: u64,
    pub results: Vec<CacheMutationResult>,
    pub evicted_keys: Vec<String>,
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
    Access {
        item: Option<CacheItem>,
    },
    Transform {
        item: CacheItem,
        changed: bool,
        result: serde_json::Value,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheChangeKind {
    Set,
    Delete,
    Increment,
    Access,
    Expire,
    Evict,
    Transform,
    Restore,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheChange {
    pub sequence: u64,
    pub revision: u64,
    pub at_ms: u64,
    pub key: String,
    pub kind: CacheChangeKind,
    pub before: Option<CacheItem>,
    pub after: Option<CacheItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheBackupMetadata {
    pub captured_revision: u64,
    pub captured_at_ms: u64,
    pub oldest_restorable_revision: u64,
    pub state_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheRestoreResult {
    pub revision: u64,
    pub restored_from_revision: u64,
    pub restored_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicatedEntry {
    value: CacheValue,
    version: u64,
    expires_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "is_zero")]
    last_access_revision: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    access_count: u64,
    #[serde(default, skip_serializing_if = "is_memory_storage")]
    storage_class: CacheStorageClass,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheChangeRecord {
    sequence: u64,
    revision: u64,
    at_ms: u64,
    key: String,
    kind: CacheChangeKind,
    before: Option<ReplicatedEntry>,
    after: Option<ReplicatedEntry>,
}

impl CacheChangeRecord {
    fn public(&self) -> CacheChange {
        CacheChange {
            sequence: self.sequence,
            revision: self.revision,
            at_ms: self.at_ms,
            key: self.key.clone(),
            kind: self.kind,
            before: self.before.as_ref().map(ReplicatedEntry::item),
            after: self.after.as_ref().map(ReplicatedEntry::item),
        }
    }
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
            storage_class: self.storage_class,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheShard {
    max_entries: usize,
    default_ttl_ms: Option<u64>,
    eviction: EvictionPolicy,
    max_memory_bytes: Option<usize>,
    max_cold_bytes: Option<usize>,
    revision: u64,
    entries: BTreeMap<String, ReplicatedEntry>,
    history_floor_revision: u64,
    next_change_sequence: u64,
    changes: Vec<CacheChangeRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedCacheShardSnapshot {
    format_version: u16,
    max_entries: usize,
    default_ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "is_no_eviction")]
    eviction: EvictionPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_memory_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_cold_bytes: Option<usize>,
    revision: u64,
    entries: BTreeMap<String, ReplicatedEntry>,
    #[serde(default, skip_serializing_if = "is_zero")]
    history_floor_revision: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    next_change_sequence: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    changes: Vec<CacheChangeRecord>,
    state_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedCacheBackup {
    format_version: u16,
    captured_revision: u64,
    captured_at_ms: u64,
    oldest_restorable_revision: u64,
    snapshot: Vec<u8>,
    state_digest: [u8; 32],
    artifact_digest: [u8; 32],
}

fn decode_cache_snapshot_document(encoded: &[u8]) -> EpochResult<VersionedCacheShardSnapshot> {
    if encoded.len() > MAX_CACHE_SHARD_SNAPSHOT_BYTES {
        return Err(EpochError::Capacity(format!(
            "Cache shard snapshot is {} bytes; maximum is {MAX_CACHE_SHARD_SNAPSHOT_BYTES}",
            encoded.len()
        )));
    }
    let snapshot: VersionedCacheShardSnapshot = serde_json::from_slice(encoded)
        .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
    if !matches!(
        snapshot.format_version,
        CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V1
            | CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V2
            | CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V3
            | CACHE_SHARD_SNAPSHOT_FORMAT_VERSION
    ) {
        return Err(EpochError::InvalidArgument(format!(
            "unsupported Cache shard snapshot version {}",
            snapshot.format_version
        )));
    }
    if serde_json::to_vec(&snapshot)
        .map_err(|error| EpochError::InvalidArgument(error.to_string()))?
        != encoded
    {
        return Err(EpochError::InvalidArgument(
            "Cache shard snapshot is not canonical".into(),
        ));
    }
    Ok(snapshot)
}

fn validate_cache_snapshot_compatibility(
    snapshot: &VersionedCacheShardSnapshot,
) -> EpochResult<()> {
    if snapshot.format_version == CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V1
        && snapshot.eviction != EvictionPolicy::NoEviction
    {
        return Err(EpochError::InvalidArgument(
            "Cache shard v1 snapshot cannot contain eviction metadata".into(),
        ));
    }
    if snapshot.format_version < CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V3
        && (snapshot.history_floor_revision != 0
            || snapshot.next_change_sequence != 0
            || !snapshot.changes.is_empty()
            || snapshot
                .entries
                .values()
                .any(|entry| is_advanced_value(&entry.value)))
    {
        return Err(EpochError::InvalidArgument(
            "legacy Cache snapshot contains v3 state".into(),
        ));
    }
    if snapshot.format_version < CACHE_SHARD_SNAPSHOT_FORMAT_VERSION
        && (snapshot.max_memory_bytes.is_some()
            || snapshot.max_cold_bytes.is_some()
            || snapshot
                .entries
                .values()
                .any(|entry| entry.storage_class == CacheStorageClass::Cold))
    {
        return Err(EpochError::InvalidArgument(
            "legacy Cache snapshot contains v4 byte-tier state".into(),
        ));
    }
    Ok(())
}

fn validate_cache_snapshot_registry(snapshot: &VersionedCacheShardSnapshot) -> EpochResult<()> {
    CacheShard::new_with_limits(
        snapshot.max_entries,
        snapshot.default_ttl_ms,
        snapshot.eviction,
        snapshot.max_memory_bytes,
        snapshot.max_cold_bytes,
    )?;
    if snapshot.entries.len() > snapshot.max_entries {
        return Err(EpochError::InvalidArgument(
            "Cache shard snapshot exceeds its configured capacity".into(),
        ));
    }
    for (key, entry) in &snapshot.entries {
        if key.is_empty()
            || entry.version == 0
            || entry.version > snapshot.revision
            || entry.expires_at_ms == Some(0)
            || entry.last_access_revision > snapshot.revision
        {
            return Err(EpochError::InvalidArgument(
                "Cache shard snapshot entry registry is invalid".into(),
            ));
        }
        validate_cache_value(&entry.value)?;
    }
    validate_byte_capacity(
        &snapshot.entries,
        snapshot.max_memory_bytes,
        snapshot.max_cold_bytes,
    )?;
    if snapshot.revision == 0 && !snapshot.entries.is_empty() {
        return Err(EpochError::InvalidArgument(
            "Cache shard snapshot revision is invalid".into(),
        ));
    }
    Ok(())
}

impl CacheShard {
    /// Creates a deterministic Cache shard.
    ///
    /// The replicated boundary rejects a zero default TTL because it cannot
    /// create a logically live value. The legacy [`crate::Cache`] remains
    /// unchanged.
    pub fn new(max_entries: usize, default_ttl_ms: Option<u64>) -> EpochResult<Self> {
        Self::new_with_eviction(max_entries, default_ttl_ms, EvictionPolicy::NoEviction)
    }

    /// Creates a deterministic Cache shard with an explicit admission policy.
    pub fn new_with_eviction(
        max_entries: usize,
        default_ttl_ms: Option<u64>,
        eviction: EvictionPolicy,
    ) -> EpochResult<Self> {
        Self::new_with_limits(max_entries, default_ttl_ms, eviction, None, None)
    }

    pub fn new_with_limits(
        max_entries: usize,
        default_ttl_ms: Option<u64>,
        eviction: EvictionPolicy,
        max_memory_bytes: Option<usize>,
        max_cold_bytes: Option<usize>,
    ) -> EpochResult<Self> {
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
        if max_memory_bytes == Some(0) || max_cold_bytes == Some(0) {
            return Err(EpochError::InvalidArgument(
                "cache shard byte capacities must be greater than zero when configured".into(),
            ));
        }
        Ok(Self {
            max_entries,
            default_ttl_ms,
            eviction,
            max_memory_bytes,
            max_cold_bytes,
            revision: 0,
            entries: BTreeMap::new(),
            history_floor_revision: 0,
            next_change_sequence: 1,
            changes: Vec::new(),
        })
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub const fn max_entries(&self) -> usize {
        self.max_entries
    }

    pub const fn default_ttl_ms(&self) -> Option<u64> {
        self.default_ttl_ms
    }

    pub const fn eviction(&self) -> EvictionPolicy {
        self.eviction
    }

    pub const fn max_memory_bytes(&self) -> Option<usize> {
        self.max_memory_bytes
    }

    pub const fn max_cold_bytes(&self) -> Option<usize> {
        self.max_cold_bytes
    }

    pub fn retained_bytes(&self, storage_class: CacheStorageClass) -> usize {
        retained_bytes(&self.entries, storage_class)
    }

    /// Returns a canonical copy of physically retained entries in one storage class.
    pub fn retained_items(&self, storage_class: CacheStorageClass) -> Vec<(String, CacheItem)> {
        self.entries
            .iter()
            .filter(|(_, entry)| entry.storage_class == storage_class)
            .map(|(key, entry)| (key.clone(), entry.item()))
            .collect()
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

    pub fn oldest_restorable_revision(&self) -> u64 {
        self.history_floor_revision
    }

    pub fn changes_from(&self, sequence: u64, limit: usize) -> EpochResult<Vec<CacheChange>> {
        if limit == 0 || limit > MAX_CACHE_CHANGE_RECORDS {
            return Err(EpochError::InvalidArgument(format!(
                "Cache change limit must be between 1 and {MAX_CACHE_CHANGE_RECORDS}"
            )));
        }
        Ok(self
            .changes
            .iter()
            .filter(|change| change.sequence >= sequence)
            .take(limit)
            .map(CacheChangeRecord::public)
            .collect())
    }

    pub fn encode_backup(&self, captured_at_ms: u64) -> EpochResult<Vec<u8>> {
        let snapshot = self.encode_snapshot()?;
        let mut backup = VersionedCacheBackup {
            format_version: CACHE_BACKUP_FORMAT_VERSION,
            captured_revision: self.revision,
            captured_at_ms,
            oldest_restorable_revision: self.history_floor_revision,
            snapshot,
            state_digest: self.recovery_state_digest(),
            artifact_digest: [0; 32],
        };
        backup.artifact_digest = backup_digest(&backup)?;
        let encoded = serde_json::to_vec(&backup)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
        if encoded.len() > MAX_CACHE_BACKUP_BYTES {
            return Err(EpochError::Capacity(format!(
                "Cache backup is {} bytes; maximum is {MAX_CACHE_BACKUP_BYTES}",
                encoded.len()
            )));
        }
        Ok(encoded)
    }

    pub fn inspect_backup(encoded: &[u8]) -> EpochResult<CacheBackupMetadata> {
        let (backup, shard) = decode_backup(encoded)?;
        Ok(CacheBackupMetadata {
            captured_revision: backup.captured_revision,
            captured_at_ms: backup.captured_at_ms,
            oldest_restorable_revision: backup.oldest_restorable_revision,
            state_digest: shard.recovery_state_digest(),
        })
    }

    pub fn restore_backup(
        &mut self,
        encoded: &[u8],
        target_revision: u64,
        now_ms: u64,
    ) -> EpochResult<CacheRestoreResult> {
        let (backup, source) = decode_backup(encoded)?;
        if source.max_entries != self.max_entries
            || source.default_ttl_ms != self.default_ttl_ms
            || source.eviction != self.eviction
            || source.max_memory_bytes != self.max_memory_bytes
            || source.max_cold_bytes != self.max_cold_bytes
        {
            return Err(EpochError::Conflict(
                "Cache backup configuration does not match the target shard".into(),
            ));
        }
        if target_revision < backup.oldest_restorable_revision
            || target_revision > backup.captured_revision
        {
            return Err(EpochError::InvalidArgument(format!(
                "target revision {target_revision} is outside the backup restoration window {}..={}",
                backup.oldest_restorable_revision, backup.captured_revision
            )));
        }
        let mut restored = source.entries_at_revision(target_revision)?;
        restored.retain(|_, entry| entry.is_live_at(now_ms));
        if restored.len() > self.max_entries {
            return Err(EpochError::Capacity(
                "restored Cache state exceeds target entry capacity".into(),
            ));
        }
        validate_byte_capacity(&restored, self.max_memory_bytes, self.max_cold_bytes)?;
        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("cache shard revision is exhausted".into()))?;
        for entry in restored.values_mut() {
            entry.version = next_revision;
            entry.last_access_revision = 0;
            entry.access_count = 0;
        }
        let previous = self.entries.clone();
        let restored_keys = restored.keys().cloned().collect::<Vec<_>>();
        self.entries = restored;
        self.revision = next_revision;
        let kinds = self
            .entries
            .keys()
            .chain(previous.keys())
            .map(|key| (key.clone(), CacheChangeKind::Restore))
            .collect();
        self.record_changes(&previous, now_ms, next_revision, &kinds);
        Ok(CacheRestoreResult {
            revision: next_revision,
            restored_from_revision: target_revision,
            restored_keys,
        })
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
        let previous = self.entries.clone();
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
            self.eviction,
            now_ms,
        )?;
        let resulting_len = resulting_len(&candidate, &transaction.operations);

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
                        | CacheMutation::CompareAndSet { .. }
                        | CacheMutation::Transform { .. } => {
                            unreachable!("validated non-delete mutations always change state")
                        }
                        CacheMutation::Access { key } => CacheMutationResult::Access {
                            item: candidate.get(&key).map(ReplicatedEntry::item),
                        },
                    })
                    .collect(),
                evicted_keys: Vec::new(),
            });
        }

        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("cache shard revision is exhausted".into()))?;
        let mut evicted_keys = select_eviction_victims(
            &candidate,
            &transaction.operations,
            resulting_len.saturating_sub(self.max_entries),
            self.eviction,
            next_revision,
            self.max_entries,
            resulting_len,
        )?;
        for key in &evicted_keys {
            candidate.remove(key);
        }
        let operations_for_admission = transaction.operations.clone();
        let results = apply_operations(
            &mut candidate,
            transaction.operations,
            self.default_ttl_ms,
            self.eviction,
            now_ms,
            next_revision,
        )?;
        let byte_victims = select_byte_eviction_victims(
            &candidate,
            &operations_for_admission,
            self.max_memory_bytes,
            self.max_cold_bytes,
            self.eviction,
            next_revision,
        )?;
        for key in &byte_victims {
            candidate.remove(key);
        }
        evicted_keys.extend(byte_victims);
        evicted_keys.sort_unstable();
        evicted_keys.dedup();
        let change_kinds =
            transaction_change_kinds(&operations_for_admission, &previous, now_ms, &evicted_keys);
        self.entries = candidate;
        self.revision = next_revision;
        self.record_changes(&previous, now_ms, next_revision, &change_kinds);
        Ok(CacheTransactionResult {
            revision: next_revision,
            results,
            evicted_keys,
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
        let previous = self.entries.clone();
        for key in &expired_keys {
            self.entries.remove(key);
        }
        self.revision = next_revision;
        let change_kinds = expired_keys
            .iter()
            .map(|key| (key.clone(), CacheChangeKind::Expire))
            .collect();
        self.record_changes(&previous, now_ms, next_revision, &change_kinds);
        Ok(CacheExpiryResult {
            revision: next_revision,
            expired_keys,
        })
    }

    /// Returns the earliest physical value-expiry deadline still retained by
    /// this shard.
    pub fn next_expiry_deadline_ms(&self) -> Option<u64> {
        self.entries
            .values()
            .filter_map(|entry| entry.expires_at_ms)
            .min()
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

    /// Encodes the complete deterministic shard state as a canonical,
    /// versioned application snapshot.
    pub fn encode_snapshot(&self) -> EpochResult<Vec<u8>> {
        let format_version = if self.uses_v4_state() {
            CACHE_SHARD_SNAPSHOT_FORMAT_VERSION
        } else if self.uses_v3_state() {
            CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V3
        } else if self.eviction == EvictionPolicy::NoEviction {
            CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V1
        } else {
            CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V2
        };
        let encoded = serde_json::to_vec(&VersionedCacheShardSnapshot {
            format_version,
            max_entries: self.max_entries,
            default_ttl_ms: self.default_ttl_ms,
            eviction: self.eviction,
            max_memory_bytes: self.max_memory_bytes,
            max_cold_bytes: self.max_cold_bytes,
            revision: self.revision,
            entries: self.entries.clone(),
            history_floor_revision: if format_version >= CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V3 {
                self.history_floor_revision
            } else {
                0
            },
            next_change_sequence: if format_version >= CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V3 {
                self.next_change_sequence
            } else {
                0
            },
            changes: if format_version >= CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V3 {
                self.changes.clone()
            } else {
                Vec::new()
            },
            state_digest: self.recovery_state_digest(),
        })
        .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
        if encoded.len() > MAX_CACHE_SHARD_SNAPSHOT_BYTES {
            return Err(EpochError::Capacity(format!(
                "Cache shard snapshot is {} bytes; maximum is {MAX_CACHE_SHARD_SNAPSHOT_BYTES}",
                encoded.len()
            )));
        }
        Ok(encoded)
    }

    /// Decodes and fully validates a canonical Cache shard snapshot.
    pub fn decode_snapshot(encoded: &[u8]) -> EpochResult<Self> {
        let snapshot = decode_cache_snapshot_document(encoded)?;
        validate_cache_snapshot_compatibility(&snapshot)?;
        validate_cache_snapshot_registry(&snapshot)?;
        let state_digest = snapshot.state_digest;
        let shard = Self {
            max_entries: snapshot.max_entries,
            default_ttl_ms: snapshot.default_ttl_ms,
            eviction: snapshot.eviction,
            max_memory_bytes: snapshot.max_memory_bytes,
            max_cold_bytes: snapshot.max_cold_bytes,
            revision: snapshot.revision,
            entries: snapshot.entries,
            history_floor_revision: snapshot.history_floor_revision,
            next_change_sequence: if snapshot.format_version
                >= CACHE_SHARD_SNAPSHOT_FORMAT_VERSION_V3
            {
                snapshot.next_change_sequence
            } else {
                1
            },
            changes: snapshot.changes,
        };
        shard.validate_change_history()?;
        if shard.recovery_state_digest() != state_digest {
            return Err(EpochError::InvalidArgument(
                "Cache shard snapshot state digest is invalid".into(),
            ));
        }
        Ok(shard)
    }

    fn encode_recovery_state(&self, sink: &mut dyn CanonicalSink) {
        let mut encoder = CanonicalEncoder::new(sink);
        let v4 = self.uses_v4_state();
        let v3 = self.uses_v3_state();
        if v4 {
            encoder.bytes(b"epoch/cache-shard/recovery/v4\0");
            encoder.u8(eviction_policy_code(self.eviction));
            encoder.option_u64(
                self.max_memory_bytes
                    .map(|value| u64::try_from(value).unwrap_or(u64::MAX)),
            );
            encoder.option_u64(
                self.max_cold_bytes
                    .map(|value| u64::try_from(value).unwrap_or(u64::MAX)),
            );
        } else if v3 {
            encoder.bytes(b"epoch/cache-shard/recovery/v3\0");
            encoder.u8(eviction_policy_code(self.eviction));
        } else if self.eviction == EvictionPolicy::NoEviction {
            encoder.bytes(b"epoch/cache-shard/recovery/v1\0");
        } else {
            encoder.bytes(b"epoch/cache-shard/recovery/v2\0");
            encoder.u8(eviction_policy_code(self.eviction));
        }
        encoder.u64(u64::try_from(self.max_entries).unwrap_or(u64::MAX));
        encoder.option_u64(self.default_ttl_ms);
        encoder.u64(self.revision);
        encoder.u64(u64::try_from(self.entries.len()).unwrap_or(u64::MAX));
        for (key, entry) in &self.entries {
            encoder.length_prefixed(key.as_bytes());
            encoder.cache_value(&entry.value);
            encoder.u64(entry.version);
            encoder.option_u64(entry.expires_at_ms);
            if v3 || v4 || self.eviction != EvictionPolicy::NoEviction {
                encoder.u64(entry.last_access_revision);
                encoder.u64(entry.access_count);
            }
            if v4 {
                encoder.u8(storage_class_code(entry.storage_class));
            }
        }
        if v3 || v4 {
            encoder.u64(self.history_floor_revision);
            encoder.u64(self.next_change_sequence);
            encoder.u64(u64::try_from(self.changes.len()).unwrap_or(u64::MAX));
            for change in &self.changes {
                let change_bytes = serde_json::to_vec(change)
                    .expect("validated Cache change records always serialize");
                encoder.length_prefixed(&change_bytes);
            }
        }
    }

    fn uses_v3_state(&self) -> bool {
        !self.changes.is_empty()
            || self.history_floor_revision != 0
            || self
                .entries
                .values()
                .any(|entry| is_advanced_value(&entry.value))
    }

    fn uses_v4_state(&self) -> bool {
        self.max_memory_bytes.is_some()
            || self.max_cold_bytes.is_some()
            || self
                .entries
                .values()
                .any(|entry| entry.storage_class == CacheStorageClass::Cold)
    }

    fn entries_at_revision(
        &self,
        target_revision: u64,
    ) -> EpochResult<BTreeMap<String, ReplicatedEntry>> {
        if target_revision < self.history_floor_revision || target_revision > self.revision {
            return Err(EpochError::InvalidArgument(
                "target revision is outside retained Cache history".into(),
            ));
        }
        let mut entries = self.entries.clone();
        for change in self
            .changes
            .iter()
            .rev()
            .filter(|change| change.revision > target_revision)
        {
            match &change.before {
                Some(before) => {
                    entries.insert(change.key.clone(), before.clone());
                }
                None => {
                    entries.remove(&change.key);
                }
            }
        }
        Ok(entries)
    }

    fn record_changes(
        &mut self,
        previous: &BTreeMap<String, ReplicatedEntry>,
        at_ms: u64,
        revision: u64,
        kinds: &BTreeMap<String, CacheChangeKind>,
    ) {
        let keys = previous
            .keys()
            .chain(self.entries.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for key in keys {
            let before = previous.get(&key);
            let after = self.entries.get(&key);
            if before == after {
                continue;
            }
            self.changes.push(CacheChangeRecord {
                sequence: self.next_change_sequence,
                revision,
                at_ms,
                key: key.clone(),
                kind: kinds.get(&key).copied().unwrap_or(CacheChangeKind::Set),
                before: before.cloned(),
                after: after.cloned(),
            });
            self.next_change_sequence = self.next_change_sequence.saturating_add(1);
        }
        while self.changes.len() > MAX_CACHE_CHANGE_RECORDS {
            let removed_revision = self.changes[0].revision;
            while self
                .changes
                .first()
                .is_some_and(|change| change.revision == removed_revision)
            {
                self.changes.remove(0);
            }
            self.history_floor_revision = removed_revision;
        }
    }

    fn validate_change_history(&self) -> EpochResult<()> {
        if self.history_floor_revision > self.revision
            || self.changes.len() > MAX_CACHE_CHANGE_RECORDS
            || self.next_change_sequence == 0
        {
            return Err(EpochError::InvalidArgument(
                "Cache change history bounds are invalid".into(),
            ));
        }
        let mut previous_sequence = 0;
        let mut previous_revision = self.history_floor_revision;
        for change in &self.changes {
            if change.sequence <= previous_sequence
                || change.revision < previous_revision
                || change.revision <= self.history_floor_revision
                || change.revision > self.revision
                || change.key.is_empty()
                || change.before == change.after
            {
                return Err(EpochError::InvalidArgument(
                    "Cache change history registry is invalid".into(),
                ));
            }
            for entry in [change.before.as_ref(), change.after.as_ref()]
                .into_iter()
                .flatten()
            {
                if entry.version == 0 || entry.version > change.revision {
                    return Err(EpochError::InvalidArgument(
                        "Cache change history entry version is invalid".into(),
                    ));
                }
                validate_cache_value(&entry.value)?;
            }
            previous_sequence = change.sequence;
            previous_revision = change.revision;
        }
        let expected_next = self
            .changes
            .last()
            .map_or(1, |change| change.sequence.saturating_add(1));
        if self.next_change_sequence != expected_next {
            return Err(EpochError::InvalidArgument(
                "Cache change sequence cursor is invalid".into(),
            ));
        }
        Ok(())
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

fn transaction_change_kinds(
    operations: &[CacheMutation],
    previous: &BTreeMap<String, ReplicatedEntry>,
    now_ms: u64,
    evicted_keys: &[String],
) -> BTreeMap<String, CacheChangeKind> {
    let mut kinds = previous
        .iter()
        .filter(|(_, entry)| !entry.is_live_at(now_ms))
        .map(|(key, _)| (key.clone(), CacheChangeKind::Expire))
        .collect::<BTreeMap<_, _>>();
    for operation in operations {
        let kind = match operation {
            CacheMutation::Set { .. } | CacheMutation::CompareAndSet { .. } => CacheChangeKind::Set,
            CacheMutation::Delete { .. } => CacheChangeKind::Delete,
            CacheMutation::Increment { .. } => CacheChangeKind::Increment,
            CacheMutation::Access { .. } => CacheChangeKind::Access,
            CacheMutation::Transform { .. } => CacheChangeKind::Transform,
        };
        kinds.insert(operation.key().to_owned(), kind);
    }
    for key in evicted_keys {
        kinds.insert(key.clone(), CacheChangeKind::Evict);
    }
    kinds
}

fn is_advanced_value(value: &CacheValue) -> bool {
    matches!(
        value,
        CacheValue::Bitmap(_)
            | CacheValue::Cardinality(_)
            | CacheValue::Bloom(_)
            | CacheValue::Cuckoo(_)
            | CacheValue::Geo(_)
            | CacheValue::Json(_)
            | CacheValue::JsonIndex(_)
            | CacheValue::Vector(_)
    )
}

fn backup_digest(backup: &VersionedCacheBackup) -> EpochResult<[u8; 32]> {
    let mut unsigned = backup.clone();
    unsigned.artifact_digest = [0; 32];
    let encoded = serde_json::to_vec(&unsigned)
        .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
    Ok(Sha256::digest(encoded).into())
}

fn decode_backup(encoded: &[u8]) -> EpochResult<(VersionedCacheBackup, CacheShard)> {
    if encoded.len() > MAX_CACHE_BACKUP_BYTES {
        return Err(EpochError::Capacity(format!(
            "Cache backup is {} bytes; maximum is {MAX_CACHE_BACKUP_BYTES}",
            encoded.len()
        )));
    }
    let backup: VersionedCacheBackup = serde_json::from_slice(encoded)
        .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
    if backup.format_version != CACHE_BACKUP_FORMAT_VERSION
        || serde_json::to_vec(&backup)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?
            != encoded
        || backup_digest(&backup)? != backup.artifact_digest
    {
        return Err(EpochError::InvalidArgument(
            "Cache backup version, canonical encoding, or digest is invalid".into(),
        ));
    }
    let shard = CacheShard::decode_snapshot(&backup.snapshot)?;
    if shard.revision != backup.captured_revision
        || shard.history_floor_revision != backup.oldest_restorable_revision
        || shard.recovery_state_digest() != backup.state_digest
    {
        return Err(EpochError::InvalidArgument(
            "Cache backup metadata does not match its snapshot".into(),
        ));
    }
    Ok((backup, shard))
}

fn validate_operations(
    entries: &BTreeMap<String, ReplicatedEntry>,
    operations: &[CacheMutation],
    default_ttl_ms: Option<u64>,
    eviction: EvictionPolicy,
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
            CacheMutation::Access { key } => {
                changed |= entries.contains_key(key) && tracks_committed_access(eviction);
            }
            CacheMutation::Transform {
                key,
                transform,
                expected_version,
                ttl_ms,
            } => {
                let current = entries.get(key);
                validate_expected_version(key, *expected_version, current)?;
                transform_value(current.map(|entry| &entry.value), transform)?;
                let effective_ttl = if current.is_some() && ttl_ms.is_none() {
                    None
                } else {
                    ttl_ms.or(default_ttl_ms)
                };
                if current.is_none() || ttl_ms.is_some() {
                    expiry_deadline(effective_ttl, now_ms)?;
                }
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
            | CacheMutation::CompareAndSet { key, .. }
            | CacheMutation::Transform { key, .. } => {
                if !entries.contains_key(key) {
                    len = len.saturating_add(1);
                }
            }
            CacheMutation::Delete { key, .. } => {
                if entries.contains_key(key) {
                    len = len.saturating_sub(1);
                }
            }
            CacheMutation::Access { .. } => {}
        }
    }
    len
}

fn apply_operations(
    entries: &mut BTreeMap<String, ReplicatedEntry>,
    operations: Vec<CacheMutation>,
    default_ttl_ms: Option<u64>,
    eviction: EvictionPolicy,
    now_ms: u64,
    revision: u64,
) -> EpochResult<Vec<CacheMutationResult>> {
    operations
        .into_iter()
        .map(|operation| {
            apply_operation(
                entries,
                operation,
                default_ttl_ms,
                eviction,
                now_ms,
                revision,
            )
        })
        .collect()
}

fn apply_operation(
    entries: &mut BTreeMap<String, ReplicatedEntry>,
    operation: CacheMutation,
    default_ttl_ms: Option<u64>,
    eviction: EvictionPolicy,
    now_ms: u64,
    revision: u64,
) -> EpochResult<CacheMutationResult> {
    Ok(match operation {
        CacheMutation::Set {
            key,
            value,
            options,
        } => CacheMutationResult::Set {
            item: upsert_entry(
                entries,
                key,
                value,
                EntryUpsert {
                    ttl_ms: options.ttl_ms.or(default_ttl_ms),
                    storage_class: Some(options.storage_class),
                    eviction,
                    now_ms,
                    revision,
                },
            )?,
        },
        CacheMutation::Delete { key, .. } => {
            let previous_version = entries.remove(&key).map(|entry| entry.version);
            CacheMutationResult::Delete {
                deleted: previous_version.is_some(),
                previous_version,
            }
        }
        CacheMutation::Increment { key, delta, .. } => apply_increment(
            entries,
            key,
            delta,
            default_ttl_ms,
            eviction,
            now_ms,
            revision,
        )?,
        CacheMutation::CompareAndSet {
            key, value, ttl_ms, ..
        } => CacheMutationResult::CompareAndSet {
            item: upsert_entry(
                entries,
                key,
                value,
                EntryUpsert {
                    ttl_ms: ttl_ms.or(default_ttl_ms),
                    storage_class: None,
                    eviction,
                    now_ms,
                    revision,
                },
            )?,
        },
        CacheMutation::Access { key } => {
            let item = entries.get_mut(&key).map(|entry| {
                record_committed_access(entry, eviction, revision);
                entry.item()
            });
            CacheMutationResult::Access { item }
        }
        CacheMutation::Transform {
            key,
            transform,
            ttl_ms,
            ..
        } => apply_transform(
            entries,
            key,
            &transform,
            ttl_ms,
            default_ttl_ms,
            eviction,
            now_ms,
            revision,
        )?,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the deterministic mutation boundary receives every admission input explicitly"
)]
fn apply_transform(
    entries: &mut BTreeMap<String, ReplicatedEntry>,
    key: String,
    transform: &CacheTransform,
    ttl_ms: Option<u64>,
    default_ttl_ms: Option<u64>,
    eviction: EvictionPolicy,
    now_ms: u64,
    revision: u64,
) -> EpochResult<CacheMutationResult> {
    let current = entries.get(&key).cloned();
    let (value, changed, result) =
        transform_value(current.as_ref().map(|entry| &entry.value), transform)?;
    let expires_at_ms = if let Some(ttl_ms) = ttl_ms {
        expiry_deadline(Some(ttl_ms), now_ms)?
    } else if let Some(current) = &current {
        current.expires_at_ms
    } else {
        expiry_deadline(default_ttl_ms, now_ms)?
    };
    let (last_access_revision, access_count) =
        next_access_metadata(current.as_ref(), eviction, revision);
    let storage_class = match transform {
        CacheTransform::Replace { storage_class, .. } => *storage_class,
        _ => current
            .as_ref()
            .map_or(CacheStorageClass::Memory, |entry| entry.storage_class),
    };
    let item = CacheItem {
        value: value.clone(),
        version: revision,
        expires_at_ms,
        storage_class,
    };
    entries.insert(
        key,
        ReplicatedEntry {
            value,
            version: revision,
            expires_at_ms,
            last_access_revision,
            access_count,
            storage_class,
        },
    );
    Ok(CacheMutationResult::Transform {
        item,
        changed,
        result,
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "one exhaustive transform interpreter is the atomic type boundary"
)]
fn transform_value(
    current: Option<&CacheValue>,
    transform: &CacheTransform,
) -> EpochResult<(CacheValue, bool, serde_json::Value)> {
    use serde_json::json;

    macro_rules! existing_or {
        ($pattern:pat => $value:expr, $initial:expr, $name:literal) => {
            match current {
                Some($pattern) => $value.clone(),
                Some(_) => {
                    return Err(EpochError::Conflict(format!(
                        "cache value is not a {}",
                        $name
                    )))
                }
                None => $initial,
            }
        };
    }

    let transformed = match transform {
        CacheTransform::Replace {
            value,
            storage_class,
        } => (
            value.clone(),
            current != Some(value),
            json!({"storage_class":storage_class}),
        ),
        CacheTransform::HashPut { field, value } => {
            let mut hash = existing_or!(CacheValue::Hash(hash) => hash, BTreeMap::new(), "hash");
            let previous = hash.insert(field.clone(), value.clone());
            (
                CacheValue::Hash(hash),
                previous.as_ref() != Some(value),
                json!({"previous":previous}),
            )
        }
        CacheTransform::HashRemove { field } => {
            let mut hash = existing_or!(CacheValue::Hash(hash) => hash, BTreeMap::new(), "hash");
            let previous = hash.remove(field);
            let changed = previous.is_some();
            (
                CacheValue::Hash(hash),
                changed,
                json!({"previous":previous}),
            )
        }
        CacheTransform::ListPush { value, front } => {
            let mut list = existing_or!(CacheValue::List(list) => list, Vec::new(), "list");
            if *front {
                list.insert(0, value.clone());
            } else {
                list.push(value.clone());
            }
            let length = list.len();
            (CacheValue::List(list), true, json!({"length":length}))
        }
        CacheTransform::ListPop { front } => {
            let mut list = existing_or!(CacheValue::List(list) => list, Vec::new(), "list");
            let value = if *front {
                (!list.is_empty()).then(|| list.remove(0))
            } else {
                list.pop()
            };
            let changed = value.is_some();
            (CacheValue::List(list), changed, json!({"value":value}))
        }
        CacheTransform::SetAdd { member } => {
            let mut set = existing_or!(CacheValue::Set(set) => set, BTreeSet::new(), "set");
            let changed = set.insert(member.clone());
            (CacheValue::Set(set), changed, json!({"added":changed}))
        }
        CacheTransform::SetRemove { member } => {
            let mut set = existing_or!(CacheValue::Set(set) => set, BTreeSet::new(), "set");
            let changed = set.remove(member);
            (CacheValue::Set(set), changed, json!({"removed":changed}))
        }
        CacheTransform::SortedSetAdd { member, score } => {
            if !score.is_finite() {
                return Err(EpochError::InvalidArgument(
                    "sorted-set score must be finite".into(),
                ));
            }
            let mut set =
                existing_or!(CacheValue::SortedSet(set) => set, BTreeMap::new(), "sorted set");
            let previous = set.insert(member.clone(), *score);
            (
                CacheValue::SortedSet(set),
                previous != Some(*score),
                json!({"previous":previous}),
            )
        }
        CacheTransform::SortedSetRemove { member } => {
            let mut set =
                existing_or!(CacheValue::SortedSet(set) => set, BTreeMap::new(), "sorted set");
            let previous = set.remove(member);
            let changed = previous.is_some();
            (
                CacheValue::SortedSet(set),
                changed,
                json!({"previous":previous}),
            )
        }
        CacheTransform::BitmapSet { bit, value } => {
            let mut bitmap = existing_or!(CacheValue::Bitmap(bitmap) => bitmap, crate::CacheBitmap::default(), "bitmap");
            let previous = bitmap.set(*bit, *value)?;
            (
                CacheValue::Bitmap(bitmap),
                previous != *value,
                json!({"previous":previous}),
            )
        }
        CacheTransform::CardinalityAdd { value, precision } => {
            let mut cardinality = match current {
                Some(CacheValue::Cardinality(value)) if value.precision() == *precision => {
                    value.clone()
                }
                Some(CacheValue::Cardinality(_)) => {
                    return Err(EpochError::Conflict(
                        "cardinality precision cannot change".into(),
                    ));
                }
                Some(_) => {
                    return Err(EpochError::Conflict(
                        "cache value is not a cardinality sketch".into(),
                    ));
                }
                None => crate::CacheCardinality::new(*precision)?,
            };
            let changed = cardinality.add(value);
            let estimate = cardinality.estimate();
            (
                CacheValue::Cardinality(cardinality),
                changed,
                json!({"estimate":estimate.to_string()}),
            )
        }
        CacheTransform::BloomAdd {
            value,
            bit_count,
            hashes,
        } => {
            let mut bloom = existing_or!(CacheValue::Bloom(value) => value, crate::CacheBloomFilter::new(*bit_count, *hashes)?, "Bloom filter");
            let changed = bloom.add(value);
            (CacheValue::Bloom(bloom), changed, json!({"added":changed}))
        }
        CacheTransform::CuckooAdd {
            value,
            bucket_count,
            bucket_size,
        } => {
            let mut cuckoo = existing_or!(CacheValue::Cuckoo(value) => value, crate::CacheCuckooFilter::new(*bucket_count, *bucket_size)?, "Cuckoo filter");
            let changed = cuckoo.add(value)?;
            (
                CacheValue::Cuckoo(cuckoo),
                changed,
                json!({"added":changed}),
            )
        }
        CacheTransform::CuckooDelete { value } => {
            let mut cuckoo = existing_or!(CacheValue::Cuckoo(value) => value, return Err(EpochError::NotFound("Cuckoo filter".into())), "Cuckoo filter");
            let changed = cuckoo.delete(value);
            (
                CacheValue::Cuckoo(cuckoo),
                changed,
                json!({"removed":changed}),
            )
        }
        CacheTransform::GeoUpsert { member, point } => {
            let mut geo = existing_or!(CacheValue::Geo(value) => value, crate::CacheGeoIndex::default(), "geospatial index");
            let added = geo.upsert(member.clone(), *point)?;
            (CacheValue::Geo(geo), true, json!({"added":added}))
        }
        CacheTransform::GeoRemove { member } => {
            let mut geo = existing_or!(CacheValue::Geo(value) => value, crate::CacheGeoIndex::default(), "geospatial index");
            let changed = geo.remove(member);
            (CacheValue::Geo(geo), changed, json!({"removed":changed}))
        }
        CacheTransform::JsonSet { pointer, value } => {
            let mut document = existing_or!(CacheValue::Json(value) => value, crate::CacheJsonDocument::new(serde_json::Value::Object(serde_json::Map::new()))?, "JSON document");
            let previous = document.set_pointer(pointer, value.clone())?;
            (
                CacheValue::Json(document),
                previous.as_ref() != Some(value),
                json!({"previous":previous}),
            )
        }
        CacheTransform::JsonRemove { pointer } => {
            let mut document = existing_or!(CacheValue::Json(value) => value, crate::CacheJsonDocument::new(serde_json::Value::Object(serde_json::Map::new()))?, "JSON document");
            let previous = document.remove_pointer(pointer)?;
            let changed = previous.is_some();
            (
                CacheValue::Json(document),
                changed,
                json!({"previous":previous}),
            )
        }
        CacheTransform::JsonIndexUpsert {
            id,
            document,
            indexed_pointers,
        } => {
            let mut index = match current {
                Some(CacheValue::JsonIndex(index)) => index.clone(),
                Some(_) => {
                    return Err(EpochError::Conflict(
                        "cache value is not a JSON secondary index".into(),
                    ));
                }
                None => crate::CacheJsonIndex::new(indexed_pointers.clone())?,
            };
            let added = index.upsert(id.clone(), document.clone())?;
            (CacheValue::JsonIndex(index), true, json!({"added":added}))
        }
        CacheTransform::JsonIndexRemove { id } => {
            let mut index = existing_or!(CacheValue::JsonIndex(value) => value, return Err(EpochError::NotFound("JSON secondary index".into())), "JSON secondary index");
            let changed = index.remove(id)?;
            (
                CacheValue::JsonIndex(index),
                changed,
                json!({"removed":changed}),
            )
        }
        CacheTransform::VectorUpsert { id, document } => {
            let mut index = existing_or!(CacheValue::Vector(value) => value, crate::CacheVectorIndex::new(document.dimensions())?, "vector index");
            let added = index.upsert(id.clone(), document.clone())?;
            (CacheValue::Vector(index), true, json!({"added":added}))
        }
        CacheTransform::VectorRemove { id } => {
            let mut index = existing_or!(CacheValue::Vector(value) => value, return Err(EpochError::NotFound("vector index".into())), "vector index");
            let changed = index.remove(id);
            (
                CacheValue::Vector(index),
                changed,
                json!({"removed":changed}),
            )
        }
    };
    validate_cache_value(&transformed.0)?;
    Ok(transformed)
}

#[derive(Debug, Clone, Copy)]
struct EntryUpsert {
    ttl_ms: Option<u64>,
    storage_class: Option<CacheStorageClass>,
    eviction: EvictionPolicy,
    now_ms: u64,
    revision: u64,
}

fn upsert_entry(
    entries: &mut BTreeMap<String, ReplicatedEntry>,
    key: String,
    value: CacheValue,
    upsert: EntryUpsert,
) -> EpochResult<CacheItem> {
    let expires_at_ms = expiry_deadline(upsert.ttl_ms, upsert.now_ms)?;
    let (last_access_revision, access_count) =
        next_access_metadata(entries.get(&key), upsert.eviction, upsert.revision);
    let storage_class = upsert.storage_class.unwrap_or_else(|| {
        entries
            .get(&key)
            .map_or(CacheStorageClass::Memory, |entry| entry.storage_class)
    });
    let item = CacheItem {
        value: value.clone(),
        version: upsert.revision,
        expires_at_ms,
        storage_class,
    };
    entries.insert(
        key,
        ReplicatedEntry {
            value,
            version: upsert.revision,
            expires_at_ms,
            last_access_revision,
            access_count,
            storage_class,
        },
    );
    Ok(item)
}

fn apply_increment(
    entries: &mut BTreeMap<String, ReplicatedEntry>,
    key: String,
    delta: i64,
    default_ttl_ms: Option<u64>,
    eviction: EvictionPolicy,
    now_ms: u64,
    revision: u64,
) -> EpochResult<CacheMutationResult> {
    let (value, expires_at_ms) = if let Some(entry) = entries.get_mut(&key) {
        let value = {
            let CacheValue::Counter(value) = &mut entry.value else {
                unreachable!("counter type was validated before application")
            };
            *value = value
                .checked_add(delta)
                .expect("counter overflow was validated before application");
            *value
        };
        entry.version = revision;
        record_committed_access(entry, eviction, revision);
        (value, entry.expires_at_ms)
    } else {
        let expires_at_ms = expiry_deadline(default_ttl_ms, now_ms)?;
        entries.insert(
            key,
            ReplicatedEntry {
                value: CacheValue::Counter(delta),
                version: revision,
                expires_at_ms,
                last_access_revision: initial_access_revision(eviction, revision),
                access_count: 0,
                storage_class: CacheStorageClass::Memory,
            },
        );
        (delta, expires_at_ms)
    };
    Ok(CacheMutationResult::Increment {
        value,
        version: revision,
        expires_at_ms,
    })
}

fn select_eviction_victims(
    entries: &BTreeMap<String, ReplicatedEntry>,
    operations: &[CacheMutation],
    required: usize,
    policy: EvictionPolicy,
    next_revision: u64,
    maximum: usize,
    resulting_len: usize,
) -> EpochResult<Vec<String>> {
    if required == 0 {
        return Ok(Vec::new());
    }
    if policy == EvictionPolicy::NoEviction {
        return Err(EpochError::Capacity(format!(
            "cache shard would contain {resulting_len} live entries; maximum is {maximum}; no-eviction is configured"
        )));
    }
    let protected = operations
        .iter()
        .filter(|operation| {
            matches!(
                operation,
                CacheMutation::Set { .. }
                    | CacheMutation::Increment { .. }
                    | CacheMutation::CompareAndSet { .. }
                    | CacheMutation::Transform { .. }
            )
        })
        .map(CacheMutation::key)
        .collect::<BTreeSet<_>>();
    let volatile_only = matches!(
        policy,
        EvictionPolicy::VolatileLru
            | EvictionPolicy::VolatileLfu
            | EvictionPolicy::VolatileRandom
            | EvictionPolicy::VolatileTtl
    );
    let mut candidates = entries
        .iter()
        .filter(|(key, entry)| {
            !protected.contains(key.as_str()) && (!volatile_only || entry.expires_at_ms.is_some())
        })
        .map(|(key, entry)| {
            (
                eviction_rank(policy, next_revision, key, entry),
                key.clone(),
            )
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    if candidates.len() < required {
        return Err(EpochError::Capacity(format!(
            "cache eviction policy {policy:?} has {} eligible victims; {required} required",
            candidates.len()
        )));
    }
    let mut victims = candidates
        .into_iter()
        .take(required)
        .map(|(_, key)| key)
        .collect::<Vec<_>>();
    victims.sort_unstable();
    Ok(victims)
}

fn select_byte_eviction_victims(
    entries: &BTreeMap<String, ReplicatedEntry>,
    operations: &[CacheMutation],
    max_memory_bytes: Option<usize>,
    max_cold_bytes: Option<usize>,
    policy: EvictionPolicy,
    next_revision: u64,
) -> EpochResult<Vec<String>> {
    if max_cold_bytes.is_none()
        && entries
            .values()
            .any(|entry| entry.storage_class == CacheStorageClass::Cold)
    {
        return Err(EpochError::Capacity(
            "cache cold tier is disabled for this resource".into(),
        ));
    }
    let protected = operations
        .iter()
        .filter(|operation| {
            matches!(
                operation,
                CacheMutation::Set { .. }
                    | CacheMutation::Increment { .. }
                    | CacheMutation::CompareAndSet { .. }
                    | CacheMutation::Transform { .. }
            )
        })
        .map(CacheMutation::key)
        .collect::<BTreeSet<_>>();
    let volatile_only = matches!(
        policy,
        EvictionPolicy::VolatileLru
            | EvictionPolicy::VolatileLfu
            | EvictionPolicy::VolatileRandom
            | EvictionPolicy::VolatileTtl
    );
    let mut retained = entries.clone();
    let mut victims = Vec::new();
    for (storage_class, maximum) in [
        (CacheStorageClass::Memory, max_memory_bytes),
        (CacheStorageClass::Cold, max_cold_bytes),
    ] {
        let Some(maximum) = maximum else {
            continue;
        };
        while retained_bytes(&retained, storage_class) > maximum {
            if policy == EvictionPolicy::NoEviction {
                return Err(EpochError::Capacity(format!(
                    "cache {storage_class:?} byte admission exceeds {maximum}; no-eviction is configured"
                )));
            }
            let candidate = retained
                .iter()
                .filter(|(key, entry)| {
                    entry.storage_class == storage_class
                        && !protected.contains(key.as_str())
                        && (!volatile_only || entry.expires_at_ms.is_some())
                })
                .map(|(key, entry)| {
                    (
                        eviction_rank(policy, next_revision, key, entry),
                        key.clone(),
                    )
                })
                .min();
            let Some((_, key)) = candidate else {
                return Err(EpochError::Capacity(format!(
                    "cache {storage_class:?} byte admission exceeds {maximum} with no eligible eviction victim"
                )));
            };
            retained.remove(&key);
            victims.push(key);
        }
    }
    victims.sort_unstable();
    Ok(victims)
}

fn validate_byte_capacity(
    entries: &BTreeMap<String, ReplicatedEntry>,
    max_memory_bytes: Option<usize>,
    max_cold_bytes: Option<usize>,
) -> EpochResult<()> {
    if max_cold_bytes.is_none()
        && entries
            .values()
            .any(|entry| entry.storage_class == CacheStorageClass::Cold)
    {
        return Err(EpochError::Capacity(
            "cache cold tier is disabled for this resource".into(),
        ));
    }
    for (storage_class, maximum) in [
        (CacheStorageClass::Memory, max_memory_bytes),
        (CacheStorageClass::Cold, max_cold_bytes),
    ] {
        if let Some(maximum) = maximum {
            let retained = retained_bytes(entries, storage_class);
            if retained > maximum {
                return Err(EpochError::Capacity(format!(
                    "cache {storage_class:?} retains {retained} bytes; maximum is {maximum}"
                )));
            }
        }
    }
    Ok(())
}

fn retained_bytes(
    entries: &BTreeMap<String, ReplicatedEntry>,
    storage_class: CacheStorageClass,
) -> usize {
    entries
        .iter()
        .filter(|(_, entry)| entry.storage_class == storage_class)
        .map(|(key, entry)| entry_size_bytes(key, entry))
        .fold(0_usize, usize::saturating_add)
}

fn entry_size_bytes(key: &str, entry: &ReplicatedEntry) -> usize {
    let value_bytes = serde_json::to_vec(&entry.value).map_or(usize::MAX, |encoded| encoded.len());
    key.len().saturating_add(value_bytes).saturating_add(40)
}

fn eviction_rank(
    policy: EvictionPolicy,
    next_revision: u64,
    key: &str,
    entry: &ReplicatedEntry,
) -> (u64, u64, [u8; 32]) {
    match policy {
        EvictionPolicy::AllKeysLru | EvictionPolicy::VolatileLru => {
            (entry.last_access_revision, 0, [0; 32])
        }
        EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu => {
            (entry.access_count, entry.last_access_revision, [0; 32])
        }
        EvictionPolicy::VolatileTtl => (entry.expires_at_ms.unwrap_or(u64::MAX), 0, [0; 32]),
        EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => {
            let mut hasher = Sha256::new();
            hasher.update(b"epoch/cache-shard/eviction-rank/v1\0");
            hasher.update(next_revision.to_be_bytes());
            hasher.update(key.as_bytes());
            (0, 0, hasher.finalize().into())
        }
        EvictionPolicy::NoEviction => (u64::MAX, u64::MAX, [u8::MAX; 32]),
    }
}

const fn tracks_committed_access(policy: EvictionPolicy) -> bool {
    matches!(
        policy,
        EvictionPolicy::AllKeysLru
            | EvictionPolicy::AllKeysLfu
            | EvictionPolicy::VolatileLru
            | EvictionPolicy::VolatileLfu
    )
}

const fn initial_access_revision(policy: EvictionPolicy, revision: u64) -> u64 {
    if tracks_committed_access(policy) {
        revision
    } else {
        0
    }
}

fn next_access_metadata(
    current: Option<&ReplicatedEntry>,
    policy: EvictionPolicy,
    revision: u64,
) -> (u64, u64) {
    if !tracks_committed_access(policy) {
        return (0, 0);
    }
    current.map_or((revision, 0), |entry| {
        (revision, entry.access_count.saturating_add(1))
    })
}

fn record_committed_access(entry: &mut ReplicatedEntry, policy: EvictionPolicy, revision: u64) {
    if tracks_committed_access(policy) {
        entry.last_access_revision = revision;
        entry.access_count = entry.access_count.saturating_add(1);
    }
}

const fn eviction_policy_code(policy: EvictionPolicy) -> u8 {
    match policy {
        EvictionPolicy::NoEviction => 0,
        EvictionPolicy::AllKeysLru => 1,
        EvictionPolicy::AllKeysLfu => 2,
        EvictionPolicy::AllKeysRandom => 3,
        EvictionPolicy::VolatileLru => 4,
        EvictionPolicy::VolatileLfu => 5,
        EvictionPolicy::VolatileRandom => 6,
        EvictionPolicy::VolatileTtl => 7,
    }
}

const fn storage_class_code(storage_class: CacheStorageClass) -> u8 {
    match storage_class {
        CacheStorageClass::Memory => 0,
        CacheStorageClass::Cold => 1,
    }
}

// Serde skip predicates are required to accept a shared reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_memory_storage(value: &CacheStorageClass) -> bool {
    matches!(value, CacheStorageClass::Memory)
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_no_eviction(policy: &EvictionPolicy) -> bool {
    matches!(policy, EvictionPolicy::NoEviction)
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
    match value {
        CacheValue::SortedSet(members) if members.values().any(|score| !score.is_finite()) => {
            return Err(EpochError::InvalidArgument(
                "sorted-set scores must be finite".into(),
            ));
        }
        CacheValue::Bitmap(value) => value.validate()?,
        CacheValue::Cardinality(value) => value.validate()?,
        CacheValue::Bloom(value) => value.validate()?,
        CacheValue::Cuckoo(value) => value.validate()?,
        CacheValue::Geo(value) => value.validate()?,
        CacheValue::Json(value) => value.validate()?,
        CacheValue::JsonIndex(value) => value.validate()?,
        CacheValue::Vector(value) => value.validate()?,
        CacheValue::String(_)
        | CacheValue::Blob(_)
        | CacheValue::Counter(_)
        | CacheValue::Hash(_)
        | CacheValue::List(_)
        | CacheValue::Set(_)
        | CacheValue::SortedSet(_) => {}
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
            CacheValue::Bitmap(value) => self.advanced_value(7, value),
            CacheValue::Cardinality(value) => self.advanced_value(8, value),
            CacheValue::Bloom(value) => self.advanced_value(9, value),
            CacheValue::Cuckoo(value) => self.advanced_value(10, value),
            CacheValue::Geo(value) => self.advanced_value(11, value),
            CacheValue::Json(value) => self.advanced_value(12, value),
            CacheValue::Vector(value) => self.advanced_value(13, value),
            CacheValue::JsonIndex(value) => self.advanced_value(14, value),
        }
    }

    fn advanced_value<T: Serialize>(&mut self, kind: u8, value: &T) {
        self.u8(kind);
        let encoded = serde_json::to_vec(value)
            .expect("validated advanced Cache values always serialize to canonical JSON");
        self.length_prefixed(&encoded);
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
    use crate::{CacheValue, EvictionPolicy, SetOptions};

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
    fn native_snapshot_round_trips_complete_shard_state_and_continues() {
        let mut shard = CacheShard::new(10, Some(100)).unwrap();
        shard
            .transact(
                transaction(vec![
                    set("alpha", CacheValue::String("one".into())),
                    set("counter", CacheValue::Counter(2)),
                ]),
                10,
            )
            .unwrap();
        let encoded = shard.encode_snapshot().unwrap();
        let mut restored = CacheShard::decode_snapshot(&encoded).unwrap();

        assert_eq!(restored.revision(), shard.revision());
        assert_eq!(
            restored.recovery_state_digest(),
            shard.recovery_state_digest()
        );
        assert_eq!(restored.observe("alpha", 10), shard.observe("alpha", 10));
        assert_eq!(restored.encode_snapshot().unwrap(), encoded);
        restored
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
            restored.observe("counter", 20).item.unwrap().value,
            CacheValue::Counter(5)
        );
    }

    #[test]
    fn native_snapshot_rejects_noncanonical_unknown_or_corrupt_images() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![set("alpha", CacheValue::String("one".into()))]),
                10,
            )
            .unwrap();
        let encoded = shard.encode_snapshot().unwrap();
        let snapshot: VersionedCacheShardSnapshot = serde_json::from_slice(&encoded).unwrap();
        assert!(
            CacheShard::decode_snapshot(&serde_json::to_vec_pretty(&snapshot).unwrap()).is_err()
        );

        let mut unknown = snapshot.clone();
        unknown.format_version = 99;
        assert!(CacheShard::decode_snapshot(&serde_json::to_vec(&unknown).unwrap()).is_err());

        let mut corrupt = snapshot;
        corrupt.state_digest[0] ^= 1;
        assert!(CacheShard::decode_snapshot(&serde_json::to_vec(&corrupt).unwrap()).is_err());
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
                    storage_class: CacheStorageClass::Memory,
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
                    storage_class: CacheStorageClass::Memory,
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
                    storage_class: CacheStorageClass::Memory,
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
                storage_class: CacheStorageClass::Memory,
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
                storage_class: CacheStorageClass::Memory,
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
    fn next_expiry_deadline_tracks_retained_values() {
        let mut shard = CacheShard::new(10, None).unwrap();
        shard
            .transact(
                transaction(vec![
                    CacheMutation::Set {
                        key: "later".into(),
                        value: CacheValue::Counter(1),
                        options: SetOptions {
                            ttl_ms: Some(10),
                            ..SetOptions::default()
                        },
                    },
                    CacheMutation::Set {
                        key: "first".into(),
                        value: CacheValue::Counter(2),
                        options: SetOptions {
                            ttl_ms: Some(5),
                            ..SetOptions::default()
                        },
                    },
                ]),
                100,
            )
            .unwrap();

        assert_eq!(shard.next_expiry_deadline_ms(), Some(105));
        shard.maintain_expiry(105, 1).unwrap();
        assert_eq!(shard.next_expiry_deadline_ms(), Some(110));
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
    fn committed_access_drives_lru_and_is_reported_in_order() {
        let mut shard = CacheShard::new_with_eviction(2, None, EvictionPolicy::AllKeysLru).unwrap();
        shard
            .transact(
                transaction(vec![
                    set("alpha", CacheValue::Counter(1)),
                    set("beta", CacheValue::Counter(2)),
                ]),
                10,
            )
            .unwrap();

        let accessed = shard
            .transact(
                transaction(vec![CacheMutation::Access {
                    key: "alpha".into(),
                }]),
                11,
            )
            .unwrap();
        assert_eq!(accessed.revision, 2);
        assert!(matches!(
            accessed.results.as_slice(),
            [CacheMutationResult::Access { item: Some(item) }]
                if item.value == CacheValue::Counter(1)
        ));

        let admitted = shard
            .transact(transaction(vec![set("gamma", CacheValue::Counter(3))]), 12)
            .unwrap();
        assert_eq!(admitted.evicted_keys, ["beta"]);
        assert!(shard.observe("alpha", 12).item.is_some());
        assert!(shard.observe("beta", 12).item.is_none());
        assert!(shard.observe("gamma", 12).item.is_some());
    }

    #[test]
    fn lfu_prefers_the_least_frequently_committed_access() {
        let mut shard = CacheShard::new_with_eviction(2, None, EvictionPolicy::AllKeysLfu).unwrap();
        shard
            .transact(
                transaction(vec![
                    set("alpha", CacheValue::Counter(1)),
                    set("beta", CacheValue::Counter(2)),
                ]),
                10,
            )
            .unwrap();
        for _ in 0..2 {
            shard
                .transact(
                    transaction(vec![CacheMutation::Access {
                        key: "alpha".into(),
                    }]),
                    11,
                )
                .unwrap();
        }
        shard
            .transact(
                transaction(vec![CacheMutation::Access { key: "beta".into() }]),
                11,
            )
            .unwrap();

        let admitted = shard
            .transact(transaction(vec![set("gamma", CacheValue::Counter(3))]), 12)
            .unwrap();
        assert_eq!(admitted.evicted_keys, ["beta"]);
    }

    #[test]
    fn volatile_ttl_uses_deadline_and_rejects_without_an_eligible_victim() {
        let mut shard =
            CacheShard::new_with_eviction(2, None, EvictionPolicy::VolatileTtl).unwrap();
        shard
            .transact(
                transaction(vec![
                    CacheMutation::Set {
                        key: "later".into(),
                        value: CacheValue::Counter(1),
                        options: SetOptions {
                            ttl_ms: Some(20),
                            ..SetOptions::default()
                        },
                    },
                    CacheMutation::Set {
                        key: "first".into(),
                        value: CacheValue::Counter(2),
                        options: SetOptions {
                            ttl_ms: Some(10),
                            ..SetOptions::default()
                        },
                    },
                ]),
                100,
            )
            .unwrap();
        let admitted = shard
            .transact(
                transaction(vec![set("permanent", CacheValue::Counter(3))]),
                101,
            )
            .unwrap();
        assert_eq!(admitted.evicted_keys, ["first"]);

        let mut no_candidate =
            CacheShard::new_with_eviction(1, None, EvictionPolicy::VolatileLru).unwrap();
        no_candidate
            .transact(
                transaction(vec![set("permanent", CacheValue::Counter(1))]),
                1,
            )
            .unwrap();
        let before = no_candidate.recovery_state_digest();
        let error = no_candidate
            .transact(transaction(vec![set("new", CacheValue::Counter(2))]), 2)
            .unwrap_err();
        assert!(matches!(error, EpochError::Capacity(_)));
        assert_eq!(no_candidate.revision(), 1);
        assert_eq!(no_candidate.recovery_state_digest(), before);
    }

    #[test]
    fn deterministic_random_eviction_replays_and_restores_exactly() {
        let mut live =
            CacheShard::new_with_eviction(3, None, EvictionPolicy::AllKeysRandom).unwrap();
        live.transact(
            transaction(vec![
                set("alpha", CacheValue::Counter(1)),
                set("beta", CacheValue::Counter(2)),
                set("gamma", CacheValue::Counter(3)),
            ]),
            1,
        )
        .unwrap();
        let image = live.encode_snapshot().unwrap();
        let mut restored = CacheShard::decode_snapshot(&image).unwrap();

        let left = live
            .transact(transaction(vec![set("delta", CacheValue::Counter(4))]), 2)
            .unwrap();
        let right = restored
            .transact(transaction(vec![set("delta", CacheValue::Counter(4))]), 2)
            .unwrap();

        assert_eq!(left.evicted_keys, right.evicted_keys);
        assert_eq!(
            live.recovery_state_digest(),
            restored.recovery_state_digest()
        );
        assert_eq!(
            live.encode_snapshot().unwrap(),
            restored.encode_snapshot().unwrap()
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
        assert_eq!(golden, 0xde6c_ff6f);
        let digest = shard.recovery_state_digest();
        assert_eq!(
            digest,
            [
                0xf5, 0x4c, 0x80, 0x03, 0x6b, 0x6e, 0x62, 0xbf, 0xf2, 0xd0, 0xfd, 0x93, 0x42, 0x3f,
                0xaa, 0xce, 0x56, 0x7a, 0x4c, 0xe9, 0x56, 0x02, 0x49, 0xb3, 0x95, 0xcf, 0xaf, 0x96,
                0x0a, 0xd5, 0x12, 0x0b,
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
                        storage_class: CacheStorageClass::Memory,
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

    #[test]
    fn durable_change_stream_covers_mutation_eviction_and_expiry() {
        let mut shard = CacheShard::new_with_eviction(2, None, EvictionPolicy::AllKeysLru).unwrap();
        shard
            .transact(
                transaction(vec![CacheMutation::Set {
                    key: "expires".into(),
                    value: CacheValue::String("v".into()),
                    options: SetOptions {
                        ttl_ms: Some(5),
                        ..SetOptions::default()
                    },
                }]),
                10,
            )
            .unwrap();
        shard
            .transact(transaction(vec![set("keep", CacheValue::Counter(1))]), 11)
            .unwrap();
        shard.maintain_expiry(15, 10).unwrap();
        shard
            .transact(transaction(vec![set("other", CacheValue::Counter(2))]), 16)
            .unwrap();
        shard
            .transact(
                transaction(vec![set("overflow", CacheValue::Counter(3))]),
                17,
            )
            .unwrap();

        let changes = shard.changes_from(1, 10).unwrap();
        assert_eq!(
            changes.iter().map(|change| change.kind).collect::<Vec<_>>(),
            vec![
                CacheChangeKind::Set,
                CacheChangeKind::Set,
                CacheChangeKind::Expire,
                CacheChangeKind::Set,
                CacheChangeKind::Evict,
                CacheChangeKind::Set,
            ]
        );
        let restored = CacheShard::decode_snapshot(&shard.encode_snapshot().unwrap()).unwrap();
        assert_eq!(restored.changes_from(1, 10).unwrap(), changes);
        assert_eq!(
            restored.recovery_state_digest(),
            shard.recovery_state_digest()
        );
    }

    #[test]
    fn backup_is_canonical_and_pitr_restore_creates_non_aba_versions() {
        let mut source = CacheShard::new(10, None).unwrap();
        source
            .transact(
                transaction(vec![set("key", CacheValue::String("one".into()))]),
                10,
            )
            .unwrap();
        source
            .transact(
                transaction(vec![set("key", CacheValue::String("two".into()))]),
                20,
            )
            .unwrap();
        let backup = source.encode_backup(25).unwrap();
        let metadata = CacheShard::inspect_backup(&backup).unwrap();
        assert_eq!(metadata.captured_revision, 2);
        assert_eq!(metadata.oldest_restorable_revision, 0);

        let mut target = source.clone();
        let result = target.restore_backup(&backup, 1, 30).unwrap();
        assert_eq!(result.revision, 3);
        assert_eq!(result.restored_from_revision, 1);
        let item = target.observe("key", 30).item.unwrap();
        assert_eq!(item.value, CacheValue::String("one".into()));
        assert_eq!(item.version, 3);
        assert_eq!(
            target.changes_from(1, 10).unwrap().last().unwrap().kind,
            CacheChangeKind::Restore
        );

        let mut corrupt = backup;
        let middle = corrupt.len() / 2;
        corrupt[middle] ^= 1;
        assert!(CacheShard::inspect_backup(&corrupt).is_err());
    }

    fn assert_mismatched_transform_rolls_back(shard: &mut CacheShard) {
        let checksum = shard.recovery_state_checksum();
        assert!(
            shard
                .transact(
                    CacheTransaction {
                        expected_revision: Some(1),
                        operations: vec![CacheMutation::Transform {
                            key: "json".into(),
                            transform: CacheTransform::BitmapSet {
                                bit: 1,
                                value: true,
                            },
                            expected_version: Some(1),
                            ttl_ms: None,
                        }],
                    },
                    11,
                )
                .is_err()
        );
        assert_eq!(shard.recovery_state_checksum(), checksum);
    }

    #[test]
    fn typed_transforms_are_atomic_versioned_and_snapshot_safe() {
        let mut shard = CacheShard::new(32, None).unwrap();
        let operations = vec![
            CacheMutation::Transform {
                key: "hash".into(),
                transform: CacheTransform::HashPut {
                    field: "name".into(),
                    value: "Ada".into(),
                },
                expected_version: None,
                ttl_ms: None,
            },
            CacheMutation::Transform {
                key: "bitmap".into(),
                transform: CacheTransform::BitmapSet {
                    bit: 63,
                    value: true,
                },
                expected_version: None,
                ttl_ms: None,
            },
            CacheMutation::Transform {
                key: "cardinality".into(),
                transform: CacheTransform::CardinalityAdd {
                    value: b"member".to_vec(),
                    precision: 10,
                },
                expected_version: None,
                ttl_ms: None,
            },
            CacheMutation::Transform {
                key: "bloom".into(),
                transform: CacheTransform::BloomAdd {
                    value: b"member".to_vec(),
                    bit_count: 1_024,
                    hashes: 5,
                },
                expected_version: None,
                ttl_ms: None,
            },
            CacheMutation::Transform {
                key: "geo".into(),
                transform: CacheTransform::GeoUpsert {
                    member: "kolkata".into(),
                    point: crate::CacheGeoPoint::from_degrees(88.3639, 22.5726).unwrap(),
                },
                expected_version: None,
                ttl_ms: None,
            },
            CacheMutation::Transform {
                key: "json".into(),
                transform: CacheTransform::JsonSet {
                    pointer: "/user/name".into(),
                    value: serde_json::Value::String("Ada".into()),
                },
                expected_version: None,
                ttl_ms: None,
            },
            CacheMutation::Transform {
                key: "vector".into(),
                transform: CacheTransform::VectorUpsert {
                    id: "doc".into(),
                    document: crate::CacheVectorDocument::new(
                        vec![1.0, 0.0],
                        "epoch",
                        BTreeMap::new(),
                    )
                    .unwrap(),
                },
                expected_version: None,
                ttl_ms: None,
            },
        ];
        let result = shard.transact(transaction(operations), 10).unwrap();
        assert_eq!(result.results.len(), 7);
        assert!(result.results.iter().all(|result| matches!(result, CacheMutationResult::Transform { item, .. } if item.version == 1)));
        let restored = CacheShard::decode_snapshot(&shard.encode_snapshot().unwrap()).unwrap();
        assert_eq!(
            restored.recovery_state_digest(),
            shard.recovery_state_digest()
        );

        assert_mismatched_transform_rolls_back(&mut shard);
    }

    #[test]
    fn byte_admission_evicts_deterministically_and_cold_tier_is_explicit() {
        let value = CacheValue::String("x".repeat(64));
        let mut measured = CacheShard::new(10, None).unwrap();
        measured
            .transact(transaction(vec![set("a", value.clone())]), 1)
            .unwrap();
        let one_item_bytes = measured.retained_bytes(CacheStorageClass::Memory);

        let mut memory = CacheShard::new_with_limits(
            10,
            None,
            EvictionPolicy::AllKeysLru,
            Some(one_item_bytes),
            None,
        )
        .unwrap();
        memory
            .transact(transaction(vec![set("a", value.clone())]), 1)
            .unwrap();
        let result = memory
            .transact(transaction(vec![set("b", value.clone())]), 2)
            .unwrap();
        assert_eq!(result.evicted_keys, ["a"]);
        assert!(memory.observe("a", 2).item.is_none());
        assert!(memory.retained_bytes(CacheStorageClass::Memory) <= one_item_bytes);

        let cold_write = CacheMutation::Transform {
            key: "archive".into(),
            transform: CacheTransform::Replace {
                value: value.clone(),
                storage_class: CacheStorageClass::Cold,
            },
            expected_version: None,
            ttl_ms: None,
        };
        assert!(
            memory
                .transact(transaction(vec![cold_write.clone()]), 3)
                .is_err()
        );

        let mut tiered = CacheShard::new_with_limits(
            10,
            None,
            EvictionPolicy::NoEviction,
            Some(one_item_bytes),
            Some(one_item_bytes.saturating_add(16)),
        )
        .unwrap();
        tiered.transact(transaction(vec![cold_write]), 1).unwrap();
        let item = tiered.observe("archive", 1).item.unwrap();
        assert_eq!(item.storage_class, CacheStorageClass::Cold);
        assert_eq!(tiered.retained_bytes(CacheStorageClass::Memory), 0);
        assert!(tiered.retained_bytes(CacheStorageClass::Cold) > 0);
        let restored = CacheShard::decode_snapshot(&tiered.encode_snapshot().unwrap()).unwrap();
        assert_eq!(restored.observe("archive", 1).item, Some(item));
        assert_eq!(
            restored.recovery_state_digest(),
            tiered.recovery_state_digest()
        );
    }
}
