//! Experimental typed Cache tablet over the bounded-voter consensus runtime.
//!
//! This module is deliberately mounted only on the internal experimental
//! listener. It does not replace the standalone volatile Cache routes and does
//! not advertise a public `quorum_durable` profile.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    fs::{self, File, OpenOptions},
    io::Write,
    marker::PhantomData,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex as StdMutex, RwLock},
    time::{Duration, Instant},
};

use axum::{
    Extension, Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::StatusCode,
    routing::{delete, get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use epoch_cache::{
    CacheBackupMetadata, CacheBitmap, CacheBloomFilter, CacheCardinality, CacheChange,
    CacheChangeKind, CacheConfig, CacheCuckooFilter, CacheGeoHit, CacheGeoIndex, CacheGeoPoint,
    CacheJsonDocument, CacheJsonHit, CacheJsonIndex, CacheShard, CacheStorageClass, CacheTransform,
    CacheValue, CacheVectorHit, CacheVectorIndex, EvictionPolicy,
};
use epoch_consensus::{
    ApplicationSnapshot, CommittedProposal, ConsensusError, ConsensusRole, ConsensusStatus,
    LogIndex, ProposalLookup,
};
use epoch_core::{Clock, DurabilityProfile};
use epoch_tablet::{
    CacheAcquireLockCommand, CacheCasExpectation, CacheCompareAndSetCommand, CacheDeleteCommand,
    CacheGetCommand, CacheIncrementCommand, CacheLockGuard, CacheMaintainCommand,
    CacheReleaseLockCommand, CacheRenewLockCommand, CacheRestoreCommand, CacheSetCommand,
    CacheTablet, CacheTabletCommand, CacheTabletDisposition, CacheTabletItem,
    CacheTabletObservation, CacheTabletOperation, CacheTabletReceipt, CacheTabletScope,
    CacheTransactionCommand, CacheTransactionMutation, CacheTransformCommand, CommittedCommand,
    MAX_CACHE_KEY_BYTES, MAX_CACHE_TABLET_COMMAND_BYTES, TabletError, cache_proposal_id_for,
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor},
};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, broadcast};

use crate::consensus::{CommittedProposalApplier, ConsensusProbeError, ConsensusProbeHandle};
use crate::regional_maintenance::{RegionalMaintenanceOperation, RegionalMaintenanceProposal};
use crate::tablet_http::{
    TabletApiError, TabletApiResult, TabletReadMetadata, deserialize_i64_from_number_or_decimal,
    deserialize_optional_u64_from_number_or_decimal, deserialize_u64_from_number_or_decimal,
    hex_digest, serialize_optional_u64_as_decimal, serialize_u64_as_decimal, tablet_read_metadata,
};

pub const EXPERIMENTAL_CACHE_TABLET_STATUS_PATH: &str = "/experimental/v1/tablets/cache/status";
pub const EXPERIMENTAL_CACHE_TABLET_MUTATIONS_PATH: &str =
    "/experimental/v1/tablets/cache/mutations";
pub const EXPERIMENTAL_CACHE_TABLET_MULTIPLEX_PATH: &str =
    "/experimental/v1/tablets/cache/multiplex";
pub const EXPERIMENTAL_CACHE_TABLET_MUTATION_PATH: &str =
    "/experimental/v1/tablets/cache/mutations/{proposal_id}";
pub const EXPERIMENTAL_CACHE_TABLET_OBSERVATIONS_PATH: &str =
    "/experimental/v1/tablets/cache/observations";
pub const EXPERIMENTAL_CACHE_TABLET_CHANGES_PATH: &str = "/experimental/v1/tablets/cache/changes";
pub const EXPERIMENTAL_CACHE_TABLET_BACKUP_PATH: &str = "/experimental/v1/tablets/cache/backup";
pub const EXPERIMENTAL_CACHE_TABLET_QUERY_PATH: &str = "/experimental/v1/tablets/cache/query";
pub const EXPERIMENTAL_CACHE_PUBSUB_SUBSCRIPTIONS_PATH: &str =
    "/experimental/v1/tablets/cache/pubsub/subscriptions";
pub const EXPERIMENTAL_CACHE_PUBSUB_SUBSCRIPTION_PATH: &str =
    "/experimental/v1/tablets/cache/pubsub/subscriptions/{subscription_id}";
pub const EXPERIMENTAL_CACHE_PUBSUB_MESSAGES_PATH: &str =
    "/experimental/v1/tablets/cache/pubsub/messages";
pub const EXPERIMENTAL_CACHE_PUBSUB_SUBSCRIPTION_MESSAGES_PATH: &str =
    "/experimental/v1/tablets/cache/pubsub/subscriptions/{subscription_id}/messages";

const TABLET_REQUEST_BODY_BYTES: usize = MAX_CACHE_TABLET_COMMAND_BYTES + 16 * 1024;
const CACHE_APPLICATION_SNAPSHOT_FORMAT_ID: [u8; 16] = *b"CACHE___STATE_V1";
const CACHE_APPLICATION_SNAPSHOT_VERSION: u16 = 1;
pub const DEFAULT_COMMIT_WAIT: Duration = Duration::from_secs(5);

const MAX_CACHE_PUBSUB_SUBSCRIPTIONS: usize = 10_000;
const MAX_CACHE_PUBSUB_FILTERS: usize = 64;
const MAX_CACHE_PUBSUB_CHANNEL_BYTES: usize = 1_024;
const MAX_CACHE_PUBSUB_PAYLOAD_BYTES: usize = 64 * 1024;
const MAX_CACHE_PUBSUB_PENDING_MESSAGES: usize = 1_024;
const MAX_CACHE_PUBSUB_POLL_MESSAGES: usize = 1_000;
const MAX_CACHE_MULTIPLEX_MUTATIONS: usize = 128;
const MAX_CACHE_CORRELATION_ID_BYTES: usize = 256;

#[derive(Debug, Clone, Serialize)]
struct CachePubSubMessage {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    sequence: u64,
    channel: String,
    payload: serde_json::Value,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    published_at_ms: u64,
}

#[derive(Debug)]
struct CachePubSubSubscription {
    channels: BTreeSet<String>,
    patterns: BTreeSet<String>,
    pending: VecDeque<CachePubSubMessage>,
    dropped_messages: u64,
}

#[derive(Debug, Default)]
struct CachePubSubHub {
    next_subscription_sequence: u64,
    next_message_sequence: u64,
    subscriptions: BTreeMap<String, CachePubSubSubscription>,
}

#[derive(Debug)]
struct CachePubSubPoll {
    messages: Vec<CachePubSubMessage>,
    dropped_messages: u64,
    remaining_messages: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColdStoredItem {
    key: String,
    item: CacheTabletItem,
}

#[derive(Debug)]
struct CacheColdStore {
    directory: PathBuf,
    read_count: AtomicU64,
    total_read_micros: AtomicU64,
    max_read_micros: AtomicU64,
}

impl CacheColdStore {
    fn open(directory: impl Into<PathBuf>) -> Result<Self, TabletError> {
        let directory = directory.into();
        fs::create_dir_all(&directory).map_err(|error| {
            TabletError::InvalidCommand(format!("Cache cold store could not be created: {error}"))
        })?;
        Ok(Self {
            directory,
            read_count: AtomicU64::new(0),
            total_read_micros: AtomicU64::new(0),
            max_read_micros: AtomicU64::new(0),
        })
    }

    fn synchronize(&self, items: Vec<(String, CacheTabletItem)>) -> Result<(), String> {
        let desired = items
            .into_iter()
            .map(|(key, item)| {
                let filename = cold_filename(&key);
                let bytes = serde_json::to_vec(&ColdStoredItem { key, item })
                    .map_err(|error| error.to_string())?;
                Ok((filename, bytes))
            })
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        let mut changed = false;
        for (filename, bytes) in &desired {
            let path = self.directory.join(filename);
            if fs::read(&path).is_ok_and(|existing| existing == *bytes) {
                continue;
            }
            let temporary = self.directory.join(format!("{filename}.tmp"));
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)
                .map_err(|error| format!("Cache cold item could not be opened: {error}"))?;
            file.write_all(bytes)
                .and_then(|()| file.sync_all())
                .map_err(|error| format!("Cache cold item could not be persisted: {error}"))?;
            drop(file);
            fs::rename(&temporary, &path)
                .map_err(|error| format!("Cache cold item could not be installed: {error}"))?;
            changed = true;
        }
        for entry in fs::read_dir(&self.directory)
            .map_err(|error| format!("Cache cold store could not be listed: {error}"))?
        {
            let path = entry
                .map_err(|error| format!("Cache cold store entry is invalid: {error}"))?
                .path();
            let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if path
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
                && !desired.contains_key(filename)
            {
                fs::remove_file(&path).map_err(|error| {
                    format!("stale Cache cold item could not be removed: {error}")
                })?;
                changed = true;
            } else if filename.ends_with(".json.tmp") {
                fs::remove_file(&path).map_err(|error| {
                    format!("stale Cache cold temporary item could not be removed: {error}")
                })?;
                changed = true;
            }
        }
        if changed {
            File::open(&self.directory)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| format!("Cache cold directory could not be persisted: {error}"))?;
        }
        Ok(())
    }

    fn load(&self, key: &str) -> Result<CacheTabletItem, String> {
        let started = Instant::now();
        let result = (|| {
            let bytes = fs::read(self.directory.join(cold_filename(key)))
                .map_err(|error| format!("Cache cold item could not be read: {error}"))?;
            let stored: ColdStoredItem = serde_json::from_slice(&bytes)
                .map_err(|error| format!("Cache cold item is invalid: {error}"))?;
            if stored.key != key
                || serde_json::to_vec(&stored).map_err(|error| error.to_string())? != bytes
                || stored.item.storage_class != CacheStorageClass::Cold
            {
                return Err("Cache cold item is non-canonical or mismatched".into());
            }
            Ok(stored.item)
        })();
        let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.read_count.fetch_add(1, Ordering::Relaxed);
        self.total_read_micros.fetch_add(micros, Ordering::Relaxed);
        self.max_read_micros.fetch_max(micros, Ordering::Relaxed);
        result
    }

    fn metrics(&self) -> (u64, u64, u64) {
        let count = self.read_count.load(Ordering::Relaxed);
        let total = self.total_read_micros.load(Ordering::Relaxed);
        let maximum = self.max_read_micros.load(Ordering::Relaxed);
        (count, total.checked_div(count).unwrap_or(0), maximum)
    }
}

fn cold_filename(key: &str) -> String {
    let digest: [u8; 32] = Sha256::digest(key.as_bytes()).into();
    format!("{}.json", hex_digest(digest))
}

impl CachePubSubHub {
    fn subscribe(
        &mut self,
        tablet_id: u64,
        channels: BTreeSet<String>,
        patterns: BTreeSet<String>,
    ) -> Result<String, String> {
        validate_pubsub_filters(&channels, &patterns)?;
        if self.subscriptions.len() >= MAX_CACHE_PUBSUB_SUBSCRIPTIONS {
            return Err(format!(
                "Cache Pub/Sub supports at most {MAX_CACHE_PUBSUB_SUBSCRIPTIONS} node-local subscriptions"
            ));
        }
        self.next_subscription_sequence = self
            .next_subscription_sequence
            .checked_add(1)
            .ok_or_else(|| "Cache Pub/Sub subscription sequence is exhausted".to_owned())?;
        let subscription_id = format!("cache-{tablet_id}-{}", self.next_subscription_sequence);
        self.subscriptions.insert(
            subscription_id.clone(),
            CachePubSubSubscription {
                channels,
                patterns,
                pending: VecDeque::new(),
                dropped_messages: 0,
            },
        );
        Ok(subscription_id)
    }

    fn unsubscribe(&mut self, subscription_id: &str) -> bool {
        self.subscriptions.remove(subscription_id).is_some()
    }

    fn publish(
        &mut self,
        channel: &str,
        payload: &serde_json::Value,
        published_at_ms: u64,
    ) -> Result<(u64, usize, usize), String> {
        validate_pubsub_channel(channel)?;
        let payload_bytes = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
        if payload_bytes.len() > MAX_CACHE_PUBSUB_PAYLOAD_BYTES {
            return Err(format!(
                "Cache Pub/Sub payload is {} bytes; maximum is {MAX_CACHE_PUBSUB_PAYLOAD_BYTES}",
                payload_bytes.len()
            ));
        }
        self.next_message_sequence = self
            .next_message_sequence
            .checked_add(1)
            .ok_or_else(|| "Cache Pub/Sub message sequence is exhausted".to_owned())?;
        let sequence = self.next_message_sequence;
        let mut delivered = 0;
        let mut dropped = 0;
        for subscription in self.subscriptions.values_mut() {
            if !subscription_matches(subscription, channel) {
                continue;
            }
            if subscription.pending.len() >= MAX_CACHE_PUBSUB_PENDING_MESSAGES {
                subscription.dropped_messages = subscription.dropped_messages.saturating_add(1);
                dropped += 1;
                continue;
            }
            subscription.pending.push_back(CachePubSubMessage {
                sequence,
                channel: channel.to_owned(),
                payload: payload.clone(),
                published_at_ms,
            });
            delivered += 1;
        }
        Ok((sequence, delivered, dropped))
    }

    fn poll(&mut self, subscription_id: &str, limit: usize) -> Result<CachePubSubPoll, String> {
        if !(1..=MAX_CACHE_PUBSUB_POLL_MESSAGES).contains(&limit) {
            return Err(format!(
                "Cache Pub/Sub poll limit must be between 1 and {MAX_CACHE_PUBSUB_POLL_MESSAGES}"
            ));
        }
        let subscription = self
            .subscriptions
            .get_mut(subscription_id)
            .ok_or_else(|| format!("Cache Pub/Sub subscription is missing: {subscription_id}"))?;
        let messages = subscription
            .pending
            .drain(..limit.min(subscription.pending.len()))
            .collect();
        let dropped_messages = std::mem::take(&mut subscription.dropped_messages);
        Ok(CachePubSubPoll {
            messages,
            dropped_messages,
            remaining_messages: subscription.pending.len(),
        })
    }
}

fn validate_pubsub_filters(
    channels: &BTreeSet<String>,
    patterns: &BTreeSet<String>,
) -> Result<(), String> {
    if channels.is_empty() && patterns.is_empty() {
        return Err("Cache Pub/Sub subscription requires a channel or pattern".into());
    }
    if channels.len().saturating_add(patterns.len()) > MAX_CACHE_PUBSUB_FILTERS {
        return Err(format!(
            "Cache Pub/Sub subscription supports at most {MAX_CACHE_PUBSUB_FILTERS} filters"
        ));
    }
    for channel in channels {
        validate_pubsub_channel(channel)?;
    }
    for pattern in patterns {
        if pattern.is_empty()
            || pattern.len() > MAX_CACHE_PUBSUB_CHANNEL_BYTES
            || pattern.chars().any(char::is_control)
        {
            return Err(
                "Cache Pub/Sub pattern is empty, oversized, or contains control characters".into(),
            );
        }
    }
    Ok(())
}

fn validate_pubsub_channel(channel: &str) -> Result<(), String> {
    if channel.is_empty()
        || channel.len() > MAX_CACHE_PUBSUB_CHANNEL_BYTES
        || channel.chars().any(char::is_control)
    {
        return Err(
            "Cache Pub/Sub channel is empty, oversized, or contains control characters".into(),
        );
    }
    Ok(())
}

fn subscription_matches(subscription: &CachePubSubSubscription, channel: &str) -> bool {
    subscription.channels.contains(channel)
        || subscription
            .patterns
            .iter()
            .any(|pattern| glob_matches(pattern.as_bytes(), channel.as_bytes()))
}

fn glob_matches(pattern: &[u8], value: &[u8]) -> bool {
    let (mut pattern_index, mut value_index) = (0, 0);
    let (mut star_index, mut star_value_index) = (None, 0);
    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            star_value_index = value_index;
        } else if let Some(star) = star_index {
            pattern_index = star + 1;
            star_value_index += 1;
            value_index = star_value_index;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

#[derive(Debug)]
pub struct CacheTabletService {
    scope: CacheTabletScope,
    config: CacheConfig,
    tablet: RwLock<CacheTablet>,
    failure: RwLock<Option<String>>,
    pubsub: StdMutex<CachePubSubHub>,
    cold_store: Option<CacheColdStore>,
}

impl CacheTabletService {
    pub fn new(scope: CacheTabletScope, config: CacheConfig) -> Result<Arc<Self>, TabletError> {
        Self::new_with_cold_store(scope, config, None::<PathBuf>)
    }

    pub fn new_with_cold_store(
        scope: CacheTabletScope,
        config: CacheConfig,
        cold_directory: Option<impl Into<PathBuf>>,
    ) -> Result<Arc<Self>, TabletError> {
        let tablet = CacheTablet::new(scope.clone(), config.clone())?;
        let cold_store = cold_directory.map(CacheColdStore::open).transpose()?;
        Ok(Arc::new(Self {
            scope,
            config,
            tablet: RwLock::new(tablet),
            failure: RwLock::new(None),
            pubsub: StdMutex::new(CachePubSubHub::default()),
            cold_store,
        }))
    }

    pub fn with_default_config(scope: CacheTabletScope) -> Result<Arc<Self>, TabletError> {
        Self::new(scope, CacheConfig::default())
    }

    pub fn scope(&self) -> &CacheTabletScope {
        &self.scope
    }

    pub fn maintenance_proposals(
        &self,
        now_ms: u64,
    ) -> Result<Vec<RegionalMaintenanceProposal>, String> {
        self.ensure_healthy()?;
        let tablet = self
            .tablet
            .read()
            .map_err(|_| "Cache tablet read lock was poisoned".to_owned())?;
        let Some(due_at_ms) = tablet
            .next_maintenance_deadline_ms()
            .filter(|deadline_ms| *deadline_ms <= now_ms)
        else {
            return Ok(Vec::new());
        };
        let key = format!(
            "epoch-auto-{}-{due_at_ms}-{}",
            RegionalMaintenanceOperation::CacheExpiry.as_str(),
            tablet.last_applied_command_index()
        );
        let command = CacheTabletCommand::new(
            &self.scope,
            key,
            due_at_ms,
            CacheTabletOperation::Maintain(CacheMaintainCommand {
                shard: 0,
                max_expirations: epoch_tablet::MAX_CACHE_MAINTENANCE_EXPIRATIONS,
            }),
        )
        .map_err(|error| error.to_string())?;
        Ok(vec![RegionalMaintenanceProposal {
            operation: RegionalMaintenanceOperation::CacheExpiry,
            due_at_ms,
            proposal_id: command
                .proposal_id(&self.scope)
                .map_err(|error| error.to_string())?,
            payload: command
                .encode(&self.scope)
                .map_err(|error| error.to_string())?,
        }])
    }

    pub fn last_profile_mutation_index(&self) -> Result<u64, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Cache tablet read lock was poisoned".to_owned())
            .map(|tablet| tablet.last_applied_command_index())
    }

    pub fn last_applied_time_ms(&self) -> Result<u64, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Cache tablet read lock was poisoned".to_owned())
            .map(|tablet| tablet.last_applied_time_ms())
    }

    fn ensure_healthy(&self) -> Result<(), String> {
        let failure = self
            .failure
            .read()
            .map_err(|_| "Cache tablet failure lock was poisoned".to_owned())?;
        if let Some(failure) = failure.as_ref() {
            Err(failure.clone())
        } else {
            Ok(())
        }
    }

    fn fail(&self, error: impl Into<String>) -> String {
        let error = error.into();
        if let Ok(mut failure) = self.failure.write() {
            failure.get_or_insert_with(|| error.clone());
        }
        error
    }

    fn apply_one(&self, committed: &CommittedProposal) -> Result<CacheTabletReceipt, String> {
        self.ensure_healthy()?;
        let mut tablet = self
            .tablet
            .write()
            .map_err(|_| "Cache tablet write lock was poisoned".to_owned())?;
        let result = tablet
            .apply(committed_command(committed))
            .map_err(|error| error.to_string())
            .and_then(|receipt| {
                self.synchronize_cold_store(&tablet)?;
                Ok(receipt)
            });
        result.map_err(|error| self.fail(error))
    }

    fn committed_receipt(
        &self,
        committed: &CommittedProposal,
    ) -> Result<CacheTabletReceipt, String> {
        self.ensure_healthy()?;
        let result = self
            .tablet
            .read()
            .map_err(|_| self.fail("Cache tablet read lock was poisoned"))?
            .receipt_for_committed(committed_command(committed));
        match result {
            Ok(Some(receipt)) => Ok(receipt),
            Ok(None) => Err(self.fail(format!(
                "consensus commit {} was not applied by the Cache profile actor",
                committed.receipt.proposal_id
            ))),
            Err(error) => Err(self.fail(error.to_string())),
        }
    }

    fn observe(&self, key: &str) -> Result<CacheTabletObservation, String> {
        self.ensure_healthy()?;
        let mut observation = self
            .tablet
            .read()
            .map_err(|_| "Cache tablet read lock was poisoned".to_owned())
            .map(|tablet| tablet.observe(key))?;
        if observation
            .item
            .as_ref()
            .is_some_and(|item| item.storage_class == CacheStorageClass::Cold)
            && let Some(store) = &self.cold_store
        {
            let stored = store.load(key).map_err(|error| self.fail(error))?;
            if observation.item.as_ref() != Some(&stored) {
                return Err(self.fail("Cache cold item diverges from replicated state"));
            }
            observation.item = Some(stored);
        }
        Ok(observation)
    }

    fn changes_from(&self, sequence: u64, limit: usize) -> Result<Vec<CacheChange>, String> {
        self.ensure_healthy()?;
        self.tablet
            .read()
            .map_err(|_| "Cache tablet read lock was poisoned".to_owned())?
            .changes_from(sequence, limit)
            .map_err(|error| error.to_string())
    }

    fn backup(&self) -> Result<(Vec<u8>, CacheBackupMetadata), String> {
        self.ensure_healthy()?;
        let encoded = self
            .tablet
            .read()
            .map_err(|_| "Cache tablet read lock was poisoned".to_owned())?
            .encode_backup()
            .map_err(|error| error.to_string())?;
        let metadata = CacheShard::inspect_backup(&encoded).map_err(|error| error.to_string())?;
        Ok((encoded, metadata))
    }

    fn pubsub_subscribe(
        &self,
        channels: BTreeSet<String>,
        patterns: BTreeSet<String>,
    ) -> Result<String, String> {
        self.ensure_healthy()?;
        self.pubsub
            .lock()
            .map_err(|_| "Cache Pub/Sub lock was poisoned".to_owned())?
            .subscribe(self.scope.tablet_id, channels, patterns)
    }

    fn pubsub_unsubscribe(&self, subscription_id: &str) -> Result<bool, String> {
        self.ensure_healthy()?;
        Ok(self
            .pubsub
            .lock()
            .map_err(|_| "Cache Pub/Sub lock was poisoned".to_owned())?
            .unsubscribe(subscription_id))
    }

    fn pubsub_publish(
        &self,
        channel: &str,
        payload: &serde_json::Value,
        published_at_ms: u64,
    ) -> Result<(u64, usize, usize), String> {
        self.ensure_healthy()?;
        self.pubsub
            .lock()
            .map_err(|_| "Cache Pub/Sub lock was poisoned".to_owned())?
            .publish(channel, payload, published_at_ms)
    }

    fn pubsub_poll(&self, subscription_id: &str, limit: usize) -> Result<CachePubSubPoll, String> {
        self.ensure_healthy()?;
        self.pubsub
            .lock()
            .map_err(|_| "Cache Pub/Sub lock was poisoned".to_owned())?
            .poll(subscription_id, limit)
    }

    fn snapshot(&self) -> Result<CacheTabletSnapshot, String> {
        self.ensure_healthy()?;
        let tablet = self
            .tablet
            .read()
            .map_err(|_| "Cache tablet read lock was poisoned".to_owned())?;
        let (cold_read_count, cold_read_average_micros, cold_read_max_micros) = self
            .cold_store
            .as_ref()
            .map_or((0, 0, 0), CacheColdStore::metrics);
        Ok(CacheTabletSnapshot {
            last_profile_mutation_index: tablet.last_applied_command_index(),
            last_applied_time_ms: tablet.last_applied_time_ms(),
            applied_command_count: usize_as_u64(
                tablet.applied_command_count(),
                "Cache tablet command count",
            )?,
            cache_revision: tablet.cache_revision(),
            retained_entry_count: usize_as_u64(
                tablet.cache_entry_count(),
                "Cache retained-entry count",
            )?,
            retained_memory_bytes: usize_as_u64(
                tablet.retained_memory_bytes(),
                "Cache retained-memory bytes",
            )?,
            retained_cold_bytes: usize_as_u64(
                tablet.retained_cold_bytes(),
                "Cache retained-cold bytes",
            )?,
            max_memory_bytes: tablet
                .max_memory_bytes()
                .map(|value| u64::try_from(value).unwrap_or(u64::MAX)),
            max_cold_bytes: tablet
                .max_cold_bytes()
                .map(|value| u64::try_from(value).unwrap_or(u64::MAX)),
            active_lock_count: usize_as_u64(tablet.active_lock_count(), "Cache active-lock count")?,
            eviction: tablet.eviction(),
            requested_durability: self.config.durability,
            cold_storage_backend: if self.cold_store.is_some() {
                "local_fsync_file_read_path"
            } else {
                "logical_class_in_replicated_memory"
            },
            cold_read_count,
            cold_read_average_micros,
            cold_read_max_micros,
            cache_recovery_state_digest: hex_digest(tablet.cache_recovery_state_digest()),
            state_digest: hex_digest(tablet.state_digest()),
        })
    }

    fn synchronize_cold_store(&self, tablet: &CacheTablet) -> Result<(), String> {
        if let Some(store) = &self.cold_store {
            store.synchronize(tablet.retained_cold_items())?;
        }
        Ok(())
    }
}

fn usize_as_u64(value: usize, field: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{field} exceeds u64"))
}

fn committed_command(committed: &CommittedProposal) -> CommittedCommand<'_> {
    CommittedCommand {
        group_id: committed.receipt.group_id.get(),
        group_epoch: committed.receipt.group_epoch.get(),
        proposal_id: committed.receipt.proposal_id.get(),
        term: committed.receipt.term.get(),
        log_index: committed.receipt.log_index.get(),
        payload: &committed.payload,
    }
}

impl CommittedProposalApplier for CacheTabletService {
    fn replay(&self, committed: &[CommittedProposal]) -> Result<(), String> {
        let mut history = committed.to_vec();
        history.sort_by_key(|proposal| proposal.receipt.log_index.get());
        let mut rebuilt = CacheTablet::new(self.scope.clone(), self.config.clone())
            .map_err(|error| error.to_string())?;
        for proposal in &history {
            rebuilt
                .apply(committed_command(proposal))
                .map_err(|error| self.fail(error.to_string()))?;
        }
        self.synchronize_cold_store(&rebuilt)
            .map_err(|error| self.fail(error))?;
        *self
            .tablet
            .write()
            .map_err(|_| self.fail("Cache tablet write lock was poisoned"))? = rebuilt;
        Ok(())
    }

    fn apply(&self, committed: &CommittedProposal) -> Result<(), String> {
        self.apply_one(committed).map(|_| ())
    }

    fn capture_snapshot(
        &self,
        checkpoint_index: LogIndex,
        retained: &[CommittedProposal],
    ) -> Result<ApplicationSnapshot, String> {
        self.ensure_healthy()?;
        let tablet = self
            .tablet
            .read()
            .map_err(|_| "Cache tablet read lock was poisoned".to_owned())?;
        if tablet.last_applied_command_index() > checkpoint_index.get() {
            return Err(format!(
                "Cache applied index {} exceeds consensus checkpoint index {}",
                tablet.last_applied_command_index(),
                checkpoint_index
            ));
        }
        let mut retained_ids = BTreeSet::new();
        for committed in retained {
            let proposal_id = committed.receipt.proposal_id.get();
            if !retained_ids.insert(proposal_id) {
                return Err(format!(
                    "Cache retry proposal {proposal_id} appears more than once"
                ));
            }
            tablet
                .receipt_for_committed(committed_command(committed))
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!("Cache retry proposal {proposal_id} has no typed applied result")
                })?;
        }
        let payload = tablet
            .encode_snapshot(&retained_ids)
            .map_err(|error| error.to_string())?;
        ApplicationSnapshot::new(
            checkpoint_index,
            CACHE_APPLICATION_SNAPSHOT_FORMAT_ID,
            CACHE_APPLICATION_SNAPSHOT_VERSION,
            tablet.state_digest(),
            payload,
        )
        .map_err(|error| error.to_string())
    }

    fn install_snapshot(&self, snapshot: &ApplicationSnapshot) -> Result<(), String> {
        self.ensure_healthy()?;
        let result: Result<CacheTablet, String> = (|| {
            if snapshot.format_id() != CACHE_APPLICATION_SNAPSHOT_FORMAT_ID
                || snapshot.format_version() != CACHE_APPLICATION_SNAPSHOT_VERSION
            {
                return Err("application snapshot is not a supported Cache image".into());
            }
            let restored = CacheTablet::decode_snapshot(&self.scope, snapshot.payload())
                .map_err(|error| error.to_string())?;
            if restored.last_applied_command_index() > snapshot.checkpoint_index().get()
                || restored.state_digest() != snapshot.state_digest()
                || restored.max_entries() != self.config.max_entries
                || restored.default_ttl_ms() != self.config.default_ttl_ms
                || restored.eviction() != self.config.eviction
                || restored.max_memory_bytes() != self.config.max_memory_bytes
                || restored.max_cold_bytes() != self.config.max_cold_bytes
            {
                return Err(
                    "Cache application snapshot index, state digest, or configuration is invalid"
                        .into(),
                );
            }
            Ok(restored)
        })();
        match result {
            Ok(restored) => {
                self.synchronize_cold_store(&restored)
                    .map_err(|error| self.fail(error))?;
                *self
                    .tablet
                    .write()
                    .map_err(|_| self.fail("Cache tablet write lock was poisoned"))? = restored;
                Ok(())
            }
            Err(error) => Err(self.fail(error)),
        }
    }

    fn supports_native_snapshots(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct CacheTabletApiState {
    service: Arc<CacheTabletService>,
    consensus: ConsensusProbeHandle,
    clock: Arc<dyn Clock>,
    commit_wait: Duration,
    write_serial: Arc<Mutex<()>>,
}

pub fn router(
    service: Arc<CacheTabletService>,
    consensus: ConsensusProbeHandle,
    clock: Arc<dyn Clock>,
    commit_wait: Duration,
) -> Router {
    let state = CacheTabletApiState {
        service,
        consensus,
        clock,
        commit_wait,
        write_serial: Arc::new(Mutex::new(())),
    };
    Router::new()
        .route(EXPERIMENTAL_CACHE_TABLET_STATUS_PATH, get(tablet_status))
        .route(
            EXPERIMENTAL_CACHE_TABLET_MUTATIONS_PATH,
            post(submit_mutation),
        )
        .route(
            EXPERIMENTAL_CACHE_TABLET_MULTIPLEX_PATH,
            post(submit_multiplex),
        )
        .route(
            EXPERIMENTAL_CACHE_TABLET_MUTATION_PATH,
            get(lookup_mutation),
        )
        .route(
            EXPERIMENTAL_CACHE_TABLET_OBSERVATIONS_PATH,
            get(observe_key),
        )
        .route(EXPERIMENTAL_CACHE_TABLET_CHANGES_PATH, get(read_changes))
        .route(EXPERIMENTAL_CACHE_TABLET_BACKUP_PATH, get(read_backup))
        .route(EXPERIMENTAL_CACHE_TABLET_QUERY_PATH, post(query_cache))
        .route(
            EXPERIMENTAL_CACHE_PUBSUB_SUBSCRIPTIONS_PATH,
            post(create_pubsub_subscription),
        )
        .route(
            EXPERIMENTAL_CACHE_PUBSUB_SUBSCRIPTION_PATH,
            delete(delete_pubsub_subscription),
        )
        .route(
            EXPERIMENTAL_CACHE_PUBSUB_MESSAGES_PATH,
            post(publish_pubsub_message),
        )
        .route(
            EXPERIMENTAL_CACHE_PUBSUB_SUBSCRIPTION_MESSAGES_PATH,
            get(poll_pubsub_messages),
        )
        .layer(DefaultBodyLimit::max(TABLET_REQUEST_BODY_BYTES))
        .with_state(state)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheMutationRequest {
    idempotency_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    operation: CacheOperationRequest,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheMultiplexRequest {
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_term: u64,
    mutations: Vec<CacheMultiplexMutationRequest>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheMultiplexMutationRequest {
    correlation_id: String,
    idempotency_key: String,
    operation: CacheOperationRequest,
}

#[derive(Debug, Serialize)]
struct CacheMultiplexResponse {
    atomic: bool,
    ordering: &'static str,
    results: Vec<CacheMultiplexResult>,
}

#[derive(Debug, Serialize)]
struct CacheMultiplexResult {
    correlation_id: String,
    http_status: u16,
    response: CacheTabletMutationResponse,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum CacheOperationRequest {
    Set {
        #[serde(default)]
        shard: u32,
        key: String,
        value: CacheValueRequest,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        ttl_ms: Option<u64>,
        #[serde(default)]
        storage_class: CacheStorageClass,
        #[serde(default)]
        lock_guard: Option<CacheLockGuardRequest>,
    },
    Delete {
        #[serde(default)]
        shard: u32,
        key: String,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        expected_version: Option<u64>,
        #[serde(default)]
        lock_guard: Option<CacheLockGuardRequest>,
    },
    CompareAndSet {
        #[serde(default)]
        shard: u32,
        key: String,
        expected: CacheCasExpectationRequest,
        value: CacheValueRequest,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        ttl_ms: Option<u64>,
        #[serde(default)]
        lock_guard: Option<CacheLockGuardRequest>,
    },
    Increment {
        #[serde(default)]
        shard: u32,
        key: String,
        #[serde(deserialize_with = "deserialize_i64_from_number_or_decimal")]
        delta: i64,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        expected_version: Option<u64>,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        ttl_ms: Option<u64>,
        #[serde(default)]
        lock_guard: Option<CacheLockGuardRequest>,
    },
    Transform {
        #[serde(default)]
        shard: u32,
        key: String,
        transform: CacheTransform,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        expected_version: Option<u64>,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        ttl_ms: Option<u64>,
        #[serde(default)]
        lock_guard: Option<CacheLockGuardRequest>,
    },
    Get {
        #[serde(default)]
        shard: u32,
        key: String,
    },
    Transaction {
        #[serde(default)]
        shard: u32,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        expected_revision: u64,
        mutations: Vec<CacheTransactionMutationRequest>,
        #[serde(default)]
        lock_guards: Vec<CacheLockGuardRequest>,
    },
    AcquireLock {
        #[serde(default)]
        shard: u32,
        lock_key: String,
        owner: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        owner_epoch: u64,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        lease_ms: u64,
    },
    RenewLock {
        #[serde(default)]
        shard: u32,
        lock_key: String,
        owner: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        owner_epoch: u64,
        lease_token: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        extension_ms: u64,
    },
    ReleaseLock {
        #[serde(default)]
        shard: u32,
        lock_key: String,
        owner: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        owner_epoch: u64,
        lease_token: String,
    },
    Maintain {
        #[serde(default)]
        shard: u32,
        max_expirations: u16,
    },
    Restore {
        #[serde(default)]
        shard: u32,
        backup_base64: String,
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        target_revision: u64,
    },
}

impl CacheOperationRequest {
    fn to_tablet_operation(&self) -> TabletApiResult<CacheTabletOperation> {
        if let Some(operation) = self.to_lifecycle_operation() {
            return Ok(operation);
        }
        Ok(match self {
            Self::Set {
                shard,
                key,
                value,
                ttl_ms,
                storage_class,
                lock_guard,
            } => set_operation(
                *shard,
                key,
                value,
                *ttl_ms,
                *storage_class,
                lock_guard.as_ref(),
            )?,
            Self::Delete {
                shard,
                key,
                expected_version,
                lock_guard,
            } => CacheTabletOperation::Delete(CacheDeleteCommand {
                shard: *shard,
                key: key.clone(),
                expected_version: *expected_version,
                lock_guard: lock_guard.as_ref().map(CacheLockGuardRequest::to_tablet),
            }),
            Self::CompareAndSet {
                shard,
                key,
                expected,
                value,
                ttl_ms,
                lock_guard,
            } => compare_and_set_operation(
                *shard,
                key,
                expected,
                value,
                *ttl_ms,
                lock_guard.as_ref(),
            )?,
            Self::Increment {
                shard,
                key,
                delta,
                expected_version,
                ttl_ms,
                lock_guard,
            } => CacheTabletOperation::Increment(CacheIncrementCommand {
                shard: *shard,
                key: key.clone(),
                delta: *delta,
                expected_version: *expected_version,
                ttl_ms: *ttl_ms,
                lock_guard: lock_guard.as_ref().map(CacheLockGuardRequest::to_tablet),
            }),
            Self::Transform {
                shard,
                key,
                transform,
                expected_version,
                ttl_ms,
                lock_guard,
            } => CacheTabletOperation::Transform(CacheTransformCommand {
                shard: *shard,
                key: key.clone(),
                transform: transform.clone(),
                expected_version: *expected_version,
                ttl_ms: *ttl_ms,
                lock_guard: lock_guard.as_ref().map(CacheLockGuardRequest::to_tablet),
            }),
            Self::Get { shard, key } => CacheTabletOperation::Get(CacheGetCommand {
                shard: *shard,
                key: key.clone(),
            }),
            Self::Transaction {
                shard,
                expected_revision,
                mutations,
                lock_guards,
            } => transaction_operation(*shard, *expected_revision, mutations, lock_guards)?,
            _ => unreachable!("Cache lifecycle variants returned before core conversion"),
        })
    }

    fn to_lifecycle_operation(&self) -> Option<CacheTabletOperation> {
        match self {
            Self::AcquireLock {
                shard,
                lock_key,
                owner,
                owner_epoch,
                lease_ms,
            } => Some(acquire_lock_operation(
                *shard,
                lock_key,
                owner,
                *owner_epoch,
                *lease_ms,
            )),
            Self::RenewLock {
                shard,
                lock_key,
                owner,
                owner_epoch,
                lease_token,
                extension_ms,
            } => Some(renew_lock_operation(
                *shard,
                lock_key,
                owner,
                *owner_epoch,
                lease_token,
                *extension_ms,
            )),
            Self::ReleaseLock {
                shard,
                lock_key,
                owner,
                owner_epoch,
                lease_token,
            } => Some(release_lock_operation(
                *shard,
                lock_key,
                owner,
                *owner_epoch,
                lease_token,
            )),
            Self::Maintain {
                shard,
                max_expirations,
            } => Some(CacheTabletOperation::Maintain(CacheMaintainCommand {
                shard: *shard,
                max_expirations: *max_expirations,
            })),
            Self::Restore {
                shard,
                backup_base64,
                target_revision,
            } => Some(CacheTabletOperation::Restore(CacheRestoreCommand {
                shard: *shard,
                backup_base64: backup_base64.clone(),
                target_revision: *target_revision,
            })),
            _ => None,
        }
    }
}

fn set_operation(
    shard: u32,
    key: &str,
    value: &CacheValueRequest,
    ttl_ms: Option<u64>,
    storage_class: CacheStorageClass,
    lock_guard: Option<&CacheLockGuardRequest>,
) -> TabletApiResult<CacheTabletOperation> {
    let value = value.to_cache_value()?;
    Ok(match storage_class {
        CacheStorageClass::Memory => CacheTabletOperation::Set(CacheSetCommand {
            shard,
            key: key.to_owned(),
            value,
            ttl_ms,
            lock_guard: lock_guard.map(CacheLockGuardRequest::to_tablet),
        }),
        CacheStorageClass::Cold => CacheTabletOperation::Transform(CacheTransformCommand {
            shard,
            key: key.to_owned(),
            transform: CacheTransform::Replace {
                value,
                storage_class,
            },
            expected_version: None,
            ttl_ms,
            lock_guard: lock_guard.map(CacheLockGuardRequest::to_tablet),
        }),
    })
}

fn compare_and_set_operation(
    shard: u32,
    key: &str,
    expected: &CacheCasExpectationRequest,
    value: &CacheValueRequest,
    ttl_ms: Option<u64>,
    lock_guard: Option<&CacheLockGuardRequest>,
) -> TabletApiResult<CacheTabletOperation> {
    Ok(CacheTabletOperation::CompareAndSet(
        CacheCompareAndSetCommand {
            shard,
            key: key.to_owned(),
            expected: expected.to_tablet(),
            value: value.to_cache_value()?,
            ttl_ms,
            lock_guard: lock_guard.map(CacheLockGuardRequest::to_tablet),
        },
    ))
}

fn transaction_operation(
    shard: u32,
    expected_revision: u64,
    mutations: &[CacheTransactionMutationRequest],
    lock_guards: &[CacheLockGuardRequest],
) -> TabletApiResult<CacheTabletOperation> {
    Ok(CacheTabletOperation::Transaction(CacheTransactionCommand {
        shard,
        expected_revision,
        mutations: mutations
            .iter()
            .map(CacheTransactionMutationRequest::to_tablet)
            .collect::<TabletApiResult<_>>()?,
        lock_guards: lock_guards
            .iter()
            .map(CacheLockGuardRequest::to_tablet)
            .collect(),
    }))
}

fn acquire_lock_operation(
    shard: u32,
    lock_key: &str,
    owner: &str,
    owner_epoch: u64,
    lease_ms: u64,
) -> CacheTabletOperation {
    CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
        shard,
        lock_key: lock_key.to_owned(),
        owner: owner.to_owned(),
        owner_epoch,
        lease_ms,
    })
}

fn renew_lock_operation(
    shard: u32,
    lock_key: &str,
    owner: &str,
    owner_epoch: u64,
    lease_token: &str,
    extension_ms: u64,
) -> CacheTabletOperation {
    CacheTabletOperation::RenewLock(CacheRenewLockCommand {
        shard,
        lock_key: lock_key.to_owned(),
        owner: owner.to_owned(),
        owner_epoch,
        lease_token: lease_token.to_owned(),
        extension_ms,
    })
}

fn release_lock_operation(
    shard: u32,
    lock_key: &str,
    owner: &str,
    owner_epoch: u64,
    lease_token: &str,
) -> CacheTabletOperation {
    CacheTabletOperation::ReleaseLock(CacheReleaseLockCommand {
        shard,
        lock_key: lock_key.to_owned(),
        owner: owner.to_owned(),
        owner_epoch,
        lease_token: lease_token.to_owned(),
    })
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum CacheTransactionMutationRequest {
    Set {
        key: String,
        value: CacheValueRequest,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        ttl_ms: Option<u64>,
        #[serde(default)]
        storage_class: CacheStorageClass,
    },
    Delete {
        key: String,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        expected_version: Option<u64>,
    },
    CompareAndSet {
        key: String,
        expected: CacheCasExpectationRequest,
        value: CacheValueRequest,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        ttl_ms: Option<u64>,
    },
    Increment {
        key: String,
        #[serde(deserialize_with = "deserialize_i64_from_number_or_decimal")]
        delta: i64,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        expected_version: Option<u64>,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        ttl_ms: Option<u64>,
    },
    Transform {
        key: String,
        transform: CacheTransform,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        expected_version: Option<u64>,
        #[serde(
            default,
            deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
        )]
        ttl_ms: Option<u64>,
    },
}

impl CacheTransactionMutationRequest {
    fn to_tablet(&self) -> TabletApiResult<CacheTransactionMutation> {
        Ok(match self {
            Self::Set {
                key,
                value,
                ttl_ms,
                storage_class,
            } => match storage_class {
                CacheStorageClass::Memory => CacheTransactionMutation::Set {
                    key: key.clone(),
                    value: value.to_cache_value()?,
                    ttl_ms: *ttl_ms,
                },
                CacheStorageClass::Cold => CacheTransactionMutation::Transform {
                    key: key.clone(),
                    transform: CacheTransform::Replace {
                        value: value.to_cache_value()?,
                        storage_class: *storage_class,
                    },
                    expected_version: None,
                    ttl_ms: *ttl_ms,
                },
            },
            Self::Delete {
                key,
                expected_version,
            } => CacheTransactionMutation::Delete {
                key: key.clone(),
                expected_version: *expected_version,
            },
            Self::CompareAndSet {
                key,
                expected,
                value,
                ttl_ms,
            } => CacheTransactionMutation::CompareAndSet {
                key: key.clone(),
                expected: expected.to_tablet(),
                value: value.to_cache_value()?,
                ttl_ms: *ttl_ms,
            },
            Self::Increment {
                key,
                delta,
                expected_version,
                ttl_ms,
            } => CacheTransactionMutation::Increment {
                key: key.clone(),
                delta: *delta,
                expected_version: *expected_version,
                ttl_ms: *ttl_ms,
            },
            Self::Transform {
                key,
                transform,
                expected_version,
                ttl_ms,
            } => CacheTransactionMutation::Transform {
                key: key.clone(),
                transform: transform.clone(),
                expected_version: *expected_version,
                ttl_ms: *ttl_ms,
            },
        })
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum CacheCasExpectationRequest {
    Missing {
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        shard_revision: u64,
    },
    Version {
        #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
        version: u64,
    },
}

impl CacheCasExpectationRequest {
    const fn to_tablet(self) -> CacheCasExpectation {
        match self {
            Self::Missing { shard_revision } => CacheCasExpectation::Missing { shard_revision },
            Self::Version { version } => CacheCasExpectation::Version { version },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheLockGuardRequest {
    lock_key: String,
    owner: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    owner_epoch: u64,
    lease_token: String,
}

impl CacheLockGuardRequest {
    fn to_tablet(&self) -> CacheLockGuard {
        CacheLockGuard {
            lock_key: self.lock_key.clone(),
            owner: self.owner.clone(),
            owner_epoch: self.owner_epoch,
            lease_token: self.lease_token.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum CacheValueRequest {
    String(String),
    Blob(Vec<u8>),
    Counter(#[serde(deserialize_with = "deserialize_i64_from_number_or_decimal")] i64),
    Hash(UniqueMap<String>),
    List(Vec<String>),
    Set(Vec<String>),
    SortedSet(UniqueMap<f64>),
    Bitmap(CacheBitmap),
    Cardinality(CacheCardinality),
    Bloom(CacheBloomFilter),
    Cuckoo(CacheCuckooFilter),
    Geo(CacheGeoIndex),
    Json(CacheJsonDocument),
    JsonIndex(CacheJsonIndex),
    Vector(CacheVectorIndex),
}

impl CacheValueRequest {
    fn to_cache_value(&self) -> TabletApiResult<CacheValue> {
        Ok(match self {
            Self::String(value) => CacheValue::String(value.clone()),
            Self::Blob(value) => CacheValue::Blob(value.clone()),
            Self::Counter(value) => CacheValue::Counter(*value),
            Self::Hash(values) => CacheValue::Hash(values.0.clone()),
            Self::List(values) => CacheValue::List(values.clone()),
            Self::Set(values) => {
                let unique = values.iter().cloned().collect::<BTreeSet<_>>();
                if unique.len() != values.len() {
                    return Err(TabletApiError::InvalidRequest(
                        "cache set value contains duplicate members".into(),
                    ));
                }
                CacheValue::Set(unique)
            }
            Self::SortedSet(values) => CacheValue::SortedSet(values.0.clone()),
            Self::Bitmap(value) => CacheValue::Bitmap(value.clone()),
            Self::Cardinality(value) => CacheValue::Cardinality(value.clone()),
            Self::Bloom(value) => CacheValue::Bloom(value.clone()),
            Self::Cuckoo(value) => CacheValue::Cuckoo(value.clone()),
            Self::Geo(value) => CacheValue::Geo(value.clone()),
            Self::Json(value) => CacheValue::Json(value.clone()),
            Self::JsonIndex(value) => CacheValue::JsonIndex(value.clone()),
            Self::Vector(value) => CacheValue::Vector(value.clone()),
        })
    }
}

#[derive(Debug, Clone)]
struct UniqueMap<V>(BTreeMap<String, V>);

impl<'de, V> Deserialize<'de> for UniqueMap<V>
where
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UniqueMapVisitor<V>(PhantomData<V>);

        impl<'de, V> Visitor<'de> for UniqueMapVisitor<V>
        where
            V: Deserialize<'de>,
        {
            type Value = UniqueMap<V>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object without duplicate keys")
            }

            fn visit_map<A>(self, mut entries: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some((key, value)) = entries.next_entry::<String, V>()? {
                    if values.insert(key.clone(), value).is_some() {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate cache collection key: {key}"
                        )));
                    }
                }
                Ok(UniqueMap(values))
            }
        }

        deserializer.deserialize_map(UniqueMapVisitor(PhantomData))
    }
}

async fn submit_mutation(
    State(state): State<CacheTabletApiState>,
    request: Result<Json<CacheMutationRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<CacheTabletMutationResponse>)> {
    let Json(request) = request.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    submit_mutation_request(&state, request).await
}

async fn submit_mutation_request(
    state: &CacheTabletApiState,
    request: CacheMutationRequest,
) -> TabletApiResult<(StatusCode, Json<CacheTabletMutationResponse>)> {
    state
        .service
        .ensure_healthy()
        .map_err(TabletApiError::Profile)?;
    let operation = request.operation.to_tablet_operation()?;
    // Validate semantic input before consulting consensus. Server time and the
    // caller's expected term are intentionally outside request identity.
    CacheTabletCommand::new(
        state.service.scope(),
        request.idempotency_key.clone(),
        0,
        operation.clone(),
    )?;
    let proposal_id = cache_proposal_id_for(state.service.scope(), &request.idempotency_key)?;
    let _write_guard = state.write_serial.lock().await;
    let commits = state.consensus.subscribe_commits();

    let initial = state.consensus.lookup(proposal_id).await?;
    let (lookup, replayed) = match initial {
        ProposalLookup::Unknown => {
            let applied_at_ms = state
                .clock
                .wall_time_ms()
                .max(state.service.last_applied_time_ms()?);
            let command = CacheTabletCommand::new(
                state.service.scope(),
                request.idempotency_key.clone(),
                applied_at_ms,
                operation,
            )?;
            let payload = command.encode(state.service.scope())?;
            let (lookup, replayed) = match state
                .consensus
                .propose(proposal_id, request.expected_term, payload)
                .await
            {
                Ok(lookup) => (lookup, false),
                Err(ConsensusProbeError::Consensus(ConsensusError::DuplicateProposal(_))) => {
                    (state.consensus.lookup(proposal_id).await?, true)
                }
                Err(error) => return Err(error.into()),
            };
            (lookup, replayed)
        }
        existing => {
            validate_existing_request(&existing, state.service.scope(), &request)?;
            (existing, true)
        }
    };

    if let Some(response) = committed_response(&state.service, &lookup, &request, replayed)? {
        return Ok((committed_http_status(replayed), Json(response)));
    }

    wait_for_committed_response(state, commits, proposal_id, &request, replayed).await
}

async fn submit_multiplex(
    State(state): State<CacheTabletApiState>,
    request: Result<Json<CacheMultiplexRequest>, JsonRejection>,
) -> TabletApiResult<Json<CacheMultiplexResponse>> {
    let Json(request) = request.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    validate_multiplex_request(&state, &request)?;
    let mut results = Vec::with_capacity(request.mutations.len());
    for mutation in request.mutations {
        let correlation_id = mutation.correlation_id;
        let (status, Json(response)) = submit_mutation_request(
            &state,
            CacheMutationRequest {
                idempotency_key: mutation.idempotency_key,
                expected_term: request.expected_term,
                operation: mutation.operation,
            },
        )
        .await?;
        results.push(CacheMultiplexResult {
            correlation_id,
            http_status: status.as_u16(),
            response,
        });
    }
    Ok(Json(CacheMultiplexResponse {
        atomic: false,
        ordering: "request_order_independent_outcomes",
        results,
    }))
}

fn validate_multiplex_request(
    state: &CacheTabletApiState,
    request: &CacheMultiplexRequest,
) -> TabletApiResult<()> {
    if !(1..=MAX_CACHE_MULTIPLEX_MUTATIONS).contains(&request.mutations.len()) {
        return Err(TabletApiError::InvalidRequest(format!(
            "Cache multiplex mutations must be between 1 and {MAX_CACHE_MULTIPLEX_MUTATIONS}"
        )));
    }
    let mut correlations = BTreeSet::new();
    let mut idempotency_keys = BTreeSet::new();
    for mutation in &request.mutations {
        if mutation.correlation_id.trim().is_empty()
            || mutation.correlation_id.len() > MAX_CACHE_CORRELATION_ID_BYTES
            || !correlations.insert(mutation.correlation_id.as_str())
        {
            return Err(TabletApiError::InvalidRequest(
                "Cache multiplex correlation IDs must be unique and 1..=256 bytes".into(),
            ));
        }
        if !idempotency_keys.insert(mutation.idempotency_key.as_str()) {
            return Err(TabletApiError::InvalidRequest(
                "Cache multiplex idempotency keys must be unique".into(),
            ));
        }
        let operation = mutation.operation.to_tablet_operation()?;
        CacheTabletCommand::new(
            state.service.scope(),
            mutation.idempotency_key.clone(),
            0,
            operation,
        )?;
        cache_proposal_id_for(state.service.scope(), &mutation.idempotency_key)?;
    }
    Ok(())
}

async fn wait_for_committed_response(
    state: &CacheTabletApiState,
    mut commits: broadcast::Receiver<CommittedProposal>,
    proposal_id: u64,
    request: &CacheMutationRequest,
    replayed: bool,
) -> TabletApiResult<(StatusCode, Json<CacheTabletMutationResponse>)> {
    let deadline = tokio::time::Instant::now() + state.commit_wait;
    loop {
        let notification = tokio::time::timeout_at(deadline, commits.recv()).await;
        match notification {
            Ok(Ok(committed)) => {
                if committed.receipt.proposal_id.get() == proposal_id {
                    let lookup = ProposalLookup::Committed(committed);
                    if let Some(response) =
                        committed_response(&state.service, &lookup, request, replayed)?
                    {
                        return Ok((committed_http_status(replayed), Json(response)));
                    }
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                let lookup = state.consensus.lookup(proposal_id).await?;
                if let Some(response) =
                    committed_response(&state.service, &lookup, request, replayed)?
                {
                    return Ok((committed_http_status(replayed), Json(response)));
                }
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                return Err(TabletApiError::Consensus(
                    ConsensusProbeError::ActorUnavailable,
                ));
            }
            Err(_) => {
                let lookup = state.consensus.lookup(proposal_id).await?;
                if let Some(response) =
                    committed_response(&state.service, &lookup, request, replayed)?
                {
                    return Ok((committed_http_status(replayed), Json(response)));
                }
                return Ok((
                    StatusCode::ACCEPTED,
                    Json(unresolved_response(proposal_id, &lookup)),
                ));
            }
        }
    }
}

fn unresolved_response(proposal_id: u64, lookup: &ProposalLookup) -> CacheTabletMutationResponse {
    match lookup {
        ProposalLookup::Unknown => CacheTabletMutationResponse::unknown(proposal_id),
        ProposalLookup::Pending { .. } => CacheTabletMutationResponse::pending(proposal_id),
        ProposalLookup::Committed(_) => unreachable!("committed lookups return a response"),
    }
}

const fn committed_http_status(replayed: bool) -> StatusCode {
    if replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    }
}

fn validate_existing_request(
    lookup: &ProposalLookup,
    scope: &CacheTabletScope,
    request: &CacheMutationRequest,
) -> TabletApiResult<()> {
    let payload = match lookup {
        ProposalLookup::Unknown => return Ok(()),
        ProposalLookup::Pending { payload } => payload,
        ProposalLookup::Committed(committed) => &committed.payload,
    };
    let command = CacheTabletCommand::decode(payload, scope).map_err(|error| {
        TabletApiError::Profile(format!(
            "tracked consensus command is not a valid Cache tablet command: {error}"
        ))
    })?;
    if command.idempotency_key != request.idempotency_key
        || command.operation != request.operation.to_tablet_operation()?
    {
        return Err(TabletApiError::IdempotencyConflict);
    }
    Ok(())
}

fn committed_response(
    service: &CacheTabletService,
    lookup: &ProposalLookup,
    request: &CacheMutationRequest,
    replayed: bool,
) -> TabletApiResult<Option<CacheTabletMutationResponse>> {
    validate_existing_request(lookup, service.scope(), request)?;
    match lookup {
        ProposalLookup::Committed(committed) => {
            let receipt = service.committed_receipt(committed)?;
            Ok(Some(CacheTabletMutationResponse::committed(
                receipt_for_response(receipt, replayed),
            )))
        }
        ProposalLookup::Unknown | ProposalLookup::Pending { .. } => Ok(None),
    }
}

fn receipt_for_response(mut receipt: CacheTabletReceipt, replayed: bool) -> CacheTabletReceipt {
    if replayed {
        receipt.disposition = CacheTabletDisposition::Replayed;
    }
    receipt
}

async fn lookup_mutation(
    State(state): State<CacheTabletApiState>,
    Path(proposal_id): Path<u64>,
) -> TabletApiResult<Json<CacheTabletMutationResponse>> {
    let lookup = state.consensus.lookup(proposal_id).await?;
    let response = match lookup {
        ProposalLookup::Unknown => CacheTabletMutationResponse::unknown(proposal_id),
        ProposalLookup::Pending { .. } => CacheTabletMutationResponse::pending(proposal_id),
        ProposalLookup::Committed(committed) => {
            CacheTabletMutationResponse::committed(state.service.committed_receipt(&committed)?)
        }
    };
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheObservationQuery {
    key: String,
}

async fn observe_key(
    State(state): State<CacheTabletApiState>,
    query: Result<Query<CacheObservationQuery>, QueryRejection>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<CacheTabletObservationResponse>> {
    let Query(query) = query.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    validate_observation_key(&query.key)?;
    Ok(Json(CacheTabletObservationResponse {
        read: tablet_read_metadata(read),
        observation: state.service.observe(&query.key)?,
    }))
}

const fn default_change_sequence() -> u64 {
    1
}

const fn default_change_limit() -> usize {
    100
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheChangesQuery {
    #[serde(
        default = "default_change_sequence",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    from_sequence: u64,
    #[serde(default = "default_change_limit")]
    limit: usize,
}

#[derive(Debug, Serialize)]
struct CacheChangeView {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    sequence: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    revision: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    at_ms: u64,
    key: String,
    kind: CacheChangeKind,
    before: Option<CacheTabletItem>,
    after: Option<CacheTabletItem>,
}

impl From<CacheChange> for CacheChangeView {
    fn from(change: CacheChange) -> Self {
        Self {
            sequence: change.sequence,
            revision: change.revision,
            at_ms: change.at_ms,
            key: change.key,
            kind: change.kind,
            before: change.before.map(CacheTabletItem::from),
            after: change.after.map(CacheTabletItem::from),
        }
    }
}

#[derive(Debug, Serialize)]
struct CacheChangesResponse {
    read: TabletReadMetadata,
    changes: Vec<CacheChangeView>,
}

async fn read_changes(
    State(state): State<CacheTabletApiState>,
    query: Result<Query<CacheChangesQuery>, QueryRejection>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<CacheChangesResponse>> {
    let Query(query) = query.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    let changes = state
        .service
        .changes_from(query.from_sequence, query.limit)?
        .into_iter()
        .map(CacheChangeView::from)
        .collect();
    Ok(Json(CacheChangesResponse {
        read: tablet_read_metadata(read),
        changes,
    }))
}

#[derive(Debug, Serialize)]
struct CacheBackupResponse {
    read: TabletReadMetadata,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    captured_revision: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    captured_at_ms: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    oldest_restorable_revision: u64,
    state_digest: String,
    artifact_base64: String,
}

async fn read_backup(
    State(state): State<CacheTabletApiState>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<CacheBackupResponse>> {
    let (backup, metadata) = state.service.backup()?;
    Ok(Json(CacheBackupResponse {
        read: tablet_read_metadata(read),
        captured_revision: metadata.captured_revision,
        captured_at_ms: metadata.captured_at_ms,
        oldest_restorable_revision: metadata.oldest_restorable_revision,
        state_digest: hex_digest(metadata.state_digest),
        artifact_base64: STANDARD_NO_PAD.encode(backup),
    }))
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum CacheQueryRequest {
    BitmapGet {
        key: String,
        bit: u32,
    },
    CardinalityEstimate {
        key: String,
    },
    BloomContains {
        key: String,
        value: Vec<u8>,
    },
    CuckooContains {
        key: String,
        value: Vec<u8>,
    },
    GeoRadius {
        key: String,
        center: CacheGeoPoint,
        radius_meters: f64,
        limit: usize,
    },
    JsonPointer {
        key: String,
        pointer: String,
    },
    JsonSearch {
        key: String,
        pointer: String,
        value: serde_json::Value,
        limit: usize,
    },
    VectorSearch {
        key: String,
        query_vector: Vec<f32>,
        #[serde(default)]
        query_text: String,
        vector_weight: f64,
        #[serde(default)]
        filters: BTreeMap<String, String>,
        limit: usize,
    },
}

impl CacheQueryRequest {
    fn key(&self) -> &str {
        match self {
            Self::BitmapGet { key, .. }
            | Self::CardinalityEstimate { key }
            | Self::BloomContains { key, .. }
            | Self::CuckooContains { key, .. }
            | Self::GeoRadius { key, .. }
            | Self::JsonPointer { key, .. }
            | Self::JsonSearch { key, .. }
            | Self::VectorSearch { key, .. } => key,
        }
    }

    fn execute(&self, value: &CacheValue) -> TabletApiResult<CacheQueryResult> {
        match (self, value) {
            (Self::BitmapGet { bit, .. }, CacheValue::Bitmap(bitmap)) => {
                Ok(CacheQueryResult::Bitmap {
                    value: bitmap
                        .get(*bit)
                        .map_err(|error| TabletApiError::InvalidRequest(error.to_string()))?,
                    count: bitmap.count().to_string(),
                })
            }
            (Self::CardinalityEstimate { .. }, CacheValue::Cardinality(cardinality)) => {
                Ok(CacheQueryResult::Cardinality {
                    estimate: cardinality.estimate().to_string(),
                })
            }
            (Self::BloomContains { value, .. }, CacheValue::Bloom(filter)) => {
                Ok(CacheQueryResult::Membership {
                    contains: filter.contains(value),
                })
            }
            (Self::CuckooContains { value, .. }, CacheValue::Cuckoo(filter)) => {
                Ok(CacheQueryResult::Membership {
                    contains: filter.contains(value),
                })
            }
            (
                Self::GeoRadius {
                    center,
                    radius_meters,
                    limit,
                    ..
                },
                CacheValue::Geo(index),
            ) => Ok(CacheQueryResult::Geo {
                hits: index
                    .radius(*center, *radius_meters, *limit)
                    .map_err(|error| TabletApiError::InvalidRequest(error.to_string()))?,
            }),
            (Self::JsonPointer { pointer, .. }, CacheValue::Json(document)) => {
                Ok(CacheQueryResult::Json {
                    value: document.pointer(pointer).cloned(),
                })
            }
            (
                Self::JsonSearch {
                    pointer,
                    value,
                    limit,
                    ..
                },
                CacheValue::JsonIndex(index),
            ) => Ok(CacheQueryResult::JsonSearch {
                hits: index
                    .search_exact(pointer, value, *limit)
                    .map_err(|error| TabletApiError::InvalidRequest(error.to_string()))?,
            }),
            (
                Self::VectorSearch {
                    query_vector,
                    query_text,
                    vector_weight,
                    filters,
                    limit,
                    ..
                },
                CacheValue::Vector(index),
            ) => Ok(CacheQueryResult::Vector {
                hits: index
                    .search(query_vector, query_text, *vector_weight, filters, *limit)
                    .map_err(|error| TabletApiError::InvalidRequest(error.to_string()))?,
            }),
            _ => Err(TabletApiError::InvalidRequest(format!(
                "cache value at {} does not match query kind",
                self.key()
            ))),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CacheQueryResult {
    Bitmap { value: bool, count: String },
    Cardinality { estimate: String },
    Membership { contains: bool },
    Geo { hits: Vec<CacheGeoHit> },
    Json { value: Option<serde_json::Value> },
    JsonSearch { hits: Vec<CacheJsonHit> },
    Vector { hits: Vec<CacheVectorHit> },
}

#[derive(Debug, Serialize)]
struct CacheQueryResponse {
    #[serde(flatten)]
    read: TabletReadMetadata,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    shard_revision: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    observed_at_ms: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    version: u64,
    result: CacheQueryResult,
}

async fn query_cache(
    State(state): State<CacheTabletApiState>,
    read: Option<Extension<TabletReadMetadata>>,
    request: Result<Json<CacheQueryRequest>, JsonRejection>,
) -> TabletApiResult<Json<CacheQueryResponse>> {
    let Json(request) = request.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    validate_observation_key(request.key())?;
    let observation = state.service.observe(request.key())?;
    let item = observation.item.ok_or_else(|| {
        TabletApiError::InvalidRequest(format!("cache key is missing: {}", request.key()))
    })?;
    let result = request.execute(&item.value)?;
    Ok(Json(CacheQueryResponse {
        read: tablet_read_metadata(read),
        shard_revision: observation.shard_revision,
        observed_at_ms: observation.observed_at_ms,
        version: item.version,
        result,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateCachePubSubSubscriptionRequest {
    #[serde(default)]
    channels: Vec<String>,
    #[serde(default)]
    patterns: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CreateCachePubSubSubscriptionResponse {
    subscription_id: String,
    delivery_semantics: &'static str,
    persistence: &'static str,
    node_affinity_required: bool,
    pending_message_limit: usize,
}

async fn create_pubsub_subscription(
    State(state): State<CacheTabletApiState>,
    request: Result<Json<CreateCachePubSubSubscriptionRequest>, JsonRejection>,
) -> TabletApiResult<(StatusCode, Json<CreateCachePubSubSubscriptionResponse>)> {
    let Json(request) = request.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    let channels = request.channels.into_iter().collect::<BTreeSet<_>>();
    let patterns = request.patterns.into_iter().collect::<BTreeSet<_>>();
    let subscription_id = state.service.pubsub_subscribe(channels, patterns)?;
    Ok((
        StatusCode::CREATED,
        Json(CreateCachePubSubSubscriptionResponse {
            subscription_id,
            delivery_semantics: "at_most_once",
            persistence: "none_node_local_memory",
            node_affinity_required: true,
            pending_message_limit: MAX_CACHE_PUBSUB_PENDING_MESSAGES,
        }),
    ))
}

#[derive(Debug, Serialize)]
struct DeleteCachePubSubSubscriptionResponse {
    subscription_id: String,
    deleted: bool,
}

async fn delete_pubsub_subscription(
    State(state): State<CacheTabletApiState>,
    Path(subscription_id): Path<String>,
) -> TabletApiResult<Json<DeleteCachePubSubSubscriptionResponse>> {
    let deleted = state.service.pubsub_unsubscribe(&subscription_id)?;
    Ok(Json(DeleteCachePubSubSubscriptionResponse {
        subscription_id,
        deleted,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishCachePubSubMessageRequest {
    channel: String,
    payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct PublishCachePubSubMessageResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    sequence: u64,
    delivered_subscriptions: usize,
    dropped_subscriptions: usize,
    delivery_semantics: &'static str,
}

async fn publish_pubsub_message(
    State(state): State<CacheTabletApiState>,
    request: Result<Json<PublishCachePubSubMessageRequest>, JsonRejection>,
) -> TabletApiResult<Json<PublishCachePubSubMessageResponse>> {
    let Json(request) = request.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    let (sequence, delivered_subscriptions, dropped_subscriptions) = state.service.pubsub_publish(
        &request.channel,
        &request.payload,
        state.clock.wall_time_ms(),
    )?;
    Ok(Json(PublishCachePubSubMessageResponse {
        sequence,
        delivered_subscriptions,
        dropped_subscriptions,
        delivery_semantics: "at_most_once",
    }))
}

const fn default_pubsub_poll_limit() -> usize {
    100
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CachePubSubPollQuery {
    #[serde(default = "default_pubsub_poll_limit")]
    limit: usize,
}

#[derive(Debug, Serialize)]
struct CachePubSubPollResponse {
    #[serde(flatten)]
    read: TabletReadMetadata,
    subscription_id: String,
    messages: Vec<CachePubSubMessage>,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    dropped_messages_since_last_poll: u64,
    remaining_messages: usize,
    delivery_semantics: &'static str,
}

async fn poll_pubsub_messages(
    State(state): State<CacheTabletApiState>,
    Path(subscription_id): Path<String>,
    query: Result<Query<CachePubSubPollQuery>, QueryRejection>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<CachePubSubPollResponse>> {
    let Query(query) = query.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    let polled = state.service.pubsub_poll(&subscription_id, query.limit)?;
    Ok(Json(CachePubSubPollResponse {
        read: tablet_read_metadata(read),
        subscription_id,
        messages: polled.messages,
        dropped_messages_since_last_poll: polled.dropped_messages,
        remaining_messages: polled.remaining_messages,
        delivery_semantics: "at_most_once",
    }))
}

fn validate_observation_key(key: &str) -> TabletApiResult<()> {
    if key.trim().is_empty() {
        return Err(TabletApiError::InvalidRequest("key is required".into()));
    }
    if key.len() > MAX_CACHE_KEY_BYTES {
        return Err(TabletApiError::InvalidRequest(format!(
            "key is {} bytes; maximum is {MAX_CACHE_KEY_BYTES}",
            key.len()
        )));
    }
    if key.chars().any(char::is_control) {
        return Err(TabletApiError::InvalidRequest(
            "key cannot contain control characters".into(),
        ));
    }
    Ok(())
}

async fn tablet_status(
    State(state): State<CacheTabletApiState>,
    read: Option<Extension<TabletReadMetadata>>,
) -> TabletApiResult<Json<CacheTabletStatus>> {
    // Profile-first sampling guarantees this document cannot report a profile
    // index ahead of its later actor-owned consensus snapshot.
    let profile = state.service.snapshot()?;
    let consensus = state.consensus.status().await?;
    Ok(Json(CacheTabletStatus::new_with_read(
        state.service.scope(),
        &consensus,
        profile,
        tablet_read_metadata(read),
    )?))
}

#[derive(Debug)]
struct CacheTabletSnapshot {
    last_profile_mutation_index: u64,
    last_applied_time_ms: u64,
    applied_command_count: u64,
    cache_revision: u64,
    retained_entry_count: u64,
    retained_memory_bytes: u64,
    retained_cold_bytes: u64,
    max_memory_bytes: Option<u64>,
    max_cold_bytes: Option<u64>,
    active_lock_count: u64,
    eviction: EvictionPolicy,
    requested_durability: DurabilityProfile,
    cold_storage_backend: &'static str,
    cold_read_count: u64,
    cold_read_average_micros: u64,
    cold_read_max_micros: u64,
    cache_recovery_state_digest: String,
    state_digest: String,
}

#[derive(Debug, Serialize)]
struct CacheTabletStatus {
    capability: &'static str,
    stability: &'static str,
    production_readiness: &'static str,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    tablet_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    tablet_epoch: u64,
    resource: String,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    node_id: u64,
    role: &'static str,
    #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
    leader_id: Option<u64>,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    term: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    consensus_commit_index: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    consensus_applied_index: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    last_profile_mutation_index: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    last_applied_time_ms: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    applied_command_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    cache_revision: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    retained_entry_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    retained_memory_bytes: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    retained_cold_bytes: u64,
    #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
    max_memory_bytes: Option<u64>,
    #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
    max_cold_bytes: Option<u64>,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    active_lock_count: u64,
    eviction: EvictionPolicy,
    requested_durability: DurabilityProfile,
    achieved_durability: DurabilityProfile,
    durability_overachieved: bool,
    cold_storage_backend: &'static str,
    cold_read_latency_disclosure: &'static str,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    cold_read_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    cold_read_average_micros: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    cold_read_max_micros: u64,
    cache_recovery_state_digest: String,
    state_digest: String,
    write_guarantee: &'static str,
    #[serde(flatten)]
    read: TabletReadMetadata,
}

impl CacheTabletStatus {
    #[cfg(test)]
    fn new(
        scope: &CacheTabletScope,
        consensus: &ConsensusStatus,
        profile: CacheTabletSnapshot,
    ) -> Result<Self, String> {
        Self::new_with_read(scope, consensus, profile, TabletReadMetadata::local_stale())
    }

    fn new_with_read(
        scope: &CacheTabletScope,
        consensus: &ConsensusStatus,
        profile: CacheTabletSnapshot,
        read: TabletReadMetadata,
    ) -> Result<Self, String> {
        if profile.last_profile_mutation_index > consensus.applied_index.get() {
            return Err(format!(
                "Cache profile mutation index {} is ahead of consensus applied index {}",
                profile.last_profile_mutation_index,
                consensus.applied_index.get()
            ));
        }
        Ok(Self {
            capability: "single_shard_cache_tablet",
            stability: "experimental",
            production_readiness: "not_production_ready",
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            node_id: consensus.node_id.get(),
            role: match consensus.role {
                ConsensusRole::Follower => "follower",
                ConsensusRole::PreCandidate => "pre_candidate",
                ConsensusRole::Candidate => "candidate",
                ConsensusRole::Leader => "leader",
            },
            leader_id: consensus.leader_id.map(epoch_consensus::NodeId::get),
            term: consensus.term.get(),
            consensus_commit_index: consensus.commit_index.get(),
            consensus_applied_index: consensus.applied_index.get(),
            last_profile_mutation_index: profile.last_profile_mutation_index,
            last_applied_time_ms: profile.last_applied_time_ms,
            applied_command_count: profile.applied_command_count,
            cache_revision: profile.cache_revision,
            retained_entry_count: profile.retained_entry_count,
            retained_memory_bytes: profile.retained_memory_bytes,
            retained_cold_bytes: profile.retained_cold_bytes,
            max_memory_bytes: profile.max_memory_bytes,
            max_cold_bytes: profile.max_cold_bytes,
            active_lock_count: profile.active_lock_count,
            eviction: profile.eviction,
            requested_durability: profile.requested_durability,
            achieved_durability: DurabilityProfile::QuorumDurable,
            durability_overachieved: profile.requested_durability
                == DurabilityProfile::ReplicatedMemory,
            cold_storage_backend: profile.cold_storage_backend,
            cold_read_latency_disclosure: "observed_local_file_read_micros_not_an_slo",
            cold_read_count: profile.cold_read_count,
            cold_read_average_micros: profile.cold_read_average_micros,
            cold_read_max_micros: profile.cold_read_max_micros,
            cache_recovery_state_digest: profile.cache_recovery_state_digest,
            state_digest: profile.state_digest,
            write_guarantee: "fixed_three_voter_majority_persisted_then_local_profile_applied",
            read,
        })
    }
}

#[derive(Debug, Serialize)]
struct CacheTabletObservationResponse {
    #[serde(flatten)]
    read: TabletReadMetadata,
    observation: CacheTabletObservation,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum MutationState {
    Unknown,
    Pending,
    Committed,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum OutcomeCertainty {
    Unknown,
    Committed,
}

#[derive(Debug, Serialize)]
struct CacheTabletMutationResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    proposal_id: u64,
    state: MutationState,
    outcome_certainty: OutcomeCertainty,
    observation_scope: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<CacheTabletReceipt>,
}

impl CacheTabletMutationResponse {
    const fn unknown(proposal_id: u64) -> Self {
        Self {
            proposal_id,
            state: MutationState::Unknown,
            outcome_certainty: OutcomeCertainty::Unknown,
            observation_scope: "local",
            receipt: None,
        }
    }

    const fn pending(proposal_id: u64) -> Self {
        Self {
            proposal_id,
            state: MutationState::Pending,
            outcome_certainty: OutcomeCertainty::Unknown,
            observation_scope: "local",
            receipt: None,
        }
    }

    fn committed(receipt: CacheTabletReceipt) -> Self {
        Self {
            proposal_id: receipt.proposal_id,
            state: MutationState::Committed,
            outcome_certainty: OutcomeCertainty::Committed,
            observation_scope: "local",
            receipt: Some(receipt),
        }
    }
}

#[cfg(test)]
mod tests;
