//! Experimental typed Cache tablet over the fixed-voter consensus runtime.
//!
//! This module is deliberately mounted only on the internal experimental
//! listener. It does not replace the standalone volatile Cache routes and does
//! not advertise a public `quorum_durable` profile.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    marker::PhantomData,
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::StatusCode,
    routing::{get, post},
};
use epoch_cache::{CacheConfig, CacheValue};
use epoch_consensus::{
    CommittedProposal, ConsensusError, ConsensusRole, ConsensusStatus, ProposalLookup,
};
use epoch_core::Clock;
use epoch_tablet::{
    CacheAcquireLockCommand, CacheCasExpectation, CacheCompareAndSetCommand, CacheDeleteCommand,
    CacheIncrementCommand, CacheLockGuard, CacheMaintainCommand, CacheReleaseLockCommand,
    CacheRenewLockCommand, CacheSetCommand, CacheTablet, CacheTabletCommand,
    CacheTabletDisposition, CacheTabletObservation, CacheTabletOperation, CacheTabletReceipt,
    CacheTabletScope, CacheTransactionCommand, CacheTransactionMutation, CommittedCommand,
    MAX_CACHE_KEY_BYTES, MAX_CACHE_TABLET_COMMAND_BYTES, TabletError, cache_proposal_id_for,
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor},
};
use tokio::sync::{Mutex, broadcast};

use crate::consensus::{CommittedProposalApplier, ConsensusProbeError, ConsensusProbeHandle};
use crate::tablet_http::{
    TabletApiError, TabletApiResult, deserialize_i64_from_number_or_decimal,
    deserialize_optional_u64_from_number_or_decimal, deserialize_u64_from_number_or_decimal,
    hex_digest, serialize_optional_u64_as_decimal, serialize_u64_as_decimal,
};

pub const EXPERIMENTAL_CACHE_TABLET_STATUS_PATH: &str = "/experimental/v1/tablets/cache/status";
pub const EXPERIMENTAL_CACHE_TABLET_MUTATIONS_PATH: &str =
    "/experimental/v1/tablets/cache/mutations";
pub const EXPERIMENTAL_CACHE_TABLET_MUTATION_PATH: &str =
    "/experimental/v1/tablets/cache/mutations/{proposal_id}";
pub const EXPERIMENTAL_CACHE_TABLET_OBSERVATIONS_PATH: &str =
    "/experimental/v1/tablets/cache/observations";

const TABLET_REQUEST_BODY_BYTES: usize = MAX_CACHE_TABLET_COMMAND_BYTES + 16 * 1024;
pub const DEFAULT_COMMIT_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct CacheTabletService {
    scope: CacheTabletScope,
    config: CacheConfig,
    tablet: RwLock<CacheTablet>,
    failure: RwLock<Option<String>>,
}

impl CacheTabletService {
    pub fn new(scope: CacheTabletScope, config: CacheConfig) -> Result<Arc<Self>, TabletError> {
        let tablet = CacheTablet::new(scope.clone(), config.clone())?;
        Ok(Arc::new(Self {
            scope,
            config,
            tablet: RwLock::new(tablet),
            failure: RwLock::new(None),
        }))
    }

    pub fn with_default_config(scope: CacheTabletScope) -> Result<Arc<Self>, TabletError> {
        Self::new(scope, CacheConfig::default())
    }

    pub fn scope(&self) -> &CacheTabletScope {
        &self.scope
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
        let result = self
            .tablet
            .write()
            .map_err(|_| "Cache tablet write lock was poisoned".to_owned())?
            .apply(committed_command(committed))
            .map_err(|error| error.to_string());
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
        self.tablet
            .read()
            .map_err(|_| "Cache tablet read lock was poisoned".to_owned())
            .map(|tablet| tablet.observe(key))
    }

    fn snapshot(&self) -> Result<CacheTabletSnapshot, String> {
        self.ensure_healthy()?;
        let tablet = self
            .tablet
            .read()
            .map_err(|_| "Cache tablet read lock was poisoned".to_owned())?;
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
            active_lock_count: usize_as_u64(tablet.active_lock_count(), "Cache active-lock count")?,
            cache_recovery_state_digest: hex_digest(tablet.cache_recovery_state_digest()),
            state_digest: hex_digest(tablet.state_digest()),
        })
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
        *self
            .tablet
            .write()
            .map_err(|_| self.fail("Cache tablet write lock was poisoned"))? = rebuilt;
        Ok(())
    }

    fn apply(&self, committed: &CommittedProposal) -> Result<(), String> {
        self.apply_one(committed).map(|_| ())
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
            EXPERIMENTAL_CACHE_TABLET_MUTATION_PATH,
            get(lookup_mutation),
        )
        .route(
            EXPERIMENTAL_CACHE_TABLET_OBSERVATIONS_PATH,
            get(observe_key),
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
}

impl CacheOperationRequest {
    fn to_tablet_operation(&self) -> TabletApiResult<CacheTabletOperation> {
        Ok(match self {
            Self::Set {
                shard,
                key,
                value,
                ttl_ms,
                lock_guard,
            } => CacheTabletOperation::Set(CacheSetCommand {
                shard: *shard,
                key: key.clone(),
                value: value.to_cache_value()?,
                ttl_ms: *ttl_ms,
                lock_guard: lock_guard.as_ref().map(CacheLockGuardRequest::to_tablet),
            }),
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
            } => CacheTabletOperation::CompareAndSet(CacheCompareAndSetCommand {
                shard: *shard,
                key: key.clone(),
                expected: expected.to_tablet(),
                value: value.to_cache_value()?,
                ttl_ms: *ttl_ms,
                lock_guard: lock_guard.as_ref().map(CacheLockGuardRequest::to_tablet),
            }),
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
            Self::Transaction {
                shard,
                expected_revision,
                mutations,
                lock_guards,
            } => transaction_operation(*shard, *expected_revision, mutations, lock_guards)?,
            Self::AcquireLock {
                shard,
                lock_key,
                owner,
                owner_epoch,
                lease_ms,
            } => acquire_lock_operation(*shard, lock_key, owner, *owner_epoch, *lease_ms),
            Self::RenewLock {
                shard,
                lock_key,
                owner,
                owner_epoch,
                lease_token,
                extension_ms,
            } => renew_lock_operation(
                *shard,
                lock_key,
                owner,
                *owner_epoch,
                lease_token,
                *extension_ms,
            ),
            Self::ReleaseLock {
                shard,
                lock_key,
                owner,
                owner_epoch,
                lease_token,
            } => release_lock_operation(*shard, lock_key, owner, *owner_epoch, lease_token),
            Self::Maintain {
                shard,
                max_expirations,
            } => CacheTabletOperation::Maintain(CacheMaintainCommand {
                shard: *shard,
                max_expirations: *max_expirations,
            }),
        })
    }
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
}

impl CacheTransactionMutationRequest {
    fn to_tablet(&self) -> TabletApiResult<CacheTransactionMutation> {
        Ok(match self {
            Self::Set { key, value, ttl_ms } => CacheTransactionMutation::Set {
                key: key.clone(),
                value: value.to_cache_value()?,
                ttl_ms: *ttl_ms,
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

    wait_for_committed_response(&state, commits, proposal_id, &request, replayed).await
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
) -> TabletApiResult<Json<CacheTabletObservationResponse>> {
    let Query(query) = query.map_err(|rejection| TabletApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    })?;
    validate_observation_key(&query.key)?;
    Ok(Json(CacheTabletObservationResponse {
        observation_scope: "local",
        read_consistency: "local_profile_applied_stale_capable",
        linearizable_read_barrier: false,
        observation: state.service.observe(&query.key)?,
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
) -> TabletApiResult<Json<CacheTabletStatus>> {
    // Profile-first sampling guarantees this document cannot report a profile
    // index ahead of its later actor-owned consensus snapshot.
    let profile = state.service.snapshot()?;
    let consensus = state.consensus.status().await?;
    Ok(Json(CacheTabletStatus::new(
        state.service.scope(),
        &consensus,
        profile,
    )?))
}

#[derive(Debug)]
struct CacheTabletSnapshot {
    last_profile_mutation_index: u64,
    last_applied_time_ms: u64,
    applied_command_count: u64,
    cache_revision: u64,
    retained_entry_count: u64,
    active_lock_count: u64,
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
    active_lock_count: u64,
    cache_recovery_state_digest: String,
    state_digest: String,
    write_guarantee: &'static str,
    read_consistency: &'static str,
    linearizable_read_barrier: bool,
}

impl CacheTabletStatus {
    fn new(
        scope: &CacheTabletScope,
        consensus: &ConsensusStatus,
        profile: CacheTabletSnapshot,
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
            active_lock_count: profile.active_lock_count,
            cache_recovery_state_digest: profile.cache_recovery_state_digest,
            state_digest: profile.state_digest,
            write_guarantee: "fixed_three_voter_majority_persisted_then_local_profile_applied",
            read_consistency: "local_profile_applied_stale_capable",
            linearizable_read_barrier: false,
        })
    }
}

#[derive(Debug, Serialize)]
struct CacheTabletObservationResponse {
    observation_scope: &'static str,
    read_consistency: &'static str,
    linearizable_read_barrier: bool,
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
