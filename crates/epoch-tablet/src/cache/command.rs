//! Versioned Cache tablet commands and their strict canonical codec.

use std::collections::BTreeSet;

use epoch_cache::{CacheValue, MAX_CACHE_ATOMIC_OPERATIONS as ENGINE_MAX_CACHE_ATOMIC_OPERATIONS};
use serde::{Deserialize, Serialize};

use crate::common::{proposal_id_from_domain, validate_idempotency_key};
use crate::{TabletError, TabletResult, TabletScope};

pub const CACHE_TABLET_COMMAND_FORMAT_VERSION: u16 = 1;
pub const MAX_CACHE_TABLET_COMMAND_BYTES: usize = 512 * 1024;
pub const MAX_CACHE_KEY_BYTES: usize = 1_024;
pub const MAX_CACHE_OWNER_BYTES: usize = 256;
pub const MAX_CACHE_LOCK_TOKEN_BYTES: usize = 4 * 1024;
pub const MAX_CACHE_VALUE_BYTES: usize = 256 * 1024;
pub const MAX_CACHE_COLLECTION_ENTRIES: usize = 1_024;
pub const MAX_CACHE_MEMBER_BYTES: usize = 4 * 1024;
pub const MAX_CACHE_TRANSACTION_OPERATIONS: usize = ENGINE_MAX_CACHE_ATOMIC_OPERATIONS;
pub const MAX_CACHE_TRANSACTION_LOCK_GUARDS: usize = 16;
pub const MAX_CACHE_MAINTENANCE_EXPIRATIONS: u16 = 1_000;
pub const MAX_CACHE_TTL_MS: u64 = 31_536_000_000;
pub const MAX_CACHE_LOCK_LEASE_MS: u64 = 86_400_000;
pub const MAX_CACHE_TABLET_ENTRIES: usize = 1_000_000;

pub type CacheTabletScope = TabletScope;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheTabletCommand {
    pub format_version: u16,
    pub tablet_id: u64,
    pub tablet_epoch: u64,
    pub resource: String,
    pub idempotency_key: String,
    pub applied_at_ms: u64,
    pub operation: CacheTabletOperation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CacheTabletOperation {
    Set(CacheSetCommand),
    Delete(CacheDeleteCommand),
    CompareAndSet(CacheCompareAndSetCommand),
    Increment(CacheIncrementCommand),
    Transaction(CacheTransactionCommand),
    AcquireLock(CacheAcquireLockCommand),
    RenewLock(CacheRenewLockCommand),
    ReleaseLock(CacheReleaseLockCommand),
    Maintain(CacheMaintainCommand),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheSetCommand {
    pub shard: u32,
    pub key: String,
    pub value: CacheValue,
    pub ttl_ms: Option<u64>,
    pub lock_guard: Option<CacheLockGuard>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheDeleteCommand {
    pub shard: u32,
    pub key: String,
    pub expected_version: Option<u64>,
    pub lock_guard: Option<CacheLockGuard>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheCompareAndSetCommand {
    pub shard: u32,
    pub key: String,
    pub expected: CacheCasExpectation,
    pub value: CacheValue,
    pub ttl_ms: Option<u64>,
    pub lock_guard: Option<CacheLockGuard>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CacheCasExpectation {
    Missing { shard_revision: u64 },
    Version { version: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheIncrementCommand {
    pub shard: u32,
    pub key: String,
    pub delta: i64,
    pub expected_version: Option<u64>,
    pub ttl_ms: Option<u64>,
    pub lock_guard: Option<CacheLockGuard>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheTransactionCommand {
    pub shard: u32,
    pub expected_revision: u64,
    pub mutations: Vec<CacheTransactionMutation>,
    pub lock_guards: Vec<CacheLockGuard>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CacheTransactionMutation {
    Set {
        key: String,
        value: CacheValue,
        ttl_ms: Option<u64>,
    },
    Delete {
        key: String,
        expected_version: Option<u64>,
    },
    CompareAndSet {
        key: String,
        expected: CacheCasExpectation,
        value: CacheValue,
        ttl_ms: Option<u64>,
    },
    Increment {
        key: String,
        delta: i64,
        expected_version: Option<u64>,
        ttl_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheAcquireLockCommand {
    pub shard: u32,
    pub lock_key: String,
    pub owner: String,
    pub owner_epoch: u64,
    pub lease_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheRenewLockCommand {
    pub shard: u32,
    pub lock_key: String,
    pub owner: String,
    pub owner_epoch: u64,
    pub lease_token: String,
    pub extension_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheReleaseLockCommand {
    pub shard: u32,
    pub lock_key: String,
    pub owner: String,
    pub owner_epoch: u64,
    pub lease_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheLockGuard {
    pub lock_key: String,
    pub owner: String,
    pub owner_epoch: u64,
    pub lease_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheMaintainCommand {
    pub shard: u32,
    pub max_expirations: u16,
}

impl CacheTabletCommand {
    pub fn new(
        scope: &CacheTabletScope,
        idempotency_key: impl Into<String>,
        applied_at_ms: u64,
        operation: CacheTabletOperation,
    ) -> TabletResult<Self> {
        let command = Self {
            format_version: CACHE_TABLET_COMMAND_FORMAT_VERSION,
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            idempotency_key: idempotency_key.into(),
            applied_at_ms,
            operation,
        };
        command.validate(scope)?;
        Ok(command)
    }

    pub fn encode(&self, scope: &CacheTabletScope) -> TabletResult<Vec<u8>> {
        self.validate(scope)?;
        let encoded =
            serde_json::to_vec(self).map_err(|error| TabletError::Encoding(error.to_string()))?;
        if encoded.len() > MAX_CACHE_TABLET_COMMAND_BYTES {
            return Err(command_too_large(encoded.len()));
        }
        Ok(encoded)
    }

    pub fn decode(payload: &[u8], scope: &CacheTabletScope) -> TabletResult<Self> {
        if payload.len() > MAX_CACHE_TABLET_COMMAND_BYTES {
            return Err(command_too_large(payload.len()));
        }
        let command: Self = serde_json::from_slice(payload)
            .map_err(|error| TabletError::Decoding(error.to_string()))?;
        command.validate(scope)?;
        let canonical = serde_json::to_vec(&command)
            .map_err(|error| TabletError::Encoding(error.to_string()))?;
        if canonical != payload {
            return Err(TabletError::Decoding(
                "command bytes are not in canonical v1 encoding".into(),
            ));
        }
        Ok(command)
    }

    pub fn proposal_id(&self, scope: &CacheTabletScope) -> TabletResult<u64> {
        self.validate(scope)?;
        cache_proposal_id_for(scope, &self.idempotency_key)
    }

    fn validate(&self, scope: &CacheTabletScope) -> TabletResult<()> {
        scope.validate()?;
        if self.format_version != CACHE_TABLET_COMMAND_FORMAT_VERSION {
            return Err(TabletError::InvalidCommand(format!(
                "unsupported format_version {}",
                self.format_version
            )));
        }
        if self.tablet_id != scope.tablet_id {
            return Err(TabletError::GroupMismatch {
                expected: scope.tablet_id,
                observed: self.tablet_id,
            });
        }
        if self.tablet_epoch != scope.tablet_epoch {
            return Err(TabletError::FencedEpoch {
                expected: scope.tablet_epoch,
                observed: self.tablet_epoch,
            });
        }
        if self.resource != scope.resource {
            return Err(TabletError::InvalidCommand(format!(
                "command targets resource {}; expected {}",
                self.resource, scope.resource
            )));
        }
        validate_idempotency_key(&self.idempotency_key)?;
        self.operation.validate()
    }
}

impl CacheTabletOperation {
    fn validate(&self) -> TabletResult<()> {
        let shard = match self {
            Self::Set(command) => {
                validate_key(&command.key)?;
                validate_value(&command.value)?;
                validate_ttl(command.ttl_ms)?;
                validate_optional_guard(command.lock_guard.as_ref())?;
                command.shard
            }
            Self::Delete(command) => {
                validate_key(&command.key)?;
                validate_optional_guard(command.lock_guard.as_ref())?;
                command.shard
            }
            Self::CompareAndSet(command) => {
                validate_key(&command.key)?;
                validate_cas_expectation(command.expected)?;
                validate_value(&command.value)?;
                validate_ttl(command.ttl_ms)?;
                validate_optional_guard(command.lock_guard.as_ref())?;
                command.shard
            }
            Self::Increment(command) => {
                validate_key(&command.key)?;
                validate_ttl(command.ttl_ms)?;
                validate_optional_guard(command.lock_guard.as_ref())?;
                command.shard
            }
            Self::Transaction(command) => {
                validate_transaction(command)?;
                command.shard
            }
            Self::AcquireLock(command) => {
                validate_lock_identity(&command.lock_key, &command.owner, command.owner_epoch)?;
                validate_duration("lease_ms", command.lease_ms, MAX_CACHE_LOCK_LEASE_MS)?;
                command.shard
            }
            Self::RenewLock(command) => {
                validate_lock_identity(&command.lock_key, &command.owner, command.owner_epoch)?;
                validate_lock_token(&command.lease_token)?;
                validate_duration(
                    "extension_ms",
                    command.extension_ms,
                    MAX_CACHE_LOCK_LEASE_MS,
                )?;
                command.shard
            }
            Self::ReleaseLock(command) => {
                validate_lock_identity(&command.lock_key, &command.owner, command.owner_epoch)?;
                validate_lock_token(&command.lease_token)?;
                command.shard
            }
            Self::Maintain(command) => {
                if !(1..=MAX_CACHE_MAINTENANCE_EXPIRATIONS).contains(&command.max_expirations) {
                    return Err(TabletError::InvalidCommand(format!(
                        "max_expirations must be between 1 and {MAX_CACHE_MAINTENANCE_EXPIRATIONS}"
                    )));
                }
                command.shard
            }
        };
        if shard != 0 {
            return Err(TabletError::InvalidCommand(
                "Cache tablet v1 supports only shard 0".into(),
            ));
        }
        Ok(())
    }
}

impl CacheTransactionMutation {
    fn key(&self) -> &str {
        match self {
            Self::Set { key, .. }
            | Self::Delete { key, .. }
            | Self::CompareAndSet { key, .. }
            | Self::Increment { key, .. } => key,
        }
    }

    fn validate(&self, transaction_revision: u64) -> TabletResult<()> {
        validate_key(self.key())?;
        match self {
            Self::Set { value, ttl_ms, .. } => {
                validate_value(value)?;
                validate_ttl(*ttl_ms)
            }
            Self::Delete { .. } => Ok(()),
            Self::CompareAndSet {
                expected,
                value,
                ttl_ms,
                ..
            } => {
                validate_cas_expectation(*expected)?;
                if let CacheCasExpectation::Missing { shard_revision } = expected
                    && *shard_revision != transaction_revision
                {
                    return Err(TabletError::InvalidCommand(format!(
                        "missing CAS shard_revision {shard_revision} must match transaction expected_revision {transaction_revision}"
                    )));
                }
                validate_value(value)?;
                validate_ttl(*ttl_ms)
            }
            Self::Increment { ttl_ms, .. } => validate_ttl(*ttl_ms),
        }
    }
}

pub fn cache_proposal_id_for(scope: &CacheTabletScope, idempotency_key: &str) -> TabletResult<u64> {
    proposal_id_from_domain(
        b"epoch/cache-tablet/proposal-id/v1\0",
        scope,
        idempotency_key,
    )
}

fn validate_transaction(command: &CacheTransactionCommand) -> TabletResult<()> {
    let operation_count = command.mutations.len();
    if !(1..=MAX_CACHE_TRANSACTION_OPERATIONS).contains(&operation_count) {
        return Err(TabletError::InvalidCommand(format!(
            "cache transaction must contain between 1 and {MAX_CACHE_TRANSACTION_OPERATIONS} mutations"
        )));
    }
    let mut keys = BTreeSet::new();
    for mutation in &command.mutations {
        mutation.validate(command.expected_revision)?;
        if !keys.insert(mutation.key()) {
            return Err(TabletError::InvalidCommand(format!(
                "cache transaction contains duplicate key: {}",
                mutation.key()
            )));
        }
    }
    if command.lock_guards.len() > MAX_CACHE_TRANSACTION_LOCK_GUARDS {
        return Err(TabletError::InvalidCommand(format!(
            "cache transaction has {} lock guards; maximum is {MAX_CACHE_TRANSACTION_LOCK_GUARDS}",
            command.lock_guards.len()
        )));
    }
    let mut lock_keys = BTreeSet::new();
    for guard in &command.lock_guards {
        validate_guard(guard)?;
        if !lock_keys.insert(guard.lock_key.as_str()) {
            return Err(TabletError::InvalidCommand(format!(
                "cache transaction contains duplicate lock guard: {}",
                guard.lock_key
            )));
        }
    }
    Ok(())
}

fn validate_optional_guard(guard: Option<&CacheLockGuard>) -> TabletResult<()> {
    guard.map_or(Ok(()), validate_guard)
}

fn validate_guard(guard: &CacheLockGuard) -> TabletResult<()> {
    validate_lock_identity(&guard.lock_key, &guard.owner, guard.owner_epoch)?;
    validate_lock_token(&guard.lease_token)
}

fn validate_lock_identity(lock_key: &str, owner: &str, owner_epoch: u64) -> TabletResult<()> {
    validate_required_bounded("lock_key", lock_key, MAX_CACHE_KEY_BYTES)?;
    validate_required_bounded("owner", owner, MAX_CACHE_OWNER_BYTES)?;
    if owner_epoch == 0 {
        return Err(TabletError::InvalidCommand(
            "owner_epoch must be non-zero".into(),
        ));
    }
    Ok(())
}

fn validate_lock_token(token: &str) -> TabletResult<()> {
    validate_required_bounded("lease_token", token, MAX_CACHE_LOCK_TOKEN_BYTES)
}

fn validate_key(key: &str) -> TabletResult<()> {
    validate_required_bounded("key", key, MAX_CACHE_KEY_BYTES)
}

fn validate_required_bounded(field: &str, value: &str, maximum: usize) -> TabletResult<()> {
    if value.trim().is_empty() {
        return Err(TabletError::InvalidCommand(format!("{field} is required")));
    }
    if value.len() > maximum {
        return Err(TabletError::InvalidCommand(format!(
            "{field} is {} bytes; maximum is {maximum}",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(TabletError::InvalidCommand(format!(
            "{field} cannot contain control characters"
        )));
    }
    Ok(())
}

fn validate_cas_expectation(expectation: CacheCasExpectation) -> TabletResult<()> {
    if let CacheCasExpectation::Version { version: 0 } = expectation {
        return Err(TabletError::InvalidCommand(
            "present CAS version must be non-zero".into(),
        ));
    }
    Ok(())
}

fn validate_ttl(ttl_ms: Option<u64>) -> TabletResult<()> {
    ttl_ms.map_or(Ok(()), |ttl_ms| {
        validate_duration("ttl_ms", ttl_ms, MAX_CACHE_TTL_MS)
    })
}

fn validate_duration(field: &str, value: u64, maximum: u64) -> TabletResult<()> {
    if value == 0 || value > maximum {
        return Err(TabletError::InvalidCommand(format!(
            "{field} must be between 1 and {maximum}"
        )));
    }
    Ok(())
}

fn validate_value(value: &CacheValue) -> TabletResult<()> {
    let encoded =
        serde_json::to_vec(value).map_err(|error| TabletError::Encoding(error.to_string()))?;
    if encoded.len() > MAX_CACHE_VALUE_BYTES {
        return Err(TabletError::InvalidCommand(format!(
            "encoded cache value is {} bytes; maximum is {MAX_CACHE_VALUE_BYTES}",
            encoded.len()
        )));
    }
    match value {
        CacheValue::String(_) | CacheValue::Blob(_) | CacheValue::Counter(_) => Ok(()),
        CacheValue::Hash(values) => {
            validate_collection_length(values.len())?;
            for (field, value) in values {
                validate_member("hash field", field)?;
                validate_member("hash value", value)?;
            }
            Ok(())
        }
        CacheValue::List(values) => {
            validate_collection_length(values.len())?;
            values
                .iter()
                .try_for_each(|value| validate_member("list item", value))
        }
        CacheValue::Set(values) => {
            validate_collection_length(values.len())?;
            values
                .iter()
                .try_for_each(|value| validate_member("set member", value))
        }
        CacheValue::SortedSet(values) => {
            validate_collection_length(values.len())?;
            for (member, score) in values {
                validate_member("sorted-set member", member)?;
                if !score.is_finite() {
                    return Err(TabletError::InvalidCommand(
                        "sorted-set score must be finite".into(),
                    ));
                }
            }
            Ok(())
        }
    }
}

fn validate_collection_length(length: usize) -> TabletResult<()> {
    if length > MAX_CACHE_COLLECTION_ENTRIES {
        return Err(TabletError::InvalidCommand(format!(
            "cache collection has {length} entries; maximum is {MAX_CACHE_COLLECTION_ENTRIES}"
        )));
    }
    Ok(())
}

fn validate_member(kind: &str, value: &str) -> TabletResult<()> {
    if value.len() > MAX_CACHE_MEMBER_BYTES {
        return Err(TabletError::InvalidCommand(format!(
            "{kind} is {} bytes; maximum is {MAX_CACHE_MEMBER_BYTES}",
            value.len()
        )));
    }
    Ok(())
}

fn command_too_large(length: usize) -> TabletError {
    TabletError::InvalidCommand(format!(
        "encoded command is {length} bytes; maximum is {MAX_CACHE_TABLET_COMMAND_BYTES}"
    ))
}
