//! Canonical replicated Cache tablet state machine.

mod command;
mod digest;
mod lock;
mod model;

use std::collections::BTreeMap;

use epoch_cache::{
    CacheConfig, CacheMutation, CacheMutationResult, CacheShard, CacheTransaction, CacheValue,
    EvictionPolicy, SetOptions,
};
use epoch_core::{EpochError, EpochResult};
use serde::Serialize;

use crate::common::{AppliedCommand, validate_committed_command_scope};
use crate::{
    AppliedCommandMetadata, CommittedCommand, TabletError, TabletResult, TabletWriteEvidence,
};

pub use command::*;
use digest::{encode_auxiliary_state, initial_state_digest, transition_digest};
pub use lock::*;
pub use model::*;

#[derive(Debug, Clone, Serialize)]
struct ActiveCacheLock {
    owner: String,
    owner_epoch: u64,
    acquired_term: u64,
    acquisition_index: u64,
    lease_generation: u64,
    lease_deadline_ms: u64,
    lease_token: String,
}

impl ActiveCacheLock {
    const fn fencing_token(&self, tablet_epoch: u64) -> CacheLockFencingToken {
        CacheLockFencingToken {
            tablet_epoch,
            acquisition_index: self.acquisition_index,
        }
    }
}

#[derive(Debug, Clone)]
struct CacheTabletBusinessState {
    shard: CacheShard,
    default_ttl_ms: Option<u64>,
    max_locks: usize,
    active_owner_epochs: BTreeMap<String, u64>,
    locks: BTreeMap<String, ActiveCacheLock>,
}

#[derive(Debug)]
pub struct CacheTablet {
    scope: CacheTabletScope,
    state: CacheTabletBusinessState,
    applied: BTreeMap<u64, AppliedCommand<CacheTabletReceipt>>,
    last_applied_command_index: u64,
    last_applied_time_ms: u64,
    state_digest: [u8; 32],
}

impl CacheTablet {
    #[allow(
        clippy::needless_pass_by_value,
        reason = "tablet constructors consistently take ownership of their validated profile configuration"
    )]
    pub fn new(scope: CacheTabletScope, config: CacheConfig) -> TabletResult<Self> {
        scope.validate()?;
        validate_config(&config)?;
        // Consensus owns persistence evidence for this tablet. CacheShard has
        // no durability path of its own, so the caller's durability label is
        // deliberately normalized away rather than entering state or digest.
        let shard = CacheShard::new(config.max_entries, config.default_ttl_ms)?;
        let state_digest = initial_state_digest(&scope, config.max_entries, config.default_ttl_ms);
        Ok(Self {
            scope,
            state: CacheTabletBusinessState {
                shard,
                default_ttl_ms: config.default_ttl_ms,
                max_locks: config.max_entries,
                active_owner_epochs: BTreeMap::new(),
                locks: BTreeMap::new(),
            },
            applied: BTreeMap::new(),
            last_applied_command_index: 0,
            last_applied_time_ms: 0,
            state_digest,
        })
    }

    pub fn with_default_config(scope: CacheTabletScope) -> TabletResult<Self> {
        Self::new(scope, CacheConfig::default())
    }

    pub fn scope(&self) -> &CacheTabletScope {
        &self.scope
    }

    pub fn apply(&mut self, committed: CommittedCommand<'_>) -> TabletResult<CacheTabletReceipt> {
        validate_committed_command_scope(&self.scope, committed)?;
        let metadata = AppliedCommandMetadata::from_committed(committed);
        if let Some(mut receipt) = self.receipt_for_committed(committed)? {
            receipt.disposition = CacheTabletDisposition::Replayed;
            return Ok(receipt);
        }
        if committed.log_index <= self.last_applied_command_index {
            return Err(TabletError::CommitOrder {
                previous: self.last_applied_command_index,
                observed: committed.log_index,
            });
        }

        let command = CacheTabletCommand::decode(committed.payload, &self.scope)?;
        let expected_proposal_id = command.proposal_id(&self.scope)?;
        if committed.proposal_id != expected_proposal_id {
            return Err(TabletError::InvalidCommand(format!(
                "proposal_id {} does not match idempotency_key hash {expected_proposal_id}",
                committed.proposal_id
            )));
        }
        // The committed prefix is the authoritative time order. A lower-clock
        // replacement leader cannot move TTL or lock time backward.
        let applied_at_ms = command.applied_at_ms.max(self.last_applied_time_ms);

        let mut candidate = self.state.clone();
        let execution = candidate.execute(&self.scope, committed, command.operation, applied_at_ms);
        let (outcome, next_state) = match execution {
            Ok(result) => (CacheTabletOutcome::Applied { result }, Some(candidate)),
            Err(error) => (recordable_rejected_outcome(error)?, None),
        };
        let receipt = CacheTabletReceipt {
            proposal_id: committed.proposal_id,
            tablet_id: self.scope.tablet_id,
            tablet_epoch: self.scope.tablet_epoch,
            term: committed.term,
            commit_index: committed.log_index,
            applied_at_ms,
            write_evidence: TabletWriteEvidence::FixedVoterMajorityPersisted,
            durable_voter_acks: 2,
            disposition: CacheTabletDisposition::New,
            outcome,
        };

        let effective_state = next_state.as_ref().unwrap_or(&self.state);
        let auxiliary_bytes = encode_auxiliary_state(effective_state, applied_at_ms)?;
        let next_digest = transition_digest(
            self.state_digest,
            committed,
            metadata.payload_digest,
            effective_state.shard.recovery_state_digest(),
            &auxiliary_bytes,
            &receipt.outcome,
        )?;

        if let Some(next_state) = next_state {
            self.state = next_state;
        }
        self.state_digest = next_digest;
        self.last_applied_command_index = committed.log_index;
        self.last_applied_time_ms = applied_at_ms;
        self.applied.insert(
            committed.proposal_id,
            AppliedCommand {
                metadata,
                receipt: receipt.clone(),
            },
        );
        Ok(receipt)
    }

    pub fn lookup(&self, proposal_id: u64) -> Option<CacheTabletReceipt> {
        self.applied
            .get(&proposal_id)
            .map(|applied| applied.receipt.clone())
    }

    pub fn receipt_for_committed(
        &self,
        committed: CommittedCommand<'_>,
    ) -> TabletResult<Option<CacheTabletReceipt>> {
        validate_committed_command_scope(&self.scope, committed)?;
        let Some(previous) = self.applied.get(&committed.proposal_id) else {
            return Ok(None);
        };
        previous.metadata.validate_exact(committed)?;
        Ok(Some(previous.receipt.clone()))
    }

    /// Returns a pure local observation at the last committed effective time.
    pub fn observe(&self, key: &str) -> CacheTabletObservation {
        let observation = self.state.shard.observe(key, self.last_applied_time_ms);
        CacheTabletObservation {
            shard_revision: observation.revision,
            observed_at_ms: self.last_applied_time_ms,
            item: observation.item.map(Into::into),
        }
    }

    pub const fn cache_revision(&self) -> u64 {
        self.state.shard.revision()
    }

    /// Returns physically retained entries, including expired values that have
    /// not yet been reclaimed by a committed maintenance command.
    pub fn cache_entry_count(&self) -> usize {
        self.state.shard.len()
    }

    pub fn active_lock_count(&self) -> usize {
        self.state
            .locks
            .values()
            .filter(|lock| lock.lease_deadline_ms > self.last_applied_time_ms)
            .count()
    }

    pub const fn last_applied_command_index(&self) -> u64 {
        self.last_applied_command_index
    }

    pub const fn last_applied_time_ms(&self) -> u64 {
        self.last_applied_time_ms
    }

    pub fn applied_command_count(&self) -> usize {
        self.applied.len()
    }

    pub fn cache_recovery_state_checksum(&self) -> u32 {
        self.state.shard.recovery_state_checksum()
    }

    pub fn cache_recovery_state_digest(&self) -> [u8; 32] {
        self.state.shard.recovery_state_digest()
    }

    pub const fn state_digest(&self) -> [u8; 32] {
        self.state_digest
    }
}

impl CacheTabletBusinessState {
    fn execute(
        &mut self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        operation: CacheTabletOperation,
        applied_at_ms: u64,
    ) -> EpochResult<CacheTabletOperationResult> {
        match operation {
            CacheTabletOperation::Set(command) => {
                self.execute_set(scope, committed, command, applied_at_ms)
            }
            CacheTabletOperation::Delete(command) => {
                self.execute_delete(scope, committed, command, applied_at_ms)
            }
            CacheTabletOperation::CompareAndSet(command) => {
                self.execute_compare_and_set(scope, committed, command, applied_at_ms)
            }
            CacheTabletOperation::Increment(command) => {
                self.execute_increment(scope, committed, command, applied_at_ms)
            }
            CacheTabletOperation::Transaction(command) => {
                self.execute_transaction(scope, committed, command, applied_at_ms)
            }
            CacheTabletOperation::AcquireLock(command) => {
                self.execute_acquire_lock(scope, committed, command, applied_at_ms)
            }
            CacheTabletOperation::RenewLock(command) => {
                self.execute_renew_lock(scope, committed, command, applied_at_ms)
            }
            CacheTabletOperation::ReleaseLock(command) => {
                self.execute_release_lock(scope, committed, command, applied_at_ms)
            }
            CacheTabletOperation::Maintain(command) => {
                self.execute_maintain(command, applied_at_ms)
            }
        }
    }

    fn execute_set(
        &mut self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        command: CacheSetCommand,
        applied_at_ms: u64,
    ) -> EpochResult<CacheTabletOperationResult> {
        self.authorize_optional_guard(
            scope,
            committed,
            command.lock_guard.as_ref(),
            applied_at_ms,
        )?;
        ensure_deadline_fits(
            applied_at_ms,
            command.ttl_ms.or(self.default_ttl_ms),
            "cache TTL deadline",
        )?;
        let key = command.key;
        let result = self.shard.transact(
            CacheTransaction {
                expected_revision: None,
                operations: vec![CacheMutation::Set {
                    key: key.clone(),
                    value: command.value,
                    options: SetOptions {
                        ttl_ms: command.ttl_ms,
                        ..SetOptions::default()
                    },
                }],
            },
            applied_at_ms,
        )?;
        let CacheMutationResult::Set { item } = one_result(result.results)? else {
            return Err(EpochError::Internal(
                "Cache Set returned a mismatched engine result".into(),
            ));
        };
        Ok(CacheTabletOperationResult::Set {
            key,
            item: item.into(),
        })
    }

    fn execute_delete(
        &mut self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        command: CacheDeleteCommand,
        applied_at_ms: u64,
    ) -> EpochResult<CacheTabletOperationResult> {
        self.authorize_optional_guard(
            scope,
            committed,
            command.lock_guard.as_ref(),
            applied_at_ms,
        )?;
        let key = command.key;
        let result = self.shard.transact(
            CacheTransaction {
                expected_revision: None,
                operations: vec![CacheMutation::Delete {
                    key: key.clone(),
                    expected_version: command.expected_version,
                }],
            },
            applied_at_ms,
        )?;
        let revision = result.revision;
        let CacheMutationResult::Delete {
            deleted,
            previous_version,
        } = one_result(result.results)?
        else {
            return Err(EpochError::Internal(
                "Cache Delete returned a mismatched engine result".into(),
            ));
        };
        Ok(CacheTabletOperationResult::Deleted {
            key,
            deleted,
            previous_version,
            revision,
        })
    }

    fn execute_compare_and_set(
        &mut self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        command: CacheCompareAndSetCommand,
        applied_at_ms: u64,
    ) -> EpochResult<CacheTabletOperationResult> {
        self.authorize_optional_guard(
            scope,
            committed,
            command.lock_guard.as_ref(),
            applied_at_ms,
        )?;
        ensure_deadline_fits(
            applied_at_ms,
            command.ttl_ms.or(self.default_ttl_ms),
            "cache TTL deadline",
        )?;
        let key = command.key;
        let (expected_revision, expected_version) = cas_engine_expectation(command.expected);
        let result = self.shard.transact(
            CacheTransaction {
                expected_revision,
                operations: vec![CacheMutation::CompareAndSet {
                    key: key.clone(),
                    expected_version,
                    value: command.value,
                    ttl_ms: command.ttl_ms,
                }],
            },
            applied_at_ms,
        )?;
        let CacheMutationResult::CompareAndSet { item } = one_result(result.results)? else {
            return Err(EpochError::Internal(
                "Cache CAS returned a mismatched engine result".into(),
            ));
        };
        Ok(CacheTabletOperationResult::ComparedAndSet {
            key,
            item: item.into(),
        })
    }

    fn execute_increment(
        &mut self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        command: CacheIncrementCommand,
        applied_at_ms: u64,
    ) -> EpochResult<CacheTabletOperationResult> {
        self.authorize_optional_guard(
            scope,
            committed,
            command.lock_guard.as_ref(),
            applied_at_ms,
        )?;
        let key = command.key;
        let missing = self.shard.observe(&key, applied_at_ms).item.is_none();
        let operation = if missing {
            ensure_deadline_fits(
                applied_at_ms,
                command.ttl_ms.or(self.default_ttl_ms),
                "cache TTL deadline",
            )?;
            CacheMutation::Set {
                key: key.clone(),
                value: CacheValue::Counter(command.delta),
                options: SetOptions {
                    ttl_ms: command.ttl_ms,
                    expected_version: command.expected_version,
                    ..SetOptions::default()
                },
            }
        } else {
            CacheMutation::Increment {
                key: key.clone(),
                delta: command.delta,
                expected_version: command.expected_version,
            }
        };
        let result = self.shard.transact(
            CacheTransaction {
                expected_revision: None,
                operations: vec![operation],
            },
            applied_at_ms,
        )?;
        let result = one_result(result.results)?;
        let (value, version, expires_at_ms) = match result {
            CacheMutationResult::Set { item } => {
                let CacheValue::Counter(value) = item.value else {
                    return Err(EpochError::Internal(
                        "translated Cache Increment created a non-counter".into(),
                    ));
                };
                (value, item.version, item.expires_at_ms)
            }
            CacheMutationResult::Increment {
                value,
                version,
                expires_at_ms,
            } => (value, version, expires_at_ms),
            _ => {
                return Err(EpochError::Internal(
                    "Cache Increment returned a mismatched engine result".into(),
                ));
            }
        };
        Ok(CacheTabletOperationResult::Incremented {
            key,
            value,
            version,
            expires_at_ms,
        })
    }

    fn execute_transaction(
        &mut self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        command: CacheTransactionCommand,
        applied_at_ms: u64,
    ) -> EpochResult<CacheTabletOperationResult> {
        for guard in &command.lock_guards {
            self.authorize_guard(scope, committed, guard, applied_at_ms)?;
        }
        let mutations = command.mutations;
        let mut engine_operations = Vec::with_capacity(mutations.len());
        for mutation in &mutations {
            engine_operations.push(self.translate_transaction_mutation(mutation, applied_at_ms)?);
        }
        let result = self.shard.transact(
            CacheTransaction {
                expected_revision: Some(command.expected_revision),
                operations: engine_operations,
            },
            applied_at_ms,
        )?;
        let revision = result.revision;
        if mutations.len() != result.results.len() {
            return Err(EpochError::Internal(
                "Cache transaction result count did not match its mutations".into(),
            ));
        }
        let results = mutations
            .into_iter()
            .zip(result.results)
            .map(|(mutation, result)| map_transaction_result(mutation, result))
            .collect::<EpochResult<Vec<_>>>()?;
        Ok(CacheTabletOperationResult::TransactionCommitted { revision, results })
    }

    fn translate_transaction_mutation(
        &self,
        mutation: &CacheTransactionMutation,
        applied_at_ms: u64,
    ) -> EpochResult<CacheMutation> {
        match mutation {
            CacheTransactionMutation::Set { key, value, ttl_ms } => {
                ensure_deadline_fits(
                    applied_at_ms,
                    ttl_ms.or(self.default_ttl_ms),
                    "cache TTL deadline",
                )?;
                Ok(CacheMutation::Set {
                    key: key.clone(),
                    value: value.clone(),
                    options: SetOptions {
                        ttl_ms: *ttl_ms,
                        ..SetOptions::default()
                    },
                })
            }
            CacheTransactionMutation::Delete {
                key,
                expected_version,
            } => Ok(CacheMutation::Delete {
                key: key.clone(),
                expected_version: *expected_version,
            }),
            CacheTransactionMutation::CompareAndSet {
                key,
                expected,
                value,
                ttl_ms,
            } => {
                ensure_deadline_fits(
                    applied_at_ms,
                    ttl_ms.or(self.default_ttl_ms),
                    "cache TTL deadline",
                )?;
                let (_, expected_version) = cas_engine_expectation(*expected);
                Ok(CacheMutation::CompareAndSet {
                    key: key.clone(),
                    expected_version,
                    value: value.clone(),
                    ttl_ms: *ttl_ms,
                })
            }
            CacheTransactionMutation::Increment {
                key,
                delta,
                expected_version,
                ttl_ms,
            } => {
                if self.shard.observe(key, applied_at_ms).item.is_none() {
                    ensure_deadline_fits(
                        applied_at_ms,
                        ttl_ms.or(self.default_ttl_ms),
                        "cache TTL deadline",
                    )?;
                    Ok(CacheMutation::Set {
                        key: key.clone(),
                        value: CacheValue::Counter(*delta),
                        options: SetOptions {
                            ttl_ms: *ttl_ms,
                            expected_version: *expected_version,
                            ..SetOptions::default()
                        },
                    })
                } else {
                    Ok(CacheMutation::Increment {
                        key: key.clone(),
                        delta: *delta,
                        expected_version: *expected_version,
                    })
                }
            }
        }
    }

    fn execute_acquire_lock(
        &mut self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        command: CacheAcquireLockCommand,
        applied_at_ms: u64,
    ) -> EpochResult<CacheTabletOperationResult> {
        self.expire_lock_if_needed(&command.lock_key, applied_at_ms);
        if self.locks.len() >= self.max_locks {
            self.reclaim_one_expired_lock(applied_at_ms);
        }
        self.remove_owner_epoch_if_no_live_locks(&command.owner, applied_at_ms);
        if self
            .active_owner_epochs
            .get(&command.owner)
            .is_some_and(|current| command.owner_epoch < *current)
        {
            return Err(EpochError::Fenced);
        }
        if self.locks.contains_key(&command.lock_key) {
            return Err(EpochError::Conflict(format!(
                "Cache lock is already held: {}",
                command.lock_key
            )));
        }
        if self.locks.len() >= self.max_locks {
            return Err(EpochError::Capacity(
                "Cache tablet active-lock capacity is exhausted".into(),
            ));
        }
        if !self.active_owner_epochs.contains_key(&command.owner)
            && self.active_owner_epochs.len() >= self.max_locks
        {
            return Err(EpochError::Capacity(
                "Cache tablet owner-epoch capacity is exhausted".into(),
            ));
        }
        let lease_deadline_ms = applied_at_ms
            .checked_add(command.lease_ms)
            .ok_or_else(|| EpochError::Capacity("Cache lock deadline overflow".into()))?;
        let metadata = CacheLockTokenMetadata::new(
            scope.tablet_id,
            scope.tablet_epoch,
            command.shard,
            committed.term,
            command.owner_epoch,
            committed.log_index,
            1,
            lease_deadline_ms,
            command.lock_key.clone(),
            command.owner.clone(),
        )?;
        let lease_token = metadata.encode()?;
        let fencing_token = CacheLockFencingToken {
            tablet_epoch: scope.tablet_epoch,
            acquisition_index: committed.log_index,
        };
        self.active_owner_epochs
            .insert(command.owner.clone(), command.owner_epoch);
        self.locks.insert(
            command.lock_key.clone(),
            ActiveCacheLock {
                owner: command.owner.clone(),
                owner_epoch: command.owner_epoch,
                acquired_term: committed.term,
                acquisition_index: committed.log_index,
                lease_generation: 1,
                lease_deadline_ms,
                lease_token: lease_token.clone(),
            },
        );
        Ok(CacheTabletOperationResult::LockAcquired {
            lock_key: command.lock_key,
            owner: command.owner,
            owner_epoch: command.owner_epoch,
            fencing_token,
            lease_token,
            lease_generation: 1,
            lease_deadline_ms,
        })
    }

    fn execute_renew_lock(
        &mut self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        command: CacheRenewLockCommand,
        applied_at_ms: u64,
    ) -> EpochResult<CacheTabletOperationResult> {
        let guard = CacheLockGuard {
            lock_key: command.lock_key.clone(),
            owner: command.owner,
            owner_epoch: command.owner_epoch,
            lease_token: command.lease_token,
        };
        self.authorize_guard(scope, committed, &guard, applied_at_ms)?;
        let current = self
            .locks
            .get(&command.lock_key)
            .cloned()
            .ok_or(EpochError::Fenced)?;
        let lease_deadline_ms = applied_at_ms
            .checked_add(command.extension_ms)
            .ok_or_else(|| EpochError::Capacity("Cache lock deadline overflow".into()))?;
        if lease_deadline_ms <= current.lease_deadline_ms {
            return Err(EpochError::Conflict(
                "Cache lock renewal must produce a later deadline".into(),
            ));
        }
        let lease_generation = current.lease_generation.checked_add(1).ok_or_else(|| {
            EpochError::Capacity("Cache lock lease generation is exhausted".into())
        })?;
        let metadata = CacheLockTokenMetadata::new(
            scope.tablet_id,
            scope.tablet_epoch,
            0,
            committed.term,
            current.owner_epoch,
            current.acquisition_index,
            lease_generation,
            lease_deadline_ms,
            command.lock_key.clone(),
            current.owner.clone(),
        )?;
        let lease_token = metadata.encode()?;
        let lock = self
            .locks
            .get_mut(&command.lock_key)
            .ok_or(EpochError::Fenced)?;
        lock.lease_deadline_ms = lease_deadline_ms;
        lock.lease_generation = lease_generation;
        lock.lease_token.clone_from(&lease_token);
        let fencing_token = lock.fencing_token(scope.tablet_epoch);
        Ok(CacheTabletOperationResult::LockRenewed {
            lock_key: command.lock_key,
            fencing_token,
            lease_token,
            lease_generation,
            lease_deadline_ms,
        })
    }

    fn execute_release_lock(
        &mut self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        command: CacheReleaseLockCommand,
        applied_at_ms: u64,
    ) -> EpochResult<CacheTabletOperationResult> {
        let guard = CacheLockGuard {
            lock_key: command.lock_key.clone(),
            owner: command.owner,
            owner_epoch: command.owner_epoch,
            lease_token: command.lease_token,
        };
        self.authorize_guard(scope, committed, &guard, applied_at_ms)?;
        let lock = self
            .locks
            .remove(&command.lock_key)
            .ok_or(EpochError::Fenced)?;
        self.remove_owner_epoch_if_inactive(&lock.owner);
        Ok(CacheTabletOperationResult::LockReleased {
            lock_key: command.lock_key,
            fencing_token: lock.fencing_token(scope.tablet_epoch),
        })
    }

    fn execute_maintain(
        &mut self,
        command: CacheMaintainCommand,
        applied_at_ms: u64,
    ) -> EpochResult<CacheTabletOperationResult> {
        let limit = usize::from(command.max_expirations);
        let expired = self.shard.maintain_expiry(applied_at_ms, limit)?;
        let remaining = limit.saturating_sub(expired.expired_keys.len());
        let mut lock_candidates: Vec<_> = self
            .locks
            .iter()
            .filter(|(_, lock)| lock.lease_deadline_ms <= applied_at_ms)
            .map(|(key, lock)| (lock.lease_deadline_ms, key.clone()))
            .collect();
        lock_candidates.sort_unstable();
        let expired_locks: Vec<_> = lock_candidates
            .into_iter()
            .take(remaining)
            .map(|(_, key)| key)
            .collect();
        let mut expired_owners = Vec::with_capacity(expired_locks.len());
        for key in &expired_locks {
            if let Some(lock) = self.locks.remove(key) {
                expired_owners.push(lock.owner);
            }
        }
        for owner in expired_owners {
            self.remove_owner_epoch_if_inactive(&owner);
        }
        Ok(CacheTabletOperationResult::Maintained {
            cache_revision: expired.revision,
            expired_keys: expired.expired_keys,
            expired_locks,
        })
    }

    fn authorize_optional_guard(
        &self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        guard: Option<&CacheLockGuard>,
        applied_at_ms: u64,
    ) -> EpochResult<()> {
        guard.map_or(Ok(()), |guard| {
            self.authorize_guard(scope, committed, guard, applied_at_ms)
        })
    }

    fn authorize_guard(
        &self,
        scope: &CacheTabletScope,
        committed: CommittedCommand<'_>,
        guard: &CacheLockGuard,
        applied_at_ms: u64,
    ) -> EpochResult<()> {
        let metadata =
            CacheLockTokenMetadata::parse(&guard.lease_token).map_err(|_| EpochError::Fenced)?;
        if metadata.tablet_id() != scope.tablet_id
            || metadata.tablet_epoch() != scope.tablet_epoch
            || metadata.shard() != 0
            || metadata.leader_term() != committed.term
            || metadata.lock_key() != guard.lock_key
            || metadata.owner() != guard.owner
            || metadata.owner_epoch() != guard.owner_epoch
            || metadata.lease_deadline_ms() <= applied_at_ms
            || self.active_owner_epochs.get(&guard.owner).copied() != Some(guard.owner_epoch)
        {
            return Err(EpochError::Fenced);
        }
        let lock = self.locks.get(&guard.lock_key).ok_or(EpochError::Fenced)?;
        if lock.owner != guard.owner
            || lock.owner_epoch != guard.owner_epoch
            || lock.acquired_term != committed.term
            || lock.acquisition_index != metadata.acquisition_index()
            || lock.lease_generation != metadata.lease_generation()
            || lock.lease_deadline_ms != metadata.lease_deadline_ms()
            || lock.lease_deadline_ms <= applied_at_ms
            || lock.lease_token != guard.lease_token
        {
            return Err(EpochError::Fenced);
        }
        Ok(())
    }

    fn expire_lock_if_needed(&mut self, lock_key: &str, applied_at_ms: u64) {
        let expired = self
            .locks
            .get(lock_key)
            .is_some_and(|lock| lock.lease_deadline_ms <= applied_at_ms);
        if expired && let Some(lock) = self.locks.remove(lock_key) {
            self.remove_owner_epoch_if_inactive(&lock.owner);
        }
    }

    fn remove_owner_epoch_if_inactive(&mut self, owner: &str) {
        if !self.locks.values().any(|lock| lock.owner == owner) {
            self.active_owner_epochs.remove(owner);
        }
    }

    fn remove_owner_epoch_if_no_live_locks(&mut self, owner: &str, applied_at_ms: u64) {
        if !self
            .locks
            .values()
            .any(|lock| lock.owner == owner && lock.lease_deadline_ms > applied_at_ms)
        {
            self.active_owner_epochs.remove(owner);
        }
    }

    fn reclaim_one_expired_lock(&mut self, applied_at_ms: u64) {
        let candidate = self
            .locks
            .iter()
            .filter(|(_, lock)| lock.lease_deadline_ms <= applied_at_ms)
            .map(|(key, lock)| (lock.lease_deadline_ms, key.clone()))
            .min();
        if let Some((_, key)) = candidate
            && let Some(lock) = self.locks.remove(&key)
        {
            self.remove_owner_epoch_if_inactive(&lock.owner);
        }
    }
}

fn map_transaction_result(
    mutation: CacheTransactionMutation,
    result: CacheMutationResult,
) -> EpochResult<CacheTransactionMutationResult> {
    match (mutation, result) {
        (CacheTransactionMutation::Set { key, .. }, CacheMutationResult::Set { item }) => {
            Ok(CacheTransactionMutationResult::Set {
                key,
                item: item.into(),
            })
        }
        (
            CacheTransactionMutation::Delete { key, .. },
            CacheMutationResult::Delete {
                deleted,
                previous_version,
            },
        ) => Ok(CacheTransactionMutationResult::Deleted {
            key,
            deleted,
            previous_version,
        }),
        (
            CacheTransactionMutation::CompareAndSet { key, .. },
            CacheMutationResult::CompareAndSet { item },
        ) => Ok(CacheTransactionMutationResult::ComparedAndSet {
            key,
            item: item.into(),
        }),
        (
            CacheTransactionMutation::Increment { key, .. },
            CacheMutationResult::Increment {
                value,
                version,
                expires_at_ms,
            },
        ) => Ok(CacheTransactionMutationResult::Incremented {
            key,
            value,
            version,
            expires_at_ms,
        }),
        (CacheTransactionMutation::Increment { key, .. }, CacheMutationResult::Set { item }) => {
            let CacheValue::Counter(value) = item.value else {
                return Err(EpochError::Internal(
                    "translated transaction Increment created a non-counter".into(),
                ));
            };
            Ok(CacheTransactionMutationResult::Incremented {
                key,
                value,
                version: item.version,
                expires_at_ms: item.expires_at_ms,
            })
        }
        _ => Err(EpochError::Internal(
            "Cache transaction returned a mismatched engine result".into(),
        )),
    }
}

fn cas_engine_expectation(expectation: CacheCasExpectation) -> (Option<u64>, u64) {
    match expectation {
        CacheCasExpectation::Missing { shard_revision } => (Some(shard_revision), 0),
        CacheCasExpectation::Version { version } => (None, version),
    }
}

fn one_result(mut results: Vec<CacheMutationResult>) -> EpochResult<CacheMutationResult> {
    if results.len() != 1 {
        return Err(EpochError::Internal(format!(
            "Cache engine returned {} results for one mutation",
            results.len()
        )));
    }
    Ok(results.pop().expect("length was checked"))
}

fn ensure_deadline_fits(
    applied_at_ms: u64,
    duration_ms: Option<u64>,
    field: &str,
) -> EpochResult<()> {
    if duration_ms.is_some_and(|duration_ms| applied_at_ms.checked_add(duration_ms).is_none()) {
        return Err(EpochError::Capacity(format!("{field} overflow")));
    }
    Ok(())
}

fn validate_config(config: &CacheConfig) -> TabletResult<()> {
    if config.max_entries == 0 || config.max_entries > MAX_CACHE_TABLET_ENTRIES {
        return Err(TabletError::Profile(EpochError::InvalidArgument(format!(
            "Cache tablet max_entries must be between 1 and {MAX_CACHE_TABLET_ENTRIES}"
        ))));
    }
    if config.eviction != EvictionPolicy::NoEviction {
        return Err(TabletError::Profile(EpochError::InvalidArgument(
            "Cache tablet v1 supports only no-eviction".into(),
        )));
    }
    if config
        .default_ttl_ms
        .is_some_and(|ttl_ms| ttl_ms == 0 || ttl_ms > MAX_CACHE_TTL_MS)
    {
        return Err(TabletError::Profile(EpochError::InvalidArgument(format!(
            "Cache tablet default_ttl_ms must be between 1 and {MAX_CACHE_TTL_MS}"
        ))));
    }
    Ok(())
}

fn recordable_rejected_outcome(error: EpochError) -> TabletResult<CacheTabletOutcome> {
    let code = match &error {
        EpochError::AlreadyExists(_) => CacheTabletRejectionCode::AlreadyExists,
        EpochError::NotFound(_) => CacheTabletRejectionCode::NotFound,
        EpochError::InvalidArgument(_) => CacheTabletRejectionCode::InvalidArgument,
        EpochError::Conflict(_) => CacheTabletRejectionCode::Conflict,
        EpochError::Fenced => CacheTabletRejectionCode::Fenced,
        EpochError::Capacity(_) => CacheTabletRejectionCode::Capacity,
        EpochError::Unavailable(_) => CacheTabletRejectionCode::Unavailable,
        EpochError::Storage(_) | EpochError::Internal(_) => {
            return Err(TabletError::Profile(error));
        }
    };
    Ok(CacheTabletOutcome::Rejected {
        code,
        detail: error.to_string(),
    })
}

#[cfg(test)]
mod tests;
