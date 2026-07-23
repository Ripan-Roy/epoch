//! Canonical replicated Queue tablet state machine.

mod command;
mod digest;
mod model;

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{DurabilityProfile, EpochError};
use epoch_queue::{FencedLeaseTokenMetadata, LeaseFence, Queue, QueueConfig, QueueCounts};

use crate::common::{AppliedCommand, validate_committed_command_scope};
use crate::{
    AppliedCommandMetadata, CommittedCommand, TabletError, TabletResult, TabletWriteEvidence,
};

pub use command::*;
use digest::{encode_auxiliary_state, initial_state_digest, transition_digest};
use model::history_ids_as_decimal;
pub use model::*;

#[derive(Debug, Clone)]
struct QueueTabletBusinessState {
    queue: Queue,
    consumer_epochs: BTreeMap<String, u64>,
    dead_letter_history: BTreeMap<u64, QueueTabletDeadLetterHistory>,
    active_dead_letters: BTreeMap<String, u64>,
    next_dead_letter_history_id: u64,
    redrive_history: BTreeMap<u64, QueueTabletRedriveHistory>,
    next_redrive_history_id: u64,
}

#[derive(Debug)]
pub struct QueueTablet {
    scope: QueueTabletScope,
    state: QueueTabletBusinessState,
    applied: BTreeMap<u64, AppliedCommand<QueueTabletReceipt>>,
    last_applied_command_index: u64,
    last_applied_time_ms: u64,
    state_digest: [u8; 32],
}

impl QueueTablet {
    pub fn new(scope: QueueTabletScope, mut config: QueueConfig) -> TabletResult<Self> {
        scope.validate()?;
        // Consensus provides persistence evidence. The embedded Queue is only
        // the deterministic ordering/lease engine and must not claim it.
        config.durability = DurabilityProfile::Volatile;
        let config_bytes = serde_json::to_vec(&config)
            .map_err(|error| TabletError::Encoding(error.to_string()))?;
        let queue = Queue::new(config)?;
        let state_digest = initial_state_digest(&scope, &config_bytes);
        Ok(Self {
            scope,
            state: QueueTabletBusinessState {
                queue,
                consumer_epochs: BTreeMap::new(),
                dead_letter_history: BTreeMap::new(),
                active_dead_letters: BTreeMap::new(),
                next_dead_letter_history_id: 0,
                redrive_history: BTreeMap::new(),
                next_redrive_history_id: 0,
            },
            applied: BTreeMap::new(),
            last_applied_command_index: 0,
            last_applied_time_ms: 0,
            state_digest,
        })
    }

    pub fn with_default_config(scope: QueueTabletScope) -> TabletResult<Self> {
        Self::new(scope, QueueConfig::default())
    }

    pub fn scope(&self) -> &QueueTabletScope {
        &self.scope
    }

    pub fn apply(&mut self, committed: CommittedCommand<'_>) -> TabletResult<QueueTabletReceipt> {
        validate_committed_command_scope(&self.scope, committed)?;
        let metadata = AppliedCommandMetadata::from_committed(committed);
        if let Some(mut receipt) = self.receipt_for_committed(committed)? {
            receipt.disposition = QueueTabletDisposition::Replayed;
            return Ok(receipt);
        }
        if committed.log_index <= self.last_applied_command_index {
            return Err(TabletError::CommitOrder {
                previous: self.last_applied_command_index,
                observed: committed.log_index,
            });
        }

        let command = QueueTabletCommand::decode(committed.payload, &self.scope)?;
        let expected_proposal_id = command.proposal_id(&self.scope)?;
        if committed.proposal_id != expected_proposal_id {
            return Err(TabletError::InvalidCommand(format!(
                "proposal_id {} does not match idempotency_key hash {expected_proposal_id}",
                committed.proposal_id
            )));
        }
        if command.applied_at_ms < self.last_applied_time_ms {
            return Err(TabletError::AppliedTimeRegression {
                previous: self.last_applied_time_ms,
                observed: command.applied_at_ms,
            });
        }

        let mut candidate = self.state.clone();
        let execution = candidate.execute(
            &self.scope,
            committed,
            command.operation,
            command.applied_at_ms,
        );
        let (outcome, next_state) = match execution {
            Ok(result) => (QueueTabletOutcome::Applied { result }, Some(candidate)),
            Err(error) => (recordable_rejected_outcome(error)?, None),
        };
        let receipt = QueueTabletReceipt {
            proposal_id: committed.proposal_id,
            tablet_id: self.scope.tablet_id,
            tablet_epoch: self.scope.tablet_epoch,
            term: committed.term,
            commit_index: committed.log_index,
            applied_at_ms: command.applied_at_ms,
            write_evidence: TabletWriteEvidence::FixedVoterMajorityPersisted,
            durable_voter_acks: 2,
            disposition: QueueTabletDisposition::New,
            outcome,
        };

        // Complete the fallible auxiliary encoding before swapping the cloned
        // business state, so an unexpected local failure remains atomic too.
        let effective_state = next_state.as_ref().unwrap_or(&self.state);
        let auxiliary_bytes = encode_auxiliary_state(effective_state, command.applied_at_ms)?;
        let next_digest = transition_digest(
            self.state_digest,
            committed,
            metadata.payload_digest,
            effective_state.queue.recovery_state_checksum(),
            &auxiliary_bytes,
            &receipt.outcome,
        );

        if let Some(next_state) = next_state {
            self.state = next_state;
        }
        self.state_digest = next_digest;
        self.last_applied_command_index = committed.log_index;
        self.last_applied_time_ms = command.applied_at_ms;
        self.applied.insert(
            committed.proposal_id,
            AppliedCommand {
                metadata,
                receipt: receipt.clone(),
            },
        );
        Ok(receipt)
    }

    pub fn lookup(&self, proposal_id: u64) -> Option<QueueTabletReceipt> {
        self.applied
            .get(&proposal_id)
            .map(|applied| applied.receipt.clone())
    }

    pub fn receipt_for_committed(
        &self,
        committed: CommittedCommand<'_>,
    ) -> TabletResult<Option<QueueTabletReceipt>> {
        validate_committed_command_scope(&self.scope, committed)?;
        let Some(previous) = self.applied.get(&committed.proposal_id) else {
            return Ok(None);
        };
        previous.metadata.validate_exact(committed)?;
        Ok(Some(previous.receipt.clone()))
    }

    pub fn counts(&self) -> QueueCounts {
        self.state.queue.counts()
    }

    pub fn consumer_epoch(&self, consumer: &str) -> Option<u64> {
        self.state.consumer_epochs.get(consumer).copied()
    }

    pub fn dead_letter_history(&self, limit: usize) -> Vec<QueueTabletDeadLetterHistory> {
        self.state
            .dead_letter_history
            .values()
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn active_dead_letter_history_id(&self, message_id: &str) -> Option<u64> {
        self.state.active_dead_letters.get(message_id).copied()
    }

    pub fn redrive_history(&self, limit: usize) -> Vec<QueueTabletRedriveHistory> {
        self.state
            .redrive_history
            .values()
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn last_applied_command_index(&self) -> u64 {
        self.last_applied_command_index
    }

    pub fn last_applied_time_ms(&self) -> u64 {
        self.last_applied_time_ms
    }

    pub fn applied_command_count(&self) -> usize {
        self.applied.len()
    }

    pub fn queue_recovery_state_checksum(&self) -> u32 {
        self.state.queue.recovery_state_checksum()
    }

    pub fn state_digest(&self) -> [u8; 32] {
        self.state_digest
    }
}

impl QueueTabletBusinessState {
    fn execute(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        operation: QueueTabletOperation,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let mut result = match operation {
            QueueTabletOperation::Enqueue(command) => {
                self.execute_enqueue(*command, applied_at_ms)?
            }
            QueueTabletOperation::Acquire(command) => {
                self.execute_acquire(scope, committed, &command, applied_at_ms)?
            }
            QueueTabletOperation::Acknowledge(command) => {
                self.execute_acknowledge(scope, committed, &command, applied_at_ms)?
            }
            QueueTabletOperation::ExtendLease(command) => {
                self.execute_extend(scope, committed, &command, applied_at_ms)?
            }
            QueueTabletOperation::Release(command) => {
                self.execute_release(scope, committed, command, applied_at_ms)?
            }
            QueueTabletOperation::Nack(command) => {
                self.execute_nack(scope, committed, command, applied_at_ms)?
            }
            QueueTabletOperation::Reject(command) => {
                self.execute_reject(scope, committed, command, applied_at_ms)?
            }
            QueueTabletOperation::Redrive(command) => {
                self.execute_redrive(committed, command, applied_at_ms)?
            }
            QueueTabletOperation::Maintain(_) => self.execute_maintain(applied_at_ms)?,
        };
        let new_history_ids = self.reconcile_dead_letter_history(committed)?;
        self.attach_dead_letter_evidence(&mut result, &new_history_ids)?;
        Ok(result)
    }

    fn execute_enqueue(
        &mut self,
        command: QueueEnqueueCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let receipt = self.queue.enqueue(command.envelope, applied_at_ms)?;
        Ok(QueueTabletOperationResult::Enqueued {
            message_id: receipt.message_id,
            duplicate: receipt.acknowledgement.duplicate,
        })
    }

    fn execute_acquire(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: &QueueAcquireCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        self.accept_consumer_epoch(&command.consumer, command.consumer_epoch)?;
        let fence = LeaseFence::new(
            scope.tablet_id,
            scope.tablet_epoch,
            command.partition,
            committed.term,
            command.consumer_epoch,
        )?;
        let deliveries = self
            .queue
            .acquire_fenced(
                &command.consumer,
                usize::from(command.max_messages),
                command.visibility_timeout_ms,
                applied_at_ms,
                fence,
            )?
            .into_iter()
            .map(|delivery| {
                let message = delivery.message;
                QueueTabletDelivery {
                    message_id: message.id,
                    envelope: message.envelope.into(),
                    attempt: message.attempt,
                    lease_token: delivery.lease_token,
                    lease_deadline_ms: delivery.lease_deadline_ms,
                }
            })
            .collect();
        Ok(QueueTabletOperationResult::Acquired {
            deliveries,
            new_dead_letter_history_ids: Vec::new(),
        })
    }

    fn execute_acknowledge(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: &QueueAcknowledgeCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let authorized = self.authorize_lease_command(
            scope,
            committed,
            &command.consumer,
            command.consumer_epoch,
            &command.lease_token,
        )?;
        self.queue
            .acknowledge_fenced(&command.lease_token, authorized.fence, applied_at_ms)?;
        Ok(QueueTabletOperationResult::Acknowledged {
            message_id: authorized.message_id,
        })
    }

    fn execute_extend(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: &QueueExtendLeaseCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let authorized = self.authorize_lease_command(
            scope,
            committed,
            &command.consumer,
            command.consumer_epoch,
            &command.lease_token,
        )?;
        let renewal = self.queue.extend_lease_fenced_bounded(
            &command.lease_token,
            authorized.fence,
            command.extension_ms,
            applied_at_ms,
        )?;
        Ok(QueueTabletOperationResult::LeaseExtended {
            message_id: authorized.message_id,
            lease_token: renewal.lease_token,
            lease_deadline_ms: renewal.lease_deadline_ms,
        })
    }

    fn execute_release(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: QueueReleaseCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let authorized = self.authorize_lease_command(
            scope,
            committed,
            &command.consumer,
            command.consumer_epoch,
            &command.lease_token,
        )?;
        self.queue.release_fenced(
            &command.lease_token,
            authorized.fence,
            command.delay_ms,
            command.reason,
            applied_at_ms,
        )?;
        Ok(QueueTabletOperationResult::Released {
            message_id: authorized.message_id,
            dead_letter_history_id: None,
        })
    }

    fn execute_nack(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: QueueNackCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let authorized = self.authorize_lease_command(
            scope,
            committed,
            &command.consumer,
            command.consumer_epoch,
            &command.lease_token,
        )?;
        self.queue.nack_fenced(
            &command.lease_token,
            authorized.fence,
            command.reason,
            applied_at_ms,
        )?;
        Ok(QueueTabletOperationResult::Nacked {
            message_id: authorized.message_id,
            dead_letter_history_id: None,
        })
    }

    fn execute_reject(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: QueueRejectCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let authorized = self.authorize_lease_command(
            scope,
            committed,
            &command.consumer,
            command.consumer_epoch,
            &command.lease_token,
        )?;
        self.queue.reject_fenced(
            &command.lease_token,
            authorized.fence,
            command.reason,
            applied_at_ms,
        )?;
        Ok(QueueTabletOperationResult::DeadLettered {
            message_id: authorized.message_id,
            dead_letter_history_id: 0,
        })
    }

    fn execute_redrive(
        &mut self,
        committed: CommittedCommand<'_>,
        command: QueueRedriveCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        match self.active_dead_letters.get(&command.message_id) {
            Some(history_id) if *history_id == command.dead_letter_history_id => {}
            Some(_) => return Err(EpochError::Fenced),
            None => return Err(EpochError::NotFound(command.message_id)),
        }
        self.queue.redrive(&command.message_id, applied_at_ms)?;
        let redrive_history_id = self.append_redrive_history(
            &command.message_id,
            command.dead_letter_history_id,
            committed,
            applied_at_ms,
        )?;
        Ok(QueueTabletOperationResult::Redriven {
            message_id: command.message_id,
            dead_letter_history_id: command.dead_letter_history_id,
            redrive_history_id,
        })
    }

    fn execute_maintain(
        &mut self,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        self.queue.maintain_fenced(applied_at_ms)?;
        Ok(QueueTabletOperationResult::Maintained {
            counts: self.queue.counts().try_into()?,
            new_dead_letter_history_ids: Vec::new(),
        })
    }

    fn attach_dead_letter_evidence(
        &self,
        result: &mut QueueTabletOperationResult,
        new_history_ids: &[u64],
    ) -> Result<(), EpochError> {
        match result {
            QueueTabletOperationResult::Acquired {
                new_dead_letter_history_ids,
                ..
            }
            | QueueTabletOperationResult::Maintained {
                new_dead_letter_history_ids,
                ..
            } => {
                *new_dead_letter_history_ids = history_ids_as_decimal(new_history_ids);
            }
            QueueTabletOperationResult::Released {
                message_id,
                dead_letter_history_id,
            }
            | QueueTabletOperationResult::Nacked {
                message_id,
                dead_letter_history_id,
            } => {
                *dead_letter_history_id = self.active_dead_letters.get(message_id).copied();
            }
            QueueTabletOperationResult::DeadLettered {
                message_id,
                dead_letter_history_id,
            } => {
                *dead_letter_history_id = self
                    .active_dead_letters
                    .get(message_id)
                    .copied()
                    .ok_or_else(|| {
                        EpochError::Internal(
                            "successful reject did not produce dead-letter evidence".into(),
                        )
                    })?;
            }
            _ => {}
        }
        Ok(())
    }

    fn accept_consumer_epoch(
        &mut self,
        consumer: &str,
        requested_epoch: u64,
    ) -> Result<(), EpochError> {
        match self.consumer_epochs.get(consumer).copied() {
            Some(current) if requested_epoch < current => Err(EpochError::Fenced),
            Some(current) if requested_epoch == current => Ok(()),
            _ => {
                self.consumer_epochs
                    .insert(consumer.to_owned(), requested_epoch);
                Ok(())
            }
        }
    }

    fn authorize_lease_command(
        &self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        consumer: &str,
        consumer_epoch: u64,
        token: &str,
    ) -> Result<AuthorizedLease, EpochError> {
        if self.consumer_epochs.get(consumer).copied() != Some(consumer_epoch) {
            return Err(EpochError::Fenced);
        }
        let metadata = FencedLeaseTokenMetadata::parse(token).map_err(|_| EpochError::Fenced)?;
        if metadata.consumer() != consumer {
            return Err(EpochError::Fenced);
        }
        let expected = LeaseFence::new(
            scope.tablet_id,
            scope.tablet_epoch,
            0,
            committed.term,
            consumer_epoch,
        )?;
        if metadata.fence() != expected {
            return Err(EpochError::Fenced);
        }
        Ok(AuthorizedLease {
            fence: expected,
            message_id: metadata.message_id().to_owned(),
        })
    }

    fn reconcile_dead_letter_history(
        &mut self,
        committed: CommittedCommand<'_>,
    ) -> Result<Vec<u64>, EpochError> {
        let current = self.queue.dead_letters(usize::MAX);
        let current_message_ids: BTreeSet<_> = current
            .iter()
            .map(|dead_letter| dead_letter.message_id.clone())
            .collect();
        self.active_dead_letters
            .retain(|message_id, _| current_message_ids.contains(message_id));

        let mut appended = Vec::new();
        for dead_letter in current {
            if self
                .active_dead_letters
                .contains_key(&dead_letter.message_id)
            {
                continue;
            }
            let history_id = self
                .next_dead_letter_history_id
                .checked_add(1)
                .ok_or_else(|| {
                    EpochError::Capacity("dead-letter history id is exhausted".into())
                })?;
            self.next_dead_letter_history_id = history_id;
            self.active_dead_letters
                .insert(dead_letter.message_id.clone(), history_id);
            self.dead_letter_history.insert(
                history_id,
                QueueTabletDeadLetterHistory {
                    history_id,
                    recorded_term: committed.term,
                    recorded_commit_index: committed.log_index,
                    source_proposal_id: committed.proposal_id,
                    dead_letter: dead_letter.into(),
                },
            );
            appended.push(history_id);
        }
        Ok(appended)
    }

    fn append_redrive_history(
        &mut self,
        message_id: &str,
        dead_letter_history_id: u64,
        committed: CommittedCommand<'_>,
        applied_at_ms: u64,
    ) -> Result<u64, EpochError> {
        let history_id = self
            .next_redrive_history_id
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("redrive history id is exhausted".into()))?;
        self.next_redrive_history_id = history_id;
        self.redrive_history.insert(
            history_id,
            QueueTabletRedriveHistory {
                history_id,
                dead_letter_history_id,
                message_id: message_id.to_owned(),
                source_proposal_id: committed.proposal_id,
                recorded_term: committed.term,
                recorded_commit_index: committed.log_index,
                redriven_at_ms: applied_at_ms,
            },
        );
        Ok(history_id)
    }
}

#[derive(Debug)]
struct AuthorizedLease {
    fence: LeaseFence,
    message_id: String,
}

fn recordable_rejected_outcome(error: EpochError) -> TabletResult<QueueTabletOutcome> {
    let code = match &error {
        EpochError::AlreadyExists(_) => QueueTabletRejectionCode::AlreadyExists,
        EpochError::NotFound(_) => QueueTabletRejectionCode::NotFound,
        EpochError::InvalidArgument(_) => QueueTabletRejectionCode::InvalidArgument,
        EpochError::Conflict(_) => QueueTabletRejectionCode::Conflict,
        EpochError::Fenced => QueueTabletRejectionCode::Fenced,
        EpochError::Capacity(_) => QueueTabletRejectionCode::Capacity,
        EpochError::Unavailable(_) => QueueTabletRejectionCode::Unavailable,
        EpochError::Storage(_) | EpochError::Internal(_) => {
            return Err(TabletError::Profile(error));
        }
    };
    Ok(QueueTabletOutcome::Rejected {
        code,
        detail: error.to_string(),
    })
}

#[cfg(test)]
mod tests;
