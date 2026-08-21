//! Canonical replicated Queue tablet state machine.

mod command;
mod digest;
mod model;

use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use epoch_core::{DurabilityProfile, EpochError};
use epoch_queue::{
    FencedLeaseTokenMetadata, LeaseFence, Queue, QueueConfig, QueueCounts, QueueIngress,
};
use serde::{Deserialize, Serialize};

use crate::common::{AppliedCommand, validate_committed_command_scope};
use crate::{
    AppliedCommandMetadata, CommittedCommand, TabletError, TabletResult, TabletWriteEvidence,
};

pub use command::*;
use digest::{encode_auxiliary_state, initial_state_digest, transition_digest};
use model::history_ids_as_decimal;
pub use model::*;

const LEGACY_QUEUE_TABLET_SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub const QUEUE_TABLET_SNAPSHOT_FORMAT_VERSION: u16 = 2;
pub const MAX_QUEUE_TABLET_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
struct QueueTabletBusinessState {
    queue: Queue,
    consumer_epochs: BTreeMap<String, u64>,
    dead_letter_history: BTreeMap<u64, QueueTabletDeadLetterHistory>,
    active_dead_letters: BTreeMap<String, u64>,
    next_dead_letter_history_id: u64,
    redrive_history: BTreeMap<u64, QueueTabletRedriveHistory>,
    next_redrive_history_id: u64,
    dead_letter_forwards: BTreeMap<u64, QueueTabletDeadLetterForward>,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedQueueTabletSnapshot {
    format_version: u16,
    scope: QueueTabletScope,
    queue_base64: String,
    consumer_epochs: BTreeMap<String, u64>,
    dead_letter_history: BTreeMap<u64, QueueTabletDeadLetterHistory>,
    active_dead_letters: BTreeMap<String, u64>,
    next_dead_letter_history_id: u64,
    redrive_history: BTreeMap<u64, QueueTabletRedriveHistory>,
    next_redrive_history_id: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    dead_letter_forwards: BTreeMap<u64, QueueTabletDeadLetterForward>,
    applied: Vec<QueueTabletAppliedSnapshot>,
    last_applied_command_index: u64,
    last_applied_time_ms: u64,
    state_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueTabletAppliedSnapshot {
    proposal_id: u64,
    applied: AppliedCommand<QueueTabletReceipt>,
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
                dead_letter_forwards: BTreeMap::new(),
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
        // The committed log, rather than any one leader's wall clock, is the
        // authoritative time order. An earlier uncommitted entry can survive a
        // leader change and precede a command assigned by a lower-clock leader.
        // Clamp at application so every voter and recovery replay derives the
        // same non-regressing effective time from the same committed prefix.
        let applied_at_ms = command.applied_at_ms.max(self.last_applied_time_ms);

        let mut candidate = self.state.clone();
        let execution = candidate.execute(&self.scope, committed, command.operation, applied_at_ms);
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
            applied_at_ms,
            write_evidence: TabletWriteEvidence::FixedVoterMajorityPersisted,
            durable_voter_acks: 2,
            disposition: QueueTabletDisposition::New,
            outcome,
        };

        // Complete the fallible auxiliary encoding before swapping the cloned
        // business state, so an unexpected local failure remains atomic too.
        let effective_state = next_state.as_ref().unwrap_or(&self.state);
        let auxiliary_bytes = encode_auxiliary_state(effective_state, applied_at_ms)?;
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

    pub fn queue_config(&self) -> &QueueConfig {
        self.state.queue.config()
    }

    pub fn next_maintenance_deadline_ms(&self) -> Option<u64> {
        self.state
            .queue
            .next_maintenance_deadline_with_pending_forwards(
                self.state.pending_dead_letter_forward_count(),
            )
    }

    pub fn consumer_epoch(&self, consumer: &str) -> Option<u64> {
        self.state.consumer_epochs.get(consumer).copied()
    }

    pub fn consumer_in_flight(&self, consumer: &str) -> usize {
        self.state.queue.in_flight_for_consumer(consumer)
    }

    pub fn consumer_flow(&self, consumer: &str) -> TabletResult<QueueTabletConsumerFlow> {
        command::validate_consumer_name(consumer)?;
        let in_flight = u64::try_from(self.consumer_in_flight(consumer))
            .map_err(|_| TabletError::Encoding("consumer in-flight count exceeds u64".into()))?;
        Ok(QueueTabletConsumerFlow {
            consumer: consumer.to_owned(),
            consumer_epoch: self.consumer_epoch(consumer),
            in_flight,
        })
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

    pub fn advanced_status(&self) -> QueueTabletAdvancedStatus {
        QueueTabletAdvancedStatus {
            state: self.state.queue.advanced_observation(),
            pending_dead_letter_forwards: self
                .state
                .dead_letter_forwards
                .values()
                .filter(|forward| forward.status != QueueTabletDeadLetterForwardStatus::Completed)
                .count(),
        }
    }

    pub fn correlation(&self, correlation_id: &str) -> Vec<epoch_queue::QueueCorrelatedMessage> {
        self.state.queue.lookup_correlation(correlation_id)
    }

    pub fn pending_dead_letter_forwards(&self, limit: usize) -> Vec<QueueTabletDeadLetterForward> {
        self.state
            .dead_letter_forwards
            .values()
            .filter(|forward| forward.status != QueueTabletDeadLetterForwardStatus::Completed)
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

    pub fn encode_snapshot(&self, retained: &BTreeSet<u64>) -> TabletResult<Vec<u8>> {
        let mut applied = self
            .applied
            .iter()
            .filter(|(proposal_id, _)| retained.contains(proposal_id))
            .map(|(proposal_id, applied)| QueueTabletAppliedSnapshot {
                proposal_id: *proposal_id,
                applied: applied.clone(),
            })
            .collect::<Vec<_>>();
        if applied.len() != retained.len() {
            return Err(TabletError::InvalidCommand(
                "Queue snapshot retry set contains an unknown proposal".into(),
            ));
        }
        applied.sort_by_key(|entry| entry.applied.metadata.log_index);
        let queue = self
            .state
            .queue
            .encode_snapshot()
            .map_err(TabletError::Profile)?;
        let encoded = serde_json::to_vec(&VersionedQueueTabletSnapshot {
            format_version: QUEUE_TABLET_SNAPSHOT_FORMAT_VERSION,
            scope: self.scope.clone(),
            queue_base64: STANDARD_NO_PAD.encode(queue),
            consumer_epochs: self.state.consumer_epochs.clone(),
            dead_letter_history: self.state.dead_letter_history.clone(),
            active_dead_letters: self.state.active_dead_letters.clone(),
            next_dead_letter_history_id: self.state.next_dead_letter_history_id,
            redrive_history: self.state.redrive_history.clone(),
            next_redrive_history_id: self.state.next_redrive_history_id,
            dead_letter_forwards: self.state.dead_letter_forwards.clone(),
            applied,
            last_applied_command_index: self.last_applied_command_index,
            last_applied_time_ms: self.last_applied_time_ms,
            state_digest: self.state_digest,
        })
        .map_err(|error| TabletError::Encoding(error.to_string()))?;
        if encoded.len() > MAX_QUEUE_TABLET_SNAPSHOT_BYTES {
            return Err(TabletError::InvalidCommand(format!(
                "Queue tablet snapshot is {} bytes; maximum is {MAX_QUEUE_TABLET_SNAPSHOT_BYTES}",
                encoded.len()
            )));
        }
        Ok(encoded)
    }

    pub fn decode_snapshot(
        expected_scope: &QueueTabletScope,
        encoded: &[u8],
    ) -> TabletResult<Self> {
        if encoded.len() > MAX_QUEUE_TABLET_SNAPSHOT_BYTES {
            return Err(TabletError::InvalidCommand(format!(
                "Queue tablet snapshot is {} bytes; maximum is {MAX_QUEUE_TABLET_SNAPSHOT_BYTES}",
                encoded.len()
            )));
        }
        let snapshot: VersionedQueueTabletSnapshot = serde_json::from_slice(encoded)
            .map_err(|error| TabletError::Decoding(error.to_string()))?;
        if ![
            LEGACY_QUEUE_TABLET_SNAPSHOT_FORMAT_VERSION,
            QUEUE_TABLET_SNAPSHOT_FORMAT_VERSION,
        ]
        .contains(&snapshot.format_version)
        {
            return Err(TabletError::InvalidCommand(format!(
                "unsupported Queue tablet snapshot version {}",
                snapshot.format_version
            )));
        }
        if &snapshot.scope != expected_scope {
            return Err(TabletError::InvalidCommand(
                "Queue tablet snapshot scope is fenced".into(),
            ));
        }
        snapshot.scope.validate()?;
        if serde_json::to_vec(&snapshot)
            .map_err(|error| TabletError::Encoding(error.to_string()))?
            != encoded
        {
            return Err(TabletError::InvalidCommand(
                "Queue tablet snapshot is not canonical".into(),
            ));
        }

        let queue_bytes = STANDARD_NO_PAD
            .decode(&snapshot.queue_base64)
            .map_err(|error| TabletError::Decoding(error.to_string()))?;
        let queue = Queue::decode_snapshot(&queue_bytes).map_err(TabletError::Profile)?;
        if queue.config().durability != DurabilityProfile::Volatile {
            return Err(TabletError::InvalidCommand(
                "Queue tablet snapshot engine durability is invalid".into(),
            ));
        }
        validate_queue_snapshot_auxiliary(&queue, &snapshot)?;

        let mut applied = BTreeMap::new();
        let mut previous_index = 0_u64;
        for entry in snapshot.applied {
            let metadata = entry.applied.metadata;
            let receipt = &entry.applied.receipt;
            if entry.proposal_id == 0
                || metadata.proposal_id != entry.proposal_id
                || metadata.term == 0
                || metadata.log_index <= previous_index
                || metadata.log_index > snapshot.last_applied_command_index
                || receipt.proposal_id != entry.proposal_id
                || receipt.tablet_id != expected_scope.tablet_id
                || receipt.tablet_epoch != expected_scope.tablet_epoch
                || receipt.term != metadata.term
                || receipt.commit_index != metadata.log_index
                || receipt.applied_at_ms > snapshot.last_applied_time_ms
                || receipt.write_evidence != TabletWriteEvidence::FixedVoterMajorityPersisted
                || receipt.durable_voter_acks != 2
                || applied.insert(entry.proposal_id, entry.applied).is_some()
            {
                return Err(TabletError::InvalidCommand(
                    "Queue tablet snapshot retry registry is invalid".into(),
                ));
            }
            previous_index = metadata.log_index;
        }
        if snapshot.last_applied_command_index == 0
            && (snapshot.last_applied_time_ms != 0 || !applied.is_empty())
        {
            return Err(TabletError::InvalidCommand(
                "Queue tablet snapshot application position is invalid".into(),
            ));
        }

        Ok(Self {
            scope: snapshot.scope,
            state: QueueTabletBusinessState {
                queue,
                consumer_epochs: snapshot.consumer_epochs,
                dead_letter_history: snapshot.dead_letter_history,
                active_dead_letters: snapshot.active_dead_letters,
                next_dead_letter_history_id: snapshot.next_dead_letter_history_id,
                redrive_history: snapshot.redrive_history,
                next_redrive_history_id: snapshot.next_redrive_history_id,
                dead_letter_forwards: snapshot.dead_letter_forwards,
            },
            applied,
            last_applied_command_index: snapshot.last_applied_command_index,
            last_applied_time_ms: snapshot.last_applied_time_ms,
            state_digest: snapshot.state_digest,
        })
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "snapshot restoration exhaustively cross-validates all Queue auxiliary registries"
)]
fn validate_queue_snapshot_auxiliary(
    queue: &Queue,
    snapshot: &VersionedQueueTabletSnapshot,
) -> TabletResult<()> {
    for (consumer, epoch) in &snapshot.consumer_epochs {
        command::validate_consumer_name(consumer)?;
        if *epoch == 0 {
            return Err(TabletError::InvalidCommand(
                "Queue tablet snapshot has a zero consumer epoch".into(),
            ));
        }
    }

    if u64::try_from(snapshot.dead_letter_history.len()).ok()
        != Some(snapshot.next_dead_letter_history_id)
        || snapshot.dead_letter_history.iter().enumerate().any(
            |(position, (history_id, record))| {
                u64::try_from(position + 1).ok() != Some(*history_id)
                    || record.history_id != *history_id
                    || record.recorded_term == 0
                    || record.recorded_commit_index == 0
                    || record.source_proposal_id == 0
                    || record.dead_letter.message_id.trim().is_empty()
                    || record.dead_letter.reason.trim().is_empty()
            },
        )
    {
        return Err(TabletError::InvalidCommand(
            "Queue tablet snapshot dead-letter history is invalid".into(),
        ));
    }
    let active_letters = queue.dead_letters(usize::MAX);
    if active_letters.len() != snapshot.active_dead_letters.len() {
        return Err(TabletError::InvalidCommand(
            "Queue tablet snapshot active dead-letter registry is invalid".into(),
        ));
    }
    for letter in active_letters {
        let Some(history_id) = snapshot.active_dead_letters.get(&letter.message_id) else {
            return Err(TabletError::InvalidCommand(
                "Queue tablet snapshot active dead-letter registry is invalid".into(),
            ));
        };
        let Some(history) = snapshot.dead_letter_history.get(history_id) else {
            return Err(TabletError::InvalidCommand(
                "Queue tablet snapshot active dead-letter registry is invalid".into(),
            ));
        };
        if history.dead_letter != QueueTabletDeadLetter::from(letter) {
            return Err(TabletError::InvalidCommand(
                "Queue tablet snapshot active dead-letter evidence disagrees with the engine"
                    .into(),
            ));
        }
    }

    if u64::try_from(snapshot.redrive_history.len()).ok() != Some(snapshot.next_redrive_history_id)
        || snapshot
            .redrive_history
            .iter()
            .enumerate()
            .any(|(position, (history_id, record))| {
                u64::try_from(position + 1).ok() != Some(*history_id)
                    || record.history_id != *history_id
                    || record.dead_letter_history_id == 0
                    || !snapshot
                        .dead_letter_history
                        .contains_key(&record.dead_letter_history_id)
                    || record.message_id.trim().is_empty()
                    || record.source_proposal_id == 0
                    || record.recorded_term == 0
                    || record.recorded_commit_index == 0
            })
    {
        return Err(TabletError::InvalidCommand(
            "Queue tablet snapshot redrive history is invalid".into(),
        ));
    }
    let configured_forward_target = queue
        .config()
        .advanced
        .as_ref()
        .and_then(|config| config.dead_letter_target.as_deref());
    for (history_id, forward) in &snapshot.dead_letter_forwards {
        let Some(history) = snapshot.dead_letter_history.get(history_id) else {
            return Err(TabletError::InvalidCommand(
                "Queue tablet snapshot dead-letter forward history is invalid".into(),
            ));
        };
        if forward.dead_letter_history_id != *history_id
            || configured_forward_target != Some(forward.target.as_str())
            || forward.envelope != history.dead_letter.envelope
        {
            return Err(TabletError::InvalidCommand(
                "Queue tablet snapshot dead-letter forward history is invalid".into(),
            ));
        }
        match forward.status {
            QueueTabletDeadLetterForwardStatus::Pending
                if forward.destination.is_none() && forward.target_message_id.is_none() => {}
            QueueTabletDeadLetterForwardStatus::Bound
                if forward.destination.is_some() && forward.target_message_id.is_none() => {}
            QueueTabletDeadLetterForwardStatus::Completed
                if forward.destination.is_some() && forward.target_message_id.is_some() => {}
            _ => {
                return Err(TabletError::InvalidCommand(
                    "Queue tablet snapshot dead-letter forward state is invalid".into(),
                ));
            }
        }
        if let Some(destination) = &forward.destination {
            destination.validate()?;
            if destination.kind != epoch_bus::EpochTargetKind::Queue
                || destination.resource != forward.target
                || destination.shard_index != 0
            {
                return Err(TabletError::InvalidCommand(
                    "Queue tablet snapshot dead-letter destination is invalid".into(),
                ));
            }
        }
    }
    Ok(())
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
            QueueTabletOperation::AcquireWithCredit(command) => {
                self.execute_acquire_with_credit(scope, committed, &command, applied_at_ms)?
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
            QueueTabletOperation::EnqueueAdvanced(command) => {
                self.execute_enqueue_advanced(*command, applied_at_ms)?
            }
            QueueTabletOperation::AcquireSession(command) => {
                self.execute_acquire_session(scope, committed, &command, applied_at_ms)?
            }
            QueueTabletOperation::RenewSessionLock(command) => {
                self.execute_renew_session_lock(scope, committed, &command, applied_at_ms)?
            }
            QueueTabletOperation::ReleaseSessionLock(command) => {
                self.execute_release_session_lock(scope, committed, &command, applied_at_ms)?
            }
            QueueTabletOperation::Defer(command) => {
                self.execute_defer(scope, committed, command, applied_at_ms)?
            }
            QueueTabletOperation::ReceiveDeferred(command) => {
                self.execute_receive_deferred(scope, committed, &command, applied_at_ms)?
            }
            QueueTabletOperation::BindDeadLetterForward(command) => {
                self.execute_bind_dead_letter_forward(command)?
            }
            QueueTabletOperation::CompleteDeadLetterForward(command) => {
                self.execute_complete_dead_letter_forward(command)?
            }
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
        let receipt = if self.queue.config().advanced.is_some() {
            self.queue.enqueue_advanced_with_pending_forwards(
                QueueIngress::new(command.envelope),
                applied_at_ms,
                self.pending_dead_letter_forward_count(),
            )?
        } else {
            self.queue.enqueue(command.envelope, applied_at_ms)?
        };
        Ok(QueueTabletOperationResult::Enqueued {
            message_id: receipt.message_id,
            duplicate: receipt.acknowledgement.duplicate,
        })
    }

    fn execute_enqueue_advanced(
        &mut self,
        command: QueueAdvancedEnqueueCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let receipt = self.queue.enqueue_advanced_with_pending_forwards(
            command.ingress,
            applied_at_ms,
            self.pending_dead_letter_forward_count(),
        )?;
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
            .acquire_advanced_with_pending_forwards(
                &command.consumer,
                usize::from(command.max_messages),
                command.visibility_timeout_ms,
                applied_at_ms,
                fence,
                self.pending_dead_letter_forward_count(),
            )?
            .into_iter()
            .map(|delivery| tablet_delivery(&self.queue, delivery))
            .collect();
        Ok(QueueTabletOperationResult::Acquired {
            deliveries,
            flow_control: None,
            new_dead_letter_history_ids: Vec::new(),
        })
    }

    fn execute_acquire_with_credit(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: &QueueCreditAcquireCommand,
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

        // The concurrency observation and acquisition must share one cloned
        // state-machine transition. Maintenance first removes leases whose
        // committed deadline has elapsed; the subsequent acquire repeats the
        // idempotent maintenance pass before creating new leases.
        self.queue
            .maintain_advanced(applied_at_ms, self.pending_dead_letter_forward_count())?;
        let in_flight_before = self.queue.in_flight_for_consumer(&command.consumer);
        let available = usize::from(command.max_in_flight).saturating_sub(in_flight_before);
        let granted = usize::from(command.credit).min(available);
        let deliveries = self
            .queue
            .acquire_advanced_with_pending_forwards(
                &command.consumer,
                granted,
                command.visibility_timeout_ms,
                applied_at_ms,
                fence,
                self.pending_dead_letter_forward_count(),
            )?
            .into_iter()
            .map(|delivery| tablet_delivery(&self.queue, delivery))
            .collect();
        let in_flight_after = self.queue.in_flight_for_consumer(&command.consumer);
        let remaining_capacity = usize::from(command.max_in_flight).saturating_sub(in_flight_after);
        let convert_count = |value: usize| {
            u64::try_from(value)
                .map_err(|_| EpochError::Internal("consumer in-flight count exceeds u64".into()))
        };
        Ok(QueueTabletOperationResult::Acquired {
            deliveries,
            flow_control: Some(QueueTabletFlowControl {
                requested_credit: command.credit,
                max_in_flight: command.max_in_flight,
                in_flight_before: convert_count(in_flight_before)?,
                in_flight_after: convert_count(in_flight_after)?,
                remaining_capacity: convert_count(remaining_capacity)?,
            }),
            new_dead_letter_history_ids: Vec::new(),
        })
    }

    fn execute_acquire_session(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: &QueueSessionAcquireCommand,
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
        self.queue
            .maintain_advanced(applied_at_ms, self.pending_dead_letter_forward_count())?;
        let in_flight_before = self.queue.in_flight_for_consumer(&command.consumer);
        let available = usize::from(command.max_in_flight).saturating_sub(in_flight_before);
        let granted = usize::from(command.credit).min(available);
        let acquired = self.queue.acquire_session_fenced_with_pending_forwards(
            &command.session_id,
            &command.consumer,
            granted,
            command.visibility_timeout_ms,
            command.session_lock_token.as_deref(),
            applied_at_ms,
            fence,
            self.pending_dead_letter_forward_count(),
        )?;
        let deliveries = acquired
            .deliveries
            .into_iter()
            .map(|delivery| tablet_delivery(&self.queue, delivery))
            .collect();
        let in_flight_after = self.queue.in_flight_for_consumer(&command.consumer);
        let remaining_capacity = usize::from(command.max_in_flight).saturating_sub(in_flight_after);
        Ok(QueueTabletOperationResult::SessionAcquired {
            session_id: acquired.session_id,
            session_lock_token: acquired.lock_token,
            session_lock_deadline_ms: acquired.lock_deadline_ms,
            deliveries,
            flow_control: QueueTabletFlowControl {
                requested_credit: command.credit,
                max_in_flight: command.max_in_flight,
                in_flight_before: queue_count_u64(in_flight_before)?,
                in_flight_after: queue_count_u64(in_flight_after)?,
                remaining_capacity: queue_count_u64(remaining_capacity)?,
            },
        })
    }

    fn execute_renew_session_lock(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: &QueueSessionLockRenewCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        self.authorize_consumer_epoch(&command.consumer, command.consumer_epoch)?;
        let fence = LeaseFence::new(
            scope.tablet_id,
            scope.tablet_epoch,
            command.partition,
            committed.term,
            command.consumer_epoch,
        )?;
        let renewal = self.queue.renew_session_lock_fenced_with_pending_forwards(
            &command.session_lock_token,
            command.extension_ms,
            applied_at_ms,
            fence,
            self.pending_dead_letter_forward_count(),
        )?;
        Ok(QueueTabletOperationResult::SessionLockRenewed {
            session_id: renewal.session_id,
            session_lock_token: renewal.lock_token,
            session_lock_deadline_ms: renewal.lock_deadline_ms,
        })
    }

    fn execute_release_session_lock(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: &QueueSessionLockReleaseCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        self.authorize_consumer_epoch(&command.consumer, command.consumer_epoch)?;
        let fence = LeaseFence::new(
            scope.tablet_id,
            scope.tablet_epoch,
            command.partition,
            committed.term,
            command.consumer_epoch,
        )?;
        self.queue
            .release_session_lock_fenced_with_pending_forwards(
                &command.session_lock_token,
                fence,
                applied_at_ms,
                self.pending_dead_letter_forward_count(),
            )?;
        Ok(QueueTabletOperationResult::SessionLockReleased)
    }

    fn execute_defer(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: QueueDeferCommand,
        applied_at_ms: u64,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let authorized = self.authorize_lease_command(
            scope,
            committed,
            &command.consumer,
            command.consumer_epoch,
            &command.lease_token,
        )?;
        let message_id = self.queue.defer_fenced_with_pending_forwards(
            &command.lease_token,
            authorized.fence,
            command.reason,
            applied_at_ms,
            self.pending_dead_letter_forward_count(),
        )?;
        Ok(QueueTabletOperationResult::Deferred { message_id })
    }

    fn execute_receive_deferred(
        &mut self,
        scope: &QueueTabletScope,
        committed: CommittedCommand<'_>,
        command: &QueueReceiveDeferredCommand,
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
        let delivery = self.queue.receive_deferred_fenced_with_pending_forwards(
            &command.message_id,
            &command.consumer,
            command.visibility_timeout_ms,
            applied_at_ms,
            fence,
            self.pending_dead_letter_forward_count(),
        )?;
        Ok(QueueTabletOperationResult::DeferredReceived {
            delivery: Box::new(tablet_delivery(&self.queue, delivery)),
        })
    }

    fn execute_bind_dead_letter_forward(
        &mut self,
        command: QueueBindDeadLetterForwardCommand,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let forward = self
            .dead_letter_forwards
            .get_mut(&command.dead_letter_history_id)
            .ok_or_else(|| EpochError::NotFound(command.dead_letter_history_id.to_string()))?;
        if forward.target != command.destination.resource {
            return Err(EpochError::Conflict(
                "dead-letter destination does not match the configured target".into(),
            ));
        }
        match forward.status {
            QueueTabletDeadLetterForwardStatus::Pending => {
                forward.destination = Some(command.destination.clone());
                forward.status = QueueTabletDeadLetterForwardStatus::Bound;
            }
            QueueTabletDeadLetterForwardStatus::Bound
                if forward.destination.as_ref() == Some(&command.destination) => {}
            _ => {
                return Err(EpochError::Conflict(
                    "dead-letter forward is already bound or completed differently".into(),
                ));
            }
        }
        Ok(QueueTabletOperationResult::DeadLetterForwardBound {
            dead_letter_history_id: command.dead_letter_history_id,
            destination: command.destination,
        })
    }

    fn execute_complete_dead_letter_forward(
        &mut self,
        command: QueueCompleteDeadLetterForwardCommand,
    ) -> Result<QueueTabletOperationResult, EpochError> {
        let forward = self
            .dead_letter_forwards
            .get_mut(&command.dead_letter_history_id)
            .ok_or_else(|| EpochError::NotFound(command.dead_letter_history_id.to_string()))?;
        match forward.status {
            QueueTabletDeadLetterForwardStatus::Bound
                if forward.destination.as_ref() == Some(&command.destination) =>
            {
                forward.target_message_id = Some(command.target_message_id.clone());
                forward.status = QueueTabletDeadLetterForwardStatus::Completed;
            }
            QueueTabletDeadLetterForwardStatus::Completed
                if forward.destination.as_ref() == Some(&command.destination)
                    && forward.target_message_id.as_deref()
                        == Some(command.target_message_id.as_str()) => {}
            _ => {
                return Err(EpochError::Fenced);
            }
        }
        Ok(QueueTabletOperationResult::DeadLetterForwardCompleted {
            dead_letter_history_id: command.dead_letter_history_id,
            target_message_id: command.target_message_id,
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
        self.queue.record_dispatch_success(applied_at_ms)?;
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
        self.queue.record_dispatch_failure(applied_at_ms)?;
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
        self.queue.record_dispatch_failure(applied_at_ms)?;
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
        self.queue
            .maintain_advanced(applied_at_ms, self.pending_dead_letter_forward_count())?;
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

    fn authorize_consumer_epoch(
        &self,
        consumer: &str,
        requested_epoch: u64,
    ) -> Result<(), EpochError> {
        if self.consumer_epochs.get(consumer).copied() == Some(requested_epoch) {
            Ok(())
        } else {
            Err(EpochError::Fenced)
        }
    }

    fn pending_dead_letter_forward_count(&self) -> usize {
        self.dead_letter_forwards
            .values()
            .filter(|forward| forward.status != QueueTabletDeadLetterForwardStatus::Completed)
            .count()
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
        let forward_target = self
            .queue
            .config()
            .advanced
            .as_ref()
            .and_then(|config| config.dead_letter_target.clone());
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
                    dead_letter: dead_letter.clone().into(),
                },
            );
            if let Some(target) = &forward_target {
                self.dead_letter_forwards.insert(
                    history_id,
                    QueueTabletDeadLetterForward {
                        dead_letter_history_id: history_id,
                        target: target.clone(),
                        envelope: dead_letter.envelope.into(),
                        status: QueueTabletDeadLetterForwardStatus::Pending,
                        destination: None,
                        target_message_id: None,
                    },
                );
            }
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

fn tablet_delivery(queue: &Queue, delivery: epoch_queue::Delivery) -> QueueTabletDelivery {
    let metadata = queue.message_metadata(&delivery.message.id);
    let message = delivery.message;
    QueueTabletDelivery {
        message_id: message.id,
        envelope: message.envelope.into(),
        attempt: message.attempt,
        lease_token: delivery.lease_token,
        lease_deadline_ms: delivery.lease_deadline_ms,
        metadata,
    }
}

fn queue_count_u64(value: usize) -> Result<u64, EpochError> {
    u64::try_from(value)
        .map_err(|_| EpochError::Internal("consumer in-flight count exceeds u64".into()))
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
