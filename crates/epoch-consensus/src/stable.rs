//! Disk-backed stable-state journal for the bounded-voter consensus adapter.
//!
//! Record zero fixes the immutable group identity. Later records contain either
//! a complete `HardState` plus application checkpoint and normal-entry batch,
//! or an additive canonical consensus checkpoint plus its contiguous retained
//! tail. A compacted baseline can atomically replace obsolete generations. The
//! outer [`FileWal`] supplies the durable, checksummed append/replacement
//! boundary; this module supplies canonical Epoch-owned framing, logical suffix
//! replacement, checkpoint installation, and prefix compaction during replay.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    mem::size_of,
    path::Path,
};

use epoch_storage::{CommitLog, FileWal, LogRecord};
use prost::Message as _;
use raft::prelude::{ConfState, Entry, EntryType, HardState};

use super::{
    CheckpointImage, CommitReceipt, CommittedProposal, ConsensusError, ConsensusResult,
    EpochRaftStorage, GroupEpoch, GroupId, LogIndex, NodeId, ProposalId, StateDigest, Term,
    checkpoint_snapshot, compute_state_digest, decode_checkpoint_image, decode_command,
    decode_membership_change, encode_checkpoint_image, expected_conf_state, membership,
    validate_checkpoint_image, validate_command_scope, validate_hard_state,
    validate_initial_membership, validate_log_order,
};

const RECORD_MAGIC: [u8; 4] = *b"EPRS";
const RECORD_VERSION: u16 = 1;
const RECORD_HEADER_LEN: usize = 12;
const IDENTITY_KIND: u16 = 1;
const IDENTITY_V2_KIND: u16 = 5;
const IDENTITY_V3_KIND: u16 = 9;
const TRANSITION_KIND: u16 = 2;
const TRANSITION_V2_KIND: u16 = 6;
const CHECKPOINT_KIND: u16 = 3;
const COMPACTED_CHECKPOINT_KIND: u16 = 4;
const CHECKPOINT_V2_KIND: u16 = 7;
const COMPACTED_CHECKPOINT_V2_KIND: u16 = 8;
const IDENTITY_PAYLOAD_LEN: usize = 48;
const IDENTITY_V2_FIXED_PAYLOAD_LEN: usize = 28;
const IDENTITY_V3_FIXED_PAYLOAD_LEN: usize = 32;
const TRANSITION_FIXED_PAYLOAD_LEN: usize = 84;
const CHECKPOINT_FIXED_PAYLOAD_LEN: usize = 88;
const CHECKPOINT_V2_FIXED_PAYLOAD_LEN: usize = 92;
const ENTRY_FIXED_PAYLOAD_LEN: usize = 20;
const ENTRY_V2_FIXED_PAYLOAD_LEN: usize = 24;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StableIdentity {
    pub(crate) node_id: NodeId,
    pub(crate) group_id: GroupId,
    pub(crate) group_epoch: GroupEpoch,
    pub(crate) initial_voters: Vec<NodeId>,
    /// Immutable provisioned member allowlist. Existing callers use an equal
    /// initial voter set and allowlist; v3 identities can bootstrap three
    /// voters inside a five-member replacement pool.
    pub(crate) voters: Vec<NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StableCheckpoint {
    pub(crate) applied_index: LogIndex,
    pub(crate) publishable_index: LogIndex,
    pub(crate) state_digest: StateDigest,
}

impl StableCheckpoint {
    pub(crate) fn empty(identity: &StableIdentity) -> ConsensusResult<Self> {
        Ok(Self {
            applied_index: LogIndex::ZERO,
            publishable_index: LogIndex::ZERO,
            state_digest: compute_state_digest(identity.group_id, identity.group_epoch, &[])?,
        })
    }
}

pub(crate) struct RecoveredDiskState {
    pub(crate) store: DiskStableStore,
    pub(crate) storage: EpochRaftStorage,
    pub(crate) stable_generation: u64,
    pub(crate) repaired_partial_tail: bool,
    pub(crate) checkpoint: StableCheckpoint,
    pub(crate) applied: Vec<CommittedProposal>,
}

impl fmt::Debug for RecoveredDiskState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveredDiskState")
            .field("store", &self.store)
            .field("stable_generation", &self.stable_generation)
            .field("repaired_partial_tail", &self.repaired_partial_tail)
            .field("checkpoint", &self.checkpoint)
            .field("applied_count", &self.applied.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct DiskStableStore {
    wal: FileWal,
    identity: StableIdentity,
    hard_state: HardState,
    snapshot: Option<CheckpointImage>,
    entries: Vec<Entry>,
    conf_state: ConfState,
    checkpoint: StableCheckpoint,
    stable_generation: u64,
    #[cfg(test)]
    fail_after_next_append: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct StableTransition {
    generation: u64,
    hard_state: HardState,
    checkpoint: StableCheckpoint,
    entries: Vec<Entry>,
}

#[derive(Clone, Debug, PartialEq)]
struct StableCheckpointTransition {
    generation: u64,
    hard_state: HardState,
    checkpoint: StableCheckpoint,
    conf_state: ConfState,
    image: CheckpointImage,
    entries: Vec<Entry>,
}

#[derive(Clone, Debug, PartialEq)]
enum StableRecord {
    Identity(StableIdentity),
    Transition(StableTransition),
    Checkpoint(StableCheckpointTransition),
    CompactedCheckpoint(StableCheckpointTransition),
}

fn initialize_or_validate_identity(
    wal: &mut FileWal,
    records: &[LogRecord],
    identity: &StableIdentity,
) -> ConsensusResult<()> {
    let Some(first) = records.first() else {
        let encoded = encode_record(&StableRecord::Identity(identity.clone()))?;
        let record = wal.append(0, &encoded, true).map_err(storage_error)?;
        if record.sequence != 0 {
            return Err(ConsensusError::InvalidState(format!(
                "stable identity was written at WAL sequence {}; expected 0",
                record.sequence
            )));
        }
        return Ok(());
    };
    if first.sequence != 0 || first.timestamp_ms != 0 {
        return Err(ConsensusError::InvalidState(
            "stable identity must be WAL sequence zero with timestamp zero".into(),
        ));
    }
    match decode_record(&first.payload)? {
        StableRecord::Identity(stored) if &stored == identity => Ok(()),
        StableRecord::Identity(stored) => Err(ConsensusError::InvalidState(format!(
            "stable identity mismatch: stored {stored:?}, requested {identity:?}"
        ))),
        StableRecord::Transition(_)
        | StableRecord::Checkpoint(_)
        | StableRecord::CompactedCheckpoint(_) => Err(ConsensusError::InvalidState(
            "stable WAL sequence zero is not an identity record".into(),
        )),
    }
}

fn replay_transitions(
    identity: &StableIdentity,
    records: &[LogRecord],
) -> ConsensusResult<(CandidateState, u64)> {
    let mut state = CandidateState {
        hard_state: HardState::default(),
        snapshot: None,
        entries: Vec::new(),
        conf_state: expected_conf_state(&identity.initial_voters),
        checkpoint: StableCheckpoint::empty(identity)?,
    };
    let mut stable_generation = 0_u64;
    for record in records.iter().skip(1) {
        if record.timestamp_ms != 0 {
            return Err(ConsensusError::InvalidState(format!(
                "stable WAL generation {} has a nonzero timestamp",
                record.sequence
            )));
        }
        let decoded = decode_record(&record.payload)?;
        let observed_generation = match &decoded {
            StableRecord::Identity(_) => {
                return Err(ConsensusError::InvalidState(format!(
                    "stable WAL generation {} repeats the identity record",
                    record.sequence
                )));
            }
            StableRecord::Transition(transition) => transition.generation,
            StableRecord::Checkpoint(transition)
            | StableRecord::CompactedCheckpoint(transition) => transition.generation,
        };
        let compacted_baseline = matches!(&decoded, StableRecord::CompactedCheckpoint(_));
        if compacted_baseline && (record.sequence != 1 || stable_generation != 0) {
            return Err(ConsensusError::InvalidState(
                "compacted stable checkpoint must be the first transition after identity".into(),
            ));
        }
        let expected_generation = if compacted_baseline {
            if observed_generation == 0 {
                return Err(ConsensusError::InvalidState(
                    "compacted stable checkpoint generation must be nonzero".into(),
                ));
            }
            observed_generation
        } else {
            stable_generation
                .checked_add(1)
                .ok_or_else(|| ConsensusError::InvalidState("stable generation overflow".into()))?
        };
        if observed_generation != expected_generation {
            return Err(ConsensusError::InvalidState(format!(
                "stable record generation {observed_generation} does not follow logical generation {stable_generation}"
            )));
        }
        state = match decoded {
            StableRecord::Identity(_) => unreachable!("identity handled above"),
            StableRecord::Transition(transition) => prepare_transition(
                identity,
                state.prior(),
                transition.hard_state,
                &transition.entries,
                transition.checkpoint,
            )?,
            StableRecord::Checkpoint(transition)
            | StableRecord::CompactedCheckpoint(transition) => prepare_checkpoint_transition(
                identity,
                state.prior(),
                transition.hard_state,
                transition.conf_state,
                transition.image,
                &transition.entries,
                transition.checkpoint,
            )?,
        };
        stable_generation = expected_generation;
    }
    Ok((state, stable_generation))
}

impl DiskStableStore {
    pub(crate) fn open(
        path: &Path,
        identity: StableIdentity,
    ) -> ConsensusResult<RecoveredDiskState> {
        validate_stable_identity(&identity)?;
        let mut wal = FileWal::open(path).map_err(storage_error)?;
        let repaired_partial_tail = wal.recovered_partial_tail();
        let records = wal.records_from(0, usize::MAX);
        initialize_or_validate_identity(&mut wal, &records, &identity)?;
        let (state, stable_generation) = replay_transitions(&identity, &records)?;
        let CandidateState {
            hard_state,
            snapshot,
            entries,
            conf_state,
            checkpoint,
        } = state;

        let applied = derive_applied_history(
            &identity,
            snapshot.as_ref(),
            &entries,
            checkpoint.applied_index,
        )?;
        let storage = materialize_storage(
            &identity,
            &conf_state,
            &hard_state,
            snapshot.as_ref(),
            &entries,
        )?;
        let store = Self {
            wal,
            identity,
            hard_state,
            snapshot,
            entries,
            conf_state,
            checkpoint,
            stable_generation,
            #[cfg(test)]
            fail_after_next_append: false,
        };
        Ok(RecoveredDiskState {
            store,
            storage,
            stable_generation,
            repaired_partial_tail,
            checkpoint,
            applied,
        })
    }

    pub(crate) fn persist(
        &mut self,
        expected_generation: u64,
        hard_state: &HardState,
        entries: &[Entry],
        checkpoint: StableCheckpoint,
    ) -> ConsensusResult<u64> {
        let next_generation = self
            .stable_generation
            .checked_add(1)
            .ok_or_else(|| ConsensusError::InvalidState("stable generation overflow".into()))?;
        if expected_generation != next_generation {
            return Err(ConsensusError::InvalidState(format!(
                "stable generation {expected_generation} does not follow {}",
                self.stable_generation
            )));
        }

        let candidate = prepare_transition(
            &self.identity,
            self.prior_state(),
            hard_state.clone(),
            entries,
            checkpoint,
        )?;
        if entries.is_empty()
            && candidate.hard_state == self.hard_state
            && candidate.checkpoint == self.checkpoint
        {
            return Err(ConsensusError::InvalidState(
                "stable transition does not change HardState, entries, or checkpoint".into(),
            ));
        }

        let transition = StableTransition {
            generation: expected_generation,
            hard_state: hard_state.clone(),
            checkpoint,
            entries: entries.to_vec(),
        };
        let encoded = encode_record(&StableRecord::Transition(transition))?;
        self.wal.append(0, &encoded, true).map_err(storage_error)?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_after_next_append) {
            return Err(ConsensusError::Storage(
                "injected failure after stable WAL fsync and before cache mutation".into(),
            ));
        }

        self.hard_state = candidate.hard_state;
        self.snapshot = candidate.snapshot;
        self.entries = candidate.entries;
        self.conf_state = candidate.conf_state;
        self.checkpoint = candidate.checkpoint;
        self.stable_generation = expected_generation;
        Ok(expected_generation)
    }

    pub(crate) fn persist_checkpoint(
        &mut self,
        expected_generation: u64,
        hard_state: &HardState,
        conf_state: &ConfState,
        image: &CheckpointImage,
        entries: &[Entry],
        checkpoint: StableCheckpoint,
    ) -> ConsensusResult<u64> {
        let next_generation = self
            .stable_generation
            .checked_add(1)
            .ok_or_else(|| ConsensusError::InvalidState("stable generation overflow".into()))?;
        if expected_generation != next_generation {
            return Err(ConsensusError::InvalidState(format!(
                "stable generation {expected_generation} does not follow {}",
                self.stable_generation
            )));
        }
        let candidate = prepare_checkpoint_transition(
            &self.identity,
            self.prior_state(),
            hard_state.clone(),
            conf_state.clone(),
            image.clone(),
            entries,
            checkpoint,
        )?;
        let transition = StableCheckpointTransition {
            generation: expected_generation,
            hard_state: hard_state.clone(),
            checkpoint,
            conf_state: conf_state.clone(),
            image: image.clone(),
            entries: entries.to_vec(),
        };
        let encoded = encode_record(&StableRecord::Checkpoint(transition.clone()))?;
        self.wal.append(0, &encoded, true).map_err(storage_error)?;
        self.replace_with_checkpoint_baseline(&transition)?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_after_next_append) {
            return Err(ConsensusError::Storage(
                "injected failure after stable checkpoint fsync and before cache mutation".into(),
            ));
        }

        self.hard_state = candidate.hard_state;
        self.snapshot = candidate.snapshot;
        self.entries = candidate.entries;
        self.conf_state = candidate.conf_state;
        self.checkpoint = candidate.checkpoint;
        self.stable_generation = expected_generation;
        Ok(expected_generation)
    }

    pub(crate) const fn stable_generation(&self) -> u64 {
        self.stable_generation
    }

    fn replace_with_checkpoint_baseline(
        &mut self,
        transition: &StableCheckpointTransition,
    ) -> ConsensusResult<()> {
        let identity = encode_record(&StableRecord::Identity(self.identity.clone()))?;
        let checkpoint = encode_record(&StableRecord::CompactedCheckpoint(transition.clone()))?;
        self.wal
            .replace_with_records(&[(0, identity.as_slice()), (0, checkpoint.as_slice())])
            .map_err(storage_error)
    }

    fn prior_state(&self) -> PriorState<'_> {
        PriorState {
            hard_state: &self.hard_state,
            snapshot: self.snapshot.as_ref(),
            entries: &self.entries,
            conf_state: &self.conf_state,
            checkpoint: self.checkpoint,
        }
    }

    pub(crate) const fn checkpoint(&self) -> StableCheckpoint {
        self.checkpoint
    }

    #[cfg(test)]
    pub(crate) fn fail_after_next_append(&mut self) {
        self.fail_after_next_append = true;
    }
}

#[derive(Debug)]
struct CandidateState {
    hard_state: HardState,
    snapshot: Option<CheckpointImage>,
    entries: Vec<Entry>,
    conf_state: ConfState,
    checkpoint: StableCheckpoint,
}

impl CandidateState {
    fn prior(&self) -> PriorState<'_> {
        PriorState {
            hard_state: &self.hard_state,
            snapshot: self.snapshot.as_ref(),
            entries: &self.entries,
            conf_state: &self.conf_state,
            checkpoint: self.checkpoint,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct PriorState<'a> {
    hard_state: &'a HardState,
    snapshot: Option<&'a CheckpointImage>,
    entries: &'a [Entry],
    conf_state: &'a ConfState,
    checkpoint: StableCheckpoint,
}

fn prepare_transition(
    identity: &StableIdentity,
    previous: PriorState<'_>,
    hard_state: HardState,
    new_entries: &[Entry],
    checkpoint: StableCheckpoint,
) -> ConsensusResult<CandidateState> {
    validate_entry_batch(new_entries)?;
    validate_hard_state_transition(identity, previous.hard_state, &hard_state)?;
    validate_checkpoint_transition(previous.checkpoint, checkpoint)?;

    let mut entries = previous.entries.to_vec();
    if let Some(first) = new_entries.first() {
        if first.index <= previous.hard_state.commit {
            return Err(ConsensusError::InvalidState(format!(
                "stable update would overwrite committed index {} with an entry beginning at {}",
                previous.hard_state.commit, first.index
            )));
        }
        let base_index = previous.snapshot.map_or(0, |image| image.index.get());
        let expected_next = u64::try_from(entries.len())
            .map_err(|_| ConsensusError::InvalidState("stable log is too large".into()))?
            .checked_add(base_index)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| ConsensusError::InvalidState("stable log index overflow".into()))?;
        if first.index > expected_next {
            return Err(ConsensusError::InvalidState(format!(
                "stable entry batch begins at {} after last index {}; gaps are not supported",
                first.index,
                expected_next - 1
            )));
        }
        let retained = usize::try_from(first.index - base_index - 1)
            .map_err(|_| ConsensusError::InvalidState("stable log index is too large".into()))?;
        entries.truncate(retained);
        entries.extend_from_slice(new_entries);
    }

    let conf_state = derive_conf_state(
        previous.conf_state,
        &entries,
        previous.checkpoint.applied_index,
        checkpoint.applied_index,
        &identity.voters,
    )?;
    validate_complete_state(
        identity,
        &conf_state,
        &hard_state,
        previous.snapshot,
        &entries,
        checkpoint,
    )?;
    Ok(CandidateState {
        hard_state,
        snapshot: previous.snapshot.cloned(),
        entries,
        conf_state,
        checkpoint,
    })
}

fn prepare_checkpoint_transition(
    identity: &StableIdentity,
    previous: PriorState<'_>,
    hard_state: HardState,
    conf_state: ConfState,
    image: CheckpointImage,
    entries: &[Entry],
    checkpoint: StableCheckpoint,
) -> ConsensusResult<CandidateState> {
    validate_hard_state_transition(identity, previous.hard_state, &hard_state)?;
    validate_checkpoint_transition(previous.checkpoint, checkpoint)?;
    validate_checkpoint_image(&image)?;
    if image.group_id != identity.group_id || image.group_epoch != identity.group_epoch {
        return Err(ConsensusError::InvalidState(
            "stable checkpoint belongs to a different group or epoch".into(),
        ));
    }
    if previous
        .snapshot
        .is_some_and(|prior| image.index < prior.index)
    {
        return Err(ConsensusError::InvalidState(
            "stable checkpoint index regresses".into(),
        ));
    }
    if image.application_snapshot.is_none() {
        let previous_applied = derive_applied_history(
            identity,
            previous.snapshot,
            previous.entries,
            previous.checkpoint.applied_index,
        )?;
        if image.applied.get(..previous_applied.len()) != Some(previous_applied.as_slice()) {
            return Err(ConsensusError::InvalidState(
                "stable v1 checkpoint does not extend the prior applied history".into(),
            ));
        }
    }
    if image.index != checkpoint.applied_index || image.state_digest != checkpoint.state_digest {
        return Err(ConsensusError::InvalidState(
            "stable checkpoint image does not match its application checkpoint".into(),
        ));
    }
    validate_entry_batch(entries)?;
    if entries
        .first()
        .is_some_and(|entry| entry.index != image.index.get().saturating_add(1))
    {
        return Err(ConsensusError::InvalidState(
            "stable checkpoint tail is not contiguous after its index".into(),
        ));
    }
    let conf_state = if conf_state.voters.is_empty() {
        previous.conf_state.clone()
    } else {
        membership::validate_conf_state(&conf_state, &identity.voters)?;
        conf_state
    };
    validate_complete_state(
        identity,
        &conf_state,
        &hard_state,
        Some(&image),
        entries,
        checkpoint,
    )?;
    Ok(CandidateState {
        hard_state,
        snapshot: Some(image),
        entries: entries.to_vec(),
        conf_state,
        checkpoint,
    })
}

fn validate_hard_state_transition(
    identity: &StableIdentity,
    previous: &HardState,
    next: &HardState,
) -> ConsensusResult<()> {
    validate_hard_state(next, &identity.voters)?;
    if next.term < previous.term {
        return Err(ConsensusError::InvalidState(format!(
            "stable HardState term decreases from {} to {}",
            previous.term, next.term
        )));
    }
    if next.commit < previous.commit {
        return Err(ConsensusError::InvalidState(format!(
            "stable HardState commit decreases from {} to {}",
            previous.commit, next.commit
        )));
    }
    if next.term == previous.term && previous.vote != 0 && next.vote != previous.vote {
        return Err(ConsensusError::InvalidState(format!(
            "stable HardState changes vote from {} to {} in term {}",
            previous.vote, next.vote, next.term
        )));
    }
    Ok(())
}

fn validate_checkpoint_transition(
    previous: StableCheckpoint,
    next: StableCheckpoint,
) -> ConsensusResult<()> {
    if next.applied_index != next.publishable_index {
        return Err(ConsensusError::InvalidState(format!(
            "stable v1 checkpoint requires applied index {} to equal publishable index {}",
            next.applied_index, next.publishable_index
        )));
    }
    if next.applied_index < previous.applied_index {
        return Err(ConsensusError::InvalidState(format!(
            "stable applied index decreases from {} to {}",
            previous.applied_index, next.applied_index
        )));
    }
    Ok(())
}

fn validate_entry_batch(entries: &[Entry]) -> ConsensusResult<()> {
    let mut previous_index: Option<u64> = None;
    for entry in entries {
        validate_stable_entry(entry)?;
        if let Some(previous) = previous_index {
            let expected = previous.checked_add(1).ok_or_else(|| {
                ConsensusError::InvalidState("stable entry index overflow".into())
            })?;
            if entry.index != expected {
                return Err(ConsensusError::InvalidState(format!(
                    "stable entry batch is not contiguous: expected index {expected}, got {}",
                    entry.index
                )));
            }
        }
        previous_index = Some(entry.index);
    }
    Ok(())
}

fn validate_stable_entry(entry: &Entry) -> ConsensusResult<()> {
    if entry.index == 0 || entry.term == 0 {
        return Err(ConsensusError::InvalidState(
            "stable entries require nonzero index and term".into(),
        ));
    }
    if !entry.context.is_empty() || entry.sync_log {
        return Err(ConsensusError::Unsupported(
            "stable entries require empty context and deprecated sync_log=false".into(),
        ));
    }
    match EntryType::from_i32(entry.entry_type) {
        Some(EntryType::EntryNormal) => {}
        Some(EntryType::EntryConfChangeV2) => {
            decode_membership_change(entry.data.as_ref())?;
        }
        Some(EntryType::EntryConfChange) | None => {
            return Err(ConsensusError::Unsupported(
                "stable entries do not support legacy or unknown membership types".into(),
            ));
        }
    }
    Ok(())
}

fn derive_conf_state(
    previous: &ConfState,
    entries: &[Entry],
    previous_applied: LogIndex,
    next_applied: LogIndex,
    allowed_members: &[NodeId],
) -> ConsensusResult<ConfState> {
    let mut state = previous.clone();
    for entry in entries
        .iter()
        .filter(|entry| entry.index > previous_applied.get() && entry.index <= next_applied.get())
    {
        match EntryType::from_i32(entry.entry_type) {
            Some(EntryType::EntryNormal) => {}
            Some(EntryType::EntryConfChangeV2) => {
                let change = decode_membership_change(entry.data.as_ref())?;
                state = membership::apply_conf_change(&state, &change, allowed_members)?;
            }
            Some(EntryType::EntryConfChange) | None => {
                return Err(ConsensusError::Unsupported(
                    "stable membership replay encountered a legacy or unknown entry type".into(),
                ));
            }
        }
    }
    membership::validate_conf_state(&state, allowed_members)?;
    Ok(state)
}

fn validate_complete_state(
    identity: &StableIdentity,
    conf_state: &ConfState,
    hard_state: &HardState,
    snapshot: Option<&CheckpointImage>,
    entries: &[Entry],
    checkpoint: StableCheckpoint,
) -> ConsensusResult<()> {
    validate_stable_identity(identity)?;
    membership::validate_conf_state(conf_state, &identity.voters)?;
    validate_hard_state(hard_state, &identity.voters)?;
    let base_index = snapshot.map_or(0, |image| image.index.get());
    let base_term = snapshot.map_or(0, |image| image.term.get());
    let last_index = entries.last().map_or(base_index, |entry| entry.index);
    validate_retained_log_order(entries, base_index, base_term, last_index, hard_state.term)?;
    if hard_state.commit > last_index {
        return Err(ConsensusError::InvalidState(format!(
            "stable HardState commit {} exceeds last index {last_index}",
            hard_state.commit
        )));
    }
    if checkpoint.applied_index.get() > hard_state.commit {
        return Err(ConsensusError::InvalidState(format!(
            "stable checkpoint applied index {} exceeds commit {}",
            checkpoint.applied_index, hard_state.commit
        )));
    }
    if checkpoint.applied_index.get() < base_index {
        return Err(ConsensusError::InvalidState(format!(
            "stable applied index {} is below checkpoint index {base_index}",
            checkpoint.applied_index
        )));
    }
    if checkpoint.applied_index != checkpoint.publishable_index {
        return Err(ConsensusError::InvalidState(
            "stable v1 checkpoint applied and publishable indexes differ".into(),
        ));
    }

    let applied = derive_applied_history(identity, snapshot, entries, checkpoint.applied_index)?;
    let expected_digest = match snapshot {
        Some(image) if image.application_snapshot.is_some() => {
            let mut digest = image.state_digest;
            for committed in applied
                .iter()
                .filter(|committed| committed.receipt.log_index > image.index)
            {
                digest = super::advance_rolling_state_digest(digest, committed)?;
            }
            digest
        }
        _ => compute_state_digest(identity.group_id, identity.group_epoch, &applied)?,
    };
    if checkpoint.state_digest != expected_digest {
        return Err(ConsensusError::InvalidState(
            "stable checkpoint digest does not match its applied log history".into(),
        ));
    }
    Ok(())
}

fn derive_applied_history(
    identity: &StableIdentity,
    snapshot: Option<&CheckpointImage>,
    entries: &[Entry],
    applied_index: LogIndex,
) -> ConsensusResult<Vec<CommittedProposal>> {
    let mut applied = snapshot.map_or_else(Vec::new, |image| image.applied.clone());
    let mut seen = applied
        .iter()
        .map(|proposal| (proposal.receipt.proposal_id, proposal.payload.clone()))
        .collect::<BTreeMap<ProposalId, Vec<u8>>>();

    for entry in entries {
        validate_stable_entry(entry)?;
        if entry.entry_type == EntryType::EntryConfChangeV2 as i32 {
            continue;
        }
        if entry.data.is_empty() {
            continue;
        }
        let command = decode_command(entry.data.as_ref())?;
        validate_command_scope(identity.group_id, identity.group_epoch, &command)?;
        match seen.get(&command.proposal_id) {
            Some(payload) if *payload != command.payload => {
                return Err(ConsensusError::ConflictingProposal(command.proposal_id));
            }
            Some(_) => {}
            None => {
                seen.insert(command.proposal_id, command.payload.clone());
                if entry.index <= applied_index.get() {
                    applied.push(CommittedProposal {
                        receipt: CommitReceipt {
                            group_id: identity.group_id,
                            group_epoch: identity.group_epoch,
                            proposal_id: command.proposal_id,
                            term: Term::new(entry.term),
                            log_index: LogIndex::new(entry.index),
                        },
                        payload: command.payload,
                    });
                }
            }
        }
    }
    Ok(applied)
}

fn materialize_storage(
    identity: &StableIdentity,
    conf_state: &ConfState,
    hard_state: &HardState,
    snapshot: Option<&CheckpointImage>,
    entries: &[Entry],
) -> ConsensusResult<EpochRaftStorage> {
    membership::validate_conf_state(conf_state, &identity.voters)?;
    let mut storage = EpochRaftStorage::new_with_conf_state(conf_state.clone());
    if let Some(image) = snapshot {
        storage.install_snapshot(
            checkpoint_snapshot(image, conf_state)?,
            entries,
            hard_state.clone(),
        )?;
    } else {
        let mut core = storage.wl();
        core.append(entries)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?;
        core.set_hardstate(hard_state.clone());
    }
    Ok(storage)
}

fn validate_retained_log_order(
    entries: &[Entry],
    base_index: u64,
    base_term: u64,
    last_index: u64,
    hard_state_term: u64,
) -> ConsensusResult<()> {
    if base_index == 0 {
        return validate_log_order(entries, last_index, hard_state_term);
    }
    let expected_len = last_index
        .checked_sub(base_index)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| ConsensusError::InvalidState("stable log length overflow".into()))?;
    if entries.len() != expected_len {
        return Err(ConsensusError::InvalidState(
            "persisted compacted log is not complete after its checkpoint".into(),
        ));
    }
    let mut previous_term = base_term;
    for (offset, entry) in entries.iter().enumerate() {
        let expected =
            base_index
                .checked_add(u64::try_from(offset).map_err(|_| {
                    ConsensusError::InvalidState("stable log index overflow".into())
                })?)
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| ConsensusError::InvalidState("stable log index overflow".into()))?;
        validate_stable_entry(entry)?;
        if entry.index != expected {
            return Err(ConsensusError::InvalidState(format!(
                "persisted compacted log entry {} is out of order; expected {expected}",
                entry.index
            )));
        }
        if entry.term < previous_term {
            return Err(ConsensusError::InvalidState(format!(
                "persisted log term {} at index {} regresses below prior term {previous_term}",
                entry.term, entry.index
            )));
        }
        previous_term = entry.term;
    }
    if previous_term > hard_state_term {
        return Err(ConsensusError::InvalidState(format!(
            "stored HardState term {hard_state_term} is below retained log term {previous_term}"
        )));
    }
    Ok(())
}

fn encode_record(record: &StableRecord) -> ConsensusResult<Vec<u8>> {
    let (kind, payload) = match record {
        StableRecord::Identity(identity) => {
            let kind = if identity.initial_voters != identity.voters {
                IDENTITY_V3_KIND
            } else if identity.voters.len() == 3 {
                IDENTITY_KIND
            } else {
                IDENTITY_V2_KIND
            };
            (kind, encode_identity(identity)?)
        }
        StableRecord::Transition(transition) => {
            if transition
                .entries
                .iter()
                .all(|entry| entry.entry_type == EntryType::EntryNormal as i32)
            {
                (TRANSITION_KIND, encode_transition(transition)?)
            } else {
                (TRANSITION_V2_KIND, encode_transition_v2(transition)?)
            }
        }
        StableRecord::Checkpoint(transition) => {
            if transition.conf_state.voters.is_empty()
                && transition
                    .entries
                    .iter()
                    .all(|entry| entry.entry_type == EntryType::EntryNormal as i32)
            {
                (CHECKPOINT_KIND, encode_checkpoint_transition(transition)?)
            } else {
                (
                    CHECKPOINT_V2_KIND,
                    encode_checkpoint_transition_v2(transition)?,
                )
            }
        }
        StableRecord::CompactedCheckpoint(transition) => {
            if transition.conf_state.voters.is_empty()
                && transition
                    .entries
                    .iter()
                    .all(|entry| entry.entry_type == EntryType::EntryNormal as i32)
            {
                (
                    COMPACTED_CHECKPOINT_KIND,
                    encode_checkpoint_transition(transition)?,
                )
            } else {
                (
                    COMPACTED_CHECKPOINT_V2_KIND,
                    encode_checkpoint_transition_v2(transition)?,
                )
            }
        }
    };
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        ConsensusError::InvalidState("stable record payload exceeds the v1 length field".into())
    })?;
    let capacity = RECORD_HEADER_LEN
        .checked_add(payload.len())
        .ok_or_else(|| ConsensusError::InvalidState("stable record length overflow".into()))?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(&RECORD_MAGIC);
    encoded.extend_from_slice(&RECORD_VERSION.to_be_bytes());
    encoded.extend_from_slice(&kind.to_be_bytes());
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_record(encoded: &[u8]) -> ConsensusResult<StableRecord> {
    if encoded.len() < RECORD_HEADER_LEN || encoded[..4] != RECORD_MAGIC {
        return Err(ConsensusError::InvalidState(
            "stable record has an invalid header".into(),
        ));
    }
    let version = u16::from_be_bytes([encoded[4], encoded[5]]);
    if version != RECORD_VERSION {
        return Err(ConsensusError::Unsupported(format!(
            "unsupported stable record version {version}"
        )));
    }
    let kind = u16::from_be_bytes([encoded[6], encoded[7]]);
    let payload_len = u32::from_be_bytes(
        encoded[8..12]
            .try_into()
            .map_err(|_| ConsensusError::InvalidState("invalid stable record length".into()))?,
    ) as usize;
    let expected_len = RECORD_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| ConsensusError::InvalidState("stable record length overflow".into()))?;
    if encoded.len() != expected_len {
        return Err(ConsensusError::InvalidState(
            "stable record payload length does not match its frame".into(),
        ));
    }
    let payload = &encoded[RECORD_HEADER_LEN..];
    let record = match kind {
        IDENTITY_KIND => StableRecord::Identity(decode_identity_v1(payload)?),
        IDENTITY_V2_KIND => StableRecord::Identity(decode_identity_v2(payload)?),
        IDENTITY_V3_KIND => StableRecord::Identity(decode_identity_v3(payload)?),
        TRANSITION_KIND => StableRecord::Transition(decode_transition(payload)?),
        TRANSITION_V2_KIND => StableRecord::Transition(decode_transition_v2(payload)?),
        CHECKPOINT_KIND => StableRecord::Checkpoint(decode_checkpoint_transition(payload)?),
        COMPACTED_CHECKPOINT_KIND => {
            StableRecord::CompactedCheckpoint(decode_checkpoint_transition(payload)?)
        }
        CHECKPOINT_V2_KIND => StableRecord::Checkpoint(decode_checkpoint_transition_v2(payload)?),
        COMPACTED_CHECKPOINT_V2_KIND => {
            StableRecord::CompactedCheckpoint(decode_checkpoint_transition_v2(payload)?)
        }
        _ => {
            return Err(ConsensusError::Unsupported(format!(
                "unsupported stable record kind {kind}"
            )));
        }
    };
    if encode_record(&record)? != encoded {
        return Err(ConsensusError::InvalidState(
            "stable record is not canonically encoded".into(),
        ));
    }
    Ok(record)
}

fn encode_identity(identity: &StableIdentity) -> ConsensusResult<Vec<u8>> {
    validate_stable_identity(identity)?;
    let has_distinct_allowlist = identity.initial_voters != identity.voters;
    let is_legacy = !has_distinct_allowlist && identity.voters.len() == 3;
    let capacity = if has_distinct_allowlist {
        IDENTITY_V3_FIXED_PAYLOAD_LEN
            .checked_add(
                identity
                    .initial_voters
                    .len()
                    .saturating_add(identity.voters.len())
                    .saturating_mul(size_of::<u64>()),
            )
            .ok_or_else(|| ConsensusError::InvalidState("stable identity length overflow".into()))?
    } else if is_legacy {
        IDENTITY_PAYLOAD_LEN
    } else {
        IDENTITY_V2_FIXED_PAYLOAD_LEN
            .checked_add(identity.voters.len().saturating_mul(size_of::<u64>()))
            .ok_or_else(|| ConsensusError::InvalidState("stable identity length overflow".into()))?
    };
    let mut payload = Vec::with_capacity(capacity);
    payload.extend_from_slice(&identity.node_id.get().to_be_bytes());
    payload.extend_from_slice(&identity.group_id.get().to_be_bytes());
    payload.extend_from_slice(&identity.group_epoch.get().to_be_bytes());
    if has_distinct_allowlist {
        let initial_count = u32::try_from(identity.initial_voters.len()).map_err(|_| {
            ConsensusError::InvalidState("stable identity initial voter count overflow".into())
        })?;
        let allowed_count = u32::try_from(identity.voters.len()).map_err(|_| {
            ConsensusError::InvalidState("stable identity member count overflow".into())
        })?;
        payload.extend_from_slice(&initial_count.to_be_bytes());
        payload.extend_from_slice(&allowed_count.to_be_bytes());
        for voter in &identity.initial_voters {
            payload.extend_from_slice(&voter.get().to_be_bytes());
        }
    } else if !is_legacy {
        let voter_count = u32::try_from(identity.voters.len()).map_err(|_| {
            ConsensusError::InvalidState("stable identity voter count overflow".into())
        })?;
        payload.extend_from_slice(&voter_count.to_be_bytes());
    }
    for voter in &identity.voters {
        payload.extend_from_slice(&voter.get().to_be_bytes());
    }
    Ok(payload)
}

fn decode_identity_v1(payload: &[u8]) -> ConsensusResult<StableIdentity> {
    if payload.len() != IDENTITY_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(format!(
            "stable identity payload is {} bytes; expected {IDENTITY_PAYLOAD_LEN}",
            payload.len()
        )));
    }
    let mut reader = Reader::new(payload);
    let mut identity = StableIdentity {
        node_id: NodeId::new(reader.read_u64("node ID")?)?,
        group_id: GroupId::new(reader.read_u64("group ID")?)?,
        group_epoch: GroupEpoch::new(reader.read_u64("group epoch")?)?,
        initial_voters: vec![
            NodeId::new(reader.read_u64("voter ID")?)?,
            NodeId::new(reader.read_u64("voter ID")?)?,
            NodeId::new(reader.read_u64("voter ID")?)?,
        ],
        voters: Vec::new(),
    };
    identity.voters.clone_from(&identity.initial_voters);
    reader.finish("stable identity")?;
    validate_stable_identity(&identity)?;
    Ok(identity)
}

fn decode_identity_v2(payload: &[u8]) -> ConsensusResult<StableIdentity> {
    if payload.len() < IDENTITY_V2_FIXED_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(
            "stable v2 identity payload is truncated".into(),
        ));
    }
    let mut reader = Reader::new(payload);
    let node_id = NodeId::new(reader.read_u64("node ID")?)?;
    let group_id = GroupId::new(reader.read_u64("group ID")?)?;
    let group_epoch = GroupEpoch::new(reader.read_u64("group epoch")?)?;
    let voter_count = reader.read_u32("voter count")? as usize;
    if voter_count > reader.remaining_len() / size_of::<u64>() {
        return Err(ConsensusError::InvalidState(
            "stable v2 identity voter count exceeds its remaining bytes".into(),
        ));
    }
    let mut voters = Vec::with_capacity(voter_count);
    for _ in 0..voter_count {
        voters.push(NodeId::new(reader.read_u64("voter ID")?)?);
    }
    reader.finish("stable v2 identity")?;
    let identity = StableIdentity {
        node_id,
        group_id,
        group_epoch,
        initial_voters: voters.clone(),
        voters,
    };
    validate_stable_identity(&identity)?;
    Ok(identity)
}

fn decode_identity_v3(payload: &[u8]) -> ConsensusResult<StableIdentity> {
    if payload.len() < IDENTITY_V3_FIXED_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(
            "stable v3 identity payload is truncated".into(),
        ));
    }
    let mut reader = Reader::new(payload);
    let node_id = NodeId::new(reader.read_u64("node ID")?)?;
    let group_id = GroupId::new(reader.read_u64("group ID")?)?;
    let group_epoch = GroupEpoch::new(reader.read_u64("group epoch")?)?;
    let initial_count = reader.read_u32("initial voter count")? as usize;
    let allowed_count = reader.read_u32("member allowlist count")? as usize;
    let total_count = initial_count.checked_add(allowed_count).ok_or_else(|| {
        ConsensusError::InvalidState("stable v3 identity member count overflow".into())
    })?;
    if total_count > reader.remaining_len() / size_of::<u64>() {
        return Err(ConsensusError::InvalidState(
            "stable v3 identity member counts exceed the remaining bytes".into(),
        ));
    }
    let mut initial_voters = Vec::with_capacity(initial_count);
    for _ in 0..initial_count {
        initial_voters.push(NodeId::new(reader.read_u64("initial voter ID")?)?);
    }
    let mut voters = Vec::with_capacity(allowed_count);
    for _ in 0..allowed_count {
        voters.push(NodeId::new(reader.read_u64("allowed member ID")?)?);
    }
    reader.finish("stable v3 identity")?;
    let identity = StableIdentity {
        node_id,
        group_id,
        group_epoch,
        initial_voters,
        voters,
    };
    validate_stable_identity(&identity)?;
    Ok(identity)
}

fn validate_stable_identity(identity: &StableIdentity) -> ConsensusResult<()> {
    validate_initial_membership(identity.node_id, &identity.initial_voters, &identity.voters)
}

fn encode_transition(transition: &StableTransition) -> ConsensusResult<Vec<u8>> {
    validate_entry_batch(&transition.entries)?;
    let entry_count = u32::try_from(transition.entries.len()).map_err(|_| {
        ConsensusError::InvalidState("stable transition has too many entries".into())
    })?;
    let mut capacity = TRANSITION_FIXED_PAYLOAD_LEN;
    for entry in &transition.entries {
        let data_len = u32::try_from(entry.data.len()).map_err(|_| {
            ConsensusError::InvalidState("stable entry data exceeds the v1 length field".into())
        })?;
        capacity = capacity
            .checked_add(ENTRY_FIXED_PAYLOAD_LEN)
            .and_then(|value| value.checked_add(data_len as usize))
            .ok_or_else(|| {
                ConsensusError::InvalidState("stable transition length overflow".into())
            })?;
    }

    let mut payload = Vec::with_capacity(capacity);
    payload.extend_from_slice(&transition.generation.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.term.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.vote.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.commit.to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.applied_index.get().to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.publishable_index.get().to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.state_digest);
    payload.extend_from_slice(&entry_count.to_be_bytes());
    for entry in &transition.entries {
        let data_len = u32::try_from(entry.data.len()).map_err(|_| {
            ConsensusError::InvalidState("stable entry data exceeds the v1 length field".into())
        })?;
        payload.extend_from_slice(&entry.index.to_be_bytes());
        payload.extend_from_slice(&entry.term.to_be_bytes());
        payload.extend_from_slice(&data_len.to_be_bytes());
        payload.extend_from_slice(entry.data.as_ref());
    }
    Ok(payload)
}

fn decode_transition(payload: &[u8]) -> ConsensusResult<StableTransition> {
    if payload.len() < TRANSITION_FIXED_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(
            "stable transition is truncated".into(),
        ));
    }
    let mut reader = Reader::new(payload);
    let generation = reader.read_u64("generation")?;
    let hard_state = HardState {
        term: reader.read_u64("HardState term")?,
        vote: reader.read_u64("HardState vote")?,
        commit: reader.read_u64("HardState commit")?,
    };
    let checkpoint = StableCheckpoint {
        applied_index: LogIndex::new(reader.read_u64("applied index")?),
        publishable_index: LogIndex::new(reader.read_u64("publishable index")?),
        state_digest: reader.read_array("state digest")?,
    };
    let entry_count = reader.read_u32("entry count")? as usize;
    if entry_count > reader.remaining_len() / ENTRY_FIXED_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(
            "stable transition entry count exceeds its remaining bytes".into(),
        ));
    }
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let index = reader.read_u64("entry index")?;
        let term = reader.read_u64("entry term")?;
        let data_len = reader.read_u32("entry data length")? as usize;
        let data = reader.read_slice(data_len, "entry data")?.to_vec();
        let mut entry = Entry {
            entry_type: EntryType::EntryNormal as i32,
            term,
            index,
            ..Entry::default()
        };
        entry.data = data;
        entries.push(entry);
    }
    reader.finish("stable transition")?;
    validate_entry_batch(&entries)?;
    Ok(StableTransition {
        generation,
        hard_state,
        checkpoint,
        entries,
    })
}

fn encode_transition_v2(transition: &StableTransition) -> ConsensusResult<Vec<u8>> {
    validate_entry_batch(&transition.entries)?;
    let entry_count = u32::try_from(transition.entries.len()).map_err(|_| {
        ConsensusError::InvalidState("stable transition has too many entries".into())
    })?;
    let mut capacity = TRANSITION_FIXED_PAYLOAD_LEN;
    for entry in &transition.entries {
        capacity = capacity
            .checked_add(ENTRY_V2_FIXED_PAYLOAD_LEN)
            .and_then(|value| value.checked_add(entry.data.len()))
            .ok_or_else(|| {
                ConsensusError::InvalidState("stable v2 transition length overflow".into())
            })?;
    }
    let mut payload = Vec::with_capacity(capacity);
    payload.extend_from_slice(&transition.generation.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.term.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.vote.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.commit.to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.applied_index.get().to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.publishable_index.get().to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.state_digest);
    payload.extend_from_slice(&entry_count.to_be_bytes());
    for entry in &transition.entries {
        let data_len = u32::try_from(entry.data.len()).map_err(|_| {
            ConsensusError::InvalidState("stable entry data exceeds its length field".into())
        })?;
        payload.extend_from_slice(&entry.index.to_be_bytes());
        payload.extend_from_slice(&entry.term.to_be_bytes());
        payload.extend_from_slice(&entry.entry_type.to_be_bytes());
        payload.extend_from_slice(&data_len.to_be_bytes());
        payload.extend_from_slice(entry.data.as_ref());
    }
    Ok(payload)
}

fn decode_transition_v2(payload: &[u8]) -> ConsensusResult<StableTransition> {
    if payload.len() < TRANSITION_FIXED_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(
            "stable v2 transition is truncated".into(),
        ));
    }
    let mut reader = Reader::new(payload);
    let generation = reader.read_u64("generation")?;
    let hard_state = HardState {
        term: reader.read_u64("HardState term")?,
        vote: reader.read_u64("HardState vote")?,
        commit: reader.read_u64("HardState commit")?,
    };
    let checkpoint = StableCheckpoint {
        applied_index: LogIndex::new(reader.read_u64("applied index")?),
        publishable_index: LogIndex::new(reader.read_u64("publishable index")?),
        state_digest: reader.read_array("state digest")?,
    };
    let entry_count = reader.read_u32("entry count")? as usize;
    if entry_count > reader.remaining_len() / ENTRY_V2_FIXED_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(
            "stable v2 transition entry count exceeds its remaining bytes".into(),
        ));
    }
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let index = reader.read_u64("entry index")?;
        let term = reader.read_u64("entry term")?;
        let entry_type = reader.read_i32("entry type")?;
        let data_len = reader.read_u32("entry data length")? as usize;
        let data = reader.read_slice(data_len, "entry data")?.to_vec();
        let mut entry = Entry {
            entry_type,
            term,
            index,
            ..Entry::default()
        };
        entry.data = data;
        entries.push(entry);
    }
    reader.finish("stable v2 transition")?;
    validate_entry_batch(&entries)?;
    Ok(StableTransition {
        generation,
        hard_state,
        checkpoint,
        entries,
    })
}

fn encode_checkpoint_transition(
    transition: &StableCheckpointTransition,
) -> ConsensusResult<Vec<u8>> {
    validate_entry_batch(&transition.entries)?;
    validate_checkpoint_image(&transition.image)?;
    let image = encode_checkpoint_image(&transition.image)?;
    let image_len = u32::try_from(image.len()).map_err(|_| {
        ConsensusError::InvalidState("stable checkpoint image exceeds its length field".into())
    })?;
    let entry_count = u32::try_from(transition.entries.len()).map_err(|_| {
        ConsensusError::InvalidState("stable checkpoint has too many tail entries".into())
    })?;
    let mut capacity = CHECKPOINT_FIXED_PAYLOAD_LEN
        .checked_add(image.len())
        .ok_or_else(|| ConsensusError::InvalidState("stable checkpoint length overflow".into()))?;
    for entry in &transition.entries {
        capacity = capacity
            .checked_add(ENTRY_FIXED_PAYLOAD_LEN)
            .and_then(|value| value.checked_add(entry.data.len()))
            .ok_or_else(|| {
                ConsensusError::InvalidState("stable checkpoint length overflow".into())
            })?;
    }

    let mut payload = Vec::with_capacity(capacity);
    payload.extend_from_slice(&transition.generation.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.term.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.vote.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.commit.to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.applied_index.get().to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.publishable_index.get().to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.state_digest);
    payload.extend_from_slice(&image_len.to_be_bytes());
    payload.extend_from_slice(&entry_count.to_be_bytes());
    payload.extend_from_slice(&image);
    for entry in &transition.entries {
        let data_len = u32::try_from(entry.data.len()).map_err(|_| {
            ConsensusError::InvalidState("stable entry data exceeds its length field".into())
        })?;
        payload.extend_from_slice(&entry.index.to_be_bytes());
        payload.extend_from_slice(&entry.term.to_be_bytes());
        payload.extend_from_slice(&data_len.to_be_bytes());
        payload.extend_from_slice(entry.data.as_ref());
    }
    Ok(payload)
}

fn decode_checkpoint_transition(payload: &[u8]) -> ConsensusResult<StableCheckpointTransition> {
    if payload.len() < CHECKPOINT_FIXED_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(
            "stable checkpoint transition is truncated".into(),
        ));
    }
    let mut reader = Reader::new(payload);
    let generation = reader.read_u64("generation")?;
    let hard_state = HardState {
        term: reader.read_u64("HardState term")?,
        vote: reader.read_u64("HardState vote")?,
        commit: reader.read_u64("HardState commit")?,
    };
    let checkpoint = StableCheckpoint {
        applied_index: LogIndex::new(reader.read_u64("applied index")?),
        publishable_index: LogIndex::new(reader.read_u64("publishable index")?),
        state_digest: reader.read_array("state digest")?,
    };
    let image_len = reader.read_u32("checkpoint image length")? as usize;
    let entry_count = reader.read_u32("tail entry count")? as usize;
    let image = decode_checkpoint_image(reader.read_slice(image_len, "checkpoint image")?)?;
    if entry_count > reader.remaining_len() / ENTRY_FIXED_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(
            "stable checkpoint tail count exceeds its remaining bytes".into(),
        ));
    }
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let index = reader.read_u64("entry index")?;
        let term = reader.read_u64("entry term")?;
        let data_len = reader.read_u32("entry data length")? as usize;
        let data = reader.read_slice(data_len, "entry data")?.to_vec();
        let mut entry = Entry {
            entry_type: EntryType::EntryNormal as i32,
            term,
            index,
            ..Entry::default()
        };
        entry.data = data;
        entries.push(entry);
    }
    reader.finish("stable checkpoint transition")?;
    validate_entry_batch(&entries)?;
    Ok(StableCheckpointTransition {
        generation,
        hard_state,
        checkpoint,
        conf_state: ConfState::default(),
        image,
        entries,
    })
}

fn encode_checkpoint_transition_v2(
    transition: &StableCheckpointTransition,
) -> ConsensusResult<Vec<u8>> {
    validate_entry_batch(&transition.entries)?;
    validate_checkpoint_image(&transition.image)?;
    validate_checkpoint_conf_state(&transition.conf_state)?;
    let conf_state = transition.conf_state.encode_to_vec();
    let conf_state_len = u32::try_from(conf_state.len()).map_err(|_| {
        ConsensusError::InvalidState("stable membership exceeds its length field".into())
    })?;
    let image = encode_checkpoint_image(&transition.image)?;
    let image_len = u32::try_from(image.len()).map_err(|_| {
        ConsensusError::InvalidState("stable checkpoint image exceeds its length field".into())
    })?;
    let entry_count = u32::try_from(transition.entries.len()).map_err(|_| {
        ConsensusError::InvalidState("stable checkpoint has too many tail entries".into())
    })?;
    let mut capacity = CHECKPOINT_V2_FIXED_PAYLOAD_LEN
        .checked_add(conf_state.len())
        .and_then(|value| value.checked_add(image.len()))
        .ok_or_else(|| ConsensusError::InvalidState("stable checkpoint length overflow".into()))?;
    for entry in &transition.entries {
        capacity = capacity
            .checked_add(ENTRY_V2_FIXED_PAYLOAD_LEN)
            .and_then(|value| value.checked_add(entry.data.len()))
            .ok_or_else(|| {
                ConsensusError::InvalidState("stable checkpoint length overflow".into())
            })?;
    }

    let mut payload = Vec::with_capacity(capacity);
    payload.extend_from_slice(&transition.generation.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.term.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.vote.to_be_bytes());
    payload.extend_from_slice(&transition.hard_state.commit.to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.applied_index.get().to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.publishable_index.get().to_be_bytes());
    payload.extend_from_slice(&transition.checkpoint.state_digest);
    payload.extend_from_slice(&conf_state_len.to_be_bytes());
    payload.extend_from_slice(&image_len.to_be_bytes());
    payload.extend_from_slice(&entry_count.to_be_bytes());
    payload.extend_from_slice(&conf_state);
    payload.extend_from_slice(&image);
    for entry in &transition.entries {
        let data_len = u32::try_from(entry.data.len()).map_err(|_| {
            ConsensusError::InvalidState("stable entry data exceeds its length field".into())
        })?;
        payload.extend_from_slice(&entry.index.to_be_bytes());
        payload.extend_from_slice(&entry.term.to_be_bytes());
        payload.extend_from_slice(&entry.entry_type.to_be_bytes());
        payload.extend_from_slice(&data_len.to_be_bytes());
        payload.extend_from_slice(entry.data.as_ref());
    }
    Ok(payload)
}

fn decode_checkpoint_transition_v2(payload: &[u8]) -> ConsensusResult<StableCheckpointTransition> {
    if payload.len() < CHECKPOINT_V2_FIXED_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(
            "stable v2 checkpoint transition is truncated".into(),
        ));
    }
    let mut reader = Reader::new(payload);
    let generation = reader.read_u64("generation")?;
    let hard_state = HardState {
        term: reader.read_u64("HardState term")?,
        vote: reader.read_u64("HardState vote")?,
        commit: reader.read_u64("HardState commit")?,
    };
    let checkpoint = StableCheckpoint {
        applied_index: LogIndex::new(reader.read_u64("applied index")?),
        publishable_index: LogIndex::new(reader.read_u64("publishable index")?),
        state_digest: reader.read_array("state digest")?,
    };
    let conf_state_len = reader.read_u32("membership length")? as usize;
    let image_len = reader.read_u32("checkpoint image length")? as usize;
    let entry_count = reader.read_u32("tail entry count")? as usize;
    let conf_state_bytes = reader.read_slice(conf_state_len, "membership")?;
    let conf_state = ConfState::decode(conf_state_bytes).map_err(|error| {
        ConsensusError::InvalidState(format!("stable membership is invalid: {error}"))
    })?;
    if conf_state.encode_to_vec() != conf_state_bytes {
        return Err(ConsensusError::InvalidState(
            "stable membership is not canonically encoded".into(),
        ));
    }
    validate_checkpoint_conf_state(&conf_state)?;
    let image = decode_checkpoint_image(reader.read_slice(image_len, "checkpoint image")?)?;
    if entry_count > reader.remaining_len() / ENTRY_V2_FIXED_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(
            "stable v2 checkpoint tail count exceeds its remaining bytes".into(),
        ));
    }
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let index = reader.read_u64("entry index")?;
        let term = reader.read_u64("entry term")?;
        let entry_type = reader.read_i32("entry type")?;
        let data_len = reader.read_u32("entry data length")? as usize;
        let data = reader.read_slice(data_len, "entry data")?.to_vec();
        let mut entry = Entry {
            entry_type,
            term,
            index,
            ..Entry::default()
        };
        entry.data = data;
        entries.push(entry);
    }
    reader.finish("stable v2 checkpoint transition")?;
    validate_entry_batch(&entries)?;
    Ok(StableCheckpointTransition {
        generation,
        hard_state,
        checkpoint,
        conf_state,
        image,
        entries,
    })
}

fn validate_checkpoint_conf_state(conf_state: &ConfState) -> ConsensusResult<()> {
    let allowed_members = conf_state
        .voters
        .iter()
        .chain(&conf_state.learners)
        .chain(&conf_state.voters_outgoing)
        .chain(&conf_state.learners_next)
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(NodeId::new)
        .collect::<ConsensusResult<Vec<_>>>()?;
    membership::validate_conf_state(conf_state, &allowed_members)
}

#[derive(Clone, Copy, Debug)]
struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u32(&mut self, field: &str) -> ConsensusResult<u32> {
        Ok(u32::from_be_bytes(self.read_array(field)?))
    }

    fn read_i32(&mut self, field: &str) -> ConsensusResult<i32> {
        Ok(i32::from_be_bytes(self.read_array(field)?))
    }

    fn read_u64(&mut self, field: &str) -> ConsensusResult<u64> {
        Ok(u64::from_be_bytes(self.read_array(field)?))
    }

    fn read_array<const SIZE: usize>(&mut self, field: &str) -> ConsensusResult<[u8; SIZE]> {
        self.read_slice(SIZE, field)?
            .try_into()
            .map_err(|_| ConsensusError::InvalidState(format!("stable record truncates {field}")))
    }

    fn read_slice(&mut self, length: usize, field: &str) -> ConsensusResult<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            ConsensusError::InvalidState(format!("stable record {field} length overflows"))
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            ConsensusError::InvalidState(format!("stable record truncates {field}"))
        })?;
        self.offset = end;
        Ok(value)
    }

    fn remaining_len(self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn finish(self, frame: &str) -> ConsensusResult<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ConsensusError::InvalidState(format!(
                "trailing bytes after {frame}"
            )))
        }
    }
}

fn storage_error(error: impl std::fmt::Display) -> ConsensusError {
    ConsensusError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use raft::{GetEntriesContext, Storage};

    use super::*;

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    #[derive(Debug)]
    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let serial = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "epoch-consensus-stable-{}-{serial}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self { path }
        }

        fn wal_path(&self) -> PathBuf {
            self.path.join("stable.wal")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn identity_codec_has_exact_version_one_bytes() {
        let encoded = encode_record(&StableRecord::Identity(identity())).unwrap();
        let expected = [
            0x45, 0x50, 0x52, 0x53, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x03,
        ];
        assert_eq!(encoded, expected);
        assert_eq!(
            decode_record(&encoded).unwrap(),
            StableRecord::Identity(identity())
        );

        let mut trailing = encoded;
        trailing.push(0);
        assert!(matches!(
            decode_record(&trailing),
            Err(ConsensusError::InvalidState(_))
        ));
    }

    #[test]
    fn five_voter_identity_uses_bounded_v2_codec_and_reopens() {
        let identity = five_voter_identity();
        let encoded = encode_record(&StableRecord::Identity(identity.clone())).unwrap();
        assert_eq!(&encoded[6..8], &IDENTITY_V2_KIND.to_be_bytes());
        assert_eq!(encoded.len(), RECORD_HEADER_LEN + 28 + (5 * 8));
        assert_eq!(
            decode_record(&encoded).unwrap(),
            StableRecord::Identity(identity.clone())
        );

        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let recovered = DiskStableStore::open(&path, identity.clone()).unwrap();
        assert_eq!(
            recovered.storage.initial_state().unwrap().conf_state.voters,
            vec![1, 2, 3, 4, 5]
        );
        drop(recovered);
        DiskStableStore::open(&path, identity).unwrap();
    }

    #[test]
    fn three_voter_bootstrap_with_five_member_allowlist_uses_v3_and_reopens() {
        let identity = StableIdentity {
            node_id: NodeId::new(4).unwrap(),
            group_id: GroupId::new(7).unwrap(),
            group_epoch: GroupEpoch::new(9).unwrap(),
            initial_voters: (1..=3).map(|value| NodeId::new(value).unwrap()).collect(),
            voters: (1..=5).map(|value| NodeId::new(value).unwrap()).collect(),
        };
        let encoded = encode_record(&StableRecord::Identity(identity.clone())).unwrap();
        assert_eq!(&encoded[6..8], &IDENTITY_V3_KIND.to_be_bytes());
        assert_eq!(encoded.len(), RECORD_HEADER_LEN + 32 + (8 * 8));
        assert_eq!(
            decode_record(&encoded).unwrap(),
            StableRecord::Identity(identity.clone())
        );

        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let recovered = DiskStableStore::open(&path, identity.clone()).unwrap();
        assert_eq!(
            recovered.storage.initial_state().unwrap().conf_state.voters,
            vec![1, 2, 3]
        );
        drop(recovered);
        DiskStableStore::open(&path, identity).unwrap();
    }

    #[test]
    fn identity_codec_rejects_unsupported_even_voter_count() {
        let mut invalid = five_voter_identity();
        invalid.voters.pop();
        assert!(matches!(
            encode_record(&StableRecord::Identity(invalid)),
            Err(ConsensusError::InvalidVoterSet(_))
        ));
    }

    #[test]
    fn creates_and_reopens_an_empty_stable_store() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let recovered = DiskStableStore::open(&path, identity()).unwrap();
        assert_eq!(recovered.stable_generation, 0);
        assert!(!recovered.repaired_partial_tail);
        assert_eq!(recovered.checkpoint, empty_checkpoint());
        assert_eq!(recovered.storage.last_index().unwrap(), 0);
        assert_eq!(
            recovered.storage.initial_state().unwrap().conf_state,
            expected_conf_state()
        );
        drop(recovered);

        let reopened = DiskStableStore::open(&path, identity()).unwrap();
        assert_eq!(reopened.stable_generation, 0);
        assert_eq!(reopened.checkpoint, empty_checkpoint());
    }

    #[test]
    fn reopen_rejects_an_immutable_identity_mismatch() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        drop(DiskStableStore::open(&path, identity()).unwrap());

        let mut mismatched = identity();
        mismatched.group_epoch = GroupEpoch::new(10).unwrap();
        assert!(matches!(
            DiskStableStore::open(&path, mismatched),
            Err(ConsensusError::InvalidState(_))
        ));
    }

    #[test]
    fn second_writer_is_rejected_until_the_first_store_closes() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let first = DiskStableStore::open(&path, identity()).unwrap();
        assert!(matches!(
            DiskStableStore::open(&path, identity()),
            Err(ConsensusError::Storage(_))
        ));
        drop(first);
        DiskStableStore::open(&path, identity()).unwrap();
    }

    #[test]
    fn entries_hard_state_and_checkpoint_replay_together() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let recovered = DiskStableStore::open(&path, identity()).unwrap();
        let mut store = recovered.store;
        let checkpoint = checkpoint_at(1);
        let hard_state = hard_state(1, 1, 1);
        assert_eq!(
            store
                .persist(1, &hard_state, &[normal_entry(1, 1)], checkpoint)
                .unwrap(),
            1
        );
        drop(store);

        let reopened = DiskStableStore::open(&path, identity()).unwrap();
        assert_eq!(reopened.stable_generation, 1);
        assert_eq!(reopened.checkpoint, checkpoint);
        assert_eq!(
            reopened.storage.initial_state().unwrap().hard_state,
            hard_state
        );
        assert_eq!(reopened.storage.last_index().unwrap(), 1);
        assert_eq!(
            reopened
                .storage
                .entries(1, 2, None, GetEntriesContext::empty(false))
                .unwrap(),
            vec![normal_entry(1, 1)]
        );
    }

    #[test]
    fn uncommitted_suffix_is_replaced_logically_during_replay() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let recovered = DiskStableStore::open(&path, identity()).unwrap();
        let mut store = recovered.store;
        store
            .persist(
                1,
                &hard_state(1, 1, 1),
                &[normal_entry(1, 1), normal_entry(2, 1)],
                checkpoint_at(1),
            )
            .unwrap();
        store
            .persist(
                2,
                &hard_state(2, 2, 1),
                &[normal_entry(2, 2)],
                checkpoint_at(1),
            )
            .unwrap();
        drop(store);

        let reopened = DiskStableStore::open(&path, identity()).unwrap();
        let entries = reopened
            .storage
            .entries(1, 3, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(entries, vec![normal_entry(1, 1), normal_entry(2, 2)]);
        assert_eq!(reopened.storage.term(2).unwrap(), 2);
        assert_eq!(reopened.stable_generation, 2);
    }

    #[test]
    fn checkpoint_record_reopens_a_compacted_prefix_and_contiguous_tail() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let recovered = DiskStableStore::open(&path, identity()).unwrap();
        let mut store = recovered.store;
        store
            .persist(
                1,
                &hard_state(1, 1, 1),
                &[normal_entry(1, 1), normal_entry(2, 1)],
                checkpoint_at(1),
            )
            .unwrap();
        let image = empty_image_at(1, 1);
        store
            .persist_checkpoint(
                2,
                &hard_state(1, 1, 1),
                &expected_conf_state(),
                &image,
                &[normal_entry(2, 1)],
                checkpoint_at(1),
            )
            .unwrap();
        drop(store);

        let reopened = DiskStableStore::open(&path, identity()).unwrap();
        assert_eq!(reopened.storage.first_index().unwrap(), 2);
        assert_eq!(reopened.storage.term(1).unwrap(), 1);
        assert_eq!(reopened.storage.last_index().unwrap(), 2);
        assert_eq!(
            reopened
                .storage
                .entries(2, 3, None, GetEntriesContext::empty(false))
                .unwrap(),
            vec![normal_entry(2, 1)]
        );
        assert_eq!(reopened.checkpoint, checkpoint_at(1));

        let mut store = reopened.store;
        store
            .persist(3, &hard_state(1, 1, 2), &[], checkpoint_at(2))
            .unwrap();
        drop(store);
        let reopened = DiskStableStore::open(&path, identity()).unwrap();
        assert_eq!(reopened.checkpoint.applied_index, LogIndex::new(2));
        assert_eq!(reopened.storage.first_index().unwrap(), 2);
        assert_eq!(reopened.storage.last_index().unwrap(), 2);
    }

    #[test]
    fn checkpoint_physically_reclaims_old_generations_and_keeps_logical_generation() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let recovered = DiskStableStore::open(&path, identity()).unwrap();
        let mut store = recovered.store;
        store
            .persist(
                1,
                &hard_state(1, 1, 1),
                &[normal_entry(1, 1)],
                checkpoint_at(1),
            )
            .unwrap();
        for generation in 2..=128_u64 {
            store
                .persist(
                    generation,
                    &hard_state(generation, 1, 1),
                    &[normal_entry(generation, generation)],
                    checkpoint_at(1),
                )
                .unwrap();
        }
        let before = fs::metadata(&path).unwrap().len();
        let image = empty_image_at(1, 1);
        let tail = (2..=128_u64)
            .map(|index| normal_entry(index, index))
            .collect::<Vec<_>>();

        assert_eq!(
            store
                .persist_checkpoint(
                    129,
                    &hard_state(128, 1, 1),
                    &expected_conf_state(),
                    &image,
                    &tail,
                    checkpoint_at(1),
                )
                .unwrap(),
            129
        );
        let after = fs::metadata(&path).unwrap().len();
        assert!(after < before, "expected {after} to be below {before}");
        drop(store);

        let reopened = DiskStableStore::open(&path, identity()).unwrap();
        assert_eq!(reopened.stable_generation, 129);
        assert_eq!(reopened.storage.first_index().unwrap(), 2);
        assert_eq!(reopened.storage.last_index().unwrap(), 128);
        let mut store = reopened.store;
        assert_eq!(
            store
                .persist(
                    130,
                    &hard_state(129, 1, 1),
                    &[normal_entry(129, 129)],
                    checkpoint_at(1),
                )
                .unwrap(),
            130
        );
        let second_tail = (2..=129_u64)
            .map(|index| normal_entry(index, index))
            .collect::<Vec<_>>();
        assert_eq!(
            store
                .persist_checkpoint(
                    131,
                    &hard_state(129, 1, 1),
                    &expected_conf_state(),
                    &image,
                    &second_tail,
                    checkpoint_at(1),
                )
                .unwrap(),
            131
        );
        drop(store);
        assert_eq!(
            DiskStableStore::open(&path, identity())
                .unwrap()
                .stable_generation,
            131
        );
    }

    #[test]
    fn compacted_checkpoint_kind_is_rejected_outside_the_baseline_position() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let mut wal = FileWal::open(&path).unwrap();
        let identity_record = encode_record(&StableRecord::Identity(identity())).unwrap();
        wal.append(0, &identity_record, true).unwrap();
        let transition = StableCheckpointTransition {
            generation: 1,
            hard_state: hard_state(1, 1, 1),
            checkpoint: checkpoint_at(1),
            conf_state: ConfState::default(),
            image: empty_image_at(1, 1),
            entries: Vec::new(),
        };
        let ordinary = encode_record(&StableRecord::Checkpoint(transition.clone())).unwrap();
        wal.append(0, &ordinary, true).unwrap();
        let misplaced = encode_record(&StableRecord::CompactedCheckpoint(
            StableCheckpointTransition {
                generation: 2,
                ..transition
            },
        ))
        .unwrap();
        wal.append(0, &misplaced, true).unwrap();
        drop(wal);

        assert!(matches!(
            DiskStableStore::open(&path, identity()),
            Err(ConsensusError::InvalidState(_))
        ));
    }

    #[test]
    fn checkpoint_codec_rejects_digest_corruption_and_noncontiguous_tail() {
        let image = empty_image_at(1, 1);
        let record = StableRecord::Checkpoint(StableCheckpointTransition {
            generation: 1,
            hard_state: hard_state(1, 1, 1),
            checkpoint: checkpoint_at(1),
            conf_state: ConfState::default(),
            image: image.clone(),
            entries: Vec::new(),
        });
        let mut encoded = encode_record(&record).unwrap();
        let image_digest_offset = RECORD_HEADER_LEN + CHECKPOINT_FIXED_PAYLOAD_LEN + 40;
        encoded[image_digest_offset] ^= 0xff;
        assert!(decode_record(&encoded).is_err());

        let directory = TestDirectory::new();
        let recovered = DiskStableStore::open(&directory.wal_path(), identity()).unwrap();
        let mut store = recovered.store;
        store
            .persist(
                1,
                &hard_state(1, 1, 1),
                &[normal_entry(1, 1)],
                checkpoint_at(1),
            )
            .unwrap();
        assert!(matches!(
            store.persist_checkpoint(
                2,
                &hard_state(1, 1, 1),
                &expected_conf_state(),
                &image,
                &[normal_entry(3, 1)],
                checkpoint_at(1),
            ),
            Err(ConsensusError::InvalidState(_))
        ));
        assert_eq!(store.stable_generation(), 1);
    }

    #[test]
    fn checkpoint_transition_rejects_a_canonical_but_divergent_applied_prefix() {
        let previous = image_with_proposal(1, b"original");
        let conflicting = image_with_proposal(1, b"different");
        let previous_checkpoint = StableCheckpoint {
            applied_index: previous.index,
            publishable_index: previous.index,
            state_digest: previous.state_digest,
        };
        let next_checkpoint = StableCheckpoint {
            applied_index: conflicting.index,
            publishable_index: conflicting.index,
            state_digest: conflicting.state_digest,
        };
        let previous_hard_state = hard_state(1, 1, 1);
        let previous_conf_state = expected_conf_state();
        let previous_state = PriorState {
            hard_state: &previous_hard_state,
            snapshot: Some(&previous),
            entries: &[],
            conf_state: &previous_conf_state,
            checkpoint: previous_checkpoint,
        };

        assert!(matches!(
            prepare_checkpoint_transition(
                &identity(),
                previous_state,
                hard_state(1, 1, 1),
                expected_conf_state(),
                conflicting,
                &[],
                next_checkpoint,
            ),
            Err(ConsensusError::InvalidState(_))
        ));
    }

    #[test]
    fn reopen_repairs_only_a_partial_outer_wal_tail() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let recovered = DiskStableStore::open(&path, identity()).unwrap();
        let mut store = recovered.store;
        store
            .persist(
                1,
                &hard_state(1, 1, 1),
                &[normal_entry(1, 1)],
                checkpoint_at(1),
            )
            .unwrap();
        drop(store);
        let stable_len = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"EPCHpartial")
            .unwrap();

        let reopened = DiskStableStore::open(&path, identity()).unwrap();
        assert!(reopened.repaired_partial_tail);
        assert_eq!(reopened.stable_generation, 1);
        assert_eq!(fs::metadata(path).unwrap().len(), stable_len);
    }

    #[test]
    fn reopen_rejects_outer_wal_checksum_corruption() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let recovered = DiskStableStore::open(&path, identity()).unwrap();
        let mut store = recovered.store;
        store
            .persist(
                1,
                &hard_state(1, 1, 1),
                &[normal_entry(1, 1)],
                checkpoint_at(1),
            )
            .unwrap();
        drop(store);

        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            DiskStableStore::open(&path, identity()),
            Err(ConsensusError::Storage(_))
        ));
    }

    #[test]
    fn safety_regressions_are_rejected_before_the_wal_advances() {
        let directory = TestDirectory::new();
        let path = directory.wal_path();
        let recovered = DiskStableStore::open(&path, identity()).unwrap();
        let mut store = recovered.store;
        assert!(matches!(
            store.persist(1, &HardState::default(), &[], empty_checkpoint()),
            Err(ConsensusError::InvalidState(_))
        ));

        store
            .persist(
                1,
                &hard_state(1, 1, 1),
                &[normal_entry(1, 1), normal_entry(2, 1)],
                checkpoint_at(1),
            )
            .unwrap();
        assert!(matches!(
            store.persist(
                2,
                &hard_state(2, 2, 1),
                &[normal_entry(1, 2)],
                checkpoint_at(1)
            ),
            Err(ConsensusError::InvalidState(_))
        ));
        assert_eq!(store.stable_generation(), 1);

        let mut membership = normal_entry(3, 2);
        membership.entry_type = EntryType::EntryConfChange as i32;
        assert!(matches!(
            store.persist(2, &hard_state(2, 2, 1), &[membership], checkpoint_at(1)),
            Err(ConsensusError::Unsupported(_))
        ));
        assert_eq!(store.stable_generation(), 1);
        assert_eq!(store.checkpoint(), checkpoint_at(1));
    }

    fn identity() -> StableIdentity {
        let voters = vec![
            NodeId::new(1).unwrap(),
            NodeId::new(2).unwrap(),
            NodeId::new(3).unwrap(),
        ];
        StableIdentity {
            node_id: NodeId::new(1).unwrap(),
            group_id: GroupId::new(7).unwrap(),
            group_epoch: GroupEpoch::new(9).unwrap(),
            initial_voters: voters.clone(),
            voters,
        }
    }

    fn five_voter_identity() -> StableIdentity {
        let voters = (1..=5)
            .map(|value| NodeId::new(value).unwrap())
            .collect::<Vec<_>>();
        StableIdentity {
            node_id: NodeId::new(1).unwrap(),
            group_id: GroupId::new(7).unwrap(),
            group_epoch: GroupEpoch::new(9).unwrap(),
            initial_voters: voters.clone(),
            voters,
        }
    }

    fn empty_checkpoint() -> StableCheckpoint {
        StableCheckpoint::empty(&identity()).unwrap()
    }

    fn checkpoint_at(index: u64) -> StableCheckpoint {
        StableCheckpoint {
            applied_index: LogIndex::new(index),
            publishable_index: LogIndex::new(index),
            state_digest: empty_checkpoint().state_digest,
        }
    }

    fn hard_state(term: u64, vote: u64, commit: u64) -> HardState {
        HardState { term, vote, commit }
    }

    fn normal_entry(index: u64, term: u64) -> Entry {
        Entry {
            entry_type: EntryType::EntryNormal as i32,
            term,
            index,
            ..Entry::default()
        }
    }

    fn empty_image_at(index: u64, term: u64) -> CheckpointImage {
        CheckpointImage {
            group_id: identity().group_id,
            group_epoch: identity().group_epoch,
            index: LogIndex::new(index),
            term: Term::new(term),
            state_digest: empty_checkpoint().state_digest,
            applied_command_count: 0,
            applied: Vec::new(),
            application_snapshot: None,
        }
    }

    fn image_with_proposal(proposal_id: u64, payload: &[u8]) -> CheckpointImage {
        let applied = vec![CommittedProposal {
            receipt: CommitReceipt {
                group_id: identity().group_id,
                group_epoch: identity().group_epoch,
                proposal_id: ProposalId::new(proposal_id).unwrap(),
                term: Term::new(1),
                log_index: LogIndex::new(1),
            },
            payload: payload.to_vec(),
        }];
        CheckpointImage {
            group_id: identity().group_id,
            group_epoch: identity().group_epoch,
            index: LogIndex::new(1),
            term: Term::new(1),
            state_digest: compute_state_digest(
                identity().group_id,
                identity().group_epoch,
                &applied,
            )
            .unwrap(),
            applied_command_count: u64::try_from(applied.len()).unwrap(),
            applied,
            application_snapshot: None,
        }
    }

    fn expected_conf_state() -> ConfState {
        ConfState::from((vec![1, 2, 3], Vec::<u64>::new()))
    }
}
