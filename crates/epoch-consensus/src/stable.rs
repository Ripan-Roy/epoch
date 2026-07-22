//! Disk-backed stable-state journal for the fixed-voter consensus adapter.
//!
//! The journal is intentionally narrower than a snapshot-capable Raft store.
//! Record zero fixes the immutable group identity. Every later record contains
//! a complete `HardState`, an Epoch checkpoint, and an optional contiguous batch
//! of normal entries. The outer [`FileWal`] supplies the durable, checksummed
//! append boundary; this module supplies canonical Epoch-owned framing and
//! logical suffix replacement during replay.

use std::{collections::BTreeMap, fmt, path::Path};

use epoch_storage::{CommitLog, FileWal};
use raft::{
    prelude::{ConfState, Entry, EntryType, HardState},
    storage::MemStorage,
};

use super::{
    CommitReceipt, CommittedProposal, ConsensusError, ConsensusResult, GroupEpoch, GroupId,
    LogIndex, NodeId, ProposalId, StateDigest, Term, compute_state_digest, decode_command,
    validate_command_scope, validate_hard_state, validate_log_order, validate_voters,
};

const RECORD_MAGIC: [u8; 4] = *b"EPRS";
const RECORD_VERSION: u16 = 1;
const RECORD_HEADER_LEN: usize = 12;
const IDENTITY_KIND: u16 = 1;
const TRANSITION_KIND: u16 = 2;
const IDENTITY_PAYLOAD_LEN: usize = 48;
const TRANSITION_FIXED_PAYLOAD_LEN: usize = 84;
const ENTRY_FIXED_PAYLOAD_LEN: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StableIdentity {
    pub(crate) node_id: NodeId,
    pub(crate) group_id: GroupId,
    pub(crate) group_epoch: GroupEpoch,
    pub(crate) voters: [NodeId; 3],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StableCheckpoint {
    pub(crate) applied_index: LogIndex,
    pub(crate) publishable_index: LogIndex,
    pub(crate) state_digest: StateDigest,
}

impl StableCheckpoint {
    pub(crate) fn empty(identity: StableIdentity) -> ConsensusResult<Self> {
        Ok(Self {
            applied_index: LogIndex::ZERO,
            publishable_index: LogIndex::ZERO,
            state_digest: compute_state_digest(identity.group_id, identity.group_epoch, &[])?,
        })
    }
}

pub(crate) struct RecoveredDiskState {
    pub(crate) store: DiskStableStore,
    pub(crate) storage: MemStorage,
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
    entries: Vec<Entry>,
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
enum StableRecord {
    Identity(StableIdentity),
    Transition(StableTransition),
}

impl DiskStableStore {
    pub(crate) fn open(
        path: &Path,
        identity: StableIdentity,
    ) -> ConsensusResult<RecoveredDiskState> {
        validate_voters(identity.node_id, identity.voters)?;
        let mut wal = FileWal::open(path).map_err(storage_error)?;
        let repaired_partial_tail = wal.recovered_partial_tail();
        let records = wal.records_from(0, usize::MAX);

        if records.is_empty() {
            let encoded = encode_record(&StableRecord::Identity(identity))?;
            let record = wal.append(0, &encoded, true).map_err(storage_error)?;
            if record.sequence != 0 {
                return Err(ConsensusError::InvalidState(format!(
                    "stable identity was written at WAL sequence {}; expected 0",
                    record.sequence
                )));
            }
        } else {
            let first = &records[0];
            if first.sequence != 0 || first.timestamp_ms != 0 {
                return Err(ConsensusError::InvalidState(
                    "stable identity must be WAL sequence zero with timestamp zero".into(),
                ));
            }
            match decode_record(&first.payload)? {
                StableRecord::Identity(stored) if stored == identity => {}
                StableRecord::Identity(stored) => {
                    return Err(ConsensusError::InvalidState(format!(
                        "stable identity mismatch: stored {stored:?}, requested {identity:?}"
                    )));
                }
                StableRecord::Transition(_) => {
                    return Err(ConsensusError::InvalidState(
                        "stable WAL sequence zero is not an identity record".into(),
                    ));
                }
            }
        }

        let mut hard_state = HardState::default();
        let mut entries = Vec::new();
        let mut checkpoint = StableCheckpoint::empty(identity)?;
        let mut stable_generation = 0_u64;

        for record in records.iter().skip(1) {
            if record.timestamp_ms != 0 {
                return Err(ConsensusError::InvalidState(format!(
                    "stable WAL generation {} has a nonzero timestamp",
                    record.sequence
                )));
            }
            let StableRecord::Transition(transition) = decode_record(&record.payload)? else {
                return Err(ConsensusError::InvalidState(format!(
                    "stable WAL generation {} repeats the identity record",
                    record.sequence
                )));
            };
            let expected_generation = stable_generation
                .checked_add(1)
                .ok_or_else(|| ConsensusError::InvalidState("stable generation overflow".into()))?;
            if record.sequence != expected_generation
                || transition.generation != expected_generation
            {
                return Err(ConsensusError::InvalidState(format!(
                    "stable generation mismatch: WAL sequence {}, record generation {}, expected {expected_generation}",
                    record.sequence, transition.generation
                )));
            }
            let candidate = prepare_transition(
                identity,
                &hard_state,
                &entries,
                checkpoint,
                transition.hard_state,
                &transition.entries,
                transition.checkpoint,
            )?;
            hard_state = candidate.hard_state;
            entries = candidate.entries;
            checkpoint = candidate.checkpoint;
            stable_generation = expected_generation;
        }

        let applied = derive_applied_history(identity, &entries, checkpoint.applied_index)?;
        let storage = materialize_storage(identity, &hard_state, &entries)?;
        let store = Self {
            wal,
            identity,
            hard_state,
            entries,
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
            self.identity,
            &self.hard_state,
            &self.entries,
            self.checkpoint,
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
        let record = self.wal.append(0, &encoded, true).map_err(storage_error)?;
        if record.sequence != expected_generation {
            return Err(ConsensusError::InvalidState(format!(
                "stable WAL returned sequence {}; expected generation {expected_generation}",
                record.sequence
            )));
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_after_next_append) {
            return Err(ConsensusError::Storage(
                "injected failure after stable WAL fsync and before cache mutation".into(),
            ));
        }

        self.hard_state = candidate.hard_state;
        self.entries = candidate.entries;
        self.checkpoint = candidate.checkpoint;
        self.stable_generation = expected_generation;
        Ok(record.sequence)
    }

    pub(crate) const fn stable_generation(&self) -> u64 {
        self.stable_generation
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
    entries: Vec<Entry>,
    checkpoint: StableCheckpoint,
}

fn prepare_transition(
    identity: StableIdentity,
    previous_hard_state: &HardState,
    previous_entries: &[Entry],
    previous_checkpoint: StableCheckpoint,
    hard_state: HardState,
    new_entries: &[Entry],
    checkpoint: StableCheckpoint,
) -> ConsensusResult<CandidateState> {
    validate_entry_batch(new_entries)?;
    validate_hard_state_transition(identity, previous_hard_state, &hard_state)?;
    validate_checkpoint_transition(previous_checkpoint, checkpoint)?;

    let mut entries = previous_entries.to_vec();
    if let Some(first) = new_entries.first() {
        if first.index <= previous_hard_state.commit {
            return Err(ConsensusError::InvalidState(format!(
                "stable update would overwrite committed index {} with an entry beginning at {}",
                previous_hard_state.commit, first.index
            )));
        }
        let expected_next = u64::try_from(entries.len())
            .map_err(|_| ConsensusError::InvalidState("stable log is too large".into()))?
            .checked_add(1)
            .ok_or_else(|| ConsensusError::InvalidState("stable log index overflow".into()))?;
        if first.index > expected_next {
            return Err(ConsensusError::InvalidState(format!(
                "stable entry batch begins at {} after last index {}; gaps are not supported",
                first.index,
                expected_next - 1
            )));
        }
        let retained = usize::try_from(first.index - 1)
            .map_err(|_| ConsensusError::InvalidState("stable log index is too large".into()))?;
        entries.truncate(retained);
        entries.extend_from_slice(new_entries);
    }

    validate_complete_state(identity, &hard_state, &entries, checkpoint)?;
    Ok(CandidateState {
        hard_state,
        entries,
        checkpoint,
    })
}

fn validate_hard_state_transition(
    identity: StableIdentity,
    previous: &HardState,
    next: &HardState,
) -> ConsensusResult<()> {
    validate_hard_state(next, identity.voters)?;
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
        validate_normal_entry(entry)?;
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

fn validate_normal_entry(entry: &Entry) -> ConsensusResult<()> {
    if entry.entry_type != EntryType::EntryNormal as i32 {
        return Err(ConsensusError::Unsupported(
            "membership entries are not supported by the fixed-voter stable store".into(),
        ));
    }
    if entry.index == 0 || entry.term == 0 {
        return Err(ConsensusError::InvalidState(
            "stable normal entries require nonzero index and term".into(),
        ));
    }
    if !entry.context.is_empty() || entry.sync_log {
        return Err(ConsensusError::Unsupported(
            "stable v1 entries require empty context and deprecated sync_log=false".into(),
        ));
    }
    Ok(())
}

fn validate_complete_state(
    identity: StableIdentity,
    hard_state: &HardState,
    entries: &[Entry],
    checkpoint: StableCheckpoint,
) -> ConsensusResult<()> {
    validate_voters(identity.node_id, identity.voters)?;
    validate_hard_state(hard_state, identity.voters)?;
    let last_index = entries.last().map_or(0, |entry| entry.index);
    validate_log_order(entries, last_index, hard_state.term)?;
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
    if checkpoint.applied_index != checkpoint.publishable_index {
        return Err(ConsensusError::InvalidState(
            "stable v1 checkpoint applied and publishable indexes differ".into(),
        ));
    }

    let applied = derive_applied_history(identity, entries, checkpoint.applied_index)?;
    let expected_digest = compute_state_digest(identity.group_id, identity.group_epoch, &applied)?;
    if checkpoint.state_digest != expected_digest {
        return Err(ConsensusError::InvalidState(
            "stable checkpoint digest does not match its applied log history".into(),
        ));
    }
    Ok(())
}

fn derive_applied_history(
    identity: StableIdentity,
    entries: &[Entry],
    applied_index: LogIndex,
) -> ConsensusResult<Vec<CommittedProposal>> {
    let mut seen = BTreeMap::<ProposalId, Vec<u8>>::new();
    let mut applied = Vec::new();

    for entry in entries {
        validate_normal_entry(entry)?;
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
    identity: StableIdentity,
    hard_state: &HardState,
    entries: &[Entry],
) -> ConsensusResult<MemStorage> {
    let storage = MemStorage::new_with_conf_state(ConfState::from((
        identity
            .voters
            .iter()
            .map(|voter| voter.get())
            .collect::<Vec<_>>(),
        Vec::<u64>::new(),
    )));
    {
        let mut core = storage.wl();
        core.append(entries)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?;
        core.set_hardstate(hard_state.clone());
    }
    Ok(storage)
}

fn encode_record(record: &StableRecord) -> ConsensusResult<Vec<u8>> {
    let (kind, payload) = match record {
        StableRecord::Identity(identity) => (IDENTITY_KIND, encode_identity(*identity)),
        StableRecord::Transition(transition) => (TRANSITION_KIND, encode_transition(transition)?),
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
        IDENTITY_KIND => StableRecord::Identity(decode_identity(payload)?),
        TRANSITION_KIND => StableRecord::Transition(decode_transition(payload)?),
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

fn encode_identity(identity: StableIdentity) -> Vec<u8> {
    let mut payload = Vec::with_capacity(IDENTITY_PAYLOAD_LEN);
    payload.extend_from_slice(&identity.node_id.get().to_be_bytes());
    payload.extend_from_slice(&identity.group_id.get().to_be_bytes());
    payload.extend_from_slice(&identity.group_epoch.get().to_be_bytes());
    for voter in identity.voters {
        payload.extend_from_slice(&voter.get().to_be_bytes());
    }
    payload
}

fn decode_identity(payload: &[u8]) -> ConsensusResult<StableIdentity> {
    if payload.len() != IDENTITY_PAYLOAD_LEN {
        return Err(ConsensusError::InvalidState(format!(
            "stable identity payload is {} bytes; expected {IDENTITY_PAYLOAD_LEN}",
            payload.len()
        )));
    }
    let mut reader = Reader::new(payload);
    let identity = StableIdentity {
        node_id: NodeId::new(reader.read_u64("node ID")?)?,
        group_id: GroupId::new(reader.read_u64("group ID")?)?,
        group_epoch: GroupEpoch::new(reader.read_u64("group epoch")?)?,
        voters: [
            NodeId::new(reader.read_u64("voter ID")?)?,
            NodeId::new(reader.read_u64("voter ID")?)?,
            NodeId::new(reader.read_u64("voter ID")?)?,
        ],
    };
    reader.finish("stable identity")?;
    validate_voters(identity.node_id, identity.voters)?;
    Ok(identity)
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
        StableIdentity {
            node_id: NodeId::new(1).unwrap(),
            group_id: GroupId::new(7).unwrap(),
            group_epoch: GroupEpoch::new(9).unwrap(),
            voters: [
                NodeId::new(1).unwrap(),
                NodeId::new(2).unwrap(),
                NodeId::new(3).unwrap(),
            ],
        }
    }

    fn empty_checkpoint() -> StableCheckpoint {
        StableCheckpoint::empty(identity()).unwrap()
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

    fn expected_conf_state() -> ConfState {
        ConfState::from((vec![1, 2, 3], Vec::<u64>::new()))
    }
}
