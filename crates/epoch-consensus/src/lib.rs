//! Epoch-owned consensus types and an isolated `raft-rs` feasibility adapter.
//!
//! [`InMemoryRaftAdapter`] deliberately uses `raft::storage::MemStorage`. Its
//! memory-store barriers model the ordering required of a future durable
//! implementation, but they are not durable writes and must not be presented as
//! an acknowledgement boundary. Snapshots remain disabled until Epoch has a
//! state-machine checkpoint format that can be installed atomically with Raft
//! metadata.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display, Formatter},
};

use prost::Message as ProstMessage;
use raft::{
    Config, GetEntriesContext, RawNode, StateRole, Storage,
    prelude::{ConfState, Entry, EntryType, HardState, Message as RaftMessage, MessageType},
    storage::MemStorage,
};
use sha2::{Digest, Sha256};
use slog::{Logger, o};

const COMMAND_MAGIC: [u8; 4] = *b"EPCM";
const COMMAND_VERSION: u16 = 1;
const COMMAND_HEADER_LEN: usize = 34;
const PEER_MESSAGE_MAGIC: [u8; 4] = *b"EPPM";
const PEER_MESSAGE_VERSION: u16 = 1;
const PEER_MESSAGE_HEADER_LEN: usize = 50;
const STATE_DIGEST_MAGIC: [u8; 4] = *b"EPDG";
const STATE_DIGEST_VERSION: u16 = 1;
const HEARTBEAT_TICK: usize = 2;
const ELECTION_TICK: usize = 10;

/// Maximum accepted size of a complete canonical Epoch peer-message frame.
pub const MAX_PEER_MESSAGE_WIRE_BYTES: usize = 1024 * 1024;
/// Maximum command payload accepted before it enters `RawNode`.
pub const MAX_PROPOSAL_PAYLOAD_BYTES: usize = 512 * 1024;

/// SHA-256 over the canonically framed applied Epoch state history.
pub type StateDigest = [u8; 32];

macro_rules! nonzero_id {
    ($name:ident, $label:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            pub fn new(value: u64) -> ConsensusResult<Self> {
                if value == 0 {
                    Err(ConsensusError::InvalidIdentifier($label))
                } else {
                    Ok(Self(value))
                }
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                Display::fmt(&self.0, formatter)
            }
        }
    };
}

nonzero_id!(NodeId, "node ID must be non-zero");
nonzero_id!(GroupId, "group ID must be non-zero");
nonzero_id!(GroupEpoch, "group epoch must be non-zero");
nonzero_id!(ProposalId, "proposal ID must be non-zero");

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Term(u64);

impl Term {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl Display for Term {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogIndex(u64);

impl LogIndex {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl Display for LogIndex {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusRole {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub group_id: GroupId,
    pub group_epoch: GroupEpoch,
    pub expected_term: Term,
    pub proposal_id: ProposalId,
    pub payload: Vec<u8>,
}

impl Proposal {
    pub fn new(
        group_id: GroupId,
        group_epoch: GroupEpoch,
        expected_term: Term,
        proposal_id: ProposalId,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            group_id,
            group_epoch,
            expected_term,
            proposal_id,
            payload: payload.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitReceipt {
    pub group_id: GroupId,
    pub group_epoch: GroupEpoch,
    pub proposal_id: ProposalId,
    pub term: Term,
    pub log_index: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedProposal {
    pub receipt: CommitReceipt,
    pub payload: Vec<u8>,
}

/// Result of looking up an idempotency key in the persisted Raft log and the
/// applied Epoch state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalLookup {
    Unknown,
    Pending,
    Committed(CommittedProposal),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsensusStatus {
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub group_epoch: GroupEpoch,
    pub role: ConsensusRole,
    pub leader_id: Option<NodeId>,
    pub term: Term,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub voter_count: usize,
    pub fail_stopped: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub struct PeerMessage {
    group_id: GroupId,
    group_epoch: GroupEpoch,
    from: NodeId,
    to: NodeId,
    term: Term,
    encoded: Vec<u8>,
}

impl PeerMessage {
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub const fn group_epoch(&self) -> GroupEpoch {
        self.group_epoch
    }

    pub const fn from(&self) -> NodeId {
        self.from
    }

    pub const fn to(&self) -> NodeId {
        self.to
    }

    pub const fn term(&self) -> Term {
        self.term
    }

    /// Encodes a bounded, canonical Epoch envelope around the private Raft
    /// message.
    pub fn to_wire(&self) -> ConsensusResult<Vec<u8>> {
        validate_embedded_message(self)?;
        let encoded_len = u32::try_from(self.encoded.len()).map_err(|_| {
            ConsensusError::InvalidMessage("peer-message payload exceeds the v1 frame limit".into())
        })?;
        let frame_len = PEER_MESSAGE_HEADER_LEN
            .checked_add(self.encoded.len())
            .ok_or_else(|| ConsensusError::InvalidMessage("peer-message length overflow".into()))?;
        if frame_len > MAX_PEER_MESSAGE_WIRE_BYTES {
            return Err(ConsensusError::InvalidMessage(format!(
                "peer-message frame is {frame_len} bytes; maximum is {MAX_PEER_MESSAGE_WIRE_BYTES}"
            )));
        }

        let mut frame = Vec::with_capacity(frame_len);
        frame.extend_from_slice(&PEER_MESSAGE_MAGIC);
        frame.extend_from_slice(&PEER_MESSAGE_VERSION.to_be_bytes());
        frame.extend_from_slice(&self.group_id.get().to_be_bytes());
        frame.extend_from_slice(&self.group_epoch.get().to_be_bytes());
        frame.extend_from_slice(&self.from.get().to_be_bytes());
        frame.extend_from_slice(&self.to.get().to_be_bytes());
        frame.extend_from_slice(&self.term.get().to_be_bytes());
        frame.extend_from_slice(&encoded_len.to_be_bytes());
        frame.extend_from_slice(&self.encoded);
        Ok(frame)
    }

    /// Decodes and validates a canonical Epoch peer-message frame for the
    /// supplied local destination. Group membership is additionally checked by
    /// [`InMemoryRaftAdapter::receive`].
    pub fn from_wire(encoded: &[u8], expected_destination: NodeId) -> ConsensusResult<Self> {
        if encoded.len() > MAX_PEER_MESSAGE_WIRE_BYTES {
            return Err(ConsensusError::InvalidMessage(format!(
                "peer-message frame is {} bytes; maximum is {MAX_PEER_MESSAGE_WIRE_BYTES}",
                encoded.len()
            )));
        }
        if encoded.len() < PEER_MESSAGE_HEADER_LEN || encoded[..4] != PEER_MESSAGE_MAGIC {
            return Err(ConsensusError::InvalidMessage(
                "peer-message frame has an invalid header".into(),
            ));
        }
        let version = u16::from_be_bytes([encoded[4], encoded[5]]);
        if version != PEER_MESSAGE_VERSION {
            return Err(ConsensusError::InvalidMessage(format!(
                "unsupported peer-message version {version}"
            )));
        }

        let message = Self {
            group_id: GroupId::new(read_u64(encoded, 6, "peer-message")?)?,
            group_epoch: GroupEpoch::new(read_u64(encoded, 14, "peer-message")?)?,
            from: NodeId::new(read_u64(encoded, 22, "peer-message")?)?,
            to: NodeId::new(read_u64(encoded, 30, "peer-message")?)?,
            term: Term::new(read_u64(encoded, 38, "peer-message")?),
            encoded: Vec::new(),
        };
        if message.to != expected_destination {
            return Err(ConsensusError::InvalidMessage(format!(
                "peer-message for node {} was decoded by node {expected_destination}",
                message.to
            )));
        }
        if message.from == message.to {
            return Err(ConsensusError::InvalidMessage(
                "self-addressed peer messages are not transport messages".into(),
            ));
        }
        let payload_len =
            u32::from_be_bytes(encoded[46..50].try_into().map_err(|_| {
                ConsensusError::InvalidMessage("invalid peer-message length".into())
            })?) as usize;
        let expected_len = PEER_MESSAGE_HEADER_LEN
            .checked_add(payload_len)
            .ok_or_else(|| ConsensusError::InvalidMessage("peer-message length overflow".into()))?;
        if encoded.len() != expected_len {
            return Err(ConsensusError::InvalidMessage(
                "peer-message payload length does not match its frame".into(),
            ));
        }
        let message = Self {
            encoded: encoded[PEER_MESSAGE_HEADER_LEN..].to_vec(),
            ..message
        };
        validate_embedded_message(&message)?;
        Ok(message)
    }
}

impl fmt::Debug for PeerMessage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerMessage")
            .field("group_id", &self.group_id)
            .field("group_epoch", &self.group_epoch)
            .field("from", &self.from)
            .field("to", &self.to)
            .field("term", &self.term)
            .field("encoded_len", &self.encoded.len())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsensusOutput {
    pub messages: Vec<PeerMessage>,
    pub commits: Vec<CommittedProposal>,
    pub status: ConsensusStatus,
}

impl ConsensusOutput {
    pub fn is_idle(&self) -> bool {
        self.messages.is_empty() && self.commits.is_empty()
    }
}

pub trait ConsensusAdapter {
    fn status(&self) -> ConsensusStatus;

    fn campaign(&mut self) -> ConsensusResult<ConsensusOutput>;

    fn tick(&mut self) -> ConsensusResult<ConsensusOutput>;

    fn propose(&mut self, proposal: Proposal) -> ConsensusResult<ConsensusOutput>;

    fn receive(&mut self, message: PeerMessage) -> ConsensusResult<ConsensusOutput>;

    fn transfer_leadership(&mut self, target: NodeId) -> ConsensusResult<ConsensusOutput>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsensusError {
    InvalidIdentifier(&'static str),
    InvalidVoterSet(String),
    InvalidState(String),
    GroupMismatch {
        expected: GroupId,
        observed: GroupId,
    },
    FencedEpoch {
        expected: GroupEpoch,
        observed: GroupEpoch,
    },
    StaleTerm {
        current: Term,
        observed: Term,
    },
    NotLeader {
        leader_hint: Option<NodeId>,
    },
    DuplicateProposal(ProposalId),
    ConflictingProposal(ProposalId),
    Poisoned(String),
    InvalidMessage(String),
    Storage(String),
    Library(String),
    Unsupported(String),
}

impl Display for ConsensusError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentifier(message) => formatter.write_str(message),
            Self::InvalidVoterSet(message)
            | Self::InvalidState(message)
            | Self::Poisoned(message)
            | Self::InvalidMessage(message)
            | Self::Storage(message)
            | Self::Library(message)
            | Self::Unsupported(message) => formatter.write_str(message),
            Self::GroupMismatch { expected, observed } => {
                write!(
                    formatter,
                    "group {observed} does not match local group {expected}"
                )
            }
            Self::FencedEpoch { expected, observed } => {
                write!(
                    formatter,
                    "group epoch {observed} is fenced by epoch {expected}"
                )
            }
            Self::StaleTerm { current, observed } => {
                write!(
                    formatter,
                    "term {observed} is stale; current term is {current}"
                )
            }
            Self::NotLeader { leader_hint } => {
                write!(
                    formatter,
                    "node is not leader; leader hint is {leader_hint:?}"
                )
            }
            Self::DuplicateProposal(proposal_id) => {
                write!(
                    formatter,
                    "proposal {proposal_id} is already pending or committed"
                )
            }
            Self::ConflictingProposal(proposal_id) => {
                write!(
                    formatter,
                    "proposal {proposal_id} reuses an idempotency key with a different payload"
                )
            }
        }
    }
}

impl Error for ConsensusError {}

pub type ConsensusResult<T> = Result<T, ConsensusError>;

/// An owned restart image for the memory-only feasibility adapter.
///
/// This value is process memory, not a durable checkpoint.
pub struct MemoryStableState {
    node_id: NodeId,
    group_id: GroupId,
    group_epoch: GroupEpoch,
    voters: [NodeId; 3],
    storage: MemStorage,
    applied_index: LogIndex,
    applied: Vec<CommittedProposal>,
    state_digest: StateDigest,
    memory_store_generation: u64,
}

impl fmt::Debug for MemoryStableState {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryStableState")
            .field("node_id", &self.node_id)
            .field("group_id", &self.group_id)
            .field("group_epoch", &self.group_epoch)
            .field("voters", &self.voters)
            .field("applied_index", &self.applied_index)
            .field("applied_count", &self.applied.len())
            .field("state_digest", &DigestDebug(&self.state_digest))
            .field("memory_store_generation", &self.memory_store_generation)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum ProcessingTrace {
    MemoryStoreBarrier(u64),
    MessageReleasedAfterMemoryStoreBarrier(u64),
    Applied(LogIndex),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TrackedProposal {
    Pending { payload: Vec<u8> },
    Committed(CommittedProposal),
}

#[derive(Debug)]
struct PlannedEntry {
    log_index: LogIndex,
    committed: Option<CommittedProposal>,
}

/// A fixed-three-voter, in-memory adapter used only to establish the Epoch
/// consensus boundary and exercise failure histories.
pub struct InMemoryRaftAdapter {
    node_id: NodeId,
    group_id: GroupId,
    group_epoch: GroupEpoch,
    voters: [NodeId; 3],
    raw_node: RawNode<MemStorage>,
    applied_index: LogIndex,
    applied: Vec<CommittedProposal>,
    state_digest: StateDigest,
    proposals: BTreeMap<ProposalId, TrackedProposal>,
    memory_store_generation: u64,
    poisoned: Option<String>,
    #[cfg(test)]
    processing_trace: Vec<ProcessingTrace>,
}

impl fmt::Debug for InMemoryRaftAdapter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryRaftAdapter")
            .field("status", &self.status())
            .field("applied_count", &self.applied.len())
            .field("state_digest", &DigestDebug(&self.state_digest))
            .field("proposals", &self.proposals)
            .field("memory_store_generation", &self.memory_store_generation)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl InMemoryRaftAdapter {
    pub fn new(
        node_id: NodeId,
        group_id: GroupId,
        group_epoch: GroupEpoch,
        voters: [NodeId; 3],
    ) -> ConsensusResult<Self> {
        validate_voters(node_id, voters)?;
        let storage = MemStorage::new_with_conf_state((
            voters.iter().map(|voter| voter.get()).collect::<Vec<_>>(),
            Vec::<u64>::new(),
        ));
        Self::restart(MemoryStableState {
            node_id,
            group_id,
            group_epoch,
            voters,
            storage,
            applied_index: LogIndex::ZERO,
            applied: Vec::new(),
            state_digest: compute_state_digest(group_id, group_epoch, &[])?,
            memory_store_generation: 0,
        })
    }

    /// Reconstructs all proposal state from the full in-memory log and rejects
    /// any restart image whose Raft and Epoch state disagree.
    pub fn restart(stable: MemoryStableState) -> ConsensusResult<Self> {
        let proposals = validate_persisted_state(PersistedStateView {
            node_id: stable.node_id,
            group_id: stable.group_id,
            group_epoch: stable.group_epoch,
            voters: stable.voters,
            storage: &stable.storage,
            applied_index: stable.applied_index,
            applied: &stable.applied,
            state_digest: stable.state_digest,
        })?;
        let config = raft_config(stable.node_id, stable.applied_index)?;
        let logger = Logger::root(slog::Discard, o!());
        let raw_node = RawNode::new(&config, stable.storage, &logger)
            .map_err(|error| ConsensusError::Library(error.to_string()))?;
        Ok(Self {
            node_id: stable.node_id,
            group_id: stable.group_id,
            group_epoch: stable.group_epoch,
            voters: stable.voters,
            raw_node,
            applied_index: stable.applied_index,
            applied: stable.applied,
            state_digest: stable.state_digest,
            proposals,
            memory_store_generation: stable.memory_store_generation,
            poisoned: None,
            #[cfg(test)]
            processing_trace: (stable.memory_store_generation != 0)
                .then_some(ProcessingTrace::MemoryStoreBarrier(
                    stable.memory_store_generation,
                ))
                .into_iter()
                .collect(),
        })
    }

    /// Extracts and validates an in-memory restart image.
    pub fn into_stable_state(self) -> ConsensusResult<MemoryStableState> {
        self.ensure_healthy()?;
        if self.raw_node.has_ready() {
            return Err(ConsensusError::InvalidState(
                "cannot extract memory state while RawNode still has Ready work".into(),
            ));
        }
        let stable = MemoryStableState {
            node_id: self.node_id,
            group_id: self.group_id,
            group_epoch: self.group_epoch,
            voters: self.voters,
            storage: self.raw_node.store().clone(),
            applied_index: self.applied_index,
            applied: self.applied,
            state_digest: self.state_digest,
            memory_store_generation: self.memory_store_generation,
        };
        validate_persisted_state(PersistedStateView {
            node_id: stable.node_id,
            group_id: stable.group_id,
            group_epoch: stable.group_epoch,
            voters: stable.voters,
            storage: &stable.storage,
            applied_index: stable.applied_index,
            applied: &stable.applied,
            state_digest: stable.state_digest,
        })?;
        Ok(stable)
    }

    pub const fn state_digest(&self) -> StateDigest {
        self.state_digest
    }

    pub fn applied_proposals(&self) -> &[CommittedProposal] {
        &self.applied
    }

    pub fn lookup_proposal(&self, proposal_id: ProposalId) -> ProposalLookup {
        match self.proposals.get(&proposal_id) {
            None => ProposalLookup::Unknown,
            Some(TrackedProposal::Pending { .. }) => ProposalLookup::Pending,
            Some(TrackedProposal::Committed(committed)) => {
                ProposalLookup::Committed(committed.clone())
            }
        }
    }

    #[cfg(test)]
    fn expire_leader_lease(&mut self) {
        while !self.raw_node.raft.pass_election_timeout() {
            self.raw_node.raft.election_elapsed =
                self.raw_node.raft.election_elapsed.saturating_add(1);
        }
    }

    fn ensure_healthy(&self) -> ConsensusResult<()> {
        match &self.poisoned {
            Some(reason) => Err(ConsensusError::Poisoned(format!(
                "consensus adapter is fail-stopped: {reason}"
            ))),
            None => Ok(()),
        }
    }

    fn process_ready(&mut self) -> ConsensusResult<ConsensusOutput> {
        self.ensure_healthy()?;
        let result = self.process_ready_inner();
        if let Err(error) = &result {
            self.poisoned = Some(error.to_string());
        }
        result
    }

    fn process_ready_inner(&mut self) -> ConsensusResult<ConsensusOutput> {
        let mut outbound = Vec::new();
        let mut commits = Vec::new();
        let mut iterations = 0_u16;

        while self.raw_node.has_ready() {
            iterations = iterations.checked_add(1).ok_or_else(|| {
                ConsensusError::Library("RawNode Ready iteration counter overflow".into())
            })?;
            if iterations > 1_000 {
                return Err(ConsensusError::Library(
                    "RawNode did not quiesce after 1,000 Ready cycles".into(),
                ));
            }

            let mut ready = self.raw_node.ready();
            reject_snapshot(ready.snapshot(), "Ready")?;
            let ready_requires_barrier = ready.must_sync();
            let immediate_messages = self.wrap_messages(ready.take_messages(), None)?;
            let persisted_messages_raw = ready.take_persisted_messages();
            let ready_committed = ready.take_committed_entries();
            let ready_plan = self.prevalidate_committed_batch(&ready_committed)?;
            let ready_barrier = self.persist_ready(&ready)?;
            if !persisted_messages_raw.is_empty()
                && ready_requires_barrier
                && ready_barrier.is_none()
            {
                return Err(ConsensusError::InvalidState(
                    "Ready requiring stable storage released messages without a memory-store barrier"
                        .into(),
                ));
            }
            let persisted_messages = self.wrap_messages(persisted_messages_raw, ready_barrier)?;

            outbound.extend(immediate_messages);
            outbound.extend(persisted_messages);
            self.apply_prevalidated_batch(ready_plan, &mut commits)?;

            let mut light_ready = self.raw_node.advance(ready);
            let light_messages_raw = light_ready.take_messages();
            let light_committed = light_ready.take_committed_entries();
            let light_plan = self.prevalidate_committed_batch(&light_committed)?;
            let light_barrier = if let Some(commit_index) = light_ready.commit_index() {
                self.raw_node.mut_store().wl().mut_hard_state().commit = commit_index;
                Some(self.record_memory_store_barrier()?)
            } else {
                ready_barrier.or_else(|| self.current_memory_store_barrier())
            };
            let light_messages = self.wrap_messages(light_messages_raw, light_barrier)?;
            outbound.extend(light_messages);
            self.apply_prevalidated_batch(light_plan, &mut commits)?;
            self.raw_node.advance_apply();
        }

        self.proposals = validate_persisted_state(PersistedStateView {
            node_id: self.node_id,
            group_id: self.group_id,
            group_epoch: self.group_epoch,
            voters: self.voters,
            storage: self.raw_node.store(),
            applied_index: self.applied_index,
            applied: &self.applied,
            state_digest: self.state_digest,
        })?;

        Ok(ConsensusOutput {
            messages: outbound,
            commits,
            status: self.status(),
        })
    }

    fn persist_ready(&mut self, ready: &raft::Ready) -> ConsensusResult<Option<u64>> {
        reject_snapshot(ready.snapshot(), "Ready")?;
        let entries = ready.entries().clone();
        let hard_state = ready.hs().cloned();
        let changed = !entries.is_empty() || hard_state.is_some();
        let next_generation = changed
            .then(|| self.next_memory_store_generation())
            .transpose()?;
        {
            let mut storage = self.raw_node.mut_store().wl();
            if !entries.is_empty() {
                storage
                    .append(&entries)
                    .map_err(|error| ConsensusError::Storage(error.to_string()))?;
            }
            if let Some(hard_state) = hard_state {
                storage.set_hardstate(hard_state);
            }
        }
        if let Some(generation) = next_generation {
            self.memory_store_generation = generation;
        }
        #[cfg(test)]
        if let Some(generation) = next_generation {
            self.processing_trace
                .push(ProcessingTrace::MemoryStoreBarrier(generation));
        }
        self.proposals = build_proposal_tracking(
            self.group_id,
            self.group_epoch,
            self.raw_node.store(),
            self.applied_index,
            &self.applied,
            self.voters,
        )?;
        Ok(next_generation.or_else(|| self.current_memory_store_barrier()))
    }

    fn next_memory_store_generation(&self) -> ConsensusResult<u64> {
        self.memory_store_generation.checked_add(1).ok_or_else(|| {
            ConsensusError::InvalidState("memory-store barrier generation overflow".into())
        })
    }

    fn record_memory_store_barrier(&mut self) -> ConsensusResult<u64> {
        let generation = self.next_memory_store_generation()?;
        self.memory_store_generation = generation;
        #[cfg(test)]
        self.processing_trace
            .push(ProcessingTrace::MemoryStoreBarrier(generation));
        Ok(generation)
    }

    fn current_memory_store_barrier(&self) -> Option<u64> {
        (self.memory_store_generation != 0).then_some(self.memory_store_generation)
    }

    fn wrap_messages(
        &mut self,
        messages: Vec<RaftMessage>,
        memory_store_barrier: Option<u64>,
    ) -> ConsensusResult<Vec<PeerMessage>> {
        let wrapped = messages
            .into_iter()
            .map(|message| {
                let from = NodeId::new(message.from).map_err(|_| {
                    ConsensusError::InvalidMessage("Raft message has zero source".into())
                })?;
                let to = NodeId::new(message.to).map_err(|_| {
                    ConsensusError::InvalidMessage("Raft message has zero destination".into())
                })?;
                if from != self.node_id {
                    return Err(ConsensusError::InvalidMessage(format!(
                        "outbound message source {from} is not local node {}",
                        self.node_id
                    )));
                }
                validate_transport_membership(self.voters, from, to)?;
                let peer_message = PeerMessage {
                    group_id: self.group_id,
                    group_epoch: self.group_epoch,
                    from,
                    to,
                    term: Term::new(message.term),
                    encoded: message.encode_to_vec(),
                };
                validate_embedded_message(&peer_message)?;
                Ok(peer_message)
            })
            .collect::<ConsensusResult<Vec<_>>>()?;

        #[cfg(test)]
        if let Some(generation) = memory_store_barrier {
            self.processing_trace.extend(
                wrapped
                    .iter()
                    .map(|_| ProcessingTrace::MessageReleasedAfterMemoryStoreBarrier(generation)),
            );
        }
        #[cfg(not(test))]
        let _ = memory_store_barrier;
        Ok(wrapped)
    }

    fn prevalidate_committed_batch(&self, entries: &[Entry]) -> ConsensusResult<Vec<PlannedEntry>> {
        let mut planned = Vec::with_capacity(entries.len());
        let mut projected_index = self.applied_index;
        let mut seen = self
            .applied
            .iter()
            .map(|committed| (committed.receipt.proposal_id, committed.payload.clone()))
            .collect::<BTreeMap<_, _>>();

        for entry in entries {
            if entry.index <= self.applied_index.get() {
                continue;
            }
            let expected_index = projected_index
                .get()
                .checked_add(1)
                .ok_or_else(|| ConsensusError::InvalidState("applied log index overflow".into()))?;
            if entry.index != expected_index {
                return Err(ConsensusError::InvalidState(format!(
                    "committed entry index {} is not contiguous after {}",
                    entry.index, projected_index
                )));
            }
            let log_index = LogIndex::new(entry.index);
            if entry.entry_type != EntryType::EntryNormal as i32 {
                return Err(ConsensusError::Unsupported(
                    "membership changes are outside this fixed-voter feasibility adapter".into(),
                ));
            }

            let committed = if entry.data.is_empty() {
                None
            } else {
                let command = decode_command(&entry.data)?;
                validate_command_scope(self.group_id, self.group_epoch, &command)?;
                match seen.get(&command.proposal_id) {
                    Some(payload) if *payload != command.payload => {
                        return Err(ConsensusError::ConflictingProposal(command.proposal_id));
                    }
                    Some(_) => None,
                    None => {
                        seen.insert(command.proposal_id, command.payload.clone());
                        Some(CommittedProposal {
                            receipt: CommitReceipt {
                                group_id: self.group_id,
                                group_epoch: self.group_epoch,
                                proposal_id: command.proposal_id,
                                term: Term::new(entry.term),
                                log_index,
                            },
                            payload: command.payload,
                        })
                    }
                }
            };
            planned.push(PlannedEntry {
                log_index,
                committed,
            });
            projected_index = log_index;
        }
        Ok(planned)
    }

    fn apply_prevalidated_batch(
        &mut self,
        planned: Vec<PlannedEntry>,
        new_commits: &mut Vec<CommittedProposal>,
    ) -> ConsensusResult<()> {
        let mut projected_applied = self.applied.clone();
        projected_applied.extend(planned.iter().filter_map(|entry| entry.committed.clone()));
        let projected_digest =
            compute_state_digest(self.group_id, self.group_epoch, &projected_applied)?;

        for planned_entry in planned {
            if let Some(committed) = planned_entry.committed {
                self.proposals.insert(
                    committed.receipt.proposal_id,
                    TrackedProposal::Committed(committed.clone()),
                );
                self.applied.push(committed.clone());
                new_commits.push(committed);
            }
            self.applied_index = planned_entry.log_index;
            #[cfg(test)]
            self.processing_trace
                .push(ProcessingTrace::Applied(planned_entry.log_index));
        }
        self.state_digest = projected_digest;
        Ok(())
    }

    fn validate_proposal(&self, proposal: &Proposal) -> ConsensusResult<()> {
        self.ensure_healthy()?;
        if proposal.group_id != self.group_id {
            return Err(ConsensusError::GroupMismatch {
                expected: self.group_id,
                observed: proposal.group_id,
            });
        }
        if proposal.group_epoch != self.group_epoch {
            return Err(ConsensusError::FencedEpoch {
                expected: self.group_epoch,
                observed: proposal.group_epoch,
            });
        }
        let status = self.status();
        if proposal.expected_term != status.term {
            return Err(ConsensusError::StaleTerm {
                current: status.term,
                observed: proposal.expected_term,
            });
        }
        if status.role != ConsensusRole::Leader {
            return Err(ConsensusError::NotLeader {
                leader_hint: status.leader_id,
            });
        }
        if let Some(tracked) = self.proposals.get(&proposal.proposal_id) {
            let payload = match tracked {
                TrackedProposal::Pending { payload } => payload,
                TrackedProposal::Committed(committed) => &committed.payload,
            };
            return if *payload == proposal.payload {
                Err(ConsensusError::DuplicateProposal(proposal.proposal_id))
            } else {
                Err(ConsensusError::ConflictingProposal(proposal.proposal_id))
            };
        }
        Ok(())
    }

    #[cfg(test)]
    fn processing_trace(&self) -> &[ProcessingTrace] {
        &self.processing_trace
    }
}

impl ConsensusAdapter for InMemoryRaftAdapter {
    fn status(&self) -> ConsensusStatus {
        let status = self.raw_node.status();
        ConsensusStatus {
            node_id: self.node_id,
            group_id: self.group_id,
            group_epoch: self.group_epoch,
            role: map_role(status.ss.raft_state),
            leader_id: NodeId::new(status.ss.leader_id).ok(),
            term: Term::new(status.hs.term),
            commit_index: LogIndex::new(status.hs.commit),
            applied_index: LogIndex::new(status.applied),
            voter_count: self.voters.len(),
            fail_stopped: self.poisoned.is_some(),
        }
    }

    fn campaign(&mut self) -> ConsensusResult<ConsensusOutput> {
        self.ensure_healthy()?;
        self.raw_node
            .campaign()
            .map_err(|error| ConsensusError::Library(error.to_string()))?;
        self.process_ready()
    }

    fn tick(&mut self) -> ConsensusResult<ConsensusOutput> {
        self.ensure_healthy()?;
        self.raw_node.tick();
        self.process_ready()
    }

    fn propose(&mut self, proposal: Proposal) -> ConsensusResult<ConsensusOutput> {
        self.validate_proposal(&proposal)?;
        let encoded = encode_command(&proposal)?;
        self.raw_node
            .propose(Vec::new(), encoded)
            .map_err(|error| ConsensusError::Library(error.to_string()))?;
        self.process_ready()
    }

    fn receive(&mut self, message: PeerMessage) -> ConsensusResult<ConsensusOutput> {
        self.ensure_healthy()?;
        if message.group_id != self.group_id {
            return Err(ConsensusError::GroupMismatch {
                expected: self.group_id,
                observed: message.group_id,
            });
        }
        if message.group_epoch != self.group_epoch {
            return Err(ConsensusError::FencedEpoch {
                expected: self.group_epoch,
                observed: message.group_epoch,
            });
        }
        if message.to != self.node_id {
            return Err(ConsensusError::InvalidMessage(format!(
                "message for node {} was delivered to node {}",
                message.to, self.node_id
            )));
        }
        validate_transport_membership(self.voters, message.from, message.to)?;
        if message.from == self.node_id {
            return Err(ConsensusError::InvalidMessage(
                "self-originated peer messages must not enter the transport".into(),
            ));
        }
        let raft_message = validate_embedded_message(&message)?;
        self.raw_node
            .step(raft_message)
            .map_err(|error| ConsensusError::Library(error.to_string()))?;
        self.process_ready()
    }

    fn transfer_leadership(&mut self, target: NodeId) -> ConsensusResult<ConsensusOutput> {
        self.ensure_healthy()?;
        let status = self.status();
        if status.role != ConsensusRole::Leader {
            return Err(ConsensusError::NotLeader {
                leader_hint: status.leader_id,
            });
        }
        if target == self.node_id || !self.voters.contains(&target) {
            return Err(ConsensusError::InvalidVoterSet(format!(
                "leadership target {target} is not another voter"
            )));
        }
        self.raw_node.transfer_leader(target.get());
        self.process_ready()
    }
}

fn raft_config(node_id: NodeId, applied_index: LogIndex) -> ConsensusResult<Config> {
    let config = Config {
        id: node_id.get(),
        election_tick: ELECTION_TICK,
        heartbeat_tick: HEARTBEAT_TICK,
        applied: applied_index.get(),
        check_quorum: true,
        pre_vote: true,
        ..Config::default()
    };
    config
        .validate()
        .map_err(|error| ConsensusError::Library(error.to_string()))?;
    Ok(config)
}

fn validate_voters(node_id: NodeId, voters: [NodeId; 3]) -> ConsensusResult<()> {
    let unique = voters.into_iter().collect::<BTreeSet<_>>();
    if unique.len() != voters.len() {
        return Err(ConsensusError::InvalidVoterSet(
            "the fixed voter set must contain three distinct nodes".into(),
        ));
    }
    if !unique.contains(&node_id) {
        return Err(ConsensusError::InvalidVoterSet(format!(
            "local node {node_id} is absent from its voter set"
        )));
    }
    Ok(())
}

fn validate_transport_membership(
    voters: [NodeId; 3],
    from: NodeId,
    to: NodeId,
) -> ConsensusResult<()> {
    if from == to {
        return Err(ConsensusError::InvalidMessage(
            "self-addressed peer messages are not transport messages".into(),
        ));
    }
    if !voters.contains(&from) || !voters.contains(&to) {
        return Err(ConsensusError::InvalidMessage(format!(
            "peer-message route {from}->{to} is outside the fixed voter set"
        )));
    }
    Ok(())
}

const fn map_role(role: StateRole) -> ConsensusRole {
    match role {
        StateRole::Follower => ConsensusRole::Follower,
        StateRole::PreCandidate => ConsensusRole::PreCandidate,
        StateRole::Candidate => ConsensusRole::Candidate,
        StateRole::Leader => ConsensusRole::Leader,
    }
}

fn reject_snapshot(snapshot: &raft::prelude::Snapshot, source: &str) -> ConsensusResult<()> {
    if snapshot.metadata.is_some() || !snapshot.data.is_empty() {
        return Err(ConsensusError::Unsupported(format!(
            "{source} snapshot rejected: Epoch checkpoint installation is not implemented"
        )));
    }
    Ok(())
}

fn validate_embedded_message(message: &PeerMessage) -> ConsensusResult<RaftMessage> {
    let max_payload = MAX_PEER_MESSAGE_WIRE_BYTES - PEER_MESSAGE_HEADER_LEN;
    if message.encoded.len() > max_payload {
        return Err(ConsensusError::InvalidMessage(format!(
            "encoded Raft message is {} bytes; maximum is {max_payload}",
            message.encoded.len()
        )));
    }
    let raft_message = RaftMessage::decode(message.encoded.as_slice())
        .map_err(|error| ConsensusError::InvalidMessage(error.to_string()))?;
    if raft_message.encode_to_vec() != message.encoded {
        return Err(ConsensusError::InvalidMessage(
            "Raft payload is not canonically encoded".into(),
        ));
    }
    if raft_message.from != message.from.get()
        || raft_message.to != message.to.get()
        || raft_message.term != message.term.get()
    {
        return Err(ConsensusError::InvalidMessage(
            "peer envelope does not match its encoded Raft message".into(),
        ));
    }
    let message_type = MessageType::from_i32(raft_message.msg_type)
        .ok_or_else(|| ConsensusError::InvalidMessage("unknown Raft message type".into()))?;
    if matches!(
        message_type,
        MessageType::MsgHup
            | MessageType::MsgBeat
            | MessageType::MsgUnreachable
            | MessageType::MsgSnapStatus
            | MessageType::MsgCheckQuorum
    ) {
        return Err(ConsensusError::InvalidMessage(format!(
            "local-only Raft message {message_type:?} cannot cross the transport"
        )));
    }
    if message_type == MessageType::MsgSnapshot || raft_message.snapshot.is_some() {
        return Err(ConsensusError::Unsupported(
            "peer snapshot rejected: Epoch checkpoint installation is not implemented".into(),
        ));
    }
    if raft_message
        .entries
        .iter()
        .any(|entry| entry.entry_type != EntryType::EntryNormal as i32)
    {
        return Err(ConsensusError::Unsupported(
            "membership-changing entries are not valid in the fixed-voter transport".into(),
        ));
    }
    Ok(raft_message)
}

struct EncodedCommand {
    group_id: GroupId,
    group_epoch: GroupEpoch,
    proposal_id: ProposalId,
    payload: Vec<u8>,
}

fn encode_command(proposal: &Proposal) -> ConsensusResult<Vec<u8>> {
    if proposal.payload.len() > MAX_PROPOSAL_PAYLOAD_BYTES {
        return Err(ConsensusError::InvalidMessage(format!(
            "proposal payload is {} bytes; maximum is {MAX_PROPOSAL_PAYLOAD_BYTES}",
            proposal.payload.len()
        )));
    }
    let payload_len = u32::try_from(proposal.payload.len()).map_err(|_| {
        ConsensusError::InvalidMessage("proposal payload exceeds the v1 command limit".into())
    })?;
    let mut encoded = Vec::with_capacity(COMMAND_HEADER_LEN + proposal.payload.len());
    encoded.extend_from_slice(&COMMAND_MAGIC);
    encoded.extend_from_slice(&COMMAND_VERSION.to_be_bytes());
    encoded.extend_from_slice(&proposal.group_id.get().to_be_bytes());
    encoded.extend_from_slice(&proposal.group_epoch.get().to_be_bytes());
    encoded.extend_from_slice(&proposal.proposal_id.get().to_be_bytes());
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(&proposal.payload);
    Ok(encoded)
}

fn decode_command(encoded: &[u8]) -> ConsensusResult<EncodedCommand> {
    if encoded.len() < COMMAND_HEADER_LEN || encoded[..4] != COMMAND_MAGIC {
        return Err(ConsensusError::InvalidMessage(
            "committed command has an invalid header".into(),
        ));
    }
    let version = u16::from_be_bytes([encoded[4], encoded[5]]);
    if version != COMMAND_VERSION {
        return Err(ConsensusError::InvalidMessage(format!(
            "unsupported committed command version {version}"
        )));
    }
    let group_id = GroupId::new(read_u64(encoded, 6, "committed command")?)?;
    let group_epoch = GroupEpoch::new(read_u64(encoded, 14, "committed command")?)?;
    let proposal_id = ProposalId::new(read_u64(encoded, 22, "committed command")?)?;
    let payload_len = u32::from_be_bytes(
        encoded[30..34]
            .try_into()
            .map_err(|_| ConsensusError::InvalidMessage("invalid payload length".into()))?,
    ) as usize;
    if payload_len > MAX_PROPOSAL_PAYLOAD_BYTES {
        return Err(ConsensusError::InvalidMessage(format!(
            "committed command payload is {payload_len} bytes; maximum is {MAX_PROPOSAL_PAYLOAD_BYTES}"
        )));
    }
    if encoded.len() != COMMAND_HEADER_LEN.saturating_add(payload_len) {
        return Err(ConsensusError::InvalidMessage(
            "committed command payload length does not match its frame".into(),
        ));
    }
    Ok(EncodedCommand {
        group_id,
        group_epoch,
        proposal_id,
        payload: encoded[COMMAND_HEADER_LEN..].to_vec(),
    })
}

fn validate_command_scope(
    expected_group: GroupId,
    expected_epoch: GroupEpoch,
    command: &EncodedCommand,
) -> ConsensusResult<()> {
    if command.group_id != expected_group {
        return Err(ConsensusError::GroupMismatch {
            expected: expected_group,
            observed: command.group_id,
        });
    }
    if command.group_epoch != expected_epoch {
        return Err(ConsensusError::FencedEpoch {
            expected: expected_epoch,
            observed: command.group_epoch,
        });
    }
    Ok(())
}

fn read_u64(encoded: &[u8], offset: usize, frame: &str) -> ConsensusResult<u64> {
    encoded
        .get(offset..offset.saturating_add(8))
        .ok_or_else(|| ConsensusError::InvalidMessage(format!("truncated {frame}")))?
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| ConsensusError::InvalidMessage(format!("invalid integer in {frame}")))
}

#[derive(Clone, Copy)]
struct PersistedStateView<'a> {
    node_id: NodeId,
    group_id: GroupId,
    group_epoch: GroupEpoch,
    voters: [NodeId; 3],
    storage: &'a MemStorage,
    applied_index: LogIndex,
    applied: &'a [CommittedProposal],
    state_digest: StateDigest,
}

fn validate_persisted_state(
    state: PersistedStateView<'_>,
) -> ConsensusResult<BTreeMap<ProposalId, TrackedProposal>> {
    validate_voters(state.node_id, state.voters)?;
    let raft_state = state
        .storage
        .initial_state()
        .map_err(|error| ConsensusError::Storage(error.to_string()))?;
    let expected_conf_state = ConfState::from((
        state
            .voters
            .iter()
            .map(|voter| voter.get())
            .collect::<Vec<_>>(),
        Vec::<u64>::new(),
    ));
    if raft_state.conf_state != expected_conf_state {
        return Err(ConsensusError::InvalidState(
            "stored ConfState does not exactly match the fixed voter set".into(),
        ));
    }
    validate_hard_state(&raft_state.hard_state, state.voters)?;
    let first_index = state
        .storage
        .first_index()
        .map_err(|error| ConsensusError::Storage(error.to_string()))?;
    if first_index != 1 {
        return Err(ConsensusError::Unsupported(format!(
            "stored first index {first_index} implies a snapshot or compaction, but Epoch checkpoints are not implemented"
        )));
    }
    let last_index = state
        .storage
        .last_index()
        .map_err(|error| ConsensusError::Storage(error.to_string()))?;
    if state.applied_index.get() > raft_state.hard_state.commit
        || raft_state.hard_state.commit > last_index
    {
        return Err(ConsensusError::InvalidState(format!(
            "restart indexes violate applied ({}) <= commit ({}) <= last ({last_index})",
            state.applied_index, raft_state.hard_state.commit
        )));
    }

    validate_applied_receipts(
        state.group_id,
        state.group_epoch,
        state.applied_index,
        state.applied,
    )?;
    let expected_digest = compute_state_digest(state.group_id, state.group_epoch, state.applied)?;
    if expected_digest != state.state_digest {
        return Err(ConsensusError::InvalidState(
            "stored state digest does not match the canonical applied history".into(),
        ));
    }
    build_proposal_tracking(
        state.group_id,
        state.group_epoch,
        state.storage,
        state.applied_index,
        state.applied,
        state.voters,
    )
}

fn validate_hard_state(hard_state: &HardState, voters: [NodeId; 3]) -> ConsensusResult<()> {
    if hard_state.vote != 0 && !voters.iter().any(|voter| voter.get() == hard_state.vote) {
        return Err(ConsensusError::InvalidState(format!(
            "stored vote {} is outside the fixed voter set",
            hard_state.vote
        )));
    }
    if hard_state.vote != 0 && hard_state.term == 0 {
        return Err(ConsensusError::InvalidState(
            "stored vote cannot exist at Raft term zero".into(),
        ));
    }
    Ok(())
}

fn validate_applied_receipts(
    group_id: GroupId,
    group_epoch: GroupEpoch,
    applied_index: LogIndex,
    applied: &[CommittedProposal],
) -> ConsensusResult<()> {
    let mut previous_index = LogIndex::ZERO;
    let mut proposal_ids = BTreeSet::new();
    for committed in applied {
        if committed.receipt.group_id != group_id || committed.receipt.group_epoch != group_epoch {
            return Err(ConsensusError::InvalidState(
                "applied receipt belongs to a different group or epoch".into(),
            ));
        }
        if committed.receipt.log_index <= previous_index
            || committed.receipt.log_index > applied_index
        {
            return Err(ConsensusError::InvalidState(
                "applied receipts must have unique, increasing indexes at or below applied_index"
                    .into(),
            ));
        }
        if !proposal_ids.insert(committed.receipt.proposal_id) {
            return Err(ConsensusError::InvalidState(format!(
                "applied proposal {} is duplicated",
                committed.receipt.proposal_id
            )));
        }
        previous_index = committed.receipt.log_index;
    }
    Ok(())
}

fn build_proposal_tracking(
    group_id: GroupId,
    group_epoch: GroupEpoch,
    storage: &MemStorage,
    applied_index: LogIndex,
    applied: &[CommittedProposal],
    voters: [NodeId; 3],
) -> ConsensusResult<BTreeMap<ProposalId, TrackedProposal>> {
    let raft_state = storage
        .initial_state()
        .map_err(|error| ConsensusError::Storage(error.to_string()))?;
    validate_hard_state(&raft_state.hard_state, voters)?;
    let last_index = storage
        .last_index()
        .map_err(|error| ConsensusError::Storage(error.to_string()))?;
    let entries = if last_index == 0 {
        Vec::new()
    } else {
        let high = last_index
            .checked_add(1)
            .ok_or_else(|| ConsensusError::InvalidState("last log index overflow".into()))?;
        storage
            .entries(1, high, None, GetEntriesContext::empty(false))
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
    };
    validate_log_order(&entries, last_index, raft_state.hard_state.term)?;

    let applied_by_id = applied
        .iter()
        .map(|committed| (committed.receipt.proposal_id, committed))
        .collect::<BTreeMap<_, _>>();
    let mut proposals = applied
        .iter()
        .cloned()
        .map(|committed| {
            (
                committed.receipt.proposal_id,
                TrackedProposal::Committed(committed),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut first_applied_occurrence = BTreeMap::new();

    for entry in entries {
        if entry.entry_type != EntryType::EntryNormal as i32 {
            return Err(ConsensusError::Unsupported(
                "stored membership change found in fixed-voter feasibility adapter".into(),
            ));
        }
        if entry.data.is_empty() {
            continue;
        }
        let command = decode_command(&entry.data)?;
        validate_command_scope(group_id, group_epoch, &command)?;

        if entry.index <= applied_index.get() {
            let committed = applied_by_id.get(&command.proposal_id).ok_or_else(|| {
                ConsensusError::InvalidState(format!(
                    "applied log command {} has no applied receipt",
                    command.proposal_id
                ))
            })?;
            if committed.payload != command.payload {
                return Err(ConsensusError::ConflictingProposal(command.proposal_id));
            }
            let first_index = first_applied_occurrence
                .entry(command.proposal_id)
                .or_insert(entry.index);
            if committed.receipt.log_index.get() != *first_index {
                return Err(ConsensusError::InvalidState(format!(
                    "applied receipt for proposal {} does not name its first applied log occurrence",
                    command.proposal_id
                )));
            }
            if committed.receipt.log_index.get() == entry.index
                && committed.receipt.term.get() != entry.term
            {
                return Err(ConsensusError::InvalidState(format!(
                    "applied receipt term for proposal {} does not match its log entry",
                    command.proposal_id
                )));
            }
            continue;
        }

        match proposals.get(&command.proposal_id) {
            Some(TrackedProposal::Pending { payload }) if *payload != command.payload => {
                return Err(ConsensusError::ConflictingProposal(command.proposal_id));
            }
            Some(TrackedProposal::Committed(committed)) if committed.payload != command.payload => {
                return Err(ConsensusError::ConflictingProposal(command.proposal_id));
            }
            Some(_) => {}
            None => {
                proposals.insert(
                    command.proposal_id,
                    TrackedProposal::Pending {
                        payload: command.payload,
                    },
                );
            }
        }
    }
    for committed in applied {
        if !first_applied_occurrence.contains_key(&committed.receipt.proposal_id) {
            return Err(ConsensusError::InvalidState(format!(
                "applied proposal {} has no matching persisted log command",
                committed.receipt.proposal_id
            )));
        }
    }
    Ok(proposals)
}

fn validate_log_order(
    entries: &[Entry],
    last_index: u64,
    hard_state_term: u64,
) -> ConsensusResult<()> {
    if last_index == 0 {
        if entries.is_empty() {
            return Ok(());
        }
        return Err(ConsensusError::InvalidState(
            "empty log reports persisted entries".into(),
        ));
    }
    if entries.len() != usize::try_from(last_index).unwrap_or(usize::MAX) {
        return Err(ConsensusError::InvalidState(
            "persisted log is not complete from index 1 through last_index".into(),
        ));
    }
    let mut previous_term = 0;
    for (offset, entry) in entries.iter().enumerate() {
        let expected = u64::try_from(offset)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| ConsensusError::InvalidState("log index overflow".into()))?;
        if entry.index != expected {
            return Err(ConsensusError::InvalidState(format!(
                "persisted log entry {} is out of canonical order; expected {expected}",
                entry.index
            )));
        }
        if entry.term == 0 {
            return Err(ConsensusError::InvalidState(format!(
                "persisted log entry {} has Raft term zero",
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
            "stored HardState term {hard_state_term} is below final log term {previous_term}"
        )));
    }
    Ok(())
}

fn compute_state_digest(
    group_id: GroupId,
    group_epoch: GroupEpoch,
    applied: &[CommittedProposal],
) -> ConsensusResult<StateDigest> {
    let count = u64::try_from(applied.len())
        .map_err(|_| ConsensusError::InvalidState("applied history is too large".into()))?;
    let mut hasher = Sha256::new();
    hasher.update(STATE_DIGEST_MAGIC);
    hasher.update(STATE_DIGEST_VERSION.to_be_bytes());
    hasher.update(group_id.get().to_be_bytes());
    hasher.update(group_epoch.get().to_be_bytes());
    hasher.update(count.to_be_bytes());
    for committed in applied {
        let payload_len = u64::try_from(committed.payload.len()).map_err(|_| {
            ConsensusError::InvalidState("applied proposal payload is too large".into())
        })?;
        hasher.update(committed.receipt.log_index.get().to_be_bytes());
        hasher.update(committed.receipt.term.get().to_be_bytes());
        hasher.update(committed.receipt.proposal_id.get().to_be_bytes());
        hasher.update(payload_len.to_be_bytes());
        hasher.update(&committed.payload);
    }
    Ok(hasher.finalize().into())
}

struct DigestDebug<'a>(&'a StateDigest);

impl fmt::Debug for DigestDebug<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
