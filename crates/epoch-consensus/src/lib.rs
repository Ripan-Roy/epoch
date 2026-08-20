//! Epoch-owned consensus types and an isolated `raft-rs` feasibility adapter.
//!
//! [`InMemoryRaftAdapter`] provides deterministic memory-only histories.
//! [`PersistentRaftAdapter`] journals each stable Raft transition and its
//! publishable application checkpoint through Epoch's checksummed WAL before it
//! releases persisted messages or commit receipts. The persistent adapter is
//! still a fixed-voter feasibility slice: it supports bounded native-profile
//! snapshots and physical journal compaction, while membership changes,
//! production transport, backup/PITR, and general repair remain disabled.

mod stable;

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display, Formatter},
    path::Path,
    sync::RwLockWriteGuard,
};

use prost::Message as ProstMessage;
use raft::{
    Config, GetEntriesContext, RaftState, RawNode, StateRole, Storage,
    prelude::{
        ConfState, Entry, EntryType, HardState, Message as RaftMessage, MessageType, Snapshot,
    },
    storage::{MemStorage, MemStorageCore},
};
use sha2::{Digest, Sha256};
use slog::{Logger, o};
use stable::{DiskStableStore, StableCheckpoint, StableIdentity};

const COMMAND_MAGIC: [u8; 4] = *b"EPCM";
const COMMAND_VERSION: u16 = 1;
const COMMAND_HEADER_LEN: usize = 34;
const PEER_MESSAGE_MAGIC: [u8; 4] = *b"EPPM";
const PEER_MESSAGE_VERSION: u16 = 1;
const PEER_MESSAGE_HEADER_LEN: usize = 50;
const STATE_DIGEST_MAGIC: [u8; 4] = *b"EPDG";
const STATE_DIGEST_VERSION: u16 = 1;
const ROLLING_STATE_DIGEST_VERSION: u16 = 2;
const SNAPSHOT_MAGIC: [u8; 4] = *b"EPSN";
const SNAPSHOT_V1_VERSION: u16 = 1;
const SNAPSHOT_V2_VERSION: u16 = 2;
const SNAPSHOT_V1_HEADER_LEN: usize = 76;
const SNAPSHOT_V2_HEADER_LEN: usize = 172;
const SNAPSHOT_V2_TRAILER_LEN: usize = 32;
const SNAPSHOT_PROPOSAL_FIXED_LEN: usize = 28;
const HEARTBEAT_TICK: usize = 2;
const ELECTION_TICK: usize = 10;
const MAX_UNCOMMITTED_BYTES: u64 = 8 * 1024 * 1024;
const MAX_COMMITTED_BYTES_PER_READY: u64 = 8 * 1024 * 1024;
const MAX_PENDING_READ_BARRIERS: usize = 1_024;
const READ_BARRIER_CONTEXT_BYTES: usize = 8;

/// Maximum accepted size of a complete canonical Epoch peer-message frame.
pub const MAX_PEER_MESSAGE_WIRE_BYTES: usize = 8 * 1024 * 1024;
/// Maximum command payload accepted before it enters `RawNode`.
pub const MAX_PROPOSAL_PAYLOAD_BYTES: usize = 512 * 1024;
/// Maximum canonical Epoch checkpoint data carried inside one Raft snapshot.
pub const MAX_SNAPSHOT_DATA_BYTES: usize = 6 * 1024 * 1024;
/// Original EPSN v1 bound retained for byte- and behavior-compatible writes.
pub const MAX_V1_SNAPSHOT_DATA_BYTES: usize = 768 * 1024;
/// Maximum canonical profile payload embedded in an EPSN v2 checkpoint.
pub const MAX_APPLICATION_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;
/// Maximum exact-retry entries retained in an EPSN v2 checkpoint.
pub const MAX_CHECKPOINT_RETRY_PROPOSALS: usize = 1_024;
/// Maximum encoded exact-retry suffix retained in an EPSN v2 checkpoint.
pub const MAX_CHECKPOINT_RETRY_BYTES: usize = 1024 * 1024;

/// SHA-256 over the canonically framed applied Epoch state history.
pub type StateDigest = [u8; 32];

/// Raft's in-memory log plus the latest canonical Epoch checkpoint payload.
///
/// `raft::MemStorage` deliberately discards snapshot data. Epoch retains the
/// complete validated snapshot so a compacted leader can catch up a lagging
/// voter without manufacturing profile state outside the consensus boundary.
#[derive(Clone, Default)]
struct EpochRaftStorage {
    inner: MemStorage,
    snapshot: Option<Snapshot>,
}

impl fmt::Debug for EpochRaftStorage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EpochRaftStorage")
            .field("first_index", &self.first_index())
            .field("last_index", &self.last_index())
            .field("checkpoint_index", &self.checkpoint_index())
            .finish_non_exhaustive()
    }
}

impl EpochRaftStorage {
    fn new_with_conf_state(conf_state: impl Into<ConfState>) -> Self {
        Self {
            inner: MemStorage::new_with_conf_state(conf_state.into()),
            snapshot: None,
        }
    }

    fn wl(&self) -> RwLockWriteGuard<'_, MemStorageCore> {
        self.inner.wl()
    }

    fn checkpoint_index(&self) -> LogIndex {
        self.snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.metadata.as_ref())
            .map_or(LogIndex::ZERO, |metadata| LogIndex::new(metadata.index))
    }

    fn retained_log_first_index(&self) -> LogIndex {
        self.first_index().map_or_else(
            |_| LogIndex::new(self.checkpoint_index().get().saturating_add(1)),
            LogIndex::new,
        )
    }

    fn install_snapshot(
        &mut self,
        snapshot: Snapshot,
        tail: &[Entry],
        hard_state: HardState,
    ) -> ConsensusResult<()> {
        let snapshot_index = snapshot
            .metadata
            .as_ref()
            .map_or(0, |metadata| metadata.index);
        if tail
            .first()
            .is_some_and(|entry| entry.index != snapshot_index.saturating_add(1))
        {
            return Err(ConsensusError::InvalidState(
                "checkpoint tail does not begin immediately after its snapshot".into(),
            ));
        }
        {
            let mut core = self.inner.wl();
            core.apply_snapshot(snapshot.clone())
                .map_err(|error| ConsensusError::Storage(error.to_string()))?;
            core.append(tail)
                .map_err(|error| ConsensusError::Storage(error.to_string()))?;
            core.set_hardstate(hard_state);
        }
        self.snapshot = Some(snapshot);
        Ok(())
    }
}

impl Storage for EpochRaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.inner.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.inner.entries(low, high, max_size, context)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        self.inner.term(index)
    }

    fn first_index(&self) -> raft::Result<u64> {
        self.inner.first_index()
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.inner.last_index()
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        };
        let checkpoint_index = snapshot
            .metadata
            .as_ref()
            .map_or(0, |metadata| metadata.index);
        if checkpoint_index < request_index {
            return Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        Ok(snapshot.clone())
    }
}

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
nonzero_id!(ReadBarrierId, "read barrier ID must be non-zero");

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

/// One leader- and term-fenced request for a quorum-confirmed read index.
///
/// The request is ephemeral and is never written into the replicated log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBarrierRequest {
    pub group_id: GroupId,
    pub group_epoch: GroupEpoch,
    pub expected_term: Term,
    pub request_id: ReadBarrierId,
}

impl ReadBarrierRequest {
    pub const fn new(
        group_id: GroupId,
        group_epoch: GroupEpoch,
        expected_term: Term,
        request_id: ReadBarrierId,
    ) -> Self {
        Self {
            group_id,
            group_epoch,
            expected_term,
            request_id,
        }
    }
}

/// Proof that a quorum confirmed the request and the local consensus state
/// machine applied through the confirmed index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletedReadBarrier {
    pub request_id: ReadBarrierId,
    pub term: Term,
    pub read_index: LogIndex,
    pub applied_index: LogIndex,
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
    Pending { payload: Vec<u8> },
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
    /// Highest locally installed canonical checkpoint, or zero before the
    /// first checkpoint is created or received.
    pub checkpoint_index: LogIndex,
    /// First Raft log index still retained after logical prefix compaction.
    pub retained_log_first_index: LogIndex,
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
    /// A complete authoritative history installed from a Raft snapshot.
    ///
    /// Runtimes must replace profile state through their replay boundary
    /// before publishing later incremental commits.
    pub installed_checkpoint: Option<InstalledCheckpoint>,
    pub read_barriers: Vec<CompletedReadBarrier>,
    pub status: ConsensusStatus,
}

impl ConsensusOutput {
    pub fn is_idle(&self) -> bool {
        self.messages.is_empty()
            && self.commits.is_empty()
            && self.installed_checkpoint.is_none()
            && self.read_barriers.is_empty()
    }
}

/// Observable facts about a locally created durable consensus checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusCheckpoint {
    pub index: LogIndex,
    pub term: Term,
    pub proposal_count: usize,
    pub encoded_bytes: usize,
}

/// A checkpoint received from another voter and durably installed locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledCheckpoint {
    pub index: LogIndex,
    pub term: Term,
    pub proposals: Vec<CommittedProposal>,
    pub application_snapshot: Option<ApplicationSnapshot>,
}

/// Opaque canonical state owned and validated by one typed profile runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationSnapshot {
    checkpoint_index: LogIndex,
    format_id: [u8; 16],
    format_version: u16,
    state_digest: StateDigest,
    payload: Vec<u8>,
}

impl ApplicationSnapshot {
    pub fn new(
        checkpoint_index: LogIndex,
        format_id: [u8; 16],
        format_version: u16,
        state_digest: StateDigest,
        payload: Vec<u8>,
    ) -> ConsensusResult<Self> {
        let snapshot = Self {
            checkpoint_index,
            format_id,
            format_version,
            state_digest,
            payload,
        };
        validate_application_snapshot(&snapshot)?;
        Ok(snapshot)
    }

    pub const fn checkpoint_index(&self) -> LogIndex {
        self.checkpoint_index
    }

    pub const fn format_id(&self) -> [u8; 16] {
        self.format_id
    }

    pub const fn format_version(&self) -> u16 {
        self.format_version
    }

    pub const fn state_digest(&self) -> StateDigest {
        self.state_digest
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub trait ConsensusAdapter {
    fn status(&self) -> ConsensusStatus;

    fn campaign(&mut self) -> ConsensusResult<ConsensusOutput>;

    fn tick(&mut self) -> ConsensusResult<ConsensusOutput>;

    fn propose(&mut self, proposal: Proposal) -> ConsensusResult<ConsensusOutput>;

    /// Submits an Epoch-internal proposal through a follower when it knows the
    /// current leader. Public write admission continues to use `propose` and
    /// remains leader-only.
    fn forward_proposal(&mut self, proposal: Proposal) -> ConsensusResult<ConsensusOutput>;

    fn read_barrier(&mut self, request: ReadBarrierRequest) -> ConsensusResult<ConsensusOutput>;

    fn cancel_read_barrier(&mut self, request_id: ReadBarrierId) -> ConsensusResult<()>;

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
    DuplicateReadBarrier(ReadBarrierId),
    TooManyReadBarriers,
    CheckpointTooLarge {
        observed_bytes: usize,
        max_bytes: usize,
    },
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
            Self::DuplicateReadBarrier(request_id) => {
                write!(formatter, "read barrier {request_id} is already pending")
            }
            Self::TooManyReadBarriers => write!(
                formatter,
                "pending read barriers reached the limit of {MAX_PENDING_READ_BARRIERS}"
            ),
            Self::CheckpointTooLarge {
                observed_bytes,
                max_bytes,
            } => write!(
                formatter,
                "consensus checkpoint is {observed_bytes} bytes; maximum is {max_bytes}"
            ),
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
    storage: EpochRaftStorage,
    applied_index: LogIndex,
    applied: Vec<CommittedProposal>,
    state_digest: StateDigest,
    stable_generation: u64,
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
            .field("stable_generation", &self.stable_generation)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum ProcessingTrace {
    StableStoreBarrier(u64),
    MessageReleasedAfterStableStoreBarrier(u64),
    Applied(LogIndex),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TrackedProposal {
    Pending { payload: Vec<u8> },
    Committed(CommittedProposal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingReadBarrier {
    term: Term,
    submitted: bool,
    read_index: Option<LogIndex>,
}

#[derive(Debug)]
struct PlannedEntry {
    log_index: LogIndex,
    committed: Option<CommittedProposal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StateDigestScheme {
    CompleteHistoryV1,
    RollingV2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointImage {
    group_id: GroupId,
    group_epoch: GroupEpoch,
    index: LogIndex,
    term: Term,
    state_digest: StateDigest,
    applied_command_count: u64,
    applied: Vec<CommittedProposal>,
    application_snapshot: Option<ApplicationSnapshot>,
}

/// A fixed-three-voter, in-memory adapter used only to establish the Epoch
/// consensus boundary and exercise failure histories.
pub struct InMemoryRaftAdapter {
    node_id: NodeId,
    group_id: GroupId,
    group_epoch: GroupEpoch,
    voters: [NodeId; 3],
    raw_node: RawNode<EpochRaftStorage>,
    applied_index: LogIndex,
    applied: Vec<CommittedProposal>,
    state_digest: StateDigest,
    state_digest_scheme: StateDigestScheme,
    applied_command_count: u64,
    proposals: BTreeMap<ProposalId, TrackedProposal>,
    pending_read_barriers: BTreeMap<ReadBarrierId, PendingReadBarrier>,
    stable_generation: u64,
    disk_store: Option<DiskStableStore>,
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
            .field("pending_read_barriers", &self.pending_read_barriers.len())
            .field("stable_generation", &self.stable_generation)
            .field("disk_backed", &self.disk_store.is_some())
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

/// Facts observed while reopening a disk-backed consensus journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistentRecovery {
    pub stable_generation: u64,
    pub applied_index: LogIndex,
    pub checkpoint_index: LogIndex,
    pub repaired_partial_tail: bool,
}

/// A fixed-three-voter adapter whose Raft stable state and publishable
/// application checkpoint are written to a checksummed local journal.
///
/// This establishes local crash recovery for the consensus boundary. The
/// experimental node runtime can attach an opaque probe or typed profile
/// applier, but the adapter does not, by itself, provide a public
/// quorum-durability mode.
pub struct PersistentRaftAdapter {
    inner: InMemoryRaftAdapter,
    recovery: PersistentRecovery,
}

/// A reopened adapter and any work that became publishable while recovery
/// caught its application checkpoint up to the durable Raft commit index.
#[derive(Debug)]
#[must_use = "recovery output can contain committed receipts or peer messages"]
pub struct PersistentOpenResult {
    pub adapter: PersistentRaftAdapter,
    pub output: ConsensusOutput,
}

impl fmt::Debug for PersistentRaftAdapter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistentRaftAdapter")
            .field("inner", &self.inner)
            .field("recovery", &self.recovery)
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
        let storage = EpochRaftStorage::new_with_conf_state((
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
            stable_generation: 0,
        })
    }

    /// Reconstructs all proposal state from the full in-memory log and rejects
    /// any restart image whose Raft and Epoch state disagree.
    pub fn restart(stable: MemoryStableState) -> ConsensusResult<Self> {
        Self::restart_with_disk_store(stable, None)
    }

    fn restart_with_disk_store(
        stable: MemoryStableState,
        disk_store: Option<DiskStableStore>,
    ) -> ConsensusResult<Self> {
        let validated = validate_persisted_state(PersistedStateView {
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
            state_digest_scheme: validated.state_digest_scheme,
            applied_command_count: validated.applied_command_count,
            proposals: validated.proposals,
            pending_read_barriers: BTreeMap::new(),
            stable_generation: stable.stable_generation,
            disk_store,
            poisoned: None,
            #[cfg(test)]
            processing_trace: vec![ProcessingTrace::StableStoreBarrier(
                stable.stable_generation,
            )],
        })
    }

    /// Extracts and validates an in-memory restart image.
    pub fn into_stable_state(self) -> ConsensusResult<MemoryStableState> {
        self.ensure_healthy()?;
        if self.disk_store.is_some() {
            return Err(ConsensusError::InvalidState(
                "disk-backed adapters must be reopened from their stable journal".into(),
            ));
        }
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
            stable_generation: self.stable_generation,
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

    fn checkpoint_retry_proposals(&self) -> ConsensusResult<Vec<CommittedProposal>> {
        self.ensure_healthy()?;
        checkpoint_retry_suffix(&self.applied)
    }

    /// Reports whether a node-local checkpoint may run now and the applied
    /// index has advanced by the configured threshold. Pending Raft `Ready`
    /// work is a transient busy state, so automatic schedulers skip it.
    pub fn checkpoint_is_due(&self, min_applied_entries: u64) -> ConsensusResult<bool> {
        self.ensure_healthy()?;
        if min_applied_entries == 0 {
            return Err(ConsensusError::InvalidState(
                "checkpoint applied-entry threshold must be non-zero".into(),
            ));
        }
        Ok(!self.raw_node.has_ready()
            && self.applied_index != LogIndex::ZERO
            && self
                .applied_index
                .get()
                .saturating_sub(self.raw_node.store().checkpoint_index().get())
                >= min_applied_entries)
    }

    fn application_snapshot(&self) -> ConsensusResult<Option<ApplicationSnapshot>> {
        self.raw_node
            .store()
            .snapshot
            .as_ref()
            .map(|snapshot| {
                decode_checkpoint_image(snapshot.data.as_ref())
                    .map(|image| image.application_snapshot)
            })
            .transpose()
            .map(Option::flatten)
    }

    /// Creates a durable checkpoint at the current applied index and compacts
    /// the local Raft prefix only after the stable-store barrier succeeds.
    pub fn checkpoint(&mut self) -> ConsensusResult<ConsensusCheckpoint> {
        if self.state_digest_scheme == StateDigestScheme::RollingV2 {
            return Err(ConsensusError::InvalidState(
                "a profile-native adapter requires an application snapshot for every later checkpoint"
                    .into(),
            ));
        }
        self.checkpoint_with_optional_application(None)
    }

    pub fn checkpoint_with_application(
        &mut self,
        application_snapshot: ApplicationSnapshot,
    ) -> ConsensusResult<ConsensusCheckpoint> {
        self.checkpoint_with_optional_application(Some(application_snapshot))
    }

    fn checkpoint_with_optional_application(
        &mut self,
        application_snapshot: Option<ApplicationSnapshot>,
    ) -> ConsensusResult<ConsensusCheckpoint> {
        self.ensure_healthy()?;
        if self.raw_node.has_ready() {
            return Err(ConsensusError::InvalidState(
                "cannot checkpoint while RawNode still has Ready work".into(),
            ));
        }
        if self.applied_index == LogIndex::ZERO {
            return Err(ConsensusError::InvalidState(
                "cannot checkpoint an empty consensus history".into(),
            ));
        }
        if let Some(application) = &application_snapshot
            && application.checkpoint_index != self.applied_index
        {
            return Err(ConsensusError::InvalidState(format!(
                "application snapshot index {} does not equal adapter applied index {}",
                application.checkpoint_index, self.applied_index
            )));
        }
        self.checkpoint_inner(application_snapshot)
    }

    fn checkpoint_inner(
        &mut self,
        application_snapshot: Option<ApplicationSnapshot>,
    ) -> ConsensusResult<ConsensusCheckpoint> {
        let image = self.build_checkpoint_image(application_snapshot)?;
        let snapshot = checkpoint_snapshot(&image, self.voters)?;
        let checkpoint = ConsensusCheckpoint {
            index: image.index,
            term: image.term,
            proposal_count: image.applied.len(),
            encoded_bytes: snapshot.data.len(),
        };
        if self.raw_node.store().checkpoint_index() == image.index {
            let existing = self
                .raw_node
                .store()
                .snapshot
                .as_ref()
                .map(|snapshot| decode_checkpoint_image(snapshot.data.as_ref()))
                .transpose()?;
            if existing.as_ref() == Some(&image) {
                return Ok(checkpoint);
            }
            return Err(ConsensusError::InvalidState(format!(
                "checkpoint index {} already has a different durable image",
                image.index
            )));
        }
        let (hard_state, tail) = self.checkpoint_storage_material(image.index)?;
        let transaction =
            self.persist_and_install_checkpoint(&image, snapshot, checkpoint, hard_state, &tail);
        if let Err(error) = &transaction {
            self.poisoned = Some(error.to_string());
        }
        transaction
    }

    fn build_checkpoint_image(
        &self,
        application_snapshot: Option<ApplicationSnapshot>,
    ) -> ConsensusResult<CheckpointImage> {
        let term = Term::new(
            self.raw_node
                .store()
                .term(self.applied_index.get())
                .map_err(|error| ConsensusError::Storage(error.to_string()))?,
        );
        let (state_digest, applied_command_count, applied) = if application_snapshot.is_some() {
            let state_digest = match self.state_digest_scheme {
                StateDigestScheme::CompleteHistoryV1 => {
                    compute_rolling_state_digest(self.group_id, self.group_epoch, &self.applied)?
                }
                StateDigestScheme::RollingV2 => self.state_digest,
            };
            (
                state_digest,
                self.applied_command_count,
                checkpoint_retry_suffix(&self.applied)?,
            )
        } else {
            (
                self.state_digest,
                self.applied_command_count,
                self.applied.clone(),
            )
        };
        Ok(CheckpointImage {
            group_id: self.group_id,
            group_epoch: self.group_epoch,
            index: self.applied_index,
            term,
            state_digest,
            applied_command_count,
            applied,
            application_snapshot,
        })
    }

    fn checkpoint_storage_material(
        &self,
        checkpoint_index: LogIndex,
    ) -> ConsensusResult<(HardState, Vec<Entry>)> {
        let hard_state = self
            .raw_node
            .store()
            .initial_state()
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .hard_state;
        let last_index = self
            .raw_node
            .store()
            .last_index()
            .map_err(|error| ConsensusError::Storage(error.to_string()))?;
        let tail = if last_index > checkpoint_index.get() {
            self.raw_node
                .store()
                .entries(
                    checkpoint_index.get().checked_add(1).ok_or_else(|| {
                        ConsensusError::InvalidState("checkpoint index overflow".into())
                    })?,
                    last_index.checked_add(1).ok_or_else(|| {
                        ConsensusError::InvalidState("last log index overflow".into())
                    })?,
                    None,
                    GetEntriesContext::empty(false),
                )
                .map_err(|error| ConsensusError::Storage(error.to_string()))?
        } else {
            Vec::new()
        };
        Ok((hard_state, tail))
    }

    fn persist_and_install_checkpoint(
        &mut self,
        image: &CheckpointImage,
        snapshot: Snapshot,
        checkpoint: ConsensusCheckpoint,
        hard_state: HardState,
        tail: &[Entry],
    ) -> ConsensusResult<ConsensusCheckpoint> {
        let stable_checkpoint = StableCheckpoint {
            applied_index: image.index,
            publishable_index: image.index,
            state_digest: image.state_digest,
        };
        let generation = self.next_stable_generation()?;
        if let Some(store) = self.disk_store.as_mut() {
            let observed = store.persist_checkpoint(
                generation,
                &hard_state,
                image,
                tail,
                stable_checkpoint,
            )?;
            if observed != generation {
                return Err(ConsensusError::InvalidState(format!(
                    "stable store returned generation {observed}; expected {generation}"
                )));
            }
        }
        self.stable_generation = generation;
        self.raw_node
            .mut_store()
            .install_snapshot(snapshot, tail, hard_state)?;
        self.state_digest = image.state_digest;
        self.state_digest_scheme = if image.application_snapshot.is_some() {
            StateDigestScheme::RollingV2
        } else {
            StateDigestScheme::CompleteHistoryV1
        };
        self.applied_command_count = image.applied_command_count;
        self.applied.clone_from(&image.applied);
        let validated = validate_persisted_state(PersistedStateView {
            node_id: self.node_id,
            group_id: self.group_id,
            group_epoch: self.group_epoch,
            voters: self.voters,
            storage: self.raw_node.store(),
            applied_index: self.applied_index,
            applied: &self.applied,
            state_digest: self.state_digest,
        })?;
        self.proposals = validated.proposals;
        self.state_digest_scheme = validated.state_digest_scheme;
        self.applied_command_count = validated.applied_command_count;
        #[cfg(test)]
        self.processing_trace
            .push(ProcessingTrace::StableStoreBarrier(generation));
        Ok(checkpoint)
    }

    pub fn lookup_proposal(&self, proposal_id: ProposalId) -> ProposalLookup {
        match self.proposals.get(&proposal_id) {
            None => ProposalLookup::Unknown,
            Some(TrackedProposal::Pending { payload }) => ProposalLookup::Pending {
                payload: payload.clone(),
            },
            Some(TrackedProposal::Committed(committed)) => {
                ProposalLookup::Committed(committed.clone())
            }
        }
    }

    fn validate_read_barrier(&self, request: &ReadBarrierRequest) -> ConsensusResult<()> {
        self.ensure_healthy()?;
        if request.group_id != self.group_id {
            return Err(ConsensusError::GroupMismatch {
                expected: self.group_id,
                observed: request.group_id,
            });
        }
        if request.group_epoch != self.group_epoch {
            return Err(ConsensusError::FencedEpoch {
                expected: self.group_epoch,
                observed: request.group_epoch,
            });
        }
        let status = self.status();
        if request.expected_term != status.term {
            return Err(ConsensusError::StaleTerm {
                current: status.term,
                observed: request.expected_term,
            });
        }
        if status.role != ConsensusRole::Leader {
            return Err(ConsensusError::NotLeader {
                leader_hint: status.leader_id,
            });
        }
        if self.pending_read_barriers.len() >= MAX_PENDING_READ_BARRIERS {
            return Err(ConsensusError::TooManyReadBarriers);
        }
        if self.pending_read_barriers.contains_key(&request.request_id) {
            return Err(ConsensusError::DuplicateReadBarrier(request.request_id));
        }
        Ok(())
    }

    fn observe_read_states(
        &mut self,
        states: impl IntoIterator<Item = raft::ReadState>,
    ) -> ConsensusResult<()> {
        for state in states {
            let request_id = decode_read_barrier_context(&state.request_ctx)?;
            let Some(pending) = self.pending_read_barriers.get_mut(&request_id) else {
                // A caller may cancel or time out after raft-rs accepted the
                // request. The late quorum response is safe to discard.
                continue;
            };
            let read_index = LogIndex::new(state.index);
            if let Some(observed) = pending.read_index
                && observed != read_index
            {
                return Err(ConsensusError::InvalidState(format!(
                    "read barrier {request_id} changed index from {observed} to {read_index}"
                )));
            }
            pending.read_index = Some(read_index);
        }
        Ok(())
    }

    fn submit_pending_read_barriers(&mut self) -> ConsensusResult<()> {
        let status = self.status();
        if status.role != ConsensusRole::Leader || status.commit_index == LogIndex::ZERO {
            return Ok(());
        }
        let committed_term = self
            .raw_node
            .store()
            .term(status.commit_index.get())
            .map_err(|error| ConsensusError::Storage(error.to_string()))?;
        if committed_term != status.term.get() {
            // raft-rs drops ReadIndex requests until this leader has committed
            // an entry in its own term. Keep the Epoch request pending and
            // submit it after the election no-op becomes durable and applied.
            return Ok(());
        }
        let request_ids = self
            .pending_read_barriers
            .iter()
            .filter_map(|(request_id, pending)| {
                (!pending.submitted && pending.term == status.term).then_some(*request_id)
            })
            .collect::<Vec<_>>();
        for request_id in request_ids {
            self.raw_node
                .read_index(encode_read_barrier_context(request_id).to_vec());
            if let Some(pending) = self.pending_read_barriers.get_mut(&request_id) {
                pending.submitted = true;
            }
        }
        Ok(())
    }

    fn take_completed_read_barriers(&mut self) -> ConsensusResult<Vec<CompletedReadBarrier>> {
        let status = self.status();
        if status.role != ConsensusRole::Leader {
            self.pending_read_barriers.clear();
            return Ok(Vec::new());
        }
        self.pending_read_barriers
            .retain(|_, pending| pending.term == status.term);

        let completed_ids = self
            .pending_read_barriers
            .iter()
            .filter_map(|(request_id, pending)| {
                pending
                    .read_index
                    .filter(|read_index| *read_index <= status.applied_index)
                    .map(|_| *request_id)
            })
            .collect::<Vec<_>>();
        let mut completed = Vec::with_capacity(completed_ids.len());
        for request_id in completed_ids {
            let pending = self
                .pending_read_barriers
                .remove(&request_id)
                .ok_or_else(|| {
                    ConsensusError::InvalidState(format!(
                        "completed read barrier {request_id} disappeared"
                    ))
                })?;
            let read_index = pending.read_index.ok_or_else(|| {
                ConsensusError::InvalidState(format!(
                    "completed read barrier {request_id} has no read index"
                ))
            })?;
            if read_index > status.commit_index {
                return Err(ConsensusError::InvalidState(format!(
                    "read barrier {request_id} index {read_index} exceeds commit index {}",
                    status.commit_index
                )));
            }
            completed.push(CompletedReadBarrier {
                request_id,
                term: pending.term,
                read_index,
                applied_index: status.applied_index,
            });
        }
        Ok(completed)
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
        let mut installed_checkpoint = None;
        let mut iterations = 0_u16;
        self.submit_pending_read_barriers()?;

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
            let incoming_checkpoint =
                if ready.snapshot().metadata.is_some() || !ready.snapshot().data.is_empty() {
                    Some(decode_checkpoint_snapshot(
                        ready.snapshot(),
                        self.group_id,
                        self.group_epoch,
                        self.voters,
                    )?)
                } else {
                    None
                };
            let ready_read_states = ready.take_read_states();
            let immediate_messages = self.wrap_messages(ready.take_messages(), None)?;
            let persisted_messages_raw = ready.take_persisted_messages();
            let ready_committed = ready.take_committed_entries();
            let ready_barrier = if let Some(image) = incoming_checkpoint {
                if !ready_committed.is_empty() {
                    return Err(ConsensusError::InvalidState(
                        "snapshot Ready unexpectedly contains committed tail entries".into(),
                    ));
                }
                let (barrier, installed) = self.persist_snapshot_ready(&ready, image)?;
                if installed_checkpoint.replace(installed).is_some() {
                    return Err(ConsensusError::InvalidState(
                        "one consensus output installed more than one checkpoint".into(),
                    ));
                }
                Some(barrier)
            } else {
                let ready_plan = self.prevalidate_committed_batch(&ready_committed)?;
                let ready_checkpoint = self.project_checkpoint(&ready_plan)?;
                let barrier = self.persist_ready(&ready, ready_checkpoint)?;
                self.apply_prevalidated_batch(ready_plan, ready_checkpoint, &mut commits);
                barrier
            };
            if !persisted_messages_raw.is_empty() && ready_barrier.is_none() {
                return Err(ConsensusError::InvalidState(
                    "Ready released persisted messages without a stable-store barrier".into(),
                ));
            }
            let persisted_messages = self.wrap_messages(persisted_messages_raw, ready_barrier)?;

            outbound.extend(immediate_messages);
            outbound.extend(persisted_messages);
            self.observe_read_states(ready_read_states)?;

            let mut light_ready = self.raw_node.advance(ready);
            let light_messages_raw = light_ready.take_messages();
            let light_committed = light_ready.take_committed_entries();
            let light_plan = self.prevalidate_committed_batch(&light_committed)?;
            let light_checkpoint = self.project_checkpoint(&light_plan)?;
            let light_barrier = self.persist_light_ready(
                light_ready.commit_index(),
                light_checkpoint,
                ready_barrier,
            )?;
            let light_messages = self.wrap_messages(light_messages_raw, light_barrier)?;
            outbound.extend(light_messages);
            self.apply_prevalidated_batch(light_plan, light_checkpoint, &mut commits);
            self.raw_node.advance_apply();
            self.submit_pending_read_barriers()?;
        }

        let validated = validate_persisted_state(PersistedStateView {
            node_id: self.node_id,
            group_id: self.group_id,
            group_epoch: self.group_epoch,
            voters: self.voters,
            storage: self.raw_node.store(),
            applied_index: self.applied_index,
            applied: &self.applied,
            state_digest: self.state_digest,
        })?;
        self.proposals = validated.proposals;
        self.state_digest_scheme = validated.state_digest_scheme;
        self.applied_command_count = validated.applied_command_count;
        let read_barriers = self.take_completed_read_barriers()?;

        Ok(ConsensusOutput {
            messages: outbound,
            commits,
            installed_checkpoint,
            read_barriers,
            status: self.status(),
        })
    }

    fn persist_ready(
        &mut self,
        ready: &raft::Ready,
        checkpoint: StableCheckpoint,
    ) -> ConsensusResult<Option<u64>> {
        if ready.snapshot().metadata.is_some() || !ready.snapshot().data.is_empty() {
            return Err(ConsensusError::InvalidState(
                "ordinary Ready persistence received a snapshot".into(),
            ));
        }
        let entries = ready.entries().clone();
        let current_hard_state = self
            .raw_node
            .store()
            .initial_state()
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .hard_state;
        let hard_state = ready.hs().cloned().unwrap_or(current_hard_state.clone());
        let changed = !entries.is_empty()
            || hard_state != current_hard_state
            || checkpoint != self.current_checkpoint();
        let barrier = if changed {
            Some(self.persist_stable_transition(&hard_state, &entries, checkpoint)?)
        } else {
            Some(self.current_stable_store_barrier())
        };
        {
            let mut storage = self.raw_node.mut_store().wl();
            if !entries.is_empty() {
                storage
                    .append(&entries)
                    .map_err(|error| ConsensusError::Storage(error.to_string()))?;
            }
            if hard_state != current_hard_state {
                storage.set_hardstate(hard_state);
            }
        }
        self.proposals = build_proposal_tracking(
            self.group_id,
            self.group_epoch,
            self.raw_node.store(),
            self.applied_index,
            &self.applied,
            self.voters,
        )?;
        Ok(barrier)
    }

    fn persist_snapshot_ready(
        &mut self,
        ready: &raft::Ready,
        image: CheckpointImage,
    ) -> ConsensusResult<(u64, InstalledCheckpoint)> {
        if image.index < self.applied_index {
            return Err(ConsensusError::InvalidState(format!(
                "incoming checkpoint index {} is below applied index {}",
                image.index, self.applied_index
            )));
        }
        if image.application_snapshot.is_none()
            && image.applied.get(..self.applied.len()) != Some(self.applied.as_slice())
        {
            return Err(ConsensusError::InvalidState(
                "incoming v1 checkpoint does not extend the local applied history".into(),
            ));
        }
        let snapshot = ready.snapshot().clone();
        let entries = ready.entries().clone();
        let current_hard_state = self
            .raw_node
            .store()
            .initial_state()
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .hard_state;
        let hard_state = ready.hs().cloned().unwrap_or(current_hard_state);
        let checkpoint = StableCheckpoint {
            applied_index: image.index,
            publishable_index: image.index,
            state_digest: image.state_digest,
        };
        let generation = self.next_stable_generation()?;
        if let Some(store) = self.disk_store.as_mut() {
            let observed =
                store.persist_checkpoint(generation, &hard_state, &image, &entries, checkpoint)?;
            if observed != generation {
                return Err(ConsensusError::InvalidState(format!(
                    "stable store returned generation {observed}; expected {generation}"
                )));
            }
        }
        self.stable_generation = generation;
        self.raw_node
            .mut_store()
            .install_snapshot(snapshot, &entries, hard_state)?;
        self.applied_index = image.index;
        self.state_digest = image.state_digest;
        self.applied.clone_from(&image.applied);
        let validated = validate_persisted_state(PersistedStateView {
            node_id: self.node_id,
            group_id: self.group_id,
            group_epoch: self.group_epoch,
            voters: self.voters,
            storage: self.raw_node.store(),
            applied_index: self.applied_index,
            applied: &self.applied,
            state_digest: self.state_digest,
        })?;
        self.proposals = validated.proposals;
        self.state_digest_scheme = validated.state_digest_scheme;
        self.applied_command_count = validated.applied_command_count;
        #[cfg(test)]
        self.processing_trace
            .push(ProcessingTrace::StableStoreBarrier(generation));
        Ok((
            generation,
            InstalledCheckpoint {
                index: image.index,
                term: image.term,
                application_snapshot: image.application_snapshot.clone(),
                proposals: image.applied,
            },
        ))
    }

    fn next_stable_generation(&self) -> ConsensusResult<u64> {
        self.stable_generation.checked_add(1).ok_or_else(|| {
            ConsensusError::InvalidState("stable-store barrier generation overflow".into())
        })
    }

    fn persist_stable_transition(
        &mut self,
        hard_state: &HardState,
        entries: &[Entry],
        checkpoint: StableCheckpoint,
    ) -> ConsensusResult<u64> {
        if checkpoint.applied_index.get() > hard_state.commit {
            return Err(ConsensusError::InvalidState(format!(
                "stable checkpoint applied index {} exceeds durable commit {}",
                checkpoint.applied_index, hard_state.commit
            )));
        }
        let generation = self.next_stable_generation()?;
        if let Some(store) = self.disk_store.as_mut() {
            let observed = store.persist(generation, hard_state, entries, checkpoint)?;
            if observed != generation {
                return Err(ConsensusError::InvalidState(format!(
                    "stable store returned generation {observed}; expected {generation}"
                )));
            }
        }
        self.stable_generation = generation;
        #[cfg(test)]
        self.processing_trace
            .push(ProcessingTrace::StableStoreBarrier(generation));
        Ok(generation)
    }

    fn persist_light_ready(
        &mut self,
        commit_index: Option<u64>,
        checkpoint: StableCheckpoint,
        fallback_barrier: Option<u64>,
    ) -> ConsensusResult<Option<u64>> {
        let current_hard_state = self
            .raw_node
            .store()
            .initial_state()
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .hard_state;
        let mut next_hard_state = current_hard_state.clone();
        if let Some(commit_index) = commit_index {
            next_hard_state.commit = commit_index;
        }
        let changed =
            next_hard_state != current_hard_state || checkpoint != self.current_checkpoint();
        if !changed {
            return Ok(fallback_barrier.or(Some(self.current_stable_store_barrier())));
        }
        let generation = self.persist_stable_transition(&next_hard_state, &[], checkpoint)?;
        if next_hard_state != current_hard_state {
            self.raw_node
                .mut_store()
                .wl()
                .set_hardstate(next_hard_state);
        }
        Ok(Some(generation))
    }

    fn current_checkpoint(&self) -> StableCheckpoint {
        StableCheckpoint {
            applied_index: self.applied_index,
            publishable_index: self.applied_index,
            state_digest: self.state_digest,
        }
    }

    fn project_checkpoint(&self, planned: &[PlannedEntry]) -> ConsensusResult<StableCheckpoint> {
        let applied_index = planned
            .last()
            .map_or(self.applied_index, |entry| entry.log_index);
        let state_digest = match self.state_digest_scheme {
            StateDigestScheme::CompleteHistoryV1 => {
                let mut projected_applied = self.applied.clone();
                projected_applied
                    .extend(planned.iter().filter_map(|entry| entry.committed.clone()));
                compute_state_digest(self.group_id, self.group_epoch, &projected_applied)?
            }
            StateDigestScheme::RollingV2 => {
                let mut digest = self.state_digest;
                for committed in planned.iter().filter_map(|entry| entry.committed.as_ref()) {
                    digest = advance_rolling_state_digest(digest, committed)?;
                }
                digest
            }
        };
        let unique_count = planned
            .iter()
            .filter(|entry| entry.committed.is_some())
            .count();
        let unique_count = u64::try_from(unique_count).map_err(|_| {
            ConsensusError::InvalidState("committed batch command count is too large".into())
        })?;
        self.applied_command_count
            .checked_add(unique_count)
            .ok_or_else(|| ConsensusError::InvalidState("applied command count overflow".into()))?;
        Ok(StableCheckpoint {
            applied_index,
            publishable_index: applied_index,
            state_digest,
        })
    }

    const fn current_stable_store_barrier(&self) -> u64 {
        self.stable_generation
    }

    fn wrap_messages(
        &mut self,
        messages: Vec<RaftMessage>,
        stable_store_barrier: Option<u64>,
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
        if let Some(generation) = stable_store_barrier {
            self.processing_trace.extend(
                wrapped
                    .iter()
                    .map(|_| ProcessingTrace::MessageReleasedAfterStableStoreBarrier(generation)),
            );
        }
        #[cfg(not(test))]
        let _ = stable_store_barrier;
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
        checkpoint: StableCheckpoint,
        new_commits: &mut Vec<CommittedProposal>,
    ) {
        debug_assert_eq!(
            checkpoint.applied_index,
            planned
                .last()
                .map_or(self.applied_index, |entry| entry.log_index)
        );
        debug_assert_eq!(checkpoint.publishable_index, checkpoint.applied_index);
        debug_assert!(self.disk_store.as_ref().is_none_or(|store| {
            store.stable_generation() == self.stable_generation && store.checkpoint() == checkpoint
        }));
        for planned_entry in planned {
            if let Some(committed) = planned_entry.committed {
                self.proposals.insert(
                    committed.receipt.proposal_id,
                    TrackedProposal::Committed(committed.clone()),
                );
                self.applied.push(committed.clone());
                new_commits.push(committed);
                self.applied_command_count = self
                    .applied_command_count
                    .checked_add(1)
                    .expect("validated applied command count must not overflow");
            }
            self.applied_index = planned_entry.log_index;
            #[cfg(test)]
            self.processing_trace
                .push(ProcessingTrace::Applied(planned_entry.log_index));
        }
        self.state_digest = checkpoint.state_digest;
    }

    fn validate_proposal_common(&self, proposal: &Proposal) -> ConsensusResult<ConsensusStatus> {
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
        Ok(status)
    }

    fn validate_proposal(&self, proposal: &Proposal) -> ConsensusResult<()> {
        let status = self.validate_proposal_common(proposal)?;
        if status.role != ConsensusRole::Leader {
            return Err(ConsensusError::NotLeader {
                leader_hint: status.leader_id,
            });
        }
        Ok(())
    }

    fn validate_forwarded_proposal(&self, proposal: &Proposal) -> ConsensusResult<()> {
        let status = self.validate_proposal_common(proposal)?;
        if status.role != ConsensusRole::Leader && status.leader_id.is_none() {
            return Err(ConsensusError::NotLeader { leader_hint: None });
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
            checkpoint_index: self.raw_node.store().checkpoint_index(),
            retained_log_first_index: self.raw_node.store().retained_log_first_index(),
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

    fn forward_proposal(&mut self, proposal: Proposal) -> ConsensusResult<ConsensusOutput> {
        self.validate_forwarded_proposal(&proposal)?;
        let encoded = encode_command(&proposal)?;
        self.raw_node
            .propose(Vec::new(), encoded)
            .map_err(|error| ConsensusError::Library(error.to_string()))?;
        self.process_ready()
    }

    fn read_barrier(&mut self, request: ReadBarrierRequest) -> ConsensusResult<ConsensusOutput> {
        self.validate_read_barrier(&request)?;
        self.pending_read_barriers.insert(
            request.request_id,
            PendingReadBarrier {
                term: request.expected_term,
                submitted: false,
                read_index: None,
            },
        );
        self.submit_pending_read_barriers()?;
        self.process_ready()
    }

    fn cancel_read_barrier(&mut self, request_id: ReadBarrierId) -> ConsensusResult<()> {
        self.ensure_healthy()?;
        self.pending_read_barriers.remove(&request_id);
        Ok(())
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
        if MessageType::from_i32(raft_message.msg_type) == Some(MessageType::MsgSnapshot) {
            let snapshot = raft_message.snapshot.as_ref().ok_or_else(|| {
                ConsensusError::InvalidMessage("snapshot message has no snapshot".into())
            })?;
            decode_checkpoint_snapshot(snapshot, self.group_id, self.group_epoch, self.voters)?;
        }
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

impl PersistentRaftAdapter {
    /// Opens or creates a per-node, per-group stable-state journal.
    ///
    /// The parent directory must already exist. A live adapter owns an
    /// exclusive writer lock, so a second open of the same path fails closed.
    pub fn open(
        path: impl AsRef<Path>,
        node_id: NodeId,
        group_id: GroupId,
        group_epoch: GroupEpoch,
        voters: [NodeId; 3],
    ) -> ConsensusResult<PersistentOpenResult> {
        let identity = StableIdentity {
            node_id,
            group_id,
            group_epoch,
            voters,
        };
        let recovered = DiskStableStore::open(path.as_ref(), identity)?;
        let recovery = PersistentRecovery {
            stable_generation: recovered.stable_generation,
            applied_index: recovered.checkpoint.applied_index,
            checkpoint_index: recovered.storage.checkpoint_index(),
            repaired_partial_tail: recovered.repaired_partial_tail,
        };
        let stable = MemoryStableState {
            node_id,
            group_id,
            group_epoch,
            voters,
            storage: recovered.storage,
            applied_index: recovered.checkpoint.applied_index,
            applied: recovered.applied,
            state_digest: recovered.checkpoint.state_digest,
            stable_generation: recovered.stable_generation,
        };
        let inner = InMemoryRaftAdapter::restart_with_disk_store(stable, Some(recovered.store))?;
        let mut adapter = Self { inner, recovery };
        let output = adapter.inner.process_ready()?;
        Ok(PersistentOpenResult { adapter, output })
    }

    pub const fn recovery(&self) -> PersistentRecovery {
        self.recovery
    }

    pub const fn state_digest(&self) -> StateDigest {
        self.inner.state_digest()
    }

    pub fn applied_proposals(&self) -> &[CommittedProposal] {
        self.inner.applied_proposals()
    }

    pub fn checkpoint_retry_proposals(&self) -> ConsensusResult<Vec<CommittedProposal>> {
        self.inner.checkpoint_retry_proposals()
    }

    pub fn checkpoint_is_due(&self, min_applied_entries: u64) -> ConsensusResult<bool> {
        self.inner.checkpoint_is_due(min_applied_entries)
    }

    pub fn application_snapshot(&self) -> ConsensusResult<Option<ApplicationSnapshot>> {
        self.inner.application_snapshot()
    }

    pub fn lookup_proposal(&self, proposal_id: ProposalId) -> ProposalLookup {
        self.inner.lookup_proposal(proposal_id)
    }

    pub fn checkpoint(&mut self) -> ConsensusResult<ConsensusCheckpoint> {
        self.inner.checkpoint()
    }

    pub fn checkpoint_with_application(
        &mut self,
        application_snapshot: ApplicationSnapshot,
    ) -> ConsensusResult<ConsensusCheckpoint> {
        self.inner.checkpoint_with_application(application_snapshot)
    }
}

impl ConsensusAdapter for PersistentRaftAdapter {
    fn status(&self) -> ConsensusStatus {
        self.inner.status()
    }

    fn campaign(&mut self) -> ConsensusResult<ConsensusOutput> {
        self.inner.campaign()
    }

    fn tick(&mut self) -> ConsensusResult<ConsensusOutput> {
        self.inner.tick()
    }

    fn propose(&mut self, proposal: Proposal) -> ConsensusResult<ConsensusOutput> {
        self.inner.propose(proposal)
    }

    fn forward_proposal(&mut self, proposal: Proposal) -> ConsensusResult<ConsensusOutput> {
        self.inner.forward_proposal(proposal)
    }

    fn read_barrier(&mut self, request: ReadBarrierRequest) -> ConsensusResult<ConsensusOutput> {
        self.inner.read_barrier(request)
    }

    fn cancel_read_barrier(&mut self, request_id: ReadBarrierId) -> ConsensusResult<()> {
        self.inner.cancel_read_barrier(request_id)
    }

    fn receive(&mut self, message: PeerMessage) -> ConsensusResult<ConsensusOutput> {
        self.inner.receive(message)
    }

    fn transfer_leadership(&mut self, target: NodeId) -> ConsensusResult<ConsensusOutput> {
        self.inner.transfer_leadership(target)
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
        max_size_per_msg: 0,
        max_uncommitted_size: MAX_UNCOMMITTED_BYTES,
        max_committed_size_per_ready: MAX_COMMITTED_BYTES_PER_READY,
        // Public admission remains explicitly leader-only. Enabling the Raft
        // transport path lets the regional runtime use the separate,
        // internal-only `forward_proposal` API for cross-tablet execution.
        disable_proposal_forwarding: false,
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

fn expected_conf_state(voters: [NodeId; 3]) -> ConfState {
    ConfState::from((
        voters.iter().map(|voter| voter.get()).collect::<Vec<_>>(),
        Vec::<u64>::new(),
    ))
}

fn validate_application_snapshot(snapshot: &ApplicationSnapshot) -> ConsensusResult<()> {
    if snapshot.checkpoint_index == LogIndex::ZERO {
        return Err(ConsensusError::InvalidState(
            "application snapshot checkpoint index must be nonzero".into(),
        ));
    }
    if snapshot.format_id == [0; 16] || snapshot.format_version == 0 {
        return Err(ConsensusError::InvalidState(
            "application snapshot format ID and version must be nonzero".into(),
        ));
    }
    if snapshot.payload.len() > MAX_APPLICATION_SNAPSHOT_BYTES {
        return Err(ConsensusError::CheckpointTooLarge {
            observed_bytes: snapshot.payload.len(),
            max_bytes: MAX_APPLICATION_SNAPSHOT_BYTES,
        });
    }
    Ok(())
}

fn encoded_retry_bytes(applied: &[CommittedProposal]) -> ConsensusResult<usize> {
    let mut bytes = 0_usize;
    for proposal in applied {
        if proposal.payload.len() > MAX_PROPOSAL_PAYLOAD_BYTES {
            return Err(ConsensusError::InvalidState(format!(
                "consensus checkpoint proposal payload is {} bytes; maximum is {MAX_PROPOSAL_PAYLOAD_BYTES}",
                proposal.payload.len()
            )));
        }
        bytes = bytes
            .checked_add(SNAPSHOT_PROPOSAL_FIXED_LEN)
            .and_then(|value| value.checked_add(proposal.payload.len()))
            .ok_or_else(|| {
                ConsensusError::InvalidState("consensus checkpoint length overflow".into())
            })?;
    }
    Ok(bytes)
}

fn checkpoint_retry_suffix(
    applied: &[CommittedProposal],
) -> ConsensusResult<Vec<CommittedProposal>> {
    let mut retained_bytes = 0_usize;
    let mut retained_count = 0_usize;
    let mut start = applied.len();
    for proposal in applied.iter().rev() {
        let proposal_bytes = SNAPSHOT_PROPOSAL_FIXED_LEN
            .checked_add(proposal.payload.len())
            .ok_or_else(|| {
                ConsensusError::InvalidState("checkpoint retry length overflow".into())
            })?;
        if retained_count == MAX_CHECKPOINT_RETRY_PROPOSALS
            || retained_bytes
                .checked_add(proposal_bytes)
                .is_none_or(|bytes| bytes > MAX_CHECKPOINT_RETRY_BYTES)
        {
            break;
        }
        retained_bytes += proposal_bytes;
        retained_count += 1;
        start -= 1;
    }
    if !applied.is_empty() && retained_count == 0 {
        return Err(ConsensusError::InvalidState(
            "the newest proposal does not fit the checkpoint retry budget".into(),
        ));
    }
    Ok(applied[start..].to_vec())
}

fn validate_checkpoint_image(image: &CheckpointImage) -> ConsensusResult<()> {
    if image.index == LogIndex::ZERO || image.term == Term::ZERO {
        return Err(ConsensusError::InvalidState(
            "consensus checkpoint index and term must be nonzero".into(),
        ));
    }
    validate_applied_receipts(
        image.group_id,
        image.group_epoch,
        image.index,
        &image.applied,
    )?;
    if image
        .applied
        .iter()
        .any(|proposal| proposal.receipt.term == Term::ZERO)
    {
        return Err(ConsensusError::InvalidState(
            "consensus checkpoint contains a zero-term proposal".into(),
        ));
    }
    match &image.application_snapshot {
        None => {
            let applied_count = u64::try_from(image.applied.len()).map_err(|_| {
                ConsensusError::InvalidState("consensus checkpoint history is too large".into())
            })?;
            if image.applied_command_count != applied_count {
                return Err(ConsensusError::InvalidState(
                    "EPSN v1 command count does not match its complete proposal history".into(),
                ));
            }
            let expected_digest =
                compute_state_digest(image.group_id, image.group_epoch, &image.applied)?;
            if image.state_digest != expected_digest {
                return Err(ConsensusError::InvalidState(
                    "consensus checkpoint digest does not match its proposal history".into(),
                ));
            }
        }
        Some(application) => {
            validate_application_snapshot(application)?;
            if application.checkpoint_index != image.index {
                return Err(ConsensusError::InvalidState(
                    "application snapshot index does not match its consensus checkpoint".into(),
                ));
            }
            let retry_count = u64::try_from(image.applied.len()).map_err(|_| {
                ConsensusError::InvalidState("checkpoint retry history is too large".into())
            })?;
            if image.applied_command_count < retry_count {
                return Err(ConsensusError::InvalidState(
                    "checkpoint total command count is below its retry suffix count".into(),
                ));
            }
            if image.applied.len() > MAX_CHECKPOINT_RETRY_PROPOSALS {
                return Err(ConsensusError::InvalidState(format!(
                    "checkpoint retry suffix has {} proposals; maximum is {MAX_CHECKPOINT_RETRY_PROPOSALS}",
                    image.applied.len()
                )));
            }
            let retry_bytes = encoded_retry_bytes(&image.applied)?;
            if retry_bytes > MAX_CHECKPOINT_RETRY_BYTES {
                return Err(ConsensusError::InvalidState(format!(
                    "checkpoint retry suffix is {retry_bytes} bytes; maximum is {MAX_CHECKPOINT_RETRY_BYTES}"
                )));
            }
        }
    }
    Ok(())
}

fn encode_checkpoint_image(image: &CheckpointImage) -> ConsensusResult<Vec<u8>> {
    validate_checkpoint_image(image)?;
    if image.application_snapshot.is_some() {
        encode_v2_checkpoint_image(image)
    } else {
        encode_v1_checkpoint_image(image)
    }
}

fn encode_v1_checkpoint_image(image: &CheckpointImage) -> ConsensusResult<Vec<u8>> {
    let proposal_count = u32::try_from(image.applied.len()).map_err(|_| {
        ConsensusError::InvalidState("consensus checkpoint has too many proposals".into())
    })?;
    let capacity = SNAPSHOT_V1_HEADER_LEN
        .checked_add(encoded_retry_bytes(&image.applied)?)
        .ok_or_else(|| ConsensusError::InvalidState("checkpoint length overflow".into()))?;
    if capacity > MAX_V1_SNAPSHOT_DATA_BYTES {
        return Err(ConsensusError::CheckpointTooLarge {
            observed_bytes: capacity,
            max_bytes: MAX_V1_SNAPSHOT_DATA_BYTES,
        });
    }

    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(&SNAPSHOT_MAGIC);
    encoded.extend_from_slice(&SNAPSHOT_V1_VERSION.to_be_bytes());
    encoded.extend_from_slice(&0_u16.to_be_bytes());
    encoded.extend_from_slice(&image.group_id.get().to_be_bytes());
    encoded.extend_from_slice(&image.group_epoch.get().to_be_bytes());
    encoded.extend_from_slice(&image.index.get().to_be_bytes());
    encoded.extend_from_slice(&image.term.get().to_be_bytes());
    encoded.extend_from_slice(&image.state_digest);
    encoded.extend_from_slice(&proposal_count.to_be_bytes());
    for proposal in &image.applied {
        encoded.extend_from_slice(&proposal.receipt.proposal_id.get().to_be_bytes());
        encoded.extend_from_slice(&proposal.receipt.term.get().to_be_bytes());
        encoded.extend_from_slice(&proposal.receipt.log_index.get().to_be_bytes());
        let payload_len = u32::try_from(proposal.payload.len()).map_err(|_| {
            ConsensusError::InvalidState(
                "consensus checkpoint proposal exceeds its length field".into(),
            )
        })?;
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        encoded.extend_from_slice(&proposal.payload);
    }
    debug_assert_eq!(encoded.len(), capacity);
    Ok(encoded)
}

fn encode_v2_checkpoint_image(image: &CheckpointImage) -> ConsensusResult<Vec<u8>> {
    let application = image
        .application_snapshot
        .as_ref()
        .expect("v2 encoder requires an application snapshot");
    let proposal_count = u32::try_from(image.applied.len())
        .map_err(|_| ConsensusError::InvalidState("checkpoint retry count exceeds u32".into()))?;
    let payload_len = u32::try_from(application.payload.len()).map_err(|_| {
        ConsensusError::InvalidState("application snapshot length exceeds u32".into())
    })?;
    let retry_bytes = encoded_retry_bytes(&image.applied)?;
    let capacity = SNAPSHOT_V2_HEADER_LEN
        .checked_add(application.payload.len())
        .and_then(|value| value.checked_add(retry_bytes))
        .and_then(|value| value.checked_add(SNAPSHOT_V2_TRAILER_LEN))
        .ok_or_else(|| ConsensusError::InvalidState("checkpoint length overflow".into()))?;
    if capacity > MAX_SNAPSHOT_DATA_BYTES {
        return Err(ConsensusError::CheckpointTooLarge {
            observed_bytes: capacity,
            max_bytes: MAX_SNAPSHOT_DATA_BYTES,
        });
    }

    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(&SNAPSHOT_MAGIC);
    encoded.extend_from_slice(&SNAPSHOT_V2_VERSION.to_be_bytes());
    encoded.extend_from_slice(&1_u16.to_be_bytes());
    encoded.extend_from_slice(&image.group_id.get().to_be_bytes());
    encoded.extend_from_slice(&image.group_epoch.get().to_be_bytes());
    encoded.extend_from_slice(&image.index.get().to_be_bytes());
    encoded.extend_from_slice(&image.term.get().to_be_bytes());
    encoded.extend_from_slice(&image.state_digest);
    encoded.extend_from_slice(&image.applied_command_count.to_be_bytes());
    encoded.extend_from_slice(&proposal_count.to_be_bytes());
    encoded.extend_from_slice(&application.format_id);
    encoded.extend_from_slice(&application.format_version.to_be_bytes());
    encoded.extend_from_slice(&0_u16.to_be_bytes());
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(&application.state_digest);
    encoded.extend_from_slice(Sha256::digest(&application.payload).as_slice());
    encoded.extend_from_slice(&application.payload);
    encode_checkpoint_proposals(&mut encoded, &image.applied)?;
    let image_digest = Sha256::digest(&encoded);
    encoded.extend_from_slice(image_digest.as_slice());
    debug_assert_eq!(encoded.len(), capacity);
    Ok(encoded)
}

fn encode_checkpoint_proposals(
    encoded: &mut Vec<u8>,
    applied: &[CommittedProposal],
) -> ConsensusResult<()> {
    for proposal in applied {
        encoded.extend_from_slice(&proposal.receipt.proposal_id.get().to_be_bytes());
        encoded.extend_from_slice(&proposal.receipt.term.get().to_be_bytes());
        encoded.extend_from_slice(&proposal.receipt.log_index.get().to_be_bytes());
        let payload_len = u32::try_from(proposal.payload.len()).map_err(|_| {
            ConsensusError::InvalidState(
                "consensus checkpoint proposal exceeds its length field".into(),
            )
        })?;
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        encoded.extend_from_slice(&proposal.payload);
    }
    Ok(())
}

fn decode_checkpoint_image(encoded: &[u8]) -> ConsensusResult<CheckpointImage> {
    if encoded.len() > MAX_SNAPSHOT_DATA_BYTES {
        return Err(ConsensusError::InvalidMessage(format!(
            "consensus checkpoint is {} bytes; maximum is {MAX_SNAPSHOT_DATA_BYTES}",
            encoded.len()
        )));
    }
    if encoded.len() < 8 || encoded[..4] != SNAPSHOT_MAGIC {
        return Err(ConsensusError::InvalidMessage(
            "consensus checkpoint has an invalid header".into(),
        ));
    }
    let version = u16::from_be_bytes([encoded[4], encoded[5]]);
    match version {
        SNAPSHOT_V1_VERSION => decode_v1_checkpoint_image(encoded),
        SNAPSHOT_V2_VERSION => decode_v2_checkpoint_image(encoded),
        _ => Err(ConsensusError::Unsupported(format!(
            "unsupported consensus checkpoint version {version}"
        ))),
    }
}

fn decode_v1_checkpoint_image(encoded: &[u8]) -> ConsensusResult<CheckpointImage> {
    if encoded.len() > MAX_V1_SNAPSHOT_DATA_BYTES || encoded.len() < SNAPSHOT_V1_HEADER_LEN {
        return Err(ConsensusError::InvalidMessage(format!(
            "EPSN v1 checkpoint is {} bytes; maximum is {MAX_V1_SNAPSHOT_DATA_BYTES}",
            encoded.len()
        )));
    }
    let mut reader = SnapshotReader::new(encoded);
    reader.read_exact(4, "magic")?;
    let version = reader.read_u16("version")?;
    debug_assert_eq!(version, SNAPSHOT_V1_VERSION);
    let flags = reader.read_u16("flags")?;
    if flags != 0 {
        return Err(ConsensusError::Unsupported(format!(
            "unsupported consensus checkpoint flags {flags:#06x}"
        )));
    }
    let group_id = GroupId::new(reader.read_u64("group ID")?)?;
    let group_epoch = GroupEpoch::new(reader.read_u64("group epoch")?)?;
    let index = LogIndex::new(reader.read_u64("checkpoint index")?);
    let term = Term::new(reader.read_u64("checkpoint term")?);
    let state_digest = reader.read_array("state digest")?;
    let proposal_count = reader.read_u32("proposal count")? as usize;
    if proposal_count > reader.remaining_len() / SNAPSHOT_PROPOSAL_FIXED_LEN {
        return Err(ConsensusError::InvalidMessage(
            "consensus checkpoint proposal count exceeds its remaining bytes".into(),
        ));
    }
    let mut applied = Vec::with_capacity(proposal_count);
    for _ in 0..proposal_count {
        let proposal_id = ProposalId::new(reader.read_u64("proposal ID")?)?;
        let proposal_term = Term::new(reader.read_u64("proposal term")?);
        let log_index = LogIndex::new(reader.read_u64("proposal log index")?);
        let payload_len = reader.read_u32("proposal payload length")? as usize;
        if payload_len > MAX_PROPOSAL_PAYLOAD_BYTES {
            return Err(ConsensusError::InvalidMessage(format!(
                "checkpoint proposal payload is {payload_len} bytes; maximum is {MAX_PROPOSAL_PAYLOAD_BYTES}"
            )));
        }
        let payload = reader.read_exact(payload_len, "proposal payload")?.to_vec();
        applied.push(CommittedProposal {
            receipt: CommitReceipt {
                group_id,
                group_epoch,
                proposal_id,
                term: proposal_term,
                log_index,
            },
            payload,
        });
    }
    reader.finish()?;
    let image = CheckpointImage {
        group_id,
        group_epoch,
        index,
        term,
        state_digest,
        applied_command_count: u64::try_from(applied.len()).map_err(|_| {
            ConsensusError::InvalidState("checkpoint applied history is too large".into())
        })?,
        applied,
        application_snapshot: None,
    };
    validate_checkpoint_image(&image)?;
    if encode_checkpoint_image(&image)? != encoded {
        return Err(ConsensusError::InvalidMessage(
            "consensus checkpoint is not canonically encoded".into(),
        ));
    }
    Ok(image)
}

fn decode_v2_checkpoint_image(encoded: &[u8]) -> ConsensusResult<CheckpointImage> {
    let minimum = SNAPSHOT_V2_HEADER_LEN + SNAPSHOT_V2_TRAILER_LEN;
    if encoded.len() < minimum {
        return Err(ConsensusError::InvalidMessage(
            "EPSN v2 checkpoint is truncated".into(),
        ));
    }
    let digest_offset = encoded.len() - SNAPSHOT_V2_TRAILER_LEN;
    let expected_image_digest = Sha256::digest(&encoded[..digest_offset]);
    if expected_image_digest.as_slice() != &encoded[digest_offset..] {
        return Err(ConsensusError::InvalidMessage(
            "EPSN v2 image digest does not match its bytes".into(),
        ));
    }

    let mut reader = SnapshotReader::new(encoded);
    reader.read_exact(4, "magic")?;
    let version = reader.read_u16("version")?;
    debug_assert_eq!(version, SNAPSHOT_V2_VERSION);
    let flags = reader.read_u16("flags")?;
    if flags != 1 {
        return Err(ConsensusError::Unsupported(format!(
            "unsupported EPSN v2 flags {flags:#06x}"
        )));
    }
    let group_id = GroupId::new(reader.read_u64("group ID")?)?;
    let group_epoch = GroupEpoch::new(reader.read_u64("group epoch")?)?;
    let index = LogIndex::new(reader.read_u64("checkpoint index")?);
    let term = Term::new(reader.read_u64("checkpoint term")?);
    let state_digest = reader.read_array("state digest")?;
    let applied_command_count = reader.read_u64("total command count")?;
    let proposal_count = reader.read_u32("retry proposal count")? as usize;
    let format_id = reader.read_array("application format ID")?;
    let format_version = reader.read_u16("application format version")?;
    let reserved = reader.read_u16("reserved application flags")?;
    if reserved != 0 {
        return Err(ConsensusError::Unsupported(format!(
            "unsupported EPSN v2 application flags {reserved:#06x}"
        )));
    }
    let application_len = reader.read_u32("application payload length")? as usize;
    if application_len > MAX_APPLICATION_SNAPSHOT_BYTES {
        return Err(ConsensusError::InvalidMessage(format!(
            "application snapshot is {application_len} bytes; maximum is {MAX_APPLICATION_SNAPSHOT_BYTES}"
        )));
    }
    let application_state_digest = reader.read_array("application state digest")?;
    let application_payload_digest: [u8; 32] = reader.read_array("application payload digest")?;
    let application_payload = reader
        .read_exact(application_len, "application payload")?
        .to_vec();
    if Sha256::digest(&application_payload).as_slice() != application_payload_digest {
        return Err(ConsensusError::InvalidMessage(
            "application snapshot payload digest does not match its bytes".into(),
        ));
    }
    if proposal_count > MAX_CHECKPOINT_RETRY_PROPOSALS
        || proposal_count > reader.remaining_len() / SNAPSHOT_PROPOSAL_FIXED_LEN
    {
        return Err(ConsensusError::InvalidMessage(
            "EPSN v2 retry proposal count exceeds its bound or remaining bytes".into(),
        ));
    }
    let applied = decode_checkpoint_proposals(&mut reader, proposal_count, group_id, group_epoch)?;
    let trailer: [u8; SNAPSHOT_V2_TRAILER_LEN] = reader.read_array("image digest")?;
    debug_assert_eq!(trailer.as_slice(), expected_image_digest.as_slice());
    reader.finish()?;
    let application_snapshot = ApplicationSnapshot::new(
        index,
        format_id,
        format_version,
        application_state_digest,
        application_payload,
    )?;
    let image = CheckpointImage {
        group_id,
        group_epoch,
        index,
        term,
        state_digest,
        applied_command_count,
        applied,
        application_snapshot: Some(application_snapshot),
    };
    validate_checkpoint_image(&image)?;
    if encode_checkpoint_image(&image)? != encoded {
        return Err(ConsensusError::InvalidMessage(
            "consensus checkpoint is not canonically encoded".into(),
        ));
    }
    Ok(image)
}

fn decode_checkpoint_proposals(
    reader: &mut SnapshotReader<'_>,
    proposal_count: usize,
    group_id: GroupId,
    group_epoch: GroupEpoch,
) -> ConsensusResult<Vec<CommittedProposal>> {
    let mut applied = Vec::with_capacity(proposal_count);
    for _ in 0..proposal_count {
        let proposal_id = ProposalId::new(reader.read_u64("proposal ID")?)?;
        let proposal_term = Term::new(reader.read_u64("proposal term")?);
        let log_index = LogIndex::new(reader.read_u64("proposal log index")?);
        let payload_len = reader.read_u32("proposal payload length")? as usize;
        if payload_len > MAX_PROPOSAL_PAYLOAD_BYTES {
            return Err(ConsensusError::InvalidMessage(format!(
                "checkpoint proposal payload is {payload_len} bytes; maximum is {MAX_PROPOSAL_PAYLOAD_BYTES}"
            )));
        }
        let payload = reader.read_exact(payload_len, "proposal payload")?.to_vec();
        applied.push(CommittedProposal {
            receipt: CommitReceipt {
                group_id,
                group_epoch,
                proposal_id,
                term: proposal_term,
                log_index,
            },
            payload,
        });
    }
    Ok(applied)
}

fn checkpoint_snapshot(image: &CheckpointImage, voters: [NodeId; 3]) -> ConsensusResult<Snapshot> {
    let mut snapshot = Snapshot {
        data: encode_checkpoint_image(image)?,
        ..Snapshot::default()
    };
    let metadata = snapshot.mut_metadata();
    metadata.index = image.index.get();
    metadata.term = image.term.get();
    metadata.set_conf_state(expected_conf_state(voters));
    Ok(snapshot)
}

fn decode_checkpoint_snapshot(
    snapshot: &Snapshot,
    group_id: GroupId,
    group_epoch: GroupEpoch,
    voters: [NodeId; 3],
) -> ConsensusResult<CheckpointImage> {
    let metadata = snapshot.metadata.as_ref().ok_or_else(|| {
        ConsensusError::InvalidMessage("consensus snapshot has no metadata".into())
    })?;
    if metadata.conf_state.as_ref() != Some(&expected_conf_state(voters)) {
        return Err(ConsensusError::InvalidMessage(
            "consensus snapshot voter set does not match the fixed group".into(),
        ));
    }
    let image = decode_checkpoint_image(snapshot.data.as_ref())?;
    if image.group_id != group_id || image.group_epoch != group_epoch {
        return Err(ConsensusError::InvalidMessage(
            "consensus snapshot belongs to a different group or epoch".into(),
        ));
    }
    if metadata.index != image.index.get() || metadata.term != image.term.get() {
        return Err(ConsensusError::InvalidMessage(
            "consensus snapshot metadata does not match its Epoch checkpoint".into(),
        ));
    }
    Ok(image)
}

#[derive(Clone, Copy)]
struct SnapshotReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u16(&mut self, field: &str) -> ConsensusResult<u16> {
        Ok(u16::from_be_bytes(self.read_array(field)?))
    }

    fn read_u32(&mut self, field: &str) -> ConsensusResult<u32> {
        Ok(u32::from_be_bytes(self.read_array(field)?))
    }

    fn read_u64(&mut self, field: &str) -> ConsensusResult<u64> {
        Ok(u64::from_be_bytes(self.read_array(field)?))
    }

    fn read_array<const SIZE: usize>(&mut self, field: &str) -> ConsensusResult<[u8; SIZE]> {
        self.read_exact(SIZE, field)?.try_into().map_err(|_| {
            ConsensusError::InvalidMessage(format!("consensus checkpoint has an invalid {field}"))
        })
    }

    fn read_exact(&mut self, length: usize, field: &str) -> ConsensusResult<&'a [u8]> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            ConsensusError::InvalidMessage(format!("consensus checkpoint {field} length overflows"))
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            ConsensusError::InvalidMessage(format!("consensus checkpoint truncates {field}"))
        })?;
        self.offset = end;
        Ok(value)
    }

    fn remaining_len(self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn finish(self) -> ConsensusResult<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ConsensusError::InvalidMessage(
                "trailing bytes after consensus checkpoint".into(),
            ))
        }
    }
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
    if message_type == MessageType::MsgSnapshot {
        if !raft_message.entries.is_empty() {
            return Err(ConsensusError::InvalidMessage(
                "snapshot message also carries log entries".into(),
            ));
        }
        let snapshot = raft_message.snapshot.as_ref().ok_or_else(|| {
            ConsensusError::InvalidMessage("snapshot message has no snapshot payload".into())
        })?;
        let metadata = snapshot.metadata.as_ref().ok_or_else(|| {
            ConsensusError::InvalidMessage("snapshot message has no metadata".into())
        })?;
        let conf_state = metadata.conf_state.as_ref().ok_or_else(|| {
            ConsensusError::InvalidMessage("snapshot message has no voter state".into())
        })?;
        let voters = conf_state
            .voters
            .iter()
            .copied()
            .map(NodeId::new)
            .collect::<ConsensusResult<Vec<_>>>()?;
        if voters.len() != 3
            || !conf_state.learners.is_empty()
            || !conf_state.voters_outgoing.is_empty()
            || !conf_state.learners_next.is_empty()
            || conf_state.auto_leave
            || voters.iter().copied().collect::<BTreeSet<_>>().len() != 3
        {
            return Err(ConsensusError::InvalidMessage(
                "snapshot message does not contain one fixed three-voter state".into(),
            ));
        }
        let image = decode_checkpoint_image(snapshot.data.as_ref())?;
        if image.group_id != message.group_id
            || image.group_epoch != message.group_epoch
            || image.index.get() != metadata.index
            || image.term.get() != metadata.term
        {
            return Err(ConsensusError::InvalidMessage(
                "snapshot envelope, metadata, and Epoch checkpoint disagree".into(),
            ));
        }
    } else if raft_message.snapshot.is_some() {
        return Err(ConsensusError::InvalidMessage(
            "non-snapshot peer message carries snapshot data".into(),
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

fn encode_read_barrier_context(request_id: ReadBarrierId) -> [u8; READ_BARRIER_CONTEXT_BYTES] {
    request_id.get().to_be_bytes()
}

fn decode_read_barrier_context(encoded: &[u8]) -> ConsensusResult<ReadBarrierId> {
    if encoded.len() != READ_BARRIER_CONTEXT_BYTES {
        return Err(ConsensusError::InvalidMessage(format!(
            "read barrier context is {} bytes; expected {READ_BARRIER_CONTEXT_BYTES}",
            encoded.len()
        )));
    }
    ReadBarrierId::new(u64::from_be_bytes(encoded.try_into().map_err(|_| {
        ConsensusError::InvalidMessage("invalid read barrier context".into())
    })?))
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
    storage: &'a EpochRaftStorage,
    applied_index: LogIndex,
    applied: &'a [CommittedProposal],
    state_digest: StateDigest,
}

struct ValidatedPersistentState {
    proposals: BTreeMap<ProposalId, TrackedProposal>,
    state_digest_scheme: StateDigestScheme,
    applied_command_count: u64,
}

fn validate_persisted_state(
    state: PersistedStateView<'_>,
) -> ConsensusResult<ValidatedPersistentState> {
    let checkpoint_image = validate_persisted_storage(state)?;
    validate_applied_receipts(
        state.group_id,
        state.group_epoch,
        state.applied_index,
        state.applied,
    )?;
    let (state_digest_scheme, applied_command_count) =
        validate_persisted_digest(state, checkpoint_image.as_ref())?;
    let proposals = build_proposal_tracking(
        state.group_id,
        state.group_epoch,
        state.storage,
        state.applied_index,
        state.applied,
        state.voters,
    )?;
    Ok(ValidatedPersistentState {
        proposals,
        state_digest_scheme,
        applied_command_count,
    })
}

fn validate_persisted_storage(
    state: PersistedStateView<'_>,
) -> ConsensusResult<Option<CheckpointImage>> {
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
    let checkpoint_image = match state.storage.snapshot.as_ref() {
        Some(snapshot) => Some(decode_checkpoint_snapshot(
            snapshot,
            state.group_id,
            state.group_epoch,
            state.voters,
        )?),
        None => None,
    };
    let checkpoint_index = checkpoint_image
        .as_ref()
        .map_or(0, |image| image.index.get());
    if first_index != checkpoint_index.saturating_add(1) {
        return Err(ConsensusError::InvalidState(format!(
            "stored first index {first_index} does not follow checkpoint index {checkpoint_index}"
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
    Ok(checkpoint_image)
}

fn validate_persisted_digest(
    state: PersistedStateView<'_>,
    checkpoint_image: Option<&CheckpointImage>,
) -> ConsensusResult<(StateDigestScheme, u64)> {
    if let Some(image) = checkpoint_image
        && (state.applied_index < image.index
            || state.applied.get(..image.applied.len()) != Some(image.applied.as_slice()))
    {
        return Err(ConsensusError::InvalidState(
            "stored applied history does not extend its consensus checkpoint".into(),
        ));
    }
    let (scheme, command_count, expected_digest) = match checkpoint_image {
        Some(image) if image.application_snapshot.is_some() => {
            let tail = state.applied.get(image.applied.len()..).ok_or_else(|| {
                ConsensusError::InvalidState(
                    "stored retry history is shorter than its v2 checkpoint".into(),
                )
            })?;
            if tail
                .iter()
                .any(|committed| committed.receipt.log_index <= image.index)
            {
                return Err(ConsensusError::InvalidState(
                    "stored v2 checkpoint tail does not follow its checkpoint index".into(),
                ));
            }
            let mut digest = image.state_digest;
            for committed in tail {
                digest = advance_rolling_state_digest(digest, committed)?;
            }
            let tail_count = u64::try_from(tail.len())
                .map_err(|_| ConsensusError::InvalidState("checkpoint tail is too large".into()))?;
            let count = image
                .applied_command_count
                .checked_add(tail_count)
                .ok_or_else(|| {
                    ConsensusError::InvalidState("applied command count overflow".into())
                })?;
            (StateDigestScheme::RollingV2, count, digest)
        }
        _ => {
            let count = u64::try_from(state.applied.len())
                .map_err(|_| ConsensusError::InvalidState("applied history is too large".into()))?;
            (
                StateDigestScheme::CompleteHistoryV1,
                count,
                compute_state_digest(state.group_id, state.group_epoch, state.applied)?,
            )
        }
    };
    if expected_digest != state.state_digest {
        return Err(ConsensusError::InvalidState(
            "stored state digest does not match the canonical applied history".into(),
        ));
    }
    Ok((scheme, command_count))
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
    storage: &EpochRaftStorage,
    applied_index: LogIndex,
    applied: &[CommittedProposal],
    voters: [NodeId; 3],
) -> ConsensusResult<BTreeMap<ProposalId, TrackedProposal>> {
    let (entries, checkpoint_image) =
        validated_retained_log(storage, group_id, group_epoch, voters)?;
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
    let mut first_applied_occurrence = checkpoint_image.map_or_else(BTreeMap::new, |image| {
        image
            .applied
            .into_iter()
            .map(|proposal| {
                (
                    proposal.receipt.proposal_id,
                    proposal.receipt.log_index.get(),
                )
            })
            .collect()
    });

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

fn validated_retained_log(
    storage: &EpochRaftStorage,
    group_id: GroupId,
    group_epoch: GroupEpoch,
    voters: [NodeId; 3],
) -> ConsensusResult<(Vec<Entry>, Option<CheckpointImage>)> {
    let raft_state = storage
        .initial_state()
        .map_err(|error| ConsensusError::Storage(error.to_string()))?;
    validate_hard_state(&raft_state.hard_state, voters)?;
    let last_index = storage
        .last_index()
        .map_err(|error| ConsensusError::Storage(error.to_string()))?;
    let first_index = storage
        .first_index()
        .map_err(|error| ConsensusError::Storage(error.to_string()))?;
    let entries = if last_index < first_index {
        Vec::new()
    } else {
        let high = last_index
            .checked_add(1)
            .ok_or_else(|| ConsensusError::InvalidState("last log index overflow".into()))?;
        storage
            .entries(first_index, high, None, GetEntriesContext::empty(false))
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
    };
    let checkpoint_image = match storage.snapshot.as_ref() {
        Some(snapshot) => Some(decode_checkpoint_snapshot(
            snapshot,
            group_id,
            group_epoch,
            voters,
        )?),
        None => None,
    };
    let base_index = checkpoint_image
        .as_ref()
        .map_or(0, |image| image.index.get());
    let base_term = checkpoint_image
        .as_ref()
        .map_or(0, |image| image.term.get());
    validate_log_order_from(
        &entries,
        base_index,
        base_term,
        last_index,
        raft_state.hard_state.term,
    )?;
    Ok((entries, checkpoint_image))
}

fn validate_log_order(
    entries: &[Entry],
    last_index: u64,
    hard_state_term: u64,
) -> ConsensusResult<()> {
    validate_log_order_from(entries, 0, 0, last_index, hard_state_term)
}

fn validate_log_order_from(
    entries: &[Entry],
    base_index: u64,
    base_term: u64,
    last_index: u64,
    hard_state_term: u64,
) -> ConsensusResult<()> {
    let expected_len = last_index
        .checked_sub(base_index)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| ConsensusError::InvalidState("persisted log length overflow".into()))?;
    if entries.len() != expected_len {
        return Err(ConsensusError::InvalidState(
            "persisted log is not complete after its checkpoint".into(),
        ));
    }
    let mut previous_term = base_term;
    for (offset, entry) in entries.iter().enumerate() {
        let expected = base_index
            .checked_add(
                u64::try_from(offset)
                    .map_err(|_| ConsensusError::InvalidState("log index overflow".into()))?,
            )
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

fn initial_rolling_state_digest(group_id: GroupId, group_epoch: GroupEpoch) -> StateDigest {
    let mut hasher = Sha256::new();
    hasher.update(STATE_DIGEST_MAGIC);
    hasher.update(ROLLING_STATE_DIGEST_VERSION.to_be_bytes());
    hasher.update(group_id.get().to_be_bytes());
    hasher.update(group_epoch.get().to_be_bytes());
    hasher.finalize().into()
}

fn advance_rolling_state_digest(
    previous: StateDigest,
    committed: &CommittedProposal,
) -> ConsensusResult<StateDigest> {
    let payload_len = u64::try_from(committed.payload.len()).map_err(|_| {
        ConsensusError::InvalidState("applied proposal payload is too large".into())
    })?;
    let mut hasher = Sha256::new();
    hasher.update(previous);
    hasher.update(committed.receipt.log_index.get().to_be_bytes());
    hasher.update(committed.receipt.term.get().to_be_bytes());
    hasher.update(committed.receipt.proposal_id.get().to_be_bytes());
    hasher.update(payload_len.to_be_bytes());
    hasher.update(&committed.payload);
    Ok(hasher.finalize().into())
}

fn compute_rolling_state_digest(
    group_id: GroupId,
    group_epoch: GroupEpoch,
    applied: &[CommittedProposal],
) -> ConsensusResult<StateDigest> {
    let mut digest = initial_rolling_state_digest(group_id, group_epoch);
    for committed in applied {
        digest = advance_rolling_state_digest(digest, committed)?;
    }
    Ok(digest)
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
mod persistent_tests;
#[cfg(test)]
mod tests;
