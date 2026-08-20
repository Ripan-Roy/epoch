//! Experimental fixed-three-voter consensus probe runtime.
//!
//! The default probe deliberately does not replicate Epoch profile data. An
//! opt-in typed profile applier can consume committed commands on the actor
//! thread, while the persistent adapter remains the authoritative replay log.
//! Both modes keep the transport private and do not raise the standalone
//! engine's durability guarantee.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Formatter},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use epoch_consensus::{
    ApplicationSnapshot, CommitReceipt, CommittedProposal, CompletedReadBarrier, ConsensusAdapter,
    ConsensusCheckpoint, ConsensusError, ConsensusOutput, ConsensusRole, ConsensusStatus,
    GroupEpoch, GroupId, LogIndex, MAX_PEER_MESSAGE_WIRE_BYTES, MAX_PROPOSAL_PAYLOAD_BYTES, NodeId,
    PeerMessage, PersistentOpenResult, PersistentRaftAdapter, PersistentRecovery, Proposal,
    ProposalId, ProposalLookup, ReadBarrierId, ReadBarrierRequest, Term,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
};
use url::Url;

pub const INTERNAL_PEER_MESSAGE_PATH: &str = "/internal/v1/consensus/messages";
pub const EXPERIMENTAL_STATUS_PATH: &str = "/experimental/v1/consensus/status";
pub const EXPERIMENTAL_CHECKPOINTS_PATH: &str = "/experimental/v1/consensus/checkpoints";
pub const EXPERIMENTAL_PROPOSALS_PATH: &str = "/experimental/v1/consensus/proposals";
pub const EXPERIMENTAL_PROPOSAL_LOOKUP_PATH: &str =
    "/experimental/v1/consensus/proposals/{proposal_id}";

const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 256;
const DEFAULT_OUTBOUND_QUEUE_CAPACITY: usize = 128;
const COMMIT_NOTIFICATION_CAPACITY: usize = 256;
const READ_BARRIER_NOTIFICATION_CAPACITY: usize = 1_024;
const MAX_QUEUE_CAPACITY: usize = 65_536;
const MIN_TICK_INTERVAL: Duration = Duration::from_millis(10);
const MAX_TICK_INTERVAL: Duration = Duration::from_mins(1);
const OUTBOUND_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const OUTBOUND_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const OUTBOUND_RETRY_DELAY: Duration = Duration::from_millis(100);
const OUTBOUND_ATTEMPTS: usize = 3;
const OUTBOUND_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PROPOSAL_JSON_BYTES: usize = MAX_PROPOSAL_PAYLOAD_BYTES * 4 + 16 * 1024;

// Keep real-HTTP unit clusters tolerant of scheduler contention. Raft uses ten
// ticks as its minimum election timeout, so this yields a one-second window
// while assertion polling can remain fast. Dedicated failover tests still
// prove leadership turnover explicitly.
#[cfg(test)]
pub(crate) const TEST_CONSENSUS_TICK_INTERVAL: Duration = Duration::from_millis(100);

pub type ConsensusProbeResult<T> = Result<T, ConsensusProbeError>;
type OutboundSenders = BTreeMap<NodeId, OutboundPeer>;
type OutboundHealthRegistry = Arc<BTreeMap<NodeId, Arc<OutboundPeerHealth>>>;
type OutboundWorkers = Vec<(NodeId, JoinHandle<()>)>;

#[derive(Debug, Clone, Error)]
pub enum ConsensusProbeError {
    #[error("invalid consensus probe configuration: {0}")]
    InvalidConfiguration(String),
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    #[error("consensus actor is unavailable")]
    ActorUnavailable,
    #[error("consensus actor thread could not be started: {0}")]
    ThreadStart(String),
    #[error("consensus actor thread panicked")]
    ActorPanicked,
    #[error("consensus outbound queue for node {0} is unavailable")]
    OutboundUnavailable(NodeId),
    #[error("consensus runtime task could not be joined: {0}")]
    TaskJoin(String),
    #[error("consensus runtime shutdown encountered errors: {0}")]
    RuntimeShutdown(String),
    #[error("peer request must use content-type application/octet-stream")]
    UnsupportedPeerContentType,
    #[error("committed profile command could not be applied: {0}")]
    ProfileApplication(String),
    #[error("read barrier {request_id} timed out after {timeout_ms} ms")]
    ReadBarrierTimeout {
        request_id: ReadBarrierId,
        timeout_ms: u128,
    },
    #[error("read barrier request identifiers are exhausted")]
    ReadBarrierIdExhausted,
}

/// Applies consensus-decoded commands to a profile state machine.
///
/// Implementations must be deterministic and idempotent. Recovery installs a
/// native checkpoint when present and then replays only the retained committed
/// tail before the runtime reports ready; legacy histories still replay in
/// full. Live application runs on the consensus actor thread before a commit
/// notification or successful lookup can be observed by the typed boundary.
pub trait CommittedProposalApplier: fmt::Debug + Send + Sync + 'static {
    fn replay(&self, committed: &[CommittedProposal]) -> Result<(), String>;

    fn apply(&self, committed: &CommittedProposal) -> Result<(), String>;

    fn capture_snapshot(
        &self,
        _checkpoint_index: LogIndex,
        _retained: &[CommittedProposal],
    ) -> Result<ApplicationSnapshot, String> {
        Err("profile-native snapshot capture is unsupported by this applier".into())
    }

    fn install_snapshot(&self, _snapshot: &ApplicationSnapshot) -> Result<(), String> {
        Err("profile-native snapshot installation is unsupported by this applier".into())
    }

    fn supports_native_snapshots(&self) -> bool {
        false
    }
}

/// Validated configuration for one fixed-three-voter probe group.
#[derive(Debug, Clone)]
pub struct ConsensusProbeConfig {
    node_id: NodeId,
    group_id: GroupId,
    group_epoch: GroupEpoch,
    voters: [NodeId; 3],
    peer_urls: BTreeMap<NodeId, Url>,
    tick_interval: Duration,
    command_queue_capacity: usize,
    outbound_queue_capacity: usize,
}

impl ConsensusProbeConfig {
    pub fn new(
        node_id: u64,
        group_id: u64,
        group_epoch: u64,
        peers: impl IntoIterator<Item = (u64, Url)>,
        tick_interval: Duration,
    ) -> ConsensusProbeResult<Self> {
        let node_id = checked_node_id(node_id)?;
        let group_id =
            GroupId::new(group_id).map_err(|error| configuration_from_consensus(&error))?;
        let group_epoch =
            GroupEpoch::new(group_epoch).map_err(|error| configuration_from_consensus(&error))?;
        validate_tick_interval(tick_interval)?;

        let mut peer_urls = BTreeMap::new();
        let mut unique_urls = BTreeSet::new();
        for (peer_id, url) in peers {
            let peer_id = checked_node_id(peer_id)?;
            validate_peer_url(&url)?;
            if peer_urls.insert(peer_id, url.clone()).is_some() {
                return Err(ConsensusProbeError::InvalidConfiguration(format!(
                    "peer node {peer_id} is listed more than once"
                )));
            }
            if !unique_urls.insert(url.as_str().to_owned()) {
                return Err(ConsensusProbeError::InvalidConfiguration(format!(
                    "peer URL {url} is assigned to more than one node"
                )));
            }
        }
        if peer_urls.len() != 3 {
            return Err(ConsensusProbeError::InvalidConfiguration(format!(
                "the probe requires exactly three peers; observed {}",
                peer_urls.len()
            )));
        }
        if !peer_urls.contains_key(&node_id) {
            return Err(ConsensusProbeError::InvalidConfiguration(format!(
                "local node {node_id} is absent from the peer map"
            )));
        }
        let voters = peer_urls
            .keys()
            .copied()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| {
                ConsensusProbeError::InvalidConfiguration(
                    "the probe requires exactly three voters".into(),
                )
            })?;

        Ok(Self {
            node_id,
            group_id,
            group_epoch,
            voters,
            peer_urls,
            tick_interval,
            command_queue_capacity: DEFAULT_COMMAND_QUEUE_CAPACITY,
            outbound_queue_capacity: DEFAULT_OUTBOUND_QUEUE_CAPACITY,
        })
    }

    pub fn from_peer_spec(
        node_id: u64,
        group_id: u64,
        group_epoch: u64,
        peer_spec: &str,
        tick_interval: Duration,
    ) -> ConsensusProbeResult<Self> {
        Self::new(
            node_id,
            group_id,
            group_epoch,
            parse_peer_urls(peer_spec)?,
            tick_interval,
        )
    }

    pub fn with_queue_capacities(
        mut self,
        command_queue_capacity: usize,
        outbound_queue_capacity: usize,
    ) -> ConsensusProbeResult<Self> {
        validate_queue_capacity("command", command_queue_capacity)?;
        validate_queue_capacity("outbound", outbound_queue_capacity)?;
        self.command_queue_capacity = command_queue_capacity;
        self.outbound_queue_capacity = outbound_queue_capacity;
        Ok(self)
    }

    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub const fn group_epoch(&self) -> GroupEpoch {
        self.group_epoch
    }

    pub const fn voters(&self) -> [NodeId; 3] {
        self.voters
    }

    pub const fn tick_interval(&self) -> Duration {
        self.tick_interval
    }

    pub fn for_group(&self, group_id: u64, group_epoch: u64) -> ConsensusProbeResult<Self> {
        Self::new(
            self.node_id.get(),
            group_id,
            group_epoch,
            self.peer_urls
                .iter()
                .map(|(node_id, url)| (node_id.get(), url.clone())),
            self.tick_interval,
        )?
        .with_queue_capacities(self.command_queue_capacity, self.outbound_queue_capacity)
    }

    pub fn peer_url(&self, node_id: NodeId) -> Option<&Url> {
        self.peer_urls.get(&node_id)
    }
}

/// Parses `1=http://node-1:7701,2=http://node-2:7701,...`.
pub fn parse_peer_urls(peer_spec: &str) -> ConsensusProbeResult<Vec<(u64, Url)>> {
    if peer_spec.trim().is_empty() {
        return Err(ConsensusProbeError::InvalidConfiguration(
            "peer specification is empty".into(),
        ));
    }
    peer_spec
        .split(',')
        .map(|entry| {
            let entry = entry.trim();
            let (node_id, url) = entry.split_once('=').ok_or_else(|| {
                ConsensusProbeError::InvalidConfiguration(format!(
                    "peer entry {entry:?} must have the form node_id=http(s)://authority"
                ))
            })?;
            let node_id = node_id.trim().parse::<u64>().map_err(|error| {
                ConsensusProbeError::InvalidConfiguration(format!(
                    "peer node ID {:?} is invalid: {error}",
                    node_id.trim()
                ))
            })?;
            let url_text = url.trim();
            let url = Url::parse(url_text).map_err(|error| {
                ConsensusProbeError::InvalidConfiguration(format!(
                    "peer URL {url_text:?} is invalid: {error}"
                ))
            })?;
            Ok((node_id, url))
        })
        .collect()
}

fn checked_node_id(value: u64) -> ConsensusProbeResult<NodeId> {
    NodeId::new(value).map_err(|error| configuration_from_consensus(&error))
}

fn configuration_from_consensus(error: &ConsensusError) -> ConsensusProbeError {
    ConsensusProbeError::InvalidConfiguration(error.to_string())
}

fn validate_tick_interval(tick_interval: Duration) -> ConsensusProbeResult<()> {
    if !(MIN_TICK_INTERVAL..=MAX_TICK_INTERVAL).contains(&tick_interval) {
        return Err(ConsensusProbeError::InvalidConfiguration(format!(
            "tick interval must be between {}ms and {}ms",
            MIN_TICK_INTERVAL.as_millis(),
            MAX_TICK_INTERVAL.as_millis()
        )));
    }
    Ok(())
}

fn validate_queue_capacity(label: &str, capacity: usize) -> ConsensusProbeResult<()> {
    if capacity == 0 || capacity > MAX_QUEUE_CAPACITY {
        return Err(ConsensusProbeError::InvalidConfiguration(format!(
            "{label} queue capacity must be between 1 and {MAX_QUEUE_CAPACITY}"
        )));
    }
    Ok(())
}

fn validate_peer_url(url: &Url) -> ConsensusProbeResult<()> {
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConsensusProbeError::InvalidConfiguration(format!(
            "peer URL must contain only an http(s) scheme and authority: {url}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct OutboundPeer {
    sender: mpsc::Sender<Vec<u8>>,
    health: Arc<OutboundPeerHealth>,
}

#[derive(Debug)]
struct OutboundPeerHealth {
    peer_id: NodeId,
    queue_capacity: usize,
    queued_frames: AtomicUsize,
    enqueued_frames: AtomicU64,
    delivered_frames: AtomicU64,
    dropped_queue_full_frames: AtomicU64,
    dropped_worker_closed_frames: AtomicU64,
    exhausted_retry_frames: AtomicU64,
}

impl OutboundPeerHealth {
    fn new(peer_id: NodeId, queue_capacity: usize) -> Self {
        Self {
            peer_id,
            queue_capacity,
            queued_frames: AtomicUsize::new(0),
            enqueued_frames: AtomicU64::new(0),
            delivered_frames: AtomicU64::new(0),
            dropped_queue_full_frames: AtomicU64::new(0),
            dropped_worker_closed_frames: AtomicU64::new(0),
            exhausted_retry_frames: AtomicU64::new(0),
        }
    }

    fn record_enqueue(&self) {
        self.enqueued_frames.fetch_add(1, Ordering::Relaxed);
        self.queued_frames.fetch_add(1, Ordering::Relaxed);
    }

    fn record_dequeue(&self) {
        let previous = self.queued_frames.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "outbound queue depth cannot underflow");
    }

    fn record_delivery(&self) {
        self.delivered_frames.fetch_add(1, Ordering::Relaxed);
    }

    fn record_queue_full_drop(&self) -> u64 {
        self.dropped_queue_full_frames
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    fn record_worker_closed_drop(&self) -> u64 {
        self.dropped_worker_closed_frames
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    fn record_exhausted_retry(&self) {
        self.exhausted_retry_frames.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ConsensusProbePeerTransportStatus {
        let delivered_frames = self.delivered_frames.load(Ordering::Relaxed);
        let dropped_queue_full_frames = self.dropped_queue_full_frames.load(Ordering::Relaxed);
        let dropped_worker_closed_frames =
            self.dropped_worker_closed_frames.load(Ordering::Relaxed);
        let exhausted_retry_frames = self.exhausted_retry_frames.load(Ordering::Relaxed);
        let observed_condition = if dropped_worker_closed_frames > 0 {
            ConsensusProbeTransportCondition::Unavailable
        } else if dropped_queue_full_frames > 0 || exhausted_retry_frames > 0 {
            ConsensusProbeTransportCondition::Degraded
        } else if delivered_frames > 0 {
            ConsensusProbeTransportCondition::Healthy
        } else {
            ConsensusProbeTransportCondition::Unknown
        };
        ConsensusProbePeerTransportStatus {
            peer_id: self.peer_id.get(),
            observed_condition,
            queue_capacity: self.queue_capacity,
            queued_frames: self.queued_frames.load(Ordering::Relaxed),
            enqueued_frames: self.enqueued_frames.load(Ordering::Relaxed),
            delivered_frames,
            dropped_queue_full_frames,
            dropped_worker_closed_frames,
            exhausted_retry_frames,
        }
    }
}

fn snapshot_outbound_health(
    registry: &OutboundHealthRegistry,
) -> Vec<ConsensusProbePeerTransportStatus> {
    registry.values().map(|health| health.snapshot()).collect()
}

type ActorReply<T> = oneshot::Sender<ConsensusProbeResult<T>>;

enum ActorCommand {
    Status {
        reply: ActorReply<ConsensusStatus>,
    },
    Campaign {
        reply: ActorReply<ConsensusStatus>,
    },
    Tick {
        reply: ActorReply<ConsensusStatus>,
    },
    Propose {
        proposal: Proposal,
        reply: ActorReply<ProposalLookup>,
    },
    ForwardProposal {
        proposal: Proposal,
        reply: ActorReply<ProposalLookup>,
    },
    ReadBarrier {
        request: ReadBarrierRequest,
        reply: ActorReply<ConsensusStatus>,
    },
    CancelReadBarrier {
        request_id: ReadBarrierId,
    },
    Receive {
        message: PeerMessage,
        reply: ActorReply<ConsensusStatus>,
    },
    Lookup {
        proposal_id: ProposalId,
        reply: ActorReply<ProposalLookup>,
    },
    AppliedProposals {
        reply: ActorReply<Vec<CommittedProposal>>,
    },
    Checkpoint {
        reply: ActorReply<ConsensusCheckpoint>,
    },
    CheckpointIfAppliedGrowth {
        min_applied_entries: u64,
        reply: ActorReply<Option<ConsensusCheckpoint>>,
    },
    #[cfg(test)]
    InjectFailure {
        error: ConsensusProbeError,
        reply: ActorReply<()>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Cloneable asynchronous interface to the dedicated consensus actor thread.
#[derive(Debug, Clone)]
pub struct ConsensusProbeHandle {
    node_id: NodeId,
    group_id: GroupId,
    group_epoch: GroupEpoch,
    commands: mpsc::Sender<ActorCommand>,
    commits: broadcast::Sender<CommittedProposal>,
    read_barriers: broadcast::Sender<CompletedReadBarrier>,
    next_read_barrier_id: Arc<AtomicU64>,
    outbound_health: OutboundHealthRegistry,
}

impl ConsensusProbeHandle {
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub const fn group_epoch(&self) -> GroupEpoch {
        self.group_epoch
    }

    pub fn subscribe_commits(&self) -> broadcast::Receiver<CommittedProposal> {
        self.commits.subscribe()
    }

    /// Returns a point-in-time, local-only view of each peer transport queue.
    pub fn outbound_transport_status(&self) -> Vec<ConsensusProbePeerTransportStatus> {
        snapshot_outbound_health(&self.outbound_health)
    }

    pub async fn status(&self) -> ConsensusProbeResult<ConsensusStatus> {
        self.request(|reply| ActorCommand::Status { reply }).await
    }

    pub async fn campaign(&self) -> ConsensusProbeResult<ConsensusStatus> {
        self.request(|reply| ActorCommand::Campaign { reply }).await
    }

    pub async fn tick(&self) -> ConsensusProbeResult<ConsensusStatus> {
        self.request(|reply| ActorCommand::Tick { reply }).await
    }

    pub async fn propose(
        &self,
        proposal_id: u64,
        expected_term: u64,
        payload: Vec<u8>,
    ) -> ConsensusProbeResult<ProposalLookup> {
        let proposal = Proposal::new(
            self.group_id,
            self.group_epoch,
            Term::new(expected_term),
            ProposalId::new(proposal_id)?,
            payload,
        );
        self.request(|reply| ActorCommand::Propose { proposal, reply })
            .await
    }

    /// Routes an internal proposal through the local follower to its known
    /// leader. Public APIs deliberately keep using leader-only `propose`.
    pub(crate) async fn forward_propose(
        &self,
        proposal_id: u64,
        expected_term: u64,
        payload: Vec<u8>,
    ) -> ConsensusProbeResult<ProposalLookup> {
        let proposal = Proposal::new(
            self.group_id,
            self.group_epoch,
            Term::new(expected_term),
            ProposalId::new(proposal_id)?,
            payload,
        );
        self.request(|reply| ActorCommand::ForwardProposal { proposal, reply })
            .await
    }

    /// Waits for a quorum-confirmed read index and for the local profile state
    /// machine to apply through that index.
    pub async fn read_barrier(
        &self,
        expected_term: u64,
        timeout: Duration,
    ) -> ConsensusProbeResult<CompletedReadBarrier> {
        if timeout.is_zero() {
            return Err(ConsensusProbeError::InvalidConfiguration(
                "read barrier timeout must be greater than zero".into(),
            ));
        }
        let raw_request_id = self
            .next_read_barrier_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| ConsensusProbeError::ReadBarrierIdExhausted)?;
        let request_id = ReadBarrierId::new(raw_request_id)?;
        let request = ReadBarrierRequest::new(
            self.group_id,
            self.group_epoch,
            Term::new(expected_term),
            request_id,
        );
        let mut completions = self.read_barriers.subscribe();
        self.request(|reply| ActorCommand::ReadBarrier { request, reply })
            .await?;

        let completed = tokio::time::timeout(timeout, async {
            loop {
                match completions.recv().await {
                    Ok(completed) if completed.request_id == request_id => break Ok(completed),
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {
                        break Err(ConsensusProbeError::ActorUnavailable);
                    }
                }
            }
        })
        .await;
        if let Ok(result) = completed {
            result
        } else {
            let _ = self
                .commands
                .send(ActorCommand::CancelReadBarrier { request_id })
                .await;
            Err(ConsensusProbeError::ReadBarrierTimeout {
                request_id,
                timeout_ms: timeout.as_millis(),
            })
        }
    }

    pub async fn receive_wire(&self, frame: &[u8]) -> ConsensusProbeResult<ConsensusStatus> {
        let message = PeerMessage::from_wire(frame, self.node_id)?;
        self.receive_message(message).await
    }

    pub(crate) async fn receive_message(
        &self,
        message: PeerMessage,
    ) -> ConsensusProbeResult<ConsensusStatus> {
        if message.to() != self.node_id {
            return Err(ConsensusError::InvalidMessage(format!(
                "peer-message for node {} was routed to node {}",
                message.to(),
                self.node_id
            ))
            .into());
        }
        if message.group_id() != self.group_id {
            return Err(ConsensusError::GroupMismatch {
                expected: self.group_id,
                observed: message.group_id(),
            }
            .into());
        }
        if message.group_epoch() != self.group_epoch {
            return Err(ConsensusError::FencedEpoch {
                expected: self.group_epoch,
                observed: message.group_epoch(),
            }
            .into());
        }
        self.request(|reply| ActorCommand::Receive { message, reply })
            .await
    }

    pub async fn lookup(&self, proposal_id: u64) -> ConsensusProbeResult<ProposalLookup> {
        let proposal_id = ProposalId::new(proposal_id)?;
        self.request(|reply| ActorCommand::Lookup { proposal_id, reply })
            .await
    }

    pub async fn applied_proposals(&self) -> ConsensusProbeResult<Vec<CommittedProposal>> {
        self.request(|reply| ActorCommand::AppliedProposals { reply })
            .await
    }

    /// Creates a durable consensus checkpoint at this voter's applied index.
    pub async fn checkpoint(&self) -> ConsensusProbeResult<ConsensusCheckpoint> {
        self.request(|reply| ActorCommand::Checkpoint { reply })
            .await
    }

    /// Creates a checkpoint only when the local applied index has advanced by
    /// at least `min_applied_entries` since the last installed checkpoint.
    /// Eligibility and creation execute as one actor command, so concurrent
    /// scheduler ticks cannot create duplicate journal generations.
    pub async fn checkpoint_if_applied_growth(
        &self,
        min_applied_entries: u64,
    ) -> ConsensusProbeResult<Option<ConsensusCheckpoint>> {
        if min_applied_entries == 0 {
            return Err(ConsensusProbeError::InvalidConfiguration(
                "checkpoint applied-entry threshold must be non-zero".into(),
            ));
        }
        self.request(|reply| ActorCommand::CheckpointIfAppliedGrowth {
            min_applied_entries,
            reply,
        })
        .await
    }

    async fn request<T>(
        &self,
        command: impl FnOnce(ActorReply<T>) -> ActorCommand,
    ) -> ConsensusProbeResult<T> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| ConsensusProbeError::ActorUnavailable)?;
        response
            .await
            .map_err(|_| ConsensusProbeError::ActorUnavailable)?
    }

    async fn shutdown_actor(&self) -> ConsensusProbeResult<()> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ActorCommand::Shutdown { reply })
            .await
            .map_err(|_| ConsensusProbeError::ActorUnavailable)?;
        response
            .await
            .map_err(|_| ConsensusProbeError::ActorUnavailable)
    }

    #[cfg(test)]
    pub(crate) async fn inject_failure(
        &self,
        error: ConsensusProbeError,
    ) -> ConsensusProbeResult<()> {
        self.request(|reply| ActorCommand::InjectFailure { error, reply })
            .await
    }

    fn try_shutdown_actor(&self) {
        let (reply, _response) = oneshot::channel();
        let _ = self.commands.try_send(ActorCommand::Shutdown { reply });
    }
}

/// Owns the tick task and allows it to be stopped without aborting the actor.
pub struct PeriodicTickHandle {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl fmt::Debug for PeriodicTickHandle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeriodicTickHandle")
            .field("stopped", &*self.stop.borrow())
            .finish_non_exhaustive()
    }
}

impl PeriodicTickHandle {
    pub async fn shutdown(mut self) -> ConsensusProbeResult<()> {
        self.request_stop();
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| ConsensusProbeError::TaskJoin(error.to_string()))?;
        }
        Ok(())
    }

    fn request_stop(&self) {
        let _ = self.stop.send(true);
    }
}

impl Drop for PeriodicTickHandle {
    fn drop(&mut self) {
        self.request_stop();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub fn spawn_periodic_ticks(
    handle: ConsensusProbeHandle,
    tick_interval: Duration,
) -> ConsensusProbeResult<PeriodicTickHandle> {
    validate_tick_interval(tick_interval)?;
    let (stop, mut stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        let start = tokio::time::Instant::now() + tick_interval;
        let mut ticks = tokio::time::interval_at(start, tick_interval);
        loop {
            tokio::select! {
                _ = ticks.tick() => {
                    if let Err(error) = handle.tick().await {
                        tracing::error!(%error, "experimental consensus tick failed");
                        break;
                    }
                }
                changed = stopped.changed() => {
                    if changed.is_err() || *stopped.borrow() {
                        break;
                    }
                }
            }
        }
    });
    Ok(PeriodicTickHandle {
        stop,
        task: Some(task),
    })
}

/// Running probe resources. Call [`Self::shutdown`] for an ordered shutdown.
pub struct ConsensusProbeRuntime {
    handle: ConsensusProbeHandle,
    recovery: PersistentRecovery,
    actor_failure: watch::Receiver<Option<ConsensusProbeError>>,
    tick_task: Option<PeriodicTickHandle>,
    actor_thread: Option<thread::JoinHandle<()>>,
    outbound_workers: OutboundWorkers,
}

impl fmt::Debug for ConsensusProbeRuntime {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsensusProbeRuntime")
            .field("handle", &self.handle)
            .field("recovery", &self.recovery)
            .field("outbound_worker_count", &self.outbound_workers.len())
            .finish_non_exhaustive()
    }
}

impl ConsensusProbeRuntime {
    pub async fn start(
        config: ConsensusProbeConfig,
        stable_path: impl AsRef<Path>,
    ) -> ConsensusProbeResult<Self> {
        Self::start_with_applier(config, stable_path, None).await
    }

    pub async fn start_with_profile_applier(
        config: ConsensusProbeConfig,
        stable_path: impl AsRef<Path>,
        applier: Arc<dyn CommittedProposalApplier>,
    ) -> ConsensusProbeResult<Self> {
        Self::start_with_applier(config, stable_path, Some(applier)).await
    }

    async fn start_with_applier(
        config: ConsensusProbeConfig,
        stable_path: impl AsRef<Path>,
        applier: Option<Arc<dyn CommittedProposalApplier>>,
    ) -> ConsensusProbeResult<Self> {
        let stable_path = stable_path.as_ref().to_path_buf();
        let client = build_outbound_client()?;
        let (outbound, outbound_health, mut outbound_workers) =
            spawn_outbound_workers(&config, &client)?;
        let (commands, command_receiver) = mpsc::channel(config.command_queue_capacity);
        let (commits, _) = broadcast::channel(COMMIT_NOTIFICATION_CAPACITY);
        let (read_barriers, _) = broadcast::channel(READ_BARRIER_NOTIFICATION_CAPACITY);
        let (actor_failure, actor_failure_receiver) = watch::channel(None);
        let (initialized, initialization) = oneshot::channel();
        let actor_commits = commits.clone();
        let actor_read_barriers = read_barriers.clone();
        let actor_config = config.clone();
        let actor_applier = applier.clone();
        let actor_thread = match thread::Builder::new()
            .name(format!("epoch-consensus-{}", config.node_id))
            .spawn(move || {
                let actor_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_persistent_actor(
                        stable_path,
                        &actor_config,
                        &ActorChannels {
                            outbound: &outbound,
                            commits: &actor_commits,
                            read_barriers: &actor_read_barriers,
                        },
                        actor_applier.as_deref(),
                        command_receiver,
                        initialized,
                    )
                }));
                match actor_result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        actor_failure.send_replace(Some(error));
                    }
                    Err(payload) => {
                        actor_failure.send_replace(Some(ConsensusProbeError::ActorPanicked));
                        std::panic::resume_unwind(payload);
                    }
                }
            }) {
            Ok(actor_thread) => actor_thread,
            Err(error) => {
                abort_workers(&mut outbound_workers).await;
                return Err(ConsensusProbeError::ThreadStart(error.to_string()));
            }
        };

        let recovery = match initialization.await {
            Ok(Ok(recovery)) => recovery,
            Ok(Err(error)) => {
                abort_workers(&mut outbound_workers).await;
                join_actor_thread(actor_thread).await?;
                return Err(error);
            }
            Err(_) => {
                abort_workers(&mut outbound_workers).await;
                join_actor_thread(actor_thread).await?;
                return Err(ConsensusProbeError::ActorUnavailable);
            }
        };
        let handle = ConsensusProbeHandle {
            node_id: config.node_id,
            group_id: config.group_id,
            group_epoch: config.group_epoch,
            commands,
            commits,
            read_barriers,
            next_read_barrier_id: Arc::new(AtomicU64::new(1)),
            outbound_health,
        };
        let tick_task = spawn_periodic_ticks(handle.clone(), config.tick_interval)?;
        Ok(Self {
            handle,
            recovery,
            actor_failure: actor_failure_receiver,
            tick_task: Some(tick_task),
            actor_thread: Some(actor_thread),
            outbound_workers,
        })
    }

    pub fn handle(&self) -> ConsensusProbeHandle {
        self.handle.clone()
    }

    pub fn subscribe_actor_failure(&self) -> watch::Receiver<Option<ConsensusProbeError>> {
        self.actor_failure.clone()
    }

    pub const fn recovery(&self) -> PersistentRecovery {
        self.recovery
    }

    pub fn internal_router(&self) -> Router {
        internal_peer_router(self.handle())
    }

    pub fn experimental_router(&self) -> Router {
        experimental_consensus_router(self.handle())
    }

    /// Waits until the actor terminates abnormally and returns its root cause.
    ///
    /// Ordered shutdown does not publish a failure, so callers can race this
    /// future against their normal process-shutdown signal without turning an
    /// operator-requested stop into an error.
    pub async fn wait_for_actor_failure(&self) -> ConsensusProbeError {
        let mut actor_failure = self.actor_failure.clone();
        loop {
            if let Some(error) = actor_failure.borrow().clone() {
                return error;
            }
            if actor_failure.changed().await.is_err() {
                return ConsensusProbeError::ActorUnavailable;
            }
        }
    }

    pub async fn shutdown(mut self) -> ConsensusProbeResult<()> {
        let mut errors = Vec::new();
        if let Some(tick_task) = self.tick_task.take() {
            collect_shutdown_error(
                &mut errors,
                "periodic tick task",
                tick_task.shutdown().await,
            );
        }
        collect_shutdown_error(
            &mut errors,
            "actor shutdown request",
            self.handle.shutdown_actor().await,
        );
        if let Some(actor_thread) = self.actor_thread.take() {
            collect_shutdown_error(
                &mut errors,
                "actor thread",
                join_actor_thread(actor_thread).await,
            );
        }
        collect_shutdown_error(
            &mut errors,
            "outbound workers",
            drain_outbound_workers(&mut self.outbound_workers).await,
        );
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConsensusProbeError::RuntimeShutdown(errors.join("; ")))
        }
    }
}

fn build_outbound_client() -> ConsensusProbeResult<reqwest::Client> {
    reqwest::Client::builder()
        // Peer authorities are an explicit consensus configuration boundary.
        // Ambient host proxy settings and HTTP redirects must not route frames
        // to any authority outside that validated map.
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(OUTBOUND_CONNECT_TIMEOUT)
        .timeout(OUTBOUND_REQUEST_TIMEOUT)
        .build()
        .map_err(|error| {
            ConsensusProbeError::InvalidConfiguration(format!(
                "HTTP transport client could not be built: {error}"
            ))
        })
}

impl Drop for ConsensusProbeRuntime {
    fn drop(&mut self) {
        if let Some(tick_task) = self.tick_task.take() {
            drop(tick_task);
        }
        if self.actor_thread.is_some() {
            self.handle.try_shutdown_actor();
        }
    }
}

struct ActorChannels<'a> {
    outbound: &'a OutboundSenders,
    commits: &'a broadcast::Sender<CommittedProposal>,
    read_barriers: &'a broadcast::Sender<CompletedReadBarrier>,
}

fn run_persistent_actor(
    stable_path: PathBuf,
    config: &ConsensusProbeConfig,
    channels: &ActorChannels<'_>,
    applier: Option<&dyn CommittedProposalApplier>,
    mut commands: mpsc::Receiver<ActorCommand>,
    initialized: oneshot::Sender<ConsensusProbeResult<PersistentRecovery>>,
) -> ConsensusProbeResult<()> {
    let PersistentOpenResult {
        mut adapter,
        output,
    } = match PersistentRaftAdapter::open(
        stable_path,
        config.node_id,
        config.group_id,
        config.group_epoch,
        config.voters,
    ) {
        Ok(opened) => opened,
        Err(error) => {
            let error = ConsensusProbeError::from(error);
            let _ = initialized.send(Err(error.clone()));
            return Err(error);
        }
    };
    let recovery = adapter.recovery();
    if let Some(applier) = applier {
        let application_snapshot = match adapter.application_snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let error = ConsensusProbeError::ProfileApplication(format!(
                    "could not inspect the durable application snapshot: {error}"
                ));
                let _ = initialized.send(Err(error.clone()));
                return Err(error);
            }
        };
        let restoration =
            if let Some(snapshot) = application_snapshot {
                applier.install_snapshot(&snapshot).and_then(|()| {
                    for committed in adapter.applied_proposals().iter().filter(|committed| {
                        committed.receipt.log_index > snapshot.checkpoint_index()
                    }) {
                        applier.apply(committed)?;
                    }
                    Ok(())
                })
            } else {
                applier.replay(adapter.applied_proposals())
            };
        if let Err(error) = restoration {
            let error = ConsensusProbeError::ProfileApplication(error);
            let _ = initialized.send(Err(error.clone()));
            return Err(error);
        }
    }
    // `PersistentRaftAdapter::open` has already included recovery commits in
    // `applied_proposals`, so replay above is authoritative. Publish only its
    // peer work/notifications here to avoid treating recovery output as a new
    // profile transition.
    if let Err(error) = publish_output(
        output,
        channels.outbound,
        channels.commits,
        channels.read_barriers,
        None,
    ) {
        let _ = initialized.send(Err(error.clone()));
        return Err(error);
    }
    if initialized.send(Ok(recovery)).is_err() {
        return Ok(());
    }

    while let Some(command) = commands.blocking_recv() {
        if handle_actor_command(
            &mut adapter,
            channels.outbound,
            channels.commits,
            channels.read_barriers,
            applier,
            command,
        )? == ActorDirective::Shutdown
        {
            return Ok(());
        }
    }
    Err(ConsensusProbeError::ActorUnavailable)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActorDirective {
    Continue,
    Shutdown,
}

fn handle_actor_command(
    adapter: &mut PersistentRaftAdapter,
    outbound: &OutboundSenders,
    commits: &broadcast::Sender<CommittedProposal>,
    read_barriers: &broadcast::Sender<CompletedReadBarrier>,
    applier: Option<&dyn CommittedProposalApplier>,
    command: ActorCommand,
) -> ConsensusProbeResult<ActorDirective> {
    match command {
        ActorCommand::Status { reply } => {
            let _ = reply.send(Ok(adapter.status()));
        }
        ActorCommand::Campaign { reply } => {
            let result = adapter.campaign().map_err(Into::into).and_then(|output| {
                publish_output(output, outbound, commits, read_barriers, applier)
            });
            deliver_actor_result(adapter, reply, result)?;
        }
        ActorCommand::Tick { reply } => {
            let result = adapter.tick().map_err(Into::into).and_then(|output| {
                publish_output(output, outbound, commits, read_barriers, applier)
            });
            deliver_actor_result(adapter, reply, result)?;
        }
        ActorCommand::Propose { proposal, reply } => {
            let proposal_id = proposal.proposal_id;
            let result = adapter
                .propose(proposal)
                .map_err(Into::into)
                .and_then(|output| {
                    publish_output(output, outbound, commits, read_barriers, applier)
                })
                .map(|_| adapter.lookup_proposal(proposal_id));
            deliver_actor_result(adapter, reply, result)?;
        }
        ActorCommand::ForwardProposal { proposal, reply } => {
            let proposal_id = proposal.proposal_id;
            let result = adapter
                .forward_proposal(proposal)
                .map_err(Into::into)
                .and_then(|output| {
                    publish_output(output, outbound, commits, read_barriers, applier)
                })
                .map(|_| adapter.lookup_proposal(proposal_id));
            deliver_actor_result(adapter, reply, result)?;
        }
        ActorCommand::ReadBarrier { request, reply } => {
            let result = adapter
                .read_barrier(request)
                .map_err(Into::into)
                .and_then(|output| {
                    publish_output(output, outbound, commits, read_barriers, applier)
                });
            deliver_actor_result(adapter, reply, result)?;
        }
        ActorCommand::CancelReadBarrier { request_id } => {
            adapter.cancel_read_barrier(request_id)?;
        }
        ActorCommand::Receive { message, reply } => {
            let result = adapter
                .receive(message)
                .map_err(Into::into)
                .and_then(|output| {
                    publish_output(output, outbound, commits, read_barriers, applier)
                });
            deliver_actor_result(adapter, reply, result)?;
        }
        ActorCommand::Lookup { proposal_id, reply } => {
            let _ = reply.send(Ok(adapter.lookup_proposal(proposal_id)));
        }
        ActorCommand::AppliedProposals { reply } => {
            let _ = reply.send(Ok(adapter.applied_proposals().to_vec()));
        }
        ActorCommand::Checkpoint { reply } => {
            let result = create_consensus_checkpoint(adapter, applier);
            deliver_actor_result(adapter, reply, result)?;
        }
        ActorCommand::CheckpointIfAppliedGrowth {
            min_applied_entries,
            reply,
        } => {
            let result = if adapter.checkpoint_is_due(min_applied_entries)? {
                create_consensus_checkpoint(adapter, applier).map(Some)
            } else {
                Ok(None)
            };
            deliver_actor_result(adapter, reply, result)?;
        }
        #[cfg(test)]
        ActorCommand::InjectFailure { error, reply } => {
            deliver_actor_result(adapter, reply, Err(error))?;
        }
        ActorCommand::Shutdown { reply } => {
            let _ = reply.send(());
            return Ok(ActorDirective::Shutdown);
        }
    }
    Ok(ActorDirective::Continue)
}

fn create_consensus_checkpoint(
    adapter: &mut PersistentRaftAdapter,
    applier: Option<&dyn CommittedProposalApplier>,
) -> ConsensusProbeResult<ConsensusCheckpoint> {
    let Some(applier) = applier.filter(|applier| applier.supports_native_snapshots()) else {
        return adapter.checkpoint().map_err(Into::into);
    };
    let checkpoint_index = adapter.status().applied_index;
    let retained = adapter.checkpoint_retry_proposals()?;
    let snapshot = applier
        .capture_snapshot(checkpoint_index, &retained)
        .map_err(ConsensusProbeError::ProfileApplication)?;
    adapter
        .checkpoint_with_application(snapshot)
        .map_err(Into::into)
}

fn deliver_actor_result<T>(
    adapter: &PersistentRaftAdapter,
    reply: ActorReply<T>,
    result: ConsensusProbeResult<T>,
) -> ConsensusProbeResult<()> {
    let failure = fatal_actor_failure(adapter, &result);
    let _ = reply.send(result);
    failure.map_or(Ok(()), Err)
}

fn publish_output(
    output: ConsensusOutput,
    outbound: &OutboundSenders,
    commits: &broadcast::Sender<CommittedProposal>,
    read_barriers: &broadcast::Sender<CompletedReadBarrier>,
    applier: Option<&dyn CommittedProposalApplier>,
) -> ConsensusProbeResult<ConsensusStatus> {
    let ConsensusOutput {
        messages,
        commits: output_commits,
        installed_checkpoint,
        read_barriers: completed_read_barriers,
        status,
    } = output;
    for message in messages {
        let destination = message.to();
        let frame = message.to_wire()?;
        let peer = outbound
            .get(&destination)
            .ok_or(ConsensusProbeError::OutboundUnavailable(destination))?;
        match peer.sender.try_reserve() {
            Ok(permit) => {
                // Increment before publishing the reserved slot so the worker
                // cannot observe a frame before its queue accounting exists.
                peer.health.record_enqueue();
                permit.send(frame);
            }
            Err(mpsc::error::TrySendError::Full(())) => {
                let dropped_frames = peer.health.record_queue_full_drop();
                if dropped_frames.is_power_of_two() {
                    tracing::warn!(
                        %destination,
                        dropped_frames,
                        "consensus peer queue is full; dropping frame without blocking other peers"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(())) => {
                let dropped_frames = peer.health.record_worker_closed_drop();
                if dropped_frames.is_power_of_two() {
                    tracing::error!(
                        %destination,
                        dropped_frames,
                        "consensus peer worker is unavailable; dropping frame without blocking other peers"
                    );
                }
            }
        }
    }
    if let Some(installed) = installed_checkpoint
        && let Some(applier) = applier
    {
        if let Some(snapshot) = installed.application_snapshot.as_ref() {
            applier
                .install_snapshot(snapshot)
                .map_err(ConsensusProbeError::ProfileApplication)?;
        } else {
            applier
                .replay(&installed.proposals)
                .map_err(ConsensusProbeError::ProfileApplication)?;
        }
    }
    for commit in output_commits {
        if let Some(applier) = applier {
            applier
                .apply(&commit)
                .map_err(ConsensusProbeError::ProfileApplication)?;
        }
        // The adapter remains the authoritative replayable record. Broadcast is
        // only a low-latency notification; late consumers can call
        // `applied_proposals` or `lookup`.
        let _ = commits.send(commit);
    }
    for completed in completed_read_barriers {
        let _ = read_barriers.send(completed);
    }
    Ok(status)
}

fn fatal_actor_failure<T>(
    adapter: &PersistentRaftAdapter,
    result: &ConsensusProbeResult<T>,
) -> Option<ConsensusProbeError> {
    let error = result.as_ref().err()?;
    let fatal = adapter.status().fail_stopped
        || matches!(
            error,
            ConsensusProbeError::ProfileApplication(_)
                | ConsensusProbeError::OutboundUnavailable(_)
                | ConsensusProbeError::Consensus(
                    ConsensusError::Poisoned(_)
                        | ConsensusError::Storage(_)
                        | ConsensusError::InvalidState(_)
                )
        );
    fatal.then(|| error.clone())
}

fn spawn_outbound_workers(
    config: &ConsensusProbeConfig,
    client: &reqwest::Client,
) -> ConsensusProbeResult<(OutboundSenders, OutboundHealthRegistry, OutboundWorkers)> {
    let mut senders = BTreeMap::new();
    let mut health_registry = BTreeMap::new();
    let mut workers = Vec::with_capacity(2);
    for (&peer_id, base_url) in &config.peer_urls {
        if peer_id == config.node_id {
            continue;
        }
        let endpoint = base_url
            .join(INTERNAL_PEER_MESSAGE_PATH.trim_start_matches('/'))
            .map_err(|error| {
                ConsensusProbeError::InvalidConfiguration(format!(
                    "peer URL {base_url} cannot resolve the internal endpoint: {error}"
                ))
            })?;
        let (sender, receiver) = mpsc::channel(config.outbound_queue_capacity);
        let health = Arc::new(OutboundPeerHealth::new(
            peer_id,
            config.outbound_queue_capacity,
        ));
        let worker_client = client.clone();
        let worker_health = Arc::clone(&health);
        workers.push((
            peer_id,
            tokio::spawn(async move {
                run_outbound_worker(peer_id, endpoint, receiver, worker_client, worker_health)
                    .await;
            }),
        ));
        health_registry.insert(peer_id, Arc::clone(&health));
        senders.insert(peer_id, OutboundPeer { sender, health });
    }
    Ok((senders, Arc::new(health_registry), workers))
}

async fn run_outbound_worker(
    peer_id: NodeId,
    endpoint: Url,
    mut frames: mpsc::Receiver<Vec<u8>>,
    client: reqwest::Client,
    health: Arc<OutboundPeerHealth>,
) {
    while let Some(frame) = frames.recv().await {
        health.record_dequeue();
        let mut delivered = false;
        for attempt in 1..=OUTBOUND_ATTEMPTS {
            let result = client
                .post(endpoint.clone())
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(frame.clone())
                .send()
                .await;
            match result {
                Ok(response) if response.status().is_success() => {
                    delivered = true;
                    health.record_delivery();
                    break;
                }
                Ok(response) => {
                    tracing::warn!(
                        %peer_id,
                        %endpoint,
                        status = %response.status(),
                        attempt,
                        "consensus peer rejected an outbound frame"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        %peer_id,
                        %endpoint,
                        %error,
                        attempt,
                        "consensus peer transport failed"
                    );
                }
            }
            if attempt < OUTBOUND_ATTEMPTS {
                tokio::time::sleep(OUTBOUND_RETRY_DELAY).await;
            }
        }
        if !delivered {
            health.record_exhausted_retry();
            tracing::error!(
                %peer_id,
                %endpoint,
                "consensus frame was not delivered after bounded retries; Raft must retransmit"
            );
        }
    }
}

async fn join_actor_thread(actor_thread: thread::JoinHandle<()>) -> ConsensusProbeResult<()> {
    tokio::task::spawn_blocking(move || actor_thread.join())
        .await
        .map_err(|error| ConsensusProbeError::TaskJoin(error.to_string()))?
        .map_err(|_| ConsensusProbeError::ActorPanicked)
}

async fn abort_workers(workers: &mut OutboundWorkers) {
    for (_, worker) in workers.iter() {
        worker.abort();
    }
    for (_, worker) in workers.drain(..) {
        let _ = worker.await;
    }
}

async fn drain_outbound_workers(workers: &mut OutboundWorkers) -> ConsensusProbeResult<()> {
    let deadline = tokio::time::Instant::now() + OUTBOUND_SHUTDOWN_TIMEOUT;
    let mut errors = Vec::new();
    for (peer_id, mut worker) in workers.drain(..) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, &mut worker).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => errors.push(format!("node {peer_id}: {error}")),
            Err(_) => {
                worker.abort();
                let _ = worker.await;
                errors.push(format!(
                    "node {peer_id}: did not drain within {OUTBOUND_SHUTDOWN_TIMEOUT:?}"
                ));
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ConsensusProbeError::TaskJoin(errors.join(", ")))
    }
}

fn collect_shutdown_error(errors: &mut Vec<String>, stage: &str, result: ConsensusProbeResult<()>) {
    if let Err(error) = result {
        errors.push(format!("{stage}: {error}"));
    }
}

/// Internal-only transport surface. It intentionally has no CORS layer.
pub fn internal_peer_router(handle: ConsensusProbeHandle) -> Router {
    Router::new()
        .route(INTERNAL_PEER_MESSAGE_PATH, post(receive_peer_message))
        .layer(DefaultBodyLimit::max(MAX_PEER_MESSAGE_WIRE_BYTES))
        .with_state(handle)
}

/// Diagnostic routes for opaque probe proposals. These routes do not expose
/// profile replication or quorum-durable profile writes.
pub fn experimental_consensus_router(handle: ConsensusProbeHandle) -> Router {
    Router::new()
        .route(EXPERIMENTAL_STATUS_PATH, get(consensus_status))
        .route(EXPERIMENTAL_CHECKPOINTS_PATH, post(create_checkpoint))
        .route(EXPERIMENTAL_PROPOSALS_PATH, post(propose))
        .route(EXPERIMENTAL_PROPOSAL_LOOKUP_PATH, get(lookup_proposal))
        .layer(DefaultBodyLimit::max(MAX_PROPOSAL_JSON_BYTES))
        .with_state(handle)
}

async fn receive_peer_message(
    State(handle): State<ConsensusProbeHandle>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ConsensusProbeApiError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if content_type != Some("application/octet-stream") {
        return Err(ConsensusProbeError::UnsupportedPeerContentType.into());
    }
    handle.receive_wire(&body).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn consensus_status(
    State(handle): State<ConsensusProbeHandle>,
) -> Result<Json<ConsensusProbeStatus>, ConsensusProbeApiError> {
    let consensus = handle.status().await?;
    Ok(Json(ConsensusProbeStatus::new(
        &consensus,
        handle.outbound_transport_status(),
    )))
}

async fn create_checkpoint(
    State(handle): State<ConsensusProbeHandle>,
) -> Result<(StatusCode, Json<ConsensusProbeCheckpointResponse>), ConsensusProbeApiError> {
    let checkpoint = handle.checkpoint().await?;
    Ok((
        StatusCode::CREATED,
        Json(ConsensusProbeCheckpointResponse::from(checkpoint)),
    ))
}

async fn propose(
    State(handle): State<ConsensusProbeHandle>,
    Json(request): Json<ConsensusProbeProposalRequest>,
) -> Result<(StatusCode, Json<ConsensusProbeProposalResponse>), ConsensusProbeApiError> {
    if request.payload.len() > MAX_PROPOSAL_PAYLOAD_BYTES {
        return Err(ConsensusError::InvalidMessage(format!(
            "proposal payload is {} bytes; maximum is {MAX_PROPOSAL_PAYLOAD_BYTES}",
            request.payload.len()
        ))
        .into());
    }
    let proposal_id = request.proposal_id;
    let lookup = handle
        .propose(proposal_id, request.expected_term, request.payload)
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(ConsensusProbeProposalResponse::from_lookup(
            proposal_id,
            lookup,
        )),
    ))
}

async fn lookup_proposal(
    State(handle): State<ConsensusProbeHandle>,
    AxumPath(proposal_id): AxumPath<u64>,
) -> Result<Json<ConsensusProbeProposalResponse>, ConsensusProbeApiError> {
    Ok(Json(ConsensusProbeProposalResponse::from_lookup(
        proposal_id,
        handle.lookup(proposal_id).await?,
    )))
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusProbeRole {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

impl From<ConsensusRole> for ConsensusProbeRole {
    fn from(role: ConsensusRole) -> Self {
        match role {
            ConsensusRole::Follower => Self::Follower,
            ConsensusRole::PreCandidate => Self::PreCandidate,
            ConsensusRole::Candidate => Self::Candidate,
            ConsensusRole::Leader => Self::Leader,
        }
    }
}

/// Cumulative local evidence about one outbound peer transport.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusProbeTransportCondition {
    Unknown,
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConsensusProbePeerTransportStatus {
    pub peer_id: u64,
    pub observed_condition: ConsensusProbeTransportCondition,
    pub queue_capacity: usize,
    pub queued_frames: usize,
    pub enqueued_frames: u64,
    pub delivered_frames: u64,
    pub dropped_queue_full_frames: u64,
    pub dropped_worker_closed_frames: u64,
    pub exhausted_retry_frames: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConsensusProbeStatus {
    pub capability: &'static str,
    pub stability: &'static str,
    pub production_readiness: &'static str,
    pub node_id: u64,
    pub group_id: u64,
    pub group_epoch: u64,
    pub role: ConsensusProbeRole,
    pub leader_id: Option<u64>,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub checkpoint_index: u64,
    pub retained_log_first_index: u64,
    pub voter_count: usize,
    pub fail_stopped: bool,
    pub observation_scope: &'static str,
    pub profile_replication: bool,
    pub profile_guarantee_ceiling: &'static str,
    pub peer_authentication: &'static str,
    pub outbound_transport: Vec<ConsensusProbePeerTransportStatus>,
}

impl ConsensusProbeStatus {
    fn new(
        status: &ConsensusStatus,
        outbound_transport: Vec<ConsensusProbePeerTransportStatus>,
    ) -> Self {
        Self {
            capability: "fixed_voter_consensus_probe",
            stability: "experimental",
            production_readiness: "not_production_ready",
            node_id: status.node_id.get(),
            group_id: status.group_id.get(),
            group_epoch: status.group_epoch.get(),
            role: status.role.into(),
            leader_id: status.leader_id.map(NodeId::get),
            term: status.term.get(),
            commit_index: status.commit_index.get(),
            applied_index: status.applied_index.get(),
            checkpoint_index: status.checkpoint_index.get(),
            retained_log_first_index: status.retained_log_first_index.get(),
            voter_count: status.voter_count,
            fail_stopped: status.fail_stopped,
            observation_scope: "local",
            profile_replication: false,
            profile_guarantee_ceiling: "local_durable",
            peer_authentication: "none",
            outbound_transport,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct ConsensusProbeCheckpointResponse {
    pub index: u64,
    pub term: u64,
    pub proposal_count: usize,
    pub encoded_bytes: usize,
    pub durability: &'static str,
    pub compaction: &'static str,
}

impl From<ConsensusCheckpoint> for ConsensusProbeCheckpointResponse {
    fn from(checkpoint: ConsensusCheckpoint) -> Self {
        Self {
            index: checkpoint.index.get(),
            term: checkpoint.term.get(),
            proposal_count: checkpoint.proposal_count,
            encoded_bytes: checkpoint.encoded_bytes,
            durability: "fsync_before_install",
            compaction: "logical_raft_prefix_and_physical_eprs",
        }
    }
}

impl From<ConsensusStatus> for ConsensusProbeStatus {
    fn from(status: ConsensusStatus) -> Self {
        Self::new(&status, Vec::new())
    }
}

#[derive(Debug, Deserialize)]
pub struct ConsensusProbeProposalRequest {
    pub proposal_id: u64,
    pub expected_term: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusProbeProposalState {
    Unknown,
    Pending,
    Committed,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConsensusProbeCommit {
    pub term: u64,
    pub log_index: u64,
    pub payload: Vec<u8>,
}

impl ConsensusProbeCommit {
    fn new(receipt: &CommitReceipt, payload: Vec<u8>) -> Self {
        Self {
            term: receipt.term.get(),
            log_index: receipt.log_index.get(),
            payload,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConsensusProbeProposalResponse {
    pub proposal_id: u64,
    pub state: ConsensusProbeProposalState,
    pub observation_scope: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<ConsensusProbeCommit>,
}

impl ConsensusProbeProposalResponse {
    fn from_lookup(proposal_id: u64, lookup: ProposalLookup) -> Self {
        let (state, commit) = match lookup {
            ProposalLookup::Unknown => (ConsensusProbeProposalState::Unknown, None),
            ProposalLookup::Pending { .. } => (ConsensusProbeProposalState::Pending, None),
            ProposalLookup::Committed(committed) => (
                ConsensusProbeProposalState::Committed,
                Some(ConsensusProbeCommit::new(
                    &committed.receipt,
                    committed.payload,
                )),
            ),
        };
        Self {
            proposal_id,
            state,
            observation_scope: "local",
            commit,
        }
    }
}

#[derive(Debug)]
pub struct ConsensusProbeApiError(ConsensusProbeError);

impl From<ConsensusProbeError> for ConsensusProbeApiError {
    fn from(error: ConsensusProbeError) -> Self {
        Self(error)
    }
}

impl From<ConsensusError> for ConsensusProbeApiError {
    fn from(error: ConsensusError) -> Self {
        Self(error.into())
    }
}

#[derive(Debug, Serialize)]
struct ConsensusProbeErrorBody {
    code: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    leader_hint: Option<u64>,
}

impl IntoResponse for ConsensusProbeApiError {
    fn into_response(self) -> Response {
        let (status, code, leader_hint) = match &self.0 {
            ConsensusProbeError::UnsupportedPeerContentType => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                None,
            ),
            ConsensusProbeError::InvalidConfiguration(_)
            | ConsensusProbeError::Consensus(
                ConsensusError::InvalidIdentifier(_)
                | ConsensusError::InvalidVoterSet(_)
                | ConsensusError::InvalidMessage(_),
            ) => (StatusCode::BAD_REQUEST, "invalid_request", None),
            ConsensusProbeError::Consensus(ConsensusError::NotLeader { leader_hint }) => (
                StatusCode::CONFLICT,
                "not_leader",
                leader_hint.map(NodeId::get),
            ),
            ConsensusProbeError::Consensus(
                ConsensusError::GroupMismatch { .. }
                | ConsensusError::FencedEpoch { .. }
                | ConsensusError::StaleTerm { .. }
                | ConsensusError::DuplicateProposal(_)
                | ConsensusError::ConflictingProposal(_)
                | ConsensusError::DuplicateReadBarrier(_),
            ) => (StatusCode::CONFLICT, "proposal_conflict", None),
            ConsensusProbeError::ReadBarrierTimeout { .. }
            | ConsensusProbeError::Consensus(ConsensusError::TooManyReadBarriers) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "read_barrier_unavailable",
                None,
            ),
            ConsensusProbeError::Consensus(ConsensusError::CheckpointTooLarge { .. }) => {
                (StatusCode::PAYLOAD_TOO_LARGE, "checkpoint_too_large", None)
            }
            ConsensusProbeError::ActorUnavailable
            | ConsensusProbeError::OutboundUnavailable(_)
            | ConsensusProbeError::ProfileApplication(_)
            | ConsensusProbeError::Consensus(
                ConsensusError::Poisoned(_) | ConsensusError::Storage(_),
            ) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "consensus_unavailable",
                None,
            ),
            ConsensusProbeError::ThreadStart(_)
            | ConsensusProbeError::ActorPanicked
            | ConsensusProbeError::TaskJoin(_)
            | ConsensusProbeError::RuntimeShutdown(_)
            | ConsensusProbeError::ReadBarrierIdExhausted
            | ConsensusProbeError::Consensus(
                ConsensusError::InvalidState(_)
                | ConsensusError::Library(_)
                | ConsensusError::Unsupported(_),
            ) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "consensus_internal",
                None,
            ),
        };
        (
            status,
            Json(ConsensusProbeErrorBody {
                code,
                message: self.0.to_string(),
                leader_hint,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{body::Body, http::Request};
    use epoch_catalog::{ApplyResource, CatalogCommand, ResourceName, ResourceSpec};
    use epoch_consensus::{InMemoryRaftAdapter, LogIndex, MAX_SNAPSHOT_DATA_BYTES};
    use epoch_core::{ResourceKind, WorkloadProfile};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::*;
    use crate::catalog_tablet::{CatalogTabletScope, CatalogTabletService};
    use crate::delivery_proposal::{ProposalRoute, propose_and_wait};

    fn peer_url(port: u16) -> Url {
        Url::parse(&format!("http://127.0.0.1:{port}/")).expect("test peer URL should parse")
    }

    fn config() -> ConsensusProbeConfig {
        ConsensusProbeConfig::new(
            1,
            7,
            3,
            [
                (1, peer_url(31_001)),
                (2, peer_url(31_002)),
                (3, peer_url(31_003)),
            ],
            Duration::from_mins(1),
        )
        .expect("test config should be valid")
    }

    #[test]
    fn peer_spec_requires_three_unique_ids_and_authority_only_urls() {
        let parsed =
            parse_peer_urls("1=http://node-1:7701, 2=http://node-2:7701,3=https://node-3:7701")
                .expect("valid peer spec should parse");
        assert_eq!(parsed.len(), 3);
        assert!(ConsensusProbeConfig::new(1, 1, 1, parsed, Duration::from_millis(100)).is_ok());

        let duplicate_id = [
            (1, peer_url(31_001)),
            (1, peer_url(31_002)),
            (3, peer_url(31_003)),
        ];
        assert!(
            ConsensusProbeConfig::new(1, 1, 1, duplicate_id, Duration::from_millis(100)).is_err()
        );
        let path_url = Url::parse("http://node-2:7701/not-a-base").expect("URL should parse");
        assert!(
            ConsensusProbeConfig::new(
                1,
                1,
                1,
                [(1, peer_url(31_001)), (2, path_url), (3, peer_url(31_003))],
                Duration::from_millis(100),
            )
            .is_err()
        );
    }

    #[test]
    fn saturated_peer_queue_does_not_block_a_healthy_peer() {
        let voters = [
            NodeId::new(1).expect("valid node"),
            NodeId::new(2).expect("valid node"),
            NodeId::new(3).expect("valid node"),
        ];
        let mut adapter = InMemoryRaftAdapter::new(
            voters[0],
            GroupId::new(7).expect("valid group"),
            GroupEpoch::new(3).expect("valid epoch"),
            voters,
        )
        .expect("adapter should open");
        let output = adapter.campaign().expect("campaign should produce output");
        assert_eq!(output.messages.len(), 2);
        let (node_2_tx, mut node_2_rx) = mpsc::channel::<Vec<u8>>(1);
        let (node_3_tx, mut node_3_rx) = mpsc::channel::<Vec<u8>>(1);
        node_2_tx
            .try_send(vec![0])
            .expect("failed peer queue should be saturated");
        let node_2_health = Arc::new(OutboundPeerHealth::new(voters[1], 1));
        let node_3_health = Arc::new(OutboundPeerHealth::new(voters[2], 1));
        let outbound = BTreeMap::from([
            (
                voters[1],
                OutboundPeer {
                    sender: node_2_tx,
                    health: Arc::clone(&node_2_health),
                },
            ),
            (
                voters[2],
                OutboundPeer {
                    sender: node_3_tx,
                    health: Arc::clone(&node_3_health),
                },
            ),
        ]);
        let (commits, _) = broadcast::channel(1);
        let (read_barriers, _) = broadcast::channel(1);

        let status = publish_output(output, &outbound, &commits, &read_barriers, None)
            .expect("initial output should be dispatched before initialization");

        assert_eq!(status.node_id, voters[0]);
        assert_eq!(
            node_2_rx
                .try_recv()
                .expect("failed peer queue should retain its original frame"),
            vec![0]
        );
        assert!(node_2_rx.try_recv().is_err());
        let node_3_frame = node_3_rx
            .try_recv()
            .expect("healthy peer should receive its frame");
        assert_eq!(
            PeerMessage::from_wire(&node_3_frame, voters[2])
                .expect("healthy peer frame should decode")
                .to(),
            voters[2]
        );
        assert_eq!(
            node_2_health
                .dropped_queue_full_frames
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(node_3_health.enqueued_frames.load(Ordering::Relaxed), 1);
    }

    async fn runtime() -> (TempDir, ConsensusProbeRuntime) {
        let directory = TempDir::new().expect("temp directory should be created");
        let runtime = ConsensusProbeRuntime::start(config(), directory.path().join("raft.wal"))
            .await
            .expect("runtime should start");
        (directory, runtime)
    }

    #[tokio::test]
    async fn expected_request_errors_do_not_stop_the_actor() {
        let (_directory, runtime) = runtime().await;
        let handle = runtime.handle();
        let proposal_id = ProposalId::new(41).unwrap();
        let expected = [
            ConsensusProbeError::Consensus(ConsensusError::NotLeader {
                leader_hint: Some(NodeId::new(2).unwrap()),
            }),
            ConsensusProbeError::Consensus(ConsensusError::StaleTerm {
                current: Term::new(2),
                observed: Term::new(1),
            }),
            ConsensusProbeError::Consensus(ConsensusError::DuplicateProposal(proposal_id)),
            ConsensusProbeError::Consensus(ConsensusError::ConflictingProposal(proposal_id)),
            ConsensusProbeError::Consensus(ConsensusError::Unsupported(
                "unsupported peer capability".into(),
            )),
            ConsensusProbeError::Consensus(ConsensusError::CheckpointTooLarge {
                observed_bytes: MAX_SNAPSHOT_DATA_BYTES + 1,
                max_bytes: MAX_SNAPSHOT_DATA_BYTES,
            }),
        ];

        for error in expected {
            let expected_message = error.to_string();
            let returned = handle.inject_failure(error).await.unwrap_err();
            assert_eq!(returned.to_string(), expected_message);
            handle
                .status()
                .await
                .expect("an expected request error must leave the actor available");
        }
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_size_error_has_a_stable_nonfatal_http_contract() {
        let response = ConsensusProbeApiError(ConsensusProbeError::Consensus(
            ConsensusError::CheckpointTooLarge {
                observed_bytes: MAX_SNAPSHOT_DATA_BYTES + 1,
                max_bytes: MAX_SNAPSHOT_DATA_BYTES,
            },
        ))
        .into_response();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("error body should collect")
            .to_bytes();
        let document: Value = serde_json::from_slice(&body).expect("error should be JSON");
        assert_eq!(document["code"], "checkpoint_too_large");
        assert!(
            document["message"]
                .as_str()
                .unwrap()
                .contains(&MAX_SNAPSHOT_DATA_BYTES.to_string())
        );
    }

    #[tokio::test]
    async fn fatal_storage_error_is_published_and_stops_the_actor() {
        let (_directory, runtime) = runtime().await;
        let handle = runtime.handle();
        let injected_message = "injected stable-store failure";

        let result = handle
            .inject_failure(ConsensusProbeError::Consensus(ConsensusError::Storage(
                injected_message.into(),
            )))
            .await;
        assert!(matches!(
            result,
            Err(ConsensusProbeError::Consensus(ConsensusError::Storage(ref message)))
                if message == injected_message
        ));

        let failure =
            tokio::time::timeout(Duration::from_secs(2), runtime.wait_for_actor_failure())
                .await
                .expect("fatal actor failure should be supervised");
        assert!(matches!(
            failure,
            ConsensusProbeError::Consensus(ConsensusError::Storage(ref message))
                if message == injected_message
        ));
        assert!(matches!(
            handle.status().await,
            Err(ConsensusProbeError::ActorUnavailable)
        ));

        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn status_route_never_claims_profile_replication() {
        let (_directory, runtime) = runtime().await;
        let response = runtime
            .experimental_router()
            .oneshot(
                Request::builder()
                    .uri(EXPERIMENTAL_STATUS_PATH)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body should collect")
            .to_bytes();
        let status: Value = serde_json::from_slice(&body).expect("status should be JSON");
        assert_eq!(status["profile_replication"], false);
        assert_eq!(status["profile_guarantee_ceiling"], "local_durable");
        assert_eq!(status["observation_scope"], "local");
        assert_eq!(status["stability"], "experimental");
        assert_eq!(status["production_readiness"], "not_production_ready");
        assert_eq!(status["checkpoint_index"], 0);
        assert_eq!(status["retained_log_first_index"], 1);
        let outbound_transport = status["outbound_transport"]
            .as_array()
            .expect("outbound transport status should be an array");
        assert_eq!(outbound_transport.len(), 2);
        assert_eq!(outbound_transport[0]["peer_id"], 2);
        assert_eq!(outbound_transport[0]["queue_capacity"], 128);
        assert!(outbound_transport[0]["dropped_queue_full_frames"].is_number());
        assert!(outbound_transport[0]["exhausted_retry_frames"].is_number());
        runtime
            .shutdown()
            .await
            .expect("runtime should stop cleanly");
    }

    #[tokio::test]
    async fn proposal_route_reports_not_leader_without_claiming_acceptance() {
        let (_directory, runtime) = runtime().await;
        let response = runtime
            .experimental_router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(EXPERIMENTAL_PROPOSALS_PATH)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "proposal_id": 9,
                            "expected_term": 0,
                            "payload": [1, 2, 3]
                        }))
                        .expect("request should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        runtime
            .shutdown()
            .await
            .expect("runtime should stop cleanly");
    }

    #[tokio::test]
    async fn internal_router_requires_raw_content_type_and_has_no_cors() {
        let (_directory, runtime) = runtime().await;
        let router = Arc::new(runtime.internal_router());
        let missing_content_type = router
            .as_ref()
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(INTERNAL_PEER_MESSAGE_PATH)
                    .header("origin", "https://example.invalid")
                    .body(Body::from(vec![0_u8; 8]))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(
            missing_content_type.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        assert!(
            missing_content_type
                .headers()
                .get("access-control-allow-origin")
                .is_none()
        );

        let oversized = router
            .as_ref()
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(INTERNAL_PEER_MESSAGE_PATH)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(Body::from(vec![0_u8; MAX_PEER_MESSAGE_WIRE_BYTES + 1]))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        runtime
            .shutdown()
            .await
            .expect("runtime should stop cleanly");
    }

    #[tokio::test]
    async fn outbound_worker_preserves_per_peer_order() {
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let receiver = Router::new().route(
            INTERNAL_PEER_MESSAGE_PATH,
            post(move |body: Bytes| {
                let observed_tx = observed_tx.clone();
                async move {
                    observed_tx
                        .send(body.to_vec())
                        .expect("observer should remain open");
                    StatusCode::NO_CONTENT
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should have address");
        let server = tokio::spawn(async move {
            axum::serve(listener, receiver)
                .await
                .expect("test server should run");
        });
        let (frames_tx, frames_rx) = mpsc::channel(2);
        let health = Arc::new(OutboundPeerHealth::new(
            NodeId::new(2).expect("valid node"),
            2,
        ));
        let worker = tokio::spawn(run_outbound_worker(
            NodeId::new(2).expect("valid node"),
            Url::parse(&format!("http://{address}{INTERNAL_PEER_MESSAGE_PATH}"))
                .expect("endpoint should parse"),
            frames_rx,
            build_outbound_client().expect("internal client should build"),
            Arc::clone(&health),
        ));
        health.record_enqueue();
        frames_tx
            .send(vec![1])
            .await
            .expect("first send should work");
        health.record_enqueue();
        frames_tx
            .send(vec![2])
            .await
            .expect("second send should work");
        drop(frames_tx);

        assert_eq!(observed_rx.recv().await, Some(vec![1]));
        assert_eq!(observed_rx.recv().await, Some(vec![2]));
        worker.await.expect("worker should stop");
        assert_eq!(health.delivered_frames.load(Ordering::Relaxed), 2);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn outbound_client_never_follows_a_peer_redirect() {
        let target_hits = Arc::new(AtomicUsize::new(0));
        let observed_target_hits = Arc::clone(&target_hits);
        let target_router = Router::new().route(
            INTERNAL_PEER_MESSAGE_PATH,
            post(move || {
                let observed_target_hits = Arc::clone(&observed_target_hits);
                async move {
                    observed_target_hits.fetch_add(1, Ordering::Relaxed);
                    StatusCode::NO_CONTENT
                }
            }),
        );
        let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("redirect target should bind");
        let target_address = target_listener
            .local_addr()
            .expect("redirect target should have an address");
        let target_server = tokio::spawn(async move {
            axum::serve(target_listener, target_router)
                .await
                .expect("redirect target should run");
        });

        let redirect_hits = Arc::new(AtomicUsize::new(0));
        let observed_redirect_hits = Arc::clone(&redirect_hits);
        let redirect_target = format!("http://{target_address}{INTERNAL_PEER_MESSAGE_PATH}");
        let redirect_router = Router::new().route(
            INTERNAL_PEER_MESSAGE_PATH,
            post(move || {
                let observed_redirect_hits = Arc::clone(&observed_redirect_hits);
                let redirect_target = redirect_target.clone();
                async move {
                    observed_redirect_hits.fetch_add(1, Ordering::Relaxed);
                    axum::response::Redirect::temporary(&redirect_target)
                }
            }),
        );
        let redirect_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("redirecting peer should bind");
        let redirect_address = redirect_listener
            .local_addr()
            .expect("redirecting peer should have an address");
        let redirect_server = tokio::spawn(async move {
            axum::serve(redirect_listener, redirect_router)
                .await
                .expect("redirecting peer should run");
        });

        let (frames_tx, frames_rx) = mpsc::channel(1);
        let peer_id = NodeId::new(2).expect("valid node");
        let health = Arc::new(OutboundPeerHealth::new(peer_id, 1));
        let worker = tokio::spawn(run_outbound_worker(
            peer_id,
            Url::parse(&format!(
                "http://{redirect_address}{INTERNAL_PEER_MESSAGE_PATH}"
            ))
            .expect("redirecting endpoint should parse"),
            frames_rx,
            build_outbound_client().expect("internal client should build"),
            Arc::clone(&health),
        ));
        health.record_enqueue();
        frames_tx
            .send(vec![1, 2, 3])
            .await
            .expect("outbound frame should queue");
        drop(frames_tx);

        worker.await.expect("worker should stop");
        assert_eq!(redirect_hits.load(Ordering::Relaxed), OUTBOUND_ATTEMPTS);
        assert_eq!(target_hits.load(Ordering::Relaxed), 0);
        assert_eq!(health.delivered_frames.load(Ordering::Relaxed), 0);
        assert_eq!(health.exhausted_retry_frames.load(Ordering::Relaxed), 1);

        redirect_server.abort();
        target_server.abort();
        let _ = redirect_server.await;
        let _ = target_server.await;
    }

    #[tokio::test]
    async fn outbound_shutdown_joins_remaining_workers_after_an_earlier_join_error() {
        let cancelled = tokio::spawn(std::future::pending::<()>());
        cancelled.abort();
        let completed = Arc::new(AtomicUsize::new(0));
        let completed_worker = Arc::clone(&completed);
        let healthy = tokio::spawn(async move {
            completed_worker.fetch_add(1, Ordering::Relaxed);
        });
        let mut workers = vec![
            (NodeId::new(2).expect("valid node"), cancelled),
            (NodeId::new(3).expect("valid node"), healthy),
        ];

        let error = drain_outbound_workers(&mut workers)
            .await
            .expect_err("the cancelled worker should be reported");

        assert!(workers.is_empty());
        assert_eq!(completed.load(Ordering::Relaxed), 1);
        assert!(error.to_string().contains("node 2"));
    }

    struct TestProbeCluster {
        directories: Vec<TempDir>,
        peers: Vec<(u64, Url)>,
        runtimes: Vec<ConsensusProbeRuntime>,
        servers: Vec<Option<JoinHandle<()>>>,
    }

    #[derive(Debug)]
    struct RejectingProfileApplier;

    impl CommittedProposalApplier for RejectingProfileApplier {
        fn replay(&self, _committed: &[CommittedProposal]) -> Result<(), String> {
            Ok(())
        }

        fn apply(&self, _committed: &CommittedProposal) -> Result<(), String> {
            Err("injected profile apply failure".into())
        }
    }

    impl TestProbeCluster {
        async fn start() -> Self {
            Self::start_with_outbound_queue_capacity(DEFAULT_OUTBOUND_QUEUE_CAPACITY).await
        }

        async fn start_with_outbound_queue_capacity(outbound_queue_capacity: usize) -> Self {
            Self::start_with_appliers(outbound_queue_capacity, vec![None, None, None]).await
        }

        async fn start_with_appliers(
            outbound_queue_capacity: usize,
            appliers: Vec<Option<Arc<dyn CommittedProposalApplier>>>,
        ) -> Self {
            assert_eq!(appliers.len(), 3, "one applier slot is required per voter");
            let listeners = bind_three_listeners().await;
            let peers = listeners
                .iter()
                .enumerate()
                .map(|(index, listener)| {
                    let node_id = u64::try_from(index + 1).expect("three node IDs fit in u64");
                    let address = listener
                        .local_addr()
                        .expect("listener should have an address");
                    (
                        node_id,
                        Url::parse(&format!("http://{address}/"))
                            .expect("peer base URL should parse"),
                    )
                })
                .collect::<Vec<_>>();
            let directories = (0..3)
                .map(|_| TempDir::new().expect("temp directory should be created"))
                .collect::<Vec<_>>();
            let mut runtimes = Vec::new();
            let mut servers = Vec::new();
            for (index, listener) in listeners.into_iter().enumerate() {
                let node_id = u64::try_from(index + 1).expect("three node IDs fit in u64");
                let config = ConsensusProbeConfig::new(
                    node_id,
                    77,
                    1,
                    peers.clone(),
                    TEST_CONSENSUS_TICK_INTERVAL,
                )
                .expect("cluster config should be valid")
                .with_queue_capacities(DEFAULT_COMMAND_QUEUE_CAPACITY, outbound_queue_capacity)
                .expect("cluster queue capacities should be valid");
                let stable_path = directories[index].path().join("raft.wal");
                let runtime = if let Some(applier) = appliers[index].clone() {
                    ConsensusProbeRuntime::start_with_profile_applier(config, stable_path, applier)
                        .await
                } else {
                    ConsensusProbeRuntime::start(config, stable_path).await
                }
                .expect("probe runtime should start");
                let router = runtime.internal_router();
                servers.push(Some(tokio::spawn(async move {
                    axum::serve(listener, router)
                        .await
                        .expect("peer server should run");
                })));
                runtimes.push(runtime);
            }
            Self {
                directories,
                peers,
                runtimes,
                servers,
            }
        }

        fn handles(&self) -> Vec<ConsensusProbeHandle> {
            self.runtimes
                .iter()
                .map(ConsensusProbeRuntime::handle)
                .collect()
        }

        async fn stop_peer_listener(&mut self, index: usize) {
            let server = self.servers[index]
                .take()
                .expect("peer listener should still be running");
            server.abort();
            let result = server.await;
            assert!(
                result.is_err_and(|error| error.is_cancelled()),
                "peer listener should stop through cancellation"
            );
        }

        async fn start_peer_listener(&mut self, index: usize) {
            assert!(
                self.servers[index].is_none(),
                "peer listener must be stopped before it is restarted"
            );
            let address = self.peers[index]
                .1
                .socket_addrs(|| None)
                .expect("peer URL should resolve without DNS")
                .into_iter()
                .next()
                .expect("peer URL should contain a socket address");
            let listener = tokio::net::TcpListener::bind(address)
                .await
                .expect("peer listener should rebind");
            let router = self.runtimes[index].internal_router();
            self.servers[index] = Some(tokio::spawn(async move {
                axum::serve(listener, router)
                    .await
                    .expect("restarted peer server should run");
            }));
        }

        async fn replace_peer_listener_with_blackhole(&mut self, index: usize) {
            self.stop_peer_listener(index).await;
            let address = self.peers[index]
                .1
                .socket_addrs(|| None)
                .expect("peer URL should resolve without DNS")
                .into_iter()
                .next()
                .expect("peer URL should contain a socket address");
            let listener = tokio::net::TcpListener::bind(address)
                .await
                .expect("blackhole peer listener should rebind");
            self.servers[index] = Some(tokio::spawn(async move {
                let mut connections = Vec::new();
                loop {
                    let (connection, _) = listener
                        .accept()
                        .await
                        .expect("blackhole peer should accept connections");
                    connections.push(connection);
                }
            }));
        }

        async fn stop(&mut self) {
            for server in &mut self.servers {
                if let Some(server) = server.take() {
                    server.abort();
                    let _ = server.await;
                }
            }
            let mut shutdowns = tokio::task::JoinSet::new();
            for runtime in std::mem::take(&mut self.runtimes) {
                shutdowns.spawn(runtime.shutdown());
            }
            while let Some(result) = shutdowns.join_next().await {
                result
                    .expect("shutdown task should not panic")
                    .expect("runtime should stop cleanly");
            }
        }

        async fn restart_with_appliers(
            &mut self,
            appliers: Vec<Option<Arc<dyn CommittedProposalApplier>>>,
        ) {
            assert!(
                self.runtimes.is_empty(),
                "cluster must be stopped before restart"
            );
            assert_eq!(appliers.len(), 3, "one applier slot is required per voter");
            let mut listeners = Vec::new();
            for (_, peer) in &self.peers {
                let address = peer
                    .socket_addrs(|| None)
                    .expect("peer URL should resolve without DNS")
                    .into_iter()
                    .next()
                    .expect("peer URL should contain a socket address");
                listeners.push(
                    tokio::net::TcpListener::bind(address)
                        .await
                        .expect("peer listener should rebind"),
                );
            }
            for (index, listener) in listeners.into_iter().enumerate() {
                let node_id = u64::try_from(index + 1).expect("three node IDs fit in u64");
                let config = ConsensusProbeConfig::new(
                    node_id,
                    77,
                    1,
                    self.peers.clone(),
                    TEST_CONSENSUS_TICK_INTERVAL,
                )
                .expect("cluster config should be valid");
                let stable_path = self.directories[index].path().join("raft.wal");
                let runtime = if let Some(applier) = appliers[index].clone() {
                    ConsensusProbeRuntime::start_with_profile_applier(config, stable_path, applier)
                        .await
                } else {
                    ConsensusProbeRuntime::start(config, stable_path).await
                }
                .expect("probe runtime should restart");
                let router = runtime.internal_router();
                self.servers.push(Some(tokio::spawn(async move {
                    axum::serve(listener, router)
                        .await
                        .expect("restarted peer server should run");
                })));
                self.runtimes.push(runtime);
            }
        }

        async fn shutdown(mut self) {
            self.stop().await;
        }

        async fn shutdown_after_actor_failure(self) {
            for server in self.servers.into_iter().flatten() {
                server.abort();
                let _ = server.await;
            }
            for runtime in self.runtimes {
                let _ = runtime.shutdown().await;
            }
        }
    }

    async fn bind_three_listeners() -> Vec<tokio::net::TcpListener> {
        let mut listeners = Vec::new();
        for _ in 0..3 {
            listeners.push(
                tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("peer listener should bind"),
            );
        }
        listeners
    }

    async fn wait_for_leader(handles: &[ConsensusProbeHandle]) -> (usize, ConsensusStatus) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut leaders = Vec::new();
                for (index, handle) in handles.iter().enumerate() {
                    let status = handle.status().await.expect("status should be available");
                    if status.role == ConsensusRole::Leader {
                        leaders.push((index, status));
                    }
                }
                if let [leader] = leaders.as_slice() {
                    break leader.clone();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("one leader should be elected")
    }

    async fn wait_for_commit(
        handles: &[ConsensusProbeHandle],
        proposal_id: u64,
    ) -> Vec<ProposalLookup> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut lookups = Vec::new();
                for handle in handles {
                    lookups.push(
                        handle
                            .lookup(proposal_id)
                            .await
                            .expect("lookup should be available"),
                    );
                }
                if lookups
                    .iter()
                    .all(|lookup| matches!(lookup, ProposalLookup::Committed(_)))
                {
                    break lookups;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("proposal should commit on all three nodes")
    }

    async fn wait_for_commit_on(
        handles: &[ConsensusProbeHandle],
        indexes: &[usize],
        proposal_id: u64,
    ) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut committed = true;
                for index in indexes {
                    committed &= matches!(
                        handles[*index]
                            .lookup(proposal_id)
                            .await
                            .expect("lookup should be available"),
                        ProposalLookup::Committed(_)
                    );
                }
                if committed {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("proposal should commit on the selected voters");
    }

    async fn propose_through_current_leader(
        handles: &[ConsensusProbeHandle],
        proposal_id: u64,
        payload: Vec<u8>,
    ) -> ProposalLookup {
        tokio::time::timeout(Duration::from_secs(5), async {
            'proposal: loop {
                for handle in handles {
                    let status = handle.status().await.expect("status should be available");
                    if status.role != ConsensusRole::Leader {
                        continue;
                    }
                    match handle
                        .propose(proposal_id, status.term.get(), payload.clone())
                        .await
                    {
                        Ok(lookup) => break 'proposal lookup,
                        Err(ConsensusProbeError::Consensus(
                            ConsensusError::NotLeader { .. } | ConsensusError::StaleTerm { .. },
                        )) => {}
                        Err(error) => panic!("current leader should accept the proposal: {error}"),
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a live majority leader should accept the proposal")
    }

    fn catalog_command(token: &str, name: &str, shards: u32) -> CatalogCommand {
        CatalogCommand::Apply(ApplyResource {
            request_token: token.into(),
            expected_generation: None,
            name: ResourceName::new(
                "acme",
                "payments",
                "production",
                "core",
                ResourceKind::Stream,
                name,
            )
            .expect("test resource name should be valid"),
            spec: ResourceSpec {
                workload_profile: WorkloadProfile::StreamLog,
                shard_count: shards,
                replica_count: 3,
                configuration: None,
                governance: None,
            },
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_probe_runtimes_elect_and_commit_over_real_http() {
        let cluster = TestProbeCluster::start().await;
        let handles = cluster.handles();
        let (leader_index, _) = wait_for_leader(&handles).await;

        let proposal_id = 41;
        let payload = b"opaque-probe-payload".to_vec();
        let proposed = propose_through_current_leader(&handles, proposal_id, payload.clone()).await;
        assert!(matches!(
            proposed,
            ProposalLookup::Pending { .. } | ProposalLookup::Committed(_)
        ));

        let committed = wait_for_commit(&handles, proposal_id).await;
        for lookup in committed {
            let ProposalLookup::Committed(committed) = lookup else {
                unreachable!("the polling condition admits only committed proposals");
            };
            assert_eq!(committed.payload, payload);
        }

        let follower_index = (0..handles.len())
            .find(|index| *index != leader_index)
            .expect("cluster should contain a follower");
        let follower_before = handles[follower_index]
            .status()
            .await
            .expect("follower status should be available");
        let applied_growth = follower_before
            .applied_index
            .get()
            .saturating_sub(follower_before.checkpoint_index.get());
        assert!(applied_growth > 0);
        assert!(
            handles[follower_index]
                .checkpoint_if_applied_growth(applied_growth.saturating_add(1))
                .await
                .expect("a below-threshold follower checkpoint should be a no-op")
                .is_none()
        );
        let follower_checkpoint = handles[follower_index]
            .checkpoint_if_applied_growth(applied_growth)
            .await
            .expect("a follower should create its own due checkpoint")
            .expect("the observed applied growth should satisfy the threshold");
        assert_eq!(follower_checkpoint.index, follower_before.applied_index);
        assert!(
            handles[follower_index]
                .checkpoint_if_applied_growth(1)
                .await
                .expect("an unchanged follower checkpoint should be a no-op")
                .is_none()
        );

        let response = cluster.runtimes[leader_index]
            .experimental_router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(EXPERIMENTAL_CHECKPOINTS_PATH)
                    .body(Body::empty())
                    .expect("checkpoint request should build"),
            )
            .await
            .expect("checkpoint route should respond");
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("checkpoint response body should collect")
            .to_bytes();
        let checkpoint: Value =
            serde_json::from_slice(&body).expect("checkpoint response should be JSON");
        assert_eq!(checkpoint["durability"], "fsync_before_install");
        assert_eq!(
            checkpoint["compaction"],
            "logical_raft_prefix_and_physical_eprs"
        );
        assert!(checkpoint["proposal_count"].as_u64().unwrap() >= 1);
        assert!(checkpoint["encoded_bytes"].as_u64().unwrap() > 0);
        let checkpoint_index = checkpoint["index"].as_u64().unwrap();
        let status = handles[leader_index].status().await.unwrap();
        assert_eq!(status.checkpoint_index.get(), checkpoint_index);
        assert_eq!(
            status.retained_log_first_index.get(),
            checkpoint_index.saturating_add(1)
        );
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn exact_proposal_replay_resolves_an_unknown_outcome_without_duplicate_commit() {
        let cluster = TestProbeCluster::start().await;
        let handles = cluster.handles();
        let (leader_index, leader_status) = wait_for_leader(&handles).await;
        let proposal_id = 42;
        let payload = b"stable-destination-command".to_vec();

        let first = propose_and_wait(
            &handles[leader_index],
            proposal_id,
            leader_status.term.get(),
            payload.clone(),
            Duration::from_secs(2),
            "test destination",
            ProposalRoute::LeaderOnly,
        )
        .await
        .expect("the first destination command should commit");
        let _ = wait_for_commit(&handles, proposal_id).await;

        // Model a target commit whose result was lost before source settlement:
        // the next worker pass must resolve the exact commit instead of append.
        let replayed = propose_and_wait(
            &handles[leader_index],
            proposal_id,
            leader_status.term.get(),
            payload.clone(),
            Duration::from_secs(2),
            "test destination",
            ProposalRoute::LeaderOnly,
        )
        .await
        .expect("the unknown outcome should resolve through proposal lookup");
        assert_eq!(replayed, first);
        for handle in &handles {
            assert_eq!(
                handle
                    .applied_proposals()
                    .await
                    .expect("applied history should be available")
                    .into_iter()
                    .filter(|proposal| proposal.receipt.proposal_id.get() == proposal_id)
                    .count(),
                1
            );
        }

        let collision = propose_and_wait(
            &handles[leader_index],
            proposal_id,
            leader_status.term.get(),
            b"different-destination-command".to_vec(),
            Duration::from_secs(2),
            "test destination",
            ProposalRoute::LeaderOnly,
        )
        .await
        .expect_err("a committed proposal identity must reject another payload");
        assert!(collision.contains("already bound to another command"));

        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_barrier_requires_a_live_majority_over_real_http() {
        let mut cluster = TestProbeCluster::start().await;
        let handles = cluster.handles();
        let (leader_index, leader_status) = wait_for_leader(&handles).await;

        let completed = handles[leader_index]
            .read_barrier(leader_status.term.get(), Duration::from_secs(2))
            .await
            .expect("leader should complete a quorum read barrier");
        assert_eq!(completed.term, leader_status.term);
        assert!(completed.applied_index >= completed.read_index);

        let follower_index = (0..handles.len())
            .find(|index| *index != leader_index)
            .expect("cluster should contain a follower");
        let follower_status = handles[follower_index].status().await.unwrap();
        assert!(matches!(
            handles[follower_index]
                .read_barrier(follower_status.term.get(), Duration::from_millis(100))
                .await,
            Err(ConsensusProbeError::Consensus(
                ConsensusError::NotLeader { .. }
            ))
        ));

        for index in 0..handles.len() {
            if index != leader_index {
                cluster.stop_peer_listener(index).await;
            }
        }
        assert!(matches!(
            handles[leader_index]
                .read_barrier(leader_status.term.get(), Duration::from_millis(100))
                .await,
            Err(ConsensusProbeError::ReadBarrierTimeout { .. })
        ));

        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn catalog_converges_recovers_and_continues_over_three_real_voters() {
        let scope = CatalogTabletScope::new(77, 1).expect("catalog scope should be valid");
        let services = (0..3)
            .map(|_| CatalogTabletService::new(scope))
            .collect::<Vec<_>>();
        let appliers = services
            .iter()
            .map(|service| Some(Arc::clone(service) as Arc<dyn CommittedProposalApplier>))
            .collect();
        let mut cluster =
            TestProbeCluster::start_with_appliers(DEFAULT_OUTBOUND_QUEUE_CAPACITY, appliers).await;
        let handles = cluster.handles();
        let (leader_index, _) = wait_for_leader(&handles).await;

        for (proposal_id, command) in [
            (701, catalog_command("orders-v1", "orders", 2)),
            (702, catalog_command("audit-v1", "audit", 1)),
        ] {
            let proposed = propose_through_current_leader(
                &handles,
                proposal_id,
                command
                    .encode()
                    .expect("catalog command should encode canonically"),
            )
            .await;
            assert!(matches!(
                proposed,
                ProposalLookup::Pending { .. } | ProposalLookup::Committed(_)
            ));
            let _ = wait_for_commit(&handles, proposal_id).await;
        }

        let before_restart = services
            .iter()
            .map(|service| service.snapshot().expect("catalog should be healthy"))
            .collect::<Vec<_>>();
        assert!(
            before_restart.windows(2).all(|pair| pair[0] == pair[1]),
            "all voters should expose the same committed catalog"
        );
        assert_eq!(before_restart[0].resource_count, 2);
        assert_eq!(before_restart[0].tablet_count, 3);
        let checkpoint = handles[leader_index]
            .checkpoint()
            .await
            .expect("Catalog profile checkpoint should persist");
        assert_eq!(checkpoint.proposal_count, 2);

        cluster.stop().await;
        let recovered_services = (0..3)
            .map(|_| CatalogTabletService::new(scope))
            .collect::<Vec<_>>();
        let recovered_appliers = recovered_services
            .iter()
            .map(|service| Some(Arc::clone(service) as Arc<dyn CommittedProposalApplier>))
            .collect();
        cluster.restart_with_appliers(recovered_appliers).await;

        let after_restart = recovered_services
            .iter()
            .map(|service| {
                service
                    .snapshot()
                    .expect("replayed catalog should be healthy")
            })
            .collect::<Vec<_>>();
        assert_eq!(after_restart, before_restart);

        let recovered_handles = cluster.handles();
        let _ = wait_for_leader(&recovered_handles).await;
        let command = catalog_command("ledger-v1", "ledger", 2);
        let proposed = propose_through_current_leader(
            &recovered_handles,
            703,
            command
                .encode()
                .expect("catalog command should encode canonically"),
        )
        .await;
        assert!(matches!(
            proposed,
            ProposalLookup::Pending { .. } | ProposalLookup::Committed(_)
        ));
        let _ = wait_for_commit(&recovered_handles, 703).await;

        let continued = recovered_services
            .iter()
            .map(|service| service.snapshot().expect("catalog should remain healthy"))
            .collect::<Vec<_>>();
        assert!(continued.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(continued[0].resource_count, 3);
        assert_eq!(continued[0].tablet_count, 5);
        assert_eq!(continued[0].applied_command_count, 3);
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lagging_profile_voter_replays_a_checkpoint_before_applying_its_tail() {
        let scope = CatalogTabletScope::new(77, 1).expect("catalog scope should be valid");
        let services = (0..3)
            .map(|_| CatalogTabletService::new(scope))
            .collect::<Vec<_>>();
        let appliers = services
            .iter()
            .map(|service| Some(Arc::clone(service) as Arc<dyn CommittedProposalApplier>))
            .collect();
        let mut cluster =
            TestProbeCluster::start_with_appliers(DEFAULT_OUTBOUND_QUEUE_CAPACITY, appliers).await;
        let handles = cluster.handles();
        let (leader_index, _) = wait_for_leader(&handles).await;
        let lagging_index = (0..handles.len())
            .find(|index| *index != leader_index)
            .expect("cluster should contain a follower");
        let healthy_indexes = (0..handles.len())
            .filter(|index| *index != lagging_index)
            .collect::<Vec<_>>();
        cluster.stop_peer_listener(lagging_index).await;

        for (proposal_id, command) in [
            (801, catalog_command("snapshot-orders-v1", "orders", 2)),
            (802, catalog_command("snapshot-audit-v1", "audit", 1)),
        ] {
            let _ = propose_through_current_leader(
                &handles,
                proposal_id,
                command.encode().expect("catalog command should encode"),
            )
            .await;
            wait_for_commit_on(&handles, &healthy_indexes, proposal_id).await;
        }

        let checkpoint = handles[leader_index]
            .checkpoint()
            .await
            .expect("leader checkpoint should persist and compact");
        assert!(checkpoint.proposal_count >= 2);

        let _ = propose_through_current_leader(
            &handles,
            803,
            catalog_command("snapshot-ledger-v1", "ledger", 2)
                .encode()
                .expect("catalog tail command should encode"),
        )
        .await;
        wait_for_commit_on(&handles, &healthy_indexes, 803).await;

        cluster.start_peer_listener(lagging_index).await;
        let _ = wait_for_commit(&handles, 803).await;

        let snapshots = services
            .iter()
            .map(|service| service.snapshot().expect("catalog should remain healthy"))
            .collect::<Vec<_>>();
        assert!(snapshots.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(snapshots[0].resource_count, 3);
        assert_eq!(snapshots[0].tablet_count, 5);
        assert_eq!(snapshots[0].applied_command_count, 3);
        let lagging_status = handles[lagging_index].status().await.unwrap();
        assert!(lagging_status.applied_index > checkpoint.index);
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_profile_apply_failure_is_observable_by_the_runtime() {
        let appliers = (0..3)
            .map(|_| Some(Arc::new(RejectingProfileApplier) as Arc<dyn CommittedProposalApplier>))
            .collect();
        let cluster =
            TestProbeCluster::start_with_appliers(DEFAULT_OUTBOUND_QUEUE_CAPACITY, appliers).await;
        let handles = cluster.handles();
        let (leader_index, proposed) = tokio::time::timeout(Duration::from_secs(5), async {
            'proposal: loop {
                for (index, handle) in handles.iter().enumerate() {
                    let status = handle.status().await.expect("status should be available");
                    if status.role != ConsensusRole::Leader {
                        continue;
                    }
                    match handle
                        .propose(51, status.term.get(), b"rejected-profile-command".to_vec())
                        .await
                    {
                        Err(ConsensusProbeError::Consensus(
                            ConsensusError::NotLeader { .. } | ConsensusError::StaleTerm { .. },
                        )) => {}
                        result => break 'proposal (index, result),
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a stable leader should reach profile application");
        assert!(
            matches!(
                proposed,
                Ok(ProposalLookup::Pending { .. } | ProposalLookup::Committed(_))
                    | Err(ConsensusProbeError::ProfileApplication(_))
            ),
            "the proposal should reach profile application: {proposed:?}"
        );

        let failure = tokio::time::timeout(
            Duration::from_secs(5),
            cluster.runtimes[leader_index].wait_for_actor_failure(),
        )
        .await
        .expect("actor failure should be published");
        assert!(matches!(
            failure,
            ConsensusProbeError::ProfileApplication(ref message)
                if message == "injected profile apply failure"
        ));
        assert!(matches!(
            handles[leader_index].status().await,
            Err(ConsensusProbeError::ActorUnavailable)
        ));

        cluster.shutdown_after_actor_failure().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sustained_minority_outage_drops_only_that_peers_frames_and_majority_commits() {
        let mut cluster = TestProbeCluster::start_with_outbound_queue_capacity(1).await;
        let handles = cluster.handles();
        let (leader_index, _) = wait_for_leader(&handles).await;
        let mut follower_indexes = (0..handles.len())
            .filter(|index| *index != leader_index)
            .collect::<Vec<_>>();
        follower_indexes.sort_unstable();
        // The lower-ID destination is deliberately failed. Raft emits peer
        // messages in destination order, so a blocking send here used to stop
        // the actor before it could dispatch to the healthy follower.
        let failed_index = follower_indexes[0];
        let healthy_index = follower_indexes[1];
        let failed_peer_id = handles[failed_index].node_id();
        let baseline = peer_transport_status(&handles[leader_index], failed_peer_id);

        cluster
            .replace_peer_listener_with_blackhole(failed_index)
            .await;
        // Generate a deterministic burst while the failed peer's worker is
        // retrying its first frame. Queue saturation must not depend on the
        // heartbeat frequency chosen by the surrounding election tests.
        for proposal_id in 90..94 {
            let proposed = propose_through_current_leader(
                &handles,
                proposal_id,
                format!("minority-outage-burst-{proposal_id}").into_bytes(),
            )
            .await;
            assert!(matches!(
                proposed,
                ProposalLookup::Pending { .. } | ProposalLookup::Committed(_)
            ));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = peer_transport_status(&handles[leader_index], failed_peer_id);
                if status.dropped_queue_full_frames > baseline.dropped_queue_full_frames {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("failed peer queue should saturate under a sustained outage");

        // Close the blackhole so the in-flight request fails promptly, then
        // prove the bounded retry policy exhausts while the peer stays down.
        cluster.stop_peer_listener(failed_index).await;
        let saturated = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = peer_transport_status(&handles[leader_index], failed_peer_id);
                if status.exhausted_retry_frames > baseline.exhausted_retry_frames {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("failed peer retries should exhaust without blocking the actor");
        assert_eq!(
            saturated.observed_condition,
            ConsensusProbeTransportCondition::Degraded
        );

        let responsive_status =
            tokio::time::timeout(Duration::from_millis(250), handles[leader_index].status())
                .await
                .expect("a saturated minority queue must not block the Raft actor")
                .expect("original leader status should remain available");
        assert!(!responsive_status.fail_stopped);

        let majority = vec![
            handles[leader_index].clone(),
            handles[healthy_index].clone(),
        ];
        let delivered_before_workload = majority
            .iter()
            .map(|handle| peer_transport_status(handle, failed_peer_id).delivered_frames)
            .sum::<u64>();
        tokio::time::timeout(Duration::from_secs(5), async {
            for proposal_id in 100..108 {
                let proposed = propose_through_current_leader(
                    &majority,
                    proposal_id,
                    format!("minority-outage-{proposal_id}").into_bytes(),
                )
                .await;
                assert!(matches!(
                    proposed,
                    ProposalLookup::Pending { .. } | ProposalLookup::Committed(_)
                ));
                let _ = wait_for_commit(&majority, proposal_id).await;
            }
        })
        .await
        .expect("healthy majority should keep committing while one peer stays unavailable");

        let delivered_after_workload = majority
            .iter()
            .map(|handle| peer_transport_status(handle, failed_peer_id).delivered_frames)
            .sum::<u64>();
        assert_eq!(
            delivered_after_workload, delivered_before_workload,
            "the unavailable minority must receive no frames during the commit workload"
        );
        cluster.shutdown().await;
    }

    fn peer_transport_status(
        handle: &ConsensusProbeHandle,
        peer_id: NodeId,
    ) -> ConsensusProbePeerTransportStatus {
        handle
            .outbound_transport_status()
            .into_iter()
            .find(|status| status.peer_id == peer_id.get())
            .expect("peer transport status should be present")
    }

    #[test]
    fn truthful_status_conversion_is_closed_over_all_consensus_roles() {
        let node_id = NodeId::new(1).expect("valid node");
        for role in [
            ConsensusRole::Follower,
            ConsensusRole::PreCandidate,
            ConsensusRole::Candidate,
            ConsensusRole::Leader,
        ] {
            let status = ConsensusProbeStatus::from(ConsensusStatus {
                node_id,
                group_id: GroupId::new(7).expect("valid group"),
                group_epoch: GroupEpoch::new(3).expect("valid epoch"),
                role,
                leader_id: None,
                term: Term::new(4),
                commit_index: LogIndex::new(5),
                applied_index: LogIndex::new(5),
                checkpoint_index: LogIndex::new(3),
                retained_log_first_index: LogIndex::new(4),
                voter_count: 3,
                fail_stopped: false,
            });
            assert!(!status.profile_replication);
            assert_eq!(status.profile_guarantee_ceiling, "local_durable");
            assert_eq!(status.observation_scope, "local");
            assert_eq!(status.stability, "experimental");
            assert_eq!(status.production_readiness, "not_production_ready");
            assert_eq!(status.checkpoint_index, 3);
            assert_eq!(status.retained_log_first_index, 4);
        }
    }
}
