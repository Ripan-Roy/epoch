use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fmt::{self, Display, Formatter},
    fs::{self, File},
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use epoch_consensus::{
    CommittedProposal, ConsensusAdapter, ConsensusError, ConsensusOutput, ConsensusRole,
    ConsensusStatus, GroupEpoch, GroupId, NodeId, PeerMessage, PersistentRaftAdapter, Proposal,
    ProposalId, ProposalLookup, Term,
};
use epoch_testkit::{PeerId, PeerTransport, TransportError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tempfile::{Builder as TempDirBuilder, TempDir};

const CHILD_MARKER: &str = "EPOCH_CONSENSUS_PROCESS_CHILD";
const CHILD_NODE_ID: &str = "EPOCH_CONSENSUS_PROCESS_NODE_ID";
const CHILD_STABLE_PATH: &str = "EPOCH_CONSENSUS_PROCESS_STABLE_PATH";
const CHILD_READY_PATH: &str = "EPOCH_CONSENSUS_PROCESS_READY_PATH";
const ARTIFACT_DIRECTORY: &str = "EPOCH_CONSENSUS_PROCESS_ARTIFACT_DIR";
const CHILD_TEST_NAME: &str = "process_fixture_child";
const CONTROL_ADDRESS: &str = "127.0.0.1:0";
const GROUP_ID: u64 = 1;
const GROUP_EPOCH: u64 = 1;
const NODE_IDS: [u64; 3] = [1, 2, 3];
const MAX_CONTROL_FRAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_DELIVERIES: usize = 20_000;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const SOCKET_TIMEOUT: Duration = Duration::from_secs(10);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub type HarnessResult<T> = Result<T, HarnessError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessError {
    message: String,
}

impl HarnessError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn with_context(context: impl Display, error: impl Display) -> Self {
        Self::new(format!("{context}: {error}"))
    }
}

impl Display for HarnessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for HarnessError {}

impl From<io::Error> for HarnessError {
    fn from(error: io::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<serde_json::Error> for HarnessError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<ConsensusError> for HarnessError {
    fn from(error: ConsensusError) -> Self {
        Self::new(error.to_string())
    }
}

impl From<TransportError> for HarnessError {
    fn from(error: TransportError) -> Self {
        Self::new(error.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

impl From<ConsensusRole> for Role {
    fn from(role: ConsensusRole) -> Self {
        match role {
            ConsensusRole::Follower => Self::Follower,
            ConsensusRole::PreCandidate => Self::PreCandidate,
            ConsensusRole::Candidate => Self::Candidate,
            ConsensusRole::Leader => Self::Leader,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub node_id: u64,
    pub role: Role,
    pub leader_id: Option<u64>,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub fail_stopped: bool,
}

impl From<ConsensusStatus> for Status {
    fn from(status: ConsensusStatus) -> Self {
        Self {
            node_id: status.node_id.get(),
            role: status.role.into(),
            leader_id: status.leader_id.map(NodeId::get),
            term: status.term.get(),
            commit_index: status.commit_index.get(),
            applied_index: status.applied_index.get(),
            fail_stopped: status.fail_stopped,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Committed {
    pub group_id: u64,
    pub group_epoch: u64,
    pub proposal_id: u64,
    pub term: u64,
    pub log_index: u64,
    pub payload: Vec<u8>,
}

impl From<CommittedProposal> for Committed {
    fn from(committed: CommittedProposal) -> Self {
        Self {
            group_id: committed.receipt.group_id.get(),
            group_epoch: committed.receipt.group_epoch.get(),
            proposal_id: committed.receipt.proposal_id.get(),
            term: committed.receipt.term.get(),
            log_index: committed.receipt.log_index.get(),
            payload: committed.payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", content = "committed", rename_all = "snake_case")]
pub enum Lookup {
    Unknown,
    Pending,
    Committed(Committed),
}

impl From<ProposalLookup> for Lookup {
    fn from(lookup: ProposalLookup) -> Self {
        match lookup {
            ProposalLookup::Unknown => Self::Unknown,
            ProposalLookup::Pending => Self::Pending,
            ProposalLookup::Committed(committed) => Self::Committed(committed.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recovery {
    pub stable_generation: u64,
    pub applied_index: u64,
    pub repaired_partial_tail: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitEvent {
    pub node_id: u64,
    pub committed: Committed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Ready {
    node_id: u64,
    process_id: u32,
    control_address: SocketAddr,
    recovery: Recovery,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WireMessage {
    from: u64,
    to: u64,
    frame: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DrainedOutput {
    messages: Vec<WireMessage>,
    commits: Vec<Committed>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum Request {
    Campaign,
    Tick,
    Propose {
        proposal_id: u64,
        expected_term: u64,
        payload: Vec<u8>,
    },
    Receive {
        frame: Vec<u8>,
    },
    Drain,
    Status,
    Lookup {
        proposal_id: u64,
    },
    Digest,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "response", content = "value", rename_all = "snake_case")]
enum Response {
    Ok,
    Drained(DrainedOutput),
    Status(Status),
    Lookup(Lookup),
    Digest([u8; 32]),
    Error(String),
}

#[derive(Debug)]
struct FixtureNode {
    node_id: NodeId,
    adapter: PersistentRaftAdapter,
    messages: Vec<WireMessage>,
    commits: Vec<Committed>,
}

impl FixtureNode {
    fn open(node_id: NodeId, path: &Path) -> HarnessResult<(Self, Recovery)> {
        let opened =
            PersistentRaftAdapter::open(path, node_id, group_id()?, group_epoch()?, voters()?)?;
        let recovered = opened.adapter.recovery();
        let recovery = Recovery {
            stable_generation: recovered.stable_generation,
            applied_index: recovered.applied_index.get(),
            repaired_partial_tail: recovered.repaired_partial_tail,
        };
        let mut node = Self {
            node_id,
            adapter: opened.adapter,
            messages: Vec::new(),
            commits: Vec::new(),
        };
        node.capture(opened.output)?;
        Ok((node, recovery))
    }

    fn handle(&mut self, request: Request) -> HarnessResult<(Response, bool)> {
        let mut should_shutdown = false;
        let response = match request {
            Request::Campaign => {
                let output = self.adapter.campaign()?;
                self.capture(output)?;
                Response::Ok
            }
            Request::Tick => {
                let output = self.adapter.tick()?;
                self.capture(output)?;
                Response::Ok
            }
            Request::Propose {
                proposal_id,
                expected_term,
                payload,
            } => {
                let proposal = Proposal::new(
                    group_id()?,
                    group_epoch()?,
                    Term::new(expected_term),
                    ProposalId::new(proposal_id)?,
                    payload,
                );
                let output = self.adapter.propose(proposal)?;
                self.capture(output)?;
                Response::Ok
            }
            Request::Receive { frame } => {
                let message = PeerMessage::from_wire(&frame, self.node_id)?;
                let output = self.adapter.receive(message)?;
                self.capture(output)?;
                Response::Ok
            }
            Request::Drain => Response::Drained(DrainedOutput {
                messages: std::mem::take(&mut self.messages),
                commits: std::mem::take(&mut self.commits),
            }),
            Request::Status => Response::Status(self.adapter.status().into()),
            Request::Lookup { proposal_id } => Response::Lookup(
                self.adapter
                    .lookup_proposal(ProposalId::new(proposal_id)?)
                    .into(),
            ),
            Request::Digest => Response::Digest(self.adapter.state_digest()),
            Request::Shutdown => {
                should_shutdown = true;
                Response::Ok
            }
        };
        Ok((response, should_shutdown))
    }

    fn capture(&mut self, output: ConsensusOutput) -> HarnessResult<()> {
        if output.status.node_id != self.node_id {
            return Err(HarnessError::new(format!(
                "node {} produced status for node {}",
                self.node_id, output.status.node_id
            )));
        }
        for message in output.messages {
            if message.from() != self.node_id {
                return Err(HarnessError::new(format!(
                    "node {} emitted a peer frame from node {}",
                    self.node_id,
                    message.from()
                )));
            }
            self.messages.push(WireMessage {
                from: message.from().get(),
                to: message.to().get(),
                frame: message.to_wire()?,
            });
        }
        self.commits
            .extend(output.commits.into_iter().map(Committed::from));
        Ok(())
    }
}

pub fn is_process_fixture_child() -> bool {
    env::var_os(CHILD_MARKER).is_some()
}

pub fn run_process_fixture_child() -> HarnessResult<()> {
    let node_id = NodeId::new(required_environment_u64(CHILD_NODE_ID)?)?;
    let stable_path = PathBuf::from(required_environment(CHILD_STABLE_PATH)?);
    let ready_path = PathBuf::from(required_environment(CHILD_READY_PATH)?);
    let listener = TcpListener::bind(CONTROL_ADDRESS)?;
    let control_address = listener.local_addr()?;
    let (mut node, recovery) = FixtureNode::open(node_id, &stable_path)?;
    let ready = Ready {
        node_id: node_id.get(),
        process_id: std::process::id(),
        control_address,
        recovery,
    };
    publish_ready_file(&ready_path, &ready)?;

    let (mut stream, _) = listener.accept()?;
    configure_stream(&stream)?;
    loop {
        let request = read_frame::<Request>(&mut stream)?;
        let (response, should_shutdown) = match node.handle(request) {
            Ok(result) => result,
            Err(error) => (Response::Error(error.to_string()), false),
        };
        write_frame(&mut stream, &response)?;
        if should_shutdown {
            return Ok(());
        }
    }
}

#[derive(Debug)]
struct NodeProcess {
    node_id: u64,
    restart: u64,
    child: Option<Child>,
    stream: Option<TcpStream>,
    ready: Ready,
    log_path: PathBuf,
}

impl NodeProcess {
    fn request(&mut self, request: &Request) -> HarnessResult<Response> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            HarnessError::new(format!("node {} has no control connection", self.node_id))
        })?;
        write_frame(stream, request).map_err(|error| {
            HarnessError::with_context(
                format!("write control request to node {}", self.node_id),
                error,
            )
        })?;
        let response = read_frame(stream).map_err(|error| {
            HarnessError::with_context(
                format!("read control response from node {}", self.node_id),
                error,
            )
        })?;
        match response {
            Response::Error(message) => Err(HarnessError::new(format!(
                "node {} rejected control request: {message}",
                self.node_id
            ))),
            response => Ok(response),
        }
    }

    fn crash(&mut self) -> HarnessResult<ExitStatus> {
        let child = self.child.as_mut().ok_or_else(|| {
            HarnessError::new(format!("node {} process is not running", self.node_id))
        })?;
        if let Some(status) = child.try_wait()? {
            return Err(HarnessError::new(format!(
                "node {} exited before SIGKILL with {status}; log: {}",
                self.node_id,
                self.log_path.display()
            )));
        }
        child.kill()?;
        let status = child.wait()?;
        assert_sigkill(self.node_id, status)?;
        self.child = None;
        if let Some(stream) = self.stream.take() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        Ok(status)
    }

    fn kill_for_cleanup(&mut self) {
        if let Some(mut child) = self.child.take() {
            let already_exited = matches!(child.try_wait(), Ok(Some(_)));
            if !already_exited {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if let Some(stream) = self.stream.take() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        self.kill_for_cleanup();
    }
}

#[derive(Debug)]
pub struct ProcessCluster {
    seed: u64,
    root: Option<TempDir>,
    nodes: BTreeMap<u64, NodeProcess>,
    restart_counts: BTreeMap<u64, u64>,
    transport: PeerTransport,
    commit_events: Vec<CommitEvent>,
    completed: bool,
}

impl ProcessCluster {
    pub fn start(seed: u64) -> HarnessResult<Self> {
        let root = create_test_root()?;
        fs::create_dir_all(root.path().join("nodes"))?;
        fs::create_dir_all(root.path().join("logs"))?;
        fs::create_dir_all(root.path().join("run"))?;
        let mut cluster = Self {
            seed,
            root: Some(root),
            nodes: BTreeMap::new(),
            restart_counts: BTreeMap::new(),
            transport: PeerTransport::new(seed),
            commit_events: Vec::new(),
            completed: false,
        };
        for node_id in NODE_IDS {
            cluster.spawn_node(node_id)?;
        }
        cluster.capture_all()?;
        cluster.drive_until_idle()?;
        Ok(cluster)
    }

    pub fn campaign(&mut self, node_id: u64) -> HarnessResult<()> {
        self.expect_ok(node_id, &Request::Campaign)?;
        self.capture_node(node_id)?;
        self.drive_until_idle()
    }

    pub fn tick(&mut self, node_id: u64) -> HarnessResult<()> {
        self.expect_ok(node_id, &Request::Tick)?;
        self.capture_node(node_id)?;
        self.drive_until_idle()
    }

    pub fn propose(&mut self, node_id: u64, proposal_id: u64, payload: &[u8]) -> HarnessResult<()> {
        let expected_term = self.status(node_id)?.term;
        self.expect_ok(
            node_id,
            &Request::Propose {
                proposal_id,
                expected_term,
                payload: payload.to_vec(),
            },
        )?;
        self.capture_node(node_id)?;
        self.drive_until_idle()
    }

    pub fn status(&mut self, node_id: u64) -> HarnessResult<Status> {
        match self.request(node_id, &Request::Status)? {
            Response::Status(status) => Ok(status),
            response => Err(unexpected_response(node_id, "status", &response)),
        }
    }

    pub fn lookup(&mut self, node_id: u64, proposal_id: u64) -> HarnessResult<Lookup> {
        match self.request(node_id, &Request::Lookup { proposal_id })? {
            Response::Lookup(lookup) => Ok(lookup),
            response => Err(unexpected_response(node_id, "lookup", &response)),
        }
    }

    pub fn digest(&mut self, node_id: u64) -> HarnessResult<[u8; 32]> {
        match self.request(node_id, &Request::Digest)? {
            Response::Digest(digest) => Ok(digest),
            response => Err(unexpected_response(node_id, "digest", &response)),
        }
    }

    pub fn partition(&mut self, left: &[u64], right: &[u64]) -> HarnessResult<()> {
        self.drive_until_idle()?;
        if self.transport.pending_len() != 0 {
            return Err(HarnessError::new(
                "cannot partition while peer deliveries are queued",
            ));
        }
        let left = left.iter().copied().map(PeerId::new).collect::<Vec<_>>();
        let right = right.iter().copied().map(PeerId::new).collect::<Vec<_>>();
        self.transport.partition(&left, &right)?;
        Ok(())
    }

    pub fn heal_all(&mut self) -> HarnessResult<()> {
        self.transport.heal_all()?;
        Ok(())
    }

    pub fn commit_events(&self) -> &[CommitEvent] {
        &self.commit_events
    }

    pub fn finish(mut self) {
        self.completed = true;
    }

    pub fn crash_and_reopen(&mut self, node_id: u64) -> HarnessResult<Recovery> {
        self.drive_until_idle()?;
        let mut process = self
            .nodes
            .remove(&node_id)
            .ok_or_else(|| HarnessError::new(format!("cannot crash unknown node {node_id}")))?;
        process.crash()?;
        drop(process);
        self.spawn_node(node_id)?;
        let recovery = self.nodes[&node_id].ready.recovery;
        self.capture_node(node_id)?;
        self.drive_until_idle()?;
        Ok(recovery)
    }

    pub fn crash_all_and_reopen(&mut self) -> HarnessResult<()> {
        self.drive_until_idle()?;
        let processes = std::mem::take(&mut self.nodes);
        for (_, mut process) in processes {
            process.crash()?;
        }
        for node_id in NODE_IDS {
            self.spawn_node(node_id)?;
        }
        self.capture_all()?;
        self.drive_until_idle()
    }

    fn root_path(&self) -> HarnessResult<&Path> {
        self.root
            .as_ref()
            .map(TempDir::path)
            .ok_or_else(|| HarnessError::new("test root has already been released"))
    }

    fn spawn_node(&mut self, node_id: u64) -> HarnessResult<()> {
        validate_node_id(node_id)?;
        if self.nodes.contains_key(&node_id) {
            return Err(HarnessError::new(format!(
                "node {node_id} is already running"
            )));
        }
        let restart_count = self.restart_counts.entry(node_id).or_default();
        let restart = *restart_count;
        *restart_count += 1;
        let root = self.root_path()?.to_path_buf();
        let process = spawn_node_process(&root, node_id, restart)?;
        if self.nodes.insert(node_id, process).is_some() {
            return Err(HarnessError::new(format!(
                "node {node_id} was inserted twice"
            )));
        }
        Ok(())
    }

    fn request(&mut self, node_id: u64, request: &Request) -> HarnessResult<Response> {
        self.nodes
            .get_mut(&node_id)
            .ok_or_else(|| HarnessError::new(format!("node {node_id} is not running")))?
            .request(request)
    }

    fn expect_ok(&mut self, node_id: u64, request: &Request) -> HarnessResult<()> {
        match self.request(node_id, request)? {
            Response::Ok => Ok(()),
            response => Err(unexpected_response(node_id, "ok", &response)),
        }
    }

    fn capture_all(&mut self) -> HarnessResult<()> {
        let node_ids = self.nodes.keys().copied().collect::<Vec<_>>();
        for node_id in node_ids {
            self.capture_node(node_id)?;
        }
        Ok(())
    }

    fn capture_node(&mut self, node_id: u64) -> HarnessResult<()> {
        let drained = match self.request(node_id, &Request::Drain)? {
            Response::Drained(drained) => drained,
            response => return Err(unexpected_response(node_id, "drained output", &response)),
        };
        for message in drained.messages {
            if message.from != node_id {
                return Err(HarnessError::new(format!(
                    "node {node_id} drained a frame from node {}",
                    message.from
                )));
            }
            validate_node_id(message.to)?;
            self.transport.send(
                PeerId::new(message.from),
                PeerId::new(message.to),
                message.frame,
            )?;
        }
        self.commit_events.extend(
            drained
                .commits
                .into_iter()
                .map(|committed| CommitEvent { node_id, committed }),
        );
        Ok(())
    }

    fn drive_until_idle(&mut self) -> HarnessResult<()> {
        self.capture_all()?;
        for _ in 0..MAX_DELIVERIES {
            let Some(delivery) = self.transport.deliver_next()? else {
                return Ok(());
            };
            let target = delivery.to.value();
            if !self.nodes.contains_key(&target) {
                continue;
            }
            self.expect_ok(
                target,
                &Request::Receive {
                    frame: delivery.payload,
                },
            )?;
            self.capture_node(target)?;
        }
        Err(HarnessError::new(format!(
            "peer transport did not quiesce after {MAX_DELIVERIES} deliveries"
        )))
    }

    fn persist_diagnostics(&self) {
        let Ok(root) = self.root_path() else {
            return;
        };
        if let Ok(trace) = self.transport.trace().to_bytes() {
            let _ = fs::write(root.join("transport.eptr"), trace);
        }
        let _ = fs::write(root.join("seed.txt"), format!("{}\n", self.seed));
    }
}

impl Drop for ProcessCluster {
    fn drop(&mut self) {
        for process in self.nodes.values_mut() {
            process.kill_for_cleanup();
        }
        self.persist_diagnostics();
        if (!self.completed || thread::panicking())
            && let Some(root) = self.root.take()
        {
            let path = root.keep();
            eprintln!(
                "preserved consensus process artifacts at {}",
                path.display()
            );
        }
    }
}

fn spawn_node_process(root: &Path, node_id: u64, restart: u64) -> HarnessResult<NodeProcess> {
    let node_directory = root.join("nodes").join(format!("node-{node_id}"));
    fs::create_dir_all(&node_directory)?;
    let stable_path = node_directory.join("consensus.eprs");
    let ready_path = root
        .join("run")
        .join(format!("node-{node_id}-restart-{restart}.json"));
    let log_path = root
        .join("logs")
        .join(format!("node-{node_id}-restart-{restart}.log"));
    let log = File::create(&log_path)?;
    let executable = env::current_exe()?;
    let child = Command::new(executable)
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CHILD_MARKER, "1")
        .env(CHILD_NODE_ID, node_id.to_string())
        .env(CHILD_STABLE_PATH, &stable_path)
        .env(CHILD_READY_PATH, &ready_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()?;
    let placeholder_ready = Ready {
        node_id,
        process_id: child.id(),
        control_address: "127.0.0.1:1"
            .parse()
            .map_err(|error| HarnessError::with_context("parse placeholder address", error))?,
        recovery: Recovery {
            stable_generation: 0,
            applied_index: 0,
            repaired_partial_tail: false,
        },
    };
    let mut process = NodeProcess {
        node_id,
        restart,
        child: Some(child),
        stream: None,
        ready: placeholder_ready,
        log_path,
    };
    let ready = wait_for_ready(&mut process, &ready_path)?;
    if ready.node_id != node_id {
        return Err(HarnessError::new(format!(
            "node {node_id} published readiness for node {}",
            ready.node_id
        )));
    }
    if ready.process_id != process.child.as_ref().unwrap().id() {
        return Err(HarnessError::new(format!(
            "node {node_id} readiness PID {} did not match child PID {}",
            ready.process_id,
            process.child.as_ref().unwrap().id()
        )));
    }
    let stream = TcpStream::connect_timeout(&ready.control_address, SOCKET_TIMEOUT)?;
    configure_stream(&stream)?;
    process.ready = ready;
    process.stream = Some(stream);
    Ok(process)
}

fn wait_for_ready(process: &mut NodeProcess, ready_path: &Path) -> HarnessResult<Ready> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        match fs::read(ready_path) {
            Ok(encoded) => return Ok(serde_json::from_slice(&encoded)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if let Some(status) = process
            .child
            .as_mut()
            .ok_or_else(|| HarnessError::new("child disappeared during startup"))?
            .try_wait()?
        {
            return Err(HarnessError::new(format!(
                "node {} restart {} exited during startup with {status}; log: {}",
                process.node_id,
                process.restart,
                process.log_path.display()
            )));
        }
        if Instant::now() >= deadline {
            return Err(HarnessError::new(format!(
                "node {} restart {} did not become ready within {STARTUP_TIMEOUT:?}; log: {}",
                process.node_id,
                process.restart,
                process.log_path.display()
            )));
        }
        thread::sleep(READY_POLL_INTERVAL);
    }
}

fn configure_stream(stream: &TcpStream) -> HarnessResult<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(SOCKET_TIMEOUT))?;
    stream.set_write_timeout(Some(SOCKET_TIMEOUT))?;
    Ok(())
}

fn write_frame<T: Serialize>(writer: &mut impl Write, value: &T) -> HarnessResult<()> {
    let encoded = serde_json::to_vec(value)?;
    if encoded.is_empty() || encoded.len() > MAX_CONTROL_FRAME_BYTES {
        return Err(HarnessError::new(format!(
            "control frame length {} is outside 1..={MAX_CONTROL_FRAME_BYTES}",
            encoded.len()
        )));
    }
    let length = u32::try_from(encoded.len())
        .map_err(|_| HarnessError::new("control frame length does not fit in u32"))?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(&encoded)?;
    writer.flush()?;
    Ok(())
}

fn read_frame<T: DeserializeOwned>(reader: &mut impl Read) -> HarnessResult<T> {
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length)?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|_| HarnessError::new("control frame length does not fit in usize"))?;
    if length == 0 || length > MAX_CONTROL_FRAME_BYTES {
        return Err(HarnessError::new(format!(
            "control frame length {length} is outside 1..={MAX_CONTROL_FRAME_BYTES}"
        )));
    }
    let mut encoded = vec![0_u8; length];
    reader.read_exact(&mut encoded)?;
    Ok(serde_json::from_slice(&encoded)?)
}

fn publish_ready_file(path: &Path, ready: &Ready) -> HarnessResult<()> {
    let encoded = serde_json::to_vec(ready)?;
    let temporary = path.with_extension(format!("tmp-{}", ready.process_id));
    fs::write(&temporary, encoded)?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn create_test_root() -> HarnessResult<TempDir> {
    match env::var_os(ARTIFACT_DIRECTORY) {
        Some(directory) => {
            let directory = PathBuf::from(directory);
            fs::create_dir_all(&directory)?;
            TempDirBuilder::new()
                .prefix("epoch-consensus-process-")
                .tempdir_in(directory)
                .map_err(Into::into)
        }
        None => TempDirBuilder::new()
            .prefix("epoch-consensus-process-")
            .tempdir()
            .map_err(Into::into),
    }
}

fn required_environment(name: &str) -> HarnessResult<String> {
    env::var(name).map_err(|error| {
        HarnessError::with_context(format!("fixture environment variable {name}"), error)
    })
}

fn required_environment_u64(name: &str) -> HarnessResult<u64> {
    let value = required_environment(name)?;
    value.parse::<u64>().map_err(|error| {
        HarnessError::with_context(format!("fixture environment variable {name}"), error)
    })
}

fn group_id() -> HarnessResult<GroupId> {
    GroupId::new(GROUP_ID).map_err(Into::into)
}

fn group_epoch() -> HarnessResult<GroupEpoch> {
    GroupEpoch::new(GROUP_EPOCH).map_err(Into::into)
}

fn voters() -> HarnessResult<[NodeId; 3]> {
    Ok([
        NodeId::new(NODE_IDS[0])?,
        NodeId::new(NODE_IDS[1])?,
        NodeId::new(NODE_IDS[2])?,
    ])
}

fn validate_node_id(node_id: u64) -> HarnessResult<()> {
    if NODE_IDS.contains(&node_id) {
        Ok(())
    } else {
        Err(HarnessError::new(format!(
            "node {node_id} is outside the fixed voter set"
        )))
    }
}

fn unexpected_response(node_id: u64, expected: &str, response: &Response) -> HarnessError {
    HarnessError::new(format!(
        "node {node_id} returned {response:?}; expected {expected} response"
    ))
}

#[cfg(unix)]
fn assert_sigkill(node_id: u64, status: ExitStatus) -> HarnessResult<()> {
    use std::os::unix::process::ExitStatusExt as _;

    if status.signal() == Some(9) {
        Ok(())
    } else {
        Err(HarnessError::new(format!(
            "node {node_id} was not terminated by SIGKILL: {status}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn control_frames_are_length_prefixed_json() {
        let mut encoded = Vec::new();
        write_frame(&mut encoded, &Request::Tick).unwrap();

        let declared_length = u32::from_be_bytes(encoded[..4].try_into().unwrap()) as usize;
        assert_eq!(declared_length, encoded.len() - 4);
        assert_eq!(
            read_frame::<Request>(&mut Cursor::new(encoded)).unwrap(),
            Request::Tick
        );
    }

    #[test]
    fn control_frames_reject_zero_and_oversized_lengths_before_reading_a_body() {
        let zero = 0_u32.to_be_bytes();
        let error = read_frame::<Request>(&mut Cursor::new(zero)).unwrap_err();
        assert!(error.to_string().contains("outside"));

        let oversized = u32::try_from(MAX_CONTROL_FRAME_BYTES + 1)
            .unwrap()
            .to_be_bytes();
        let error = read_frame::<Request>(&mut Cursor::new(oversized)).unwrap_err();
        assert!(error.to_string().contains("outside"));
    }
}

#[cfg(not(unix))]
fn assert_sigkill(node_id: u64, status: ExitStatus) -> HarnessResult<()> {
    if status.success() {
        Err(HarnessError::new(format!(
            "node {node_id} exited successfully instead of being forcibly terminated"
        )))
    } else {
        Ok(())
    }
}
