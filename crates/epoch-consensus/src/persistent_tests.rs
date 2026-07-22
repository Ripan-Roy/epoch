use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write as _,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use epoch_testkit::{PeerId, PeerTransport};
use raft::prelude::{Entry, EntryType, HardState};

use super::{
    CommittedProposal, ConsensusAdapter, ConsensusError, ConsensusOutput, ConsensusRole,
    GroupEpoch, GroupId, LogIndex, NodeId, PeerMessage, PersistentRaftAdapter, ProcessingTrace,
    Proposal, ProposalId, ProposalLookup, StateDigest, Term, encode_command,
    stable::{DiskStableStore, StableCheckpoint, StableIdentity},
};

const MAX_DELIVERIES: usize = 20_000;
const TEST_SEED: u64 = 0x4550_4f43_485f_5053;
static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct PersistentCluster {
    nodes: BTreeMap<NodeId, PersistentRaftAdapter>,
    paths: BTreeMap<NodeId, PathBuf>,
    transport: PeerTransport,
    commits: Vec<(NodeId, CommittedProposal)>,
    root: PathBuf,
}

impl PersistentCluster {
    fn new(seed: u64) -> Self {
        let root = test_directory("cluster");
        let paths = voters()
            .into_iter()
            .map(|node_id| (node_id, root.join(format!("node-{}.wal", node_id.get()))))
            .collect::<BTreeMap<_, _>>();
        let mut cluster = Self {
            nodes: BTreeMap::new(),
            paths,
            transport: PeerTransport::new(seed),
            commits: Vec::new(),
            root,
        };
        cluster.reopen_all();
        cluster
    }

    fn campaign(&mut self, node_id: NodeId) {
        let output = self.nodes.get_mut(&node_id).unwrap().campaign().unwrap();
        self.capture(node_id, output);
        self.drain();
    }

    fn propose(&mut self, node_id: NodeId, proposal_id: u64, payload: &[u8]) {
        let proposal = Proposal::new(
            group_id(),
            group_epoch(),
            self.nodes[&node_id].status().term,
            proposal(proposal_id),
            payload,
        );
        let output = self
            .nodes
            .get_mut(&node_id)
            .unwrap()
            .propose(proposal)
            .unwrap();
        self.capture(node_id, output);
        self.drain();
    }

    fn tick(&mut self, node_id: NodeId) {
        let output = self.nodes.get_mut(&node_id).unwrap().tick().unwrap();
        self.capture(node_id, output);
        self.drain();
    }

    fn tick_repeatedly(&mut self, node_id: NodeId, count: usize) {
        for _ in 0..count {
            self.tick(node_id);
        }
    }

    fn isolate(&mut self, isolated: NodeId) {
        let others = voters()
            .into_iter()
            .filter(|candidate| *candidate != isolated)
            .map(peer)
            .collect::<Vec<_>>();
        self.transport
            .partition(&[peer(isolated)], &others)
            .unwrap();
    }

    fn reopen_all(&mut self) {
        self.nodes.clear();
        let mut outputs = Vec::new();
        for node_id in voters() {
            let opened = PersistentRaftAdapter::open(
                &self.paths[&node_id],
                node_id,
                group_id(),
                group_epoch(),
                voters(),
            )
            .unwrap();
            outputs.push((node_id, opened.output));
            assert!(self.nodes.insert(node_id, opened.adapter).is_none());
        }
        for (node_id, output) in outputs {
            self.capture(node_id, output);
        }
        self.drain();
    }

    fn reopen_one(&mut self, node_id: NodeId) {
        assert!(self.nodes.remove(&node_id).is_some());
        let opened = PersistentRaftAdapter::open(
            &self.paths[&node_id],
            node_id,
            group_id(),
            group_epoch(),
            voters(),
        )
        .unwrap();
        let output = opened.output;
        assert!(self.nodes.insert(node_id, opened.adapter).is_none());
        self.capture(node_id, output);
        self.drain();
    }

    fn capture(&mut self, node_id: NodeId, output: ConsensusOutput) {
        assert_eq!(output.status.node_id, node_id);
        self.commits
            .extend(output.commits.into_iter().map(|commit| (node_id, commit)));
        for message in output.messages {
            assert_eq!(message.from(), node_id);
            let wire = message.to_wire().unwrap();
            self.transport
                .send(peer(message.from()), peer(message.to()), wire)
                .unwrap();
        }
    }

    fn drain(&mut self) {
        for _ in 0..MAX_DELIVERIES {
            let Some(delivery) = self.transport.deliver_next().unwrap() else {
                return;
            };
            let target = node(delivery.to.value());
            let Some(adapter) = self.nodes.get_mut(&target) else {
                continue;
            };
            let message = PeerMessage::from_wire(&delivery.payload, target).unwrap();
            let output = adapter.receive(message).unwrap();
            self.capture(target, output);
        }
        panic!("persistent test transport did not quiesce after {MAX_DELIVERIES} deliveries");
    }

    fn applied_history(&self, node_id: NodeId) -> Vec<(ProposalId, Vec<u8>)> {
        self.nodes[&node_id]
            .applied_proposals()
            .iter()
            .map(|committed| (committed.receipt.proposal_id, committed.payload.clone()))
            .collect()
    }

    fn digests(&self) -> BTreeMap<NodeId, StateDigest> {
        self.nodes
            .iter()
            .map(|(node_id, adapter)| (*node_id, adapter.state_digest()))
            .collect()
    }
}

impl Drop for PersistentCluster {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn committed_history_and_digest_survive_reopening_every_adapter() {
    let mut cluster = PersistentCluster::new(TEST_SEED);
    cluster.campaign(node(1));
    assert_eq!(cluster.nodes[&node(1)].status().role, ConsensusRole::Leader);
    cluster.propose(node(1), 1, b"durable-history");
    cluster.tick_repeatedly(node(1), 4);

    let expected_history = vec![(proposal(1), b"durable-history".to_vec())];
    for node_id in voters() {
        assert_eq!(cluster.applied_history(node_id), expected_history);
        assert!(matches!(
            cluster.nodes[&node_id].lookup_proposal(proposal(1)),
            ProposalLookup::Committed(_)
        ));
    }
    let expected_digests = cluster.digests();
    assert_eq!(
        expected_digests.values().copied().collect::<Vec<_>>(),
        vec![expected_digests[&node(1)]; 3]
    );

    cluster.reopen_all();

    for node_id in voters() {
        assert_eq!(cluster.applied_history(node_id), expected_history);
        assert_eq!(
            cluster.nodes[&node_id].state_digest(),
            expected_digests[&node_id]
        );
        assert!(cluster.nodes[&node_id].recovery().stable_generation > 0);
        assert!(matches!(
            cluster.nodes[&node_id].lookup_proposal(proposal(1)),
            ProposalLookup::Committed(_)
        ));
    }
}

#[test]
fn isolated_leader_pending_proposal_survives_disk_reopen() {
    let mut cluster = PersistentCluster::new(TEST_SEED + 1);
    cluster.campaign(node(1));
    cluster.isolate(node(1));
    cluster.propose(node(1), 7, b"minority-only");

    assert_eq!(
        cluster.nodes[&node(1)].lookup_proposal(proposal(7)),
        ProposalLookup::Pending
    );
    assert!(
        cluster
            .commits
            .iter()
            .all(|(_, commit)| commit.receipt.proposal_id != proposal(7))
    );

    cluster.reopen_one(node(1));

    assert_eq!(
        cluster.nodes[&node(1)].lookup_proposal(proposal(7)),
        ProposalLookup::Pending
    );
    assert!(cluster.nodes[&node(1)].applied_proposals().is_empty());
}

#[test]
fn stable_path_enforces_exclusive_ownership_and_immutable_identity() {
    let root = test_directory("identity");
    let path = root.join("stable.wal");
    let first =
        PersistentRaftAdapter::open(&path, node(1), group_id(), group_epoch(), voters()).unwrap();
    assert!(matches!(
        PersistentRaftAdapter::open(&path, node(1), group_id(), group_epoch(), voters()),
        Err(ConsensusError::Storage(_))
    ));
    drop(first);

    assert!(matches!(
        PersistentRaftAdapter::open(
            &path,
            node(1),
            group_id(),
            GroupEpoch::new(group_epoch().get() + 1).unwrap(),
            voters()
        ),
        Err(ConsensusError::InvalidState(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn partial_tail_is_repaired_but_checksum_corruption_fails_closed() {
    let root = test_directory("corruption");
    let partial_path = root.join("partial.wal");
    let mut opened =
        PersistentRaftAdapter::open(&partial_path, node(1), group_id(), group_epoch(), voters())
            .unwrap();
    opened.adapter.campaign().unwrap();
    drop(opened);
    let stable_len = fs::metadata(&partial_path).unwrap().len();
    OpenOptions::new()
        .append(true)
        .open(&partial_path)
        .unwrap()
        .write_all(b"EPCHpartial")
        .unwrap();

    let repaired =
        PersistentRaftAdapter::open(&partial_path, node(1), group_id(), group_epoch(), voters())
            .unwrap();
    assert!(repaired.adapter.recovery().repaired_partial_tail);
    assert_eq!(fs::metadata(&partial_path).unwrap().len(), stable_len);
    drop(repaired);

    let corrupt_path = root.join("corrupt.wal");
    let mut corrupt =
        PersistentRaftAdapter::open(&corrupt_path, node(1), group_id(), group_epoch(), voters())
            .unwrap();
    corrupt.adapter.campaign().unwrap();
    drop(corrupt);
    let mut bytes = fs::read(&corrupt_path).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    fs::write(&corrupt_path, bytes).unwrap();
    assert!(matches!(
        PersistentRaftAdapter::open(&corrupt_path, node(1), group_id(), group_epoch(), voters()),
        Err(ConsensusError::Storage(_))
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn disk_backed_persisted_messages_follow_a_stable_barrier() {
    let mut cluster = PersistentCluster::new(TEST_SEED + 2);
    cluster.campaign(node(1));
    cluster.propose(node(1), 3, b"barrier");

    for adapter in cluster.nodes.values() {
        let mut latest_barrier = 0;
        let mut saw_barrier = false;
        let mut released = 0;
        for event in adapter.inner.processing_trace() {
            match event {
                ProcessingTrace::StableStoreBarrier(generation) => {
                    assert!(*generation >= latest_barrier);
                    latest_barrier = *generation;
                    saw_barrier = true;
                }
                ProcessingTrace::MessageReleasedAfterStableStoreBarrier(generation) => {
                    assert!(saw_barrier);
                    assert!(*generation <= latest_barrier);
                    released += 1;
                }
                ProcessingTrace::Applied(_) => {}
            }
        }
        assert!(saw_barrier);
        assert!(released > 0);
    }
}

#[test]
fn fsynced_proposal_is_recovered_after_injected_post_append_failure() {
    let mut cluster = PersistentCluster::new(TEST_SEED + 3);
    cluster.campaign(node(1));
    cluster.isolate(node(1));
    let failed_proposal = Proposal::new(
        group_id(),
        group_epoch(),
        cluster.nodes[&node(1)].status().term,
        proposal(31),
        b"fsynced-before-failure",
    );
    cluster
        .nodes
        .get_mut(&node(1))
        .unwrap()
        .inner
        .disk_store
        .as_mut()
        .unwrap()
        .fail_after_next_append();

    let error = cluster
        .nodes
        .get_mut(&node(1))
        .unwrap()
        .propose(failed_proposal)
        .unwrap_err();
    assert!(matches!(error, ConsensusError::Storage(_)));
    assert_eq!(cluster.transport.pending_len(), 0);
    assert!(
        cluster
            .commits
            .iter()
            .all(|(_, commit)| commit.receipt.proposal_id != proposal(31))
    );
    assert!(cluster.nodes[&node(1)].status().fail_stopped);
    assert!(matches!(
        cluster.nodes.get_mut(&node(1)).unwrap().tick(),
        Err(ConsensusError::Poisoned(_))
    ));

    cluster.reopen_one(node(1));

    assert_eq!(
        cluster.nodes[&node(1)].lookup_proposal(proposal(31)),
        ProposalLookup::Pending
    );
    assert!(!cluster.nodes[&node(1)].status().fail_stopped);
}

#[test]
fn committed_entry_ahead_of_checkpoint_is_published_once_during_recovery() {
    let root = test_directory("commit-ahead");
    let path = root.join("stable.wal");
    let identity = stable_identity(node(1));
    let recovered = DiskStableStore::open(&path, identity).unwrap();
    let mut store = recovered.store;
    let seeded_proposal = Proposal::new(
        group_id(),
        group_epoch(),
        Term::new(1),
        proposal(41),
        b"recover-me",
    );
    let mut entry = Entry {
        entry_type: EntryType::EntryNormal as i32,
        term: 1,
        index: 1,
        ..Entry::default()
    };
    entry.data = encode_command(&seeded_proposal).unwrap();
    store
        .persist(
            1,
            &HardState {
                term: 1,
                vote: node(1).get(),
                commit: 1,
            },
            &[entry],
            StableCheckpoint::empty(identity).unwrap(),
        )
        .unwrap();
    drop(store);

    let first =
        PersistentRaftAdapter::open(&path, node(1), group_id(), group_epoch(), voters()).unwrap();
    assert_eq!(first.adapter.recovery().applied_index, LogIndex::ZERO);
    assert_eq!(first.output.commits.len(), 1);
    assert_eq!(first.output.commits[0].receipt.proposal_id, proposal(41));
    assert_eq!(first.output.commits[0].payload, b"recover-me");
    assert_eq!(first.adapter.status().applied_index, LogIndex::new(1));
    assert_eq!(first.adapter.applied_proposals(), first.output.commits);
    assert!(matches!(
        first.adapter.lookup_proposal(proposal(41)),
        ProposalLookup::Committed(_)
    ));
    let expected_digest = first.adapter.state_digest();
    drop(first);

    let second =
        PersistentRaftAdapter::open(&path, node(1), group_id(), group_epoch(), voters()).unwrap();
    assert!(second.output.commits.is_empty());
    assert_eq!(second.adapter.recovery().applied_index, LogIndex::new(1));
    assert_eq!(second.adapter.applied_proposals().len(), 1);
    assert_eq!(second.adapter.state_digest(), expected_digest);
    drop(second);
    fs::remove_dir_all(root).unwrap();
}

fn test_directory(label: &str) -> PathBuf {
    let serial = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "epoch-consensus-persistent-{label}-{}-{serial}",
        std::process::id()
    ));
    fs::create_dir(&path).unwrap();
    path
}

fn stable_identity(node_id: NodeId) -> StableIdentity {
    StableIdentity {
        node_id,
        group_id: group_id(),
        group_epoch: group_epoch(),
        voters: voters(),
    }
}

fn voters() -> [NodeId; 3] {
    [node(1), node(2), node(3)]
}

fn node(value: u64) -> NodeId {
    NodeId::new(value).unwrap()
}

fn peer(node_id: NodeId) -> PeerId {
    PeerId::new(node_id.get())
}

fn group_id() -> GroupId {
    GroupId::new(77).unwrap()
}

fn group_epoch() -> GroupEpoch {
    GroupEpoch::new(5).unwrap()
}

fn proposal(value: u64) -> ProposalId {
    ProposalId::new(value).unwrap()
}
