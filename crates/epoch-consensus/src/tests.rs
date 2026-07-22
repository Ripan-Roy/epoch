use std::collections::{BTreeMap, BTreeSet};

use epoch_testkit::{
    Delivery, DropReason, FaultAction, FaultPoint, MessageId, PeerId, PeerTransport, SendOutcome,
    Trace,
};
use prost::Message as _;
use raft::{
    GetEntriesContext, Storage,
    prelude::{ConfState, Entry as RaftEntry, Message as RaftWireMessage, MessageType},
};

use super::*;

const MAX_DELIVERIES: usize = 20_000;
const SCENARIO_SEED: u64 = 0x4550_4f43_485f_5331;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ObservedSend {
    from: NodeId,
    to: NodeId,
    outcome: SendOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ObservedDelivery {
    message_id: MessageId,
    copy_index: u16,
    from: NodeId,
    to: NodeId,
    delivered_at_ms: u64,
    wire: Vec<u8>,
}

impl ObservedDelivery {
    fn from_delivery(delivery: &Delivery) -> Self {
        Self {
            message_id: delivery.message_id,
            copy_index: delivery.copy_index,
            from: node(delivery.from.value()),
            to: node(delivery.to.value()),
            delivered_at_ms: delivery.delivered_at_ms,
            wire: delivery.payload.clone(),
        }
    }
}

#[derive(Debug)]
struct TestCluster {
    nodes: BTreeMap<NodeId, InMemoryRaftAdapter>,
    stopped: BTreeMap<NodeId, MemoryStableState>,
    transport: PeerTransport,
    sends: Vec<ObservedSend>,
    deliveries: Vec<ObservedDelivery>,
    commits: Vec<(NodeId, CommittedProposal)>,
}

impl TestCluster {
    fn new(seed: u64) -> Self {
        let voters = voters();
        let nodes = voters
            .into_iter()
            .map(|node_id| {
                (
                    node_id,
                    InMemoryRaftAdapter::new(node_id, group_id(), group_epoch(), voters).unwrap(),
                )
            })
            .collect();
        let cluster = Self {
            nodes,
            stopped: BTreeMap::new(),
            transport: PeerTransport::new(seed),
            sends: Vec::new(),
            deliveries: Vec::new(),
            commits: Vec::new(),
        };
        cluster.assert_invariants();
        cluster
    }

    fn campaign(&mut self, node_id: NodeId) {
        let output = self.nodes.get_mut(&node_id).unwrap().campaign().unwrap();
        self.capture(node_id, output);
        self.drain();
    }

    fn propose(&mut self, node_id: NodeId, proposal_id: u64, payload: &[u8]) {
        self.propose_without_drain(node_id, proposal_id, payload);
        self.drain();
    }

    fn propose_without_drain(&mut self, node_id: NodeId, proposal_id: u64, payload: &[u8]) {
        let proposal = self.proposal(node_id, proposal_id, payload);
        let output = self
            .nodes
            .get_mut(&node_id)
            .unwrap()
            .propose(proposal)
            .unwrap();
        self.capture(node_id, output);
    }

    fn raw_propose_without_drain(
        &mut self,
        node_id: NodeId,
        proposal_id: u64,
        payload: &[u8],
    ) -> ConsensusResult<()> {
        let proposal = self.proposal(node_id, proposal_id, payload);
        let encoded = encode_command(&proposal)?;
        let output = {
            let adapter = self.nodes.get_mut(&node_id).unwrap();
            adapter
                .raw_node
                .propose(Vec::new(), encoded)
                .map_err(|error| ConsensusError::Library(error.to_string()))?;
            adapter.process_ready()?
        };
        self.capture(node_id, output);
        Ok(())
    }

    fn proposal(&self, node_id: NodeId, proposal_id: u64, payload: &[u8]) -> Proposal {
        Proposal::new(
            group_id(),
            group_epoch(),
            self.nodes[&node_id].status().term,
            proposal(proposal_id),
            payload,
        )
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

    fn expire_leader_leases(&mut self, node_ids: [NodeId; 2]) {
        for node_id in node_ids {
            self.nodes.get_mut(&node_id).unwrap().expire_leader_lease();
        }
        self.assert_invariants();
    }

    fn transfer(&mut self, leader: NodeId, target: NodeId) {
        let output = self
            .nodes
            .get_mut(&leader)
            .unwrap()
            .transfer_leadership(target)
            .unwrap();
        self.capture(leader, output);
        self.drain();
    }

    fn partition(&mut self, left: &[NodeId], right: &[NodeId]) {
        let left = left.iter().copied().map(peer).collect::<Vec<_>>();
        let right = right.iter().copied().map(peer).collect::<Vec<_>>();
        self.transport.partition(&left, &right).unwrap();
    }

    fn isolate(&mut self, node_id: NodeId) {
        let other_nodes = voters()
            .into_iter()
            .filter(|candidate| *candidate != node_id)
            .collect::<Vec<_>>();
        self.partition(&[node_id], &other_nodes);
    }

    fn block_link(&mut self, from: NodeId, to: NodeId) {
        self.transport.block_link(peer(from), peer(to)).unwrap();
    }

    fn heal_all(&mut self) {
        self.transport.heal_all().unwrap();
    }

    fn inject_next_send(&mut self, action: FaultAction) {
        let point = FaultPoint::transport_send();
        let next_occurrence = self
            .transport
            .fault_plan_mut()
            .occurrence_count(&point)
            .checked_add(1)
            .unwrap();
        self.transport
            .fault_plan_mut()
            .add(point, next_occurrence, action)
            .unwrap();
    }

    fn stop(&mut self, node_id: NodeId) {
        assert_eq!(
            self.transport.pending_len(),
            0,
            "stop requires a quiet link"
        );
        let node = self.nodes.remove(&node_id).unwrap();
        self.stopped
            .insert(node_id, node.into_stable_state().unwrap());
    }

    fn restart(&mut self, node_id: NodeId) {
        let stable = self.stopped.remove(&node_id).unwrap();
        self.nodes
            .insert(node_id, InMemoryRaftAdapter::restart(stable).unwrap());
        self.assert_invariants();
    }

    fn leader(&self) -> NodeId {
        let leaders = self
            .nodes
            .iter()
            .filter_map(|(node_id, adapter)| {
                (adapter.status().role == ConsensusRole::Leader).then_some(*node_id)
            })
            .collect::<Vec<_>>();
        assert_eq!(leaders.len(), 1, "expected one leader, found {leaders:?}");
        leaders[0]
    }

    fn capture(&mut self, node_id: NodeId, output: ConsensusOutput) {
        assert_eq!(output.status.node_id, node_id);
        self.commits
            .extend(output.commits.into_iter().map(|commit| (node_id, commit)));
        for message in output.messages {
            assert_eq!(message.from(), node_id);
            let from = message.from();
            let to = message.to();
            let wire = message.to_wire().unwrap();
            let outcome = self.transport.send(peer(from), peer(to), wire).unwrap();
            self.sends.push(ObservedSend { from, to, outcome });
        }
        self.assert_invariants();
    }

    fn drain(&mut self) {
        if let Some((node_id, error)) = self.drain_until_error() {
            panic!("node {node_id} rejected a deterministic delivery: {error}");
        }
        assert_eq!(self.transport.pending_len(), 0);
    }

    fn drain_until_error(&mut self) -> Option<(NodeId, ConsensusError)> {
        for _ in 0..MAX_DELIVERIES {
            let delivery = self.transport.deliver_next().unwrap()?;
            let observed = ObservedDelivery::from_delivery(&delivery);
            let target = observed.to;
            self.deliveries.push(observed);
            let Some(adapter) = self.nodes.get_mut(&target) else {
                continue;
            };
            let message = match PeerMessage::from_wire(&delivery.payload, target) {
                Ok(message) => message,
                Err(error) => return Some((target, error)),
            };
            assert_eq!(message.from().get(), delivery.from.value());
            assert_eq!(message.to().get(), delivery.to.value());
            match adapter.receive(message) {
                Ok(output) => self.capture(target, output),
                Err(error) => return Some((target, error)),
            }
        }
        assert_eq!(
            self.transport.pending_len(),
            0,
            "deterministic network did not quiesce after {MAX_DELIVERIES} deliveries"
        );
        None
    }

    fn assert_invariants(&self) {
        for adapter in self.nodes.values() {
            assert_adapter_invariants(adapter);
        }
    }

    fn observed_commit_ids(&self, node_id: NodeId) -> Vec<ProposalId> {
        self.commits
            .iter()
            .filter_map(|(observed_at, committed)| {
                (*observed_at == node_id).then_some(committed.receipt.proposal_id)
            })
            .collect()
    }

    fn applied_history(&self, node_id: NodeId) -> Vec<(ProposalId, Vec<u8>)> {
        self.nodes[&node_id]
            .applied_proposals()
            .iter()
            .map(|committed| (committed.receipt.proposal_id, committed.payload.clone()))
            .collect()
    }

    fn all_digests(&self) -> Vec<[u8; 32]> {
        self.nodes
            .values()
            .map(InMemoryRaftAdapter::state_digest)
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScenarioHistory {
    seed: u64,
    trace_bytes: Vec<u8>,
    trace_digest: u64,
    sends: Vec<ObservedSend>,
    deliveries: Vec<ObservedDelivery>,
    commits: Vec<(NodeId, CommittedProposal)>,
    state_digests: Vec<[u8; 32]>,
}

#[test]
fn deterministic_campaign_elects_the_requested_voter() {
    let mut cluster = TestCluster::new(1);
    cluster.campaign(node(1));

    assert_eq!(cluster.leader(), node(1));
    assert_eq!(cluster.nodes[&node(1)].status().term, Term::new(1));
    assert_eq!(cluster.observed_commit_ids(node(1)), Vec::new());
}

#[test]
fn proposal_is_reported_only_after_a_majority_commit() {
    let mut cluster = TestCluster::new(2);
    cluster.campaign(node(1));
    cluster.isolate(node(1));

    cluster.propose(node(1), 1, b"needs-majority");
    assert_eq!(cluster.observed_commit_ids(node(1)), Vec::new());
    assert_eq!(
        cluster.nodes[&node(1)].lookup_proposal(proposal(1)),
        ProposalLookup::Pending
    );

    cluster.heal_all();
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK + 2);

    assert_eq!(cluster.observed_commit_ids(node(1)), vec![proposal(1)]);
    assert_eq!(
        cluster.applied_history(node(1)),
        vec![(proposal(1), b"needs-majority".to_vec())]
    );
}

#[test]
fn isolated_old_leader_cannot_acknowledge_and_majority_re_elects() {
    let mut cluster = TestCluster::new(3);
    cluster.campaign(node(1));
    let old_term = cluster.nodes[&node(1)].status().term;
    cluster.isolate(node(1));
    cluster.propose(node(1), 1, b"minority-only");
    assert_eq!(cluster.observed_commit_ids(node(1)), Vec::new());

    cluster.expire_leader_leases([node(2), node(3)]);
    cluster.campaign(node(2));
    assert_eq!(cluster.nodes[&node(2)].status().role, ConsensusRole::Leader);
    assert_eq!(cluster.nodes[&node(3)].status().leader_id, Some(node(2)));
    assert!(cluster.nodes[&node(2)].status().term > old_term);
    cluster.propose(node(2), 2, b"majority-value");
    assert_eq!(cluster.observed_commit_ids(node(2)), vec![proposal(2)]);

    cluster.heal_all();
    cluster.tick_repeatedly(node(2), HEARTBEAT_TICK * 4);
    assert_eq!(
        cluster.nodes[&node(1)].status().role,
        ConsensusRole::Follower
    );
    assert_eq!(cluster.all_digests(), vec![cluster.all_digests()[0]; 3]);
    let expected = vec![(proposal(2), b"majority-value".to_vec())];
    assert_eq!(cluster.applied_history(node(1)), expected);
    assert_eq!(cluster.applied_history(node(2)), expected);
    assert_eq!(cluster.applied_history(node(3)), expected);
}

#[test]
fn pending_proposal_survives_a_graceful_memory_state_restart() {
    let mut cluster = TestCluster::new(4);
    cluster.campaign(node(1));
    cluster.isolate(node(1));
    cluster.propose(node(1), 10, b"pending-across-restart");
    assert_eq!(
        cluster.nodes[&node(1)].lookup_proposal(proposal(10)),
        ProposalLookup::Pending
    );

    cluster.stop(node(1));
    cluster.restart(node(1));

    assert_eq!(
        cluster.nodes[&node(1)].lookup_proposal(proposal(10)),
        ProposalLookup::Pending
    );
    assert_eq!(cluster.observed_commit_ids(node(1)), Vec::new());
}

#[test]
fn overwritten_pending_proposal_id_becomes_reusable() {
    let mut cluster = TestCluster::new(5);
    cluster.campaign(node(1));
    cluster.isolate(node(1));
    cluster.propose(node(1), 10, b"will-be-overwritten");

    cluster.expire_leader_leases([node(2), node(3)]);
    cluster.campaign(node(2));
    cluster.propose(node(2), 20, b"majority-history");
    cluster.heal_all();
    cluster.tick_repeatedly(node(2), HEARTBEAT_TICK * 4);

    assert_eq!(
        cluster.nodes[&node(1)].lookup_proposal(proposal(10)),
        ProposalLookup::Unknown
    );
    cluster.transfer(node(2), node(1));
    assert_eq!(cluster.leader(), node(1));
    cluster.propose(node(1), 10, b"reused-after-overwrite");
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK * 2);

    let expected = vec![
        (proposal(20), b"majority-history".to_vec()),
        (proposal(10), b"reused-after-overwrite".to_vec()),
    ];
    assert_eq!(cluster.applied_history(node(1)), expected);
    assert_eq!(cluster.applied_history(node(2)), expected);
    assert_eq!(cluster.applied_history(node(3)), expected);
}

#[test]
fn graceful_leader_restart_preserves_emitted_commit_lookup_and_digest() {
    let mut cluster = TestCluster::new(6);
    cluster.campaign(node(1));
    cluster.propose(node(1), 1, b"before-restart");
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);
    let digest = cluster.nodes[&node(1)].state_digest();
    let lookup = cluster.nodes[&node(1)].lookup_proposal(proposal(1));
    let commits = cluster.commits.clone();

    cluster.stop(node(1));
    cluster.restart(node(1));

    assert_eq!(cluster.nodes[&node(1)].state_digest(), digest);
    assert_eq!(cluster.nodes[&node(1)].lookup_proposal(proposal(1)), lookup);
    assert_eq!(
        cluster.commits, commits,
        "restart must not re-emit a commit"
    );
    assert_eq!(
        cluster.applied_history(node(1)),
        vec![(proposal(1), b"before-restart".to_vec())]
    );
}

#[test]
fn stopped_follower_restarts_at_its_stable_state_and_catches_up() {
    let mut cluster = TestCluster::new(7);
    cluster.campaign(node(1));
    cluster.propose(node(1), 1, b"before-restart");
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);
    let stable_digest = cluster.nodes[&node(3)].state_digest();

    cluster.stop(node(3));
    cluster.propose(node(1), 2, b"while-stopped");
    cluster.restart(node(3));
    assert_eq!(cluster.nodes[&node(3)].state_digest(), stable_digest);

    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK * 4);
    assert_eq!(cluster.all_digests(), vec![cluster.all_digests()[0]; 3]);
    let expected = vec![
        (proposal(1), b"before-restart".to_vec()),
        (proposal(2), b"while-stopped".to_vec()),
    ];
    assert_eq!(cluster.applied_history(node(1)), expected);
    assert_eq!(cluster.applied_history(node(2)), expected);
    assert_eq!(cluster.applied_history(node(3)), expected);
}

#[test]
fn stale_term_and_mismatched_group_epoch_are_rejected_before_proposal() {
    let mut cluster = TestCluster::new(8);
    cluster.campaign(node(1));
    let stale_term = cluster.nodes[&node(1)].status().term;
    cluster.isolate(node(1));
    cluster.expire_leader_leases([node(2), node(3)]);
    cluster.campaign(node(2));
    let current_term = cluster.nodes[&node(2)].status().term;

    let stale = Proposal::new(
        group_id(),
        group_epoch(),
        stale_term,
        proposal(10),
        b"stale-term",
    );
    assert_eq!(
        cluster.nodes.get_mut(&node(2)).unwrap().propose(stale),
        Err(ConsensusError::StaleTerm {
            current: current_term,
            observed: stale_term,
        })
    );

    let mismatched_epoch = GroupEpoch::new(group_epoch().get() + 1).unwrap();
    let mismatched = Proposal::new(
        group_id(),
        mismatched_epoch,
        current_term,
        proposal(11),
        b"mismatched-epoch",
    );
    assert_eq!(
        cluster.nodes.get_mut(&node(2)).unwrap().propose(mismatched),
        Err(ConsensusError::FencedEpoch {
            expected: group_epoch(),
            observed: mismatched_epoch,
        })
    );
    cluster.assert_invariants();
}

#[test]
fn leadership_transfer_is_deterministic_when_target_is_caught_up() {
    let mut cluster = TestCluster::new(9);
    cluster.campaign(node(1));
    cluster.propose(node(1), 1, b"transfer-base");
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);

    cluster.transfer(node(1), node(2));
    assert_eq!(cluster.leader(), node(2));
    cluster.propose(node(2), 2, b"after-transfer");
    cluster.tick_repeatedly(node(2), HEARTBEAT_TICK);
    assert_eq!(cluster.all_digests(), vec![cluster.all_digests()[0]; 3]);
}

#[test]
fn directed_partition_blocks_only_the_selected_response_path() {
    let mut cluster = TestCluster::new(10);
    cluster.campaign(node(1));
    cluster.block_link(node(2), node(1));
    assert!(
        cluster
            .transport
            .is_link_blocked(peer(node(2)), peer(node(1)))
    );
    assert!(
        !cluster
            .transport
            .is_link_blocked(peer(node(1)), peer(node(2)))
    );
    let send_start = cluster.sends.len();
    let delivery_start = cluster.deliveries.len();

    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);

    let path_sends = cluster.sends[send_start..]
        .iter()
        .filter(|send| {
            (send.from == node(1) && send.to == node(2))
                || (send.from == node(2) && send.to == node(1))
        })
        .collect::<Vec<_>>();
    assert_eq!(path_sends.len(), 2);
    assert_eq!((path_sends[0].from, path_sends[0].to), (node(1), node(2)));
    assert!(matches!(
        path_sends[0].outcome,
        SendOutcome::Scheduled { .. }
    ));
    assert_eq!((path_sends[1].from, path_sends[1].to), (node(2), node(1)));
    assert!(matches!(
        path_sends[1].outcome,
        SendOutcome::Dropped {
            reason: DropReason::Partition,
            ..
        }
    ));
    let path_deliveries = cluster.deliveries[delivery_start..]
        .iter()
        .map(|delivery| (delivery.from, delivery.to))
        .filter(|pair| *pair == (node(1), node(2)) || *pair == (node(2), node(1)))
        .collect::<Vec<_>>();
    assert_eq!(path_deliveries, vec![(node(1), node(2))]);
    cluster.assert_invariants();
}

#[test]
fn delay_reorders_messages_and_duplicate_delivery_is_idempotent() {
    let mut cluster = TestCluster::new(11);
    cluster.campaign(node(1));
    let delivery_start = cluster.deliveries.len();
    cluster.inject_next_send(FaultAction::Delay { by_ms: 20 });
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);

    let leader_deliveries = cluster.deliveries[delivery_start..]
        .iter()
        .filter(|delivery| delivery.from == node(1))
        .collect::<Vec<_>>();
    assert_eq!(leader_deliveries[0].to, node(3));
    assert_eq!(leader_deliveries.last().unwrap().to, node(2));
    assert_eq!(leader_deliveries.last().unwrap().delivered_at_ms, 20);

    cluster.inject_next_send(FaultAction::Duplicate {
        additional_copies: 1,
        spacing_ms: 1,
    });
    let send_start = cluster.sends.len();
    let delivery_start = cluster.deliveries.len();
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);
    let duplicated_id = match cluster.sends[send_start].outcome {
        SendOutcome::Scheduled {
            message_id,
            copies: 2,
            ..
        } => message_id,
        ref outcome => panic!("expected two scheduled copies, got {outcome:?}"),
    };
    let duplicate_copies = cluster.deliveries[delivery_start..]
        .iter()
        .filter(|delivery| delivery.message_id == duplicated_id)
        .map(|delivery| delivery.copy_index)
        .collect::<Vec<_>>();
    assert_eq!(duplicate_copies, vec![0, 1]);
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK * 2);
    assert_eq!(cluster.all_digests(), vec![cluster.all_digests()[0]; 3]);
}

#[test]
fn peer_wire_round_trips_and_rejects_wrong_destination_corruption_and_oversize() {
    let message = valid_peer_message();
    let wire = message.to_wire().unwrap();
    let decoded = PeerMessage::from_wire(&wire, message.to()).unwrap();
    assert_eq!(decoded, message);

    assert!(matches!(
        PeerMessage::from_wire(&wire, node(99)),
        Err(ConsensusError::InvalidMessage(_))
    ));

    let mut corrupted = wire.clone();
    corrupted[0] ^= 0xff;
    assert!(matches!(
        PeerMessage::from_wire(&corrupted, message.to()),
        Err(ConsensusError::InvalidMessage(_))
    ));

    let oversized = vec![0; MAX_PEER_MESSAGE_WIRE_BYTES + 1];
    assert!(matches!(
        PeerMessage::from_wire(&oversized, message.to()),
        Err(ConsensusError::InvalidMessage(_))
    ));
}

#[test]
fn peer_wire_rejects_noncanonical_protobuf_and_local_only_message_classes() {
    let message = valid_peer_message();
    let mut noncanonical = message.to_wire().unwrap();
    // Unknown protobuf field 127 with varint value zero. Prost discards it, so
    // canonical re-encoding must differ from the received payload.
    noncanonical.extend_from_slice(&[0xf8, 0x07, 0x00]);
    let payload_len = u32::try_from(noncanonical.len() - PEER_MESSAGE_HEADER_LEN).unwrap();
    noncanonical[46..50].copy_from_slice(&payload_len.to_be_bytes());
    assert!(matches!(
        PeerMessage::from_wire(&noncanonical, message.to()),
        Err(ConsensusError::InvalidMessage(_))
    ));

    for message_type in [
        MessageType::MsgHup,
        MessageType::MsgBeat,
        MessageType::MsgUnreachable,
        MessageType::MsgSnapStatus,
        MessageType::MsgCheckQuorum,
    ] {
        let mut local_only = message.clone();
        let mut raft_message = RaftWireMessage::decode(local_only.encoded.as_slice()).unwrap();
        raft_message.msg_type = message_type as i32;
        local_only.encoded = raft_message.encode_to_vec();
        assert!(matches!(
            local_only.to_wire(),
            Err(ConsensusError::InvalidMessage(_))
        ));
    }
}

#[test]
fn snapshot_transport_message_is_rejected_until_checkpoint_installation_exists() {
    let mut message = valid_peer_message();
    let mut raft_message = RaftWireMessage::decode(message.encoded.as_slice()).unwrap();
    raft_message.msg_type = MessageType::MsgSnapshot as i32;
    message.encoded = raft_message.encode_to_vec();

    assert!(matches!(
        message.to_wire(),
        Err(ConsensusError::Unsupported(_))
    ));
}

#[test]
fn oversized_proposal_is_rejected_before_raft_without_poisoning_state() {
    let mut cluster = TestCluster::new(17);
    cluster.campaign(node(1));
    let before_status = cluster.nodes[&node(1)].status();
    let before_digest = cluster.nodes[&node(1)].state_digest();
    let oversized = cluster.proposal(node(1), 99, &vec![0; MAX_PROPOSAL_PAYLOAD_BYTES + 1]);

    assert!(matches!(
        cluster.nodes.get_mut(&node(1)).unwrap().propose(oversized),
        Err(ConsensusError::InvalidMessage(_))
    ));
    let after_status = cluster.nodes[&node(1)].status();
    assert_eq!(after_status.commit_index, before_status.commit_index);
    assert_eq!(after_status.applied_index, before_status.applied_index);
    assert!(!after_status.fail_stopped);
    assert_eq!(cluster.nodes[&node(1)].state_digest(), before_digest);
    assert_eq!(
        cluster.nodes[&node(1)].lookup_proposal(proposal(99)),
        ProposalLookup::Unknown
    );
}

#[test]
fn outsider_source_is_rejected_even_when_envelope_matches_encoded_raft_message() {
    let mut message = valid_peer_message();
    let mut raft_message = RaftWireMessage::decode(message.encoded.as_slice()).unwrap();
    raft_message.from = 99;
    message.from = node(99);
    message.encoded = raft_message.encode_to_vec();
    let wire = message.to_wire().unwrap();
    let decoded = PeerMessage::from_wire(&wire, message.to()).unwrap();
    let mut receiver =
        InMemoryRaftAdapter::new(message.to(), group_id(), group_epoch(), voters()).unwrap();

    assert!(matches!(
        receiver.receive(decoded),
        Err(ConsensusError::InvalidMessage(_))
    ));
}

#[test]
fn exact_duplicate_at_apply_is_suppressed_without_changing_history_or_digest() {
    let mut cluster = TestCluster::new(12);
    cluster.campaign(node(1));
    cluster.propose(node(1), 1, b"idempotent");
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);
    let before_commits = cluster.commits.clone();
    let before_histories = voters()
        .into_iter()
        .map(|node_id| cluster.applied_history(node_id))
        .collect::<Vec<_>>();
    let before_digests = cluster.all_digests();

    cluster
        .raw_propose_without_drain(node(1), 1, b"idempotent")
        .unwrap();
    cluster.drain();
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);

    assert_eq!(cluster.commits, before_commits);
    assert_eq!(cluster.all_digests(), before_digests);
    let after_histories = voters()
        .into_iter()
        .map(|node_id| cluster.applied_history(node_id))
        .collect::<Vec<_>>();
    assert_eq!(after_histories, before_histories);
}

#[test]
fn conflicting_payload_for_applied_proposal_id_fails_closed_and_poison_sticks() {
    let mut cluster = TestCluster::new(13);
    cluster.campaign(node(1));
    cluster.propose(node(1), 1, b"canonical");
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);
    let before_history = cluster.applied_history(node(1));
    let before_digest = cluster.nodes[&node(1)].state_digest();

    let error = cluster
        .raw_propose_without_drain(node(1), 1, b"conflicting")
        .unwrap_err();
    assert_eq!(error, ConsensusError::ConflictingProposal(proposal(1)));
    assert_eq!(cluster.applied_history(node(1)), before_history);
    assert_eq!(cluster.nodes[&node(1)].state_digest(), before_digest);
    assert!(cluster.nodes[&node(1)].status().fail_stopped);
    assert!(matches!(
        cluster.nodes.get_mut(&node(1)).unwrap().tick(),
        Err(ConsensusError::Poisoned(_))
    ));
}

#[test]
fn restart_rejects_corrupted_digest_and_applied_index_invariants() {
    let mut bad_digest = stable_state_after_one_commit(14);
    bad_digest.state_digest[0] ^= 0xff;
    let digest_error = InMemoryRaftAdapter::restart(bad_digest).unwrap_err();
    assert!(matches!(digest_error, ConsensusError::InvalidState(_)));

    let mut bad_index = stable_state_after_one_commit(15);
    bad_index.applied_index = LogIndex::ZERO;
    let index_error = InMemoryRaftAdapter::restart(bad_index).unwrap_err();
    assert!(matches!(index_error, ConsensusError::InvalidState(_)));

    let bad_conf_state = stable_state_after_one_commit(18);
    bad_conf_state
        .storage
        .wl()
        .set_conf_state(ConfState::from((vec![1, 2], Vec::<u64>::new())));
    let conf_state_error = InMemoryRaftAdapter::restart(bad_conf_state).unwrap_err();
    assert!(matches!(conf_state_error, ConsensusError::InvalidState(_)));

    let mut bad_receipt = stable_state_after_one_commit(19);
    bad_receipt.applied[0].receipt.log_index = LogIndex::ZERO;
    let receipt_error = InMemoryRaftAdapter::restart(bad_receipt).unwrap_err();
    assert!(matches!(receipt_error, ConsensusError::InvalidState(_)));
}

#[test]
fn restart_rejects_corrupted_raft_term_and_vote_invariants() {
    let outsider_vote = stable_state_after_one_commit(20);
    outsider_vote.storage.wl().mut_hard_state().vote = 99;
    assert!(matches!(
        InMemoryRaftAdapter::restart(outsider_vote),
        Err(ConsensusError::InvalidState(_))
    ));

    let mut zero_term = stable_state_after_one_commit(21);
    let mut entries = retained_entries(&zero_term);
    entries[0].term = 0;
    replace_entries(&mut zero_term, &entries);
    assert!(matches!(
        InMemoryRaftAdapter::restart(zero_term),
        Err(ConsensusError::InvalidState(_))
    ));

    let mut regressing_term = stable_state_after_one_commit(22);
    let mut entries = retained_entries(&regressing_term);
    assert!(entries.len() >= 2);
    entries[0].term = 2;
    entries[1].term = 1;
    replace_entries(&mut regressing_term, &entries);
    regressing_term.storage.wl().mut_hard_state().term = 2;
    assert!(matches!(
        InMemoryRaftAdapter::restart(regressing_term),
        Err(ConsensusError::InvalidState(_))
    ));

    let mut future_log_term = stable_state_after_one_commit(23);
    let hard_state_term = future_log_term
        .storage
        .initial_state()
        .unwrap()
        .hard_state
        .term;
    let mut entries = retained_entries(&future_log_term);
    entries.last_mut().unwrap().term = hard_state_term.checked_add(1).unwrap();
    replace_entries(&mut future_log_term, &entries);
    assert!(matches!(
        InMemoryRaftAdapter::restart(future_log_term),
        Err(ConsensusError::InvalidState(_))
    ));
}

#[test]
fn memory_store_messages_are_released_only_after_their_barrier() {
    let mut cluster = TestCluster::new(16);
    cluster.campaign(node(1));
    cluster.propose(node(1), 1, b"barrier-check");

    for adapter in cluster.nodes.values() {
        let mut latest_barrier = 0;
        let mut barrier_count = 0;
        let mut released_count = 0;
        for event in adapter.processing_trace() {
            match event {
                ProcessingTrace::StableStoreBarrier(generation) => {
                    barrier_count += 1;
                    latest_barrier = *generation;
                }
                ProcessingTrace::MessageReleasedAfterStableStoreBarrier(generation) => {
                    released_count += 1;
                    assert!(*generation <= latest_barrier);
                }
                ProcessingTrace::Applied(_) => {}
            }
        }
        assert!(barrier_count > 0);
        assert!(released_count > 0);
    }
}

#[test]
fn seed_produces_a_canonical_transport_trace_and_full_history() {
    let first = deterministic_scenario();
    let second = deterministic_scenario();
    assert_eq!(first, second);
    let decoded = Trace::from_bytes(&first.trace_bytes).unwrap();
    assert_eq!(decoded.to_bytes().unwrap(), first.trace_bytes);
    assert_eq!(decoded.digest().unwrap().value(), first.trace_digest);
    assert_eq!(
        (
            first.trace_digest,
            first.trace_bytes.len(),
            decoded.events().len()
        ),
        // Compatibility golden: update only with an intentional wire, scheduler,
        // raft dependency, or scenario change.
        (0x8a41_6191_ed27_17f2, 28_743, 188)
    );
    assert_eq!(first.seed, SCENARIO_SEED);
    assert!(!first.sends.is_empty());
    assert!(!first.deliveries.is_empty());
    assert!(!first.commits.is_empty());
}

fn deterministic_scenario() -> ScenarioHistory {
    let mut cluster = TestCluster::new(SCENARIO_SEED);
    cluster.campaign(node(1));
    cluster.propose(node(1), 1, b"first");
    cluster.inject_next_send(FaultAction::Delay { by_ms: 7 });
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);
    cluster.inject_next_send(FaultAction::Duplicate {
        additional_copies: 1,
        spacing_ms: 2,
    });
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);
    cluster.transfer(node(1), node(2));
    cluster.propose(node(2), 2, b"second");
    cluster.tick_repeatedly(node(2), HEARTBEAT_TICK * 2);
    let trace_bytes = cluster.transport.trace().to_bytes().unwrap();
    let trace_digest = cluster.transport.trace().digest().unwrap().value();
    let state_digests = cluster.all_digests();
    ScenarioHistory {
        seed: SCENARIO_SEED,
        trace_bytes,
        trace_digest,
        sends: cluster.sends,
        deliveries: cluster.deliveries,
        commits: cluster.commits,
        state_digests,
    }
}

fn stable_state_after_one_commit(seed: u64) -> MemoryStableState {
    let mut cluster = TestCluster::new(seed);
    cluster.campaign(node(1));
    cluster.propose(node(1), 1, b"stable");
    cluster.tick_repeatedly(node(1), HEARTBEAT_TICK);
    assert_eq!(cluster.transport.pending_len(), 0);
    cluster
        .nodes
        .remove(&node(1))
        .unwrap()
        .into_stable_state()
        .unwrap()
}

fn retained_entries(stable: &MemoryStableState) -> Vec<RaftEntry> {
    let last_index = stable.storage.last_index().unwrap();
    stable
        .storage
        .entries(
            1,
            last_index.checked_add(1).unwrap(),
            None,
            GetEntriesContext::empty(false),
        )
        .unwrap()
}

fn replace_entries(stable: &mut MemoryStableState, entries: &[RaftEntry]) {
    stable.storage.wl().append(entries).unwrap();
}

fn valid_peer_message() -> PeerMessage {
    let mut adapter =
        InMemoryRaftAdapter::new(node(1), group_id(), group_epoch(), voters()).unwrap();
    adapter
        .campaign()
        .unwrap()
        .messages
        .into_iter()
        .next()
        .expect("campaign should emit a peer message")
}

fn assert_adapter_invariants(adapter: &InMemoryRaftAdapter) {
    let status = adapter.status();
    assert!(status.applied_index <= status.commit_index);
    assert_eq!(status.node_id, adapter.node_id);
    assert_eq!(status.group_id, adapter.group_id);
    assert_eq!(status.group_epoch, adapter.group_epoch);
    assert_eq!(status.voter_count, 3);
    assert!(!status.fail_stopped);

    let mut previous_index = LogIndex::ZERO;
    let mut proposal_ids = BTreeSet::new();
    for committed in adapter.applied_proposals() {
        assert!(committed.receipt.log_index > previous_index);
        assert!(committed.receipt.log_index <= status.applied_index);
        assert!(proposal_ids.insert(committed.receipt.proposal_id));
        assert_eq!(
            adapter.lookup_proposal(committed.receipt.proposal_id),
            ProposalLookup::Committed(committed.clone())
        );
        previous_index = committed.receipt.log_index;
    }
}

fn node(value: u64) -> NodeId {
    NodeId::new(value).unwrap()
}

fn peer(node_id: NodeId) -> PeerId {
    PeerId::new(node_id.get())
}

fn proposal(value: u64) -> ProposalId {
    ProposalId::new(value).unwrap()
}

fn voters() -> [NodeId; 3] {
    [node(1), node(2), node(3)]
}

fn group_id() -> GroupId {
    GroupId::new(7).unwrap()
}

fn group_epoch() -> GroupEpoch {
    GroupEpoch::new(1).unwrap()
}
