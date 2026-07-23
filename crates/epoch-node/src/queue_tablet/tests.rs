use std::{path::PathBuf, sync::Arc, time::Duration};

use epoch_consensus::{
    CommitReceipt, ConsensusRole, ConsensusStatus, GroupEpoch, GroupId, LogIndex, NodeId,
    ProposalId, Term,
};
use epoch_core::{EventEnvelope, ManualClock};
use epoch_tablet::{QueueTabletOperationResult, QueueTabletOutcome};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};
use url::Url;

use super::*;
use crate::consensus::{ConsensusProbeConfig, ConsensusProbeRuntime};

fn scope() -> QueueTabletScope {
    QueueTabletScope::new(7, 3, "jobs").unwrap()
}

fn event(message_id: &str) -> EventEnvelope {
    let mut envelope = EventEnvelope::new("tests", "job.created", json!({"id": message_id}), 1);
    envelope.id = message_id.to_owned();
    envelope
}

fn committed(
    key: &str,
    operation: QueueTabletOperation,
    applied_at_ms: u64,
    term: u64,
    index: u64,
) -> CommittedProposal {
    let scope = scope();
    let command =
        QueueTabletCommand::new(&scope, key, applied_at_ms, operation).expect("valid command");
    CommittedProposal {
        receipt: CommitReceipt {
            group_id: GroupId::new(7).unwrap(),
            group_epoch: GroupEpoch::new(3).unwrap(),
            proposal_id: ProposalId::new(command.proposal_id(&scope).unwrap()).unwrap(),
            term: Term::new(term),
            log_index: LogIndex::new(index),
        },
        payload: command.encode(&scope).unwrap(),
    }
}

fn enqueue(key: &str, message_id: &str, applied_at_ms: u64, index: u64) -> CommittedProposal {
    committed(
        key,
        QueueTabletOperation::Enqueue(Box::new(QueueEnqueueCommand {
            partition: 0,
            envelope: event(message_id),
        })),
        applied_at_ms,
        2,
        index,
    )
}

#[test]
fn recovery_rebuilds_queue_profile_before_exposing_it() {
    let service = QueueTabletService::with_default_config(scope()).unwrap();
    let acquire = committed(
        "acquire-1",
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker-a".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(100),
        }),
        11,
        2,
        5,
    );
    service
        .replay(&[acquire.clone(), enqueue("enqueue-1", "job-1", 10, 4)])
        .unwrap();

    assert_eq!(service.last_profile_mutation_index().unwrap(), 5);
    assert_eq!(service.last_applied_time_ms().unwrap(), 11);
    let snapshot = service.snapshot().unwrap();
    assert_eq!(snapshot.applied_command_count, 2);
    assert_eq!(snapshot.counts.in_flight, 1);
    let receipt = service.committed_receipt(&acquire).unwrap();
    assert!(matches!(
        receipt.outcome,
        QueueTabletOutcome::Applied {
            result: QueueTabletOperationResult::Acquired { .. }
        }
    ));
}

#[test]
fn recovery_normalizes_descending_assigned_times_in_committed_log_order() {
    let first = enqueue("pending-before-failover", "job-1", 1_000, 4);
    let after_failover = committed(
        "lower-clock-new-leader",
        QueueTabletOperation::Maintain(QueueMaintainCommand { partition: 0 }),
        500,
        3,
        5,
    );
    let live = QueueTabletService::with_default_config(scope()).unwrap();
    live.apply(&first).unwrap();
    live.apply(&after_failover).unwrap();
    let live_snapshot = live.snapshot().unwrap();

    let recovered = QueueTabletService::with_default_config(scope()).unwrap();
    recovered
        .replay(&[after_failover.clone(), first])
        .expect("committed log order must define a non-regressing effective time");

    let snapshot = recovered.snapshot().unwrap();
    assert_eq!(snapshot.last_profile_mutation_index, 5);
    assert_eq!(snapshot.last_applied_time_ms, 1_000);
    assert_eq!(snapshot.applied_command_count, 2);
    assert_eq!(snapshot.state_digest, live_snapshot.state_digest);
    assert_eq!(snapshot.counts, live_snapshot.counts);
    assert_eq!(
        recovered
            .committed_receipt(&after_failover)
            .unwrap()
            .applied_at_ms,
        1_000
    );
    assert_eq!(
        recovered.committed_receipt(&after_failover).unwrap(),
        live.committed_receipt(&after_failover).unwrap()
    );
}

#[test]
fn malformed_commit_fail_stops_queue_profile_reads_and_future_apply() {
    let service = QueueTabletService::with_default_config(scope()).unwrap();
    let mut malformed = enqueue("enqueue-1", "job-1", 10, 4);
    malformed.payload = b"not a Queue command".to_vec();

    assert!(service.apply(&malformed).is_err());
    assert!(service.snapshot().is_err());
    assert!(
        service
            .apply(&enqueue("enqueue-2", "job-2", 11, 5))
            .is_err()
    );
}

#[test]
fn exact_live_commit_is_applied_only_once() {
    let service = QueueTabletService::with_default_config(scope()).unwrap();
    let command = enqueue("enqueue-1", "job-1", 10, 4);

    service.apply(&command).unwrap();
    service.apply(&command).unwrap();

    let snapshot = service.snapshot().unwrap();
    assert_eq!(snapshot.applied_command_count, 1);
    assert_eq!(snapshot.counts.ready, 1);
}

#[test]
fn committed_lookup_cannot_apply_a_commit_the_actor_missed() {
    let service = QueueTabletService::with_default_config(scope()).unwrap();

    assert!(
        service
            .committed_receipt(&enqueue("enqueue-1", "job-1", 10, 4))
            .is_err()
    );
    assert!(service.snapshot().is_err());
}

#[test]
fn mutation_request_accepts_decimal_u64_fields_and_strict_envelopes() {
    let document = json!({
        "idempotency_key": "enqueue-1",
        "expected_term": u64::MAX.to_string(),
        "operation": {
            "kind": "enqueue",
            "partition": 0,
            "envelope": {
                "id": "job-1",
                "source": "tests",
                "type": "job.created",
                "time_ms": u64::MAX.to_string(),
                "deliver_at_ms": (u64::MAX - 1).to_string(),
                "ttl_ms": "1",
                "payload": {"id": 1}
            }
        }
    });
    let request: QueueMutationRequest = serde_json::from_value(document).unwrap();

    assert_eq!(request.expected_term, u64::MAX);
    let QueueTabletOperation::Enqueue(enqueue) = request.operation.to_tablet_operation() else {
        panic!("expected enqueue");
    };
    assert_eq!(enqueue.envelope.time_ms, u64::MAX);
    assert_eq!(enqueue.envelope.deliver_at_ms, Some(u64::MAX - 1));

    let unknown_nested = json!({
        "idempotency_key": "enqueue-1",
        "expected_term": "1",
        "operation": {
            "kind": "enqueue",
            "envelope": {
                "id": "job-1",
                "source": "tests",
                "type": "job.created",
                "time_ms": "1",
                "payload": {},
                "paylod": "typo"
            }
        }
    });
    assert!(serde_json::from_value::<QueueMutationRequest>(unknown_nested).is_err());
}

#[test]
fn lease_and_history_inputs_accept_decimal_u64_fields() {
    let acquire: QueueMutationRequest = serde_json::from_value(json!({
        "idempotency_key": "acquire-1",
        "expected_term": 2,
        "operation": {
            "kind": "acquire",
            "consumer": "worker-a",
            "consumer_epoch": u64::MAX.to_string(),
            "max_messages": 1,
            "visibility_timeout_ms": (u64::MAX - 1).to_string()
        }
    }))
    .unwrap();
    let QueueTabletOperation::Acquire(acquire) = acquire.operation.to_tablet_operation() else {
        panic!("expected acquire");
    };
    assert_eq!(acquire.consumer_epoch, u64::MAX);
    assert_eq!(acquire.visibility_timeout_ms, Some(u64::MAX - 1));

    let redrive: QueueMutationRequest = serde_json::from_value(json!({
        "idempotency_key": "redrive-1",
        "expected_term": "2",
        "operation": {
            "kind": "redrive",
            "message_id": "job-1",
            "dead_letter_history_id": u64::MAX.to_string()
        }
    }))
    .unwrap();
    let QueueTabletOperation::Redrive(redrive) = redrive.operation.to_tablet_operation() else {
        panic!("expected redrive");
    };
    assert_eq!(redrive.dead_letter_history_id, u64::MAX);
}

#[test]
fn request_identity_ignores_only_expected_term_and_server_time() {
    let request: QueueMutationRequest = serde_json::from_value(json!({
        "idempotency_key": "enqueue-1",
        "expected_term": "999",
        "operation": {
            "kind": "enqueue",
            "envelope": {
                "id": "job-1",
                "source": "tests",
                "type": "job.created",
                "time_ms": "1",
                "payload": {"id": 1}
            }
        }
    }))
    .unwrap();
    let command = QueueTabletCommand::new(
        &scope(),
        "enqueue-1",
        123_456,
        request.operation.to_tablet_operation(),
    )
    .unwrap();
    let lookup = ProposalLookup::Pending {
        payload: command.encode(&scope()).unwrap(),
    };

    validate_existing_request(&lookup, &scope(), &request).unwrap();

    let mut conflict: Value = serde_json::to_value(json!({
        "idempotency_key": "enqueue-1",
        "expected_term": "999",
        "operation": {
            "kind": "enqueue",
            "envelope": {
                "id": "job-1",
                "source": "tests",
                "type": "job.created",
                "time_ms": "1",
                "payload": {"id": 2}
            }
        }
    }))
    .unwrap();
    conflict["expected_term"] = json!(1);
    let conflict: QueueMutationRequest = serde_json::from_value(conflict).unwrap();
    assert!(matches!(
        validate_existing_request(&lookup, &scope(), &conflict),
        Err(TabletApiError::IdempotencyConflict)
    ));
}

#[test]
fn queue_status_rejects_profile_ahead_and_serializes_all_u64_as_strings() {
    let consensus = ConsensusStatus {
        node_id: NodeId::new(u64::MAX).unwrap(),
        group_id: GroupId::new(7).unwrap(),
        group_epoch: GroupEpoch::new(3).unwrap(),
        role: ConsensusRole::Leader,
        leader_id: Some(NodeId::new(u64::MAX - 1).unwrap()),
        term: Term::new(u64::MAX),
        commit_index: LogIndex::new(u64::MAX),
        applied_index: LogIndex::new(u64::MAX - 1),
        voter_count: 3,
        fail_stopped: false,
    };
    let snapshot = QueueTabletSnapshot {
        last_profile_mutation_index: u64::MAX - 2,
        last_applied_time_ms: u64::MAX - 3,
        applied_command_count: u64::MAX - 4,
        counts: QueueTabletCounts {
            ready: u64::MAX,
            scheduled: 0,
            in_flight: 0,
            acknowledged: 0,
            expired: 0,
            dead_lettered: 0,
        },
        state_digest: "00".repeat(32),
    };
    let document =
        serde_json::to_value(QueueTabletStatus::new(&scope(), &consensus, snapshot).unwrap())
            .unwrap();
    for field in [
        "tablet_id",
        "tablet_epoch",
        "node_id",
        "leader_id",
        "term",
        "consensus_commit_index",
        "consensus_applied_index",
        "last_profile_mutation_index",
        "last_applied_time_ms",
        "applied_command_count",
    ] {
        assert!(document[field].is_string(), "{field}: {document}");
    }
    assert!(document["counts"]["ready"].is_string());

    let ahead = QueueTabletSnapshot {
        last_profile_mutation_index: u64::MAX,
        last_applied_time_ms: 0,
        applied_command_count: 0,
        counts: QueueTabletCounts {
            ready: 0,
            scheduled: 0,
            in_flight: 0,
            acknowledged: 0,
            expired: 0,
            dead_lettered: 0,
        },
        state_digest: "00".repeat(32),
    };
    assert!(QueueTabletStatus::new(&scope(), &consensus, ahead).is_err());
}

#[test]
fn mutation_ids_are_browser_safe_decimal_strings() {
    let document = serde_json::to_value(QueueTabletMutationResponse::pending(u64::MAX)).unwrap();
    assert_eq!(document["proposal_id"], u64::MAX.to_string());
}

struct RunningQueueNode {
    runtime: ConsensusProbeRuntime,
    server: JoinHandle<()>,
    base_url: Url,
    clock: Arc<ManualClock>,
}

struct RunningQueueCluster {
    nodes: Vec<RunningQueueNode>,
}

impl RunningQueueCluster {
    async fn start(paths: &[PathBuf], wall_time_ms: u64) -> Self {
        let mut listeners = Vec::new();
        for _ in 0..3 {
            listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let urls = listeners
            .iter()
            .map(|listener| {
                Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap()
            })
            .collect::<Vec<_>>();
        let mut nodes = Vec::new();
        for (index, (listener, stable_path)) in listeners.into_iter().zip(paths.iter()).enumerate()
        {
            if let Some(parent) = stable_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            let node_id = u64::try_from(index).unwrap() + 1;
            let config = ConsensusProbeConfig::new(
                node_id,
                7,
                3,
                urls.iter()
                    .enumerate()
                    .map(|(peer, url)| (u64::try_from(peer).unwrap() + 1, url.clone())),
                Duration::from_millis(20),
            )
            .unwrap();
            let service = QueueTabletService::with_default_config(scope()).unwrap();
            let applier: Arc<dyn CommittedProposalApplier> = service.clone();
            let runtime =
                ConsensusProbeRuntime::start_with_profile_applier(config, stable_path, applier)
                    .await
                    .unwrap();
            let clock = Arc::new(ManualClock::new(wall_time_ms));
            let app = runtime.internal_router().merge(router(
                service,
                runtime.handle(),
                clock.clone(),
                Duration::from_secs(2),
            ));
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            nodes.push(RunningQueueNode {
                runtime,
                server,
                base_url: urls[index].clone(),
                clock,
            });
        }
        Self { nodes }
    }

    async fn leader(&self) -> (usize, u64) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                for (index, node) in self.nodes.iter().enumerate() {
                    if let Ok(status) = node.runtime.handle().status().await
                        && status.role == ConsensusRole::Leader
                    {
                        return (index, status.term.get());
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("fixed-voter Queue cluster should elect a leader")
    }

    async fn shutdown(self) {
        for node in &self.nodes {
            node.server.abort();
        }
        for node in self.nodes {
            let _ = node.server.await;
            node.runtime.shutdown().await.unwrap();
        }
    }
}

fn tablet_paths(root: &std::path::Path) -> Vec<PathBuf> {
    (1..=3)
        .map(|node_id| root.join(format!("node-{node_id}.wal")))
        .collect()
}

fn mutation_url(node: &RunningQueueNode) -> Url {
    node.base_url
        .join(EXPERIMENTAL_QUEUE_TABLET_MUTATIONS_PATH.trim_start_matches('/'))
        .unwrap()
}

async fn post_to_leader(
    cluster: &RunningQueueCluster,
    client: &reqwest::Client,
    body: &Value,
) -> (StatusCode, Value) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let (leader, term) = cluster.leader().await;
            let mut attempt = body.clone();
            attempt["expected_term"] = json!(term.to_string());
            let response = client
                .post(mutation_url(&cluster.nodes[leader]))
                .json(&attempt)
                .send()
                .await
                .unwrap();
            let status = response.status();
            let document: Value = response.json().await.unwrap();
            if (status == StatusCode::SERVICE_UNAVAILABLE
                && document["error"]["code"] == "not_leader")
                || (status == StatusCode::CONFLICT && document["error"]["code"] == "stale_term")
                || (status == StatusCode::ACCEPTED && document["outcome_certainty"] == "unknown")
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            }
            return (status, document);
        }
    })
    .await
    .expect("Queue mutation should resolve under stable leadership")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_queue_tablet_commits_retries_converges_and_rebuilds() {
    let temporary = TempDir::new().unwrap();
    let paths = tablet_paths(temporary.path());
    let cluster = RunningQueueCluster::start(&paths, 1_000).await;
    let client = reqwest::Client::new();
    assert_follower_and_invalid_request(&cluster, &client).await;
    exercise_enqueue_acquire_ack(&cluster, &client).await;
    exercise_retry_dead_letter_and_redrive(&cluster, &client).await;
    let digest = converged_digest(&cluster, &client).await;
    cluster.shutdown().await;

    let reopened = RunningQueueCluster::start(&paths, 2_000).await;
    assert_rebuilt(&reopened, &client, &digest).await;
    reopened.shutdown().await;
}

async fn assert_follower_and_invalid_request(
    cluster: &RunningQueueCluster,
    client: &reqwest::Client,
) {
    let invalid = json!({
        "idempotency_key": "invalid-1",
        "expected_term": "1",
        "operation": {
            "kind": "enqueue",
            "envelope": {
                "id": "invalid",
                "source": "tests",
                "type": "job.created",
                "time_ms": "1",
                "payload": {},
                "paylod": "typo"
            }
        }
    });
    let response = client
        .post(mutation_url(&cluster.nodes[0]))
        .json(&invalid)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let document: Value = response.json().await.unwrap();
    assert_eq!(document["error"]["code"], "invalid_request");
    assert_eq!(
        document["error"]["outcome_certainty"],
        "definite_not_committed"
    );

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (leader, term) = cluster.leader().await;
            let follower = (leader + 1) % cluster.nodes.len();
            let request = json!({
                "idempotency_key": "follower-1",
                "expected_term": term.to_string(),
                "operation": {"kind": "maintain"}
            });
            let response = client
                .post(mutation_url(&cluster.nodes[follower]))
                .json(&request)
                .send()
                .await
                .unwrap();
            let status = response.status();
            let document: Value = response.json().await.unwrap();
            if status == StatusCode::SERVICE_UNAVAILABLE
                && document["error"]["code"] == "not_leader"
            {
                assert_eq!(document["error"]["outcome_certainty"], "unknown");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a stable follower should reject a Queue write");
}

async fn exercise_retry_dead_letter_and_redrive(
    cluster: &RunningQueueCluster,
    client: &reqwest::Client,
) {
    let enqueue = json!({
        "idempotency_key": "enqueue-2",
        "expected_term": "0",
        "operation": {
            "kind": "enqueue",
            "envelope": {
                "id": "job-2",
                "source": "tests",
                "type": "job.created",
                "time_ms": "900",
                "payload": {"id": 2}
            }
        }
    });
    let (_, enqueued) = post_to_leader(cluster, client, &enqueue).await;
    assert_eq!(operation_result(&enqueued)["kind"], "enqueued");

    let first_token = acquire_one(cluster, client, "acquire-2", "worker-b").await;
    let renewed_token = extend_and_replay(cluster, client, &first_token).await;
    release(cluster, client, &renewed_token).await;
    let released_token = acquire_one(cluster, client, "acquire-3", "worker-b").await;
    nack(cluster, client, &released_token).await;

    for node in &cluster.nodes {
        node.clock.set_wall_time_ms(5_000);
    }
    let maintain = json!({
        "idempotency_key": "maintain-1",
        "expected_term": "0",
        "operation": {"kind": "maintain"}
    });
    let (_, maintained) = post_to_leader(cluster, client, &maintain).await;
    assert_eq!(operation_result(&maintained)["kind"], "maintained");

    let retry_token = acquire_one(cluster, client, "acquire-4", "worker-b").await;
    let dead_letter_history_id = reject(cluster, client, &retry_token).await;
    redrive(cluster, client, dead_letter_history_id).await;
    let redriven_token = acquire_one(cluster, client, "acquire-5", "worker-b").await;
    acknowledge(cluster, client, "ack-2", "worker-b", &redriven_token).await;
}

async fn acquire_one(
    cluster: &RunningQueueCluster,
    client: &reqwest::Client,
    idempotency_key: &str,
    consumer: &str,
) -> String {
    let acquire = json!({
        "idempotency_key": idempotency_key,
        "expected_term": "0",
        "operation": {
            "kind": "acquire",
            "consumer": consumer,
            "consumer_epoch": "1",
            "max_messages": 1,
            "visibility_timeout_ms": "1000"
        }
    });
    let (_, acquired) = post_to_leader(cluster, client, &acquire).await;
    let result = operation_result(&acquired);
    assert_eq!(result["kind"], "acquired");
    assert_eq!(result["deliveries"].as_array().unwrap().len(), 1);
    result["deliveries"][0]["lease_token"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn extend_and_replay(
    cluster: &RunningQueueCluster,
    client: &reqwest::Client,
    token: &str,
) -> String {
    let extend = json!({
        "idempotency_key": "extend-1",
        "expected_term": "0",
        "operation": {
            "kind": "extend_lease",
            "consumer": "worker-b",
            "consumer_epoch": "1",
            "lease_token": token,
            "extension_ms": "2000"
        }
    });
    let (_, extended) = post_to_leader(cluster, client, &extend).await;
    assert_eq!(
        operation_result(&extended)["kind"],
        "lease_extended",
        "{extended}"
    );
    let renewed_token = operation_result(&extended)["lease_token"]
        .as_str()
        .unwrap()
        .to_owned();
    let (retry_status, replayed) = post_to_leader(cluster, client, &extend).await;
    assert_eq!(retry_status, StatusCode::OK);
    assert_eq!(replayed["receipt"]["disposition"], "replayed");
    assert_eq!(operation_result(&replayed)["lease_token"], renewed_token);
    renewed_token
}

async fn release(cluster: &RunningQueueCluster, client: &reqwest::Client, token: &str) {
    let release = json!({
        "idempotency_key": "release-1",
        "expected_term": "0",
        "operation": {
            "kind": "release",
            "consumer": "worker-b",
            "consumer_epoch": "1",
            "lease_token": token,
            "delay_ms": "0",
            "reason": "yield"
        }
    });
    let (_, released) = post_to_leader(cluster, client, &release).await;
    assert_eq!(operation_result(&released)["kind"], "released");
}

async fn nack(cluster: &RunningQueueCluster, client: &reqwest::Client, token: &str) {
    let nack = json!({
        "idempotency_key": "nack-1",
        "expected_term": "0",
        "operation": {
            "kind": "nack",
            "consumer": "worker-b",
            "consumer_epoch": "1",
            "lease_token": token,
            "reason": "retry"
        }
    });
    let (_, nacked) = post_to_leader(cluster, client, &nack).await;
    assert_eq!(operation_result(&nacked)["kind"], "nacked");
}

async fn reject(cluster: &RunningQueueCluster, client: &reqwest::Client, token: &str) -> u64 {
    let reject = json!({
        "idempotency_key": "reject-1",
        "expected_term": "0",
        "operation": {
            "kind": "reject",
            "consumer": "worker-b",
            "consumer_epoch": "1",
            "lease_token": token,
            "reason": "poison"
        }
    });
    let (_, rejected) = post_to_leader(cluster, client, &reject).await;
    let result = operation_result(&rejected);
    assert_eq!(result["kind"], "dead_lettered");
    result["dead_letter_history_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

async fn redrive(
    cluster: &RunningQueueCluster,
    client: &reqwest::Client,
    dead_letter_history_id: u64,
) {
    let redrive = json!({
        "idempotency_key": "redrive-1",
        "expected_term": "0",
        "operation": {
            "kind": "redrive",
            "message_id": "job-2",
            "dead_letter_history_id": dead_letter_history_id.to_string()
        }
    });
    let (_, redriven) = post_to_leader(cluster, client, &redrive).await;
    assert_eq!(operation_result(&redriven)["kind"], "redriven");
}

async fn acknowledge(
    cluster: &RunningQueueCluster,
    client: &reqwest::Client,
    idempotency_key: &str,
    consumer: &str,
    token: &str,
) {
    let acknowledge = json!({
        "idempotency_key": idempotency_key,
        "expected_term": "0",
        "operation": {
            "kind": "acknowledge",
            "consumer": consumer,
            "consumer_epoch": "1",
            "lease_token": token
        }
    });
    let (_, acknowledged) = post_to_leader(cluster, client, &acknowledge).await;
    assert_eq!(operation_result(&acknowledged)["kind"], "acknowledged");
}

fn operation_result(document: &Value) -> &Value {
    &document["receipt"]["outcome"]["result"]
}

async fn exercise_enqueue_acquire_ack(cluster: &RunningQueueCluster, client: &reqwest::Client) {
    let enqueue = json!({
        "idempotency_key": "enqueue-1",
        "expected_term": "0",
        "operation": {
            "kind": "enqueue",
            "envelope": {
                "id": "job-1",
                "source": "tests",
                "type": "job.created",
                "time_ms": "900",
                "payload": {"id": 1}
            }
        }
    });

    let (status, first) = post_to_leader(cluster, client, &enqueue).await;
    assert!(matches!(status, StatusCode::CREATED | StatusCode::OK));
    assert_eq!(first["state"], "committed");
    assert_eq!(first["receipt"]["outcome"]["result"]["kind"], "enqueued");
    assert_eq!(first["receipt"]["applied_at_ms"], "1000");
    assert_eq!(first["receipt"]["durable_voter_acks"], 2);

    let (retry_status, retry) = post_to_leader(cluster, client, &enqueue).await;
    assert_eq!(retry_status, StatusCode::OK);
    assert_eq!(retry["receipt"]["disposition"], "replayed");
    assert_eq!(
        retry["receipt"]["proposal_id"],
        first["receipt"]["proposal_id"]
    );

    let mut conflict = enqueue.clone();
    conflict["operation"]["envelope"]["payload"] = json!({"id": 99});
    let (conflict_status, conflict) = post_to_leader(cluster, client, &conflict).await;
    assert_eq!(conflict_status, StatusCode::CONFLICT);
    assert_eq!(conflict["error"]["code"], "idempotency_conflict");

    for node in &cluster.nodes {
        node.clock.set_wall_time_ms(500);
    }
    let acquire = json!({
        "idempotency_key": "acquire-1",
        "expected_term": "0",
        "operation": {
            "kind": "acquire",
            "consumer": "worker-a",
            "consumer_epoch": "1",
            "max_messages": 1,
            "visibility_timeout_ms": "100"
        }
    });
    let (_, acquired) = post_to_leader(cluster, client, &acquire).await;
    assert_eq!(acquired["receipt"]["applied_at_ms"], "1000");
    let token = acquired["receipt"]["outcome"]["result"]["deliveries"][0]["lease_token"]
        .as_str()
        .unwrap()
        .to_owned();
    acknowledge(cluster, client, "ack-1", "worker-a", &token).await;

    let stale_ack = json!({
        "idempotency_key": "stale-ack-1",
        "expected_term": "0",
        "operation": {
            "kind": "acknowledge",
            "consumer": "worker-a",
            "consumer_epoch": "1",
            "lease_token": token
        }
    });
    let (status, rejected) = post_to_leader(cluster, client, &stale_ack).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(rejected["outcome_certainty"], "committed");
    assert_eq!(rejected["receipt"]["outcome"]["status"], "rejected");
}

async fn converged_digest(cluster: &RunningQueueCluster, client: &reqwest::Client) -> Value {
    let mut reference_digest = None;
    for node in &cluster.nodes {
        let status = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status: Value = client
                    .get(
                        node.base_url
                            .join(EXPERIMENTAL_QUEUE_TABLET_STATUS_PATH.trim_start_matches('/'))
                            .unwrap(),
                    )
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                if status["applied_command_count"] == "16" {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("every voter should profile-apply the acknowledged message");
        assert_eq!(status["applied_command_count"], "16");
        assert_eq!(status["counts"]["acknowledged"], "2");
        assert_eq!(status["counts"]["dead_lettered"], "0");
        assert_queue_read_routes(node, client).await;
        if let Some(reference) = &reference_digest {
            assert_eq!(&status["state_digest"], reference);
        } else {
            reference_digest = Some(status["state_digest"].clone());
        }
    }
    reference_digest.unwrap()
}

async fn assert_rebuilt(
    cluster: &RunningQueueCluster,
    client: &reqwest::Client,
    expected_digest: &Value,
) {
    for node in &cluster.nodes {
        let status: Value = client
            .get(
                node.base_url
                    .join(EXPERIMENTAL_QUEUE_TABLET_STATUS_PATH.trim_start_matches('/'))
                    .unwrap(),
            )
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(status["applied_command_count"], "16");
        assert_eq!(status["counts"]["acknowledged"], "2");
        assert_eq!(status["counts"]["dead_lettered"], "0");
        assert_eq!(&status["state_digest"], expected_digest);
        assert_queue_read_routes(node, client).await;
    }
}

async fn assert_queue_read_routes(node: &RunningQueueNode, client: &reqwest::Client) {
    let counts: Value = client
        .get(
            node.base_url
                .join(EXPERIMENTAL_QUEUE_TABLET_COUNTS_PATH.trim_start_matches('/'))
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(counts["observation_scope"], "local");
    assert_eq!(counts["counts"]["acknowledged"], "2");

    let dead_letters: Value = client
        .get(
            node.base_url
                .join(EXPERIMENTAL_QUEUE_TABLET_DEAD_LETTERS_PATH.trim_start_matches('/'))
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dead_letters["records"].as_array().unwrap().len(), 1);
    assert_eq!(
        dead_letters["records"][0]["dead_letter"]["message_id"],
        "job-2"
    );
    assert!(dead_letters["records"][0]["history_id"].is_string());

    let redrives: Value = client
        .get(
            node.base_url
                .join(EXPERIMENTAL_QUEUE_TABLET_REDRIVES_PATH.trim_start_matches('/'))
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(redrives["records"].as_array().unwrap().len(), 1);
    assert_eq!(redrives["records"][0]["message_id"], "job-2");
    assert!(redrives["records"][0]["history_id"].is_string());

    let proposal_id = queue_proposal_id_for(&scope(), "enqueue-1").unwrap();
    let mutation: Value = client
        .get(
            node.base_url
                .join(&format!(
                    "experimental/v1/tablets/queue/mutations/{proposal_id}"
                ))
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mutation["state"], "committed");
    assert_eq!(mutation["outcome_certainty"], "committed");
}
