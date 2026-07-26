use std::{path::PathBuf, sync::Arc, time::Duration};

use epoch_consensus::{
    CommitReceipt, ConsensusRole, ConsensusStatus, GroupEpoch, GroupId, LogIndex, NodeId,
    ProposalId, Term,
};
use epoch_tablet::{CacheTabletOperationResult, CacheTabletOutcome, CacheTabletWriteEvidence};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};

use super::*;
use crate::consensus::{ConsensusProbeConfig, ConsensusProbeRuntime};

fn scope() -> CacheTabletScope {
    CacheTabletScope::new(7, 3, "sessions").unwrap()
}

fn committed(
    key: &str,
    operation: CacheTabletOperation,
    applied_at_ms: u64,
    term: u64,
    index: u64,
) -> CommittedProposal {
    let scope = scope();
    let command =
        CacheTabletCommand::new(&scope, key, applied_at_ms, operation).expect("valid command");
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

fn set_counter(key: &str, value: i64, applied_at_ms: u64, index: u64) -> CommittedProposal {
    committed(
        key,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "visits".into(),
            value: CacheValue::Counter(value),
            ttl_ms: None,
            lock_guard: None,
        }),
        applied_at_ms,
        2,
        index,
    )
}

#[test]
fn recovery_rebuilds_cache_profile_in_commit_order_before_exposing_it() {
    let set = set_counter("set-1", 1, 1_000, 4);
    let increment = committed(
        "increment-1",
        CacheTabletOperation::Increment(CacheIncrementCommand {
            shard: 0,
            key: "visits".into(),
            delta: 2,
            expected_version: Some(1),
            ttl_ms: None,
            lock_guard: None,
        }),
        500,
        3,
        5,
    );
    let service = CacheTabletService::with_default_config(scope()).unwrap();

    service
        .replay(&[increment.clone(), set.clone()])
        .expect("replay must sort the committed history");

    let snapshot = service.snapshot().unwrap();
    assert_eq!(snapshot.last_profile_mutation_index, 5);
    assert_eq!(snapshot.last_applied_time_ms, 1_000);
    assert_eq!(snapshot.applied_command_count, 2);
    assert_eq!(snapshot.cache_revision, 2);
    assert_eq!(snapshot.retained_entry_count, 1);
    let observation = service.observe("visits").unwrap();
    assert_eq!(observation.observed_at_ms, 1_000);
    assert_eq!(observation.item.unwrap().value, CacheValue::Counter(3));
    assert!(matches!(
        service.committed_receipt(&increment).unwrap().outcome,
        CacheTabletOutcome::Applied {
            result: CacheTabletOperationResult::Incremented { value: 3, .. }
        }
    ));

    let live = CacheTabletService::with_default_config(scope()).unwrap();
    live.apply(&set).unwrap();
    live.apply(&increment).unwrap();
    assert_eq!(snapshot.state_digest, live.snapshot().unwrap().state_digest);
}

#[test]
fn malformed_commit_fail_stops_cache_reads_and_future_apply() {
    let service = CacheTabletService::with_default_config(scope()).unwrap();
    let mut malformed = set_counter("set-1", 1, 10, 4);
    malformed.payload = b"not a Cache command".to_vec();

    assert!(service.apply(&malformed).is_err());
    assert!(service.snapshot().is_err());
    assert!(service.observe("visits").is_err());
    assert!(service.apply(&set_counter("set-2", 2, 11, 5)).is_err());
}

#[test]
fn exact_live_commit_is_applied_only_once() {
    let service = CacheTabletService::with_default_config(scope()).unwrap();
    let command = set_counter("set-1", 1, 10, 4);

    service.apply(&command).unwrap();
    service.apply(&command).unwrap();

    let snapshot = service.snapshot().unwrap();
    assert_eq!(snapshot.applied_command_count, 1);
    assert_eq!(snapshot.cache_revision, 1);
    assert_eq!(snapshot.retained_entry_count, 1);
}

#[test]
fn committed_lookup_cannot_apply_a_commit_the_actor_missed() {
    let service = CacheTabletService::with_default_config(scope()).unwrap();

    assert!(
        service
            .committed_receipt(&set_counter("set-1", 1, 10, 4))
            .is_err()
    );
    assert!(service.snapshot().is_err());
}

#[test]
fn request_dtos_accept_decimal_extrema_for_value_mutations() {
    let set: CacheMutationRequest = serde_json::from_value(json!({
        "idempotency_key": "set-1",
        "expected_term": u64::MAX.to_string(),
        "operation": {
            "kind": "set",
            "key": "counter",
            "value": {"kind": "counter", "value": i64::MIN.to_string()},
            "ttl_ms": "1"
        }
    }))
    .unwrap();
    assert_eq!(set.expected_term, u64::MAX);
    assert!(matches!(
        set.operation.to_tablet_operation().unwrap(),
        CacheTabletOperation::Set(CacheSetCommand {
            value: CacheValue::Counter(i64::MIN),
            ttl_ms: Some(1),
            ..
        })
    ));

    let operations = [
        json!({
            "idempotency_key": "delete-1",
            "expected_term": 1,
            "operation": {"kind": "delete", "key": "a", "expected_version": "2"}
        }),
        json!({
            "idempotency_key": "cas-1",
            "expected_term": "1",
            "operation": {
                "kind": "compare_and_set",
                "key": "a",
                "expected": {"kind": "missing", "shard_revision": u64::MAX.to_string()},
                "value": {"kind": "string", "value": "ready"}
            }
        }),
        json!({
            "idempotency_key": "increment-1",
            "expected_term": "1",
            "operation": {
                "kind": "increment",
                "key": "a",
                "delta": i64::MAX.to_string(),
                "expected_version": "2"
            }
        }),
    ]
    .map(operation_from);
    assert!(matches!(operations[0], CacheTabletOperation::Delete(_)));
    assert!(matches!(
        operations[1],
        CacheTabletOperation::CompareAndSet(_)
    ));
    assert!(matches!(operations[2], CacheTabletOperation::Increment(_)));
}

#[test]
fn request_dtos_cover_transactions_locks_and_maintenance() {
    let operations = [
        json!({
            "idempotency_key": "transaction-1",
            "expected_term": "1",
            "operation": {
                "kind": "transaction",
                "expected_revision": "2",
                "mutations": [
                    {"kind": "set", "key": "a", "value": {"kind": "blob", "value": [0, 255]}},
                    {"kind": "increment", "key": "b", "delta": "-1"}
                ],
                "lock_guards": [{
                    "lock_key": "guard",
                    "owner": "worker",
                    "owner_epoch": u64::MAX.to_string(),
                    "lease_token": "token"
                }]
            }
        }),
        json!({
            "idempotency_key": "acquire-1",
            "expected_term": "1",
            "operation": {
                "kind": "acquire_lock",
                "lock_key": "guard",
                "owner": "worker",
                "owner_epoch": "3",
                "lease_ms": "100"
            }
        }),
        json!({
            "idempotency_key": "renew-1",
            "expected_term": "1",
            "operation": {
                "kind": "renew_lock",
                "lock_key": "guard",
                "owner": "worker",
                "owner_epoch": "3",
                "lease_token": "token",
                "extension_ms": "100"
            }
        }),
        json!({
            "idempotency_key": "release-1",
            "expected_term": "1",
            "operation": {
                "kind": "release_lock",
                "lock_key": "guard",
                "owner": "worker",
                "owner_epoch": "3",
                "lease_token": "token"
            }
        }),
        json!({
            "idempotency_key": "maintain-1",
            "expected_term": "1",
            "operation": {"kind": "maintain", "max_expirations": 1000}
        }),
    ]
    .map(operation_from);
    assert!(matches!(
        operations[0],
        CacheTabletOperation::Transaction(_)
    ));
    assert!(matches!(
        operations[1],
        CacheTabletOperation::AcquireLock(_)
    ));
    assert!(matches!(operations[2], CacheTabletOperation::RenewLock(_)));
    assert!(matches!(
        operations[3],
        CacheTabletOperation::ReleaseLock(_)
    ));
    assert!(matches!(operations[4], CacheTabletOperation::Maintain(_)));
}

#[test]
fn value_dtos_cover_every_bounded_collection_family() {
    let values = [
        json!({"kind": "hash", "value": {"field": "value"}}),
        json!({"kind": "list", "value": ["first", "second"]}),
        json!({"kind": "set", "value": ["first", "second"]}),
        json!({"kind": "sorted_set", "value": {"first": 1.5, "second": -2.0}}),
    ]
    .map(|document| {
        serde_json::from_value::<CacheValueRequest>(document)
            .unwrap()
            .to_cache_value()
            .unwrap()
    });

    assert!(matches!(values[0], CacheValue::Hash(_)));
    assert!(matches!(values[1], CacheValue::List(_)));
    assert!(matches!(values[2], CacheValue::Set(_)));
    assert!(matches!(values[3], CacheValue::SortedSet(_)));
}

fn operation_from(document: Value) -> CacheTabletOperation {
    serde_json::from_value::<CacheMutationRequest>(document)
        .unwrap()
        .operation
        .to_tablet_operation()
        .unwrap()
}

#[test]
fn request_dtos_reject_unknown_fields_and_ambiguous_collections() {
    let unknown_nested = json!({
        "idempotency_key": "set-1",
        "expected_term": "1",
        "operation": {
            "kind": "set",
            "key": "a",
            "value": {"kind": "string", "value": "ready", "typo": true}
        }
    });
    assert!(serde_json::from_value::<CacheMutationRequest>(unknown_nested).is_err());

    let duplicate_map = r#"{
        "idempotency_key":"set-1",
        "expected_term":"1",
        "operation":{
            "kind":"set",
            "key":"a",
            "value":{"kind":"hash","value":{"field":"one","field":"two"}}
        }
    }"#;
    assert!(serde_json::from_str::<CacheMutationRequest>(duplicate_map).is_err());

    let duplicate_set: CacheMutationRequest = serde_json::from_value(json!({
        "idempotency_key": "set-1",
        "expected_term": "1",
        "operation": {
            "kind": "set",
            "key": "a",
            "value": {"kind": "set", "value": ["member", "member"]}
        }
    }))
    .unwrap();
    assert!(matches!(
        duplicate_set.operation.to_tablet_operation(),
        Err(TabletApiError::InvalidRequest(_))
    ));
}

#[test]
fn request_identity_ignores_only_expected_term_and_server_time() {
    let request: CacheMutationRequest = serde_json::from_value(json!({
        "idempotency_key": "set-1",
        "expected_term": "999",
        "operation": {
            "kind": "set",
            "key": "a",
            "value": {"kind": "string", "value": "one"}
        }
    }))
    .unwrap();
    let command = CacheTabletCommand::new(
        &scope(),
        "set-1",
        123_456,
        request.operation.to_tablet_operation().unwrap(),
    )
    .unwrap();
    let lookup = ProposalLookup::Pending {
        payload: command.encode(&scope()).unwrap(),
    };

    validate_existing_request(&lookup, &scope(), &request).unwrap();

    let conflict: CacheMutationRequest = serde_json::from_value(json!({
        "idempotency_key": "set-1",
        "expected_term": "1",
        "operation": {
            "kind": "set",
            "key": "a",
            "value": {"kind": "string", "value": "two"}
        }
    }))
    .unwrap();
    assert!(matches!(
        validate_existing_request(&lookup, &scope(), &conflict),
        Err(TabletApiError::IdempotencyConflict)
    ));
}

#[test]
fn status_is_browser_safe_and_truthful_about_retained_storage() {
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
    let snapshot = CacheTabletSnapshot {
        last_profile_mutation_index: u64::MAX - 2,
        last_applied_time_ms: u64::MAX - 3,
        applied_command_count: u64::MAX - 4,
        cache_revision: u64::MAX - 5,
        retained_entry_count: u64::MAX - 6,
        active_lock_count: u64::MAX - 7,
        cache_recovery_state_digest: "11".repeat(32),
        state_digest: "22".repeat(32),
    };
    let status = CacheTabletStatus::new(&scope(), &consensus, snapshot).unwrap();
    let document = serde_json::to_value(status).unwrap();
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
        "cache_revision",
        "retained_entry_count",
        "active_lock_count",
    ] {
        assert!(document[field].is_string(), "{field}: {document}");
    }
    assert_eq!(document["capability"], "single_shard_cache_tablet");
    assert_eq!(document["stability"], "experimental");
    assert_eq!(document["production_readiness"], "not_production_ready");
    assert_eq!(
        document["write_guarantee"],
        "fixed_three_voter_majority_persisted_then_local_profile_applied"
    );
    assert_eq!(
        document["read_consistency"],
        "local_profile_applied_stale_capable"
    );
    assert_eq!(document["linearizable_read_barrier"], false);
    assert!(document.get("durability").is_none());
    assert!(document.get("active_entry_count").is_none());

    let ahead = CacheTabletSnapshot {
        last_profile_mutation_index: u64::MAX,
        last_applied_time_ms: 0,
        applied_command_count: 0,
        cache_revision: 0,
        retained_entry_count: 0,
        active_lock_count: 0,
        cache_recovery_state_digest: "00".repeat(32),
        state_digest: "00".repeat(32),
    };
    assert!(CacheTabletStatus::new(&scope(), &consensus, ahead).is_err());
}

#[test]
fn observation_and_mutation_metadata_are_browser_safe_and_local() {
    let service = CacheTabletService::with_default_config(scope()).unwrap();
    service
        .apply(&set_counter("set-1", i64::MAX, 10, 4))
        .unwrap();
    let response = CacheTabletObservationResponse {
        observation_scope: "local",
        read_consistency: "local_profile_applied_stale_capable",
        linearizable_read_barrier: false,
        observation: service.observe("visits").unwrap(),
    };
    let observation = serde_json::to_value(response).unwrap();
    assert_eq!(observation["observation_scope"], "local");
    assert_eq!(observation["linearizable_read_barrier"], false);
    assert!(observation["observation"]["shard_revision"].is_string());
    assert!(observation["observation"]["observed_at_ms"].is_string());
    assert!(observation["observation"]["item"]["version"].is_string());
    assert_eq!(
        observation["observation"]["item"]["value"]["value"],
        i64::MAX.to_string()
    );

    let mutation = serde_json::to_value(CacheTabletMutationResponse::pending(u64::MAX)).unwrap();
    assert_eq!(mutation["proposal_id"], u64::MAX.to_string());
    assert_eq!(mutation["observation_scope"], "local");
}

#[test]
fn observation_key_validation_matches_the_tablet_key_boundary() {
    assert!(validate_observation_key("session/user-1").is_ok());
    assert!(validate_observation_key("").is_err());
    assert!(validate_observation_key(" \t").is_err());
    assert!(validate_observation_key("line\nbreak").is_err());
    assert!(validate_observation_key(&"x".repeat(MAX_CACHE_KEY_BYTES + 1)).is_err());
}

#[test]
fn missing_observation_returns_revision_instead_of_a_not_found_error() {
    let service = CacheTabletService::with_default_config(scope()).unwrap();
    service.apply(&set_counter("set-1", 1, 10, 4)).unwrap();

    let missing = service.observe("missing").unwrap();
    assert_eq!(missing.shard_revision, 1);
    assert_eq!(missing.observed_at_ms, 10);
    assert!(missing.item.is_none());
}

#[test]
fn receipt_replay_changes_only_the_http_disposition() {
    let service = CacheTabletService::with_default_config(scope()).unwrap();
    let command = set_counter("set-1", 1, 10, 4);
    service.apply(&command).unwrap();
    let original = service.committed_receipt(&command).unwrap();
    let replayed = receipt_for_response(original.clone(), true);

    assert_eq!(original.disposition, CacheTabletDisposition::New);
    assert_eq!(replayed.disposition, CacheTabletDisposition::Replayed);
    let mut original_document: Value = serde_json::to_value(original).unwrap();
    let mut replayed_document: Value = serde_json::to_value(replayed).unwrap();
    original_document["disposition"] = Value::Null;
    replayed_document["disposition"] = Value::Null;
    assert_eq!(original_document, replayed_document);
}

struct RunningCacheNode {
    runtime: ConsensusProbeRuntime,
    server: JoinHandle<()>,
    service: Arc<CacheTabletService>,
}

struct RunningCacheCluster {
    nodes: Vec<RunningCacheNode>,
}

impl RunningCacheCluster {
    async fn start(paths: &[PathBuf]) -> Self {
        let mut listeners = Vec::new();
        for _ in 0..3 {
            listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let urls = listeners
            .iter()
            .map(|listener| {
                url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap()
            })
            .collect::<Vec<_>>();
        let mut nodes = Vec::new();
        for (index, (listener, stable_path)) in listeners.into_iter().zip(paths).enumerate() {
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
            let service = CacheTabletService::with_default_config(scope()).unwrap();
            let applier: Arc<dyn CommittedProposalApplier> = service.clone();
            let runtime =
                ConsensusProbeRuntime::start_with_profile_applier(config, stable_path, applier)
                    .await
                    .unwrap();
            let app = runtime.internal_router();
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            nodes.push(RunningCacheNode {
                runtime,
                server,
                service,
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
        .expect("fixed-voter Cache cluster should elect a leader")
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

#[derive(Debug, Clone, PartialEq)]
struct CacheRuntimeEvidence {
    observation: CacheTabletObservation,
    receipts: Vec<CacheTabletReceipt>,
    cache_recovery_state_digest: String,
    state_digest: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_three_voter_cache_history_converges_and_reopens_from_eprs() {
    let temporary = TempDir::new().unwrap();
    let paths = cache_runtime_paths(temporary.path());
    let cluster = RunningCacheCluster::start(&paths).await;
    let commits = commit_fenced_cache_history(&cluster).await;
    let evidence = converged_cache_evidence(&cluster, &commits).await;
    cluster.shutdown().await;

    let reopened = RunningCacheCluster::start(&paths).await;
    wait_for_cache_apply(&reopened, commits.len()).await;
    assert_eq!(
        converged_cache_evidence(&reopened, &commits).await,
        evidence
    );
    reopened.shutdown().await;
}

fn cache_runtime_paths(root: &std::path::Path) -> Vec<PathBuf> {
    (1..=3)
        .map(|node_id| root.join(format!("node-{node_id}.wal")))
        .collect()
}

async fn commit_fenced_cache_history(cluster: &RunningCacheCluster) -> Vec<CommittedProposal> {
    let acquisition = commit_cache_operation(
        cluster,
        "acquire-1",
        1_000,
        CacheTabletOperation::AcquireLock(CacheAcquireLockCommand {
            shard: 0,
            lock_key: "session-guard".into(),
            owner: "worker-a".into(),
            owner_epoch: 1,
            lease_ms: 1_000,
        }),
    )
    .await;
    wait_for_cache_apply(cluster, 1).await;
    let guard = acquired_guard(
        &cluster.nodes[0]
            .service
            .committed_receipt(&acquisition)
            .unwrap(),
    );
    let set = commit_cache_operation(
        cluster,
        "set-1",
        1_001,
        CacheTabletOperation::Set(CacheSetCommand {
            shard: 0,
            key: "session".into(),
            value: CacheValue::String("one".into()),
            ttl_ms: None,
            lock_guard: Some(guard.clone()),
        }),
    )
    .await;
    let compared = commit_cache_operation(
        cluster,
        "cas-1",
        1_002,
        CacheTabletOperation::CompareAndSet(CacheCompareAndSetCommand {
            shard: 0,
            key: "session".into(),
            expected: CacheCasExpectation::Version { version: 1 },
            value: CacheValue::String("two".into()),
            ttl_ms: None,
            lock_guard: Some(guard.clone()),
        }),
    )
    .await;
    let released = commit_cache_operation(
        cluster,
        "release-1",
        1_003,
        CacheTabletOperation::ReleaseLock(CacheReleaseLockCommand {
            shard: 0,
            lock_key: guard.lock_key.clone(),
            owner: guard.owner.clone(),
            owner_epoch: guard.owner_epoch,
            lease_token: guard.lease_token.clone(),
        }),
    )
    .await;
    let commits = vec![acquisition, set, compared, released];
    wait_for_cache_apply(cluster, commits.len()).await;
    commits
}

fn acquired_guard(receipt: &CacheTabletReceipt) -> CacheLockGuard {
    let CacheTabletOutcome::Applied {
        result:
            CacheTabletOperationResult::LockAcquired {
                lock_key,
                owner,
                owner_epoch,
                lease_token,
                ..
            },
    } = &receipt.outcome
    else {
        panic!("lock acquisition must return a guardable token");
    };
    CacheLockGuard {
        lock_key: lock_key.clone(),
        owner: owner.clone(),
        owner_epoch: *owner_epoch,
        lease_token: lease_token.clone(),
    }
}

async fn commit_cache_operation(
    cluster: &RunningCacheCluster,
    idempotency_key: &str,
    applied_at_ms: u64,
    operation: CacheTabletOperation,
) -> CommittedProposal {
    let (leader, term) = cluster.leader().await;
    let command = CacheTabletCommand::new(&scope(), idempotency_key, applied_at_ms, operation)
        .expect("typed Cache command must validate");
    let proposal_id = command.proposal_id(&scope()).unwrap();
    cluster.nodes[leader]
        .runtime
        .handle()
        .propose(proposal_id, term, command.encode(&scope()).unwrap())
        .await
        .expect("leader must accept the typed Cache command");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let ProposalLookup::Committed(committed) = cluster.nodes[leader]
                .runtime
                .handle()
                .lookup(proposal_id)
                .await
                .unwrap()
            {
                return committed;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("typed Cache command must reach majority commit")
}

async fn wait_for_cache_apply(cluster: &RunningCacheCluster, expected_count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if cluster.nodes.iter().all(|node| {
                node.service
                    .snapshot()
                    .is_ok_and(|snapshot| snapshot.applied_command_count == expected_count as u64)
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("every voter must apply the committed Cache history");
}

async fn converged_cache_evidence(
    cluster: &RunningCacheCluster,
    commits: &[CommittedProposal],
) -> CacheRuntimeEvidence {
    wait_for_cache_apply(cluster, commits.len()).await;
    let mut reference = None;
    for node in &cluster.nodes {
        let snapshot = node.service.snapshot().unwrap();
        assert_eq!(snapshot.cache_revision, 2);
        assert_eq!(snapshot.retained_entry_count, 1);
        assert_eq!(snapshot.active_lock_count, 0);
        let evidence = CacheRuntimeEvidence {
            observation: node.service.observe("session").unwrap(),
            receipts: commits
                .iter()
                .map(|committed| node.service.committed_receipt(committed).unwrap())
                .collect(),
            cache_recovery_state_digest: snapshot.cache_recovery_state_digest,
            state_digest: snapshot.state_digest,
        };
        assert_majority_evidence(&evidence);
        if let Some(reference) = &reference {
            assert_eq!(&evidence, reference);
        } else {
            reference = Some(evidence);
        }
    }
    let evidence = reference.unwrap();
    assert_eq!(evidence.observation.shard_revision, 2);
    assert_eq!(evidence.observation.observed_at_ms, 1_003);
    assert_eq!(
        evidence.observation.item.as_ref().unwrap().value,
        CacheValue::String("two".into())
    );
    evidence
}

fn assert_majority_evidence(evidence: &CacheRuntimeEvidence) {
    for receipt in &evidence.receipts {
        assert_eq!(
            receipt.write_evidence,
            CacheTabletWriteEvidence::FixedVoterMajorityPersisted
        );
        assert_eq!(receipt.durable_voter_acks, 2);
        assert!(matches!(
            receipt.outcome,
            CacheTabletOutcome::Applied { .. }
        ));
    }
}
