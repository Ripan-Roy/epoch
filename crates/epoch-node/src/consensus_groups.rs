//! Node-local routing for multiple consensus groups.
//!
//! A node exposes one bounded peer endpoint. Each frame is decoded exactly
//! once, fenced against the registered group epoch, and then forwarded to the
//! dedicated actor for that group without holding the registry lock.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::post,
};
use epoch_consensus::{
    ConsensusStatus, GroupEpoch, GroupId, MAX_PEER_MESSAGE_WIRE_BYTES, NodeId, PeerMessage,
};
use serde::Serialize;
use thiserror::Error;
use tokio::{sync::broadcast, task::JoinHandle};

use crate::consensus::{
    CommittedProposalApplier, ConsensusProbeApiError, ConsensusProbeConfig, ConsensusProbeError,
    ConsensusProbeHandle, ConsensusProbeRuntime, INTERNAL_PEER_MESSAGE_PATH,
};

const MAX_SUPERVISED_GROUPS: usize = 65_536;
const GROUP_FAILURE_NOTIFICATION_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RegisteredConsensusGroup {
    pub group_id: u64,
    pub group_epoch: u64,
}

#[derive(Debug, Clone, Error)]
pub enum ConsensusGroupRegistryError {
    #[error(transparent)]
    Consensus(#[from] ConsensusProbeError),
    #[error("consensus group {group_id} epoch {group_epoch} is already registered on this node")]
    AlreadyRegistered { group_id: u64, group_epoch: u64 },
    #[error("consensus handle belongs to node {observed}; registry belongs to node {expected}")]
    NodeMismatch { expected: u64, observed: u64 },
    #[error("consensus group {group_id} is not registered on this node")]
    UnknownGroup { group_id: u64 },
    #[error("consensus group {group_id} epoch {observed} is fenced by registered epoch {expected}")]
    FencedEpoch {
        group_id: u64,
        expected: u64,
        observed: u64,
    },
    #[error("consensus group registry lock is poisoned")]
    RegistryUnavailable,
}

pub type ConsensusGroupRegistryResult<T> = Result<T, ConsensusGroupRegistryError>;

#[derive(Debug)]
struct RegisteredGroup {
    epoch: GroupEpoch,
    handle: ConsensusProbeHandle,
}

/// Cloneable registry backing one shared peer endpoint on a node.
#[derive(Debug, Clone)]
pub struct ConsensusGroupRegistry {
    node_id: NodeId,
    groups: Arc<RwLock<BTreeMap<GroupId, RegisteredGroup>>>,
}

impl ConsensusGroupRegistry {
    pub fn new(node_id: u64) -> ConsensusGroupRegistryResult<Self> {
        let node_id = NodeId::new(node_id)
            .map_err(ConsensusProbeError::from)
            .map_err(ConsensusGroupRegistryError::from)?;
        Ok(Self {
            node_id,
            groups: Arc::new(RwLock::new(BTreeMap::new())),
        })
    }

    pub const fn node_id(&self) -> u64 {
        self.node_id.get()
    }

    pub fn register(&self, handle: ConsensusProbeHandle) -> ConsensusGroupRegistryResult<()> {
        if handle.node_id() != self.node_id {
            return Err(ConsensusGroupRegistryError::NodeMismatch {
                expected: self.node_id.get(),
                observed: handle.node_id().get(),
            });
        }
        let mut groups = self
            .groups
            .write()
            .map_err(|_| ConsensusGroupRegistryError::RegistryUnavailable)?;
        if let Some(existing) = groups.get(&handle.group_id()) {
            return Err(ConsensusGroupRegistryError::AlreadyRegistered {
                group_id: handle.group_id().get(),
                group_epoch: existing.epoch.get(),
            });
        }
        groups.insert(
            handle.group_id(),
            RegisteredGroup {
                epoch: handle.group_epoch(),
                handle,
            },
        );
        Ok(())
    }

    pub fn unregister(
        &self,
        group_id: u64,
        group_epoch: u64,
    ) -> ConsensusGroupRegistryResult<ConsensusProbeHandle> {
        let group_id = GroupId::new(group_id)
            .map_err(ConsensusProbeError::from)
            .map_err(ConsensusGroupRegistryError::from)?;
        let observed_epoch = GroupEpoch::new(group_epoch)
            .map_err(ConsensusProbeError::from)
            .map_err(ConsensusGroupRegistryError::from)?;
        let mut groups = self
            .groups
            .write()
            .map_err(|_| ConsensusGroupRegistryError::RegistryUnavailable)?;
        let registered =
            groups
                .get(&group_id)
                .ok_or(ConsensusGroupRegistryError::UnknownGroup {
                    group_id: group_id.get(),
                })?;
        if registered.epoch != observed_epoch {
            return Err(ConsensusGroupRegistryError::FencedEpoch {
                group_id: group_id.get(),
                expected: registered.epoch.get(),
                observed: observed_epoch.get(),
            });
        }
        groups
            .remove(&group_id)
            .map(|registered| registered.handle)
            .ok_or(ConsensusGroupRegistryError::UnknownGroup {
                group_id: group_id.get(),
            })
    }

    pub fn registered_groups(&self) -> ConsensusGroupRegistryResult<Vec<RegisteredConsensusGroup>> {
        self.groups
            .read()
            .map_err(|_| ConsensusGroupRegistryError::RegistryUnavailable)
            .map(|groups| {
                groups
                    .iter()
                    .map(|(group_id, registered)| RegisteredConsensusGroup {
                        group_id: group_id.get(),
                        group_epoch: registered.epoch.get(),
                    })
                    .collect()
            })
    }

    pub fn handles(&self) -> ConsensusGroupRegistryResult<Vec<ConsensusProbeHandle>> {
        self.groups
            .read()
            .map_err(|_| ConsensusGroupRegistryError::RegistryUnavailable)
            .map(|groups| {
                groups
                    .values()
                    .map(|registered| registered.handle.clone())
                    .collect()
            })
    }

    pub fn handle(
        &self,
        group_id: u64,
        group_epoch: u64,
    ) -> ConsensusGroupRegistryResult<ConsensusProbeHandle> {
        let group_id = GroupId::new(group_id)
            .map_err(ConsensusProbeError::from)
            .map_err(ConsensusGroupRegistryError::from)?;
        let observed_epoch = GroupEpoch::new(group_epoch)
            .map_err(ConsensusProbeError::from)
            .map_err(ConsensusGroupRegistryError::from)?;
        let groups = self
            .groups
            .read()
            .map_err(|_| ConsensusGroupRegistryError::RegistryUnavailable)?;
        let registered =
            groups
                .get(&group_id)
                .ok_or(ConsensusGroupRegistryError::UnknownGroup {
                    group_id: group_id.get(),
                })?;
        if registered.epoch != observed_epoch {
            return Err(ConsensusGroupRegistryError::FencedEpoch {
                group_id: group_id.get(),
                expected: registered.epoch.get(),
                observed: observed_epoch.get(),
            });
        }
        Ok(registered.handle.clone())
    }

    pub async fn receive_wire(
        &self,
        frame: &[u8],
    ) -> ConsensusGroupRegistryResult<ConsensusStatus> {
        let message = PeerMessage::from_wire(frame, self.node_id)
            .map_err(ConsensusProbeError::from)
            .map_err(ConsensusGroupRegistryError::from)?;
        let group_id = message.group_id();
        let observed_epoch = message.group_epoch();
        let handle = {
            let groups = self
                .groups
                .read()
                .map_err(|_| ConsensusGroupRegistryError::RegistryUnavailable)?;
            let registered =
                groups
                    .get(&group_id)
                    .ok_or(ConsensusGroupRegistryError::UnknownGroup {
                        group_id: group_id.get(),
                    })?;
            if registered.epoch != observed_epoch {
                return Err(ConsensusGroupRegistryError::FencedEpoch {
                    group_id: group_id.get(),
                    expected: registered.epoch.get(),
                    observed: observed_epoch.get(),
                });
            }
            registered.handle.clone()
        };
        handle
            .receive_message(message)
            .await
            .map_err(ConsensusGroupRegistryError::from)
    }
}

#[derive(Debug, Error)]
pub enum ConsensusGroupSupervisorError {
    #[error(transparent)]
    Registry(#[from] ConsensusGroupRegistryError),
    #[error(transparent)]
    Runtime(#[from] ConsensusProbeError),
    #[error("consensus group capacity must be between 1 and {MAX_SUPERVISED_GROUPS}")]
    InvalidCapacity,
    #[error("consensus group capacity {capacity} is exhausted")]
    CapacityExhausted { capacity: usize },
    #[error("consensus group {group_id} is not supervised by this node")]
    UnknownGroup { group_id: u64 },
    #[error("consensus group {group_id} epoch {observed} is fenced by supervised epoch {expected}")]
    FencedEpoch {
        group_id: u64,
        expected: u64,
        observed: u64,
    },
    #[error("consensus group shutdown encountered errors: {0}")]
    Shutdown(String),
}

pub type ConsensusGroupSupervisorResult<T> = Result<T, ConsensusGroupSupervisorError>;

#[derive(Debug)]
struct SupervisedConsensusGroup {
    epoch: GroupEpoch,
    runtime: ConsensusProbeRuntime,
    failure_monitor: JoinHandle<()>,
}

#[derive(Debug, Clone, Error)]
#[error("consensus group {group_id} epoch {group_epoch} failed: {error}")]
pub struct SupervisedConsensusGroupFailure {
    pub group_id: u64,
    pub group_epoch: u64,
    #[source]
    pub error: ConsensusProbeError,
}

/// Owns the lifecycle of every consensus actor hosted by one node process.
#[derive(Debug)]
pub struct ConsensusGroupSupervisor {
    registry: ConsensusGroupRegistry,
    max_groups: usize,
    groups: BTreeMap<GroupId, SupervisedConsensusGroup>,
    failures: broadcast::Sender<SupervisedConsensusGroupFailure>,
}

impl ConsensusGroupSupervisor {
    pub fn new(node_id: u64, max_groups: usize) -> ConsensusGroupSupervisorResult<Self> {
        if max_groups == 0 || max_groups > MAX_SUPERVISED_GROUPS {
            return Err(ConsensusGroupSupervisorError::InvalidCapacity);
        }
        let (failures, _) = broadcast::channel(GROUP_FAILURE_NOTIFICATION_CAPACITY);
        Ok(Self {
            registry: ConsensusGroupRegistry::new(node_id)?,
            max_groups,
            groups: BTreeMap::new(),
            failures,
        })
    }

    pub fn registry(&self) -> ConsensusGroupRegistry {
        self.registry.clone()
    }

    pub const fn max_groups(&self) -> usize {
        self.max_groups
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn handles(&self) -> Vec<ConsensusProbeHandle> {
        self.groups
            .values()
            .map(|group| group.runtime.handle())
            .collect()
    }

    pub fn subscribe_failures(&self) -> broadcast::Receiver<SupervisedConsensusGroupFailure> {
        self.failures.subscribe()
    }

    pub async fn start_group(
        &mut self,
        config: ConsensusProbeConfig,
        stable_path: impl AsRef<Path>,
        applier: Option<Arc<dyn CommittedProposalApplier>>,
    ) -> ConsensusGroupSupervisorResult<ConsensusProbeHandle> {
        if config.node_id().get() != self.registry.node_id() {
            return Err(ConsensusGroupRegistryError::NodeMismatch {
                expected: self.registry.node_id(),
                observed: config.node_id().get(),
            }
            .into());
        }
        if let Some(existing) = self.groups.get(&config.group_id()) {
            return Err(ConsensusGroupRegistryError::AlreadyRegistered {
                group_id: config.group_id().get(),
                group_epoch: existing.epoch.get(),
            }
            .into());
        }
        if self.groups.len() >= self.max_groups {
            return Err(ConsensusGroupSupervisorError::CapacityExhausted {
                capacity: self.max_groups,
            });
        }

        let runtime = if let Some(applier) = applier {
            ConsensusProbeRuntime::start_with_profile_applier(config, stable_path, applier).await?
        } else {
            ConsensusProbeRuntime::start(config, stable_path).await?
        };
        let handle = runtime.handle();
        if let Err(error) = self.registry.register(handle.clone()) {
            let shutdown_error = runtime.shutdown().await.err();
            return Err(match shutdown_error {
                Some(shutdown_error) => ConsensusGroupSupervisorError::Shutdown(format!(
                    "{error}; rollback: {shutdown_error}"
                )),
                None => error.into(),
            });
        }
        let mut actor_failure = runtime.subscribe_actor_failure();
        let failures = self.failures.clone();
        let group_id = handle.group_id().get();
        let group_epoch = handle.group_epoch().get();
        let failure_monitor = tokio::spawn(async move {
            loop {
                if let Some(error) = actor_failure.borrow().clone() {
                    let _ = failures.send(SupervisedConsensusGroupFailure {
                        group_id,
                        group_epoch,
                        error,
                    });
                    return;
                }
                if actor_failure.changed().await.is_err() {
                    return;
                }
            }
        });
        self.groups.insert(
            handle.group_id(),
            SupervisedConsensusGroup {
                epoch: handle.group_epoch(),
                runtime,
                failure_monitor,
            },
        );
        Ok(handle)
    }

    pub async fn stop_group(
        &mut self,
        group_id: u64,
        group_epoch: u64,
    ) -> ConsensusGroupSupervisorResult<()> {
        let group_id = GroupId::new(group_id)
            .map_err(ConsensusProbeError::from)
            .map_err(ConsensusGroupSupervisorError::from)?;
        let observed_epoch = GroupEpoch::new(group_epoch)
            .map_err(ConsensusProbeError::from)
            .map_err(ConsensusGroupSupervisorError::from)?;
        let supervised =
            self.groups
                .get(&group_id)
                .ok_or(ConsensusGroupSupervisorError::UnknownGroup {
                    group_id: group_id.get(),
                })?;
        if supervised.epoch != observed_epoch {
            return Err(ConsensusGroupSupervisorError::FencedEpoch {
                group_id: group_id.get(),
                expected: supervised.epoch.get(),
                observed: observed_epoch.get(),
            });
        }

        self.registry
            .unregister(group_id.get(), observed_epoch.get())?;
        let supervised =
            self.groups
                .remove(&group_id)
                .ok_or(ConsensusGroupSupervisorError::UnknownGroup {
                    group_id: group_id.get(),
                })?;
        supervised.failure_monitor.abort();
        let _ = supervised.failure_monitor.await;
        supervised.runtime.shutdown().await?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> ConsensusGroupSupervisorResult<()> {
        let groups = self
            .groups
            .iter()
            .map(|(group_id, group)| (group_id.get(), group.epoch.get()))
            .collect::<Vec<_>>();
        let mut errors = Vec::new();
        for (group_id, group_epoch) in groups {
            if let Err(error) = self.stop_group(group_id, group_epoch).await {
                errors.push(format!("{group_id}/{group_epoch}: {error}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConsensusGroupSupervisorError::Shutdown(errors.join("; ")))
        }
    }
}

/// Internal-only shared transport surface. It intentionally has no CORS layer.
pub fn shared_internal_peer_router(registry: ConsensusGroupRegistry) -> Router {
    Router::new()
        .route(INTERNAL_PEER_MESSAGE_PATH, post(receive_peer_message))
        .layer(DefaultBodyLimit::max(MAX_PEER_MESSAGE_WIRE_BYTES))
        .with_state(registry)
}

async fn receive_peer_message(
    State(registry): State<ConsensusGroupRegistry>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ConsensusGroupRegistryApiError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if content_type != Some("application/octet-stream") {
        return Err(ConsensusProbeError::UnsupportedPeerContentType.into());
    }
    registry.receive_wire(&body).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug)]
struct ConsensusGroupRegistryApiError(ConsensusGroupRegistryError);

impl From<ConsensusGroupRegistryError> for ConsensusGroupRegistryApiError {
    fn from(error: ConsensusGroupRegistryError) -> Self {
        Self(error)
    }
}

impl From<ConsensusProbeError> for ConsensusGroupRegistryApiError {
    fn from(error: ConsensusProbeError) -> Self {
        Self(error.into())
    }
}

#[derive(Debug, Serialize)]
struct RegistryErrorBody {
    code: &'static str,
    message: String,
}

impl IntoResponse for ConsensusGroupRegistryApiError {
    fn into_response(self) -> Response {
        match self.0 {
            ConsensusGroupRegistryError::Consensus(error) => {
                ConsensusProbeApiError::from(error).into_response()
            }
            error @ ConsensusGroupRegistryError::UnknownGroup { .. } => (
                StatusCode::NOT_FOUND,
                Json(RegistryErrorBody {
                    code: "unknown_consensus_group",
                    message: error.to_string(),
                }),
            )
                .into_response(),
            error @ ConsensusGroupRegistryError::FencedEpoch { .. } => (
                StatusCode::CONFLICT,
                Json(RegistryErrorBody {
                    code: "fenced_group_epoch",
                    message: error.to_string(),
                }),
            )
                .into_response(),
            error @ (ConsensusGroupRegistryError::AlreadyRegistered { .. }
            | ConsensusGroupRegistryError::NodeMismatch { .. }) => (
                StatusCode::CONFLICT,
                Json(RegistryErrorBody {
                    code: "consensus_group_conflict",
                    message: error.to_string(),
                }),
            )
                .into_response(),
            ConsensusGroupRegistryError::RegistryUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(RegistryErrorBody {
                    code: "consensus_registry_unavailable",
                    message: ConsensusGroupRegistryError::RegistryUnavailable.to_string(),
                }),
            )
                .into_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{body::Body, http::Request};
    use epoch_consensus::{CommittedProposal, ConsensusAdapter, InMemoryRaftAdapter};
    use tempfile::TempDir;
    use tower::ServiceExt;
    use url::Url;

    use super::*;
    use crate::consensus::{ConsensusProbeConfig, ConsensusProbeRuntime};

    #[derive(Debug)]
    struct RejectingReplayApplier;

    impl CommittedProposalApplier for RejectingReplayApplier {
        fn replay(&self, _committed: &[CommittedProposal]) -> Result<(), String> {
            Err("injected replay failure".into())
        }

        fn apply(&self, _committed: &CommittedProposal) -> Result<(), String> {
            Ok(())
        }
    }

    fn peer_url(port: u16) -> Url {
        Url::parse(&format!("http://127.0.0.1:{port}/")).expect("test peer URL should parse")
    }

    async fn runtime(group_id: u64) -> (TempDir, ConsensusProbeRuntime) {
        let directory = TempDir::new().expect("temp directory should be created");
        let config = ConsensusProbeConfig::new(
            2,
            group_id,
            1,
            [
                (1, peer_url(39_001)),
                (2, peer_url(39_002)),
                (3, peer_url(39_003)),
            ],
            Duration::from_mins(1),
        )
        .expect("group config should be valid");
        let runtime = ConsensusProbeRuntime::start(config, directory.path().join("raft.wal"))
            .await
            .expect("group runtime should start");
        (directory, runtime)
    }

    fn campaign_frame(group_id: u64, group_epoch: u64) -> Vec<u8> {
        let voters = [
            NodeId::new(1).expect("valid node"),
            NodeId::new(2).expect("valid node"),
            NodeId::new(3).expect("valid node"),
        ];
        let mut sender = InMemoryRaftAdapter::new(
            voters[0],
            GroupId::new(group_id).expect("valid group"),
            GroupEpoch::new(group_epoch).expect("valid epoch"),
            voters,
        )
        .expect("sender should start");
        sender
            .campaign()
            .expect("campaign should emit peer frames")
            .messages
            .into_iter()
            .find(|message| message.to() == voters[1])
            .expect("campaign should target node two")
            .to_wire()
            .expect("peer frame should encode")
    }

    #[tokio::test]
    async fn registry_routes_by_group_without_cross_delivery_and_fences_epochs() {
        let (_first_directory, first) = runtime(77).await;
        let (_second_directory, second) = runtime(88).await;
        let registry = ConsensusGroupRegistry::new(2).expect("registry should be valid");
        registry
            .register(first.handle())
            .expect("first group should register");
        registry
            .register(second.handle())
            .expect("second group should register");

        let second_before = second
            .handle()
            .status()
            .await
            .expect("second group should answer");
        let routed = registry
            .receive_wire(&campaign_frame(77, 1))
            .await
            .expect("known group frame should route");
        let second_after = second
            .handle()
            .status()
            .await
            .expect("second group should still answer");
        assert_eq!(routed.group_id.get(), 77);
        assert_eq!(second_after.term, second_before.term);
        assert_eq!(second_after.role, second_before.role);

        assert!(matches!(
            registry.receive_wire(&campaign_frame(99, 1)).await,
            Err(ConsensusGroupRegistryError::UnknownGroup { group_id: 99 })
        ));
        assert!(matches!(
            registry.receive_wire(&campaign_frame(77, 2)).await,
            Err(ConsensusGroupRegistryError::FencedEpoch {
                group_id: 77,
                expected: 1,
                observed: 2
            })
        ));

        first.shutdown().await.expect("first group should stop");
        second.shutdown().await.expect("second group should stop");
    }

    #[tokio::test]
    async fn registry_rejects_duplicate_and_wrong_node_registration() {
        let (_directory, runtime) = runtime(77).await;
        let local = ConsensusGroupRegistry::new(2).expect("registry should be valid");
        local
            .register(runtime.handle())
            .expect("group should register once");
        assert!(matches!(
            local.register(runtime.handle()),
            Err(ConsensusGroupRegistryError::AlreadyRegistered {
                group_id: 77,
                group_epoch: 1
            })
        ));

        let wrong_node = ConsensusGroupRegistry::new(3).expect("registry should be valid");
        assert!(matches!(
            wrong_node.register(runtime.handle()),
            Err(ConsensusGroupRegistryError::NodeMismatch {
                expected: 3,
                observed: 2
            })
        ));
        assert_eq!(
            local
                .unregister(77, 1)
                .expect("matching epoch should unregister")
                .group_id()
                .get(),
            77
        );
        assert!(local.registered_groups().unwrap().is_empty());
        runtime.shutdown().await.expect("group should stop");
    }

    #[tokio::test]
    async fn shared_peer_endpoint_reports_unknown_groups_and_content_type() {
        let registry = ConsensusGroupRegistry::new(2).expect("registry should be valid");
        let router = shared_internal_peer_router(registry);

        let unsupported = router
            .clone()
            .oneshot(
                Request::post(INTERNAL_PEER_MESSAGE_PATH)
                    .body(Body::from(campaign_frame(99, 1)))
                    .expect("request should build"),
            )
            .await
            .expect("router should answer");
        assert_eq!(unsupported.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let unknown = router
            .oneshot(
                Request::post(INTERNAL_PEER_MESSAGE_PATH)
                    .header(CONTENT_TYPE, "application/octet-stream; charset=binary")
                    .body(Body::from(campaign_frame(99, 1)))
                    .expect("request should build"),
            )
            .await
            .expect("router should answer");
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn supervisor_caps_groups_rolls_back_failed_start_and_stops_independently() {
        let first_directory = TempDir::new().expect("temp directory should be created");
        let failed_directory = TempDir::new().expect("temp directory should be created");
        let second_directory = TempDir::new().expect("temp directory should be created");
        let mut supervisor =
            ConsensusGroupSupervisor::new(2, 2).expect("supervisor should be valid");

        let first = supervisor
            .start_group(
                group_config(77),
                first_directory.path().join("raft.wal"),
                None,
            )
            .await
            .expect("first group should start");
        assert_eq!(first.group_id().get(), 77);

        let failed = supervisor
            .start_group(
                group_config(88),
                failed_directory.path().join("raft.wal"),
                Some(Arc::new(RejectingReplayApplier)),
            )
            .await;
        assert!(matches!(
            failed,
            Err(ConsensusGroupSupervisorError::Runtime(
                ConsensusProbeError::ProfileApplication(ref message)
            )) if message == "injected replay failure"
        ));
        assert_eq!(supervisor.group_count(), 1);
        assert_eq!(
            first
                .status()
                .await
                .expect("first group should survive")
                .group_id,
            GroupId::new(77).unwrap()
        );

        let second = supervisor
            .start_group(
                group_config(88),
                second_directory.path().join("raft.wal"),
                None,
            )
            .await
            .expect("second group should start after rollback");
        assert_eq!(supervisor.group_count(), 2);

        let capacity_directory = TempDir::new().expect("temp directory should be created");
        assert!(matches!(
            supervisor
                .start_group(
                    group_config(99),
                    capacity_directory.path().join("raft.wal"),
                    None
                )
                .await,
            Err(ConsensusGroupSupervisorError::CapacityExhausted { capacity: 2 })
        ));

        supervisor
            .stop_group(77, 1)
            .await
            .expect("first group should stop independently");
        assert_eq!(supervisor.group_count(), 1);
        assert_eq!(
            second
                .status()
                .await
                .expect("second group should remain healthy")
                .group_id,
            GroupId::new(88).unwrap()
        );
        assert_eq!(
            supervisor.registry().registered_groups().unwrap(),
            vec![RegisteredConsensusGroup {
                group_id: 88,
                group_epoch: 1
            }]
        );
        supervisor
            .shutdown()
            .await
            .expect("remaining group should drain");
    }

    #[tokio::test]
    async fn supervisor_reports_one_actor_failure_without_hiding_healthy_groups() {
        let failed_directory = TempDir::new().expect("temp directory should be created");
        let healthy_directory = TempDir::new().expect("temp directory should be created");
        let mut supervisor =
            ConsensusGroupSupervisor::new(2, 2).expect("supervisor should be valid");
        let mut failures = supervisor.subscribe_failures();
        let failed = supervisor
            .start_group(
                group_config(77),
                failed_directory.path().join("raft.wal"),
                None,
            )
            .await
            .expect("first group should start");
        let healthy = supervisor
            .start_group(
                group_config(88),
                healthy_directory.path().join("raft.wal"),
                None,
            )
            .await
            .expect("second group should start");

        let injected = failed
            .inject_failure(ConsensusProbeError::ProfileApplication(
                "injected supervised failure".into(),
            ))
            .await;
        assert!(matches!(
            injected,
            Err(ConsensusProbeError::ProfileApplication(ref message))
                if message == "injected supervised failure"
        ));
        let failure = tokio::time::timeout(Duration::from_secs(1), failures.recv())
            .await
            .expect("failure should be published promptly")
            .expect("failure channel should remain open");
        assert_eq!(failure.group_id, 77);
        assert_eq!(failure.group_epoch, 1);
        assert!(failure.to_string().contains("injected supervised failure"));
        assert_eq!(
            healthy
                .status()
                .await
                .expect("unrelated group should remain available")
                .group_id,
            GroupId::new(88).unwrap()
        );

        assert!(
            supervisor.shutdown().await.is_err(),
            "shutdown should preserve the failed group's root cause"
        );
    }

    fn group_config(group_id: u64) -> ConsensusProbeConfig {
        ConsensusProbeConfig::new(
            2,
            group_id,
            1,
            [
                (1, peer_url(39_001)),
                (2, peer_url(39_002)),
                (3, peer_url(39_003)),
            ],
            Duration::from_mins(1),
        )
        .expect("group config should be valid")
    }
}
