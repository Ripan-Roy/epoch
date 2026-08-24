//! Internal mTLS-only inventory and leadership-drain operations.

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use epoch_consensus::{ConsensusMembership, ConsensusPeerProgress, ConsensusRole};
use serde::{Deserialize, Serialize};

use crate::{
    consensus::{ConsensusProbeError, ConsensusProbeHandle},
    consensus_groups::{ConsensusGroupRegistry, ConsensusGroupRegistryError},
};

pub const INTERNAL_MAINTENANCE_GROUPS_PATH: &str = "/internal/v1/maintenance/groups";
pub const INTERNAL_MAINTENANCE_TRANSFER_PATH: &str =
    "/internal/v1/maintenance/groups/{group_id}/leadership";
const MAX_MAINTENANCE_REQUEST_BYTES: usize = 4 * 1024;

pub fn regional_maintenance_router(registry: ConsensusGroupRegistry) -> Router {
    Router::new()
        .route(INTERNAL_MAINTENANCE_GROUPS_PATH, get(group_inventory))
        .route(
            INTERNAL_MAINTENANCE_TRANSFER_PATH,
            post(transfer_group_leadership),
        )
        .layer(DefaultBodyLimit::max(MAX_MAINTENANCE_REQUEST_BYTES))
        .with_state(registry)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceInventory {
    pub node_id: u64,
    pub groups: Vec<MaintenanceGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceGroup {
    pub group_id: u64,
    pub group_epoch: u64,
    pub role: MaintenanceRole,
    pub leader_id: Option<u64>,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub checkpoint_index: u64,
    pub fail_stopped: bool,
    pub membership: MaintenanceMembership,
    pub replication_progress: Vec<MaintenancePeerProgress>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceRole {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

impl From<ConsensusRole> for MaintenanceRole {
    fn from(role: ConsensusRole) -> Self {
        match role {
            ConsensusRole::Follower => Self::Follower,
            ConsensusRole::PreCandidate => Self::PreCandidate,
            ConsensusRole::Candidate => Self::Candidate,
            ConsensusRole::Leader => Self::Leader,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceMembership {
    pub allowed_members: Vec<u64>,
    pub voters: Vec<u64>,
    pub outgoing_voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub staged_learners: Vec<u64>,
    pub auto_leave: bool,
}

impl From<&ConsensusMembership> for MaintenanceMembership {
    fn from(membership: &ConsensusMembership) -> Self {
        let ids = |members: &[epoch_consensus::NodeId]| {
            members.iter().map(|node_id| node_id.get()).collect()
        };
        Self {
            allowed_members: ids(&membership.allowed_members),
            voters: ids(&membership.voters),
            outgoing_voters: ids(&membership.outgoing_voters),
            learners: ids(&membership.learners),
            staged_learners: ids(&membership.staged_learners),
            auto_leave: membership.auto_leave,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaintenancePeerProgress {
    pub node_id: u64,
    pub matched_index: u64,
    pub committed_index: u64,
    pub pending_snapshot_index: u64,
    pub recent_active: bool,
}

impl From<&ConsensusPeerProgress> for MaintenancePeerProgress {
    fn from(progress: &ConsensusPeerProgress) -> Self {
        Self {
            node_id: progress.node_id.get(),
            matched_index: progress.matched_index.get(),
            committed_index: progress.committed_index.get(),
            pending_snapshot_index: progress.pending_snapshot_index.get(),
            recent_active: progress.recent_active,
        }
    }
}

async fn group_inventory(
    State(registry): State<ConsensusGroupRegistry>,
) -> Result<Json<MaintenanceInventory>, MaintenanceApiError> {
    let mut groups = Vec::new();
    for handle in registry.handles()? {
        groups.push(group_observation(&handle).await?);
    }
    groups.sort_unstable_by_key(|group| group.group_id);
    Ok(Json(MaintenanceInventory {
        node_id: registry.node_id(),
        groups,
    }))
}

async fn group_observation(
    handle: &ConsensusProbeHandle,
) -> Result<MaintenanceGroup, ConsensusProbeError> {
    let status = handle.status().await?;
    let membership = handle.membership().await?;
    Ok(MaintenanceGroup {
        group_id: status.group_id.get(),
        group_epoch: status.group_epoch.get(),
        role: status.role.into(),
        leader_id: status.leader_id.map(epoch_consensus::NodeId::get),
        term: status.term.get(),
        commit_index: status.commit_index.get(),
        applied_index: status.applied_index.get(),
        checkpoint_index: status.checkpoint_index.get(),
        fail_stopped: status.fail_stopped,
        membership: MaintenanceMembership::from(&membership),
        replication_progress: status
            .replication_progress
            .iter()
            .map(MaintenancePeerProgress::from)
            .collect(),
    })
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferLeadershipRequest {
    group_epoch: u64,
    expected_term: u64,
    target_node_id: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct TransferLeadershipResponse {
    state: &'static str,
    group_id: u64,
    group_epoch: u64,
    expected_term: u64,
    target_node_id: u64,
}

async fn transfer_group_leadership(
    State(registry): State<ConsensusGroupRegistry>,
    Path(group_id): Path<u64>,
    Json(request): Json<TransferLeadershipRequest>,
) -> Result<(StatusCode, Json<TransferLeadershipResponse>), MaintenanceApiError> {
    let handle = registry.handle(group_id, request.group_epoch)?;
    handle
        .transfer_leadership_if_term(request.target_node_id, request.expected_term)
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(TransferLeadershipResponse {
            state: "initiated",
            group_id,
            group_epoch: request.group_epoch,
            expected_term: request.expected_term,
            target_node_id: request.target_node_id,
        }),
    ))
}

#[derive(Debug)]
struct MaintenanceApiError(String);

impl From<ConsensusProbeError> for MaintenanceApiError {
    fn from(error: ConsensusProbeError) -> Self {
        Self(error.to_string())
    }
}

impl From<ConsensusGroupRegistryError> for MaintenanceApiError {
    fn from(error: ConsensusGroupRegistryError) -> Self {
        Self(error.to_string())
    }
}

impl IntoResponse for MaintenanceApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "code": "maintenance_precondition_failed",
                "message": self.0,
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{
        body::Body,
        http::{Request, header::CONTENT_TYPE},
    };
    use http_body_util::BodyExt as _;
    use tempfile::TempDir;
    use tower::ServiceExt as _;
    use url::Url;

    use super::*;
    use crate::consensus::{ConsensusProbeConfig, ConsensusProbeRuntime};

    fn peer_url(port: u16) -> Url {
        Url::parse(&format!("http://127.0.0.1:{port}/")).expect("peer URL")
    }

    async fn registered_runtime() -> (TempDir, ConsensusProbeRuntime, ConsensusGroupRegistry) {
        let directory = TempDir::new().expect("temporary directory");
        let config = ConsensusProbeConfig::new(
            2,
            41,
            7,
            [
                (1, peer_url(39_101)),
                (2, peer_url(39_102)),
                (3, peer_url(39_103)),
            ],
            Duration::from_mins(1),
        )
        .expect("consensus config");
        let runtime =
            ConsensusProbeRuntime::start(config, directory.path().join("maintenance.wal"))
                .await
                .expect("consensus runtime");
        let registry = ConsensusGroupRegistry::new(2).expect("registry");
        registry.register(runtime.handle()).expect("registration");
        (directory, runtime, registry)
    }

    #[tokio::test]
    async fn inventory_is_canonical_and_reports_registered_consensus_state() {
        let (_directory, runtime, registry) = registered_runtime().await;
        let response = regional_maintenance_router(registry)
            .oneshot(
                Request::builder()
                    .uri(INTERNAL_MAINTENANCE_GROUPS_PATH)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let inventory: MaintenanceInventory = serde_json::from_slice(&body).expect("inventory");
        assert_eq!(inventory.node_id, 2);
        assert_eq!(inventory.groups.len(), 1);
        assert_eq!(inventory.groups[0].group_id, 41);
        assert_eq!(inventory.groups[0].group_epoch, 7);
        assert_eq!(inventory.groups[0].membership.voters, vec![1, 2, 3]);
        runtime.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn leadership_transfer_is_epoch_and_term_fenced() {
        let (_directory, runtime, registry) = registered_runtime().await;
        let router = regional_maintenance_router(registry);
        let wrong_epoch = transfer_request(8, 0);
        let response = router.clone().oneshot(wrong_epoch).await.expect("response");
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let stale_term = transfer_request(7, 99);
        let response = router.oneshot(stale_term).await.expect("response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert!(String::from_utf8_lossy(&body).contains("term 99 is stale"));
        runtime.shutdown().await.expect("shutdown");
    }

    fn transfer_request(group_epoch: u64, expected_term: u64) -> Request<Body> {
        let body = serde_json::to_vec(&serde_json::json!({
            "group_epoch": group_epoch,
            "expected_term": expected_term,
            "target_node_id": 1,
        }))
        .expect("request body");
        Request::builder()
            .method("POST")
            .uri("/internal/v1/maintenance/groups/41/leadership")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .expect("request")
    }
}
