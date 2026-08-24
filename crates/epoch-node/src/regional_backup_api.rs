//! Quorum-barriered native regional snapshot artifact API.

use std::{
    collections::BTreeMap, fmt::Write as _, fs::File, io::Read as _, path::Path, sync::Arc,
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use epoch_catalog::ResourceRecord;
use epoch_consensus::{
    ApplicationSnapshot, ConsensusMembership, ConsensusRestoreSnapshot, ConsensusRole, GroupEpoch,
    GroupId, LogIndex, NodeId, PersistentRaftAdapter, Term,
};
use epoch_core::Clock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    catalog_tablet::{CatalogTabletScope, CatalogTabletService},
    consensus::{ConsensusProbeConfig, ConsensusProbeError, ConsensusProbeHandle},
    tablet_materializer::{MaterializedTabletMetadata, TabletDirectory, TabletDirectoryError},
};

pub const REGIONAL_BACKUP_PATH: &str = "/v1/admin/backups";
pub const INTERNAL_REGIONAL_BACKUP_GROUP_PATH: &str = "/internal/v1/backups/groups/{group_id}";
const REGIONAL_BACKUP_FORMAT_VERSION: u16 = 1;
const MAX_REGIONAL_BACKUP_BYTES: usize = 128 * 1024 * 1024;
const MAX_REGIONAL_BACKUP_GROUP_BYTES: usize = 32 * 1024 * 1024;
const REGIONAL_BACKUP_REQUEST_BYTES: usize = 1024;

#[derive(Debug, Clone)]
pub struct RegionalBackupState {
    catalog: ConsensusProbeHandle,
    directory: TabletDirectory,
    clock: Arc<dyn Clock>,
    barrier_timeout: Duration,
    coordinator_config: ConsensusProbeConfig,
}

impl RegionalBackupState {
    pub fn new(
        catalog: ConsensusProbeHandle,
        directory: TabletDirectory,
        clock: Arc<dyn Clock>,
        barrier_timeout: Duration,
        coordinator_config: ConsensusProbeConfig,
    ) -> Result<Self, RegionalBackupError> {
        if barrier_timeout.is_zero() {
            return Err(RegionalBackupError::InvalidConfiguration(
                "backup barrier timeout must be non-zero".into(),
            ));
        }
        if catalog.node_id() != coordinator_config.node_id()
            || catalog.group_id() != coordinator_config.group_id()
            || catalog.group_epoch() != coordinator_config.group_epoch()
        {
            return Err(RegionalBackupError::InvalidConfiguration(
                "backup coordinator configuration does not match the catalog handle".into(),
            ));
        }
        Ok(Self {
            catalog,
            directory,
            clock,
            barrier_timeout,
            coordinator_config,
        })
    }

    pub async fn capture(&self) -> Result<RegionalBackupArtifact, RegionalBackupError> {
        let captured_at_ms = self.clock.now_ms();
        if captured_at_ms == 0 {
            return Err(RegionalBackupError::InvalidConfiguration(
                "backup capture clock returned zero".into(),
            ));
        }

        let catalog_status = self.catalog.status().await?;
        if catalog_status.role != ConsensusRole::Leader {
            return Err(RegionalBackupError::CoordinatorNotLeader {
                leader_id: catalog_status.leader_id.map(NodeId::get),
            });
        }
        let catalog_group = self
            .capture_handle(None, self.catalog.clone(), catalog_status.term.get())
            .await?;
        let catalog_application = catalog_group.application_snapshot()?;
        let catalog_scope =
            CatalogTabletScope::new(catalog_group.group_id, catalog_group.group_epoch)
                .map_err(RegionalBackupError::CatalogState)?;
        let resources = CatalogTabletService::resources_from_application_snapshot(
            catalog_scope,
            &catalog_application,
        )
        .map_err(RegionalBackupError::CatalogState)?;

        let mut expected = resources
            .iter()
            .flat_map(|resource| {
                resource
                    .tablets
                    .iter()
                    .map(|descriptor| MaterializedTabletMetadata {
                        resource: resource.name.clone(),
                        shard_count: resource.spec.shard_count,
                        configuration: resource.spec.configuration.clone(),
                        descriptor: descriptor.clone(),
                    })
            })
            .collect::<Vec<_>>();
        expected.sort_by_key(|metadata| metadata.descriptor.consensus_group_id);

        let mut snapshots = Vec::with_capacity(expected.len().saturating_add(1));
        snapshots.push(catalog_group);
        for metadata in expected {
            snapshots.push(self.capture_data_group(&metadata).await?);
        }
        let artifact =
            RegionalBackupArtifact::new(captured_at_ms, self.catalog.node_id().get(), snapshots)?;
        artifact.validate_catalog_resources(&resources)?;
        Ok(artifact)
    }

    async fn capture_data_group(
        &self,
        expected: &MaterializedTabletMetadata,
    ) -> Result<RegionalBackupGroup, RegionalBackupError> {
        let group_id = expected.descriptor.consensus_group_id;
        let group_epoch = expected.descriptor.tablet_epoch;
        let deadline = tokio::time::Instant::now() + self.barrier_timeout;
        loop {
            let mut attempts = Vec::new();
            for &node_id in &expected.descriptor.voter_node_ids {
                let node_id = NodeId::new(node_id)
                    .map_err(|error| RegionalBackupError::Artifact(error.to_string()))?;
                let result = if node_id == self.coordinator_config.node_id() {
                    self.capture_local_data_group(group_id, group_epoch).await
                } else {
                    self.capture_remote_data_group(node_id, group_id, group_epoch)
                        .await
                };
                match result {
                    Ok(group) => {
                        if group.resource.as_ref() != Some(expected)
                            || group.group_id != group_id
                            || group.group_epoch != group_epoch
                        {
                            return Err(RegionalBackupError::Artifact(format!(
                                "backup group {group_id} does not match the captured catalog inventory"
                            )));
                        }
                        group.validate_payload()?;
                        return Ok(group);
                    }
                    Err(error) => attempts.push(format!("node {}: {error}", node_id.get())),
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RegionalBackupError::DistributedGroupUnavailable {
                    group_id,
                    attempts: attempts.join("; "),
                });
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn capture_local_data_group(
        &self,
        group_id: u64,
        group_epoch: u64,
    ) -> Result<RegionalBackupGroup, RegionalBackupError> {
        let route = self
            .directory
            .routes()?
            .into_iter()
            .find(|route| {
                route.metadata().descriptor.consensus_group_id == group_id
                    && route.metadata().descriptor.tablet_epoch == group_epoch
            })
            .ok_or(RegionalBackupError::GroupNotHosted { group_id })?;
        let status = route.consensus().status().await?;
        if status.role != ConsensusRole::Leader {
            return Err(RegionalBackupError::GroupNotLeader {
                group_id,
                leader_id: status.leader_id.map(NodeId::get),
            });
        }
        self.capture_handle(
            Some(route.metadata().clone()),
            route.consensus(),
            status.term.get(),
        )
        .await
    }

    async fn capture_handle(
        &self,
        resource: Option<MaterializedTabletMetadata>,
        handle: ConsensusProbeHandle,
        expected_term: u64,
    ) -> Result<RegionalBackupGroup, RegionalBackupError> {
        let barrier = handle
            .read_barrier(expected_term, self.barrier_timeout)
            .await?;
        let snapshot = handle
            .export_restore_snapshot(barrier.applied_index)
            .await?;
        RegionalBackupGroup::new(
            resource,
            barrier.read_index.get(),
            barrier.applied_index.get(),
            &snapshot,
        )
    }

    async fn capture_remote_data_group(
        &self,
        node_id: NodeId,
        group_id: u64,
        group_epoch: u64,
    ) -> Result<RegionalBackupGroup, RegionalBackupError> {
        let base_url = self
            .coordinator_config
            .peer_url(node_id)
            .ok_or_else(|| RegionalBackupError::Peer(format!("node {node_id} has no peer URL")))?;
        let endpoint = base_url
            .join(&format!("internal/v1/backups/groups/{group_id}"))
            .map_err(|error| RegionalBackupError::Peer(error.to_string()))?;
        let response = self
            .coordinator_config
            .outbound_client()
            .post(endpoint.clone())
            .json(&RegionalBackupGroupRequest { group_epoch })
            .send()
            .await
            .map_err(|error| {
                RegionalBackupError::Peer(format!("request to {endpoint} failed: {error}"))
            })?;
        let status = response.status();
        let encoded = bounded_response_bytes(response).await?;
        if !status.is_success() {
            return Err(RegionalBackupError::Peer(format!(
                "request to {endpoint} returned {status}: {}",
                String::from_utf8_lossy(&encoded)
            )));
        }
        let group: RegionalBackupGroup = serde_json::from_slice(&encoded)
            .map_err(|error| RegionalBackupError::Peer(error.to_string()))?;
        if serde_json::to_vec(&group)
            .map_err(|error| RegionalBackupError::Peer(error.to_string()))?
            != encoded
        {
            return Err(RegionalBackupError::Peer(
                "peer backup group encoding is not canonical".into(),
            ));
        }
        Ok(group)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegionalBackupGroupRequest {
    group_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionalBackupArtifact {
    pub format_version: u16,
    pub epoch_version: String,
    pub captured_at_ms: u64,
    pub coordinator_node_id: u64,
    pub groups: Vec<RegionalBackupGroup>,
    pub manifest_sha256: String,
}

impl RegionalBackupArtifact {
    fn new(
        captured_at_ms: u64,
        coordinator_node_id: u64,
        groups: Vec<RegionalBackupGroup>,
    ) -> Result<Self, RegionalBackupError> {
        let mut artifact = Self {
            format_version: REGIONAL_BACKUP_FORMAT_VERSION,
            epoch_version: env!("CARGO_PKG_VERSION").into(),
            captured_at_ms,
            coordinator_node_id,
            groups,
            manifest_sha256: String::new(),
        };
        artifact.validate_structure()?;
        for group in &artifact.groups {
            group.validate_payload()?;
        }
        artifact.manifest_sha256 = artifact.digest()?;
        artifact.validate_size()?;
        Ok(artifact)
    }

    pub fn encode(&self) -> Result<Vec<u8>, RegionalBackupError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| RegionalBackupError::Artifact(error.to_string()))
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RegionalBackupError> {
        if encoded.is_empty() || encoded.len() > MAX_REGIONAL_BACKUP_BYTES {
            return Err(RegionalBackupError::Artifact(
                "regional backup size is outside the supported bounds".into(),
            ));
        }
        let artifact: Self = serde_json::from_slice(encoded)
            .map_err(|error| RegionalBackupError::Artifact(error.to_string()))?;
        if serde_json::to_vec(&artifact)
            .map_err(|error| RegionalBackupError::Artifact(error.to_string()))?
            != encoded
        {
            return Err(RegionalBackupError::Artifact(
                "regional backup encoding is not canonical".into(),
            ));
        }
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn read_from_path(path: &Path) -> Result<Self, RegionalBackupError> {
        let file = File::open(path).map_err(|error| {
            RegionalBackupError::Artifact(format!(
                "could not open regional backup {}: {error}",
                path.display()
            ))
        })?;
        let mut encoded = Vec::new();
        file.take(u64::try_from(MAX_REGIONAL_BACKUP_BYTES).unwrap() + 1)
            .read_to_end(&mut encoded)
            .map_err(|error| {
                RegionalBackupError::Artifact(format!(
                    "could not read regional backup {}: {error}",
                    path.display()
                ))
            })?;
        Self::decode(&encoded)
    }

    pub fn validate_for_restore(&self) -> Result<(), RegionalBackupError> {
        self.validate()
    }

    pub(crate) fn validate_catalog_resources(
        &self,
        resources: &[ResourceRecord],
    ) -> Result<(), RegionalBackupError> {
        self.validate()?;
        let mut expected = BTreeMap::new();
        for resource in resources {
            for descriptor in &resource.tablets {
                let metadata = MaterializedTabletMetadata {
                    resource: resource.name.clone(),
                    shard_count: resource.spec.shard_count,
                    configuration: resource.spec.configuration.clone(),
                    descriptor: descriptor.clone(),
                };
                if expected
                    .insert(descriptor.consensus_group_id, metadata)
                    .is_some()
                {
                    return Err(RegionalBackupError::Artifact(format!(
                        "catalog repeats consensus group {}",
                        descriptor.consensus_group_id
                    )));
                }
            }
        }
        let actual = self
            .groups
            .iter()
            .skip(1)
            .map(|group| {
                group
                    .resource
                    .clone()
                    .map(|resource| (group.group_id, resource))
                    .ok_or_else(|| {
                        RegionalBackupError::Artifact(format!(
                            "backup data group {} has no resource identity",
                            group.group_id
                        ))
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if actual != expected {
            return Err(RegionalBackupError::Artifact(
                "backup data-group inventory does not exactly match the catalog snapshot".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn restore_group(
        &self,
        config: &ConsensusProbeConfig,
        stable_path: &Path,
    ) -> Result<(), RegionalBackupError> {
        self.validate()?;
        let group = self
            .groups
            .iter()
            .find(|group| {
                group.group_id == config.group_id().get()
                    && group.group_epoch == config.group_epoch().get()
            })
            .ok_or_else(|| {
                RegionalBackupError::Artifact(format!(
                    "backup has no checkpoint for group {} epoch {}",
                    config.group_id(),
                    config.group_epoch()
                ))
            })?;
        let snapshot = group.restore_snapshot()?;
        PersistentRaftAdapter::restore_from_snapshot_with_members(
            stable_path,
            config.node_id(),
            config.group_id(),
            config.group_epoch(),
            config.voters().to_vec(),
            config.members().collect::<Vec<_>>(),
            &snapshot,
        )?;
        Ok(())
    }

    fn validate(&self) -> Result<(), RegionalBackupError> {
        self.validate_structure()?;
        self.validate_size()?;
        if self.manifest_sha256.len() != 64 || self.digest()? != self.manifest_sha256 {
            return Err(RegionalBackupError::Artifact(
                "regional backup manifest digest does not match".into(),
            ));
        }
        for group in &self.groups {
            group.validate_payload()?;
        }
        Ok(())
    }

    fn validate_structure(&self) -> Result<(), RegionalBackupError> {
        if self.format_version != REGIONAL_BACKUP_FORMAT_VERSION
            || self.epoch_version.is_empty()
            || self.captured_at_ms == 0
            || self.coordinator_node_id == 0
            || self.groups.is_empty()
        {
            return Err(RegionalBackupError::Artifact(
                "regional backup header is invalid".into(),
            ));
        }
        if self.groups[0].resource.is_some() {
            return Err(RegionalBackupError::Artifact(
                "catalog snapshot must be the first backup group".into(),
            ));
        }
        let mut previous = 0_u64;
        for (index, group) in self.groups.iter().enumerate() {
            if group.group_id <= previous
                || group.group_epoch == 0
                || group.read_index == 0
                || group.applied_index < group.read_index
                || group.snapshot_index != group.applied_index
                || group.checkpoint_term == 0
                || group.hard_state_term < group.checkpoint_term
                || group.format_version == 0
                || group.format_id.len() != 32
                || group.state_sha256.len() != 64
                || group.checkpoint_sha256.len() != 64
            {
                return Err(RegionalBackupError::Artifact(format!(
                    "backup group {} metadata is invalid",
                    group.group_id
                )));
            }
            if index > 0 && group.resource.is_none() {
                return Err(RegionalBackupError::Artifact(format!(
                    "backup data group {} has no resource identity",
                    group.group_id
                )));
            }
            if let Some(resource) = group.resource.as_ref()
                && (resource.descriptor.consensus_group_id != group.group_id
                    || resource.descriptor.tablet_epoch != group.group_epoch)
            {
                return Err(RegionalBackupError::Artifact(format!(
                    "backup data group {} disagrees with its tablet identity",
                    group.group_id
                )));
            }
            previous = group.group_id;
        }
        Ok(())
    }

    fn validate_size(&self) -> Result<(), RegionalBackupError> {
        let encoded = serde_json::to_vec(self)
            .map_err(|error| RegionalBackupError::Artifact(error.to_string()))?;
        if encoded.len() > MAX_REGIONAL_BACKUP_BYTES {
            return Err(RegionalBackupError::Artifact(format!(
                "regional backup exceeds {MAX_REGIONAL_BACKUP_BYTES} bytes"
            )));
        }
        Ok(())
    }

    fn digest(&self) -> Result<String, RegionalBackupError> {
        let mut unsigned = self.clone();
        unsigned.manifest_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|encoded| hex_digest(&encoded))
            .map_err(|error| RegionalBackupError::Artifact(error.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionalBackupGroup {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<MaterializedTabletMetadata>,
    pub group_id: u64,
    pub group_epoch: u64,
    pub read_index: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,
    pub checkpoint_term: u64,
    pub hard_state_term: u64,
    pub membership: RegionalBackupMembership,
    pub format_id: String,
    pub format_version: u16,
    pub state_sha256: String,
    pub checkpoint_sha256: String,
    pub checkpoint_base64: String,
}

impl RegionalBackupGroup {
    fn new(
        resource: Option<MaterializedTabletMetadata>,
        read_index: u64,
        applied_index: u64,
        snapshot: &ConsensusRestoreSnapshot,
    ) -> Result<Self, RegionalBackupError> {
        let application = snapshot
            .application_snapshot()
            .map_err(|error| RegionalBackupError::Artifact(error.to_string()))?;
        let checkpoint = snapshot.checkpoint_bytes();
        Ok(Self {
            resource,
            group_id: snapshot.group_id().get(),
            group_epoch: snapshot.group_epoch().get(),
            read_index,
            applied_index,
            snapshot_index: snapshot.checkpoint_index().get(),
            checkpoint_term: snapshot.checkpoint_term().get(),
            hard_state_term: snapshot.hard_state_term().get(),
            membership: RegionalBackupMembership::from(snapshot.membership()),
            format_id: hex_bytes(&application.format_id()),
            format_version: application.format_version(),
            state_sha256: hex_bytes(&application.state_digest()),
            checkpoint_sha256: hex_digest(checkpoint),
            checkpoint_base64: BASE64.encode(checkpoint),
        })
    }

    pub fn application_snapshot(&self) -> Result<ApplicationSnapshot, RegionalBackupError> {
        self.validate_payload()?;
        self.restore_snapshot()?
            .application_snapshot()
            .map_err(|error| RegionalBackupError::Artifact(error.to_string()))
    }

    pub fn restore_snapshot(&self) -> Result<ConsensusRestoreSnapshot, RegionalBackupError> {
        let checkpoint = BASE64.decode(&self.checkpoint_base64).map_err(|error| {
            RegionalBackupError::Artifact(format!("consensus checkpoint is not base64: {error}"))
        })?;
        if hex_digest(&checkpoint) != self.checkpoint_sha256 {
            return Err(RegionalBackupError::Artifact(format!(
                "backup group {} checkpoint digest does not match",
                self.group_id
            )));
        }
        ConsensusRestoreSnapshot::from_parts(
            GroupId::new(self.group_id)
                .map_err(|error| RegionalBackupError::Artifact(error.to_string()))?,
            GroupEpoch::new(self.group_epoch)
                .map_err(|error| RegionalBackupError::Artifact(error.to_string()))?,
            LogIndex::new(self.snapshot_index),
            Term::new(self.checkpoint_term),
            Term::new(self.hard_state_term),
            self.membership.consensus_membership()?,
            checkpoint,
        )
        .map_err(|error| RegionalBackupError::Artifact(error.to_string()))
    }

    fn validate_payload(&self) -> Result<(), RegionalBackupError> {
        if decode_hex(&self.format_id)?.len() != 16 || decode_hex(&self.state_sha256)?.len() != 32 {
            return Err(RegionalBackupError::Artifact(format!(
                "backup group {} snapshot identity is invalid",
                self.group_id
            )));
        }
        let application = self
            .restore_snapshot()?
            .application_snapshot()
            .map_err(|error| RegionalBackupError::Artifact(error.to_string()))?;
        if application.checkpoint_index().get() != self.snapshot_index
            || hex_bytes(&application.format_id()) != self.format_id
            || application.format_version() != self.format_version
            || hex_bytes(&application.state_digest()) != self.state_sha256
        {
            return Err(RegionalBackupError::Artifact(format!(
                "backup group {} application metadata disagrees with its checkpoint",
                self.group_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionalBackupMembership {
    pub allowed_members: Vec<u64>,
    pub voters: Vec<u64>,
    pub outgoing_voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub staged_learners: Vec<u64>,
    pub auto_leave: bool,
}

impl From<&ConsensusMembership> for RegionalBackupMembership {
    fn from(membership: &ConsensusMembership) -> Self {
        let ids = |nodes: &[NodeId]| nodes.iter().map(|node_id| node_id.get()).collect();
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

impl RegionalBackupMembership {
    fn consensus_membership(&self) -> Result<ConsensusMembership, RegionalBackupError> {
        let ids = |values: &[u64]| {
            values
                .iter()
                .copied()
                .map(NodeId::new)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| RegionalBackupError::Artifact(error.to_string()))
        };
        Ok(ConsensusMembership {
            allowed_members: ids(&self.allowed_members)?,
            voters: ids(&self.voters)?,
            outgoing_voters: ids(&self.outgoing_voters)?,
            learners: ids(&self.learners)?,
            staged_learners: ids(&self.staged_learners)?,
            auto_leave: self.auto_leave,
        })
    }
}

#[derive(Debug, Error)]
pub enum RegionalBackupError {
    #[error("invalid regional backup configuration: {0}")]
    InvalidConfiguration(String),
    #[error(
        "regional backup coordinator is not the catalog leader; current leader is {leader_id:?}"
    )]
    CoordinatorNotLeader { leader_id: Option<u64> },
    #[error("consensus group {group_id} is not hosted on this node")]
    GroupNotHosted { group_id: u64 },
    #[error("consensus group {group_id} is not led by this node; current leader is {leader_id:?}")]
    GroupNotLeader {
        group_id: u64,
        leader_id: Option<u64>,
    },
    #[error("no tablet leader could produce backup group {group_id}: {attempts}")]
    DistributedGroupUnavailable { group_id: u64, attempts: String },
    #[error("regional backup peer protocol failed: {0}")]
    Peer(String),
    #[error("captured catalog snapshot is invalid: {0}")]
    CatalogState(String),
    #[error("regional backup artifact is invalid: {0}")]
    Artifact(String),
    #[error(transparent)]
    ConsensusState(#[from] epoch_consensus::ConsensusError),
    #[error(transparent)]
    Consensus(#[from] ConsensusProbeError),
    #[error(transparent)]
    Directory(#[from] TabletDirectoryError),
}

pub fn regional_backup_router(state: RegionalBackupState) -> Router {
    Router::new()
        .route(REGIONAL_BACKUP_PATH, post(create_backup))
        .with_state(state)
}

pub fn regional_backup_peer_router(state: RegionalBackupState) -> Router {
    Router::new()
        .route(
            INTERNAL_REGIONAL_BACKUP_GROUP_PATH,
            post(create_backup_group),
        )
        .layer(DefaultBodyLimit::max(REGIONAL_BACKUP_REQUEST_BYTES))
        .with_state(state)
}

async fn create_backup(State(state): State<RegionalBackupState>) -> impl IntoResponse {
    match state.capture().await {
        Ok(artifact) => (StatusCode::CREATED, Json(artifact)).into_response(),
        Err(error @ RegionalBackupError::CoordinatorNotLeader { .. }) => backup_error_response(
            StatusCode::CONFLICT,
            "backup_coordinator_not_leader",
            &error.to_string(),
        ),
        Err(error) => backup_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "backup_unavailable",
            &error.to_string(),
        ),
    }
}

async fn create_backup_group(
    AxumPath(group_id): AxumPath<u64>,
    State(state): State<RegionalBackupState>,
    Json(request): Json<RegionalBackupGroupRequest>,
) -> impl IntoResponse {
    match state
        .capture_local_data_group(group_id, request.group_epoch)
        .await
    {
        Ok(group) => (StatusCode::CREATED, Json(group)).into_response(),
        Err(error @ RegionalBackupError::GroupNotHosted { .. }) => backup_error_response(
            StatusCode::NOT_FOUND,
            "backup_group_not_hosted",
            &error.to_string(),
        ),
        Err(error @ RegionalBackupError::GroupNotLeader { .. }) => backup_error_response(
            StatusCode::CONFLICT,
            "backup_group_not_leader",
            &error.to_string(),
        ),
        Err(error) => backup_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "backup_group_unavailable",
            &error.to_string(),
        ),
    }
}

fn backup_error_response(status: StatusCode, code: &str, detail: &str) -> axum::response::Response {
    (
        status,
        Json(serde_json::json!({"error": {"code": code, "detail": detail}})),
    )
        .into_response()
}

async fn bounded_response_bytes(
    mut response: reqwest::Response,
) -> Result<Vec<u8>, RegionalBackupError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_REGIONAL_BACKUP_GROUP_BYTES as u64)
    {
        return Err(RegionalBackupError::Peer(
            "peer backup group exceeds the response size limit".into(),
        ));
    }
    let mut encoded = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| RegionalBackupError::Peer(error.to_string()))?
    {
        if encoded.len().saturating_add(chunk.len()) > MAX_REGIONAL_BACKUP_GROUP_BYTES {
            return Err(RegionalBackupError::Peer(
                "peer backup group exceeds the response size limit".into(),
            ));
        }
        encoded.extend_from_slice(&chunk);
    }
    Ok(encoded)
}

fn hex_digest(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut encoded, byte| {
            write!(&mut encoded, "{byte:02x}")
                .expect("writing hexadecimal into String cannot fail");
            encoded
        },
    )
}

fn decode_hex(encoded: &str) -> Result<Vec<u8>, RegionalBackupError> {
    if !encoded.len().is_multiple_of(2)
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RegionalBackupError::Artifact(
            "snapshot digest is not lowercase hexadecimal".into(),
        ));
    }
    (0..encoded.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&encoded[index..index + 2], 16)
                .map_err(|error| RegionalBackupError::Artifact(error.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};

    use epoch_consensus::{
        ConsensusAdapter, ConsensusOutput, GroupEpoch, GroupId, NodeId, PersistentRaftAdapter,
        Proposal, ProposalId,
    };
    use tempfile::TempDir;

    use super::*;

    fn snapshot(group_id: u64, payload: &[u8]) -> ConsensusRestoreSnapshot {
        let directory = TempDir::new().unwrap();
        let group_id = GroupId::new(group_id).unwrap();
        let group_epoch = GroupEpoch::new(1).unwrap();
        let members = (1..=3)
            .map(|value| NodeId::new(value).unwrap())
            .collect::<Vec<_>>();
        let mut adapters = members
            .iter()
            .copied()
            .map(|node_id| {
                let opened = PersistentRaftAdapter::open(
                    directory.path().join(format!("node-{}.wal", node_id.get())),
                    node_id,
                    group_id,
                    group_epoch,
                    members.clone(),
                )
                .unwrap();
                (node_id, opened.adapter)
            })
            .collect::<BTreeMap<_, _>>();
        let leader = members[0];
        let output = adapters.get_mut(&leader).unwrap().campaign().unwrap();
        drain(&mut adapters, output);
        let term = adapters[&leader].status().term;
        let proposal = Proposal::new(
            group_id,
            group_epoch,
            term,
            ProposalId::new(1).unwrap(),
            b"seed".to_vec(),
        );
        let output = adapters
            .get_mut(&leader)
            .unwrap()
            .propose(proposal)
            .unwrap();
        drain(&mut adapters, output);
        let index = adapters[&leader].status().applied_index;
        let application = ApplicationSnapshot::new(
            index,
            *b"epoch-test-image",
            1,
            Sha256::digest(payload).into(),
            payload.to_vec(),
        )
        .unwrap();
        adapters
            .get_mut(&leader)
            .unwrap()
            .export_restore_snapshot(application)
            .unwrap()
    }

    fn drain(adapters: &mut BTreeMap<NodeId, PersistentRaftAdapter>, output: ConsensusOutput) {
        let mut messages = VecDeque::from(output.messages);
        let mut delivered = 0;
        while let Some(message) = messages.pop_front() {
            delivered += 1;
            assert!(delivered < 1_000);
            let target = message.to();
            let output = adapters.get_mut(&target).unwrap().receive(message).unwrap();
            messages.extend(output.messages);
        }
    }

    #[test]
    fn regional_artifact_is_canonical_tamper_evident_and_snapshot_decodable() {
        let snapshot = snapshot(1, b"catalog");
        let index = snapshot.checkpoint_index().get();
        let catalog = RegionalBackupGroup::new(None, index, index, &snapshot).unwrap();
        let artifact = RegionalBackupArtifact::new(42, 1, vec![catalog]).unwrap();
        let encoded = artifact.encode().unwrap();
        let decoded = RegionalBackupArtifact::decode(&encoded).unwrap();
        assert_eq!(decoded, artifact);
        assert_eq!(
            decoded.groups[0].application_snapshot().unwrap().payload(),
            b"catalog"
        );

        let mut tampered: RegionalBackupArtifact = serde_json::from_slice(&encoded).unwrap();
        tampered.groups[0].checkpoint_base64 = BASE64.encode(b"tampered");
        assert!(RegionalBackupArtifact::decode(&serde_json::to_vec(&tampered).unwrap()).is_err());
    }

    #[test]
    fn regional_artifact_requires_sorted_groups_and_catalog_first() {
        let second = snapshot(2, b"second");
        let first = snapshot(1, b"first");
        let groups = vec![
            RegionalBackupGroup::new(
                None,
                second.checkpoint_index().get(),
                second.checkpoint_index().get(),
                &second,
            )
            .unwrap(),
            RegionalBackupGroup::new(
                None,
                first.checkpoint_index().get(),
                first.checkpoint_index().get(),
                &first,
            )
            .unwrap(),
        ];
        assert!(RegionalBackupArtifact::new(42, 1, groups).is_err());
    }
}
