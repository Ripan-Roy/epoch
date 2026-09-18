//! Deterministic regional resource and tablet catalog.
//!
//! The catalog is a profile-neutral state machine. Consensus and persistence
//! live outside this crate so the same commands can be replayed by the
//! standalone, clustered, and managed regional runtimes.

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{ResourceKind, WorkloadProfile};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const MAX_NAME_COMPONENT_BYTES: usize = 128;
const MAX_REQUEST_TOKEN_BYTES: usize = 256;
const MAX_SHARDS_PER_RESOURCE: u32 = 4_096;
const MAX_REPLICAS_PER_TABLET: u16 = 9;
const MAX_COMMAND_BYTES: usize = 512 * 1024;
const MAX_PROFILE_CONFIGURATION_BYTES: usize = 64 * 1024;
const MAX_GOVERNANCE_OWNER_BYTES: usize = 128;
const MAX_GOVERNANCE_COST_CENTER_BYTES: usize = 64;
const MAX_GOVERNANCE_TAGS: usize = 32;
const MAX_GOVERNANCE_TAG_KEY_BYTES: usize = 63;
const MAX_GOVERNANCE_TAG_VALUE_BYTES: usize = 256;
const MAX_MANAGED_DOCUMENT_BYTES: usize = 128 * 1024;
const MAX_MANAGED_BATCH_RESOURCES: usize = 128;
const MAX_MANAGED_IMPORT_RECORDS: usize = 4_096;
const MAX_TRANSIENT_CONTROL_REQUESTS: usize = 8;
const MAX_CONTROL_OWNER_BYTES: usize = 128;
const MIN_CONTROL_LEASE_TTL_MS: u64 = 1_000;
const MAX_CONTROL_LEASE_TTL_MS: u64 = 60_000;
const MAX_CHANGE_PAGE_SIZE: usize = 1_000;
const MAX_CHANGE_HISTORY: usize = 4_096;
const RESERVED_GOVERNANCE_TAG_PREFIX: &str = "epoch.io/";
pub const CATALOG_COMMAND_FORMAT_VERSION: u16 = 1;
pub const CATALOG_CONFIG_COMMAND_FORMAT_VERSION: u16 = 2;
pub const CATALOG_GOVERNANCE_COMMAND_FORMAT_VERSION: u16 = 3;
pub const CATALOG_PLACEMENT_COMMAND_FORMAT_VERSION: u16 = 4;
pub const CATALOG_MEMBERSHIP_COMMAND_FORMAT_VERSION: u16 = 5;
pub const CATALOG_CONTROL_COMMAND_FORMAT_VERSION: u16 = 6;
pub const CATALOG_SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub const CATALOG_CONFIG_SNAPSHOT_FORMAT_VERSION: u16 = 2;
pub const CATALOG_GOVERNANCE_SNAPSHOT_FORMAT_VERSION: u16 = 3;
pub const CATALOG_PLACEMENT_SNAPSHOT_FORMAT_VERSION: u16 = 4;
pub const CATALOG_MEMBERSHIP_SNAPSHOT_FORMAT_VERSION: u16 = 5;
pub const CATALOG_CONTROL_SNAPSHOT_FORMAT_VERSION: u16 = 6;
pub const MAX_CATALOG_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;

pub type CatalogResult<T> = Result<T, CatalogError>;

/// Deterministic consensus proposal identity for a regional catalog request.
///
/// Reusing a request token with different command bytes therefore reaches the
/// consensus conflict boundary before it could be applied under a new ID.
pub fn catalog_proposal_id_for(
    group_id: u64,
    group_epoch: u64,
    request_token: &str,
) -> CatalogResult<u64> {
    if group_id == 0 || group_epoch == 0 {
        return Err(CatalogError::InvalidSpec(
            "catalog group ID and epoch must be non-zero".into(),
        ));
    }
    validate_request_token(request_token)?;
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/catalog/proposal-id/v1\0");
    hasher.update(group_id.to_be_bytes());
    hasher.update(group_epoch.to_be_bytes());
    hasher.update(
        u64::try_from(request_token.len())
            .map_err(|_| CatalogError::IdentityExhausted)?
            .to_be_bytes(),
    );
    hasher.update(request_token.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let proposal_id = u64::from_be_bytes(bytes);
    Ok(if proposal_id == 0 { 1 } else { proposal_id })
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CatalogError {
    #[error("invalid resource name: {0}")]
    InvalidName(String),
    #[error("invalid resource specification: {0}")]
    InvalidSpec(String),
    #[error("request token is required")]
    MissingRequestToken,
    #[error("request token must be at most {MAX_REQUEST_TOKEN_BYTES} bytes")]
    RequestTokenTooLong,
    #[error("resource was not found: {0}")]
    NotFound(String),
    #[error("expected resource generation {expected}, found {actual}")]
    GenerationConflict { expected: u64, actual: u64 },
    #[error("workload profile is immutable: current {current:?}, requested {requested:?}")]
    ProfileMismatch {
        current: WorkloadProfile,
        requested: WorkloadProfile,
    },
    #[error("profile configuration is immutable after resource creation")]
    ConfigurationMismatch,
    #[error("shard count cannot decrease from {current} to {requested}")]
    ShardCountDecrease { current: u32, requested: u32 },
    #[error("request token is already bound to a different catalog command")]
    IdempotencyConflict,
    #[error("catalog identity or generation space was exhausted")]
    IdentityExhausted,
    #[error("catalog command exceeds the {MAX_COMMAND_BYTES}-byte limit")]
    CommandTooLarge,
    #[error("catalog command could not be decoded: {0}")]
    Decoding(String),
    #[error("catalog state could not be encoded: {0}")]
    Encoding(String),
    #[error("unsupported catalog command format version {0}")]
    UnsupportedCommandVersion(u16),
    #[error("catalog command is not in canonical v1 encoding")]
    NonCanonicalCommand,
    #[error("unsupported catalog snapshot format version {0}")]
    UnsupportedSnapshotVersion(u16),
    #[error("catalog snapshot is not in canonical v1 encoding")]
    NonCanonicalSnapshot,
    #[error("catalog snapshot state digest does not match its contents")]
    SnapshotDigestMismatch,
    #[error("catalog snapshot exceeds the {MAX_CATALOG_SNAPSHOT_BYTES}-byte limit")]
    SnapshotTooLarge,
    #[error("control lease is held by {owner_id} until {valid_until_ms}")]
    ControlLeaseHeld {
        owner_id: String,
        valid_until_ms: u64,
    },
    #[error("control lease is fenced: active owner {expected_owner} has fence {expected_fence}")]
    ControlLeaseFenced {
        expected_owner: String,
        expected_fence: u64,
    },
    #[error("control lease expired at {valid_until_ms}")]
    ControlLeaseExpired { valid_until_ms: u64 },
    #[error(
        "node {node_id} capacity observation is stale: expected {expected} catalog groups, observed {actual}"
    )]
    CapacityObservationConflict {
        node_id: u64,
        expected: u32,
        actual: u32,
    },
    #[error("node {node_id} requires {required} consensus groups but its limit is {limit}")]
    CapacityExceeded {
        node_id: u64,
        required: u32,
        limit: u32,
    },
    #[error("change cursor {requested} is older than retained cursor {earliest}")]
    StaleChangeCursor { requested: u64, earliest: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceName {
    pub organization: String,
    pub project: String,
    pub environment: String,
    pub namespace: String,
    pub kind: ResourceKind,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataClassification {
    Public,
    Internal,
    Confidential,
    Restricted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceGovernance {
    pub owner: String,
    pub cost_center: String,
    pub classification: DataClassification,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

impl ResourceGovernance {
    fn validate(&self) -> CatalogResult<()> {
        validate_governance_identifier(
            "governance owner",
            &self.owner,
            MAX_GOVERNANCE_OWNER_BYTES,
        )?;
        validate_governance_identifier(
            "governance cost center",
            &self.cost_center,
            MAX_GOVERNANCE_COST_CENTER_BYTES,
        )?;
        if self.tags.len() > MAX_GOVERNANCE_TAGS {
            return Err(CatalogError::InvalidSpec(format!(
                "governance supports at most {MAX_GOVERNANCE_TAGS} tags"
            )));
        }
        for (key, value) in &self.tags {
            validate_governance_tag_key(key)?;
            validate_governance_tag_value(value)?;
        }
        Ok(())
    }
}

impl ResourceName {
    pub fn new(
        organization: impl Into<String>,
        project: impl Into<String>,
        environment: impl Into<String>,
        namespace: impl Into<String>,
        kind: ResourceKind,
        name: impl Into<String>,
    ) -> CatalogResult<Self> {
        let resource_name = Self {
            organization: organization.into(),
            project: project.into(),
            environment: environment.into(),
            namespace: namespace.into(),
            kind,
            name: name.into(),
        };
        resource_name.validate()?;
        Ok(resource_name)
    }

    pub fn validate(&self) -> CatalogResult<()> {
        validate_name_component("organization", &self.organization)?;
        validate_name_component("project", &self.project)?;
        validate_name_component("environment", &self.environment)?;
        validate_name_component("namespace", &self.namespace)?;
        validate_name_component("name", &self.name)
    }

    pub fn canonical_name(&self) -> String {
        format!(
            "{}/{}/{}/{}/{:?}/{}",
            self.organization, self.project, self.environment, self.namespace, self.kind, self.name
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSpec {
    pub workload_profile: WorkloadProfile,
    pub shard_count: u32,
    pub replica_count: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub governance: Option<ResourceGovernance>,
}

impl ResourceSpec {
    fn validate(&self, kind: ResourceKind) -> CatalogResult<()> {
        if self.shard_count == 0 || self.shard_count > MAX_SHARDS_PER_RESOURCE {
            return Err(CatalogError::InvalidSpec(format!(
                "shard_count must be between 1 and {MAX_SHARDS_PER_RESOURCE}"
            )));
        }
        if self.replica_count == 0 || self.replica_count > MAX_REPLICAS_PER_TABLET {
            return Err(CatalogError::InvalidSpec(format!(
                "replica_count must be between 1 and {MAX_REPLICAS_PER_TABLET}"
            )));
        }
        let expected_profile = match kind {
            ResourceKind::Cache | ResourceKind::Table => WorkloadProfile::CacheAndState,
            ResourceKind::Stream => WorkloadProfile::StreamLog,
            ResourceKind::Queue => WorkloadProfile::WorkQueue,
            ResourceKind::EventBus => WorkloadProfile::EventBus,
            ResourceKind::Subscription
            | ResourceKind::Schema
            | ResourceKind::Pipe
            | ResourceKind::Connector
            | ResourceKind::Policy => {
                return Err(CatalogError::InvalidSpec(format!(
                    "{kind:?} is not a data-bearing tablet resource"
                )));
            }
        };
        if self.workload_profile != expected_profile {
            return Err(CatalogError::InvalidSpec(format!(
                "{kind:?} requires the {expected_profile:?} workload profile"
            )));
        }
        if let Some(configuration) = &self.configuration {
            if !configuration.is_object() {
                return Err(CatalogError::InvalidSpec(
                    "profile configuration must be a JSON object".into(),
                ));
            }
            let encoded = serde_json::to_vec(configuration)
                .map_err(|error| CatalogError::InvalidSpec(error.to_string()))?;
            if encoded.len() > MAX_PROFILE_CONFIGURATION_BYTES {
                return Err(CatalogError::InvalidSpec(format!(
                    "profile configuration exceeds {MAX_PROFILE_CONFIGURATION_BYTES} bytes"
                )));
            }
        }
        if let Some(governance) = &self.governance {
            governance.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TabletDescriptor {
    pub tablet_id: u64,
    pub consensus_group_id: u64,
    pub shard_index: u32,
    pub tablet_epoch: u64,
    pub resource_generation: u64,
    pub workload_profile: WorkloadProfile,
    pub replica_count: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub voter_node_ids: Vec<u64>,
    /// Immutable voter set used only when creating a fresh local Raft journal.
    /// Legacy descriptors omit it and use `voter_node_ids` as the bootstrap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bootstrap_voter_node_ids: Vec<u64>,
    /// One learner-first replacement target. While present, materializers host
    /// the union of current and target voters; only consensus may finalize it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_voter_node_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRecord {
    pub name: ResourceName,
    pub generation: u64,
    pub spec: ResourceSpec,
    pub tablets: Vec<TabletDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyResource {
    pub request_token: String,
    pub expected_generation: Option<u64>,
    pub name: ResourceName,
    pub spec: ResourceSpec,
    /// Concrete, per-shard voter assignments selected from the regional node
    /// inventory. Legacy callers may omit this field; once a resource has
    /// explicit assignments, every subsequent apply must carry all shards.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tablet_placements: Vec<TabletPlacement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TabletPlacement {
    pub shard_index: u32,
    pub voter_node_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteResource {
    pub request_token: String,
    pub expected_generation: Option<u64>,
    pub name: ResourceName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanTabletMembership {
    pub request_token: String,
    pub tablet_id: u64,
    pub expected_tablet_epoch: u64,
    pub expected_resource_generation: u64,
    pub target_voter_node_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizeTabletMembership {
    pub request_token: String,
    pub tablet_id: u64,
    pub expected_tablet_epoch: u64,
    pub expected_resource_generation: u64,
    pub target_voter_node_ids: Vec<u64>,
}

/// Replicated declarative metadata owned by the managed control plane.
///
/// `desired` and `status` are canonical bounded JSON objects so the Rust
/// catalog can durably arbitrate versions without taking ownership of the
/// versioned Protobuf schema interpreted by Go.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedResourceRecord {
    pub name: ResourceName,
    pub generation: u64,
    pub desired: serde_json::Value,
    pub status: serde_json::Value,
    #[serde(default)]
    pub deletion_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedResourceApplyResult {
    pub resource: ManagedResourceRecord,
    pub created: bool,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesiredResourceWrite {
    pub name: ResourceName,
    pub expected_generation: Option<u64>,
    pub desired: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyDesiredResources {
    pub request_token: String,
    pub resources: Vec<DesiredResourceWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteDesiredResource {
    pub request_token: String,
    pub expected_generation: Option<u64>,
    pub name: ResourceName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteManagedResource {
    pub request_token: String,
    pub lease: ControlLeaseGuard,
    pub name: ResourceName,
    pub expected_desired_generation: u64,
    pub expected_catalog_generation: u64,
}

/// One-time import of the previous single-owner Go registry. Import is only
/// accepted while replicated managed state is empty, and preserves live and
/// tombstoned generation high-water marks atomically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportManagedResources {
    pub request_token: String,
    pub resources: Vec<ManagedResourceRecord>,
    pub generations: Vec<ResourceGeneration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlLeaseGuard {
    pub owner_id: String,
    pub fence: u64,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquireControlLease {
    pub request_token: String,
    pub owner_id: String,
    pub now_ms: u64,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlLease {
    pub owner_id: String,
    pub fence: u64,
    pub valid_until_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateManagedResourceStatus {
    pub request_token: String,
    pub lease: ControlLeaseGuard,
    pub name: ResourceName,
    pub expected_generation: u64,
    pub status: serde_json::Value,
}

/// One desired generation to materialize in the native tablet catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedResourcePlacement {
    pub name: ResourceName,
    pub expected_desired_generation: u64,
    pub expected_catalog_generation: u64,
    pub spec: ResourceSpec,
    pub tablet_placements: Vec<TabletPlacement>,
}

/// A complete capacity sample used to fence concurrent admission decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeCapacityObservation {
    pub node_id: u64,
    pub max_consensus_groups: u32,
    pub used_consensus_groups: u32,
    pub catalog_groups: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileManagedResources {
    pub request_token: String,
    pub lease: ControlLeaseGuard,
    pub capacity: Vec<NodeCapacityObservation>,
    pub resources: Vec<ManagedResourcePlacement>,
}

/// One lease-fenced, capacity-reserved learner-first membership transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanManagedTabletMembership {
    pub request_token: String,
    pub lease: ControlLeaseGuard,
    pub capacity: Vec<NodeCapacityObservation>,
    pub name: ResourceName,
    pub expected_desired_generation: u64,
    pub tablet_id: u64,
    pub expected_tablet_epoch: u64,
    pub expected_resource_generation: u64,
    pub target_voter_node_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum CatalogCommand {
    Apply(ApplyResource),
    Delete(DeleteResource),
    PlanMembership(PlanTabletMembership),
    FinalizeMembership(FinalizeTabletMembership),
    ApplyDesired(ApplyDesiredResources),
    DeleteDesired(DeleteDesiredResource),
    DeleteManaged(DeleteManagedResource),
    ImportManaged(ImportManagedResources),
    AcquireControlLease(AcquireControlLease),
    UpdateManagedStatus(UpdateManagedResourceStatus),
    ReconcileManaged(ReconcileManagedResources),
    PlanManagedMembership(PlanManagedTabletMembership),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedCatalogCommand {
    format_version: u16,
    command: CatalogCommand,
}

impl CatalogCommand {
    pub fn encode(&self) -> CatalogResult<Vec<u8>> {
        let encoded = serde_json::to_vec(&VersionedCatalogCommand {
            format_version: self.format_version(),
            command: self.clone(),
        })
        .map_err(|error| CatalogError::Decoding(error.to_string()))?;
        if encoded.len() > MAX_COMMAND_BYTES {
            return Err(CatalogError::CommandTooLarge);
        }
        Ok(encoded)
    }

    pub fn decode(payload: &[u8]) -> CatalogResult<Self> {
        if payload.len() > MAX_COMMAND_BYTES {
            return Err(CatalogError::CommandTooLarge);
        }
        let envelope: VersionedCatalogCommand = serde_json::from_slice(payload)
            .map_err(|error| CatalogError::Decoding(error.to_string()))?;
        if !matches!(
            envelope.format_version,
            CATALOG_COMMAND_FORMAT_VERSION
                | CATALOG_CONFIG_COMMAND_FORMAT_VERSION
                | CATALOG_GOVERNANCE_COMMAND_FORMAT_VERSION
                | CATALOG_PLACEMENT_COMMAND_FORMAT_VERSION
                | CATALOG_MEMBERSHIP_COMMAND_FORMAT_VERSION
                | CATALOG_CONTROL_COMMAND_FORMAT_VERSION
        ) {
            return Err(CatalogError::UnsupportedCommandVersion(
                envelope.format_version,
            ));
        }
        let command = envelope.command;
        if envelope.format_version != command.format_version() {
            return Err(CatalogError::UnsupportedCommandVersion(
                envelope.format_version,
            ));
        }
        if command.encode()?.as_slice() != payload {
            return Err(CatalogError::NonCanonicalCommand);
        }
        Ok(command)
    }

    const fn format_version(&self) -> u16 {
        match self {
            Self::ApplyDesired(_)
            | Self::DeleteDesired(_)
            | Self::DeleteManaged(_)
            | Self::ImportManaged(_)
            | Self::AcquireControlLease(_)
            | Self::UpdateManagedStatus(_)
            | Self::ReconcileManaged(_)
            | Self::PlanManagedMembership(_) => CATALOG_CONTROL_COMMAND_FORMAT_VERSION,
            Self::PlanMembership(_) | Self::FinalizeMembership(_) => {
                CATALOG_MEMBERSHIP_COMMAND_FORMAT_VERSION
            }
            Self::Apply(request) if !request.tablet_placements.is_empty() => {
                CATALOG_PLACEMENT_COMMAND_FORMAT_VERSION
            }
            Self::Apply(request) if request.spec.governance.is_some() => {
                CATALOG_GOVERNANCE_COMMAND_FORMAT_VERSION
            }
            Self::Apply(request) if request.spec.configuration.is_some() => {
                CATALOG_CONFIG_COMMAND_FORMAT_VERSION
            }
            Self::Apply(_) | Self::Delete(_) => CATALOG_COMMAND_FORMAT_VERSION,
        }
    }

    pub fn request_token(&self) -> &str {
        match self {
            Self::Apply(request) => &request.request_token,
            Self::Delete(request) => &request.request_token,
            Self::PlanMembership(request) => &request.request_token,
            Self::FinalizeMembership(request) => &request.request_token,
            Self::ApplyDesired(request) => &request.request_token,
            Self::DeleteDesired(request) => &request.request_token,
            Self::DeleteManaged(request) => &request.request_token,
            Self::ImportManaged(request) => &request.request_token,
            Self::AcquireControlLease(request) => &request.request_token,
            Self::UpdateManagedStatus(request) => &request.request_token,
            Self::ReconcileManaged(request) => &request.request_token,
            Self::PlanManagedMembership(request) => &request.request_token,
        }
    }

    fn resource_names(&self) -> Vec<ResourceName> {
        let mut names = match self {
            Self::Apply(request) => vec![request.name.clone()],
            Self::Delete(request) => vec![request.name.clone()],
            Self::PlanMembership(_)
            | Self::FinalizeMembership(_)
            | Self::AcquireControlLease(_) => Vec::new(),
            Self::ApplyDesired(request) => request
                .resources
                .iter()
                .map(|resource| resource.name.clone())
                .collect(),
            Self::DeleteDesired(request) => vec![request.name.clone()],
            Self::DeleteManaged(request) => vec![request.name.clone()],
            Self::ImportManaged(request) => request
                .resources
                .iter()
                .map(|resource| resource.name.clone())
                .chain(
                    request
                        .generations
                        .iter()
                        .map(|generation| generation.name.clone()),
                )
                .collect(),
            Self::UpdateManagedStatus(request) => vec![request.name.clone()],
            Self::ReconcileManaged(request) => request
                .resources
                .iter()
                .map(|resource| resource.name.clone())
                .collect(),
            Self::PlanManagedMembership(request) => vec![request.name.clone()],
        };
        names.sort();
        names.dedup();
        names
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CatalogMutation {
    Applied {
        resource: ResourceRecord,
        created: bool,
        changed: bool,
        replayed: bool,
    },
    Deleted {
        name: ResourceName,
        generation: u64,
        deleted: bool,
        replayed: bool,
    },
    DesiredApplied {
        resources: Vec<ManagedResourceApplyResult>,
        changed: bool,
        replayed: bool,
    },
    DesiredDeleted {
        name: ResourceName,
        generation: u64,
        deleted: bool,
        replayed: bool,
    },
    ManagedDeleted {
        name: ResourceName,
        desired_generation: u64,
        catalog_generation: u64,
        deleted: bool,
        replayed: bool,
    },
    ControlLeaseAcquired {
        lease: ControlLease,
        replayed: bool,
    },
    ManagedStatusUpdated {
        resource: ManagedResourceRecord,
        changed: bool,
        replayed: bool,
    },
    ManagedReconciled {
        resources: Vec<ResourceRecord>,
        changed: bool,
        replayed: bool,
    },
    Rejected {
        code: CatalogRejectionCode,
        message: String,
        replayed: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogRejectionCode {
    InvalidArgument,
    Conflict,
    Fenced,
    CapacityExceeded,
}

impl CatalogRejectionCode {
    const fn for_error(error: &CatalogError) -> Self {
        match error {
            CatalogError::ControlLeaseHeld { .. }
            | CatalogError::GenerationConflict { .. }
            | CatalogError::IdempotencyConflict
            | CatalogError::CapacityObservationConflict { .. }
            | CatalogError::StaleChangeCursor { .. } => Self::Conflict,
            CatalogError::ControlLeaseFenced { .. } | CatalogError::ControlLeaseExpired { .. } => {
                Self::Fenced
            }
            CatalogError::CapacityExceeded { .. } => Self::CapacityExceeded,
            _ => Self::InvalidArgument,
        }
    }
}

impl CatalogMutation {
    pub fn resource(&self) -> Option<&ResourceRecord> {
        match self {
            Self::Applied { resource, .. } => Some(resource),
            Self::Deleted { .. }
            | Self::DesiredApplied { .. }
            | Self::DesiredDeleted { .. }
            | Self::ManagedDeleted { .. }
            | Self::ControlLeaseAcquired { .. }
            | Self::ManagedStatusUpdated { .. }
            | Self::ManagedReconciled { .. }
            | Self::Rejected { .. } => None,
        }
    }

    fn as_replayed(&self) -> Self {
        match self {
            Self::Applied {
                resource,
                created,
                changed,
                ..
            } => Self::Applied {
                resource: resource.clone(),
                created: *created,
                changed: *changed,
                replayed: true,
            },
            Self::Deleted {
                name,
                generation,
                deleted,
                ..
            } => Self::Deleted {
                name: name.clone(),
                generation: *generation,
                deleted: *deleted,
                replayed: true,
            },
            Self::DesiredApplied {
                resources, changed, ..
            } => Self::DesiredApplied {
                resources: resources.clone(),
                changed: *changed,
                replayed: true,
            },
            Self::DesiredDeleted {
                name,
                generation,
                deleted,
                ..
            } => Self::DesiredDeleted {
                name: name.clone(),
                generation: *generation,
                deleted: *deleted,
                replayed: true,
            },
            Self::ManagedDeleted {
                name,
                desired_generation,
                catalog_generation,
                deleted,
                ..
            } => Self::ManagedDeleted {
                name: name.clone(),
                desired_generation: *desired_generation,
                catalog_generation: *catalog_generation,
                deleted: *deleted,
                replayed: true,
            },
            Self::ControlLeaseAcquired { lease, .. } => Self::ControlLeaseAcquired {
                lease: lease.clone(),
                replayed: true,
            },
            Self::ManagedStatusUpdated {
                resource, changed, ..
            } => Self::ManagedStatusUpdated {
                resource: resource.clone(),
                changed: *changed,
                replayed: true,
            },
            Self::ManagedReconciled {
                resources, changed, ..
            } => Self::ManagedReconciled {
                resources: resources.clone(),
                changed: *changed,
                replayed: true,
            },
            Self::Rejected { code, message, .. } => Self::Rejected {
                code: *code,
                message: message.clone(),
                replayed: true,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogChangeKind {
    DesiredApplied,
    DesiredDeleted,
    StatusUpdated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogChange {
    pub cursor: u64,
    pub kind: CatalogChangeKind,
    pub name: ResourceName,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogChangePage {
    pub earliest_cursor: u64,
    pub latest_cursor: u64,
    pub changes: Vec<CatalogChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogOperation {
    pub request_token: String,
    /// Canonical affected identities retained independently of the mutation
    /// result so rejected operations can still be authorized safely.
    pub resource_names: Vec<ResourceName>,
    pub mutation: CatalogMutation,
    pub first_change_cursor: u64,
    pub last_change_cursor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletRoute {
    pub resource: ResourceName,
    pub tablet: TabletDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletedRequest {
    command: CatalogCommand,
    mutation: CatalogMutation,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    first_change_cursor: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    last_change_cursor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceGeneration {
    pub name: ResourceName,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSnapshot {
    resources: Vec<ResourceRecord>,
    last_generations: Vec<ResourceGeneration>,
    next_tablet_id: u64,
    next_consensus_group_id: u64,
    reserved_consensus_group_ids: Vec<u64>,
    completed_requests: BTreeMap<String, CompletedRequest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    managed_resources: Vec<ManagedResourceRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    managed_last_generations: Vec<ResourceGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    control_lease: Option<ControlLease>,
    #[serde(default = "default_one_u64", skip_serializing_if = "is_one_u64")]
    next_control_fence: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    control_clock_ms: u64,
    #[serde(default = "default_one_u64", skip_serializing_if = "is_one_u64")]
    next_change_cursor: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    changes: Vec<CatalogChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedCatalogSnapshot {
    format_version: u16,
    state_digest: [u8; 32],
    snapshot: CatalogSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    resources: BTreeMap<ResourceName, ResourceRecord>,
    last_generations: BTreeMap<ResourceName, u64>,
    tablet_index: BTreeMap<u64, (ResourceName, u32)>,
    next_tablet_id: u64,
    next_consensus_group_id: u64,
    reserved_consensus_group_ids: BTreeSet<u64>,
    completed_requests: BTreeMap<String, CompletedRequest>,
    managed_resources: BTreeMap<ResourceName, ManagedResourceRecord>,
    managed_last_generations: BTreeMap<ResourceName, u64>,
    control_lease: Option<ControlLease>,
    next_control_fence: u64,
    control_clock_ms: u64,
    next_change_cursor: u64,
    changes: Vec<CatalogChange>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub const fn new() -> Self {
        Self {
            resources: BTreeMap::new(),
            last_generations: BTreeMap::new(),
            tablet_index: BTreeMap::new(),
            next_tablet_id: 1,
            next_consensus_group_id: 1,
            reserved_consensus_group_ids: BTreeSet::new(),
            completed_requests: BTreeMap::new(),
            managed_resources: BTreeMap::new(),
            managed_last_generations: BTreeMap::new(),
            control_lease: None,
            next_control_fence: 1,
            control_clock_ms: 0,
            next_change_cursor: 1,
            changes: Vec::new(),
        }
    }

    pub fn with_reserved_consensus_group(group_id: u64) -> CatalogResult<Self> {
        if group_id == 0 {
            return Err(CatalogError::InvalidSpec(
                "reserved consensus group ID must be non-zero".into(),
            ));
        }
        let mut catalog = Self::new();
        catalog.reserved_consensus_group_ids.insert(group_id);
        Ok(catalog)
    }

    pub fn apply(&mut self, command: CatalogCommand) -> CatalogResult<CatalogMutation> {
        validate_request_token(command.request_token())?;
        if let Some(completed) = self.completed_requests.get(command.request_token()) {
            if completed.command != command {
                return Err(CatalogError::IdempotencyConflict);
            }
            return Ok(completed.mutation.as_replayed());
        }

        let mutation = match &command {
            CatalogCommand::Apply(request) => self.apply_resource(request)?,
            CatalogCommand::Delete(request) => self.delete_resource(request)?,
            CatalogCommand::PlanMembership(request) => self.plan_tablet_membership(request)?,
            CatalogCommand::FinalizeMembership(request) => {
                self.finalize_tablet_membership(request)?
            }
            CatalogCommand::ApplyDesired(request) => self
                .apply_desired_resources(request)
                .unwrap_or_else(|error| rejected_mutation(&error)),
            CatalogCommand::DeleteDesired(request) => self
                .delete_desired_resource(request)
                .unwrap_or_else(|error| rejected_mutation(&error)),
            CatalogCommand::DeleteManaged(request) => self
                .delete_managed_resource(request)
                .unwrap_or_else(|error| rejected_mutation(&error)),
            CatalogCommand::ImportManaged(request) => self
                .import_managed_resources(request)
                .unwrap_or_else(|error| rejected_mutation(&error)),
            CatalogCommand::AcquireControlLease(request) => self
                .acquire_control_lease(request)
                .unwrap_or_else(|error| rejected_mutation(&error)),
            CatalogCommand::UpdateManagedStatus(request) => self
                .update_managed_status(request)
                .unwrap_or_else(|error| rejected_mutation(&error)),
            CatalogCommand::ReconcileManaged(request) => self
                .reconcile_managed_resources(request)
                .unwrap_or_else(|error| rejected_mutation(&error)),
            CatalogCommand::PlanManagedMembership(request) => self
                .plan_managed_tablet_membership(request)
                .unwrap_or_else(|error| rejected_mutation(&error)),
        };
        let (first_change_cursor, last_change_cursor) = self.record_changes(&mutation)?;
        self.completed_requests.insert(
            command.request_token().to_owned(),
            CompletedRequest {
                command,
                mutation: mutation.clone(),
                first_change_cursor,
                last_change_cursor,
            },
        );
        self.prune_transient_control_requests();
        Ok(mutation)
    }

    fn prune_transient_control_requests(&mut self) {
        let mut transient = self
            .completed_requests
            .iter()
            .filter_map(|(token, completed)| {
                let clock = match &completed.command {
                    CatalogCommand::AcquireControlLease(request) => request.now_ms,
                    CatalogCommand::UpdateManagedStatus(request) => request.lease.now_ms,
                    _ => return None,
                };
                Some((clock, token.clone()))
            })
            .collect::<Vec<_>>();
        if transient.len() <= MAX_TRANSIENT_CONTROL_REQUESTS {
            return;
        }
        transient.sort();
        let remove = transient.len() - MAX_TRANSIENT_CONTROL_REQUESTS;
        for (_, token) in transient.into_iter().take(remove) {
            self.completed_requests.remove(&token);
        }
    }

    pub fn resource(&self, name: &ResourceName) -> CatalogResult<&ResourceRecord> {
        self.resources
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.canonical_name()))
    }

    pub fn resources(&self) -> impl ExactSizeIterator<Item = &ResourceRecord> {
        self.resources.values()
    }

    pub fn managed_resource(&self, name: &ResourceName) -> CatalogResult<&ManagedResourceRecord> {
        self.managed_resources
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.canonical_name()))
    }

    pub fn managed_resources(&self) -> impl ExactSizeIterator<Item = &ManagedResourceRecord> {
        self.managed_resources.values()
    }

    pub fn managed_resource_count(&self) -> usize {
        self.managed_resources.len()
    }

    pub fn node_allocations(&self) -> CatalogResult<BTreeMap<u64, u32>> {
        catalog_group_allocations(&self.resources)
    }

    pub fn control_lease(&self) -> Option<&ControlLease> {
        self.control_lease.as_ref()
    }

    pub const fn latest_change_cursor(&self) -> u64 {
        self.next_change_cursor.saturating_sub(1)
    }

    pub fn changes_after(&self, cursor: u64, limit: usize) -> CatalogResult<CatalogChangePage> {
        if limit == 0 || limit > MAX_CHANGE_PAGE_SIZE {
            return Err(CatalogError::InvalidSpec(format!(
                "change page size must be between 1 and {MAX_CHANGE_PAGE_SIZE}"
            )));
        }
        let latest = self.latest_change_cursor();
        if cursor > latest {
            return Err(CatalogError::InvalidSpec(format!(
                "change cursor {cursor} is in the future; latest cursor is {latest}"
            )));
        }
        let earliest = self
            .changes
            .first()
            .map_or(self.next_change_cursor, |change| change.cursor);
        if cursor != 0 && cursor.saturating_add(1) < earliest {
            return Err(CatalogError::StaleChangeCursor {
                requested: cursor,
                earliest,
            });
        }
        Ok(CatalogChangePage {
            earliest_cursor: earliest,
            latest_cursor: latest,
            changes: self
                .changes
                .iter()
                .filter(|change| change.cursor > cursor)
                .take(limit)
                .cloned()
                .collect(),
        })
    }

    pub fn operation(&self, request_token: &str) -> Option<CatalogOperation> {
        self.completed_requests
            .get(request_token)
            .map(|completed| CatalogOperation {
                request_token: request_token.to_owned(),
                resource_names: completed.command.resource_names(),
                mutation: completed.mutation.clone(),
                first_change_cursor: completed.first_change_cursor,
                last_change_cursor: completed.last_change_cursor,
            })
    }

    pub fn route(&self, name: &ResourceName, shard_index: u32) -> CatalogResult<&TabletDescriptor> {
        self.resource(name)?
            .tablets
            .get(usize::try_from(shard_index).map_err(|_| {
                CatalogError::InvalidSpec("shard index cannot be represented".into())
            })?)
            .filter(|tablet| tablet.shard_index == shard_index)
            .ok_or_else(|| {
                CatalogError::NotFound(format!("{} shard {shard_index}", name.canonical_name()))
            })
    }

    pub fn tablet(&self, tablet_id: u64) -> CatalogResult<TabletRoute> {
        let (name, shard_index) = self
            .tablet_index
            .get(&tablet_id)
            .ok_or_else(|| CatalogError::NotFound(format!("tablet {tablet_id}")))?;
        Ok(TabletRoute {
            resource: name.clone(),
            tablet: self.route(name, *shard_index)?.clone(),
        })
    }

    pub fn resource_count(&self) -> usize {
        self.resources.len()
    }

    pub fn tablet_count(&self) -> usize {
        self.tablet_index.len()
    }

    pub fn is_consensus_group_reserved(&self, group_id: u64) -> bool {
        self.reserved_consensus_group_ids.contains(&group_id)
    }

    pub fn snapshot(&self) -> CatalogSnapshot {
        CatalogSnapshot {
            resources: self.resources.values().cloned().collect(),
            last_generations: self
                .last_generations
                .iter()
                .map(|(name, generation)| ResourceGeneration {
                    name: name.clone(),
                    generation: *generation,
                })
                .collect(),
            next_tablet_id: self.next_tablet_id,
            next_consensus_group_id: self.next_consensus_group_id,
            reserved_consensus_group_ids: self
                .reserved_consensus_group_ids
                .iter()
                .copied()
                .collect(),
            completed_requests: self.completed_requests.clone(),
            managed_resources: self.managed_resources.values().cloned().collect(),
            managed_last_generations: self
                .managed_last_generations
                .iter()
                .map(|(name, generation)| ResourceGeneration {
                    name: name.clone(),
                    generation: *generation,
                })
                .collect(),
            control_lease: self.control_lease.clone(),
            next_control_fence: self.next_control_fence,
            control_clock_ms: self.control_clock_ms,
            next_change_cursor: self.next_change_cursor,
            changes: self.changes.clone(),
        }
    }

    pub fn state_digest(&self) -> CatalogResult<[u8; 32]> {
        let encoded = serde_json::to_vec(&self.snapshot())
            .map_err(|error| CatalogError::Encoding(error.to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(b"epoch/catalog/state/v1\0");
        hasher.update(
            u64::try_from(encoded.len())
                .map_err(|_| CatalogError::IdentityExhausted)?
                .to_be_bytes(),
        );
        hasher.update(encoded);
        Ok(hasher.finalize().into())
    }

    pub fn encode_snapshot(&self) -> CatalogResult<Vec<u8>> {
        let encoded = serde_json::to_vec(&VersionedCatalogSnapshot {
            format_version: self.snapshot_format_version(),
            state_digest: self.state_digest()?,
            snapshot: self.snapshot(),
        })
        .map_err(|error| CatalogError::Encoding(error.to_string()))?;
        if encoded.len() > MAX_CATALOG_SNAPSHOT_BYTES {
            return Err(CatalogError::SnapshotTooLarge);
        }
        Ok(encoded)
    }

    pub fn decode_snapshot(encoded: &[u8]) -> CatalogResult<Self> {
        if encoded.len() > MAX_CATALOG_SNAPSHOT_BYTES {
            return Err(CatalogError::SnapshotTooLarge);
        }
        let envelope: VersionedCatalogSnapshot = serde_json::from_slice(encoded)
            .map_err(|error| CatalogError::Decoding(error.to_string()))?;
        if !matches!(
            envelope.format_version,
            CATALOG_SNAPSHOT_FORMAT_VERSION
                | CATALOG_CONFIG_SNAPSHOT_FORMAT_VERSION
                | CATALOG_GOVERNANCE_SNAPSHOT_FORMAT_VERSION
                | CATALOG_PLACEMENT_SNAPSHOT_FORMAT_VERSION
                | CATALOG_MEMBERSHIP_SNAPSHOT_FORMAT_VERSION
                | CATALOG_CONTROL_SNAPSHOT_FORMAT_VERSION
        ) {
            return Err(CatalogError::UnsupportedSnapshotVersion(
                envelope.format_version,
            ));
        }
        let catalog = Self::from_snapshot(envelope.snapshot)?;
        if envelope.format_version != catalog.snapshot_format_version() {
            return Err(CatalogError::UnsupportedSnapshotVersion(
                envelope.format_version,
            ));
        }
        if catalog.state_digest()? != envelope.state_digest {
            return Err(CatalogError::SnapshotDigestMismatch);
        }
        if catalog.encode_snapshot()?.as_slice() != encoded {
            return Err(CatalogError::NonCanonicalSnapshot);
        }
        Ok(catalog)
    }

    fn snapshot_format_version(&self) -> u16 {
        if !self.managed_resources.is_empty()
            || !self.managed_last_generations.is_empty()
            || self.control_lease.is_some()
            || self.control_clock_ms != 0
            || self.next_control_fence != 1
            || self.next_change_cursor != 1
            || !self.changes.is_empty()
            || self.completed_requests.values().any(|completed| {
                matches!(
                    completed.command,
                    CatalogCommand::ApplyDesired(_)
                        | CatalogCommand::DeleteDesired(_)
                        | CatalogCommand::DeleteManaged(_)
                        | CatalogCommand::ImportManaged(_)
                        | CatalogCommand::AcquireControlLease(_)
                        | CatalogCommand::UpdateManagedStatus(_)
                        | CatalogCommand::ReconcileManaged(_)
                        | CatalogCommand::PlanManagedMembership(_)
                )
            })
        {
            return CATALOG_CONTROL_SNAPSHOT_FORMAT_VERSION;
        }
        let membership_resource = self.resources.values().any(|resource| {
            resource.tablets.iter().any(|tablet| {
                !tablet.bootstrap_voter_node_ids.is_empty()
                    || !tablet.target_voter_node_ids.is_empty()
            })
        });
        let membership_request = self.completed_requests.values().any(|completed| {
            matches!(
                completed.command,
                CatalogCommand::PlanMembership(_) | CatalogCommand::FinalizeMembership(_)
            )
        });
        if membership_resource || membership_request {
            return CATALOG_MEMBERSHIP_SNAPSHOT_FORMAT_VERSION;
        }
        let placed_resource = self.resources.values().any(|resource| {
            resource
                .tablets
                .iter()
                .any(|tablet| !tablet.voter_node_ids.is_empty())
        });
        let placed_request = self.completed_requests.values().any(|completed| {
            matches!(
                &completed.command,
                CatalogCommand::Apply(request) if !request.tablet_placements.is_empty()
            )
        });
        if placed_resource || placed_request {
            return CATALOG_PLACEMENT_SNAPSHOT_FORMAT_VERSION;
        }
        let governed_resource = self
            .resources
            .values()
            .any(|resource| resource.spec.governance.is_some());
        let governed_request = self.completed_requests.values().any(|completed| {
            matches!(
                &completed.command,
                CatalogCommand::Apply(request) if request.spec.governance.is_some()
            )
        });
        if governed_resource || governed_request {
            return CATALOG_GOVERNANCE_SNAPSHOT_FORMAT_VERSION;
        }
        let configured_resource = self
            .resources
            .values()
            .any(|resource| resource.spec.configuration.is_some());
        let configured_request = self.completed_requests.values().any(|completed| {
            matches!(
                &completed.command,
                CatalogCommand::Apply(request) if request.spec.configuration.is_some()
            )
        });
        if configured_resource || configured_request {
            CATALOG_CONFIG_SNAPSHOT_FORMAT_VERSION
        } else {
            CATALOG_SNAPSHOT_FORMAT_VERSION
        }
    }

    fn from_snapshot(snapshot: CatalogSnapshot) -> CatalogResult<Self> {
        let reserved_consensus_group_ids = snapshot
            .reserved_consensus_group_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        validate_snapshot_identity(&snapshot)?;
        let restored =
            restore_snapshot_resources(&snapshot.resources, &reserved_consensus_group_ids)?;
        let last_generations =
            restore_snapshot_generations(&snapshot.last_generations, &restored.resources)?;
        let managed_resources = restore_managed_resources(&snapshot.managed_resources)?;
        let managed_last_generations =
            restore_managed_generations(&snapshot.managed_last_generations, &managed_resources)?;
        validate_snapshot_high_water_marks(
            snapshot.next_tablet_id,
            snapshot.next_consensus_group_id,
            &restored.tablet_index,
            &restored.allocated_groups,
        )?;
        validate_completed_requests(&snapshot.completed_requests)?;
        validate_control_snapshot(
            snapshot.control_lease.as_ref(),
            snapshot.next_control_fence,
            snapshot.control_clock_ms,
            snapshot.next_change_cursor,
            &snapshot.changes,
            &managed_last_generations,
        )?;

        Ok(Self {
            resources: restored.resources,
            last_generations,
            tablet_index: restored.tablet_index,
            next_tablet_id: snapshot.next_tablet_id,
            next_consensus_group_id: snapshot.next_consensus_group_id,
            reserved_consensus_group_ids,
            completed_requests: snapshot.completed_requests,
            managed_resources,
            managed_last_generations,
            control_lease: snapshot.control_lease,
            next_control_fence: snapshot.next_control_fence,
            control_clock_ms: snapshot.control_clock_ms,
            next_change_cursor: snapshot.next_change_cursor,
            changes: snapshot.changes,
        })
    }

    fn apply_desired_resources(
        &mut self,
        request: &ApplyDesiredResources,
    ) -> CatalogResult<CatalogMutation> {
        validate_managed_batch_len(request.resources.len())?;
        validate_strictly_sorted_resource_writes(&request.resources)?;
        let mut next_resources = self.managed_resources.clone();
        let mut next_generations = self.managed_last_generations.clone();
        let mut results = Vec::with_capacity(request.resources.len());
        let mut changed = false;

        for write in &request.resources {
            write.name.validate()?;
            validate_managed_document("desired resource", &write.desired)?;
            let current = next_resources.get(&write.name).cloned();
            let actual_generation = current.as_ref().map_or(0, |resource| resource.generation);
            validate_expected_generation(write.expected_generation, actual_generation)?;
            if let Some(resource) = current
                .as_ref()
                .filter(|resource| {
                    !resource.deletion_requested && resource.desired == write.desired
                })
                .cloned()
            {
                results.push(ManagedResourceApplyResult {
                    resource,
                    created: false,
                    changed: false,
                });
                continue;
            }
            let previous_generation = current.as_ref().map_or_else(
                || next_generations.get(&write.name).copied().unwrap_or(0),
                |resource| resource.generation,
            );
            let generation = next_generation(previous_generation)?;
            let resource = ManagedResourceRecord {
                name: write.name.clone(),
                generation,
                desired: write.desired.clone(),
                status: current
                    .as_ref()
                    .map_or_else(default_managed_status, |resource| resource.status.clone()),
                deletion_requested: false,
            };
            next_resources.insert(write.name.clone(), resource.clone());
            next_generations.insert(write.name.clone(), generation);
            results.push(ManagedResourceApplyResult {
                resource,
                created: current.is_none(),
                changed: true,
            });
            changed = true;
        }

        self.managed_resources = next_resources;
        self.managed_last_generations = next_generations;
        Ok(CatalogMutation::DesiredApplied {
            resources: results,
            changed,
            replayed: false,
        })
    }

    fn delete_desired_resource(
        &mut self,
        request: &DeleteDesiredResource,
    ) -> CatalogResult<CatalogMutation> {
        request.name.validate()?;
        let current = self.managed_resources.get(&request.name).cloned();
        let actual_generation = current.as_ref().map_or(0, |resource| resource.generation);
        validate_expected_generation(request.expected_generation, actual_generation)?;
        let Some(resource) = current else {
            return Ok(CatalogMutation::DesiredDeleted {
                name: request.name.clone(),
                generation: self
                    .managed_last_generations
                    .get(&request.name)
                    .copied()
                    .unwrap_or(0),
                deleted: false,
                replayed: false,
            });
        };
        let generation = next_generation(resource.generation)?;
        self.managed_resources.remove(&request.name);
        self.managed_last_generations
            .insert(request.name.clone(), generation);
        Ok(CatalogMutation::DesiredDeleted {
            name: request.name.clone(),
            generation,
            deleted: true,
            replayed: false,
        })
    }

    fn delete_managed_resource(
        &mut self,
        request: &DeleteManagedResource,
    ) -> CatalogResult<CatalogMutation> {
        self.validate_control_guard(&request.lease)?;
        request.name.validate()?;
        let desired = self.managed_resource(&request.name)?.clone();
        validate_expected_generation(
            Some(request.expected_desired_generation),
            desired.generation,
        )?;

        let mut candidate = self.clone();
        let catalog_generation = if candidate.resources.contains_key(&request.name) {
            let mutation = candidate.delete_resource(&DeleteResource {
                request_token: request.request_token.clone(),
                expected_generation: Some(request.expected_catalog_generation),
                name: request.name.clone(),
            })?;
            let CatalogMutation::Deleted { generation, .. } = mutation else {
                unreachable!("native delete returns a delete mutation")
            };
            generation
        } else {
            validate_expected_generation(Some(request.expected_catalog_generation), 0)?;
            candidate
                .last_generations
                .get(&request.name)
                .copied()
                .unwrap_or(0)
        };
        let desired_generation = next_generation(desired.generation)?;
        candidate.managed_resources.remove(&request.name);
        candidate
            .managed_last_generations
            .insert(request.name.clone(), desired_generation);
        candidate.control_clock_ms = request.lease.now_ms;
        *self = candidate;
        Ok(CatalogMutation::ManagedDeleted {
            name: request.name.clone(),
            desired_generation,
            catalog_generation,
            deleted: true,
            replayed: false,
        })
    }

    fn import_managed_resources(
        &mut self,
        request: &ImportManagedResources,
    ) -> CatalogResult<CatalogMutation> {
        validate_managed_import(request)?;
        if !self.managed_resources.is_empty() || !self.managed_last_generations.is_empty() {
            return Err(CatalogError::GenerationConflict {
                expected: 0,
                actual: 1,
            });
        }
        let generations = request
            .generations
            .iter()
            .map(|entry| (entry.name.clone(), entry.generation))
            .collect::<BTreeMap<_, _>>();
        let resources = request
            .resources
            .iter()
            .map(|resource| (resource.name.clone(), resource.clone()))
            .collect::<BTreeMap<_, _>>();
        let results = request
            .resources
            .iter()
            .cloned()
            .map(|resource| ManagedResourceApplyResult {
                resource,
                created: true,
                changed: true,
            })
            .collect();
        self.managed_resources = resources;
        self.managed_last_generations = generations;
        Ok(CatalogMutation::DesiredApplied {
            resources: results,
            changed: !request.resources.is_empty(),
            replayed: false,
        })
    }

    fn acquire_control_lease(
        &mut self,
        request: &AcquireControlLease,
    ) -> CatalogResult<CatalogMutation> {
        validate_control_owner(&request.owner_id)?;
        if request.now_ms == 0 || request.now_ms < self.control_clock_ms {
            return Err(CatalogError::InvalidSpec(
                "control lease time must be non-zero and monotonic".into(),
            ));
        }
        if !(MIN_CONTROL_LEASE_TTL_MS..=MAX_CONTROL_LEASE_TTL_MS).contains(&request.ttl_ms) {
            return Err(CatalogError::InvalidSpec(format!(
                "control lease TTL must be between {MIN_CONTROL_LEASE_TTL_MS} and {MAX_CONTROL_LEASE_TTL_MS} milliseconds"
            )));
        }
        let valid_until_ms = request
            .now_ms
            .checked_add(request.ttl_ms)
            .ok_or(CatalogError::IdentityExhausted)?;
        let active = self
            .control_lease
            .as_ref()
            .filter(|lease| request.now_ms < lease.valid_until_ms);
        let fence = match active {
            Some(lease) if lease.owner_id == request.owner_id => lease.fence,
            Some(lease) => {
                return Err(CatalogError::ControlLeaseHeld {
                    owner_id: lease.owner_id.clone(),
                    valid_until_ms: lease.valid_until_ms,
                });
            }
            None => {
                let fence = self.next_control_fence;
                self.next_control_fence = self
                    .next_control_fence
                    .checked_add(1)
                    .ok_or(CatalogError::IdentityExhausted)?;
                fence
            }
        };
        let lease = ControlLease {
            owner_id: request.owner_id.clone(),
            fence,
            valid_until_ms,
        };
        self.control_clock_ms = request.now_ms;
        self.control_lease = Some(lease.clone());
        Ok(CatalogMutation::ControlLeaseAcquired {
            lease,
            replayed: false,
        })
    }

    fn update_managed_status(
        &mut self,
        request: &UpdateManagedResourceStatus,
    ) -> CatalogResult<CatalogMutation> {
        self.validate_control_guard(&request.lease)?;
        request.name.validate()?;
        validate_managed_document("managed resource status", &request.status)?;
        let current = self
            .managed_resources
            .get(&request.name)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(request.name.canonical_name()))?;
        validate_expected_generation(Some(request.expected_generation), current.generation)?;
        if current.deletion_requested {
            return Err(CatalogError::InvalidSpec(
                "cannot update status for a resource pending deletion".into(),
            ));
        }
        let changed = current.status != request.status;
        let mut resource = current;
        if changed {
            resource.status.clone_from(&request.status);
            self.managed_resources
                .insert(request.name.clone(), resource.clone());
        }
        self.control_clock_ms = request.lease.now_ms;
        Ok(CatalogMutation::ManagedStatusUpdated {
            resource,
            changed,
            replayed: false,
        })
    }

    fn reconcile_managed_resources(
        &mut self,
        request: &ReconcileManagedResources,
    ) -> CatalogResult<CatalogMutation> {
        self.validate_control_guard(&request.lease)?;
        validate_managed_batch_len(request.resources.len())?;
        validate_strictly_sorted_placements(&request.resources)?;
        let current_allocations = catalog_group_allocations(&self.resources)?;
        validate_capacity_observations(&request.capacity, &current_allocations)?;

        let mut candidate = self.clone();
        let mut results = Vec::with_capacity(request.resources.len());
        let mut changed = false;
        for placement in &request.resources {
            let desired = candidate.managed_resource(&placement.name)?;
            if desired.deletion_requested {
                return Err(CatalogError::InvalidSpec(format!(
                    "{} is pending deletion",
                    placement.name.canonical_name()
                )));
            }
            validate_expected_generation(
                Some(placement.expected_desired_generation),
                desired.generation,
            )?;
            let mutation = candidate.apply_resource(&ApplyResource {
                request_token: request.request_token.clone(),
                expected_generation: Some(placement.expected_catalog_generation),
                name: placement.name.clone(),
                spec: placement.spec.clone(),
                tablet_placements: placement.tablet_placements.clone(),
            })?;
            let CatalogMutation::Applied {
                resource,
                changed: resource_changed,
                ..
            } = mutation
            else {
                unreachable!("native apply returns an applied resource")
            };
            changed |= resource_changed;
            results.push(resource);
        }
        let next_allocations = catalog_group_allocations(&candidate.resources)?;
        validate_reserved_capacity(&request.capacity, &next_allocations)?;
        candidate.control_clock_ms = request.lease.now_ms;
        *self = candidate;
        Ok(CatalogMutation::ManagedReconciled {
            resources: results,
            changed,
            replayed: false,
        })
    }

    fn plan_managed_tablet_membership(
        &mut self,
        request: &PlanManagedTabletMembership,
    ) -> CatalogResult<CatalogMutation> {
        self.validate_control_guard(&request.lease)?;
        request.name.validate()?;
        let desired = self.managed_resource(&request.name)?;
        validate_expected_generation(
            Some(request.expected_desired_generation),
            desired.generation,
        )?;
        if desired.deletion_requested {
            return Err(CatalogError::InvalidSpec(format!(
                "{} is pending deletion",
                request.name.canonical_name()
            )));
        }
        let route = self.tablet(request.tablet_id)?;
        if route.resource != request.name {
            return Err(CatalogError::InvalidSpec(format!(
                "tablet {} does not belong to {}",
                request.tablet_id,
                request.name.canonical_name()
            )));
        }
        let current_allocations = catalog_group_allocations(&self.resources)?;
        validate_capacity_observations(&request.capacity, &current_allocations)?;

        let mut candidate = self.clone();
        let mutation = candidate.plan_tablet_membership(&PlanTabletMembership {
            request_token: request.request_token.clone(),
            tablet_id: request.tablet_id,
            expected_tablet_epoch: request.expected_tablet_epoch,
            expected_resource_generation: request.expected_resource_generation,
            target_voter_node_ids: request.target_voter_node_ids.clone(),
        })?;
        let next_allocations = catalog_group_allocations(&candidate.resources)?;
        validate_reserved_capacity(&request.capacity, &next_allocations)?;
        candidate.control_clock_ms = request.lease.now_ms;
        *self = candidate;
        Ok(mutation)
    }

    fn validate_control_guard(&self, guard: &ControlLeaseGuard) -> CatalogResult<()> {
        validate_control_owner(&guard.owner_id)?;
        let Some(active) = self.control_lease.as_ref() else {
            return Err(CatalogError::ControlLeaseFenced {
                expected_owner: String::new(),
                expected_fence: 0,
            });
        };
        if active.owner_id != guard.owner_id || active.fence != guard.fence {
            return Err(CatalogError::ControlLeaseFenced {
                expected_owner: active.owner_id.clone(),
                expected_fence: active.fence,
            });
        }
        if guard.now_ms < self.control_clock_ms {
            return Err(CatalogError::InvalidSpec(
                "control mutation time must be monotonic".into(),
            ));
        }
        if guard.now_ms >= active.valid_until_ms {
            return Err(CatalogError::ControlLeaseExpired {
                valid_until_ms: active.valid_until_ms,
            });
        }
        Ok(())
    }

    fn record_changes(&mut self, mutation: &CatalogMutation) -> CatalogResult<(u64, u64)> {
        let pending = match mutation {
            CatalogMutation::DesiredApplied {
                resources,
                changed: true,
                ..
            } => resources
                .iter()
                .filter(|resource| resource.changed)
                .map(|resource| {
                    (
                        CatalogChangeKind::DesiredApplied,
                        resource.resource.name.clone(),
                        resource.resource.generation,
                    )
                })
                .collect::<Vec<_>>(),
            CatalogMutation::ManagedStatusUpdated {
                resource,
                changed: true,
                ..
            } => vec![(
                CatalogChangeKind::StatusUpdated,
                resource.name.clone(),
                resource.generation,
            )],
            CatalogMutation::DesiredDeleted {
                name,
                generation,
                deleted: true,
                ..
            } => vec![(CatalogChangeKind::DesiredDeleted, name.clone(), *generation)],
            CatalogMutation::ManagedDeleted {
                name,
                desired_generation,
                deleted: true,
                ..
            } => vec![(
                CatalogChangeKind::DesiredDeleted,
                name.clone(),
                *desired_generation,
            )],
            _ => Vec::new(),
        };
        if pending.is_empty() {
            return Ok((0, 0));
        }
        let pending_len =
            u64::try_from(pending.len()).map_err(|_| CatalogError::IdentityExhausted)?;
        self.next_change_cursor
            .checked_add(pending_len)
            .ok_or(CatalogError::IdentityExhausted)?;
        let first = self.next_change_cursor;
        for (kind, name, generation) in pending {
            self.changes.push(CatalogChange {
                cursor: self.next_change_cursor,
                kind,
                name,
                generation,
            });
            self.next_change_cursor += 1;
        }
        if self.changes.len() > MAX_CHANGE_HISTORY {
            let excess = self.changes.len() - MAX_CHANGE_HISTORY;
            self.changes.drain(..excess);
        }
        Ok((first, self.next_change_cursor - 1))
    }

    fn apply_resource(&mut self, request: &ApplyResource) -> CatalogResult<CatalogMutation> {
        request.name.validate()?;
        let current = self.resources.get(&request.name).cloned();
        let actual_generation = current.as_ref().map_or(0, |resource| resource.generation);
        validate_expected_generation(request.expected_generation, actual_generation)?;

        if let Some(resource) = current.as_ref()
            && resource.spec.workload_profile != request.spec.workload_profile
        {
            return Err(CatalogError::ProfileMismatch {
                current: resource.spec.workload_profile,
                requested: request.spec.workload_profile,
            });
        }
        request.spec.validate(request.name.kind)?;
        let placements = validate_tablet_placements(request, current.as_ref())?;

        if let Some(resource) = current.as_ref() {
            if resource.spec.configuration != request.spec.configuration {
                return Err(CatalogError::ConfigurationMismatch);
            }
            if request.spec.shard_count < resource.spec.shard_count {
                return Err(CatalogError::ShardCountDecrease {
                    current: resource.spec.shard_count,
                    requested: request.spec.shard_count,
                });
            }
            if let Some(placements) = placements.as_ref() {
                for tablet in &resource.tablets {
                    if placements.get(&tablet.shard_index) != Some(&tablet.voter_node_ids) {
                        return Err(CatalogError::InvalidSpec(format!(
                            "tablet {} placement changes require a learner-first membership plan",
                            tablet.tablet_id
                        )));
                    }
                }
            }
            let placement_unchanged = placements.as_ref().is_none_or(|placements| {
                resource.tablets.iter().all(|tablet| {
                    placements.get(&tablet.shard_index) == Some(&tablet.voter_node_ids)
                })
            });
            if resource.spec == request.spec && placement_unchanged {
                return Ok(CatalogMutation::Applied {
                    resource: resource.clone(),
                    created: false,
                    changed: false,
                    replayed: false,
                });
            }
        }

        let generation = next_generation(current.as_ref().map_or_else(
            || {
                self.last_generations
                    .get(&request.name)
                    .copied()
                    .unwrap_or(0)
            },
            |r| r.generation,
        ))?;
        let tablets =
            self.tablets_for_apply(request, current.as_ref(), placements.as_ref(), generation)?;
        let resource = ResourceRecord {
            name: request.name.clone(),
            generation,
            spec: request.spec.clone(),
            tablets,
        };
        for tablet in &resource.tablets {
            self.tablet_index.insert(
                tablet.tablet_id,
                (resource.name.clone(), tablet.shard_index),
            );
        }
        let created = !self.resources.contains_key(&request.name);
        self.resources
            .insert(request.name.clone(), resource.clone());
        self.last_generations
            .insert(request.name.clone(), generation);
        Ok(CatalogMutation::Applied {
            resource,
            created,
            changed: true,
            replayed: false,
        })
    }

    fn tablets_for_apply(
        &mut self,
        request: &ApplyResource,
        current: Option<&ResourceRecord>,
        placements: Option<&BTreeMap<u32, Vec<u64>>>,
        generation: u64,
    ) -> CatalogResult<Vec<TabletDescriptor>> {
        let mut tablets = current.map_or_else(Vec::new, |resource| resource.tablets.clone());
        for tablet in &mut tablets {
            tablet.resource_generation = generation;
            tablet.replica_count = request.spec.replica_count;
            if let Some(placements) = placements {
                tablet.voter_node_ids = placements
                    .get(&tablet.shard_index)
                    .expect("validated placements cover every requested shard")
                    .clone();
            }
        }
        let additional = request
            .spec
            .shard_count
            .checked_sub(u32::try_from(tablets.len()).map_err(|_| CatalogError::IdentityExhausted)?)
            .ok_or(CatalogError::IdentityExhausted)?;
        self.ensure_identity_capacity(additional)?;
        for shard_index in u32::try_from(tablets.len())
            .map_err(|_| CatalogError::IdentityExhausted)?
            ..request.spec.shard_count
        {
            let consensus_group_id = self.allocate_consensus_group_id()?;
            let tablet = TabletDescriptor {
                tablet_id: self.next_tablet_id,
                consensus_group_id,
                shard_index,
                tablet_epoch: 1,
                resource_generation: generation,
                workload_profile: request.spec.workload_profile,
                replica_count: request.spec.replica_count,
                voter_node_ids: placements.map_or_else(Vec::new, |placements| {
                    placements
                        .get(&shard_index)
                        .expect("validated placements cover every requested shard")
                        .clone()
                }),
                bootstrap_voter_node_ids: Vec::new(),
                target_voter_node_ids: Vec::new(),
            };
            self.next_tablet_id += 1;
            tablets.push(tablet);
        }
        Ok(tablets)
    }

    fn plan_tablet_membership(
        &mut self,
        request: &PlanTabletMembership,
    ) -> CatalogResult<CatalogMutation> {
        let (name, shard_index) = self.tablet_identity(request.tablet_id)?;
        let current = self
            .resources
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(name.canonical_name()))?;
        validate_expected_generation(
            Some(request.expected_resource_generation),
            current.generation,
        )?;
        let tablet = current
            .tablets
            .get(usize::try_from(shard_index).map_err(|_| CatalogError::IdentityExhausted)?)
            .filter(|tablet| tablet.tablet_id == request.tablet_id)
            .ok_or_else(|| CatalogError::NotFound(format!("tablet {}", request.tablet_id)))?;
        validate_membership_request_identity(
            tablet,
            request.expected_tablet_epoch,
            &request.target_voter_node_ids,
        )?;
        if !tablet.target_voter_node_ids.is_empty() {
            if tablet.target_voter_node_ids == request.target_voter_node_ids {
                return Ok(unchanged_resource_mutation(current));
            }
            return Err(CatalogError::InvalidSpec(format!(
                "tablet {} already has a different membership transition",
                request.tablet_id
            )));
        }
        if tablet.voter_node_ids == request.target_voter_node_ids {
            return Ok(unchanged_resource_mutation(current));
        }
        validate_single_voter_replacement(&tablet.voter_node_ids, &request.target_voter_node_ids)?;

        let mut resource = current;
        for descriptor in &mut resource.tablets {
            if descriptor.tablet_id == request.tablet_id {
                if descriptor.bootstrap_voter_node_ids.is_empty() {
                    descriptor
                        .bootstrap_voter_node_ids
                        .clone_from(&descriptor.voter_node_ids);
                }
                descriptor
                    .target_voter_node_ids
                    .clone_from(&request.target_voter_node_ids);
            }
        }
        self.resources.insert(name.clone(), resource.clone());
        Ok(changed_resource_mutation(resource))
    }

    fn finalize_tablet_membership(
        &mut self,
        request: &FinalizeTabletMembership,
    ) -> CatalogResult<CatalogMutation> {
        let (name, shard_index) = self.tablet_identity(request.tablet_id)?;
        let current = self
            .resources
            .get(&name)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(name.canonical_name()))?;
        validate_expected_generation(
            Some(request.expected_resource_generation),
            current.generation,
        )?;
        let tablet = current
            .tablets
            .get(usize::try_from(shard_index).map_err(|_| CatalogError::IdentityExhausted)?)
            .filter(|tablet| tablet.tablet_id == request.tablet_id)
            .ok_or_else(|| CatalogError::NotFound(format!("tablet {}", request.tablet_id)))?;
        validate_membership_request_identity(
            tablet,
            request.expected_tablet_epoch,
            &request.target_voter_node_ids,
        )?;
        if tablet.target_voter_node_ids != request.target_voter_node_ids {
            return Err(CatalogError::InvalidSpec(format!(
                "tablet {} membership target is not the committed plan",
                request.tablet_id
            )));
        }

        let mut resource = current;
        for descriptor in &mut resource.tablets {
            if descriptor.tablet_id == request.tablet_id {
                descriptor
                    .voter_node_ids
                    .clone_from(&request.target_voter_node_ids);
                descriptor.target_voter_node_ids.clear();
            }
        }
        self.resources.insert(name.clone(), resource.clone());
        Ok(changed_resource_mutation(resource))
    }

    fn tablet_identity(&self, tablet_id: u64) -> CatalogResult<(ResourceName, u32)> {
        if tablet_id == 0 {
            return Err(CatalogError::InvalidSpec(
                "tablet ID must be non-zero".into(),
            ));
        }
        self.tablet_index
            .get(&tablet_id)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(format!("tablet {tablet_id}")))
    }

    fn delete_resource(&mut self, request: &DeleteResource) -> CatalogResult<CatalogMutation> {
        request.name.validate()?;
        let current = self.resources.get(&request.name).cloned();
        let actual_generation = current.as_ref().map_or(0, |resource| resource.generation);
        validate_expected_generation(request.expected_generation, actual_generation)?;
        let Some(resource) = current else {
            return Ok(CatalogMutation::Deleted {
                name: request.name.clone(),
                generation: self
                    .last_generations
                    .get(&request.name)
                    .copied()
                    .unwrap_or(0),
                deleted: false,
                replayed: false,
            });
        };
        let generation = next_generation(resource.generation)?;
        for tablet in &resource.tablets {
            self.tablet_index.remove(&tablet.tablet_id);
        }
        self.resources.remove(&request.name);
        self.last_generations
            .insert(request.name.clone(), generation);
        Ok(CatalogMutation::Deleted {
            name: request.name.clone(),
            generation,
            deleted: true,
            replayed: false,
        })
    }

    fn ensure_identity_capacity(&self, additional: u32) -> CatalogResult<()> {
        let additional = u64::from(additional);
        self.next_tablet_id
            .checked_add(additional)
            .and_then(|next| next.checked_sub(1))
            .ok_or(CatalogError::IdentityExhausted)?;
        let mut next_group_id = self.next_consensus_group_id;
        for _ in 0..additional {
            while self.reserved_consensus_group_ids.contains(&next_group_id) {
                next_group_id = next_group_id
                    .checked_add(1)
                    .ok_or(CatalogError::IdentityExhausted)?;
            }
            next_group_id = next_group_id
                .checked_add(1)
                .ok_or(CatalogError::IdentityExhausted)?;
        }
        Ok(())
    }

    fn allocate_consensus_group_id(&mut self) -> CatalogResult<u64> {
        while self
            .reserved_consensus_group_ids
            .contains(&self.next_consensus_group_id)
        {
            self.next_consensus_group_id = self
                .next_consensus_group_id
                .checked_add(1)
                .ok_or(CatalogError::IdentityExhausted)?;
        }
        let allocated = self.next_consensus_group_id;
        self.next_consensus_group_id = self
            .next_consensus_group_id
            .checked_add(1)
            .ok_or(CatalogError::IdentityExhausted)?;
        Ok(allocated)
    }
}

const fn default_one_u64() -> u64 {
    1
}

// Serde's skip_serializing_if callback contract requires a shared reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

// Serde's skip_serializing_if callback contract requires a shared reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_one_u64(value: &u64) -> bool {
    *value == 1
}

fn default_managed_status() -> serde_json::Value {
    serde_json::json!({
        "observed_generation": 0,
        "phase": "pending"
    })
}

fn validate_managed_batch_len(len: usize) -> CatalogResult<()> {
    if len == 0 || len > MAX_MANAGED_BATCH_RESOURCES {
        return Err(CatalogError::InvalidSpec(format!(
            "managed batches must contain between 1 and {MAX_MANAGED_BATCH_RESOURCES} resources"
        )));
    }
    Ok(())
}

fn validate_managed_import(request: &ImportManagedResources) -> CatalogResult<()> {
    if request.generations.is_empty()
        || request.generations.len() > MAX_MANAGED_IMPORT_RECORDS
        || request.resources.len() > MAX_MANAGED_IMPORT_RECORDS
        || request.resources.len() > request.generations.len()
    {
        return Err(CatalogError::InvalidSpec(format!(
            "managed import must contain 1-{MAX_MANAGED_IMPORT_RECORDS} generation records and no more live resources than generations"
        )));
    }
    if request
        .generations
        .windows(2)
        .any(|pair| pair[0].name >= pair[1].name)
        || request
            .resources
            .windows(2)
            .any(|pair| pair[0].name >= pair[1].name)
    {
        return Err(CatalogError::InvalidSpec(
            "managed import records must be strictly sorted by resource name".into(),
        ));
    }
    let generations = request
        .generations
        .iter()
        .map(|entry| (&entry.name, entry.generation))
        .collect::<BTreeMap<_, _>>();
    for entry in &request.generations {
        entry.name.validate()?;
        if entry.generation == 0 {
            return Err(CatalogError::InvalidSpec(
                "managed import generations must be positive".into(),
            ));
        }
    }
    for resource in &request.resources {
        resource.name.validate()?;
        validate_managed_document("imported desired resource", &resource.desired)?;
        validate_managed_document("imported resource status", &resource.status)?;
        if resource.deletion_requested
            || generations.get(&resource.name).copied() != Some(resource.generation)
        {
            return Err(CatalogError::InvalidSpec(
                "each live managed import resource must match a generation record".into(),
            ));
        }
    }
    Ok(())
}

fn validate_managed_document(label: &str, document: &serde_json::Value) -> CatalogResult<()> {
    if !document.is_object() {
        return Err(CatalogError::InvalidSpec(format!(
            "{label} must be a JSON object"
        )));
    }
    let encoded = serde_json::to_vec(document)
        .map_err(|error| CatalogError::InvalidSpec(error.to_string()))?;
    if encoded.len() > MAX_MANAGED_DOCUMENT_BYTES {
        return Err(CatalogError::InvalidSpec(format!(
            "{label} exceeds {MAX_MANAGED_DOCUMENT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_control_owner(owner_id: &str) -> CatalogResult<()> {
    if owner_id.is_empty()
        || owner_id.len() > MAX_CONTROL_OWNER_BYTES
        || owner_id.trim() != owner_id
        || !owner_id.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-')
        })
    {
        return Err(CatalogError::InvalidSpec(format!(
            "control owner must be a canonical 1-{MAX_CONTROL_OWNER_BYTES} byte identifier"
        )));
    }
    Ok(())
}

fn validate_strictly_sorted_resource_writes(writes: &[DesiredResourceWrite]) -> CatalogResult<()> {
    if writes.windows(2).any(|pair| pair[0].name >= pair[1].name) {
        return Err(CatalogError::InvalidSpec(
            "desired resource writes must be strictly sorted by resource name".into(),
        ));
    }
    Ok(())
}

fn validate_strictly_sorted_placements(
    placements: &[ManagedResourcePlacement],
) -> CatalogResult<()> {
    if placements
        .windows(2)
        .any(|pair| pair[0].name >= pair[1].name)
    {
        return Err(CatalogError::InvalidSpec(
            "managed placements must be strictly sorted by resource name".into(),
        ));
    }
    Ok(())
}

fn catalog_group_allocations(
    resources: &BTreeMap<ResourceName, ResourceRecord>,
) -> CatalogResult<BTreeMap<u64, u32>> {
    let mut allocations = BTreeMap::<u64, u32>::new();
    for resource in resources.values() {
        for tablet in &resource.tablets {
            let nodes = tablet
                .voter_node_ids
                .iter()
                .chain(&tablet.target_voter_node_ids)
                .copied()
                .collect::<BTreeSet<_>>();
            for node_id in nodes {
                let current = allocations.get(&node_id).copied().unwrap_or(0);
                allocations.insert(
                    node_id,
                    current
                        .checked_add(1)
                        .ok_or(CatalogError::IdentityExhausted)?,
                );
            }
        }
    }
    Ok(allocations)
}

fn validate_capacity_observations(
    observations: &[NodeCapacityObservation],
    current: &BTreeMap<u64, u32>,
) -> CatalogResult<()> {
    if observations.is_empty() || observations.len() > 1_024 {
        return Err(CatalogError::InvalidSpec(
            "capacity admission requires a complete 1-1024 node observation".into(),
        ));
    }
    if observations
        .windows(2)
        .any(|pair| pair[0].node_id >= pair[1].node_id)
    {
        return Err(CatalogError::InvalidSpec(
            "capacity observations must be strictly sorted by node ID".into(),
        ));
    }
    let observed_nodes = observations
        .iter()
        .map(|observation| observation.node_id)
        .collect::<BTreeSet<_>>();
    if current
        .keys()
        .any(|node_id| !observed_nodes.contains(node_id))
    {
        return Err(CatalogError::InvalidSpec(
            "capacity observations omit a node with a catalog allocation".into(),
        ));
    }
    for observation in observations {
        if observation.node_id == 0
            || observation.max_consensus_groups == 0
            || observation.used_consensus_groups > observation.max_consensus_groups
            || observation.catalog_groups > observation.used_consensus_groups
        {
            return Err(CatalogError::InvalidSpec(
                "capacity observations contain an invalid node or group count".into(),
            ));
        }
        let expected = current.get(&observation.node_id).copied().unwrap_or(0);
        if observation.catalog_groups != expected {
            return Err(CatalogError::CapacityObservationConflict {
                node_id: observation.node_id,
                expected,
                actual: observation.catalog_groups,
            });
        }
    }
    Ok(())
}

fn validate_reserved_capacity(
    observations: &[NodeCapacityObservation],
    next: &BTreeMap<u64, u32>,
) -> CatalogResult<()> {
    let by_node = observations
        .iter()
        .map(|observation| (observation.node_id, observation))
        .collect::<BTreeMap<_, _>>();
    for (&node_id, &catalog_groups) in next {
        let observation = by_node.get(&node_id).ok_or_else(|| {
            CatalogError::InvalidSpec(format!(
                "managed placement references unobserved node {node_id}"
            ))
        })?;
        let base_groups = observation
            .used_consensus_groups
            .checked_sub(observation.catalog_groups)
            .ok_or(CatalogError::IdentityExhausted)?;
        let required = base_groups
            .checked_add(catalog_groups)
            .ok_or(CatalogError::IdentityExhausted)?;
        if required > observation.max_consensus_groups {
            return Err(CatalogError::CapacityExceeded {
                node_id,
                required,
                limit: observation.max_consensus_groups,
            });
        }
    }
    Ok(())
}

fn rejected_mutation(error: &CatalogError) -> CatalogMutation {
    CatalogMutation::Rejected {
        code: CatalogRejectionCode::for_error(error),
        message: error.to_string(),
        replayed: false,
    }
}

fn unchanged_resource_mutation(resource: ResourceRecord) -> CatalogMutation {
    CatalogMutation::Applied {
        resource,
        created: false,
        changed: false,
        replayed: false,
    }
}

fn changed_resource_mutation(resource: ResourceRecord) -> CatalogMutation {
    CatalogMutation::Applied {
        resource,
        created: false,
        changed: true,
        replayed: false,
    }
}

fn validate_membership_request_identity(
    tablet: &TabletDescriptor,
    expected_tablet_epoch: u64,
    target_voter_node_ids: &[u64],
) -> CatalogResult<()> {
    if expected_tablet_epoch == 0 || tablet.tablet_epoch != expected_tablet_epoch {
        return Err(CatalogError::InvalidSpec(format!(
            "tablet {} epoch {} does not match expected epoch {expected_tablet_epoch}",
            tablet.tablet_id, tablet.tablet_epoch
        )));
    }
    validate_voter_assignment(
        target_voter_node_ids,
        tablet.replica_count,
        "membership target",
    )
}

fn validate_single_voter_replacement(current: &[u64], target: &[u64]) -> CatalogResult<()> {
    if current.is_empty() {
        return Err(CatalogError::InvalidSpec(
            "learner-first replacement requires an explicit current voter placement".into(),
        ));
    }
    let current = current.iter().copied().collect::<BTreeSet<_>>();
    let target = target.iter().copied().collect::<BTreeSet<_>>();
    let removed = current.difference(&target).count();
    let added = target.difference(&current).count();
    if removed != 1 || added != 1 {
        return Err(CatalogError::InvalidSpec(
            "a membership plan must replace exactly one voter".into(),
        ));
    }
    Ok(())
}

fn validate_governance_identifier(label: &str, value: &str, maximum: usize) -> CatalogResult<()> {
    if value.is_empty() || value.trim() != value {
        return Err(CatalogError::InvalidSpec(format!(
            "{label} must be non-empty and have no surrounding whitespace"
        )));
    }
    if value.len() > maximum {
        return Err(CatalogError::InvalidSpec(format!(
            "{label} must be at most {maximum} bytes"
        )));
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-')
    }) || !value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
    {
        return Err(CatalogError::InvalidSpec(format!(
            "{label} must be a canonical lowercase identifier"
        )));
    }
    Ok(())
}

fn validate_governance_tag_key(key: &str) -> CatalogResult<()> {
    validate_governance_identifier("governance tag key", key, MAX_GOVERNANCE_TAG_KEY_BYTES)?;
    if key.starts_with(RESERVED_GOVERNANCE_TAG_PREFIX) {
        return Err(CatalogError::InvalidSpec(format!(
            "governance tag prefix {RESERVED_GOVERNANCE_TAG_PREFIX} is reserved"
        )));
    }
    Ok(())
}

fn validate_governance_tag_value(value: &str) -> CatalogResult<()> {
    if value.is_empty() || value.trim() != value {
        return Err(CatalogError::InvalidSpec(
            "governance tag values must be non-empty and have no surrounding whitespace".into(),
        ));
    }
    if value.len() > MAX_GOVERNANCE_TAG_VALUE_BYTES {
        return Err(CatalogError::InvalidSpec(format!(
            "governance tag values must be at most {MAX_GOVERNANCE_TAG_VALUE_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(CatalogError::InvalidSpec(
            "governance tag values cannot contain control characters".into(),
        ));
    }
    Ok(())
}

fn validate_name_component(label: &str, value: &str) -> CatalogResult<()> {
    if value.is_empty() || value.trim() != value {
        return Err(CatalogError::InvalidName(format!(
            "{label} must be non-empty and have no surrounding whitespace"
        )));
    }
    if value.len() > MAX_NAME_COMPONENT_BYTES {
        return Err(CatalogError::InvalidName(format!(
            "{label} must be at most {MAX_NAME_COMPONENT_BYTES} bytes"
        )));
    }
    if value.contains('/') {
        return Err(CatalogError::InvalidName(format!(
            "{label} cannot contain '/'"
        )));
    }
    Ok(())
}

struct RestoredCatalogResources {
    resources: BTreeMap<ResourceName, ResourceRecord>,
    tablet_index: BTreeMap<u64, (ResourceName, u32)>,
    allocated_groups: BTreeSet<u64>,
}

fn validate_snapshot_identity(snapshot: &CatalogSnapshot) -> CatalogResult<()> {
    if snapshot.next_tablet_id == 0 || snapshot.next_consensus_group_id == 0 {
        return Err(CatalogError::InvalidSpec(
            "catalog snapshot identity high-water marks must be nonzero".into(),
        ));
    }
    validate_strictly_sorted(
        &snapshot.reserved_consensus_group_ids,
        "reserved consensus group IDs",
    )?;
    if snapshot
        .reserved_consensus_group_ids
        .first()
        .is_some_and(|group_id| *group_id == 0)
    {
        return Err(CatalogError::InvalidSpec(
            "reserved consensus group IDs must be nonzero".into(),
        ));
    }
    Ok(())
}

fn restore_snapshot_resources(
    snapshot_resources: &[ResourceRecord],
    reserved_groups: &BTreeSet<u64>,
) -> CatalogResult<RestoredCatalogResources> {
    let mut restored = RestoredCatalogResources {
        resources: BTreeMap::new(),
        tablet_index: BTreeMap::new(),
        allocated_groups: BTreeSet::new(),
    };
    let mut previous_resource: Option<&ResourceName> = None;
    for resource in snapshot_resources {
        if previous_resource.is_some_and(|previous| previous >= &resource.name) {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot resources are not strictly sorted".into(),
            ));
        }
        previous_resource = Some(&resource.name);
        resource.name.validate()?;
        resource.spec.validate(resource.name.kind)?;
        if resource.generation == 0
            || resource.tablets.len()
                != usize::try_from(resource.spec.shard_count)
                    .map_err(|_| CatalogError::IdentityExhausted)?
        {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot resource generation or shard count is invalid".into(),
            ));
        }
        restore_resource_tablets(resource, reserved_groups, &mut restored)?;
        if restored
            .resources
            .insert(resource.name.clone(), resource.clone())
            .is_some()
        {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot contains duplicate resources".into(),
            ));
        }
    }
    Ok(restored)
}

fn restore_resource_tablets(
    resource: &ResourceRecord,
    reserved_groups: &BTreeSet<u64>,
    restored: &mut RestoredCatalogResources,
) -> CatalogResult<()> {
    for (shard_index, tablet) in resource.tablets.iter().enumerate() {
        let expected_shard =
            u32::try_from(shard_index).map_err(|_| CatalogError::IdentityExhausted)?;
        if tablet.tablet_id == 0
            || tablet.consensus_group_id == 0
            || tablet.tablet_epoch == 0
            || tablet.shard_index != expected_shard
            || tablet.resource_generation != resource.generation
            || tablet.workload_profile != resource.spec.workload_profile
            || tablet.replica_count != resource.spec.replica_count
            || reserved_groups.contains(&tablet.consensus_group_id)
        {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot contains an invalid tablet descriptor".into(),
            ));
        }
        if !tablet.voter_node_ids.is_empty() {
            validate_voter_assignment(
                &tablet.voter_node_ids,
                tablet.replica_count,
                "catalog snapshot tablet",
            )?;
        }
        if !tablet.bootstrap_voter_node_ids.is_empty() {
            validate_voter_assignment(
                &tablet.bootstrap_voter_node_ids,
                tablet.replica_count,
                "catalog snapshot tablet bootstrap",
            )?;
            if tablet.voter_node_ids.is_empty() {
                return Err(CatalogError::InvalidSpec(
                    "catalog snapshot tablet bootstrap requires explicit current voters".into(),
                ));
            }
        }
        if !tablet.target_voter_node_ids.is_empty() {
            validate_voter_assignment(
                &tablet.target_voter_node_ids,
                tablet.replica_count,
                "catalog snapshot tablet membership target",
            )?;
            if tablet.bootstrap_voter_node_ids.is_empty() {
                return Err(CatalogError::InvalidSpec(
                    "catalog snapshot membership target requires bootstrap voters".into(),
                ));
            }
            validate_single_voter_replacement(
                &tablet.voter_node_ids,
                &tablet.target_voter_node_ids,
            )?;
        }
        if restored
            .tablet_index
            .insert(
                tablet.tablet_id,
                (resource.name.clone(), tablet.shard_index),
            )
            .is_some()
            || !restored.allocated_groups.insert(tablet.consensus_group_id)
        {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot reuses a tablet or consensus-group identity".into(),
            ));
        }
    }
    Ok(())
}

fn restore_snapshot_generations(
    generations: &[ResourceGeneration],
    resources: &BTreeMap<ResourceName, ResourceRecord>,
) -> CatalogResult<BTreeMap<ResourceName, u64>> {
    let mut restored = BTreeMap::new();
    let mut previous_name: Option<&ResourceName> = None;
    for generation in generations {
        if previous_name.is_some_and(|previous| previous >= &generation.name)
            || generation.generation == 0
        {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot generations are invalid or unsorted".into(),
            ));
        }
        previous_name = Some(&generation.name);
        generation.name.validate()?;
        if resources
            .get(&generation.name)
            .is_some_and(|resource| resource.generation > generation.generation)
            || restored
                .insert(generation.name.clone(), generation.generation)
                .is_some()
        {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot generation history is inconsistent".into(),
            ));
        }
    }
    if resources
        .iter()
        .any(|(name, resource)| restored.get(name) != Some(&resource.generation))
    {
        return Err(CatalogError::InvalidSpec(
            "catalog snapshot omits a live resource generation".into(),
        ));
    }
    Ok(restored)
}

fn restore_managed_resources(
    records: &[ManagedResourceRecord],
) -> CatalogResult<BTreeMap<ResourceName, ManagedResourceRecord>> {
    let mut restored = BTreeMap::new();
    let mut previous: Option<&ResourceName> = None;
    for record in records {
        if previous.is_some_and(|name| name >= &record.name) || record.generation == 0 {
            return Err(CatalogError::InvalidSpec(
                "managed resource snapshot is invalid or unsorted".into(),
            ));
        }
        previous = Some(&record.name);
        record.name.validate()?;
        validate_managed_document("desired resource", &record.desired)?;
        validate_managed_document("managed resource status", &record.status)?;
        restored.insert(record.name.clone(), record.clone());
    }
    Ok(restored)
}

fn restore_managed_generations(
    generations: &[ResourceGeneration],
    resources: &BTreeMap<ResourceName, ManagedResourceRecord>,
) -> CatalogResult<BTreeMap<ResourceName, u64>> {
    let mut restored = BTreeMap::new();
    let mut previous: Option<&ResourceName> = None;
    for generation in generations {
        if previous.is_some_and(|name| name >= &generation.name) || generation.generation == 0 {
            return Err(CatalogError::InvalidSpec(
                "managed generation snapshot is invalid or unsorted".into(),
            ));
        }
        previous = Some(&generation.name);
        generation.name.validate()?;
        if resources
            .get(&generation.name)
            .is_some_and(|resource| resource.generation > generation.generation)
            || restored
                .insert(generation.name.clone(), generation.generation)
                .is_some()
        {
            return Err(CatalogError::InvalidSpec(
                "managed generation history is inconsistent".into(),
            ));
        }
    }
    if resources
        .iter()
        .any(|(name, resource)| restored.get(name) != Some(&resource.generation))
    {
        return Err(CatalogError::InvalidSpec(
            "managed generation history omits a live resource".into(),
        ));
    }
    Ok(restored)
}

fn validate_control_snapshot(
    lease: Option<&ControlLease>,
    next_fence: u64,
    clock_ms: u64,
    next_change_cursor: u64,
    changes: &[CatalogChange],
    generations: &BTreeMap<ResourceName, u64>,
) -> CatalogResult<()> {
    if next_fence == 0 || next_change_cursor == 0 || changes.len() > MAX_CHANGE_HISTORY {
        return Err(CatalogError::InvalidSpec(
            "control snapshot contains an invalid identity high-water mark".into(),
        ));
    }
    if let Some(lease) = lease {
        validate_control_owner(&lease.owner_id)?;
        if lease.fence == 0 || lease.fence >= next_fence || lease.valid_until_ms <= clock_ms {
            return Err(CatalogError::InvalidSpec(
                "control snapshot contains an invalid lease".into(),
            ));
        }
    }
    let mut previous = None;
    for change in changes {
        if change.cursor == 0
            || previous.is_some_and(|cursor| change.cursor != cursor + 1)
            || change.cursor >= next_change_cursor
            || change.generation == 0
        {
            return Err(CatalogError::InvalidSpec(
                "control snapshot change history is invalid".into(),
            ));
        }
        change.name.validate()?;
        if generations
            .get(&change.name)
            .is_none_or(|generation| change.generation > *generation)
        {
            return Err(CatalogError::InvalidSpec(
                "control snapshot change exceeds its resource generation history".into(),
            ));
        }
        previous = Some(change.cursor);
    }
    if changes
        .last()
        .is_some_and(|change| change.cursor + 1 != next_change_cursor)
    {
        return Err(CatalogError::InvalidSpec(
            "control snapshot change high-water mark is inconsistent".into(),
        ));
    }
    Ok(())
}

fn validate_snapshot_high_water_marks(
    next_tablet_id: u64,
    next_group_id: u64,
    tablet_index: &BTreeMap<u64, (ResourceName, u32)>,
    allocated_groups: &BTreeSet<u64>,
) -> CatalogResult<()> {
    let max_tablet_id = tablet_index.keys().next_back().copied().unwrap_or(0);
    let max_group_id = allocated_groups.iter().next_back().copied().unwrap_or(0);
    if next_tablet_id <= max_tablet_id || next_group_id <= max_group_id {
        return Err(CatalogError::InvalidSpec(
            "catalog snapshot identity high-water mark would reuse an allocated identity".into(),
        ));
    }
    Ok(())
}

fn validate_strictly_sorted(values: &[u64], label: &str) -> CatalogResult<()> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CatalogError::InvalidSpec(format!(
            "catalog snapshot {label} are not strictly sorted"
        )));
    }
    Ok(())
}

fn validate_completed_requests(
    completed_requests: &BTreeMap<String, CompletedRequest>,
) -> CatalogResult<()> {
    for (token, completed) in completed_requests {
        validate_request_token(token)?;
        if completed.command.request_token() != token {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot request token does not match its completed command".into(),
            ));
        }
        if (completed.first_change_cursor == 0) != (completed.last_change_cursor == 0)
            || (completed.first_change_cursor != 0
                && completed.first_change_cursor > completed.last_change_cursor)
        {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot operation change cursors are invalid".into(),
            ));
        }
        completed.command.encode()?;
        if !completed_request_matches(&completed.command, &completed.mutation) {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot completed request and mutation disagree".into(),
            ));
        }
    }
    Ok(())
}

fn completed_request_matches(command: &CatalogCommand, mutation: &CatalogMutation) -> bool {
    match (command, mutation) {
        (
            CatalogCommand::Apply(request),
            CatalogMutation::Applied {
                resource, replayed, ..
            },
        ) => resource.name == request.name && !replayed,
        (CatalogCommand::Delete(request), CatalogMutation::Deleted { name, replayed, .. }) => {
            name == &request.name && !replayed
        }
        (
            CatalogCommand::PlanMembership(request),
            CatalogMutation::Applied {
                resource, replayed, ..
            },
        ) => {
            !replayed
                && resource.tablets.iter().any(|tablet| {
                    tablet.tablet_id == request.tablet_id
                        && (tablet.target_voter_node_ids == request.target_voter_node_ids
                            || (tablet.target_voter_node_ids.is_empty()
                                && tablet.voter_node_ids == request.target_voter_node_ids))
                })
        }
        (
            CatalogCommand::FinalizeMembership(request),
            CatalogMutation::Applied {
                resource, replayed, ..
            },
        ) => {
            !replayed
                && resource.tablets.iter().any(|tablet| {
                    tablet.tablet_id == request.tablet_id
                        && tablet.voter_node_ids == request.target_voter_node_ids
                        && tablet.target_voter_node_ids.is_empty()
                })
        }
        _ => managed_completed_request_matches(command, mutation),
    }
}

fn managed_completed_request_matches(command: &CatalogCommand, mutation: &CatalogMutation) -> bool {
    match (command, mutation) {
        (
            CatalogCommand::ApplyDesired(request),
            CatalogMutation::DesiredApplied {
                resources,
                replayed,
                ..
            },
        ) => {
            !replayed
                && resources.len() == request.resources.len()
                && resources
                    .iter()
                    .zip(&request.resources)
                    .all(|(result, write)| result.resource.name == write.name)
        }
        (
            CatalogCommand::DeleteDesired(request),
            CatalogMutation::DesiredDeleted { name, replayed, .. },
        ) => !replayed && name == &request.name,
        (
            CatalogCommand::DeleteManaged(request),
            CatalogMutation::ManagedDeleted { name, replayed, .. },
        ) => !replayed && name == &request.name,
        (
            CatalogCommand::ImportManaged(request),
            CatalogMutation::DesiredApplied {
                resources,
                replayed,
                ..
            },
        ) => {
            !replayed
                && resources.len() == request.resources.len()
                && resources
                    .iter()
                    .zip(&request.resources)
                    .all(|(result, imported)| result.resource == *imported)
        }
        (
            CatalogCommand::AcquireControlLease(request),
            CatalogMutation::ControlLeaseAcquired { lease, replayed },
        ) => !replayed && lease.owner_id == request.owner_id,
        (
            CatalogCommand::UpdateManagedStatus(request),
            CatalogMutation::ManagedStatusUpdated {
                resource, replayed, ..
            },
        ) => !replayed && resource.name == request.name,
        (
            CatalogCommand::ReconcileManaged(request),
            CatalogMutation::ManagedReconciled {
                resources,
                replayed,
                ..
            },
        ) => {
            !replayed
                && resources.len() == request.resources.len()
                && resources
                    .iter()
                    .zip(&request.resources)
                    .all(|(resource, placement)| resource.name == placement.name)
        }
        (
            CatalogCommand::PlanManagedMembership(request),
            CatalogMutation::Applied {
                resource, replayed, ..
            },
        ) => {
            !replayed
                && resource.name == request.name
                && resource.tablets.iter().any(|tablet| {
                    tablet.tablet_id == request.tablet_id
                        && (tablet.target_voter_node_ids == request.target_voter_node_ids
                            || (tablet.target_voter_node_ids.is_empty()
                                && tablet.voter_node_ids == request.target_voter_node_ids))
                })
        }
        (
            CatalogCommand::ApplyDesired(_)
            | CatalogCommand::DeleteDesired(_)
            | CatalogCommand::DeleteManaged(_)
            | CatalogCommand::ImportManaged(_)
            | CatalogCommand::AcquireControlLease(_)
            | CatalogCommand::UpdateManagedStatus(_)
            | CatalogCommand::ReconcileManaged(_)
            | CatalogCommand::PlanManagedMembership(_),
            CatalogMutation::Rejected { replayed, .. },
        ) => !replayed,
        _ => false,
    }
}

fn validate_request_token(token: &str) -> CatalogResult<()> {
    if token.is_empty() || token.trim() != token {
        return Err(CatalogError::MissingRequestToken);
    }
    if token.len() > MAX_REQUEST_TOKEN_BYTES {
        return Err(CatalogError::RequestTokenTooLong);
    }
    Ok(())
}

fn validate_expected_generation(expected: Option<u64>, actual: u64) -> CatalogResult<()> {
    if let Some(expected) = expected
        && expected != actual
    {
        return Err(CatalogError::GenerationConflict { expected, actual });
    }
    Ok(())
}

fn validate_tablet_placements(
    request: &ApplyResource,
    current: Option<&ResourceRecord>,
) -> CatalogResult<Option<BTreeMap<u32, Vec<u64>>>> {
    let has_current_placements = current.is_some_and(|resource| {
        resource
            .tablets
            .iter()
            .any(|tablet| !tablet.voter_node_ids.is_empty())
    });
    if request.tablet_placements.is_empty() {
        if has_current_placements {
            return Err(CatalogError::InvalidSpec(
                "an explicitly placed resource apply must include every shard assignment".into(),
            ));
        }
        return Ok(None);
    }
    if !matches!(request.spec.replica_count, 3 | 5) {
        return Err(CatalogError::InvalidSpec(
            "explicit tablet placement requires exactly three or five replicas".into(),
        ));
    }
    if request.tablet_placements.len()
        != usize::try_from(request.spec.shard_count).map_err(|_| CatalogError::IdentityExhausted)?
    {
        return Err(CatalogError::InvalidSpec(
            "explicit tablet placement must contain every requested shard exactly once".into(),
        ));
    }

    let mut placements = BTreeMap::new();
    for (expected_shard, placement) in request.tablet_placements.iter().enumerate() {
        let expected_shard =
            u32::try_from(expected_shard).map_err(|_| CatalogError::IdentityExhausted)?;
        if placement.shard_index != expected_shard {
            return Err(CatalogError::InvalidSpec(
                "tablet placements must be strictly ordered by contiguous shard index".into(),
            ));
        }
        validate_voter_assignment(
            &placement.voter_node_ids,
            request.spec.replica_count,
            "tablet placement",
        )?;
        placements.insert(placement.shard_index, placement.voter_node_ids.clone());
    }
    Ok(Some(placements))
}

fn validate_voter_assignment(
    voter_node_ids: &[u64],
    replica_count: u16,
    label: &str,
) -> CatalogResult<()> {
    if voter_node_ids.len() != usize::from(replica_count)
        || voter_node_ids.contains(&0)
        || !voter_node_ids.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(CatalogError::InvalidSpec(format!(
            "{label} voters must match replica_count and contain sorted, distinct, non-zero node IDs"
        )));
    }
    Ok(())
}

fn next_generation(current: u64) -> CatalogResult<u64> {
    current
        .checked_add(1)
        .ok_or(CatalogError::IdentityExhausted)
}
