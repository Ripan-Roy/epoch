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
const RESERVED_GOVERNANCE_TAG_PREFIX: &str = "epoch.io/";
pub const CATALOG_COMMAND_FORMAT_VERSION: u16 = 1;
pub const CATALOG_CONFIG_COMMAND_FORMAT_VERSION: u16 = 2;
pub const CATALOG_GOVERNANCE_COMMAND_FORMAT_VERSION: u16 = 3;
pub const CATALOG_SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub const CATALOG_CONFIG_SNAPSHOT_FORMAT_VERSION: u16 = 2;
pub const CATALOG_GOVERNANCE_SNAPSHOT_FORMAT_VERSION: u16 = 3;
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteResource {
    pub request_token: String,
    pub expected_generation: Option<u64>,
    pub name: ResourceName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum CatalogCommand {
    Apply(ApplyResource),
    Delete(DeleteResource),
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
            Self::Apply(request) if request.spec.governance.is_some() => {
                CATALOG_GOVERNANCE_COMMAND_FORMAT_VERSION
            }
            Self::Apply(request) if request.spec.configuration.is_some() => {
                CATALOG_CONFIG_COMMAND_FORMAT_VERSION
            }
            Self::Apply(_) | Self::Delete(_) => CATALOG_COMMAND_FORMAT_VERSION,
        }
    }

    fn request_token(&self) -> &str {
        match self {
            Self::Apply(request) => &request.request_token,
            Self::Delete(request) => &request.request_token,
        }
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
}

impl CatalogMutation {
    pub fn resource(&self) -> Option<&ResourceRecord> {
        match self {
            Self::Applied { resource, .. } => Some(resource),
            Self::Deleted { .. } => None,
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
        }
    }
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceGeneration {
    name: ResourceName,
    generation: u64,
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
        };
        self.completed_requests.insert(
            command.request_token().to_owned(),
            CompletedRequest {
                command,
                mutation: mutation.clone(),
            },
        );
        Ok(mutation)
    }

    pub fn resource(&self, name: &ResourceName) -> CatalogResult<&ResourceRecord> {
        self.resources
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.canonical_name()))
    }

    pub fn resources(&self) -> impl ExactSizeIterator<Item = &ResourceRecord> {
        self.resources.values()
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
        validate_snapshot_high_water_marks(
            snapshot.next_tablet_id,
            snapshot.next_consensus_group_id,
            &restored.tablet_index,
            &restored.allocated_groups,
        )?;
        validate_completed_requests(&snapshot.completed_requests)?;

        Ok(Self {
            resources: restored.resources,
            last_generations,
            tablet_index: restored.tablet_index,
            next_tablet_id: snapshot.next_tablet_id,
            next_consensus_group_id: snapshot.next_consensus_group_id,
            reserved_consensus_group_ids,
            completed_requests: snapshot.completed_requests,
        })
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
            if resource.spec == request.spec {
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
        let mut tablets = current.map_or_else(Vec::new, |resource| resource.tablets);
        for tablet in &mut tablets {
            tablet.resource_generation = generation;
            tablet.replica_count = request.spec.replica_count;
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
            };
            self.next_tablet_id += 1;
            tablets.push(tablet);
        }
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
        completed.command.encode()?;
        let consistent = match (&completed.command, &completed.mutation) {
            (
                CatalogCommand::Apply(request),
                CatalogMutation::Applied {
                    resource, replayed, ..
                },
            ) => resource.name == request.name && !replayed,
            (CatalogCommand::Delete(request), CatalogMutation::Deleted { name, replayed, .. }) => {
                name == &request.name && !replayed
            }
            _ => false,
        };
        if !consistent {
            return Err(CatalogError::InvalidSpec(
                "catalog snapshot completed request and mutation disagree".into(),
            ));
        }
    }
    Ok(())
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

fn next_generation(current: u64) -> CatalogResult<u64> {
    current
        .checked_add(1)
        .ok_or(CatalogError::IdentityExhausted)
}
