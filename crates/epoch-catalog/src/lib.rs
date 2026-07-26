//! Deterministic regional resource and tablet catalog.
//!
//! The catalog is a profile-neutral state machine. Consensus and persistence
//! live outside this crate so the same commands can be replayed by the
//! standalone, clustered, and managed regional runtimes.

use std::collections::BTreeMap;

use epoch_core::{ResourceKind, WorkloadProfile};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_NAME_COMPONENT_BYTES: usize = 128;
const MAX_REQUEST_TOKEN_BYTES: usize = 256;
const MAX_SHARDS_PER_RESOURCE: u32 = 4_096;
const MAX_REPLICAS_PER_TABLET: u16 = 9;
const MAX_COMMAND_BYTES: usize = 512 * 1024;
pub const CATALOG_COMMAND_FORMAT_VERSION: u16 = 1;

pub type CatalogResult<T> = Result<T, CatalogError>;

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
    #[error("unsupported catalog command format version {0}")]
    UnsupportedCommandVersion(u16),
    #[error("catalog command is not in canonical v1 encoding")]
    NonCanonicalCommand,
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

    fn display_name(&self) -> String {
        format!(
            "{}/{}/{}/{}/{:?}/{}",
            self.organization, self.project, self.environment, self.namespace, self.kind, self.name
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSpec {
    pub workload_profile: WorkloadProfile,
    pub shard_count: u32,
    pub replica_count: u16,
}

impl ResourceSpec {
    fn validate(self, kind: ResourceKind) -> CatalogResult<()> {
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
            format_version: CATALOG_COMMAND_FORMAT_VERSION,
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
        if envelope.format_version != CATALOG_COMMAND_FORMAT_VERSION {
            return Err(CatalogError::UnsupportedCommandVersion(
                envelope.format_version,
            ));
        }
        let command = envelope.command;
        if command.encode()?.as_slice() != payload {
            return Err(CatalogError::NonCanonicalCommand);
        }
        Ok(command)
    }

    fn request_token(&self) -> &str {
        match self {
            Self::Apply(request) => &request.request_token,
            Self::Delete(request) => &request.request_token,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompletedRequest {
    command: CatalogCommand,
    mutation: CatalogMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSnapshot {
    resources: Vec<ResourceRecord>,
    last_generations: BTreeMap<ResourceName, u64>,
    next_tablet_id: u64,
    next_consensus_group_id: u64,
    completed_requests: BTreeMap<String, CompletedRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    resources: BTreeMap<ResourceName, ResourceRecord>,
    last_generations: BTreeMap<ResourceName, u64>,
    tablet_index: BTreeMap<u64, (ResourceName, u32)>,
    next_tablet_id: u64,
    next_consensus_group_id: u64,
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
            completed_requests: BTreeMap::new(),
        }
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
            .ok_or_else(|| CatalogError::NotFound(name.display_name()))
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
                CatalogError::NotFound(format!("{} shard {shard_index}", name.display_name()))
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

    pub fn snapshot(&self) -> CatalogSnapshot {
        CatalogSnapshot {
            resources: self.resources.values().cloned().collect(),
            last_generations: self.last_generations.clone(),
            next_tablet_id: self.next_tablet_id,
            next_consensus_group_id: self.next_consensus_group_id,
            completed_requests: self.completed_requests.clone(),
        }
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
            let tablet = TabletDescriptor {
                tablet_id: self.next_tablet_id,
                consensus_group_id: self.next_consensus_group_id,
                shard_index,
                tablet_epoch: 1,
                resource_generation: generation,
                workload_profile: request.spec.workload_profile,
                replica_count: request.spec.replica_count,
            };
            self.next_tablet_id += 1;
            self.next_consensus_group_id += 1;
            tablets.push(tablet);
        }
        let resource = ResourceRecord {
            name: request.name.clone(),
            generation,
            spec: request.spec,
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
        self.next_consensus_group_id
            .checked_add(additional)
            .and_then(|next| next.checked_sub(1))
            .ok_or(CatalogError::IdentityExhausted)?;
        Ok(())
    }
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
