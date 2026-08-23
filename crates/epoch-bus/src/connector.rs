//! Replicated connector lifecycle, checkpoints, partial outcomes, and replay intent.

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{EpochError, EpochResult, validate_resource_name};
use serde::{Deserialize, Serialize};

const MAX_CONNECTORS: usize = 10_000;
const MAX_CONNECTOR_CONFIG_ENTRIES: usize = 256;
const MAX_CONNECTOR_SECRET_REFS: usize = 32;
const MAX_CONNECTOR_ALLOWLIST: usize = 256;
const MAX_CONNECTOR_BATCH_RECORDS: usize = 10_000;
const MAX_CONNECTOR_HISTORY: usize = 10_000;
const MAX_CONNECTOR_TEXT_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorKind {
    S3Compatible,
    AzureBlob,
    AzureDataLake,
    Gcs,
    PostgresCdc,
    MySqlCdc,
    Kafka,
    Http,
    Snowflake,
    BigQuery,
    Redshift,
    Elasticsearch,
    OpenSearch,
    ClickHouse,
    Databricks,
    CloudEventBus,
    CustomManaged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorDirection {
    Source,
    Target,
    Bidirectional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorStatus {
    Active,
    Paused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorSpec {
    pub name: String,
    pub kind: ConnectorKind,
    pub direction: ConnectorDirection,
    #[serde(default)]
    pub secret_refs: BTreeSet<String>,
    #[serde(default)]
    pub outbound_allowlist: BTreeSet<String>,
    pub identity: String,
    #[serde(default)]
    pub config: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorCheckpoint {
    pub source_position: String,
    pub target_idempotency_key: String,
    pub batch_id: String,
    pub committed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConnectorRecordResult {
    Applied { record_id: String },
    RoutedToError { record_id: String, reason: String },
    RetryableFailure { record_id: String, reason: String },
}

impl ConnectorRecordResult {
    fn record_id(&self) -> &str {
        match self {
            Self::Applied { record_id }
            | Self::RoutedToError { record_id, .. }
            | Self::RetryableFailure { record_id, .. } => record_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorBatchCommit {
    pub batch_id: String,
    pub source_from: String,
    pub source_to: String,
    pub target_idempotency_key: String,
    pub records: Vec<ConnectorRecordResult>,
    pub committed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorBatchReceipt {
    pub batch_id: String,
    pub applied: usize,
    pub error_routes: usize,
    pub retryable_failures: usize,
    pub checkpoint_advanced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorRecordError {
    pub batch_id: String,
    pub record_id: String,
    pub reason: String,
    pub retryable: bool,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorReplayRequest {
    pub sequence: u64,
    pub source_from: String,
    pub source_to: String,
    pub requested_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorResource {
    pub spec: ConnectorSpec,
    pub revision: u64,
    pub status: ConnectorStatus,
    pub updated_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<ConnectorCheckpoint>,
    #[serde(default)]
    errors: Vec<ConnectorRecordError>,
    #[serde(default)]
    replays: Vec<ConnectorReplayRequest>,
    #[serde(default)]
    batches: BTreeMap<String, (ConnectorBatchCommit, ConnectorBatchReceipt)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorSecretVersion {
    pub reference: String,
    pub version: u64,
    pub rotated_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorRegistry {
    connectors: BTreeMap<String, ConnectorResource>,
    secret_versions: BTreeMap<String, ConnectorSecretVersion>,
}

impl ConnectorRegistry {
    pub fn upsert(&mut self, spec: ConnectorSpec, updated_at_ms: u64) -> EpochResult<u64> {
        validate_spec(&spec)?;
        if !self.connectors.contains_key(&spec.name) && self.connectors.len() >= MAX_CONNECTORS {
            return Err(EpochError::Capacity(format!(
                "connector registry reached its {MAX_CONNECTORS} connector limit"
            )));
        }
        let revision = self.connectors.get(&spec.name).map_or(Ok(1), |resource| {
            resource
                .revision
                .checked_add(1)
                .ok_or_else(|| EpochError::Capacity("connector revision overflow".into()))
        })?;
        let (status, checkpoint, errors, replays, batches) =
            self.connectors.remove(&spec.name).map_or_else(
                || {
                    (
                        ConnectorStatus::Active,
                        None,
                        Vec::new(),
                        Vec::new(),
                        BTreeMap::new(),
                    )
                },
                |previous| {
                    (
                        previous.status,
                        previous.checkpoint,
                        previous.errors,
                        previous.replays,
                        previous.batches,
                    )
                },
            );
        let name = spec.name.clone();
        self.connectors.insert(
            name,
            ConnectorResource {
                spec,
                revision,
                status,
                updated_at_ms,
                checkpoint,
                errors,
                replays,
                batches,
            },
        );
        Ok(revision)
    }

    pub fn pause(&mut self, name: &str, updated_at_ms: u64) -> EpochResult<()> {
        self.set_status(name, ConnectorStatus::Paused, updated_at_ms)
    }

    pub fn resume(&mut self, name: &str, updated_at_ms: u64) -> EpochResult<()> {
        self.set_status(name, ConnectorStatus::Active, updated_at_ms)
    }

    pub fn commit_batch(
        &mut self,
        name: &str,
        commit: ConnectorBatchCommit,
    ) -> EpochResult<ConnectorBatchReceipt> {
        validate_batch(&commit)?;
        validate_resource_name(name)?;
        let resource = self
            .connectors
            .get_mut(name)
            .ok_or_else(|| EpochError::NotFound(name.to_owned()))?;
        if let Some((previous, receipt)) = resource.batches.get(&commit.batch_id) {
            if same_batch_identity(previous, &commit) {
                return Ok(receipt.clone());
            }
            return Err(EpochError::Conflict(format!(
                "connector batch {} was already committed with different content",
                commit.batch_id
            )));
        }
        if resource.status != ConnectorStatus::Active {
            return Err(EpochError::Unavailable(format!(
                "connector {name} is paused"
            )));
        }
        if resource.batches.len() >= MAX_CONNECTOR_HISTORY {
            return Err(EpochError::Capacity(format!(
                "connector {name} reached its {MAX_CONNECTOR_HISTORY} batch history limit"
            )));
        }

        let mut receipt = ConnectorBatchReceipt {
            batch_id: commit.batch_id.clone(),
            applied: 0,
            error_routes: 0,
            retryable_failures: 0,
            checkpoint_advanced: false,
        };
        for record in &commit.records {
            match record {
                ConnectorRecordResult::Applied { .. } => receipt.applied += 1,
                ConnectorRecordResult::RoutedToError { record_id, reason } => {
                    receipt.error_routes += 1;
                    push_error(
                        resource,
                        ConnectorRecordError {
                            batch_id: commit.batch_id.clone(),
                            record_id: record_id.clone(),
                            reason: reason.clone(),
                            retryable: false,
                            recorded_at_ms: commit.committed_at_ms,
                        },
                    )?;
                }
                ConnectorRecordResult::RetryableFailure { record_id, reason } => {
                    receipt.retryable_failures += 1;
                    push_error(
                        resource,
                        ConnectorRecordError {
                            batch_id: commit.batch_id.clone(),
                            record_id: record_id.clone(),
                            reason: reason.clone(),
                            retryable: true,
                            recorded_at_ms: commit.committed_at_ms,
                        },
                    )?;
                }
            }
        }
        if receipt.retryable_failures == 0 {
            if resource.checkpoint.as_ref().is_some_and(|checkpoint| {
                checkpoint.source_position == commit.source_to
                    && checkpoint.target_idempotency_key != commit.target_idempotency_key
            }) {
                return Err(EpochError::Conflict(
                    "connector target idempotency metadata conflicts at the source position".into(),
                ));
            }
            resource.checkpoint = Some(ConnectorCheckpoint {
                source_position: commit.source_to.clone(),
                target_idempotency_key: commit.target_idempotency_key.clone(),
                batch_id: commit.batch_id.clone(),
                committed_at_ms: commit.committed_at_ms,
            });
            receipt.checkpoint_advanced = true;
        }
        resource
            .batches
            .insert(commit.batch_id.clone(), (commit, receipt.clone()));
        Ok(receipt)
    }

    pub fn request_replay(
        &mut self,
        name: &str,
        source_from: &str,
        source_to: &str,
        requested_at_ms: u64,
    ) -> EpochResult<ConnectorReplayRequest> {
        validate_text("replay source_from", source_from)?;
        validate_text("replay source_to", source_to)?;
        let resource = self.active_connector_mut(name)?;
        if resource.replays.len() >= MAX_CONNECTOR_HISTORY {
            return Err(EpochError::Capacity(format!(
                "connector {name} reached its replay history limit"
            )));
        }
        let sequence = u64::try_from(resource.replays.len())
            .map_err(|_| EpochError::Capacity("connector replay sequence overflow".into()))?
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("connector replay sequence overflow".into()))?;
        let request = ConnectorReplayRequest {
            sequence,
            source_from: source_from.into(),
            source_to: source_to.into(),
            requested_at_ms,
        };
        resource.replays.push(request.clone());
        Ok(request)
    }

    pub fn rotate_secret(
        &mut self,
        reference: &str,
        version: u64,
        rotated_at_ms: u64,
    ) -> EpochResult<()> {
        validate_resource_name(reference)?;
        if version == 0 {
            return Err(EpochError::InvalidArgument(
                "connector secret version must be non-zero".into(),
            ));
        }
        if self
            .secret_versions
            .get(reference)
            .is_some_and(|current| version <= current.version)
        {
            return Err(EpochError::Conflict(
                "connector secret rotation version must increase".into(),
            ));
        }
        self.secret_versions.insert(
            reference.into(),
            ConnectorSecretVersion {
                reference: reference.into(),
                version,
                rotated_at_ms,
            },
        );
        Ok(())
    }

    pub fn connector(&self, name: &str) -> Option<&ConnectorResource> {
        self.connectors.get(name)
    }

    pub fn resources(&self) -> impl Iterator<Item = (&str, &ConnectorResource)> {
        self.connectors
            .iter()
            .map(|(name, resource)| (name.as_str(), resource))
    }

    pub fn checkpoint(&self, name: &str) -> Option<&ConnectorCheckpoint> {
        self.connectors
            .get(name)
            .and_then(|resource| resource.checkpoint.as_ref())
    }

    pub fn errors(&self, name: &str) -> EpochResult<&[ConnectorRecordError]> {
        self.connectors
            .get(name)
            .map(|resource| resource.errors.as_slice())
            .ok_or_else(|| EpochError::NotFound(name.to_owned()))
    }

    pub fn secret_version(&self, reference: &str) -> Option<&ConnectorSecretVersion> {
        self.secret_versions.get(reference)
    }

    pub fn connector_count(&self) -> usize {
        self.connectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty() && self.secret_versions.is_empty()
    }

    pub(crate) fn validate_snapshot(&self) -> EpochResult<()> {
        if self.connectors.len() > MAX_CONNECTORS {
            return Err(EpochError::InvalidArgument(
                "connector snapshot exceeds the connector limit".into(),
            ));
        }
        for (name, resource) in &self.connectors {
            validate_connector_snapshot(name, resource)?;
        }
        for (reference, version) in &self.secret_versions {
            validate_resource_name(reference)?;
            if version.reference != *reference || version.version == 0 {
                return Err(EpochError::InvalidArgument(format!(
                    "connector secret {reference} snapshot version is invalid"
                )));
            }
        }
        Ok(())
    }

    fn set_status(
        &mut self,
        name: &str,
        status: ConnectorStatus,
        updated_at_ms: u64,
    ) -> EpochResult<()> {
        validate_resource_name(name)?;
        let resource = self
            .connectors
            .get_mut(name)
            .ok_or_else(|| EpochError::NotFound(name.to_owned()))?;
        resource.status = status;
        resource.updated_at_ms = updated_at_ms;
        Ok(())
    }

    fn active_connector_mut(&mut self, name: &str) -> EpochResult<&mut ConnectorResource> {
        validate_resource_name(name)?;
        let resource = self
            .connectors
            .get_mut(name)
            .ok_or_else(|| EpochError::NotFound(name.to_owned()))?;
        if resource.status != ConnectorStatus::Active {
            return Err(EpochError::Unavailable(format!(
                "connector {name} is paused"
            )));
        }
        Ok(resource)
    }
}

fn validate_connector_snapshot(name: &str, resource: &ConnectorResource) -> EpochResult<()> {
    if resource.spec.name != name || resource.revision == 0 {
        return Err(EpochError::InvalidArgument(format!(
            "connector {name} snapshot identity is invalid"
        )));
    }
    validate_spec(&resource.spec)?;
    if resource.errors.len() > MAX_CONNECTOR_HISTORY
        || resource.replays.len() > MAX_CONNECTOR_HISTORY
        || resource.batches.len() > MAX_CONNECTOR_HISTORY
    {
        return Err(EpochError::InvalidArgument(format!(
            "connector {name} snapshot history exceeds its limit"
        )));
    }
    for (index, replay) in resource.replays.iter().enumerate() {
        let expected_sequence = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1));
        if Some(replay.sequence) != expected_sequence {
            return Err(EpochError::InvalidArgument(format!(
                "connector {name} replay sequence is invalid"
            )));
        }
        validate_text("replay source_from", &replay.source_from)?;
        validate_text("replay source_to", &replay.source_to)?;
    }
    for error in &resource.errors {
        validate_text("connector error batch_id", &error.batch_id)?;
        validate_text("connector error record_id", &error.record_id)?;
        validate_text("connector error reason", &error.reason)?;
    }
    for (batch_id, (batch, receipt)) in &resource.batches {
        validate_batch_snapshot(name, batch_id, batch, receipt)?;
    }
    if let Some(checkpoint) = &resource.checkpoint {
        validate_checkpoint_snapshot(name, resource, checkpoint)?;
    }
    Ok(())
}

fn validate_batch_snapshot(
    connector_name: &str,
    batch_id: &str,
    batch: &ConnectorBatchCommit,
    receipt: &ConnectorBatchReceipt,
) -> EpochResult<()> {
    validate_batch(batch)?;
    let applied = batch
        .records
        .iter()
        .filter(|record| matches!(record, ConnectorRecordResult::Applied { .. }))
        .count();
    let error_routes = batch
        .records
        .iter()
        .filter(|record| matches!(record, ConnectorRecordResult::RoutedToError { .. }))
        .count();
    let retryable_failures = batch
        .records
        .iter()
        .filter(|record| matches!(record, ConnectorRecordResult::RetryableFailure { .. }))
        .count();
    if batch.batch_id != batch_id
        || receipt.batch_id != batch_id
        || receipt.applied != applied
        || receipt.error_routes != error_routes
        || receipt.retryable_failures != retryable_failures
        || receipt.checkpoint_advanced != (retryable_failures == 0)
    {
        return Err(EpochError::InvalidArgument(format!(
            "connector {connector_name} batch {batch_id} snapshot receipt is invalid"
        )));
    }
    Ok(())
}

fn validate_checkpoint_snapshot(
    connector_name: &str,
    resource: &ConnectorResource,
    checkpoint: &ConnectorCheckpoint,
) -> EpochResult<()> {
    for (field, value) in [
        (
            "checkpoint source_position",
            checkpoint.source_position.as_str(),
        ),
        (
            "checkpoint target_idempotency_key",
            checkpoint.target_idempotency_key.as_str(),
        ),
        ("checkpoint batch_id", checkpoint.batch_id.as_str()),
    ] {
        validate_text(field, value)?;
    }
    let valid = resource
        .batches
        .get(&checkpoint.batch_id)
        .is_some_and(|(batch, receipt)| {
            receipt.checkpoint_advanced
                && batch.source_to == checkpoint.source_position
                && batch.target_idempotency_key == checkpoint.target_idempotency_key
                && batch.committed_at_ms == checkpoint.committed_at_ms
        });
    if !valid {
        return Err(EpochError::InvalidArgument(format!(
            "connector {connector_name} checkpoint does not match a committed batch"
        )));
    }
    Ok(())
}

fn same_batch_identity(left: &ConnectorBatchCommit, right: &ConnectorBatchCommit) -> bool {
    left.batch_id == right.batch_id
        && left.source_from == right.source_from
        && left.source_to == right.source_to
        && left.target_idempotency_key == right.target_idempotency_key
        && left.records == right.records
}

fn validate_spec(spec: &ConnectorSpec) -> EpochResult<()> {
    validate_resource_name(&spec.name)?;
    validate_resource_name(&spec.identity)?;
    if spec.secret_refs.len() > MAX_CONNECTOR_SECRET_REFS {
        return Err(EpochError::InvalidArgument(format!(
            "connector has too many secret references; maximum is {MAX_CONNECTOR_SECRET_REFS}"
        )));
    }
    if spec.outbound_allowlist.len() > MAX_CONNECTOR_ALLOWLIST {
        return Err(EpochError::InvalidArgument(format!(
            "connector has too many outbound allowlist entries; maximum is {MAX_CONNECTOR_ALLOWLIST}"
        )));
    }
    if spec.config.len() > MAX_CONNECTOR_CONFIG_ENTRIES {
        return Err(EpochError::InvalidArgument(format!(
            "connector has too many config entries; maximum is {MAX_CONNECTOR_CONFIG_ENTRIES}"
        )));
    }
    for secret in &spec.secret_refs {
        validate_resource_name(secret)?;
    }
    for destination in &spec.outbound_allowlist {
        validate_text("connector outbound allowlist entry", destination)?;
    }
    for (key, value) in &spec.config {
        validate_text("connector config key", key)?;
        validate_text("connector config value", value)?;
        if key.to_ascii_lowercase().contains("password")
            || key.to_ascii_lowercase().contains("secret")
            || key.to_ascii_lowercase().contains("token")
        {
            return Err(EpochError::InvalidArgument(
                "connector config must use secret references instead of inline credentials".into(),
            ));
        }
    }
    Ok(())
}

fn validate_batch(batch: &ConnectorBatchCommit) -> EpochResult<()> {
    for (field, value) in [
        ("batch_id", batch.batch_id.as_str()),
        ("source_from", batch.source_from.as_str()),
        ("source_to", batch.source_to.as_str()),
        (
            "target_idempotency_key",
            batch.target_idempotency_key.as_str(),
        ),
    ] {
        validate_text(field, value)?;
    }
    if batch.records.is_empty() || batch.records.len() > MAX_CONNECTOR_BATCH_RECORDS {
        return Err(EpochError::InvalidArgument(format!(
            "connector batch records must be between 1 and {MAX_CONNECTOR_BATCH_RECORDS}"
        )));
    }
    let mut record_ids = BTreeSet::new();
    for record in &batch.records {
        validate_text("connector record_id", record.record_id())?;
        if !record_ids.insert(record.record_id()) {
            return Err(EpochError::InvalidArgument(format!(
                "connector record {} is duplicated",
                record.record_id()
            )));
        }
        match record {
            ConnectorRecordResult::RoutedToError { reason, .. }
            | ConnectorRecordResult::RetryableFailure { reason, .. } => {
                validate_text("connector record failure reason", reason)?;
            }
            ConnectorRecordResult::Applied { .. } => {}
        }
    }
    Ok(())
}

fn push_error(resource: &mut ConnectorResource, error: ConnectorRecordError) -> EpochResult<()> {
    if resource.errors.len() >= MAX_CONNECTOR_HISTORY {
        return Err(EpochError::Capacity(
            "connector error history reached its limit".into(),
        ));
    }
    resource.errors.push(error);
    Ok(())
}

fn validate_text(field: &str, value: &str) -> EpochResult<()> {
    if value.is_empty()
        || value.len() > MAX_CONNECTOR_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(EpochError::InvalidArgument(format!(
            "{field} must be between 1 and {MAX_CONNECTOR_TEXT_BYTES} printable bytes"
        )));
    }
    Ok(())
}
