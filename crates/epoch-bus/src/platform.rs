//! Cross-cutting Event Bus schema validation and bounded enrichment state.

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{EpochError, EpochResult, EventEnvelope, validate_resource_name};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::{
    ConnectorBatchCommit, ConnectorBatchReceipt, ConnectorRegistry, ConnectorReplayRequest,
    ConnectorSpec, ConnectorStatus, EndpointObservation, EndpointRegistry, EventCatalog,
    EventCatalogEntry, MqttBrokerState, MqttConnect, MqttPublish, MqttPublishPlan,
    MqttRetainedMessage, MqttSubscription, SchemaRegistration, SchemaRegistry,
};

const MAX_VALIDATION_POLICIES: usize = 10_000;
const MAX_ENRICHMENTS: usize = 10_000;
const MAX_ENRICHMENT_RECORDS: usize = 100_000;
const MAX_ENRICHMENT_TIMEOUT_MS: u64 = 1_000;
const MAX_ENRICHMENT_DOCUMENT_BYTES: usize = 1024 * 1024;
const MAX_INTEGRATION_TEXT_BYTES: usize = 4 * 1024;
const MAX_FUNCTIONS: usize = 10_000;
const MAX_FUNCTION_ALLOWLIST: usize = 256;
const MAX_FUNCTION_TIMEOUT_MS: u64 = 30_000;
const MAX_FUNCTION_INPUT_BYTES: usize = 1024 * 1024;
pub const MAX_EVENT_INTEGRATION_STATE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaValidationMode {
    Disabled,
    Producer,
    Broker,
    ProducerAndBroker,
}

impl SchemaValidationMode {
    const fn validates_producer(self) -> bool {
        matches!(self, Self::Producer | Self::ProducerAndBroker)
    }

    const fn validates_broker(self) -> bool {
        matches!(self, Self::Broker | Self::ProducerAndBroker)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaValidationPolicy {
    pub name: String,
    pub event_type_pattern: String,
    pub schema_ref: String,
    pub mode: SchemaValidationMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrichmentLimits {
    pub timeout_ms: u64,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    #[serde(default)]
    pub network_access: bool,
}

impl Default for EnrichmentLimits {
    fn default() -> Self {
        Self {
            timeout_ms: 100,
            max_input_bytes: 256 * 1024,
            max_output_bytes: 256 * 1024,
            network_access: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrichmentDefinition {
    pub name: String,
    pub lookup_path: String,
    pub output_field: String,
    pub records: BTreeMap<String, Value>,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub limits: EnrichmentLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionStatus {
    Active,
    Paused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionDefinition {
    pub name: String,
    pub endpoint: String,
    pub identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<String>,
    pub timeout_ms: u64,
    pub max_input_bytes: usize,
    #[serde(default)]
    pub outbound_allowlist: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionResource {
    pub definition: FunctionDefinition,
    pub revision: u64,
    pub status: FunctionStatus,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventIntegrationState {
    #[serde(default)]
    schemas: SchemaRegistry,
    #[serde(default)]
    connectors: ConnectorRegistry,
    #[serde(default)]
    mqtt: MqttBrokerState,
    #[serde(default)]
    catalog: EventCatalog,
    #[serde(default)]
    endpoints: EndpointRegistry,
    #[serde(default)]
    validation_policies: BTreeMap<String, SchemaValidationPolicy>,
    #[serde(default)]
    enrichments: BTreeMap<String, EnrichmentDefinition>,
    #[serde(default)]
    functions: BTreeMap<String, FunctionResource>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IntegrationOperation {
    RegisterSchema {
        registration: SchemaRegistration,
    },
    UpsertValidationPolicy {
        policy: SchemaValidationPolicy,
    },
    RemoveValidationPolicy {
        name: String,
    },
    UpsertEnrichment {
        definition: EnrichmentDefinition,
    },
    RemoveEnrichment {
        name: String,
    },
    UpsertFunction {
        definition: FunctionDefinition,
    },
    SetFunctionStatus {
        name: String,
        status: FunctionStatus,
    },
    UpsertConnector {
        spec: ConnectorSpec,
    },
    SetConnectorStatus {
        name: String,
        status: ConnectorStatus,
    },
    CommitConnectorBatch {
        name: String,
        commit: ConnectorBatchCommit,
    },
    RequestConnectorReplay {
        name: String,
        source_from: String,
        source_to: String,
    },
    RotateConnectorSecret {
        reference: String,
        version: u64,
    },
    MqttConnect {
        connect: MqttConnect,
    },
    MqttDisconnect {
        client_id: String,
    },
    MqttSubscribe {
        client_id: String,
        subscription: MqttSubscription,
    },
    MqttUnsubscribe {
        client_id: String,
        subscription: MqttSubscription,
    },
    MqttPublish {
        publish: Box<MqttPublish>,
    },
    MqttClearRetained {
        topic: String,
    },
    MqttExpireSessions {
        limit: u16,
    },
    UpsertCatalogEntry {
        entry: EventCatalogEntry,
    },
    RemoveCatalogEntry {
        event_type: String,
    },
    ObserveEndpoint {
        observation: EndpointObservation,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IntegrationOutcome {
    SchemaRegistered {
        reference: String,
    },
    ValidationPolicyUpserted {
        name: String,
    },
    ValidationPolicyRemoved {
        name: String,
        removed: bool,
    },
    EnrichmentUpserted {
        name: String,
    },
    EnrichmentRemoved {
        name: String,
        removed: bool,
    },
    FunctionUpserted {
        name: String,
        revision: u64,
    },
    FunctionStatusSet {
        name: String,
        status: FunctionStatus,
    },
    ConnectorUpserted {
        name: String,
        revision: u64,
    },
    ConnectorStatusSet {
        name: String,
        status: ConnectorStatus,
    },
    ConnectorBatchCommitted {
        name: String,
        receipt: ConnectorBatchReceipt,
    },
    ConnectorReplayRequested {
        name: String,
        request: ConnectorReplayRequest,
    },
    ConnectorSecretRotated {
        reference: String,
        version: u64,
    },
    MqttConnected {
        client_id: String,
        resumed: bool,
    },
    MqttDisconnected {
        client_id: String,
    },
    MqttSubscribed {
        client_id: String,
        retained: Vec<MqttRetainedMessage>,
    },
    MqttUnsubscribed {
        client_id: String,
        removed: bool,
    },
    MqttPublished {
        plan: MqttPublishPlan,
    },
    MqttRetainedCleared {
        topic: String,
        removed: bool,
    },
    MqttSessionsExpired {
        expired: usize,
    },
    CatalogEntryUpserted {
        event_type: String,
        revision: u64,
    },
    CatalogEntryRemoved {
        event_type: String,
        removed: bool,
    },
    EndpointObserved {
        pool: String,
        endpoint: String,
    },
}

impl EventIntegrationState {
    pub fn apply(
        &mut self,
        operation: IntegrationOperation,
        applied_at_ms: u64,
    ) -> EpochResult<IntegrationOutcome> {
        let mut candidate = self.clone();
        let outcome = candidate.apply_unchecked(operation, applied_at_ms)?;
        candidate.validate_snapshot()?;
        *self = candidate;
        Ok(outcome)
    }

    fn apply_unchecked(
        &mut self,
        operation: IntegrationOperation,
        applied_at_ms: u64,
    ) -> EpochResult<IntegrationOutcome> {
        match operation {
            IntegrationOperation::RegisterSchema { registration } => {
                let revision = self.schemas.register(registration, applied_at_ms)?;
                Ok(IntegrationOutcome::SchemaRegistered {
                    reference: revision.reference(),
                })
            }
            IntegrationOperation::UpsertValidationPolicy { policy } => {
                let name = policy.name.clone();
                self.upsert_validation_policy(policy)?;
                Ok(IntegrationOutcome::ValidationPolicyUpserted { name })
            }
            IntegrationOperation::RemoveValidationPolicy { name } => {
                let removed = self.remove_validation_policy(&name)?;
                Ok(IntegrationOutcome::ValidationPolicyRemoved { name, removed })
            }
            IntegrationOperation::UpsertEnrichment { definition } => {
                let name = definition.name.clone();
                self.upsert_enrichment(definition)?;
                Ok(IntegrationOutcome::EnrichmentUpserted { name })
            }
            IntegrationOperation::RemoveEnrichment { name } => {
                let removed = self.remove_enrichment(&name)?;
                Ok(IntegrationOutcome::EnrichmentRemoved { name, removed })
            }
            IntegrationOperation::UpsertFunction { definition } => {
                self.apply_function_upsert(definition, applied_at_ms)
            }
            IntegrationOperation::SetFunctionStatus { name, status } => {
                self.set_function_status(&name, status, applied_at_ms)?;
                Ok(IntegrationOutcome::FunctionStatusSet { name, status })
            }
            IntegrationOperation::UpsertConnector { spec } => {
                let name = spec.name.clone();
                let revision = self.connectors.upsert(spec, applied_at_ms)?;
                Ok(IntegrationOutcome::ConnectorUpserted { name, revision })
            }
            IntegrationOperation::SetConnectorStatus { name, status } => {
                match status {
                    ConnectorStatus::Active => self.connectors.resume(&name, applied_at_ms)?,
                    ConnectorStatus::Paused => self.connectors.pause(&name, applied_at_ms)?,
                }
                Ok(IntegrationOutcome::ConnectorStatusSet { name, status })
            }
            IntegrationOperation::CommitConnectorBatch { name, commit } => {
                self.apply_connector_batch(name, commit, applied_at_ms)
            }
            IntegrationOperation::RequestConnectorReplay {
                name,
                source_from,
                source_to,
            } => self.apply_connector_replay(name, &source_from, &source_to, applied_at_ms),
            IntegrationOperation::RotateConnectorSecret { reference, version } => {
                self.connectors
                    .rotate_secret(&reference, version, applied_at_ms)?;
                Ok(IntegrationOutcome::ConnectorSecretRotated { reference, version })
            }
            IntegrationOperation::MqttConnect { connect } => {
                self.apply_mqtt_connect(connect, applied_at_ms)
            }
            IntegrationOperation::MqttDisconnect { client_id } => {
                self.mqtt.disconnect(&client_id, applied_at_ms)?;
                Ok(IntegrationOutcome::MqttDisconnected { client_id })
            }
            IntegrationOperation::MqttSubscribe {
                client_id,
                subscription,
            } => self.apply_mqtt_subscribe(client_id, subscription),
            IntegrationOperation::MqttUnsubscribe {
                client_id,
                subscription,
            } => {
                let removed = self.mqtt.unsubscribe(&client_id, &subscription)?;
                Ok(IntegrationOutcome::MqttUnsubscribed { client_id, removed })
            }
            IntegrationOperation::MqttPublish { publish } => {
                self.apply_mqtt_publish(*publish, applied_at_ms)
            }
            IntegrationOperation::MqttClearRetained { topic } => {
                let removed = self.mqtt.clear_retained(&topic)?;
                Ok(IntegrationOutcome::MqttRetainedCleared { topic, removed })
            }
            IntegrationOperation::MqttExpireSessions { limit } => {
                let expired = self
                    .mqtt
                    .expire_sessions(applied_at_ms, usize::from(limit))?;
                Ok(IntegrationOutcome::MqttSessionsExpired { expired })
            }
            IntegrationOperation::UpsertCatalogEntry { entry } => self.apply_catalog_upsert(entry),
            IntegrationOperation::RemoveCatalogEntry { event_type } => {
                let removed = self.catalog.remove(&event_type)?;
                Ok(IntegrationOutcome::CatalogEntryRemoved {
                    event_type,
                    removed,
                })
            }
            IntegrationOperation::ObserveEndpoint { observation } => {
                self.apply_endpoint_observation(observation, applied_at_ms)
            }
        }
    }

    fn apply_function_upsert(
        &mut self,
        definition: FunctionDefinition,
        applied_at_ms: u64,
    ) -> EpochResult<IntegrationOutcome> {
        let name = definition.name.clone();
        let revision = self.upsert_function(definition, applied_at_ms)?;
        Ok(IntegrationOutcome::FunctionUpserted { name, revision })
    }

    fn apply_connector_batch(
        &mut self,
        name: String,
        mut commit: ConnectorBatchCommit,
        applied_at_ms: u64,
    ) -> EpochResult<IntegrationOutcome> {
        commit.committed_at_ms = applied_at_ms;
        let receipt = self.connectors.commit_batch(&name, commit)?;
        Ok(IntegrationOutcome::ConnectorBatchCommitted { name, receipt })
    }

    fn apply_connector_replay(
        &mut self,
        name: String,
        source_from: &str,
        source_to: &str,
        applied_at_ms: u64,
    ) -> EpochResult<IntegrationOutcome> {
        let request =
            self.connectors
                .request_replay(&name, source_from, source_to, applied_at_ms)?;
        Ok(IntegrationOutcome::ConnectorReplayRequested { name, request })
    }

    fn apply_mqtt_connect(
        &mut self,
        mut connect: MqttConnect,
        applied_at_ms: u64,
    ) -> EpochResult<IntegrationOutcome> {
        connect.connected_at_ms = applied_at_ms;
        let client_id = connect.client_id.clone();
        let resumed = self.mqtt.connect(connect)?;
        Ok(IntegrationOutcome::MqttConnected { client_id, resumed })
    }

    fn apply_mqtt_subscribe(
        &mut self,
        client_id: String,
        subscription: MqttSubscription,
    ) -> EpochResult<IntegrationOutcome> {
        let retained = self.mqtt.subscribe(&client_id, subscription)?;
        Ok(IntegrationOutcome::MqttSubscribed {
            client_id,
            retained,
        })
    }

    fn apply_mqtt_publish(
        &mut self,
        mut publish: MqttPublish,
        applied_at_ms: u64,
    ) -> EpochResult<IntegrationOutcome> {
        publish.published_at_ms = applied_at_ms;
        let plan = self.mqtt.publish(publish)?;
        Ok(IntegrationOutcome::MqttPublished { plan })
    }

    fn apply_catalog_upsert(
        &mut self,
        entry: EventCatalogEntry,
    ) -> EpochResult<IntegrationOutcome> {
        let event_type = entry.event_type.clone();
        let revision = self.catalog.upsert(entry)?;
        Ok(IntegrationOutcome::CatalogEntryUpserted {
            event_type,
            revision,
        })
    }

    fn apply_endpoint_observation(
        &mut self,
        mut observation: EndpointObservation,
        applied_at_ms: u64,
    ) -> EpochResult<IntegrationOutcome> {
        observation.observed_at_ms = applied_at_ms;
        let pool = observation.pool.clone();
        let endpoint = observation.endpoint.clone();
        self.endpoints.observe(observation)?;
        Ok(IntegrationOutcome::EndpointObserved { pool, endpoint })
    }

    pub(crate) fn validate_snapshot(&self) -> EpochResult<()> {
        let encoded = serde_json::to_vec(self)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
        if encoded.len() > MAX_EVENT_INTEGRATION_STATE_BYTES {
            return Err(EpochError::Capacity(format!(
                "Event integration state is {} bytes; maximum is {MAX_EVENT_INTEGRATION_STATE_BYTES}",
                encoded.len()
            )));
        }
        self.schemas.validate_snapshot()?;
        self.connectors.validate_snapshot()?;
        self.mqtt.validate_snapshot()?;
        self.catalog.validate_snapshot()?;
        self.endpoints.validate_snapshot()?;
        if self.validation_policies.len() > MAX_VALIDATION_POLICIES
            || self.enrichments.len() > MAX_ENRICHMENTS
            || self.functions.len() > MAX_FUNCTIONS
        {
            return Err(EpochError::InvalidArgument(
                "Event integration snapshot registry capacity is invalid".into(),
            ));
        }
        for (name, policy) in &self.validation_policies {
            if policy.name != *name {
                return Err(EpochError::InvalidArgument(format!(
                    "schema validation policy {name} snapshot identity is invalid"
                )));
            }
            validate_resource_name(name)?;
            validate_pattern(&policy.event_type_pattern)?;
            self.schemas.revision(&policy.schema_ref)?;
        }
        for (name, definition) in &self.enrichments {
            if definition.name != *name {
                return Err(EpochError::InvalidArgument(format!(
                    "enrichment {name} snapshot identity is invalid"
                )));
            }
            validate_enrichment(definition)?;
        }
        for (name, resource) in &self.functions {
            if resource.definition.name != *name || resource.revision == 0 {
                return Err(EpochError::InvalidArgument(format!(
                    "function {name} snapshot identity is invalid"
                )));
            }
            validate_function(&resource.definition)?;
        }
        Ok(())
    }

    pub fn schemas(&self) -> &SchemaRegistry {
        &self.schemas
    }

    pub fn schemas_mut(&mut self) -> &mut SchemaRegistry {
        &mut self.schemas
    }

    pub fn connectors(&self) -> &ConnectorRegistry {
        &self.connectors
    }

    pub fn connectors_mut(&mut self) -> &mut ConnectorRegistry {
        &mut self.connectors
    }

    pub fn mqtt(&self) -> &MqttBrokerState {
        &self.mqtt
    }

    pub fn mqtt_mut(&mut self) -> &mut MqttBrokerState {
        &mut self.mqtt
    }

    pub fn catalog(&self) -> &EventCatalog {
        &self.catalog
    }

    pub fn catalog_mut(&mut self) -> &mut EventCatalog {
        &mut self.catalog
    }

    pub fn endpoints(&self) -> &EndpointRegistry {
        &self.endpoints
    }

    pub fn endpoints_mut(&mut self) -> &mut EndpointRegistry {
        &mut self.endpoints
    }

    pub fn upsert_validation_policy(&mut self, policy: SchemaValidationPolicy) -> EpochResult<()> {
        validate_resource_name(&policy.name)?;
        validate_pattern(&policy.event_type_pattern)?;
        self.schemas.revision(&policy.schema_ref)?;
        if !self.validation_policies.contains_key(&policy.name)
            && self.validation_policies.len() >= MAX_VALIDATION_POLICIES
        {
            return Err(EpochError::Capacity(format!(
                "schema validation policy registry reached its {MAX_VALIDATION_POLICIES} limit"
            )));
        }
        self.validation_policies.insert(policy.name.clone(), policy);
        Ok(())
    }

    pub fn remove_validation_policy(&mut self, name: &str) -> EpochResult<bool> {
        validate_resource_name(name)?;
        Ok(self.validation_policies.remove(name).is_some())
    }

    pub fn validation_policy(&self, name: &str) -> Option<&SchemaValidationPolicy> {
        self.validation_policies.get(name)
    }

    pub fn validate_for_producer(&self, event: &EventEnvelope) -> EpochResult<()> {
        self.validate_event(event, SchemaValidationMode::validates_producer)
    }

    pub fn validate_for_broker(&self, event: &EventEnvelope) -> EpochResult<()> {
        self.validate_event(event, SchemaValidationMode::validates_broker)
    }

    pub fn upsert_enrichment(&mut self, definition: EnrichmentDefinition) -> EpochResult<()> {
        validate_enrichment(&definition)?;
        if !self.enrichments.contains_key(&definition.name)
            && self.enrichments.len() >= MAX_ENRICHMENTS
        {
            return Err(EpochError::Capacity(format!(
                "enrichment registry reached its {MAX_ENRICHMENTS} definition limit"
            )));
        }
        self.enrichments.insert(definition.name.clone(), definition);
        Ok(())
    }

    pub fn remove_enrichment(&mut self, name: &str) -> EpochResult<bool> {
        validate_resource_name(name)?;
        Ok(self.enrichments.remove(name).is_some())
    }

    pub fn enrichment(&self, name: &str) -> Option<&EnrichmentDefinition> {
        self.enrichments.get(name)
    }

    pub fn function(&self, name: &str) -> Option<&FunctionResource> {
        self.functions.get(name)
    }

    pub fn upsert_function(
        &mut self,
        definition: FunctionDefinition,
        updated_at_ms: u64,
    ) -> EpochResult<u64> {
        validate_function(&definition)?;
        if !self.functions.contains_key(&definition.name) && self.functions.len() >= MAX_FUNCTIONS {
            return Err(EpochError::Capacity(format!(
                "function registry reached its {MAX_FUNCTIONS} definition limit"
            )));
        }
        let revision = self
            .functions
            .get(&definition.name)
            .map_or(Ok(1), |resource| {
                resource
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| EpochError::Capacity("function revision overflow".into()))
            })?;
        let status = self
            .functions
            .get(&definition.name)
            .map_or(FunctionStatus::Active, |resource| resource.status);
        self.functions.insert(
            definition.name.clone(),
            FunctionResource {
                definition,
                revision,
                status,
                updated_at_ms,
            },
        );
        Ok(revision)
    }

    pub fn set_function_status(
        &mut self,
        name: &str,
        status: FunctionStatus,
        updated_at_ms: u64,
    ) -> EpochResult<()> {
        validate_resource_name(name)?;
        let resource = self
            .functions
            .get_mut(name)
            .ok_or_else(|| EpochError::NotFound(name.to_owned()))?;
        resource.status = status;
        resource.updated_at_ms = updated_at_ms;
        Ok(())
    }

    pub fn enrich(&self, name: &str, event: &EventEnvelope) -> EpochResult<EventEnvelope> {
        let definition = self
            .enrichments
            .get(name)
            .ok_or_else(|| EpochError::NotFound(name.to_owned()))?;
        let input_size = serde_json::to_vec(&event.payload)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?
            .len();
        if input_size > definition.limits.max_input_bytes {
            return Err(EpochError::Capacity(format!(
                "enrichment input is {input_size} bytes; configured maximum is {}",
                definition.limits.max_input_bytes
            )));
        }
        let key = value_at_path(&event.payload, &definition.lookup_path)
            .and_then(scalar_key)
            .ok_or_else(|| {
                EpochError::InvalidArgument(format!(
                    "enrichment lookup path {} must resolve to a scalar",
                    definition.lookup_path
                ))
            })?;
        let Some(value) = definition.records.get(&key) else {
            if definition.required {
                return Err(EpochError::NotFound(format!(
                    "enrichment {} has no record for lookup key",
                    definition.name
                )));
            }
            return Ok(event.clone());
        };
        let mut output = event.clone();
        let object = output.payload.as_object_mut().ok_or_else(|| {
            EpochError::InvalidArgument("enrichment input payload must be an object".into())
        })?;
        object.insert(definition.output_field.clone(), value.clone());
        let output_size = serde_json::to_vec(&output.payload)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?
            .len();
        if output_size > definition.limits.max_output_bytes {
            return Err(EpochError::Capacity(format!(
                "enrichment output is {output_size} bytes; configured maximum is {}",
                definition.limits.max_output_bytes
            )));
        }
        Ok(output)
    }

    pub fn is_empty(&self) -> bool {
        self.schemas.schema_count() == 0
            && self.connectors.is_empty()
            && self.mqtt.is_empty()
            && self.catalog.is_empty()
            && self.validation_policies.is_empty()
            && self.enrichments.is_empty()
            && self.functions.is_empty()
            && self.endpoints.is_empty()
    }

    fn validate_event(
        &self,
        event: &EventEnvelope,
        enabled: fn(SchemaValidationMode) -> bool,
    ) -> EpochResult<()> {
        event.validate()?;
        for policy in self.validation_policies.values().filter(|policy| {
            enabled(policy.mode) && glob_matches(&policy.event_type_pattern, &event.event_type)
        }) {
            if event.schema_ref.as_deref() != Some(policy.schema_ref.as_str()) {
                return Err(EpochError::InvalidArgument(format!(
                    "event type {} requires schema {} under policy {}",
                    event.event_type, policy.schema_ref, policy.name
                )));
            }
            self.schemas
                .validate_payload(&policy.schema_ref, &event.payload)?;
        }
        Ok(())
    }
}

fn validate_function(definition: &FunctionDefinition) -> EpochResult<()> {
    validate_resource_name(&definition.name)?;
    validate_resource_name(&definition.identity)?;
    if let Some(secret_ref) = &definition.secret_ref {
        validate_resource_name(secret_ref)?;
    }
    if definition.timeout_ms == 0 || definition.timeout_ms > MAX_FUNCTION_TIMEOUT_MS {
        return Err(EpochError::InvalidArgument(format!(
            "function timeout_ms must be between 1 and {MAX_FUNCTION_TIMEOUT_MS}"
        )));
    }
    if definition.max_input_bytes == 0 || definition.max_input_bytes > MAX_FUNCTION_INPUT_BYTES {
        return Err(EpochError::InvalidArgument(format!(
            "function max_input_bytes must be between 1 and {MAX_FUNCTION_INPUT_BYTES}"
        )));
    }
    if definition.outbound_allowlist.is_empty()
        || definition.outbound_allowlist.len() > MAX_FUNCTION_ALLOWLIST
    {
        return Err(EpochError::InvalidArgument(format!(
            "function outbound_allowlist must contain between 1 and {MAX_FUNCTION_ALLOWLIST} hosts"
        )));
    }
    let endpoint = Url::parse(&definition.endpoint).map_err(|error| {
        EpochError::InvalidArgument(format!("invalid function endpoint: {error}"))
    })?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(EpochError::InvalidArgument(
            "function endpoint must be an absolute credential-free HTTP(S) URL without a fragment"
                .into(),
        ));
    }
    for host in &definition.outbound_allowlist {
        if host.is_empty()
            || host.len() > MAX_INTEGRATION_TEXT_BYTES
            || host.chars().any(char::is_control)
        {
            return Err(EpochError::InvalidArgument(
                "function outbound allowlist host is invalid".into(),
            ));
        }
    }
    if !definition
        .outbound_allowlist
        .contains(endpoint.host_str().unwrap_or_default())
    {
        return Err(EpochError::InvalidArgument(
            "function endpoint host must be present in outbound_allowlist".into(),
        ));
    }
    Ok(())
}

fn validate_enrichment(definition: &EnrichmentDefinition) -> EpochResult<()> {
    validate_resource_name(&definition.name)?;
    validate_path(&definition.lookup_path)?;
    validate_output_field(&definition.output_field)?;
    if definition.records.len() > MAX_ENRICHMENT_RECORDS {
        return Err(EpochError::InvalidArgument(format!(
            "enrichment has {} records; maximum is {MAX_ENRICHMENT_RECORDS}",
            definition.records.len()
        )));
    }
    if definition.limits.timeout_ms == 0
        || definition.limits.timeout_ms > MAX_ENRICHMENT_TIMEOUT_MS
        || definition.limits.max_input_bytes == 0
        || definition.limits.max_input_bytes > MAX_ENRICHMENT_DOCUMENT_BYTES
        || definition.limits.max_output_bytes == 0
        || definition.limits.max_output_bytes > MAX_ENRICHMENT_DOCUMENT_BYTES
        || definition.limits.network_access
    {
        return Err(EpochError::InvalidArgument(
            "enrichment limits must be bounded and network access must remain disabled".into(),
        ));
    }
    for (key, value) in &definition.records {
        validate_text("enrichment lookup key", key)?;
        let size = serde_json::to_vec(value)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?
            .len();
        if size > definition.limits.max_output_bytes {
            return Err(EpochError::InvalidArgument(format!(
                "enrichment record is {size} bytes; configured maximum is {}",
                definition.limits.max_output_bytes
            )));
        }
    }
    Ok(())
}

fn validate_pattern(pattern: &str) -> EpochResult<()> {
    validate_text("event type pattern", pattern)
}

fn validate_path(path: &str) -> EpochResult<()> {
    validate_text("enrichment lookup path", path)?;
    if path.split('.').any(str::is_empty) {
        return Err(EpochError::InvalidArgument(
            "enrichment lookup path is invalid".into(),
        ));
    }
    Ok(())
}

fn validate_output_field(field: &str) -> EpochResult<()> {
    validate_text("enrichment output field", field)?;
    if field.contains('.') {
        return Err(EpochError::InvalidArgument(
            "enrichment output field cannot contain dots".into(),
        ));
    }
    Ok(())
}

fn validate_text(field: &str, value: &str) -> EpochResult<()> {
    if value.is_empty()
        || value.len() > MAX_INTEGRATION_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(EpochError::InvalidArgument(format!(
            "{field} must be between 1 and {MAX_INTEGRATION_TEXT_BYTES} printable bytes"
        )));
    }
    Ok(())
}

fn value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(value, |current, segment| {
        current.as_object().and_then(|object| object.get(segment))
    })
}

fn scalar_key(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Bool(_) | Value::Number(_) => Some(value.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let value = value.chars().collect::<Vec<_>>();
    let (mut pattern_index, mut value_index) = (0, 0);
    let (mut star, mut checkpoint) = (None, 0);
    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == '?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == '*' {
            star = Some(pattern_index);
            pattern_index += 1;
            checkpoint = value_index;
        } else if let Some(star_index) = star {
            pattern_index = star_index + 1;
            checkpoint += 1;
            value_index = checkpoint;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == '*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn snapshot_validation_rejects_mismatched_function_identity() {
        let state: EventIntegrationState = serde_json::from_value(json!({
            "functions": {
                "wrong-key": {
                    "definition": {
                        "name": "actual-name",
                        "endpoint": "https://example.com/invoke",
                        "identity": "runner",
                        "timeout_ms": 100,
                        "max_input_bytes": 1024,
                        "outbound_allowlist": ["example.com"]
                    },
                    "revision": 1,
                    "status": "active",
                    "updated_at_ms": 1
                }
            }
        }))
        .unwrap();

        assert!(state.validate_snapshot().is_err());
    }

    #[test]
    fn integration_operations_are_atomic_at_the_snapshot_capacity() {
        let mut state = EventIntegrationState::default();
        let record = "x".repeat(300 * 1024);
        let mut rejected = None;
        for index in 0..16 {
            let name = format!("lookup-{index}");
            let operation = IntegrationOperation::UpsertEnrichment {
                definition: EnrichmentDefinition {
                    name: name.clone(),
                    lookup_path: "id".into(),
                    output_field: "profile".into(),
                    records: BTreeMap::from([("one".into(), json!(record))]),
                    required: false,
                    limits: EnrichmentLimits {
                        timeout_ms: 100,
                        max_input_bytes: 1024,
                        max_output_bytes: 512 * 1024,
                        network_access: false,
                    },
                },
            };
            if let Err(error) = state.apply(operation, 1) {
                assert!(matches!(error, EpochError::Capacity(_)));
                rejected = Some(name);
                break;
            }
        }

        let rejected = rejected.expect("bounded state must reject before sixteen records");
        assert!(state.enrichment(&rejected).is_none());
        state.validate_snapshot().unwrap();
    }
}
