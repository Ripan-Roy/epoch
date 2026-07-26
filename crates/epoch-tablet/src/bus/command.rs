//! Versioned Event Bus tablet commands and their strict canonical codec.

use epoch_bus::Subscription;
use epoch_core::{EventEnvelope, validate_resource_name};
use serde::{Deserialize, Serialize};

use crate::common::{proposal_id_from_domain, validate_idempotency_key};
use crate::{TabletError, TabletResult, TabletScope};

pub const BUS_TABLET_COMMAND_FORMAT_VERSION: u16 = 1;
pub const MAX_BUS_TABLET_COMMAND_BYTES: usize = 512 * 1024;

pub type BusTabletScope = TabletScope;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BusTabletCommand {
    pub format_version: u16,
    pub tablet_id: u64,
    pub tablet_epoch: u64,
    pub resource: String,
    pub idempotency_key: String,
    pub applied_at_ms: u64,
    pub operation: BusTabletOperation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BusTabletOperation {
    UpsertSubscription { subscription: Subscription },
    RemoveSubscription { name: String },
    Publish { envelope: EventEnvelope },
}

impl BusTabletCommand {
    pub fn upsert_subscription(
        scope: &BusTabletScope,
        idempotency_key: impl Into<String>,
        subscription: Subscription,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        Self::new(
            scope,
            idempotency_key,
            applied_at_ms,
            BusTabletOperation::UpsertSubscription { subscription },
        )
    }

    pub fn remove_subscription(
        scope: &BusTabletScope,
        idempotency_key: impl Into<String>,
        name: impl Into<String>,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        Self::new(
            scope,
            idempotency_key,
            applied_at_ms,
            BusTabletOperation::RemoveSubscription { name: name.into() },
        )
    }

    pub fn publish(
        scope: &BusTabletScope,
        idempotency_key: impl Into<String>,
        envelope: EventEnvelope,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        Self::new(
            scope,
            idempotency_key,
            applied_at_ms,
            BusTabletOperation::Publish { envelope },
        )
    }

    pub fn new(
        scope: &BusTabletScope,
        idempotency_key: impl Into<String>,
        applied_at_ms: u64,
        operation: BusTabletOperation,
    ) -> TabletResult<Self> {
        let command = Self {
            format_version: BUS_TABLET_COMMAND_FORMAT_VERSION,
            tablet_id: scope.tablet_id,
            tablet_epoch: scope.tablet_epoch,
            resource: scope.resource.clone(),
            idempotency_key: idempotency_key.into(),
            applied_at_ms,
            operation,
        };
        command.validate(scope)?;
        Ok(command)
    }

    pub fn encode(&self, scope: &BusTabletScope) -> TabletResult<Vec<u8>> {
        self.validate(scope)?;
        let encoded =
            serde_json::to_vec(self).map_err(|error| TabletError::Encoding(error.to_string()))?;
        if encoded.len() > MAX_BUS_TABLET_COMMAND_BYTES {
            return Err(command_too_large(encoded.len()));
        }
        Ok(encoded)
    }

    pub fn decode(payload: &[u8], scope: &BusTabletScope) -> TabletResult<Self> {
        if payload.len() > MAX_BUS_TABLET_COMMAND_BYTES {
            return Err(command_too_large(payload.len()));
        }
        let command: Self = serde_json::from_slice(payload)
            .map_err(|error| TabletError::Decoding(error.to_string()))?;
        command.validate(scope)?;
        let canonical = serde_json::to_vec(&command)
            .map_err(|error| TabletError::Encoding(error.to_string()))?;
        if canonical != payload {
            return Err(TabletError::Decoding(
                "command bytes are not in canonical v1 encoding".into(),
            ));
        }
        Ok(command)
    }

    pub fn proposal_id(&self, scope: &BusTabletScope) -> TabletResult<u64> {
        self.validate(scope)?;
        bus_proposal_id_for(scope, &self.idempotency_key)
    }

    fn validate(&self, scope: &BusTabletScope) -> TabletResult<()> {
        scope.validate()?;
        if self.format_version != BUS_TABLET_COMMAND_FORMAT_VERSION {
            return Err(TabletError::InvalidCommand(format!(
                "unsupported format_version {}",
                self.format_version
            )));
        }
        if self.tablet_id != scope.tablet_id {
            return Err(TabletError::GroupMismatch {
                expected: scope.tablet_id,
                observed: self.tablet_id,
            });
        }
        if self.tablet_epoch != scope.tablet_epoch {
            return Err(TabletError::FencedEpoch {
                expected: scope.tablet_epoch,
                observed: self.tablet_epoch,
            });
        }
        if self.resource != scope.resource {
            return Err(TabletError::InvalidCommand(format!(
                "command targets resource {}; expected {}",
                self.resource, scope.resource
            )));
        }
        validate_idempotency_key(&self.idempotency_key)?;
        match &self.operation {
            BusTabletOperation::UpsertSubscription { subscription } => {
                subscription.validate()?;
            }
            BusTabletOperation::RemoveSubscription { name } => {
                validate_resource_name(name)?;
            }
            BusTabletOperation::Publish { envelope } => {
                envelope.validate()?;
            }
        }
        Ok(())
    }
}

pub fn bus_proposal_id_for(scope: &BusTabletScope, idempotency_key: &str) -> TabletResult<u64> {
    proposal_id_from_domain(
        b"epoch/event-bus-tablet/proposal-id/v1\0",
        scope,
        idempotency_key,
    )
}

fn command_too_large(length: usize) -> TabletError {
    TabletError::InvalidCommand(format!(
        "encoded command is {length} bytes; maximum is {MAX_BUS_TABLET_COMMAND_BYTES}"
    ))
}
