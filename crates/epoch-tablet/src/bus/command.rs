//! Versioned Event Bus tablet commands and their strict canonical codec.

use epoch_bus::{
    EpochTargetDestination, IntegrationOperation, MAX_DELIVERY_ACQUIRE_BATCH,
    MAX_DELIVERY_REASON_BYTES, Subscription,
};
use epoch_core::{EventEnvelope, validate_resource_name};
use serde::{Deserialize, Serialize};

use crate::common::{proposal_id_from_domain, validate_idempotency_key};
use crate::{TabletError, TabletResult, TabletScope};

pub const BUS_TABLET_COMMAND_FORMAT_VERSION: u16 = 5;
const INTEGRATION_BUS_TABLET_COMMAND_FORMAT_VERSION: u16 = 4;
const EPOCH_TARGET_BUS_TABLET_COMMAND_FORMAT_VERSION: u16 = 3;
const SIGNED_WEBHOOK_BUS_TABLET_COMMAND_FORMAT_VERSION: u16 = 2;
const LEGACY_BUS_TABLET_COMMAND_FORMAT_VERSION: u16 = 1;
pub const MAX_BUS_TABLET_COMMAND_BYTES: usize = 512 * 1024;
pub const MAX_BUS_DELIVERY_ID_BYTES: usize = 512;
pub const MAX_BUS_DELIVERY_LEASE_TOKEN_BYTES: usize = 256;

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
    ApplyIntegration {
        operation: Box<IntegrationOperation>,
    },
    UpsertSubscription {
        subscription: Subscription,
    },
    RemoveSubscription {
        name: String,
    },
    Publish {
        envelope: EventEnvelope,
    },
    AcquireDeliveries {
        subscription: String,
        dispatcher: String,
        dispatcher_epoch: u64,
        max_deliveries: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_delivery_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        destination: Option<EpochTargetDestination>,
    },
    AcknowledgeDelivery {
        delivery_id: String,
        dispatcher: String,
        dispatcher_epoch: u64,
        lease_token: String,
    },
    FailDelivery {
        delivery_id: String,
        dispatcher: String,
        dispatcher_epoch: u64,
        lease_token: String,
        reason: String,
    },
    RejectDelivery {
        delivery_id: String,
        dispatcher: String,
        dispatcher_epoch: u64,
        lease_token: String,
        reason: String,
    },
    RedriveDelivery {
        delivery_id: String,
    },
    MaintainArchive {
        max_events: u16,
    },
    MaintainDeliveries {
        max_deliveries: u16,
    },
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
            format_version: operation.format_version(),
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
        if self.format_version != self.operation.format_version() {
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
        validate_operation(&self.operation)
    }
}

impl BusTabletOperation {
    fn format_version(&self) -> u16 {
        match self {
            Self::MaintainArchive { .. } => BUS_TABLET_COMMAND_FORMAT_VERSION,
            Self::ApplyIntegration { .. } | Self::RedriveDelivery { .. } => {
                INTEGRATION_BUS_TABLET_COMMAND_FORMAT_VERSION
            }
            Self::AcquireDeliveries {
                destination: Some(_),
                ..
            } => EPOCH_TARGET_BUS_TABLET_COMMAND_FORMAT_VERSION,
            Self::RejectDelivery { .. }
            | Self::AcquireDeliveries {
                expected_delivery_id: Some(_),
                destination: None,
                ..
            } => SIGNED_WEBHOOK_BUS_TABLET_COMMAND_FORMAT_VERSION,
            Self::UpsertSubscription { subscription } if subscription_uses_v4(subscription) => {
                BUS_TABLET_COMMAND_FORMAT_VERSION
            }
            Self::UpsertSubscription { subscription }
                if subscription.target.signing_key_id().is_some() =>
            {
                SIGNED_WEBHOOK_BUS_TABLET_COMMAND_FORMAT_VERSION
            }
            Self::UpsertSubscription { .. }
            | Self::RemoveSubscription { .. }
            | Self::Publish { .. }
            | Self::AcquireDeliveries {
                expected_delivery_id: None,
                destination: None,
                ..
            }
            | Self::AcknowledgeDelivery { .. }
            | Self::FailDelivery { .. }
            | Self::MaintainDeliveries { .. } => LEGACY_BUS_TABLET_COMMAND_FORMAT_VERSION,
        }
    }
}

fn validate_operation(operation: &BusTabletOperation) -> TabletResult<()> {
    match operation {
        BusTabletOperation::ApplyIntegration { .. } => Ok(()),
        BusTabletOperation::UpsertSubscription { subscription } => {
            subscription.validate()?;
            Ok(())
        }
        BusTabletOperation::RemoveSubscription { name } => {
            validate_resource_name(name)?;
            Ok(())
        }
        BusTabletOperation::Publish { envelope } => {
            envelope.validate()?;
            Ok(())
        }
        BusTabletOperation::AcquireDeliveries {
            subscription,
            dispatcher,
            dispatcher_epoch,
            max_deliveries,
            expected_delivery_id,
            destination,
        } => validate_acquire(
            subscription,
            dispatcher,
            *dispatcher_epoch,
            *max_deliveries,
            expected_delivery_id.as_deref(),
            destination.as_ref(),
        ),
        BusTabletOperation::AcknowledgeDelivery {
            delivery_id,
            dispatcher,
            dispatcher_epoch,
            lease_token,
        } => validate_delivery_settlement(delivery_id, dispatcher, *dispatcher_epoch, lease_token),
        BusTabletOperation::FailDelivery {
            delivery_id,
            dispatcher,
            dispatcher_epoch,
            lease_token,
            reason,
        }
        | BusTabletOperation::RejectDelivery {
            delivery_id,
            dispatcher,
            dispatcher_epoch,
            lease_token,
            reason,
        } => {
            validate_delivery_settlement(delivery_id, dispatcher, *dispatcher_epoch, lease_token)?;
            validate_required_bounded("reason", reason, MAX_DELIVERY_REASON_BYTES)
        }
        BusTabletOperation::RedriveDelivery { delivery_id } => {
            validate_required_bounded("delivery_id", delivery_id, MAX_BUS_DELIVERY_ID_BYTES)
        }
        BusTabletOperation::MaintainArchive { max_events } => {
            validate_archive_maintenance_batch(*max_events)
        }
        BusTabletOperation::MaintainDeliveries { max_deliveries } => {
            validate_delivery_batch(*max_deliveries)
        }
    }
}

fn subscription_uses_v4(subscription: &Subscription) -> bool {
    subscription.delivery_policy.rate_limit.is_some()
        || subscription
            .delivery_policy
            .dead_letter_retention_ms
            .is_some()
        || !subscription.filter.topic_patterns.is_empty()
        || !subscription.transform.rename_fields.is_empty()
        || !subscription.transform.constants.is_empty()
        || !subscription.transform.templates.is_empty()
        || subscription.transform.enrichment_ref.is_some()
        || subscription.transform.limits != epoch_bus::TransformLimits::default()
        || matches!(
            subscription.target,
            epoch_bus::SubscriptionTarget::ApiDestination { .. }
                | epoch_bus::SubscriptionTarget::EndpointPool { .. }
                | epoch_bus::SubscriptionTarget::Function { .. }
                | epoch_bus::SubscriptionTarget::Connector { .. }
        )
}

fn validate_acquire(
    subscription: &str,
    dispatcher: &str,
    dispatcher_epoch: u64,
    max_deliveries: u16,
    expected_delivery_id: Option<&str>,
    destination: Option<&EpochTargetDestination>,
) -> TabletResult<()> {
    validate_resource_name(subscription)?;
    validate_dispatcher(dispatcher, dispatcher_epoch)?;
    validate_delivery_batch(max_deliveries)?;
    if let Some(delivery_id) = expected_delivery_id {
        validate_required_bounded(
            "expected_delivery_id",
            delivery_id,
            MAX_BUS_DELIVERY_ID_BYTES,
        )?;
        if max_deliveries != 1 {
            return Err(TabletError::InvalidCommand(
                "expected_delivery_id requires max_deliveries 1".into(),
            ));
        }
    }
    if let Some(destination) = destination {
        destination.validate()?;
        if expected_delivery_id.is_none() || max_deliveries != 1 {
            return Err(TabletError::InvalidCommand(
                "destination requires expected_delivery_id and max_deliveries 1".into(),
            ));
        }
    }
    Ok(())
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

fn validate_dispatcher(dispatcher: &str, dispatcher_epoch: u64) -> TabletResult<()> {
    validate_resource_name(dispatcher)?;
    if dispatcher_epoch == 0 {
        return Err(TabletError::InvalidCommand(
            "dispatcher_epoch must be non-zero".into(),
        ));
    }
    Ok(())
}

fn validate_delivery_batch(max_deliveries: u16) -> TabletResult<()> {
    if max_deliveries == 0 || usize::from(max_deliveries) > MAX_DELIVERY_ACQUIRE_BATCH {
        return Err(TabletError::InvalidCommand(format!(
            "max_deliveries must be between 1 and {MAX_DELIVERY_ACQUIRE_BATCH}"
        )));
    }
    Ok(())
}

fn validate_archive_maintenance_batch(max_events: u16) -> TabletResult<()> {
    if max_events == 0 || usize::from(max_events) > epoch_bus::MAX_REPLAY_EVENTS {
        return Err(TabletError::InvalidCommand(format!(
            "max_events must be between 1 and {}",
            epoch_bus::MAX_REPLAY_EVENTS
        )));
    }
    Ok(())
}

fn validate_delivery_settlement(
    delivery_id: &str,
    dispatcher: &str,
    dispatcher_epoch: u64,
    lease_token: &str,
) -> TabletResult<()> {
    validate_required_bounded("delivery_id", delivery_id, MAX_BUS_DELIVERY_ID_BYTES)?;
    validate_dispatcher(dispatcher, dispatcher_epoch)?;
    validate_required_bounded(
        "lease_token",
        lease_token,
        MAX_BUS_DELIVERY_LEASE_TOKEN_BYTES,
    )
}

fn validate_required_bounded(field: &str, value: &str, maximum: usize) -> TabletResult<()> {
    if value.is_empty() || value.len() > maximum {
        return Err(TabletError::InvalidCommand(format!(
            "{field} must be between 1 and {maximum} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(TabletError::InvalidCommand(format!(
            "{field} cannot contain control characters"
        )));
    }
    Ok(())
}
