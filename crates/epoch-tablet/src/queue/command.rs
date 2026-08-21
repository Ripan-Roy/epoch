//! Versioned Queue tablet commands and their strict canonical codec.

use epoch_bus::EpochTargetDestination;
use epoch_core::EventEnvelope;
use epoch_queue::{
    MAX_FENCED_LEASE_TOKEN_BYTES, MAX_QUEUE_ADVANCED_IDENTIFIER_BYTES,
    MAX_QUEUE_DEFER_REASON_BYTES, QueueIngress,
};
use serde::{Deserialize, Serialize};

use crate::common::{proposal_id_from_domain, validate_idempotency_key};
use crate::{TabletError, TabletResult, TabletScope};

const QUEUE_TABLET_COMMAND_FORMAT_VERSION_V1: u16 = 1;
const QUEUE_TABLET_COMMAND_FORMAT_VERSION_V2: u16 = 2;
pub const QUEUE_TABLET_COMMAND_FORMAT_VERSION: u16 = 3;
pub const MAX_QUEUE_TABLET_COMMAND_BYTES: usize = 512 * 1024;
pub const MAX_QUEUE_ACQUIRE_BATCH_SIZE: u16 = 100;
pub const MAX_QUEUE_CONSUMER_IN_FLIGHT: u16 = 10_000;
pub const MAX_QUEUE_CONSUMER_BYTES: usize = 256;
pub const MAX_QUEUE_REASON_BYTES: usize = 4 * 1024;
// Leaves enough room for the largest valid v1 consumer plus all fixed token
// fields inside epoch-queue's 4 KiB fenced-token ceiling.
pub const MAX_QUEUE_MESSAGE_ID_BYTES: usize = 1024;

pub type QueueTabletScope = TabletScope;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueTabletCommand {
    pub format_version: u16,
    pub tablet_id: u64,
    pub tablet_epoch: u64,
    pub resource: String,
    pub idempotency_key: String,
    pub applied_at_ms: u64,
    pub operation: QueueTabletOperation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueueTabletOperation {
    Enqueue(Box<QueueEnqueueCommand>),
    Acquire(QueueAcquireCommand),
    AcquireWithCredit(QueueCreditAcquireCommand),
    Acknowledge(QueueAcknowledgeCommand),
    ExtendLease(QueueExtendLeaseCommand),
    Release(QueueReleaseCommand),
    Nack(QueueNackCommand),
    Reject(QueueRejectCommand),
    Redrive(QueueRedriveCommand),
    Maintain(QueueMaintainCommand),
    EnqueueAdvanced(Box<QueueAdvancedEnqueueCommand>),
    AcquireSession(QueueSessionAcquireCommand),
    RenewSessionLock(QueueSessionLockRenewCommand),
    ReleaseSessionLock(QueueSessionLockReleaseCommand),
    Defer(QueueDeferCommand),
    ReceiveDeferred(QueueReceiveDeferredCommand),
    BindDeadLetterForward(QueueBindDeadLetterForwardCommand),
    CompleteDeadLetterForward(QueueCompleteDeadLetterForwardCommand),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueEnqueueCommand {
    pub partition: u32,
    pub envelope: EventEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueAcquireCommand {
    pub partition: u32,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub max_messages: u16,
    pub visibility_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCreditAcquireCommand {
    pub partition: u32,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub credit: u16,
    pub max_in_flight: u16,
    pub visibility_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueAcknowledgeCommand {
    pub partition: u32,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub lease_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueExtendLeaseCommand {
    pub partition: u32,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub lease_token: String,
    pub extension_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueReleaseCommand {
    pub partition: u32,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub lease_token: String,
    pub delay_ms: u64,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueNackCommand {
    pub partition: u32,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub lease_token: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRejectCommand {
    pub partition: u32,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub lease_token: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRedriveCommand {
    pub partition: u32,
    pub message_id: String,
    pub dead_letter_history_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueMaintainCommand {
    pub partition: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueAdvancedEnqueueCommand {
    pub partition: u32,
    pub ingress: QueueIngress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueSessionAcquireCommand {
    pub partition: u32,
    pub session_id: String,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub credit: u16,
    pub max_in_flight: u16,
    pub visibility_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_lock_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueSessionLockRenewCommand {
    pub partition: u32,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub session_lock_token: String,
    pub extension_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueSessionLockReleaseCommand {
    pub partition: u32,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub session_lock_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueDeferCommand {
    pub partition: u32,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub lease_token: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueReceiveDeferredCommand {
    pub partition: u32,
    pub message_id: String,
    pub consumer: String,
    pub consumer_epoch: u64,
    pub visibility_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueBindDeadLetterForwardCommand {
    pub partition: u32,
    pub dead_letter_history_id: u64,
    pub destination: EpochTargetDestination,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCompleteDeadLetterForwardCommand {
    pub partition: u32,
    pub dead_letter_history_id: u64,
    pub destination: EpochTargetDestination,
    pub target_message_id: String,
}

impl QueueTabletCommand {
    pub fn new(
        scope: &QueueTabletScope,
        idempotency_key: impl Into<String>,
        applied_at_ms: u64,
        operation: QueueTabletOperation,
    ) -> TabletResult<Self> {
        let format_version = operation.format_version();
        let command = Self {
            format_version,
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

    pub fn enqueue(
        scope: &QueueTabletScope,
        idempotency_key: impl Into<String>,
        envelope: EventEnvelope,
        applied_at_ms: u64,
    ) -> TabletResult<Self> {
        Self::new(
            scope,
            idempotency_key,
            applied_at_ms,
            QueueTabletOperation::Enqueue(Box::new(QueueEnqueueCommand {
                partition: 0,
                envelope,
            })),
        )
    }

    pub fn encode(&self, scope: &QueueTabletScope) -> TabletResult<Vec<u8>> {
        self.validate(scope)?;
        let encoded =
            serde_json::to_vec(self).map_err(|error| TabletError::Encoding(error.to_string()))?;
        if encoded.len() > MAX_QUEUE_TABLET_COMMAND_BYTES {
            return Err(command_too_large(encoded.len()));
        }
        Ok(encoded)
    }

    pub fn decode(payload: &[u8], scope: &QueueTabletScope) -> TabletResult<Self> {
        if payload.len() > MAX_QUEUE_TABLET_COMMAND_BYTES {
            return Err(command_too_large(payload.len()));
        }
        let command: Self = serde_json::from_slice(payload)
            .map_err(|error| TabletError::Decoding(error.to_string()))?;
        command.validate(scope)?;
        let canonical = serde_json::to_vec(&command)
            .map_err(|error| TabletError::Encoding(error.to_string()))?;
        if canonical != payload {
            return Err(TabletError::Decoding(
                "command bytes are not in canonical Queue tablet encoding".into(),
            ));
        }
        Ok(command)
    }

    pub fn proposal_id(&self, scope: &QueueTabletScope) -> TabletResult<u64> {
        self.validate(scope)?;
        queue_proposal_id_for(scope, &self.idempotency_key)
    }

    fn validate(&self, scope: &QueueTabletScope) -> TabletResult<()> {
        scope.validate()?;
        if self.format_version != self.operation.format_version() {
            return Err(TabletError::InvalidCommand(format!(
                "operation requires format_version {}; observed {}",
                self.operation.format_version(),
                self.format_version,
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
        self.operation.validate()
    }
}

impl QueueTabletOperation {
    const fn format_version(&self) -> u16 {
        match self {
            Self::AcquireWithCredit(_) => QUEUE_TABLET_COMMAND_FORMAT_VERSION_V2,
            Self::EnqueueAdvanced(_)
            | Self::AcquireSession(_)
            | Self::RenewSessionLock(_)
            | Self::ReleaseSessionLock(_)
            | Self::Defer(_)
            | Self::ReceiveDeferred(_)
            | Self::BindDeadLetterForward(_)
            | Self::CompleteDeadLetterForward(_) => QUEUE_TABLET_COMMAND_FORMAT_VERSION,
            _ => QUEUE_TABLET_COMMAND_FORMAT_VERSION_V1,
        }
    }

    fn validate(&self) -> TabletResult<()> {
        self.validate_fields()?;
        if self.partition() != 0 {
            return Err(TabletError::InvalidCommand(
                "Queue tablet supports only partition 0".into(),
            ));
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the exhaustive operation match keeps versioned Queue validation auditable"
    )]
    fn validate_fields(&self) -> TabletResult<()> {
        match self {
            Self::Enqueue(command) => {
                command.envelope.validate()?;
                validate_required_bounded(
                    "envelope.id",
                    &command.envelope.id,
                    MAX_QUEUE_MESSAGE_ID_BYTES,
                )?;
            }
            Self::Acquire(command) => {
                validate_acquire(
                    &command.consumer,
                    command.consumer_epoch,
                    command.max_messages,
                    "max_messages",
                    command.visibility_timeout_ms,
                )?;
            }
            Self::AcquireWithCredit(command) => {
                validate_acquire(
                    &command.consumer,
                    command.consumer_epoch,
                    command.credit,
                    "credit",
                    command.visibility_timeout_ms,
                )?;
                if !(1..=MAX_QUEUE_CONSUMER_IN_FLIGHT).contains(&command.max_in_flight) {
                    return Err(TabletError::InvalidCommand(format!(
                        "max_in_flight must be between 1 and {MAX_QUEUE_CONSUMER_IN_FLIGHT}"
                    )));
                }
            }
            Self::Acknowledge(command) => {
                validate_settlement(
                    &command.consumer,
                    command.consumer_epoch,
                    &command.lease_token,
                )?;
            }
            Self::ExtendLease(command) => {
                validate_settlement(
                    &command.consumer,
                    command.consumer_epoch,
                    &command.lease_token,
                )?;
                if command.extension_ms == 0 {
                    return Err(TabletError::InvalidCommand(
                        "extension_ms must be greater than zero".into(),
                    ));
                }
            }
            Self::Release(command) => {
                validate_settlement(
                    &command.consumer,
                    command.consumer_epoch,
                    &command.lease_token,
                )?;
                if let Some(reason) = &command.reason {
                    validate_reason(reason)?;
                }
            }
            Self::Nack(command) => {
                validate_settlement(
                    &command.consumer,
                    command.consumer_epoch,
                    &command.lease_token,
                )?;
                validate_reason(&command.reason)?;
            }
            Self::Reject(command) => {
                validate_settlement(
                    &command.consumer,
                    command.consumer_epoch,
                    &command.lease_token,
                )?;
                validate_reason(&command.reason)?;
            }
            Self::Redrive(command) => {
                validate_required_bounded(
                    "message_id",
                    &command.message_id,
                    MAX_QUEUE_MESSAGE_ID_BYTES,
                )?;
                if command.dead_letter_history_id == 0 {
                    return Err(TabletError::InvalidCommand(
                        "dead_letter_history_id must be non-zero".into(),
                    ));
                }
            }
            Self::Maintain(_) => {}
            Self::EnqueueAdvanced(command) => validate_advanced_enqueue(command)?,
            Self::AcquireSession(command) => validate_session_acquire(command)?,
            Self::RenewSessionLock(command) => validate_session_renew(command)?,
            Self::ReleaseSessionLock(command) => validate_session_release(command)?,
            Self::Defer(command) => validate_defer(command)?,
            Self::ReceiveDeferred(command) => validate_receive_deferred(command)?,
            Self::BindDeadLetterForward(command) => {
                validate_dead_letter_forward(command.dead_letter_history_id, &command.destination)?;
            }
            Self::CompleteDeadLetterForward(command) => {
                validate_dead_letter_forward(command.dead_letter_history_id, &command.destination)?;
                validate_required_bounded(
                    "target_message_id",
                    &command.target_message_id,
                    MAX_QUEUE_MESSAGE_ID_BYTES,
                )?;
            }
        }
        Ok(())
    }

    const fn partition(&self) -> u32 {
        match self {
            Self::Enqueue(command) => command.partition,
            Self::Acquire(command) => command.partition,
            Self::AcquireWithCredit(command) => command.partition,
            Self::Acknowledge(command) => command.partition,
            Self::ExtendLease(command) => command.partition,
            Self::Release(command) => command.partition,
            Self::Nack(command) => command.partition,
            Self::Reject(command) => command.partition,
            Self::Redrive(command) => command.partition,
            Self::Maintain(command) => command.partition,
            Self::EnqueueAdvanced(command) => command.partition,
            Self::AcquireSession(command) => command.partition,
            Self::RenewSessionLock(command) => command.partition,
            Self::ReleaseSessionLock(command) => command.partition,
            Self::Defer(command) => command.partition,
            Self::ReceiveDeferred(command) => command.partition,
            Self::BindDeadLetterForward(command) => command.partition,
            Self::CompleteDeadLetterForward(command) => command.partition,
        }
    }
}

pub fn queue_proposal_id_for(scope: &QueueTabletScope, idempotency_key: &str) -> TabletResult<u64> {
    proposal_id_from_domain(
        b"epoch/queue-tablet/proposal-id/v1\0",
        scope,
        idempotency_key,
    )
}

fn command_too_large(length: usize) -> TabletError {
    TabletError::InvalidCommand(format!(
        "encoded command is {length} bytes; maximum is {MAX_QUEUE_TABLET_COMMAND_BYTES}"
    ))
}

fn validate_consumer(consumer: &str, consumer_epoch: u64) -> TabletResult<()> {
    validate_consumer_name(consumer)?;
    if consumer_epoch == 0 {
        return Err(TabletError::InvalidCommand(
            "consumer_epoch must be non-zero".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_consumer_name(consumer: &str) -> TabletResult<()> {
    validate_required_bounded("consumer", consumer, MAX_QUEUE_CONSUMER_BYTES)
}

fn validate_acquire(
    consumer: &str,
    consumer_epoch: u64,
    credit: u16,
    credit_field: &str,
    visibility_timeout_ms: Option<u64>,
) -> TabletResult<()> {
    validate_consumer(consumer, consumer_epoch)?;
    if !(1..=MAX_QUEUE_ACQUIRE_BATCH_SIZE).contains(&credit) {
        return Err(TabletError::InvalidCommand(format!(
            "{credit_field} must be between 1 and {MAX_QUEUE_ACQUIRE_BATCH_SIZE}"
        )));
    }
    if visibility_timeout_ms == Some(0) {
        return Err(TabletError::InvalidCommand(
            "visibility_timeout_ms must be greater than zero".into(),
        ));
    }
    Ok(())
}

fn validate_settlement(consumer: &str, consumer_epoch: u64, token: &str) -> TabletResult<()> {
    validate_consumer(consumer, consumer_epoch)?;
    validate_required_bounded("lease_token", token, MAX_FENCED_LEASE_TOKEN_BYTES)
}

fn validate_reason(reason: &str) -> TabletResult<()> {
    validate_required_bounded("reason", reason, MAX_QUEUE_REASON_BYTES)
}

fn validate_advanced_enqueue(command: &QueueAdvancedEnqueueCommand) -> TabletResult<()> {
    command.ingress.envelope.validate()?;
    validate_required_bounded(
        "envelope.id",
        &command.ingress.envelope.id,
        MAX_QUEUE_MESSAGE_ID_BYTES,
    )?;
    validate_optional_advanced_identifier("session_id", command.ingress.session_id.as_deref())?;
    validate_optional_advanced_identifier(
        "correlation_id",
        command.ingress.correlation_id.as_deref(),
    )?;
    validate_optional_advanced_identifier("reply_to", command.ingress.reply_to.as_deref())
}

fn validate_session_acquire(command: &QueueSessionAcquireCommand) -> TabletResult<()> {
    validate_required_bounded(
        "session_id",
        &command.session_id,
        MAX_QUEUE_ADVANCED_IDENTIFIER_BYTES,
    )?;
    validate_acquire(
        &command.consumer,
        command.consumer_epoch,
        command.credit,
        "credit",
        command.visibility_timeout_ms,
    )?;
    if !(1..=MAX_QUEUE_CONSUMER_IN_FLIGHT).contains(&command.max_in_flight) {
        return Err(TabletError::InvalidCommand(format!(
            "max_in_flight must be between 1 and {MAX_QUEUE_CONSUMER_IN_FLIGHT}"
        )));
    }
    if let Some(token) = &command.session_lock_token {
        validate_required_bounded("session_lock_token", token, 4 * 1024)?;
    }
    Ok(())
}

fn validate_session_renew(command: &QueueSessionLockRenewCommand) -> TabletResult<()> {
    validate_consumer(&command.consumer, command.consumer_epoch)?;
    validate_required_bounded("session_lock_token", &command.session_lock_token, 4 * 1024)?;
    if command.extension_ms == 0 {
        return Err(TabletError::InvalidCommand(
            "extension_ms must be greater than zero".into(),
        ));
    }
    Ok(())
}

fn validate_session_release(command: &QueueSessionLockReleaseCommand) -> TabletResult<()> {
    validate_consumer(&command.consumer, command.consumer_epoch)?;
    validate_required_bounded("session_lock_token", &command.session_lock_token, 4 * 1024)
}

fn validate_defer(command: &QueueDeferCommand) -> TabletResult<()> {
    validate_settlement(
        &command.consumer,
        command.consumer_epoch,
        &command.lease_token,
    )?;
    validate_required_bounded("reason", &command.reason, MAX_QUEUE_DEFER_REASON_BYTES)
}

fn validate_receive_deferred(command: &QueueReceiveDeferredCommand) -> TabletResult<()> {
    validate_consumer(&command.consumer, command.consumer_epoch)?;
    validate_required_bounded(
        "message_id",
        &command.message_id,
        MAX_QUEUE_MESSAGE_ID_BYTES,
    )?;
    if command.visibility_timeout_ms == Some(0) {
        return Err(TabletError::InvalidCommand(
            "visibility_timeout_ms must be greater than zero".into(),
        ));
    }
    Ok(())
}

fn validate_optional_advanced_identifier(field: &str, value: Option<&str>) -> TabletResult<()> {
    if let Some(value) = value {
        validate_required_bounded(field, value, MAX_QUEUE_ADVANCED_IDENTIFIER_BYTES)?;
        if value.chars().any(char::is_control) {
            return Err(TabletError::InvalidCommand(format!(
                "{field} cannot contain control characters"
            )));
        }
    }
    Ok(())
}

fn validate_dead_letter_forward(
    history_id: u64,
    destination: &EpochTargetDestination,
) -> TabletResult<()> {
    if history_id == 0 {
        return Err(TabletError::InvalidCommand(
            "dead_letter_history_id must be non-zero".into(),
        ));
    }
    destination.validate()?;
    if destination.kind != epoch_bus::EpochTargetKind::Queue || destination.shard_index != 0 {
        return Err(TabletError::InvalidCommand(
            "dead-letter forwarding requires Queue partition 0".into(),
        ));
    }
    Ok(())
}

fn validate_required_bounded(field: &str, value: &str, maximum: usize) -> TabletResult<()> {
    if value.trim().is_empty() {
        return Err(TabletError::InvalidCommand(format!("{field} is required")));
    }
    if value.len() > maximum {
        return Err(TabletError::InvalidCommand(format!(
            "{field} is {} bytes; maximum is {maximum}",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(TabletError::InvalidCommand(format!(
            "{field} cannot contain control characters"
        )));
    }
    Ok(())
}
