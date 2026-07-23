//! Versioned Queue tablet commands and their strict canonical codec.

use epoch_core::EventEnvelope;
use epoch_queue::MAX_FENCED_LEASE_TOKEN_BYTES;
use serde::{Deserialize, Serialize};

use crate::common::{proposal_id_from_domain, validate_idempotency_key};
use crate::{TabletError, TabletResult, TabletScope};

pub const QUEUE_TABLET_COMMAND_FORMAT_VERSION: u16 = 1;
pub const MAX_QUEUE_TABLET_COMMAND_BYTES: usize = 512 * 1024;
pub const MAX_QUEUE_ACQUIRE_BATCH_SIZE: u16 = 100;
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
    Acknowledge(QueueAcknowledgeCommand),
    ExtendLease(QueueExtendLeaseCommand),
    Release(QueueReleaseCommand),
    Nack(QueueNackCommand),
    Reject(QueueRejectCommand),
    Redrive(QueueRedriveCommand),
    Maintain(QueueMaintainCommand),
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

impl QueueTabletCommand {
    pub fn new(
        scope: &QueueTabletScope,
        idempotency_key: impl Into<String>,
        applied_at_ms: u64,
        operation: QueueTabletOperation,
    ) -> TabletResult<Self> {
        let command = Self {
            format_version: QUEUE_TABLET_COMMAND_FORMAT_VERSION,
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
                "command bytes are not in canonical v1 encoding".into(),
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
        if self.format_version != QUEUE_TABLET_COMMAND_FORMAT_VERSION {
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
        self.operation.validate()
    }
}

impl QueueTabletOperation {
    fn validate(&self) -> TabletResult<()> {
        let partition = match self {
            Self::Enqueue(command) => {
                command.envelope.validate()?;
                validate_required_bounded(
                    "envelope.id",
                    &command.envelope.id,
                    MAX_QUEUE_MESSAGE_ID_BYTES,
                )?;
                command.partition
            }
            Self::Acquire(command) => {
                validate_consumer(&command.consumer, command.consumer_epoch)?;
                if !(1..=MAX_QUEUE_ACQUIRE_BATCH_SIZE).contains(&command.max_messages) {
                    return Err(TabletError::InvalidCommand(format!(
                        "max_messages must be between 1 and {MAX_QUEUE_ACQUIRE_BATCH_SIZE}"
                    )));
                }
                if command.visibility_timeout_ms == Some(0) {
                    return Err(TabletError::InvalidCommand(
                        "visibility_timeout_ms must be greater than zero".into(),
                    ));
                }
                command.partition
            }
            Self::Acknowledge(command) => {
                validate_settlement(
                    &command.consumer,
                    command.consumer_epoch,
                    &command.lease_token,
                )?;
                command.partition
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
                command.partition
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
                command.partition
            }
            Self::Nack(command) => {
                validate_settlement(
                    &command.consumer,
                    command.consumer_epoch,
                    &command.lease_token,
                )?;
                validate_reason(&command.reason)?;
                command.partition
            }
            Self::Reject(command) => {
                validate_settlement(
                    &command.consumer,
                    command.consumer_epoch,
                    &command.lease_token,
                )?;
                validate_reason(&command.reason)?;
                command.partition
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
                command.partition
            }
            Self::Maintain(command) => command.partition,
        };
        if partition != 0 {
            return Err(TabletError::InvalidCommand(
                "Queue tablet v1 supports only partition 0".into(),
            ));
        }
        Ok(())
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
    validate_required_bounded("consumer", consumer, MAX_QUEUE_CONSUMER_BYTES)?;
    if consumer_epoch == 0 {
        return Err(TabletError::InvalidCommand(
            "consumer_epoch must be non-zero".into(),
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
