//! Versioned Queue tablet state and outcome digest encoding.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use super::QueueTabletBusinessState;
use super::command::QueueTabletScope;
use super::model::{
    QueueTabletEnvelope, QueueTabletOperationResult, QueueTabletOutcome, QueueTabletRejectionCode,
};
use crate::common::hash_length_prefixed;
use crate::{CommittedCommand, TabletError, TabletResult};

pub(super) fn initial_state_digest(scope: &QueueTabletScope, config_bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/queue-tablet/state/v1\0");
    hasher.update(scope.tablet_id.to_be_bytes());
    hasher.update(scope.tablet_epoch.to_be_bytes());
    hash_length_prefixed(&mut hasher, scope.resource.as_bytes());
    hash_length_prefixed(&mut hasher, config_bytes);
    hasher.update(0_u64.to_be_bytes());
    hasher.finalize().into()
}

pub(super) fn encode_auxiliary_state(
    state: &QueueTabletBusinessState,
    last_applied_time_ms: u64,
) -> TabletResult<Vec<u8>> {
    serde_json::to_vec(&(
        &state.consumer_epochs,
        &state.dead_letter_history,
        &state.active_dead_letters,
        state.next_dead_letter_history_id,
        &state.redrive_history,
        state.next_redrive_history_id,
        last_applied_time_ms,
    ))
    .map_err(|error| TabletError::Encoding(error.to_string()))
}

pub(super) fn transition_digest(
    previous: [u8; 32],
    committed: CommittedCommand<'_>,
    payload_digest: [u8; 32],
    queue_checksum: u32,
    auxiliary_state: &[u8],
    outcome: &QueueTabletOutcome,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/queue-tablet/state-transition/v1\0");
    hasher.update(previous);
    hasher.update(committed.proposal_id.to_be_bytes());
    hasher.update(committed.term.to_be_bytes());
    hasher.update(committed.log_index.to_be_bytes());
    hasher.update(payload_digest);
    hasher.update(queue_checksum.to_be_bytes());
    hash_length_prefixed(&mut hasher, auxiliary_state);
    hash_outcome_v1(&mut hasher, outcome);
    hasher.finalize().into()
}

fn hash_outcome_v1(hasher: &mut Sha256, outcome: &QueueTabletOutcome) {
    match outcome {
        QueueTabletOutcome::Applied { result } => {
            hasher.update([0]);
            hash_operation_result_v1(hasher, result);
        }
        QueueTabletOutcome::Rejected { code, detail } => {
            hasher.update([1]);
            let tag = match code {
                QueueTabletRejectionCode::AlreadyExists => 0,
                QueueTabletRejectionCode::NotFound => 1,
                QueueTabletRejectionCode::InvalidArgument => 2,
                QueueTabletRejectionCode::Conflict => 3,
                QueueTabletRejectionCode::Fenced => 4,
                QueueTabletRejectionCode::Capacity => 5,
                QueueTabletRejectionCode::Unavailable => 6,
            };
            hasher.update([tag]);
            hash_length_prefixed(hasher, detail.as_bytes());
        }
    }
}

fn hash_operation_result_v1(hasher: &mut Sha256, result: &QueueTabletOperationResult) {
    match result {
        QueueTabletOperationResult::Enqueued {
            message_id,
            duplicate,
        } => {
            hasher.update([0, u8::from(*duplicate)]);
            hash_length_prefixed(hasher, message_id.as_bytes());
        }
        QueueTabletOperationResult::Acquired {
            deliveries,
            new_dead_letter_history_ids,
        } => {
            hasher.update([1]);
            hash_collection_length(hasher, deliveries.len());
            for delivery in deliveries {
                hash_length_prefixed(hasher, delivery.message_id.as_bytes());
                hash_envelope_v1(hasher, &delivery.envelope);
                hasher.update(delivery.attempt.to_be_bytes());
                hash_length_prefixed(hasher, delivery.lease_token.as_bytes());
                hasher.update(delivery.lease_deadline_ms.to_be_bytes());
            }
            hash_strings_v1(hasher, new_dead_letter_history_ids);
        }
        QueueTabletOperationResult::Acknowledged { message_id } => {
            hasher.update([2]);
            hash_length_prefixed(hasher, message_id.as_bytes());
        }
        QueueTabletOperationResult::LeaseExtended {
            message_id,
            lease_token,
            lease_deadline_ms,
        } => {
            hasher.update([3]);
            hash_length_prefixed(hasher, message_id.as_bytes());
            hash_length_prefixed(hasher, lease_token.as_bytes());
            hasher.update(lease_deadline_ms.to_be_bytes());
        }
        QueueTabletOperationResult::Released {
            message_id,
            dead_letter_history_id,
        } => {
            hasher.update([4]);
            hash_length_prefixed(hasher, message_id.as_bytes());
            hash_optional_u64_v1(hasher, *dead_letter_history_id);
        }
        QueueTabletOperationResult::Nacked {
            message_id,
            dead_letter_history_id,
        } => {
            hasher.update([5]);
            hash_length_prefixed(hasher, message_id.as_bytes());
            hash_optional_u64_v1(hasher, *dead_letter_history_id);
        }
        QueueTabletOperationResult::DeadLettered {
            message_id,
            dead_letter_history_id,
        } => {
            hasher.update([6]);
            hash_length_prefixed(hasher, message_id.as_bytes());
            hasher.update(dead_letter_history_id.to_be_bytes());
        }
        QueueTabletOperationResult::Redriven {
            message_id,
            dead_letter_history_id,
            redrive_history_id,
        } => {
            hasher.update([7]);
            hash_length_prefixed(hasher, message_id.as_bytes());
            hasher.update(dead_letter_history_id.to_be_bytes());
            hasher.update(redrive_history_id.to_be_bytes());
        }
        QueueTabletOperationResult::Maintained {
            counts,
            new_dead_letter_history_ids,
        } => {
            hasher.update([8]);
            hasher.update(counts.ready.to_be_bytes());
            hasher.update(counts.scheduled.to_be_bytes());
            hasher.update(counts.in_flight.to_be_bytes());
            hasher.update(counts.acknowledged.to_be_bytes());
            hasher.update(counts.expired.to_be_bytes());
            hasher.update(counts.dead_lettered.to_be_bytes());
            hash_strings_v1(hasher, new_dead_letter_history_ids);
        }
    }
}

fn hash_envelope_v1(hasher: &mut Sha256, envelope: &QueueTabletEnvelope) {
    hash_length_prefixed(hasher, envelope.id.as_bytes());
    hash_length_prefixed(hasher, envelope.source.as_bytes());
    hash_length_prefixed(hasher, envelope.event_type.as_bytes());
    hash_optional_string_v1(hasher, envelope.subject.as_deref());
    hasher.update(envelope.time_ms.to_be_bytes());
    hash_optional_string_v1(hasher, envelope.key.as_deref());
    hash_string_map_v1(hasher, &envelope.headers);
    hash_length_prefixed(hasher, envelope.content_type.as_bytes());
    hash_optional_string_v1(hasher, envelope.schema_ref.as_deref());
    hash_optional_string_v1(hasher, envelope.traceparent.as_deref());
    hash_json_value_v1(hasher, &envelope.payload);
    hash_optional_u64_v1(hasher, envelope.deliver_at_ms);
    hash_optional_u64_v1(hasher, envelope.ttl_ms);
    hasher.update([envelope.priority]);
    hash_optional_string_v1(hasher, envelope.dedupe_id.as_deref());
    hash_optional_string_v1(hasher, envelope.transaction_id.as_deref());
    hash_collection_length(hasher, envelope.extensions.len());
    for (key, value) in &envelope.extensions {
        hash_length_prefixed(hasher, key.as_bytes());
        hash_json_value_v1(hasher, value);
    }
}

fn hash_json_value_v1(hasher: &mut Sha256, value: &serde_json::Value) {
    match value {
        serde_json::Value::Null => hasher.update([0]),
        serde_json::Value::Bool(value) => hasher.update([1, u8::from(*value)]),
        serde_json::Value::Number(value) => {
            hasher.update([2]);
            hash_length_prefixed(hasher, value.to_string().as_bytes());
        }
        serde_json::Value::String(value) => {
            hasher.update([3]);
            hash_length_prefixed(hasher, value.as_bytes());
        }
        serde_json::Value::Array(values) => {
            hasher.update([4]);
            hash_collection_length(hasher, values.len());
            for value in values {
                hash_json_value_v1(hasher, value);
            }
        }
        serde_json::Value::Object(values) => {
            hasher.update([5]);
            hash_collection_length(hasher, values.len());
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            for (key, value) in entries {
                hash_length_prefixed(hasher, key.as_bytes());
                hash_json_value_v1(hasher, value);
            }
        }
    }
}

fn hash_string_map_v1(hasher: &mut Sha256, values: &BTreeMap<String, String>) {
    hash_collection_length(hasher, values.len());
    for (key, value) in values {
        hash_length_prefixed(hasher, key.as_bytes());
        hash_length_prefixed(hasher, value.as_bytes());
    }
}

fn hash_strings_v1(hasher: &mut Sha256, values: &[String]) {
    hash_collection_length(hasher, values.len());
    for value in values {
        hash_length_prefixed(hasher, value.as_bytes());
    }
}

fn hash_collection_length(hasher: &mut Sha256, length: usize) {
    hasher.update(u64::try_from(length).unwrap_or(u64::MAX).to_be_bytes());
}

fn hash_optional_string_v1(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_length_prefixed(hasher, value.as_bytes());
        }
        None => hasher.update([0]),
    }
}

fn hash_optional_u64_v1(hasher: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.to_be_bytes());
        }
        None => hasher.update([0]),
    }
}
