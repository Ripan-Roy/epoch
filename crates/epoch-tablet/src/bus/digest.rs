//! Versioned Event Bus tablet state, transition, and delivery-plan digests.

use epoch_bus::RoutedDelivery;
use epoch_core::{EpochError, EpochResult};
use sha2::{Digest, Sha256};

use super::{BusTabletOutcome, BusTabletScope};
use crate::common::hash_length_prefixed;
use crate::{CommittedCommand, TabletError, TabletResult};

pub(super) fn initial_state_digest(
    scope: &BusTabletScope,
    business_state_digest: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/event-bus-tablet/state/v2\0");
    hasher.update(scope.tablet_id.to_be_bytes());
    hasher.update(scope.tablet_epoch.to_be_bytes());
    hash_length_prefixed(&mut hasher, scope.resource.as_bytes());
    hasher.update(business_state_digest);
    hasher.finalize().into()
}

pub(super) fn transition_digest(
    previous: [u8; 32],
    committed: CommittedCommand<'_>,
    payload_digest: [u8; 32],
    business_state_digest: [u8; 32],
    applied_at_ms: u64,
    outcome: &BusTabletOutcome,
) -> TabletResult<[u8; 32]> {
    let outcome =
        serde_json::to_vec(outcome).map_err(|error| TabletError::Encoding(error.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/event-bus-tablet/state-transition/v2\0");
    hasher.update(previous);
    hasher.update(committed.proposal_id.to_be_bytes());
    hasher.update(committed.term.to_be_bytes());
    hasher.update(committed.log_index.to_be_bytes());
    hasher.update(payload_digest);
    hasher.update(business_state_digest);
    hasher.update(applied_at_ms.to_be_bytes());
    hash_length_prefixed(&mut hasher, &outcome);
    Ok(hasher.finalize().into())
}

pub(super) fn delivery_plan_digest(deliveries: &[RoutedDelivery]) -> EpochResult<String> {
    let encoded =
        serde_json::to_vec(deliveries).map_err(|error| EpochError::Internal(error.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/event-bus-tablet/delivery-plan/v2\0");
    hasher.update(
        u64::try_from(encoded.len())
            .map_err(|_| EpochError::Internal("delivery plan exceeds u64 bytes".into()))?
            .to_be_bytes(),
    );
    hasher.update(encoded);
    let digest: [u8; 32] = hasher.finalize().into();
    Ok(lower_hex(&digest))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
