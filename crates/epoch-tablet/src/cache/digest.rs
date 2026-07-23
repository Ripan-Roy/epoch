//! Versioned Cache tablet state and outcome digest encoding.

use sha2::{Digest, Sha256};

use super::CacheTabletBusinessState;
use super::command::CacheTabletScope;
use super::model::CacheTabletOutcome;
use crate::common::hash_length_prefixed;
use crate::{CommittedCommand, TabletError, TabletResult};

pub(super) fn initial_state_digest(
    scope: &CacheTabletScope,
    max_entries: usize,
    default_ttl_ms: Option<u64>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/cache-tablet/state/v1\0");
    hasher.update(scope.tablet_id.to_be_bytes());
    hasher.update(scope.tablet_epoch.to_be_bytes());
    hash_length_prefixed(&mut hasher, scope.resource.as_bytes());
    hasher.update(u64::try_from(max_entries).unwrap_or(u64::MAX).to_be_bytes());
    hash_optional_u64(&mut hasher, default_ttl_ms);
    hasher.finalize().into()
}

pub(super) fn encode_auxiliary_state(
    state: &CacheTabletBusinessState,
    last_applied_time_ms: u64,
) -> TabletResult<Vec<u8>> {
    serde_json::to_vec(&(
        &state.active_owner_epochs,
        &state.locks,
        last_applied_time_ms,
    ))
    .map_err(|error| TabletError::Encoding(error.to_string()))
}

pub(super) fn transition_digest(
    previous: [u8; 32],
    committed: CommittedCommand<'_>,
    payload_digest: [u8; 32],
    cache_state_digest: [u8; 32],
    auxiliary_state: &[u8],
    outcome: &CacheTabletOutcome,
) -> TabletResult<[u8; 32]> {
    let outcome =
        serde_json::to_vec(outcome).map_err(|error| TabletError::Encoding(error.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/cache-tablet/state-transition/v1\0");
    hasher.update(previous);
    hasher.update(committed.proposal_id.to_be_bytes());
    hasher.update(committed.term.to_be_bytes());
    hasher.update(committed.log_index.to_be_bytes());
    hasher.update(payload_digest);
    hasher.update(cache_state_digest);
    hash_length_prefixed(&mut hasher, auxiliary_state);
    hash_length_prefixed(&mut hasher, &outcome);
    Ok(hasher.finalize().into())
}

fn hash_optional_u64(hasher: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.to_be_bytes());
        }
        None => hasher.update([0]),
    }
}
