//! Profile-neutral tablet command and application primitives.

use epoch_core::{EpochError, validate_resource_name};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;

pub type TabletResult<T> = Result<T, TabletError>;

/// Fail-closed errors shared by typed tablet command codecs and state machines.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TabletError {
    #[error("invalid tablet command: {0}")]
    InvalidCommand(String),
    #[error("tablet command targets group {observed}; expected {expected}")]
    GroupMismatch { expected: u64, observed: u64 },
    #[error("tablet command epoch {observed} was fenced by epoch {expected}")]
    FencedEpoch { expected: u64, observed: u64 },
    #[error("tablet command {proposal_id} conflicts with its committed payload")]
    ConflictingCommand { proposal_id: u64 },
    #[error("committed tablet commands are out of order: index {observed} follows {previous}")]
    CommitOrder { previous: u64, observed: u64 },
    #[error("tablet command could not be encoded: {0}")]
    Encoding(String),
    #[error("tablet command could not be decoded: {0}")]
    Decoding(String),
    #[error(transparent)]
    Profile(#[from] EpochError),
}

/// Stable identity and fencing epoch shared by profile-specific tablets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTabletScope {
    pub tablet_id: u64,
    pub tablet_epoch: u64,
    pub resource: String,
}

impl StreamTabletScope {
    pub fn new(
        tablet_id: u64,
        tablet_epoch: u64,
        resource: impl Into<String>,
    ) -> TabletResult<Self> {
        let scope = Self {
            tablet_id,
            tablet_epoch,
            resource: resource.into(),
        };
        scope.validate()?;
        Ok(scope)
    }

    pub(crate) fn validate(&self) -> TabletResult<()> {
        if self.tablet_id == 0 {
            return Err(TabletError::InvalidCommand(
                "tablet_id must be non-zero".into(),
            ));
        }
        if self.tablet_epoch == 0 {
            return Err(TabletError::InvalidCommand(
                "tablet_epoch must be non-zero".into(),
            ));
        }
        validate_resource_name(&self.resource)?;
        Ok(())
    }
}

/// Profile-neutral name for the original Stream tablet scope contract.
pub use StreamTabletScope as TabletScope;

/// Consensus metadata and canonical payload presented to a profile applier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedCommand<'a> {
    pub group_id: u64,
    pub group_epoch: u64,
    pub proposal_id: u64,
    pub term: u64,
    pub log_index: u64,
    pub payload: &'a [u8],
}

/// Bounded durability evidence supported by the current fixed-voter milestone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TabletWriteEvidence {
    FixedVoterMajorityPersisted,
}

/// Exact command identity retained with a deterministic applied result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedCommandMetadata {
    pub proposal_id: u64,
    pub term: u64,
    pub log_index: u64,
    pub payload_digest: [u8; 32],
}

impl AppliedCommandMetadata {
    /// Captures the complete consensus identity and SHA-256 payload digest.
    pub fn from_committed(committed: CommittedCommand<'_>) -> Self {
        Self {
            proposal_id: committed.proposal_id,
            term: committed.term,
            log_index: committed.log_index,
            payload_digest: Sha256::digest(committed.payload).into(),
        }
    }

    /// Rejects any attempt to reuse a proposal ID with different committed data.
    pub fn validate_exact(self, committed: CommittedCommand<'_>) -> TabletResult<()> {
        let observed = Self::from_committed(committed);
        if self == observed {
            Ok(())
        } else {
            Err(TabletError::ConflictingCommand {
                proposal_id: committed.proposal_id,
            })
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AppliedCommand<Receipt> {
    pub metadata: AppliedCommandMetadata,
    pub receipt: Receipt,
}

pub(crate) fn validate_committed_command_scope(
    scope: &TabletScope,
    committed: CommittedCommand<'_>,
) -> TabletResult<()> {
    if committed.group_id != scope.tablet_id {
        return Err(TabletError::GroupMismatch {
            expected: scope.tablet_id,
            observed: committed.group_id,
        });
    }
    if committed.group_epoch != scope.tablet_epoch {
        return Err(TabletError::FencedEpoch {
            expected: scope.tablet_epoch,
            observed: committed.group_epoch,
        });
    }
    if committed.proposal_id == 0 || committed.term == 0 || committed.log_index == 0 {
        return Err(TabletError::InvalidCommand(
            "committed proposal_id, term, and log_index must be non-zero".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_idempotency_key(value: &str) -> TabletResult<()> {
    let length = value.len();
    if value.trim().is_empty() {
        return Err(TabletError::InvalidCommand(
            "idempotency_key is required".into(),
        ));
    }
    if length > MAX_IDEMPOTENCY_KEY_BYTES {
        return Err(TabletError::InvalidCommand(format!(
            "idempotency_key is {length} bytes; maximum is {MAX_IDEMPOTENCY_KEY_BYTES}"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(TabletError::InvalidCommand(
            "idempotency_key cannot contain control characters".into(),
        ));
    }
    Ok(())
}

pub(crate) fn proposal_id_from_domain(
    domain: &[u8],
    scope: &TabletScope,
    idempotency_key: &str,
) -> TabletResult<u64> {
    scope.validate()?;
    validate_idempotency_key(idempotency_key)?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(scope.tablet_id.to_be_bytes());
    hasher.update(scope.tablet_epoch.to_be_bytes());
    hash_length_prefixed(&mut hasher, scope.resource.as_bytes());
    hash_length_prefixed(&mut hasher, idempotency_key.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let proposal_id = u64::from_be_bytes(bytes);
    Ok(if proposal_id == 0 { 1 } else { proposal_id })
}

pub(crate) fn hash_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde serialize_with requires a shared reference"
)]
pub(crate) fn serialize_u64_as_decimal<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}
