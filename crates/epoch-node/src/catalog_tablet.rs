//! Replicated regional catalog state machine boundary.
//!
//! Catalog commands are decoded and applied only by the consensus actor after
//! commit. The service is intentionally independent from HTTP and group
//! supervision so the same authoritative state can drive standalone tests,
//! node-local reconciliation, and the future regional administration API.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use epoch_catalog::{
    Catalog, CatalogChangePage, CatalogCommand, CatalogError, CatalogMutation, CatalogOperation,
    ControlLease, ManagedResourceRecord, ResourceRecord,
};
use epoch_consensus::{ApplicationSnapshot, CommittedProposal, LogIndex};
use serde::{Deserialize, Serialize};

use crate::{
    consensus::CommittedProposalApplier,
    tablet_http::{deserialize_u64_from_number_or_decimal, hex_digest, serialize_u64_as_decimal},
};

const CATALOG_APPLICATION_SNAPSHOT_FORMAT_ID: [u8; 16] = *b"CATALOG_STATE_V1";
const LEGACY_CATALOG_APPLICATION_SNAPSHOT_VERSION: u16 = 1;
const CATALOG_APPLICATION_SNAPSHOT_VERSION: u16 = 2;
const CATALOG_APPLICATION_SNAPSHOT_MAGIC: [u8; 4] = *b"ECAT";
const CATALOG_APPLICATION_SNAPSHOT_HEADER_BYTES: usize = 4 + 2 + (4 * 8) + 4;
const _: () = assert!(
    epoch_catalog::MAX_CATALOG_SNAPSHOT_BYTES + CATALOG_APPLICATION_SNAPSHOT_HEADER_BYTES
        <= epoch_consensus::MAX_APPLICATION_SNAPSHOT_BYTES
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogTabletScope {
    group_id: u64,
    group_epoch: u64,
}

#[derive(Debug)]
pub enum CatalogTabletQueryError {
    Unavailable(String),
    Catalog(CatalogError),
}

impl CatalogTabletScope {
    pub fn new(group_id: u64, group_epoch: u64) -> Result<Self, String> {
        if group_id == 0 {
            return Err("catalog group ID must be non-zero".into());
        }
        if group_epoch == 0 {
            return Err("catalog group epoch must be non-zero".into());
        }
        Ok(Self {
            group_id,
            group_epoch,
        })
    }

    pub const fn group_id(self) -> u64 {
        self.group_id
    }

    pub const fn group_epoch(self) -> u64 {
        self.group_epoch
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogTabletReceipt {
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub proposal_id: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub term: u64,
    #[serde(
        serialize_with = "serialize_u64_as_decimal",
        deserialize_with = "deserialize_u64_from_number_or_decimal"
    )]
    pub commit_index: u64,
    pub mutation: CatalogMutation,
    pub state_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogTabletSnapshot {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub group_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub group_epoch: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub last_applied_index: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub applied_command_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub resource_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub tablet_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub managed_resource_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub latest_change_cursor: u64,
    pub state_digest: String,
    pub resources: Vec<ResourceRecord>,
    pub managed_resources: Vec<ManagedResourceRecord>,
    pub node_allocations: BTreeMap<u64, u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_lease: Option<ControlLease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AppliedCatalogCommand {
    payload: Vec<u8>,
    receipt: CatalogTabletReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyCatalogApplicationCheckpoint {
    format_version: u16,
    group_id: u64,
    group_epoch: u64,
    checkpoint_index: u64,
    last_applied_index: u64,
    catalog_base64: String,
    applied: Vec<AppliedCatalogCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppliedCatalogCheckpoint {
    proposal_id: u64,
    command: AppliedCatalogCommand,
}

struct DecodedCatalogApplicationCheckpoint {
    catalog: Catalog,
    last_applied_index: u64,
    applied: BTreeMap<u64, AppliedCatalogCommand>,
}

#[derive(Debug)]
struct CatalogTabletState {
    catalog: Catalog,
    applied: BTreeMap<u64, AppliedCatalogCommand>,
    last_applied_index: u64,
}

impl CatalogTabletState {
    fn new(scope: CatalogTabletScope) -> Result<Self, String> {
        Ok(Self {
            catalog: Catalog::with_reserved_consensus_group(scope.group_id)
                .map_err(|error| error.to_string())?,
            applied: BTreeMap::new(),
            last_applied_index: 0,
        })
    }
}

#[derive(Debug)]
pub struct CatalogTabletService {
    scope: CatalogTabletScope,
    state: RwLock<CatalogTabletState>,
    failure: RwLock<Option<String>>,
}

impl CatalogTabletService {
    pub fn new(scope: CatalogTabletScope) -> Arc<Self> {
        let state = CatalogTabletState::new(scope)
            .expect("a validated nonzero catalog scope must reserve its consensus group");
        Arc::new(Self {
            scope,
            state: RwLock::new(state),
            failure: RwLock::new(None),
        })
    }

    pub const fn scope(&self) -> CatalogTabletScope {
        self.scope
    }

    pub fn ensure_healthy(&self) -> Result<(), String> {
        let failure = self
            .failure
            .read()
            .map_err(|_| "catalog failure lock was poisoned".to_owned())?;
        if let Some(error) = failure.as_ref() {
            Err(error.clone())
        } else {
            Ok(())
        }
    }

    pub fn receipt(&self, proposal_id: u64) -> Result<Option<CatalogTabletReceipt>, String> {
        self.ensure_healthy()?;
        self.state
            .read()
            .map_err(|_| "catalog state read lock was poisoned".to_owned())
            .map(|state| {
                state
                    .applied
                    .get(&proposal_id)
                    .map(|applied| applied.receipt.clone())
            })
    }

    /// Rebuilds a response for a deterministic proposal retained by consensus
    /// when its application receipt was intentionally omitted from a compact
    /// checkpoint. Catalog request-token outcomes remain the durable replay
    /// authority; periodic outcomes may disappear after their bounded window.
    pub fn durable_replay_receipt(
        &self,
        committed: &CommittedProposal,
    ) -> Result<Option<CatalogTabletReceipt>, String> {
        self.ensure_healthy()?;
        if committed.receipt.group_id.get() != self.scope.group_id
            || committed.receipt.group_epoch.get() != self.scope.group_epoch
        {
            return Err("catalog replay receipt has a foreign consensus scope".into());
        }
        let command =
            CatalogCommand::decode(&committed.payload).map_err(|error| error.to_string())?;
        let state = self
            .state
            .read()
            .map_err(|_| "catalog state read lock was poisoned".to_owned())?;
        let Some(operation) = state.catalog.operation(command.request_token()) else {
            return Ok(None);
        };
        Ok(Some(CatalogTabletReceipt {
            proposal_id: committed.receipt.proposal_id.get(),
            term: committed.receipt.term.get(),
            commit_index: committed.receipt.log_index.get(),
            mutation: operation.mutation.as_replayed(),
            state_digest: hex_digest(
                state
                    .catalog
                    .state_digest()
                    .map_err(|error| error.to_string())?,
            ),
        }))
    }

    pub fn snapshot(&self) -> Result<CatalogTabletSnapshot, String> {
        self.ensure_healthy()?;
        let state = self
            .state
            .read()
            .map_err(|_| "catalog state read lock was poisoned".to_owned())?;
        let applied_command_count = u64::try_from(state.applied.len())
            .map_err(|_| "catalog applied command count exceeds u64".to_owned())?;
        let resource_count = u64::try_from(state.catalog.resource_count())
            .map_err(|_| "catalog resource count exceeds u64".to_owned())?;
        let tablet_count = u64::try_from(state.catalog.tablet_count())
            .map_err(|_| "catalog tablet count exceeds u64".to_owned())?;
        let managed_resource_count = u64::try_from(state.catalog.managed_resource_count())
            .map_err(|_| "managed resource count exceeds u64".to_owned())?;
        let state_digest = state
            .catalog
            .state_digest()
            .map(hex_digest)
            .map_err(|error| error.to_string())?;
        Ok(CatalogTabletSnapshot {
            group_id: self.scope.group_id,
            group_epoch: self.scope.group_epoch,
            last_applied_index: state.last_applied_index,
            applied_command_count,
            resource_count,
            tablet_count,
            managed_resource_count,
            latest_change_cursor: state.catalog.latest_change_cursor(),
            state_digest,
            resources: state.catalog.resources().cloned().collect(),
            managed_resources: state.catalog.managed_resources().cloned().collect(),
            node_allocations: state
                .catalog
                .node_allocations()
                .map_err(|error| error.to_string())?,
            control_lease: state.catalog.control_lease().cloned(),
        })
    }

    pub fn operation(&self, request_token: &str) -> Result<Option<CatalogOperation>, String> {
        self.ensure_healthy()?;
        self.state
            .read()
            .map_err(|_| "catalog state read lock was poisoned".to_owned())
            .map(|state| state.catalog.operation(request_token))
    }

    pub fn changes_after(
        &self,
        cursor: u64,
        limit: usize,
    ) -> Result<CatalogChangePage, CatalogTabletQueryError> {
        self.ensure_healthy()
            .map_err(CatalogTabletQueryError::Unavailable)?;
        self.state
            .read()
            .map_err(|_| {
                CatalogTabletQueryError::Unavailable(
                    "catalog state read lock was poisoned".to_owned(),
                )
            })?
            .catalog
            .changes_after(cursor, limit)
            .map_err(CatalogTabletQueryError::Catalog)
    }

    /// Reads the exact catalog inventory carried by a native application
    /// checkpoint. Backup coordinators use this image instead of sampling the
    /// live service after the barrier, so concurrent catalog mutations cannot
    /// change the set of tablet checkpoints included in the artifact.
    pub(crate) fn resources_from_application_snapshot(
        scope: CatalogTabletScope,
        snapshot: &ApplicationSnapshot,
    ) -> Result<Vec<ResourceRecord>, String> {
        let checkpoint = decode_catalog_application_checkpoint(scope, snapshot)?;
        Ok(checkpoint.catalog.resources().cloned().collect())
    }

    fn fail(&self, error: impl Into<String>) -> String {
        let error = error.into();
        if let Ok(mut failure) = self.failure.write() {
            failure.get_or_insert_with(|| error.clone());
        }
        error
    }

    fn apply_one(&self, committed: &CommittedProposal) -> Result<CatalogTabletReceipt, String> {
        self.ensure_healthy()?;
        let result = self
            .state
            .write()
            .map_err(|_| "catalog state write lock was poisoned".to_owned())
            .and_then(|mut state| apply_committed(self.scope, &mut state, committed));
        result.map_err(|error| self.fail(error))
    }
}

impl CommittedProposalApplier for CatalogTabletService {
    fn replay(&self, committed: &[CommittedProposal]) -> Result<(), String> {
        let mut history = committed.to_vec();
        history.sort_by_key(|proposal| proposal.receipt.log_index.get());
        let mut rebuilt = CatalogTabletState::new(self.scope).map_err(|error| self.fail(error))?;
        for proposal in &history {
            apply_committed(self.scope, &mut rebuilt, proposal)
                .map_err(|error| self.fail(error))?;
        }
        *self
            .state
            .write()
            .map_err(|_| self.fail("catalog state write lock was poisoned"))? = rebuilt;
        Ok(())
    }

    fn apply(&self, committed: &CommittedProposal) -> Result<(), String> {
        self.apply_one(committed).map(|_| ())
    }

    fn capture_snapshot(
        &self,
        checkpoint_index: LogIndex,
        retained: &[CommittedProposal],
    ) -> Result<ApplicationSnapshot, String> {
        self.ensure_healthy()?;
        let mut state = self
            .state
            .write()
            .map_err(|_| "catalog state write lock was poisoned".to_owned())?;
        if state.last_applied_index > checkpoint_index.get() {
            return Err(format!(
                "catalog applied index {} exceeds consensus checkpoint index {}",
                state.last_applied_index, checkpoint_index
            ));
        }
        for committed in retained {
            let proposal_id = committed.receipt.proposal_id.get();
            let decoded =
                CatalogCommand::decode(&committed.payload).map_err(|error| error.to_string())?;
            let is_periodic_control = decoded.is_transient_control();
            if let Some(command) = state.applied.get(&proposal_id) {
                if command.payload != committed.payload
                    || command.receipt.term != committed.receipt.term.get()
                    || command.receipt.commit_index != committed.receipt.log_index.get()
                {
                    return Err(format!(
                        "catalog retry proposal {proposal_id} disagrees with consensus"
                    ));
                }
            } else if committed.receipt.log_index.get() > state.last_applied_index
                || (!is_periodic_control
                    && state.catalog.operation(decoded.request_token()).is_none())
            {
                return Err(format!(
                    "catalog retry proposal {proposal_id} has no durable applied result"
                ));
            }
        }
        // Keep the complete consensus retry suffix in the live process so a
        // response racing this checkpoint can still resolve its receipt. The
        // durable Catalog already owns public request-token outcomes, so the
        // application image does not duplicate the potentially 1 MiB retry
        // suffix. Internal periodic outcomes intentionally use their bounded
        // Catalog retention policy.
        let retained_ids = retained
            .iter()
            .map(|entry| entry.receipt.proposal_id.get())
            .collect::<BTreeSet<_>>();
        let catalog_bytes = state
            .catalog
            .encode_snapshot()
            .map_err(|error| error.to_string())?;
        let state_digest = state
            .catalog
            .state_digest()
            .map_err(|error| error.to_string())?;
        let payload = encode_catalog_application_checkpoint(
            self.scope,
            checkpoint_index,
            state.last_applied_index,
            &catalog_bytes,
        )?;
        let snapshot = ApplicationSnapshot::new(
            checkpoint_index,
            CATALOG_APPLICATION_SNAPSHOT_FORMAT_ID,
            CATALOG_APPLICATION_SNAPSHOT_VERSION,
            state_digest,
            payload,
        )
        .map_err(|error| error.to_string())?;
        state.applied.retain(|proposal_id, command| {
            command.receipt.commit_index > checkpoint_index.get()
                || retained_ids.contains(proposal_id)
        });
        Ok(snapshot)
    }

    fn install_snapshot(&self, snapshot: &ApplicationSnapshot) -> Result<(), String> {
        self.ensure_healthy()?;
        let result: Result<CatalogTabletState, String> = (|| {
            let checkpoint = decode_catalog_application_checkpoint(self.scope, snapshot)?;
            Ok(CatalogTabletState {
                catalog: checkpoint.catalog,
                applied: checkpoint.applied,
                last_applied_index: checkpoint.last_applied_index,
            })
        })();
        match result {
            Ok(restored) => {
                *self
                    .state
                    .write()
                    .map_err(|_| self.fail("catalog state write lock was poisoned"))? = restored;
                Ok(())
            }
            Err(error) => Err(self.fail(error)),
        }
    }

    fn restore_checkpoint_receipts(&self, retained: &[CommittedProposal]) -> Result<(), String> {
        self.ensure_healthy()?;
        let result = self
            .state
            .write()
            .map_err(|_| "catalog state write lock was poisoned".to_owned())
            .and_then(|mut state| {
                restore_catalog_checkpoint_receipts(self.scope, &mut state, retained)
            });
        result.map_err(|error| self.fail(error))
    }

    fn supports_native_snapshots(&self) -> bool {
        true
    }
}

fn restore_catalog_checkpoint_receipts(
    scope: CatalogTabletScope,
    state: &mut CatalogTabletState,
    retained: &[CommittedProposal],
) -> Result<(), String> {
    let mut retained = retained.to_vec();
    retained.sort_by_key(|proposal| proposal.receipt.log_index.get());
    let state_digest = hex_digest(
        state
            .catalog
            .state_digest()
            .map_err(|error| error.to_string())?,
    );
    for committed in retained {
        if committed.receipt.group_id.get() != scope.group_id
            || committed.receipt.group_epoch.get() != scope.group_epoch
            || committed.receipt.log_index.get() > state.last_applied_index
        {
            return Err("Catalog checkpoint retry receipt has invalid scope or index".into());
        }
        let proposal_id = committed.receipt.proposal_id.get();
        if let Some(existing) = state.applied.get(&proposal_id) {
            if existing.payload != committed.payload
                || existing.receipt.term != committed.receipt.term.get()
                || existing.receipt.commit_index != committed.receipt.log_index.get()
            {
                return Err(format!(
                    "catalog retry proposal {proposal_id} disagrees with its installed receipt"
                ));
            }
            continue;
        }
        let command =
            CatalogCommand::decode(&committed.payload).map_err(|error| error.to_string())?;
        if command.is_transient_control() {
            continue;
        }
        let operation = state
            .catalog
            .operation(command.request_token())
            .ok_or_else(|| {
                format!("catalog retry proposal {proposal_id} has no durable applied result")
            })?;
        let replayed = state
            .catalog
            .apply(command)
            .map_err(|error| error.to_string())?;
        if replayed != operation.mutation.as_replayed() {
            return Err(format!(
                "catalog retry proposal {proposal_id} disagrees with its durable outcome"
            ));
        }
        let applied = AppliedCatalogCommand {
            payload: committed.payload,
            receipt: CatalogTabletReceipt {
                proposal_id,
                term: committed.receipt.term.get(),
                commit_index: committed.receipt.log_index.get(),
                mutation: operation.mutation,
                state_digest: state_digest.clone(),
            },
        };
        state.applied.insert(proposal_id, applied);
    }
    Ok(())
}

fn encode_catalog_application_checkpoint(
    scope: CatalogTabletScope,
    checkpoint_index: LogIndex,
    last_applied_index: u64,
    catalog_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    if last_applied_index > checkpoint_index.get() {
        return Err("Catalog application snapshot applied index is ahead of its checkpoint".into());
    }
    let catalog_len = u32::try_from(catalog_bytes.len())
        .map_err(|_| "Catalog snapshot length exceeds u32".to_owned())?;
    let capacity = CATALOG_APPLICATION_SNAPSHOT_HEADER_BYTES
        .checked_add(catalog_bytes.len())
        .ok_or_else(|| "Catalog application snapshot length overflow".to_owned())?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(&CATALOG_APPLICATION_SNAPSHOT_MAGIC);
    encoded.extend_from_slice(&CATALOG_APPLICATION_SNAPSHOT_VERSION.to_be_bytes());
    encoded.extend_from_slice(&scope.group_id.to_be_bytes());
    encoded.extend_from_slice(&scope.group_epoch.to_be_bytes());
    encoded.extend_from_slice(&checkpoint_index.get().to_be_bytes());
    encoded.extend_from_slice(&last_applied_index.to_be_bytes());
    encoded.extend_from_slice(&catalog_len.to_be_bytes());
    encoded.extend_from_slice(catalog_bytes);
    debug_assert_eq!(encoded.len(), capacity);
    Ok(encoded)
}

fn decode_catalog_application_checkpoint(
    scope: CatalogTabletScope,
    snapshot: &ApplicationSnapshot,
) -> Result<DecodedCatalogApplicationCheckpoint, String> {
    if snapshot.format_id() != CATALOG_APPLICATION_SNAPSHOT_FORMAT_ID {
        return Err("application snapshot is not a supported Catalog image".into());
    }
    let decoded = match snapshot.format_version() {
        LEGACY_CATALOG_APPLICATION_SNAPSHOT_VERSION => {
            decode_legacy_catalog_application_checkpoint(snapshot)?
        }
        CATALOG_APPLICATION_SNAPSHOT_VERSION => {
            decode_binary_catalog_application_checkpoint(snapshot)?
        }
        _ => return Err("application snapshot is not a supported Catalog image".into()),
    };
    if decoded.0 != scope.group_id
        || decoded.1 != scope.group_epoch
        || decoded.2 != snapshot.checkpoint_index().get()
        || decoded.3 > decoded.2
    {
        return Err("Catalog application snapshot scope or index is invalid".into());
    }
    let catalog = Catalog::decode_snapshot(&decoded.4).map_err(|error| error.to_string())?;
    if !catalog.is_consensus_group_reserved(scope.group_id)
        || catalog.state_digest().map_err(|error| error.to_string())? != snapshot.state_digest()
    {
        return Err("Catalog application snapshot state digest or reservation is invalid".into());
    }
    Ok(DecodedCatalogApplicationCheckpoint {
        catalog,
        last_applied_index: decoded.3,
        applied: decoded.5,
    })
}

type DecodedCatalogCheckpointFields = (
    u64,
    u64,
    u64,
    u64,
    Vec<u8>,
    BTreeMap<u64, AppliedCatalogCommand>,
);

fn decode_legacy_catalog_application_checkpoint(
    snapshot: &ApplicationSnapshot,
) -> Result<DecodedCatalogCheckpointFields, String> {
    let checkpoint: LegacyCatalogApplicationCheckpoint =
        serde_json::from_slice(snapshot.payload()).map_err(|error| error.to_string())?;
    if serde_json::to_vec(&checkpoint).map_err(|error| error.to_string())? != snapshot.payload() {
        return Err("Catalog application snapshot is not canonical".into());
    }
    if checkpoint.format_version != LEGACY_CATALOG_APPLICATION_SNAPSHOT_VERSION {
        return Err("Catalog application snapshot version is invalid".into());
    }
    let catalog_bytes = STANDARD_NO_PAD
        .decode(&checkpoint.catalog_base64)
        .map_err(|error| format!("Catalog snapshot base64 is invalid: {error}"))?;
    let applied =
        validate_legacy_applied_registry(checkpoint.applied, checkpoint.last_applied_index)?;
    Ok((
        checkpoint.group_id,
        checkpoint.group_epoch,
        checkpoint.checkpoint_index,
        checkpoint.last_applied_index,
        catalog_bytes,
        applied,
    ))
}

fn validate_legacy_applied_registry(
    entries: Vec<AppliedCatalogCheckpoint>,
    last_applied_index: u64,
) -> Result<BTreeMap<u64, AppliedCatalogCommand>, String> {
    let mut applied = BTreeMap::new();
    let mut previous_index = 0_u64;
    for entry in entries {
        let receipt = &entry.command.receipt;
        if entry.proposal_id == 0
            || receipt.proposal_id != entry.proposal_id
            || receipt.term == 0
            || receipt.commit_index <= previous_index
            || receipt.commit_index > last_applied_index
            || applied
                .insert(entry.proposal_id, entry.command.clone())
                .is_some()
        {
            return Err("Catalog application retry registry is invalid".into());
        }
        CatalogCommand::decode(&entry.command.payload).map_err(|error| error.to_string())?;
        previous_index = receipt.commit_index;
    }
    Ok(applied)
}

fn decode_binary_catalog_application_checkpoint(
    snapshot: &ApplicationSnapshot,
) -> Result<DecodedCatalogCheckpointFields, String> {
    let payload = snapshot.payload();
    if payload.len() < CATALOG_APPLICATION_SNAPSHOT_HEADER_BYTES
        || payload[..4] != CATALOG_APPLICATION_SNAPSHOT_MAGIC
    {
        return Err("Catalog application snapshot header is invalid".into());
    }
    let mut offset = 4;
    let version = read_catalog_checkpoint_u16(payload, &mut offset, "version")?;
    if version != CATALOG_APPLICATION_SNAPSHOT_VERSION {
        return Err("Catalog application snapshot version is invalid".into());
    }
    let group_id = read_catalog_checkpoint_u64(payload, &mut offset, "group ID")?;
    let group_epoch = read_catalog_checkpoint_u64(payload, &mut offset, "group epoch")?;
    let checkpoint_index = read_catalog_checkpoint_u64(payload, &mut offset, "checkpoint index")?;
    let last_applied_index =
        read_catalog_checkpoint_u64(payload, &mut offset, "last applied index")?;
    let catalog_len = usize::try_from(read_catalog_checkpoint_u32(
        payload,
        &mut offset,
        "Catalog length",
    )?)
    .map_err(|_| "Catalog application snapshot length exceeds usize".to_owned())?;
    if payload.len().checked_sub(offset) != Some(catalog_len) {
        return Err("Catalog application snapshot length is invalid".into());
    }
    Ok((
        group_id,
        group_epoch,
        checkpoint_index,
        last_applied_index,
        payload[offset..].to_vec(),
        BTreeMap::new(),
    ))
}

fn read_catalog_checkpoint_u16(
    payload: &[u8],
    offset: &mut usize,
    label: &str,
) -> Result<u16, String> {
    read_catalog_checkpoint_array::<2>(payload, offset, label).map(u16::from_be_bytes)
}

fn read_catalog_checkpoint_u32(
    payload: &[u8],
    offset: &mut usize,
    label: &str,
) -> Result<u32, String> {
    read_catalog_checkpoint_array::<4>(payload, offset, label).map(u32::from_be_bytes)
}

fn read_catalog_checkpoint_u64(
    payload: &[u8],
    offset: &mut usize,
    label: &str,
) -> Result<u64, String> {
    read_catalog_checkpoint_array::<8>(payload, offset, label).map(u64::from_be_bytes)
}

fn read_catalog_checkpoint_array<const N: usize>(
    payload: &[u8],
    offset: &mut usize,
    label: &str,
) -> Result<[u8; N], String> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| format!("Catalog application snapshot {label} offset overflow"))?;
    let bytes = payload
        .get(*offset..end)
        .ok_or_else(|| format!("Catalog application snapshot {label} is truncated"))?;
    *offset = end;
    bytes
        .try_into()
        .map_err(|_| format!("Catalog application snapshot {label} is invalid"))
}

fn apply_committed(
    scope: CatalogTabletScope,
    state: &mut CatalogTabletState,
    committed: &CommittedProposal,
) -> Result<CatalogTabletReceipt, String> {
    if committed.receipt.group_id.get() != scope.group_id {
        return Err(format!(
            "catalog command targets group {}; expected {}",
            committed.receipt.group_id.get(),
            scope.group_id
        ));
    }
    if committed.receipt.group_epoch.get() != scope.group_epoch {
        return Err(format!(
            "catalog command targets group epoch {}; expected {}",
            committed.receipt.group_epoch.get(),
            scope.group_epoch
        ));
    }
    let proposal_id = committed.receipt.proposal_id.get();
    if let Some(applied) = state.applied.get(&proposal_id) {
        if applied.payload != committed.payload {
            return Err(format!(
                "catalog proposal {proposal_id} is already bound to different command bytes"
            ));
        }
        return Ok(applied.receipt.clone());
    }
    let commit_index = committed.receipt.log_index.get();
    if commit_index <= state.last_applied_index {
        return Err(format!(
            "catalog commit index {commit_index} does not follow {}",
            state.last_applied_index
        ));
    }
    let command = CatalogCommand::decode(&committed.payload).map_err(|error| error.to_string())?;
    let mutation = state
        .catalog
        .apply(command)
        .map_err(|error| error.to_string())?;
    let receipt = CatalogTabletReceipt {
        proposal_id,
        term: committed.receipt.term.get(),
        commit_index,
        mutation,
        state_digest: hex_digest(
            state
                .catalog
                .state_digest()
                .map_err(|error| error.to_string())?,
        ),
    };
    state.applied.insert(
        proposal_id,
        AppliedCatalogCommand {
            payload: committed.payload.clone(),
            receipt: receipt.clone(),
        },
    );
    state.last_applied_index = commit_index;
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use epoch_catalog::{
        AcquireControlLease, ApplyDesiredResources, ApplyResource, CatalogCommand, CatalogMutation,
        CatalogRejectionCode, ControlLeaseGuard, DesiredResourceWrite, ReconcileManagedResources,
        ResourceName, ResourceSpec, UpdateManagedResourceStatus,
    };
    use epoch_consensus::{
        CommitReceipt, GroupEpoch, GroupId, LogIndex, MAX_APPLICATION_SNAPSHOT_BYTES, ProposalId,
        Term,
    };
    use epoch_core::{ResourceKind, WorkloadProfile};
    use serde_json::json;

    use super::*;

    fn command(token: &str, name: &str, shards: u32) -> CatalogCommand {
        CatalogCommand::Apply(ApplyResource {
            request_token: token.into(),
            expected_generation: None,
            name: ResourceName::new(
                "acme",
                "payments",
                "production",
                "core",
                ResourceKind::Stream,
                name,
            )
            .unwrap(),
            spec: ResourceSpec {
                workload_profile: WorkloadProfile::StreamLog,
                shard_count: shards,
                replica_count: 3,
                configuration: None,
                governance: None,
            },
            tablet_placements: Vec::new(),
        })
    }

    fn committed(
        proposal_id: u64,
        term: u64,
        log_index: u64,
        command: &CatalogCommand,
    ) -> CommittedProposal {
        CommittedProposal {
            receipt: CommitReceipt {
                group_id: GroupId::new(9).unwrap(),
                group_epoch: GroupEpoch::new(4).unwrap(),
                proposal_id: ProposalId::new(proposal_id).unwrap(),
                term: Term::new(term),
                log_index: LogIndex::new(log_index),
            },
            payload: command.encode().unwrap(),
        }
    }

    fn managed_status_sequence(service: &CatalogTabletService) -> u64 {
        service.snapshot().unwrap().managed_resources[0].status["sequence"]
            .as_u64()
            .unwrap()
    }

    #[test]
    fn replay_rebuilds_resources_receipts_and_digest_in_commit_order() {
        let service = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        let first = committed(31, 2, 1, &command("orders-v1", "orders", 2));
        let second = committed(32, 2, 2, &command("audit-v1", "audit", 1));
        service.replay(&[second.clone(), first.clone()]).unwrap();

        let snapshot = service.snapshot().unwrap();
        assert_eq!(snapshot.last_applied_index, 2);
        assert_eq!(snapshot.applied_command_count, 2);
        assert_eq!(snapshot.resource_count, 2);
        assert_eq!(snapshot.tablet_count, 3);
        assert_eq!(snapshot.resources[0].name.name, "audit");
        assert_eq!(snapshot.resources[1].name.name, "orders");
        assert_eq!(
            service.receipt(32).unwrap().unwrap().state_digest,
            snapshot.state_digest
        );

        let live = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        live.apply(&first).unwrap();
        live.apply(&second).unwrap();
        assert_eq!(live.snapshot().unwrap(), snapshot);
    }

    #[test]
    fn malformed_or_mismatched_commit_fail_stops_the_catalog() {
        let service = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        let mut malformed = committed(31, 2, 1, &command("orders-v1", "orders", 1));
        malformed.payload.push(b' ');
        assert!(service.apply(&malformed).is_err());
        assert!(service.ensure_healthy().is_err());
        assert!(service.snapshot().is_err());
        assert!(
            service
                .apply(&committed(32, 2, 2, &command("audit-v1", "audit", 1)))
                .is_err()
        );

        let wrong_scope = CatalogTabletService::new(CatalogTabletScope::new(10, 4).unwrap());
        assert!(
            wrong_scope
                .apply(&committed(33, 2, 1, &command("other-v1", "other", 1)))
                .is_err()
        );
    }

    #[test]
    fn exact_duplicate_commit_is_idempotent_but_rebinding_fails_closed() {
        let service = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        let original = committed(31, 2, 1, &command("orders-v1", "orders", 1));
        service.apply(&original).unwrap();
        let receipt = service.receipt(31).unwrap().unwrap();
        service.apply(&original).unwrap();
        assert_eq!(service.receipt(31).unwrap().unwrap(), receipt);
        assert_eq!(service.snapshot().unwrap().applied_command_count, 1);

        let rebound = committed(31, 2, 2, &command("audit-v1", "audit", 1));
        assert!(service.apply(&rebound).is_err());
        assert!(service.ensure_healthy().is_err());
    }

    #[test]
    fn native_snapshot_restores_full_catalog_and_rebuilds_retained_receipts_durably() {
        let service = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        let first = committed(31, 2, 1, &command("orders-v1", "orders", 2));
        let second = committed(32, 2, 2, &command("audit-v1", "audit", 1));
        service.apply(&first).unwrap();
        service.apply(&second).unwrap();
        let expected = service.snapshot().unwrap();

        let image = service
            .capture_snapshot(LogIndex::new(2), std::slice::from_ref(&second))
            .unwrap();
        let restored = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        restored.install_snapshot(&image).unwrap();

        let actual = restored.snapshot().unwrap();
        assert_eq!(actual.resources, expected.resources);
        assert_eq!(actual.state_digest, expected.state_digest);
        assert_eq!(actual.last_applied_index, 2);
        assert_eq!(actual.applied_command_count, 0);
        assert!(restored.receipt(31).unwrap().is_none());
        assert!(restored.receipt(32).unwrap().is_none());
        let replayed = restored.durable_replay_receipt(&second).unwrap().unwrap();
        assert_eq!(replayed.proposal_id, 32);
        assert!(matches!(
            replayed.mutation,
            CatalogMutation::Applied { replayed: true, .. }
        ));
        restored
            .restore_checkpoint_receipts(std::slice::from_ref(&second))
            .unwrap();
        assert_eq!(restored.snapshot().unwrap().applied_command_count, 1);
        assert_eq!(restored.receipt(32).unwrap(), service.receipt(32).unwrap());
        restored
            .apply(&committed(33, 2, 3, &command("next-v1", "next", 1)))
            .unwrap();
        assert_eq!(restored.snapshot().unwrap().resource_count, 3);
    }

    #[test]
    fn native_snapshot_v2_keeps_v1_json_images_readable() {
        let source = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        let proposal = committed(31, 2, 1, &command("orders-v1", "orders", 1));
        source.apply(&proposal).unwrap();
        let receipt = source.receipt(31).unwrap().unwrap();
        let state = source.state.read().unwrap();
        let catalog_bytes = state.catalog.encode_snapshot().unwrap();
        let state_digest = state.catalog.state_digest().unwrap();
        drop(state);
        let legacy = LegacyCatalogApplicationCheckpoint {
            format_version: LEGACY_CATALOG_APPLICATION_SNAPSHOT_VERSION,
            group_id: 9,
            group_epoch: 4,
            checkpoint_index: 1,
            last_applied_index: 1,
            catalog_base64: STANDARD_NO_PAD.encode(catalog_bytes),
            applied: vec![AppliedCatalogCheckpoint {
                proposal_id: 31,
                command: AppliedCatalogCommand {
                    payload: proposal.payload.clone(),
                    receipt,
                },
            }],
        };
        let payload = serde_json::to_vec(&legacy).unwrap();
        let image = ApplicationSnapshot::new(
            LogIndex::new(1),
            CATALOG_APPLICATION_SNAPSHOT_FORMAT_ID,
            LEGACY_CATALOG_APPLICATION_SNAPSHOT_VERSION,
            state_digest,
            payload,
        )
        .unwrap();

        let restored = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        restored.install_snapshot(&image).unwrap();
        restored
            .restore_checkpoint_receipts(std::slice::from_ref(&proposal))
            .unwrap();
        assert_eq!(restored.snapshot().unwrap().resource_count, 1);
        assert_eq!(restored.snapshot().unwrap().last_applied_index, 1);
        assert_eq!(restored.snapshot().unwrap().applied_command_count, 1);
    }

    #[test]
    fn native_snapshot_bounds_large_managed_retry_payloads() {
        let service = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        let managed_name = ResourceName::new(
            "acme",
            "payments",
            "production",
            "core",
            ResourceKind::Stream,
            "managed-orders",
        )
        .unwrap();
        let desired = CatalogCommand::ApplyDesired(ApplyDesiredResources {
            request_token: "managed-desired".into(),
            resources: vec![DesiredResourceWrite {
                name: managed_name.clone(),
                expected_generation: Some(0),
                desired: json!({"padding": "d".repeat(60 * 1024)}),
            }],
        });
        service.apply(&committed(100, 2, 1, &desired)).unwrap();
        let lease = CatalogCommand::AcquireControlLease(AcquireControlLease {
            request_token: "managed-lease".into(),
            owner_id: "control-a".into(),
            now_ms: 1_000,
            ttl_ms: 60_000,
        });
        service.apply(&committed(101, 2, 2, &lease)).unwrap();

        let mut retained = Vec::new();
        for index in 0..14_u64 {
            let status = CatalogCommand::UpdateManagedStatus(UpdateManagedResourceStatus {
                request_token: format!("managed-status-{index:02}"),
                lease: ControlLeaseGuard {
                    owner_id: "control-a".into(),
                    fence: 1,
                    now_ms: 1_001 + index,
                },
                name: managed_name.clone(),
                expected_generation: 1,
                status: json!({
                    "phase": "ready",
                    "sequence": index,
                    "padding": "s".repeat(60 * 1024),
                }),
            });
            let applied = committed(102 + index, 2, 3 + index, &status);
            service.apply(&applied).unwrap();
            retained.push(applied);
        }

        let image = service
            .capture_snapshot(LogIndex::new(16), &retained)
            .expect("bounded retry payloads must fit a Catalog application snapshot");
        assert!(image.payload().len() <= MAX_APPLICATION_SNAPSHOT_BYTES);
        assert!(service.receipt(115).unwrap().is_some());
        let restored = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        restored.install_snapshot(&image).unwrap();
        assert!(restored.receipt(115).unwrap().is_none());
        let repeated = restored
            .capture_snapshot(LogIndex::new(16), &retained)
            .expect("a restored compact retry suffix must remain checkpointable");
        assert!(repeated.payload().len() <= MAX_APPLICATION_SNAPSHOT_BYTES);
        let retained_public = committed(100, 2, 1, &desired);
        restored
            .capture_snapshot(LogIndex::new(16), &[retained_public])
            .expect("a durable public outcome does not need a duplicate retry receipt");
        let replayed = restored
            .durable_replay_receipt(retained.last().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(replayed.proposal_id, 115);
        assert!(matches!(
            replayed.mutation,
            CatalogMutation::ManagedStatusUpdated { replayed: true, .. }
        ));
        assert_eq!(managed_status_sequence(&restored), 13);
        let next_status = CatalogCommand::UpdateManagedStatus(UpdateManagedResourceStatus {
            request_token: "managed-status-after-restore".into(),
            lease: ControlLeaseGuard {
                owner_id: "control-a".into(),
                fence: 1,
                now_ms: 1_015,
            },
            name: managed_name,
            expected_generation: 1,
            status: json!({"phase": "ready", "sequence": 14}),
        });
        restored
            .apply(&committed(116, 2, 17, &next_status))
            .unwrap();
        assert_eq!(managed_status_sequence(&restored), 14);
    }

    #[test]
    fn native_snapshot_compacts_reconciliation_retry_receipts() {
        let service = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        let lease = CatalogCommand::AcquireControlLease(AcquireControlLease {
            request_token: "lease-a".into(),
            owner_id: "control-a".into(),
            now_ms: 1_000,
            ttl_ms: 10_000,
        });
        service.apply(&committed(100, 2, 1, &lease)).unwrap();
        let reconcile = CatalogCommand::ReconcileManaged(ReconcileManagedResources {
            request_token: "reconcile-a".into(),
            lease: ControlLeaseGuard {
                owner_id: "control-a".into(),
                fence: 1,
                now_ms: 1_001,
            },
            capacity: Vec::new(),
            resources: Vec::new(),
        });
        let retained = committed(101, 2, 2, &reconcile);
        service.apply(&retained).unwrap();

        let image = service
            .capture_snapshot(LogIndex::new(2), std::slice::from_ref(&retained))
            .unwrap();
        let restored = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        restored.install_snapshot(&image).unwrap();
        assert!(restored.receipt(101).unwrap().is_none());
        let replayed = restored.durable_replay_receipt(&retained).unwrap().unwrap();
        assert!(matches!(
            replayed.mutation,
            CatalogMutation::Rejected { replayed: true, .. }
        ));
    }

    #[test]
    fn snapshot_capacity_rejection_does_not_fail_stop_the_catalog_tablet() {
        let service = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        let mut rejected = None;
        for index in 0..32_u64 {
            let command = CatalogCommand::ApplyDesired(ApplyDesiredResources {
                request_token: format!("large-desired-{index}"),
                resources: vec![DesiredResourceWrite {
                    name: ResourceName::new(
                        "acme",
                        "payments",
                        "production",
                        "core",
                        ResourceKind::Stream,
                        format!("orders-{index}"),
                    )
                    .unwrap(),
                    expected_generation: Some(0),
                    desired: json!({"padding": "x".repeat(120 * 1024)}),
                }],
            });
            let committed = committed(index + 1, 2, index + 1, &command);
            let receipt = service.apply_one(&committed).unwrap();
            if matches!(
                receipt.mutation,
                CatalogMutation::Rejected {
                    code: CatalogRejectionCode::CapacityExceeded,
                    ..
                }
            ) {
                rejected = Some((receipt, committed));
                break;
            }
        }
        let (receipt, committed) = rejected.expect("the bounded Catalog must reject growth");
        service.ensure_healthy().unwrap();
        let catalog_bytes = service
            .state
            .read()
            .unwrap()
            .catalog
            .encode_snapshot()
            .unwrap();
        assert!(catalog_bytes.len() <= epoch_catalog::MAX_CATALOG_SNAPSHOT_BYTES);
        assert!(matches!(
            service
                .operation(
                    CatalogCommand::decode(&committed.payload)
                        .unwrap()
                        .request_token()
                )
                .unwrap()
                .unwrap()
                .mutation,
            CatalogMutation::Rejected {
                replayed: false,
                ..
            }
        ));

        let image = service
            .capture_snapshot(
                LogIndex::new(receipt.commit_index),
                std::slice::from_ref(&committed),
            )
            .expect("a capacity-bound Catalog must remain checkpointable");
        assert_eq!(image.format_version(), CATALOG_APPLICATION_SNAPSHOT_VERSION);
        assert!(image.payload().len() <= MAX_APPLICATION_SNAPSHOT_BYTES);
        assert!(
            image
                .payload()
                .starts_with(&CATALOG_APPLICATION_SNAPSHOT_MAGIC)
        );
        let restored = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        restored.install_snapshot(&image).unwrap();
        let replayed = restored
            .durable_replay_receipt(&committed)
            .unwrap()
            .expect("capacity rejection must survive checkpoint restore");
        assert!(matches!(
            replayed.mutation,
            CatalogMutation::Rejected {
                code: CatalogRejectionCode::CapacityExceeded,
                replayed: true,
                ..
            }
        ));
    }

    #[test]
    fn native_snapshot_install_rejects_foreign_scope_without_partial_state() {
        let source = CatalogTabletService::new(CatalogTabletScope::new(9, 4).unwrap());
        let proposal = committed(31, 2, 1, &command("orders-v1", "orders", 1));
        source.apply(&proposal).unwrap();
        let image = source
            .capture_snapshot(LogIndex::new(1), std::slice::from_ref(&proposal))
            .unwrap();
        let target = CatalogTabletService::new(CatalogTabletScope::new(10, 4).unwrap());

        assert!(target.install_snapshot(&image).is_err());
        assert!(target.ensure_healthy().is_err());
    }
}
