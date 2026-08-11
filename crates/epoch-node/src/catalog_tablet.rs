//! Replicated regional catalog state machine boundary.
//!
//! Catalog commands are decoded and applied only by the consensus actor after
//! commit. The service is intentionally independent from HTTP and group
//! supervision so the same authoritative state can drive standalone tests,
//! node-local reconciliation, and the future regional administration API.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use epoch_catalog::{Catalog, CatalogCommand, CatalogMutation, ResourceRecord};
use epoch_consensus::{ApplicationSnapshot, CommittedProposal, LogIndex};
use serde::{Deserialize, Serialize};

use crate::{
    consensus::CommittedProposalApplier,
    tablet_http::{deserialize_u64_from_number_or_decimal, hex_digest, serialize_u64_as_decimal},
};

const CATALOG_APPLICATION_SNAPSHOT_FORMAT_ID: [u8; 16] = *b"CATALOG_STATE_V1";
const CATALOG_APPLICATION_SNAPSHOT_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogTabletScope {
    group_id: u64,
    group_epoch: u64,
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
    pub state_digest: String,
    pub resources: Vec<ResourceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AppliedCatalogCommand {
    payload: Vec<u8>,
    receipt: CatalogTabletReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogApplicationCheckpoint {
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
            state_digest,
            resources: state.catalog.resources().cloned().collect(),
        })
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
        let state = self
            .state
            .read()
            .map_err(|_| "catalog state read lock was poisoned".to_owned())?;
        if state.last_applied_index > checkpoint_index.get() {
            return Err(format!(
                "catalog applied index {} exceeds consensus checkpoint index {}",
                state.last_applied_index, checkpoint_index
            ));
        }
        let mut applied = Vec::with_capacity(retained.len());
        for committed in retained {
            let proposal_id = committed.receipt.proposal_id.get();
            let command = state.applied.get(&proposal_id).ok_or_else(|| {
                format!("catalog retry proposal {proposal_id} has no typed applied result")
            })?;
            if command.payload != committed.payload
                || command.receipt.term != committed.receipt.term.get()
                || command.receipt.commit_index != committed.receipt.log_index.get()
            {
                return Err(format!(
                    "catalog retry proposal {proposal_id} disagrees with consensus"
                ));
            }
            applied.push(AppliedCatalogCheckpoint {
                proposal_id,
                command: command.clone(),
            });
        }
        applied.sort_by_key(|entry| entry.command.receipt.commit_index);
        let catalog_bytes = state
            .catalog
            .encode_snapshot()
            .map_err(|error| error.to_string())?;
        let state_digest = state
            .catalog
            .state_digest()
            .map_err(|error| error.to_string())?;
        let checkpoint = CatalogApplicationCheckpoint {
            format_version: CATALOG_APPLICATION_SNAPSHOT_VERSION,
            group_id: self.scope.group_id,
            group_epoch: self.scope.group_epoch,
            checkpoint_index: checkpoint_index.get(),
            last_applied_index: state.last_applied_index,
            catalog_base64: STANDARD_NO_PAD.encode(catalog_bytes),
            applied,
        };
        let payload = serde_json::to_vec(&checkpoint).map_err(|error| error.to_string())?;
        ApplicationSnapshot::new(
            checkpoint_index,
            CATALOG_APPLICATION_SNAPSHOT_FORMAT_ID,
            CATALOG_APPLICATION_SNAPSHOT_VERSION,
            state_digest,
            payload,
        )
        .map_err(|error| error.to_string())
    }

    fn install_snapshot(&self, snapshot: &ApplicationSnapshot) -> Result<(), String> {
        self.ensure_healthy()?;
        let result: Result<CatalogTabletState, String> = (|| {
            if snapshot.format_id() != CATALOG_APPLICATION_SNAPSHOT_FORMAT_ID
                || snapshot.format_version() != CATALOG_APPLICATION_SNAPSHOT_VERSION
            {
                return Err("application snapshot is not a supported Catalog image".into());
            }
            let checkpoint: CatalogApplicationCheckpoint =
                serde_json::from_slice(snapshot.payload()).map_err(|error| error.to_string())?;
            if serde_json::to_vec(&checkpoint).map_err(|error| error.to_string())?
                != snapshot.payload()
            {
                return Err("Catalog application snapshot is not canonical".into());
            }
            if checkpoint.format_version != CATALOG_APPLICATION_SNAPSHOT_VERSION
                || checkpoint.group_id != self.scope.group_id
                || checkpoint.group_epoch != self.scope.group_epoch
                || checkpoint.checkpoint_index != snapshot.checkpoint_index().get()
                || checkpoint.last_applied_index > checkpoint.checkpoint_index
            {
                return Err("Catalog application snapshot scope or index is invalid".into());
            }
            let catalog_bytes = STANDARD_NO_PAD
                .decode(&checkpoint.catalog_base64)
                .map_err(|error| format!("Catalog snapshot base64 is invalid: {error}"))?;
            let catalog =
                Catalog::decode_snapshot(&catalog_bytes).map_err(|error| error.to_string())?;
            if !catalog.is_consensus_group_reserved(self.scope.group_id)
                || catalog.state_digest().map_err(|error| error.to_string())?
                    != snapshot.state_digest()
            {
                return Err(
                    "Catalog application snapshot state digest or reservation is invalid".into(),
                );
            }

            let mut applied = BTreeMap::new();
            let mut previous_index = 0_u64;
            for entry in checkpoint.applied {
                let receipt = &entry.command.receipt;
                if entry.proposal_id == 0
                    || receipt.proposal_id != entry.proposal_id
                    || receipt.term == 0
                    || receipt.commit_index <= previous_index
                    || receipt.commit_index > checkpoint.last_applied_index
                    || applied
                        .insert(entry.proposal_id, entry.command.clone())
                        .is_some()
                {
                    return Err("Catalog application retry registry is invalid".into());
                }
                CatalogCommand::decode(&entry.command.payload)
                    .map_err(|error| error.to_string())?;
                previous_index = receipt.commit_index;
            }
            Ok(CatalogTabletState {
                catalog,
                applied,
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

    fn supports_native_snapshots(&self) -> bool {
        true
    }
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
    use epoch_catalog::{ApplyResource, CatalogCommand, ResourceName, ResourceSpec};
    use epoch_consensus::{CommitReceipt, GroupEpoch, GroupId, LogIndex, ProposalId, Term};
    use epoch_core::{ResourceKind, WorkloadProfile};

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
            },
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
    fn native_snapshot_restores_full_catalog_and_only_the_retained_retry_suffix() {
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
        assert_eq!(actual.applied_command_count, 1);
        assert!(restored.receipt(31).unwrap().is_none());
        assert_eq!(restored.receipt(32).unwrap(), service.receipt(32).unwrap());
        restored
            .apply(&committed(33, 2, 3, &command("next-v1", "next", 1)))
            .unwrap();
        assert_eq!(restored.snapshot().unwrap().resource_count, 3);
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
