//! Learner-first reconciliation for catalog-planned tablet voter replacement.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use epoch_catalog::{FinalizeTabletMembership, TabletDescriptor};
use epoch_consensus::{ConsensusMembership, ConsensusRole, ConsensusStatus};
use sha2::{Digest, Sha256};

use crate::{
    catalog_api::RegionalCatalogState,
    consensus::ConsensusProbeHandle,
    tablet_materializer::{MaterializedTabletRoute, TabletDirectory},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingTabletMembershipAction {
    AddLearner(u64),
    Reconfigure(Vec<u64>),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TabletMembershipPass {
    pub transitions_observed: usize,
    pub leaders_observed: usize,
    pub learners_added: usize,
    pub learner_snapshots_refreshed: usize,
    pub voter_reconfigurations: usize,
    pub catalog_finalizations: usize,
    pub pending: usize,
}

/// Advances at most one membership action per locally led tablet group. Every
/// action is reconstructed from committed Catalog state and committed Raft
/// membership, so restarting the worker cannot skip learner catch-up.
pub async fn run_tablet_membership_pass(
    catalog: &RegionalCatalogState,
    directory: &TabletDirectory,
    pending: &mut BTreeMap<u64, PendingTabletMembershipAction>,
) -> Result<TabletMembershipPass, String> {
    let snapshot = catalog.catalog_snapshot()?;
    let transitions = snapshot
        .resources
        .iter()
        .flat_map(|resource| &resource.tablets)
        .filter(|tablet| !tablet.target_voter_node_ids.is_empty())
        .map(|tablet| tablet.consensus_group_id)
        .collect::<BTreeSet<_>>();
    pending.retain(|group_id, _| transitions.contains(group_id));

    let mut pass = TabletMembershipPass {
        transitions_observed: transitions.len(),
        ..TabletMembershipPass::default()
    };
    for route in directory.routes().map_err(|error| error.to_string())? {
        if route.metadata().descriptor.target_voter_node_ids.is_empty() {
            continue;
        }
        reconcile_route(catalog, &route, pending, &mut pass).await?;
    }
    Ok(pass)
}

async fn reconcile_route(
    catalog: &RegionalCatalogState,
    route: &MaterializedTabletRoute,
    pending: &mut BTreeMap<u64, PendingTabletMembershipAction>,
    pass: &mut TabletMembershipPass,
) -> Result<(), String> {
    let descriptor = &route.metadata().descriptor;
    validate_transition(descriptor)?;
    let consensus = route.consensus();
    let status = consensus
        .status()
        .await
        .map_err(|error| error.to_string())?;
    let membership = consensus
        .membership()
        .await
        .map_err(|error| error.to_string())?;
    let group_id = descriptor.consensus_group_id;
    if status.role != ConsensusRole::Leader {
        pending.remove(&group_id);
        pass.pending += 1;
        return Ok(());
    }
    pass.leaders_observed += 1;
    validate_membership_directory(descriptor, &membership)?;

    if !membership.outgoing_voters.is_empty()
        || !membership.staged_learners.is_empty()
        || membership.auto_leave
    {
        pass.pending += 1;
        return Ok(());
    }
    if let Some(action) = pending.get(&group_id) {
        if pending_action_observed(action, &membership) {
            pending.remove(&group_id);
        } else {
            pass.pending += 1;
            return Ok(());
        }
    }

    let current = node_ids(&membership.voters);
    if current == descriptor.target_voter_node_ids {
        finalize_catalog_transition(catalog, descriptor).await?;
        pass.catalog_finalizations += 1;
        return Ok(());
    }
    if current != descriptor.voter_node_ids {
        return Err(format!(
            "group {group_id} committed voters {current:?} match neither Catalog current {:?} nor target {:?}",
            descriptor.voter_node_ids, descriptor.target_voter_node_ids
        ));
    }

    let added = replacement_node(descriptor)?;
    if membership
        .learners
        .iter()
        .any(|node_id| node_id.get() == added)
    {
        return reconcile_existing_learner(&consensus, &status, descriptor, added, pending, pass)
            .await;
    }
    if membership
        .voters
        .iter()
        .chain(&membership.staged_learners)
        .any(|node_id| node_id.get() == added)
    {
        pass.pending += 1;
        return Ok(());
    }
    consensus
        .add_learner(added)
        .await
        .map_err(|error| error.to_string())?;
    pending.insert(group_id, PendingTabletMembershipAction::AddLearner(added));
    pass.learners_added += 1;
    Ok(())
}

async fn reconcile_existing_learner(
    consensus: &ConsensusProbeHandle,
    status: &ConsensusStatus,
    descriptor: &TabletDescriptor,
    added: u64,
    pending: &mut BTreeMap<u64, PendingTabletMembershipAction>,
    pass: &mut TabletMembershipPass,
) -> Result<(), String> {
    let group_id = descriptor.consensus_group_id;
    let progress = status
        .replication_progress
        .iter()
        .find(|progress| progress.node_id.get() == added)
        .ok_or_else(|| format!("group {group_id} learner {added} has no leader progress"))?;
    if progress.pending_snapshot_index.get() != 0 && status.checkpoint_index < status.applied_index
    {
        consensus
            .checkpoint()
            .await
            .map_err(|error| error.to_string())?;
        pass.learner_snapshots_refreshed += 1;
        pass.pending += 1;
        return Ok(());
    }
    if progress.matched_index < status.commit_index
        || progress.committed_index < status.commit_index
        || progress.pending_snapshot_index.get() != 0
        || !progress.recent_active
    {
        pass.pending += 1;
        return Ok(());
    }
    consensus
        .reconfigure_voters(descriptor.target_voter_node_ids.iter().copied())
        .await
        .map_err(|error| error.to_string())?;
    pending.insert(
        group_id,
        PendingTabletMembershipAction::Reconfigure(descriptor.target_voter_node_ids.clone()),
    );
    pass.voter_reconfigurations += 1;
    Ok(())
}

fn validate_transition(descriptor: &TabletDescriptor) -> Result<(), String> {
    if descriptor.tablet_id == 0
        || descriptor.consensus_group_id == 0
        || descriptor.tablet_epoch == 0
        || descriptor.resource_generation == 0
        || !matches!(descriptor.replica_count, 3 | 5)
        || descriptor.voter_node_ids.len() != usize::from(descriptor.replica_count)
        || descriptor.target_voter_node_ids.len() != usize::from(descriptor.replica_count)
        || !strictly_sorted(&descriptor.voter_node_ids)
        || !strictly_sorted(&descriptor.target_voter_node_ids)
    {
        return Err(format!(
            "tablet {} has an invalid membership transition",
            descriptor.tablet_id
        ));
    }
    replacement_node(descriptor).map(|_| ())
}

fn validate_membership_directory(
    descriptor: &TabletDescriptor,
    membership: &ConsensusMembership,
) -> Result<(), String> {
    let allowed = node_ids(&membership.allowed_members);
    if !strictly_sorted(&allowed)
        || descriptor
            .voter_node_ids
            .iter()
            .chain(&descriptor.target_voter_node_ids)
            .any(|node_id| allowed.binary_search(node_id).is_err())
    {
        return Err(format!(
            "group {} membership target is outside its immutable member directory",
            descriptor.consensus_group_id
        ));
    }
    Ok(())
}

fn replacement_node(descriptor: &TabletDescriptor) -> Result<u64, String> {
    let current = descriptor
        .voter_node_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let target = descriptor
        .target_voter_node_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if current.difference(&target).count() != 1 || target.difference(&current).count() != 1 {
        return Err(format!(
            "tablet {} membership transition must replace exactly one voter",
            descriptor.tablet_id
        ));
    }
    target
        .difference(&current)
        .next()
        .copied()
        .ok_or_else(|| "validated membership transition has no replacement node".into())
}

fn pending_action_observed(
    action: &PendingTabletMembershipAction,
    membership: &ConsensusMembership,
) -> bool {
    match action {
        PendingTabletMembershipAction::AddLearner(expected) => membership
            .learners
            .iter()
            .chain(&membership.voters)
            .any(|node_id| node_id.get() == *expected),
        PendingTabletMembershipAction::Reconfigure(expected) => {
            membership.outgoing_voters.is_empty() && node_ids(&membership.voters) == *expected
        }
    }
}

async fn finalize_catalog_transition(
    catalog: &RegionalCatalogState,
    descriptor: &TabletDescriptor,
) -> Result<(), String> {
    catalog
        .finalize_membership(FinalizeTabletMembership {
            request_token: finalization_token(descriptor),
            tablet_id: descriptor.tablet_id,
            expected_tablet_epoch: descriptor.tablet_epoch,
            expected_resource_generation: descriptor.resource_generation,
            target_voter_node_ids: descriptor.target_voter_node_ids.clone(),
        })
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn finalization_token(descriptor: &TabletDescriptor) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epoch/tablet-membership-finalize/v1\0");
    digest.update(descriptor.tablet_id.to_be_bytes());
    digest.update(descriptor.tablet_epoch.to_be_bytes());
    digest.update(descriptor.resource_generation.to_be_bytes());
    for node_id in &descriptor.target_voter_node_ids {
        digest.update(node_id.to_be_bytes());
    }
    let digest = digest.finalize();
    let mut token = String::with_capacity("membership-finalize-v1-".len() + digest.len() * 2);
    token.push_str("membership-finalize-v1-");
    for byte in digest {
        write!(&mut token, "{byte:02x}").expect("writing to a String cannot fail");
    }
    token
}

fn node_ids(nodes: &[epoch_consensus::NodeId]) -> Vec<u64> {
    nodes.iter().map(|node_id| node_id.get()).collect()
}

fn strictly_sorted(nodes: &[u64]) -> bool {
    !nodes.is_empty()
        && nodes.iter().all(|node_id| *node_id != 0)
        && nodes.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use epoch_core::WorkloadProfile;

    use super::*;

    fn descriptor() -> TabletDescriptor {
        TabletDescriptor {
            tablet_id: 41,
            consensus_group_id: 51,
            shard_index: 0,
            tablet_epoch: 7,
            resource_generation: 3,
            workload_profile: WorkloadProfile::StreamLog,
            replica_count: 3,
            voter_node_ids: vec![1, 2, 3],
            bootstrap_voter_node_ids: vec![1, 2, 3],
            target_voter_node_ids: vec![1, 2, 4],
        }
    }

    #[test]
    fn replacement_validation_and_finalization_identity_are_deterministic() {
        let descriptor = descriptor();
        assert_eq!(replacement_node(&descriptor).unwrap(), 4);
        assert_eq!(
            finalization_token(&descriptor),
            finalization_token(&descriptor)
        );
        assert!(finalization_token(&descriptor).starts_with("membership-finalize-v1-"));

        let mut invalid = descriptor;
        invalid.target_voter_node_ids = vec![2, 4, 5];
        assert!(validate_transition(&invalid).is_err());
    }
}
