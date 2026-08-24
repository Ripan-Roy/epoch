//! Canonical bounded-membership validation and reconfiguration planning.

use std::collections::BTreeSet;

use raft::prelude::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
};

use super::{
    ConsensusError, ConsensusResult, MAX_PROVISIONED_MEMBERS, MAX_VOTER_COUNT, MIN_VOTER_COUNT,
    NodeId,
};

pub(crate) fn validate_conf_state(
    state: &ConfState,
    allowed_members: &[NodeId],
) -> ConsensusResult<()> {
    validate_allowed_members(allowed_members)?;
    let allowed = allowed_members
        .iter()
        .map(|node_id| node_id.get())
        .collect::<BTreeSet<_>>();
    let voters = validate_ids("incoming voters", &state.voters, &allowed, false)?;
    let outgoing = validate_ids("outgoing voters", &state.voters_outgoing, &allowed, true)?;
    let learners = validate_ids("learners", &state.learners, &allowed, true)?;
    let learners_next = validate_ids("staged learners", &state.learners_next, &allowed, true)?;

    validate_voter_count("incoming voter set", voters.len())?;
    if outgoing.is_empty() {
        if state.auto_leave || !learners_next.is_empty() {
            return Err(ConsensusError::InvalidVoterSet(
                "non-joint membership cannot auto-leave or contain staged learners".into(),
            ));
        }
    } else {
        validate_voter_count("outgoing voter set", outgoing.len())?;
    }
    if !voters.is_disjoint(&learners)
        || !voters.is_disjoint(&learners_next)
        || !learners.is_disjoint(&outgoing)
        || !learners.is_disjoint(&learners_next)
        || !learners_next.is_subset(&outgoing)
    {
        return Err(ConsensusError::InvalidVoterSet(
            "voters, learners, and staged learners violate joint-consensus disjointness".into(),
        ));
    }
    Ok(())
}

pub(crate) fn plan_add_learner(
    state: &ConfState,
    learner: NodeId,
    allowed_members: &[NodeId],
) -> ConsensusResult<ConfChangeV2> {
    validate_conf_state(state, allowed_members)?;
    if !state.voters_outgoing.is_empty() {
        return Err(ConsensusError::InvalidState(
            "a learner cannot be added while membership is joint".into(),
        ));
    }
    if !allowed_members.contains(&learner) {
        return Err(ConsensusError::InvalidVoterSet(format!(
            "learner {learner} is outside the configured member allowlist"
        )));
    }
    if state.voters.contains(&learner.get())
        || state.learners.contains(&learner.get())
        || state.learners_next.contains(&learner.get())
    {
        return Err(ConsensusError::InvalidState(format!(
            "node {learner} is already part of the committed membership"
        )));
    }
    Ok(conf_change(vec![change(
        ConfChangeType::AddLearnerNode,
        learner,
    )]))
}

pub(crate) fn plan_voter_reconfiguration(
    state: &ConfState,
    target_voters: &[NodeId],
    allowed_members: &[NodeId],
) -> ConsensusResult<ConfChangeV2> {
    validate_conf_state(state, allowed_members)?;
    if !state.voters_outgoing.is_empty() {
        return Err(ConsensusError::InvalidState(
            "a new voter plan cannot begin while membership is joint".into(),
        ));
    }
    validate_target_voters(target_voters, allowed_members)?;
    let current = state.voters.iter().copied().collect::<BTreeSet<_>>();
    let target = target_voters
        .iter()
        .map(|node_id| node_id.get())
        .collect::<BTreeSet<_>>();
    if current == target {
        return Err(ConsensusError::InvalidState(
            "target voter membership is already committed".into(),
        ));
    }

    let learners = state.learners.iter().copied().collect::<BTreeSet<_>>();
    let uncaught_up = target
        .difference(&current)
        .copied()
        .filter(|node_id| !learners.contains(node_id))
        .collect::<Vec<_>>();
    if !uncaught_up.is_empty() {
        return Err(ConsensusError::InvalidState(format!(
            "new voters {uncaught_up:?} must first join and catch up as learners"
        )));
    }

    let mut changes = current
        .difference(&target)
        .copied()
        .map(|node_id| change(ConfChangeType::RemoveNode, node(node_id)))
        .collect::<Vec<_>>();
    changes.extend(
        target
            .difference(&current)
            .copied()
            .map(|node_id| change(ConfChangeType::AddNode, node(node_id))),
    );
    changes.sort_unstable_by_key(|change| (change.node_id, change.change_type));

    let mut plan = conf_change(changes);
    plan.set_transition(ConfChangeTransition::Auto);
    Ok(plan)
}

pub(crate) fn apply_conf_change(
    state: &ConfState,
    change: &ConfChangeV2,
    allowed_members: &[NodeId],
) -> ConsensusResult<ConfState> {
    validate_conf_state(state, allowed_members)?;
    let mut next = state.clone();
    if change.leave_joint() {
        if next.voters_outgoing.is_empty() {
            return Err(ConsensusError::InvalidState(
                "cannot leave a non-joint membership".into(),
            ));
        }
        next.learners.append(&mut next.learners_next);
        next.learners.sort_unstable();
        next.learners.dedup();
        next.voters_outgoing.clear();
        next.auto_leave = false;
        validate_conf_state(&next, allowed_members)?;
        return Ok(next);
    }

    let enter_joint = change.enter_joint();
    if enter_joint.is_some() {
        if !next.voters_outgoing.is_empty() {
            return Err(ConsensusError::InvalidState(
                "membership is already in a joint state".into(),
            ));
        }
        next.voters_outgoing.clone_from(&next.voters);
    } else if !next.voters_outgoing.is_empty() {
        return Err(ConsensusError::InvalidState(
            "simple membership changes cannot run while membership is joint".into(),
        ));
    }

    let previous_voters = next.voters.iter().copied().collect::<BTreeSet<_>>();
    for step in &change.changes {
        if step.node_id == 0 {
            continue;
        }
        if !allowed_members
            .iter()
            .any(|node_id| node_id.get() == step.node_id)
        {
            return Err(ConsensusError::InvalidVoterSet(format!(
                "membership change references node {} outside the configured allowlist",
                step.node_id
            )));
        }
        apply_step(&mut next, step)?;
    }
    canonicalize_conf_state(&mut next);
    if let Some(auto_leave) = enter_joint {
        next.auto_leave = auto_leave;
    } else {
        let next_voters = next.voters.iter().copied().collect::<BTreeSet<_>>();
        if previous_voters.symmetric_difference(&next_voters).count() > 1 {
            return Err(ConsensusError::InvalidState(
                "more than one voter changed without joint consensus".into(),
            ));
        }
    }
    validate_conf_state(&next, allowed_members)?;
    Ok(next)
}

fn apply_step(state: &mut ConfState, step: &ConfChangeSingle) -> ConsensusResult<()> {
    match step.get_change_type() {
        ConfChangeType::AddNode => {
            insert(&mut state.voters, step.node_id);
            remove(&mut state.learners, step.node_id);
            remove(&mut state.learners_next, step.node_id);
        }
        ConfChangeType::AddLearnerNode => {
            remove(&mut state.voters, step.node_id);
            remove(&mut state.learners, step.node_id);
            remove(&mut state.learners_next, step.node_id);
            if state.voters_outgoing.contains(&step.node_id) {
                insert(&mut state.learners_next, step.node_id);
            } else {
                insert(&mut state.learners, step.node_id);
            }
        }
        ConfChangeType::RemoveNode => {
            remove(&mut state.voters, step.node_id);
            remove(&mut state.learners, step.node_id);
            remove(&mut state.learners_next, step.node_id);
        }
    }
    if state.voters.is_empty() {
        return Err(ConsensusError::InvalidVoterSet(
            "membership change removes every incoming voter".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_allowed_members(allowed_members: &[NodeId]) -> ConsensusResult<()> {
    if allowed_members.len() < MIN_VOTER_COUNT
        || allowed_members.len() > MAX_PROVISIONED_MEMBERS
        || !allowed_members.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(ConsensusError::InvalidVoterSet(format!(
            "configured members must contain {MIN_VOTER_COUNT}..={MAX_PROVISIONED_MEMBERS} sorted unique nodes"
        )));
    }
    Ok(())
}

fn validate_target_voters(
    target_voters: &[NodeId],
    allowed_members: &[NodeId],
) -> ConsensusResult<()> {
    validate_voter_count("target voter set", target_voters.len())?;
    if !target_voters.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(ConsensusError::InvalidVoterSet(
            "target voter IDs must be distinct and strictly sorted".into(),
        ));
    }
    if target_voters
        .iter()
        .any(|node_id| !allowed_members.contains(node_id))
    {
        return Err(ConsensusError::InvalidVoterSet(
            "target voters must belong to the configured member allowlist".into(),
        ));
    }
    Ok(())
}

fn validate_voter_count(label: &str, count: usize) -> ConsensusResult<()> {
    if matches!(count, MIN_VOTER_COUNT | MAX_VOTER_COUNT) {
        Ok(())
    } else {
        Err(ConsensusError::InvalidVoterSet(format!(
            "{label} must contain {MIN_VOTER_COUNT} or {MAX_VOTER_COUNT} nodes; observed {count}"
        )))
    }
}

fn validate_ids(
    label: &str,
    ids: &[u64],
    allowed: &BTreeSet<u64>,
    allow_empty: bool,
) -> ConsensusResult<BTreeSet<u64>> {
    if !allow_empty && ids.is_empty() {
        return Err(ConsensusError::InvalidVoterSet(format!(
            "{label} cannot be empty"
        )));
    }
    if ids.contains(&0) || !ids.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(ConsensusError::InvalidVoterSet(format!(
            "{label} must contain nonzero, sorted, unique node IDs"
        )));
    }
    let values = ids.iter().copied().collect::<BTreeSet<_>>();
    if !values.is_subset(allowed) {
        return Err(ConsensusError::InvalidVoterSet(format!(
            "{label} contains a node outside the configured member allowlist"
        )));
    }
    Ok(values)
}

fn change(change_type: ConfChangeType, node_id: NodeId) -> ConfChangeSingle {
    ConfChangeSingle {
        change_type: change_type as i32,
        node_id: node_id.get(),
    }
}

fn conf_change(changes: Vec<ConfChangeSingle>) -> ConfChangeV2 {
    ConfChangeV2 {
        changes,
        ..ConfChangeV2::default()
    }
}

fn node(node_id: u64) -> NodeId {
    NodeId::new(node_id).expect("validated membership node IDs are nonzero")
}

fn insert(values: &mut Vec<u64>, value: u64) {
    match values.binary_search(&value) {
        Ok(_) => {}
        Err(index) => values.insert(index, value),
    }
}

fn remove(values: &mut Vec<u64>, value: u64) {
    if let Ok(index) = values.binary_search(&value) {
        values.remove(index);
    }
}

pub(crate) fn canonicalize_conf_state(state: &mut ConfState) {
    for values in [
        &mut state.voters,
        &mut state.learners,
        &mut state.voters_outgoing,
        &mut state.learners_next,
    ] {
        values.sort_unstable();
        values.dedup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expansion_requires_learners_then_enters_and_leaves_joint_consensus() {
        let allowed = nodes(1..=5);
        let mut state = stable_state(&[1, 2, 3], &[]);

        assert!(matches!(
            plan_voter_reconfiguration(&state, &allowed, &allowed),
            Err(ConsensusError::InvalidState(_))
        ));

        for learner in [node(4), node(5)] {
            let plan = plan_add_learner(&state, learner, &allowed).unwrap();
            state = apply_conf_change(&state, &plan, &allowed).unwrap();
        }
        assert_eq!(state.learners, vec![4, 5]);

        let plan = plan_voter_reconfiguration(&state, &allowed, &allowed).unwrap();
        state = apply_conf_change(&state, &plan, &allowed).unwrap();
        assert_eq!(state.voters, vec![1, 2, 3, 4, 5]);
        assert_eq!(state.voters_outgoing, vec![1, 2, 3]);
        assert!(state.auto_leave);
        assert!(state.learners.is_empty());

        state = apply_conf_change(&state, &ConfChangeV2::default(), &allowed).unwrap();
        assert_eq!(state, stable_state(&[1, 2, 3, 4, 5], &[]));
    }

    #[test]
    fn replacement_requires_caught_up_learner_and_preserves_three_voters() {
        let allowed = nodes(1..=5);
        let initial = stable_state(&[1, 2, 3], &[4]);
        let target = nodes([1, 2, 4]);
        let plan = plan_voter_reconfiguration(&initial, &target, &allowed).unwrap();
        let joint = apply_conf_change(&initial, &plan, &allowed).unwrap();
        assert_eq!(joint.voters, vec![1, 2, 4]);
        assert_eq!(joint.voters_outgoing, vec![1, 2, 3]);
        assert!(joint.auto_leave);

        let stable = apply_conf_change(&joint, &ConfChangeV2::default(), &allowed).unwrap();
        assert_eq!(stable, stable_state(&[1, 2, 4], &[]));
    }

    #[test]
    fn malformed_membership_and_unsafe_direct_promotion_fail_closed() {
        let allowed = nodes(1..=5);
        let state = stable_state(&[1, 2, 3], &[]);
        assert!(plan_voter_reconfiguration(&state, &nodes([1, 2, 4]), &allowed).is_err());

        let mut malformed = state.clone();
        malformed.learners = vec![3];
        assert!(validate_conf_state(&malformed, &allowed).is_err());

        let even = ConfState {
            voters: vec![1, 2, 3, 4],
            ..ConfState::default()
        };
        assert!(validate_conf_state(&even, &allowed).is_err());
    }

    fn stable_state(voters: &[u64], learners: &[u64]) -> ConfState {
        ConfState {
            voters: voters.to_vec(),
            learners: learners.to_vec(),
            ..ConfState::default()
        }
    }

    fn nodes(values: impl IntoIterator<Item = u64>) -> Vec<NodeId> {
        values.into_iter().map(node).collect()
    }
}
