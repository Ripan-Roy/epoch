//! Shared exact-proposal resolution for leader-owned delivery workers.

use std::time::Duration;

use epoch_consensus::{CommittedProposal, ConsensusError, ProposalLookup};

use crate::consensus::{ConsensusProbeError, ConsensusProbeHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProposalRoute {
    LeaderOnly,
    ForwardToKnownLeader,
}

/// Resolves or commits one deterministic proposal and waits for its exact
/// committed payload. The caller subscribes before lookup/proposal through
/// this function, closing the fast-commit notification race for every target
/// executor.
pub(crate) async fn propose_and_wait(
    consensus: &ConsensusProbeHandle,
    proposal_id: u64,
    expected_term: u64,
    payload: Vec<u8>,
    commit_wait: Duration,
    identity_label: &str,
    route: ProposalRoute,
) -> Result<CommittedProposal, String> {
    let mut commits = consensus.subscribe_commits();
    let lookup = match consensus.lookup(proposal_id).await {
        Ok(ProposalLookup::Committed(committed)) => ProposalLookup::Committed(committed),
        Ok(ProposalLookup::Pending { payload: pending }) => {
            if pending != payload {
                return Err(format!(
                    "{identity_label} proposal identity is already bound to another command"
                ));
            }
            ProposalLookup::Pending { payload: pending }
        }
        Ok(ProposalLookup::Unknown) => {
            let proposed = match route {
                ProposalRoute::LeaderOnly => {
                    consensus
                        .propose(proposal_id, expected_term, payload.clone())
                        .await
                }
                ProposalRoute::ForwardToKnownLeader => {
                    consensus
                        .forward_propose(proposal_id, expected_term, payload.clone())
                        .await
                }
            };
            match proposed {
                Ok(lookup) => lookup,
                Err(ConsensusProbeError::Consensus(
                    ConsensusError::NotLeader { .. }
                    | ConsensusError::StaleTerm { .. }
                    | ConsensusError::DuplicateProposal(_),
                )) => consensus
                    .lookup(proposal_id)
                    .await
                    .map_err(|error| error.to_string())?,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err(error) => return Err(error.to_string()),
    };
    if let ProposalLookup::Committed(committed) = lookup {
        return validate_committed_payload(committed, &payload, identity_label);
    }

    let deadline = tokio::time::Instant::now() + commit_wait;
    loop {
        match tokio::time::timeout_at(deadline, commits.recv()).await {
            Ok(Ok(committed)) if committed.receipt.proposal_id.get() == proposal_id => {
                return validate_committed_payload(committed, &payload, identity_label);
            }
            Ok(Ok(_)) => {}
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                if let ProposalLookup::Committed(committed) = consensus
                    .lookup(proposal_id)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    return validate_committed_payload(committed, &payload, identity_label);
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                return Err("consensus commit notification channel closed".into());
            }
            Err(_) => {
                if let ProposalLookup::Committed(committed) = consensus
                    .lookup(proposal_id)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    return validate_committed_payload(committed, &payload, identity_label);
                }
                return Err(format!(
                    "{identity_label} proposal {proposal_id} did not commit within {} ms",
                    commit_wait.as_millis()
                ));
            }
        }
    }
}

fn validate_committed_payload(
    committed: CommittedProposal,
    expected_payload: &[u8],
    identity_label: &str,
) -> Result<CommittedProposal, String> {
    if committed.payload != expected_payload {
        return Err(format!(
            "{identity_label} proposal identity is already bound to another command"
        ));
    }
    Ok(committed)
}
