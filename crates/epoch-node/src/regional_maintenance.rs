//! Leader-only, consensus-proposed maintenance for regional profile tablets.

use std::{
    fmt::{self, Formatter},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use epoch_consensus::{ConsensusError, ConsensusRole, ProposalLookup};
use serde::Serialize;

use crate::{consensus::ConsensusProbeError, tablet_materializer::TabletDirectory};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RegionalMaintenanceOperation {
    StreamRetention,
    StreamConsumerSession,
    StreamCapture,
    QueueTimers,
    CacheExpiry,
    BusDeliveryLease,
}

impl RegionalMaintenanceOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StreamRetention => "stream_retention",
            Self::StreamConsumerSession => "stream_consumer_session",
            Self::StreamCapture => "stream_capture",
            Self::QueueTimers => "queue_timers",
            Self::CacheExpiry => "cache_expiry",
            Self::BusDeliveryLease => "bus_delivery_lease",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionalMaintenanceProposal {
    pub operation: RegionalMaintenanceOperation,
    pub due_at_ms: u64,
    pub proposal_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RegionalMaintenancePass {
    pub tablets_examined: u64,
    pub leaders_examined: u64,
    pub due_operations: u64,
    pub proposals_submitted: u64,
    pub pending_operations: u64,
    pub errors: u64,
}

#[derive(Default)]
pub struct RegionalMaintenanceStatus {
    interval_ms: u64,
    passes: AtomicU64,
    tablets_examined: AtomicU64,
    leader_passes: AtomicU64,
    due_operations: AtomicU64,
    proposals_submitted: AtomicU64,
    pending_operations: AtomicU64,
    errors: AtomicU64,
    last_pass_at_ms: AtomicU64,
    last_error: RwLock<Option<String>>,
}

impl fmt::Debug for RegionalMaintenanceStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegionalMaintenanceStatus")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl RegionalMaintenanceStatus {
    pub fn new(interval_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            interval_ms,
            ..Self::default()
        })
    }

    pub fn record(&self, now_ms: u64, pass: RegionalMaintenancePass, last_error: Option<String>) {
        self.passes.fetch_add(1, Ordering::Relaxed);
        self.tablets_examined
            .fetch_add(pass.tablets_examined, Ordering::Relaxed);
        self.leader_passes
            .fetch_add(pass.leaders_examined, Ordering::Relaxed);
        self.due_operations
            .fetch_add(pass.due_operations, Ordering::Relaxed);
        self.proposals_submitted
            .fetch_add(pass.proposals_submitted, Ordering::Relaxed);
        self.pending_operations
            .fetch_add(pass.pending_operations, Ordering::Relaxed);
        self.errors.fetch_add(pass.errors, Ordering::Relaxed);
        self.last_pass_at_ms.store(now_ms, Ordering::Relaxed);
        if let Some(last_error) = last_error
            && let Ok(mut error) = self.last_error.write()
        {
            *error = Some(last_error);
        }
    }

    pub fn snapshot(&self) -> RegionalMaintenanceStatusSnapshot {
        RegionalMaintenanceStatusSnapshot {
            enabled: true,
            interval_ms: self.interval_ms,
            passes: self.passes.load(Ordering::Relaxed),
            tablets_examined: self.tablets_examined.load(Ordering::Relaxed),
            leader_passes: self.leader_passes.load(Ordering::Relaxed),
            due_operations: self.due_operations.load(Ordering::Relaxed),
            proposals_submitted: self.proposals_submitted.load(Ordering::Relaxed),
            pending_operations: self.pending_operations.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            last_pass_at_ms: self.last_pass_at_ms.load(Ordering::Relaxed),
            last_error: self.last_error.read().ok().and_then(|error| error.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegionalMaintenanceStatusSnapshot {
    pub enabled: bool,
    pub interval_ms: u64,
    pub passes: u64,
    pub tablets_examined: u64,
    pub leader_passes: u64,
    pub due_operations: u64,
    pub proposals_submitted: u64,
    pub pending_operations: u64,
    pub errors: u64,
    pub last_pass_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

pub async fn run_regional_maintenance_pass(
    directory: &TabletDirectory,
    now_ms: u64,
) -> (RegionalMaintenancePass, Option<String>) {
    let routes = match directory.routes() {
        Ok(routes) => routes,
        Err(error) => {
            return (
                RegionalMaintenancePass {
                    errors: 1,
                    ..RegionalMaintenancePass::default()
                },
                Some(error.to_string()),
            );
        }
    };
    let mut pass = RegionalMaintenancePass::default();
    let mut last_error = None;
    for route in routes {
        pass.tablets_examined = pass.tablets_examined.saturating_add(1);
        let status = match route.consensus().status().await {
            Ok(status) => status,
            Err(error) => {
                record_error(&mut pass, &mut last_error, error.to_string());
                continue;
            }
        };
        if status.role != ConsensusRole::Leader || status.fail_stopped {
            continue;
        }
        pass.leaders_examined = pass.leaders_examined.saturating_add(1);
        let proposals = match route.maintenance_proposals(now_ms) {
            Ok(proposals) => proposals,
            Err(error) => {
                record_error(&mut pass, &mut last_error, error);
                continue;
            }
        };
        pass.due_operations = pass.due_operations.saturating_add(proposals.len() as u64);
        for proposal in proposals {
            match route.consensus().lookup(proposal.proposal_id).await {
                Ok(ProposalLookup::Committed(_)) => {}
                Ok(ProposalLookup::Pending { .. }) => {
                    pass.pending_operations = pass.pending_operations.saturating_add(1);
                }
                Ok(ProposalLookup::Unknown) => match route
                    .consensus()
                    .propose(proposal.proposal_id, status.term.get(), proposal.payload)
                    .await
                {
                    Ok(_) => {
                        pass.proposals_submitted = pass.proposals_submitted.saturating_add(1);
                    }
                    Err(ConsensusProbeError::Consensus(
                        ConsensusError::NotLeader { .. }
                        | ConsensusError::StaleTerm { .. }
                        | ConsensusError::DuplicateProposal(_),
                    )) => {}
                    Err(error) => record_error(&mut pass, &mut last_error, error.to_string()),
                },
                Err(error) => record_error(&mut pass, &mut last_error, error.to_string()),
            }
        }
    }
    (pass, last_error)
}

fn record_error(
    pass: &mut RegionalMaintenancePass,
    last_error: &mut Option<String>,
    error: String,
) {
    pass.errors = pass.errors.saturating_add(1);
    *last_error = Some(error);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_accumulates_passes_and_preserves_the_latest_error() {
        let status = RegionalMaintenanceStatus::new(100);
        status.record(
            10,
            RegionalMaintenancePass {
                tablets_examined: 4,
                leaders_examined: 2,
                due_operations: 1,
                proposals_submitted: 1,
                errors: 1,
                ..RegionalMaintenancePass::default()
            },
            Some("route unavailable".into()),
        );
        status.record(
            20,
            RegionalMaintenancePass {
                tablets_examined: 4,
                leaders_examined: 1,
                pending_operations: 1,
                ..RegionalMaintenancePass::default()
            },
            None,
        );

        assert_eq!(
            status.snapshot(),
            RegionalMaintenanceStatusSnapshot {
                enabled: true,
                interval_ms: 100,
                passes: 2,
                tablets_examined: 8,
                leader_passes: 3,
                due_operations: 1,
                proposals_submitted: 1,
                pending_operations: 1,
                errors: 1,
                last_pass_at_ms: 20,
                last_error: Some("route unavailable".into()),
            }
        );
    }
}
