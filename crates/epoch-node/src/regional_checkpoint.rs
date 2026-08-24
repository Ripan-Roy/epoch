//! Automatic, node-local consensus checkpoints for regional groups.

use std::{
    fmt::{self, Formatter},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use epoch_consensus::ConsensusStatus;
use serde::Serialize;

use crate::{
    consensus::{ConsensusProbeHandle, ConsensusProbeResult},
    tablet_materializer::TabletDirectory,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RegionalCheckpointPass {
    pub groups_examined: u64,
    pub eligible_groups: u64,
    pub checkpoints_created: u64,
    pub compacted_log_entries: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegionalCheckpointGroupObservation {
    pub group_id: String,
    pub group_epoch: String,
    pub applied_index: String,
    pub checkpoint_index: String,
    pub retained_log_first_index: String,
}

impl RegionalCheckpointGroupObservation {
    fn from_status(status: &ConsensusStatus) -> Self {
        Self {
            group_id: status.group_id.get().to_string(),
            group_epoch: status.group_epoch.get().to_string(),
            applied_index: status.applied_index.get().to_string(),
            checkpoint_index: status.checkpoint_index.get().to_string(),
            retained_log_first_index: status.retained_log_first_index.get().to_string(),
        }
    }
}

#[derive(Default)]
pub struct RegionalCheckpointStatus {
    interval_ms: u64,
    min_applied_entries: u64,
    passes: AtomicU64,
    groups_examined: AtomicU64,
    eligible_groups: AtomicU64,
    checkpoints_created: AtomicU64,
    compacted_log_entries: AtomicU64,
    errors: AtomicU64,
    last_pass_at_ms: AtomicU64,
    last_error: RwLock<Option<String>>,
    groups: RwLock<Vec<RegionalCheckpointGroupObservation>>,
}

impl fmt::Debug for RegionalCheckpointStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegionalCheckpointStatus")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl RegionalCheckpointStatus {
    pub fn new(interval_ms: u64, min_applied_entries: u64) -> Arc<Self> {
        Arc::new(Self {
            interval_ms,
            min_applied_entries,
            ..Self::default()
        })
    }

    pub fn record(
        &self,
        now_ms: u64,
        pass: RegionalCheckpointPass,
        mut groups: Vec<RegionalCheckpointGroupObservation>,
        last_error: Option<String>,
    ) {
        groups.sort_by(|left, right| {
            left.group_id
                .parse::<u64>()
                .unwrap_or(u64::MAX)
                .cmp(&right.group_id.parse::<u64>().unwrap_or(u64::MAX))
        });
        self.passes.fetch_add(1, Ordering::Relaxed);
        self.groups_examined
            .fetch_add(pass.groups_examined, Ordering::Relaxed);
        self.eligible_groups
            .fetch_add(pass.eligible_groups, Ordering::Relaxed);
        self.checkpoints_created
            .fetch_add(pass.checkpoints_created, Ordering::Relaxed);
        self.compacted_log_entries
            .fetch_add(pass.compacted_log_entries, Ordering::Relaxed);
        self.errors.fetch_add(pass.errors, Ordering::Relaxed);
        self.last_pass_at_ms.store(now_ms, Ordering::Relaxed);
        if let Ok(mut observed) = self.groups.write() {
            *observed = groups;
        }
        if let Some(last_error) = last_error
            && let Ok(mut error) = self.last_error.write()
        {
            *error = Some(last_error);
        }
    }

    pub fn snapshot(&self) -> RegionalCheckpointStatusSnapshot {
        RegionalCheckpointStatusSnapshot {
            enabled: true,
            interval_ms: self.interval_ms,
            min_applied_entries: self.min_applied_entries,
            passes: self.passes.load(Ordering::Relaxed),
            groups_examined: self.groups_examined.load(Ordering::Relaxed),
            eligible_groups: self.eligible_groups.load(Ordering::Relaxed),
            checkpoints_created: self.checkpoints_created.load(Ordering::Relaxed),
            compacted_log_entries: self.compacted_log_entries.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            last_pass_at_ms: self.last_pass_at_ms.load(Ordering::Relaxed),
            last_error: self.last_error.read().ok().and_then(|error| error.clone()),
            groups: self
                .groups
                .read()
                .map_or_else(|_| Vec::new(), |groups| groups.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegionalCheckpointStatusSnapshot {
    pub enabled: bool,
    pub interval_ms: u64,
    pub min_applied_entries: u64,
    pub passes: u64,
    pub groups_examined: u64,
    pub eligible_groups: u64,
    pub checkpoints_created: u64,
    pub compacted_log_entries: u64,
    pub errors: u64,
    pub last_pass_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub groups: Vec<RegionalCheckpointGroupObservation>,
}

pub async fn run_regional_checkpoint_pass(
    catalog: &ConsensusProbeHandle,
    directory: &TabletDirectory,
    min_applied_entries: u64,
) -> (
    RegionalCheckpointPass,
    Vec<RegionalCheckpointGroupObservation>,
    Option<String>,
) {
    let mut handles = vec![catalog.clone()];
    match directory.routes() {
        Ok(routes) => handles.extend(routes.into_iter().map(|route| route.consensus())),
        Err(error) => {
            return (
                RegionalCheckpointPass {
                    errors: 1,
                    ..RegionalCheckpointPass::default()
                },
                Vec::new(),
                Some(error.to_string()),
            );
        }
    }

    let mut pass = RegionalCheckpointPass::default();
    let mut groups = Vec::with_capacity(handles.len());
    let mut last_error = None;
    for handle in handles {
        pass.groups_examined = pass.groups_examined.saturating_add(1);
        match inspect_and_checkpoint(&handle, min_applied_entries).await {
            Ok((eligible, created, reclaimed, status)) => {
                pass.eligible_groups = pass.eligible_groups.saturating_add(u64::from(eligible));
                pass.checkpoints_created =
                    pass.checkpoints_created.saturating_add(u64::from(created));
                pass.compacted_log_entries = pass.compacted_log_entries.saturating_add(reclaimed);
                groups.push(RegionalCheckpointGroupObservation::from_status(&status));
            }
            Err(error) => {
                pass.errors = pass.errors.saturating_add(1);
                last_error = Some(error.to_string());
            }
        }
    }
    (pass, groups, last_error)
}

async fn inspect_and_checkpoint(
    handle: &ConsensusProbeHandle,
    min_applied_entries: u64,
) -> ConsensusProbeResult<(bool, bool, u64, ConsensusStatus)> {
    let before = handle.status().await?;
    let eligible = checkpoint_is_due(&before, min_applied_entries);
    let checkpoint = handle
        .checkpoint_if_applied_growth(min_applied_entries)
        .await?;
    let reclaimed = checkpoint.as_ref().map_or(0, |created| {
        created
            .index
            .get()
            .saturating_sub(before.checkpoint_index.get())
    });
    let after = if checkpoint.is_some() {
        handle.status().await?
    } else {
        before
    };
    Ok((eligible, checkpoint.is_some(), reclaimed, after))
}

fn checkpoint_is_due(status: &ConsensusStatus, min_applied_entries: u64) -> bool {
    !status.fail_stopped
        && min_applied_entries > 0
        && status
            .applied_index
            .get()
            .saturating_sub(status.checkpoint_index.get())
            >= min_applied_entries
}

#[cfg(test)]
mod tests {
    use epoch_consensus::{
        ConsensusRole, ConsensusStatus, GroupEpoch, GroupId, LogIndex, NodeId, Term,
    };

    use super::*;

    fn consensus_status(applied_index: u64, checkpoint_index: u64) -> ConsensusStatus {
        ConsensusStatus {
            node_id: NodeId::new(2).unwrap(),
            group_id: GroupId::new(7).unwrap(),
            group_epoch: GroupEpoch::new(3).unwrap(),
            role: ConsensusRole::Follower,
            leader_id: Some(NodeId::new(1).unwrap()),
            term: Term::new(4),
            commit_index: LogIndex::new(applied_index),
            applied_index: LogIndex::new(applied_index),
            checkpoint_index: LogIndex::new(checkpoint_index),
            retained_log_first_index: LogIndex::new(checkpoint_index.saturating_add(1).max(1)),
            voter_count: 3,
            replication_progress: Vec::new(),
            fail_stopped: false,
        }
    }

    #[test]
    fn checkpoint_policy_uses_the_exact_applied_growth_boundary_on_every_role() {
        assert!(!checkpoint_is_due(&consensus_status(10, 6), 5));
        assert!(checkpoint_is_due(&consensus_status(11, 6), 5));
        assert!(checkpoint_is_due(&consensus_status(5, 0), 5));

        let mut failed = consensus_status(11, 6);
        failed.fail_stopped = true;
        assert!(!checkpoint_is_due(&failed, 5));
    }

    #[test]
    fn status_accumulates_checkpoint_work_and_publishes_exact_group_boundaries() {
        let status = RegionalCheckpointStatus::new(250, 5);
        status.record(
            10,
            RegionalCheckpointPass {
                groups_examined: 2,
                eligible_groups: 1,
                checkpoints_created: 1,
                compacted_log_entries: 5,
                errors: 1,
            },
            vec![RegionalCheckpointGroupObservation::from_status(
                &consensus_status(11, 11),
            )],
            Some("checkpoint too large".into()),
        );
        status.record(
            20,
            RegionalCheckpointPass {
                groups_examined: 2,
                ..RegionalCheckpointPass::default()
            },
            vec![RegionalCheckpointGroupObservation::from_status(
                &consensus_status(12, 11),
            )],
            None,
        );

        assert_eq!(
            status.snapshot(),
            RegionalCheckpointStatusSnapshot {
                enabled: true,
                interval_ms: 250,
                min_applied_entries: 5,
                passes: 2,
                groups_examined: 4,
                eligible_groups: 1,
                checkpoints_created: 1,
                compacted_log_entries: 5,
                errors: 1,
                last_pass_at_ms: 20,
                last_error: Some("checkpoint too large".into()),
                groups: vec![RegionalCheckpointGroupObservation {
                    group_id: "7".into(),
                    group_epoch: "3".into(),
                    applied_index: "12".into(),
                    checkpoint_index: "11".into(),
                    retained_log_first_index: "12".into(),
                }],
            }
        );
    }
}
