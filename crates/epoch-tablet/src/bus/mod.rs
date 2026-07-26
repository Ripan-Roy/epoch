//! Canonical replicated Event Bus route-plan state machine.

mod command;
mod digest;
mod model;

use std::collections::BTreeMap;

use epoch_bus::{ArchivedEvent, BusConfig, EventBus, EventFilter};
use epoch_core::{DurabilityProfile, EpochError, EpochResult};

use crate::common::{AppliedCommand, validate_committed_command_scope};
use crate::{
    AppliedCommandMetadata, CommittedCommand, TabletError, TabletResult, TabletWriteEvidence,
};

pub use command::*;
use digest::{delivery_plan_digest, initial_state_digest, transition_digest};
pub use model::*;

#[derive(Debug)]
pub struct BusTablet {
    scope: BusTabletScope,
    bus: EventBus,
    applied: BTreeMap<u64, AppliedCommand<BusTabletReceipt>>,
    last_applied_command_index: u64,
    last_applied_time_ms: u64,
    business_state_digest: [u8; 32],
    state_digest: [u8; 32],
}

impl BusTablet {
    #[allow(
        clippy::needless_pass_by_value,
        reason = "tablet constructors consistently own their validated profile configuration"
    )]
    pub fn new(scope: BusTabletScope, mut config: BusConfig) -> TabletResult<Self> {
        scope.validate()?;
        // Consensus supplies durability evidence; the standalone engine label
        // cannot truthfully describe a committed tablet mutation.
        config.durability = DurabilityProfile::Volatile;
        let bus = EventBus::new(config)?;
        let business_state_digest = bus.recovery_state_digest()?;
        let state_digest = initial_state_digest(&scope, business_state_digest);
        Ok(Self {
            scope,
            bus,
            applied: BTreeMap::new(),
            last_applied_command_index: 0,
            last_applied_time_ms: 0,
            business_state_digest,
            state_digest,
        })
    }

    pub fn with_default_config(scope: BusTabletScope) -> TabletResult<Self> {
        Self::new(scope, BusConfig::default())
    }

    pub fn scope(&self) -> &BusTabletScope {
        &self.scope
    }

    pub fn apply(&mut self, committed: CommittedCommand<'_>) -> TabletResult<BusTabletReceipt> {
        validate_committed_command_scope(&self.scope, committed)?;
        let metadata = AppliedCommandMetadata::from_committed(committed);
        if let Some(mut receipt) = self.receipt_for_committed(committed)? {
            receipt.disposition = BusTabletDisposition::Replayed;
            return Ok(receipt);
        }
        if committed.log_index <= self.last_applied_command_index {
            return Err(TabletError::CommitOrder {
                previous: self.last_applied_command_index,
                observed: committed.log_index,
            });
        }

        let command = BusTabletCommand::decode(committed.payload, &self.scope)?;
        let expected_proposal_id = command.proposal_id(&self.scope)?;
        if committed.proposal_id != expected_proposal_id {
            return Err(TabletError::InvalidCommand(format!(
                "proposal_id {} does not match idempotency_key hash {expected_proposal_id}",
                committed.proposal_id
            )));
        }
        let applied_at_ms = command.applied_at_ms.max(self.last_applied_time_ms);
        let mut candidate = self.bus.clone();
        let execution = execute(&mut candidate, command.operation, applied_at_ms);
        let (outcome, next_bus) = match execution {
            Ok(result) => (BusTabletOutcome::Applied { result }, Some(candidate)),
            Err(error) => (recordable_rejected_outcome(error)?, None),
        };
        let receipt = BusTabletReceipt {
            proposal_id: committed.proposal_id,
            tablet_id: self.scope.tablet_id,
            tablet_epoch: self.scope.tablet_epoch,
            term: committed.term,
            commit_index: committed.log_index,
            applied_at_ms,
            write_evidence: TabletWriteEvidence::FixedVoterMajorityPersisted,
            durable_voter_acks: 2,
            disposition: BusTabletDisposition::New,
            outcome,
        };

        let effective_bus = next_bus.as_ref().unwrap_or(&self.bus);
        let business_state_digest = effective_bus.recovery_state_digest()?;
        let next_digest = transition_digest(
            self.state_digest,
            committed,
            metadata.payload_digest,
            business_state_digest,
            applied_at_ms,
            &receipt.outcome,
        )?;
        if let Some(next_bus) = next_bus {
            self.bus = next_bus;
        }
        self.business_state_digest = business_state_digest;
        self.state_digest = next_digest;
        self.last_applied_command_index = committed.log_index;
        self.last_applied_time_ms = applied_at_ms;
        self.applied.insert(
            committed.proposal_id,
            AppliedCommand {
                metadata,
                receipt: receipt.clone(),
            },
        );
        Ok(receipt)
    }

    pub fn lookup(&self, proposal_id: u64) -> Option<BusTabletReceipt> {
        self.applied
            .get(&proposal_id)
            .map(|applied| applied.receipt.clone())
    }

    pub fn receipt_for_committed(
        &self,
        committed: CommittedCommand<'_>,
    ) -> TabletResult<Option<BusTabletReceipt>> {
        validate_committed_command_scope(&self.scope, committed)?;
        let Some(previous) = self.applied.get(&committed.proposal_id) else {
            return Ok(None);
        };
        previous.metadata.validate_exact(committed)?;
        Ok(Some(previous.receipt.clone()))
    }

    pub fn replay(
        &self,
        from_ms: u64,
        to_ms: u64,
        filter: Option<&EventFilter>,
        limit: usize,
    ) -> TabletResult<Vec<ArchivedEvent>> {
        Ok(self.bus.replay(from_ms, to_ms, filter, limit)?)
    }

    pub const fn last_applied_command_index(&self) -> u64 {
        self.last_applied_command_index
    }

    pub const fn last_applied_time_ms(&self) -> u64 {
        self.last_applied_time_ms
    }

    pub fn applied_command_count(&self) -> usize {
        self.applied.len()
    }

    pub fn route_plan_version(&self) -> u64 {
        self.bus.route_plan_version()
    }

    pub fn subscription_count(&self) -> usize {
        self.bus.subscription_count()
    }

    pub const fn commit_position(&self) -> u64 {
        self.bus.commit_position()
    }

    pub fn archived_event_count(&self) -> usize {
        self.bus.archived_event_count()
    }

    pub const fn business_state_digest(&self) -> [u8; 32] {
        self.business_state_digest
    }

    pub const fn state_digest(&self) -> [u8; 32] {
        self.state_digest
    }
}

fn execute(
    bus: &mut EventBus,
    operation: BusTabletOperation,
    applied_at_ms: u64,
) -> EpochResult<BusTabletOperationResult> {
    match operation {
        BusTabletOperation::UpsertSubscription { subscription } => {
            let name = subscription.name.clone();
            let replaced = bus.has_subscription(&name);
            let route_plan_version = bus.upsert_subscription(subscription)?;
            Ok(BusTabletOperationResult::SubscriptionUpserted {
                name,
                replaced,
                route_plan_version,
            })
        }
        BusTabletOperation::RemoveSubscription { name } => {
            let removed = bus.remove_subscription(&name)?;
            Ok(BusTabletOperationResult::SubscriptionRemoved {
                name,
                removed,
                route_plan_version: bus.route_plan_version(),
            })
        }
        BusTabletOperation::Publish { envelope } => {
            let result = bus.publish(envelope, applied_at_ms)?;
            Ok(BusTabletOperationResult::Published {
                position: result.acknowledgement.commit_position,
                route_plan_version: bus.route_plan_version(),
                delivery_count: result.deliveries.len(),
                delivery_plan_digest: delivery_plan_digest(&result.deliveries)?,
            })
        }
    }
}

fn recordable_rejected_outcome(error: EpochError) -> TabletResult<BusTabletOutcome> {
    let code = match &error {
        EpochError::AlreadyExists(_) => BusTabletRejectionCode::AlreadyExists,
        EpochError::NotFound(_) => BusTabletRejectionCode::NotFound,
        EpochError::InvalidArgument(_) => BusTabletRejectionCode::InvalidArgument,
        EpochError::Conflict(_) => BusTabletRejectionCode::Conflict,
        EpochError::Fenced => BusTabletRejectionCode::Fenced,
        EpochError::Capacity(_) => BusTabletRejectionCode::Capacity,
        EpochError::Unavailable(_) => BusTabletRejectionCode::Unavailable,
        EpochError::Storage(_) | EpochError::Internal(_) => {
            return Err(TabletError::Profile(error));
        }
    };
    Ok(BusTabletOutcome::Rejected {
        code,
        detail: error.to_string(),
    })
}

#[cfg(test)]
mod tests;
