use std::collections::BTreeSet;

use epoch_bus::{EpochTargetDestination, EpochTargetKind};
use epoch_core::{DurabilityProfile, EventEnvelope};
use epoch_queue::{
    QueueAdvancedConfig, QueueConfig, QueueIngress, QueueOverflowPolicy, RetryPolicy,
};
use epoch_tablet::{
    CommittedCommand, QUEUE_TABLET_COMMAND_FORMAT_VERSION, QueueAdvancedEnqueueCommand,
    QueueBindDeadLetterForwardCommand, QueueCompleteDeadLetterForwardCommand,
    QueueSessionAcquireCommand, QueueTablet, QueueTabletCommand, QueueTabletOperation,
    QueueTabletOperationResult, QueueTabletOutcome, QueueTabletScope,
};
use serde_json::json;

fn scope() -> QueueTabletScope {
    QueueTabletScope::new(41, 6, "advanced-jobs").expect("scope")
}

fn config() -> QueueConfig {
    QueueConfig {
        durability: DurabilityProfile::QuorumDurable,
        visibility_timeout_ms: 50,
        max_messages: 10,
        retry: RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::default()
        },
        dedupe_window_ms: Some(100),
        advanced: Some(QueueAdvancedConfig {
            max_active_bytes: Some(512 * 1024),
            overflow: QueueOverflowPolicy::DeadLetterOldest,
            idle_expiry_ms: Some(10_000),
            priority_aging_interval_ms: Some(10),
            dispatch: None,
            dead_letter_target: Some("failed-jobs".into()),
        }),
    }
}

fn event(id: &str) -> EventEnvelope {
    let mut envelope = EventEnvelope::new("tests", "job.created", json!({"id": id}), 1);
    envelope.id = id.into();
    envelope
}

fn command(key: &str, at: u64, operation: QueueTabletOperation) -> QueueTabletCommand {
    QueueTabletCommand::new(&scope(), key, at, operation).expect("command")
}

fn apply(
    tablet: &mut QueueTablet,
    command: &QueueTabletCommand,
    term: u64,
    index: u64,
) -> epoch_tablet::QueueTabletReceipt {
    let payload = command.encode(&scope()).expect("encode");
    let proposal_id = command.proposal_id(&scope()).expect("proposal");
    tablet
        .apply(CommittedCommand {
            group_id: scope().tablet_id,
            group_epoch: scope().tablet_epoch,
            proposal_id,
            term,
            log_index: index,
            payload: &payload,
        })
        .expect("apply")
}

#[test]
fn v3_session_command_is_canonical_and_v1_remains_readable() {
    let session = command(
        "session-acquire",
        12,
        QueueTabletOperation::AcquireSession(QueueSessionAcquireCommand {
            partition: 0,
            session_id: "account-7".into(),
            consumer: "worker-a".into(),
            consumer_epoch: 3,
            credit: 2,
            max_in_flight: 5,
            visibility_timeout_ms: Some(30_000),
            session_lock_token: None,
        }),
    );
    assert_eq!(QUEUE_TABLET_COMMAND_FORMAT_VERSION, 3);
    assert_eq!(session.format_version, 3);
    let encoded = session.encode(&scope()).expect("encode");
    assert_eq!(
        String::from_utf8(encoded.clone()).expect("utf8"),
        r#"{"format_version":3,"tablet_id":41,"tablet_epoch":6,"resource":"advanced-jobs","idempotency_key":"session-acquire","applied_at_ms":12,"operation":{"kind":"acquire_session","partition":0,"session_id":"account-7","consumer":"worker-a","consumer_epoch":3,"credit":2,"max_in_flight":5,"visibility_timeout_ms":30000}}"#
    );
    assert_eq!(
        QueueTabletCommand::decode(&encoded, &scope()).expect("decode"),
        session
    );

    let legacy = QueueTabletCommand::enqueue(&scope(), "legacy", event("legacy"), 1)
        .expect("legacy command");
    assert_eq!(legacy.format_version, 1);
    QueueTabletCommand::decode(&legacy.encode(&scope()).expect("legacy encode"), &scope())
        .expect("legacy decode");
}

#[test]
fn pending_dead_letter_forward_blocks_idle_expiry_during_other_commands() {
    let mut tablet = QueueTablet::new(scope(), config()).expect("tablet");
    apply(
        &mut tablet,
        &QueueTabletCommand::enqueue(&scope(), "poison-enqueue", event("poison"), 1)
            .expect("enqueue"),
        2,
        1,
    );
    let acquired = apply(
        &mut tablet,
        &command(
            "poison-acquire",
            2,
            QueueTabletOperation::Acquire(epoch_tablet::QueueAcquireCommand {
                partition: 0,
                consumer: "worker-a".into(),
                consumer_epoch: 1,
                max_messages: 1,
                visibility_timeout_ms: None,
            }),
        ),
        2,
        2,
    );
    let QueueTabletOutcome::Applied {
        result: QueueTabletOperationResult::Acquired { deliveries, .. },
    } = acquired.outcome
    else {
        panic!("expected acquisition");
    };
    apply(
        &mut tablet,
        &command(
            "poison-reject",
            3,
            QueueTabletOperation::Reject(epoch_tablet::QueueRejectCommand {
                partition: 0,
                consumer: "worker-a".into(),
                consumer_epoch: 1,
                lease_token: deliveries[0].lease_token.clone(),
                reason: "poison".into(),
            }),
        ),
        2,
        3,
    );
    assert_eq!(tablet.pending_dead_letter_forwards(10).len(), 1);

    let late = apply(
        &mut tablet,
        &command(
            "late-enqueue",
            20_000,
            QueueTabletOperation::EnqueueAdvanced(Box::new(QueueAdvancedEnqueueCommand {
                partition: 0,
                ingress: QueueIngress::new(event("late")),
            })),
        ),
        2,
        4,
    );
    assert!(matches!(
        late.outcome,
        QueueTabletOutcome::Applied {
            result: QueueTabletOperationResult::Enqueued { .. }
        }
    ));
    assert!(!tablet.advanced_status().state.expired);
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one recovery scenario proves session metadata and the complete durable outbox lifecycle"
)]
fn session_state_and_dead_letter_outbox_survive_snapshot_and_fence_completion() {
    let mut tablet = QueueTablet::new(scope(), config()).expect("tablet");
    let ingress = QueueIngress::new(event("session-one"))
        .with_session("account-7")
        .with_correlation("request-7")
        .with_reply_to("reply-temporary");
    apply(
        &mut tablet,
        &command(
            "advanced-enqueue",
            1,
            QueueTabletOperation::EnqueueAdvanced(Box::new(QueueAdvancedEnqueueCommand {
                partition: 0,
                ingress,
            })),
        ),
        2,
        1,
    );
    assert_eq!(tablet.correlation("request-7").len(), 1);

    let session = apply(
        &mut tablet,
        &command(
            "session-acquire",
            2,
            QueueTabletOperation::AcquireSession(QueueSessionAcquireCommand {
                partition: 0,
                session_id: "account-7".into(),
                consumer: "worker-a".into(),
                consumer_epoch: 1,
                credit: 1,
                max_in_flight: 2,
                visibility_timeout_ms: Some(30),
                session_lock_token: None,
            }),
        ),
        2,
        2,
    );
    let QueueTabletOutcome::Applied {
        result:
            QueueTabletOperationResult::SessionAcquired {
                deliveries,
                session_lock_token,
                ..
            },
    } = session.outcome
    else {
        panic!("expected session acquisition")
    };
    assert_eq!(deliveries[0].message_id, "session-one");
    assert_eq!(
        deliveries[0]
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.reply_to.as_deref()),
        Some("reply-temporary")
    );
    assert!(!session_lock_token.is_empty());

    apply(
        &mut tablet,
        &QueueTabletCommand::enqueue(&scope(), "poison-enqueue", event("poison"), 3)
            .expect("enqueue poison"),
        2,
        3,
    );
    let acquired = apply(
        &mut tablet,
        &command(
            "poison-acquire",
            4,
            QueueTabletOperation::Acquire(epoch_tablet::QueueAcquireCommand {
                partition: 0,
                consumer: "worker-b".into(),
                consumer_epoch: 1,
                max_messages: 1,
                visibility_timeout_ms: Some(30),
            }),
        ),
        2,
        4,
    );
    let QueueTabletOutcome::Applied {
        result: QueueTabletOperationResult::Acquired { deliveries, .. },
    } = acquired.outcome
    else {
        panic!("expected ordinary acquisition")
    };
    let poison_token = deliveries[0].lease_token.clone();
    apply(
        &mut tablet,
        &command(
            "poison-reject",
            5,
            QueueTabletOperation::Reject(epoch_tablet::QueueRejectCommand {
                partition: 0,
                consumer: "worker-b".into(),
                consumer_epoch: 1,
                lease_token: poison_token,
                reason: "poison".into(),
            }),
        ),
        2,
        5,
    );
    let forward = tablet.pending_dead_letter_forwards(10).remove(0);
    assert_eq!(forward.dead_letter_history_id, 1);
    assert_eq!(forward.target, "failed-jobs");

    // Retry retention is optional evidence; an empty set keeps this snapshot
    // focused on the complete business state and outbox.
    let retained = BTreeSet::new();
    let snapshot = tablet.encode_snapshot(&retained).expect("snapshot");
    let mut restored = QueueTablet::decode_snapshot(&scope(), &snapshot).expect("restore");
    assert_eq!(restored.pending_dead_letter_forwards(10), vec![forward]);

    let destination =
        EpochTargetDestination::new(EpochTargetKind::Queue, "failed-jobs", 9, 0, 52, 3)
            .expect("destination");
    apply(
        &mut restored,
        &command(
            "bind-forward-1",
            6,
            QueueTabletOperation::BindDeadLetterForward(QueueBindDeadLetterForwardCommand {
                partition: 0,
                dead_letter_history_id: 1,
                destination: destination.clone(),
            }),
        ),
        2,
        6,
    );
    apply(
        &mut restored,
        &command(
            "complete-forward-1",
            7,
            QueueTabletOperation::CompleteDeadLetterForward(
                QueueCompleteDeadLetterForwardCommand {
                    partition: 0,
                    dead_letter_history_id: 1,
                    destination,
                    target_message_id: "poison".into(),
                },
            ),
        ),
        2,
        7,
    );
    assert!(restored.pending_dead_letter_forwards(10).is_empty());
}
