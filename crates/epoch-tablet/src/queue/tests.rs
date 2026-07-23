use epoch_core::{DurabilityProfile, EventEnvelope};
use epoch_queue::{
    BackoffStrategy, FencedLeaseTokenMetadata, MAX_FENCED_LEASE_TOKEN_BYTES, QueueConfig,
    QueueState, RetryPolicy,
};
use serde_json::{Value, json};

use super::*;

fn scope() -> QueueTabletScope {
    QueueTabletScope::new(7, 3, "jobs").unwrap()
}

fn config() -> QueueConfig {
    QueueConfig {
        durability: DurabilityProfile::QuorumDurable,
        visibility_timeout_ms: 50,
        max_messages: 100,
        retry: RetryPolicy {
            strategy: BackoffStrategy::Fixed,
            initial_delay_ms: 0,
            max_delay_ms: 0,
            jitter_percent: 0,
            max_attempts: 1,
            max_age_ms: None,
        },
        dedupe_window_ms: Some(1_000),
    }
}

fn event(id: &str, time_ms: u64) -> EventEnvelope {
    let mut envelope = EventEnvelope::new("tests", "job.created", json!({"id": id}), time_ms);
    envelope.id = id.into();
    envelope
}

fn command(key: &str, applied_at_ms: u64, operation: QueueTabletOperation) -> QueueTabletCommand {
    QueueTabletCommand::new(&scope(), key, applied_at_ms, operation).unwrap()
}

fn committed(proposal_id: u64, term: u64, log_index: u64, payload: &[u8]) -> CommittedCommand<'_> {
    CommittedCommand {
        group_id: 7,
        group_epoch: 3,
        proposal_id,
        term,
        log_index,
        payload,
    }
}

fn apply_command(
    tablet: &mut QueueTablet,
    command: &QueueTabletCommand,
    term: u64,
    log_index: u64,
) -> QueueTabletReceipt {
    let payload = command.encode(&scope()).unwrap();
    let proposal_id = command.proposal_id(&scope()).unwrap();
    tablet
        .apply(committed(proposal_id, term, log_index, &payload))
        .unwrap()
}

fn apply_all(
    tablets: &mut [QueueTablet; 3],
    command: &QueueTabletCommand,
    term: u64,
    log_index: u64,
) -> Vec<QueueTabletReceipt> {
    let payload = command.encode(&scope()).unwrap();
    let proposal_id = command.proposal_id(&scope()).unwrap();
    tablets
        .iter_mut()
        .map(|tablet| {
            tablet
                .apply(committed(proposal_id, term, log_index, &payload))
                .unwrap()
        })
        .collect()
}

fn acquired_delivery(receipt: &QueueTabletReceipt) -> &QueueTabletDelivery {
    let QueueTabletOutcome::Applied {
        result: QueueTabletOperationResult::Acquired { deliveries, .. },
    } = &receipt.outcome
    else {
        panic!("expected an acquired result: {receipt:?}");
    };
    deliveries.first().expect("one delivery")
}

fn assert_rejected(receipt: &QueueTabletReceipt, expected: QueueTabletRejectionCode) {
    assert!(matches!(
        &receipt.outcome,
        QueueTabletOutcome::Rejected { code, .. } if *code == expected
    ));
}

fn settlement_at(now_ms: u64) -> QueueTabletReceipt {
    let mut tablet = QueueTablet::new(scope(), config()).unwrap();
    let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", 1), 1).unwrap();
    apply_command(&mut tablet, &enqueue, 2, 1);
    let acquire = command(
        "acquire",
        10,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(10),
        }),
    );
    let token = acquired_delivery(&apply_command(&mut tablet, &acquire, 2, 2))
        .lease_token
        .clone();
    let acknowledge = command(
        "ack",
        now_ms,
        QueueTabletOperation::Acknowledge(QueueAcknowledgeCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            lease_token: token,
        }),
    );
    apply_command(&mut tablet, &acknowledge, 2, 3)
}

#[test]
fn command_codec_is_strict_canonical_bounded_and_golden() {
    let command = QueueTabletCommand::enqueue(&scope(), "request-1", event("one", 10), 11).unwrap();
    let encoded = command.encode(&scope()).unwrap();
    assert_eq!(
        String::from_utf8(encoded.clone()).unwrap(),
        r#"{"format_version":1,"tablet_id":7,"tablet_epoch":3,"resource":"jobs","idempotency_key":"request-1","applied_at_ms":11,"operation":{"kind":"enqueue","partition":0,"envelope":{"id":"one","source":"tests","type":"job.created","time_ms":10,"headers":{},"content_type":"application/json","payload":{"id":"one"},"priority":0,"extensions":{}}}}"#
    );

    let pretty = serde_json::to_vec_pretty(&command).unwrap();
    assert!(matches!(
        QueueTabletCommand::decode(&pretty, &scope()),
        Err(TabletError::Decoding(_))
    ));

    let mut document: Value = serde_json::from_slice(&encoded).unwrap();
    document["operation"]["unknown"] = json!(true);
    assert!(matches!(
        QueueTabletCommand::decode(&serde_json::to_vec(&document).unwrap(), &scope()),
        Err(TabletError::Decoding(_))
    ));

    document["operation"]
        .as_object_mut()
        .unwrap()
        .remove("unknown");
    document["format_version"] = json!(99);
    assert!(matches!(
        QueueTabletCommand::decode(&serde_json::to_vec(&document).unwrap(), &scope()),
        Err(TabletError::InvalidCommand(_))
    ));
    assert!(matches!(
        QueueTabletCommand::decode(&vec![b'x'; MAX_QUEUE_TABLET_COMMAND_BYTES + 1], &scope()),
        Err(TabletError::InvalidCommand(_))
    ));
}

#[test]
fn maximum_message_and_consumer_fit_a_fenced_token_and_poison_ids_fail_early() {
    let maximum_id = "m".repeat(MAX_QUEUE_MESSAGE_ID_BYTES);
    let maximum_consumer = "c".repeat(MAX_QUEUE_CONSUMER_BYTES);
    let mut tablet = QueueTablet::new(scope(), config()).unwrap();
    let enqueue =
        QueueTabletCommand::enqueue(&scope(), "enqueue-max", event(&maximum_id, 1), 1).unwrap();
    apply_command(&mut tablet, &enqueue, 2, 1);
    let acquire = command(
        "acquire-max",
        2,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: maximum_consumer,
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(10),
        }),
    );
    let delivery = apply_command(&mut tablet, &acquire, 2, 2);
    assert!(acquired_delivery(&delivery).lease_token.len() <= MAX_FENCED_LEASE_TOKEN_BYTES);

    let too_large = "x".repeat(MAX_QUEUE_MESSAGE_ID_BYTES + 1);
    assert!(matches!(
        QueueTabletCommand::enqueue(&scope(), "too-large", event(&too_large, 3), 3),
        Err(TabletError::InvalidCommand(_))
    ));
    assert!(matches!(
        QueueTabletCommand::enqueue(&scope(), "control", event("bad\nid", 3), 3),
        Err(TabletError::InvalidCommand(_))
    ));
}

#[test]
fn committed_queue_history_converges_on_three_voters() {
    let mut tablets = [
        QueueTablet::new(scope(), config()).unwrap(),
        QueueTablet::new(scope(), config()).unwrap(),
        QueueTablet::new(scope(), config()).unwrap(),
    ];
    let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", 10), 10).unwrap();
    let enqueue_receipts = apply_all(&mut tablets, &enqueue, 2, 1);
    assert!(enqueue_receipts.windows(2).all(|pair| pair[0] == pair[1]));

    let acquire = command(
        "acquire-1",
        11,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(50),
        }),
    );
    let acquire_receipts = apply_all(&mut tablets, &acquire, 2, 2);
    assert!(acquire_receipts.windows(2).all(|pair| pair[0] == pair[1]));
    let token = acquired_delivery(&acquire_receipts[0]).lease_token.clone();
    let metadata = FencedLeaseTokenMetadata::parse(&token).unwrap();
    assert_eq!(metadata.fence(), LeaseFence::new(7, 3, 0, 2, 1).unwrap());
    assert_eq!(metadata.consumer(), "worker");

    let reject = command(
        "reject",
        12,
        QueueTabletOperation::Reject(QueueRejectCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            lease_token: token,
            reason: "poison".into(),
        }),
    );
    apply_all(&mut tablets, &reject, 2, 3);
    let history_id = tablets[0].active_dead_letter_history_id("one").unwrap();
    assert_eq!(history_id, 1);

    let redrive = command(
        "redrive",
        13,
        QueueTabletOperation::Redrive(QueueRedriveCommand {
            partition: 0,
            message_id: "one".into(),
            dead_letter_history_id: history_id,
        }),
    );
    apply_all(&mut tablets, &redrive, 2, 4);

    let acquire_again = command(
        "acquire-2",
        14,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(50),
        }),
    );
    let second_receipts = apply_all(&mut tablets, &acquire_again, 2, 5);
    let second_token = acquired_delivery(&second_receipts[0]).lease_token.clone();
    let acknowledge = command(
        "ack",
        15,
        QueueTabletOperation::Acknowledge(QueueAcknowledgeCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            lease_token: second_token,
        }),
    );
    apply_all(&mut tablets, &acknowledge, 2, 6);

    let expected_digest = tablets[0].state_digest();
    let expected_checksum = tablets[0].queue_recovery_state_checksum();
    let expected_dead_letters = tablets[0].dead_letter_history(10);
    let expected_redrives = tablets[0].redrive_history(10);
    for tablet in &tablets[1..] {
        assert_eq!(tablet.state_digest(), expected_digest);
        assert_eq!(tablet.queue_recovery_state_checksum(), expected_checksum);
        assert_eq!(tablet.dead_letter_history(10), expected_dead_letters);
        assert_eq!(tablet.redrive_history(10), expected_redrives);
        assert_eq!(tablet.counts(), tablets[0].counts());
    }
    assert_eq!(tablets[0].counts().acknowledged, 1);
    assert_eq!(expected_dead_letters.len(), 1);
    assert_eq!(expected_redrives.len(), 1);
    assert!(tablets[0].active_dead_letter_history_id("one").is_none());
}

#[test]
fn exact_lease_renewal_replay_returns_the_rotated_token_without_mutation() {
    let mut tablet = QueueTablet::new(scope(), config()).unwrap();
    let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", 10), 10).unwrap();
    apply_command(&mut tablet, &enqueue, 2, 1);
    let acquire = command(
        "acquire",
        11,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(100),
        }),
    );
    let acquired = apply_command(&mut tablet, &acquire, 2, 2);
    let original_token = acquired_delivery(&acquired).lease_token.clone();
    let extend = command(
        "extend",
        12,
        QueueTabletOperation::ExtendLease(QueueExtendLeaseCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            lease_token: original_token,
            extension_ms: 200,
        }),
    );
    let payload = extend.encode(&scope()).unwrap();
    let proposal_id = extend.proposal_id(&scope()).unwrap();
    let commit = committed(proposal_id, 2, 3, &payload);
    let original = tablet.apply(commit).unwrap();
    let digest = tablet.state_digest();
    let replayed = tablet.apply(commit).unwrap();
    assert_eq!(replayed.disposition, QueueTabletDisposition::Replayed);
    assert_eq!(replayed.outcome, original.outcome);
    assert_eq!(tablet.state_digest(), digest);
    assert_eq!(tablet.applied_command_count(), 3);
    let QueueTabletOutcome::Applied {
        result:
            QueueTabletOperationResult::LeaseExtended {
                lease_token,
                lease_deadline_ms,
                ..
            },
    } = replayed.outcome
    else {
        panic!("expected renewal");
    };
    assert_eq!(lease_deadline_ms, 212);
    assert_eq!(
        FencedLeaseTokenMetadata::parse(&lease_token)
            .unwrap()
            .lease_deadline_ms(),
        212
    );
}

#[test]
fn stale_consumer_epoch_and_leader_term_are_committed_fenced_rejections() {
    let mut stale_term = QueueTablet::new(scope(), config()).unwrap();
    let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", 10), 10).unwrap();
    apply_command(&mut stale_term, &enqueue, 2, 1);
    let acquire = command(
        "acquire",
        11,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(50),
        }),
    );
    let acquired = apply_command(&mut stale_term, &acquire, 2, 2);
    let token = acquired_delivery(&acquired).lease_token.clone();
    let stale_leader_ack = command(
        "stale-leader",
        12,
        QueueTabletOperation::Acknowledge(QueueAcknowledgeCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            lease_token: token.clone(),
        }),
    );
    let rejected = apply_command(&mut stale_term, &stale_leader_ack, 3, 3);
    assert_rejected(&rejected, QueueTabletRejectionCode::Fenced);
    assert_eq!(stale_term.counts().in_flight, 1);

    let mut stale_consumer = QueueTablet::new(scope(), config()).unwrap();
    apply_command(&mut stale_consumer, &enqueue, 2, 1);
    let acquired = apply_command(&mut stale_consumer, &acquire, 2, 2);
    let token = acquired_delivery(&acquired).lease_token.clone();
    let advance_epoch = command(
        "advance-epoch",
        12,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 2,
            max_messages: 1,
            visibility_timeout_ms: Some(50),
        }),
    );
    apply_command(&mut stale_consumer, &advance_epoch, 2, 3);
    assert_eq!(stale_consumer.consumer_epoch("worker"), Some(2));
    let stale_epoch_ack = command(
        "stale-consumer",
        13,
        QueueTabletOperation::Acknowledge(QueueAcknowledgeCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            lease_token: token,
        }),
    );
    let payload = stale_epoch_ack.encode(&scope()).unwrap();
    let proposal_id = stale_epoch_ack.proposal_id(&scope()).unwrap();
    let commit = committed(proposal_id, 2, 4, &payload);
    let rejected = stale_consumer.apply(commit).unwrap();
    assert_rejected(&rejected, QueueTabletRejectionCode::Fenced);
    let digest = stale_consumer.state_digest();
    let replayed = stale_consumer.apply(commit).unwrap();
    assert_eq!(replayed.disposition, QueueTabletDisposition::Replayed);
    assert_eq!(replayed.outcome, rejected.outcome);
    assert_eq!(stale_consumer.state_digest(), digest);
    assert_eq!(stale_consumer.last_applied_time_ms(), 13);
}

#[test]
fn dead_letter_and_redrive_audits_are_immutable_and_stale_ids_are_fenced() {
    let mut tablet = QueueTablet::new(scope(), config()).unwrap();
    let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", 10), 10).unwrap();
    apply_command(&mut tablet, &enqueue, 2, 1);
    let acquire = command(
        "acquire-1",
        11,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(50),
        }),
    );
    let token = acquired_delivery(&apply_command(&mut tablet, &acquire, 2, 2))
        .lease_token
        .clone();
    let nack = command(
        "nack-1",
        12,
        QueueTabletOperation::Nack(QueueNackCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            lease_token: token,
            reason: "failed".into(),
        }),
    );
    apply_command(&mut tablet, &nack, 2, 3);
    let first = tablet.dead_letter_history(10)[0].clone();
    assert_eq!(first.history_id, 1);
    assert_eq!(
        first.source_proposal_id,
        nack.proposal_id(&scope()).unwrap()
    );

    let redrive = command(
        "redrive-1",
        13,
        QueueTabletOperation::Redrive(QueueRedriveCommand {
            partition: 0,
            message_id: "one".into(),
            dead_letter_history_id: 1,
        }),
    );
    apply_command(&mut tablet, &redrive, 2, 4);
    assert_eq!(tablet.dead_letter_history(10)[0], first);
    assert_eq!(tablet.redrive_history(10).len(), 1);
    assert_eq!(tablet.redrive_history(10)[0].dead_letter_history_id, 1);

    let acquire_again = command(
        "acquire-2",
        14,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(50),
        }),
    );
    let token = acquired_delivery(&apply_command(&mut tablet, &acquire_again, 2, 5))
        .lease_token
        .clone();
    let nack_again = command(
        "nack-2",
        15,
        QueueTabletOperation::Nack(QueueNackCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            lease_token: token,
            reason: "failed-again".into(),
        }),
    );
    apply_command(&mut tablet, &nack_again, 2, 6);
    assert_eq!(tablet.active_dead_letter_history_id("one"), Some(2));
    assert_eq!(tablet.dead_letter_history(10).len(), 2);
    assert_eq!(tablet.dead_letter_history(10)[0], first);

    let stale_redrive = command(
        "stale-redrive",
        16,
        QueueTabletOperation::Redrive(QueueRedriveCommand {
            partition: 0,
            message_id: "one".into(),
            dead_letter_history_id: 1,
        }),
    );
    let rejected = apply_command(&mut tablet, &stale_redrive, 2, 7);
    assert_rejected(&rejected, QueueTabletRejectionCode::Fenced);
    assert_eq!(tablet.active_dead_letter_history_id("one"), Some(2));
    assert_eq!(tablet.redrive_history(10).len(), 1);
}

#[test]
fn applied_time_is_monotonic_equal_and_regressed_assignments_are_clamped() {
    let mut tablet = QueueTablet::new(scope(), config()).unwrap();
    let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", 100), 100).unwrap();
    apply_command(&mut tablet, &enqueue, 2, 1);
    let equal = command(
        "equal",
        100,
        QueueTabletOperation::Maintain(QueueMaintainCommand { partition: 0 }),
    );
    apply_command(&mut tablet, &equal, 2, 2);
    let regression = command(
        "regression",
        99,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(50),
        }),
    );
    let digest_before_regression = tablet.state_digest();
    let applied_before_regression = tablet.applied_command_count();
    let receipt = apply_command(&mut tablet, &regression, 2, 3);
    assert_eq!(receipt.applied_at_ms, 100);
    assert_eq!(acquired_delivery(&receipt).lease_deadline_ms, 150);
    assert_eq!(tablet.last_applied_time_ms(), 100);
    assert_eq!(tablet.last_applied_command_index(), 3);
    assert_ne!(tablet.state_digest(), digest_before_regression);
    assert_eq!(
        tablet.applied_command_count(),
        applied_before_regression + 1
    );

    let replayed = apply_command(&mut tablet, &regression, 2, 3);
    assert_eq!(replayed.applied_at_ms, 100);
    assert_eq!(acquired_delivery(&replayed).lease_deadline_ms, 150);
    assert_eq!(replayed.disposition, QueueTabletDisposition::Replayed);

    let later = command(
        "later",
        101,
        QueueTabletOperation::Maintain(QueueMaintainCommand { partition: 0 }),
    );
    assert_eq!(apply_command(&mut tablet, &later, 3, 4).applied_at_ms, 101);
    assert_eq!(tablet.last_applied_time_ms(), 101);
}

#[test]
fn lease_deadline_is_exclusive() {
    assert!(matches!(
        settlement_at(19).outcome,
        QueueTabletOutcome::Applied {
            result: QueueTabletOperationResult::Acknowledged { .. }
        }
    ));
    assert_rejected(&settlement_at(20), QueueTabletRejectionCode::Fenced);
}

#[test]
fn rejected_deadline_settlement_is_atomic_then_maintenance_redelivers() {
    let mut retrying = config();
    retrying.retry.max_attempts = 3;
    let mut tablet = QueueTablet::new(scope(), retrying).unwrap();
    let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", 1), 1).unwrap();
    apply_command(&mut tablet, &enqueue, 2, 1);
    let acquire = command(
        "acquire",
        10,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(10),
        }),
    );
    let token = acquired_delivery(&apply_command(&mut tablet, &acquire, 2, 2))
        .lease_token
        .clone();
    let checksum_before = tablet.queue_recovery_state_checksum();
    let counts_before = tablet.counts();
    let history_before = tablet.dead_letter_history(10);
    let acknowledge = command(
        "ack-at-deadline",
        20,
        QueueTabletOperation::Acknowledge(QueueAcknowledgeCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            lease_token: token,
        }),
    );
    let rejected = apply_command(&mut tablet, &acknowledge, 2, 3);
    assert_rejected(&rejected, QueueTabletRejectionCode::Fenced);
    assert_eq!(tablet.queue_recovery_state_checksum(), checksum_before);
    assert_eq!(tablet.counts(), counts_before);
    assert_eq!(tablet.dead_letter_history(10), history_before);
    assert_eq!(tablet.last_applied_time_ms(), 20);

    let maintain = command(
        "maintain",
        20,
        QueueTabletOperation::Maintain(QueueMaintainCommand { partition: 0 }),
    );
    apply_command(&mut tablet, &maintain, 2, 4);
    assert_eq!(tablet.counts().ready, 1);
    assert_eq!(tablet.counts().in_flight, 0);
    let reacquire = command(
        "reacquire",
        20,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(10),
        }),
    );
    let redelivered = apply_command(&mut tablet, &reacquire, 2, 5);
    assert_eq!(acquired_delivery(&redelivered).attempt, 2);
}

#[test]
fn a_new_leader_waits_for_old_lease_deadline_then_issues_its_own_fence() {
    let mut retrying = config();
    retrying.retry.max_attempts = 3;
    let mut tablet = QueueTablet::new(scope(), retrying).unwrap();
    let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", 1), 1).unwrap();
    apply_command(&mut tablet, &enqueue, 2, 1);
    let acquire = command(
        "term-2-acquire",
        10,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(10),
        }),
    );
    apply_command(&mut tablet, &acquire, 2, 2);

    let before_deadline = command(
        "term-3-before",
        19,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(10),
        }),
    );
    let empty = apply_command(&mut tablet, &before_deadline, 3, 3);
    let QueueTabletOutcome::Applied {
        result: QueueTabletOperationResult::Acquired { deliveries, .. },
    } = empty.outcome
    else {
        panic!("expected acquire result");
    };
    assert!(deliveries.is_empty());

    let at_deadline = command(
        "term-3-deadline",
        20,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(10),
        }),
    );
    let redelivered = apply_command(&mut tablet, &at_deadline, 3, 4);
    let delivery = acquired_delivery(&redelivered);
    assert_eq!(delivery.attempt, 2);
    assert_eq!(
        FencedLeaseTokenMetadata::parse(&delivery.lease_token)
            .unwrap()
            .fence()
            .leader_term(),
        3
    );
}

#[test]
fn terminal_deadlines_win_over_scheduled_readiness() {
    for use_ttl in [true, false] {
        let mut expiring = config();
        expiring.retry.max_attempts = 3;
        expiring.retry.max_age_ms = (!use_ttl).then_some(10);
        let mut tablet = QueueTablet::new(scope(), expiring).unwrap();
        let mut scheduled = event(if use_ttl { "ttl" } else { "max-age" }, 10);
        scheduled.deliver_at_ms = Some(20);
        scheduled.ttl_ms = use_ttl.then_some(10);
        let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", scheduled, 10).unwrap();
        apply_command(&mut tablet, &enqueue, 2, 1);
        let maintain = command(
            "maintain",
            20,
            QueueTabletOperation::Maintain(QueueMaintainCommand { partition: 0 }),
        );
        apply_command(&mut tablet, &maintain, 2, 2);
        assert_eq!(tablet.counts().expired, 1);
        assert_eq!(tablet.counts().ready, 0);
        assert_eq!(tablet.counts().scheduled, 0);
    }
}

#[test]
fn nonzero_jitter_and_followup_after_rejection_are_deterministic() {
    let mut jittered = config();
    jittered.retry.max_attempts = 3;
    jittered.retry.initial_delay_ms = 100;
    jittered.retry.max_delay_ms = 100;
    jittered.retry.jitter_percent = 50;
    let mut tablets = [
        QueueTablet::new(scope(), jittered.clone()).unwrap(),
        QueueTablet::new(scope(), jittered.clone()).unwrap(),
        QueueTablet::new(scope(), jittered).unwrap(),
    ];
    let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", 1), 1).unwrap();
    apply_all(&mut tablets, &enqueue, 2, 1);
    let acquire = command(
        "acquire",
        2,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(10),
        }),
    );
    let acquired = apply_all(&mut tablets, &acquire, 2, 2);
    let token = acquired_delivery(&acquired[0]).lease_token.clone();
    let nack = command(
        "nack",
        3,
        QueueTabletOperation::Nack(QueueNackCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            lease_token: token,
            reason: "retry".into(),
        }),
    );
    apply_all(&mut tablets, &nack, 2, 3);
    let states: Vec<_> = tablets
        .iter()
        .map(|tablet| tablet.state.queue.get("one").unwrap().state)
        .collect();
    assert!(states.windows(2).all(|pair| pair[0] == pair[1]));
    assert!(matches!(
        states[0],
        QueueState::Scheduled { eligible_at_ms } if eligible_at_ms > 3
    ));
    assert!(
        tablets
            .windows(2)
            .all(|pair| pair[0].state_digest() == pair[1].state_digest())
    );

    let missing_redrive = command(
        "missing-redrive",
        4,
        QueueTabletOperation::Redrive(QueueRedriveCommand {
            partition: 0,
            message_id: "missing".into(),
            dead_letter_history_id: 1,
        }),
    );
    let rejected = apply_command(&mut tablets[0], &missing_redrive, 2, 4);
    assert_rejected(&rejected, QueueTabletRejectionCode::NotFound);
    let followup = QueueTabletCommand::enqueue(&scope(), "followup", event("two", 5), 5).unwrap();
    let applied = apply_command(&mut tablets[0], &followup, 2, 5);
    assert!(matches!(
        applied.outcome,
        QueueTabletOutcome::Applied { .. }
    ));
    assert_eq!(tablets[0].last_applied_time_ms(), 5);
    assert_eq!(tablets[0].applied_command_count(), 5);
}

#[test]
fn receipt_json_keeps_nested_u64_values_browser_safe() {
    let large = 9_007_199_254_740_993_u64;
    let mut tablet = QueueTablet::new(scope(), config()).unwrap();
    let enqueue =
        QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", large), large).unwrap();
    apply_command(&mut tablet, &enqueue, large, large);
    let acquire = command(
        "acquire",
        large + 1,
        QueueTabletOperation::Acquire(QueueAcquireCommand {
            partition: 0,
            consumer: "worker".into(),
            consumer_epoch: 1,
            max_messages: 1,
            visibility_timeout_ms: Some(50),
        }),
    );
    let receipt = apply_command(&mut tablet, &acquire, large + 2, large + 3);
    let document = serde_json::to_value(receipt).unwrap();
    assert_eq!(document["term"], (large + 2).to_string());
    assert_eq!(document["commit_index"], (large + 3).to_string());
    assert_eq!(document["applied_at_ms"], (large + 1).to_string());
    assert_eq!(
        document["outcome"]["result"]["deliveries"][0]["envelope"]["time_ms"],
        large.to_string()
    );
    assert_eq!(
        document["outcome"]["result"]["deliveries"][0]["lease_deadline_ms"],
        (large + 51).to_string()
    );
    assert!(document.to_string().find("queue_commit_position").is_none());
}

#[test]
fn digest_hashes_complete_outcomes_and_has_a_golden_state_vector() {
    let first = QueueTabletOutcome::Applied {
        result: QueueTabletOperationResult::Enqueued {
            message_id: "one".into(),
            duplicate: false,
        },
    };
    let second = QueueTabletOutcome::Applied {
        result: QueueTabletOperationResult::Enqueued {
            message_id: "two".into(),
            duplicate: false,
        },
    };
    let commit = committed(1, 2, 3, b"payload");
    assert_ne!(
        transition_digest([0; 32], commit, [1; 32], 7, b"aux", &first),
        transition_digest([0; 32], commit, [1; 32], 7, b"aux", &second)
    );

    let mut tablet = QueueTablet::new(scope(), config()).unwrap();
    let enqueue = QueueTabletCommand::enqueue(&scope(), "enqueue", event("one", 10), 10).unwrap();
    apply_command(&mut tablet, &enqueue, 2, 1);
    assert_eq!(
        tablet.state_digest(),
        [
            0xa0, 0x0c, 0x37, 0x2f, 0xa4, 0x7a, 0x65, 0x52, 0x15, 0xf1, 0x00, 0x92, 0x15, 0x4a,
            0xe3, 0x8f, 0x8a, 0x16, 0xf5, 0x9b, 0x62, 0x76, 0x61, 0x83, 0xf6, 0xd7, 0x65, 0x3e,
            0x71, 0x86, 0x32, 0x9e,
        ]
    );
}
