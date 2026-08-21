use std::collections::BTreeSet;

use epoch_core::EventEnvelope;
use epoch_stream::{
    StreamCaptureFormat, StreamOffsetCommit, StreamProducerDisposition, StreamTransactionStatus,
};
use epoch_tablet::{
    CommittedCommand, STREAM_TABLET_STATE_COMMAND_FORMAT_VERSION, StreamStateCommand, StreamTablet,
    StreamTabletCommand, StreamTabletMutationReceipt, StreamTabletScope,
    StreamTabletStateDisposition, StreamTabletStateRejectionCode, StreamTabletStateResult,
    TabletError,
};
use serde_json::json;

fn scope() -> StreamTabletScope {
    StreamTabletScope::new(7, 3, "orders").unwrap()
}

fn event(id: &str, key: Option<&str>, value: i64) -> EventEnvelope {
    let mut envelope = EventEnvelope::new(id, "order.updated", json!({"value": value}), 1);
    id.clone_into(&mut envelope.id);
    envelope.key = key.map(str::to_owned);
    envelope
}

fn apply(
    tablet: &mut StreamTablet,
    command: &StreamTabletCommand,
    term: u64,
    index: u64,
) -> StreamTabletMutationReceipt {
    let scope = scope();
    let payload = command.encode(&scope).unwrap();
    tablet
        .apply_mutation(CommittedCommand {
            group_id: scope.consensus_group_id,
            group_epoch: scope.tablet_epoch,
            proposal_id: command.proposal_id(&scope).unwrap(),
            term,
            log_index: index,
            payload: &payload,
        })
        .unwrap()
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the integration test deliberately exercises one complete replicated and recovered history"
)]
fn producer_transactions_tiering_and_snapshot_recovery_are_one_replicated_history() {
    let scope = scope();
    let mut tablet = StreamTablet::new_with_cluster_id(scope.clone(), "west").unwrap();
    let commands = [
        StreamTabletCommand::state(
            &scope,
            "producer-append",
            StreamStateCommand::AppendIdempotent {
                producer_id: "checkout".into(),
                producer_epoch: 1,
                sequence: 0,
                partition: 0,
                envelope: Box::new(event("one", Some("order-1"), 1)),
            },
            10,
        )
        .unwrap(),
        StreamTabletCommand::state(
            &scope,
            "capture-schedule",
            StreamStateCommand::ConfigureCaptureSchedule {
                schedule_id: "analytics".into(),
                partition: 0,
                interval_ms: 1_000,
                format: StreamCaptureFormat::JsonLines,
            },
            16,
        )
        .unwrap(),
        StreamTabletCommand::state(
            &scope,
            "capture-maintenance",
            StreamStateCommand::MaintainCaptureSchedule {
                schedule_id: "analytics".into(),
            },
            1_016,
        )
        .unwrap(),
        StreamTabletCommand::state(
            &scope,
            "producer-retry",
            StreamStateCommand::AppendIdempotent {
                producer_id: "checkout".into(),
                producer_epoch: 1,
                sequence: 0,
                partition: 0,
                envelope: Box::new(event("one", Some("order-1"), 1)),
            },
            11,
        )
        .unwrap(),
        StreamTabletCommand::state(
            &scope,
            "tx-begin",
            StreamStateCommand::BeginTransaction {
                transaction_id: "tx-1".into(),
                producer_id: "checkout".into(),
                producer_epoch: 1,
            },
            12,
        )
        .unwrap(),
        StreamTabletCommand::state(
            &scope,
            "tx-append",
            StreamStateCommand::AppendTransaction {
                transaction_id: "tx-1".into(),
                producer_id: "checkout".into(),
                producer_epoch: 1,
                sequence: 1,
                partition: 0,
                envelopes: vec![event("two", Some("order-2"), 2)],
            },
            13,
        )
        .unwrap(),
        StreamTabletCommand::state(
            &scope,
            "tx-commit",
            StreamStateCommand::CommitTransaction {
                transaction_id: "tx-1".into(),
                offset_commit: Some(StreamOffsetCommit {
                    group: "workers".into(),
                    partition: 0,
                    next_offset: 2,
                }),
            },
            14,
        )
        .unwrap(),
        StreamTabletCommand::state(
            &scope,
            "tier-prefix",
            StreamStateCommand::TierPrefix {
                partition: 0,
                before_offset: 1,
                max_records: 1,
            },
            15,
        )
        .unwrap(),
    ];

    let first = apply(&mut tablet, &commands[0], 1, 1);
    let retry = apply(&mut tablet, &commands[3], 1, 2);
    assert!(matches!(
        first,
        StreamTabletMutationReceipt::State(ref receipt)
            if matches!(receipt.result, StreamTabletStateResult::ProducerAppend(ref result)
                if result.disposition == StreamProducerDisposition::New)
    ));
    assert!(matches!(
        retry,
        StreamTabletMutationReceipt::State(ref receipt)
            if matches!(receipt.result, StreamTabletStateResult::ProducerAppend(ref result)
                if result.disposition == StreamProducerDisposition::Duplicate)
    ));

    apply(&mut tablet, &commands[4], 1, 3);
    apply(&mut tablet, &commands[5], 1, 4);
    assert_eq!(tablet.fetch(0, 10).unwrap().len(), 1);
    assert_eq!(tablet.fetch_uncommitted(0, 10).unwrap().len(), 2);
    apply(&mut tablet, &commands[6], 1, 5);
    assert_eq!(tablet.fetch(0, 10).unwrap().len(), 2);
    assert_eq!(
        tablet.transaction("tx-1").unwrap().status,
        StreamTransactionStatus::Committed
    );
    assert_eq!(
        tablet
            .group_observation("workers")
            .unwrap()
            .committed_offset,
        2
    );
    apply(&mut tablet, &commands[7], 1, 6);
    assert_eq!(tablet.tier_objects().len(), 1);
    assert_eq!(tablet.fetch(0, 10).unwrap().len(), 2);
    apply(&mut tablet, &commands[1], 1, 7);
    assert_eq!(tablet.due_capture_schedules(1_015), []);
    assert_eq!(
        tablet.due_capture_schedules(1_016),
        [(1_016, "analytics".into())]
    );
    apply(&mut tablet, &commands[2], 1, 8);
    assert_eq!(tablet.capture_schedule("analytics").unwrap().next_offset, 2);
    assert_eq!(
        tablet
            .capture_artifact("auto-analytics-00000000000000001016")
            .unwrap()
            .record_count,
        2
    );

    let retained = commands
        .iter()
        .map(|command| command.proposal_id(&scope).unwrap())
        .collect::<BTreeSet<_>>();
    let snapshot = tablet.encode_snapshot(&retained).unwrap();
    let restored = StreamTablet::decode_snapshot(&scope, &snapshot).unwrap();
    assert_eq!(restored.fetch(0, 10).unwrap(), tablet.fetch(0, 10).unwrap());
    assert_eq!(restored.tier_objects(), tablet.tier_objects());
    assert_eq!(restored.transaction("tx-1"), tablet.transaction("tx-1"));
    assert_eq!(restored.state_digest(), tablet.state_digest());

    let replayed = apply(&mut tablet, &commands[7], 1, 6);
    assert!(matches!(
        replayed,
        StreamTabletMutationReceipt::State(receipt)
            if receipt.disposition == StreamTabletStateDisposition::Replayed
    ));
}

#[test]
fn v7_state_command_is_canonical_version_and_kind_locked() {
    let scope = scope();
    let command = StreamTabletCommand::state(
        &scope,
        "state-golden",
        StreamStateCommand::AppendIdempotent {
            producer_id: "checkout".into(),
            producer_epoch: 1,
            sequence: 0,
            partition: 0,
            envelope: Box::new(event("one", Some("order-1"), 1)),
        },
        10,
    )
    .unwrap();
    assert_eq!(
        command.format_version,
        STREAM_TABLET_STATE_COMMAND_FORMAT_VERSION
    );

    let encoded = command.encode(&scope).unwrap();
    assert_eq!(
        String::from_utf8(encoded.clone()).unwrap(),
        r#"{"format_version":7,"tablet_id":7,"tablet_epoch":3,"resource":"orders","idempotency_key":"state-golden","applied_at_ms":10,"operation":{"kind":"state","action":"append_idempotent","producer_id":"checkout","producer_epoch":"1","sequence":"0","partition":0,"envelope":{"id":"one","source":"one","type":"order.updated","time_ms":1,"key":"order-1","headers":{},"content_type":"application/json","payload":{"value":1},"priority":0,"extensions":{}}}}"#
    );
    assert_eq!(
        StreamTabletCommand::decode(&encoded, &scope).unwrap(),
        command
    );

    let mut wrong_version: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    wrong_version["format_version"] = json!(6);
    assert!(matches!(
        StreamTabletCommand::decode(&serde_json::to_vec(&wrong_version).unwrap(), &scope),
        Err(TabletError::InvalidCommand(_))
    ));

    let mut wrong_kind: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    wrong_kind["operation"]["kind"] = json!("append");
    assert!(matches!(
        StreamTabletCommand::decode(&serde_json::to_vec(&wrong_kind).unwrap(), &scope),
        Err(TabletError::Decoding(_))
    ));

    let pretty = serde_json::to_vec_pretty(&command).unwrap();
    assert!(matches!(
        StreamTabletCommand::decode(&pretty, &scope),
        Err(TabletError::Decoding(_))
    ));
}

#[test]
fn state_commands_validate_before_consensus() {
    let scope = scope();
    let command = StreamTabletCommand::state(
        &scope,
        "invalid-partition",
        StreamStateCommand::Compact {
            partition: 1,
            tombstone_retention_ms: 1,
        },
        1,
    );
    assert!(command.is_err());
}

#[test]
fn committed_state_rejections_are_replayable_and_do_not_mutate_or_stop_the_tablet() {
    let scope = scope();
    let mut tablet = StreamTablet::new_with_cluster_id(scope.clone(), "west").unwrap();
    let gap = StreamTabletCommand::state(
        &scope,
        "producer-gap",
        StreamStateCommand::AppendIdempotent {
            producer_id: "checkout".into(),
            producer_epoch: 1,
            sequence: 1,
            partition: 0,
            envelope: Box::new(event("gap", Some("order-gap"), 1)),
        },
        10,
    )
    .unwrap();

    let rejected = apply(&mut tablet, &gap, 1, 1);
    assert!(matches!(
        rejected,
        StreamTabletMutationReceipt::State(ref receipt)
            if receipt.disposition == StreamTabletStateDisposition::New
                && matches!(receipt.result, StreamTabletStateResult::Rejected(ref rejection)
                    if rejection.code == StreamTabletStateRejectionCode::Conflict)
    ));
    assert!(tablet.fetch(0, 10).unwrap().is_empty());

    let replayed = apply(&mut tablet, &gap, 1, 1);
    assert!(matches!(
        replayed,
        StreamTabletMutationReceipt::State(ref receipt)
            if receipt.disposition == StreamTabletStateDisposition::Replayed
                && matches!(receipt.result, StreamTabletStateResult::Rejected(ref rejection)
                    if rejection.code == StreamTabletStateRejectionCode::Conflict)
    ));

    let valid = StreamTabletCommand::state(
        &scope,
        "producer-zero-after-gap",
        StreamStateCommand::AppendIdempotent {
            producer_id: "checkout".into(),
            producer_epoch: 1,
            sequence: 0,
            partition: 0,
            envelope: Box::new(event("valid", Some("order-1"), 2)),
        },
        11,
    )
    .unwrap();
    let accepted = apply(&mut tablet, &valid, 1, 2);
    assert!(matches!(
        accepted,
        StreamTabletMutationReceipt::State(ref receipt)
            if matches!(receipt.result, StreamTabletStateResult::ProducerAppend(_))
    ));
    assert_eq!(tablet.fetch(0, 10).unwrap().len(), 1);

    let retained = [
        gap.proposal_id(&scope).unwrap(),
        valid.proposal_id(&scope).unwrap(),
    ]
    .into_iter()
    .collect();
    let snapshot = tablet.encode_snapshot(&retained).unwrap();
    let mut restored = StreamTablet::decode_snapshot(&scope, &snapshot).unwrap();
    let restored_rejection = apply(&mut restored, &gap, 1, 1);
    assert!(matches!(
        restored_rejection,
        StreamTabletMutationReceipt::State(ref receipt)
            if receipt.disposition == StreamTabletStateDisposition::Replayed
                && matches!(receipt.result, StreamTabletStateResult::Rejected(ref rejection)
                    if rejection.code == StreamTabletStateRejectionCode::Conflict)
    ));
}
