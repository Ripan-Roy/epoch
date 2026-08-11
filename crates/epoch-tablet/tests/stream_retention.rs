use epoch_core::EventEnvelope;
use epoch_stream::StreamRetentionPolicy;
use epoch_tablet::{
    CommittedCommand, STREAM_TABLET_RETENTION_COMMAND_FORMAT_VERSION, StreamGroupOffsetMode,
    StreamTablet, StreamTabletCommand, StreamTabletMutationReceipt, StreamTabletRetentionMode,
    StreamTabletScope, TabletError,
};
use serde_json::json;

fn scope() -> StreamTabletScope {
    StreamTabletScope::new(7, 3, "orders").unwrap()
}

fn event(id: &str) -> EventEnvelope {
    let mut envelope = EventEnvelope::new("retention-tests", "order.created", json!({"id": id}), 1);
    envelope.id = id.into();
    envelope
}

fn committed<'a>(
    command: &StreamTabletCommand,
    log_index: u64,
    payload: &'a [u8],
) -> CommittedCommand<'a> {
    CommittedCommand {
        group_id: 7,
        group_epoch: 3,
        proposal_id: command.proposal_id(&scope()).unwrap(),
        term: 2,
        log_index,
        payload,
    }
}

fn apply(
    tablet: &mut StreamTablet,
    command: &StreamTabletCommand,
    log_index: u64,
) -> StreamTabletMutationReceipt {
    let payload = command.encode(&scope()).unwrap();
    tablet
        .apply_mutation(committed(command, log_index, &payload))
        .unwrap()
}

#[test]
fn v4_retention_command_is_canonical_and_kind_locked() {
    let command = StreamTabletCommand::configure_retention(
        &scope(),
        "retention-policy-1",
        StreamRetentionPolicy {
            max_records_per_partition: Some(100),
            max_bytes_per_partition: Some(1_048_576),
            max_age_ms: Some(86_400_000),
        },
        123,
    )
    .unwrap();

    assert_eq!(
        command.format_version,
        STREAM_TABLET_RETENTION_COMMAND_FORMAT_VERSION
    );
    let encoded = command.encode(&scope()).unwrap();
    assert_eq!(
        String::from_utf8(encoded.clone()).unwrap(),
        r#"{"format_version":4,"tablet_id":7,"tablet_epoch":3,"resource":"orders","idempotency_key":"retention-policy-1","applied_at_ms":123,"operation":{"kind":"retention","mode":"configure","policy":{"max_records_per_partition":100,"max_bytes_per_partition":1048576,"max_age_ms":86400000}}}"#
    );
    assert_eq!(
        StreamTabletCommand::decode(&encoded, &scope()).unwrap(),
        command
    );

    let mut wrong_version: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    wrong_version["format_version"] = json!(3);
    assert!(matches!(
        StreamTabletCommand::decode(&serde_json::to_vec(&wrong_version).unwrap(), &scope()),
        Err(TabletError::InvalidCommand(_))
    ));
}

#[test]
fn committed_time_and_byte_retention_are_deterministic_and_offset_preserving() {
    let mut tablet = StreamTablet::new(scope()).unwrap();
    let configure_age = StreamTabletCommand::configure_retention(
        &scope(),
        "configure-age",
        StreamRetentionPolicy {
            max_age_ms: Some(10),
            ..StreamRetentionPolicy::default()
        },
        99,
    )
    .unwrap();
    apply(&mut tablet, &configure_age, 1);

    let append_one =
        StreamTabletCommand::append(&scope(), "append-one", event("one"), 100).unwrap();
    let append_two =
        StreamTabletCommand::append(&scope(), "append-two", event("two"), 109).unwrap();
    apply(&mut tablet, &append_one, 2);
    apply(&mut tablet, &append_two, 3);
    let maintain = StreamTabletCommand::maintain_retention(&scope(), "sweep-110", 110).unwrap();
    let StreamTabletMutationReceipt::Retention(receipt) = apply(&mut tablet, &maintain, 4) else {
        panic!("maintenance must return retention evidence");
    };
    assert_eq!(receipt.mode, StreamTabletRetentionMode::Maintain);
    assert_eq!(receipt.removed_records, 1);
    assert_eq!(
        (
            receipt.previous_base_offset,
            receipt.base_offset,
            receipt.end_offset
        ),
        (0, 1, 2)
    );
    assert_eq!(tablet.fetch(1, 10).unwrap()[0].envelope.id, "two");

    let retained_bytes = tablet.retention_observation().unwrap().retained_bytes;
    let configure_bytes = StreamTabletCommand::configure_retention(
        &scope(),
        "configure-bytes",
        StreamRetentionPolicy {
            max_bytes_per_partition: Some(retained_bytes),
            ..StreamRetentionPolicy::default()
        },
        111,
    )
    .unwrap();
    apply(&mut tablet, &configure_bytes, 5);
    let append_three =
        StreamTabletCommand::append(&scope(), "append-three", event("six"), 112).unwrap();
    apply(&mut tablet, &append_three, 6);

    let observation = tablet.retention_observation().unwrap();
    assert_eq!((observation.base_offset, observation.end_offset), (2, 3));
    assert!(observation.retained_bytes <= retained_bytes);
}

#[test]
fn retention_exposes_stale_group_checkpoint_and_survives_snapshot_restore() {
    let mut tablet = StreamTablet::new(scope()).unwrap();
    let configure = StreamTabletCommand::configure_retention(
        &scope(),
        "configure-one-record",
        StreamRetentionPolicy {
            max_records_per_partition: Some(1),
            ..StreamRetentionPolicy::default()
        },
        1,
    )
    .unwrap();
    let append_one = StreamTabletCommand::append(&scope(), "append-one", event("one"), 2).unwrap();
    let commit = StreamTabletCommand::group_offset(
        &scope(),
        "commit-zero",
        "billing",
        "worker-a",
        1,
        0,
        0,
        StreamGroupOffsetMode::Commit,
        3,
    )
    .unwrap();
    let append_two = StreamTabletCommand::append(&scope(), "append-two", event("two"), 4).unwrap();
    for (index, command) in [&configure, &append_one, &commit, &append_two]
        .into_iter()
        .enumerate()
    {
        apply(&mut tablet, command, index as u64 + 1);
    }

    let checkpoint = tablet.group_observation("billing").unwrap();
    assert_eq!(
        (checkpoint.base_offset, checkpoint.committed_offset),
        (1, 0)
    );
    assert!(checkpoint.checkpoint_out_of_range);
    assert!(tablet.fetch_for_group("billing", 10).is_err());

    let encoded = tablet.encode_snapshot(&BTreeSet::default()).unwrap();
    let restored = StreamTablet::decode_snapshot(&scope(), &encoded).unwrap();
    assert_eq!(
        restored.retention_observation().unwrap(),
        tablet.retention_observation().unwrap()
    );
    assert_eq!(restored.group_observation("billing").unwrap(), checkpoint);
    assert_eq!(restored.state_digest(), tablet.state_digest());
}
use std::collections::BTreeSet;
