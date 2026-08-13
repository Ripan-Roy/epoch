use std::collections::BTreeSet;

use epoch_tablet::{
    CommittedCommand, STREAM_TABLET_SESSION_COMMAND_FORMAT_VERSION, StreamTablet,
    StreamTabletCommand, StreamTabletMutationReceipt, StreamTabletScope,
    StreamTabletSessionDisposition, StreamTabletSessionOutcome, StreamTabletSessionReceipt,
    StreamTabletSessionRejection, TabletError,
};
use serde_json::json;

fn scope() -> StreamTabletScope {
    StreamTabletScope::new(7, 3, "orders").unwrap()
}

fn committed(proposal_id: u64, log_index: u64, payload: &[u8]) -> CommittedCommand<'_> {
    CommittedCommand {
        group_id: 7,
        group_epoch: 3,
        proposal_id,
        term: 2,
        log_index,
        payload,
    }
}

fn apply_session(
    tablet: &mut StreamTablet,
    command: &StreamTabletCommand,
    log_index: u64,
) -> StreamTabletSessionReceipt {
    let proposal_id = command.proposal_id(&scope()).unwrap();
    let payload = command.encode(&scope()).unwrap();
    let StreamTabletMutationReceipt::Session(receipt) = tablet
        .apply_mutation(committed(proposal_id, log_index, &payload))
        .unwrap()
    else {
        panic!("session command must return session evidence");
    };
    receipt
}

#[test]
fn v5_join_command_is_canonical_and_kind_locked() {
    let command = StreamTabletCommand::join_group_session(
        &scope(),
        "join-a",
        "billing",
        "worker-a",
        3,
        30_000,
        1_000,
    )
    .unwrap();
    assert_eq!(
        command.format_version,
        STREAM_TABLET_SESSION_COMMAND_FORMAT_VERSION
    );
    let encoded = command.encode(&scope()).unwrap();
    assert_eq!(
        String::from_utf8(encoded.clone()).unwrap(),
        r#"{"format_version":5,"tablet_id":7,"tablet_epoch":3,"resource":"orders","idempotency_key":"join-a","applied_at_ms":1000,"operation":{"kind":"group_session","group":"billing","shard_count":3,"action":{"kind":"join","member_id":"worker-a","session_timeout_ms":30000}}}"#
    );
    assert_eq!(
        StreamTabletCommand::decode(&encoded, &scope()).unwrap(),
        command
    );

    let mut wrong_version: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    wrong_version["format_version"] = json!(4);
    assert!(matches!(
        StreamTabletCommand::decode(&serde_json::to_vec(&wrong_version).unwrap(), &scope()),
        Err(TabletError::InvalidCommand(_))
    ));
}

#[test]
fn join_heartbeat_leave_and_rebalance_are_deterministic() {
    let join_a = StreamTabletCommand::join_group_session(
        &scope(),
        "join-a",
        "billing",
        "worker-a",
        3,
        30_000,
        1_000,
    )
    .unwrap();
    let join_b = StreamTabletCommand::join_group_session(
        &scope(),
        "join-b",
        "billing",
        "worker-b",
        3,
        30_000,
        2_000,
    )
    .unwrap();
    let heartbeat_a = StreamTabletCommand::heartbeat_group_session(
        &scope(),
        "heartbeat-a",
        "billing",
        "worker-a",
        3,
        2,
        3_000,
    )
    .unwrap();
    let leave_a = StreamTabletCommand::leave_group_session(
        &scope(),
        "leave-a",
        "billing",
        "worker-a",
        3,
        2,
        4_000,
    )
    .unwrap();
    let stale_heartbeat = StreamTabletCommand::heartbeat_group_session(
        &scope(),
        "heartbeat-a-stale",
        "billing",
        "worker-a",
        3,
        2,
        5_000,
    )
    .unwrap();

    let mut tablet = StreamTablet::new(scope()).unwrap();
    let first = apply_session(&mut tablet, &join_a, 1);
    assert_eq!(first.outcome, StreamTabletSessionOutcome::Applied);
    assert_eq!(first.group_generation, 1);
    assert_eq!(first.assigned_shards, [0, 1, 2]);

    let second = apply_session(&mut tablet, &join_b, 2);
    assert_eq!(second.group_generation, 2);
    assert_eq!(second.assigned_shards, [1]);
    let observation = tablet.session_observation("billing").unwrap();
    assert_eq!(observation.group_generation, Some(2));
    assert_eq!(observation.members[0].member_id, "worker-a");
    assert_eq!(observation.members[0].assigned_shards, [0, 2]);
    assert_eq!(observation.members[1].member_id, "worker-b");
    assert_eq!(observation.members[1].assigned_shards, [1]);

    let heartbeat = apply_session(&mut tablet, &heartbeat_a, 3);
    assert_eq!(heartbeat.group_generation, 2);
    assert_eq!(heartbeat.assigned_shards, [0, 2]);
    assert_eq!(heartbeat.members[0].deadline_ms, 33_000);

    let left = apply_session(&mut tablet, &leave_a, 4);
    assert_eq!(left.group_generation, 3);
    assert!(left.assigned_shards.is_empty());
    assert_eq!(left.members.len(), 1);
    assert_eq!(left.members[0].member_id, "worker-b");
    assert_eq!(left.members[0].assigned_shards, [0, 1, 2]);

    let rejected = apply_session(&mut tablet, &stale_heartbeat, 5);
    assert_eq!(rejected.outcome, StreamTabletSessionOutcome::Rejected);
    assert_eq!(
        rejected.rejection,
        Some(StreamTabletSessionRejection::UnknownMember)
    );
    assert_eq!(rejected.group_generation, 3);

    let expected_digest = tablet.state_digest();
    let mut rebuilt = StreamTablet::new(scope()).unwrap();
    for (index, command) in [&join_a, &join_b, &heartbeat_a, &leave_a, &stale_heartbeat]
        .into_iter()
        .enumerate()
    {
        apply_session(&mut rebuilt, command, index as u64 + 1);
    }
    assert_eq!(
        rebuilt.session_observation("billing").unwrap(),
        tablet.session_observation("billing").unwrap()
    );
    assert_eq!(rebuilt.state_digest(), expected_digest);
}

#[test]
fn inclusive_expiry_rebalances_once_and_time_never_moves_backwards() {
    let join_a = StreamTabletCommand::join_group_session(
        &scope(),
        "join-a",
        "billing",
        "worker-a",
        4,
        1_000,
        10_000,
    )
    .unwrap();
    let join_b = StreamTabletCommand::join_group_session(
        &scope(),
        "join-b",
        "billing",
        "worker-b",
        4,
        5_000,
        10_500,
    )
    .unwrap();
    let maintain = StreamTabletCommand::maintain_group_sessions(
        &scope(),
        "maintain-11s",
        "billing",
        4,
        11_000,
    )
    .unwrap();
    let rollback = StreamTabletCommand::maintain_group_sessions(
        &scope(),
        "maintain-rollback",
        "billing",
        4,
        9_000,
    )
    .unwrap();

    let mut tablet = StreamTablet::new(scope()).unwrap();
    apply_session(&mut tablet, &join_a, 1);
    apply_session(&mut tablet, &join_b, 2);
    let expired = apply_session(&mut tablet, &maintain, 3);
    assert_eq!(expired.group_generation, 3);
    assert_eq!(expired.expired_members, ["worker-a"]);
    assert_eq!(expired.members[0].assigned_shards, [0, 1, 2, 3]);
    assert_eq!(expired.watermark_ms, 11_000);

    let no_rollback = apply_session(&mut tablet, &rollback, 4);
    assert_eq!(no_rollback.group_generation, 3);
    assert!(no_rollback.expired_members.is_empty());
    assert_eq!(no_rollback.watermark_ms, 11_000);
}

#[test]
fn session_state_and_exact_retry_survive_native_snapshot_restore() {
    let join = StreamTabletCommand::join_group_session(
        &scope(),
        "join-a",
        "billing",
        "worker-a",
        3,
        30_000,
        1_000,
    )
    .unwrap();
    let proposal_id = join.proposal_id(&scope()).unwrap();
    let payload = join.encode(&scope()).unwrap();
    let mut tablet = StreamTablet::new(scope()).unwrap();
    apply_session(&mut tablet, &join, 1);
    let expected = tablet.session_observation("billing").unwrap();
    let expected_digest = tablet.state_digest();

    let snapshot = tablet
        .encode_snapshot(&BTreeSet::from([proposal_id]))
        .unwrap();
    let mut restored = StreamTablet::decode_snapshot(&scope(), &snapshot).unwrap();
    assert_eq!(restored.session_observation("billing").unwrap(), expected);
    assert_eq!(restored.state_digest(), expected_digest);

    let StreamTabletMutationReceipt::Session(replayed) = restored
        .apply_mutation(committed(proposal_id, 1, &payload))
        .unwrap()
    else {
        panic!("session retry must return session evidence");
    };
    assert_eq!(
        replayed.disposition,
        StreamTabletSessionDisposition::Replayed
    );
    assert_eq!(restored.state_digest(), expected_digest);
}

#[test]
fn session_static_bounds_fail_before_proposal() {
    for command in [
        StreamTabletCommand::join_group_session(
            &scope(),
            "invalid",
            "",
            "worker-a",
            3,
            30_000,
            1_000,
        ),
        StreamTabletCommand::join_group_session(
            &scope(),
            "invalid",
            "billing",
            "",
            3,
            30_000,
            1_000,
        ),
        StreamTabletCommand::join_group_session(
            &scope(),
            "invalid",
            "billing",
            "worker-a",
            0,
            30_000,
            1_000,
        ),
        StreamTabletCommand::join_group_session(
            &scope(),
            "invalid",
            "billing",
            "worker-a",
            3,
            999,
            1_000,
        ),
        StreamTabletCommand::heartbeat_group_session(
            &scope(),
            "invalid",
            "billing",
            "worker-a",
            3,
            0,
            1_000,
        ),
    ] {
        assert!(matches!(command, Err(TabletError::InvalidCommand(_))));
    }
}

#[test]
fn deadline_overflow_is_a_committed_rejection_without_a_phantom_group() {
    let join = StreamTabletCommand::join_group_session(
        &scope(),
        "overflowing-join",
        "billing",
        "worker-a",
        3,
        1_000,
        u64::MAX,
    )
    .unwrap();
    let mut tablet = StreamTablet::new(scope()).unwrap();

    let rejected = apply_session(&mut tablet, &join, 1);

    assert_eq!(rejected.outcome, StreamTabletSessionOutcome::Rejected);
    assert_eq!(
        rejected.rejection,
        Some(StreamTabletSessionRejection::DeadlineOverflow)
    );
    assert_eq!(rejected.group_generation, 0);
    assert!(!tablet.session_observation("billing").unwrap().exists);

    let snapshot = tablet.encode_snapshot(&BTreeSet::new()).unwrap();
    StreamTablet::decode_snapshot(&scope(), &snapshot).unwrap();
}
