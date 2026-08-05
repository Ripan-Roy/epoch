use epoch_core::EventEnvelope;
use epoch_tablet::{
    CommittedCommand, STREAM_TABLET_GROUP_COMMAND_FORMAT_VERSION, StreamGroupOffsetMode,
    StreamTablet, StreamTabletCommand, StreamTabletGroupDisposition, StreamTabletGroupOutcome,
    StreamTabletGroupReceipt, StreamTabletGroupRejection, StreamTabletMutationReceipt,
    StreamTabletScope, TabletError,
};
use serde_json::json;

fn scope() -> StreamTabletScope {
    StreamTabletScope::new(7, 3, "orders").unwrap()
}

fn event(id: &str) -> EventEnvelope {
    let mut envelope = EventEnvelope::new("group-tests", "order.created", json!({"id": id}), 10);
    envelope.id = id.into();
    envelope
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

fn apply_group(
    tablet: &mut StreamTablet,
    proposal_id: u64,
    log_index: u64,
    payload: &[u8],
) -> StreamTabletGroupReceipt {
    let StreamTabletMutationReceipt::Group(receipt) = tablet
        .apply_mutation(committed(proposal_id, log_index, payload))
        .unwrap()
    else {
        panic!("group command must return group evidence");
    };
    receipt
}

fn group_command(
    key: &str,
    member: &str,
    generation: u64,
    next_offset: u64,
    mode: StreamGroupOffsetMode,
) -> StreamTabletCommand {
    StreamTabletCommand::group_offset(
        &scope(),
        key,
        "billing",
        member,
        generation,
        0,
        next_offset,
        mode,
        12,
    )
    .unwrap()
}

#[test]
fn v3_group_offset_command_is_canonical_and_version_kind_locked() {
    let command = group_command(
        "group-golden",
        "worker-a",
        1,
        2,
        StreamGroupOffsetMode::Commit,
    );
    assert_eq!(
        command.format_version,
        STREAM_TABLET_GROUP_COMMAND_FORMAT_VERSION
    );
    let encoded = command.encode(&scope()).unwrap();
    assert_eq!(
        String::from_utf8(encoded.clone()).unwrap(),
        r#"{"format_version":3,"tablet_id":7,"tablet_epoch":3,"resource":"orders","idempotency_key":"group-golden","applied_at_ms":12,"operation":{"kind":"group_offset","group":"billing","member_id":"worker-a","group_generation":1,"partition":0,"next_offset":2,"mode":"commit"}}"#
    );
    assert_eq!(
        StreamTabletCommand::decode(&encoded, &scope()).unwrap(),
        command
    );

    let mut wrong_version: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    wrong_version["format_version"] = json!(2);
    assert!(matches!(
        StreamTabletCommand::decode(&serde_json::to_vec(&wrong_version).unwrap(), &scope()),
        Err(TabletError::InvalidCommand(_))
    ));
}

#[test]
fn commits_resets_fences_and_exact_replay_are_deterministic() {
    let append_one = StreamTabletCommand::append(&scope(), "append-1", event("one"), 10).unwrap();
    let append_one_id = append_one.proposal_id(&scope()).unwrap();
    let append_one_payload = append_one.encode(&scope()).unwrap();
    let append_two = StreamTabletCommand::append(&scope(), "append-2", event("two"), 11).unwrap();
    let append_two_id = append_two.proposal_id(&scope()).unwrap();
    let append_two_payload = append_two.encode(&scope()).unwrap();

    let first = group_command("group-1", "worker-a", 1, 1, StreamGroupOffsetMode::Commit);
    let first_id = first.proposal_id(&scope()).unwrap();
    let first_payload = first.encode(&scope()).unwrap();
    let wrong_owner = group_command(
        "group-wrong-owner",
        "worker-b",
        1,
        2,
        StreamGroupOffsetMode::Commit,
    );
    let wrong_owner_id = wrong_owner.proposal_id(&scope()).unwrap();
    let wrong_owner_payload = wrong_owner.encode(&scope()).unwrap();
    let reset = group_command(
        "group-reset",
        "worker-b",
        2,
        0,
        StreamGroupOffsetMode::Reset,
    );
    let reset_id = reset.proposal_id(&scope()).unwrap();
    let reset_payload = reset.encode(&scope()).unwrap();
    let stale = group_command(
        "group-stale",
        "worker-a",
        1,
        2,
        StreamGroupOffsetMode::Commit,
    );
    let stale_id = stale.proposal_id(&scope()).unwrap();
    let stale_payload = stale.encode(&scope()).unwrap();

    let history = [
        (append_one_id, append_one_payload.as_slice()),
        (append_two_id, append_two_payload.as_slice()),
        (first_id, first_payload.as_slice()),
        (wrong_owner_id, wrong_owner_payload.as_slice()),
        (reset_id, reset_payload.as_slice()),
        (stale_id, stale_payload.as_slice()),
    ];
    let mut tablet = StreamTablet::new(scope()).unwrap();
    tablet
        .apply(committed(history[0].0, 1, history[0].1))
        .unwrap();
    tablet
        .apply(committed(history[1].0, 2, history[1].1))
        .unwrap();

    let first_receipt = apply_group(&mut tablet, first_id, 3, &first_payload);
    assert_eq!(first_receipt.outcome, StreamTabletGroupOutcome::Applied);
    assert_eq!(first_receipt.committed_offset, 1);
    assert_eq!(first_receipt.lag, 1);

    let digest = tablet.state_digest();
    let replayed = apply_group(&mut tablet, first_id, 3, &first_payload);
    assert_eq!(replayed.disposition, StreamTabletGroupDisposition::Replayed);
    assert_eq!(tablet.state_digest(), digest);

    let wrong_owner_receipt = apply_group(&mut tablet, wrong_owner_id, 4, &wrong_owner_payload);
    assert_eq!(
        wrong_owner_receipt.outcome,
        StreamTabletGroupOutcome::Rejected
    );
    assert_eq!(
        wrong_owner_receipt.rejection,
        Some(StreamTabletGroupRejection::OwnerMismatch)
    );
    assert_eq!(wrong_owner_receipt.committed_offset, 1);

    let reset_receipt = apply_group(&mut tablet, reset_id, 5, &reset_payload);
    assert_eq!(reset_receipt.outcome, StreamTabletGroupOutcome::Applied);
    assert_eq!(reset_receipt.previous_offset, 1);
    assert_eq!(reset_receipt.committed_offset, 0);
    assert_eq!(reset_receipt.lag, 2);

    let stale_receipt = apply_group(&mut tablet, stale_id, 6, &stale_payload);
    assert_eq!(
        stale_receipt.rejection,
        Some(StreamTabletGroupRejection::StaleGeneration)
    );
    let observation = tablet.group_observation("billing").unwrap();
    assert!(observation.exists);
    assert_eq!(observation.member_id.as_deref(), Some("worker-b"));
    assert_eq!(observation.group_generation, Some(2));
    assert_eq!(observation.committed_offset, 0);
    assert_eq!(observation.end_offset, 2);
    assert_eq!(observation.lag, 2);
    assert_eq!(tablet.fetch_for_group("billing", 10).unwrap().len(), 2);

    let expected_digest = tablet.state_digest();
    let mut rebuilt = StreamTablet::new(scope()).unwrap();
    for (index, (proposal_id, payload)) in history.into_iter().enumerate() {
        rebuilt
            .apply_mutation(committed(proposal_id, index as u64 + 1, payload))
            .unwrap();
    }
    assert_eq!(rebuilt.group_observation("billing").unwrap(), observation);
    assert_eq!(rebuilt.state_digest(), expected_digest);
}

#[test]
fn offset_and_generation_rejections_never_mutate_group_state() {
    let append = StreamTabletCommand::append(&scope(), "append", event("one"), 10).unwrap();
    let append_id = append.proposal_id(&scope()).unwrap();
    let append_payload = append.encode(&scope()).unwrap();
    let generation_gap = group_command("gap", "worker-a", 2, 0, StreamGroupOffsetMode::Commit);
    let generation_gap_id = generation_gap.proposal_id(&scope()).unwrap();
    let generation_gap_payload = generation_gap.encode(&scope()).unwrap();
    let beyond_end = group_command("beyond", "worker-a", 1, 2, StreamGroupOffsetMode::Commit);
    let beyond_end_id = beyond_end.proposal_id(&scope()).unwrap();
    let beyond_end_payload = beyond_end.encode(&scope()).unwrap();

    let mut tablet = StreamTablet::new(scope()).unwrap();
    tablet
        .apply(committed(append_id, 1, &append_payload))
        .unwrap();
    for (proposal_id, log_index, payload, rejection) in [
        (
            generation_gap_id,
            2,
            generation_gap_payload.as_slice(),
            StreamTabletGroupRejection::GenerationGap,
        ),
        (
            beyond_end_id,
            3,
            beyond_end_payload.as_slice(),
            StreamTabletGroupRejection::OffsetBeyondEnd,
        ),
    ] {
        let StreamTabletMutationReceipt::Group(receipt) = tablet
            .apply_mutation(committed(proposal_id, log_index, payload))
            .unwrap()
        else {
            panic!("group command must return group evidence");
        };
        assert_eq!(receipt.outcome, StreamTabletGroupOutcome::Rejected);
        assert_eq!(receipt.rejection, Some(rejection));
    }
    assert!(!tablet.group_observation("billing").unwrap().exists);
    assert_eq!(tablet.fetch(0, 10).unwrap().len(), 1);
}

#[test]
fn ordinary_commit_cannot_rewind_an_existing_checkpoint() {
    let append = StreamTabletCommand::append(&scope(), "append", event("one"), 10).unwrap();
    let append_id = append.proposal_id(&scope()).unwrap();
    let append_payload = append.encode(&scope()).unwrap();
    let committed_offset =
        group_command("forward", "worker-a", 1, 1, StreamGroupOffsetMode::Commit);
    let committed_id = committed_offset.proposal_id(&scope()).unwrap();
    let committed_payload = committed_offset.encode(&scope()).unwrap();
    let rewind = group_command("rewind", "worker-a", 1, 0, StreamGroupOffsetMode::Commit);
    let rewind_id = rewind.proposal_id(&scope()).unwrap();
    let rewind_payload = rewind.encode(&scope()).unwrap();

    let mut tablet = StreamTablet::new(scope()).unwrap();
    tablet
        .apply(committed(append_id, 1, &append_payload))
        .unwrap();
    assert_eq!(
        apply_group(&mut tablet, committed_id, 2, &committed_payload).outcome,
        StreamTabletGroupOutcome::Applied
    );
    let rejected = apply_group(&mut tablet, rewind_id, 3, &rewind_payload);
    assert_eq!(
        rejected.rejection,
        Some(StreamTabletGroupRejection::CommitRewind)
    );
    assert_eq!(rejected.committed_offset, 1);
}

#[test]
fn static_group_bounds_fail_before_command_encoding() {
    for (group, member, generation, partition) in [
        ("", "worker", 1, 0),
        ("billing", "", 1, 0),
        ("billing", "worker", 0, 0),
        ("billing", "worker", 1, 1),
    ] {
        assert!(matches!(
            StreamTabletCommand::group_offset(
                &scope(),
                "invalid",
                group,
                member,
                generation,
                partition,
                0,
                StreamGroupOffsetMode::Commit,
                12,
            ),
            Err(TabletError::InvalidCommand(_))
        ));
    }

    let oversized = "x".repeat(257);
    for (group, member) in [
        (oversized.as_str(), "worker"),
        ("billing", oversized.as_str()),
    ] {
        assert!(
            StreamTabletCommand::group_offset(
                &scope(),
                "oversized",
                group,
                member,
                1,
                0,
                0,
                StreamGroupOffsetMode::Commit,
                12,
            )
            .is_err()
        );
    }
}
