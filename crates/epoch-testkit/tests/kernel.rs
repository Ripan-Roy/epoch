use epoch_testkit::{
    FaultAction, FaultPlan, FaultPoint, PeerId, PeerTransport, SeededScheduler, SendOutcome, Trace,
    VirtualClock,
};

#[test]
fn scheduler_preserves_insertion_order_at_the_same_deadline() {
    let mut scheduler = SeededScheduler::new(7);
    scheduler.schedule_at(10, "first").unwrap();
    scheduler.schedule_at(10, "second").unwrap();
    scheduler.schedule_at(10, "third").unwrap();

    assert_eq!(scheduler.pop_next().unwrap().unwrap().value, "first");
    assert_eq!(scheduler.pop_next().unwrap().unwrap().value, "second");
    assert_eq!(scheduler.pop_next().unwrap().unwrap().value, "third");
}

#[test]
fn same_seed_produces_the_same_trace_and_digest() {
    fn scenario(seed: u64) -> Trace {
        let mut transport = PeerTransport::new(seed);
        for payload in [b"one".as_slice(), b"two", b"three"] {
            transport
                .send_with_jitter(PeerId::new(1), PeerId::new(2), payload, 20)
                .unwrap();
        }
        while transport.deliver_next().unwrap().is_some() {}
        transport.into_trace()
    }

    let first = scenario(0x5eed);
    let second = scenario(0x5eed);

    assert_eq!(first.to_bytes().unwrap(), second.to_bytes().unwrap());
    assert_eq!(first.digest().unwrap(), second.digest().unwrap());
    assert_eq!(first.digest().unwrap().value(), 0xa8a3_98aa_a651_da3c);
    assert_eq!(
        Trace::from_bytes(&first.to_bytes().unwrap()).unwrap(),
        first
    );
}

#[test]
fn a_wall_clock_jump_never_moves_monotonic_time_backwards() {
    let mut clock = VirtualClock::new(10_000);
    clock.advance(250).unwrap();
    let before_jump = clock.snapshot();

    clock.jump_wall(-8_000).unwrap();
    let after_jump = clock.snapshot();
    assert_eq!(after_jump.monotonic_ms, before_jump.monotonic_ms);
    assert_eq!(after_jump.wall_time_ms, 2_250);

    clock.advance(25).unwrap();
    assert_eq!(clock.monotonic_ms(), before_jump.monotonic_ms + 25);
}

#[test]
fn a_partition_drops_the_minority_but_not_majority_links() {
    let one = PeerId::new(1);
    let two = PeerId::new(2);
    let three = PeerId::new(3);
    let mut transport = PeerTransport::new(1);
    transport.partition(&[one], &[two, three]).unwrap();

    assert!(matches!(
        transport.send(one, two, b"minority").unwrap(),
        SendOutcome::Dropped { .. }
    ));
    assert!(matches!(
        transport.send(two, one, b"minority-reply").unwrap(),
        SendOutcome::Dropped { .. }
    ));
    assert!(matches!(
        transport.send(two, three, b"majority").unwrap(),
        SendOutcome::Scheduled { copies: 1, .. }
    ));

    let delivered = transport.deliver_next().unwrap().unwrap();
    assert_eq!(delivered.from, two);
    assert_eq!(delivered.to, three);
    assert_eq!(delivered.payload, b"majority");
    assert!(transport.deliver_next().unwrap().is_none());
}

#[test]
fn a_one_way_link_block_does_not_block_the_reverse_link() {
    let one = PeerId::new(1);
    let two = PeerId::new(2);
    let mut transport = PeerTransport::new(1);

    assert!(transport.block_link(one, two).unwrap());
    assert!(matches!(
        transport.send(one, two, b"blocked").unwrap(),
        SendOutcome::Dropped { .. }
    ));
    assert!(matches!(
        transport.send(two, one, b"reverse").unwrap(),
        SendOutcome::Scheduled { copies: 1, .. }
    ));

    let delivered = transport.deliver_next().unwrap().unwrap();
    assert_eq!(delivered.from, two);
    assert_eq!(delivered.to, one);
    assert_eq!(delivered.payload, b"reverse");

    assert!(transport.heal_link(one, two).unwrap());
    assert!(matches!(
        transport.send(one, two, b"healed").unwrap(),
        SendOutcome::Scheduled { copies: 1, .. }
    ));
}

#[test]
fn a_scripted_fault_fires_on_only_the_requested_occurrence() {
    let point = FaultPoint::new("disk.segment.write").unwrap();
    let mut plan = FaultPlan::new();
    plan.add(point.clone(), 2, FaultAction::PartialWrite { bytes: 3 })
        .unwrap();

    let first = plan.trigger(&point);
    let second = plan.trigger(&point);
    let third = plan.trigger(&point);

    assert_eq!(first.occurrence, 1);
    assert_eq!(first.action, None);
    assert_eq!(second.occurrence, 2);
    assert_eq!(second.action, Some(FaultAction::PartialWrite { bytes: 3 }));
    assert_eq!(third.occurrence, 3);
    assert_eq!(third.action, None);
}

#[test]
fn fault_plan_represents_every_required_fault_action() {
    let actions = [
        FaultAction::Crash,
        FaultAction::IoError,
        FaultAction::PartialWrite { bytes: 7 },
        FaultAction::Drop,
        FaultAction::Delay { by_ms: 9 },
        FaultAction::Duplicate {
            additional_copies: 2,
            spacing_ms: 3,
        },
    ];
    let mut plan = FaultPlan::new();
    for (index, action) in actions.iter().copied().enumerate() {
        let point = FaultPoint::new(format!("point.{index}")).unwrap();
        plan.add(point.clone(), 1, action).unwrap();
        assert_eq!(plan.trigger(&point).action, Some(action));
    }
}

#[test]
fn duplicate_and_delay_faults_create_duplicates_and_reordering() {
    let mut plan = FaultPlan::new();
    plan.add(
        FaultPoint::transport_send(),
        1,
        FaultAction::Delay { by_ms: 10 },
    )
    .unwrap();
    plan.add(
        FaultPoint::transport_send(),
        2,
        FaultAction::Duplicate {
            additional_copies: 1,
            spacing_ms: 2,
        },
    )
    .unwrap();

    let mut transport = PeerTransport::with_fault_plan(9, plan);
    transport
        .send(PeerId::new(1), PeerId::new(2), b"delayed")
        .unwrap();
    transport
        .send(PeerId::new(3), PeerId::new(2), b"duplicated")
        .unwrap();

    let first = transport.deliver_next().unwrap().unwrap();
    let second = transport.deliver_next().unwrap().unwrap();
    let third = transport.deliver_next().unwrap().unwrap();

    assert_eq!(first.payload, b"duplicated");
    assert_eq!(first.copy_index, 0);
    assert_eq!(second.payload, b"duplicated");
    assert_eq!(second.copy_index, 1);
    assert_eq!(third.payload, b"delayed");
    assert!(first.delivered_at_ms < second.delivered_at_ms);
    assert!(second.delivered_at_ms < third.delivered_at_ms);
}
